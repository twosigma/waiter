;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns waiter.token
  (:require [clj-time.coerce :as tc]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [schema.core :as s]
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (org.joda.time DateTime)))

(def ^:const ANY-USER "*")

(defn ensure-history
  "Ensures a non-nil previous entry exists in `token-data`.
   If one already was present when this function was called, returns `token-data` unmodified.
   Else assoc `(or previous {})` into `token-data`."
  [token-data previous]
  (update token-data "previous" (fn [current-previous] (or current-previous previous {}))))

(defn- token-description->token-hash
  "Converts the token metadata to a hash."
  [{:keys [service-parameter-template token-metadata]}]
  (sd/token-data->token-hash (merge service-parameter-template token-metadata)))

(defn- validate-token-modification-based-on-hash
  "Validates whether the token modification should be allowed on based on the provided token version-hash."
  [{:keys [token-metadata] :as token-description} version-hash]
  (when version-hash
    (when (not= (str (token-description->token-hash token-description)) (str version-hash))
      (throw (ex-info "Cannot modify stale token"
                      {:provided-version version-hash
                       :status 412
                       :token-metadata token-metadata})))))

;; We'd like to maintain an index of tokens by their owner.
;; We'll store an index in the key "^TOKEN_OWNERS" that maintains
;; a map of owner to another key, in which we'll store the tokens
;; that that owner owns.

(defn- make-index-entry
  "Factory method for the token index entry."
  [token-hash deleted last-update-time]
  {:deleted (true? deleted)
   :etag token-hash
   :last-update-time last-update-time})

(let [token-lock "TOKEN_LOCK"
      token-owners-key "^TOKEN_OWNERS"
      update-kv! (fn update-kv [kv-store k f]
                   (->> (kv/fetch kv-store k :refresh true)
                        f
                        (kv/store kv-store k)))
      validate-kv! (fn validate-kv [kv-store k f]
                     (f (kv/fetch kv-store k :refresh true)))
      new-owner-key (fn [] (str "^TOKEN_OWNERS_" (utils/unique-identifier)))
      ensure-owner-key (fn ensure-owner-key [kv-store owner->owner-key owner] ;; must be invoked inside a critical section
                         (when-not owner
                           (throw (ex-info "nil owner passed to ensure-owner-key"
                                           {:owner->owner-key owner->owner-key})))
                         (or (get owner->owner-key owner)
                             (let [new-owner-key (new-owner-key)]
                               (log/info "storing" new-owner-key "for" owner "in the token-owners-key")
                               (kv/store kv-store token-owners-key (assoc owner->owner-key owner new-owner-key))
                               new-owner-key)))]

  (defn store-service-description-for-token
    "Store the token mapping of the service description template in the key-value store.
     When token-limit is nil, the token count check is avoided."
    [synchronize-fn kv-store history-length token-limit ^String token service-parameter-template token-metadata &
     {:keys [version-hash]}]
    (synchronize-fn
      token-lock
      (fn inner-store-service-description-for-token []
        (log/info "storing service description for token:" token)
        (let [{:strs [deleted last-update-time owner] :as new-token-data}
              (-> (merge service-parameter-template token-metadata)
                  (select-keys sd/token-data-keys)
                  (sd/sanitize-service-description sd/token-data-keys))
              raw-token-data (kv/fetch kv-store token :refresh true)
              existing-token-data (if-not (get raw-token-data "deleted") raw-token-data {})
              existing-token-description (sd/token-data->token-description existing-token-data)
              owner->owner-key (kv/fetch kv-store token-owners-key)]
          ; Validate the token modification for concurrency races
          (validate-token-modification-based-on-hash existing-token-description version-hash)
          ; Validate that the maximum number of tokens per owner limit has not been reached
          ; token limit is not always enforced, e.g., for admin operations.
          (when token-limit
            (let [owner-key (ensure-owner-key kv-store owner->owner-key owner)]
              (validate-kv!
                kv-store owner-key
                (fn validate-limit-per-owner [index]
                  (let [active-index (utils/filterm (fn [[_ {:keys [deleted]}]] (not deleted)) index)]
                    (when (>= (count (dissoc active-index token)) token-limit)
                      (let [message (str "You have reached the limit of number of tokens allowed. "
                                         "Please delete at least one of your existing tokens to create this token.")]
                        (throw (ex-info message
                                        {:allowed-owned-tokens token-limit
                                         :num-owned-tokens (count active-index)
                                         :owner owner
                                         :status 403})))))))))
          ; Store the service description
          (kv/store kv-store token (-> new-token-data
                                       (ensure-history existing-token-data)
                                       (utils/sanitize-history history-length "previous")))
          ; Remove token from previous owner
          (let [existing-owner (get raw-token-data "owner")]
            (when (and existing-owner (not= owner existing-owner))
              (let [previous-owner-key (ensure-owner-key kv-store owner->owner-key existing-owner)]
                (log/info "removing" token "from index of" existing-owner)
                (update-kv! kv-store previous-owner-key (fn [index] (dissoc index token))))))
          ; Add token to new owner
          (when owner
            (let [owner-key (ensure-owner-key kv-store owner->owner-key owner)
                  token-hash' (sd/token-data->token-hash new-token-data)]
              (log/info "inserting" token "into index of" owner)
              (update-kv! kv-store owner-key (fn [index]
                                               (->> (make-index-entry token-hash' deleted last-update-time)
                                                    (assoc index token))))))
          (log/info "stored service description template for" token)))))

  (defn delete-service-description-for-token
    "Delete a token from the KV"
    [clock synchronize-fn kv-store history-length token owner authenticated-user &
     {:keys [hard-delete version-hash] :or {hard-delete false}}]
    (synchronize-fn
      token-lock
      (fn inner-delete-service-description-for-token []
        (log/info "attempting to delete service description for token:" token "hard-delete:" hard-delete)
        (let [existing-token-data (kv/fetch kv-store token)
              existing-token-description (sd/token-data->token-description existing-token-data)]
          ; Validate the token modification for concurrency races
          (validate-token-modification-based-on-hash existing-token-description version-hash)
          (if hard-delete
            (kv/delete kv-store token)
            (when existing-token-data
              (let [new-token-data (assoc existing-token-data
                                     "deleted" true
                                     "last-update-time" (.getMillis ^DateTime (clock))
                                     "last-update-user" authenticated-user)]
                (kv/store kv-store token (-> new-token-data
                                             (assoc "previous" existing-token-data)
                                             (utils/sanitize-history history-length "previous")))))))
        ; Remove token from owner (hard-delete) or set the deleted flag (soft-delete)
        (when owner
          (let [owner->owner-key (kv/fetch kv-store token-owners-key)
                owner-key (ensure-owner-key kv-store owner->owner-key owner)]
            (update-kv! kv-store owner-key (fn [index] (dissoc index token)))
            (when-not hard-delete
              (let [{:keys [last-update-time] :as token-data} (kv/fetch kv-store token)
                    token-hash (sd/token-data->token-hash token-data)]
                (update-kv! kv-store owner-key (fn [index]
                                                 (->> (make-index-entry token-hash true last-update-time)
                                                      (assoc index token))))))))
        ; Don't bother removing owner from token-owners, even if they have no tokens now
        (log/info "deleted token for" token))))

  (defn refresh-token
    "Refresh the KV cache for a given token"
    [kv-store token owner]
    (let [refreshed-token (kv/fetch kv-store token :refresh true)]
      (when owner
        ; NOTE: The token may still show up temporarily in the old owners list
        (let [owner->owner-key (kv/fetch kv-store token-owners-key :refresh true)]
          (if-let [owner-key (owner->owner-key owner)]
            (kv/fetch kv-store owner-key :refresh true)
            (throw (ex-info "no owner-key found" {:owner owner :status 500})))))
      refreshed-token))

  (defn refresh-token-index
    "Refresh the KV cache for token index keys"
    [kv-store]
    (let [owner->owner-key (kv/fetch kv-store token-owners-key :refresh true)]
      (doseq [[_ owner-key] owner->owner-key]
        (kv/fetch kv-store owner-key :refresh true))))

  (defn list-index-entries-for-owner
    "List all tokens for a given user."
    [kv-store owner]
    (let [owner->owner-key (kv/fetch kv-store token-owners-key)]
      (if-let [owner-key (get owner->owner-key owner)]
        (kv/fetch kv-store owner-key)
        (log/info "no owner-key found for owner" owner))))

  (defn list-token-owners
    "List token owners."
    [kv-store]
    (-> (kv/fetch kv-store token-owners-key)
        keys
        set))

  (defn token-owners-map
    "Get the token owners map state"
    [kv-store]
    (into {} (kv/fetch kv-store token-owners-key)))

  (defn reindex-tokens
    "Reindex all tokens. `tokens` is a sequence of token maps.  Remove existing index entries."
    [synchronize-fn kv-store tokens]
    (synchronize-fn
      token-lock
      (fn inner-reindex-tokens []
        (let [owner->owner-key (kv/fetch kv-store token-owners-key)]
          (when (map? owner->owner-key)
            ; Delete each owner node
            (doseq [[_ owner-key] owner->owner-key]
              (kv/delete kv-store owner-key)))
          ; Delete owner map
          (kv/delete kv-store token-owners-key))
        (let [owner->tokens (->> tokens
                                 (map (fn [token] (let [{:strs [owner]} (kv/fetch kv-store token)]
                                                    {:owner owner
                                                     :token token})))
                                 (filter :owner)
                                 (group-by :owner)
                                 (pc/map-vals (fn [entries] (map :token entries))))
              owner->index-entries (pc/map-vals
                                     (fn [tokens]
                                       (pc/map-from-keys
                                         (fn [token]
                                           (let [{:strs [deleted last-update-time] :as token-data} (kv/fetch kv-store token)
                                                 token-hash (sd/token-data->token-hash token-data)]
                                             (make-index-entry token-hash deleted last-update-time)))
                                         tokens))
                                     owner->tokens)
              owner->owner-key (pc/map-from-keys (fn [_] (new-owner-key)) (keys owner->index-entries))]
          ; Create new owner map
          (kv/store kv-store token-owners-key owner->owner-key)
          ; Write each owner node
          (doseq [[owner index-entries] owner->index-entries]
            (let [owner-key (get owner->owner-key owner)]
              (kv/store kv-store owner-key index-entries))))))))

(defprotocol ClusterCalculator
  (get-default-cluster [this]
    "Returns the default token root")
  (calculate-cluster [this request]
    "Returns the token root computed by using the incoming request"))

(defn new-configured-cluster-calculator
  "Returns a cluster calculator that looks up the cluster name based on the host name when configured in host->cluster.
   Else it returns the default configured value as the cluster."
  [{:keys [default-cluster host->cluster]}]
  {:pre [(not (str/blank? default-cluster))
         host->cluster]}
  (reify ClusterCalculator
    (get-default-cluster [_] default-cluster)
    (calculate-cluster [_ {:keys [headers]}]
      (-> (get headers "host")
          str
          (str/split #":")
          first
          host->cluster
          (or default-cluster)))))

(defn- handle-delete-token-request
  "Deletes the token configuration if found."
  [clock synchronize-fn kv-store history-length waiter-hostnames entitlement-manager make-peer-requests-fn
   {:keys [headers] :as request}]
  (let [{:keys [token]} (sd/retrieve-token-from-service-description-or-hostname headers headers waiter-hostnames)
        authenticated-user (get request :authorization/user)
        request-params (-> request ru/query-params-request :query-params)
        hard-delete (utils/request-flag request-params "hard-delete")]
    (if token
      (let [token-description (sd/token->token-description kv-store token :include-deleted hard-delete)
            {:keys [service-parameter-template token-metadata]} token-description
            user-metadata-template (select-keys token-metadata sd/user-metadata-keys)]
        (if (or (seq service-parameter-template) (seq user-metadata-template))
          (let [token-owner (get token-metadata "owner")
                version-hash (get headers "if-match")]
            (if hard-delete
              (do
                (when-not (or (get token-metadata "deleted") version-hash)
                  (throw (ex-info "Must specify if-match header for token hard deletes"
                                  {:request-headers headers, :status 400
                                   :log-level :warn})))
                (when-not (authz/administer-token? entitlement-manager authenticated-user token token-metadata)
                  (throw (ex-info "Cannot hard-delete token"
                                  {:metadata token-metadata
                                   :status 403
                                   :user authenticated-user
                                   :log-level :warn}))))
              (when-not (authz/manage-token? entitlement-manager authenticated-user token token-metadata)
                (throw (ex-info "User not allowed to delete token"
                                {:owner token-owner
                                 :status 403
                                 :user authenticated-user
                                 :log-level :warn}))))
            (delete-service-description-for-token
              clock synchronize-fn kv-store history-length token token-owner authenticated-user
              :hard-delete hard-delete :version-hash version-hash)
            ; notify peers of token delete and ask them to refresh their caches
            (make-peer-requests-fn "tokens/refresh"
                                   :body (utils/clj->json {:owner token-owner, :token token})
                                   :method :post)
            (utils/clj->json-response {:delete token, :hard-delete hard-delete, :success true}
                                      :headers {"etag" version-hash}))
          (throw (ex-info (str "Token " token " does not exist")
                          {:status 404 :token token :log-level :warn}))))
      (throw (ex-info "Couldn't find token in request" {:status 400 :token token :log-level :warn})))))

(defn- handle-get-token-request
  "Returns the configuration if found.
   Anyone can see the configuration, b/c it shouldn't contain any sensitive data."
  [kv-store cluster-calculator token-root waiter-hostnames {:keys [headers] :as request}]
  (let [request-params (-> request ru/query-params-request :query-params)
        include-deleted (utils/param-contains? request-params "include" "deleted")
        show-metadata (utils/param-contains? request-params "include" "metadata")
        token (or (get request-params "token")
                  (:token (sd/retrieve-token-from-service-description-or-hostname headers headers waiter-hostnames)))
        token-description (sd/token->token-description kv-store token :include-deleted include-deleted)
        {:keys [service-parameter-template token-metadata]} token-description
        user-metadata-template (select-keys token-metadata sd/user-metadata-keys)
        token-hash (token-description->token-hash token-description)]
    (if (or (seq service-parameter-template) (seq user-metadata-template))
      ;;NB do not ever return the password to the user
      (let [epoch-time->date-time (fn [epoch-time] (DateTime. epoch-time))]
        (log/info "successfully retrieved token" token)
        (utils/clj->json-response
          (cond-> (merge service-parameter-template
                         (select-keys token-metadata sd/user-metadata-keys))
            show-metadata
            (merge (cond-> (loop [loop-token-metadata (select-keys token-metadata sd/system-metadata-keys)
                                  nested-last-update-time-path ["last-update-time"]]
                             (if (get-in loop-token-metadata nested-last-update-time-path)
                               (recur (update-in loop-token-metadata nested-last-update-time-path epoch-time->date-time)
                                      (concat ["previous"] nested-last-update-time-path))
                               loop-token-metadata))
                     (not (contains? token-metadata "cluster"))
                     (assoc "cluster" (get-default-cluster cluster-calculator))
                     (not (contains? token-metadata "root"))
                     (assoc "root" token-root))))
          :headers {"etag" token-hash}))
      (throw (ex-info (str "Couldn't find token " token)
                      {:headers {}
                       :status 404
                       :token token
                       :log-level :warn})))))

(defn- handle-post-token-request
  "Validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [clock synchronize-fn kv-store cluster-calculator token-root history-length limit-per-owner waiter-hostnames
   entitlement-manager make-peer-requests-fn validate-service-description-fn {:keys [headers] :as request}]
  (let [request-params (-> request ru/query-params-request :query-params)
        admin-mode? (= "admin" (get request-params "update-mode"))
        authenticated-user (get request :authorization/user)
        {:strs [token] :as new-token-data} (-> request
                                               ru/json-request
                                               :body
                                               sd/transform-allowed-params-token-entry)
        token-param (get request-params "token")
        _ (when (and (not (str/blank? token-param)) (not (str/blank? token)))
            (throw (ex-info "The token should be provided only as a query parameter or in the json payload"
                            {:status 400
                             :token {:json-payload token
                                     :query-parameter token-param}
                             :log-level :warn})))
        token (or token token-param)
        new-token-metadata (select-keys new-token-data sd/token-metadata-keys)
        new-user-metadata (select-keys new-token-metadata sd/user-metadata-keys)
        {:strs [authentication interstitial-secs permitted-user run-as-user] :as new-service-parameter-template}
        (select-keys new-token-data sd/service-parameter-keys)
        existing-token-metadata (sd/token->token-metadata kv-store token :error-on-missing false)
        owner (or (get new-token-metadata "owner")
                  (get existing-token-metadata "owner")
                  authenticated-user)
        version-hash (get headers "if-match")]
    (when (str/blank? token)
      (throw (ex-info "Must provide the token" {:status 400 :log-level :warn})))
    (when (some #(= token %) waiter-hostnames)
      (throw (ex-info "Token name is reserved" {:status 403 :token token :log-level :warn})))
    (when (empty? (select-keys new-token-data sd/token-user-editable-keys))
      (throw (ex-info (str "No parameters provided for " token) {:status 400 :log-level :warn})))
    (sd/validate-token token)
    (validate-service-description-fn new-service-parameter-template)
    (when-let [user-metadata-check (s/check sd/user-metadata-schema new-user-metadata)]
      (throw (ex-info "User metadata validation failed"
                      {:failed-check (str user-metadata-check) :status 400 :token token :log-level :warn})))
    (let [unknown-keys (-> new-token-data
                           keys
                           set
                           (set/difference sd/token-data-keys)
                           (disj "token"))]
      (when (not-empty unknown-keys)
        (throw (ex-info (str "Unsupported key(s) in token: " (str (vec unknown-keys)))
                        {:status 400 :token token :log-level :warn}))))
    (when (= authentication "disabled")
      (when (not= permitted-user "*")
        (throw (ex-info (str "Tokens with authentication disabled must specify"
                             " permitted-user as *, instead provided " permitted-user)
                        {:status 400 :token token :log-level :warn})))
      ;; partial tokens not supported when authentication is disabled
      (when-not (sd/required-keys-present? new-service-parameter-template)
        (throw (ex-info "Tokens with authentication disabled must specify all required parameters"
                        {:missing-parameters (->> sd/service-required-keys
                                                  (remove #(contains? new-service-parameter-template %1)) seq)
                         :service-description new-service-parameter-template
                         :status 400
                         :log-level :warn}))))
    (when (and interstitial-secs (not (sd/required-keys-present? new-service-parameter-template)))
      (throw (ex-info (str "Tokens with missing required parameters cannot use interstitial support")
                      {:status 400 :token token :log-level :warn})))
    (case (get request-params "update-mode")
      "admin"
      (do
        (when (and (seq existing-token-metadata) (not version-hash))
          (throw (ex-info "Must specify if-match header for admin mode token updates"
                          {:request-headers headers, :status 400 :log-level :warn})))
        (when-not (authz/administer-token? entitlement-manager authenticated-user token new-token-metadata)
          (throw (ex-info "Cannot administer token"
                          {:status 403
                           :token-metadata new-token-metadata
                           :user authenticated-user
                           :log-level :warn}))))

      nil
      (do
        (when (and run-as-user (not= "*" run-as-user))
          (when-not (authz/run-as? entitlement-manager authenticated-user run-as-user)
            (throw (ex-info "Cannot run as user"
                            {:authenticated-user authenticated-user
                             :run-as-user run-as-user
                             :status 403
                             :log-level :warn}))))
        (let [existing-service-description-owner (get existing-token-metadata "owner")]
          (if-not (str/blank? existing-service-description-owner)
            (when-not (authz/manage-token? entitlement-manager authenticated-user token existing-token-metadata)
              (throw (ex-info "Cannot change owner of token"
                              {:existing-owner existing-service-description-owner
                               :new-user owner
                               :status 403
                               :log-level :warn})))
            (when-not (authz/run-as? entitlement-manager authenticated-user owner)
              (throw (ex-info "Cannot create token as user"
                              {:authenticated-user authenticated-user
                               :owner owner
                               :status 403
                               :log-level :warn})))))
        (when (contains? new-token-metadata "last-update-time")
          (throw (ex-info "Cannot modify last-update-time token metadata"
                          {:status 400
                           :token-metadata new-token-metadata
                           :log-level :warn})))
        (when (contains? new-token-metadata "last-update-user")
          (throw (ex-info "Cannot modify last-update-user token metadata"
                          {:status 400
                           :token-metadata new-token-metadata
                           :log-level :warn})))
        (when (contains? new-token-metadata "root")
          (throw (ex-info "Cannot modify root token metadata"
                          {:status 400
                           :token-metadata new-token-metadata
                           :log-level :warn})))
        (when (contains? new-token-metadata "previous")
          (throw (ex-info "Cannot modify previous token metadata"
                          {:status 400
                           :token-metadata new-token-metadata
                           :log-level :warn}))))

      (throw (ex-info "Invalid update-mode"
                      {:mode (get request-params "update-mode")
                       :status 400
                       :log-level :warn})))

    (when-let [previous (get new-token-metadata "previous")]
      (when-not (map? previous)
        (throw (ex-info (str "Token previous must be a map")
                        {:previous previous :status 400 :token token :log-level :warn}))))

    ; Store the token
    (let [new-token-metadata (merge {"cluster" (calculate-cluster cluster-calculator request)
                                     "last-update-time" (.getMillis ^DateTime (clock))
                                     "last-update-user" authenticated-user
                                     "owner" owner
                                     "root" (or (get existing-token-metadata "root") token-root)}
                                    new-token-metadata)
          new-user-editable-token-data (-> (merge new-service-parameter-template new-token-metadata)
                                           (select-keys sd/token-user-editable-keys))
          existing-token-description (sd/token->token-description kv-store token :include-deleted false)
          existing-editable-token-data (-> (merge (:service-parameter-template existing-token-description)
                                                  (:token-metadata existing-token-description))
                                           (select-keys sd/token-user-editable-keys))]
      (if (and (not admin-mode?)
               (= existing-editable-token-data new-user-editable-token-data))
        (utils/clj->json-response
          {:message (str "No changes detected for " token)
           :service-description (:service-parameter-template existing-token-description)}
          :headers {"etag" (token-description->token-hash existing-token-description)})
        (let [token-limit (if admin-mode?
                            (do
                              (log/info "will not enforce count limit on owner tokens in admin mode" {:owner owner})
                              nil)
                            limit-per-owner)]
          (store-service-description-for-token
            synchronize-fn kv-store history-length token-limit token new-service-parameter-template new-token-metadata
            :version-hash version-hash)
          ; notify peers of token update
          (make-peer-requests-fn "tokens/refresh"
                                 :method :post
                                 :body (utils/clj->json {:token token, :owner owner}))
          (let [creation-mode (if (and (seq existing-token-metadata)
                                       (not (get existing-token-metadata "deleted")))
                                "updated "
                                "created ")]
            (utils/clj->json-response
              {:message (str "Successfully " creation-mode token)
               :service-description new-service-parameter-template}
              :headers {"etag" (token-description->token-hash
                                 {:service-parameter-template new-service-parameter-template
                                  :token-metadata new-token-metadata})})))))))

(defn handle-token-request
  "Ring handler for dealing with tokens.

   If handling DELETE, deletes the token configuration if found.

   If handling GET, returns the configuration if found.
   Anyone can see the configuration, b/c it shouldn't contain any sensitive data.

   If handling POST, validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [clock synchronize-fn kv-store cluster-calculator token-root history-length limit-per-owner waiter-hostnames entitlement-manager
   make-peer-requests-fn validate-service-description-fn {:keys [request-method] :as request}]
  (try
    (case request-method
      :delete (handle-delete-token-request clock synchronize-fn kv-store history-length waiter-hostnames entitlement-manager
                                           make-peer-requests-fn request)
      :get (handle-get-token-request kv-store cluster-calculator token-root waiter-hostnames request)
      :post (handle-post-token-request clock synchronize-fn kv-store cluster-calculator token-root history-length limit-per-owner
                                       waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                                       request)
      (throw (ex-info "Invalid request method" {:log-level :info :request-method request-method :status 405})))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn handle-list-tokens-request
  [kv-store entitlement-manager {:keys [request-method] :as req}]
  (try
    (case request-method
      :get (let [{:strs [can-manage-as-user] :as request-params} (-> req ru/query-params-request :query-params)
                 include-deleted (utils/param-contains? request-params "include" "deleted")
                 show-metadata (utils/param-contains? request-params "include" "metadata")
                 owner (get request-params "owner")
                 owners (if owner #{owner} (list-token-owners kv-store))]
             (->> owners
                  (map
                    (fn [owner]
                      (->> (list-index-entries-for-owner kv-store owner)
                           (filter
                             (fn [[_ entry]]
                               (or include-deleted (not (:deleted entry)))))
                           (filter
                             (fn [[token _]]
                               (or (nil? can-manage-as-user)
                                   (authz/manage-token? entitlement-manager can-manage-as-user token {"owner" owner}))))
                           (map
                             (fn [[token entry]]
                               (-> (if show-metadata
                                     (update entry :last-update-time tc/from-long)
                                     (dissoc entry :deleted :etag :last-update-time))
                                   (assoc :owner owner :token token)))))))
                  flatten
                  utils/clj->streaming-json-response))
      (throw (ex-info "Only GET supported" {:log-level :info
                                            :request-method request-method
                                            :status 405})))
    (catch Exception ex
      (utils/exception->response ex req))))

(defn handle-list-token-owners-request
  "Handle a request to list owners
  This method is intended mainly for use by Waiter operator for state inspection,
  but could be in theory used by end users.
  The response contains a map, owner -> internal KV key.  The value of the key
  stores the tokens for that particular owner."
  [kv-store {:keys [request-method] :as req}]
  (try
    (case request-method
      :get (let [owner->owner-ref (token-owners-map kv-store)]
             (utils/clj->json-response owner->owner-ref))
      (throw (ex-info "Only GET supported" {:log-level :info
                                            :request-method request-method
                                            :status 405})))
    (catch Exception ex
      (utils/exception->response ex req))))

(defn handle-refresh-token-request
  "Handle a request to refresh token data directly from the KV store, skipping the cache."
  [kv-store {{:keys [src-router-id]} :basic-authentication :as req}]
  (try
    (let [{:strs [token owner index] :as json-data} (-> req ru/json-request :body)]
      (log/info "received token refresh request" json-data)
      (when index
        (log/info src-router-id "is force refreshing the token index")
        (refresh-token-index kv-store))
      (when token
        (log/info src-router-id "is force refreshing token" token)
        (refresh-token kv-store token owner))
      (utils/clj->json-response {:success true}))
    (catch Exception ex
      (utils/exception->response ex req))))

(defn handle-reindex-tokens-request
  "Load all tokens and re-index them."
  [synchronize-fn make-peer-requests-fn kv-store list-tokens-fn {:keys [request-method] :as req}]
  (try
    (case request-method
      :post (let [tokens (list-tokens-fn)]
              (reindex-tokens synchronize-fn kv-store tokens)
              (make-peer-requests-fn "tokens/refresh"
                                     :method :post
                                     :body (utils/clj->json {:index true}))
              (utils/clj->json-response {:message "Successfully re-indexed" :tokens (count tokens)}))
      (throw (ex-info "Only POST supported" {:log-level :info
                                             :request-method request-method
                                             :status 405})))
    (catch Exception ex
      (utils/exception->response ex req))))
