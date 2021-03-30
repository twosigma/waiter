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
            [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [schema.core :as s]
            [waiter.authorization :as authz]
            [waiter.correlation-id :as cid]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
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

(defn- get-refresh-metric-name
  [refresh]
  (if refresh "with-refresh" "without-refresh"))

;; We'd like to maintain an index of tokens by their owner.
;; We'll store an index in the key "^TOKEN_OWNERS" that maintains
;; a map of owner to another key, in which we'll store the tokens
;; that that owner owns.

(defn make-index-entry
  "Factory method for the token index entry."
  [token-hash deleted last-update-time maintenance]
  {:deleted (true? deleted)
   :etag token-hash
   :last-update-time last-update-time
   :maintenance (some? maintenance)})

(defn send-internal-index-event
  "Send an internal event to be processed by the tokens-watch-maintainer daemon process"
  [tokens-update-chan token]
  (log/info "sending internal index event" {:token token})
  (async/put! tokens-update-chan {:token token}))

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
        (let [{:strs [deleted last-update-time maintenance owner] :as new-token-data}
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
                                         :status http-403-forbidden})))))))))
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
                                               (->> (make-index-entry token-hash' deleted last-update-time maintenance)
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
              (let [{:strs [last-update-time maintenance] :as token-data} (kv/fetch kv-store token)
                    token-hash (sd/token-data->token-hash token-data)]
                (update-kv! kv-store owner-key (fn [index]
                                                 (->> (make-index-entry token-hash true last-update-time maintenance)
                                                      (assoc index token))))))))

        ; Don't bother removing owner from token-owners, even if they have no tokens now
        (log/info "deleted token for" token))))

  (defn list-index-entries-for-owner-key
    "List all tokens for a given user by fetching the owner index in the kv-store with the owner-key"
    [kv-store owner-key & {:keys [refresh] :or {refresh false}}]
    (timers/start-stop-time!
      (metrics/waiter-timer "core" "token" "list-index-entries-for-owner-key" (get-refresh-metric-name refresh))
      (kv/fetch kv-store owner-key :refresh refresh)))

  (defn refresh-token
    "Refresh the KV cache for a given token"
    [kv-store token owner]
    (let [refreshed-token (kv/fetch kv-store token :refresh true)]
      (when owner
        ; NOTE: The token may still show up temporarily in the old owners list
        (let [owner->owner-key (kv/fetch kv-store token-owners-key :refresh true)]
          (if-let [owner-key (owner->owner-key owner)]
            (list-index-entries-for-owner-key kv-store owner-key :refresh true)
            (throw (ex-info "no owner-key found" {:owner owner :status http-500-internal-server-error})))))
      refreshed-token))

  (defn refresh-token-index
    "Refresh the KV cache for token index keys"
    [kv-store]
    (let [owner->owner-key (kv/fetch kv-store token-owners-key :refresh true)]
      (doseq [[_ owner-key] owner->owner-key]
        (list-index-entries-for-owner-key kv-store owner-key :refresh true))))

  (defn list-index-entries-for-owner
    "List all tokens for a given user by fetching the owner index in the kv-store"
    [kv-store owner & {:keys [refresh] :or {refresh false}}]
    (let [owner->owner-key (kv/fetch kv-store token-owners-key :refresh refresh)]
      (if-let [owner-key (get owner->owner-key owner)]
        (list-index-entries-for-owner-key kv-store owner-key :refresh refresh)
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

  (defn get-token-index
    "Given a token, determine the correct index entry of the token. Returns nil if token doesn't exist."
    [kv-store token & {:keys [refresh] :or {refresh false}}]
    (timers/start-stop-time!
      (metrics/waiter-timer "core" "token" "get-token-index" (get-refresh-metric-name refresh))
      (let [{:strs [deleted last-update-time maintenance owner] :as token-data} (kv/fetch kv-store token :refresh refresh)
            token-hash (sd/token-data->token-hash token-data)]
        (when token-data
          (-> (make-index-entry token-hash deleted last-update-time maintenance)
              (assoc :owner owner :token token))))))

  (defn reindex-tokens
    "Reindex all tokens. `tokens` is a sequence of token maps.  Remove existing index entries.
     Writes new entries before deleting old ones to avoid intervening index queries from reading empty results."
    [synchronize-fn kv-store tokens]
    (synchronize-fn
      token-lock
      (fn inner-reindex-tokens []
        (let [existing-owner->owner-key (kv/fetch kv-store token-owners-key)
              owner->tokens (->> tokens
                              (map (fn [token] (let [{:strs [owner]} (kv/fetch kv-store token)]
                                                 {:owner owner
                                                  :token token})))
                              (filter :owner)
                              (group-by :owner)
                              (pc/map-vals (fn [entries] (map :token entries))))
              owner->index-entries (pc/map-vals
                                     (fn [tokens]
                                       (pc/map-from-keys
                                         #(-> (get-token-index kv-store %)
                                              (dissoc :owner :token))
                                         tokens))
                                     owner->tokens)
              owner->owner-key (pc/map-from-keys (fn [_] (new-owner-key)) (keys owner->index-entries))]
          ; Write new owner nodes
          (doseq [[owner index-entries] owner->index-entries]
            (let [owner-key (get owner->owner-key owner)]
              (kv/store kv-store owner-key index-entries)))
          ; Overwrite new owner-key map _after_ writing the new owner nodes
          (kv/store kv-store token-owners-key owner->owner-key)
          ; Delete existing owner nodes
          (when (map? existing-owner->owner-key)
            (let [new-owner-keys (-> owner->owner-key vals set)]
              (doseq [[_ owner-key] existing-owner->owner-key]
                ;; defensive check to avoid deleting duplicated keys after reindex
                (when-not (contains? new-owner-keys owner-key)
                  (kv/delete kv-store owner-key)))))))))

  (defn get-token->index
    "Return a map of ALL token to token index entry. The token index entries also include the owner and token.
     Specifying :refresh true will refresh all owner/token indexes and get the most up to date map."
    [kv-store & {:keys [refresh] :or {refresh false}}]
    (timers/start-stop-time!
      (metrics/waiter-timer "core" "token" "get-token->index" (get-refresh-metric-name refresh))
      (let [owner->owner-key (kv/fetch kv-store token-owners-key :refresh refresh)]
        (reduce
          (fn [outer-token->index [owner owner-key]]
            (reduce
              (fn [inner-token->index [token entry]]
                (->> (assoc entry :owner owner :token token)
                     (assoc inner-token->index token)))
              outer-token->index
              (list-index-entries-for-owner-key kv-store owner-key :refresh refresh)))
          {}
          owner->owner-key)))))

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

(defn- parse-last-update-time
  "Parses input string as a ISO 8601 format date string."
  [last-update-time-str]
  (try
    (-> last-update-time-str du/str-to-date tc/to-long)
    (catch Throwable throwable
      (throw (ex-info "Invalid date format for last-update-time string"
                      {:last-update-time last-update-time-str
                       :log-level :warn
                       :status http-400-bad-request}
                      throwable)))))

(defn- handle-delete-token-request
  "Deletes the token configuration if found."
  [clock synchronize-fn kv-store history-length waiter-hostnames entitlement-manager make-peer-requests-fn tokens-update-chan
   {:keys [headers] :as request}]
  (let [{:keys [token]} (sd/retrieve-token-from-service-description-or-hostname headers headers waiter-hostnames)
        authenticated-user (get request :authorization/user)
        request-params (-> request ru/query-params-request :query-params)
        hard-delete (utils/request-flag request-params "hard-delete")]
    (log/info "request to delete token" token)
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
                                  {:request-headers headers, :status http-400-bad-request
                                   :log-level :warn})))
                (when-not (authz/administer-token? entitlement-manager authenticated-user token token-metadata)
                  (throw (ex-info "Cannot hard-delete token"
                                  {:metadata token-metadata
                                   :status http-403-forbidden
                                   :user authenticated-user
                                   :log-level :warn}))))
              (when-not (authz/manage-token? entitlement-manager authenticated-user token token-metadata)
                (throw (ex-info "User not allowed to delete token"
                                {:owner token-owner
                                 :status http-403-forbidden
                                 :user authenticated-user
                                 :log-level :warn}))))
            (delete-service-description-for-token
              clock synchronize-fn kv-store history-length token token-owner authenticated-user
              :hard-delete hard-delete :version-hash version-hash)
            ; notify peers of token delete and ask them to refresh their caches
            (make-peer-requests-fn "tokens/refresh"
                                   :body (utils/clj->json {:owner token-owner, :token token})
                                   :method :post)
            (send-internal-index-event tokens-update-chan token)
            (-> {:delete token, :hard-delete hard-delete, :success true}
              (utils/clj->json-response :headers {"etag" version-hash})
              (assoc :waiter/token token)))
          (throw (ex-info (str "Token " token " does not exist")
                          {:status http-404-not-found :token token :log-level :warn}))))
      (throw (ex-info "Couldn't find token in request" {:status http-400-bad-request :token token :log-level :warn})))))

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
        (-> (utils/clj->json-response
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
              :headers {"etag" token-hash})
          (assoc :waiter/token token)))
      (throw (ex-info (str "Couldn't find token " token)
                      {:headers {}
                       :status http-404-not-found
                       :token token
                       :log-level :warn})))))

(defn- token-description->editable-token-parameters
  "Retrieves the user editable token parameters from the token description."
  [token-description]
  (-> (merge (:service-parameter-template token-description)
             (:token-metadata token-description))
    (select-keys sd/token-user-editable-keys)))

(defn- handle-post-token-request
  "Validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [clock synchronize-fn kv-store cluster-calculator token-root history-length limit-per-owner waiter-hostnames
   entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
   tokens-update-chan {:keys [headers] :as request}]
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
                            {:status http-400-bad-request
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
    (log/info "request to edit token" token)
    (when (str/blank? token)
      (throw (ex-info "Must provide the token" {:status http-400-bad-request :log-level :warn})))
    (when (some #(= token %) waiter-hostnames)
      (throw (ex-info "Token name is reserved" {:status http-403-forbidden :token token :log-level :warn})))
    (when (empty? (select-keys new-token-data sd/token-user-editable-keys))
      (throw (ex-info (str "No parameters provided for " token) {:status http-400-bad-request :log-level :warn})))
    (sd/validate-token token)
    (validate-service-description-fn new-service-parameter-template)
    (sd/validate-user-metadata-schema new-user-metadata)
    (let [unknown-keys (-> new-token-data
                           keys
                           set
                           (set/difference sd/token-data-keys)
                           (disj "token"))]
      (when (not-empty unknown-keys)
        (throw (ex-info (str "Unsupported key(s) in token: " (str (vec unknown-keys)))
                        {:status http-400-bad-request :token token :log-level :warn}))))
    (let [service-parameter-with-service-defaults (attach-service-defaults-fn new-service-parameter-template)
          missing-parameters (->> sd/service-required-keys (remove #(contains? service-parameter-with-service-defaults %1)) seq)]
      (when (= authentication "disabled")
        (when (not= permitted-user "*")
          (throw (ex-info (str "Tokens with authentication disabled must specify"
                               " permitted-user as *, instead provided " permitted-user)
                          {:status http-400-bad-request :token token :log-level :warn})))
        ;; partial tokens not supported when authentication is disabled
        (when-not (sd/required-keys-present? service-parameter-with-service-defaults)
          (throw (ex-info "Tokens with authentication disabled must specify all required parameters"
                          {:log-level :warn
                           :missing-parameters missing-parameters
                           :service-description new-service-parameter-template
                           :status http-400-bad-request}))))
      (when (and interstitial-secs (not (sd/required-keys-present? service-parameter-with-service-defaults)))
        (throw (ex-info (str "Tokens with missing required parameters cannot use interstitial support")
                        {:log-level :warn
                         :missing-parameters missing-parameters
                         :status http-400-bad-request
                         :token token}))))
    (case (get request-params "update-mode")
      "admin"
      (do
        (when (and (seq existing-token-metadata) (not version-hash))
          (throw (ex-info "Must specify if-match header for admin mode token updates"
                          {:request-headers headers, :status http-400-bad-request :log-level :warn})))
        (when-not (authz/administer-token? entitlement-manager authenticated-user token new-token-metadata)
          (throw (ex-info "Cannot administer token"
                          {:status http-403-forbidden
                           :token-metadata new-token-metadata
                           :user authenticated-user
                           :log-level :warn}))))

      nil
      (let [existing-editor (get existing-token-metadata "editor")
            existing-owner (get existing-token-metadata "owner")
            creating-token? (empty? existing-token-metadata)
            current-owner? (and existing-owner
                                (authz/manage-token? entitlement-manager authenticated-user token existing-token-metadata))
            editing? (and (not creating-token?)
                          (not current-owner?)
                          existing-editor
                          (authz/run-as? entitlement-manager authenticated-user existing-editor))]
        (when editing?
          (log/info "applying editor privileges to operation" {:editor authenticated-user :owner existing-owner})
          (let [existing-token-parameters (sd/token->token-parameters kv-store token :include-deleted false)]
            (doseq [parameter-name ["editor" "owner" "run-as-user"]]
              (let [existing-value (get existing-token-parameters parameter-name)
                    new-value (get new-token-data parameter-name)]
                (when (not= existing-value new-value)
                  (throw (ex-info (str "Not allowed to edit parameter " parameter-name)
                                  {:authenticated-user authenticated-user
                                   :existing-token-description existing-token-parameters
                                   :parameter parameter-name
                                   :parameter-exiting-value existing-value
                                   :parameter-new-value new-value
                                   :privileges {:current-owner? current-owner? :editor? editing?}
                                   :status http-403-forbidden
                                   :log-level :warn})))))))
        ;; only check run-as-user rules when not running as editor, editor cannot change run-as-user from previous check
        (when (and (not editing?) run-as-user (not= "*" run-as-user))
          (when-not (authz/run-as? entitlement-manager authenticated-user run-as-user)
            (throw (ex-info "Cannot run as user"
                            {:authenticated-user authenticated-user
                             :run-as-user run-as-user
                             :status http-403-forbidden
                             :log-level :warn}))))
        (if creating-token?
          ;; new token creation
          (when-not (authz/run-as? entitlement-manager authenticated-user owner)
            (throw (ex-info "Cannot create token as user"
                            {:authenticated-user authenticated-user
                             :owner owner
                             :status http-403-forbidden
                             :log-level :warn})))
          ;; editing token
          (let [delegated-user (or (when editing? existing-owner) authenticated-user)]
            (when-not (authz/manage-token? entitlement-manager delegated-user token existing-token-metadata)
              (throw (ex-info "Cannot update token"
                              {:authenticated-user authenticated-user
                               :existing-owner existing-owner
                               :new-user owner
                               :privileges {:editor? editing? :owner? current-owner?}
                               :status http-403-forbidden
                               :log-level :warn})))))
        ;; Neither owner nor editor may modify system metadata fields
        (doseq [parameter-name ["last-update-time" "last-update-user" "root" "previous"]]
          (when (contains? new-token-metadata parameter-name)
            (throw (ex-info (str "Cannot modify " parameter-name " token metadata")
                            {:status http-400-bad-request
                             :token-metadata new-token-metadata
                             :log-level :warn})))))

      (throw (ex-info "Invalid update-mode"
                      {:mode (get request-params "update-mode")
                       :status http-400-bad-request
                       :log-level :warn})))

    (when-let [previous (get new-token-metadata "previous")]
      (when-not (map? previous)
        (throw (ex-info (str "Token previous must be a map")
                        {:previous previous :status http-400-bad-request :token token :log-level :warn}))))

    ; Store the token
    (let [{:strs [last-update-time] :as new-token-metadata}
          (merge {"cluster" (calculate-cluster cluster-calculator request)
                  "last-update-time" (.getMillis ^DateTime (clock))
                  "last-update-user" authenticated-user
                  "owner" owner
                  "root" (or (get existing-token-metadata "root") token-root)}
                 new-token-metadata)
          new-token-metadata (cond-> new-token-metadata
                               (string? last-update-time)
                               (update "last-update-time" parse-last-update-time))
          new-user-editable-token-data (-> (merge new-service-parameter-template new-token-metadata)
                                           (select-keys sd/token-user-editable-keys))
          existing-token-description (sd/token->token-description kv-store token :include-deleted false)
          existing-editable-token-data (token-description->editable-token-parameters existing-token-description)
          [overridden-token-data overriding-token-data _] (data/diff existing-editable-token-data new-user-editable-token-data)]
      (if (and (not admin-mode?)
               (= existing-editable-token-data new-user-editable-token-data))
        (-> (utils/clj->json-response
              {:message (str "No changes detected for " token)
               :service-description (:service-parameter-template existing-token-description)}
              :headers {"etag" (token-description->token-hash existing-token-description)})
          (assoc :waiter/token token))
        (let [token-limit (if admin-mode?
                            (do
                              (log/info "will not enforce count limit on owner tokens in admin mode" {:owner owner})
                              nil)
                            limit-per-owner)]
          (if (contains? overriding-token-data "maintenance")
            (if (contains? overridden-token-data "maintenance")
              (log/info "updating maintenance mode for token" {:token token})
              (log/info "starting maintenance mode for token" {:token token}))
            (when (contains? overridden-token-data "maintenance")
              (log/info "stopping maintenance mode for token" {:token token})))
          (store-service-description-for-token
            synchronize-fn kv-store history-length token-limit token new-service-parameter-template new-token-metadata
            :version-hash version-hash)
          ; notify peers of token update
          (make-peer-requests-fn "tokens/refresh"
                                 :method :post
                                 :body (utils/clj->json {:token token, :owner owner}))
          (send-internal-index-event tokens-update-chan token)
          (let [creation-mode (if (and (seq existing-token-metadata)
                                       (not (get existing-token-metadata "deleted")))
                                "updated "
                                "created ")]
            (-> (utils/clj->json-response
                  {:message (str "Successfully " creation-mode token)
                   :service-description new-service-parameter-template}
                  :headers {"etag" (token-description->token-hash
                                     {:service-parameter-template new-service-parameter-template
                                      :token-metadata new-token-metadata})})
              (assoc :waiter/token token))))))))

(defn handle-token-request
  "Ring handler for dealing with tokens.

   If handling DELETE, deletes the token configuration if found.

   If handling GET, returns the configuration if found.
   Anyone can see the configuration, b/c it shouldn't contain any sensitive data.

   If handling POST, validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [clock synchronize-fn kv-store cluster-calculator token-root history-length limit-per-owner waiter-hostnames entitlement-manager
   make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn tokens-update-chan {:keys [request-method] :as request}]
  (try
    (case request-method
      :delete (handle-delete-token-request clock synchronize-fn kv-store history-length waiter-hostnames entitlement-manager
                                           make-peer-requests-fn tokens-update-chan request)
      :get (handle-get-token-request kv-store cluster-calculator token-root waiter-hostnames request)
      :post (handle-post-token-request clock synchronize-fn kv-store cluster-calculator token-root history-length limit-per-owner
                                       waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                                       attach-service-defaults-fn tokens-update-chan request)
      (throw (ex-info "Invalid request method" {:log-level :info :request-method request-method :status http-405-method-not-allowed})))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn handle-list-tokens-watch
  [streaming-timeout-ms index-filter-fn metadata-transducer-fn tokens-watch-channels-update-chan {:keys [ctrl] :as request}]
  (let [{:strs [streaming-timeout]} (-> request ru/query-params-request :query-params)]
    (if-let [configured-streaming-timeout-ms (if streaming-timeout
                                               (utils/parse-int streaming-timeout)
                                               streaming-timeout-ms)]
      (let [_ (log/info "request will use streaming timeout of" configured-streaming-timeout-ms "ms")
            correlation-id (cid/get-correlation-id)
            watch-chan-xform
            (comp
              (map
                (fn event-filter [{:keys [object type] :as event}]
                  (cid/cinfo correlation-id "received event from token-watch-maintainer daemon" {:type (:type event)})
                  (cid/cdebug correlation-id "full tokens event data received from daemon" {:event event})
                  (case type
                    :INITIAL
                    (assoc event :object (->> object
                                              (filter index-filter-fn)
                                              (map metadata-transducer-fn)))
                    :EVENTS
                    (assoc event :object (->> object
                                              (filter (fn [{:keys [object]}]
                                                        (index-filter-fn object)))
                                              (map (fn [{:keys [type] :as entry}]
                                                     (if (= type :UPDATE)
                                                       (update entry :object metadata-transducer-fn)
                                                       entry)))))
                    (throw (ex-info "Invalid event type provided" {:event event})))))
              (filter
                (fn empty-aggregate-events [{:keys [object type] :as event}]
                  (case type
                    :INITIAL true
                    :EVENTS (not-empty object)
                    (throw (ex-info "Invalid event type provided" {:event event})))))
              (map
                (fn [event]
                  (cid/cinfo correlation-id "forwarding tokens event to client" {:type (:type event)})
                  (cid/cdebug correlation-id "full tokens event data being sent to client" {:event event})
                  (utils/clj->json event))))
            watch-chan-ex-handler-fn
            (fn watch-chan-ex-handler [e]
              (async/put! ctrl e)
              (cid/cerror correlation-id e "error during transformation of a token watch event"))
            watch-chan-buffer (async/buffer 1000)
            watch-chan (async/chan watch-chan-buffer watch-chan-xform watch-chan-ex-handler-fn)
            _ (log/info "created watch-chan" watch-chan)
            watch-mult (async/mult watch-chan)
            response-chan (async/chan 1024)
            _ (async/tap watch-mult response-chan)
            trigger-chan (au/latest-chan)
            _ (async/tap watch-mult trigger-chan)]
        (async/go-loop [timeout-ch (async/timeout configured-streaming-timeout-ms)]
          (let [[message chan] (async/alts! [trigger-chan timeout-ch] :priority true)]
            (cond
              (= chan trigger-chan)
              (if (nil? message)
                (log/info "watch-chan has been closed")
                (recur (async/timeout configured-streaming-timeout-ms)))
              :else ;; timeout channel has been triggered
              (do
                (log/info "closing watch-chan due to streaming timeout" configured-streaming-timeout-ms "ms")
                (async/close! watch-chan)))))
        (if (async/put! tokens-watch-channels-update-chan watch-chan)
          (do
            (async/go
              (let [data (async/<! ctrl)]
                (log/info "closing watch-chan as ctrl channel has been triggered" {:data data})
                (async/close! watch-chan)))
            (utils/attach-waiter-source
              {:body response-chan
               :headers {"content-type" "application/json"}
               :status http-200-ok}))
          (utils/exception->response (ex-info "tokens-watch-channels-update-chan is closed!" {}) request)))
      (let [ex (ex-info "streaming-timeout query parameter must be an integer"
                        {:log-level :info
                         :status http-400-bad-request
                         :streaming-timeout streaming-timeout})]
        (utils/exception->response ex request)))))

(defn handle-list-tokens-request
  [kv-store entitlement-manager streaming-timeout-ms tokens-watch-channels-update-chan {:keys [request-method] :as req}]
  (try
    (case request-method
      :get (let [{:strs [can-manage-as-user] :as request-params} (-> req ru/query-params-request :query-params)
                 include-deleted (utils/param-contains? request-params "include" "deleted")
                 show-metadata (utils/param-contains? request-params "include" "metadata")
                 should-watch? (utils/request-flag request-params "watch")
                 should-filter-maintenance? (contains? request-params "maintenance")
                 maintenance-active? (utils/request-flag request-params "maintenance")
                 include-run-as-requester (when (contains? request-params "run-as-requester")
                                            (utils/request-flag request-params "run-as-requester"))
                 include-requires-parameters (when (contains? request-params "requires-parameters")
                                               (utils/request-flag request-params "requires-parameters"))
                 owner-param (get request-params "owner")
                 owners (cond
                          (string? owner-param) #{owner-param}
                          (coll? owner-param) (set owner-param)
                          :else (list-token-owners kv-store))
                 filterable-parameter? (disj sd/token-data-keys "owner" "maintenance")
                 parameter-filter-predicates (for [[parameter-name raw-param] request-params
                                                   :when (filterable-parameter? parameter-name)]
                                               (let [search-parameter-values (cond
                                                                               (string? raw-param) #{raw-param}
                                                                               :else (set raw-param))]
                                                 (fn [token-parameters]
                                                   (and (contains? token-parameters parameter-name)
                                                        (contains? search-parameter-values
                                                                   (str (get token-parameters parameter-name)))))))
                 index-filter-fn
                 (every-pred
                   (fn list-tokens-delete-predicate [entry]
                     (or include-deleted (not (:deleted entry))))
                   (fn list-tokens-maintenance-predicate [entry]
                     (or (not should-filter-maintenance?)
                         (= (-> entry :maintenance true?) maintenance-active?)))
                   (fn list-tokens-auth-predicate [{:keys [owner token]}]
                     (or (nil? can-manage-as-user)
                         (authz/manage-token? entitlement-manager can-manage-as-user token {"owner" owner})))
                   (fn list-tokens-parameters-predicate [{:keys [token]}]
                     (let [token-parameters (sd/token->token-parameters
                                              kv-store token
                                              :error-on-missing false
                                              :include-deleted include-deleted)]
                       (and (every? #(% token-parameters) parameter-filter-predicates)
                            (or (nil? include-run-as-requester)
                                (= include-run-as-requester (sd/run-as-requester? token-parameters)))
                            (or (nil? include-requires-parameters)
                                (= include-requires-parameters (sd/requires-parameters? token-parameters)))))))
                 metadata-transducer-fn
                 (fn metadata-predicate [entry]
                   (if show-metadata
                     (update entry :last-update-time tc/from-long)
                     (dissoc entry :deleted :etag :last-update-time)))]
             (if should-watch?
               (handle-list-tokens-watch streaming-timeout-ms index-filter-fn metadata-transducer-fn tokens-watch-channels-update-chan req)
               (->> owners
                    (map
                      (fn [owner]
                        (->> (list-index-entries-for-owner kv-store owner)
                             (map
                               (fn [[token entry]]
                                 (assoc entry :owner owner :token token)))
                             (filter index-filter-fn)
                             (map metadata-transducer-fn))))
                    flatten
                    utils/clj->streaming-json-response)))
      (throw (ex-info "Only GET supported" {:log-level :info
                                            :request-method request-method
                                            :status http-405-method-not-allowed})))
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
                                            :status http-405-method-not-allowed})))
    (catch Exception ex
      (utils/exception->response ex req))))

(defn handle-refresh-token-request
  "Handle a request to refresh token data directly from the KV store, skipping the cache."
  [kv-store tokens-update-chan {{:keys [src-router-id]} :basic-authentication :as req}]
  (try
    (let [{:strs [token owner index] :as json-data} (-> req ru/json-request :body)]
      (log/info "received token refresh request" json-data)
      (when index
        (log/info src-router-id "is force refreshing the token index")
        (refresh-token-index kv-store))
      (when token
        (log/info src-router-id "is force refreshing token" token)
        (refresh-token kv-store token owner)
        (send-internal-index-event tokens-update-chan token))
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
                                             :status http-405-method-not-allowed})))
    (catch Exception ex
      (utils/exception->response ex req))))
