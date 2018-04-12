;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.token
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.ring-utils :as ru]
            [waiter.service-description :as sd]
            [waiter.utils :as utils])
  (:import (org.joda.time DateTime)))

(def ^:const ANY-USER "*")
(def ^:const valid-token-re #"[a-zA-Z]([a-zA-Z0-9\-_$\.])+")

(defn sanitize-history
  "Limits the history length stored in the token-data."
  [token-data history-length]
  (utils/dissoc-in token-data (repeat history-length "previous")))

(defn ensure-history
  "Ensures a non-nil previous entry exists in `token-data`.
   If one already was present when this function was called, returns `token-data` unmodified.
   Else assoc `(or previous {})` into `token-data`."
  [token-data previous]
  (update token-data "previous" (fn [current-previous] (or current-previous previous {}))))

(let [etag-prefix "E-"]
  (defn token-data->etag
    "Converts the merged map of service-description and token-metadata to an etag."
    [token-data]
    (when (seq token-data)
      (str etag-prefix (-> token-data
                           (select-keys sd/token-data-keys)
                           (dissoc "previous")
                           utils/parameters->id)))))

(defn- token-description->etag
  "Converts the token metadata to an etag."
  [{:keys [service-description-template token-metadata]}]
  (-> (merge service-description-template token-metadata)
      token-data->etag))

(defn- validate-token-modification-based-on-etag
  "Validates whether the token modification should be allowed on based on the provided etag."
  [{:keys [token-metadata] :as token-description} version-etag]
  (when version-etag
    (when (not= (token-description->etag token-description) (str version-etag))
      (throw (ex-info "Cannot modify stale token"
                      {:etag version-etag
                       :status 412
                       :token-metadata token-metadata})))))

;; We'd like to maintain an index of tokens by their owner.
;; We'll store an index in the key "^TOKEN_OWNERS" that maintains
;; a map of owner to another key, in which we'll store the tokens
;; that that owner owns.

(let [token-lock "TOKEN_LOCK"
      token-owners-key "^TOKEN_OWNERS"
      update-kv! (fn update-kv [kv-store k f]
                   (->> (kv/fetch kv-store k :refresh true)
                        f
                        (kv/store kv-store k)))
      new-owner-key (fn [] (str "^TOKEN_OWNERS_" (utils/unique-identifier)))
      ensure-owner-key (fn ensure-owner-key [kv-store owner->owner-key owner] ;; must be invoked inside a critical section
                         (when-not owner
                           (throw (ex-info "nil owner passed to ensure-owner-key"
                                           {:owner->owner-key owner->owner-key})))
                         (or (get owner->owner-key owner)
                             (let [new-owner-key (new-owner-key)]
                               (log/info "storing" new-owner-key "for" owner "in the token-owners-key")
                               (kv/store kv-store token-owners-key (assoc owner->owner-key owner new-owner-key))
                               new-owner-key)))
      delete-token-from-index (fn delete-token-from-index [index-entries token-to-remove]
                                (dissoc index-entries token-to-remove))
      insert-token-into-index (fn insert-token-into-index [index-entries token-to-insert token-etag deleted]
                                (assoc index-entries token-to-insert {:deleted (true? deleted) :etag token-etag}))]

  (defn store-service-description-for-token
    "Store the token mapping of the service description template in the key-value store."
    [synchronize-fn kv-store history-length ^String token service-description-template token-metadata & {:keys [version-etag]}]
    (synchronize-fn
      token-lock
      (fn inner-store-service-description-for-token []
        (log/info "storing service description for token:" token)
        (let [token-description (merge service-description-template (select-keys token-metadata sd/token-metadata-keys))
              {:strs [deleted owner] :as new-token-data} (sd/sanitize-service-description token-description sd/token-data-keys)
              existing-token-data (kv/fetch kv-store token :refresh true)
              existing-token-data (if-not (get existing-token-data "deleted") existing-token-data {})
              existing-token-description (sd/token-data->token-description existing-token-data)
              existing-owner (get existing-token-data "owner")
              owner->owner-key (kv/fetch kv-store token-owners-key)]
          ; Validate the token modification for concurrency races
          (validate-token-modification-based-on-etag existing-token-description version-etag)
          ; Store the service description
          (kv/store kv-store token (-> new-token-data
                                       (ensure-history existing-token-data)
                                       (sanitize-history history-length)))
          ; Remove token from previous owner
          (when (and existing-owner (not= owner existing-owner))
            (let [previous-owner-key (ensure-owner-key kv-store owner->owner-key existing-owner)]
              (log/info "removing" token "from index of" existing-owner)
              (update-kv! kv-store previous-owner-key (fn [index] (delete-token-from-index index token)))))
          ; Add token to new owner
          (when owner
            (let [owner-key (ensure-owner-key kv-store owner->owner-key owner)
                  token-etag' (token-data->etag new-token-data)]
              (log/info "inserting" token "into index of" owner)
              (update-kv! kv-store owner-key (fn [index] (insert-token-into-index index token token-etag' deleted)))))
          (log/info "stored service description template for" token)))))

  (defn delete-service-description-for-token
    "Delete a token from the KV"
    [clock synchronize-fn kv-store history-length token owner authenticated-user &
     {:keys [hard-delete version-etag] :or {hard-delete false}}]
    (synchronize-fn
      token-lock
      (fn inner-delete-service-description-for-token []
        (log/info "attempting to delete service description for token:" token " hard-delete:" hard-delete)
        (let [existing-token-data (kv/fetch kv-store token)
              existing-token-description (sd/token-data->token-description existing-token-data)]
          ; Validate the token modification for concurrency races
          (validate-token-modification-based-on-etag existing-token-description version-etag)
          (if hard-delete
            (kv/delete kv-store token)
            (when existing-token-data
              (let [new-token-data (assoc existing-token-data
                                     "deleted" true
                                     "last-update-time" (.getMillis ^DateTime (clock))
                                     "last-update-user" authenticated-user)]
                (kv/store kv-store token (-> new-token-data
                                             (ensure-history existing-token-data)
                                             (sanitize-history history-length)))))))
        ; Remove token from owner (hard-delete) or set the deleted flag (soft-delete)
        (when owner
          (let [owner->owner-key (kv/fetch kv-store token-owners-key)
                owner-key (ensure-owner-key kv-store owner->owner-key owner)]
            (update-kv! kv-store owner-key (fn [index] (delete-token-from-index index token)))
            (when (not hard-delete)
              (let [etag (token-data->etag (kv/fetch kv-store token))]
                (update-kv! kv-store owner-key (fn [index] (insert-token-into-index index token etag true)))))))
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
      (if-let [owner-key (owner->owner-key owner)]
        (kv/fetch kv-store owner-key)
        (throw (ex-info "no owner-key found" {:owner owner :status 500})))))

  (defn list-token-owners
    "List token owners."
    [kv-store]
    (-> (kv/fetch kv-store token-owners-key)
        keys
        set))

  (defn token-owners-map
    "Get the token owners map state"
    [kv-store]
    (-> (kv/fetch kv-store token-owners-key) (into {})))

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
                                           (let [{:strs [deleted] :as token-data} (kv/fetch kv-store token)
                                                 token-etag (token-data->etag token-data)]
                                             {:deleted (true? deleted)
                                              :etag token-etag}))
                                         tokens))
                                     owner->tokens)
              owner->owner-key (pc/map-from-keys (fn [_] (new-owner-key)) (keys owner->index-entries))]
          ; Create new owner map
          (kv/store kv-store token-owners-key owner->owner-key)
          ; Write each owner node
          (doseq [[owner index-entries] owner->index-entries]
            (let [owner-key (get owner->owner-key owner)]
              (kv/store kv-store owner-key index-entries))))))))

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
            {:keys [service-description-template token-metadata]} token-description]
        (if (and service-description-template (not-empty service-description-template))
          (let [token-owner (get token-metadata "owner")
                version-etag (get headers "if-match")]
            (if hard-delete
              (do
                (when-not version-etag
                  (throw (ex-info "Must specify if-match header for token hard deletes"
                                  {:request-headers headers, :status 400})))
                (when-not (authz/administer-token? entitlement-manager authenticated-user token token-metadata)
                  (throw (ex-info "Cannot hard-delete token"
                                  {:metadata token-metadata
                                   :status 403
                                   :user authenticated-user}))))
              (when-not (authz/manage-token? entitlement-manager authenticated-user token token-metadata)
                (throw (ex-info "User not allowed to delete token"
                                {:owner token-owner
                                 :status 403
                                 :user authenticated-user}))))
            (delete-service-description-for-token
              clock synchronize-fn kv-store history-length token token-owner authenticated-user
              :hard-delete hard-delete :version-etag version-etag)
            ; notify peers of token delete and ask them to refresh their caches
            (make-peer-requests-fn "tokens/refresh"
                                   :body (json/write-str {:owner token-owner, :token token})
                                   :method :post)
            (utils/map->json-response {:delete token, :hard-delete hard-delete, :success true}
                                      :headers {"etag" version-etag}))
          (throw (ex-info (str "Token " token " does not exist")
                          {:status 404 :token token}))))
      (throw (ex-info "Couldn't find token in request" {:status 400 :token token})))))

(defn- handle-get-token-request
  "Returns the configuration if found.
   Anyone can see the configuration, b/c it shouldn't contain any sensitive data."
  [kv-store token-root waiter-hostnames {:keys [headers] :as request}]
  (let [request-params (-> request ru/query-params-request :query-params)
        include-deleted (utils/param-contains? request-params "include" "deleted")
        show-metadata (utils/param-contains? request-params "include" "metadata")
        token (or (get request-params "token")
                  (:token (sd/retrieve-token-from-service-description-or-hostname headers headers waiter-hostnames)))
        token-description (sd/token->token-description kv-store token :include-deleted include-deleted)
        {:keys [service-description-template token-metadata]} token-description
        token-etag (token-description->etag token-description)]
    (if (and service-description-template (not-empty service-description-template))
      ;;NB do not ever return the password to the user
      (let [epoch-time->date-time (fn [epoch-time] (DateTime. epoch-time))]
        (log/info "successfully retrieved token " token)
        (utils/map->json-response
          (cond-> service-description-template
                  show-metadata
                  (merge (cond-> (loop [loop-token-metadata token-metadata
                                        nested-last-update-time-path ["last-update-time"]]
                                   (if (get-in loop-token-metadata nested-last-update-time-path)
                                     (recur (update-in loop-token-metadata nested-last-update-time-path epoch-time->date-time)
                                            (concat ["previous"] nested-last-update-time-path))
                                     loop-token-metadata))
                                 (not (contains? token-metadata "root"))
                                 (assoc "root" token-root))))
          :headers {"etag" token-etag}))
      (do
        (throw (ex-info (str "Couldn't find token " token)
                        {:headers {"etag" token-etag}
                         :status 404
                         :token token}))))))

(defn- handle-post-token-request
  "Validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [clock synchronize-fn kv-store token-root history-length waiter-hostnames entitlement-manager
   make-peer-requests-fn validate-service-description-fn {:keys [headers] :as request}]
  (let [request-params (-> request ru/query-params-request :query-params)
        authenticated-user (get request :authorization/user)
        {:strs [token] :as new-token-description} (-> request
                                                      ru/json-request
                                                      :body
                                                      sd/transform-allowed-params-token-entry)
        new-token-metadata (select-keys new-token-description sd/token-metadata-keys)
        {:strs [authentication interstitial-secs permitted-user run-as-user] :as new-service-description-template}
        (select-keys new-token-description sd/service-description-keys)
        {existing-token-metadata :token-metadata} (sd/token->token-description kv-store token)
        owner (or (get new-token-metadata "owner")
                  (get existing-token-metadata "owner")
                  authenticated-user)
        version-etag (get headers "if-match")]
    (when (str/blank? token)
      (throw (ex-info "Must provide the token" {:status 400})))
    (when (some #(= token %) waiter-hostnames)
      (throw (ex-info "Token name is reserved" {:status 403 :token token})))
    (when-not (re-matches valid-token-re token)
      (throw (ex-info "Token must match pattern"
                      {:status 400 :token token :pattern (str valid-token-re)})))
    (validate-service-description-fn new-service-description-template)
    (let [unknown-keys (-> new-token-description
                           keys
                           set
                           (set/difference sd/token-data-keys)
                           (disj "token"))]
      (when (not-empty unknown-keys)
        (throw (ex-info (str "Unsupported key(s) in token: " (str (vec unknown-keys)))
                        {:status 400 :token token}))))
    (when (= authentication "disabled")
      (when (not= permitted-user "*")
        (throw (ex-info (str "Tokens with authentication disabled must specify"
                             " permitted-user as *, instead provided " permitted-user)
                        {:status 400 :token token})))
      ;; partial tokens not supported when authentication is disabled
      (when-not (sd/required-keys-present? new-service-description-template)
        (throw (ex-info "Tokens with authentication disabled must specify all required parameters"
                        {:missing-parameters (->> sd/service-required-keys
                                                  (remove #(contains? new-service-description-template %1)) seq)
                         :service-description new-service-description-template
                         :status 400}))))
    (when (and interstitial-secs (not (sd/required-keys-present? new-service-description-template)))
      (throw (ex-info (str "Tokens with missing required parameters cannot use interstitial support")
                      {:status 400 :token token})))
    (case (get request-params "update-mode")
      "admin"
      (do
        (when (and (seq existing-token-metadata) (not version-etag))
          (throw (ex-info "Must specify if-match header for admin mode token updates"
                          {:request-headers headers, :status 400})))
        (when-not (authz/administer-token? entitlement-manager authenticated-user token new-token-metadata)
          (throw (ex-info "Cannot administer token"
                          {:status 403
                           :token-metadata new-token-metadata
                           :user authenticated-user}))))

      nil
      (do
        (when (and run-as-user (not= "*" run-as-user))
          (when-not (authz/run-as? entitlement-manager authenticated-user run-as-user)
            (throw (ex-info "Cannot run as user"
                            {:authenticated-user authenticated-user
                             :run-as-user run-as-user
                             :status 403}))))
        (let [existing-service-description-owner (get existing-token-metadata "owner")]
          (if-not (str/blank? existing-service-description-owner)
            (when-not (authz/manage-token? entitlement-manager authenticated-user token existing-token-metadata)
              (throw (ex-info "Cannot change owner of token"
                              {:existing-owner existing-service-description-owner
                               :new-user owner
                               :status 403})))
            (when-not (authz/run-as? entitlement-manager authenticated-user owner)
              (throw (ex-info "Cannot create token as user"
                              {:authenticated-user authenticated-user
                               :owner owner
                               :status 403})))))
        (when (contains? new-token-metadata "last-update-time")
          (throw (ex-info "Cannot modify last-update-time token metadata"
                          {:status 400
                           :token-metadata new-token-metadata})))
        (when (contains? new-token-metadata "last-update-user")
          (throw (ex-info "Cannot modify last-update-user token metadata"
                          {:status 400
                           :token-metadata new-token-metadata})))
        (when (contains? new-token-metadata "root")
          (throw (ex-info "Cannot modify root token metadata"
                          {:status 400
                           :token-metadata new-token-metadata})))
        (when (contains? new-token-metadata "previous")
          (throw (ex-info "Cannot modify previous token metadata"
                          {:status 400
                           :token-metadata new-token-metadata}))))

      (throw (ex-info "Invalid update-mode"
                      {:mode (get request-params "update-mode")
                       :status 400})))

    (when-let [previous (get new-token-metadata "previous")]
      (when-not (map? previous)
        (throw (ex-info (str "Token previous must be a map")
                        {:previous previous :status 400 :token token}))))

    ; Store the token
    (let [new-token-metadata (merge {"last-update-time" (.getMillis ^DateTime (clock))
                                     "last-update-user" authenticated-user
                                     "owner" owner
                                     "root" (or (get existing-token-metadata "root") token-root)}
                                    new-token-metadata)]
      (store-service-description-for-token
        synchronize-fn kv-store history-length token new-service-description-template new-token-metadata
        :version-etag version-etag)
      ; notify peers of token update
      (make-peer-requests-fn "tokens/refresh"
                             :method :post
                             :body (json/write-str {:token token, :owner owner}))
      (utils/map->json-response {:message (str "Successfully created " token)
                                 :service-description new-service-description-template}
                                :headers {"etag" (-> {:service-description-template new-service-description-template
                                                      :token-metadata new-token-metadata}
                                                     token-description->etag)}))))

(defn handle-token-request
  "Ring handler for dealing with tokens.

   If handling DELETE, deletes the token configuration if found.

   If handling GET, returns the configuration if found.
   Anyone can see the configuration, b/c it shouldn't contain any sensitive data.

   If handling POST, validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [clock synchronize-fn kv-store token-root history-length waiter-hostnames entitlement-manager make-peer-requests-fn
   validate-service-description-fn {:keys [request-method] :as request}]
  (try
    (case request-method
      :delete (handle-delete-token-request clock synchronize-fn kv-store history-length waiter-hostnames entitlement-manager
                                           make-peer-requests-fn request)
      :get (handle-get-token-request kv-store token-root waiter-hostnames request)
      :post (handle-post-token-request clock synchronize-fn kv-store token-root history-length waiter-hostnames entitlement-manager
                                       make-peer-requests-fn validate-service-description-fn request)
      (throw (ex-info "Invalid request method" {:request-method request-method, :status 405})))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn handle-list-tokens-request
  [kv-store {:keys [request-method] :as req}]
  (try
    (case request-method
      :get (let [request-params (-> req ru/query-params-request :query-params)
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
                           (map
                             (fn [[token entry]]
                               (cond-> (assoc entry :owner owner :token token)
                                       (not show-metadata)
                                       (dissoc :deleted :etag)))))))
                  flatten
                  utils/map->streaming-json-response))
      (throw (ex-info "Only GET supported" {:request-method request-method
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
             (utils/map->json-response owner->owner-ref))
      (throw (ex-info "Only GET supported" {:request-method request-method
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
      (utils/map->json-response {:success true}))
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
                                     :body (json/write-str {:index true}))
              (utils/map->json-response {:message "Successfully re-indexed" :tokens (count tokens)}))
      (throw (ex-info "Only POST supported" {:request-method request-method
                                             :status 405})))
    (catch Exception ex
      (utils/exception->response ex req))))
