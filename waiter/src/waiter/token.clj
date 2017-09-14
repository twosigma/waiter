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
            [ring.middleware.params :as ring-params]
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.utils :as utils])
  (:import (org.joda.time DateTime)))

(def ^:const ANY-USER "*")
(def ^:const valid-token-re #"[a-zA-Z]([a-zA-Z0-9\-_$\.])+")

;; We'd like to maintain an index of tokens by their owner.
;; We'll store an index in the key "^TOKEN_OWNERS" that maintains
;; a map of owner to another key, in which we'll store the tokens
;; that that owner owns.

(let [token-lock "TOKEN_LOCK"
      token-owners-key "^TOKEN_OWNERS"
      update-kv (fn update-kv [kv-store k f]
                  (->> (kv/fetch kv-store k :refresh true)
                       f
                       (kv/store kv-store k)))
      new-owner-key (fn [] (str "^TOKEN_OWNERS_" (utils/unique-identifier)))
      ensure-owner-key (fn ensure-owner-key [kv-store owner->owner-key owner]
                         (when-not owner
                           (throw (ex-info "nil owner passed to ensure-owner-key"
                                           {:owner->owner-key owner->owner-key})))
                         (or (get owner->owner-key owner)
                             (let [new-owner-key (new-owner-key)]
                               (kv/store kv-store token-owners-key (assoc owner->owner-key owner new-owner-key))
                               new-owner-key)))]

  (defn store-service-description-for-token
    "Store the token mapping of the service description template in the key-value store."
    [synchronize-fn kv-store ^String token service-description-template token-metadata]
    (synchronize-fn
      token-lock
      (fn inner-store-service-description-for-token []
        (log/info "storing service description for token:" token)
        (let [token-description (merge service-description-template (select-keys token-metadata sd/token-metadata-keys))
              {:strs [owner] :as filtered-service-desc} (sd/sanitize-service-description token-description sd/token-description-keys)
              previous-owner (get (kv/fetch kv-store token :refresh true) "owner")
              owner->owner-key (kv/fetch kv-store token-owners-key)]
          ; Store the service description
          (kv/store kv-store token filtered-service-desc)
          ; Remove token from previous owner
          (when (and previous-owner (not= owner previous-owner))
            (let [previous-owner-key (ensure-owner-key kv-store owner->owner-key previous-owner)]
              (update-kv kv-store previous-owner-key (fn [v] (disj v token)))))
          ; Add token to new owner
          (when owner
            (let [owner-key (ensure-owner-key kv-store owner->owner-key owner)]
              (update-kv kv-store owner-key (fn [v] (conj (or v #{}) token)))))
          (log/info "stored service description template for" token)))))

  (defn delete-service-description-for-token
    "Delete a token from the KV"
    [clock synchronize-fn kv-store token owner & {:keys [hard-delete] :or {hard-delete false}}]
    (synchronize-fn
      token-lock
      (fn inner-delete-service-description-for-token []
        (log/info "attempting to delete service description for token:" token " hard-delete:" hard-delete)
        (if hard-delete
          (kv/delete kv-store token)
          (when-let [existing-token-description (kv/fetch kv-store token)]
            (let [new-token-description (assoc existing-token-description
                                          "deleted" true
                                          "last-update-time" (.getMillis ^DateTime (clock)))]
              (kv/store kv-store token new-token-description))))
        ; Remove token from owner
        (when owner
          (let [owner->owner-key (kv/fetch kv-store token-owners-key)
                owner-key (ensure-owner-key kv-store owner->owner-key owner)]
            (update-kv kv-store owner-key (fn [v] (disj v token)))))
        ; Don't bother removing owner from token-owners, even if they have no tokens now
        (log/info "deleted token for" token))))

  (defn refresh-token
    "Refresh the KV cache for a given token"
    [kv-store token owner]
    (let [refreshed-token (kv/fetch kv-store token :refresh true)]
      (when owner
        ; NOTE: The token may still show up temporarily in the old owners list
        (let [owner->owner-key (kv/fetch kv-store token-owners-key)
              owner-key (ensure-owner-key kv-store owner->owner-key owner)]
          (kv/fetch kv-store owner-key :refresh true)))
      refreshed-token))

  (defn refresh-token-index
    "Refresh the KV cache for token index keys"
    [kv-store]
    (let [owner->owner-key (kv/fetch kv-store token-owners-key :refresh true)]
      (doseq [[_ owner-key] owner->owner-key]
        (kv/fetch kv-store owner-key :refresh true))))

  (defn list-tokens-for-owner
    "List all tokens for a given user."
    [kv-store owner]
    (set (let [owner->owner-key (kv/fetch kv-store token-owners-key)
               owner-key (ensure-owner-key kv-store owner->owner-key owner)]
           (kv/fetch kv-store owner-key))))

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
                                 (group-by :owner))
              owner->owner-key (->> owner->tokens
                                    keys
                                    (map (fn [owner] [owner (new-owner-key)]))
                                    (into {}))]
          ; Create new owner map
          (kv/store kv-store token-owners-key owner->owner-key)
          ; Write each owner node
          (doseq [[owner tokens] owner->tokens]
            (let [owner-key (get owner->owner-key owner)
                  token-set (->> tokens (map :token) set)]
              (kv/store kv-store owner-key token-set))))))))

(defn handle-token-request
  "Ring handler for dealing with tokens.

   If handling DELETE, deletes the token configuration if found.

   If handling GET, returns the configuration if found.
   Anyone can see the configuration, b/c it shouldn't contain any sensitive data.

   If handling POST, validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [clock synchronize-fn kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
   {:keys [headers request-method] :as req}]
  ;;if post, validate that this is a valid job schema & that the user == the kerberos user, then sign & store in riak
  ;;  remember that we need an extra field in this schema, which is who is allowed to use this. Could be "*" or a string username
  ;;if get, return whatever data's in riak
  (case request-method
    :delete (try
              (let [{:keys [token]} (sd/retrieve-token-from-service-description-or-hostname headers headers waiter-hostname)
                    authenticated-user (get req :authorization/user)
                    request-params (:query-params (ring-params/params-request req))
                    hard-delete (utils/request-flag request-params "hard-delete")]
                (if token
                  (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token :include-deleted hard-delete)]
                    (if (and service-description-template (not-empty service-description-template))
                      (let [token-owner (get token-metadata "owner")]
                        (if hard-delete
                          (when-not (authz/administer-token? entitlement-manager authenticated-user token token-metadata)
                            (throw (ex-info "Cannot hard-delete token."
                                            {:metadata token-metadata
                                             :status 403
                                             :user authenticated-user})))
                          (when-not (authz/manage-token? entitlement-manager authenticated-user token token-metadata)
                            (throw (ex-info "User not allowed to delete token."
                                            {:owner token-owner
                                             :status 403
                                             :user authenticated-user}))))
                        (delete-service-description-for-token clock synchronize-fn kv-store token token-owner :hard-delete hard-delete)
                        ; notify peers of token delete and ask them to refresh their caches
                        (make-peer-requests-fn "tokens/refresh"
                                               :body (json/write-str {:owner token-owner, :token token})
                                               :method :post)
                        (utils/map->json-response {:delete token, :hard-delete hard-delete, :success true}))
                      (throw (ex-info (str "Token " token " does not exist.")
                                      {:status 404 :token token}))))
                  (throw (ex-info "Couldn't find token in request."
                                  {:status 400 :token token}))))
              (catch Exception ex
                (utils/exception->response ex req)))
    :get (try
           (let [request-params (:query-params (ring-params/params-request req))
                 include-deleted (utils/request-flag request-params "include-deleted")
                 {:keys [token]} (sd/retrieve-token-from-service-description-or-hostname headers headers waiter-hostname)]
             (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token :include-deleted include-deleted)]
               (if (and service-description-template (not-empty service-description-template))
                 ;;NB do not ever return the password to the user
                 (do
                   (log/info "successfully retrieved token " token)
                   (utils/map->json-response (merge service-description-template token-metadata)))
                 (do
                   (throw (ex-info (str "Couldn't find token " token) 
                                   {:status 404 :token token}))))))
           (catch Exception ex
             (utils/exception->response ex req)))
    :post (try
            (let [request-params (:query-params (ring-params/params-request req))
                  authenticated-user (get req :authorization/user)
                  {:strs [token] :as new-token-description} (json/read-str (slurp (:body req)))
                  new-token-metadata (select-keys new-token-description sd/token-metadata-keys)
                  {:strs [authentication permitted-user run-as-user] :as new-service-description-template}
                  (select-keys new-token-description sd/service-description-keys)
                  {existing-token-metadata :token-metadata} (sd/token->token-description kv-store token)
                  owner (or (get new-token-metadata "owner")
                            (get existing-token-metadata "owner")
                            authenticated-user)]
              (when (str/blank? token)
                (throw (ex-info "Must provide the token." {:status 400})))
              (when (= waiter-hostname token)
                (throw (ex-info "Token name is reserved." {:status 403 :token token})))
              (when-not (re-matches valid-token-re token)
                (throw (ex-info "Token must match pattern." 
                                {:status 400 :token token :pattern (str valid-token-re)})))
              (validate-service-description-fn new-service-description-template)
              (let [unknown-keys (set/difference (-> new-token-description keys set)
                                                 (set sd/token-description-keys)
                                                 #{"token"})]
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
              (case (get request-params "update-mode")
                "admin"
                (when-not (authz/administer-token? entitlement-manager authenticated-user token new-token-metadata)
                  (throw (ex-info "Cannot administer token" 
                                  {:status 403
                                   :token-metadata new-token-metadata
                                   :user authenticated-user})))

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
                                     :token-metadata new-token-metadata}))))
                (throw (ex-info "Invalid update-mode" 
                                {:mode (get request-params "update-mode")
                                 :status 400})))

              ; Store the token
              (let [new-token-metadata (merge {"last-update-time" (.getMillis ^DateTime (clock))
                                               "owner" owner}
                                              new-token-metadata)]
                (store-service-description-for-token synchronize-fn kv-store token new-service-description-template new-token-metadata))
              ; notify peers of token update
              (make-peer-requests-fn "tokens/refresh"
                                     :method :post
                                     :body (json/write-str {:token token, :owner owner}))
              (utils/map->json-response {:message (str "Successfully created " token)
                                         :service-description new-service-description-template}))
            (catch Exception ex
              (utils/exception->response ex req)))))

(defn handle-list-tokens-request
  [kv-store {:keys [request-method] :as req}]
  (try
    (case request-method
      :get (let [request-params (:params (ring-params/params-request req))
                 owner (get request-params "owner")
                 owners (if owner (set [owner]) (list-token-owners kv-store))]
             (->> owners
                  (map (fn [owner] (->> (list-tokens-for-owner kv-store owner)
                                        (map (fn [v] {:token v, :owner owner})))))
                  flatten
                  utils/map->streaming-json-response))
      (throw (ex-info "Only GET supported." {:request-method request-method
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
      (throw (ex-info "Only GET supported." {:request-method request-method
                                             :status 405})))
    (catch Exception ex
      (utils/exception->response ex req))))

(defn handle-refresh-token-request
  "Handle a request to refresh token data directly from the KV store, skipping the cache."
  [kv-store src-router-id {:keys [body] :as req}]
  (try
    (let [{:strs [token owner index] :as json-data} (json/read-str (slurp body))]
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
              (utils/map->json-response {:message "Successfully re-indexed." :tokens (count tokens)}))
      (throw (ex-info "Only POST supported." {:request-method request-method
                                              :status 405})))
    (catch Exception ex
      (utils/exception->response ex req))))
