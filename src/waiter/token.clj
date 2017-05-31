;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.token
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.utils :as utils]))

(def ^:const ANY-USER "*")
(def ^:const valid-token-re #"[a-zA-Z]([a-zA-Z0-9\-_$\.])+")

(defn handle-token-request
  "Ring handler for dealing with tokens.

   If handling DELETE, deletes the token configuration if found.

   If handling GET, returns the configuration if found.
   Anyone can see the configuration, b/c it shouldn't contain any sensitive data.

   If handling POST, validates that the user is the creator of the token if it already exists.
   Then, updates the configuration for the token in the database using the newest password."
  [kv-store waiter-hostname can-run-as? make-peer-requests-fn validate-service-description-fn req]
  ;;if post, validate that this is a valid job schema & that the user == the kerberos user, then sign & store in riak
  ;;  remember that we need an extra field in this schema, which is who is allowed to use this. Could be "*" or a string username
  ;;if get, return whatever data's in riak
  (case (:request-method req)
    :delete (try
              (let [req-headers (:headers req)
                    {:keys [token]} (sd/retrieve-token-from-service-description-or-hostname req-headers req-headers waiter-hostname)
                    current-user (get req :authorization/user)]
                (if token
                  (let [service-description-template (sd/token->service-description-template kv-store token :error-on-missing false)]
                    (if (and service-description-template (not-empty service-description-template))
                      (let [token-owner (get service-description-template "owner")]
                        (when-not (can-run-as? current-user token-owner)
                          (throw (ex-info "User not allowed to delete token"
                                          {:existing-owner token-owner
                                           :current-user current-user
                                           :status 403})))
                        (log/info "Deleting token" token)
                        (kv/delete kv-store token)
                        ; notify peers of token delete and ask them to refresh their caches
                        (make-peer-requests-fn (str "token/" token "/refresh") :method :get)
                        (utils/map->json-response {:delete token, :success true}))
                      (utils/map->json-response {:message (str "token " token " does not exist!")} :status 404)))
                  (utils/map->json-response {:message "couldn't find token in request"} :status 400)))
              (catch Exception e
                (log/error e "Token delete failed")
                (utils/exception->json-response e)))
    :get (try
           (let [req-headers (:headers req)
                 {:keys [token]} (sd/retrieve-token-from-service-description-or-hostname req-headers req-headers waiter-hostname)]
             (let [service-description-template (sd/token->service-description-template kv-store token :error-on-missing false)]
               (if (and service-description-template (not-empty service-description-template))
                 ;;NB do not ever return the password to the user
                 (do
                   (log/info "successfully retrieved token " token)
                   (utils/map->json-response service-description-template))
                 (do
                   (log/info "token not found " token)
                   (utils/map->json-response {:message (str "couldn't find token " token)} :status 404)))))
           (catch Exception e
             (log/error e "token GET failed")
             (utils/exception->json-response e)))
    :post (try
            (let [authenticated-user (get req :authorization/user)
                  {:strs [token run-as-user] :as service-description-template} (json/read-str (slurp (:body req)))
                  existing-service-description-template (sd/token->service-description-template kv-store token :error-on-missing false)
                  owner (or (get service-description-template "owner")
                            (get existing-service-description-template "owner")
                            authenticated-user)
                  service-description-template (dissoc service-description-template "token")]
              (when (str/blank? token)
                (throw (ex-info "Must provide the token" {})))
              (when (= waiter-hostname token)
                (throw (ex-info "Token name is reserved" {:token token})))
              (when-not (re-matches valid-token-re token)
                (throw (ex-info "Token must match pattern." {:token token :pattern (str valid-token-re)})))
              (validate-service-description-fn service-description-template)
              (let [unknown-keys (set/difference (set (keys service-description-template)) (set sd/token-service-description-template-keys))]
                (when (not-empty unknown-keys)
                  (throw (ex-info (str "Unsupported key(s) in token: " (str (vec unknown-keys))) {:token token}))))
              (when run-as-user
                (when-not (can-run-as? authenticated-user run-as-user)
                  (throw (ex-info "Cannot run as user" {:authenticated-user authenticated-user :run-as-user run-as-user}))))
              (let [existing-service-description-owner (get existing-service-description-template "owner")]
                (if-not (str/blank? existing-service-description-owner)
                  (when-not (can-run-as? authenticated-user existing-service-description-owner)
                    (throw (ex-info "Cannot change owner of token"
                                    {:existing-owner existing-service-description-owner
                                     :new-user owner})))
                  (when-not (can-run-as? authenticated-user owner)
                    (throw (ex-info "Cannot create token as user" {:authenticated-user authenticated-user :owner owner})))))
              ; Store the token
              (sd/store-service-description-for-token kv-store token (assoc service-description-template "owner" owner))
              ; notify peers of token update
              (make-peer-requests-fn (str "token/" token "/refresh") :method :get)
              (utils/map->json-response {:message (str "Successfully created " token)
                                         :service-description service-description-template}))
            (catch Exception e
              (log/error e "Token post failed")
              (utils/exception->json-response e)))))

(defn handle-refresh-token-request
  [kv-store token src-router-id _]
  (try
    (when (str/blank? token)
      (throw (ex-info "Token is missing!" {})))
    (let [refreshed-token (kv/fetch kv-store token :refresh true)]
      (log/info src-router-id "has force refreshed token" token "token contents:" refreshed-token))
    (utils/map->json-response {:success true, :token token})
    (catch Exception e
      (utils/exception->json-response e))))
