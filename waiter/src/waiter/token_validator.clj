(ns waiter.token-validator
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.authorization :as authz]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]))

(defprotocol TokenValidator
  "A protocol for validating a new token configuration"

  (state [this include-flags]
    "Returns the global state of the token validator")

  (validate [this token-data]
    "Throws an error if the token-data is invalid, and otherwise returns nil"))

(defrecord DefaultTokenValidator [entitlement-manager kv-store]
  TokenValidator

  (state [_ _]
    {:supported-include-params []
     :type "DefaultTokenValidator"})

  (validate [_ {:keys [authenticated-user existing-token-metadata headers new-service-parameter-template new-token-data
                       new-token-metadata new-user-metadata owner service-parameter-with-service-defaults token update-mode
                       validate-service-description-fn version-hash waiter-hostnames]}]
    (let [{:strs [authentication interstitial-secs permitted-user run-as-user]} new-service-parameter-template
          admin-mode? (= "admin" update-mode)
          deleted? (true? (get new-token-data "deleted"))]
      (when (str/blank? token)
        (throw (ex-info "Must provide the token" {:status http-400-bad-request :log-level :warn})))
      (when (some #(= token %) waiter-hostnames)
        (throw (ex-info "Token name is reserved" {:status http-403-forbidden :token token :log-level :warn})))
      (when (empty? (select-keys new-token-data sd/token-user-editable-keys))
        (throw (ex-info (str "No parameters provided for " token) {:status http-400-bad-request :log-level :warn})))
      (sd/validate-token token)

      ; skip user provided service description validation if the update mode is admin and the resulting token will be soft-deleted
      (when (not (and admin-mode? deleted?))
        (validate-service-description-fn new-service-parameter-template)
        (sd/validate-user-metadata-schema new-user-metadata new-service-parameter-template))

      (let [unknown-keys (-> new-token-data
                             keys
                             set
                             (set/difference sd/token-data-keys)
                             (disj "token"))]
        (when (not-empty unknown-keys)
          (throw (ex-info (str "Unsupported key(s) in token: " (str (vec unknown-keys)))
                          {:status http-400-bad-request :token token :log-level :warn}))))
      (let [missing-parameters (->> sd/service-required-keys (remove #(contains? service-parameter-with-service-defaults %1)) seq)]
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

      (case update-mode
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
              (throw (ex-info (str "Cannot run as user: " run-as-user)
                              {:authenticated-user authenticated-user
                               :run-as-user run-as-user
                               :status http-403-forbidden
                               :log-level :warn}))))
          (if creating-token?
            ;; new token creation
            (when-not (authz/run-as? entitlement-manager authenticated-user owner)
              (throw (ex-info (str "Cannot create token as provided owner: " owner)
                              {:authenticated-user authenticated-user
                               :owner owner
                               :status http-403-forbidden
                               :log-level :warn})))
            ;; editing token
            (let [delegated-user (or (when editing? existing-owner) authenticated-user)]
              (when-not (authz/manage-token? entitlement-manager delegated-user token existing-token-metadata)
                (throw (ex-info (str "Cannot update token ("
                                     (when existing-editor
                                       (str "editor=" existing-editor ", "))
                                     "owner=" existing-owner ") as user: " delegated-user)
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

        (throw (ex-info (str "Invalid update-mode: " update-mode)
                        {:mode update-mode
                         :status http-400-bad-request
                         :log-level :warn})))

      (when-let [previous (get new-token-metadata "previous")]
        (when-not (map? previous)
          (throw (ex-info (str "Token previous must be a map")
                          {:previous previous :status http-400-bad-request :token token :log-level :warn})))))))

(defn create-default-token-validator
  "Creates the default token validator."
  [{:keys [entitlement-manager kv-store]}]
  {:pre [(some? entitlement-manager)
         (some? kv-store)]}
  (->DefaultTokenValidator entitlement-manager kv-store))
