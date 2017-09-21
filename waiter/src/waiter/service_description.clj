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
(ns waiter.service-description
  (:require [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [digest]
            [plumbing.core :as pc]
            [schema.core :as s]
            [waiter.authorization :as authz]
            [waiter.headers :as headers]
            [waiter.kv :as kv]
            [waiter.schema :as schema]
            [waiter.utils :as utils]
            [slingshot.slingshot :as sling])
  (:import (org.joda.time DateTime)
           (schema.core RequiredKey)
           (schema.utils ValidationError)))

(def ^:const default-health-check-path "/status")

(def reserved-environment-vars #{"WAITER_USERNAME"
                                 "WAITER_PASSWORD"
                                 "HOME"
                                 "LOGNAME"
                                 "USER"})

(defn reserved-environment-variable? [name]
  (or (contains? reserved-environment-vars name)
      (str/starts-with? name "MARATHON_")
      (str/starts-with? name "MESOS_")
      (re-matches #"^PORT\d*$" name)))

(def service-description-schema
  {;; Required
   (s/required-key "cpus") schema/positive-num
   (s/required-key "mem") schema/positive-num
   (s/required-key "cmd") schema/non-empty-string
   (s/required-key "version") schema/non-empty-string
   (s/required-key "run-as-user") schema/non-empty-string
   ;; Optional
   (s/optional-key "authentication") schema/valid-authentication
   (s/optional-key "backend-proto") schema/valid-backend-proto
   (s/optional-key "cmd-type") schema/non-empty-string
   (s/optional-key "metadata") (s/constrained {(s/both schema/valid-string-length #"^[a-z][a-z0-9\\-]*$")
                                               schema/valid-string-length}
                                              #(< (count %) 100))
   (s/optional-key "distribution-scheme") (s/enum "balanced" "simple")
   ; Marathon imposes a 512 character limit on environment variable keys and values
   (s/optional-key "env") (s/constrained {(s/both (s/constrained s/Str #(<= 1 (count %) 512))
                                                  #"^[A-Za-z][A-Za-z0-9_]*$"
                                                  (s/pred #(not (reserved-environment-variable? %)) 'reserved-environment-variable))
                                          (s/constrained s/Str #(<= 1 (count %) 512))}
                                         #(< (count %) 100))
   (s/optional-key "name") schema/non-empty-string
   (s/optional-key "permitted-user") schema/non-empty-string
   (s/optional-key "ports") schema/valid-number-of-ports
   (s/optional-key "metric-group") schema/valid-metric-group
   ; start-up related
   (s/optional-key "health-check-url") schema/non-empty-string
   (s/optional-key "grace-period-secs") (s/both s/Int (s/pred #(<= 1 % (t/in-seconds (t/minutes 60))) 'at-most-60-minutes))
   (s/optional-key "idle-timeout-mins") (s/both s/Int (s/pred #(<= 1 % (t/in-minutes (t/weeks 1))) 'between-1-minute-and-1-week))
   (s/optional-key "restart-backoff-factor") schema/positive-number-greater-than-or-equal-to-1
   ; auto-scaling related
   (s/optional-key "min-instances") (s/both s/Int (s/pred #(<= 0 % 2) 'between-zero-and-two))
   (s/optional-key "max-instances") (s/both s/Int (s/pred #(<= 1 % 1000) 'between-one-and-1000))
   (s/optional-key "scale-factor") schema/positive-fraction-less-than-or-equal-to-1
   (s/optional-key "scale-up-factor") schema/positive-fraction-less-than-1
   (s/optional-key "scale-down-factor") schema/positive-fraction-less-than-1
   (s/optional-key "jitter-threshold") schema/greater-than-or-equal-to-0-less-than-1
   (s/optional-key "concurrency-level") (s/both s/Int (s/pred #(<= 1 % 10000) 'between-one-and-10000))
   (s/optional-key "instance-expiry-mins") (s/constrained s/Int #(<= 1 %))
   (s/optional-key "expired-instance-restart-rate") schema/positive-fraction-less-than-or-equal-to-1
   ; per-request related
   (s/optional-key "blacklist-on-503") s/Bool
   (s/optional-key "max-queue-length") schema/positive-int
   s/Str s/Any})

(def ^:const service-required-keys (->> (keys service-description-schema)
                                        (filter #(instance? RequiredKey %1))
                                        (map :k)
                                        (set)))

; keys used in computing the service-id from the service description
(def ^:const service-override-keys
  #{"authentication" "blacklist-on-503" "concurrency-level" "distribution-scheme" "expired-instance-restart-rate"
    "grace-period-secs" "idle-timeout-mins" "instance-expiry-mins" "jitter-threshold" "max-queue-length" "min-instances"
    "max-instances" "restart-backoff-factor" "scale-down-factor" "scale-factor" "scale-up-factor"})

; keys stored in the service description
(def ^:const service-description-keys
  (set/union service-override-keys
             #{"backend-proto" "cmd" "cmd-type" "cpus" "env" "health-check-url" "mem" "metadata" "metric-group" "name"
               "permitted-user" "ports" "run-as-user" "version"}))

; keys allowed in a service description for on-the-fly requests
(def ^:const on-the-fly-service-description-keys (set/union service-description-keys #{"token"}))

; keys allowed in metadata for tokens, these need to be distinct from service description keys
(def ^:const token-metadata-keys #{"deleted" "last-update-time" "owner"})

; keys allowed in a token description
(def ^:const token-description-keys (set/union service-description-keys token-metadata-keys))

(defn map-validation-helper [issue key]
  (when-let [error (get issue key)]
    (cond
      (map? error)
      (let [keys-with-bad-values (filter string? (keys error))
            bad-key-values (->> (select-keys error keys-with-bad-values)
                                (pc/map-vals #(.value %))
                                (map #(str/join ": " %)))
            bad-keys (filter #(instance? ValidationError %) (keys error))]
        (cond-> {}
                (not-empty keys-with-bad-values) (assoc :bad-key-values bad-key-values)
                (not-empty bad-keys) (assoc :bad-keys bad-keys)))
      (instance? ValidationError error)
      (let [provided (.value ^ValidationError error)]
        (cond
          (map? provided) {:bad-size (count provided)}
          :default {:bad-type provided})))))

(defn generate-friendly-environment-variable-error-message
  "If the provided metadata was invalid, attempt to generate a friendly error message. Return nil for unknown error."
  [issue]
  (when-let [env-var-error (map-validation-helper issue "env")]
    (when-not (empty? env-var-error)
      (str
        (let [bad-keys (group-by #(= 'reserved-environment-variable (:pred-name (.schema %))) (get env-var-error :bad-keys))
              reserved-keys (map #(.value %) (get bad-keys true))
              misformatted-keys (map #(.value %) (get bad-keys false))]
          (str
            (when (not-empty misformatted-keys)
              (str "The following environment variable keys are invalid: " (str/join ", " misformatted-keys)
                   ". Keys must be made up of letters, underscores, and numbers and must start with a letter. "
                   "Keys cannot be longer than 512 characters. "))
            (when (not-empty reserved-keys)
              (str "The following environment variable keys are reserved: " (str/join ", " reserved-keys)
                   ". Environment variables cannot start with MESOS_, MARATHON_, or PORT and cannot be "
                   (str/join ", " reserved-environment-vars) ". "))))
        (when-let [bad-key-values (get env-var-error :bad-key-values)]
          (str "The following environment variable keys did not have string values: " (str/join ", " bad-key-values)
               ". Environment variable values must be strings no longer than 512 characters. "))
        (when-let [bad-size (get env-var-error :bad-size)]
          (str "Environment variables can only contain 100 keys. You provided " bad-size ". "))
        (when (contains? env-var-error :bad-type)
          "Environment variables must be a map from string to string. ")
        (utils/message :environment-variable-error-info)))))

(defn generate-friendly-metadata-error-message
  "If the provided environment variables were invalid, attempt to generate a friendly error message. Return nil for unknown error."
  [issue]
  (when-let [metadata-error (map-validation-helper issue "metadata")]
    (when-not (empty? metadata-error)
      (str
        (when-let [bad-keys (seq (map #(.value %) (get metadata-error :bad-keys)))]
          (str "The following metadata keys are invalid: " (str/join ", " bad-keys)
               ". Keys must be made up of letters, numbers, and hyphens and must start with a letter. "))
        (when-let [bad-key-values (seq (get metadata-error :bad-key-values))]
          (str "The following metadata keys did not have string values: " (str/join ", " bad-key-values)
               ". Metadata values must be strings. "))
        (when-let [bad-size (get metadata-error :bad-size)]
          (str "Metadata can only contain 100 keys. You provided " bad-size ". "))
        (when (contains? metadata-error :bad-type)
          "Metadata must be a map from string to string. ")
        (utils/message :metadata-error-info)))))

(defn name->metric-group
  "Given a collection of mappings, where each mapping is a
  [regex, metric-group] pair, and a service name, returns the first
  metric-group whose regex matches against the service name, or nil if
  there is no match"
  [mappings service-name]
  (when service-name
    (some #(when (re-matches (first %) service-name) (second %)) mappings)))

(defn metric-group-filter
  "Filter for descriptors which resolves the metric group"
  [{:strs [name metric-group] :as descriptor} mappings]
  (cond-> descriptor
          (nil? metric-group) (assoc "metric-group" (or (name->metric-group mappings name) "other"))))

(defn merge-defaults
  "Merges the defaults into the existing service description."
  [service-description-without-defaults service-description-defaults metric-group-mappings]
  (->
    service-description-defaults
    (merge service-description-without-defaults)
    (metric-group-filter metric-group-mappings)))

(defn- merge-overrides
  "Merges the overrides into the service description."
  [service-description-without-overrides service-description-overrides]
  (cond-> service-description-without-overrides
          service-description-overrides (merge service-description-overrides)))

(defn sanitize-service-description
  "Sanitizes the service description by removing unsupported keys."
  ([service-description] (sanitize-service-description service-description service-description-keys))
  ([service-description allowed-fields]
   (if (not-every? #(allowed-fields (key %)) service-description)
     (utils/filterm #(allowed-fields (key %)) service-description)
     service-description)))

(let [service-id->key #(str "^OVERRIDE#" %)]
  (defn store-service-description-overrides
    "Stores an entry in the key-value store marking the service has overrides."
    [kv-store service-id username service-description-template]
    (let [service-description-to-store (sanitize-service-description service-description-template service-override-keys)]
      (kv/store kv-store (service-id->key service-id) {:overrides service-description-to-store, :last-updated-by username, :time (t/now)})))

  (defn clear-service-description-overrides
    "Stores a blank entry in the key-value store marking the service has no overrides."
    [kv-store service-id username]
    (kv/store kv-store (service-id->key service-id) {:overrides {}, :last-updated-by username, :time (t/now)}))

  (defn service-id->overrides
    "Retrieves the overridden service description for a service from the key-value store."
    [kv-store service-id & {:keys [refresh] :or {refresh false}}]
    (kv/fetch kv-store (service-id->key service-id) :refresh refresh)))

(defn default-and-override
  "Adds defaults and overrides to the provided service-description"
  [service-description metric-group-mappings kv-store defaults service-id]
  (-> service-description
      (merge-defaults defaults metric-group-mappings)
      (merge-overrides (:overrides (service-id->overrides kv-store service-id)))))

(defn service-description->service-id
  "Create an id for marathon from the name (if available), cmd, universe, and resource requirements.
   Keys defined in `keys-filtered-from-service-id` will be excluded from the id computation logic."
  [service-id-prefix service-description]
  ; sanitize before sending to marathon, limit to lower-case letters and digits
  (let [{:strs [name]} service-description
        prefix (cond-> service-id-prefix
                       name (str (str/replace (str/lower-case name) #"[^a-z0-9]" "") "-"))
        sorted-service-desc (-> (select-keys service-description service-description-keys)
                                sort)
        service-id (loop [[[k v] & kvs] sorted-service-desc
                          acc (transient [])]
                     (if k
                       (recur kvs (-> acc
                                      (conj! k)
                                      (conj! (str v))))
                       (str prefix (digest/digest "MD5" (str/join "" (persistent! acc))))))]
    (log/debug "Got ID for app" service-description sorted-service-desc service-id)
    service-id))

(defn required-keys-present?
  "Returns true if every required parameter is available in the service description.
   Note: It does not perform any validation on the values stored against the parameters."
  [service-description]
  (every? #(contains? service-description %) service-required-keys))

(defn validate-schema
  "Validates the provided service description template.
   When requested to do so, it populates required fields to ensure validation does not fail for missing required fields."
  [service-description-template
   {:keys [allow-missing-required-fields?] :or {allow-missing-required-fields? true} :as args-map}]
  (let [default-valid-service-description (when allow-missing-required-fields?
                                            {"cpus" 1
                                             "mem" 1
                                             "cmd" "default-cmd"
                                             "version" "default-version"
                                             "run-as-user" "default-run-as-user"})
        service-description-to-use (merge default-valid-service-description service-description-template)
        exception-message (utils/message :invalid-service-description)]
    (try
      (s/validate service-description-schema service-description-to-use)
      (catch Exception e
        (let [issue (s/check service-description-schema service-description-to-use)
              friendly-error-message (utils/filterm val
                                                    {:metadata (generate-friendly-metadata-error-message issue)
                                                     :env (generate-friendly-environment-variable-error-message issue)})]
          (sling/throw+ (cond-> {:type :service-description-error
                                 :message exception-message
                                 :service-description service-description-template
                                 :status 400
                                 :issue issue}
                                (not-empty friendly-error-message) (assoc :friendly-error-message friendly-error-message))
                        e))))

    ; Validate max-instances >= min-instances
    (let [{:strs [min-instances max-instances]} service-description-to-use]
      (when (and (integer? min-instances) (integer? max-instances))
        (when (> min-instances max-instances)
          (sling/throw+ {:type :service-description-error
                         :message exception-message
                         :friendly-error-message (str "Minimum instances (" min-instances 
                                                      ") must be <= Maximum instances (" 
                                                      max-instances ")")
                         :status 400}))))

    ; Validate the cmd-type field
    (let [cmd-type (service-description-to-use "cmd-type")]
      (when (and (not (str/blank? cmd-type)) (not ((:valid-cmd-types args-map) cmd-type)))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "Command type " cmd-type 
                                                    " is not supported")
                       :status 400})))))

(defprotocol ServiceDescriptionBuilder
  "A protocol for constructing a service description from the various sources. Implementations
  allow for plugging in different schemes (e.g. manipulating the cmd) based on different needs."

  (build [this core-service-description args-map]
    "Returns a map of {:service-id ..., :service-description ..., :core-service-description...}")

  (validate [this service-description args-map]
    "Throws if the provided service-description is not valid"))

(defn assoc-run-as-requester-fields
  "Attaches the run-as-user and permitted-user fields to the service description.
   We intentionally force permitted-user to be the username for run-as-requester feature."
  [service-description username]
  (assoc service-description "run-as-user" username "permitted-user" username))

(defrecord DefaultServiceDescriptionBuilder [_]
  ServiceDescriptionBuilder

  (build [_ user-service-description {:keys [service-id-prefix metric-group-mappings kv-store defaults assoc-run-as-user-approved? username]}]
    (let [core-service-description (if (get user-service-description "run-as-user")
                                     user-service-description
                                     (let [candidate-service-description (assoc-run-as-requester-fields user-service-description username)
                                           candidate-service-id (service-description->service-id service-id-prefix candidate-service-description)]
                                       (if (assoc-run-as-user-approved? candidate-service-id)
                                         (do
                                           (log/debug "appending run-as-user into pre-approved service" candidate-service-id)
                                           candidate-service-description)
                                         user-service-description)))
          service-id (service-description->service-id service-id-prefix core-service-description)
          service-description (default-and-override core-service-description metric-group-mappings
                                                    kv-store defaults service-id)]
      {:core-service-description core-service-description
       :service-description service-description
       :service-id service-id}))

  (validate [_ service-description args-map]
    (validate-schema service-description (merge-with set/union args-map {:valid-cmd-types #{"shell"}}))))

(defn service-description->health-check-url
  "Returns the configured health check Url or a default value (available in `default-health-check-path`)"
  [service-description]
  (or (get service-description "health-check-url") default-health-check-path))

(defn- token->kv-data
  "Retrieves the data stored against the token in the kv-store."
  [kv-store ^String token error-on-missing include-deleted]
  (let [{:strs [deleted run-as-user] :as data} (when token (kv/fetch kv-store token))
        data (when data ; populate token owner for backwards compatibility
               (update-in data ["owner"] (fn [current-owner] (or current-owner run-as-user))))]
    (when (and error-on-missing (not data))
      (throw (ex-info (str "Token not found: " token) {:status 400})))
    (log/debug "Extracted data for" token "is" data)
    (when (or (not deleted) include-deleted)
      data)))

(defn token->token-description
  "Retrieves the token description for the given token."
  [kv-store ^String token & {:keys [include-deleted] :or {include-deleted false}}]
  (let [config (token->kv-data kv-store token false include-deleted)]
    {:service-description-template (select-keys config service-description-keys)
     :token-metadata (select-keys config token-metadata-keys)}))

(defn token->service-description-template
  "Retrieves the service description template for the given token."
  [kv-store ^String token & {:keys [error-on-missing] :or {error-on-missing true}}]
  (let [config (token->kv-data kv-store token error-on-missing false)]
    (if (not-empty config)
      (select-keys config service-description-keys)
      {})))

(defn retrieve-token-from-service-description-or-hostname
  "Retrieve the token name from the service description map using the x-waiter-token key.
   If such a token is not found, then revert to using the host name (without the port) as the token."
  [waiter-headers request-headers waiter-hostname]
  (let [token-header (headers/get-waiter-header waiter-headers "token")
        host-header (get request-headers "host")
        hostname (first (str/split (str host-header) #":"))]
    (cond
      (not (str/blank? token-header)) {:token token-header, :source :waiter-header}
      (and (not= waiter-hostname hostname) (not (str/blank? hostname))) {:token hostname, :source :host-header}
      :else nil)))

(defn token-preauthorized?
  "Returns true if the token is pre-authorized and will not need authorization before being launched."
  [{:strs [permitted-user run-as-user]}]
  (and (-> permitted-user str/blank? not)
       (not= "*" run-as-user)
       (-> run-as-user str/blank? not)))

(defn token-authentication-disabled?
  "Returns true if the token is authentication-disabled and will not need authorization before being launched."
  [{:strs [authentication permitted-user run-as-user] :as description}]
  (and (= "disabled" authentication)
       (= "*" permitted-user)
       (not= "*" run-as-user)
       (-> run-as-user str/blank? not)
       (required-keys-present? description)))

(defn- prepare-service-description-template-from-tokens
  "Prepares the service description using the token(s)."
  [waiter-headers request-headers kv-store waiter-hostname]
  (let [{:keys [token source]} (retrieve-token-from-service-description-or-hostname waiter-headers request-headers waiter-hostname)]
    (cond
      (= source :host-header)
      (let [service-description-template (sanitize-service-description
                                           (token->service-description-template kv-store token :error-on-missing false))]
        {:service-description-template service-description-template
         :token-authentication-disabled (token-authentication-disabled? service-description-template)
         :token-preauthorized (token-preauthorized? service-description-template)})

      (= source :waiter-header)
      (let [token-names (str/split (str token) #",")]
        (loop [service-description-template {}
               [token-name & remaining-token-names] token-names]
          (let [service-description-template' (merge service-description-template
                                                     (token->service-description-template kv-store token-name))]
            (if remaining-token-names
              (recur service-description-template' remaining-token-names)
              {:service-description-template (sanitize-service-description service-description-template')
               :token-authentication-disabled (and (= 1 (count token-names))
                                                   (token-authentication-disabled? service-description-template'))
               :token-preauthorized (and (= 1 (count token-names))
                                         (token-preauthorized? service-description-template'))}))))

      :else
      {:service-description-template {}
       :token-authentication-disabled false
       :token-preauthorized false})))

(let [service-id->key #(str "^SERVICE-ID#" %)]
  (defn store-core
    "Store the service-id mapping of the service description in the key-value store.
     It also validates the service description before storing it."
    [kv-store ^String service-id {:strs [run-as-user] :as service-description} validate-service-description-fn]
    (log/info "Storing service description for service-id:" service-id "and run-as-user:" run-as-user)
    (validate-service-description-fn service-description)
    (let [filtered-service-desc (sanitize-service-description service-description)]
      (kv/store kv-store (service-id->key service-id) filtered-service-desc)
      (log/info "Stored service description for service-id" service-id)))

  (defn fetch-core
    "Loads the service description for the specified service-id from the key-value store."
    [kv-store ^String service-id & {:keys [refresh nil-on-missing?] :or {refresh false nil-on-missing? true}}]
    (let [service-description (kv/fetch kv-store (service-id->key service-id) :refresh refresh)]
      (if (map? service-description)
        service-description
        (when-not nil-on-missing?
          (throw (ex-info "No description found!" {:service-id service-id})))))))

(let [service-id->key #(str "^STATUS#" %)]
  (defn suspend-service
    "Stores an entry in the key-value store marking the service as suspended."
    [kv-store service-id username]
    (kv/store kv-store (service-id->key service-id) {:suspended true, :last-updated-by username, :time (t/now)}))

  (defn resume-service
    "Stores an entry in the key-value store marking the service as resumed."
    [kv-store service-id username]
    (kv/store kv-store (service-id->key service-id) {:suspended false, :last-updated-by username, :time (t/now)}))

  (defn service-id->suspended-state
    "Retrieves the suspended state from the key-value store."
    [kv-store service-id & {:keys [refresh] :or {refresh false}}]
    (kv/fetch kv-store (service-id->key service-id) :refresh refresh))

  (defn merge-suspended
    "Associates the suspended state into the descriptor."
    [{:keys [service-id] :as descriptor} kv-store]
    (let [suspended-state (service-id->suspended-state kv-store service-id)]
      (cond-> descriptor
              suspended-state (assoc :suspended-state suspended-state)))))

(defn- parse-metadata-headers [service-description]
  (let [metadata-keys (filter (fn [key] (str/starts-with? key "metadata-")) (keys service-description))
        metadata-map (select-keys service-description metadata-keys)]
    (if (empty? metadata-map)
      service-description
      (let [renamed-metadata-map (pc/map-keys #(str/replace % #"^metadata-" "") metadata-map)
            sanitized-service-description (apply dissoc service-description metadata-keys)]
        (assoc sanitized-service-description "metadata" renamed-metadata-map)))))

(defn prepare-service-description-sources
  [{:keys [waiter-headers passthrough-headers]} kv-store waiter-hostname service-description-defaults]
  "Prepare the service description sources from the current request.
   Populates the service description for on-the-fly waiter-specific headers.
   Also populates for the service description for a token (first looked in headers and then using the host name).
   Finally, it also includes the service configuration defaults."
  (let [service-description-template-from-headers
        (sanitize-service-description (-> waiter-headers
                                          headers/drop-waiter-header-prefix
                                          parse-metadata-headers))
        {:keys [service-description-template token-authentication-disabled token-preauthorized]}
        (prepare-service-description-template-from-tokens waiter-headers passthrough-headers kv-store waiter-hostname)]
    {:defaults service-description-defaults
     :headers service-description-template-from-headers
     :token-authentication-disabled token-authentication-disabled
     :token-preauthorized token-preauthorized
     :tokens service-description-template}))

(defn- merge-service-description-sources
  [descriptor kv-store waiter-hostname service-description-defaults]
  "Merges the sources for a service-description into the descriptor."
  (let [sources (prepare-service-description-sources descriptor kv-store waiter-hostname service-description-defaults)]
    (assoc descriptor :sources sources)))

(defn- sanitize-metadata [{:strs [metadata] :as service-description}]
  (if metadata
    (let [sanitized-metadata (pc/map-keys #(str/lower-case %) metadata)]
      (assoc service-description "metadata" sanitized-metadata))
    service-description))

(let [error-message-map-fn (fn [passthrough-headers waiter-headers]
                             {:status 400
                              :non-waiter-headers (dissoc passthrough-headers "authorization")
                              :x-waiter-headers waiter-headers})]
  (defn compute-service-description
    "Computes the service description applying any processing rules,
     It also validates the services description.
     It creates the service-description using the following preferential order:
      - on-the-fly headers
      - token description
      - configured defaults.
     If after the merge a permitted-user is not available, then `username` becomes the permitted-user.
     If after the merge a run-as-user is not available, then `username` becomes the run-as-user."
    [{:keys [headers token-authentication-disabled token-preauthorized tokens] :as sources}
     waiter-headers passthrough-headers kv-store service-id-prefix username metric-group-mappings
     service-description-builder assoc-run-as-user-approved?]
    (let [service-description-based-on-headers (cond->> headers
                                                        ; any change with the on-the-fly must change the run-as-user if it doesn't already exist
                                                        (seq headers) (merge {"run-as-user" username}))
          service-description-from-headers-and-token-sources (merge tokens service-description-based-on-headers)
          sanitized-service-description-from-sources (cond-> service-description-from-headers-and-token-sources
                                                             ;; * run-as-user is the same as a missing run-as-user
                                                             (= "*" (get service-description-from-headers-and-token-sources "run-as-user"))
                                                             (dissoc service-description-from-headers-and-token-sources "run-as-user"))
          sanitized-metadata-description (sanitize-metadata sanitized-service-description-from-sources)
          ; run-as-user will not be set if description-from-headers or the token description contains it.
          ; else rely on presence of x-waiter headers to set the run-as-user
          contains-waiter-header? (headers/contains-waiter-header waiter-headers on-the-fly-service-description-keys)
          contains-service-parameter-header? (headers/contains-waiter-header waiter-headers service-description-keys)
          user-service-description (cond-> sanitized-metadata-description
                                           (and (not (contains? sanitized-metadata-description "run-as-user")) contains-waiter-header?)
                                           ; can only set the run-as-user if some on-the-fly-service-description-keys waiter header was provided
                                           (assoc-run-as-requester-fields username)
                                           contains-service-parameter-header?
                                           ; can only set the permitted-user if some service-description-keys waiter header was provided
                                           (assoc "permitted-user" (or (get headers "permitted-user") username)))
          defaults (:defaults sources)]
      (when (empty? user-service-description)
        (throw (ex-info (utils/message :cannot-identify-service)
                        (error-message-map-fn passthrough-headers waiter-headers))))
      (sling/try+
        (let [{:keys [service-id service-description core-service-description]}
              (build service-description-builder user-service-description {:service-id-prefix service-id-prefix
                                                                           :metric-group-mappings metric-group-mappings
                                                                           :kv-store kv-store
                                                                           :defaults defaults
                                                                           :assoc-run-as-user-approved? assoc-run-as-user-approved?
                                                                           :username username})
              service-preauthorized (and token-preauthorized (empty? service-description-based-on-headers))
              service-authentication-disabled (and token-authentication-disabled (empty? service-description-based-on-headers))
              stored-service-description? (fetch-core kv-store service-id)]
          ; Validating is expensive, so avoid validating if we've validated before, relying on the fact
          ; that we'll only store validated service descriptions
          (when-not stored-service-description?
            (validate service-description-builder core-service-description {:allow-missing-required-fields? false})
            (validate service-description-builder service-description {:allow-missing-required-fields? false}))
          {:core-service-description core-service-description
           :service-authentication-disabled service-authentication-disabled
           :service-description service-description
           :service-id service-id
           :service-preauthorized service-preauthorized})
        (catch [:type :service-description-error] ex-data
          (throw (ex-info (:message ex-data)
                          (merge (error-message-map-fn passthrough-headers waiter-headers) (dissoc ex-data :message))
                          (:throwable &throw-context))))))))

(defn merge-service-description-and-id
  "Populates the descriptor with the service-description and service-id."
  [{:keys [passthrough-headers sources waiter-headers] :as descriptor} kv-store service-id-prefix username
   metric-group-mappings service-description-builder assoc-run-as-user-approved?]
  (merge descriptor (compute-service-description sources waiter-headers passthrough-headers kv-store service-id-prefix
                                                 username metric-group-mappings service-description-builder assoc-run-as-user-approved?)))

(defn request->descriptor
  "Creates the service descriptor from the request.
   The result map contains the following elements:
   {:keys [waiter-headers passthrough-headers sources service-id service-description core-service-description suspended-state]}"
  [service-description-defaults service-id-prefix kv-store waiter-hostname request metric-group-mappings
   service-description-builder assoc-run-as-user-approved?]
  (let [current-request-user (get request :authorization/user)]
    (-> (headers/split-headers (:headers request))
        (merge-service-description-sources kv-store waiter-hostname service-description-defaults)
        (merge-service-description-and-id kv-store service-id-prefix current-request-user metric-group-mappings
                                          service-description-builder assoc-run-as-user-approved?)
        (merge-suspended kv-store))))

(defn service-id->service-description
  "Loads the service description for the specified service-id including any overrides."
  [kv-store service-id service-description-defaults metric-group-mappings &
   {:keys [refresh nil-on-missing? effective?] :or {refresh false nil-on-missing? true effective? true}}]
  (let [core-service-description (fetch-core
                                   kv-store
                                   service-id
                                   :refresh refresh
                                   :nil-on-missing? nil-on-missing?)]
    (cond-> core-service-description
            effective? (default-and-override metric-group-mappings kv-store service-description-defaults service-id))))

(defn can-manage-service?
  "Returns whether the `username` is allowed to modify the specified service description."
  [kv-store entitlement-manager service-id username]
  ; the stored service description should already have a run-as-user
  (let [service-description (service-id->service-description kv-store service-id {} [])]
    (authz/manage-service? entitlement-manager username service-id service-description)))

(defn consent-cookie-value
  "Creates the consent cookie value vector based on the mode.
   The returned vector is in the format: mode timestamp service-id|token [token-owner]"
  [clock mode service-id token {:strs [owner]}]
  (when mode
    (-> [mode (.getMillis ^DateTime (clock))]
        (concat (case mode
                  "service" (when service-id [service-id])
                  "token" (when (and owner token) [token owner])
                  nil))
        (vec))))

(defn assoc-run-as-user-approved?
  "Deconstructs the decoded cookie and validates whether the service has been pre-approved.
   The validation step includes:
   a. Ensuring service-id or token/owner pair are valid based on the mode, and
   b. the consent has not expired based on the timestamp in the cookie."
  [clock consent-expiry-days service-id token {:strs [owner]} decoded-consent-cookie]
  (let [[consent-mode auth-timestamp consent-id consent-owner] (vec decoded-consent-cookie)]
    (and
      consent-id
      auth-timestamp
      (or (and (= "service" consent-mode) (= consent-id service-id))
          (and (= "token" consent-mode) (= consent-id token) (= consent-owner owner)))
      (> (+ auth-timestamp (-> consent-expiry-days t/days t/in-millis))
         (.getMillis ^DateTime (clock))))))
