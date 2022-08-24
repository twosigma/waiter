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
(ns waiter.service-description
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [digest]
            [metrics.meters :as meters]
            [plumbing.core :as pc]
            [schema.core :as s]
            [slingshot.slingshot :as sling]
            [waiter.authorization :as authz]
            [waiter.config :as config]
            [waiter.headers :as headers]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.schema :as schema]
            [waiter.status-codes :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.descriptor-utils :as descriptor-utils]
            [waiter.util.utils :as utils])
  (:import (java.util.regex Pattern)
           (org.joda.time DateTime)
           (schema.core Constrained Predicate RequiredKey)
           (schema.utils ValidationError)))

(def ^:const default-health-check-path "/status")

(def ^:const minimum-min-instances 1)

(def ^:const maximum-default-namespace-min-instances 4)

(def reserved-environment-vars #{"HOME" "LOGNAME" "USER"})

(defn reserved-environment-variable? [name]
  (or (contains? reserved-environment-vars name)
      (str/starts-with? name "MARATHON_")
      (str/starts-with? name "MESOS_")
      (re-matches #"^PORT\d*$" name)
      (and (str/starts-with? name "WAITER_") (not (str/starts-with? name "WAITER_CONFIG_")))))

(def environment-variable-schema
  (s/both (s/constrained s/Str #(<= 1 (count %) 512))
          #"^[A-Za-z][A-Za-z0-9_]*$"
          (s/pred #(not (reserved-environment-variable? %)) 'reserved-environment-variable)))

(def service-description-schema
  {;; Required
   (s/required-key "cmd") schema/non-empty-string
   (s/required-key "cpus") schema/positive-num
   (s/required-key "mem") schema/positive-num
   (s/required-key "run-as-user") schema/non-empty-string
   (s/required-key "version") schema/non-empty-string
   ;; Optional
   (s/optional-key "allowed-params") #{environment-variable-schema}
   (s/optional-key "authentication") schema/non-empty-string
   (s/optional-key "backend-proto") schema/valid-backend-proto
   (s/optional-key "cmd-type") schema/non-empty-string
   (s/optional-key "distribution-scheme") (s/enum "balanced" "simple")
   ; Marathon imposes a 512 character limit on environment variable keys and values
   (s/optional-key "env") (s/constrained {environment-variable-schema (s/constrained s/Str #(<= 1 (count %) 512))}
                                         #(< (count %) 100))
   (s/optional-key "image") schema/non-empty-string
   (s/optional-key "metadata") (s/constrained {(s/both schema/valid-string-length #"^[a-z][a-z0-9\\-]*$")
                                               schema/valid-string-length}
                                              #(< (count %) 100))
   (s/optional-key "metric-group") schema/valid-metric-group
   (s/optional-key "name") schema/non-empty-string
   (s/optional-key "namespace") schema/non-empty-string
   (s/optional-key "permitted-user") schema/non-empty-string
   (s/optional-key "ports") schema/valid-number-of-ports
   (s/optional-key "profile") schema/non-empty-string
   ; start-up related
   (s/optional-key "grace-period-secs") (s/both s/Int (s/pred #(<= 0 % (t/in-seconds (t/minutes 60))) 'at-most-60-minutes))
   (s/optional-key "health-check-authentication") schema/valid-health-check-authentication
   (s/optional-key "health-check-interval-secs") (s/both s/Int (s/pred #(<= 5 % 600) 'between-5-seconds-and-10-minutes))
   (s/optional-key "health-check-max-consecutive-failures") (s/both s/Int (s/pred #(<= 1 % 36) 'at-most-36))
   (s/optional-key "health-check-port-index") schema/valid-health-check-port-index
   (s/optional-key "health-check-proto") schema/valid-health-check-proto
   (s/optional-key "health-check-url") schema/non-empty-string
   (s/optional-key "idle-timeout-mins") (s/both s/Int (s/pred #(<= 0 % (t/in-minutes (t/days 30))) 'between-0-minute-and-30-days))
   (s/optional-key "interstitial-secs") (s/both s/Int (s/pred #(<= 0 % (t/in-seconds (t/minutes 60))) 'at-most-60-minutes))
   (s/optional-key "liveness-check-authentication") schema/valid-health-check-authentication
   (s/optional-key "liveness-check-interval-secs") (s/both s/Int (s/pred #(<= 5 % 600) 'between-5-seconds-and-10-minutes))
   (s/optional-key "liveness-check-max-consecutive-failures") (s/both s/Int (s/pred #(<= 1 % 36) 'at-most-36))
   (s/optional-key "liveness-check-port-index") schema/valid-health-check-port-index
   (s/optional-key "liveness-check-proto") schema/valid-health-check-proto
   (s/optional-key "liveness-check-url") schema/non-empty-string
   (s/optional-key "restart-backoff-factor") schema/positive-number-greater-than-or-equal-to-1
   (s/optional-key "scheduler") schema/non-empty-string
   (s/optional-key "termination-grace-period-secs") (s/both s/Int (s/pred #(<= 0 % (t/in-seconds (t/minutes 5))) 'at-most-5-minutes))
   ; auto-scaling related
   (s/optional-key "concurrency-level") (s/both s/Int (s/pred #(<= 1 % 10000) 'between-one-and-10000))
   (s/optional-key "expired-instance-restart-rate") schema/positive-fraction-less-than-or-equal-to-1
   (s/optional-key "instance-expiry-mins") (s/constrained s/Int #(<= 0 %))
   (s/optional-key "jitter-threshold") schema/greater-than-or-equal-to-0-less-than-1
   (s/optional-key "load-balancing") schema/valid-load-balancing
   (s/optional-key "max-instances") (s/both s/Int (s/pred #(<= minimum-min-instances % 1000) 'between-one-and-1000))
   (s/optional-key "min-instances") (s/both s/Int (s/pred #(<= minimum-min-instances % 1000) 'between-one-and-1000))
   (s/optional-key "scale-down-factor") schema/positive-fraction-less-than-1
   (s/optional-key "scale-factor") schema/positive-fraction-less-than-or-equal-to-2
   (s/optional-key "scale-up-factor") schema/positive-fraction-less-than-1
   ; per-request related
   (s/optional-key "max-queue-length") schema/positive-int
   s/Str s/Any})

(def user-metadata-schema
  {(s/optional-key "cors-rules") [{(s/required-key "origin-regex") schema/regex-pattern
                                   (s/optional-key "target-path-regex") schema/regex-pattern
                                   (s/optional-key "methods") (s/both (s/pred not-empty) [schema/http-method])}]
   (s/optional-key "editor") schema/non-empty-string
   (s/optional-key "fallback-period-secs") (s/both s/Int (s/pred #(<= 0 % (t/in-seconds (t/days 1))) 'at-most-1-day))
   (s/optional-key "https-redirect") s/Bool
   (s/optional-key "maintenance") {(s/required-key "message") (s/constrained s/Str #(<= 1 (count %) 512))}
   (s/optional-key "owner") schema/non-empty-string
   (s/optional-key "service-mapping") (s/pred #(contains? #{"default" "exclusive" "legacy"} %) 'valid-service-mapping?)
   (s/optional-key "stale-timeout-mins") (s/both s/Int (s/pred #(<= 0 % (t/in-minutes (t/hours 4))) 'at-most-4-hours))
   s/Str s/Any})

(def ^:const service-required-keys (->> (keys service-description-schema)
                                        (filter #(instance? RequiredKey %1))
                                        (map :k)
                                        (set)))

(def ^:const service-override-keys
  #{"authentication" "concurrency-level" "distribution-scheme" "expired-instance-restart-rate"
    "grace-period-secs" "health-check-interval-secs" "health-check-max-consecutive-failures"
    "idle-timeout-mins" "instance-expiry-mins" "interstitial-secs" "jitter-threshold"
    "liveness-check-interval-secs" "liveness-check-max-consecutive-failures" "load-balancing"
    "max-queue-length" "min-instances" "max-instances" "restart-backoff-factor"
    "scale-down-factor" "scale-factor" "scale-up-factor" "termination-grace-period-secs"})

(def ^:const service-non-override-keys
  #{"allowed-params" "backend-proto" "cmd" "cmd-type" "cpus" "env"
    "health-check-authentication" "health-check-port-index" "health-check-proto" "health-check-url"
    "image" "liveness-check-authentication" "liveness-check-port-index" "liveness-check-proto" "liveness-check-url"
    "mem" "metadata" "metric-group" "name" "namespace" "permitted-user" "ports" "profile"
    "run-as-user" "scheduler" "version"})

; keys used as parameters in the service description
(def ^:const service-parameter-keys
  (set/union service-override-keys service-non-override-keys))

; keys in service description metadata that need a deep merge
(def ^:const service-deep-merge-keys #{"env" "metadata"})

; keys allowed in service description metadata, these need to be distinct from service parameter keys
(def ^:const service-metadata-keys #{})

; keys used in computing the service-id from the service description
(def ^:const service-description-keys (set/union service-parameter-keys service-metadata-keys))

(def ^:const service-description-from-header-keys (set/union service-parameter-keys #{"param"}))

; keys allowed in a service description for on-the-fly requests
(def ^:const on-the-fly-service-description-keys (set/union service-parameter-keys #{"token"}))

; keys allowed in system metadata for tokens, these need to be distinct from service description keys
(def ^:const system-metadata-keys #{"cluster" "deleted" "last-update-time" "last-update-user" "previous" "root"})

; keys allowed in user metadata for tokens, these need to be distinct from service description keys
(def ^:const user-metadata-keys #{"cors-rules" "editor" "fallback-period-secs" "https-redirect" "maintenance" "owner" "service-mapping" "stale-timeout-mins"})

; keys allowed in metadata for tokens, these need to be distinct from service description keys
(def ^:const token-metadata-keys (set/union system-metadata-keys user-metadata-keys))

; keys editable by users in the token data
(def ^:const token-user-editable-keys (set/union service-parameter-keys user-metadata-keys))

; keys allowed in the token data
(def ^:const token-data-keys (set/union service-parameter-keys token-metadata-keys))

; parameter names that should get default values from the parameters they map to
(def ^:const param-to-param-default-mapping
  {"liveness-check-authentication" "health-check-authentication"
   "liveness-check-interval-secs" "health-check-interval-secs"
   "liveness-check-max-consecutive-failures" "health-check-max-consecutive-failures"
   "liveness-check-port-index" "health-check-port-index"
   "liveness-check-proto" "health-check-proto"
   "liveness-check-url" "health-check-url"})

(defn transform-allowed-params-header
  "Converts allowed-params comma-separated string in the service-description to a set."
  [service-description]
  (cond-> service-description
    (contains? service-description "allowed-params")
    (update "allowed-params"
            (fn [allowed-params]
              (when-not (string? allowed-params)
                (throw (ex-info "Provided allowed-params is not a string"
                                {:allowed-params allowed-params :status http-400-bad-request})))
              (if-not (str/blank? allowed-params)
                (set (str/split allowed-params #","))
                #{})))))

(defn transform-allowed-params-token-entry
  "Converts allowed-params vector in the service-description to a set."
  [service-description]
  (cond-> service-description
    (contains? service-description "allowed-params")
    (update "allowed-params"
            (fn [allowed-params]
              (when-not (coll? allowed-params)
                (throw (ex-info "Provided allowed-params is not a vector"
                                {:allowed-params allowed-params :status http-400-bad-request})))
              (set allowed-params)))))

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

(defn generate-friendly-cors-rules-error-message
  "If the provided cors-rules was invalid, attempt to generate a friendly error message."
  [issue]
  (when-let [cors-rules (get issue "cors-rules")]
    (if (coll? cors-rules)
      (->> cors-rules
           (map
             (fn [rule]
               (reduce
                 (fn [error-messages [key _]]
                   (conj error-messages
                         (str key " "
                              (case key
                                ("origin-regex" "target-path-regex") "must be a valid regular expression"
                                "methods" "must be a non-empty list of http methods (in upper-case)"
                                "is not a valid key"))))
                 []
                 rule)))
           (str/join ", ")
           (str "cors-rules list has invalid items: "))
      "cors-rules must be a list")))

(defn generate-friendly-maintenance-error-message
  "If the provided maintenance field was invalid, attempt to generate a friendly error message."
  [issue]
  (when-let [maintenance (get issue "maintenance")]
    (cond
      (get maintenance "message")
      "maintenance message must be a non-empty string with length at most 512"
      :else "maintenance must be a map")))

(defn generate-friendly-environment-variable-error-message
  "If the provided metadata was invalid, attempt to generate a friendly error message. Return nil for unknown error."
  [issue]
  (when-let [var-error (map-validation-helper issue "env")]
    (when-not (empty? var-error)
      (str
        (let [bad-keys (group-by #(= 'reserved-environment-variable (:pred-name (.schema %))) (get var-error :bad-keys))
              reserved-keys (map #(.value %) (get bad-keys true))
              misformatted-keys (map #(.value %) (get bad-keys false))]
          (str
            (when (not-empty misformatted-keys)
              (str "The following environment variable keys are invalid: " (str/join ", " misformatted-keys)
                   ". Keys must be made up of letters, underscores, and numbers and must start with a letter. "
                   "Keys cannot be longer than 512 characters. "))
            (when (not-empty reserved-keys)
              (str "The following environment variable keys are reserved: " (str/join ", " reserved-keys)
                   ". Environment variables cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be "
                   (str/join ", " reserved-environment-vars) ". "))))
        (when-let [bad-key-values (get var-error :bad-key-values)]
          (str "The following environment variable keys did not have string values: " (str/join ", " bad-key-values)
               ". Environment variable values must be strings no longer than 512 characters. "))
        (when-let [bad-size (get var-error :bad-size)]
          (str "Environment variables can only contain 100 keys. You provided " bad-size ". "))
        (when (contains? var-error :bad-type)
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
               ". Keys must be made up of lower-case letters, numbers, and hyphens and must start with a letter. "))
        (when-let [bad-key-values (seq (get metadata-error :bad-key-values))]
          (str "The following metadata keys did not have string values: " (str/join ", " bad-key-values)
               ". Metadata values must be strings. "))
        (when-let [bad-size (get metadata-error :bad-size)]
          (str "Metadata can only contain 100 keys. You provided " bad-size ". "))
        (when (contains? metadata-error :bad-type)
          "Metadata must be a map from string to string. ")
        (utils/message :metadata-error-info)))))

(defn generate-friendly-allowed-params-error-message
  "If the provided allowed-params was invalid, attempt to generate a friendly error message.
   Return nil for unknown error."
  [issue]
  (when-let [allowed-params-issue (get issue "allowed-params")]
    (let [length-violation (some #(instance? Constrained (.schema %))
                                 allowed-params-issue)
          regex-violation (some #(instance? Pattern (.schema %))
                                allowed-params-issue)
          reserved-violation (some #(instance? Predicate (.schema %))
                                   allowed-params-issue)]
      (str "allowed-params is invalid. "
           (when length-violation
             "Individual params may not be empty. ")
           (when regex-violation
             "Individual params must be made up of letters, numbers, and underscores and must start with a letter. ")
           (when reserved-violation
             (str "Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be "
                  (str/join ", " reserved-environment-vars) ". "))))))

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
  [{:strs [name metric-group] :as service-description} mappings]
  (cond-> service-description
    (nil? metric-group)
    (assoc "metric-group" (or (name->metric-group mappings name)
                              "other"))))

(defn- merge-overrides
  "Merges the overrides into the service description."
  [service-description-without-overrides service-description-overrides]
  (cond-> service-description-without-overrides
    service-description-overrides (merge service-description-overrides)))

(defn sanitize-service-description
  "Sanitizes the service description by removing unsupported keys."
  ([service-description] (sanitize-service-description service-description service-description-keys))
  ([service-description allowed-fields] (select-keys service-description allowed-fields)))

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

(defn- compute-valid-profiles-str
  "Computes the string representation of supported profiles"
  [profile->defaults]
  (let [supported-profiles-str (->> profile->defaults keys sort (str/join ", "))]
    (if (str/blank? supported-profiles-str)
      ", there are no supported profiles"
      (str ", supported profile(s) are " supported-profiles-str))))

(defn validate-profile-parameter
  "Throws an exception when the profile parameter is provided but does not map to a supported profile."
  [profile->defaults profile]
  (when-not (contains? profile->defaults profile)
    (let [supported-profiles-str (compute-valid-profiles-str profile->defaults)]
      (sling/throw+ {:type :service-description-error
                     :friendly-error-message (str "Unsupported profile: " profile supported-profiles-str)
                     :status http-400-bad-request
                     :log-level :warn}))))

(defn- adjust-default-min-instances
  "Adjusts the min-instances if the max-instances is provided without min-instances and
   the max-instances is smaller than the default min-instances."
  [default-parameters {:strs [max-instances min-instances]}]
  (let [default-min-instances (get default-parameters "min-instances")]
    (cond-> default-parameters
      (and (integer? max-instances)
           (nil? min-instances)
           (integer? default-min-instances)
           (> default-min-instances max-instances))
      (assoc "min-instances" (max max-instances minimum-min-instances)))))

(defn- merge-parameters
  "Merges the default and provided parameters.
   Parameters in the `service-deep-merge-keys` sequence are deep merged, other parameters are shallow merged."
  [default-params provided-params]
  (loop [[parameter-key & remaining-parameter-keys] service-deep-merge-keys
         loop-params (merge default-params provided-params)]
    (if (some? parameter-key)
      (let [default-param-value (get default-params parameter-key)
            provided-param-value (get provided-params parameter-key)]
        (recur remaining-parameter-keys
               (cond-> loop-params
                 (and (map? default-param-value) (seq default-param-value) (map? provided-param-value))
                 (assoc parameter-key (merge default-param-value provided-param-value)))))
      loop-params)))

(defn- compute-profile-defaults
  "Returns the default parameters for the specified allowed-keys in the profile.
   Throws an error if the profile is not supported.
   The config-defaults are overridden with overrides from the specified profile."
  [allowed-keys config-defaults profile->defaults profile]
  (if (nil? profile)
    config-defaults
    (do
      (validate-profile-parameter profile->defaults profile)
      (let [profile-defaults (get profile->defaults profile)
            profile-parameters (select-keys profile-defaults allowed-keys)]
        (-> config-defaults
          (adjust-default-min-instances profile-parameters)
          (merge-parameters profile-parameters))))))

(defn compute-service-defaults
  "Returns the default service parameters for the specified profile.
   Throws an error if the profile is not supported.
   The service-description-defaults are overridden with overrides from a specified profile."
  [service-description-defaults profile->defaults profile]
  (compute-profile-defaults
    service-parameter-keys service-description-defaults profile->defaults profile))

(defn compute-token-defaults
  "Returns the default token parameters for the specified profile.
   Throws an error if the profile is not supported.
   The token-defaults are overridden with overrides from a specified profile."
  [token-defaults profile->defaults profile]
  (compute-profile-defaults
    user-metadata-keys token-defaults profile->defaults profile))

(defn apply-param-to-param-defaults
  "Returns a service description where any parameter that is a key in the given parameter
   default mapping and has no value in the service description will be given a value from
   another parameter in the service description (the parameter name that the original
   parameter name maps to in the parameter default mapping)."
  [service-description-defaults param-default-mapping]
  (let [valid-mapping-parms (map key (filter (fn [[_ mapped-param]] (contains? service-description-defaults mapped-param)) param-default-mapping))
        new-params (into (sorted-set) (concat (keys service-description-defaults) valid-mapping-parms))]
    (pc/map-from-keys (fn [param-name] (or (get service-description-defaults param-name)
                                           (get service-description-defaults (get param-default-mapping param-name))))
                      new-params)))

(defn merge-defaults
  "Merges the defaults into the existing service description."
  ([service-description-without-defaults service-description-defaults profile->defaults metric-group-mappings]
   (merge-defaults service-description-without-defaults service-description-defaults profile->defaults metric-group-mappings {}))
  ([{:strs [profile] :as service-description-without-defaults} service-description-defaults profile->defaults metric-group-mappings param-default-mapping]
   (-> service-description-defaults
       (compute-service-defaults profile->defaults profile)
       (adjust-default-min-instances service-description-without-defaults)
       (merge-parameters service-description-without-defaults)
       (metric-group-filter metric-group-mappings)
       (apply-param-to-param-defaults param-default-mapping))))

(defn default-and-override
  "Adds defaults and overrides to the provided service-description"
  ([service-description service-description-defaults profile->defaults metric-group-mappings kv-store service-id]
   (default-and-override service-description service-description-defaults profile->defaults metric-group-mappings kv-store service-id {}))
  ([service-description service-description-defaults profile->defaults metric-group-mappings kv-store service-id param-default-mapping]
   (-> service-description
       (merge-defaults service-description-defaults profile->defaults metric-group-mappings param-default-mapping)
       (merge-overrides (:overrides (service-id->overrides kv-store service-id))))))

(defn parameters->id
  "Generates a deterministic ID from the input parameter map."
  [parameters]
  (let [sorted-parameters (sort parameters)
        id (loop [[[k v] & kvs] sorted-parameters
                  acc (transient [])]
             (if k
               (recur kvs (-> acc
                              (conj! k)
                              (conj! (str v))))
               (str (digest/digest "MD5" (str/join "" (persistent! acc))))))]
    (log/debug "got ID" id "for" sorted-parameters)
    id))

(defn service-description->service-id
  "Create an id for marathon from the name (if available), cmd, universe, and resource requirements.
   Keys defined in `keys-filtered-from-service-id` will be excluded from the id computation logic."
  [service-id-prefix service-description]
  ; sanitize before sending to marathon, limit to lower-case letters and digits
  (let [{:strs [name]} service-description
        prefix (cond-> service-id-prefix
                 name (str (str/replace (str/lower-case name) #"[^a-z0-9]" "") "-"))
        service-hash (parameters->id (select-keys service-description service-description-keys))]
    (str prefix service-hash)))

(defn required-keys-present?
  "Returns true if every required parameter is available in the service description.
   Note: It does not perform any validation on the values stored against the parameters."
  [service-description]
  (every? #(contains? service-description %) service-required-keys))

(defmacro attach-error-message-for-parameter
  "Helper function that attaches a parameter error message to the parameter->error-message map
   if an issue exists for that parameter."
  [parameter->error-message parameter->issues parameter error-message-body]
  `(let [parameter->issues# ~parameter->issues
         parameter-key# ~parameter
         parameter-name# (name parameter-key#)]
     (cond-> ~parameter->error-message
       (contains? parameter->issues# parameter-name#)
       (assoc parameter-key# (do ~error-message-body)))))

(defn validate-user-metadata-schema
  "Validates provided user-metadata and service-parameter-template of a token and throws an error with user readable validation issues."
  [user-metadata service-parameter-template]
  (try
    (s/validate user-metadata-schema user-metadata)
    (catch Exception _
      (let [parameter->issues (s/check user-metadata-schema user-metadata)
            parameter->error-message (-> {}
                                         (attach-error-message-for-parameter
                                           parameter->issues
                                           :cors-rules
                                           (generate-friendly-cors-rules-error-message parameter->issues))
                                         (attach-error-message-for-parameter
                                           parameter->issues
                                           :fallback-period-secs
                                           "fallback-period-secs must be an integer between 0 and 86400 (inclusive)")
                                         (attach-error-message-for-parameter
                                           parameter->issues
                                           :https-redirect
                                           "https-redirect must be a boolean value")
                                         (attach-error-message-for-parameter
                                           parameter->issues
                                           :maintenance
                                           (generate-friendly-maintenance-error-message parameter->issues))
                                         (attach-error-message-for-parameter
                                           parameter->issues
                                           :owner
                                           "owner must be a non-empty string")
                                         (attach-error-message-for-parameter
                                           parameter->issues
                                           :service-mapping
                                           "service-mapping must be one of default, exclusive or legacy")
                                         (attach-error-message-for-parameter
                                           parameter->issues
                                           :stale-timeout-mins
                                           "stale-timeout-mins must be an integer between 0 and 240 (inclusive)"))]
        (throw (ex-info (str "Validation failed for token:\n" (str/join "\n" (vals parameter->error-message)))
                        {:failed-check (str parameter->issues) :status http-400-bad-request :log-level :warn})))))

  ;; validate environment in sharing mode
  (let [{:strs [env]} service-parameter-template
        {:strs [service-mapping]} user-metadata]
    (when (and (= "exclusive" service-mapping) (contains? env "WAITER_CONFIG_TOKEN"))
      (throw (ex-info "Service environment cannot contain WAITER_CONFIG_TOKEN when service-mapping is exclusive."
                      {:status http-400-bad-request :log-level :warn})))))

(defn validate-schema
  "Validates the provided service description template.
   When requested to do so, it populates required fields to ensure validation does not fail for missing required fields."
  [{:strs [profile] :as service-description-template} max-constraints-schema profile->defaults
   {:keys [allow-missing-required-fields?]
    :or {allow-missing-required-fields? true}
    :as args-map}]
  (let [default-valid-service-description (cond-> nil
                                            allow-missing-required-fields?
                                            (merge {"cpus" 1
                                                    "mem" 1
                                                    "cmd" "default-cmd"
                                                    "version" "default-version"
                                                    "run-as-user" "default-run-as-user"})
                                            ;; handle scenario where profile provides defaults for required keys
                                            ;; Since the results of validation are memoized, this implies that once
                                            ;; a profile provides defaults for required keys, future modifications
                                            ;; of the profile may not remove values for those keys.
                                            profile
                                            (merge (select-keys (get profile->defaults profile) service-required-keys)))
        service-description-to-use (merge default-valid-service-description service-description-template)
        exception-message (utils/message :invalid-service-description)
        throw-error (fn throw-error [e issue friendly-error-message]
                      (sling/throw+ (cond-> {:type :service-description-error
                                             :message exception-message
                                             :service-description service-description-template
                                             :status http-400-bad-request
                                             :issue issue
                                             :log-level :warn}
                                      (not (str/blank? friendly-error-message))
                                      (assoc :friendly-error-message friendly-error-message))
                                    e))]
    (try
      (s/validate service-description-schema service-description-to-use)
      (catch Exception e
        (let [parameter->issues (s/check service-description-schema service-description-to-use)
              parameter->error-message (-> {}
                                           (attach-error-message-for-parameter
                                             parameter->issues
                                             :allowed-params
                                             (generate-friendly-allowed-params-error-message parameter->issues))
                                           (attach-error-message-for-parameter
                                             parameter->issues :cmd "cmd must be a non-empty string.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :authentication
                                             (str "authentication must be 'disabled', 'standard', "
                                                  "or the specific authentication scheme if supported by the configured authenticator."))
                                           (attach-error-message-for-parameter
                                             parameter->issues :backend-proto "backend-proto must be one of h2, h2c, http, or https.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :concurrency-level
                                             "concurrency-level must be an integer in the range [1, 10000].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :cpus "cpus must be a positive number.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :distribution-scheme
                                             "distribution-scheme must be one of balanced or simple.")
                                           (attach-error-message-for-parameter
                                             parameter->issues
                                             :env
                                             (generate-friendly-environment-variable-error-message parameter->issues))
                                           (attach-error-message-for-parameter
                                             parameter->issues :grace-period-secs
                                             "grace-period-secs must be an integer in the range [0, 3600].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :health-check-authentication
                                             "health-check-authentication must be one of standard or disabled.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :health-check-port-index
                                             "health-check-port-index must be an integer in the range [0, 9].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :health-check-proto
                                             "health-check-proto, when provided, must be one of h2, h2c, http, or https.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :health-check-url "health-check-url must be a non-empty string.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :idle-timeout-mins
                                             "idle-timeout-mins must be an integer in the range [0, 43200].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :liveness-check-authentication
                                             "liveness-check-authentication must be one of standard or disabled.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :liveness-check-port-index
                                             "liveness-check-port-index must be an integer in the range [0, 9].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :liveness-check-proto
                                             "liveness-check-proto, when provided, must be one of h2, h2c, http, or https.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :liveness-check-url "liveness-check-url must be a non-empty string.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :load-balancing
                                             (str "load-balancing must be one of 'oldest', 'youngest' or 'random'."))
                                           (attach-error-message-for-parameter
                                             parameter->issues :max-instances "max-instances must be between 1 and 1000.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :mem "mem must be a positive number.")
                                           (attach-error-message-for-parameter
                                             parameter->issues
                                             :metadata
                                             (generate-friendly-metadata-error-message parameter->issues))
                                           (attach-error-message-for-parameter
                                             parameter->issues
                                             :metric-group
                                             (str "The metric-group must be be between 2 and 32 characters; "
                                                  "only contain lowercase letters, numbers, dashes, and underscores; "
                                                  "start with a lowercase letter; and "
                                                  "only use dash and/or underscore as separators between alphanumeric portions."))
                                           (attach-error-message-for-parameter
                                             parameter->issues :min-instances "min-instances must be between 1 and 1000.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :name "name must be a non-empty string.")
                                           (attach-error-message-for-parameter
                                             parameter->issues :ports "ports must be an integer in the range [1, 10].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :profile
                                             (str "profile must be a non-empty string"
                                                  (compute-valid-profiles-str profile->defaults)))
                                           (attach-error-message-for-parameter
                                             parameter->issues :scale-down-factor "scale-down-factor must be a double in the range (0, 1).")
                                           (attach-error-message-for-parameter
                                             parameter->issues :scale-factor "scale-factor must be a double in the range (0, 2].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :scale-up-factor "scale-up-factor must be a double in the range (0, 1).")
                                           (attach-error-message-for-parameter
                                             parameter->issues :termination-grace-period-secs
                                             "termination-grace-period-secs must be an integer in the range [0, 300].")
                                           (attach-error-message-for-parameter
                                             parameter->issues :version "version must be a non-empty string."))
              unresolved-parameters (set/difference (-> parameter->issues keys set)
                                                    (->> parameter->error-message keys (map name) set))
              friendly-error-message (str/join (str \newline) (vals parameter->error-message))]
          (throw-error e (select-keys parameter->issues unresolved-parameters) friendly-error-message))))

    (try
      (s/validate max-constraints-schema service-description-to-use)
      (catch Exception e
        (let [issue (s/check max-constraints-schema service-description-to-use)
              issue->param->limit (fn [issue param]
                                    (-> issue
                                        (get param)
                                        .schema
                                        .pred-name
                                        (str/replace "limit-" "")))
              param->message (fn [param]
                               (let [value (get service-description-to-use param)
                                     limit (issue->param->limit issue param)]
                                 (if (contains? headers/params-with-str-value param)
                                   (str param " must be at most " limit " characters")
                                   (str param " is " value " but the max allowed is " limit))))
              friendly-error-message (str "The following fields exceed their allowed limits: "
                                          (str/join ", " (->> issue
                                                              keys
                                                              sort
                                                              (map param->message))))]
          (throw-error e issue friendly-error-message))))

    ; Validate max-instances >= min-instances
    (let [{:strs [min-instances max-instances]} service-description-to-use]
      (when (and (integer? min-instances) (integer? max-instances))
        (when (> min-instances max-instances)
          (let [error-message (str "min-instances (" min-instances ") must be less than or equal to max-instances (" max-instances ")")]
            (sling/throw+ {:type :service-description-error
                           :message exception-message
                           :friendly-error-message error-message
                           :status http-400-bad-request
                           :log-level :warn})))))

    ; Validate the cmd-type field
    (let [cmd-type (service-description-to-use "cmd-type")
          {:keys [valid-cmd-types]} args-map]
      (when (and (not (str/blank? cmd-type)) (not (contains? valid-cmd-types cmd-type)))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "Command type " cmd-type " is not supported")
                       :status http-400-bad-request
                       :log-level :info})))

    ; validate authentication and health-check-authentication combination
    (let [{:strs [authentication health-check-authentication]} service-description-to-use]
      (when (and authentication
                 health-check-authentication
                 (= authentication "disabled")
                 (= health-check-authentication "standard"))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "The health check authentication (" health-check-authentication ") "
                                                    "cannot be enabled when authentication (" authentication ") is disabled")
                       :status http-400-bad-request
                       :log-level :info})))

    ; validate the health-check-port-index
    (let [{:strs [health-check-port-index ports]} service-description-to-use]
      (when (and health-check-port-index ports (>= health-check-port-index ports))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "The health check port index (" health-check-port-index ") "
                                                    "must be smaller than ports (" ports ")")
                       :status http-400-bad-request
                       :log-level :info})))

    ; validate the backend-proto and health-check-proto combination on same port
    (let [{:strs [backend-proto health-check-port-index health-check-proto]} service-description-to-use]
      (when (and backend-proto health-check-port-index health-check-proto
                 (zero? health-check-port-index) (not= backend-proto health-check-proto))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "The backend-proto (" backend-proto ") and health check proto (" health-check-proto ") "
                                                    "must match when health-check-port-index is zero")
                       :status http-400-bad-request
                       :log-level :info})))

    ; validate authentication and liveness-check-authentication combination
    (let [{:strs [authentication liveness-check-authentication]} service-description-to-use]
      (when (and authentication
                 liveness-check-authentication
                 (= authentication "disabled")
                 (= liveness-check-authentication "standard"))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "The liveness check authentication (" liveness-check-authentication ") "
                                                    "cannot be enabled when authentication (" authentication ") is disabled")
                       :status http-400-bad-request
                       :log-level :info})))

    ; validate the liveness-check-port-index
    (let [{:strs [liveness-check-port-index ports]} service-description-to-use]
      (when (and liveness-check-port-index ports (>= liveness-check-port-index ports))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "The liveness check port index (" liveness-check-port-index ") "
                                                    "must be smaller than ports (" ports ")")
                       :status http-400-bad-request
                       :log-level :info})))

    ; validate the backend-proto and liveness-check-proto combination on same port
    (let [{:strs [backend-proto liveness-check-port-index liveness-check-proto]} service-description-to-use]
      (when (and backend-proto liveness-check-port-index liveness-check-proto
                 (zero? liveness-check-port-index) (not= backend-proto liveness-check-proto))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message (str "The backend-proto (" backend-proto ") and liveness check proto (" liveness-check-proto ") "
                                                    "must match when liveness-check-port-index is zero")
                       :status http-400-bad-request
                       :log-level :info})))

    ;; currently, if manually specified, the namespace *must* match the run-as-user
    ;; (but we expect the common case to be falling back to the default)
    (let [{:strs [namespace run-as-user]} service-description-to-use]
      (when (and (some? namespace) (not= namespace run-as-user))
        (sling/throw+ {:type :service-description-error
                       :friendly-error-message "Service namespace must either be omitted or match the run-as-user."
                       :status http-400-bad-request
                       :log-level :info})))

    ; validate the profile when it is configured
    (let [{:strs [profile]} service-description-to-use]
      (when-not (str/blank? profile)
        (validate-profile-parameter profile->defaults profile)))))

(defn validate-default-namespace-min-instances
  "When using the default namespace, there are stricter restrictions on min-instances on allowed values."
  [min-instances]
  (when (and (integer? min-instances)
             (> min-instances maximum-default-namespace-min-instances))
    (let [error-message (str "min-instances (" min-instances ") in the default namespace "
                             "must be less than or equal to " maximum-default-namespace-min-instances)]
      (throw (ex-info error-message {:type :service-description-error
                                     :friendly-error-message error-message
                                     :status http-400-bad-request
                                     :log-level :info})))))

(defprotocol ServiceDescriptionBuilder
  "A protocol for constructing a service description from the various sources. Implementations
  allow for plugging in different schemes (e.g. manipulating the cmd) based on different needs."

  (build [this core-service-description args-map]
    "Returns a map of {:service-id ..., :service-description ..., :core-service-description...}")

  (compute-effective [this service-id core-service-description]
    "Returns the effective service description after processing the core description.")

  (retrieve-reference-type->stale-info-fn [this context]
    "Returns a map of reference type to stale function for references of the specified type.
     The values are functions that have the following signature (fn reference-entry)")

  (state [this include-flags]
    "Returns the global (i.e. non-service-specific) state the service description builder is maintaining")

  (validate [this service-description args-map]
    "Throws if the provided service-description is not valid"))

(defn assoc-run-as-requester-fields
  "Attaches the run-as-user and permitted-user fields to the service description.
   We intentionally force permitted-user to be the username for run-as-requester feature."
  [service-description username]
  (assoc service-description "run-as-user" username "permitted-user" username))

(defn run-as-requester?
  "Returns true if the service description maps to a run-as-requester service."
  [{:strs [run-as-user]}]
  (= "*" run-as-user))

(defn requires-parameters?
  "Returns true if the service description maps to a parameterized service whose parameters do not have a default value."
  [{:strs [allowed-params env]}]
  (boolean
    (and (seq allowed-params)
         (seq (set/difference allowed-params (set (keys env)))))))

(let [hash-prefix "E-"]
  (defn token-data->token-hash
    "Converts the merged map of service-description and token-metadata to a hash."
    [token-data]
    (when (and (seq token-data)
               (not (get token-data "deleted")))
      (str hash-prefix (-> token-data
                         (select-keys token-data-keys)
                         (dissoc "previous")
                         parameters->id)))))

(defn retrieve-token-update-epoch-time
  "Returns the time at which the specific version of a token went stale.
   If no such version can be found, then the stale time is unknown and we, conservatively,
   use the last known update time.
   Else if the token is not stale or no data is known about the token, nil is returned."
  [token current-token-data token-version]
  (let [token-data->update-time #(get % "last-update-time")
        token-data->previous #(get % "previous")
        update-time (descriptor-utils/retrieve-update-time
                      token-data->token-hash token-data->update-time token-data->previous current-token-data token-version)]
    (if (string? update-time)
      (do
        (log/warn token "has string last update time" update-time)
        (-> update-time du/str-to-date tc/to-long))
      update-time)))

;; forward declaration of the remove-service-entry-tokens function to avoid reordering a bunch of related functions
(declare remove-service-entry-tokens)

(defn retrieve-token-stale-info
  "The provided source-tokens are stale if every token used to access a service has been updated.
   The criteria of every token being updated is a conservative choice since we do not compute the merged
   results to verify if changing any of the tokens changed the service description and caused the service
   to go stale earlier.
   Returns a map containing two keys:
   - :stale? is true if the provided source-tokens are stale;
   - :update-epoch-time is either nil or the max time of the individual update times of when the
     source tokens with the specified version was edited."
  [token->token-hash token->token-parameters source-tokens]
  ;; safe assumption mark a service stale when every token used to access it is stale
  (let [sanitized-tokens (remove-service-entry-tokens source-tokens #(-> % (:token) (str)))
        stale? (and (not (empty? source-tokens)) ;; ensures boolean value
                    (or (empty? sanitized-tokens)
                        (every? (fn [{:keys [token version]}]
                                  (not= (token->token-hash token) version))
                                sanitized-tokens)))]
    (cond-> {:stale? stale?}
      stale? (assoc :update-epoch-time
                    (->> sanitized-tokens
                      (map (fn [{:keys [token version]}]
                             (retrieve-token-update-epoch-time token (token->token-parameters token) version)))
                      (reduce utils/nil-safe-max nil))))))

(def ^:const waiter-config-token-path ["env" "WAITER_CONFIG_TOKEN"])

(defn- assoc-token-in-env
  "Attaches the token sequence as an environment variable in the service description."
  [service-description token-sequence]
  (assoc-in service-description waiter-config-token-path (str/join "," token-sequence)))

(defn promote-default-to-exclusive?
  "Determines if a service description should be promoted to exclusive mode based on reference last update time."
  [reference-update-epoch-time]
  (let [exclusive-promotion-start-epoch-time (config/retrieve-exclusive-promotion-start-epoch-time)]
    (< exclusive-promotion-start-epoch-time reference-update-epoch-time)))

(defn- assoc-approved-run-as-requester-parameters
  "Attaches run-as-requester parameters if the services has been approved."
  [service-description assoc-run-as-user-approved? service-id-prefix username]
  (let [candidate-service-description (assoc-run-as-requester-fields service-description username)
        candidate-service-id (service-description->service-id service-id-prefix candidate-service-description)]
    (if (assoc-run-as-user-approved? candidate-service-id)
      (do
        (log/debug "appending run-as-user into pre-approved service" candidate-service-id)
        candidate-service-description)
      service-description)))

(defn retrieve-most-recent-component-update-time
  "Computes the most recent last-update-time from the component->last-update-epoch-time map."
  [component->last-update-epoch-time]
  (reduce utils/nil-safe-max 0 (vals component->last-update-epoch-time)) )

(defrecord DefaultServiceDescriptionBuilder [kv-store max-constraints-schema metric-group-mappings profile->defaults service-description-defaults]
  ServiceDescriptionBuilder

  (build [this user-service-description
          {:keys [assoc-run-as-user-approved? component->last-update-epoch-time component->previous-descriptor-fns reference-type->entry run-as-user-changed?
                  service-id-prefix source-tokens token-sequence token-service-mapping username]}]
    (let [exclusive-mode? (or (= "exclusive" token-service-mapping)
                              (and (= "default" token-service-mapping)
                                   (-> component->last-update-epoch-time
                                     (retrieve-most-recent-component-update-time)
                                     (promote-default-to-exclusive?))))
          core-service-description
          (cond-> user-service-description
            ;; include token name in the service description to enforce unique token->service mappings
            ;; leverage WAITER_CONFIG_ prefixed environment variables being allowed
            exclusive-mode?
            (assoc-token-in-env token-sequence)
            (not (contains? user-service-description "run-as-user"))
            (assoc-approved-run-as-requester-parameters assoc-run-as-user-approved? service-id-prefix username))
          service-id (service-description->service-id service-id-prefix core-service-description)
          service-description (compute-effective this service-id core-service-description)
          reference-type->entry (cond-> (or reference-type->entry {})
                                  (seq source-tokens)
                                  (assoc :token {:sources (map walk/keywordize-keys source-tokens)}))
          ;; only for exclusive mapping can we be sure that the service will always have the run-as-user coming from the token
          ;; for legacy mapping services, it is possible for the run-as-user to come from the token or the request
          run-as-user-source (if (and (not run-as-user-changed?) exclusive-mode?) "token" "unknown")]
      {:component->previous-descriptor-fns component->previous-descriptor-fns
       :core-service-description core-service-description
       :reference-type->entry reference-type->entry
       :run-as-user-source run-as-user-source
       :service-description service-description
       :service-id service-id}))

  (compute-effective [_ service-id core-service-description]
    ;; attaches defaults and overrides to the core service description
    (default-and-override
      core-service-description service-description-defaults profile->defaults
      metric-group-mappings kv-store service-id param-to-param-default-mapping))

  (retrieve-reference-type->stale-info-fn [_ {:keys [token->token-hash token->token-parameters]}]
    {:token (fn [{:keys [sources]}] (retrieve-token-stale-info token->token-hash token->token-parameters sources))})

  (state [_ _]
    {})

  (validate [_ service-description args-map]
    (->> (update args-map :valid-cmd-types set/union #{"docker" "shell"})
         (validate-schema service-description max-constraints-schema profile->defaults))))

(defn extract-max-constraints
  "Extracts the max constraints from the generic constraints definition."
  [constraints]
  (->> constraints
       (filter (fn [[_ constraint]] (contains? constraint :max)))
       (pc/map-vals :max)))

(defn extract-max-constraints-schema
  "Extracts the max limit constraints from the set of all constraints."
  [constraints]
  (->> constraints
    extract-max-constraints
    (map (fn [[k v]]
           (let [string-param? (contains? headers/params-with-str-value k)]
             [(s/optional-key k)
              (s/pred #(<= (if string-param? (count %) %) v)
                      (symbol (str "limit-" v)))])))
    (into {s/Str s/Any})))

(defn create-default-service-description-builder
  "Returns a new DefaultServiceDescriptionBuilder which uses the specified resource limits."
  [{:keys [constraints kv-store metric-group-mappings profile->defaults service-description-defaults]}]
  (let [max-constraints-schema (extract-max-constraints-schema constraints)]
    (->DefaultServiceDescriptionBuilder
      kv-store max-constraints-schema metric-group-mappings profile->defaults service-description-defaults)))

(defn service-description->health-check-url
  "Returns the configured health check Url or a default value (available in `default-health-check-path`)"
  [service-description]
  (or (get service-description "health-check-url") default-health-check-path))

(def invalid-chars-re #"[^a-zA-Z0-9\-_$\.]")
(def valid-token-re #"[a-zA-Z]([a-zA-Z0-9\-_$\.])+")

(defn validate-token
  "Validate token name against regex and throw an exception if not valid."
  [token]
  (let [bad-char (re-find invalid-chars-re token)]
    (cond
      bad-char (throw (ex-info (str "Token contains invalid character: \"" bad-char "\"") 
                               {:status http-400-bad-request
                                :token token
                                :pattern (str invalid-chars-re)
                                :invalid-char bad-char
                                :char-index (str/index-of token bad-char)
                                :char-codepoint (format "u%04X" (.codePointAt bad-char 0))
                                :log-level :warn}))

      (not (re-matches valid-token-re token)) (throw (ex-info "Token must match pattern"
                                                              {:status http-400-bad-request 
                                                               :token token 
                                                               :pattern (str valid-token-re) 
                                                               :log-level :warn})))))

(defn- token->token-data
  "Retrieves the data stored against the token in the kv-store."
  [kv-store ^String token allowed-keys error-on-missing include-deleted]
  (let [{:strs [deleted run-as-user] :as token-data}
        (when token
          (try
            (kv/fetch kv-store token)
            (catch Exception e
              (if error-on-missing
                (do
                  (log/info e "Error in kv-store fetch")
                  (throw (ex-info (str "Token not found: " token) {:status http-404-not-found :log-level :warn} e)))
                (log/info e "Ignoring kv-store fetch exception")))))
        token-data (when (seq token-data) ; populate token owner for backwards compatibility
                     (-> token-data
                         (utils/assoc-if-absent "owner" run-as-user)
                         (utils/assoc-if-absent "previous" {})))]
    (when (and error-on-missing (not token-data))
      (throw (ex-info (str "Token not found: " token) {:status http-404-not-found :log-level :warn})))
    (log/debug "Extracted data for" token "is" token-data)
    (when (or (not deleted) include-deleted)
      (select-keys token-data allowed-keys))))

(defn token->token-hash
  "Retrieves the hash for a token"
  [kv-store token]
  (token-data->token-hash (token->token-data kv-store token token-data-keys false false)))

(defn token-data->token-description
  "Retrieves the token description for the given token when the raw kv data (merged value of service
   parameters and metadata) is provided.
   The token-description consists of the following keys: :service-description-template and :token-metadata"
  [config]
  {:service-parameter-template (select-keys config service-parameter-keys)
   :token-metadata (select-keys config token-metadata-keys)})

(defn token->token-description
  "Retrieves the token description for the given token."
  [kv-store ^String token & {:keys [include-deleted] :or {include-deleted false}}]
  (token-data->token-description (token->token-data kv-store token token-data-keys false include-deleted)))

(defn token->service-parameter-template
  "Retrieves the service description template for the given token containing only the service parameters."
  [kv-store ^String token & {:keys [error-on-missing] :or {error-on-missing true}}]
  (token->token-data kv-store token service-parameter-keys error-on-missing false))

(defn token->token-parameters
  "Retrieves the template for the given token containing only the token data parameters."
  [kv-store ^String token & {:keys [error-on-missing include-deleted]
                             :or {error-on-missing true
                                  include-deleted false}}]
  (token->token-data kv-store token token-data-keys error-on-missing include-deleted))

(defn token->service-description-template
  "Retrieves the service description template for the given token including the service metadata values."
  [kv-store ^String token & {:keys [error-on-missing] :or {error-on-missing true}}]
  (let [token-data (token->token-data kv-store token token-data-keys error-on-missing false)]
    (select-keys token-data service-parameter-keys)))

(defn token->token-metadata
  "Retrieves the token metadata for the given token."
  [kv-store ^String token & {:keys [error-on-missing] :or {error-on-missing true}}]
  (token->token-data kv-store token token-metadata-keys error-on-missing false))

(defn retrieve-token-from-service-description-or-hostname
  "Retrieve the token name from the service description map using the x-waiter-token key.
   If such a token is not found, then revert to using the host name (without the port) as the token."
  [waiter-headers request-headers waiter-hostnames]
  (let [token-header (headers/get-waiter-header waiter-headers "token")
        host-header (get request-headers "host")
        hostname (first (str/split (str host-header) #":"))]
    (cond
      (not (str/blank? token-header))
      {:source :waiter-header :token token-header}
      (and (not (contains? waiter-hostnames hostname)) (not (str/blank? hostname)))
      {:source :host-header :token hostname}
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

(defn token-sequence->merged-data
  "Computes the merged token-data using the provided token-sequence and token->token-data mapping.
   It removes the metadata keys from the returned result."
  [token->token-data token-sequence]
  (loop [loop-token-data {}
         [token & remaining-tokens] token-sequence]
    (if token
      (recur (merge-parameters loop-token-data (token->token-data token))
             remaining-tokens)
      loop-token-data)))

(defn source-tokens-entry
  "Creates an entry for the source-tokens field"
  [token token-data]
  {"token" token "version" (token-data->token-hash token-data)})

(defn attach-token-defaults
  "Attaches token defaults into the token parameters."
  [{:strs [profile] :as token-parameters} token-defaults profile->defaults]
  (merge
    (compute-token-defaults token-defaults profile->defaults profile)
    token-parameters))

(defn compute-service-description-template-from-tokens
  "Computes the service description, preauthorization and authentication data using the token-sequence and token-data."
  [attach-service-defaults-fn attach-token-defaults-fn token-sequence token->token-data]
  (let [merged-token-data (attach-token-defaults-fn
                            (token-sequence->merged-data token->token-data token-sequence))
        token-service-mapping (or (when (-> token-sequence (remove-service-entry-tokens identity) (seq))
                                    (get merged-token-data "service-mapping"))
                                  ;; use legacy mode in the absence of tokens
                                  "legacy")
        service-description-template (select-keys merged-token-data service-parameter-keys)
        source-tokens (mapv #(source-tokens-entry % (token->token-data %)) token-sequence)]
    {:fallback-period-secs (get merged-token-data "fallback-period-secs")
     :service-description-template service-description-template
     :source-tokens source-tokens
     :token->token-data token->token-data
     :token-authentication-disabled (and (= 1 (count token-sequence))
                                         (token-authentication-disabled? service-description-template))
     :token-preauthorized (and (= 1 (count token-sequence))
                               (-> service-description-template
                                 (attach-service-defaults-fn)
                                 (token-preauthorized?)))
     :token-sequence token-sequence
     :token-service-mapping token-service-mapping}))

(defn- prepare-service-description-template-from-tokens
  "Prepares the service description using the token(s)."
  [waiter-headers request-headers kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn]
  (let [{:keys [token source]}
        (retrieve-token-from-service-description-or-hostname waiter-headers request-headers waiter-hostnames)]
    (cond
      (= source :host-header)
      (let [token-data (token->token-data kv-store token token-data-keys false false)
            token-sequence (if (seq token-data) [token] [])
            token->token-data (if (seq token-data) {token token-data} {})]
        (compute-service-description-template-from-tokens
          attach-service-defaults-fn attach-token-defaults-fn token-sequence token->token-data))

      (= source :waiter-header)
      (let [token-sequence (str/split (str token) #",")]
        (loop [loop-token->token-data {}
               [loop-token & remaining-tokens] token-sequence]
          (if loop-token
            (let [token-data (token->token-data kv-store loop-token token-data-keys true false)]
              (recur (assoc loop-token->token-data loop-token token-data) remaining-tokens))
            (compute-service-description-template-from-tokens
              attach-service-defaults-fn attach-token-defaults-fn token-sequence loop-token->token-data))))

      :else
      (compute-service-description-template-from-tokens attach-service-defaults-fn attach-token-defaults-fn [] {}))))

(let [service-entry-prefix "^SERVICE-ID#"
      service-id->key #(str service-entry-prefix %)]
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
    (when refresh
      (log/info "force fetching core service description for" service-id))
    (let [service-description (kv/fetch kv-store (service-id->key service-id) :refresh refresh)]
      (if (map? service-description)
        service-description
        (if nil-on-missing?
          (log/warn "No description found!" {:refresh refresh :service-id service-id})
          (throw (ex-info "No description found!" {:refresh refresh :service-id service-id}))))))

  (defn fetch-stats
    "Loads the stats for the specified service-id from the key-value store."
    [kv-store ^String service-id]
    (kv/stats kv-store (service-id->key service-id)))

  (defn remove-service-entry-tokens
    "Removes tokens that represent direct service ID entries."
    [entry-seq entry->token]
    (remove #(str/starts-with? (entry->token %) service-entry-prefix) entry-seq)))

(defn refresh-service-descriptions
  "Refreshes missing service descriptions for the specified service ids.
   Returns the set of service-ids which have valid service descriptions."
  [kv-store service-ids]
  (->> service-ids
       (filter
         (fn [service-id]
           (or (fetch-core kv-store service-id :refresh false)
               (log/info "refreshing the service description for" service-id)
               (fetch-core kv-store service-id :refresh true)
               (log/warn "filtering" service-id "as no matching service description was found"))))
       set))

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

(defn- parse-env-map-headers
  "Parses headers into an environment map.
   The keys in the environment map are formed by stripping the `(str key-name \"-\")` prefix and then upper casing the string."
  [service-description key-name]
  (let [env-keys (filter (fn [key] (str/starts-with? key (str key-name "-"))) (keys service-description))
        env-map (select-keys service-description env-keys)]
    (if (empty? env-map)
      service-description
      (let [key-regex (re-pattern (str "^" key-name "-"))
            renamed-env-map (pc/map-keys #(str/upper-case (str/replace % key-regex "")) env-map)
            sanitized-service-description (utils/remove-keys service-description env-keys)]
        (assoc sanitized-service-description key-name renamed-env-map)))))

(defn- parse-metadata-headers
  "Parses metadata headers into the metadata map.
   The keys in the metadata map are formed by stripping the `metadata-` prefix."
  [service-description]
  (let [metadata-keys (filter (fn [key] (str/starts-with? key "metadata-")) (keys service-description))
        metadata-map (select-keys service-description metadata-keys)]
    (if (empty? metadata-map)
      service-description
      (let [renamed-metadata-map (pc/map-keys #(str/replace % #"^metadata-" "") metadata-map)
            sanitized-service-description (utils/remove-keys service-description metadata-keys)]
        (assoc sanitized-service-description "metadata" renamed-metadata-map)))))

(defn waiter-headers->service-parameters
  "Converts the x-waiter headers to service parameters."
  [waiter-headers]
  (-> waiter-headers
    headers/drop-waiter-header-prefix
    (parse-env-map-headers "env")
    (parse-env-map-headers "param")
    parse-metadata-headers
    transform-allowed-params-header
    (sanitize-service-description service-description-from-header-keys)))

(defn prepare-service-description-sources
  [{:keys [waiter-headers passthrough-headers]} kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn]
  "Prepare the service description sources from the current request.
   Populates the service description for on-the-fly waiter-specific headers.
   Also populates for the service description for a token (first looked in headers and then using the host name).
   Finally, it also includes the service configuration defaults."
  (let [service-description-template-from-headers (waiter-headers->service-parameters waiter-headers)]
    (-> (prepare-service-description-template-from-tokens
          waiter-headers passthrough-headers kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn)
        (assoc :headers service-description-template-from-headers))))

(defn merge-service-description-sources
  [descriptor kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn]
  "Merges the sources for a service-description into the descriptor."
  (->> (prepare-service-description-sources
         descriptor kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn)
       (assoc descriptor :sources)))

(defn- sanitize-metadata [{:strs [metadata] :as service-description}]
  (if metadata
    (let [sanitized-metadata (pc/map-keys #(str/lower-case %) metadata)]
      (assoc service-description "metadata" sanitized-metadata))
    service-description))

(defn retrieve-most-recent-last-update-time
  "Computes the most recent last-update-time from the token->token-data map."
  [token->token-data]
  (->> token->token-data
    (pc/map-vals (fn [{:strs [last-update-time]}] (or last-update-time 0)))
    (vals)
    (reduce max 0)))

(let [error-message-map-fn (fn [passthrough-headers waiter-headers]
                             {:error-class error-class-service-misconfigured
                              :log-level :warn
                              :non-waiter-headers (dissoc passthrough-headers "authorization")
                              :status http-400-bad-request
                              :x-waiter-headers waiter-headers})]
  (defn compute-service-description
    "Computes the service description applying any processing rules,
     It also validates the services description.
     It creates the service-description using the following preferential order:
      - env from param headers
      - on-the-fly headers
      - token description
      - configured defaults.
     If a non-param on-the-fly header is provided, the username is included as the run-as-user in on-the-fly headers.
     If after the merge a run-as-user is not available, then `username` becomes the run-as-user.
     If after the merge a permitted-user is not available, then `username` becomes the permitted-user."
    [{:keys [headers service-description-template source-tokens token->token-data token-authentication-disabled token-preauthorized token-sequence token-service-mapping]}
     waiter-headers passthrough-headers component->previous-descriptor-fns service-id-prefix username
     assoc-run-as-user-approved? service-description-builder]
    (let [headers-without-params (dissoc headers "param")
          header-params (get headers "param")
          ; any change with the on-the-fly must change the run-as-user if it doesn't already exist
          service-description-based-on-headers (if (seq headers-without-params)
                                                 (utils/assoc-if-absent headers-without-params "run-as-user" username)
                                                 headers-without-params)
          merge-params (fn [{:strs [allowed-params] :as service-description}]
                         (if header-params
                           (do
                             (when-not (every? #(contains? allowed-params %) (keys header-params))
                               (throw (ex-info "Some params cannot be configured"
                                               {:allowed-params allowed-params :params header-params :status http-400-bad-request :log-level :warn})))
                             (-> service-description
                                 (update "env" merge header-params)
                                 (dissoc "param")))
                           service-description))
          service-description-from-headers-and-token-sources (-> (merge service-description-template
                                                                        service-description-based-on-headers)
                                                                 ; param headers need to update the environment
                                                                 merge-params)
          raw-run-as-user (get service-description-from-headers-and-token-sources "run-as-user")
          raw-namespace (get service-description-from-headers-and-token-sources "namespace")
          sanitized-service-description-from-sources (cond-> service-description-from-headers-and-token-sources
                                                       ;; * run-as-user is the same as a missing run-as-user
                                                       (= "*" raw-run-as-user)
                                                       (dissoc "run-as-user")
                                                       ;; * namespace means match the current user (for use with run-as-requester)
                                                       (= "*" raw-namespace)
                                                       (assoc "namespace" username))
          sanitized-run-as-user (get sanitized-service-description-from-sources "run-as-user")
          sanitized-metadata-description (sanitize-metadata sanitized-service-description-from-sources)
          ; run-as-user will not be set if description-from-headers or the token description contains it.
          ; else rely on presence of x-waiter headers to set the run-as-user
          on-the-fly? (headers/contains-waiter-header waiter-headers on-the-fly-service-description-keys)
          contains-service-parameter-header? (headers/contains-waiter-header waiter-headers service-parameter-keys)
          user-service-description (cond-> sanitized-metadata-description
                                     (and (not (contains? sanitized-metadata-description "run-as-user")) on-the-fly?)
                                     ; can only set the run-as-user if some on-the-fly-service-description-keys waiter header was provided
                                     (assoc-run-as-requester-fields username)
                                     contains-service-parameter-header?
                                     ; can only set the permitted-user if some service-description-keys waiter header was provided
                                     (assoc "permitted-user" (or (get headers "permitted-user") username)))
          ;; determine if the service's run-as-user is different from token's if token was used to create the service
          run-as-user-changed? (not= (get user-service-description "run-as-user" username)
                                     (get service-description-template "run-as-user"))]
      (when-not (seq user-service-description)
        (throw (ex-info (utils/message :cannot-identify-service)
                        (-> (error-message-map-fn passthrough-headers waiter-headers)
                          (assoc :error-class error-class-service-unidentified)))))
      (when (and (= "*" raw-run-as-user) raw-namespace (not= "*" raw-namespace))
        (throw (ex-info "Cannot use run-as-requester with a specific namespace"
                        {:error-class error-class-service-misconfigured
                         :namespace raw-namespace
                         :run-as-user raw-run-as-user
                         :status http-400-bad-request
                         :log-level :warn})))
      (when-let [idle-timeout-mins-header (get waiter-headers (str headers/waiter-header-prefix "idle-timeout-mins"))]
        (when (zero? idle-timeout-mins-header)
          (throw (ex-info "idle-timeout-mins on-the-fly header configured to a value of zero is not supported"
                          {:error-class error-class-service-misconfigured
                           :log-level :info
                           :status http-400-bad-request
                           :waiter-headers waiter-headers}))))
      (sling/try+
        (let [component->last-update-epoch-time (cond-> {}
                                                  (seq token->token-data)
                                                  (assoc :token (retrieve-most-recent-last-update-time token->token-data)))
              build-map (build service-description-builder user-service-description
                               {:assoc-run-as-user-approved? assoc-run-as-user-approved?
                                :component->last-update-epoch-time component->last-update-epoch-time
                                :component->previous-descriptor-fns component->previous-descriptor-fns
                                :reference-type->entry {}
                                :run-as-user-changed? run-as-user-changed?
                                :service-id-prefix service-id-prefix
                                :source-tokens source-tokens
                                :token-sequence token-sequence
                                :token-service-mapping token-service-mapping
                                :username username})
              service-preauthorized (and token-preauthorized (empty? service-description-based-on-headers))
              service-authentication-disabled (and token-authentication-disabled (empty? service-description-based-on-headers))]
          (-> build-map
            (select-keys [:component->previous-descriptor-fns :core-service-description :reference-type->entry
                          :run-as-user-source :service-description :service-id])
            (assoc :on-the-fly? on-the-fly?
                   :service-authentication-disabled service-authentication-disabled
                   :service-preauthorized service-preauthorized
                   :source-tokens source-tokens)))
        (catch [:type :service-description-error] ex-data
          (throw (ex-info (:message ex-data)
                          (merge (error-message-map-fn passthrough-headers waiter-headers) ex-data)
                          (:throwable &throw-context)))))))

  (defn validate-service-description
    "Returns nil if the provided descriptor contains a valid service description.
     Else it returns an instance of Throwable that reflects the validation error."
    [kv-store service-description-builder
     {:keys [core-service-description passthrough-headers service-description service-id waiter-headers]}]
    (sling/try+
      (let [stored-service-description (fetch-core kv-store service-id)]
        ; Validating is expensive, so avoid validating if we've validated before, relying on the fact
        ; that we'll only store validated service descriptions
        (when-not (seq stored-service-description)
          (validate service-description-builder core-service-description
                    {:allow-missing-required-fields? false})
          (validate service-description-builder service-description
                    {:allow-missing-required-fields? false}))
        nil)
      (catch [:type :service-description-error] ex-data
        (ex-info (:message ex-data)
                 (merge (error-message-map-fn passthrough-headers waiter-headers) ex-data)
                 (:throwable &throw-context)))
      (catch Throwable th
        th))))

(defn merge-service-description-and-id
  "Populates the descriptor with the service-description and service-id."
  [{:keys [component->previous-descriptor-fns passthrough-headers sources waiter-headers] :as descriptor}
   service-id-prefix username service-description-builder assoc-run-as-user-approved?]
  (->> (compute-service-description
         sources waiter-headers passthrough-headers component->previous-descriptor-fns service-id-prefix
         username assoc-run-as-user-approved? service-description-builder)
       (merge descriptor)))

(defn make-build-service-description-and-id-helper
  "Factory function to return the function used to complete creating a descriptor using the builder."
  [kv-store service-id-prefix current-request-user service-description-builder
   assoc-run-as-user-approved?]
  (fn build-service-description-and-id-helper
    ;; If there is an error in attaching the service description or id and throw-exception? is false,
    ;; the descriptor is returned with the exception in the :error key.
    [descriptor throw-exception?]
    (try
      (-> descriptor
        (merge-service-description-and-id service-id-prefix current-request-user
                                          service-description-builder assoc-run-as-user-approved?)
        (merge-suspended kv-store))
      (catch Throwable ex
        (if throw-exception?
          (throw ex)
          (do
            (log/info ex "error in building service-description")
            (assoc descriptor :error ex)))))))

(defn retrieve-most-recently-modified-token
  "Computes the most recently modified token from the token->token-data map."
  [token->token-data]
  (->> token->token-data
       (apply max-key (fn [[_ {:strs [last-update-time]}]] (or last-update-time 0)))
       first))

(defn retrieve-most-recently-modified-token-update-time
  "Computes the last-update-time of the most recently modified token from the descriptor."
  [descriptor]
  (or
    (some->> descriptor
      :sources
      :token->token-data
      (retrieve-most-recent-last-update-time))
    0))

(defn service-id->service-description
  "Loads the service description for the specified service-id including any overrides."
  [service-description-builder kv-store service-id & {:keys [effective?] :or {effective? true}}]
  (let [core-service-description (or (fetch-core kv-store service-id :refresh false)
                                     ;; make best-effort to retrieve the service description from kv store
                                     (fetch-core kv-store service-id :refresh true))]
    (if effective?
      (compute-effective service-description-builder service-id core-service-description)
      core-service-description)))

(defn service-id->creation-time
  "Loads the creation time, as a a DateTime instance, for the specified service-id.
   Returns nil if no creation time is known for the service."
  [kv-store service-id]
  (let [{:keys [creation-time]} (fetch-stats kv-store service-id)]
    (some-> creation-time
      (tc/from-long))))

(defn can-manage-service?
  "Returns whether the `username` is allowed to modify the specified service description."
  [kv-store entitlement-manager service-id username]
  ;; the stored service description should already have a run-as-user
  ;; none of the overridden parameters should affect who can manage a service
  (let [core-service-description (fetch-core kv-store service-id :refresh false)]
    (authz/manage-service? entitlement-manager username service-id core-service-description)))

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

(defn consent-cookie->map
  "Converts the consent cookie value to a map"
  [cookie-value]
  (let [[mode issue-time service-id-or-token owner] cookie-value]
    (cond-> {}
      issue-time (assoc "issue-time" issue-time)
      mode (assoc "mode" mode)
      (= mode "service") (assoc "service-id" service-id-or-token)
      (= mode "token") (assoc "token" service-id-or-token)
      owner (assoc "owner" owner))))

(defn assoc-run-as-user-approved?
  "Deconstructs the decoded cookie and validates whether the service has been pre-approved.
   The validation step includes:
   a. Ensuring service-id or token/owner pair are valid based on the mode, and
   b. the consent has not expired based on the timestamp in the cookie."
  [clock consent-expiry-days service-id token {:strs [owner]} decoded-consent-cookie]
  (let [[consent-mode auth-timestamp consent-id consent-owner] (vec decoded-consent-cookie)]
    (log/info "consent cookie decoded as" consent-mode auth-timestamp consent-id consent-owner)
    (and
      consent-id
      auth-timestamp
      (or (and (= "service" consent-mode) (= consent-id service-id))
          (and (= "token" consent-mode) (= consent-id token) (= consent-owner owner)))
      (> (+ auth-timestamp (-> consent-expiry-days t/days t/in-millis))
         (.getMillis ^DateTime (clock))))))

(defn source-tokens->gc-time-secs
  "Computes the seconds after which a service created using provided source tokens should be GC-ed
   based on the token fallback-period-secs and stale-timeout-mins.
   The fallback-period-secs and stale-timeout-mins are computed from the latest versions of the tokens
   as fallback period now applies to the latest service.
   Using stale-timeout-mins allows users to revert their changes and have their traffic served by
   the existing service without having to wait for a new instance to get started.
   The function also assumes that source-tokens-seq is non-empty.
   It ignores whether GC has been disabled by configuring idle-timeout-mins=0.
   The caller must take care to handle the scenario where GC has been disabled."
  [token->token-parameters attach-token-defaults-fn source-tokens-seq]
  (->> source-tokens-seq
    (map (fn source-tokens->gc-time-helper [source-tokens]
           (let [{:strs [fallback-period-secs stale-timeout-mins]}
                 (->> source-tokens
                   (map #(some-> % :token token->token-parameters))
                   (reduce merge {})
                   (attach-token-defaults-fn))]
             (+ fallback-period-secs (-> stale-timeout-mins t/minutes t/in-seconds)))))
    (reduce max)))

(defn accumulate-stale-info
  "Merges the two stale info maps.
   The result map has the following keys:
   - :stale? true if either of the :stale? entries of the inputs were true;
   - :update-epoch-time
      if the new info map is stale, contains the min edit epoch time of the two inputs
      else contains the edit epoch time of the first stale info."
  [current-stale-info {:keys [stale? update-epoch-time]}]
  (cond-> current-stale-info
    stale? (-> (assoc :stale? true)
               (update :update-epoch-time utils/nil-safe-min update-epoch-time))))

(defn- reference->stale-info
  "Returns the stale info for the provided reference.
   An empty, i.e. directly accessible, reference is never stale.
   The result map has the following keys:
   - :stale? true if any of entries in the reference has gone stale;
   - :update-epoch-time
      nil if none of the entries in the reference is stale or none of the stale entries have a known stale time
      else contains the min edit epoch time of the stale entries from the reference."
  [reference-type->stale-info-fn reference]
  (reduce
    (fn accumulate-reference->stale-info [acc-info [type entry]]
      (if-let [stale-info-fn (reference-type->stale-info-fn type)]
        (accumulate-stale-info acc-info (stale-info-fn entry))
        acc-info))
    {:stale? false :update-epoch-time nil}
    (seq reference)))

(defn references->stale-info
  "Returns the stale info for the provided references.
   The result map has the following keys:
   - :stale? true if all references have gone stale;
   - :update-epoch-time
      nil if none of the references are stale or none of the stale references have a known stale time
      else contains the max edit epoch time of the stale references."
  [reference-type->stale-info-fn references]
  (try
    (let [stale-info-seq (map #(reference->stale-info reference-type->stale-info-fn %) references)
          stale? (and (not (empty? references))
                      (every? true? (map :stale? stale-info-seq)))]
      {:stale? stale?
       :update-epoch-time (when stale?
                            (->> stale-info-seq
                              (map :update-epoch-time stale-info-seq)
                              (reduce utils/nil-safe-max nil)))})
    (catch Exception ex
      (log/error ex (str "error in computing staleness of references"))
      {:stale? false
       :update-epoch-time nil})))

(defn service->gc-time
  "Computes the time when a given service should be GC-ed.
   If the service is active and has GC disabled (by configuring idle-timeout-mins=0), nil is returned.
   Else if the service is active or was created without references, the GC time is calculated using
   the service's idle-timeout-mins and the most recent completion time of the service's requests.
   Else, the GC time is the computed from the reference stale time, fallback period seconds and the stale service timeout."
  [service-id->service-description-fn service-id->references-fn token->token-parameters reference-type->stale-info-fn
   attach-token-defaults-fn service-id last-modified-time]
  (let [{:strs [idle-timeout-mins]} (service-id->service-description-fn service-id)
        references (service-id->references-fn service-id)
        {:keys [stale? update-epoch-time]} (references->stale-info reference-type->stale-info-fn references)]
    (cond
      ;; when stale, use token info to compute GC time
      ;; Note: a service created without references never goes stale
      stale?
      (let [update-time (if update-epoch-time (tc/from-long update-epoch-time) last-modified-time)
            gc-delay-duration (if-let [source-tokens (->> references (map :token) (remove nil?) (map :sources) seq)]
                                ;; use time from fallback and stale timeout for a service that uses tokens
                                (t/seconds (source-tokens->gc-time-secs token->token-parameters attach-token-defaults-fn source-tokens))
                                ;; use the default token stale timeout for a stale service built without tokens
                                ;; (this stale-timeout-mins value is configured in the default token values :( )
                                (let [{:strs [stale-timeout-mins]} (attach-token-defaults-fn {})]
                                  (t/minutes stale-timeout-mins)))]
        (log/info service-id "that uses references went stale at" (du/date-to-str update-time)
                  "and will be gc-ed in" (t/in-seconds gc-delay-duration) "seconds")
        (t/plus update-time gc-delay-duration))
      ;; when GC is enabled, use idle-timeout
      (pos? idle-timeout-mins)
      (t/plus last-modified-time (t/minutes idle-timeout-mins)))))

(defn service-id->stale?
  "Returns true if all the references used to access the service have gone stale.
   Note: a service created without references never goes stale."
  [reference-type->stale-info-fn service-id->references-fn service-id]
  (let [references (service-id->references-fn service-id)
        {:keys [stale?]} (references->stale-info reference-type->stale-info-fn references)]
    (true? stale?)))

(let [service-id->key #(str "^REFERENCES#" %)]

  (defn service-id->references
    "Retrieves the reference entries (as a set) for a service from the key-value store."
    [kv-store service-id & {:keys [refresh] :or {refresh false}}]
    (let [keys (service-id->key service-id)]
      (kv/fetch kv-store keys :refresh refresh)))

  (defn store-reference!
    "Stores the reference entries in the key-value store against a service."
    [synchronize-fn kv-store service-id reference]
    (when-not (-> (service-id->references kv-store service-id)
                (or #{})
                (contains? reference))
      (let [reference-lock-prefix "REFERENCES-LOCK-"
            bucket (-> service-id hash int (Math/abs) (mod 16))
            reference-lock (str reference-lock-prefix bucket)]
        (meters/mark! (metrics/waiter-meter "core" "reference" "store" "all"))
        (meters/mark! (metrics/waiter-meter "core" "reference" "store" (str "bucket-" bucket)))
        (synchronize-fn
          reference-lock
          (fn inner-store-reference! []
            (let [existing-entries (or (service-id->references kv-store service-id :refresh true) #{})]
              (if-not (contains? existing-entries reference)
                (do
                  (meters/mark! (metrics/waiter-meter "core" "reference" "store" "new-entry"))
                  (log/info "storing new reference for" service-id reference)
                  (kv/store kv-store (service-id->key service-id) (conj existing-entries reference))
                  ;; refresh the entry
                  (service-id->references kv-store service-id :refresh true))
                (log/debug "reference already associated with" service-id reference)))))))))

(let [service-id->key #(str "^SOURCE-TOKENS#" %)]

  (defn service-id->source-tokens-entries
    "Retrieves the source-tokens entries (as a set) for a service from the key-value store."
    [kv-store service-id & {:keys [refresh] :or {refresh false}}]
    (let [keys (service-id->key service-id)]
      (kv/fetch kv-store keys :refresh refresh)))

  (defn- has-source-tokens?
    "Returns true if the source-tokens are already mapped against the service-id."
    [kv-store service-id source-tokens]
    (-> (service-id->source-tokens-entries kv-store service-id)
        (or #{})
        (contains? source-tokens)))

  (defn store-source-tokens!
    "Stores a source-tokens entry in the key-value store against a service."
    [synchronize-fn kv-store service-id source-tokens]
    (when (and (seq source-tokens)
               ;; guard to avoid relatively expensive synchronize-fn invocation
               (not (has-source-tokens? kv-store service-id source-tokens)))
      (let [source-tokens-lock-prefix "SOURCE-TOKENS-LOCK-"
            bucket (-> service-id hash int (Math/abs) (mod 16))
            source-tokens-lock (str source-tokens-lock-prefix bucket)]
        (meters/mark! (metrics/waiter-meter "core" "source-tokens" "store" "all"))
        (meters/mark! (metrics/waiter-meter "core" "source-tokens" "store" (str "bucket-" bucket)))
        (synchronize-fn
          source-tokens-lock
          (fn inner-store-source-tokens! []
            (let [existing-entries (-> (service-id->source-tokens-entries kv-store service-id :refresh true)
                                       (or #{}))]
              (if-not (contains? existing-entries source-tokens)
                (do
                  (log/info "associating" source-tokens "with" service-id "source-tokens entries")
                  (kv/store kv-store (service-id->key service-id) (conj existing-entries source-tokens))
                  ;; refresh the entry
                  (service-id->source-tokens-entries kv-store service-id :refresh true))
                (do
                  (meters/mark! (metrics/waiter-meter "core" "source-tokens" "store" "no-op"))
                  (log/info source-tokens "source-tokens already associated with" service-id))))))))))

(defn discover-service-parameters
  "Processing the request headers to identify the Waiter service parameters.
   Returns a map of the waiter and passthrough headers, the identified token, and
   the service description template created from the token."
  [kv-store attach-service-defaults-fn attach-token-defaults-fn waiter-hostnames headers unsupported-headers]
  (let [{:keys [passthrough-headers waiter-headers]} (headers/split-headers headers)
        {:keys [token]} (retrieve-token-from-service-description-or-hostname
                          waiter-headers passthrough-headers waiter-hostnames)
        service-parameter-template-from-token (when token
                                                (token->service-parameter-template kv-store token :error-on-missing false))
        contains-service-parameter-header? (headers/contains-waiter-header waiter-headers service-parameter-keys)
        service-parameter-template-from-headers (when contains-service-parameter-header?
                                                  (waiter-headers->service-parameters waiter-headers))
        service-description-template (cond-> service-parameter-template-from-token
                                       contains-service-parameter-header?
                                       (->
                                         (merge service-parameter-template-from-headers)
                                         ;; remove user fields from service description
                                         (dissoc "permitted-user" "run-as-user"))
                                       (seq service-parameter-template-from-token)
                                       (attach-service-defaults-fn))
        token-metadata (when token
                         (-> (token->token-parameters kv-store token :error-on-missing false)
                           (attach-token-defaults-fn)
                           (select-keys token-metadata-keys)))
        unsupported-headers-in-request (set/intersection (set (keys waiter-headers)) unsupported-headers)]
    (when (not-empty unsupported-headers-in-request)
      (let [error-details {:unsupported-headers-in-request unsupported-headers-in-request}]
        (log/info "Unsupported waiter headers found" error-details)
        (throw (ex-info "Unsupported waiter headers found"
                        {:details error-details
                         :log-level :info
                         :status http-400-bad-request}))))
    {:passthrough-headers passthrough-headers
     :service-description-template service-description-template
     :token token
     :token-metadata token-metadata
     :waiter-headers waiter-headers}))

(defn param-value->filter-fn
  "Accepts a single string or a sequence of strings as input.
   Creates the filter function that does substring match for the input string or any string in the sequence."
  [param-value star-means-all?]
  (if (string? param-value)
    (utils/str->filter-fn param-value star-means-all?)
    (utils/strs->filter-fn param-value star-means-all?)))

(defn query-params->service-description-filter-predicate
  "Creates the filter function for service descriptions that matches every parameter provided in the request-params map."
  [request-params parameter-keys star-means-all?]
  (let [service-description-params (utils/filterm
                                     (fn [[param-name _]]
                                       (->> (str/split param-name #"\.")
                                         (first)
                                         (contains? parameter-keys)))
                                     request-params)
        param-predicates (map (fn [[param-name param-value]]
                                (let [param-predicate (param-value->filter-fn param-value star-means-all?)]
                                  (fn [service-description]
                                    (->> (str/split param-name #"\.")
                                      (get-in service-description)
                                      (str)
                                      (param-predicate)))))
                              (seq service-description-params))]
    (fn [service-description]
      (or (empty? param-predicates)
          (every? #(%1 service-description) param-predicates)))))

(defn service-description-bypass-enabled?
  "Returns true if the provided service description has bypass enabled and false otherwise."
  [service-desc]
  (= "true"
     (get-in service-desc ["metadata" "waiter-proxy-bypass-opt-in"])))
