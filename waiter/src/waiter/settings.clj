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
(ns waiter.settings
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [waiter.schema :as schema]
            [waiter.utils :as utils]))

(def settings-schema
  {(s/required-key :authenticator-config) (s/constrained
                                            {:kind s/Keyword
                                             s/Keyword schema/require-symbol-factory-fn}
                                            schema/contains-kind-sub-map?)
   (s/required-key :blacklist-config) {(s/required-key :blacklist-backoff-base-time-ms) schema/positive-int
                                       (s/required-key :max-blacklist-time-ms) schema/positive-int}
   (s/required-key :cors-config) (s/constrained
                                   {:kind s/Keyword
                                    (s/optional-key :ttl) schema/positive-int
                                    (s/required-key :max-age) schema/positive-int
                                    s/Keyword schema/require-symbol-factory-fn}
                                   schema/contains-kind-sub-map?)
   (s/required-key :cluster-config) {(s/required-key :min-routers) schema/positive-int
                                     (s/required-key :name) schema/non-empty-string}
   (s/required-key :consent-expiry-days) schema/positive-int
   (s/required-key :deployment-error-config) {(s/required-key :min-failed-instances) schema/positive-int
                                              (s/required-key :min-hosts) schema/positive-int}
   (s/required-key :entitlement-config) (s/constrained
                                          {:kind s/Keyword
                                           (s/optional-key :cache) {(s/required-key :threshold) schema/positive-int
                                                                    (s/required-key :ttl) schema/positive-int}
                                           s/Keyword schema/require-symbol-factory-fn}
                                          schema/contains-kind-sub-map?)
   (s/optional-key :git-version) s/Any
   (s/required-key :health-check-config) {(s/required-key :health-check-timeout-ms) schema/positive-int
                                          (s/required-key :failed-check-threshold) schema/positive-int}
   (s/required-key :host) schema/non-empty-string
   (s/required-key :hostname) schema/non-empty-string
   (s/required-key :instance-request-properties) {(s/required-key :async-check-interval-ms) schema/positive-int
                                                  (s/required-key :async-request-timeout-ms) schema/positive-int
                                                  (s/required-key :connection-timeout-ms) schema/positive-int
                                                  (s/required-key :initial-socket-timeout-ms) schema/positive-int
                                                  (s/required-key :output-buffer-size) schema/positive-int
                                                  (s/required-key :streaming-timeout-ms) schema/positive-int
                                                  (s/required-key :queue-timeout-ms) schema/positive-int}
   (s/required-key :kv-config) (s/constrained
                                 {:kind s/Keyword
                                  (s/optional-key :encrypt) s/Bool
                                  (s/optional-key :cache) {(s/required-key :threshold) schema/positive-int
                                                           (s/required-key :ttl) schema/positive-int}
                                  s/Keyword schema/require-symbol-factory-fn}
                                 schema/contains-kind-sub-map?)
   (s/optional-key :messages) {s/Keyword s/Str}
   (s/required-key :metric-group-mappings) schema/valid-metric-group-mappings
   (s/required-key :metrics-config) {(s/required-key :inter-router-metrics-idle-timeout-ms) schema/positive-int
                                     (s/required-key :metrics-gc-interval-ms) schema/positive-int
                                     (s/required-key :metrics-sync-interval-ms) schema/positive-int
                                     (s/required-key :router-update-interval-ms) schema/positive-int
                                     (s/required-key :transient-metrics-timeout-ms) schema/positive-int}
   (s/required-key :password-store-config) (s/constrained
                                             {:kind s/Keyword
                                              s/Keyword schema/require-symbol-factory-fn}
                                             schema/contains-kind-sub-map?)
   (s/required-key :port) schema/positive-int
   (s/required-key :router-id-prefix) s/Str
   (s/required-key :router-syncer) {(s/required-key :delay-ms) schema/positive-int
                                    (s/required-key :interval-ms) schema/positive-int}
   (s/required-key :scaling) {(s/required-key :autoscaler-interval-ms) schema/positive-int
                              (s/required-key :inter-kill-request-wait-time-ms) schema/positive-int}
   (s/required-key :scheduler-config) (s/constrained
                                        {:kind s/Keyword
                                         s/Keyword schema/require-symbol-factory-fn}
                                        schema/contains-kind-sub-map?)
   (s/required-key :scheduler-gc-config) {(s/required-key :broken-service-min-hosts) schema/positive-int
                                          (s/required-key :broken-service-timeout-mins) schema/positive-int
                                          (s/required-key :scheduler-gc-broken-service-interval-ms) schema/positive-int
                                          (s/required-key :scheduler-gc-interval-ms) schema/positive-int}
   (s/required-key :scheduler-syncer-interval-secs) schema/positive-int
   (s/required-key :service-description-builder-config) (s/constrained
                                                          {:kind s/Keyword
                                                           s/Keyword schema/require-symbol-factory-fn}
                                                          schema/contains-kind-sub-map?)
   ; service-description-defaults should never contain default values for required fields, e.g. version, cmd, run-as-user, etc.
   (s/required-key :service-description-defaults) {(s/required-key "authentication") schema/valid-authentication
                                                   (s/required-key "backend-proto") schema/valid-backend-proto
                                                   (s/required-key "blacklist-on-503") s/Bool
                                                   (s/required-key "concurrency-level") schema/positive-int
                                                   (s/required-key "distribution-scheme") (s/enum "balanced" "simple")
                                                   (s/required-key "env") {s/Str s/Str}
                                                   (s/required-key "expired-instance-restart-rate") schema/positive-fraction-less-than-or-equal-to-1
                                                   (s/required-key "grace-period-secs") schema/positive-int
                                                   (s/required-key "health-check-url") schema/non-empty-string
                                                   (s/required-key "idle-timeout-mins") schema/positive-int
                                                   (s/required-key "instance-expiry-mins") schema/positive-int
                                                   (s/required-key "jitter-threshold") schema/greater-than-or-equal-to-0-less-than-1
                                                   (s/required-key "max-instances") schema/positive-int
                                                   (s/required-key "max-queue-length") schema/positive-int
                                                   (s/required-key "metadata") {s/Str s/Str}
                                                   (s/required-key "min-instances") schema/positive-int
                                                   (s/required-key "permitted-user") schema/non-empty-string
                                                   (s/required-key "ports") schema/valid-number-of-ports
                                                   (s/required-key "restart-backoff-factor") schema/positive-number-greater-than-or-equal-to-1
                                                   (s/required-key "scale-factor") schema/positive-fraction-less-than-or-equal-to-1
                                                   (s/required-key "scale-up-factor") schema/positive-fraction-less-than-1
                                                   (s/required-key "scale-down-factor") schema/positive-fraction-less-than-1}
   (s/required-key :statsd) (s/either (s/eq :disabled)
                                      {(s/required-key :cluster) schema/non-empty-string
                                       (s/required-key :environment) schema/non-empty-string
                                       (s/optional-key :histogram-max-size) schema/positive-int
                                       (s/required-key :host) schema/non-empty-string
                                       (s/required-key :port) schema/positive-int
                                       (s/required-key :publish-interval-ms) schema/positive-int
                                       (s/required-key :server) schema/non-empty-string})
   (s/required-key :support-info) [{(s/required-key :label) s/Keyword
                                    (s/required-key :link) {(s/required-key :type) schema/non-empty-string
                                                            (s/required-key :value) schema/non-empty-string}}]
   (s/required-key :thread-stack-state-refresh-interval-ms) schema/positive-int
   (s/required-key :websocket-config) {(s/required-key :ws-max-binary-message-size) schema/positive-int
                                       (s/required-key :ws-max-text-message-size) schema/positive-int}
   (s/required-key :work-stealing) {(s/required-key :offer-help-interval-ms) schema/positive-int
                                    (s/required-key :reserve-timeout-ms) schema/positive-int}
   (s/required-key :zookeeper) {(s/required-key :base-path) schema/non-empty-string
                                (s/required-key :connect-string) schema/valid-zookeeper-connect-config
                                (s/required-key :curator-retry-policy) {(s/required-key :base-sleep-time-ms) schema/positive-int
                                                                        (s/required-key :max-sleep-time-ms) schema/positive-int
                                                                        (s/required-key :max-retries) schema/positive-int}
                                (s/required-key :discovery-relative-path) schema/non-empty-string
                                (s/required-key :gc-relative-path) schema/non-empty-string
                                (s/required-key :leader-latch-relative-path) schema/non-empty-string
                                (s/required-key :mutex-timeout-ms) schema/positive-int}})

(defn env [var-name config-file-path]
  (let [value (System/getenv var-name)]
    (when-not value
      (throw (ex-info (format "Environment variable '%s' referenced in config file '%s' is not set." var-name config-file-path)
                      {:var-name var-name :config-file-path config-file-path})))
    value))

(defn load-settings-file
  "Loads the edn config in the specified file, it relies on having the filename being a path to the file."
  [filename]
  (let [config-file (-> filename str io/file)
        config-file-path (.getAbsolutePath config-file)]
    (if (.exists config-file)
      (do
        (log/info "reading settings from file:" config-file-path)
        (let [edn-readers {:readers {'config/regex (fn [expr] (re-pattern expr))
                                     'config/env #(env % config-file-path)
                                     'config/env-int #(Integer/parseInt (env % config-file-path))}}
              settings (edn/read-string edn-readers (slurp config-file-path))]
          (log/info "configured settings:\n" (with-out-str (clojure.pprint/pprint settings)))
          settings))
      (do
        (log/info "unable to find configuration file:" config-file-path)
        (utils/exit 1 (str "Unable to find configuration file: " config-file-path))))))

(defn sanitize-settings
  "Sanitizes settings for eventual conversion to JSON"
  [settings]
  (utils/dissoc-in settings [:zookeeper :connect-string]))

(defn display-settings
  "Endpoint to display the current settings in use."
  [settings]
  (utils/map->json-response (into (sorted-map) (sanitize-settings settings))))

(def settings-defaults
  {:authenticator-config {:kind :one-user
                          :one-user {:factory-fn 'waiter.auth.authentication/one-user-authenticator}}
   :cors-config {:kind :patterns
                 :patterns {:factory-fn 'waiter.cors/pattern-based-validator
                            :allowed-origins []}
                 :allow-all {:factory-fn 'waiter.cors/allow-all-validator}
                 :max-age 3600}
   :blacklist-config {:blacklist-backoff-base-time-ms 10000
                      :max-blacklist-time-ms 300000}
   ;; To be considered part of the same cluster, routers need to
   ;; 1. have the same leader-latch-path to participate in leadership election
   ;; 2. have the same discovery path with the same cluster name to allow computing router endpoints
   :cluster-config {:min-routers 1
                    :name "waiter"}
   :consent-expiry-days 90
   :deployment-error-config {:min-failed-instances 2
                             :min-hosts 2}
   :entitlement-config {:kind :simple
                        :simple {:factory-fn 'waiter.authorization/->SimpleEntitlementManager}}
   :health-check-config {:health-check-timeout-ms 200
                         :failed-check-threshold 5}
   :host "0.0.0.0"
   :hostname "localhost"
   :instance-request-properties {:async-check-interval-ms 3000
                                 :async-request-timeout-ms 60000
                                 :connection-timeout-ms 5000 ; 5 seconds
                                 :initial-socket-timeout-ms 900000 ; 15 minutes
                                 :output-buffer-size 4096
                                 :queue-timeout-ms 300000
                                 :streaming-timeout-ms 20000}
   :kv-config {:kind :zk
               :zk {:factory-fn 'waiter.kv/new-zk-kv-store
                    :sync-timeout-ms 2000}
               :cache {:threshold 1000
                       :ttl 60}
               :encrypt true
               :relative-path "tokens"}
   :messages {:backend-request-failed "Request to service backend failed"
              :backend-request-timed-out "Request to service backend timed out"
              :bad-startup-command "Invalid startup command"
              :cannot-connect "Unable to connect to run health checks"
              :cannot-identify-service "Unable to identify service using waiter headers/token"
              :health-check-requires-authentication "Health check requires authentication"
              :health-check-timed-out "Health check timed out"
              :invalid-health-check-response "Health check returned an invalid response"
              :invalid-service-description "Service description using waiter headers/token improperly configured"
              :not-enough-memory "Not enough memory allocated"
              :prestashed-tickets-not-available "Prestashed jobsystem tickets not available"}
   :metric-group-mappings []
   :metrics-config {:inter-router-metrics-idle-timeout-ms 2000
                    :metrics-gc-interval-ms 60000
                    :metrics-sync-interval-ms 50
                    :router-update-interval-ms 5000
                    :transient-metrics-timeout-ms 300000}
   :password-store-config {:kind :configured
                           :configured {:factory-fn 'waiter.password-store/configured-provider
                                        :passwords ["open-sesame"]}}
   :port 9091
   :router-id-prefix ""
   :router-syncer {:delay-ms 750
                   :interval-ms 1500}
   :scaling {:autoscaler-interval-ms 1000
             ; throttles the rate at which kill requests are sent to the scheduler
             :inter-kill-request-wait-time-ms 1000}
   :scheduler-config {:kind :marathon
                      :marathon {:factory-fn 'waiter.marathon/marathon-scheduler
                                 :home-path-prefix "/home/"
                                 :http-options {:conn-timeout 10000
                                                :socket-timeout 10000
                                                :spnego-auth true}
                                 :force-kill-after-ms 60000
                                 :framework-id-ttl 900000}
                      :shell {:factory-fn 'waiter.shell-scheduler/shell-scheduler
                              :failed-instance-retry-interval-ms 5000
                              :health-check-interval-ms 5000
                              :health-check-timeout-ms 200
                              :port-grace-period-ms 120000
                              :port-range [10000 10999]
                              :work-directory "scheduler"}}
   :scheduler-gc-config {:broken-service-min-hosts 2
                         :broken-service-timeout-mins 30
                         :scheduler-gc-broken-service-interval-ms 60000
                         :scheduler-gc-interval-ms 60000}
   :scheduler-syncer-interval-secs 5
   :service-description-builder-config {:kind :default
                                        :default {:factory-fn
                                                  'waiter.service-description/->DefaultServiceDescriptionBuilder}}
   :service-description-defaults {"authentication" "standard"
                                  "backend-proto" "http"
                                  "blacklist-on-503" true
                                  "concurrency-level" 1
                                  "distribution-scheme" "balanced"
                                  "env" {}
                                  "expired-instance-restart-rate" 0.1
                                  "grace-period-secs" 30
                                  "health-check-url" "/status"
                                  "idle-timeout-mins" 30
                                  "instance-expiry-mins" 7200 ; 5 days
                                  "jitter-threshold" 0.5
                                  "max-instances" 500
                                  "max-queue-length" 1000000
                                  "metadata" {}
                                  "min-instances" 1
                                  "permitted-user" "*"
                                  "ports" 1
                                  "restart-backoff-factor" 2
                                  "scale-down-factor" 0.001
                                  "scale-factor" 1
                                  "scale-up-factor" 0.1}
   :statsd :disabled
   :support-info [{:label "Waiter on GitHub"
                   :link {:type :url
                          :value "http://github.com/twosigma/waiter"}}]
   :thread-stack-state-refresh-interval-ms 600000 ; 10 minutes
   :websocket-config {:ws-max-binary-message-size  (* 1024 1024 40)
                      :ws-max-text-message-size (* 1024 1024 40)}
   :work-stealing {:offer-help-interval-ms 100
                   :reserve-timeout-ms 1000}
   :zookeeper {:base-path "/waiter"
               :curator-retry-policy {:base-sleep-time-ms 100
                                      :max-retries 10
                                      :max-sleep-time-ms 120000}
               :discovery-relative-path "discovery"
               :gc-relative-path "gc-state"
               :leader-latch-relative-path "leader-latch"
               :mutex-timeout-ms 1000}})

(defn deep-merge-settings
  "Merges the two settings maps, with special handling for maps with :kind"
  [map-1 map-2]
  (merge-with
    (fn [x y]
      (if (and (map? x) (map? y))
        (let [x-kind (:kind x)
              y-kind (:kind y)]
          (if (and x-kind y-kind)
            (let [kind y-kind
                  x-sub-map (get x kind)
                  y-sub-map (get y kind)]
              (-> x
                  (merge y)
                  (assoc kind (merge x-sub-map y-sub-map))))
            (deep-merge-settings x y)))
        y))
    map-1 map-2))

(defn load-settings
  [config-file git-version]
  (deep-merge-settings
    settings-defaults
    (assoc
      (load-settings-file config-file)
      :git-version git-version)))
