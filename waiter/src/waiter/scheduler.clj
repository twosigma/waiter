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
(ns waiter.scheduler
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [slingshot.slingshot :as ss]
            [waiter.config :as config]
            [waiter.correlation-id :as cid]
            [waiter.descriptor :as descriptor]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.request-log :as rlog]
            [waiter.service-description :as sd]
            [waiter.statsd :as statsd]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (java.io EOFException)
           (java.net ConnectException SocketTimeoutException)
           (java.util.concurrent TimeoutException)
           (javax.net.ssl SSLException)
           (org.eclipse.jetty.client HttpClient)
           (org.joda.time DateTime)))

(def ^:const max-failed-instances-to-keep
  "The maximum number of failed instances tracked per service."
  10)

(def ^:const max-killed-instances-to-keep
  "The maximum number of kill instances tracked per service."
  ;; track more killed instances than failed instances
  (+ max-failed-instances-to-keep 2))

(defmacro log
  "Log Scheduler-specific messages."
  [& args]
  `(log/log "Scheduler" :debug nil (print-str ~@args)))

(def instance-tracker-keys [:eject-period-ms
                            :exit-code
                            :extra-ports
                            :flags
                            :health-check-status
                            :healthy?
                            :host
                            :id
                            :log-directory
                            :message
                            :port
                            :service-id
                            :started-at])

(defn log-service-instance
  "Log InstanceTracker-specific messages."
  [instance event-type log-level]
  (let [extra-ports (count (:extra-ports instance))
        instance-to-log (select-keys instance instance-tracker-keys)
        flags-map (->> instance-to-log
                       :flags
                       (map (fn [flag] [(str "flag_" (name flag)) true]))
                       (into {}))
        log-map (assoc (merge
                         (dissoc instance-to-log :flags)
                         flags-map)
                    :extra-ports extra-ports
                    :timestamp (t/now)
                    :event-type event-type)]
    (log/log "InstanceTracker" log-level nil (utils/clj->json log-map))))

(defrecord Service
  [^String id
   instances
   task-count
   task-stats])

(def ^:const UNKNOWN-IP "0.0.0.0")

(defn make-Service [value-map]
  (map->Service (merge {:task-stats {:running 0
                                     :healthy 0
                                     :unhealthy 0
                                     :staged 0}}
                       value-map)))

(defrecord ServiceInstance
  [^String id
   ^String service-id
   ^DateTime started-at
   healthy?
   health-check-status
   flags
   exit-code
   ^String host
   port
   extra-ports
   ^String log-directory
   ^String message
   ^String status])

(defn make-ServiceInstance [value-map]
  (map->ServiceInstance (merge {:extra-ports [] :flags #{}} value-map)))

(defprotocol ServiceScheduler

  (get-services [this]
    "Returns a list of scheduler/Service records")

  (signal-instance [this instance signal-type timeout] 
    "Instructs the scheduler to send specified signal to a specific ServiceInstance.
     Returns a map containing the following structure:
     {:success <boolean>, :message <string>, :status status-code}")

  (kill-instance [this instance]
    "Instructs the scheduler to kill a specific ServiceInstance.
     Returns a map containing the following structure:
     {:instance-id instance-id, :killed? <boolean>, :message <string>,  :service-id service-id, :status status-code}")

  (service-exists? [this ^String service-id]
    "Returns truth-y value if the service exists and nil otherwise.")

  (create-service-if-new [this descriptor]
    "Sends a call to Scheduler to start a service with the descriptor if the service does not already exist.
     Returns truth-y value if the service creation was successful and nil otherwise.")

  (delete-service [this ^String service-id]
    "Instructs the scheduler to delete the specified service.
     Returns a map containing the following structure:
     {:message message
      :result :deleted|:error|:no-such-service-exists
      :success true|false}")

  (deployment-error-config [this ^String service-id]
    "Returns nil, or a map containing a subset of the global :deployment-error-config options.
     These options should be merged (to override) the global :deployment-error-config options
     in the waiter.state module when computing the deployment errors for the given service-id.")

  (request-protocol [this instance port-index service-description]
     "Returns the protocol to use for a request to the given instance on the given port.
      Will return one of the following values: http, https, h2, h2c")

  (use-authenticated-health-checks? [this ^String service-id]
    "Returns whether to use authenticated requests while performing health checks.")

  (scale-service [this ^String service-id target-instances force]
    "Instructs the scheduler to scale up/down instances of the specified service to the specified number
     of instances. The force flag can be used enforce the scaling by ignoring previous pending operations.")

  (retrieve-directory-content [this ^String service-id ^String instance-id ^String host ^String directory]
    "Retrieves the content of the directory for the specified instance (identified by `instance-id`) on the
     specified `host`. It includes links to browse subdirectories and download files.")

  (service-id->state [this ^String service-id]
    "Retrieves the state the scheduler is maintaining for the given service-id.")

  (state [this include-flags]
    "Returns the global (i.e. non-service-specific) state the scheduler is maintaining")

  (validate-service [this ^String service-id]
    "Verify creating a services on the underlying scheduler platform;
     e.g., by checking that the run-as-user has the proper permissions.")

  (compute-instance-usage [this ^String service-id]
    "Returns a dictionary with :cpus and :mem keys indicating the cpu and mem resource usage of individual instances."))

(defn retry-on-transient-server-exceptions-fn
  "Helper function for `retry-on-transient-server-exceptions`.
   Calls the body which we assume includes calls to marathon.
   If a transient marathon exception occurs this macro catches it, and logs it and retries a fixed number of times.
   Will return the output of body if the first call or any of the retry calls returns successfully without
   throwing a transient exception.
   Will NOT catch unknown exceptions."
  [msg retry-status-codes f & options]
  (let [with-retries (utils/retry-strategy
                       (merge {:delay-multiplier 1.5, :initial-delay-ms 200, :max-retries 5} options))
        {:keys [error success]} (with-retries
                                  (fn []
                                    (ss/try+
                                      {:success (f)}
                                      (catch #(contains? retry-status-codes (:status %)) e
                                        (log/warn (str "scheduler unavailable (error code:" (:status e) ").") msg)
                                        (ss/throw+ e))
                                      (catch ConnectException e
                                        (log/warn "connection to scheduler failed." msg)
                                        (ss/throw+ e))
                                      (catch SocketTimeoutException e
                                        (log/warn "socket timeout in connection to scheduler." msg)
                                        (ss/throw+ e))
                                      (catch TimeoutException e
                                        (log/warn "timeout in connection to scheduler." msg)
                                        (ss/throw+ e))
                                      (catch Throwable th
                                        {:error th})
                                      (catch #(not (nil? %)) _
                                        {:error (:throwable &throw-context)}))))]
    (if error
      (throw error)
      success)))

(defmacro retry-on-transient-server-exceptions
  "Calls the body which we assume includes calls to marathon.
   If a transient marathon exception occurs this macro catches it, and logs it and retries a fixed number of times.
   Will return the output of body if the first call or any of the retry calls returns successfully without throwing
   a transient exception.
   Will NOT catch unknown exceptions."
  [msg & body]
  `(retry-on-transient-server-exceptions-fn
     ~msg
     #{http-500-internal-server-error http-501-not-implemented http-502-bad-gateway http-503-service-unavailable http-504-gateway-timeout}
     (fn [] ~@body)))

(defn suppress-transient-server-exceptions-fn
  "Helper function for `suppress-transient-server-exceptions`.
   Calls the body which we assume includes calls to marathon.
   If a transient marathon exception occurs this macro catches and logs it and returns nil.
   Will return the output of body otherwise.
   Will NOT catch unknown exceptions."
  [msg f]
  (ss/try+
    (f)
    (catch #(contains?
              #{http-500-internal-server-error http-501-not-implemented http-502-bad-gateway http-503-service-unavailable http-504-gateway-timeout}
              (:status %)) e
      (log/warn (str "Scheduler unavailable (Error code: " (:status e) ").") msg)
      (log/debug (:throwable &throw-context) "Scheduler unavailable." msg))))

(defmacro suppress-transient-server-exceptions
  "Calls the body which we assume includes calls to marathon.
   If a transient marathon exception occurs this macro catches and logs it and returns nil
   Will return the output of body otherwise. Will NOT catch unknown exceptions."
  [msg & body]
  `(suppress-transient-server-exceptions-fn ~msg (fn [] ~@body)))

(defn instance->service-id
  "Returns the name of the service, stripping out any preceding slashes."
  [service-instance]
  (:service-id service-instance))

(defn instance->port
  "Retrieves the port to use from the instance based on the port index."
  [{:keys [extra-ports port]} port-index]
  (if (pos? port-index)
    (->> port-index dec (nth extra-ports))
    port))

(defn retrieve-protocol
  "Get the protocol for the given port index based off the service description.
   Used as the default implementation for the request-protocol function
   on most ServiceScheduler protocol implementations."
  [port-index {:strs [backend-proto health-check-port-index health-check-proto] :as service-description}]
  (cond
    (zero? port-index) backend-proto
    (= port-index health-check-port-index) (or health-check-proto backend-proto)
    :else (throw (ex-info "Unrecognized port index"
                          {:port-index port-index
                           :service-description service-description}))))

(defn- base-url
  "Returns the url at which the service definition resides."
  [^String protocol ^String host port]
  (let [scheme (hu/backend-proto->scheme protocol)]
    (str scheme "://" host ":" port)))

(defn end-point-url
  "Returns the endpoint url which can be queried on the url specified by the url-info map."
  [^String protocol ^String host port ^String end-point]
  (str (base-url protocol host port)
       (if (and end-point (str/starts-with? end-point "/")) end-point (str "/" end-point))))

(defn build-health-check-url
  "Returns the health check url which can be queried on the service instance."
  [scheduler {:keys [host] :as instance} {:strs [health-check-port-index] :as service-description}]
  (let [health-check-path (sd/service-description->health-check-url service-description)
        proto (request-protocol scheduler instance health-check-port-index service-description)
        url-port (instance->port instance health-check-port-index)]
    (end-point-url proto host url-port health-check-path)))

(defn log-health-check-issues
  "Logs messages based on the type of error (if any) encountered by a health check"
  [service-instance instance-health-check-url {:keys [error headers status]}]
  (if error
    (let [error-map {:instance service-instance
                     :service instance-health-check-url}]
      (condp instance? error
        ConnectException (log/debug error "error while connecting to backend for health check" error-map)
        SocketTimeoutException (log/debug error "timeout while connecting to backend for health check" error-map)
        TimeoutException (log/debug error "timeout while connecting to backend for health check" error-map)
        EOFException (log/info "Got an EOF when connecting to backend for health check" error-map)
        Throwable (log/error error "unexpected error while connecting to backend for health check" error-map)))
    (when-not (or (hu/status-2XX? status)
                  (= http-404-not-found status)
                  (= http-504-gateway-timeout status))
      (log/info "unexpected status from health check" {:headers headers
                                                       :instance service-instance
                                                       :service instance-health-check-url
                                                       :status status}))))

(defn service-description->health-check-protocol
  "Determines the protocol to use for health checks."
  [{:strs [backend-proto health-check-proto]}]
  (or health-check-proto backend-proto))

(defn retrieve-auth-headers
  "Retrieves the map of auth headers to be passed to health checks."
  [service-id->password-fn service-id]
  (let [service-password (service-id->password-fn service-id)
        waiter-principal (config/retrieve-waiter-principal)]
    (headers/retrieve-basic-auth-headers
      "waiter" service-password waiter-principal)))

(defn merge-auth-headers
  "Merges the map of auth headers to be passed to health checks with the existing headers."
  [existing-headers service-id->password-fn service-id]
  (merge existing-headers
         (retrieve-auth-headers service-id->password-fn service-id)))

(defn classify-response-errors
  "Return a corresponding error flag for this request, or nil if there was no error."
  [{:keys [error] :as response}]
  (cond
    (some? error)
    (cond
      (instance? ConnectException error) :connect-exception
      (instance? EOFException error) :hangup-exception
      (instance? SocketTimeoutException error) :timeout-exception
      (instance? SSLException error) :ssl-exception
      (instance? TimeoutException error) :timeout-exception)
    (utils/raven-proxy-response? response)
    (when-let [raven-error-flags (utils/raven-response-flags response)]
      (let [raven-error-details (utils/raven-status-details response)
            connection-error? (= raven-error-flags envoy-upstream-connection-failure)
            ssl-exception? (and connection-error? (some-> raven-error-details (str/includes? "TLS_error")))]
        (cond
          ssl-exception? :ssl-exception
          connection-error? :connect-exception
          (= raven-error-flags envoy-upstream-connection-termination) :hangup-exception
          (= raven-error-flags envoy-stream-idle-timeout) :timeout-exception
          (= raven-error-flags envoy-upstream-request-timeout) :timeout-exception)))))

(defn available?
  "Async go block which returns the status code and success of a health check.
   Returns {:healthy? false} if such a connection cannot be established."
  [service-id->password-fn ^HttpClient http-client scheduler scheduler-name
   {:keys [host port service-id] :as service-instance}
   {:strs [health-check-authentication health-check-port-index health-check-url]
    :or {health-check-port-index 0} :as service-description}]
  (async/go
    (try
      (if (and port (pos? port) host (not= UNKNOWN-IP host))
        (let [protocol (request-protocol scheduler service-instance health-check-port-index service-description)
              instance-health-check-url (build-health-check-url scheduler service-instance service-description)
              request-timeout-ms (max (+ (.getConnectTimeout http-client) (.getIdleTimeout http-client)) http-200-ok)
              request-abort-chan (async/chan 1)
              request-time (t/now)
              request-time-ns (System/nanoTime)
              correlation-id (str "waiter-health-check-" (utils/unique-identifier))
              authenticate-health-check? (use-authenticated-health-checks? scheduler service-id)
              request-headers (cond-> (merge headers/waiter-health-check-headers
                                             {"host" host
                                              "user-agent" (some-> http-client .getUserAgentField .getValue)
                                              "x-cid" correlation-id})
                                authenticate-health-check?
                                (merge-auth-headers service-id->password-fn service-id))
              health-check-response-chan (http/get http-client instance-health-check-url
                                                   {:abort-ch request-abort-chan
                                                    :headers request-headers})
              _ (meters/mark! (metrics/waiter-meter "scheduler" scheduler-name "health-check" "invocation-rate"))
              {:keys [error-flag status]}
              (async/alt!
                health-check-response-chan
                ([response]
                 (let [{:keys [error status] :as response} response
                       response-time-ns (System/nanoTime)
                       error-flag (classify-response-errors response)]
                   (log-health-check-issues service-instance instance-health-check-url response)
                   (let [proto-version (hu/backend-protocol->http-version protocol)
                         proto-scheme (hu/backend-proto->scheme protocol)
                         request {:client-protocol proto-version
                                  :headers request-headers
                                  :internal-protocol proto-version
                                  :request-id correlation-id
                                  :request-method :get
                                  :request-time request-time
                                  :scheme proto-scheme
                                  :uri health-check-url}
                         backend-response-latency-ns (- response-time-ns request-time-ns)
                         response (assoc response
                                    :backend-response-latency-ns backend-response-latency-ns
                                    :descriptor {:service-description service-description
                                                 :service-id service-id}
                                    :error-class (some-> error .getClass .getCanonicalName)
                                    :instance service-instance
                                    :instance-proto protocol
                                    :latest-service-id service-id
                                    :protocol proto-version
                                    :request-type "health-check"
                                    :waiter-api-call? false)]
                     (rlog/log-request! request response))
                   {:error-flag error-flag
                    :status status}))

                (async/timeout request-timeout-ms)
                ([_]
                 (let [ex (TimeoutException. "Health check request exceeded its allocated time")
                       callback (fn abort-health-check-callback [aborted?]
                                  (log/info "health check aborted:" aborted?))]
                   (async/>! request-abort-chan [ex callback]))
                 (meters/mark! (metrics/waiter-meter "scheduler" scheduler-name "health-check" "timeout-rate"))
                 (log/info "health check timed out before receiving response"
                           {:health-check-url health-check-url
                            :instance service-instance
                            :scheduler scheduler-name
                            :timeout request-timeout-ms})
                 {:error-flag :operation-timeout}))]
          (async/close! request-abort-chan)
          {:healthy? (and (nil? error-flag) (hu/status-2XX? status))
           :status status
           :error error-flag})
        {:error :unknown-authority
         :healthy? false})
      (catch Exception e
        (log/error e "exception thrown while performing health check" {:instance service-instance
                                                                       :health-check-url health-check-url})
        {:healthy? false}))))

(defn authenticated-health-check-configured?
  "Returns true if service is configured authenticated health checks."
  [{:strs [health-check-authentication]}]
  (= "standard" health-check-authentication))

(defn instance-comparator
  "The comparison order is: service-id, started-at, and finally id."
  [x y]
  (let [service-id-comp (compare (:service-id x) (:service-id y))]
    (if (zero? service-id-comp)
      (let [started-at-comp (compare (:started-at x) (:started-at y))]
        (if (zero? started-at-comp)
          (compare (:id x) (:id y))
          started-at-comp))
      service-id-comp)))

(defn sort-instances
  "Sorts two service instances, the comparison order specified by `instance-comparator`."
  [coll]
  (sort instance-comparator coll))

(defn- scheduler-gc-sanitize-state
  "Helper function to sanitize the state in the scheduler GC routines."
  [prev-service->state cur-services]
  (let [cur-services-set (set cur-services)]
    (utils/filterm (fn [[service _]] (contains? cur-services-set service)) prev-service->state)))

(defn scheduler-services-gc
  "Performs scheduler GC by tracking which services are idle (i.e. have no outstanding requests and positive idle-timeout-mins).
   The function launches a go-block that tracks the metrics state of all services currently being managed by the scheduler.
   Idle services are detected based on no changes to the metrics state past the `idle-timeout-mins` period.
   They are then deleted by the leader using the `delete-service` function.
   If an error occurs while deleting a service, there will be repeated attempts to delete it later."
  [scheduler query-state-fn service-id->metrics-fn {:keys [scheduler-gc-interval-ms]} service-gc-go-routine
   service->gc-time-fn]
  (let [service->raw-data-fn (fn scheduler-services-gc-service->raw-data-fn []
                               (let [{:keys [all-available-service-ids]} (query-state-fn)
                                     global-state (service-id->metrics-fn)]
                                 (pc/map-from-keys
                                   (fn scheduler-services-gc-service->raw-data-fn [service-id]
                                     (let [{:strs [last-request-time outstanding]} (get global-state service-id)]
                                       ;; if outstanding and total are missing return unknown which results in cur state
                                       (if (or last-request-time outstanding)
                                         {"last-request-time" last-request-time
                                          "outstanding" (or outstanding 0)}
                                         ::unknown)))
                                   all-available-service-ids)))
        service->state-fn (fn [_ cur-data new-data]
                            (-> (if (= new-data ::unknown)
                                  cur-data
                                  new-data)
                                (utils/assoc-if-absent "outstanding" 0)
                                (update "last-request-time" du/max-time (get cur-data "last-request-time"))))
        gc-service?-fn (fn [service-id {:keys [last-modified-time state]} current-time]
                         (try
                           (let [outstanding (get state "outstanding")]
                             (and (number? outstanding)
                                  (zero? outstanding)
                                  (let [gc-time (service->gc-time-fn service-id last-modified-time)]
                                    (log/debug service-id "gc time:" (du/date-to-str gc-time) "current:" (du/date-to-str current-time))
                                    (when gc-time
                                      (t/after? current-time gc-time)))))
                           (catch Exception ex
                             (log/error ex "error while computing GC time for" service-id)
                             false)))
        perform-gc-fn (fn [service-id]
                        (log/info "deleting idle service" service-id)
                        (try
                          (delete-service scheduler service-id)
                          (catch Exception e
                            (log/error e "unable to delete idle service" service-id))))]
    (log/info "starting scheduler-services-gc")
    (service-gc-go-routine
      "scheduler-services-gc"
      service->raw-data-fn
      scheduler-gc-interval-ms
      scheduler-gc-sanitize-state
      service->state-fn
      gc-service?-fn
      perform-gc-fn)))

(defn scheduler-broken-services-gc
  "Performs scheduler GC by tracking which services are broken (i.e. has no healthy instance, but at least one failed instance possibly due to a broken command).
   The function launches a go-block that tracks the metrics state of all services currently being managed by the scheduler.
   Faulty services are detected based on no changes to the healthy/failed instances state past the `broken-service-timeout-mins` period, respectively.
   They are then deleted by the leader using the `delete-service` function.
   If an error occurs while deleting a service, there will be repeated attempts to delete it later."
  [service-gc-go-routine query-state-fn service-id->service-description-fn scheduler
   {:keys [broken-service-timeout-mins broken-service-min-hosts scheduler-gc-broken-service-interval-ms]}]
  (let [service->raw-data-fn (fn scheduler-broken-services-gc-service->raw-data-fn []
                               (let [{:keys [all-available-service-ids service-id->failed-instances
                                             service-id->healthy-instances]} (query-state-fn)]
                                 (pc/map-from-keys
                                   (fn compute-broken-services-raw-data [service-id]
                                     (let [failed-instances (get service-id->failed-instances service-id)
                                           healthy-instances (get service-id->healthy-instances service-id)]
                                       {"failed-instance-hosts" (set (map :host failed-instances))
                                        "has-healthy-instances" (not (empty? healthy-instances))
                                        "has-failed-instances" (not (empty? failed-instances))}))
                                   all-available-service-ids)))
        service->state-fn (fn [_ {:strs [failed-hosts-limit-reached]} {:strs [failed-instance-hosts has-healthy-instances] :as data}]
                            (if (and (not has-healthy-instances)
                                     (or failed-hosts-limit-reached (>= (count failed-instance-hosts) broken-service-min-hosts)))
                              (-> data (dissoc "failed-instance-hosts") (assoc "failed-hosts-limit-reached" true))
                              data))
        gc-service?-fn (fn [service-id {:keys [last-modified-time state]} current-time]
                         (and (false? (get state "has-healthy-instances" false))
                              (true? (get state "has-failed-instances" false))
                              (get state "failed-hosts-limit-reached" false)
                              (let [{:strs [grace-period-secs]} (service-id->service-description-fn service-id)
                                    ;; include the grace period to allow enough time to elapse for replacement instance to become healthy
                                    gc-time (-> last-modified-time
                                              (t/plus (t/seconds grace-period-secs))
                                              (t/plus (t/minutes broken-service-timeout-mins)))]
                                (t/after? current-time gc-time))))
        perform-gc-fn (fn [service-id]
                        (log/info "deleting broken service" service-id)
                        (try
                          (delete-service scheduler service-id)
                          (catch Exception e
                            (log/error e "unable to delete broken service" service-id))))]
    (log/info "starting scheduler-broken-services-gc")
    (service-gc-go-routine
      "scheduler-broken-services-gc"
      service->raw-data-fn
      scheduler-gc-broken-service-interval-ms
      scheduler-gc-sanitize-state
      service->state-fn
      gc-service?-fn
      perform-gc-fn)))

(defn- request-available-waiter-services
  "Queries the scheduler and builds a list of available Waiter services."
  [scheduler-name get-service->instances-fn]
  (when-let [service->service-instances (timers/start-stop-time!
                                          (metrics/waiter-timer "scheduler" scheduler-name "get-services")
                                          (retry-on-transient-server-exceptions
                                            "request-available-waiter-services"
                                            (get-service->instances-fn)))]
    (log/trace "request-available-waiter-services:services" (keys service->service-instances))
    service->service-instances))

(defn- retrieve-instances-for-service
  "Queries the scheduler and builds a list of healthy and unhealthy instances for the specified service-id."
  [service-id service-instances]
  (when service-instances
    (let [{healthy-instances true, unhealthy-instances false} (group-by (comp boolean :healthy?) service-instances)]
      (log/trace "retrieve-instances-for-service" service-id "has" (count healthy-instances) "healthy instance(s)"
                 "and" (count unhealthy-instances) " unhealthy instance(s).")
      {:healthy-instances (mapv #(utils/assoc-if-absent % :status "Healthy") healthy-instances)
       :unhealthy-instances (mapv #(utils/assoc-if-absent % :status "Unhealthy") unhealthy-instances)})))

(defn start-health-checks
  "Takes a map from service -> service instances and replaces each active instance with a ref which performs a
   health check if necessary, or returns the instance immediately."
  [scheduler-name service->service-instances available? service-id->service-description-fn]
  (let [num-unhealthy-instances (reduce
                                  (fn [accum-count {:keys [active-instances]}]
                                    (->> active-instances
                                         (filter #(not (:healthy? %)))
                                         count
                                         (+ accum-count)))
                                  0
                                  (vals service->service-instances))]
    (log/info scheduler-name "scheduler has" num-unhealthy-instances "unhealthy instance(s)")
    (metrics/reset-counter
      (metrics/waiter-counter "scheduler" scheduler-name "health-checks" "unhealthy-instances")
      num-unhealthy-instances))
  (loop [[[service {:keys [active-instances] :as instances}] & rest] (seq service->service-instances)
         service->service-instances' {}]
    (if-not service
      service->service-instances'
      (let [service-description (some-> service :id service-id->service-description-fn)
            connection-errors #{:connect-exception :hangup-exception :timeout-exception :ssl-exception}
            update-unhealthy-instance (fn [instance status error]
                                        (-> instance
                                            (assoc :healthy? false
                                                   :health-check-status status)
                                            (update :flags
                                                    (fn [flags]
                                                      (cond-> flags
                                                        (= error :hangup-exception)
                                                        (conj :hangup-exception)

                                                        (= error :ssl-exception)
                                                        (conj :ssl-exception)

                                                        (= error :timeout-exception)
                                                        (conj :timeout-exception)

                                                        (and (not= error :unknown-authority)
                                                             (not= error :ssl-exception)
                                                             (not= error :connect-exception))
                                                        (conj :has-connected)

                                                        (and (not= error :unknown-authority)
                                                             (not (contains? connection-errors error)))
                                                        (conj :has-responded))))))
            health-check-refs (map (fn [instance]
                                     (let [chan (async/promise-chan)]
                                       (if (:healthy? instance)
                                         (async/put! chan instance)
                                         (async/pipeline
                                           1 chan (map (fn [{:keys [healthy? status error]}]
                                                         (if healthy?
                                                           (assoc instance :healthy? true)
                                                           (update-unhealthy-instance instance status error))))
                                           (available? scheduler-name instance service-description)))
                                       chan))
                                   active-instances)]
        (recur rest (assoc service->service-instances' service
                                                       (assoc instances :active-instances health-check-refs)))))))

(defn do-health-checks
  "Takes a map from service -> service instances and performs health checks in parallel. Returns a map of service -> service instances."
  [scheduler-name service->service-instances available? service-id->service-description-fn]
  (let [service->service-instance-futures
        (start-health-checks scheduler-name service->service-instances available? service-id->service-description-fn)]
    (loop [[[service {:keys [active-instances] :as instances}] & rest] (seq service->service-instance-futures)
           service->service-instances' {}]
      (if-not service
        service->service-instances'
        (let [active-instances (doall (map async/<!! active-instances))]
          (recur rest (assoc service->service-instances'
                        service
                        (assoc instances :active-instances active-instances))))))))

;;
;; Support for tracking killed instance candidates
;;
(let [kill-candidate-id->state-atom (atom {})]

  (defn- cleanup-kill-candidate
    "Cleans up entries for instance-id from the state atom."
    [instance-id kill-identifier]
    (swap! kill-candidate-id->state-atom
           (fn cleanup-kill-candidate-helper [kill-candidate-id->state]
             (let [{:keys [identifier reasons]} (get kill-candidate-id->state instance-id)]
               (if (= kill-identifier identifier)
                 (do
                   (log/info "end tracking instance as kill candidate"
                             {:id instance-id :kill-identifier identifier :reasons reasons})
                   (dissoc kill-candidate-id->state instance-id))
                 (do
                   (log/info "ignoring kill candidate cleanup as identifier has changed"
                             {:id instance-id :cleanup-identifier kill-identifier :current-identifier identifier})
                   kill-candidate-id->state))))))

  (defn track-kill-candidate!
    "Tracks which instances are being used as kill candidates for the provided duration.
     When reason is :prepare-to-kill, attaches a unique identifier to the state.
     This allows multiple invocations with the same instance-id where only the last call triggers state cleanup."
    [instance-id reason duration-ms]
    (let [kill-identifier (utils/unique-identifier)]
      (log/info "tracking instance as kill candidate"
                (cond-> {:id instance-id :reason reason}
                  (= :prepare-to-kill reason) (assoc :identifier kill-identifier)))
      (swap! kill-candidate-id->state-atom update instance-id
             (fn update-kill-candidate-state [candidate-state]
               (cond-> (update candidate-state :reasons (fnil conj #{}) reason)
                 (= :prepare-to-kill reason) (assoc :identifier kill-identifier))))
      (when (and (= :prepare-to-kill reason)
                 (pos? duration-ms))
        (async/go
          (async/<! (async/timeout duration-ms))
          (cleanup-kill-candidate instance-id kill-identifier)))))

  (defn is-kill-candidate?
    "Returns true if the queried instance is currently being tracked as a kill candidate."
    [instance-id]
    (contains? @kill-candidate-id->state-atom instance-id)))

(defn- update-scheduler-state
  "Queries given scheduler, sends data on service and instance statuses to router state maintainer, and returns scheduler state"
  [scheduler-name get-service->instances-fn service-id->service-description-fn available? failed-check-threshold service-id->health-check-context]
  (let [^DateTime request-services-time (t/now)
        timing-message-fn (fn [] (let [^DateTime now (t/now)]
                                   (str "scheduler-syncer: " scheduler-name " sync took " (- (.getMillis now) (.getMillis request-services-time)) " ms")))]
    (log/trace "scheduler-syncer: querying" scheduler-name "scheduler")
    (if-let [service->service-instances (timers/start-stop-time!
                                          (metrics/waiter-timer "scheduler" scheduler-name "service->available-tasks")
                                          (do-health-checks
                                            scheduler-name
                                            (request-available-waiter-services scheduler-name get-service->instances-fn)
                                            available?
                                            service-id->service-description-fn))]
      (let [available-services (keys service->service-instances)
            available-service-ids (->> available-services (map :id) (set))]
        (log/debug "scheduler-syncer:" scheduler-name "has" (count service->service-instances) "available services:" available-service-ids)
        (doseq [service available-services]
          (when (->> (select-keys (:task-stats service) [:staged :running :healthy :unhealthy])
                     vals
                     (filter number?)
                     (reduce + 0)
                     zero?)
            (log/info "scheduler-syncer:" (:id service) "in" scheduler-name "has no live instances!" (:task-stats service))))
        (loop [service-id->health-check-context' {}
               healthy-service-ids #{}
               scheduler-messages []
               [[{service-id :id :as service} {:keys [active-instances failed-instances]}] & remaining] (seq service->service-instances)]
          (if service-id
            (let [request-instances-time (t/now)
                  active-instance-ids (->> active-instances (map :id) set)
                  {:keys [deployment-error instances task-count]} service
                  {:keys [instance-id->unhealthy-instance instance-id->tracked-failed-instance instance-id->failed-health-check-count]}
                  (get service-id->health-check-context service-id)
                  {:keys [healthy-instances unhealthy-instances] :as service-instance-info}
                  (retrieve-instances-for-service service-id active-instances)
                  ;; track instances that have gone missing with state updates as failed instances
                  instance-id->tracked-failed-instance'
                  ((fnil into {}) instance-id->tracked-failed-instance
                   (keep (fn [[instance-id unhealthy-instance]]
                           (when (and (not (contains? active-instance-ids instance-id))
                                      (>= (or (get instance-id->failed-health-check-count instance-id) 0) failed-check-threshold))
                             ;; treat any pod that went missing and was a kill candidate as not a failed instance
                             (if-not (is-kill-candidate? instance-id)
                               (do
                                 (log/info "tracking missing instance as a failed instance" {:instance-id instance-id})
                                 [instance-id (update-in unhealthy-instance [:flags] conj :never-passed-health-checks)])
                               (do
                                 (log/info "not tracking missing instance as a failed instance" {:instance-id instance-id})
                                 nil))))
                         instance-id->unhealthy-instance))
                  all-failed-instances
                  (-> (fn [failed-instance tracked-instance]
                        (-> failed-instance
                            (update-in [:flags] into (:flags tracked-instance))
                            (assoc :health-check-status (:health-check-status tracked-instance))))
                      (merge-with (pc/map-from-vals :id failed-instances) instance-id->tracked-failed-instance')
                      vals)
                  scheduler-messages' (if service-instance-info
                                        ; Assume nil service-instance-info means there was a failure in invoking marathon
                                        (conj scheduler-messages
                                              [:update-service-instances
                                               (assoc service-instance-info
                                                 :deployment-error deployment-error
                                                 :failed-instances all-failed-instances
                                                 :instance-counts {:healthy (count healthy-instances)
                                                                   :requested instances
                                                                   :scheduled task-count}
                                                 :scheduler-sync-time request-instances-time
                                                 :service-id service-id)])
                                        scheduler-messages)
                  instance-id->unhealthy-instance' (->> unhealthy-instances
                                                        (map (fn [{:keys [id] :as instance}]
                                                               (let [flags (get-in instance-id->unhealthy-instance [id :flags])]
                                                                 (update instance :flags into flags))))
                                                        (pc/map-from-vals :id))
                  instance-id->failed-health-check-count' (pc/map-from-keys #((fnil inc 0) (get instance-id->failed-health-check-count %))
                                                                            (keys instance-id->unhealthy-instance'))]
              (metrics/reset-counter
                (metrics/service-counter service-id "instance-counts" "failed")
                (count all-failed-instances))
              (recur
                (assoc service-id->health-check-context'
                  service-id {:instance-id->unhealthy-instance instance-id->unhealthy-instance'
                              :instance-id->tracked-failed-instance instance-id->tracked-failed-instance'
                              :instance-id->failed-health-check-count instance-id->failed-health-check-count'})
                (cond-> healthy-service-ids
                  (seq healthy-instances) (conj service-id))
                scheduler-messages'
                remaining))
            (let [summary-message [:update-available-services
                                   {:available-service-ids available-service-ids
                                    :healthy-service-ids healthy-service-ids
                                    :scheduler-sync-time request-services-time}]]
              (log/info (timing-message-fn) "for" (count service->service-instances) "services.")
              {:scheduler-messages (concat [summary-message] scheduler-messages)
               :service-id->health-check-context service-id->health-check-context'}))))
      (do
        (log/info (timing-message-fn) "and found no active services")
        {:service-id->health-check-context service-id->health-check-context}))))

(defn retrieve-syncer-state
  "Retrieves the scheduler and syncer state either for the entire scheduler or when provided for a specific service."
  ([syncer-state] syncer-state)
  ([{:keys [last-update-time service-id->health-check-context]} service-id]
   (let [health-check-context (get service-id->health-check-context service-id)]
     (assoc health-check-context :last-update-time last-update-time))))

(defn scheduler-syncer-timer-chan
  "Creates and returns a core.async channel that 'chimes' at the intervals of scheduler-syncer-interval-secs seconds."
  [scheduler-syncer-interval-secs]
  (-> scheduler-syncer-interval-secs t/seconds t/in-millis au/timer-chan))

(defn start-scheduler-syncer
  "Starts loop to query underlying scheduler for the service and instance statuses,
  maintains a state consisting of one map with elements of shape:

    service-id {:instance-id->unhealthy-instance        {...}
                :instance-id->tracked-failed-instance   {...}
                :instance-id->failed-health-check-count {...}}

  and sends the data to the router state maintainer.

  get-service->instances-fn is a function that returns
  a map of scheduler/Service records -> map of scheduler/ServiceInstance records.
  The nested map has the following keys: :active-instances and :failed-instances.
  The active-instances should not be assumed to be healthy (or live).
  The failed-instances are guaranteed to be dead.\""
  [clock trigger-ch service-id->service-description-fn available? failed-check-threshold
   scheduler-name get-service->instances-fn scheduler-state-chan]
  (log/info "starting scheduler-syncer" scheduler-name)
  (let [exit-chan (async/chan 1)
        state-query-chan (async/chan 32)
        syncer-state-atom (atom {})]
    (async/go
      (try
        (loop [{:keys [service-id->health-check-context] :as current-state} @syncer-state-atom]
          (reset! syncer-state-atom current-state)
          (when-let [next-state
                     (async/alt!
                       exit-chan
                       ([message]
                         (log/warn "stopping scheduler-syncer")
                         (when (not= :exit message)
                           (throw (ex-info "Stopping scheduler-syncer" {:time (t/now), :reason message}))))

                       state-query-chan
                       ([{:keys [response-chan service-id]}]
                         (async/>! response-chan
                                   (if service-id
                                     (retrieve-syncer-state current-state service-id)
                                     (retrieve-syncer-state current-state)))
                         current-state)

                       trigger-ch
                       ([]
                         (try
                           (timers/start-stop-time!
                             (metrics/waiter-timer "scheduler" scheduler-name "syncer")
                             (let [{:keys [service-id->health-check-context scheduler-messages]}
                                   (update-scheduler-state
                                     scheduler-name get-service->instances-fn service-id->service-description-fn available?
                                     failed-check-threshold service-id->health-check-context)]
                               (when scheduler-messages
                                 (async/>! scheduler-state-chan scheduler-messages))
                               (assoc current-state
                                 :last-update-time (clock)
                                 :service-id->health-check-context service-id->health-check-context)))
                           (catch Throwable th
                             (log/error th "scheduler-syncer unable to receive updates")
                             (counters/inc! (metrics/waiter-counter "scheduler" scheduler-name "syncer" "errors"))
                             (meters/mark! (metrics/waiter-meter "scheduler" scheduler-name "syncer" "error-rate"))
                             current-state)))
                       :priority true)]
            (recur next-state)))
        (catch Exception e
          (log/error e "fatal error in scheduler-syncer")
          (System/exit 1))))
    {:exit-chan exit-chan
     :query-chan state-query-chan
     :retrieve-syncer-state-fn (fn retrieve-syncer-state-fn
                                 ([] (retrieve-syncer-state @syncer-state-atom))
                                 ([service-id] (retrieve-syncer-state @syncer-state-atom service-id)))}))

;;
;; Support for tracking killed and failed instances
;;

(defn add-instance-to-buffered-collection!
  "Helper function to add/remove entries into the transient store"
  [transient-store max-instances-to-keep service-id instance-entry initial-value-fn remove-fn]
  (swap! transient-store
         (fn [service-id->failed-instances]
           (update-in service-id->failed-instances [service-id]
                      #(cond-> (or % (initial-value-fn))
                         (= max-instances-to-keep (count %)) (remove-fn)
                         true (conj instance-entry))))))

(defn add-to-store-and-track-failed-instance!
  [transient-store max-instances-to-keep service-id instance]
  (log-service-instance instance :fail :info)
  (add-instance-to-buffered-collection! transient-store max-instances-to-keep service-id instance
                                        (fn [] #{}) (fn [instances] (-> (sort-instances instances) (rest) (set)))))

(defn environment
  "Returns a new environment variable map with some basic variables added in"
  [service-id {:strs [concurrency-level cpus env mem metric-group run-as-user]} service-id->password-fn home-path]
  (let [waiter-token (get env "WAITER_CONFIG_TOKEN")]
    (cond-> (-> env
              (dissoc "WAITER_CONFIG_TOKEN")
              (assoc "HOME" home-path
                     "LOGNAME" run-as-user
                     "USER" run-as-user
                     "WAITER_CLUSTER" (str (config/retrieve-cluster-name))
                     "WAITER_CONCURRENCY_LEVEL" (str concurrency-level)
                     "WAITER_CPUS" (str cpus)
                     "WAITER_MEM_MB" (str mem)
                     "WAITER_PASSWORD" (service-id->password-fn service-id)
                     "WAITER_SERVICE_ID" service-id
                     "WAITER_USERNAME" "waiter"))
      metric-group (assoc "WAITER_METRIC_GROUP" (str metric-group))
      waiter-token (assoc "WAITER_TOKEN" waiter-token))))

;; Support for tracking service instance launching time stats

(def instance-counts-zero {:healthy 0 :requested 0 :scheduled 0})

(defn- make-launch-tracker
  ([service-id service-id->service-description-fn]
   (make-launch-tracker service-id service-id->service-description-fn instance-counts-zero #{}))
  ([service-id service-id->service-description-fn instance-counts known-instance-ids]
   {:instance-counts instance-counts
    :instance-scheduling-start-times []
    :known-instance-ids known-instance-ids
    :metric-group (-> service-id service-id->service-description-fn (get "metric-group"))
    :service-schedule-timer (metrics/service-timer service-id "launch-overhead" "schedule-time")
    :service-startup-timer (metrics/service-timer service-id "launch-overhead" "startup-time")
    :starting-instance-id->start-timestamp {}}))

(defn- track-scheduling-instances
  "Compute updated state for not-yet-scheduled instances of this service.
   This is a private helper for update-launch-trackers, doing three things:
   1) Record timestamps for newly-requested instances that are being scheduled.
   2) Match (and remove) timestamps with now-scheduled (new) instances.
   3) Drop timestamps for cancelled instance requests (due to scaling)."
  [current-time instance-counts instance-counts' instance-scheduling-start-times
   new-instance-ids removed-instance-ids service-id]
  (let [;; 1) Record timestamps for newly-requested instances that are being scheduled.
        instances-requested-delta (+ (count removed-instance-ids)
                                     (- (:requested instance-counts')
                                        (:requested instance-counts)))
        all-start-times (into instance-scheduling-start-times
                              (repeat instances-requested-delta current-time))
        ;; 2) Match (and remove) timestamps with now-scheduled (new) instances.
        [matched-start-times unmatched-start-times]
        (split-at (count new-instance-ids) all-start-times)
        ;; 3) Drop timestamps for cancelled instance requests (due to scaling).
        ;; We choose to drop the newest timestamps since the older requests
        ;; are more likely already in the process of scheduling.
        instance-scheduling-start-times'
        (cond->> unmatched-start-times
          (neg? instances-requested-delta)
          (drop-last (- instances-requested-delta)))
        ;; Check for new instances without a corresponding scheduling-start-time
        unmatched-instance-count (- (count new-instance-ids)
                                    (count all-start-times))]
    ;; Warn if too few timestamps were dropped for downward-scaling
    (when (neg? (+ (count unmatched-start-times)
                   instances-requested-delta))
      (log/warn "Tracker data out of sync during downward-scaling of" service-id
                "Removing" instances-requested-delta
                "scheduling instances, but only found"
                (count unmatched-start-times)))
    ;; Warn about any new instances for which we couldn't track the scheduling time
    (when (pos? unmatched-instance-count)
      (log/warn unmatched-instance-count
                "untracked scheduled instances discovered in instance launch metric loop for service"
                service-id))
    {:new-instance-start-times matched-start-times
     :outstanding-instance-start-times instance-scheduling-start-times'}))

(defn- track-starting-instances
  "Compute updated state for not-yet-started instances of this service.
   This is a private helper for update-launch-trackers, doing three things:
   1) Drop timestamps for killed instances.
   2) Record timestamps for newly-started instances that are not yet healthy.
   3) Match (and remove) timestamps with now-running (healthy) instances."
  [current-time healthy-instances new-instance-ids
   removed-instance-ids starting-instance-id->start-timestamp]
  (let [healthy-instance-id? (->> healthy-instances (map :id) set)
        ;; 1) Drop timestamps for killed instances.
        ;; 2) Record timestamps for newly-started instances that are not yet healthy.
        instance-id->start-timestamp
        (merge (reduce dissoc starting-instance-id->start-timestamp removed-instance-ids)
               (pc/map-from-keys (constantly current-time) new-instance-ids))
        ;; 3) Match (and remove) timestamps with now-running (healthy) instances.
        ;; i.e., partition starting (not yet healthy) and started (now healthy) instances
        {started-instance-ids :started starting-instances-ids :starting}
        (group-by #(if (healthy-instance-id? %) :started :starting)
                  (keys instance-id->start-timestamp))]
    {:outstanding-instance-id->start-timestamp
     (select-keys instance-id->start-timestamp starting-instances-ids)
     :started-instance-times
     (mapv instance-id->start-timestamp started-instance-ids)}))

(defn update-launch-trackers
  "Track metrics on launching service instances, specifically:

     1) the time from requesting a new instance until it is scheduled (the \"schedule\" time), and
     2) the time from scheduling a new instance until it becomes healthy (the \"startup\" time).

   Reports the updated metrics to the configured metrics and statsd services,
   and returns the updated service-id->launch-tracker mapping."
  [service-id->launch-tracker new-service-ids removed-service-ids service-id->healthy-instances
   service-id->unhealthy-instances service-id->instance-counts
   service-id->service-description-fn leader? waiter-schedule-timer]
  (let [;; Use a consistent timestamp for all trackers updated in this function call
        current-time (t/now)
        ;; Remove deleted services, and add newly-discovered services
        live-service-id->launch-tracker
        (merge (reduce dissoc service-id->launch-tracker removed-service-ids)
               (pc/map-from-keys #(make-launch-tracker % service-id->service-description-fn)
                                 new-service-ids))]
    ;; Update all trackers for live services...
    ;;
    ;; See the definition of make-launch-tracker above for the full list of tracker-map fields.
    ;; The tracker-map fields that change during this update are the following:
    ;;
    ;;   :instance-counts
    ;;   Cached value of service-id->instance-counts from the last time this tracker was updated.
    ;;   We currently only use the :requested field (the total number of requested/desired instances).
    ;;
    ;;   :instance-scheduling-start-times
    ;;   Vector of DateTime objects, sorted oldest to newest.
    ;;   These are the times when we assume a new instance request was sent to the scheduler.
    ;;
    ;;   :known-instance-ids
    ;;   Vector of all Waiter instance-ids of this service that were live during the last tracker update.
    ;;   We use this value to determine when a new "unknown" instance-id appears.
    ;;
    ;;   :starting-instance-id->start-timestamp
    ;;   Mapping of known instance-ids onto the DateTime when we first observed that instance-id
    ;;   (i.e., the approximate time that the instance was successfully scheduled).
    ;;   Once an instance is observed healthy, it is removed from this mapping,
    ;;   leaving only the instances that are still in the "starting" phase.
    ;;
    (pc/for-map [[service-id
                  {:keys [instance-counts instance-scheduling-start-times known-instance-ids
                          metric-group service-schedule-timer service-startup-timer
                          starting-instance-id->start-timestamp] :as tracker-state}]
                 live-service-id->launch-tracker]
      service-id
      (let [healthy-instances (get service-id->healthy-instances service-id)
            healthy-instances? (set healthy-instances)
            instance-counts' (get service-id->instance-counts service-id instance-counts-zero)
            known-instances' (->> service-id
                                  (get service-id->unhealthy-instances)
                                  (concat healthy-instances))
            known-instance-ids' (->> known-instances' (map :id) set)
            previously-known-instance? (comp known-instance-ids :id)
            new-instances (->> known-instances'
                               (remove previously-known-instance?)
                               (sort instance-comparator))
            new-instance-ids (mapv :id new-instances)
            removed-instance-ids (set/difference known-instance-ids known-instance-ids')
            ;; Compute updated state for not-yet-scheduled instances of this service
            {:keys [new-instance-start-times outstanding-instance-start-times]}
            (track-scheduling-instances
              current-time instance-counts instance-counts'
              instance-scheduling-start-times new-instance-ids removed-instance-ids service-id)
            ;; Compute updated state for not-yet-started instances of this service
            {:keys [outstanding-instance-id->start-timestamp started-instance-times]}
            (track-starting-instances
              current-time healthy-instances new-instance-ids
              removed-instance-ids starting-instance-id->start-timestamp)]
        ;; Report schedule time for now-known instances
        (doseq [start-scheduling-time new-instance-start-times]
          (let [duration (metrics/duration-between start-scheduling-time current-time)]
            (metrics/report-duration waiter-schedule-timer duration)
            (metrics/report-duration service-schedule-timer duration)
            (when leader?
              (statsd/histo! metric-group "schedule_time"
                             (du/interval->nanoseconds duration)))))
        ;; Report startup time for now-healthy instances
        (doseq [start-running-time started-instance-times]
          (let [duration (metrics/duration-between start-running-time current-time)]
            (metrics/report-duration service-startup-timer duration)
            (when leader?
              (statsd/histo! metric-group "startup_time"
                             (du/interval->nanoseconds duration)))))
        (doseq [new-instance new-instances]
          (when (contains? healthy-instances? new-instance)
            (log-service-instance new-instance :launch :info)))
        ;; tracker-state' for this service
        (assoc tracker-state
          :instance-counts instance-counts'
          :instance-scheduling-start-times outstanding-instance-start-times
          :known-instance-ids known-instance-ids'
          :starting-instance-id->start-timestamp outstanding-instance-id->start-timestamp)))))

(defn build-initial-service-launch-trackers
  "Build the initially-observed state for the launch-metrics-maintainer's state loop
   from the first state observed on the router-state-updates channel."
  [{:keys [iteration service-id->healthy-instances service-id->instance-counts
           service-id->unhealthy-instances] :as initial-router-state}
   service-id->service-description-fn]
  (let [initial-service-ids (-> service-id->instance-counts keys set)
        initial-service-id->launch-tracker
        (pc/for-map [service-id initial-service-ids]
          service-id
          (let [instance-counts (get service-id->instance-counts service-id)
                known-instance-ids
                (set (for [service-id->instances [service-id->healthy-instances
                                                  service-id->unhealthy-instances]
                           instance (get service-id->instances service-id)]
                       (:id instance)))]
            (make-launch-tracker service-id
                                 service-id->service-description-fn
                                 instance-counts
                                 known-instance-ids)))]
    (log/info "Starting launch metrics loop on iteration" iteration
              "with services:" initial-service-ids)
    {:known-service-ids initial-service-ids
     :previous-iteration iteration
     :service-id->launch-tracker initial-service-id->launch-tracker}))

(defn start-launch-metrics-maintainer
  "go block to collect metrics on the instance launch overhead of waiter services.

  Updates to state for the router are then read from `router-state-updates-chan`."
  [router-state-updates-chan leader?-fn service-id->service-description-fn]
  (let [exit-chan (async/chan 1)
        query-chan (au/latest-chan)
        update-state-timer (metrics/waiter-timer "state" "launch-metrics-maintainer" "update-state")
        waiter-schedule-timer (metrics/waiter-timer "launch-overhead" "schedule-time")]
    (async/go
      (try
        (loop [{:keys [known-service-ids previous-iteration service-id->launch-tracker] :as current-state}
               ;; NOTE: the iteration initial state read from router-state-query-chan is NOT guaranteed
               ;; as <= the iteration of states later read from router-state-updates-chan.
               ;; This is because the router-state-maintainer puts! to the channels asynchronously.
               ;; The if-not < iteration check below to addresses this problem.
               {:known-service-ids #{}
                :previous-iteration nil
                :service-id->launch-tracker nil}]
          (let [new-state
                (async/alt!
                  exit-chan
                  ([message]
                    (if (= :exit message)
                      (do
                        (log/warn "stopping launch-metrics-maintainer")
                        (comment "Return nil to exit the loop"))
                      current-state))

                  router-state-updates-chan
                  ([router-state]
                    (cond
                      (nil? service-id->launch-tracker)
                      (build-initial-service-launch-trackers router-state service-id->service-description-fn)

                      (< previous-iteration (:iteration router-state))
                      (timers/start-stop-time!
                        update-state-timer
                        (let [leader? (leader?-fn)
                              {:keys [iteration service-id->healthy-instances
                                      service-id->instance-counts service-id->unhealthy-instances]} router-state
                              incoming-service-ids (set (keys service-id->instance-counts))
                              new-service-ids (set/difference incoming-service-ids known-service-ids)
                              removed-service-ids (set/difference known-service-ids incoming-service-ids)
                              service-id->launch-tracker' (update-launch-trackers
                                                            service-id->launch-tracker new-service-ids removed-service-ids
                                                            service-id->healthy-instances service-id->unhealthy-instances
                                                            service-id->instance-counts service-id->service-description-fn
                                                            leader? waiter-schedule-timer)]
                          {:known-service-ids incoming-service-ids
                           :previous-iteration iteration
                           :service-id->launch-tracker service-id->launch-tracker'}))

                      :else ;; ignoring out-of-order state updates
                      current-state))

                  query-chan
                  ([{:keys [cid response-chan]}]
                    (cid/cinfo cid (str "returning current launch state " current-state))
                    (let [sanitized-state
                          (update-in
                            current-state [:service-id->launch-tracker]
                            (partial
                              pc/map-vals
                              #(dissoc % :service-schedule-timer :service-startup-timer)))]
                      (async/put! response-chan sanitized-state))
                    current-state)
                  :priority true)]
            (when new-state
              (recur new-state))))
        (catch Exception e
          (log/error e "Error in launch-metrics-maintainer. Instance launch metrics will not be collected."))))
    {:exit-chan exit-chan
     :query-chan query-chan}))

(defn create-service-id->last-request-time-from-metrics
  "Using the 'service-id->metrics-fn' which provides a map of service-id->metrics, this function creates a new map
  service-id->last-request-time."
  [service-id->metrics-fn]
  (->> (service-id->metrics-fn)
       (pc/map-vals
         (fn [{:strs [last-request-time]}]
           last-request-time))
       (utils/filterm
         (fn [[_ last-request-time]]
           (some? last-request-time)))))

(defn make-service-id-new-last-request-time?-fn
  "Returns a function that returns true if the proposed 'last-request-time' is after the last request time of value in the
  provided 'service-id->last-request-time' for the 'service-id'. If the last request time in 'service-id->last-request-time'
  map is nil, then the function will return true."
  [service-id->last-request-time]
  (fn service-id-new-last-request-time?
    [[service-id new-last-request-time]]
    (let [old-last-request-time (get service-id->last-request-time service-id)]
      (or (nil? old-last-request-time)
          (t/after? new-last-request-time old-last-request-time)))))

(defn make-service-id-received-request-after-becoming-stale?-fn
  "Returns a function that returns true if the provided 'last-request-time' associated with the 'service-id' is after
  that service became stale based on its entry in the 'service-id->stale-info' map returned by the provided 'get-service-id->stale-info'.
  This is used to determine whether a service-id is stale? and received requests after becoming stale."
  [get-service-id->stale-info]
  (fn service-id-received-request-after-becoming-stale?
    [[service-id new-last-request-time]]
    (let [{:keys [stale? update-time]} (get-service-id->stale-info service-id)]
      (and stale?
           ; assume that update-time is before last-request-time when update-time is nil to avoid missing
           ; an event to start new service.
           (or (nil? update-time)
               (t/before? update-time new-last-request-time)
               (t/equal? update-time new-last-request-time))))))

(defn make-one-source-token?-fn
  "Returns a function that returns true if there is only 1 unique source token."
  [correlation-id]
  (fn one-source-token?
    [{:keys [service-id source-tokens]}]
    (let [many-source-tokens? (> 1 (count source-tokens))]
      (when many-source-tokens?
        (cid/cinfo correlation-id "skipping service-id because it maps to many source tokens."
                   {:service-id service-id
                    :source-tokens source-tokens}))
      (= 1 (count source-tokens)))))

(defn make-token-is-not-run-as-requester-or-parameterized?-fn
  "Returns a function that returns true if the token is not run-as-requester or parameterized."
  [correlation-id kv-store]
  (fn token-is-not-run-as-requester-or-parameterized?
    [token]
    (when-let
      [{:strs [run-as-user] :as service-description-template}
       (sd/token->service-parameter-template kv-store token :error-on-missing false)]
      (let [supported-token?
            (and
              ; run-as-user defaults to '*', so confirm it is defined
              (some? run-as-user)
              (not (sd/run-as-requester? service-description-template))
              (not (sd/requires-parameters? service-description-template)))]
        (when-not supported-token?
          (cid/cinfo correlation-id "skipping token because it is either run-as-requester or parameterized."
                     {:token token}))
        supported-token?))))

(defn- get-tokens-to-start-services-for
  "Get lazy sequence of tokens that need a service to be started for using the service-id->last-request-time maps to determine
  which services are active."
  [correlation-id service-id->stale-info service-id->source-tokens-entries-fn kv-store service-id->last-request-time
   new-service-id->last-request-time]
  (->> (seq new-service-id->last-request-time)
       (filter (make-service-id-new-last-request-time?-fn service-id->last-request-time))
       (filter (make-service-id-received-request-after-becoming-stale?-fn service-id->stale-info))
       (map
         (fn get-source-tokens-from-service-id
           [[service-id _]]
           {:service-id service-id
            :source-tokens (->> service-id
                                service-id->source-tokens-entries-fn
                                vec
                                flatten
                                (map #(get % "token"))
                                ; There may be many (token, version) pairs but only 1 unique token, so
                                ; make a set with just the token values instead.
                                set)}))
       (filter
         ; TODO: support the legacy token mapping use case by using individual token references.
         ; For now, ignore the services that map to many source tokens (legacy mapping) and
         ; log them. Services with many:many token mapping have an edge case because
         ; the 'service-id->stale-info' function does not consider it stale unless
         ; ALL references to tokens are stale. This means that a service could be stale
         ; for one token and not stale for another token, resulting in no new service
         ; starting for the former token.
         (make-one-source-token?-fn correlation-id))
       (map
         (fn get-single-source-token [{:keys [source-tokens]}]
           ; Because we previously filtered out services that did not have exactly 1 source token,
           ; we can just pick the first one.
           (first source-tokens)))
       set
       (filter
         ; TODO: support run-as-requester and parameterized services by keeping track of original service-id and getting
         ; the run-as-user and parameters from the description. These tokens are filtered out for now.
         (make-token-is-not-run-as-requester-or-parameterized?-fn correlation-id kv-store))))

(defn start-latest-services-for-tokens
  "Given a list of tokens, try to start the latest service for the token. If an individual token errors, we skip that token
  and continue to the next. Returns the error results."
  [correlation-id kv-store fallback-state start-new-service-fn retrieve-descriptor-fn tokens]
  (let [error-results (->> tokens
                           (map
                             (fn start-latest-service
                               [token]
                               (try
                                 (let [{:strs [run-as-user]} (sd/token->service-parameter-template kv-store token :error-on-missing false)
                                       ; we use retrieve-descriptor-fn instead of retrieve-latest-descriptor-fn because
                                       ; it has a side effect for updating the service-id and source token references
                                       {{:keys [service-id] :as latest-descriptor} :latest-descriptor} (retrieve-descriptor-fn run-as-user token)
                                       should-start-service? (not (descriptor/service-exists? fallback-state service-id))]
                                   (when should-start-service?
                                     (cid/cinfo correlation-id "starting service due to new last-request-time"
                                                {:service-id service-id
                                                 :token token})
                                     (start-new-service-fn latest-descriptor)
                                     (meters/mark! (metrics/waiter-meter "core" "start-new-services-maintainer" "start-new-service")))
                                   {:token token :success true})
                                 (catch Exception e
                                   {:token token
                                    :success false
                                    :error e}))))
                           (filter
                             (fn result-errored?
                               [{:keys [success]}]
                               (not success)))
                           doall)]
    (when (> (count error-results) 0)
      (cid/cerror correlation-id "Failed to start latest service for tokens" {:tokens (map :token error-results)
                                                                              :sample-10-errors (take 10 error-results)}))
    error-results))

(defn start-new-services-maintainer
  "This daemon ensures that tokens receiving requests will have the latest service corresponding to it running. Whenever there
   are any implicit changes to the token's service description resulting in a new service-id while that token is handling
   requests. This daemon will start the new service corresponding to that service-id. We do this by building a map,
   service-id->last-request-time, whenever the 'trigger-ch' is triggered, and seeing if any service-ids have a new
   last-request-time. We then build a set of corresponding source tokens to the service-ids and see if they point to a
   service-id that is not running. We attempt to start a new service for those tokens/service-ids (not including
   run-as-requester or parameterized services).

   Returns a map with keys:
   :exit-chan when a message is put on the exit-chan, the daemon terminates
   :go-chan the corresponding async/go chan of the daemon. This can be used to see if the daemon has terminated.
   :query-chan channel that puts the state in a response-chan after the daemon processes other channels first (e.g trigger-ch)
   :query-state-fn function that queries the current state of the daemon"
  [clock trigger-ch service-id->source-tokens-entries-fn service-id->metrics-fn service-id->stale-info start-new-service-fn
   retrieve-descriptor-fn fallback-state-atom kv-store]
  (cid/with-correlation-id
    "start-new-services-maintainer"
    (let [correlation-id (cid/get-correlation-id)
          exit-chan (async/promise-chan)
          query-chan (async/chan)
          state-atom (atom {:last-update-time nil
                            :service-id->last-request-time {}})
          query-state-fn (fn query-state-fn [_]
                           (assoc @state-atom :supported-include-params []))
          go-chan
          (async/go
            (try
              (loop [{:keys [service-id->last-request-time] :as current-state} @state-atom]
                (reset! state-atom current-state)
                (let [[msg current-chan]
                      (async/alts! [exit-chan trigger-ch query-chan] :priority true)
                      next-state
                      (condp = current-chan
                        exit-chan
                        (do
                          (log/warn "stopping start-new-services-maintainer")
                          (when (not= :exit msg)
                            (throw (ex-info "Stopping start-new-services-maintainer" {:time (clock) :reason msg}))))

                        trigger-ch
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "start-new-services-maintainer" "process-services")
                          (try
                            (let [new-service-id->last-request-time (create-service-id->last-request-time-from-metrics service-id->metrics-fn)
                                  tokens (get-tokens-to-start-services-for
                                           correlation-id service-id->stale-info service-id->source-tokens-entries-fn kv-store
                                           service-id->last-request-time new-service-id->last-request-time)]
                              (start-latest-services-for-tokens
                                correlation-id kv-store @fallback-state-atom start-new-service-fn retrieve-descriptor-fn tokens)
                              (counters/inc! (metrics/waiter-counter "core" "start-new-services-maintainer" "refresh-sync"))
                              {:service-id->last-request-time new-service-id->last-request-time})
                            (catch Exception e
                              (log/error e "error while processing services in start-new-services-maintainer.")
                              current-state)))
                        query-chan
                        (let [{:keys [response-chan include-flags]} msg]
                          (async/put! response-chan (query-state-fn include-flags))
                          current-state))]
                  (if next-state
                    (recur (assoc next-state :last-update-time (clock)))
                    (log/info "Stopping start-new-services-maintainer as next loop values are nil"))))
              (catch Exception e
                (log/error e "Fatal error in start-new-services-maintainer")
                (System/exit 1))))]
      {:exit-chan exit-chan
       :go-chan go-chan
       :query-chan query-chan
       :query-state-fn query-state-fn})))
