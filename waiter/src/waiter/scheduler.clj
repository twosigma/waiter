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
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.statsd :as statsd]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (java.io EOFException)
           (java.net ConnectException SocketTimeoutException)
           (java.util.concurrent TimeoutException)
           (org.joda.time DateTime)))

(defmacro log
  "Log Scheduler-specific messages."
  [& args]
  `(log/log "Scheduler" :debug nil (print-str ~@args)))

(defrecord Service
  [^String id
   instances
   task-count
   task-stats])

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
   ^String protocol
   ^String log-directory
   ^String message])

(defn make-ServiceInstance [value-map]
  (map->ServiceInstance (merge {:extra-ports [] :flags #{}} value-map)))

(defn attach-name
  "Attaches the name metadata to the scheduler."
  [scheduler scheduler-name]
  {:pre [(not (str/blank? scheduler-name))]}
  (vary-meta scheduler assoc ::name scheduler-name))

(defn scheduler->name
  "Retrieves the scheduler name."
  [scheduler]
  (some-> scheduler meta ::name))

(defprotocol ServiceScheduler

  ;; TODO shams move this method to a different protocol
  (get-service->instances [this]
    "Returns a map of scheduler/Service records -> map of scheduler/ServiceInstance records.
     The nested map has the following keys: :active-instances and :failed-instances.
     The active-instances should not be assumed to be healthy (or live).
     The failed-instances are guaranteed to be dead.")

  (get-services [this]
    "Returns a list of scheduler/Service records")

  (kill-instance [this instance]
    "Instructs the scheduler to kill a specific ServiceInstance.
     Returns a map containing the following structure:
     {:instance-id instance-id, :killed? <boolean>, :message <string>,  :service-id service-id, :status status-code}")

  (service-exists? [this ^String service-id]
    "Returns truth-y value if the app exists and nil otherwise.")

  (create-service-if-new [this descriptor]
    "Sends a call to Scheduler to start an app with the descriptor if the app does not already exist.
     Returns truth-y value if the app creation was successful and nil otherwise.")

  (delete-service [this ^String service-id]
    "Instructs the scheduler to delete the specified service.
     Returns a map containing the following structure:
     {:message message
      :result :deleted|:error|:no-such-service-exists
      :success true|false}")

  (scale-service [this ^String service-id target-instances force]
    "Instructs the scheduler to scale up/down instances of the specified service to the specified number
     of instances. The force flag can be used enforce the scaling by ignoring previous pending operations.")

  (retrieve-directory-content [this ^String service-id ^String instance-id ^String host ^String directory]
    "Retrieves the content of the directory for the specified instance (identified by `instance-id`) on the
     specified `host`. It includes links to browse subdirectories and download files.")

  (service-id->state [this ^String service-id]
    "Retrieves the state the scheduler is maintaining for the given service-id.")

  (state [this]
    "Returns the global (i.e. non-service-specific) state the scheduler is maintaining"))

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
  `(retry-on-transient-server-exceptions-fn ~msg #{500 501 502 503 504} (fn [] ~@body)))

(defn suppress-transient-server-exceptions-fn
  "Helper function for `suppress-transient-server-exceptions`.
   Calls the body which we assume includes calls to marathon.
   If a transient marathon exception occurs this macro catches and logs it and returns nil.
   Will return the output of body otherwise.
   Will NOT catch unknown exceptions."
  [msg f]
  (ss/try+
    (f)
    (catch #(contains? #{500 501 502 503 504} (:status %)) e
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

(defn base-url
  "Returns the url at which the service definition resides."
  [{:keys [host port protocol]}]
  (str protocol "://" host ":" port))

(defn end-point-url
  "Returns the endpoint url which can be queried on the service instance."
  [service-instance ^String end-point]
  (str (base-url service-instance)
       (if (and end-point (str/starts-with? end-point "/")) end-point (str "/" end-point))))

(defn health-check-url
  "Returns the health check url which can be queried on the service instance."
  [service-instance health-check-path]
  (end-point-url service-instance health-check-path))

(defn log-health-check-issues
  "Logs messages based on the type of error (if any) encountered by a health check"
  [service-instance instance-health-check-url status error]
  (if error
    (let [error-map {:instance service-instance
                     :service instance-health-check-url}]
      (condp instance? error
        ConnectException (log/debug error "error while connecting to backend for health check" error-map)
        SocketTimeoutException (log/debug error "timeout while connecting to backend for health check" error-map)
        TimeoutException (log/debug error "timeout while connecting to backend for health check" error-map)
        Throwable (log/error error "unexpected error while connecting to backend for health check" error-map)))
    (when (not (or (<= 200 status 299)
                   (= 404 status)
                   (= 504 status)))
      (log/info "unexpected status from health check" {:status status
                                                       :instance service-instance
                                                       :service instance-health-check-url}))))

(defn available?
  "Async go block which returns the status code and success of a health check.
  Returns false if such a connection cannot be established."
  [http-client {:keys [port] :as service-instance} health-check-path]
  (async/go
    (try
      (if (pos? port)
        (let [instance-health-check-url (health-check-url service-instance health-check-path)
              {:keys [status error]} (async/<! (http/get http-client instance-health-check-url))
              error-flag (cond
                           (instance? ConnectException error) :connect-exception
                           (instance? EOFException error) :hangup-exception
                           (instance? SocketTimeoutException error) :timeout-exception
                           (instance? TimeoutException error) :timeout-exception)]
          (log-health-check-issues service-instance instance-health-check-url status error)
          {:healthy? (and (not error) (<= 200 status 299))
           :status status
           :error error-flag})
        {:healthy? false})
      (catch Exception e
        (log/error e "exception thrown while performing health check" {:instance service-instance
                                                                       :health-check-path health-check-path})
        {:healthy? false}))))

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
  "Performs scheduler GC by tracking which services are idle (i.e. have no outstanding requests).
   The function launches a go-block that tracks the metrics state of all services currently being managed by the scheduler.
   Idle services are detected based on no changes to the metrics state past the `idle-timeout-mins` period.
   They are then deleted by the leader using the `delete-service` function.
   If an error occurs while deleting a service, there will be repeated attempts to delete it later."
  [scheduler query-state-fn service-id->metrics-fn {:keys [scheduler-gc-interval-ms]} service-gc-go-routine
   service-id->idle-timeout]
  (let [service->raw-data-fn (fn scheduler-services-gc-service->raw-data-fn []
                               (let [{:keys [all-available-service-ids]} (query-state-fn)
                                     global-state (service-id->metrics-fn)]
                                 (pc/map-from-keys
                                   (fn scheduler-services-gc-service->raw-data-fn [service-id]
                                     (-> (select-keys (get global-state service-id) ["outstanding" "total"])
                                         (utils/assoc-if-absent "outstanding" 0)
                                         (utils/assoc-if-absent "total" 0)))
                                   all-available-service-ids)))
        service->state-fn (fn [_ _ data] data)
        gc-service?-fn (fn [service-id {:keys [last-modified-time state]} current-time]
                         (let [outstanding (get state "outstanding")]
                           (and (number? outstanding)
                                (zero? outstanding)
                                (let [idle-timeout-mins (service-id->idle-timeout service-id)
                                      timeout-time (t/plus last-modified-time (t/minutes idle-timeout-mins))]
                                  (log/debug service-id "timeout:" (du/date-to-str timeout-time) "current:" (du/date-to-str current-time))
                                  (t/after? current-time timeout-time)))))
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
  [service-gc-go-routine query-state-fn scheduler
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
        gc-service?-fn (fn [_ {:keys [last-modified-time state]} current-time]
                         (and (false? (get state "has-healthy-instances" false))
                              (true? (get state "has-failed-instances" false))
                              (get state "failed-hosts-limit-reached" false)
                              (let [gc-time (t/plus last-modified-time (t/minutes broken-service-timeout-mins))]
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

(defn- request-available-waiter-apps
  "Queries the scheduler and builds a list of available Waiter apps."
  [scheduler]
  (when-let [service->service-instances (timers/start-stop-time!
                                          (metrics/waiter-timer "core" "scheduler" "get-services")
                                          (retry-on-transient-server-exceptions
                                            "request-available-waiter-apps"
                                            (get-service->instances scheduler)))]
    (log/trace "request-available-waiter-apps:apps" (keys service->service-instances))
    service->service-instances))

(defn- retrieve-instances-for-app
  "Queries the scheduler and builds a list of healthy and unhealthy instances for the specified service-id."
  [service-id service-instances]
  (when service-instances
    (let [{healthy-instances true, unhealthy-instances false} (group-by (comp boolean :healthy?) service-instances)]
      (log/trace "request-instances-for-app" service-id "has" (count healthy-instances) "healthy instance(s)"
                 "and" (count unhealthy-instances) " unhealthy instance(s).")
      {:healthy-instances (vec healthy-instances)
       :unhealthy-instances (vec unhealthy-instances)})))

(defn start-health-checks
  "Takes a map from service -> service instances and replaces each active instance with a ref which performs a
   health check if necessary, or returns the instance immediately."
  [service->service-instances available? service-id->service-description-fn]
  (loop [[[service {:keys [active-instances] :as instances}] & rest] (seq service->service-instances)
         service->service-instances' {}]
    (if-not service
      service->service-instances'
      (let [{:strs [health-check-url]} (service-id->service-description-fn (:id service))
            connection-errors #{:connect-exception :hangup-exception :timeout-exception}
            update-unhealthy-instance (fn [instance status error]
                                        (-> instance
                                            (assoc :healthy? false
                                                   :health-check-status status)
                                            (update :flags
                                                    (fn [flags]
                                                      (cond-> flags
                                                              (not= error :connect-exception)
                                                              (conj :has-connected)

                                                              (not (contains? connection-errors error))
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
                                           (available? instance health-check-url)))
                                       chan))
                                   active-instances)]
        (recur rest (assoc service->service-instances' service
                                                       (assoc instances :active-instances health-check-refs)))))))

(defn do-health-checks
  "Takes a map from service -> service instances and performs health checks in parallel. Returns a map of service -> service instances."
  [service->service-instances available? service-id->service-description-fn]
  (let [service->service-instance-futures (start-health-checks service->service-instances available? service-id->service-description-fn)]
    (loop [[[service {:keys [active-instances] :as instances}] & rest] (seq service->service-instance-futures)
           service->service-instances' {}]
      (if-not service
        service->service-instances'
        (let [active-instances (doall (map async/<!! active-instances))]
          (recur rest (assoc service->service-instances'
                        service
                        (assoc instances :active-instances active-instances))))))))

(defn- update-scheduler-state
  "Queries marathon, sends data on app and instance statuses to router state maintainer, and returns scheduler state"
  [scheduler service-id->service-description-fn available? failed-check-threshold service-id->health-check-context]
  (let [^DateTime request-apps-time (t/now)
        timing-message-fn (fn [] (let [^DateTime now (t/now)]
                                   (str "scheduler-syncer: sync took " (- (.getMillis now) (.getMillis request-apps-time)) " ms")))]
    (log/trace "scheduler-syncer: querying" (scheduler->name scheduler) "scheduler")
    (if-let [service->service-instances (timers/start-stop-time!
                                          (metrics/waiter-timer "core" "scheduler" "app->available-tasks")
                                          (do-health-checks (request-available-waiter-apps scheduler)
                                                            available?
                                                            service-id->service-description-fn))]
      (let [available-services (keys service->service-instances)
            available-service-ids (->> available-services (map :id) (set))]
        (log/debug "scheduler-syncer:" (scheduler->name scheduler) "scheduler has" (count service->service-instances)
                   "available services:" available-service-ids)
        (doseq [service available-services]
          (when (->> (select-keys (:task-stats service) [:staged :running :healthy :unhealthy])
                     vals
                     (filter number?)
                     (reduce + 0)
                     zero?)
            (log/info "scheduler-syncer:" (:id service) "in" (scheduler->name scheduler) "scheduler"
                      "has no live instances!" (:task-stats service))))
        (loop [service-id->health-check-context' {}
               healthy-service-ids #{}
               scheduler-messages []
               [[{service-id :id :as service} {:keys [active-instances failed-instances]}] & remaining] (seq service->service-instances)]
          (if service-id
            (let [request-instances-time (t/now)
                  active-instance-ids (->> active-instances (map :id) set)
                  {:keys [instances task-count]} service
                  {:keys [instance-id->unhealthy-instance instance-id->tracked-failed-instance instance-id->failed-health-check-count]}
                  (get service-id->health-check-context service-id)
                  {:keys [healthy-instances unhealthy-instances] :as service-instance-info}
                  (retrieve-instances-for-app service-id active-instances)
                  instance-id->tracked-failed-instance'
                  ((fnil into {}) instance-id->tracked-failed-instance
                    (keep (fn [[instance-id unhealthy-instance]]
                            (when (and (not (contains? active-instance-ids instance-id))
                                       (>= (or (get instance-id->failed-health-check-count instance-id) 0) failed-check-threshold))
                              [instance-id (update-in unhealthy-instance [:flags] conj :never-passed-health-checks)]))
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
                                    :scheduler-sync-time request-apps-time}]]
              (log/info (timing-message-fn) "for" (count service->service-instances) "services.")
              {:scheduler-messages (concat [summary-message] scheduler-messages)
               :service-id->health-check-context service-id->health-check-context'}))))
      (do
        (log/info (timing-message-fn) "and found no active services")
        {:service-id->health-check-context service-id->health-check-context}))))

(defn retrieve-scheduler-state
  "Retrieves the scheduler and syncer state either for the entire scheduler or when provided for a specific service."
  ([syncer-state-atom]
   @syncer-state-atom)
  ([syncer-state-atom service-id]
    (let [{:keys [last-update-time service-id->health-check-context]} @syncer-state-atom
          health-check-context (get service-id->health-check-context service-id)]
      (assoc health-check-context :last-update-time last-update-time))))

(defn start-scheduler-syncer
  "Starts loop to query marathon for the app and instance statuses,
  maintains a state consisting of one map with elements of shape:

    service-id {:instance-id->unhealthy-instance        {...}
                :instance-id->tracked-failed-instance   {...}
                :instance-id->failed-health-check-count {...}}

  and sends the data to the router state maintainer."
  [clock timeout-chan service-id->service-description-fn available? failed-check-threshold
   scheduler scheduler-state-chan syncer-state-atom]
  (log/info "Starting scheduler syncer")
  (let [exit-chan (async/chan 1)
        state-query-chan (async/chan 32)]
    (async/go
      (try
        (loop [{:keys [service-id->health-check-context] :as current-state}
               @syncer-state-atom]
          (reset! syncer-state-atom current-state)
          (when-let [next-state
                     (async/alt!
                       exit-chan
                       ([message]
                         (log/warn "Stopping scheduler-syncer")
                         (when (not= :exit message)
                           (throw (ex-info "Stopping scheduler-syncer" {:time (t/now), :reason message}))))

                       state-query-chan
                       ([{:keys [response-chan service-id]}]
                         (->> (if service-id
                                (retrieve-scheduler-state syncer-state-atom service-id)
                                (retrieve-scheduler-state syncer-state-atom))
                              (async/>! response-chan))
                         current-state)

                       timeout-chan
                       ([]
                         (try
                           (timers/start-stop-time!
                             (metrics/waiter-timer "state" "scheduler-sync")
                             (let [{:keys [service-id->health-check-context scheduler-messages]}
                                   (update-scheduler-state scheduler service-id->service-description-fn available?
                                                           failed-check-threshold service-id->health-check-context)]
                               (when scheduler-messages
                                 (async/>! scheduler-state-chan scheduler-messages))
                               {:last-update-time (clock)
                                :service-id->health-check-context service-id->health-check-context}))
                           (catch Throwable th
                             (log/error th "scheduler-syncer unable to receive updates")
                             (counters/inc! (metrics/waiter-counter "state" "scheduler-sync" "errors"))
                             (meters/mark! (metrics/waiter-meter "state" "scheduler-sync" "error-rate"))
                             current-state)))
                       :priority true)]
            (recur next-state)))
        (catch Exception e
          (log/error e "fatal error in scheduler-syncer")
          (System/exit 1))))
    {:exit-chan exit-chan
     :query-chan state-query-chan}))

;;
;; Support for tracking killed instances
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

(defn environment
  "Returns a new environment variable map with some basic variables added in"
  [service-id {:strs [cpus env mem run-as-user]} service-id->password-fn home-path]
  (merge env
         {"HOME" home-path
          "LOGNAME" run-as-user
          "USER" run-as-user
          "WAITER_CPUS" (-> cpus str)
          "WAITER_MEM_MB" (-> mem str)
          "WAITER_PASSWORD" (service-id->password-fn service-id)
          "WAITER_SERVICE_ID" service-id
          "WAITER_USERNAME" "waiter"}))

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

(defn update-launch-trackers
  "Track metrics on launching service instances, specifically:
   1) the time from requesting a new instance until it is scheduled (the \"schedule\" time), and
   2) the time from scheduling a new instance until it becomes healthy (the \"startup\" time)."
  [service-id->launch-tracker new-service-ids removed-service-ids service-id->healthy-instances
   service-id->unhealthy-instances service-id->instance-counts
   service-id->service-description-fn leader? waiter-schedule-timer]
  (let [current-time (t/now)
        ;; Remove deleted services and add newly-discovered services
        service-id->launch-tracker'
        (merge (reduce dissoc service-id->launch-tracker removed-service-ids)
               (pc/map-from-keys #(make-launch-tracker % service-id->service-description-fn)
                                 new-service-ids))]
    ;; Update all trackers...
    (pc/for-map [[service-id
                  {:keys [instance-counts instance-scheduling-start-times known-instance-ids
                          metric-group service-schedule-timer service-startup-timer
                          starting-instance-id->start-timestamp] :as tracker-state}]
                 service-id->launch-tracker']
      service-id
      (let [healthy-instances (get service-id->healthy-instances service-id)
            healthy-instance-id? (->> healthy-instances (map :id) set)
            known-instances' (->> service-id
                                  (get service-id->unhealthy-instances)
                                  (concat healthy-instances))
            previously-known-instance? (comp known-instance-ids :id)
            new-instance-ids (->> known-instances'
                                  (remove previously-known-instance?)
                                  (sort instance-comparator)
                                  (mapv :id))
            known-instance-ids' (->> known-instances' (map :id) set)
            removed-instance-ids (set/difference known-instance-ids known-instance-ids')
            ;; React to upward-scaling
            instance-counts' (get service-id->instance-counts service-id instance-counts-zero)
            instances-requested-delta (- (:requested instance-counts')
                                         (:requested instance-counts))
            instance-scheduling-start-times'
            (->> current-time
                 (repeat instances-requested-delta)
                 (into instance-scheduling-start-times))
            ;; Track scheduling instances
            unmatched-instance-count (- (count new-instance-ids)
                                        (count instance-scheduling-start-times'))
            [matched-start-times instance-scheduling-start-times'']
            (split-at (count new-instance-ids) instance-scheduling-start-times')
            ;; Track starting instances
            new-instance-id->start-timestamp
            (pc/map-from-keys (constantly current-time) new-instance-ids)
            starting-instance-id->start-timestamp'
            (->> removed-instance-ids
                 (reduce dissoc starting-instance-id->start-timestamp)
                 (merge new-instance-id->start-timestamp))
            {started-instance-ids :started starting-instances-ids :starting}
            (group-by #(if (healthy-instance-id? %) :started :starting)
                      (keys starting-instance-id->start-timestamp'))
            starting-instance-id->start-timestamp''
            (select-keys starting-instance-id->start-timestamp' starting-instances-ids)]
        ;; Warn about any new instances for which we didn't track the scheduling time
        (when (pos? unmatched-instance-count)
          (log/warn unmatched-instance-count
                    "untracked scheduled instances discovered in instance launch metric loop for service"
                    service-id))
        ;; Report schedule time for now-known instances
        (doseq [start-time matched-start-times]
          (let [duration (metrics/duration-between start-time current-time)]
            (metrics/report-duration waiter-schedule-timer duration)
            (metrics/report-duration service-schedule-timer duration)
            (when leader?
              (statsd/histo! metric-group "schedule_time"
                             (du/interval->nanoseconds duration)))))
        ;; Report startup time for instances that are now healthy
        (doseq [instance-id started-instance-ids]
          (let [start-time (starting-instance-id->start-timestamp' instance-id)
                duration (metrics/duration-between start-time current-time)]
            (metrics/report-duration service-startup-timer duration)
            (when leader?
              (statsd/histo! metric-group "startup_time"
                             (du/interval->nanoseconds duration)))))
        ;; tracker-state'
        (assoc tracker-state
               :instance-counts instance-counts'
               :instance-scheduling-start-times (vec instance-scheduling-start-times'')
               :known-instance-ids known-instance-ids'
               :starting-instance-id->start-timestamp starting-instance-id->start-timestamp'')))))

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

                     :else  ;; ignoring out-of-order state updates
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
