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
(ns waiter.scaling
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [metrics.core]
            [metrics.counters :as counters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.util.async-utils :as au]
            [waiter.util.utils :as utils])
  (:import (org.joda.time DateTime)))

(defn get-app-instance-stats
  "Queries scheduler to find the number of instances and running tasks for all apps"
  [scheduler]
  (when-let [apps (try
                    (scheduler/retry-on-transient-server-exceptions
                      "get-app-instance-stats"
                      (scheduler/get-services scheduler))
                    (catch Exception ex
                      (log/warn ex "fetch failed for instance counts from scheduler")))]
    (zipmap (map :id apps)
            (map #(select-keys % [:instances :task-count]) apps))))

(defn service-scaling-multiplexer
  "Sends request to scale instances to the correct scaling executor go routine.
   Maintains the mapping of service to service-scaling-executor."
  [scaling-executor-factory initial-state]
  (let [executor-multiplexer-chan (async/chan)
        query-chan (async/chan 1)]
    (async/go
      (try
        (loop [service-id->scaling-executor-chan initial-state]
          (recur
            (let [[data channel] (async/alts! [executor-multiplexer-chan query-chan] :priority true)]
              (condp = channel
                executor-multiplexer-chan
                (let [{:keys [service-id scale-amount correlation-id] :as scaling-data} data]
                  (counters/dec! (metrics/waiter-counter "autoscaler" "scaling" "pending"))
                  (cid/with-correlation-id
                    correlation-id
                    (log/info "service-scaling-multiplexer received" {:service-id service-id :scale-amount scale-amount})
                    (let [service-id->scaling-executor-chan
                          (cond-> service-id->scaling-executor-chan
                            (not (get service-id->scaling-executor-chan service-id))
                            (assoc service-id (scaling-executor-factory service-id)))
                          {:keys [executor-chan]} (get service-id->scaling-executor-chan service-id)]
                      (if scale-amount
                        (do
                          (log/info "sending" service-id "executor to scale by" scale-amount "instances")
                          (async/>! executor-chan scaling-data)
                          service-id->scaling-executor-chan)
                        (do
                          (log/info "shutting down scaling executor channel for" service-id)
                          (async/close! executor-chan)
                          (dissoc service-id->scaling-executor-chan service-id))))))

                query-chan
                (let [{:keys [response-chan service-id]} data]
                  (if service-id
                    (if-let [query-chan (get-in service-id->scaling-executor-chan [service-id :query-chan])]
                      (async/>! query-chan data)
                      (async/>! response-chan :no-data-available))
                    (async/>! response-chan service-id->scaling-executor-chan))
                  service-id->scaling-executor-chan)))))
        (catch Exception e
          (log/error e "error in instance-killer-multiplexer"))))
    {:executor-multiplexer-chan executor-multiplexer-chan
     :query-chan query-chan}))

(defn- execute-scale-service-request
  "Helper function to scale instances of a service.
   The force? flag can be used to detmerine whether we will make a best effort or a forced scale operation."
  [scheduler service-id scale-to-instances force?]
  (let [mode (if force? "scale-force" "scale-up")]
    (try
      (scheduler/suppress-transient-server-exceptions
        "autoscaler"
        (log/info mode "service to" scale-to-instances "instances")
        (scheduler/scale-service scheduler service-id scale-to-instances force?)
        (counters/inc! (metrics/service-counter service-id "scaling" mode "success")))
      (catch Exception e
        (counters/inc! (metrics/service-counter service-id "scaling" mode "fail"))
        (log/warn e "unexpected error when trying to scale" service-id "to" scale-to-instances "instances")))))

(defn- execute-scale-down-request
  "Helper function to scale-down instances of a service.
   Instances needs to be approved for killing by peers before an actual kill attempt is made.
   When an instance receives a veto or is not killed, we will iteratively search for another instance to successfully kill.
   The function stops and returns true when a successful kill is made.
   Else, it terminates after we have exhausted all candidate instances to kill or when a kill attempt returns a non-truthy value."
  [notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn scheduler instance-rpc-chan timeout-config
   service-id correlation-id num-instances-to-kill response-chan]
  (let [{:keys [blacklist-backoff-base-time-ms inter-kill-request-wait-time-ms max-blacklist-time-ms]} timeout-config]
    (async/go
      (cid/with-correlation-id
        correlation-id
        (try
          (let [request-id (utils/unique-identifier) ; new unique identifier for this reservation request
                reason-map-fn (fn [] {:cid correlation-id :reason :kill-instance :request-id request-id :time (t/now)})
                result-map-fn (fn [status] {:cid correlation-id :request-id request-id :status status})]
            (log/info "requested to scale down by" num-instances-to-kill "but will attempt to kill only one instance")
            (timers/start-stop-time!
              (metrics/service-timer service-id "kill-instance")
              (loop [exclude-ids-set #{}]
                (let [instance (service/get-rand-inst instance-rpc-chan service-id (reason-map-fn) exclude-ids-set inter-kill-request-wait-time-ms)]
                  (if-let [instance-id (:id instance)]
                    (if (peers-acknowledged-blacklist-requests-fn instance true blacklist-backoff-base-time-ms :prepare-to-kill)
                      (do
                        (log/info "scaling down instance candidate" instance)
                        (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "attempt"))
                        (let [{:keys [killed?] :as kill-result} (scheduler/kill-instance scheduler instance)]
                          (if killed?
                            (do
                              (log/info "marking instance" instance-id "as killed")
                              (counters/inc! (metrics/service-counter service-id "instance-counts" "killed"))
                              (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "success"))
                              (service/release-instance! instance-rpc-chan instance (result-map-fn :killed))
                              (notify-instance-killed-fn instance)
                              (peers-acknowledged-blacklist-requests-fn instance false max-blacklist-time-ms :killed))
                            (do
                              (log/info "failed kill attempt, releasing instance" instance-id)
                              (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "kill-fail"))
                              (service/release-instance! instance-rpc-chan instance (result-map-fn :not-killed))))
                          (when response-chan (async/>! response-chan kill-result))
                          killed?))
                      (do
                        (log/info "kill was vetoed, releasing instance" instance-id)
                        (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "vetoed-instance"))
                        (service/release-instance! instance-rpc-chan instance (result-map-fn :not-killed))
                        ;; make best effort to find another instance that is not veto-ed
                        (recur (conj exclude-ids-set instance-id))))
                    (do
                      (log/info "no instance available to kill")
                      (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "unavailable"))
                      false))))))
          (catch Exception ex
            (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "fail"))
            (log/error ex "unable to scale down service" service-id)))))))

(defn kill-instance-handler
  "Handler that supports killing instances of a particular service on a specific router."
  [notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn scheduler instance-rpc-chan timeout-config
   {:keys [route-params] {:keys [src-router-id]} :basic-authentication}]
  (let [{:keys [service-id]} route-params
        correlation-id (cid/get-correlation-id)]
    (cid/cinfo correlation-id "received request to kill instance of" service-id "from" src-router-id)
    (async/go
      (let [response-chan (async/promise-chan)
            instance-killed? (async/<!
                               (execute-scale-down-request
                                 notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn
                                 scheduler instance-rpc-chan timeout-config
                                 service-id correlation-id 1 response-chan))
            {:keys [instance-id status] :as kill-response} (or (async/poll! response-chan)
                                                               {:message :no-instance-killed, :status 404})]
        (if instance-killed?
          (cid/cinfo correlation-id "killed instance" instance-id)
          (cid/cinfo correlation-id "unable to kill instance" kill-response))
        (-> (utils/clj->json-response {:kill-response kill-response
                                       :service-id service-id
                                       :source-router-id src-router-id
                                       :success instance-killed?}
                                      :status (or status 500))
            (update :headers assoc "x-cid" correlation-id))))))

(defn compute-scale-amount-restricted-by-quanta
  "Computes the new scale amount subject to quanta restrictions.
   The returned value is guaranteed to be at least 1."
  [service-description quanta-constraints scale-amount]
  {:pre [(pos? scale-amount) (integer? scale-amount)]
   :post [(pos? %) (<= % scale-amount)]}
  (-> scale-amount
      (min (quot (:cpus quanta-constraints) (get service-description "cpus"))
           (quot (:mem quanta-constraints) (get service-description "mem")))
      (max 1)))

(defn service-scaling-executor
  "The scaling executor that scales individual services up or down.
   It uses the scheduler to trigger scale up/down operations.
   While a scale-up request can cause many new instances to be spawned, a scale-down request can end up killing at most one instance.
   Killing of an instance may be delegated to peer routers via delegate-instance-kill-request-fn if no instance is available locally.
   The executor also respects inter-kill-request-wait-time-ms between successive scale-down operations."
  [notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn delegate-instance-kill-request-fn service-id->service-description-fn
   scheduler instance-rpc-chan quanta-constraints {:keys [inter-kill-request-wait-time-ms] :as timeout-config} service-id]
  {:pre [(>= inter-kill-request-wait-time-ms 0)]}
  (log/info "[scaling-executor] starting instance killer for" service-id)
  (let [base-correlation-id (str "scaling-executor-" service-id)
        inter-kill-request-wait-time-in-millis (t/millis inter-kill-request-wait-time-ms)
        executor-chan (au/latest-chan)
        query-chan (async/chan 1)
        exit-chan (async/chan 1)]
    (async/go
      (try
        (cid/cinfo base-correlation-id "awaiting scaling details on executor channel")
        (loop [{:keys [last-scale-down-time] :as executor-state} {}]
          (let [[data channel] (async/alts! [exit-chan executor-chan query-chan] :priority true)
                executor-state'
                (cond
                  (= channel query-chan)
                  (let [{:keys [response-chan]} data]
                    (async/>! response-chan executor-state)
                    executor-state)

                  (= channel exit-chan)
                  (when (not= :exit data)
                    executor-state)

                  (nil? data) ; executor-chan was closed, trigger exit
                  nil

                  :else
                  (let [{:keys [correlation-id response-chan scale-amount scale-to-instances task-count total-instances]} data
                        num-instances-to-kill (if (neg? scale-amount) (max 0 (- task-count scale-to-instances)) 0)]
                    (cid/with-correlation-id
                      (str base-correlation-id "." correlation-id)
                      (counters/inc! (metrics/service-counter service-id "scaling" "total"))
                      (cond
                        (pos? scale-amount)
                        (do
                          (counters/inc! (metrics/service-counter service-id "scaling" "scale-up" "total"))
                          (if (< task-count total-instances)
                            (do
                              (log/info "allowing previous scale operation to complete before scaling up again")
                              (counters/inc! (metrics/service-counter service-id "scaling" "scale-up" "ignore")))
                            (let [service-description (service-id->service-description-fn service-id)
                                  scale-amount' (compute-scale-amount-restricted-by-quanta
                                                  service-description quanta-constraints scale-amount)
                                  scale-adjustment (- scale-amount' scale-amount)
                                  scale-to-instances' (+ scale-to-instances scale-adjustment)]
                              (when-not (zero? scale-adjustment)
                                (log/info service-id "scale amount adjusted"
                                          {:scale-adjustment scale-adjustment
                                           :scale-amount scale-amount
                                           :scale-to-instances' scale-to-instances'}))
                              (execute-scale-service-request scheduler service-id scale-to-instances' false)))
                          executor-state)

                        (pos? num-instances-to-kill)
                        (do
                          (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "total"))
                          (if (or (nil? last-scale-down-time)
                                  (t/after? (t/now) (t/plus last-scale-down-time inter-kill-request-wait-time-in-millis)))
                            (if (or (async/<!
                                      (execute-scale-down-request
                                        notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn
                                        scheduler instance-rpc-chan timeout-config
                                        service-id correlation-id num-instances-to-kill response-chan))
                                    (delegate-instance-kill-request-fn service-id))
                              (assoc executor-state :last-scale-down-time (t/now))
                              executor-state)
                            (do
                              (log/debug "skipping scale-down as" inter-kill-request-wait-time-ms
                                         "ms has not elapsed since last scale down operation")
                              (counters/inc! (metrics/service-counter service-id "scaling" "scale-down" "ignore"))
                              executor-state)))

                        (and (neg? scale-amount) (< task-count scale-to-instances))
                        (do
                          (log/info "potential overshoot detected, triggering scale-force for service"
                                    {:scale-to-instances scale-to-instances :task-count task-count})
                          (counters/inc! (metrics/service-counter service-id "scaling" "scale-force" "total"))
                          (execute-scale-service-request scheduler service-id scale-to-instances true)
                          executor-state)

                        :else
                        (do
                          (counters/inc! (metrics/service-counter service-id "scaling" "noop"))
                          executor-state)))))]
            (if executor-state'
              (recur executor-state')
              (log/info "[scaling-executor] exiting for" service-id))))
        (catch Exception ex
          (log/error ex "[scaling-executor] fatal exception while scaling instances for" service-id)
          (System/exit 1))))
    {:executor-chan executor-chan
     :exit-chan exit-chan
     :query-chan query-chan}))

(defn normalize-factor
  "Applies an exponential smoothing factor n times.
  Ex: (normalize-factor 0.5 1) => 0.5
      (normalize-factor 0.5 2) => 0.75
      (normalize-factor 0.9 2) => 0.99"
  [^double factor n]
  (loop [i 0
         result 0.0
         remaining 1.0]
    (if (>= i n)
      result
      (recur (inc i)
             (+ result (* factor remaining))
             (* remaining (- 1 factor))))))

(defn apply-scaling!
  "Given a scale-amount and scale-to-instances, performs the scaling operation."
  [executor-multiplexer-chan service-id scaling-data]
  (try
    (let [correlation-id (cid/get-correlation-id)]
      (log/info "scaling service" service-id "with" scaling-data)
      (counters/inc! (metrics/waiter-counter "autoscaler" "scaling" "pending"))
      (async/go
        (->> (assoc scaling-data :correlation-id correlation-id :service-id service-id)
             (async/>! executor-multiplexer-chan))))
    (catch Exception e
      (log/warn e "Unexpected error when trying to scale" service-id))))

(defn scale-service
  "Scales an individual service.
  The ideal number of instances follows outstanding-requests for any given service.
  Scaling is controlled by two exponentially weighted moving averages, one for scaling up and one for scaling down.
  Scaling up dominates scaling down.
  The Exponential Moving Average (EMA) is recursive, and works by applying a smoothing factor, such that:
    target-instances' = outstanding-requests * smoothing-factor + ((1 - smoothing-factor) * target-instances)
  The ideal number of instances, target-instances, is a continuous (float) number.
  The scheduler needs a whole number (int) of instances, total-instances, which is mapped from target-instances
  using a threshold to prevent jitter.
  Smoothing factors are normalized such that they are independent of the amount of time that has passed
  between scaling operations.
  Additionally, the autoscaler attempts to replace expired instances. For every expired instance, the autoscaler increases
  its target by one additional instance. Then, it scales down one instance for each healthy instance in excess of its target."
  [{:strs [concurrency-level expired-instance-restart-rate jitter-threshold max-instances min-instances
           scale-down-factor scale-factor scale-ticks scale-up-factor]}
   {:keys [expired-instances healthy-instances outstanding-requests target-instances total-instances]}]
  {:pre [(<= 0 scale-up-factor 1)
         (<= 0 scale-down-factor 1)
         (pos? scale-factor)
         (<= scale-factor 1)
         (>= concurrency-level 1)
         (> 1 jitter-threshold)
         (utils/non-neg? jitter-threshold)
         (utils/non-neg? scale-ticks)
         (utils/non-neg? min-instances)
         (utils/non-neg? max-instances)]}
  (let [epsilon 1e-2
        scale-up-factor (normalize-factor scale-up-factor scale-ticks)
        scale-down-factor (normalize-factor scale-down-factor scale-ticks)
        ideal-instances (int (Math/ceil (/ (* outstanding-requests scale-factor) concurrency-level)))
        smoothed-scale-up-target (+ (* scale-up-factor ideal-instances)
                                    (* (- 1 scale-up-factor) target-instances))
        smoothed-scale-down-target (+ (* scale-down-factor ideal-instances)
                                      (* (- 1 scale-down-factor) target-instances))
        target-instances' (->> (if (>= smoothed-scale-up-target target-instances)
                                 smoothed-scale-up-target
                                 smoothed-scale-down-target)
                               (min max-instances)
                               (max min-instances))
        ^double delta (- target-instances' total-instances)
        ;; constrain scaling-up when there are enough instances, but continue to compute the EMA
        scale-amount (if (or (and (pos? delta)
                                  (<= ideal-instances total-instances)
                                  (>= total-instances min-instances))
                             (< (Math/abs delta) jitter-threshold))
                       0
                       (int (Math/ceil (- delta epsilon))))
        scale-to-instances (+ total-instances scale-amount)
        ; number of expired instances already replaced by healthy instances
        excess-instances (max 0 (- healthy-instances scale-to-instances))
        expired-instances-to-replace (int (Math/ceil (* expired-instances expired-instance-restart-rate)))
        ; if we are scaling down and all instances are healthy, do not account for expired instances
        ; since the instance killer will kill the expired instances
        scaling-down (< scale-amount 0)
        all-instances-are-healthy (= total-instances healthy-instances)
        scale-to-instances' (cond-> scale-to-instances
                              (and (pos? expired-instances)
                                   (not (and scaling-down all-instances-are-healthy)))
                              (+ (- expired-instances-to-replace excess-instances)))
        scale-amount' (int (- scale-to-instances' total-instances))]
    {:scale-amount scale-amount'
     :scale-to-instances scale-to-instances'
     :target-instances target-instances'}))

(defn scale-services
  "Scales a sequence of services given the scale state of each service, and returns a new scale state which
  is fed back in the for the next call to scale-services."
  [service-ids service-id->service-description service-id->outstanding-requests service-id->scale-state apply-scaling-fn
   scale-ticks scale-service-fn service-id->router-state service-id->scheduler-state]
  (try
    (log/trace "scaling apps" {:service-ids service-ids
                               :service-id->service-description service-id->service-description
                               :service-id->outstanding-requests service-id->outstanding-requests
                               :service-id->router-state service-id->router-state
                               :service-id->scheduler-state service-id->scheduler-state
                               :service-id->scale-state service-id->scale-state
                               :scale-ticks scale-ticks})
    (pc/map-from-keys
      (fn [service-id]
        (let [outstanding-requests (or (service-id->outstanding-requests service-id) 0)
              {:keys [healthy-instances expired-instances]} (service-id->router-state service-id)
              {:keys [instances task-count] :as scheduler-state} (service-id->scheduler-state service-id)
              ; if we don't have a target instance count, default to the number of tasks
              target-instances (get-in service-id->scale-state [service-id :target-instances] task-count)
              service-description (get service-id->service-description service-id)
              {:keys [target-instances scale-to-instances scale-amount]}
              (if (and target-instances scale-ticks)
                (scale-service-fn (assoc service-description "scale-ticks" scale-ticks)
                                  {:healthy-instances healthy-instances
                                   :expired-instances expired-instances
                                   :outstanding-requests outstanding-requests
                                   :target-instances target-instances
                                   :total-instances instances})
                (do
                  (log/info "no target instances available for service"
                            {:scheduler-state scheduler-state, :service-id service-id})
                  {:scale-to-instances instances :target-instances target-instances :scale-amount 0}))]
          (when (< instances (service-description "min-instances"))
            (log/warn "scheduler reported service had fewer instances than min-instances"
                      {:service-id service-id :instances instances :min-instances (service-description "min-instances")}))
          (when-not (zero? scale-amount)
            (apply-scaling-fn service-id
                              {:outstanding-requests outstanding-requests
                               :scale-amount scale-amount
                               :scale-to-instances scale-to-instances
                               :target-instances target-instances
                               :task-count task-count
                               :total-instances instances}))
          {:target-instances target-instances
           :scale-to-instances scale-to-instances
           :scale-amount scale-amount}))
      service-ids)
    (catch Exception e
      (log/error e "exception in scale-services")
      service-id->scale-state)))

(defn- difference-in-millis
  "Helper method to compute time difference in millis.
   Added type hints to avoid warn-on-relfection warnings."
  [^DateTime end-time ^DateTime start-time]
  (- (.getMillis end-time) (.getMillis start-time)))

(defn query-autoscaler-service-state
  "Retrieves the autoscaler state for the specified service-id."
  [{:keys [global-state service-id->router-state service-id->scale-state service-id->scheduler-state]}
   {:keys [service-id]}]
  (merge (service-id->scale-state service-id)
         (service-id->scheduler-state service-id)
         {:outstanding-requests (get-in global-state [service-id "outstanding"])}
         (service-id->router-state service-id)))

(defn autoscaler-goroutine
  "Autoscaler encapsulated in goroutine.
   Acquires state of services and passes to scale-services."
  [initial-state leader?-fn service-id->metrics-fn executor-multiplexer-chan scheduler timeout-interval-ms scale-service-fn
   service-id->service-description-fn state-mult]
  (let [state-atom (atom (merge {:continue-looping true
                                 :global-state {}
                                 :iter-counter 1
                                 :previous-cycle-start-time nil
                                 :service-id->router-state {}
                                 :service-id->scale-state {}
                                 :service-id->scheduler-state {}
                                 :timeout-chan (async/timeout timeout-interval-ms)}
                                initial-state))
        exit-chan (async/chan)
        query-chan (async/chan 10)
        state-chan (au/latest-chan)
        apply-scaling-fn (fn apply-scaling-fn [service-id scaling-data]
                           (apply-scaling! executor-multiplexer-chan service-id scaling-data))]
    (async/tap state-mult state-chan)
    (let [prefix-cid "SCALING"]
      (async/go
        (try
          (loop [{:keys [global-state iter-counter previous-cycle-start-time service-id->router-state
                         service-id->scale-state timeout-chan] :as current-state}
                 @state-atom]
            (reset! state-atom current-state)
            (let [correlation-id (str prefix-cid "-" iter-counter)
                  new-state
                  (timers/start-stop-time!
                    (metrics/waiter-timer "autoscaler" "iteration")
                    (let [[args chan] (async/alts! [exit-chan state-chan timeout-chan query-chan] :priority true)]
                      (condp = chan
                        exit-chan
                        (assoc current-state :continue-looping false :timeout-chan nil)
                        state-chan
                        (let [{:keys [service-id->healthy-instances service-id->unhealthy-instances service-id->expired-instances]} args
                              existing-service-ids-set (-> service-id->router-state keys set)
                              service-ids-set (into (-> service-id->healthy-instances keys set)
                                                    (-> service-id->unhealthy-instances keys))
                              deleted-service-ids (set/difference existing-service-ids-set service-ids-set)
                              new-service-ids (set/difference service-ids-set existing-service-ids-set)
                              service-id->router-state' (pc/map-from-keys
                                                          (fn [service-id]
                                                            {:healthy-instances (count (service-id->healthy-instances service-id))
                                                             :expired-instances (count (service-id->expired-instances service-id))})
                                                          service-ids-set)]
                          (cid/with-correlation-id
                            correlation-id
                            (when (seq new-service-ids)
                              (log/info "started tracking following services:" new-service-ids))
                            (when (seq deleted-service-ids)
                              (log/info "no longer tracking following services:" deleted-service-ids)
                              ; trigger closing of the scaling executors for the services
                              (doseq [service-id deleted-service-ids]
                                (apply-scaling-fn service-id {}))))
                          (assoc current-state :service-id->router-state service-id->router-state'))
                        timeout-chan
                        (if (leader?-fn)
                          (let [global-state' (or (service-id->metrics-fn) global-state)
                                cycle-start-time (t/now)
                                service-id->scheduler-state' (get-app-instance-stats scheduler)]
                            (timers/start-stop-time!
                              (metrics/waiter-timer "autoscaler" "processing")
                              (let [service->scale-state'
                                    (if (seq service-id->router-state)
                                      (cid/with-correlation-id
                                        correlation-id
                                        (let [router-service-ids (set (keys service-id->router-state))
                                              scheduler-service-ids (set (keys service-id->scheduler-state'))
                                              scalable-service-ids (set/intersection router-service-ids scheduler-service-ids)
                                              excluded-service-ids (-> (set/union router-service-ids scheduler-service-ids)
                                                                       (set/difference scalable-service-ids))
                                              scale-ticks (when previous-cycle-start-time
                                                            (-> (difference-in-millis cycle-start-time previous-cycle-start-time)
                                                                (/ 1000)
                                                                int))]
                                          (when (seq excluded-service-ids)
                                            (cid/cinfo correlation-id "services excluded this iteration" excluded-service-ids))
                                          (scale-services scalable-service-ids
                                                          (pc/map-from-keys #(service-id->service-description-fn %) scalable-service-ids)
                                                          ; default to 0 outstanding requests for services without metrics
                                                          (pc/map-from-keys #(get-in global-state' [% "outstanding"] 0) scalable-service-ids)
                                                          service-id->scale-state
                                                          apply-scaling-fn
                                                          scale-ticks
                                                          scale-service-fn
                                                          service-id->router-state
                                                          service-id->scheduler-state')))
                                      service-id->scale-state)]
                                (cid/cinfo correlation-id "scaling iteration took" (difference-in-millis (t/now) cycle-start-time)
                                           "ms for" (count service->scale-state') "services.")
                                (assoc current-state
                                  :global-state global-state'
                                  :previous-cycle-start-time cycle-start-time
                                  :service-id->scale-state service->scale-state'
                                  :service-id->scheduler-state service-id->scheduler-state'
                                  :continue-looping true
                                  :timeout-chan (async/timeout timeout-interval-ms)))))
                          (assoc current-state :timeout-chan (async/timeout timeout-interval-ms)
                                               :previous-cycle-start-time nil))
                        query-chan
                        (let [{:keys [service-id response-chan]} args
                              service-state (query-autoscaler-service-state current-state {:service-id service-id})]
                          (async/>! response-chan service-state)
                          current-state))))]
              (if (:continue-looping new-state)
                (recur (update-in new-state [:iter-counter] inc))
                (cid/cinfo prefix-cid "exiting"))))
          (catch Exception e
            (log/error e "fatal error in autoscaler")
            (System/exit 1)))))
    {:exit exit-chan
     :query query-chan
     :query-service-state-fn (fn query-service-state-fn [query-params]
                               (query-autoscaler-service-state @state-atom query-params))}))
