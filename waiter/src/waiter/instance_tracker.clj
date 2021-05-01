(ns waiter.instance-tracker
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.util.cache-utils :as cu]))

; Events are being handled by all routers in a cluster for resiliency
(defprotocol InstanceEventHandler

  (handle-instances-event! [this instances-event]
    "handles the instances event")

  (state [this include-flags]
    "returns the state of the handler:
    {:last-error-time :supported-include-params :type}"))

(defrecord DefaultInstanceFailureHandler [clock
                                          handler-state]

  InstanceEventHandler

  (handle-instances-event! [this {:keys [new-failed-instances]}]
    (let [{:keys [clock handler-state]} this
          {:keys [id->failed-date-cache]} @handler-state]
      (log/info "default failed-instance handler received new new-failed-instances" {:new-failed-instances new-failed-instances})
      (swap! handler-state assoc :last-error-time (clock))
      (doseq [inst new-failed-instances]
        (cu/cache-put! id->failed-date-cache (:id inst) (clock)))))

  (state [this include-flags]
    (let [{:keys [handler-state]} this
          {:keys [last-error-time id->failed-date-cache]} @handler-state]
      (cond-> {:last-error-time last-error-time
               :supported-include-params ["id->failed-date"]
               :type "DefaultInstanceFailureHandler"}
              (contains? include-flags "id->failed-date")
              (assoc :id->failed-date (reduce
                                        (fn [transformed-map [id value]]
                                          (assoc transformed-map id (:data value)))
                                        {}
                                        (cu/cache->map id->failed-date-cache)))))))

(defn create-instance-failure-event-handler
  [{:keys [clock config]}]
  {:pre [(-> config :recent-failed-instance-cache :threshold pos-int?)
         (-> config :recent-failed-instance-cache :ttl pos-int?)]}
  (let [{:keys [recent-failed-instance-cache]} config
        id->failed-date-cache (cu/cache-factory recent-failed-instance-cache)
        handler-state (atom {:last-error-time nil
                             :id->failed-date-cache id->failed-date-cache})]
    (DefaultInstanceFailureHandler. clock handler-state)))

(defn start-instance-tracker
  "Starts daemon thread that tracks instances and produces events based on state changes. It routes these events to the
  proper instance handler component"
  [clock router-state-chan instance-failure-handler-component]
  (cid/with-correlation-id
    "instance-tracker"
    (let [exit-chan (async/promise-chan)
          query-chan (async/chan)
          state-atom (atom {:id->failed-instance {}
                            :last-update-time nil})
          query-state-fn
          (fn instance-tracker-query-state-fn
            [include-flags]
            (let [{:keys [id->failed-instance last-update-time]} @state-atom]
              (cond-> {:last-update-time last-update-time
                       :supported-include-params ["id->failed-instance" "instance-failure-handler"]}
                      (contains? include-flags "id->failed-instance")
                      (assoc :id->failed-instance id->failed-instance)
                      (contains? include-flags "instance-failure-handler")
                      (assoc :instance-failure-handler (state instance-failure-handler-component include-flags)))))
          go-chan
          (async/go
            (try
              (loop [{:keys [id->failed-instance] :as current-state} @state-atom]
                (reset! state-atom current-state)
                (let [[msg current-chan] (async/alts! [exit-chan router-state-chan query-chan] :priority true)
                      next-state
                      (condp = current-chan
                        exit-chan
                        (do
                          (log/warn "stopping instance-tracker")
                          (when (not= :exit msg)
                            (throw (ex-info "Stopping instance-tracker" {:time (clock) :reason msg}))))

                        router-state-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "instance-tracker" "router-state-chan")
                          (let [{:keys [service-id->failed-instances]} msg
                                id->failed-instance' (reduce
                                                       (fn [cur-id->failed-instance cur-failed-instances]
                                                         (reduce
                                                           (fn [inner-cur-id->failed-instance failed-instance]
                                                             (assoc inner-cur-id->failed-instance (:id failed-instance) failed-instance))
                                                           cur-id->failed-instance
                                                           cur-failed-instances))
                                                       {}
                                                       (vals service-id->failed-instances))
                                all-failed-instances-ids' (set (keys id->failed-instance'))
                                all-failed-instances-ids (set (keys id->failed-instance))
                                new-failed-instances-ids (filter (complement all-failed-instances-ids) all-failed-instances-ids')
                                new-failed-instances (map id->failed-instance' new-failed-instances-ids)]
                            (when (not (empty? new-failed-instances))
                              (log/info "new failed instances" {:new-failed-instances (map :id new-failed-instances)})
                              (handle-instances-event! instance-failure-handler-component {:new-failed-instances new-failed-instances}))
                            (assoc current-state :id->failed-instance id->failed-instance')))

                        query-chan
                        (let [{:keys [include-flags response-chan]} msg]
                          (async/put! response-chan (query-state-fn include-flags))
                          current-state))]
                  (if next-state
                    (recur (assoc next-state :last-update-time (clock)))
                    (log/info "stopping instance-tracker as next loop state is nil"))))
              (catch Exception e
                (log/error e "fatal error in instance-tracker")
                (System/exit 1))))]
      {:exit-chan exit-chan
       :go-chan go-chan
       :query-chan query-chan
       :query-state-fn query-state-fn})))
