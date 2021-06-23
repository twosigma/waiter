(ns waiter.instance-tracker
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.util.cache-utils :as cu]
            [clojure.set :as set]))

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
              (assoc :id->failed-date (pc/map-vals :data (cu/cache->map id->failed-date-cache)))))))

(defn create-instance-failure-event-handler
  [{:keys [clock config]}]
  {:pre [(-> config :recent-failed-instance-cache :threshold pos-int?)
         (-> config :recent-failed-instance-cache :ttl pos-int?)]}
  (let [{:keys [recent-failed-instance-cache]} config
        id->failed-date-cache (cu/cache-factory recent-failed-instance-cache)
        handler-state (atom {:id->failed-date-cache id->failed-date-cache
                             :last-error-time nil})]
    (DefaultInstanceFailureHandler. clock handler-state)))

(defn make-id->instance [service-id->instances]
  (reduce
    (fn [cur-id->inst cur-instances]
      (reduce
        (fn [inner-cur-id->inst {:keys [id] :as inst}]
          (assoc inner-cur-id->inst id inst))
        cur-id->inst
        cur-instances))
    {}
    (vals service-id->instances)))

(defn get-new-and-old-instances [id->instance id->instance']
  (let [inst-ids (set (keys id->instance))
        inst-ids' (set (keys id->instance'))
        in-both (set/intersection inst-ids inst-ids')]

    [(map id->instance (set/difference inst-ids in-both))
     (map id->instance' (set/difference inst-ids' in-both))]))

(defn start-instance-tracker
  "Starts daemon thread that tracks instances and produces events based on state changes. It routes these events to the
  proper instance handler component"
  [clock router-state-chan instance-failure-handler-component]
  (cid/with-correlation-id
    "instance-tracker"
    (let [exit-chan (async/promise-chan)
          query-chan (async/chan)
          state-atom (atom {:id->failed-instance {}
                            :id->healthy-instance {}
                            :last-update-time nil})
          query-state-fn
          (fn instance-tracker-query-state-fn
            [include-flags]
            (let [{:keys [id->failed-instance id->healthy-instance last-update-time]} @state-atom]
              (cond-> {:last-update-time last-update-time
                       :supported-include-params ["id->failed-instance" "id->healthy-instance" "instance-failure-handler"]}
                      (contains? include-flags "id->failed-instance")
                      (assoc :id->failed-instance id->failed-instance)
                      (contains? include-flags "id->healthy-instance")
                      (assoc :id->healthy-instance id->healthy-instance)
                      (contains? include-flags "instance-failure-handler")
                      (assoc :instance-failure-handler (state instance-failure-handler-component include-flags)))))
          go-chan
          (async/go
            (try
              (loop [{:keys [id->failed-instance id->healthy-instance] :as current-state} @state-atom]
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
                          (let [{:keys [service-id->failed-instances service-id->healthy-instances]} msg
                                id->failed-instance' (make-id->instance service-id->failed-instances)
                                [_ new-failed-instances]
                                (get-new-and-old-instances id->failed-instance id->failed-instance')
                                id->healthy-instance' (make-id->instance service-id->healthy-instances)
                                [removed-healthy-instances new-healthy-instances]
                                (get-new-and-old-instances id->healthy-instance id->healthy-instance')]

                            (when (not-empty new-failed-instances)
                              (log/info "new failed instances" {:new-failed-instances (map :id new-failed-instances)})
                              (handle-instances-event! instance-failure-handler-component {:new-failed-instances new-failed-instances}))

                            (when (not-empty new-healthy-instances)
                              (log/info "new healthy instances" {:new-healthy-instances (map :id new-healthy-instances)}))

                            (when (not-empty removed-healthy-instances)
                              (log/info "new not healthy instances" {:removed-healthy-instances (map :id removed-healthy-instances)}))

                            (assoc current-state :id->failed-instance id->failed-instance'
                                                 :id->healthy-instance id->healthy-instance')))

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
