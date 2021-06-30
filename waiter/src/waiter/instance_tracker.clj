(ns waiter.instance-tracker
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.cache-utils :as cu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils]))

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

(defn make-instance-event
  "Create an event map with keys :id :object :type
  :id is an id for tracking a debugging issues
  :object is general data
  :type provides the client a information to interpret the object data"
  [id type object]
  {:id id
   :object object
   :type type})

(defn make-id->instance
  "Takes a service-id->instances mapping and returns an instance-id->instance mapping"
  [service-id->instances]
  (->> service-id->instances vals flatten (pc/map-from-vals :id)))

(defn get-new-changed-and-old-instances
  "Returns map {:old-instances :new-instances :changed-instances} given two maps id->instance and id->instance'.
  Instances that exist only in id->instance are considered an old-instances, instances that exist only in id->instance'
  are considered new instances, and instances in both input maps with different fields are considered updated"
  [id->instance id->instance']
  (let [inst-ids (set (keys id->instance))
        inst-ids' (set (keys id->instance'))
        in-both (set/intersection inst-ids inst-ids')]
    {:changed-instances (map id->instance' (filter
                                             (fn changed? [inst-id]
                                               (not= (get id->instance inst-id) (get id->instance' inst-id)))
                                             in-both))
     :new-instances (map id->instance' (set/difference inst-ids' in-both))
     :old-instances (map id->instance (set/difference inst-ids in-both))}))

(defn start-instance-tracker
  "Starts daemon thread that tracks instances and produces events based on state changes. It routes these events to the
  proper instance handler component"
  [clock router-state-chan instance-failure-handler-component]
  (cid/with-correlation-id
    "instance-tracker"
    (let [exit-chan (async/promise-chan)
          query-chan (async/chan)
          channels-update-chan-buffer-size 1024
          instance-watch-channels-update-chan-buffer (async/buffer channels-update-chan-buffer-size)
          instance-watch-channels-update-chan (async/chan instance-watch-channels-update-chan-buffer)
          state-atom (atom {:id->failed-instance {}
                            :id->healthy-instance {}
                            :last-update-time nil
                            :watch-chans #{}})
          query-state-fn
          (fn instance-tracker-query-state-fn
            [include-flags]
            (let [{:keys [id->failed-instance id->healthy-instance last-update-time watch-chans]} @state-atom]
              (cond-> {:last-update-time last-update-time
                       :supported-include-params ["buffer-state" "id->failed-instance" "id->healthy-instance"
                                                  "instance-failure-handler"]
                       :watch-count (count watch-chans)}
                      (contains? include-flags "buffer-state")
                      (assoc :buffer-state {:instance-watch-channels-update-chan-count
                                            (.count instance-watch-channels-update-chan-buffer)})
                      (contains? include-flags "id->failed-instance")
                      (assoc :id->failed-instance id->failed-instance)
                      (contains? include-flags "id->healthy-instance")
                      (assoc :id->healthy-instance id->healthy-instance)
                      (contains? include-flags "instance-failure-handler")
                      (assoc :instance-failure-handler (state instance-failure-handler-component include-flags)))))

          go-chan
          (async/go
            (try
              (loop [{:keys [id->failed-instance id->healthy-instance watch-chans] :as current-state} @state-atom]
                (reset! state-atom current-state)
                (let [external-event-cid (utils/unique-identifier)
                      processing-cid (str "instance-tracker" "." external-event-cid)
                      [msg current-chan] (async/alts! [exit-chan router-state-chan instance-watch-channels-update-chan
                                                       query-chan] :priority true)
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
                          (cid/with-correlation-id
                            processing-cid
                            (let [{:keys [service-id->failed-instances service-id->healthy-instances]} msg
                                  id->failed-instance' (make-id->instance service-id->failed-instances)
                                  {new-failed-instances :new-instances}
                                  (get-new-changed-and-old-instances id->failed-instance id->failed-instance')
                                  id->healthy-instance' (make-id->instance service-id->healthy-instances)
                                  {changed-healthy-instances :changed-instances
                                   new-healthy-instances :new-instances
                                   removed-healthy-instances :old-instances}
                                  (get-new-changed-and-old-instances id->healthy-instance id->healthy-instance')
                                  updated-healthy-instances (concat new-healthy-instances changed-healthy-instances)]
                              (when (not-empty new-failed-instances)
                                (log/info "new failed instances" {:new-failed-instances new-failed-instances})
                                (handle-instances-event! instance-failure-handler-component {:new-failed-instances new-failed-instances}))
                              (when (not-empty updated-healthy-instances)
                                (log/info "new healthy instances" {:new-healthy-instances (map :id new-healthy-instances)}))
                              (when (not-empty removed-healthy-instances)
                                (log/info "removed healthy instances" {:removed-healthy-instances (map :id removed-healthy-instances)}))
                              (let [healthy-instances-event
                                    (cond-> {}
                                            (not-empty updated-healthy-instances)
                                            (assoc :updated updated-healthy-instances)
                                            (not-empty removed-healthy-instances)
                                            (assoc :removed removed-healthy-instances))
                                    events {:healthy-instances healthy-instances-event}
                                    instance-event (make-instance-event external-event-cid :events events)
                                    watch-chans' (if (or (not-empty updated-healthy-instances)
                                                         (not-empty removed-healthy-instances))
                                                   ; only send event if there were changes to set of healthy-instances
                                                   (utils/send-event-to-channels! watch-chans instance-event)
                                                   watch-chans)]
                                (assoc current-state :id->failed-instance id->failed-instance'
                                                     :id->healthy-instance id->healthy-instance'
                                                     :watch-chans watch-chans')))))

                        instance-watch-channels-update-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "instance-tracker" "channel-update")
                          (cid/with-correlation-id
                            processing-cid
                            (log/info "received watch-chan" msg)
                            (let [watch-chan msg
                                  event-object {:healthy-instances {:updated (or (vals id->healthy-instance) [])}}
                                  initial-event (make-instance-event external-event-cid :initial event-object)]
                              (async/put! watch-chan initial-event)
                              (assoc current-state :watch-chans (conj watch-chans watch-chan)))))

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
       :instance-watch-channels-update-chan instance-watch-channels-update-chan
       :query-chan query-chan
       :query-state-fn query-state-fn})))

(defn handle-list-instances-request
  "Handle a request to list instances. Currently this endpoint only supports watch=true query parameter. The current list
  of healthy instances will be streamed first and subsequent changes in set of healthy instances will be streamed
  in the response."
  [instance-watch-channels-update-chan service-id->service-description-fn default-streaming-timeout-ms
   {:keys [ctrl request-method] :as req}]
  (try
    (case request-method
      :get
      (let [{:strs [service-id streaming-timeout] :as request-params} (-> req ru/query-params-request :query-params)]
        (if-let [configured-streaming-timeout-ms (if streaming-timeout
                                                   (utils/parse-int streaming-timeout)
                                                   default-streaming-timeout-ms)]
          (let [should-watch? (utils/request-flag request-params "watch")
                service-description-filter-predicate
                (sd/query-params->service-description-filter-predicate request-params)
                correlation-id (cid/get-correlation-id)
                watch-chan-xform
                (comp
                  (map
                    (fn event-filter [{:keys [id object type] :as event}]
                      (cid/cinfo correlation-id "received event from instance-tracker daemon" {:id id})
                      (cid/cinfo correlation-id "full instances event data received from instance-tracker daemon" {:event event})
                      (let [service-filter
                            (filter
                              (fn filter-service-id [inst]
                                (let [inst-service-id (:service-id inst)
                                      service-description
                                      (service-id->service-description-fn inst-service-id :effective? true)]
                                  (and
                                    (or (nil? service-id)
                                        (= inst-service-id service-id))
                                    (service-description-filter-predicate service-description)))))
                            updated-healthy-instances (some->> (get-in object [:healthy-instances :updated])
                                                               (sequence service-filter))
                            removed-healthy-instances (some->> (get-in object [:healthy-instances :removed])
                                                               (sequence service-filter))
                            new-object (cond-> {}
                                               (not-empty updated-healthy-instances)
                                               (assoc-in [:healthy-instances :updated] updated-healthy-instances)
                                               (not-empty removed-healthy-instances)
                                               (assoc-in [:healthy-instances :removed] removed-healthy-instances))]
                        {:id id
                         :object new-object
                         :type type})))
                  (filter
                    (fn empty-event? [{:keys [object type] :as event}]
                      (case type
                        :initial true
                        :events (not= {} object)
                        (throw (ex-info "Invalid event type provided" {:event event})))))
                  (map (fn [{:keys [id type] :as event}]
                         (cid/cinfo correlation-id "forwarding instances event to client" {:id id :type type})
                         (cid/cinfo correlation-id "full instances event data sent to watch client" {:event event})
                         (utils/clj->json event))))
                watch-chan-ex-handler-fn
                (fn watch-chan-ex-handler [e]
                  (async/put! ctrl e)
                  (cid/cerror correlation-id e "error during transformation of a instances watch event"))]
            (if should-watch?
              (let [watch-chan (async/chan 1024 watch-chan-xform watch-chan-ex-handler-fn)
                    watch-mult (async/mult watch-chan)
                    response-chan (async/chan 1024)
                    _ (async/tap watch-mult response-chan)
                    trigger-chan (au/latest-chan)
                    _ (async/tap watch-mult trigger-chan)]
                (async/go-loop [timeout-ch (async/timeout configured-streaming-timeout-ms)]
                  (let [[message chan] (async/alts! [trigger-chan timeout-ch] :priority true)]
                    (cond
                      (= chan trigger-chan)
                      (if (nil? message)
                        (log/info "watch-chan has been closed")
                        (recur (async/timeout configured-streaming-timeout-ms)))
                      :else                                 ;; timeout channel has been triggered
                      (do
                        (log/info "closing watch-chan due to streaming timeout" configured-streaming-timeout-ms "ms")
                        (async/close! watch-chan)))))
                (if (async/put! instance-watch-channels-update-chan watch-chan)
                  (do
                    (async/go
                      (let [data (async/<! ctrl)]
                        (log/info "closing watch-chan as ctrl channel has been triggered" {:data data})
                        (async/close! watch-chan)))
                    (utils/attach-waiter-source
                      {:body response-chan
                       :headers {"content-type" "application/json"}
                       :status http-200-ok}))
                  (utils/exception->response (ex-info "tokens-watch-channels-update-chan is closed!" {}) req)))
              (let [ex (ex-info "watch query parameter must be true, no other option is supported."
                                {:log-level :info
                                 :status http-400-bad-request
                                 :watch-param should-watch?})]
                (utils/exception->response ex req))))
          (let [ex (ex-info "streaming-timeout query parameter must be an integer"
                            {:log-level :info
                             :status http-400-bad-request
                             :streaming-timeout streaming-timeout})]
            (utils/exception->response ex req))))
      (let [ex (ex-info "Only GET supported" {:log-level :info
                                              :request-method request-method
                                              :status http-405-method-not-allowed})]
        (utils/exception->response ex req)))
    (catch Exception ex
      (utils/exception->response ex req))))
