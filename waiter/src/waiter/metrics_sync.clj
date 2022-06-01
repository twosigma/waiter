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
(ns waiter.metrics-sync
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [qbits.jet.client.websocket :as ws]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (qbits.jet.websocket WebSocket)))

(defmacro with-catch
  [default-value & body]
  `(try
     ~@body
     (catch Throwable th#
       (log/error th# "error in processing agent message")
       (counters/inc! (metrics/waiter-counter "metrics-syncer" "errors"))
       ~default-value)))

(defn- close-router-metrics-request
  "Closes the websocket request after sending it a message."
  [{:keys [out request-id]} message]
  (cid/cdebug request-id "closing request" message)
  (async/go
    (async/put! out message)
    (async/close! out)))

(defmacro router-ws-key->name
  "Generates a display name from the `router-ws-key`."
  [router-ws-key]
  `(let [key-name# (name ~router-ws-key)]
     (cond
       (str/includes? key-name# "incoming") "incoming"
       (str/includes? key-name# "outgoing") "outgoing"
       :else key-name#)))

(defn deregister-router-ws
  "Deregisters the websocket request with the specified request-id from the agent's state."
  [router-metrics-state router-ws-key router-id request-id encrypt]
  (with-catch
    router-metrics-state
    (let [ws-request (get-in router-metrics-state [router-ws-key router-id])]
      (if (= request-id (:request-id ws-request))
        (do
          (cid/cinfo request-id "deregistering request from router" router-id)
          (counters/inc! (metrics/waiter-counter "metrics-syncer" (router-ws-key->name router-ws-key) router-id "deregister"))
          (close-router-metrics-request ws-request (encrypt {:message "deregistering existing websocket request"}))
          (utils/dissoc-in router-metrics-state [router-ws-key router-id]))
        (do
          (cid/cinfo "metrics-router-syncer" "ignoring deregister request for" request-id
                     ", current" [router-ws-key router-id] "request-id is" (:request-id ws-request))
          router-metrics-state)))))

(defn- listen-on-ctrl-chan
  "Deregister any requests corresponding to request-id on router-ws-key when data is received on ctrl channel."
  [ctrl router-ws-key router-id request-id encrypt router-metrics-agent]
  (async/go
    (when-let [ctrl-data (async/<! ctrl)]
      (cid/cinfo request-id "triggering deregister, data received on control channel is" ctrl-data)
      (send router-metrics-agent deregister-router-ws router-ws-key router-id request-id encrypt))))

(defn register-router-ws
  "Registers the websocket request with the specified request-id into the agent's state.
   It also attaches a callback to deregister the request when the connection receives data on the `ctrl` channel."
  [router-metrics-state router-ws-key router-id {:keys [ctrl request-id] :as ws-request} encrypt router-metrics-agent]
  (with-catch
    router-metrics-state
    (if request-id
      (do
        (cid/cinfo request-id "registering" (name router-ws-key) "request" router-id)
        (counters/inc! (metrics/waiter-counter "metrics-syncer" (router-ws-key->name router-ws-key) router-id "register"))
        (if ctrl
          (do
            (listen-on-ctrl-chan ctrl router-ws-key router-id request-id encrypt router-metrics-agent)
            (assoc-in router-metrics-state [router-ws-key router-id]
                      (select-keys ws-request [:ctrl :in :out :request-id :time])))
          (do
            (cid/cerror request-id "not registering request as no ctrl-chan available to monitor request")
            router-metrics-state)))
      (do
        (log/error "not registering request as it is missing request id")
        router-metrics-state))))

(defn- clean-service-id->instance-id->metric
  "Remove services from outer map that are not tracked by this router. Remove instances in the instance-id->metric map
  that are not tracked by this router. This is done to prevent memory leaks in the service-id->instance-id->metric map"
  [service-id-exists?-fn service-id-instance-id-active?-fn service-id->instance-id->metric]
  (->> service-id->instance-id->metric
       (utils/select-keys-pred service-id-exists?-fn)
       keys
       (pc/map-from-keys
         (fn [service-id]
           (let [instance-id->metric (get service-id->instance-id->metric service-id)]
             (utils/select-keys-pred
               (fn [instance-id]
                 (service-id-instance-id-active?-fn service-id instance-id))
               instance-id->metric))))))

(defn- merge-instance-id->metric-maps
  "Merges two instance-id->metric maps iterating through the instance-ids in both maps and checking which map has the
  latest metric based on 'updated-at'. If the metric does not exist in one of the maps, then defaults to the non nil
  metric."
  [instance-id->metric-1 instance-id->metric-2]
  (pc/map-from-keys
    (fn [instance-id]
      (let [metric-1 (get instance-id->metric-1 instance-id)
            updated-at-1 (get metric-1 "updated-at")
            metric-2 (get instance-id->metric-2 instance-id)
            updated-at-2 (get metric-2 "updated-at")]
        (if (and (some? updated-at-1) (some? updated-at-2))
          (let [updated-at-1 (du/str-to-date updated-at-1)
                updated-at-2 (du/str-to-date updated-at-2)]
            (if (t/before? updated-at-1 updated-at-2)
              metric-2
              metric-1))
          (if (some? updated-at-1) metric-1 metric-2))))
    (set (concat (keys instance-id->metric-1) (keys instance-id->metric-2)))))

(defn- merge-service-id->instance-id->metric-maps
  "Merges two service-id->instance-id maps by calling merge-instance-id->metric-maps on the values with the same
  service-id. This will ultimately merge two metrics for the same instance-id by choose the latest :updated-at or non
  nil metric. The result will be filtered, and will only contain service-ids and instance-ids that are known by the
  current router."
  [service-id->instance-id->metric-1 service-id->instance-id->metric-2 service-id-exists?-fn service-id-instance-id-active?-fn]
  (let [map-1 (clean-service-id->instance-id->metric
                service-id-exists?-fn service-id-instance-id-active?-fn service-id->instance-id->metric-1)
        map-2 (clean-service-id->instance-id->metric
                service-id-exists?-fn service-id-instance-id-active?-fn service-id->instance-id->metric-2)]
    (pc/map-from-keys
      (fn [service-id]
        (merge-instance-id->metric-maps
          (get map-1 service-id)
          (get map-2 service-id)))
      (set (concat (keys map-1) (keys map-2))))))

(defn update-router-metrics
  "Updates the agent state with the latest metrics from a router.
   It will remove entries for missing services and only update leaf level values for data available from services."
  [router-metrics-state service-id-exists?-fn service-id-instance-id-active?-fn
   {:keys [external-metrics router-metrics source-router-id time]}]
  (with-catch
    router-metrics-state
    (if source-router-id
      (-> router-metrics-state
          (update-in [:metrics :routers source-router-id]
                     (fn [existing-router-metrics]
                       (utils/deep-merge-maps (fn [x y] (or x y)) router-metrics
                                              (select-keys existing-router-metrics (keys router-metrics)))))
          (assoc-in [:last-update-times source-router-id] time)

          ; :external-metrics are merged based on 'updated-at' timestamp for individual instance metrics.
          ; These metrics are absolute (one per waiter cluster, instead of waiter router), as they are provided
          ; by an external source periodically with the /instance-metrics endpoint.
          (update :external-metrics merge-service-id->instance-id->metric-maps external-metrics service-id-exists?-fn
                  service-id-instance-id-active?-fn))
      router-metrics-state)))

(defn update-router-metrics-with-external-metrics
  "Merges the service external metrics with existing external service metrics. External metrics are provided to the
  waiter routers from another entity. These metrics must be merged based on the 'updated-at' timestamp."
  [router-metrics-state incoming-service-id->instance-id->metric service-id-exists?-fn service-id-instance-id-active?-fn]
  (with-catch
    router-metrics-state
    (-> router-metrics-state
        (update :external-metrics merge-service-id->instance-id->metric-maps incoming-service-id->instance-id->metric
                service-id-exists?-fn service-id-instance-id-active?-fn))))

(defn- process-incoming-router-metrics
  "Receives peer router metrics data and forwards it for processing in `router-metrics-agent`.
   The rate of receiving metrics is throttled by `metrics-sync-interval-ms` ms."
  [source-router-id encrypt decrypt router-metrics-agent metrics-sync-interval-ms service-id-exists?-fn
   service-id-instance-id-active?-fn {:keys [in out request-id]}]
  (let [in-latest-chan (au/latest-chan)]
    (async/pipe in in-latest-chan) ; consume data from `in`
    (async/go-loop []
      (if-let [received-data (async/<! in-latest-chan)]
        (do
          (async/>! out :ack) ; acknowledge receipt of data and reset the idle timeout
          (meters/mark! (metrics/waiter-meter "metrics-syncer" "received-rate" source-router-id))
          (meters/mark! (metrics/waiter-meter "metrics-syncer" "received-bytes" source-router-id) (.capacity received-data))
          (let [decrypted-data (timers/start-stop-time!
                                 (metrics/waiter-timer "metrics-syncer" "decrypt" source-router-id)
                                 (decrypt received-data))]
            (cid/cdebug request-id "received metrics data from router" source-router-id)
            (send router-metrics-agent update-router-metrics service-id-exists?-fn service-id-instance-id-active?-fn decrypted-data)
            (async/<! (async/timeout metrics-sync-interval-ms)) ; throttle rate of receiving metrics
            (recur)))
        (do
          (cid/cinfo request-id "deregistering router socket as received nil data from router" source-router-id)
          (send router-metrics-agent deregister-router-ws :router-id->incoming-ws source-router-id request-id encrypt))))))

(defn incoming-router-metrics-handler
  "Receive connections for metrics from peer routers.
   Once the connection is authenticated, it invokes `process-incoming-router-metrics ` for receiving and processing metrics."
  [router-metrics-agent metrics-sync-interval-ms encrypt decrypt service-id-exists?-fn service-id-instance-id-active?-fn
   {:keys [in out] :as request}]
  (async/go
    (try
      (let [raw-data (async/<! in)
            {:keys [request-id source-router-id] :as data} (when raw-data (decrypt raw-data))
            request-id (or request-id (str "metrics-" (utils/unique-identifier)))
            request (assoc request :request-id request-id :time (t/now))]
        (cid/cinfo request-id "received request from router" source-router-id)
        (if (nil? source-router-id)
          (close-router-metrics-request request (encrypt {:message "Missing source router!", :data data}))
          (do
            (async/>! out :authenticated)
            (send router-metrics-agent register-router-ws :router-id->incoming-ws source-router-id request encrypt router-metrics-agent)
            (process-incoming-router-metrics source-router-id encrypt decrypt router-metrics-agent metrics-sync-interval-ms
                                             service-id-exists?-fn service-id-instance-id-active?-fn request))))
      (catch Exception e
        (log/error e "error in processing incoming router metrics request")))
    ;; return an empty response map to maintain consistency with the http case
    {}))

(defn preserve-metrics-from-routers
  "Removes last-update-time and metrics entries for obsolete routers from the agent state."
  [router-metrics-state router-ids]
  (with-catch
    router-metrics-state
    (let [router-ids-to-delete (set/difference (-> (get-in router-metrics-state [:metrics :routers])
                                                   keys
                                                   set)
                                               (set router-ids))]
      (loop [[router-id-to-delete & remaining-ids] (seq router-ids-to-delete)
             loop-state router-metrics-state]
        (if router-id-to-delete
          (let [loop-state' (-> loop-state
                                (utils/dissoc-in [:metrics :routers router-id-to-delete])
                                (utils/dissoc-in [:last-update-times router-id-to-delete]))]
            (recur remaining-ids loop-state'))
          loop-state)))))

(defn publish-router-metrics
  "Publishes router metrics to peer routers."
  [{:keys [router-id router-id->outgoing-ws external-metrics] :as router-metrics-state} encrypt router-metrics tag
   service-id-exists?-fn service-id-instance-id-active?-fn]
  (with-catch
    router-metrics-state
    (let [time (du/date-to-str (t/now))
          metrics-data {:external-metrics external-metrics
                        :router-metrics router-metrics,
                        :source-router-id router-id,
                        :time time}]
      (doseq [[target-router-id {:keys [out request-id]}] (seq router-id->outgoing-ws)]
        (let [encrypted-data (timers/start-stop-time!
                               (metrics/waiter-timer "metrics-syncer" "encrypt" target-router-id)
                               (encrypt metrics-data))]
          (cid/cdebug request-id "sending" tag "metrics to" target-router-id "of size" (count router-metrics))
          (meters/mark! (metrics/waiter-meter "metrics-syncer" "sent-rate" target-router-id))
          (meters/mark! (metrics/waiter-meter "metrics-syncer" "sent-bytes" target-router-id) (.capacity encrypted-data))
          (async/put! out encrypted-data)))
      (update-router-metrics router-metrics-state service-id-exists?-fn service-id-instance-id-active?-fn metrics-data))))

(defn- cleanup-router-requests
  "Close and remove websocket connections for obsolete routers."
  [router-ws-key known-router-ids encrypt router-metrics-state]
  (let [router-id->requests (get-in router-metrics-state [router-ws-key])
        obsolete-router-id->requests (utils/filterm #(not (contains? known-router-ids (first %))) router-id->requests)]
    (if (seq obsolete-router-id->requests)
      (do
        (cid/cinfo "metrics-router-syncer" "obsolete routers in" (router-ws-key->name router-ws-key) "are" (keys obsolete-router-id->requests))
        (doseq [[router-id {:keys [request-id]}] (seq obsolete-router-id->requests)]
          (deregister-router-ws router-metrics-state router-ws-key router-id request-id encrypt))
        (utils/filterm #(contains? known-router-ids (first %)) router-id->requests))
      router-id->requests)))

(defn update-metrics-router-state
  "Updates the internal state with knowledge of new routers.
   Will initiate websocket connections with new routers.
   Cleanup requests with obsolete routers."
  [{:keys [router-id->incoming-ws router-id->outgoing-ws] :as router-metrics-state}
   websocket-client router-id->http-endpoint encrypt connect-options router-metrics-agent]
  (with-catch
    router-metrics-state
    (let [my-router-id (:router-id router-metrics-state)
          known-router-ids (disj (-> router-id->http-endpoint keys set) my-router-id)
          prev-incoming-router-ids (-> router-id->incoming-ws keys set)
          prev-outgoing-router-ids (-> router-id->outgoing-ws keys set)]
      (if (or (not= known-router-ids prev-incoming-router-ids) (not= known-router-ids prev-outgoing-router-ids))
        (let [router-id->incoming-ws' (cleanup-router-requests :router-id->incoming-ws known-router-ids encrypt router-metrics-state)
              router-id->outgoing-ws' (cleanup-router-requests :router-id->outgoing-ws known-router-ids encrypt router-metrics-state)
              new-outgoing-router-ids (->> router-metrics-state
                                           :router-id->outgoing-ws
                                           keys
                                           set
                                           (set/difference known-router-ids))]
          (when (seq new-outgoing-router-ids)
            (cid/cinfo "metrics-router-syncer" "new routers:" new-outgoing-router-ids ", known routers" known-router-ids)
            (doseq [router-id new-outgoing-router-ids]
              (let [ws-endpoint (-> (get router-id->http-endpoint router-id)
                                    (str/replace "http://" "ws://")
                                    (str "waiter-router-metrics"))
                    request-id (str "inter-router-metrics-" (utils/unique-identifier))
                    _ (cid/cinfo request-id "connecting to" router-id "at" ws-endpoint)
                    {:keys [^WebSocket socket]}
                    (ws/connect! websocket-client ws-endpoint
                                 (fn register-outgoing-request [{:keys [out] :as ws-request}]
                                   (cid/cinfo request-id "successfully connected to" router-id)
                                   (async/>!! out (encrypt {:dest-router-id router-id
                                                            :request-id request-id
                                                            :source-router-id my-router-id
                                                            :tag :initiated}))
                                   (let [ws-request (assoc ws-request :request-id request-id :time (t/now))]
                                     (send router-metrics-agent register-router-ws :router-id->outgoing-ws router-id
                                           ws-request encrypt router-metrics-agent)))
                                 connect-options)
                    ctrl (.ctrl socket)]
                ;; register outside connect! callback to handle messages on the ctrl channel
                (listen-on-ctrl-chan ctrl :router-id->outgoing-ws router-id request-id encrypt router-metrics-agent))))
          (-> router-metrics-state
              (preserve-metrics-from-routers
                (set/union #{my-router-id} (-> router-id->http-endpoint keys set)))
              (assoc :router-id->incoming-ws router-id->incoming-ws'
                     :router-id->outgoing-ws router-id->outgoing-ws')))
        router-metrics-state))))

(defn setup-router-syncer
  "Go-block that listens along the router channel for router state and propagates it to the router-metrics agent.
   The rate of listening for router state updates is throttle at `router-update-interval-ms`."
  [router-state-chan router-metrics-agent router-update-interval-ms inter-router-metrics-idle-timeout-ms
   metrics-sync-interval-ms websocket-client encrypt attach-auth-cookie!]
  (let [exit-chan (async/chan 1)
        query-chan (async/chan 10)]
    (async/go-loop [iteration 0
                    timeouts 0
                    timeout-chan nil]
      (let [channels (cond-> [exit-chan]
                       (nil? timeout-chan) (conj router-state-chan)
                       timeout-chan (conj timeout-chan)
                       true (conj query-chan))
            [data channel] (async/alts! channels :priority true)]
        (condp = channel
          exit-chan
          (if (not= :exit data)
            (recur (inc iteration) timeouts timeout-chan)
            (log/info "exiting router-syncer"))

          router-state-chan
          (let [router-id->http-endpoint data]
            (when (seq router-id->http-endpoint)
              (let [connect-options {:async-write-timeout metrics-sync-interval-ms
                                     :in au/latest-chan
                                     :max-idle-timeout inter-router-metrics-idle-timeout-ms
                                     :middleware (fn router-syncer-middleware [_ request] (attach-auth-cookie! request))
                                     :out au/latest-chan}]
                (send router-metrics-agent update-metrics-router-state websocket-client router-id->http-endpoint
                      encrypt connect-options router-metrics-agent)))
            (recur (inc iteration) timeouts (async/timeout router-update-interval-ms)))

          timeout-chan
          (recur (inc iteration) (inc timeouts) nil)

          query-chan
          (let [{:keys [response-chan]} data]
            (async/>! response-chan {:iteration iteration, :timeouts timeouts})
            (recur iteration timeouts timeout-chan)))))
    {:exit-chan exit-chan
     :query-chan query-chan}))

(defn setup-metrics-syncer
  "Launches a go-block that trigger publishing of metrics with peer routers.
   `metrics-sync-interval-ms` is used to throttle the rate of sending metrics."
  [router-metrics-agent local-usage-agent metrics-sync-interval-ms encrypt service-id-exists?-fn
   service-id-instance-id-active?-fn]
  (let [exit-chan (async/chan 1)
        query-chan (async/chan 1)]
    (cid/with-correlation-id
      "setup-metrics-syncer"
      (async/go-loop [iteration 0
                      timeout-chan (async/timeout metrics-sync-interval-ms)]
        (let [[data channel] (async/alts! [exit-chan timeout-chan query-chan] :priority true)]
          (condp = channel
            exit-chan
            (when (not= :exit data)
              (recur (inc iteration) timeout-chan))

            timeout-chan
            (do
              (try
                (let [service-id->usage-metrics @local-usage-agent
                      service-id->metrics (timers/start-stop-time!
                                            (metrics/waiter-timer "metrics-syncer" "metrics" "computation")
                                            (let [service-id->codahale-metrics
                                                  (utils/filterm (fn [[_ metrics]] (some pos? (vals metrics)))
                                                                 (metrics/get-core-codahale-metrics))]
                                              (pc/map-from-keys
                                                (fn service-id->metrics-fn [service-id]
                                                  (merge (service-id->codahale-metrics service-id)
                                                         (service-id->usage-metrics service-id)))
                                                (keys service-id->codahale-metrics))))]
                  (metrics/reset-counter
                    (metrics/waiter-counter "metrics-syncer" "metrics" "services")
                    (count service-id->metrics))
                  (send router-metrics-agent publish-router-metrics encrypt service-id->metrics "core"
                        service-id-exists?-fn service-id-instance-id-active?-fn))
                (catch Exception e
                  (log/error e "error in making broadcast router metrics request" {:iteration iteration})))
              (recur (inc iteration) (async/timeout metrics-sync-interval-ms)))

            query-chan
            (let [{:keys [response-chan]} data]
              (async/>! response-chan {:iteration iteration})
              (recur iteration timeout-chan))))))
    {:exit-chan exit-chan
     :query-chan query-chan}))

(defn new-router-metrics-agent
  "Factory method for the router metrics agent."
  [router-id agent-initial-state]
  (let [initial-state (merge {:external-metrics {}
                              :last-update-times {}
                              :metrics {:routers {}}
                              :router-id router-id
                              :router-id->incoming-ws {}
                              :router-id->outgoing-ws {}}
                             (dissoc agent-initial-state :router-id))
        metrics-agent (agent initial-state)]
    metrics-agent))

(defn- merge-router-metrics
  "Merges all the metrics shared among routers.
   last-request-time is combined using the max operator.
   Counters are combined using sum reduction."
  [& maps]
  (when (some identity maps)
    (letfn [(merge-fn [key current-val new-val]
              (cond
                ;; last-request-time
                (= key "last-request-time") (t/max-date current-val new-val)
                ;; counters
                (every? number? [current-val new-val]) (+ current-val new-val)
                ;; error scenario
                :else (throw (ex-info "Unable to merge" {:current current-val :key key :new new-val}))))]
      (apply utils/merge-by merge-fn maps))))

(defn agent->service-id->metrics
  "Retrieves aggregated view of service-id->metrics using data available from all peer routers in the agent."
  [router-metrics-agent]
  (try
    (let [router-id->service-id->metrics (get-in @router-metrics-agent [:metrics :routers])
          aggregate-router-metrics (fn aggregate-router-metrics [router->metrics]
                                     (apply merge-router-metrics (vals router->metrics)))
          service-ids (->> router-id->service-id->metrics
                           (vals)
                           (map keys)
                           (map set)
                           (reduce set/union #{}))]
      (log/info "aggregating metrics for" (count service-ids) "services from" (count router-id->service-id->metrics)
                "routers with distribution" (pc/map-vals count router-id->service-id->metrics))
      (->> (seq service-ids)
           (pc/map-from-keys (fn [service-id]
                               (let [router->metrics (pc/map-vals (fn [service-id->metrics]
                                                                    (service-id->metrics service-id))
                                                                  router-id->service-id->metrics)]
                                 (try
                                   (->> router->metrics
                                        (utils/filterm val)
                                        aggregate-router-metrics)
                                   (catch Exception e
                                     (log/error e "error in retrieving aggregated metrics for" service-id))))))
           (filter second)
           (into {})))
    (catch Exception e
      (log/error e "unable to retrieve service-id->metrics"))))

(defn agent->service-id->router-id->metrics
  "Retrieves the `router-id->metrics` obtained by reading the current state of `router-metrics-agent`."
  [router-metrics-agent service-id]
  (log/debug "retrieving router-id->metrics for" service-id)
  (try
    (let [router->service-id->metrics (get-in @router-metrics-agent [:metrics :routers])]
      (pc/map-vals (fn [service-id->metrics] (service-id->metrics service-id))
                   router->service-id->metrics))
    (catch Exception e
      (log/error e "error in obtaining router-id->metrics data for" service-id))))

(defn handle-instance-metrics-request
  "Handle incoming external instance metrics and update metrics stored in memory. Expect the json body to be in the format
  service-id->instance-id->metric where the metric is:
  {'updated-at' ISO-8601 timestamp
   'metric': {'active-request-count' non-negative-int
              'last-request-time' ISO-8601 timestamp}}

   There may be extra fields provided in the metric at any level. We just validate that those fields are there in the
   correct format."
  [router-metrics-agent service-id-exists?-fn service-id-instance-id-active?-fn {:keys [request-method] :as request}]
  (when (not= request-method :post)
    (throw (ex-info "Invalid request method. Only POST is supported." {:log-level :info
                                                                       :request-method request-method
                                                                       :status http-400-bad-request})))
  (let [service-metrics (-> request
                            ru/json-request
                          :body)
        throw-error-response-if-invalid-fn
        (fn throw-error-response-if-invalid-fn
          [valid?-fn map keys error-msg]
          (let [val (get-in map keys)]
            (when (not (valid?-fn val))
              (throw (ex-info
                       (str "Invalid '" (str/join "." keys) "' field. " error-msg)
                       {:log-level :info
                        :status http-400-bad-request})))))

        valid-time-str?-fn (fn [time-str]
                             (try
                               (du/str-to-date-safe time-str)
                               true
                               (catch Exception _
                                 false)))
        invalid-time-error-msg "Must be ISO-8601 time."]

    ; throw error if any of the metrics for an instance is invalid, and any of the required metrics are missing
    (doseq [[service-id instance-id->metric] service-metrics]
      (doseq [[instance-id _] instance-id->metric]

        (throw-error-response-if-invalid-fn
          valid-time-str?-fn service-metrics [service-id instance-id "updated-at"] invalid-time-error-msg)

        (throw-error-response-if-invalid-fn
          valid-time-str?-fn service-metrics [service-id instance-id "metrics" "last-request-time"] invalid-time-error-msg)

        (throw-error-response-if-invalid-fn
          #(nat-int? %) service-metrics [service-id instance-id "metrics" "active-request-count"] "Must be non-negative integer.")))

    (log/info "received service metrics from external source." {:service-ids-preview (take 10 (keys service-metrics))
                                                                :service-ids-count (count (keys service-metrics))})
    (let [clean-service-metrics (clean-service-id->instance-id->metric
                                  service-id-exists?-fn service-id-instance-id-active?-fn service-metrics)]
      (log/info "removed irrelevant services and instances." {:service-ids-preview (take 10 (keys clean-service-metrics))
                                                              :service-ids-count (count (keys clean-service-metrics))})
      (when (not-empty clean-service-metrics)
        (send router-metrics-agent update-router-metrics-with-external-metrics clean-service-metrics
              service-id-exists?-fn service-id-instance-id-active?-fn))
      (utils/clj->json-response {:no-op (empty? clean-service-metrics)}))))
