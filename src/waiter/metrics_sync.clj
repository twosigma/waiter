;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.metrics-sync
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [qbits.jet.client.websocket :as ws]
            [waiter.async-utils :as au]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.utils :as utils]))

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

(defn register-router-ws
  "Registers the websocket request with the specified request-id into the agent's state.
   It also attaches a callback to deregister the request when the connection receives data on the `ctrl` channel."
  [{:keys [router-id] :as router-metrics-state} router-ws-key source-router-id {:keys [ctrl request-id] :as ws-request} encrypt router-metrics-agent]
  (with-catch
    router-metrics-state
    (if request-id
      (do
        (cid/cdebug request-id "registering request from router" source-router-id)
        (counters/inc! (metrics/waiter-counter "metrics-syncer" (router-ws-key->name router-ws-key) (str source-router-id) "register"))
        (when ctrl
          (let [control-mult (async/mult ctrl)
                request-terminate-chan (async/tap control-mult (au/sliding-buffer-chan 1))]
            (async/go
              (when-let [ctrl-data (async/<! request-terminate-chan)]
                (cid/cinfo request-id "received" ctrl-data "in control channel, deregistering request")
                (send router-metrics-agent deregister-router-ws router-ws-key source-router-id request-id encrypt)))))
        (assoc-in router-metrics-state [router-ws-key source-router-id]
                  (select-keys ws-request [:in :ctrl :out :request-id :tag])))
      (do
        (log/warn "not registering request as it is missing request id")
        router-metrics-state))))

(defn update-router-metrics
  "Updates the agent state with the latest metrics from a router.
   It will remove entries for missing services and only update leaf level values for data available from services."
  [router-metrics-state {:keys [router-metrics source-router-id time]}]
  (with-catch
    router-metrics-state
    (if source-router-id
      (-> router-metrics-state
          (update-in [:metrics :routers source-router-id]
                     (fn [existing-router-metrics]
                       (utils/deep-merge-maps (fn [x y] (or x y)) router-metrics
                                              (select-keys existing-router-metrics (keys router-metrics)))))
          (assoc-in [:last-update-times source-router-id] time))
      router-metrics-state)))

(defn- process-incoming-router-metrics
  "Receives peer router metrics data and forwards it for processing in `router-metrics-agent`.
   The rate of receiving metrics is throttled by `metrics-sync-interval-ms` ms."
  [source-router-id encrypt decrypt router-metrics-agent metrics-sync-interval-ms {:keys [in out request-id]}]
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
            (send router-metrics-agent update-router-metrics decrypted-data)
            (async/timeout metrics-sync-interval-ms) ; throttle rate of receiving metrics
            (recur)))
        (do
          (cid/cinfo request-id "deregistering router socket as recevied nil data from router" source-router-id)
          (send router-metrics-agent deregister-router-ws :router-id->incoming-ws source-router-id request-id encrypt))))))

(defn incoming-router-metrics-handler
  "Receive connections for metrics from peer routers.
   Once the connection is authenticated, it invokes `process-incoming-router-metrics ` for receiving and processing metrics."
  [router-metrics-agent metrics-sync-interval-ms encrypt decrypt {:keys [in out] :as request}]
  (async/go
    (try
      (let [raw-data (async/<! in)
            {:keys [request-id source-router-id] :as data} (when raw-data (decrypt raw-data))
            request-id (or request-id (str "metrics-" (utils/unique-identifier)))
            request (assoc request :request-id request-id)]
        (cid/cinfo request-id "received request from router" source-router-id)
        (if (nil? source-router-id)
          (close-router-metrics-request request (encrypt {:message "Missing source router!", :data data}))
          (do
            (async/>! out :authenticated)
            (send router-metrics-agent register-router-ws :router-id->incoming-ws source-router-id request encrypt router-metrics-agent)
            (process-incoming-router-metrics source-router-id encrypt decrypt router-metrics-agent metrics-sync-interval-ms request))))
      (catch Exception e
        (log/error e "error in processing incoming router metrics request")))))

(defn preserve-metrics-from-routers
  "Removes last-update-time and metrics entries for obsolete routers from the agent state."
  [router-metrics-state router-ids]
  (with-catch
    router-metrics-state
    (let [router-ids-to-delete (set/difference (-> (get-in router-metrics-state [:metrics :routers]) (keys) (set))
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
  [{:keys [router-id router-id->outgoing-ws] :as router-metrics-state} encrypt router-metrics tag]
  (with-catch
    router-metrics-state
    (let [time (utils/date-to-str (t/now))
          metrics-data {:router-metrics router-metrics, :source-router-id router-id, :time time}]
      (loop [[[target-router-id {:keys [out request-id]}] & remaining-router-id->outgoing-ws] (seq router-id->outgoing-ws)]
        (if target-router-id
          (let [encrypted-data (timers/start-stop-time!
                                 (metrics/waiter-timer "metrics-syncer" "encrypt" target-router-id)
                                 (encrypt metrics-data))]
            (cid/cdebug request-id "sending" tag "metrics to" target-router-id "of size" (count router-metrics))
            (meters/mark! (metrics/waiter-meter "metrics-syncer" "sent-rate" target-router-id))
            (meters/mark! (metrics/waiter-meter "metrics-syncer" "sent-bytes" target-router-id) (.capacity encrypted-data))
            (async/put! out encrypted-data)
            (recur remaining-router-id->outgoing-ws))
          (update-router-metrics router-metrics-state metrics-data))))))

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
   router-id->http-endpoint encrypt connect-options router-metrics-agent]
  (with-catch
    router-metrics-state
    (let [my-router-id (:router-id router-metrics-state)
          known-router-ids (disj (set (keys router-id->http-endpoint)) my-router-id)
          prev-incoming-router-ids (set (keys router-id->incoming-ws))
          prev-outgoing-router-ids (set (keys router-id->outgoing-ws))]
      (if (or (not= known-router-ids prev-incoming-router-ids) (not= known-router-ids prev-outgoing-router-ids))
        (let [router-id->incoming-ws' (cleanup-router-requests :router-id->incoming-ws known-router-ids encrypt router-metrics-state)
              router-id->outgoing-ws' (cleanup-router-requests :router-id->outgoing-ws known-router-ids encrypt router-metrics-state)
              new-outgoing-router-ids (->> (:router-id->outgoing-ws router-metrics-state) (keys) (set) (set/difference known-router-ids))]
          (cid/cinfo "metrics-router-syncer" "new routers:" new-outgoing-router-ids)
          (doseq [router-id new-outgoing-router-ids]
            (let [ws-endpoint (-> (get router-id->http-endpoint router-id)
                                  (str/replace "http://" "ws://")
                                  (str "router-metrics"))
                  request-id (str "inter-router-metrics-" (utils/unique-identifier))]
              (cid/cinfo request-id "connecting to" router-id "at" ws-endpoint)
              (ws/connect! ws-endpoint
                           (fn register-outgoing-request [{:keys [out] :as ws-request}]
                             (let [ws-request (assoc ws-request :request-id request-id)]
                               (async/>!! out (encrypt {:dest-router-id router-id
                                                        :request-id request-id
                                                        :source-router-id my-router-id
                                                        :tag :initiate}))
                               (send router-metrics-agent register-router-ws :router-id->outgoing-ws router-id
                                     ws-request encrypt router-metrics-agent)))
                           connect-options)))
          (-> (preserve-metrics-from-routers router-metrics-state (set/union #{my-router-id} (set (keys router-id->http-endpoint))))
              (assoc :router-id->incoming-ws router-id->incoming-ws'
                     :router-id->outgoing-ws router-id->outgoing-ws')))
        router-metrics-state))))

(defn setup-router-syncer
  "Go-block that listens along the router channel for router state and propagates it to the router-metrics agent.
   The rate of listening for router state updates is throttle at `router-update-interval-ms`."
  [router-state-chan router-metrics-agent router-update-interval-ms
   inter-router-metrics-idle-timeout-ms metrics-sync-interval-ms encrypt]
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
                                     :out au/latest-chan}]
                (send router-metrics-agent update-metrics-router-state router-id->http-endpoint
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
  [router-metrics-agent metrics-sync-interval-ms encrypt]
  (let [exit-chan (async/chan 1)
        query-chan (async/chan 1)]
    (async/go-loop [iteration 0
                    timeout-chan (async/timeout metrics-sync-interval-ms)]
      (let [[data channel] (async/alts! [exit-chan timeout-chan query-chan] :priority true)]
        (condp = channel
          exit-chan
          (when (not= :exit data)
            (recur (inc iteration) timeout-chan))

          timeout-chan
          (do
            (cid/with-correlation-id
              (str "setup-metrics-syncer-" iteration)
              (try
                (let [service->metrics (utils/filterm (fn [[_ metrics]] (some pos? (vals metrics)))
                                                      (metrics/get-core-metrics))]
                  (send router-metrics-agent publish-router-metrics encrypt service->metrics "core"))
                (catch Exception e
                  (log/error e "error in making broadcast router metrics request" {:iteration iteration}))))
            (recur (inc iteration) (async/timeout metrics-sync-interval-ms)))

          query-chan
          (let [{:keys [response-chan]} data]
            (async/>! response-chan {:iteration iteration})
            (recur iteration timeout-chan)))))
    {:exit-chan exit-chan
     :query-chan query-chan}))

(defn new-router-metrics-agent
  "Factory method for the router metrics agent."
  [router-id agent-initial-state]
  (let [initial-state (merge {:last-update-times {}
                              :metrics {:routers {}}
                              :router-id router-id
                              :router-id->incoming-ws {}
                              :router-id->outgoing-ws {}}
                             (dissoc agent-initial-state :router-id))
        metrics-agent (agent initial-state)]
    metrics-agent))

(defn agent->service-id->metrics
  "Retrieves aggregated view of service-id->metrics using data available from all peer routers in the agent."
  [router-metrics-agent]
  (try
    (let [router-id->service-id->metrics (get-in @router-metrics-agent [:metrics :routers])
          service-ids (->> router-id->service-id->metrics
                           (vals)
                           (map keys)
                           (map set)
                           (reduce set/union #{}))]
      (log/info "aggregating metrics for" (count service-ids) "services from" (count router-id->service-id->metrics) "routers")
      (loop [[service-id & remaining-service-ids] (seq service-ids)
             service-id->metrics {}]
        (if-not service-id
          service-id->metrics
          (let [router->metrics (pc/map-vals (fn [service-id->metrics] (service-id->metrics service-id))
                                             router-id->service-id->metrics)
                aggregate-metrics (try
                                    (->> router->metrics (utils/filterm val) (metrics/aggregate-router-data))
                                    (catch Exception e
                                      (log/error e "error in retrieving aggregated metrics for" service-id)))]
            (if aggregate-metrics
              (recur remaining-service-ids (assoc service-id->metrics service-id aggregate-metrics))
              (recur remaining-service-ids service-id->metrics))))))
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
       (log/error e "error in obtaining router->metrics data"))))
