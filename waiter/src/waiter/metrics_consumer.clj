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
(ns waiter.metrics-consumer
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metrics.meters :as meters]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.service-description :as sd]
            [waiter.token :as token]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils]))

(defn make-metrics-watch-request
  "Creates a watch request to a metrics service and pushes token events to the provided event-chan. If the watch request
  fails for any reason, it will retry after the retry-delay-ms. The metrics service needs to stream chunked json on the
  /token-stats?watch=true endpoint. Each json blob has the structure:
  {:object [
    {:token \"token\"
     :lastRequestTime iso-time-string}
    ...]
   :type \"initial\" or \"update\"}

  A map is returned with keys:
  :exit-chan is a channel that when a message is pushed to, terminates the watch immediately and go-chan
  :go-chan channel closed when watch is terminated (used primarily for cleaning up tests)
  :query-state-fn function called with include flags for introspecting the state of the watch (e.g. last-event-time)"
  [http-client http-streaming-request-async-fn router-id cur-cid {:keys [cluster url] :as metrics-service-conf}
   event-chan clock retry-delay-ms]
  (cid/with-correlation-id
    (str cur-cid ".metrics-watch." cluster)
    (let [_ (log/info "starting metrics-service-watch" {:metrics-service-conf metrics-service-conf})
          exit-chan (async/promise-chan)
          exit-chan-mult (async/mult exit-chan)
          state-atom (atom {:last-event-time nil
                            :last-watch-time nil})
          metrics-endpoint (str url "/token-stats")
          query-string (str "watch=true&watcher=" router-id)
          query-state-fn
          (fn query-metrics-watch-fn
            [_]
            (let [{:keys [last-event-time last-watch-time]} @state-atom]
              {:endpoint metrics-endpoint
               :last-event-time last-event-time
               :last-watch-time last-watch-time
               :supported-include-params []}))
          go-chan
          (async/go-loop []
            (try
              (log/info "starting watch on metrics service:" {:metrics-endpoint metrics-endpoint})
              (let [abort-ch (async/promise-chan)
                    {:keys [body-chan error-chan] :as res}
                    (async/<! (http-streaming-request-async-fn http-client metrics-endpoint
                                                               :abort-ch abort-ch
                                                               :query-string query-string))]
                ; when async chan is triggered, we want to also trigger the abort channel on the watch connection
                (async/tap exit-chan-mult abort-ch)
                (when (instance? Throwable res)
                  (throw res))
                (swap! state-atom assoc :last-watch-time (clock))
                (doseq [{:strs [object type]} (utils/chan-to-json-seq!! body-chan)]
                  (meters/mark! (metrics/waiter-meter "core" "metrics-consumer" cluster "event-rate"))
                  (if (contains? #{"initial" "update"} type)
                    (do
                      (log/info "received event payload" {:count (count object) :type type})
                      (swap! state-atom assoc :last-event-time (clock))
                      (doseq [event object]
                        (async/put! event-chan event)))
                    (log/warn "received unknown metrics event" {:type type})))
                (log/warn "watch request was closed unexpectedly" {:error-chan (when error-chan (async/<! error-chan))
                                                                   :metrics-endpoint metrics-endpoint}))
              (catch Exception e
                (log/error e "watch request to external request metrics service failed. Going to start retrying"
                           {:url url})))
            ; unnecessary to do exponential backoff because there is a static number of clients (waiter routers) making watch requests
            (log/info (str "waiting " retry-delay-ms " ms before attempting to make the watch connection"))
            (let [timeout-ch (async/timeout retry-delay-ms)
                  [msg current-chan]
                  (async/alts! [exit-chan timeout-ch] :priority true)]
              (condp = current-chan
                exit-chan (log/warn "exiting watch because exit chan was triggered" {:msg msg})
                timeout-ch (recur))))]
      {:exit-chan exit-chan
       :go-chan go-chan
       :query-state-fn query-state-fn})))

(defn start-metrics-consumer-maintainer
  "Initializes all external metrics service watch connections and filters/processes metric events that are then exposed
  for other components to listen on to with `async/tap`.

  Returns a map with keys:
  :exit-chan is as channel that when message is pushed to it, causes all watch connections to be terminated
  :query-state-fn is a query function for introspecting the state of the metrics events channels and watches
  :token-metric-chan-mult is a mult for other components to listen in on when last-request-time was updated for a token"
  [http-client clock kv-store token-cluster-calculator retrieve-descriptor-fn service-id->metrics-fn
   make-metrics-watch-request-fn local-usage-agent router-id metrics-services token-metric-chan-buffer-size
   retry-delay-ms]
  (cid/with-correlation-id
    "metrics-consumer-maintainer"
    (let [exit-chan (async/promise-chan)
          exit-mult (async/mult exit-chan)
          correlation-id (cid/get-correlation-id)
          default-cluster (token/get-default-cluster token-cluster-calculator)
          state-atom (atom {:last-token-event-time nil})
          token-metric-chan-buffer (async/buffer token-metric-chan-buffer-size)
          token-metric-chan
          (async/chan
            token-metric-chan-buffer
            (comp
              (map (fn format-event
                     [{:strs [lastRequestTime token]}]
                     {:last-request-time (f/parse lastRequestTime)
                      :token token}))
              (filter (fn token-in-same-cluster?
                        [{:keys [token]}]
                        (let [token-parameters (sd/token->token-parameters kv-store token :error-on-missing false)
                              token-cluster (get token-parameters "cluster")]
                          (and (some? token-parameters)
                               (= token-cluster default-cluster)))))
              ; TODO: bypass only supports non run-as-requester and parameterized services
              (filter (fn token-bypass-eligible-service-description?
                        [{:keys [token]}]
                        (when-let [{:strs [run-as-user] :as service-description-template}
                                   (sd/token->service-parameter-template kv-store token :error-on-missing false)]
                          (and run-as-user
                               (not (sd/run-as-requester? service-description-template))
                               (not (sd/requires-parameters? service-description-template))))))
              (filter (fn is-new-last-request-time?
                        [{:keys [token last-request-time]}]
                        (let [{:strs [run-as-user]}
                              (sd/token->service-parameter-template kv-store token :error-on-missing false)
                              {:keys [descriptor]} (retrieve-descriptor-fn run-as-user token)
                              {fallback-service-id :service-id} descriptor
                              stored-last-request-time (get-in (service-id->metrics-fn) [fallback-service-id "last-request-time"])
                              new-last-request-time? (or (nil? stored-last-request-time)
                                                         (t/before? stored-last-request-time last-request-time))]
                          (when new-last-request-time?
                            (cid/cinfo correlation-id "updating last request time for service" {:service-id fallback-service-id
                                                                                                :last-request-time last-request-time})
                            (send local-usage-agent metrics/update-last-request-time-usage-metric fallback-service-id last-request-time)
                            (swap! state-atom assoc :last-token-event-time (clock)))
                          new-last-request-time?))))
            (fn metric-chan-ex-handler
              [e]
              (cid/cerror correlation-id e "unexpected error when transforming token metric event")))
          metrics-service-url->conf (pc/map-from-vals :url metrics-services)
          metrics-service-url->watch
          (pc/map-vals
            (fn [metrics-service-conf]
              (let [{:keys [exit-chan] :as watch}
                    (make-metrics-watch-request-fn
                      http-client hu/http-streaming-request-async router-id correlation-id metrics-service-conf
                      token-metric-chan clock retry-delay-ms)]
                (async/tap exit-mult exit-chan)
                watch))
            metrics-service-url->conf)
          query-state-fn
          (fn query-metrics-consumer-maintainer-fn
            [include-flags]
            (let [{:keys [last-token-event-time]} @state-atom]
              (cond-> {:last-token-event-time last-token-event-time
                       :buffer-state {:token-metric-chan-count (.count token-metric-chan-buffer)}
                       :supported-include-params ["watches-state"]}
                (contains? include-flags "watches-state")
                (assoc :watches-state
                       (pc/map-vals
                         (fn [{:keys [query-state-fn]}]
                           (query-state-fn include-flags))
                         metrics-service-url->watch)))))]
      (metrics/waiter-gauge #(.count token-metric-chan-buffer)
                            "core" "metrics-consumer" "token-metric-chan-count")
      {:exit-chan exit-chan
       :query-state-fn query-state-fn
       :token-metric-chan-mult (async/mult token-metric-chan)})))
