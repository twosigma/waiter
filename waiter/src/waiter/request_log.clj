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
(ns waiter.request-log
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [waiter.metrics :as metrics]
            [waiter.util.date-utils :as du]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils]))

(defn log
  "Log log-data as JSON"
  [log-data]
  (log/log "RequestLog" :info nil (utils/clj->json log-data)))

(defn request->context
  "Convert a request into a context suitable for logging."
  [{:keys [client-protocol headers internal-protocol query-string remote-addr request-id request-method request-time uri] :as request}]
  (let [{:strs [content-type host origin user-agent x-cid x-forwarded-for]} headers]
    (cond-> {:cid x-cid
             :host host
             :path uri
             :remote-addr (or x-forwarded-for remote-addr)
             :request-id request-id
             :scheme (-> request utils/request->scheme name)}
      origin (assoc :origin origin)
      request-method (assoc :method (-> request-method name str/upper-case))
      client-protocol (assoc :client-protocol client-protocol)
      internal-protocol (assoc :internal-protocol internal-protocol)
      query-string (assoc :query-string query-string)
      content-type (assoc :request-content-type content-type)
      request-time (assoc :request-time (du/date-to-str request-time))
      user-agent (assoc :user-agent user-agent))))

(defn response->context
  "Convert a response into a context suitable for logging."
  [{:keys [authorization/principal backend-response-latency-ns descriptor latest-service-id get-instance-latency-ns
           handle-request-latency-ns headers instance protocol status] :as response}]
  (let [{:keys [service-id service-description]} descriptor
        {:strs [content-type grpc-status server]} headers]
    (cond-> {:status (or status 200)}
      backend-response-latency-ns (assoc :backend-response-latency-ns backend-response-latency-ns)
      content-type (assoc :response-content-type content-type)
      descriptor (assoc :metric-group (get service-description "metric-group")
                        :service-id service-id
                        :service-name (get service-description "name")
                        :service-version (get service-description "version"))
      grpc-status (assoc :grpc-status grpc-status)
      instance (assoc :instance-host (:host instance)
                      :instance-id (:id instance)
                      :instance-port (:port instance)
                      :get-instance-latency-ns get-instance-latency-ns)
      latest-service-id (assoc :latest-service-id latest-service-id)
      principal (assoc :principal principal)
      protocol (assoc :backend-protocol protocol)
      server (assoc :server server)
      handle-request-latency-ns (assoc :handle-request-latency-ns handle-request-latency-ns))))

(defn log-request!
  "Log a request"
  [request response]
  (log (merge (request->context request) (response->context response))))

(defn wrap-log
  "Wraps a handler logging data from requests and responses."
  [handler]
  (fn [request]
    (let [timer (timers/start (metrics/waiter-timer "handle-request"))
          response (handler request)]
      (ru/update-response
        response
        (fn [response]
          (let [elapsed-ns (timers/stop timer)]
            (log-request! request (assoc response :handle-request-latency-ns elapsed-ns)))
          response)))))
