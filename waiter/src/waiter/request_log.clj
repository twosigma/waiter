;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.request-log
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [waiter.metrics :as metrics]
            [waiter.util.date-utils :as du]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils]))

(defn log
  "Log log-data as JSON"
  [log-data]
  (log/log "RequestLog" :info nil (json/write-str log-data :escape-slash false)))

(defn request->context
  "Convert a request into a context suitable for logging."
  [{:keys [headers query-string remote-addr request-id request-method request-time uri] :as request}]
  (let [{:strs [host user-agent x-cid x-forwarded-for]} headers]
    (cond-> {:cid x-cid
             :host host
             :path uri
             :remote-addr (or x-forwarded-for remote-addr)
             :request-id request-id
             :scheme (-> request utils/request->scheme name)}
      request-method (assoc :method (-> request-method name str/upper-case))
      query-string (assoc :query-string query-string)
      request-time (assoc :request-time (du/date-to-str request-time))
      user-agent (assoc :user-agent user-agent))))

(defn response->context
  "Convert a response into a context suitable for logging."
  [{:keys [authorization/principal backend-response-latency-ns descriptor latest-service-id get-instance-latency-ns
           handle-request-latency-ns instance status] :as response}]
  (let [{:keys [service-id service-description]} descriptor]
    (cond-> {:status (or status 200)}
      backend-response-latency-ns (assoc :backend-response-latency-ns backend-response-latency-ns)
      descriptor (assoc :metric-group (get service-description "metric-group")
                        :service-id service-id
                        :service-name (get service-description "name")
                        :service-version (get service-description "version"))
      instance (assoc :instance-host (:host instance)
                      :instance-id (:id instance)
                      :instance-port (:port instance)
                      :instance-proto (:protocol instance)
                      :get-instance-latency-ns get-instance-latency-ns)
      latest-service-id (assoc :latest-service-id latest-service-id)
      principal (assoc :principal principal)
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
