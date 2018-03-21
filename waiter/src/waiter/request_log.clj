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
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.async-utils :as au]
            [waiter.ring-utils :as ru]
            [waiter.utils :as utils]))

(defn log
  "Log log-data as JSON"
  [log-data]
  (log/info (json/write-str log-data :escape-slash false)))

(defn request->context
  "Convert a request into a context suitable for logging."
  [{:keys [headers query-string remote-addr request-id request-method request-time uri] :as request}]
  (let [{:strs [host x-cid x-forwarded-for]} headers]
    (cond-> {:cid x-cid
             :host host
             :path uri
             :remote-addr (or x-forwarded-for remote-addr)
             :request-id request-id
             :scheme (let [scheme (utils/request->scheme request)]
                       (if (keyword? scheme)
                         (name scheme)
                         scheme))}
      request-method (assoc :method (-> request-method name str/upper-case))
      query-string (assoc :query-string query-string)
      request-time (assoc :request-time (utils/date-to-str request-time)))))

(defn response->context
  "Convert a response into a context suitable for logging."
  [{:keys [authorization/principal backend-response-latency-ms descriptor instance
           get-instance-latency-ms response-body-size status total-latency-ms] :as response}]
  (let [{:keys [service-id service-description]} descriptor]
    (cond-> {:status (or status 200)}
      backend-response-latency-ms (assoc :backend-response-latency-ms backend-response-latency-ms)
      descriptor (assoc :metric-group (get service-description "metric-group")
                        :service-id service-id
                        :service-name (get service-description "name")
                        :service-version (get service-description "version"))
      instance (assoc :instance-host (:host instance)
                      :instance-id (:id instance)
                      :instance-port (:port instance)
                      :instance-proto (:protocol instance)
                      :get-instance-latency-ms get-instance-latency-ms)
      principal (assoc :principal principal)
      response-body-size (assoc :response-body-size response-body-size)
      total-latency-ms (assoc :total-latency-ms total-latency-ms))))

(defn stream-countable-body!
  "Stream a body from in to out, counting the size of values passing through."
  [in out]
  (async/go
    (loop [response-body-size 0]
      (let [val (async/<! in)]
        (if-not val
          (do
            (async/close! out)
            response-body-size)
          (do
            (async/>! out val)
            (if (utils/byte-array? val)
              (recur (+ response-body-size (alength val)))
              (recur response-body-size))))))))

(defn log-request!
  "Log a request"
  [{:keys [request-time] :as request} response]
  (let [elapsed-ms (t/in-millis (t/interval request-time (t/now)))]
    (log (merge (request->context request) (response->context (-> response
                                                                  (assoc :total-latency-ms elapsed-ms)))))))

(defn wrap-log
  "Wraps a handler logging data from requests and responses."
  [handler]
  (fn [{:keys [request-time] :as request}]
    (let [response (handler request)]
      (if (au/chan? response)
        (async/go
          (let [{:keys [body] :as response'} (async/<! response)]
            (if (au/chan? body)
              (let [body' (async/chan 5)]
                (async/go
                  (let [response-body-size (async/<! (stream-countable-body! body body'))]
                    (log-request! request (assoc response'
                                                 :response-body-size response-body-size))))
                (assoc response' :body body'))
              (do
                (log-request! request response')
                response'))))
        (do
          (log-request! request response)
          response)))))
