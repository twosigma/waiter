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
            [clj-time.coerce :as coerce]
            [clojure.string :as str]
            [waiter.utils :as utils]))

(def ^:dynamic *request-log-file* "log/request.log")

(defn log
  "Log a request context."
  [context]
  (spit *request-log-file* (str context \newline) :append true))

(defn request->context
  "Convert a request into a context suitable for logging."
  [{:keys [authenticated-principal headers instance-id instance-host instance-port
           instance-proto metric-group query-string request-method service-id service-name
           service-version timing uri] :as request}]
  (let [{:keys [received service-discovered instance-reserved sent-to-backend closed]} timing
        {:strs [host x-cid]} headers]
    (cond-> {:cid x-cid
             :host host
             :method (-> request-method name str/upper-case)
             :path uri
             :principal authenticated-principal
             :scheme (utils/request->scheme request)}
      instance-host (assoc :instance-host instance-host)
      instance-id (assoc :instance-id instance-id)
      instance-port (assoc :instance-port instance-port)
      instance-proto (assoc :instance-proto instance-proto)
      metric-group (assoc :metric-group metric-group)
      query-string (assoc :query-string query-string)
      service-id (assoc :service-id service-id)
      service-name (assoc :service-name service-name)
      service-version (assoc :service-version service-version)
      received (assoc :timestamp (-> received
                                     (coerce/from-long)
                                     (utils/date-to-str)))
      (and sent-to-backend closed) (assoc :backend-latency (t/in-millis (t/interval sent-to-backend closed)))
      (and received service-discovered) (assoc :discovery-latency (t/in-millis (t/interval received service-discovered)))
      (and received instance-reserved) (assoc :instance-latency (t/in-millis (t/interval received instance-reserved)))
      (and received sent-to-backend) (assoc :overhead-latency (t/in-millis (t/interval received sent-to-backend)))
      (and received closed) (assoc :total-latency (t/in-millis (t/interval received closed))))))

(defn log-request
  "Logs a request and any additional context."
  [request & {:as additional-context}]
  (log (merge (request->context request) additional-context)))
