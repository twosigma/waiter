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
  (:require [clj-time.coerce :as coerce]
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
           instance-proto query-string request-method service-id timing uri] :as request}]
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
      query-string (assoc :query-string query-string)
      service-id (assoc :service-id service-id)
      received (assoc :timestamp (-> received
                                     (coerce/from-long)
                                     (utils/date-to-str)))
      (and closed sent-to-backend) (assoc :backend-latency (- closed sent-to-backend))
      (and service-discovered received) (assoc :discovery-latency (- service-discovered received))
      (and instance-reserved received) (assoc :instance-latency (- instance-reserved received))
      (and sent-to-backend received) (assoc :overhead-latency (- sent-to-backend received))
      (and closed received) (assoc :total-latency (- closed received)))))
