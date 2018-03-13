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
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.async-utils :as au]
            [waiter.utils :as utils]))

;; TODO (shams) introduce metrics

;; TODO (shams) create state handler to expose this state.
(def ^:private request-id->log-store-atom (atom {}))

(defn request-id->request-log
  "Return the request-log associated with request-id in the request-id->log-store-atom."
  [request-id]
  (if-let [log-store (get @request-id->log-store-atom request-id)]
    (deref log-store)
    ;; TODO (shams) ensure that the else branch is never taken
    (log/error "debug why is log-store nil" {:request-id request-id})))

(defn merge-log!
  "Merge m into the request-log associated with request-id in the request-id->log-store-atom."
  [request-id m]
  ;; TODO (shams) log error missing entry
  (when-let [log-store (get @request-id->log-store-atom request-id)]
    (swap! log-store merge m)))

(defn assoc-log!
  "Associated k with v in the request-log associated with request-id in the request-id->log-store-atom."
  [request-id k v]
  ;; TODO (shams) log error missing entry
  (when-let [log-store (get @request-id->log-store-atom request-id)]
    (swap! log-store assoc k v)))

(defn- dissoc-log!
  "Removes the request-log associated with request-id from the request-id->log-store-atom."
  [request-id]
  (swap! request-id->log-store-atom dissoc request-id))

(defn mark-request-time!
  "TODO (shams) docstring"
  ([request-id k]
   (mark-request-time! request-id k (t/now)))
  ([request-id k time]
   (when-let [log-store (get @request-id->log-store-atom request-id)]
     (swap! log-store assoc-in [:timing k] time))))

(defn log-instance!
  "TODO (shams) docstring"
  [request-id {:keys [id host port protocol]}]
  (mark-request-time! request-id :instance-reserved)
  (merge-log! request-id {:instance-host host
                          :instance-id id
                          :instance-port port
                          :instance-proto protocol}))

(defn- log-context!
  "Log a request context."
  [context]
  ;; TODO (shams) output request log with a different logger-ns
  (log/log :info nil context))

(defn- interval-in-ms
  "TODO (shams) docstring"
  [start-time end-time]
  (t/in-millis (t/interval start-time end-time)))

(defn- request-log->context
  "Convert a request into a context suitable for logging."
  [{:keys [authenticated-principal content-length headers query-string request-method timing uri] :as request-log}]
  (let [{:keys [closed instance-reserved received rcvd-from-backend sent-to-backend service-discovered]} timing
        {:strs [host x-cid]} headers]
    (cond-> (select-keys request-log
                         [:bytes-streamed :instance-host :instance-id :instance-port :instance-proto
                          :metric-group :request-id :scheme :service-id :service-name :service-version :status
                          :termination-state])
            headers (assoc :cid x-cid
                           :host host)
            authenticated-principal (assoc :principal authenticated-principal)
            content-length (assoc :bytes-in-request-body content-length)
            query-string (assoc :query-string query-string)
            request-method (assoc :method (-> request-method name str/upper-case))
            received (assoc :timestamp (utils/date-to-str received))
            (not (str/blank? uri)) (assoc :path uri)
            (and sent-to-backend rcvd-from-backend) (assoc :backend-initial-latency-ms (interval-in-ms sent-to-backend rcvd-from-backend))
            (and sent-to-backend closed) (assoc :backend-total-latency-ms (interval-in-ms sent-to-backend closed))
            (and received service-discovered) (assoc :discovery-latency-ms (interval-in-ms received service-discovered))
            (and received instance-reserved) (assoc :instance-latency-ms (interval-in-ms received instance-reserved))
            (and received sent-to-backend) (assoc :overhead-latency-ms (interval-in-ms received sent-to-backend))
            (and received closed) (assoc :total-latency-ms (interval-in-ms received closed)))))

(defn- log-request!
  "Logs a request and any additional context."
  [request-id]
  (-> request-id
      request-id->request-log
      request-log->context
      log-context!))

(defn request->received-time
  "TODO (shams) docstring"
  [request]
  (:received request))

(defn request->request-id
  "Returns the request-id associated with request."
  [{:keys [request-id]}]
  request-id)

(defn ensure-request-id
  "Ensures that the request-id is present in the request map under the key :request-id.
   When not initially present, id-factory is used to generate the new request-id."
  [request]
  (let [current-request-id (:request-id request)]
    (if (nil? current-request-id)
      (->> (str (utils/unique-identifier) "-" (-> request utils/request->scheme name))
           (assoc request :request-id))
      request)))

(defn- initialize-log!
  "Initializes the request-log associated with request in the request-id->log-store-atom."
  [request request-time]
  (let [request-id (request->request-id request)]
    (log/debug "initializing request-log for request" request-id (get-in request [:headers "x-cid"]))
    (swap! request-id->log-store-atom
           assoc
           request-id
           (-> request
               (select-keys [:authenticated-principal :content-length :headers :query-string :request-method :uri])
               (assoc :request-id request-id :scheme (utils/request->scheme request))
               atom))
    (mark-request-time! request-id :received request-time)))

(defn- finalize-log!
  "TODO (shams) docstring"
  [request-id source]
  (try
    (log/debug source "publishing request-log for request" request-id)
    (mark-request-time! request-id :closed)
    (log-request! request-id)
    (finally
      (dissoc-log! request-id))))

(defn request-log-middleware
  "Attaches a request-id to the request and writes the request log when the request terminates."
  [handler]
  (fn request-log-middleware-fn [{:keys [ctrl] :as request}]
    (let [control-mult (async/mult ctrl)
          request-time (t/now)
          request (-> request
                      ensure-request-id
                      (assoc :received request-time)
                      ;; ideally replacing the ctrl chan with control-mult should be in its own middleware
                      (dissoc :ctrl)
                      (assoc :control-mult control-mult))
          request-id (request->request-id request)]

      (initialize-log! request request-time)

      (let [response (handler request)]
        (if (au/chan? response)
          (let [request-state-chan (async/tap control-mult (au/latest-chan) true)]
            (au/on-chan-close request-state-chan #(finalize-log! request-id "async")
                              (fn [e] (log/error e "error writing request log"))))
          (finalize-log! request-id "sync"))

        response))))
