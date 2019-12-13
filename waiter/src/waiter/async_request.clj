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
(ns waiter.async-request
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.timers :as timers]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.request-log :as rlog]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.statsd :as statsd]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (java.net ConnectException SocketTimeoutException URI URLEncoder)
           (java.util.concurrent TimeoutException)))

(def daemon-counter (metrics/waiter-counter "async" "monitor" "daemon"))

(def in-flight-requests-counter (metrics/waiter-counter "async" "monitor" "request"))

(def status-check-timer (metrics/waiter-timer "async" "monitor" "status-check"))

(defn normalize-location-header
  "Uses the absolute url from the request to create a sanitized version of the location header.
   If the location has the same scheme, host and port as the request url, then a relative version is returned.
   Else the normalized absolute url of the location is returned.
   This function expects request-absolute-url to be a valid url including host and port."
  [request-absolute-url location-header]
  (if (str/blank? location-header)
    ""
    (let [request-uri (URI. (str request-absolute-url))
          location-uri (URI. (str location-header))
          location-absolute-url (str (.resolve (.normalize request-uri) (.normalize location-uri)))
          request-base-url (str (.getScheme request-uri) "://" (.getAuthority request-uri))]
      (if (str/starts-with? location-absolute-url request-base-url)
        (subs location-absolute-url (count request-base-url))
        location-absolute-url))))

(defn monitor-async-request
  "Launches a go-block that monitors the state of an async request at specified intervals.
   It makes calls to the backend instance and inspects the responses to decide when to treat the request as complete.
   A request is not complete as long as the backend keeps returning a 200 response.
   The request is forcefully completed at timeout."
  [make-http-request complete-async-request request-still-active? status-endpoint check-interval-ms request-timeout-ms
   correlation-id exit-chan]
  (cid/with-correlation-id
    (str correlation-id "|status-check")
    (async/go
      (try
        (counters/inc! daemon-counter)
        (log/info "monitoring async request at intervals of" check-interval-ms "ms")
        (loop [ttl request-timeout-ms]
          (if-not (pos? ttl)
            (do
              (log/info "request has timed out, releasing allocated instance")
              (complete-async-request :success)
              :monitor-timed-out)
            (let [timeout-chan (async/timeout (min check-interval-ms ttl))
                  [message trigger-chan] (async/alts! [exit-chan timeout-chan] :priority true)
                  continue-looping (if (= trigger-chan timeout-chan) (request-still-active?) (not= message :exit))]
              (if-not continue-looping
                (do
                  (log/info "request has been cleared from store, exiting monitoring loop")
                  (complete-async-request :success)
                  (if (= trigger-chan exit-chan) :request-terminated :request-no-longer-active))
                (let [{:keys [body headers error status]}
                      (metrics/with-counter
                        in-flight-requests-counter
                        (timers/start-stop-time!
                          status-check-timer
                          (async/<! (make-http-request))))]
                  (when body
                    (async/close! body))
                  (if error
                    (do
                      (condp instance? error
                        ConnectException (log/debug error "error in performing status check")
                        SocketTimeoutException (log/debug error "timeout in performing status check")
                        TimeoutException (log/debug error "timeout in performing status check")
                        Throwable (log/warn error "unexpected error in performing status check"))
                      (log/info (.getMessage error) "releasing allocated instance")
                      (complete-async-request :instance-error)
                      :make-request-error)
                    (case (int status)
                      200
                      (do
                        (log/debug "async request has not yet completed")
                        (recur (max 0 (- ttl check-interval-ms))))
                      303
                      (do
                        (log/info "async request has completed, result headers" headers)
                        (let [location-header (get headers "location")
                              location (normalize-location-header status-endpoint location-header)]
                          (if (str/starts-with? (str location) "/")
                            (do
                              (log/info "continuing async request status checks as location is a relative path:" location)
                              (recur (max 0 (- ttl check-interval-ms))))
                            (do
                              (log/info "completing async request as result location is not a relative path:" location)
                              (complete-async-request :success)
                              :status-see-other))))
                      410
                      (do
                        (log/info "async request has completed, result is no longer available!")
                        (complete-async-request :success)
                        :status-gone)
                      (do
                        (log/warn "status check returned unsupported status" status ", releasing reserved instance")
                        (complete-async-request :success)
                        :unknown-status-code))))))))
        (finally
          (counters/dec! daemon-counter))))))

(defn complete-async-request-locally
  "Helper function that stops tracking an async request locally and releases the instance associated with it.
   It indirectly influences the monitor as the request is treated as 'inactive' on removal.
   release-instance-fn is expected to be indempotent."
  [async-request-store-atom release-instance-fn request-id result-status]
  (when (contains? @async-request-store-atom request-id)
    (log/info "async request" request-id "has completed, releasing instance")
    (release-instance-fn result-status)
    (swap! async-request-store-atom dissoc request-id)))

(defn async-request-terminate
  "Helper function that terminates the tracking of an async request locally by sending a message along the `exit-chan`."
  [async-request-store-atom request-id]
  (let [{:keys [exit-chan]} (get @async-request-store-atom request-id)]
    (when exit-chan
      (async/put! exit-chan :exit))))

(defn async-trigger-terminate
  "Helper function that triggers local or remote termination of an async request."
  [async-request-terminate-fn make-inter-router-requests-fn local-router-id target-router-id service-id request-id]
  (if (= local-router-id target-router-id)
    (do
      (log/info "terminating async request" request-id "locally at router" target-router-id)
      (async-request-terminate-fn request-id))
    (let [endpoint (str "waiter-async/complete/" request-id "/" service-id)]
      (log/info "requesting termination of async request" request-id "at router" target-router-id)
      (make-inter-router-requests-fn endpoint :acceptable-router? #(= target-router-id %) :method :get))))

(defn route-params->uri
  "Converts the route params to a uri.
   The function expects prefix to end with a slash and location to begin with a slash.
   Returns a formatted url: prefix/{request-id}/{router-id}/{service-id}/{host}/{port}{location}"
  [prefix {:keys [host location port request-id router-id service-id]}]
  (let [encode #(if %1 (URLEncoder/encode %1 "UTF-8") (str %1))]
    (str prefix (encode request-id) "/" (encode router-id) "/" service-id "/" (str host) "/" (str port) location)))

(defn sanitize-check-interval
  "Computes the async-check-interval to use by restricting the total number of checks to be performed
   to async-request-max-status-checks."
  [async-request-timeout-ms async-check-interval-ms async-request-max-status-checks]
  (let [sanitized-check-interval-ms (int (/ async-request-timeout-ms async-request-max-status-checks))]
    (if (>= async-check-interval-ms sanitized-check-interval-ms)
      async-check-interval-ms
      (do
        (log/info "increasing async check interval to" sanitized-check-interval-ms
                  "from" async-check-interval-ms)
        sanitized-check-interval-ms))))

(defn- make-async-status-get-request
  "Performs the get request for the async request status and returns the response.
   Also, adds a corresponding entry into the request log."
  [make-http-request-fn auth-params-map user-agent {:keys [service-description service-id] :as descriptor}
   {:keys [host] :as instance} location query-string]
  (counters/inc! (metrics/service-counter service-id "request-counts" "async-monitor"))
  (let [{:strs [metric-group backend-proto]} service-description
        backend-protocol (hu/backend-protocol->http-version backend-proto)
        backend-scheme (hu/backend-proto->scheme backend-proto)
        request-stub (assoc auth-params-map
                       :body nil
                       :client-protocol backend-protocol
                       :headers {"host" host
                                 "user-agent" user-agent
                                 "x-cid" (cid/get-correlation-id)}
                       :internal-protocol backend-protocol
                       :query-string query-string
                       :request-id (str "waiter-async-status-check-" (utils/unique-identifier))
                       :request-method :get
                       :request-time (t/now)
                       :scheme backend-scheme
                       :uri location)
        response-ch (make-http-request-fn instance request-stub location metric-group backend-proto)]
    (async/go
      (let [{:keys [error] :as response} (async/<! response-ch)]
        (let [response (-> (merge auth-params-map response)
                         (utils/assoc-if-absent :instance instance)
                         (assoc :descriptor descriptor
                                :error-class (some-> error .getClass .getCanonicalName)
                                :instance-proto backend-proto
                                :latest-service-id service-id
                                :protocol backend-protocol
                                :request-type "async-status-check"
                                :waiter-api-call? false))]
          (rlog/log-request! request-stub response))
        response))))

(defn post-process-async-request-response
  "Triggers execution of monitoring system for an async request.
   The function assumes location begins with a slash.
   This method wires up the completion and status check callbacks for the monitoring system.
   It also modifies the status check endpoint in the response header."
  [router-id async-request-store-atom make-http-request-fn auth-params-map instance-rpc-chan user-agent
   response {:keys [service-description service-id] :as descriptor} {:keys [host port] :as instance}
   {:keys [request-id] :as reason-map} request-properties location query-string]
  (let [correlation-id (cid/get-correlation-id)
        {:strs [metric-group backend-proto]} service-description
        status-endpoint (scheduler/end-point-url backend-proto host port location)
        _ (log/info "status endpoint for async request is" status-endpoint query-string)
        {:keys [async-check-interval-ms async-request-max-status-checks async-request-timeout-ms]} request-properties
        exit-chan (async/chan 1)]
    ;; register async request
    (swap! async-request-store-atom assoc request-id (assoc reason-map :exit-chan exit-chan))
    (counters/inc! (metrics/service-counter service-id "request-counts" "async"))
    ;; trigger execution of monitoring system
    (letfn [(make-get-request-fn []
              (make-async-status-get-request make-http-request-fn auth-params-map user-agent descriptor
                                             instance location query-string))
            (release-instance-fn [status]
              (log/info "decrementing outstanding requests as an async request has completed:" status)
              (counters/dec! (metrics/service-counter service-id "request-counts" "async"))
              (counters/dec! (metrics/service-counter service-id "request-counts" "outstanding"))
              (when (= :success status)
                (counters/inc! (metrics/service-counter service-id "request-counts" "successful")))
              (statsd/gauge-delta! metric-group "request_outstanding" -1)
              (service/release-instance-go instance-rpc-chan instance {:status status, :cid correlation-id, :request-id request-id}))
            (complete-async-request-fn [status]
              (complete-async-request-locally async-request-store-atom release-instance-fn request-id status))
            (request-still-active? []
              (contains? @async-request-store-atom request-id))]
      (let [check-interval-ms (sanitize-check-interval async-request-timeout-ms async-check-interval-ms async-request-max-status-checks)]
        (monitor-async-request make-get-request-fn complete-async-request-fn request-still-active? status-endpoint
                               check-interval-ms async-request-timeout-ms correlation-id exit-chan)))
    ;; modify the location header in the response
    (let [param-map {:host host, :location location, :port port, :request-id request-id, :router-id router-id, :service-id service-id}
          status-location (route-params->uri "/waiter-async/status/" param-map)
          status-url (cond-> status-location
                       query-string (str "?" query-string))]
      (log/info "updating status location to" status-location "from" location "with query string" query-string)
      (assoc-in response [:headers "location"] status-url))))
