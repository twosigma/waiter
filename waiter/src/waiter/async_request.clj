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
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.statsd :as statsd])
  (:import [java.net ConnectException SocketTimeoutException URI URLEncoder]
           java.util.concurrent.TimeoutException))

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
  [make-http-request complete-async-request request-still-active? status-endpoint check-interval-ms request-timeout-ms correlation-id exit-chan]
  (cid/with-correlation-id
    (str correlation-id "|status-check")
    (async/go
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
              (let [{:keys [body headers error status]} (async/<! (make-http-request))]
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
                      (cid/cdebug correlation-id "async request has not yet completed")
                      (recur (max 0 (- ttl check-interval-ms))))
                    303
                    (do
                      (log/info "async request has completed, result headers" headers)
                      (let [location-header (get headers "location")
                            location (normalize-location-header status-endpoint location-header)]
                        (if (str/starts-with? (str location) "/")
                          (recur (max 0 (- ttl check-interval-ms)))
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
                      :unknown-status-code)))))))))))

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

(defn post-process-async-request-response
  "Triggers execution of monitoring system for an async request.
   The function assumes location begins with a slash.
   This method wires up the completion and status check callbacks for the monitoring system.
   It also modifies the status check endpoint in the response header."
  [router-id async-request-store-atom make-http-request-fn instance-rpc-chan response
   service-id metric-group backend-proto {:keys [host port] :as instance}
   {:keys [request-id] :as reason-map} request-properties location query-string]
  (let [correlation-id (cid/get-correlation-id)
        status-endpoint (scheduler/end-point-url instance location)
        _ (log/info "status endpoint for async request is" status-endpoint query-string)
        {:keys [async-check-interval-ms async-request-timeout-ms]} request-properties
        exit-chan (async/chan 1)]
    ;; register async request
    (swap! async-request-store-atom assoc request-id (assoc reason-map :exit-chan exit-chan))
    (counters/inc! (metrics/service-counter service-id "request-counts" "async"))
    ;; trigger execution of monitoring system
    (letfn [(make-get-request-fn []
              (counters/inc! (metrics/service-counter service-id "request-counts" "async-monitor"))
              (let [request-stub {:body nil :headers {} :query-string query-string :request-method :get}]
                (make-http-request-fn instance request-stub location metric-group backend-proto)))
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
      (monitor-async-request make-get-request-fn complete-async-request-fn request-still-active? status-endpoint
                             async-check-interval-ms async-request-timeout-ms correlation-id exit-chan))
    ;; modify the location header in the response
    (let [param-map {:host host, :location location, :port port, :request-id request-id, :router-id router-id, :service-id service-id}
          status-location (route-params->uri "/waiter-async/status/" param-map)
          status-url (cond-> status-location
                             query-string (str "?" query-string))]
      (log/info "updating status location to" status-location "from" location "with query string" query-string)
      (assoc-in response [:headers "location"] status-url))))
