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
(ns waiter.process-request
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [full.async :as fa]
            [metrics.core]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [qbits.jet.client.http :as http]
            [qbits.jet.servlet :as servlet]
            [slingshot.slingshot :refer [try+]]
            [waiter.async-request :as async-req]
            [waiter.auth.authentication :as auth]
            [waiter.correlation-id :as cid]
            [waiter.handler :as handler]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.statsd :as statsd]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (java.io InputStream IOException)
           (java.util.concurrent TimeoutException)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.io EofException)
           (org.eclipse.jetty.server HttpChannel HttpOutput)))

(defn check-control [control-chan]
  (let [state (au/poll! control-chan :still-running)]
    (cond
      (= :still-running state) :still-running
      (= (first state) ::servlet/error) (throw (ex-info "Error in server" {:event (second state)}))
      (= (first state) ::servlet/timeout) (throw (ex-info "Connection timed out" {:event (second state)}))
      :else (throw (ex-info "Connection closed while still processing" {})))))

(defn confirm-live-connection-factory
  "Confirms that the connection to the client is live by checking the ctrl channel, else it throws an exception."
  [control-mult reservation-status-promise error-callback]
  (let [confirm-live-chan (async/tap control-mult (au/sliding-buffer-chan 5))]
    (fn confirm-live-connection []
      (try
        (check-control confirm-live-chan)
        (catch Exception e
          ; flag the error as an I/O error as the connection is no longer live
          (deliver reservation-status-promise :client-error)
          (when error-callback
            (error-callback e))
          (throw e))))))

(defn set-idle-timeout!
  "Configures the idle timeout in the response output stream (HttpOutput) to `idle-timeout-ms` ms."
  [^HttpOutput output-stream idle-timeout-ms]
  (try
    (log/debug "executing pill to adjust idle timeout to" idle-timeout-ms "ms.")
    (let [^HttpChannel http-channel (.getHttpChannel output-stream)]
      (.setIdleTimeout http-channel idle-timeout-ms))
    (catch Exception e
      (log/error e "gobbling unexpected error while setting idle timeout"))))

(defn- configure-idle-timeout-pill-fn
  "Creates a function that configures the idle timeout in the response output stream (HttpOutput) to `streaming-timeout-ms` ms."
  [correlation-id streaming-timeout-ms]
  (fn configure-idle-timeout-pill [output-stream]
    (cid/with-correlation-id
      correlation-id
      (set-idle-timeout! output-stream streaming-timeout-ms))))

(defn- poison-pill-fn
  "Sends a faulty InputStream directly into Jetty's response output stream (HttpOutput) to trigger a failure path.
   When a streaming chunked transfer from a backend fails, Waiter needs to relay this error to its client by not
   sending a terminating chunk."
  [correlation-id]
  (fn poison-pill [^HttpOutput output-stream]
    ; set the failed status by sending poison along the input-stream
    ; lowering the idle timeout is necessary for clients (e.g. HAProxy) configured to keep the connection alive
    (try
      (cid/with-correlation-id
        correlation-id
        (set-idle-timeout! output-stream 1)
        (let [input-stream (proxy [InputStream] []
                             (available [] true) ; claim data is available to enable trigger exception on read()
                             (close [])
                             (read [_ _ _]
                               (let [message "read(byte[], int, int) used to trigger exception!"]
                                 (log/debug "poison pill:" message)
                                 (throw (IOException. message)))))]
          (.sendContent output-stream input-stream)))
      (catch Exception e
        (cid/cdebug correlation-id "gobbling expected error while passing poison pill" (.getMessage e))))))

(defn prepare-request-properties
  [instance-request-properties waiter-headers]
  (let [service-configured-timeout-lookup
        (fn [old-value header-value display-name]
          (let [parsed-value (when header-value
                               (try
                                 (log/info "Request wants to configure" display-name "to" header-value)
                                 (Integer/parseInt (str header-value))
                                 (catch Exception _
                                   (log/warn "Cannot convert header for" display-name "to an int:" header-value)
                                   nil)))]
            (if (and parsed-value (pos? parsed-value)) parsed-value old-value)))]
    (-> instance-request-properties
        (update :async-check-interval-ms service-configured-timeout-lookup
                (headers/get-waiter-header waiter-headers "async-check-interval") "async request check interval")
        (update :async-request-timeout-ms service-configured-timeout-lookup
                (headers/get-waiter-header waiter-headers "async-request-timeout") "async request timeout")
        (update :initial-socket-timeout-ms service-configured-timeout-lookup
                (headers/get-waiter-header waiter-headers "timeout") "socket timeout")
        (update :queue-timeout-ms service-configured-timeout-lookup
                (headers/get-waiter-header waiter-headers "queue-timeout") "instance timeout")
        (update :streaming-timeout-ms service-configured-timeout-lookup
                (headers/get-waiter-header waiter-headers "streaming-timeout") "streaming timeout"))))

(defn- prepare-instance
  "Tries to acquire an instance and set up a mechanism to release the instance when
   `request-state-chan` is closed. Takes `instance-rpc-chan`, `service-id` and
   `reason-map` to acquire the instance.
   If an exception has occurred, no instance was acquired.
   Returns the instance if it was acquired successfully,
   or an exception if there was an error"
  [instance-rpc-chan service-id {:keys [request-id] :as reason-map} start-new-service-fn request-state-chan
   queue-timeout-ms reservation-status-promise metric-group]
  (fa/go-try
    (log/debug "retrieving instance for" service-id "using" (dissoc reason-map :cid :time))
    (let [correlation-id (cid/get-correlation-id)
          instance (fa/<? (service/get-available-instance
                            instance-rpc-chan service-id reason-map start-new-service-fn queue-timeout-ms metric-group))]
      (au/on-chan-close request-state-chan
                        (fn on-request-state-chan-close []
                          (cid/cdebug correlation-id "request-state-chan closed")
                          ; assume request did not process successfully if no value in promise
                          (deliver reservation-status-promise :generic-error)
                          (let [status @reservation-status-promise]
                            (cid/cinfo correlation-id "done processing request" status)
                            (when (= :success status)
                              (counters/inc! (metrics/service-counter service-id "request-counts" "successful")))
                            (when (= :generic-error status)
                              (cid/cerror correlation-id "there was a generic error in processing the request;"
                                          "if this is a client or server related issue, the code needs to be updated."))
                            (when (not= :success-async status)
                              (counters/dec! (metrics/service-counter service-id "request-counts" "outstanding"))
                              (statsd/gauge-delta! metric-group "request_outstanding" -1))
                            (service/release-instance-go instance-rpc-chan instance {:status status, :cid correlation-id, :request-id request-id})))
                        (fn [e] (log/error e "error releasing instance!")))
      instance)))

(defn request->endpoint
  "Retrieves the relative url Waiter should use to forward requests to service instances."
  [{:keys [uri]} waiter-headers]
  (if (= "/secrun" uri)
    (headers/get-waiter-header waiter-headers "endpoint-path" "/req")
    uri))

(defn- handle-response-error
  "Handles error responses from the backend."
  [error reservation-status-promise service-id request]
  (let [metrics-map (metrics/retrieve-local-stats-for-service service-id)
        [promise-value message status]
        (cond (instance? EofException error)
              [:client-error "Connection unexpectedly closed while sending request" 400]
              (instance? TimeoutException error)
              [:instance-error (utils/message :backend-request-timed-out) 504]
              :else
              [:instance-error (utils/message :backend-request-failed) 502])]
    (deliver reservation-status-promise promise-value)
    (-> (ex-info message (assoc metrics-map :status status) error)
        (utils/exception->response request))))

(defn- make-http-request
  "Makes an asynchronous request to the endpoint using Basic authentication."
  [^HttpClient http-client make-basic-auth-fn request-method endpoint query-string headers body service-password
   {:keys [username principal]} idle-timeout output-buffer-size]
  (let [auth (make-basic-auth-fn endpoint "waiter" service-password)
        headers (headers/assoc-auth-headers headers username principal)]
    (http/request
      http-client
      {:as :bytes
       :auth auth
       :body body
       :headers headers
       :fold-chunked-response? true
       :fold-chunked-response-buffer-size output-buffer-size
       :follow-redirects? false
       :idle-timeout idle-timeout
       :method request-method
       :query-string query-string
       :url endpoint})))

(defn make-request
  "Makes an asynchronous http request to the instance endpoint and returns a channel."
  [http-client make-basic-auth-fn service-id->password-fn instance {:keys [body query-string request-method] :as request}
   {:keys [initial-socket-timeout-ms output-buffer-size]} passthrough-headers end-route metric-group]
  (let [instance-endpoint (scheduler/end-point-url instance end-route)
        service-id (scheduler/instance->service-id instance)
        service-password (service-id->password-fn service-id)
        ; Removing expect may be dangerous http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html, but makes requests 3x faster =}
        ; Also remove hop-by-hop headers https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.5.1
        headers (-> (dissoc passthrough-headers "authorization" "expect")
                    (headers/dissoc-hop-by-hop-headers)
                    (assoc "cookie" (auth/remove-auth-cookie (get passthrough-headers "cookie"))))
        waiter-debug-enabled? (utils/request->debug-enabled? request)]
    (try
      (let [content-length-str (get passthrough-headers "content-length")
            content-length (if content-length-str (Integer/parseInt content-length-str) 0)]
        (when (and (integer? content-length) (pos? content-length))
          ; computing the actual bytes will currently require synchronously reading all data in the request body
          (histograms/update! (metrics/service-histogram service-id "request-size") content-length)
          (statsd/inc! metric-group "request_bytes" content-length)))
      (catch Exception e
        (log/error e "Unable to track content-length on request")))
    (when waiter-debug-enabled?
      (log/info "connecting to" instance-endpoint))
    (make-http-request
      http-client make-basic-auth-fn request-method instance-endpoint query-string headers body service-password
      (handler/make-auth-user-map request) initial-socket-timeout-ms output-buffer-size)))

(defn extract-async-request-response-data
  "Helper function that inspects the response and returns the location and query-string if the response
   has a status code is 202 and contains a location header with a base path."
  [{:keys [headers status]} endpoint]
  (when (= status 202)
    (let [location-header (str (get headers "location"))
          [endpoint _] (str/split endpoint #"\?" 2)
          [location-header query-string] (str/split location-header #"\?" 2)
          location (async-req/normalize-location-header endpoint location-header)]
      (if (str/starts-with? location "/")
        {:location location :query-string query-string}
        (log/info "response status 202, not treating as an async request as location is" location)))))

(defn stream-http-response
  "Writes byte data to the resp-chan. If the body is a string, just writes the string.
   Otherwise, it is assumed the body is a input stream, in which case, the function
   buffers bytes, and push byte input streams onto the channel until the body input
   stream is exhausted."
  [{:keys [body error-chan]} confirm-live-connection request-abort-callback resp-chan
   {:keys [streaming-timeout-ms]}
   reservation-status-promise request-state-chan metric-group waiter-debug-enabled?
   {:keys [throughput-meter requests-streaming requests-waiting-to-stream
           stream-request-rate stream-complete-rate
           stream-exception-meter stream-back-pressure stream-read-body
           stream-onto-resp-chan stream service-id]}]
  (async/go
    (let [output-stream-atom (atom nil)]
      (try
        (counters/dec! requests-waiting-to-stream)
        ; configure the idle timeout to the value specified by streaming-timeout-ms
        (async/>! resp-chan (configure-idle-timeout-pill-fn (cid/get-correlation-id) streaming-timeout-ms))
        (async/>! resp-chan (fn [output-stream] (reset! output-stream-atom output-stream)))
        (metrics/with-meter
          stream-request-rate
          stream-complete-rate
          (metrics/with-counter
            requests-streaming
            (timers/start-stop-time!
              stream
              (loop [bytes-streamed 0
                     bytes-reported-to-statsd 0]
                (let [[bytes-streamed' more-bytes-possibly-available?]
                      (try
                        (confirm-live-connection)
                        (let [buffer (timers/start-stop-time! stream-read-body (async/<! body))
                              bytes-read (if buffer
                                           (count buffer)
                                           -1)]
                          (if-not (= -1 bytes-read)
                            (do
                              (meters/mark! throughput-meter bytes-read)
                              (if (timers/start-stop-time!
                                    stream-onto-resp-chan
                                    (au/timed-offer! resp-chan buffer streaming-timeout-ms)) ; don't wait forever to write to server
                                [(+ bytes-streamed bytes-read) true]
                                (let [ex (ex-info "Unable to stream, back pressure in resp-chan. Is connection live?"
                                                  {:cid (cid/get-correlation-id), :bytes-streamed bytes-streamed})]
                                  (meters/mark! stream-back-pressure)
                                  (deliver reservation-status-promise :client-error)
                                  (request-abort-callback (IOException. ^Exception ex))
                                  (when waiter-debug-enabled?
                                    (log/info "unable to stream, back pressure in resp-chan"))
                                  (throw ex))))
                            (do
                              (let [{:keys [error]} (async/alt!
                                                      error-chan ([error] error)
                                                      (async/timeout 5000) ([_] {:error (IllegalStateException. "Timeout while waiting on error chan")}))]
                                (when error
                                  (throw error)))
                              (histograms/update! (metrics/service-histogram service-id "response-size") bytes-streamed)
                              (log/info bytes-streamed "bytes streamed in response")
                              (deliver reservation-status-promise :success)
                              [bytes-streamed false])))
                        (catch Exception e
                          (histograms/update! (metrics/service-histogram service-id "response-size") bytes-streamed)
                          (log/error "Error occurred after streaming" bytes-streamed "bytes in response.")
                          ; Handle lower down
                          (throw e)))]
                  (let [bytes-reported-to-statsd'
                        (let [unreported-bytes (- bytes-streamed' bytes-reported-to-statsd)]
                          (if (or (and (not more-bytes-possibly-available?) (> unreported-bytes 0))
                                  (>= unreported-bytes 1000000))
                            (do
                              (statsd/inc! metric-group "response_bytes" unreported-bytes)
                              bytes-streamed')
                            bytes-reported-to-statsd))]
                    (when more-bytes-possibly-available?
                      (recur bytes-streamed' bytes-reported-to-statsd'))))))))
        (catch Exception e
          (meters/mark! stream-exception-meter)
          (deliver reservation-status-promise :instance-error)
          (log/info "sending poison pill to response channel")
          (let [poison-pill-function (poison-pill-fn (cid/get-correlation-id))]
            (when-not (au/timed-offer! resp-chan poison-pill-function 5000)
              (log/info "poison pill offer on response channel timed out!")
              (when-let [output-stream @output-stream-atom]
                (log/info "invoking poison pill directly on output stream")
                (poison-pill-function output-stream))))
          (log/error e "Exception occurred while streaming response for" service-id))
        (finally
          (async/close! resp-chan)
          (async/close! body)
          (async/close! request-state-chan))))))

(defn wrap-response-status-metrics
  "Wraps a handler and updates service metrics based upon the result."
  [handler]
  (fn wrap-response-status-metrics-fn
    [{:keys [descriptor] :as request}]
    (let [{:keys [service-id] {:strs [metric-group]} :service-description} descriptor
          response (handler request)
          update! (fn [{:keys [status] :as response}]
                    (counters/inc! (metrics/service-counter service-id "response-status" (str status)))
                    (statsd/inc! metric-group (str "response_status_" status))
                    response)]
      (ru/update-response response update!))))

(defn abort-http-request-callback-factory
  "Creates a callback to abort the http request."
  [response]
  (fn abort-http-request-callback [^Exception e]
    (let [ex (if (instance? IOException e) e (IOException. e))
          aborted (if-let [request (:request response)]
                    (.abort request ex)
                    (log/warn "unable to abort as request not found inside response!"))]
      (log/info "aborted backend request:" aborted))))

(defn process-http-response
  "Processes a response resulting from a http request.
   It includes book-keeping for async requests and asycnhronously streaming the content."
  [post-process-async-request-response-fn _ instance-request-properties descriptor instance
   request reason-map reservation-status-promise confirm-live-connection-with-abort
   request-state-chan {:keys [status] :as response}]
  (let [{:keys [service-description service-id waiter-headers]} descriptor
        {:strs [metric-group]} service-description
        waiter-debug-enabled? (utils/request->debug-enabled? request)
        endpoint (request->endpoint request waiter-headers)
        resp-chan (async/chan 5)]
    (when (and (= 503 status) (get service-description "blacklist-on-503"))
      (log/info "Instance returned 503: " {:instance instance})
      (deliver reservation-status-promise :instance-busy))
    (meters/mark! (metrics/service-meter service-id "response-status-rate" (str status)))
    (counters/inc! (metrics/service-counter service-id "request-counts" "waiting-to-stream"))
    (confirm-live-connection-with-abort)
    (let [{:keys [location query-string]} (extract-async-request-response-data response endpoint)
          request-abort-callback (abort-http-request-callback-factory response)]
      (when location
        ;; backend is processing as an asynchronous request, eagerly trigger the write to the promise
        (deliver reservation-status-promise :success-async))
      (stream-http-response response confirm-live-connection-with-abort request-abort-callback
                            resp-chan instance-request-properties reservation-status-promise
                            request-state-chan metric-group waiter-debug-enabled?
                            (metrics/stream-metric-map service-id))
      (-> (cond-> response
            location (post-process-async-request-response-fn
                       service-id metric-group instance (handler/make-auth-user-map request) reason-map
                       instance-request-properties location query-string))
          (assoc :body resp-chan)
          (update-in [:headers] (fn update-response-headers [headers]
                                  (utils/filterm #(not= "connection" (str/lower-case (str (key %)))) headers)))))))

(defn track-process-error-metrics
  "Updates metrics for process errors."
  [descriptor]
  (meters/mark! (metrics/waiter-meter "core" "process-errors"))
  (let [{:keys [service-description service-id]} descriptor
        {:strs [metric-group]} service-description]
    (meters/mark! (metrics/service-meter service-id "process-error"))
    (statsd/inc! metric-group "process_error")))

(defn handle-process-exception
  "Handles an error during process."
  [exception {:keys [descriptor] :as request}]
  (log/error exception "error during process")
  (track-process-error-metrics descriptor)
  (utils/exception->response exception request))

(let [process-timer (metrics/waiter-timer "core" "process")]
  (defn process
    "Process the incoming request and stream back the response."
    [make-request-fn instance-rpc-chan start-new-service-fn
     instance-request-properties determine-priority-fn process-backend-response-fn
     request-abort-callback-factory local-usage-agent
     {:keys [ctrl descriptor request-id request-time] :as request}]
    (let [reservation-status-promise (promise)
          control-mult (async/mult ctrl)
          request (-> request (dissoc :ctrl) (assoc :ctrl-mult control-mult))
          confirm-live-connection-factory #(confirm-live-connection-factory control-mult reservation-status-promise %1)
          confirm-live-connection-without-abort (confirm-live-connection-factory nil)
          waiter-debug-enabled? (utils/request->debug-enabled? request)
          assoc-debug-header (fn [response header value]
                               (if waiter-debug-enabled?
                                 (assoc-in response [:headers header] value)
                                 response))]
      (async/go
        (if waiter-debug-enabled?
          (log/info "process request to" (get-in request [:headers "host"]) "at path" (:uri request))
          (log/debug "process request to" (get-in request [:headers "host"]) "at path" (:uri request)))
        (timers/start-stop-time!
          process-timer
          (let [{:keys [service-id service-description]} descriptor
                {:strs [metric-group]} service-description]
            (send local-usage-agent metrics/update-last-request-time-usage-metric service-id request-time)
            (try
              (let [{:keys [waiter-headers passthrough-headers]} descriptor]
                (meters/mark! (metrics/service-meter service-id "request-rate"))
                (counters/inc! (metrics/service-counter service-id "request-counts" "total"))
                (statsd/inc! metric-group "request")
                (when-let [auth-user (:authorization/user request)]
                  (statsd/unique! metric-group "auth_users" auth-user))
                (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding"))
                (statsd/gauge-delta! metric-group "request_outstanding" +1)
                (metrics/with-timer!
                  (metrics/service-timer service-id "process")
                  (fn [nanos] (statsd/histo! metric-group "process" nanos))
                  (let [instance-request-properties (prepare-request-properties instance-request-properties waiter-headers)
                        start-new-service-fn (fn start-new-service-in-process [] (start-new-service-fn descriptor))
                        priority (determine-priority-fn waiter-headers)
                        reason-map (cond-> {:reason :serve-request
                                            :state {:initial (metrics/retrieve-local-stats-for-service service-id)}
                                            :time request-time
                                            :cid (cid/get-correlation-id)
                                            :request-id request-id}
                                           priority (assoc :priority priority))
                        ; pass false to keep request-state-chan open after control-mult is closed
                        ; request-state-chan should be explicitly closed after the request finishes processing
                        request-state-chan (async/tap control-mult (au/latest-chan) false)
                        queue-timeout-ms (:queue-timeout-ms instance-request-properties)
                        timed-instance (metrics/with-timer
                                         (metrics/service-timer service-id "get-available-instance")
                                         (fa/<? (prepare-instance instance-rpc-chan service-id reason-map
                                                                  start-new-service-fn request-state-chan queue-timeout-ms
                                                                  reservation-status-promise metric-group)))
                        instance (:out timed-instance)
                        instance-elapsed (:elapsed timed-instance)]
                    (statsd/histo! metric-group "get_instance" instance-elapsed)
                    (-> (try
                          (log/info "suggested instance:" (:id instance) (:host instance) (:port instance))
                          (confirm-live-connection-without-abort)
                          (let [endpoint (request->endpoint request waiter-headers)
                                timed-response (metrics/with-timer
                                                 (metrics/service-timer service-id "backend-response")
                                                 (async/<!
                                                   (make-request-fn instance request instance-request-properties
                                                                    passthrough-headers endpoint metric-group)))
                                response-elapsed (:elapsed timed-response)
                                {:keys [error] :as response} (:out timed-response)]
                            (statsd/histo! metric-group "backend_response" response-elapsed)
                            (-> (if error
                                  (let [error-response (handle-response-error error reservation-status-promise service-id request)]
                                    ; must close `request-state-chan` after calling `handle-response-error`
                                    ; which resolves the `reservation-status-promise`
                                    (async/close! request-state-chan)
                                    error-response)
                                  (try
                                    (let [request-abort-callback (request-abort-callback-factory response)
                                          confirm-live-connection-with-abort (confirm-live-connection-factory request-abort-callback)]
                                      (process-backend-response-fn local-usage-agent instance-request-properties descriptor
                                                                   instance request reason-map reservation-status-promise
                                                                   confirm-live-connection-with-abort request-state-chan response))
                                    (catch Exception e
                                      (async/close! request-state-chan)
                                      (handle-process-exception e request))))
                                (assoc :backend-response-latency-ns response-elapsed)
                                (assoc-debug-header "x-waiter-backend-response-ns" (str response-elapsed))))
                          (catch Exception e
                            (async/close! request-state-chan)
                            (handle-process-exception e request)))
                        (assoc :get-instance-latency-ns instance-elapsed
                               :instance instance)
                        (assoc-debug-header "x-waiter-get-available-instance-ns" (str instance-elapsed))))))
              (catch Exception e ; Handle case where we couldn't get an instance
                (counters/dec! (metrics/service-counter service-id "request-counts" "outstanding"))
                (statsd/gauge-delta! metric-group "request_outstanding" -1)
                (handle-process-exception e request)))))))))

(defn wrap-suspended-service
  "Check if a service has been suspended and immediately return a 503 response"
  [handler]
  (fn [{{:keys [suspended-state service-id]} :descriptor :as request}]
    (if (get suspended-state :suspended false)
      (let [{:keys [last-updated-by time]} suspended-state
            response-map (cond-> {:service-id service-id}
                                 time (assoc :suspended-at (du/date-to-str time))
                                 (not (str/blank? last-updated-by)) (assoc :last-updated-by last-updated-by))]
        (log/info "Service has been suspended" response-map)
        (meters/mark! (metrics/service-meter service-id "response-rate" "error" "suspended"))
        (-> {:details response-map, :message "Service has been suspended", :status 503}
            (utils/data->error-response request)))
      (handler request))))

(defn wrap-too-many-requests
  "Check if a service has more pending requests than max-queue-length and immediately return a 503"
  [handler]
  (fn [{{:keys [service-id service-description]} :descriptor :as request}]
    (let [max-queue-length (get service-description "max-queue-length")
          current-queue-length (counters/value (metrics/service-counter service-id "request-counts" "waiting-for-available-instance"))]
      (if (> current-queue-length max-queue-length)
        (let [outstanding-requests (counters/value (metrics/service-counter service-id "request-counts" "outstanding"))
              response-map {:current-queue-length current-queue-length
                            :max-queue-length max-queue-length
                            :outstanding-requests outstanding-requests
                            :service-id service-id}]
          (log/info "Max queue length exceeded" response-map)
          (meters/mark! (metrics/service-meter service-id "response-rate" "error" "queue-length"))
          (-> {:details response-map, :message "Max queue length exceeded", :status 503}
              (utils/data->error-response request)))
        (handler request)))))

(defn determine-priority
  "Retrieves the priority Waiter should use to service this request.
   The position-generator-atom is used to determine how to break ties between equal priority requests.
   If no priority header has been provided, it returns nil."
  [position-generator-atom waiter-headers]
  (when-let [priority (when-let [value (headers/get-waiter-header waiter-headers "priority")]
                        (Integer/parseInt (str value)))]
    (let [position (swap! position-generator-atom inc)]
      (log/info "associating priority" priority "at position" position "with request")
      [priority (unchecked-negate position)])))
