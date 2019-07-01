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
            [clojure.core.async.impl.protocols :as ap]
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
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.io ByteArrayOutputStream InputStream IOException)
           (java.nio ByteBuffer)
           (java.util.concurrent LinkedBlockingQueue ThreadPoolExecutor TimeoutException TimeUnit)
           (javax.servlet ServletOutputStream)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.io EofException)
           (org.eclipse.jetty.server HttpChannel HttpOutput Response)))

(defn check-control
  [control-chan correlation-id]
  (let [state (au/poll! control-chan :still-running)]
    (cond
      (= :still-running state) :still-running
      (= (first state) ::servlet/error) (let [ex (ex-info "Error in server" {:cid correlation-id} (second state))]
                                          (log/error ex "error discovered in check-control")
                                          (throw ex))
      (= (first state) ::servlet/timeout) (let [ex (ex-info "Operation timed out" {:cid correlation-id} (second state))]
                                            (log/error ex "timeout discovered in check-control")
                                            (throw ex))
      :else (let [ex (ex-info "Connection closed while still processing" {:cid correlation-id})]
              (log/error ex "connection closed in check-control")
              (throw ex)))))

(defn confirm-live-connection-factory
  "Confirms that the connection to the client is live by checking the ctrl channel, else it throws an exception."
  [control-mult reservation-status-promise correlation-id error-callback]
  (let [confirm-live-chan (async/tap control-mult (au/sliding-buffer-chan 5))]
    (fn confirm-live-connection []
      (try
        (check-control confirm-live-chan correlation-id)
        (catch Exception e
          ; flag the error as an I/O error as the connection is no longer live
          (deliver reservation-status-promise :client-error)
          (when error-callback
            (error-callback e))
          (throw e))))))

(defn set-idle-timeout!
  "Configures the idle timeout in the response output stream (HttpOutput) to `idle-timeout-ms` ms."
  [output-stream idle-timeout-ms]
  (if (instance? HttpOutput output-stream)
    (try
      (log/debug "executing pill to adjust idle timeout to" idle-timeout-ms "ms.")
      (let [^HttpChannel http-channel (.getHttpChannel ^HttpOutput output-stream)]
        (.setIdleTimeout http-channel idle-timeout-ms))
      (catch Exception e
        (log/error e "gobbling unexpected error while setting idle timeout")))
    (log/info "cannot set idle timeout since output stream is not an instance of HttpOutput")))

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
                                 (log/info "request wants to configure" display-name "to" header-value)
                                 (Integer/parseInt (str header-value))
                                 (catch Exception _
                                   (log/warn "cannot convert header for" display-name "to an int:" header-value)
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
                          (cid/with-correlation-id
                            correlation-id
                            (log/debug "request-state-chan closed")
                            ; assume request did not process successfully if no value in promise
                            (deliver reservation-status-promise :generic-error)
                            (let [status @reservation-status-promise]
                              (log/info "done processing request" status)
                              (when (= :success status)
                                (counters/inc! (metrics/service-counter service-id "request-counts" "successful")))
                              (when (= :generic-error status)
                                (log/error "there was a generic error in processing the request;"
                                            "if this is a client or server related issue, the code needs to be updated."))
                              (when (not= :success-async status)
                                (counters/dec! (metrics/service-counter service-id "request-counts" "outstanding"))
                                (statsd/gauge-delta! metric-group "request_outstanding" -1))
                              (service/release-instance-go instance-rpc-chan instance {:status status, :cid correlation-id, :request-id request-id}))))
                        (fn [e]
                          (cid/with-correlation-id
                            correlation-id
                            (log/error e "error releasing instance!"))))
      instance)))

(defn- classify-error
  "Classifies the error responses from the backend into the following vector:
   - error cause (:client-error, :instance-error or :generic-error),
   - associated error message, and
   - the http status code."
  [error]
  (cond (instance? ExceptionInfo error)
        (let [[error-cause message status] (classify-error (ex-cause error))
              error-cause (or (-> error ex-data :error-cause) error-cause)]
          [error-cause message status])
        (instance? IllegalStateException error)
        [:generic-error (.getMessage error) 400]
        (instance? EofException error)
        [:client-error "Connection unexpectedly closed while streaming request" 400]
        (instance? TimeoutException error)
        [:instance-error (utils/message :backend-request-timed-out) 504]
        :else
        [:instance-error (utils/message :backend-request-failed) 502]))

(defn- handle-response-error
  "Handles error responses from the backend."
  [error reservation-status-promise service-id request]
  (let [[error-cause message status] (classify-error error)
        metrics-map (metrics/retrieve-local-stats-for-service service-id)]
    (deliver reservation-status-promise error-cause)
    (utils/exception->response (ex-info message (assoc metrics-map :status status) error) request)))

(defn make-stream-reader-executor
  "Creates and returns the ThreadPoolExecutor used for reading the input stream."
  [concurrency-level keep-alive-mins queue-limit]
  (let [queue (LinkedBlockingQueue. ^int queue-limit)
        executor (ThreadPoolExecutor. concurrency-level concurrency-level keep-alive-mins TimeUnit/MINUTES queue)]
    (metrics/waiter-gauge #(.getActiveCount executor)
                          "core" "stream-reader" "active-thread-count")
    (metrics/waiter-gauge #(- concurrency-level (.getActiveCount executor))
                          "core" "stream-reader" "available-thread-count")
    (metrics/waiter-gauge #(.getMaximumPoolSize executor)
                          "core" "stream-reader" "max-thread-count")
    (metrics/waiter-gauge #(-> executor .getQueue .size)
                          "core" "stream-reader" "pending-task-count")
    (metrics/waiter-gauge #(.getTaskCount executor)
                          "core" "stream-reader" "scheduled-task-count")
    executor))

(defn- submit-request-streaming-task
  "Submits a task to the executor and invokes the error handler with custom error message if the submission fails."
  [^ThreadPoolExecutor executor runnable-task]
  (try
    (.execute executor runnable-task)
    (catch Throwable throwable
      (log/error "unable to submit task to queue")
      (throw (ex-info "Too many concurrent requests on router"
                      {:details {:executor-max-parallelism (.getMaximumPoolSize executor)
                                 :queue-size (.size (.getQueue executor))}
                       :error-cause :generic-error
                       :status 503}
                      throwable)))))

(let [min-buffer-size 1024
      max-buffer-size 32768
      min-buffer-increment-size 1024
      buffer-increment-mask (bit-not (dec min-buffer-increment-size))]

  (defn stream-http-request
    "Reads data from the input stream and queues it into the provided body channel as a ByteBuffer.
     When no more data is available on the input stream, an asynchronous task is scheduled
     on the provided executor to read data from the input stream later.
     Reports an error to the error handler whenever:
     - there is an error trying to read from the input channel,
     - the body channel fails to accept the byte buffer that was read from the input stream."
    [executor service-id metric-group error-handler-fn request-control-chan streaming-timeout-ms
     ^InputStream input-stream body-ch bytes-streamed]
    (let [bytes-streamed-atom (atom bytes-streamed)
          correlation-id (cid/get-correlation-id)
          stream-http-request-task (fn invoke-stream-http-request []
                                     (cid/with-correlation-id
                                       correlation-id
                                       (stream-http-request
                                         executor service-id metric-group error-handler-fn request-control-chan
                                         streaming-timeout-ms input-stream body-ch @bytes-streamed-atom)))
          stream-error-handler (fn [throwable]
                                 (log/info "request failed after streaming" @bytes-streamed-atom "bytes")
                                 (histograms/update! (metrics/service-histogram service-id "request-size") @bytes-streamed-atom)
                                 (error-handler-fn throwable))]
      (try
        (log/info "starting task to read from input stream, body chan closed:" (ap/closed? body-ch))
        (loop [unreported-bytes-to-statsd 0]
          (let [available-bytes (.available input-stream)
                ;; get a buffer size between min-buffer-size and max-buffer-size
                buffer-size (-> available-bytes
                              (bit-and buffer-increment-mask)
                              (max min-buffer-size)
                              (min max-buffer-size))
                buffer-bytes (byte-array buffer-size)
                _ (log/info "attempting to read bytes from input stream" {:bytes-streamed @bytes-streamed-atom})
                _ (check-control request-control-chan correlation-id)
                bytes-read (.read input-stream buffer-bytes)]
            (log/info bytes-read "bytes read from request this iteration")
            (cond
              (neg? bytes-read)
              (let [bytes-streamed @bytes-streamed-atom]
                (log/info bytes-streamed "bytes streamed from request")
                (histograms/update! (metrics/service-histogram service-id "request-size") @bytes-streamed-atom)
                (async/close! body-ch))

              (pos? bytes-read)
              (if (au/timed-offer!! body-ch (ByteBuffer/wrap buffer-bytes 0 bytes-read) streaming-timeout-ms)
                (let [unreported-bytes-to-statsd' (+ unreported-bytes-to-statsd bytes-read)]
                  (swap! bytes-streamed-atom + bytes-read)
                  (if (pos? (.available input-stream))
                    (do
                      (if (>= unreported-bytes-to-statsd' 1000000)
                        (do
                          (statsd/inc! metric-group "request_bytes" unreported-bytes-to-statsd')
                          (recur 0))
                        (recur unreported-bytes-to-statsd')))
                    (do
                      (when (pos? unreported-bytes-to-statsd')
                        (statsd/inc! metric-group "request_bytes" unreported-bytes-to-statsd'))
                      (log/info "no more bytes available, submitting task to read input stream later")
                      (submit-request-streaming-task executor stream-http-request-task))))
                (do
                  (when (pos? unreported-bytes-to-statsd)
                    (statsd/inc! metric-group "request_bytes" unreported-bytes-to-statsd))
                  (let [description-map {:bytes-pending bytes-read
                                         :bytes-streamed bytes-streamed
                                         :status 503
                                         :streaming-timeout-ms streaming-timeout-ms}]
                    (log/error "unable to stream request bytes" description-map)
                    (stream-error-handler (ex-info "unable to stream request bytes" description-map)))))

              :else
              (do
                (log/info "submitting task to read input stream later") ;; TODO shams remove log
                (submit-request-streaming-task executor stream-http-request-task)))))
        (catch ExceptionInfo ex
          (stream-error-handler ex))
        (catch Throwable th
          ;; error reading bytes from request input stream
          (stream-error-handler (ex-info (.getMessage th) {:error-cause :client-error} th)))))))

(defn- abort-backend-request
  "Attempts to abort the backend request using the provided throwable as the cause.
   Closes the abort-ch if the abort attempt is successful.
   Returns a channel that contains the boolean result of the attempted abort operation."
  [abort-ch throwable correlation-id]
  (async/go
    (cid/with-correlation-id
      correlation-id
      (try
        (let [response-chan (async/promise-chan)
              send-success? (async/>! abort-ch [throwable response-chan])
              _ (log/info "abort backend request accepted:" send-success?)
              [aborted? abort-cause] (when send-success?
                                       (async/<! response-chan))]
          (log/info "backend request aborted:" aborted?)
          (when (and (not aborted?) abort-cause)
            (log/info abort-cause "backend request aborted due to another cause"))
          (when (or aborted? abort-cause)
            (async/close! abort-ch))
          aborted?)
        (catch Throwable ex
          (log/error ex "error in aborting backend request")
          false)))))

(defn input-stream->channel
  "Returns a channel that will contain the ByteBuffers read from the input stream.
   The input stream is read asynchronously by creating tasks on the provided executor.
   It will report any errors while reading data on the provided abort channel."
  [^ThreadPoolExecutor executor service-id metric-group streaming-timeout-ms abort-ch request-control-chan
   ^InputStream input-stream]
  (let [correlation-id (cid/get-correlation-id)
        body-ch (async/chan 2048)
        error-handler-fn (fn handle-request-streaming-error [throwable]
                           (log/error throwable "unable to stream request bytes")
                           (async/<!! (abort-backend-request abort-ch throwable correlation-id))
                           (async/close! body-ch))
        stream-http-request-fn (fn stream-http-request-fn []
                                 (cid/with-correlation-id
                                   correlation-id
                                   (stream-http-request
                                     executor service-id metric-group error-handler-fn request-control-chan
                                     streaming-timeout-ms input-stream body-ch 0)))]
    (try
      (submit-request-streaming-task executor stream-http-request-fn)
      (catch Throwable throwable
        (error-handler-fn throwable)))
    body-ch))

(defn- make-http-request
  "Makes an asynchronous request to the endpoint using Basic authentication."
  [^ThreadPoolExecutor executor ^HttpClient http-client make-basic-auth-fn
   request-method endpoint query-string headers body trailers-fn
   service-id service-password metric-group {:keys [username principal]}
   idle-timeout streaming-timeout-ms output-buffer-size proto-version request-control-chan]
  (let [auth (make-basic-auth-fn endpoint "waiter" service-password)
        headers (headers/assoc-auth-headers headers username principal)
        abort-ch (async/chan 10)
        body' (cond->> body
                (instance? InputStream body)
                (input-stream->channel
                  executor service-id metric-group streaming-timeout-ms abort-ch request-control-chan))]
    (http/request
      http-client
      {:abort-ch abort-ch
       :as :bytes
       :auth auth
       :body body'
       :headers headers
       :fold-chunked-response? (not (hu/http2? proto-version))
       :fold-chunked-response-buffer-size output-buffer-size
       :follow-redirects? false
       :idle-timeout idle-timeout
       :method request-method
       :query-string query-string
       :trailers-fn trailers-fn
       :url endpoint
       :version proto-version})))

(defn make-request
  "Makes an asynchronous http request to the instance endpoint and returns a channel."
  [executor http-clients make-basic-auth-fn service-id->password-fn {:keys [host] :as instance}
   {:keys [body instance-request-overrides query-string request-method trailers-fn] :as request}
   {:keys [initial-socket-timeout-ms output-buffer-size streaming-timeout-ms]}
   passthrough-headers end-route metric-group backend-proto proto-version request-control-chan]
  (let [port-index (get instance-request-overrides :port-index 0)
        port (scheduler/instance->port instance port-index)
        instance-endpoint (scheduler/end-point-url backend-proto host port end-route)
        service-id (scheduler/instance->service-id instance)
        service-password (service-id->password-fn service-id)
        ; Removing expect may be dangerous http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html, but makes requests 3x faster =}
        ; Also remove hop-by-hop headers https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.5.1
        headers (-> (dissoc passthrough-headers "authorization" "expect")
                    (headers/dissoc-hop-by-hop-headers proto-version)
                    (assoc "cookie" (auth/remove-auth-cookie (get passthrough-headers "cookie"))))
        waiter-debug-enabled? (utils/request->debug-enabled? request)]
    (try
      (let [content-length-str (get passthrough-headers "content-length")
            content-length (if content-length-str (Long/parseLong content-length-str) 0)]
        (when (and (integer? content-length) (pos? content-length))
          ; computing the actual bytes will currently require synchronously reading all data in the request body
          (histograms/update! (metrics/service-histogram service-id "request-content-length") content-length)
          (statsd/inc! metric-group "request_content_length" content-length)))
      (catch Exception e
        (log/error e "unable to track content-length on request")))
    (when waiter-debug-enabled?
      (log/info "connecting to" instance-endpoint "using" proto-version))
    (let [auth-user-map (handler/make-auth-user-map request)
          http-client (hu/select-http-client backend-proto http-clients)]
      (make-http-request
        executor http-client make-basic-auth-fn request-method instance-endpoint query-string headers body trailers-fn
        service-id service-password metric-group auth-user-map initial-socket-timeout-ms streaming-timeout-ms
        output-buffer-size proto-version request-control-chan))))

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
                              bytes-read (if buffer (count buffer) -1)]
                          (if-not (= -1 bytes-read)
                            (do
                              (meters/mark! throughput-meter bytes-read)
                              (if (or (zero? bytes-read) ;; don't write empty buffer, channel may be potentially closed
                                      (timers/start-stop-time!
                                        stream-onto-resp-chan
                                        ;; don't wait forever to write to server
                                        (au/timed-offer! resp-chan buffer streaming-timeout-ms)))
                                [(+ bytes-streamed bytes-read) true]
                                (let [ex (ex-info "Unable to stream, back pressure in resp-chan. Is connection live?"
                                                  {:cid (cid/get-correlation-id), :bytes-streamed bytes-streamed})]
                                  (meters/mark! stream-back-pressure)
                                  (deliver reservation-status-promise :client-error)
                                  (request-abort-callback ex)
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
                          (log/error "error occurred after streaming" bytes-streamed "bytes in response.")
                          ; Handle lower down
                          (throw e)))]
                  (let [bytes-reported-to-statsd'
                        (let [unreported-bytes (- bytes-streamed' bytes-reported-to-statsd)]
                          (if (or (and (not more-bytes-possibly-available?) (pos? unreported-bytes))
                                  (>= unreported-bytes 1000000))
                            (do
                              (statsd/inc! metric-group "response_bytes" unreported-bytes)
                              bytes-streamed')
                            bytes-reported-to-statsd))]
                    (when more-bytes-possibly-available?
                      (recur bytes-streamed' bytes-reported-to-statsd'))))))))
        (catch Exception e
          (meters/mark! stream-exception-meter)
          ;; assign instance-error or client-error correctly
          (let [[error-cause message _] (classify-error e)]
            (log/error (.getMessage e) "identified as" error-cause "with message" message)
            (deliver reservation-status-promise error-cause))
          (log/info "sending poison pill to response channel")
          (let [poison-pill-function (poison-pill-fn (cid/get-correlation-id))]
            (when-not (au/timed-offer! resp-chan poison-pill-function 5000)
              (log/info "poison pill offer on response channel timed out!")
              (when-let [output-stream @output-stream-atom]
                (log/info "invoking poison pill directly on output stream")
                (poison-pill-function output-stream))))
          (log/error e "exception occurred while streaming response for" service-id))
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
  ;; TODO rewrite to use (abort-backend-request ...)
  (fn abort-http-request-callback [^Exception e]
    (let [ex (if (instance? IOException e) e (IOException. e))
          aborted (if-let [request (:request response)]
                    (.abort request ex)
                    (log/warn "unable to abort as request not found inside response!"))]
      (log/info "aborted backend request:" aborted))))

(defn- track-trailers
  "Adds logging for tracking response trailers for requests."
  [{:keys [trailers] :as response}]
  (if trailers
    (let [correlation-id (cid/get-correlation-id)
          trailers-copy-ch (async/chan 5)]
      (async/go
        (cid/with-correlation-id
          correlation-id
          (when-let [trailers-map (async/<! trailers)]
            (log/info "response trailers:" trailers-map)
            (async/>! trailers-copy-ch trailers-map))
          (log/info "closing trailers channel")
          (async/close! trailers-copy-ch)))
      (assoc response :trailers trailers-copy-ch))
    response))

(defn process-http-response
  "Processes a response resulting from a http request.
   It includes book-keeping for async requests and asynchronously streaming the content."
  [post-process-async-request-response-fn _ instance-request-properties descriptor instance
   {:keys [uri] :as request} reason-map reservation-status-promise confirm-live-connection-with-abort
   request-state-chan {:keys [status] :as response}]
  (when (utils/request->debug-enabled? request)
    (log/info "backend response status:" (:status response) "and headers:" (:headers response)))
  (let [{:keys [service-description service-id]} descriptor
        {:strs [backend-proto blacklist-on-503 metric-group]} service-description
        waiter-debug-enabled? (utils/request->debug-enabled? request)
        resp-chan (async/chan 5)]
    (when (and blacklist-on-503 (hu/service-unavailable? request response))
      (log/info "service unavailable according to response status"
                {:instance instance
                 :response (select-keys response [:headers :status])})
      (deliver reservation-status-promise :instance-busy))
    (meters/mark! (metrics/service-meter service-id "response-status-rate" (str status)))
    (counters/inc! (metrics/service-counter service-id "request-counts" "waiting-to-stream"))
    (confirm-live-connection-with-abort)
    (let [{:keys [location query-string]} (extract-async-request-response-data response uri)
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
                       service-id metric-group backend-proto instance (handler/make-auth-user-map request)
                       reason-map instance-request-properties location query-string))
          (assoc :body resp-chan)
          (track-trailers)
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
          {:keys [uri] :as request} (-> request (dissoc :ctrl) (assoc :ctrl-mult control-mult))
          correlation-id (cid/get-correlation-id)
          confirm-live-connection-factory #(confirm-live-connection-factory
                                             control-mult reservation-status-promise correlation-id %1)
          confirm-live-connection-without-abort (confirm-live-connection-factory nil)
          waiter-debug-enabled? (utils/request->debug-enabled? request)
          assoc-debug-header (fn [response header value]
                               (if waiter-debug-enabled?
                                 (assoc-in response [:headers header] value)
                                 response))]
      (async/go
        (if waiter-debug-enabled?
          (log/info "process request to" (get-in request [:headers "host"]) "at path" uri)
          (log/debug "process request to" (get-in request [:headers "host"]) "at path" uri))
        (timers/start-stop-time!
          process-timer
          (let [{:keys [service-id service-description]} descriptor
                {:strs [metric-group]} service-description
                backend-proto (or (get-in request [:instance-request-overrides :backend-proto])
                                  (get service-description "backend-proto"))]
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
                                            :cid correlation-id
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
                        instance-elapsed (:elapsed timed-instance)
                        proto-version (hu/backend-protocol->http-version backend-proto)
                        request-control-chan (async/tap control-mult (au/latest-chan))]
                    (statsd/histo! metric-group "get_instance" instance-elapsed)
                    (-> (try
                          (log/info "suggested instance:" (:id instance) (:host instance) (:port instance))
                          (confirm-live-connection-without-abort)
                          (let [timed-response (metrics/with-timer
                                                 (metrics/service-timer service-id "backend-response")
                                                 (async/<!
                                                   (make-request-fn instance request instance-request-properties
                                                                    passthrough-headers uri metric-group backend-proto
                                                                    proto-version request-control-chan)))
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
                        (update :headers headers/dissoc-hop-by-hop-headers proto-version)
                        (assoc :get-instance-latency-ns instance-elapsed
                               :instance instance
                               :protocol proto-version)
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
        (log/info "service has been suspended" response-map)
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
          (log/info "max queue length exceeded" response-map)
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

(defn make-health-check-request
  "Makes a health check request to the backend using the specified proto and port from the descriptor.
   Returns the health check response from an arbitrary backend or the failure response."
  [process-request-handler-fn idle-timeout-ms {:keys [descriptor] :as request}]
  (async/go
    (try
      (let [{:keys [service-description]} descriptor
            {:strs [health-check-url health-check-port-index]} service-description
            health-check-protocol (scheduler/service-description->health-check-protocol service-description)
            ctrl-ch (async/chan)
            attach-empty-content (fn attach-empty-content [request]
                                   (-> request
                                     (assoc :body nil)
                                     (assoc :content-length 0)
                                     (update :headers assoc
                                             "accept" "*/*"
                                             "content-length" 0)))
            output-stream (ByteArrayOutputStream.)
            servlet-output-stream (proxy [ServletOutputStream] []
                                    (close [] (.close output-stream))
                                    (flush [] (.flush output-stream))
                                    (write [data]
                                      (try
                                        (if (integer? data)
                                          (.write output-stream ^int data)
                                          (.write output-stream ^bytes data))
                                        (catch Exception ex
                                          (async/put! ctrl-ch [::error ex])))))
            servlet-response (proxy [Response] [nil nil]
                               (getOutputStream [] servlet-output-stream)
                               (flushBuffer [] (.flush servlet-output-stream)))
            new-request (-> request
                          (select-keys [:character-encoding :client-protocol :content-type :descriptor :headers
                                        :internal-protocol :remote-addr :request-id :request-time :router-id
                                        :scheme :server-name :server-port :support-info])
                          (attach-empty-content)
                          (assoc :ctrl ctrl-ch
                                 ;; override the protocol and port used while talking to the backend
                                 :instance-request-overrides {:backend-proto health-check-protocol
                                                              :port-index health-check-port-index}
                                 :request-method :get
                                 :uri health-check-url))
            response-ch (process-request-handler-fn new-request)
            timeout-ch (async/timeout idle-timeout-ms)
            [response source-ch] (async/alts! [response-ch timeout-ch] :priority true)
            {:keys [body] :as health-check-response} (cond-> response
                                                       (au/chan? response) (async/<!))
            body-str (cond
                       (au/chan? body) (do
                                         (async/<! (servlet/write-body! body servlet-response new-request))
                                         (String. (.toByteArray output-stream)))
                       body (str body))]
        (async/close! ctrl-ch)
        (-> health-check-response
          (assoc :body body-str)
          (assoc :result (if (= source-ch timeout-ch) :timed-out :received-response))))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn ping-service
  "Performs a health check on an arbitrary instance of the service specified in the descriptor.
   If the service is not running, an instance will be started.
   The response body contains the following map: {:ping-response ..., :service-state ...}"
  [process-request-handler-fn service-state-fn {:keys [descriptor headers] :as request}]
  (async/go
    (try
      (let [{:keys [core-service-description service-id]} descriptor
            idle-timeout-ms (Integer/parseInt (get headers "x-waiter-timeout" "300000"))
            ping-response (async/<! (make-health-check-request process-request-handler-fn idle-timeout-ms request))]
        (merge
          (dissoc ping-response [:body :error-chan :headers :request :result :status :trailers])
          (utils/clj->json-response
            {:ping-response (select-keys ping-response [:body :headers :result :status])
             :service-description core-service-description
             :service-state (service-state-fn service-id)}))))))
