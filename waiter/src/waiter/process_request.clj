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
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as protocols]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [full.async :as fa]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [qbits.jet.client.http :as http]
            [qbits.jet.servlet :as servlet]
            [waiter.async-request :as async-req]
            [waiter.auth.authentication :as auth]
            [waiter.correlation-id :as cid]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.request-log :as rlog]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.statsd :as statsd]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.io ByteArrayOutputStream InputStream IOException)
           (java.nio ByteBuffer)
           (java.util.concurrent TimeoutException)
           (javax.servlet ReadListener ServletInputStream ServletOutputStream)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.io EofException)
           (org.eclipse.jetty.server HttpChannel HttpOutput Response)))

(defn make-auth-user-map
  "Creates a map containing the username and principal from a request"
  [{:keys [authorization/metadata authorization/principal]}]
  {:metadata metadata
   :principal principal})

(defn check-control
  [control-chan correlation-id]
  (let [state (au/poll! control-chan :still-running)]
    (cond
      (= :still-running state) :still-running
      (= (first state) ::servlet/error) (throw (ex-info "Error in server" {:cid correlation-id} (second state)))
      (= (first state) ::servlet/timeout) (throw (ex-info "Operation timed out" {:cid correlation-id} (second state)))
      :else (throw (ex-info "Connection closed while still processing" {:cid correlation-id})))))

(defn classify-error
  "Classifies the error responses from the backend into the following vector:
   - error cause (:client-eagerly-closed, :client-error, :instance-error or :generic-error),
   - associated error message,
   - the http status code, and
   - the canonical name of the exception that 'caused' the error."
  [error]
  (let [error-class (-> error .getClass .getCanonicalName)
        error-message (str (.getMessage error))
        classification (cond (instance? ExceptionInfo error)
                             (if-let [ex-info-cause (ex-cause error)]
                               (let [[error-cause message status error-class] (classify-error ex-info-cause)
                                     error-cause (or (-> error ex-data :error-cause) error-cause)]
                                 [error-cause message status error-class])
                               (let [error-status (or (-> error ex-data :status) http-500-internal-server-error)
                                     error-cause (or (-> error ex-data :error-cause) :generic-error)]
                                 [error-cause error-message error-status error-class]))
                             (instance? IllegalStateException error)
                             [:generic-error error-message http-400-bad-request error-class]
                             ;; internal_error due to reset stream for http/1 requests means client send bad data to server
                             ;; TODO shams verify http/1 request
                             (and (instance? IOException error)
                                  (= "internal_error" error-message)
                                  (when-let [^StackTraceElement stack-element (some-> error (.getStackTrace) (seq) (first))]
                                    (and (str/ends-with? (.getClassName stack-element) "HttpReceiverOverHTTP2")
                                         (= (.getMethodName stack-element) "onReset"))))
                             [:client-error "Client send invalid data to HTTP/2 backend" http-400-bad-request error-class]
                             ;; cancel_stream_error is used to indicate that the stream is no longer needed
                             (and (instance? IOException error) (= "cancel_stream_error" error-message))
                             [:client-error "Client action means stream is no longer needed" http-400-bad-request error-class]
                             ;; no_error is used to indicate that the stream is no longer needed
                             ;; HTTP2 spec: The associated condition is not a result of an error...indicate graceful shutdown of a connection.
                             (and (instance? IOException error) (= "no_error" error-message))
                             [:generic-error "Server closed connection as stream no longer needed" http-400-bad-request error-class]
                             ;; connection has already been closed by the client
                             (and (instance? EofException error) (= "reset" error-message))
                             [:client-eagerly-closed "Connection eagerly closed by client" http-400-bad-request error-class]
                             (instance? EofException error)
                             [:client-error "Connection unexpectedly closed while streaming request" http-400-bad-request error-class]
                             (instance? TimeoutException error)
                             [:instance-error (utils/message :backend-request-timed-out) http-504-gateway-timeout error-class]
                             :else
                             [:instance-error (utils/message :backend-request-failed) http-502-bad-gateway error-class])
        error-cause (first classification)]
    (log/info error-class error-message "identified as" error-cause)
    classification))

(defn- determine-client-error
  "Classifies the error into one of :client-eagerly-closed or :client-error"
  [error]
  (let [[error-cause _ _] (classify-error error)]
    (or (get #{:client-eagerly-closed :client-error} error-cause)
        :client-error)))

(defn confirm-live-connection-factory
  "Confirms that the connection to the client is live by checking the ctrl channel, else it throws an exception."
  [control-mult reservation-status-promise correlation-id error-callback]
  (let [confirm-live-chan (async/tap control-mult (au/sliding-buffer-chan 5))]
    (fn confirm-live-connection []
      (try
        (check-control confirm-live-chan correlation-id)
        (catch Throwable throwable
          ; flag the error as an I/O error as the connection is no longer live
          (deliver reservation-status-promise (determine-client-error throwable))
          (log/debug throwable "error while checking for live connection")
          (when error-callback
            (error-callback throwable))
          (throw throwable))))))

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

(defn- lookup-configured-timeout
  "Parses the header value to an integer and return it if it is inside the allowed maximum value.
   Else it returns the old value."
  ([old-value header-value display-name]
   (lookup-configured-timeout old-value header-value display-name nil))
  ([old-value header-value display-name allowed-max-value]
   (let [parsed-value (when header-value
                        (try
                          (log/info "request wants to configure" display-name "to" header-value)
                          (Integer/parseInt (str header-value))
                          (catch Exception _
                            (log/warn "cannot convert header for" display-name "to an int:" header-value)
                            nil)))]
     (if (and parsed-value (pos? parsed-value))
       (if (and (integer? allowed-max-value)
                (> parsed-value allowed-max-value))
         (do
           (log/warn "limiting" display-name "to" allowed-max-value)
           allowed-max-value)
         parsed-value)
       old-value))))

(def ^:const one-hour-in-millis (-> 1 t/hours t/in-millis))

(defn prepare-request-properties
  [instance-request-properties waiter-headers]
  (-> instance-request-properties
    (update :async-check-interval-ms lookup-configured-timeout
            (headers/get-waiter-header waiter-headers "async-check-interval") "async request check interval")
    (update :async-request-timeout-ms lookup-configured-timeout
            (headers/get-waiter-header waiter-headers "async-request-timeout") "async request timeout" one-hour-in-millis)
    (update :initial-socket-timeout-ms lookup-configured-timeout
            (headers/get-waiter-header waiter-headers "timeout") "socket timeout")
    (update :queue-timeout-ms lookup-configured-timeout
            (headers/get-waiter-header waiter-headers "queue-timeout") "instance timeout")
    (update :streaming-timeout-ms lookup-configured-timeout
            (headers/get-waiter-header waiter-headers "streaming-timeout") "streaming timeout")))

(defn- prepare-instance
  "Tries to acquire an instance and set up a mechanism to release the instance when `request-state-chan` is closed.
   Takes `populate-maintainer-chan!`, `service-id` and `reason-map` to acquire the instance.
   If an exception has occurred, no instance was acquired.
   Returns the instance if it was acquired successfully, or an exception if there was an error"
  [populate-maintainer-chan! service-id {:keys [request-id] :as reason-map} start-new-service-fn request-state-chan
   queue-timeout-ms reservation-status-promise metric-group]
  (fa/go-try
    (log/debug "retrieving instance for" service-id "using" (dissoc reason-map :cid :time))
    (let [correlation-id (cid/get-correlation-id)
          instance (fa/<? (service/get-available-instance
                            populate-maintainer-chan! service-id reason-map start-new-service-fn queue-timeout-ms metric-group))]
      (au/on-chan-close request-state-chan
                        (fn on-request-state-chan-close []
                          (cid/with-correlation-id
                            correlation-id
                            (log/debug "request-state-chan closed")
                            ; assume request did not process successfully if no value in promise
                            (deliver reservation-status-promise :generic-error)
                            (let [status @reservation-status-promise
                                  reservation-result {:cid correlation-id :request-id request-id :status status}]
                              (log/info "done processing request" status)
                              (when (= :success status)
                                (counters/inc! (metrics/service-counter service-id "request-counts" "successful")))
                              (when (contains? #{:client-error :generic-error :instance-error} status)
                                (counters/inc! (metrics/service-counter service-id "request-counts" (name status))))
                              (when (= :generic-error status)
                                (log/error "there was a generic error in processing the request;"
                                           "if this is a client or server related issue, the code needs to be updated."))
                              (when (not= :success-async status)
                                (counters/dec! (metrics/service-counter service-id "request-counts" "outstanding"))
                                (statsd/gauge-delta! metric-group "request_outstanding" -1))
                              (service/release-instance-go populate-maintainer-chan! instance reservation-result))))
                        (fn [e]
                          (cid/with-correlation-id
                            correlation-id
                            (log/error e "error releasing instance!"))))
      instance)))

(defn- handle-response-error
  "Handles error responses from the backend."
  [error reservation-status-promise service-id request]
  (let [[error-cause message status error-class] (classify-error error)
        metrics-map (metrics/retrieve-local-stats-for-service service-id)
        error-map (assoc metrics-map 
                    :error-class error-class
                    :status status)]
    (deliver reservation-status-promise error-cause)
    (utils/exception->response (ex-info message error-map error) request)))

(let [min-buffer-size 1024
      max-buffer-size 32768
      min-buffer-increment-size 1024
      buffer-increment-mask (bit-not (dec min-buffer-increment-size))]

  (defn read-available-bytes!!
    "Reads the available bytes from the input stream and propagates it to the body channel.
     Throws an error if the offer to the body channel times out.
     Returns the bytes read from the input stream."
    [streaming-timeout-ms error-handler-fn ^ServletInputStream input-stream body-ch]
    (let [bytes-available (.available input-stream)
          buffer-size (-> bytes-available
                        (bit-and buffer-increment-mask)
                        (max min-buffer-size)
                        (min max-buffer-size))
          buffer-bytes (byte-array buffer-size)
          bytes-read (.read input-stream buffer-bytes)]
      (log/debug "processed bytes from the request input stream" {:available bytes-available :read bytes-read})
      (when (pos? bytes-read)
        (let [byte-buffer (ByteBuffer/wrap buffer-bytes 0 bytes-read)]
          (when-not (au/timed-offer!! body-ch byte-buffer streaming-timeout-ms)
            (let [description-map {:bytes-pending bytes-read
                                   :status http-503-service-unavailable
                                   :streaming-timeout-ms streaming-timeout-ms}]
              (log/error "unable to stream request bytes" description-map)
              (error-handler-fn (ex-info "unable to stream request bytes" description-map))))))
      bytes-read)))

(defn servlet-input-stream->channel
  "Returns a channel that will contain the ByteBuffers read from the input stream.
   The input stream is read asynchronously using the ReadListener from the servlet api.
   Bytes read are reported to statsd and codahale metrics.
   It will report any errors while reading data on the provided abort channel."
  [service-id metric-group streaming-timeout-ms abort-ch ctrl-ch ^ServletInputStream input-stream]
  (let [correlation-id (cid/get-correlation-id)
        request-body-streaming-counter (metrics/service-counter service-id "request-counts" "request-body-streaming")
        body-ch (async/chan 2048)
        report-request-size-metrics (let [bytes-streamed-atom (atom 0)
                                          statsd-unreported-bytes-atom (atom 0)
                                          throughput-meter (metrics/service-meter service-id "streaming" "request-bytes")
                                          throughput-meter-global (metrics/waiter-meter "streaming" "request-bytes")
                                          throughput-iterations-meter (metrics/service-meter service-id "streaming" "request-iterations")
                                          throughput-iterations-meter-global (metrics/waiter-meter "streaming" "request-iterations")]
                                      (fn report-request-size-metrics [bytes-read complete?]
                                        (try
                                          (when (pos? bytes-read)
                                            (meters/mark! throughput-meter bytes-read)
                                            (meters/mark! throughput-meter-global bytes-read)
                                            (meters/mark! throughput-iterations-meter)
                                            (meters/mark! throughput-iterations-meter-global)
                                            (swap! bytes-streamed-atom + bytes-read)
                                            (swap! statsd-unreported-bytes-atom + bytes-read))
                                          (if complete?
                                            (let [bytes-streamed @bytes-streamed-atom
                                                  unreported-bytes-to-statsd @statsd-unreported-bytes-atom]
                                              (log/info "streamed" bytes-streamed "bytes from request input stream")
                                              (histograms/update! (metrics/service-histogram service-id "request-size") bytes-streamed)
                                              (statsd/inc! metric-group "request_bytes" unreported-bytes-to-statsd))
                                            (let [unreported-bytes-to-statsd @statsd-unreported-bytes-atom]
                                              (when (>= unreported-bytes-to-statsd 1000000)
                                                (statsd/inc! metric-group "request_bytes" unreported-bytes-to-statsd)
                                                (swap! statsd-unreported-bytes-atom - unreported-bytes-to-statsd))))
                                          (catch Throwable throwable
                                            (log/error throwable "error in reporting request size metrics")))))
        complete-request-streaming (let [complete-triggered-promise (promise)]
                                     (fn complete-request-streaming [message throwable]
                                       (cid/with-correlation-id
                                         correlation-id
                                         (let [complete-trigger-id (utils/unique-identifier)]
                                           (deliver complete-triggered-promise complete-trigger-id)
                                           (when (= complete-trigger-id @complete-triggered-promise)
                                             ;; avoid decrementing the counter multiple times
                                             (counters/dec! request-body-streaming-counter)
                                             (report-request-size-metrics 0 true))
                                           (let [[error-cause _ _] (when throwable
                                                                     (classify-error throwable))]
                                             (if (and throwable (not= :client-eagerly-closed error-cause))
                                               (let [identifier-map {:identifier complete-trigger-id}
                                                     ;; the callback is necessary as there is a data race between aborting the
                                                     ;; request and closing of the body channel triggering a normal complete before
                                                     ;; the abort request gets processed.
                                                     callback (fn complete-request-streaming-abort-result-callback [aborted?]
                                                                (cid/with-correlation-id
                                                                  correlation-id
                                                                  (log/info "result of aborted request:" aborted? identifier-map)
                                                                  (async/close! body-ch)))]
                                                 (log/info throwable "aborting request" message identifier-map)
                                                 (when-not (async/put! abort-ch [throwable callback])
                                                   ;; abort channel already closed, cleanup by closing the body channel
                                                   (async/close! body-ch)))
                                               (do
                                                 (log/debug "closing request input body" message)
                                                 (async/close! body-ch))))))))]
    (counters/inc! request-body-streaming-counter)
    (when ctrl-ch
      (async/go
        (let [ctrl-data (async/<! ctrl-ch)
              ctrl-ex (some-> ctrl-data second)]
          (complete-request-streaming "ctrl-chan has been triggered" ctrl-ex)
          (async/close! ctrl-ch))))
    (try
      (.setReadListener
        input-stream
        (reify ReadListener
          (onAllDataRead [_]
            (complete-request-streaming "all data has been read" nil))
          (onDataAvailable [_]
            (try
              (cid/with-correlation-id
                correlation-id
                (log/debug "reading available bytes from request input stream")
                (loop []
                  (let [bytes-read (read-available-bytes!! streaming-timeout-ms complete-request-streaming input-stream body-ch)]
                    (when (pos? bytes-read)
                      (report-request-size-metrics bytes-read false))
                    (when (and (utils/non-neg? bytes-read) (.isReady input-stream))
                      (recur)))))
              (catch Throwable throwable
                (let [ex (ex-info "error reading available data" {:error-cause :client-error} throwable)]
                  (complete-request-streaming "there was error in streaming data" ex))
                (throw throwable))))
          (onError [_ throwable]
            (let [ex (ex-info "error in frontend request" {:error-cause :client-error} throwable)]
              (complete-request-streaming "there was error in request data stream" ex)))))
      (catch Throwable throwable
        (let [ex (ex-info "error in registering read listener on request stream" {:error-cause :generic-error} throwable)]
          (complete-request-streaming "there was error in registering read listener" ex))))
    body-ch))

(defn- make-http-request
  "Makes an asynchronous request to the endpoint using Basic authentication."
  [^HttpClient http-client make-basic-auth-fn
   request-method endpoint query-string headers body trailers-fn
   service-id service-password metric-group {:keys [metadata principal]}
   idle-timeout streaming-timeout-ms output-buffer-size proto-version ctrl-ch]
  (let [auth (make-basic-auth-fn endpoint "waiter" service-password)
        headers (headers/assoc-auth-headers headers principal metadata)
        abort-ch (async/chan 10)
        body' (cond->> body
                (instance? ServletInputStream body)
                (servlet-input-stream->channel service-id metric-group streaming-timeout-ms abort-ch ctrl-ch))]
    (http/request
      http-client
      (cond-> {:abort-ch abort-ch
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
               :url endpoint
               :version proto-version}
        ;; some of our http/1 backends do not support trailers, so disable for all of them
        (hu/http2? proto-version) (assoc :trailers-fn trailers-fn)))))

(defn make-request
  "Makes an asynchronous http request to the instance endpoint and returns a channel."
  [http-clients make-basic-auth-fn service-id->password-fn {:keys [host] :as instance}
   {:keys [body ctrl-mult instance-request-overrides query-string request-method trailers-fn] :as request}
   {:keys [initial-socket-timeout-ms output-buffer-size streaming-timeout-ms]}
   passthrough-headers end-route metric-group backend-proto proto-version]
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
        ctrl-ch (when ctrl-mult
                  (async/tap ctrl-mult (au/latest-chan) true))
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
    (let [auth-user-map (make-auth-user-map request)
          http-client (hu/select-http-client backend-proto http-clients)]
      (make-http-request
        http-client make-basic-auth-fn request-method instance-endpoint query-string headers body trailers-fn
        service-id service-password metric-group auth-user-map initial-socket-timeout-ms streaming-timeout-ms
        output-buffer-size proto-version ctrl-ch))))

(defn extract-async-request-response-data
  "Helper function that inspects the response and returns the location and query-string if the response
   has a status code is 202 and contains a location header with a base path."
  [{:keys [headers status]} endpoint]
  (when (= status http-202-accepted)
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
   {:keys [requests-streaming requests-waiting-to-stream service-id
           stream stream-back-pressure stream-complete-rate stream-exception-meter
           stream-onto-resp-chan stream-read-body stream-request-rate
           throughput-iterations-meter throughput-iterations-meter-global throughput-meter throughput-meter-global]}]
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
                              (meters/mark! throughput-meter-global bytes-read)
                              (meters/mark! throughput-iterations-meter)
                              (meters/mark! throughput-iterations-meter-global)
                              (if (or (zero? bytes-read) ;; don't write empty buffer, channel may be potentially closed
                                      (timers/start-stop-time!
                                        stream-onto-resp-chan
                                        ;; don't wait forever to write to server
                                        (au/timed-offer! resp-chan buffer streaming-timeout-ms)))
                                [(+ bytes-streamed bytes-read) true]
                                (let [timeout-ch (async/timeout 5000)
                                      [{:keys [error]} source-ch] (async/alts! [error-chan timeout-ch] :priority true)
                                      _ (when (= timeout-ch source-ch)
                                          (log/warn "timeout while reading from error-chan"))
                                      ex (ex-info "Unable to stream, back pressure in resp-chan. Is connection live?"
                                                  {:body-source-closed? (protocols/closed? body)
                                                   :body-target-closed? (protocols/closed? resp-chan)
                                                   :bytes-pending bytes-read
                                                   :bytes-streamed bytes-streamed
                                                   :correlation-id (cid/get-correlation-id)
                                                   :error-cause :client-error}
                                                  error)]
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
                          ; Handle lower down
                          (throw (ex-info (str "error occurred after streaming " bytes-streamed " bytes in response") {} e))))]
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
          (log/info e "exception occurred while streaming response for" service-id)
          (meters/mark! stream-exception-meter)
          (let [[error-cause _ _] (classify-error e)]
            (deliver reservation-status-promise error-cause)
            (when-not (= :client-eagerly-closed error-cause)
              (log/info "sending poison pill to response channel")
              (let [poison-pill-function (poison-pill-fn (cid/get-correlation-id))]
                (when-not (au/timed-offer! resp-chan poison-pill-function 5000)
                  (log/info "poison pill offer on response channel timed out!")
                  (when-let [output-stream @output-stream-atom]
                    (log/info "invoking poison pill directly on output stream")
                    (poison-pill-function output-stream)))))))
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
  [{:keys [abort-ch] :as response}]
  (fn abort-backend-request-callback [^Exception ex]
    (let [correlation-id (cid/get-correlation-id)
          callback (fn abort-http-request-result-callback [aborted?]
                     (cid/with-correlation-id
                       correlation-id
                       (log/info ex "aborted backend request:" aborted?)))]
      (when-not abort-ch
        (log/error "abort-ch not available in response"))
      (log/debug "aborting backend request")
      (when-not (and abort-ch (async/put! abort-ch [ex callback]))
        (log/debug "aborting backend request directly")
        (if-let [request (:request response)]
          (let [aborted? (.abort request ex)]
            (log/info ex "result of aborting backend request directly:" aborted?))
          (log/error ex "cannot abort request as it is not available in response"))))))

(defn- introspect-trailers
  "Introspects and logs trailers received in the response"
  [{:keys [trailers] :as response}]
  (if trailers
    (let [trailers-ch (async/promise-chan)
          correlation-id (cid/get-correlation-id)]
      (async/go
        (when-let [trailers-data (async/<! trailers)]
          (async/>! trailers-ch trailers-data)
          (cid/cinfo correlation-id "response trailers:" trailers-data))
        (async/close! trailers-ch))
      (assoc response :trailers trailers-ch))
    response))

(defn- forward-grpc-status-headers-in-trailers
  "Adds logging for tracking response trailers for requests.
   When only headers are provided jetty terminates the request with an empty data frame,
   we work around that limitation by sending trailers that carry the same grpc error message."
  [{:keys [headers trailers] :as response}]
  (if trailers
    (let [correlation-id (cid/get-correlation-id)
          trailers-copy-ch (async/chan 1)
          grpc-headers (select-keys headers ["grpc-message" "grpc-status"])]
      (if (seq grpc-headers)
        (do
          (async/go
            (cid/with-correlation-id
              correlation-id
              (try
                (let [trailers-map (async/<! trailers)
                      modified-trailers (merge grpc-headers trailers-map)]
                  (log/info "attaching grpc headers into trailer:" grpc-headers)
                  (async/>! trailers-copy-ch modified-trailers))
                (catch Throwable th
                  (log/error th "error in parsing response trailers")))
              (log/info "closing response trailers channel")
              (async/close! trailers-copy-ch)))
          (assoc response :trailers trailers-copy-ch))
        response))
    response))

(defn- handle-grpc-error-response
  "Eagerly terminates grpc requests with error status headers.
   We cannot rely on jetty to close the request for us in a timely manner,
   please see https://github.com/eclipse/jetty.project/issues/3842 for details."
  [{:keys [abort-ch body error-chan trailers] :as response} request backend-proto reservation-status-promise]
  (let [request-headers (:headers request)
        {:strs [grpc-status]} (:headers response)
        proto-version (hu/backend-protocol->http-version backend-proto)]
    (when (and (hu/grpc? request-headers proto-version)
               (not (str/blank? grpc-status))
               (not= (str grpc-0-ok) grpc-status)
               (au/chan? body))
      (log/info "eagerly closing response body as grpc status is" grpc-status)
      ;; mark the request as successful, grpc failures are reported in the headers
      (deliver reservation-status-promise :success)
      (when abort-ch
        ;; disallow aborting the request as we deem the request a success and will trigger normal
        ;; request completion by closing the body channel
        (async/close! abort-ch))
      ;; stop writing any content in the body from stream-http-response and trigger request completion
      (async/close! body)
      (when trailers
        ;; do not expect any trailers either in the response
        (async/close! trailers))
      ;; eagerly close channel as request is deemed a success and avoid blocking in stream-http-response
      (async/close! error-chan))
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
        {:strs [backend-proto metric-group]} service-description
        waiter-debug-enabled? (utils/request->debug-enabled? request)
        resp-chan (async/chan 5)]
    (when (hu/service-unavailable? request response)
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
                       descriptor instance (make-auth-user-map request)
                       reason-map instance-request-properties location query-string))
        (utils/attach-waiter-source :backend)
        (introspect-trailers)
        (handle-grpc-error-response request backend-proto reservation-status-promise)
        (forward-grpc-status-headers-in-trailers)
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
    [make-request-fn populate-maintainer-chan! start-new-service-fn
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
                                         (fa/<? (prepare-instance populate-maintainer-chan! service-id reason-map
                                                                  start-new-service-fn request-state-chan queue-timeout-ms
                                                                  reservation-status-promise metric-group)))
                        instance (:out timed-instance)
                        instance-elapsed (:elapsed timed-instance)
                        proto-version (hu/backend-protocol->http-version backend-proto)]
                    (when-not instance
                      (throw (ex-info "Suggested instance was nil" reason-map)))
                    (statsd/histo! metric-group "get_instance" instance-elapsed)
                    (-> (try
                          (log/info "suggested instance:" (:id instance) (:host instance) (:port instance))
                          (confirm-live-connection-without-abort)
                          (let [timed-response (metrics/with-timer
                                                 (metrics/service-timer service-id "backend-response")
                                                 (async/<!
                                                   (make-request-fn instance request instance-request-properties
                                                                    passthrough-headers uri metric-group backend-proto proto-version)))
                                response-elapsed (:elapsed timed-response)
                                {:keys [error] :as response} (assoc (:out timed-response)
                                                               :backend-response-latency-ns response-elapsed)]
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
                                (assoc-debug-header "x-waiter-backend-response-ns" (str response-elapsed))))
                          (catch Exception e
                            (async/close! request-state-chan)
                            (handle-process-exception e request)))
                        (update :headers headers/dissoc-hop-by-hop-headers proto-version)
                        (assoc :get-instance-latency-ns instance-elapsed
                               :instance instance
                               :instance-proto backend-proto
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
        (-> {:details response-map, :message "Service has been suspended", :status http-503-service-unavailable}
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
          (-> {:details response-map, :message "Max queue length exceeded", :status http-503-service-unavailable}
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
  [process-request-handler-fn idle-timeout-ms {:keys [descriptor] :as request} health-check-protocol]
  (async/go
    (try
      (let [{:keys [service-description]} descriptor
            {:strs [health-check-url health-check-port-index]} service-description
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
                          (select-keys [:authorization/principal  :authorization/user
                                        :character-encoding :client-protocol :content-type :descriptor :headers
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
          (assoc :body body-str
                 :result (if (= source-ch timeout-ch) :timed-out :received-response))))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn ping-service
  "Performs a health check on an arbitrary instance of the service specified in the descriptor.
   If the service is not running, an instance will be started.
   The response body contains the following map: {:ping-response ..., :service-state ...}"
  [user-agent process-request-handler-fn service-state-fn {:keys [descriptor headers] :as request}]
  (async/go
    (try
      (let [{:keys [core-service-description service-description service-id]} descriptor
            request (assoc-in request [:headers "user-agent"] user-agent)
            idle-timeout-ms (Integer/parseInt (get headers "x-waiter-timeout" "300000"))
            health-check-protocol (scheduler/service-description->health-check-protocol service-description)
            ping-response (async/<! (make-health-check-request process-request-handler-fn idle-timeout-ms request health-check-protocol))
            ping-successful? (= (:result ping-response) :received-response)]
        (let [{:strs [health-check-url]} service-description
              backend-protocol (hu/backend-protocol->http-version health-check-protocol)
              backend-scheme (hu/backend-proto->scheme health-check-protocol)
              request (assoc request
                        :client-protocol backend-protocol
                        :internal-protocol backend-protocol
                        :request-method :get
                        :scheme backend-scheme
                        :uri health-check-url)
              auth-params (auth/select-auth-params request)
              response (-> (merge auth-params ping-response)
                         (assoc :descriptor descriptor
                                :latest-service-id service-id
                                :request-type "ping"
                                :waiter-api-call? false))]
          (rlog/log-request! request response))
        (merge
          (dissoc ping-response [:body :error-chan :headers :request :result :status :trailers])
          (utils/clj->json-response
            {:ping-response (select-keys ping-response [:body :headers :result :status])
             :service-description core-service-description
             :service-state (fa/<? (service-state-fn service-id ping-successful?))})))
      (catch Exception ex
        (utils/exception->response ex request)))))
