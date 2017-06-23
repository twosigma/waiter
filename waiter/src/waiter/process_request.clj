;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.process-request
  (:require [clj-http.client]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [digest]
            [full.async :refer (<?? <? go-try)]
            [metrics.core]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [qbits.jet.client.http :as http]
            [qbits.jet.servlet :as servlet]
            [slingshot.slingshot :refer [try+]]
            [waiter.async-request :as async-req]
            [waiter.async-utils :as au]
            [waiter.correlation-id :as cid]
            [waiter.handler :as handler]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.service-description :as sd]
            [waiter.statsd :as statsd]
            [waiter.token :as token]
            [waiter.utils :as utils])
  (:import java.io.InputStream
           java.io.IOException
           org.eclipse.jetty.io.EofException
           org.eclipse.jetty.server.HttpChannel
           org.eclipse.jetty.server.HttpInput
           org.eclipse.jetty.server.HttpOutput))

(defn check-control [control-chan]
  (let [state (au/poll! control-chan :still-running)]
    (cond
      (= :still-running state) :still-running
      (= (first state) ::servlet/error) (throw (ex-info "Error in server" {:event (second state)}))
      (= (first state) ::servlet/timeout) (throw (ex-info "Connection timed out" {:event (second state)}))
      :else (throw (ex-info "Connection closed while still processing" {})))))



(defn- set-idle-timeout!
  "Configures the idle timeout in the response output stream (HttpOutput) to `idle-timeout-ms` ms."
  [^HttpOutput output-stream idle-timeout-ms]
  (try
    (log/debug "executing pill to adjust idle timeout to" idle-timeout-ms "ms.")
    (let [channel-field (.getDeclaredField HttpOutput "_channel")]
      (.setAccessible channel-field true)
      (let [^HttpChannel http-channel (.get channel-field output-stream)]
        (.setIdleTimeout http-channel idle-timeout-ms)))
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
  (let [service-configured-integer-lookup
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
        (update :async-check-interval-ms service-configured-integer-lookup
                (headers/get-waiter-header waiter-headers "async-check-interval") "async request check interval")
        (update :async-request-timeout-ms service-configured-integer-lookup
                (headers/get-waiter-header waiter-headers "async-request-timeout") "async request timeout")
        (update :initial-socket-timeout-ms service-configured-integer-lookup
                (headers/get-waiter-header waiter-headers "timeout") "socket timeout")
        (update :output-buffer-size service-configured-integer-lookup
                (headers/get-waiter-header waiter-headers "output-buffer-size") "output buffer size")
        (update :queue-timeout-ms service-configured-integer-lookup
                (headers/get-waiter-header waiter-headers "queue-timeout") "instance timeout")
        (update :streaming-timeout-ms service-configured-integer-lookup
                (headers/get-waiter-header waiter-headers "streaming-timeout") "streaming timeout"))))

(defn prepare-instance
  "Tries to acquire an instance and set up a mechanism to release the instance when
   `request-state-chan` is closed. Takes `instance-rpc-chan`, `service-id` and
   `reason-map` to acquire the instance.
   `ex-handler` is called if an exception occurs while trying to acquire the instance.
   If an exception has occurred, no instance was acquired.

   Returns the instance if it was acquired successfully, otherwise,
   will return the output of `ex-handler`"
  [instance-rpc-chan service-id {:keys [request-id] :as reason-map} start-new-service-fn request-state-chan ex-handler
   queue-timeout-ms reservation-status-promise metric-group add-debug-header-into-response]
  (async/go
    (try
      (log/debug "retrieving instance for" service-id "using" (dissoc reason-map :cid :time))
      (let [correlation-id (cid/get-correlation-id)
            instance (<? (service/get-available-instance
                           instance-rpc-chan service-id reason-map start-new-service-fn queue-timeout-ms metric-group add-debug-header-into-response))]
        (au/on-chan-close request-state-chan
                          (fn on-request-state-chan-close []
                            (cid/cdebug correlation-id "request-state-chan closed")
                            ; request potentially completing normally if no previous status delivered
                            (deliver reservation-status-promise :success)
                            (let [status (deref reservation-status-promise 100 :unrealized-promise)] ; don't wait forever
                              (cid/cinfo correlation-id "Done processing request" status)
                              (when (= :success status)
                                (counters/inc! (metrics/service-counter service-id "request-counts" "successful")))
                              (when (= :generic-error status)
                                (cid/cerror correlation-id "There was a generic error in processing the request."
                                            "If this is a client or server related issue, the code needs to be updated."))
                              (when (= :unrealized-promise status)
                                (cid/cerror correlation-id "Reservation status not received!"))
                              (when (not= :success-async status)
                                (counters/dec! (metrics/service-counter service-id "request-counts" "outstanding"))
                                (statsd/gauge-delta! metric-group "request_outstanding" -1))
                              (service/release-instance-go instance-rpc-chan instance {:status status, :cid correlation-id, :request-id request-id})))
                          (fn [e] (log/error e "Error releasing instance!")))
        instance)
      (catch Exception e
        (ex-handler e)))))

(defn request->endpoint
  "Retrieves the relative url Waiter should use to forward requests to service instances."
  [request waiter-headers]
  (let [request-uri (:uri request)
        query-string (:query-string request)]
    (if (= "/secrun" request-uri)
      (headers/get-waiter-header waiter-headers "endpoint-path" "/req")
      (cond-> request-uri
              (not (str/blank? query-string)) (str "?" query-string)))))

(defn- instance->debug-headers
  "Returns instance specific information as a map for use in the response headers."
  [instance prepend-waiter-url]
  (let [backend-id (:id instance)
        backend-host (:host instance)
        backend-port (str (:port instance))
        backend-directory (:log-directory instance)
        backend-headers {"X-Waiter-Backend-Id" backend-id
                         "X-Waiter-Backend-Host" backend-host
                         "X-Waiter-Backend-Port" backend-port}]
    (if backend-directory
      (let [backend-log-url (handler/generate-log-url prepend-waiter-url instance)]
        (assoc backend-headers
          "X-Waiter-Backend-Directory" backend-directory
          "X-Waiter-Backend-Log-Url" backend-log-url))
      backend-headers)))

(defn- wrap-exception
  "Includes metadata such as cid and status along with the exception."
  [exception instance message status]
  (ex-info message
           (merge (metrics/retrieve-local-stats-for-service (scheduler/instance->service-id instance))
                  {:instance instance
                   :cid (cid/get-correlation-id)
                   :status status})
           exception))

(defn http-method-fn
  "Retrieves the clj-http client function that corresponds to the http method."
  [request-method]
  (let [default-http-method http/post]
    (case request-method
      :delete http/delete
      :get http/get
      :head http/head
      :post http/post
      :put http/put
      default-http-method)))

(defn request->http-method-fn
  "Retrieves the clj-http client function that corresponds to the http method used in the request."
  [{:keys [request-method]}]
  (http-method-fn request-method))

(defn make-http-request
  "Makes an asynchronous request to the endpoint using Basic authentication."
  [http-client make-basic-auth-fn request-method endpoint headers body app-password {:keys [username principal]} idle-timeout]
  (let [auth (make-basic-auth-fn endpoint "waiter" app-password)
        headers (cond-> headers
                        username (assoc "x-waiter-auth-principal" username)
                        principal (assoc "x-waiter-authenticated-principal" principal))
        http-method (http-method-fn request-method)]
    (http-method
      http-client endpoint
      {:as :bytes, :auth auth, :body body, :headers headers, :follow-redirects? false, :fold-chunked-response? false, :idle-timeout idle-timeout})))

(defn make-request
  "Makes an asynchronous http request to the instance endpoint and returns a channel."
  [http-client make-basic-auth-fn instance {:keys [request-method] :as request} request-properties passthrough-headers
   end-route app-password metric-group]
  (let [instance-endpoint (scheduler/end-point-url instance end-route)
        service-id (scheduler/instance->service-id instance)
        ; Removing expect may be dangerous http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html, but makes requests 3x faster =}
        ; Also remove hop-by-hop headers https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.5.1
        headers (dissoc passthrough-headers "content-length" "expect" "authorization"
                        ; hop-by-hop headers
                        "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                        "te" "trailers" "transfer-encoding" "upgrade")
        waiter-debug-enabled (boolean (get-in request [:headers "x-waiter-debug"]))]
    (try
      (let [content-length-str (get passthrough-headers "content-length")
            content-length (if content-length-str (Integer/parseInt content-length-str) 0)]
        (when (and (integer? content-length) (pos? content-length))
          ; computing the actual bytes will currently require synchronously reading all data in the request body
          (histograms/update! (metrics/service-histogram service-id "request-size") content-length)
          (statsd/inc! metric-group "request_bytes" content-length)))
      (catch Exception e
        (log/error e "Unable to track content-length on request")))
    (when waiter-debug-enabled
      (log/info "connecting to" instance-endpoint))
    (let [request-body (:body request)
          content-available (if (instance? HttpInput request-body)
                              (pos? (.available ^HttpInput request-body))
                              true)
          body-to-send (when content-available request-body)
          idle-timeout (:initial-socket-timeout-ms request-properties)]
      (make-http-request http-client make-basic-auth-fn request-method instance-endpoint headers body-to-send app-password
                         (handler/make-auth-user-map request) idle-timeout))))

(defn inspect-for-202-async-request-response
  "Helper function that inspects the response and triggers async-request post processing."
  [post-process-async-request-response-fn instance-request-properties service-id metric-group instance endpoint request
   reason-map {:keys [headers status]} response-headers reservation-status-promise]
  (let [location-header (str (get headers "location"))
        location (async-req/normalize-location-header endpoint location-header)]
    (when (= status 202)
      (if (str/starts-with? location "/")
        (do
          (deliver reservation-status-promise :success-async) ;; backend is processing as an asynchronous request
          (post-process-async-request-response-fn service-id metric-group instance (handler/make-auth-user-map request)
                                                  reason-map instance-request-properties location response-headers))
        (log/info "response status 202, not treating as an async request as location is" location)))))

(defn stream-metric-map [service-id]
  {:throughput-meter (metrics/service-meter service-id "stream-throughput")
   :requests-streaming (metrics/service-counter service-id "request-counts" "streaming")
   :requests-waiting-to-stream (metrics/service-counter service-id "request-counts" "waiting-to-stream")
   :percentile-of-buffer-filled (metrics/service-histogram service-id "percent-buffer-filled")
   :stream-request-rate (metrics/service-meter service-id "stream-request-rate")
   :stream-complete-rate (metrics/service-meter service-id "stream-complete-rate")
   :stream-exception-meter (metrics/service-meter service-id "stream-error")
   :stream-back-pressure (metrics/service-meter service-id "stream-backpressure")
   :stream-onto-resp-chan (metrics/service-timer service-id "stream-onto-resp-chan")
   :stream-read-body (metrics/service-timer service-id "stream-read-body")
   :stream (metrics/service-timer service-id "stream")
   :service-id service-id})

(defn buffer-body-chan
  "Makes best effort to buffer all the bytes received in body-chan into chunks of buffer-size.
   When large chunks are received they are forwarded without splitting into smaller buffers."
  [body-chan buffer-size throughput-meter]
  (let [buffer-chan (async/chan 1)
        pack-bytes (fn pack-bytes [buffer-data buffer-used]
                     (when (and buffer-data (pos? buffer-used))
                       (let [result-bytes (byte-array buffer-used)]
                         (System/arraycopy buffer-data 0 result-bytes 0 buffer-used)
                         result-bytes)))]
    (async/go
      (try
        ;; lazily initialize buffer-data to avoid superfluous byte array allocation
        (loop [buffer-data nil
               buffer-used 0]
          (let [body-data (async/<! body-chan)
                bytes-read (if body-data (alength body-data) -1)
                buffer-used' (+ buffer-used (Math/max bytes-read 0))]
            (when (pos? bytes-read)
              (meters/mark! throughput-meter bytes-read))
            (cond
              ;; read all bytes: flush packed buffer and close the buffer
              (= -1 bytes-read)
              (do
                (when-let [result-bytes (pack-bytes buffer-data buffer-used)]
                  (async/put! buffer-chan result-bytes))
                (async/close! buffer-chan))

              ;; body data is empty: do nothing and recur
              (zero? bytes-read)
              (recur buffer-data buffer-used)

              ;; body data is bigger than buffer: flush packed buffer, flush body bytes, reuse buffer array
              (> bytes-read buffer-size)
              (do
                (when-let [result-bytes (pack-bytes buffer-data buffer-used)]
                  (async/put! buffer-chan result-bytes))
                (async/put! buffer-chan body-data)
                (recur buffer-data 0))

              ;; body data does not fill buffer: copy into buffer
              (< buffer-used' buffer-size)
              (let [buffer-data' (or buffer-data (byte-array buffer-size))]
                (System/arraycopy body-data 0 buffer-data' buffer-used bytes-read)
                (recur buffer-data' buffer-used'))

              :else ;; buffer is full: fill buffer and flush, buffer remaining bytes
              (let [buffer-data' (or buffer-data (byte-array buffer-size))
                    buffer-available (- buffer-size buffer-used)
                    buffer-data'' (byte-array buffer-size)
                    buffer-used'' (- buffer-used' buffer-size)]
                (System/arraycopy body-data 0 buffer-data' buffer-used buffer-available)
                (async/put! buffer-chan buffer-data')
                (if (pos? buffer-used'')
                  (do
                    (System/arraycopy body-data buffer-available buffer-data'' 0 buffer-used'')
                    (recur buffer-data'' buffer-used''))
                  (recur nil 0))))))
        (catch Exception e
          (log/error e "error in buffering response body"))
        (finally
          (async/close! buffer-chan))))
    buffer-chan))

(defn stream-http-response
  "Writes byte data to the resp-chan. If the body is a string, just writes the string.
   Otherwise, it is assumed the body is a input stream, in which case, the function
   buffers bytes, and push byte input streams onto the channel until the body input
   stream is exhausted."
  [{:keys [body error-chan]} confirm-live-connection request-abort-callback resp-chan
   {:keys [output-buffer-size streaming-timeout-ms]}
   reservation-status-promise request-state-chan metric-group waiter-debug-enabled
   {:keys [throughput-meter requests-streaming requests-waiting-to-stream
           stream-request-rate stream-complete-rate
           stream-exception-meter stream-back-pressure stream-read-body
           stream-onto-resp-chan stream service-id]}]
  (async/go
    (let [output-stream-atom (atom nil)
          body-chan (if (pos? output-buffer-size)
                      (buffer-body-chan body output-buffer-size throughput-meter)
                      body)]
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
                        (let [buffer (timers/start-stop-time! stream-read-body (async/<! body-chan))
                              bytes-read (if buffer (alength buffer) -1)]
                          (if-not (= -1 bytes-read)
                            (if (timers/start-stop-time!
                                  stream-onto-resp-chan
                                  (au/timed-offer! resp-chan buffer streaming-timeout-ms)) ; don't wait forever to write to server
                              [(+ bytes-streamed bytes-read) true]
                              (let [ex (ex-info "Unable to stream, back pressure in resp-chan. Is connection live?"
                                                {:cid (cid/get-correlation-id), :bytes-streamed bytes-streamed})]
                                (meters/mark! stream-back-pressure)
                                (deliver reservation-status-promise :client-error)
                                (request-abort-callback (IOException. ^Exception ex))
                                (when waiter-debug-enabled
                                  (log/info "unable to stream, back pressure in resp-chan"))
                                (throw ex)))
                            (do
                              (let [{:keys [error]} (async/alt!
                                                      error-chan ([error] error)
                                                      (async/timeout 5000) ([_] {:error (IllegalStateException. "Timeout while waiting on error chan")}))]
                                (when error
                                  (throw error)))
                              (histograms/update! (metrics/service-histogram service-id "response-size") bytes-streamed)
                              (log/info bytes-streamed "bytes streamed in response")
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

(defn- track-response-status-metrics
  [service-id {:strs [metric-group]} status]
  (counters/inc! (metrics/service-counter service-id "response-status" (str status)))
  (statsd/inc! metric-group (str "response_status_" status)))

(defn missing-run-as-user?
  "Returns true if the exception is due to a missing run-as-user validation on the service description."
  [exception]
  (let [{:keys [issue type x-waiter-headers]} (ex-data exception)]
    (and (= :service-description-error type)
         (map? issue)
         (= 1 (count issue))
         (= "missing-required-key" (str (get issue "run-as-user")))
         (-> (keys x-waiter-headers)
             (set)
             (set/intersection sd/on-the-fly-service-description-keys)
             (empty?)))))

(let [process-timer (metrics/waiter-timer "core" "process")]
  (defn process
    "Process the incoming request and stream back the response."
    [router-id make-request-fn instance-rpc-chan request->descriptor-fn start-new-service-fn service-id->password-fn
     instance-request-properties handlers prepend-waiter-url post-process-async-request-response-fn determine-priority-fn
     request]
    (let [reservation-status-promise (promise)
          control-mult (async/mult (:ctrl request))
          confirm-live-chan (async/tap control-mult (au/sliding-buffer-chan 5))
          confirm-live-connection-factory (fn confirm-live-connection-factory [error-callback]
                                            (fn confirm-live-connection []
                                              (try
                                                (check-control confirm-live-chan)
                                                (catch Exception e
                                                  ; flag the error as an I/O error as the connection is no longer live
                                                  (deliver reservation-status-promise :client-error)
                                                  (when error-callback
                                                    (error-callback e))
                                                  (throw e)))))
          confirm-live-connection-without-abort (confirm-live-connection-factory nil)
          waiter-debug-enabled (boolean (get-in request [:headers "x-waiter-debug"]))
          ; update response headers eagerly to enable reporting these in case of failure
          response-headers (atom (cond-> {cid/HEADER-CORRELATION-ID (cid/get-correlation-id)}
                                         waiter-debug-enabled (assoc "X-Waiter-Router-Id" router-id)))
          add-debug-header-into-response (fn [name value]
                                           (when waiter-debug-enabled
                                             (swap! response-headers assoc (str name) (str value))))]
      (async/go
        (if waiter-debug-enabled
          (log/info "process request to" (get-in request [:headers "host"]) "at path" (:uri request))
          (log/debug "process request to" (get-in request [:headers "host"]) "at path" (:uri request)))
        (timers/start-stop-time!
          process-timer
          (try
            (confirm-live-connection-without-abort)
            (let [{:keys [service-id service-description] :as descriptor} (request->descriptor-fn request)
                  {:strs [metric-group]} service-description]
              (loop [[handler & remaining-handlers] handlers]
                (if handler
                  (let [response (handler request descriptor)]
                    (if-not response
                      (recur remaining-handlers)
                      response))
                  (try
                    (let [{:keys [service-id waiter-headers service-description passthrough-headers]} descriptor]
                      (meters/mark! (metrics/service-meter service-id "request-rate"))
                      (counters/inc! (metrics/service-counter service-id "request-counts" "total"))
                      (statsd/inc! metric-group "request")
                      (statsd/unique! metric-group "auth_users" (:authorization/user request))
                      (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding"))
                      (statsd/gauge-delta! metric-group "request_outstanding" +1)
                      (metrics/with-timer!
                        (metrics/service-timer service-id "process")
                        (fn [nanos] (statsd/histo! metric-group "process" nanos))
                        (let [instance-request-properties (prepare-request-properties instance-request-properties waiter-headers)
                              start-new-service-fn (fn start-new-service-in-process [] (start-new-service-fn descriptor))
                              request-id (utils/unique-identifier) ; new unique identifier for this reservation request
                              priority (determine-priority-fn waiter-headers)
                              reason-map (cond-> {:reason :serve-request
                                                  :state {:initial (metrics/retrieve-local-stats-for-service service-id)}
                                                  :time (t/now)
                                                  :cid (cid/get-correlation-id)
                                                  :request-id request-id}
                                                 priority (assoc :priority priority))
                              ; pass false to keep request-state-chan open after control-mult is closed
                              ; request-state-chan should be explicitly closed after the request finishes processing
                              request-state-chan (async/tap control-mult (au/latest-chan) false)
                              unable-to-create-instance (fn unable-to-create-instance [e]
                                                          (counters/dec! (metrics/service-counter service-id "request-counts" "outstanding"))
                                                          (statsd/gauge-delta! metric-group "request_outstanding" -1)
                                                          e)
                              queue-timeout-ms (:queue-timeout-ms instance-request-properties)
                              instance (<? (prepare-instance instance-rpc-chan service-id reason-map
                                                             start-new-service-fn request-state-chan unable-to-create-instance
                                                             queue-timeout-ms reservation-status-promise metric-group add-debug-header-into-response))]
                          (try
                            (log/info "Suggested instance: " (:id instance) (:host instance) (:port instance))
                            (when waiter-debug-enabled
                              (swap! response-headers merge (instance->debug-headers instance prepend-waiter-url)))
                            (confirm-live-connection-without-abort)
                            (let [endpoint (request->endpoint request waiter-headers)
                                  password (service-id->password-fn service-id)
                                  {:keys [status error] :as response} (metrics/with-timer!
                                                                        (metrics/service-timer service-id "backend-response")
                                                                        (fn [nanos]
                                                                          (add-debug-header-into-response "X-Waiter-Backend-Response-ns" nanos)
                                                                          (statsd/histo! metric-group "backend_response" nanos))
                                                                        (async/<!
                                                                          (make-request-fn instance request instance-request-properties
                                                                                           passthrough-headers endpoint password metric-group)))
                                  resp-chan (async/chan 5)
                                  request-abort-callback (fn request-abort-callback [^Exception e]
                                                           (let [ex (IOException. e)
                                                                 aborted (.abort (:request response) ex)]
                                                             (log/info "aborted backend request:" aborted)))
                                  confirm-live-connection-with-abort (confirm-live-connection-factory request-abort-callback)]
                              (when error
                                (if (instance? EofException error)
                                  (do
                                    (deliver reservation-status-promise :client-error)
                                    (throw (wrap-exception error instance "Connection unexpectedly closed while sending request" 400)))
                                  (do
                                    (deliver reservation-status-promise :instance-error)
                                    (throw (wrap-exception error instance "Connection error while sending request to instance. Has it been killed?" 503)))))
                              (when (and (= 503 status) (get service-description "blacklist-on-503"))
                                (log/info "Instance returned 503: " {:instance instance})
                                (deliver reservation-status-promise :instance-busy))
                              (track-response-status-metrics service-id service-description status)
                              (meters/mark! (metrics/service-meter service-id "response-status-rate" (str status)))
                              (counters/inc! (metrics/service-counter service-id "request-counts" "waiting-to-stream"))
                              (confirm-live-connection-with-abort)
                              (inspect-for-202-async-request-response
                                post-process-async-request-response-fn instance-request-properties service-id metric-group
                                instance endpoint request reason-map response response-headers reservation-status-promise)
                              (stream-http-response response confirm-live-connection-with-abort request-abort-callback
                                                    resp-chan instance-request-properties reservation-status-promise
                                                    request-state-chan metric-group waiter-debug-enabled (stream-metric-map service-id))
                              (-> response
                                  (assoc :body resp-chan)
                                  (update-in [:headers] (fn update-response-headers [headers]
                                                          (-> (utils/filterm #(not= "connection" (str/lower-case (str (key %)))) headers)
                                                              (merge @response-headers))))))
                            (catch Exception e
                              ; A :client-error or :instance-error may already be in the channel in which case our :generic-error will be ignored.
                              (deliver reservation-status-promise :generic-error)
                              ; close request-state-chan to mark the request as finished
                              (async/close! request-state-chan)
                              ; Handle error lower down
                              (throw e))))))
                    (catch Exception e
                      (if (missing-run-as-user? e)
                        (do
                          (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "redirect"))
                          (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "redirect")))
                        (do
                          (meters/mark! (metrics/service-meter service-id "process-error"))
                          (statsd/inc! metric-group "process_error")))
                      (track-response-status-metrics service-id service-description (utils/exception->status e))
                      ; Handle error lower down
                      (throw e))))))
            (catch Exception e
              (meters/mark! (metrics/waiter-meter "core" "process-errors"))
              (if (missing-run-as-user? e)
                (let [{:keys [query-string uri]} request
                      location (str "/waiter-consent" uri (when (not (str/blank? query-string)) (str "?" query-string)))
                      exception (ex-info (.getMessage e) (dissoc (ex-data e) :status))]
                  (swap! response-headers #(assoc %1 "Location" location))
                  (utils/exception->response "Missing run-as-user:" exception :headers @response-headers :status 303))
                (utils/exception->response "Error in process, response headers:" e :headers @response-headers)))))))))

(defn handle-suspended-service
  "Check if a service has been suspended and immediately return a 503 response"
  [_ {:keys [suspended-state service-description service-id]}]
  (when (get suspended-state :suspended false)
    (let [{:keys [last-updated-by time]} suspended-state
          response-map (cond-> {:message "Service has been suspended!"
                                :service-id service-id}
                               time (assoc :suspended-at (utils/date-to-str time))
                               (not (str/blank? last-updated-by)) (assoc :last-updated-by last-updated-by))]
      (log/info (:message response-map) (dissoc response-map :message))
      (track-response-status-metrics service-id service-description 503)
      (utils/map->json-response response-map :status 503))))

(defn handle-too-many-requests
  "Check if a service has more pending requests than max-queue-length and immediately return a 503"
  [_ {:keys [service-id service-description]}]
  (let [max-queue-length (get service-description "max-queue-length")
        current-queue-length (counters/value (metrics/service-counter service-id "request-counts" "waiting-for-available-instance"))]
    (when (> current-queue-length max-queue-length)
      (let [outstanding-requests (counters/value (metrics/service-counter service-id "request-counts" "outstanding"))
            response-map {:message "Max queue length exceeded!"
                          :max-queue-length max-queue-length
                          :current-queue-length current-queue-length
                          :outstanding-requests outstanding-requests
                          :service-id service-id}]
        (log/info (:message response-map) (dissoc response-map :message))
        (track-response-status-metrics service-id service-description 503)
        (utils/map->json-response response-map :status 503)))))

(defn request-authorized?
  "Takes the request w/ kerberos auth info & the app headers, and returns true if the user is allowed to use "
  [user permitted-user]
  (log/debug "validating:" (str "permitted=" permitted-user) (str "actual=" user))
  (or (= token/ANY-USER permitted-user)
      (= ":any" (str permitted-user)) ; support ":any" for backwards compatibility
      (and (not (nil? permitted-user)) (= user permitted-user))))

(let [request->descriptor-timer (metrics/waiter-timer "core" "request->descriptor")]
  (defn request->descriptor
    "Extract the service descriptor from a request.
     It also performs the necessary authorization."
    [service-description-defaults service-id-prefix kv-store waiter-hostname can-run-as? metric-group-mappings
     service-description-builder assoc-run-as-user-approved? request]
    (timers/start-stop-time!
      request->descriptor-timer
      (let [auth-user (:authorization/user request)]
        (when-not auth-user
          (throw (ex-info "kerberos auth failed" {})))
        (let [service-approved? (fn service-approved? [service-id] (assoc-run-as-user-approved? request service-id))
              {:keys [service-description service-preauthorized] :as descriptor}
              (sd/request->descriptor service-description-defaults service-id-prefix kv-store waiter-hostname
                                      request metric-group-mappings service-description-builder service-approved?)
              {:strs [run-as-user permitted-user]} service-description]
          (when-not (or service-preauthorized (can-run-as? auth-user run-as-user))
            (throw (ex-info "Authenticated user cannot run service" {:authenticated-user auth-user :run-as-user run-as-user})))
          (when-not (request-authorized? auth-user permitted-user)
            (throw (ex-info "This user isn't allowed to invoke this service" {:user auth-user :service-description service-description})))
          descriptor)))))

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
