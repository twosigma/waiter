;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.websocket
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [qbits.jet.client.websocket :as ws-client]
            [slingshot.slingshot :refer [try+]]
            [waiter.async-utils :as au]
            [waiter.auth.authentication :as auth]
            [waiter.cookie-support :as cookie-support]
            [waiter.correlation-id :as cid]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.statsd :as statsd]
            [waiter.utils :as utils])
  (:import (java.net HttpCookie SocketTimeoutException URLDecoder URLEncoder)
           (java.nio ByteBuffer)
           (org.eclipse.jetty.websocket.api MessageTooLargeException UpgradeRequest)
           (org.eclipse.jetty.websocket.common WebSocketSession)
           (org.eclipse.jetty.websocket.servlet ServletUpgradeResponse)))

;; https://tools.ietf.org/html/rfc6455#section-7.4
(def ^:const server-termination-on-unexpected-condition 1011)

(defn request-authenticator
  "Authenticates the request using the x-waiter-auth cookie.
   If authentication fails, a 403 Unauthorized response is sent and false returned to avoid websocket creation in jet.
   If authentication succeeds, true is returned and the websocket handler is eventually invoked."
  [password ^UpgradeRequest request ^ServletUpgradeResponse response]
  (try
    (let [auth-cookie (some (fn auth-filter [^HttpCookie cookie]
                              (when (= auth/AUTH-COOKIE-NAME (.getName cookie))
                                (.getValue cookie)))
                            (seq (.getCookies request)))
          auth-cookie-valid? (and auth-cookie
                                  (-> auth-cookie
                                      (URLDecoder/decode "UTF-8")
                                      (auth/decode-auth-cookie password)
                                      auth/decoded-auth-valid?))]
      (when-not auth-cookie-valid?
        (log/info "failed to authenticate" {:auth-cookie auth-cookie})
        (.sendForbidden response "Unauthorized"))
      auth-cookie-valid?)
    (catch Throwable e
      (log/error e "error while authenticating websocket request")
      (.sendError response 500 (.getMessage e))
      false)))

(defn inter-router-request-middleware
  "Attaches a dummy x-waiter-auth cookie into the request to enable mimic-ing auth in inter-router websocket requests."
  [router-id password ^UpgradeRequest request]
  (let [cookie-value [(str router-id "@waiter-peer-router") (System/currentTimeMillis)]
        auth-cookie-value (-> (cookie-support/encode-cookie cookie-value password)
                              (URLEncoder/encode "UTF-8"))]
    (log/info "attaching" auth-cookie-value "to websocket request")
    (-> request
        (.getCookies)
        (.add (HttpCookie. auth/AUTH-COOKIE-NAME auth-cookie-value)))))

(defn request-handler
  "Handler for websocket requests.
   It populates the kerberos credentials and invokes process-request-fn."
  [password process-request-fn {:keys [headers] :as request}]
  (let [auth-cookie (-> headers (get "cookie") str auth/get-auth-cookie-value) ;; auth-cookie is assumed to be valid
        [auth-principal auth-time] (auth/decode-auth-cookie auth-cookie password)]
    (log/info "processing websocket request" {:user auth-principal})
    (-> (auth/assoc-auth-in-request request auth-principal)
        (assoc :authorization/time auth-time)
        process-request-fn)))

(defn abort-request-callback-factory
  "Creates a callback to abort the http request."
  [response]
  (fn abort-websocket-request-callback [^Exception e]
    (log/error e "aborting backend websocket request")
    (let [backend-in (-> response :request :in)
          backend-out (-> response :request :out)]
      (async/close! backend-in)
      (async/close! backend-out))))

(defn add-headers-to-upgrade-request!
  "Sets the headers an on UpgradeRequest."
  [^UpgradeRequest upgrade-request headers]
  (doseq [[key value] headers]
    (let [header-name (if (keyword? key) (name key) (str key))]
      (.setHeader upgrade-request header-name (str value)))))

(defn- dissoc-forbidden-headers
  "Remove websocket forbidden headers based on
   http://archive.eclipse.org/jetty/9.1.0.M0/xref/org/eclipse/jetty/websocket/client/ClientUpgradeRequest.html#52"
  [headers]
  (dissoc headers "cache-control" "cookie" "connection" "host" "pragma" "sec-websocket-accept" "sec-websocket-extensions"
          "sec-websocket-key" "sec-websocket-protocol" "sec-websocket-version" "upgrade"))

(defn make-request
  "Makes an asynchronous websocket request to the instance endpoint and returns a channel."
  [websocket-client service-id->password-fn instance ws-request request-properties passthrough-headers end-route _]
  (let [ws-middleware (fn ws-middleware [_ ^UpgradeRequest request]
                        (let [service-password (-> instance scheduler/instance->service-id service-id->password-fn)
                              headers
                              (-> (dissoc passthrough-headers "content-length" "expect" "authorization")
                                  (headers/dissoc-hop-by-hop-headers)
                                  (dissoc-forbidden-headers)
                                  (assoc "Authorization" (str "Basic " (String. ^bytes (b64/encode (.getBytes (str "waiter:" service-password) "utf-8")) "utf-8")))
                                  (headers/assoc-auth-headers (:authorization/user ws-request) (:authenticated-principal ws-request)))]
                          (add-headers-to-upgrade-request! request headers)))
        response (async/promise-chan)
        ctrl-chan (async/chan)
        control-mult (async/mult ctrl-chan)
        ws-request-properties {:async-write-timeout (:async-request-timeout-ms request-properties)
                               :connect-timeout (:connection-timeout-ms request-properties)
                               :ctrl (fn ctrl-factory [] ctrl-chan)
                               :max-idle-timeout (:initial-socket-timeout-ms request-properties)
                               :middleware ws-middleware}
        ws-protocol (if (= "https" (:protocol instance)) "wss" "ws")
        instance-endpoint (scheduler/end-point-url (assoc instance :protocol ws-protocol) end-route)
        service-id (scheduler/instance->service-id instance)
        correlation-id (cid/get-correlation-id)]
    (try
      (log/info "forwarding request for service" service-id "to" instance-endpoint)
      (let [ctrl-copy-chan (async/tap control-mult (async/chan (async/dropping-buffer 1)))]
        (async/go
          (let [[close-code error] (async/<! ctrl-copy-chan)]
            (when (= :qbits.jet.websocket/error close-code)
              ;; the put! is a no-op if the connection was successful
              (log/info "propagating error to response in case websocket connection failed")
              (async/put! response {:error error})))))
      (ws-client/connect! websocket-client instance-endpoint
                          (fn [request]
                            (cid/with-correlation-id
                              correlation-id
                              (log/info "successfully connected with backend")
                              (async/put! response {:ctrl-mult control-mult, :request request})))
                          ws-request-properties)
      (let [{:keys [requests-waiting-to-stream]} (metrics/stream-metric-map service-id)]
        (counters/inc! requests-waiting-to-stream))
      (catch Exception exception
        (log/error exception "error while making websocket connection to backend instance")
        (async/put! response {:error exception})))
    response))

(defn- close-requests!
  "Closes all channels associated with a websocket request.
   This includes:
   1. in and out channels from the client,
   2. in and out channels to the backend,
   3. the request-state-chan opened to track the state of the request internally."
  [request response request-state-chan]
  (let [client-out (:out request)
        backend-out (-> response :request :out)]
    (log/info "closing websocket channels")
    (async/close! client-out)
    (async/close! backend-out)
    (async/close! request-state-chan)))

(defn- process-incoming-data
  "Processes the incoming data and return the tuple [bytes-read data-to-send].
   If the incoming data is a ByteBuffer, it is consumed and copied into a newly created byte array (the data-to-send).
   The bytes-read is the size of the byte array in this case.
   In all other scenarios, in-data is converted to a String as data-to-send and the the utf-8 encoding is used for bytes read."
  [in-data]
  (if (instance? ByteBuffer in-data)
    (let [bytes-read (.remaining in-data)
          data-to-send (byte-array bytes-read)]
      (.get in-data data-to-send)
      [bytes-read data-to-send])
    (let [bytes-read (-> in-data (.getBytes "utf-8") count)]
      [bytes-read in-data])))

(defn- stream-helper
  "Helper function to stream data between two channels with support for timeout that recognizes backpressure."
  [src-name src-chan dest-name dest-chan streaming-timeout-ms reservation-status-promise stream-error-type
   request-close-chan stream-onto-upload-chan-timer stream-back-pressure-meter notify-bytes-read-fn]
  (let [upload-chan (async/chan 5)] ;; use same magic 5 as resp-chan in stream-http-response
    (async/pipe upload-chan dest-chan)
    (async/go
      (try
        (loop [bytes-streamed 0]
          (if-let [in-data (async/<! src-chan)]
            (let [[bytes-read send-data] (process-incoming-data in-data)]
              (log/info "received" bytes-read "bytes from" src-name)
              (notify-bytes-read-fn bytes-read)
              (if (timers/start-stop-time!
                    stream-onto-upload-chan-timer
                    (au/timed-offer! upload-chan send-data streaming-timeout-ms))
                (recur (+ bytes-streamed bytes-read))
                (do
                  (log/error "unable to stream to " dest-name {:cid (cid/get-correlation-id), :bytes-streamed bytes-streamed})
                  (meters/mark! stream-back-pressure-meter)
                  (deliver reservation-status-promise stream-error-type)
                  (async/>! request-close-chan stream-error-type))))
            (do
              (log/info src-name "input channel has been closed, bytes streamed:" bytes-streamed))))
        (catch Exception e
          (log/error e "error in streaming data from" src-name "to" dest-name)
          (deliver reservation-status-promise :generic-error)
          (async/>! request-close-chan :generic-error))))))

(defn- stream-response
  "Writes byte data to the resp-chan.
   It is assumed the body is an input stream.
   The function buffers bytes, and pushes byte input streams onto the channel until the body input stream is exhausted."
  [request response descriptor {:keys [streaming-timeout-ms]} reservation-status-promise request-close-chan
   {:keys [requests-streaming requests-waiting-to-stream stream-back-pressure stream-read-body stream-onto-resp-chan throughput-meter]}]
  (let [{:keys [service-description service-id]} descriptor
        {:strs [metric-group]} service-description]
    (counters/dec! requests-waiting-to-stream)
    (counters/inc! requests-streaming)
    ;; launch go-block to stream data from client to instance
    (let [client-in (:in request)
          instance-out (-> response :request :out)]
      (stream-helper "client" client-in "instance" instance-out streaming-timeout-ms reservation-status-promise
                     :instance-error request-close-chan stream-read-body stream-back-pressure
                     (fn ws-bytes-uploaded [bytes-streamed]
                       (histograms/update! (metrics/service-histogram service-id "request-size") bytes-streamed)
                       (statsd/inc! metric-group "request_bytes" bytes-streamed))))
    ;; launch go-block to stream data from instance to client
    (let [client-out (:out request)
          instance-in (-> response :request :in)]
      (stream-helper "instance" instance-in "client" client-out streaming-timeout-ms reservation-status-promise
                     :client-error request-close-chan stream-onto-resp-chan stream-back-pressure
                     (fn ws-bytes-downloaded [bytes-streamed]
                       (meters/mark! throughput-meter bytes-streamed)
                       (histograms/update! (metrics/service-histogram service-id "response-size") bytes-streamed)
                       (statsd/inc! metric-group "response_bytes" bytes-streamed))))))

(defn watch-ctrl-chan
  "Inspects the return value by tapping on the control-mult and triggers closing of the websocket request."
  [source control-mult reservation-status-promise request-close-promise-chan on-close-callback]
  (let [tapped-ctrl-chan (async/tap control-mult (async/chan (async/dropping-buffer 1)))]
    ;; go-block to trigger close when control-mult has been notified of an event
    (async/go
      (let [[ctrl-code return-code-or-exception close-message] (async/<! tapped-ctrl-chan)]
        (log/info "received on" (name source) "ctrl chan:" ctrl-code
                  (when (integer? return-code-or-exception)
                    ;; Close status codes https://tools.ietf.org/html/rfc6455#section-7.4
                    (case (int return-code-or-exception)
                      1000 "closed normally"
                      1002 "protocol error"
                      1003 "unsupported input data"
                      1006 "closed abnormally"
                      (str "status code " return-code-or-exception))))
        (when on-close-callback
          (if (integer? return-code-or-exception)
            (on-close-callback return-code-or-exception)
            (on-close-callback server-termination-on-unexpected-condition)))
        (let [close-code (condp = ctrl-code
                           nil :connection-closed
                           :qbits.jet.websocket/close :success
                           :qbits.jet.websocket/error
                           (let [error-code (cond
                                              (instance? MessageTooLargeException return-code-or-exception) :generic-error
                                              (instance? SocketTimeoutException return-code-or-exception) :socket-timeout
                                              :else (keyword (str (name source) "-error")))]
                             (deliver reservation-status-promise error-code)
                             (log/error return-code-or-exception "error from" (name source) "websocket request")
                             error-code)
                           :unknown)]
          (async/>! request-close-promise-chan [source close-code return-code-or-exception close-message]))))))

(defn- close-client-session!
  "Explicitly closes the client connection using the provided status and message."
  [request status-code close-message]
  (try
    (let [^WebSocketSession client-session (-> request :ws (.session))]
      (when (some-> client-session .isOpen)
        (when (some-> client-session .isOpen)
          (log/info "closing client session with code" status-code close-message)
          (.close client-session status-code close-message))))
    (catch Exception e
      (log/error e "error in explicitly closing client websocket using" status-code close-message))))

(defn process-response!
  "Processes a response resulting from a websocket request.
   It includes asynchronously streaming the content."
  [instance-request-properties descriptor _ request _ _ reservation-status-promise confirm-live-connection-with-abort
   request-state-chan response]
  (let [{:keys [service-description service-id]} descriptor
        {:strs [metric-group]} service-description
        request-close-promise-chan (async/promise-chan)
        {:keys [requests-streaming stream stream-complete-rate stream-request-rate] :as metrics-map}
        (metrics/stream-metric-map service-id)]

    ;; go-block that handles cleanup by closing all channels related to the websocket request
    (async/go
      ;; approximate streaming rate by when the connection is closed
      (metrics/with-meter
        stream-request-rate
        stream-complete-rate
        (timers/start-stop-time!
          stream
          (when-let [close-message (async/<! request-close-promise-chan)]
            (let [[source close-code status-code-or-exception close-message] close-message]
              (log/info "websocket connections requested to be closed due to" source close-code close-message)
              (counters/dec! requests-streaming)
              ;; explicitly close the client connection if backend triggered the close
              (when (= :instance source)
                (let [correlation-id (cid/get-correlation-id)]
                  (async/>!
                    (-> request :out)
                    (fn close-session [_]
                      (cid/with-correlation-id
                        correlation-id
                        (if (integer? status-code-or-exception)
                          (close-client-session! request status-code-or-exception close-message)
                          (let [ex-message (.getMessage status-code-or-exception)]
                            (close-client-session! request server-termination-on-unexpected-condition ex-message))))))))
              ;; close client and backend channels
              (close-requests! request response request-state-chan))))))

    ;; watch for ctrl-chan events
    (watch-ctrl-chan :client (-> request :ctrl-mult) reservation-status-promise request-close-promise-chan nil)
    (let [on-close-callback (fn instance-on-close-callback [status]
                              (counters/inc! (metrics/service-counter service-id "response-status" (str status)))
                              (statsd/inc! metric-group (str "response_status_" status)))]
      (watch-ctrl-chan :instance (-> response :ctrl-mult) reservation-status-promise request-close-promise-chan on-close-callback))

    (try
      ;; stream data between client and instance
      (stream-response request response descriptor instance-request-properties reservation-status-promise
                       request-close-promise-chan metrics-map)

      ;; force close connection when cookie expires
      (let [auth-time (:authorization/time request)
            one-day-in-millis (-> 1 t/days t/in-millis)
            expiry-time-ms (+ auth-time one-day-in-millis)
            time-left-ms (max (- expiry-time-ms (System/currentTimeMillis)) 0)]
        (async/go
          (let [timeout-ch (async/timeout time-left-ms)
                [_ selected-chan] (async/alts! [request-close-promise-chan timeout-ch] :priority true)]
            (if (= timeout-ch selected-chan)
              (try
                ;; close connections if the request is still live
                (confirm-live-connection-with-abort)
                (log/info "cookie has expired, triggering closing of websocket connections")
                (async/>! request-close-promise-chan :cookie-expired)
                (catch Exception _
                  (log/debug "ignoring exception generated from closed connection")))))))
      (catch Exception e
        (async/>!! request-close-promise-chan :process-error)
        (throw e)))))

(defn process-exception-in-request
  "Processes exceptions thrown while processing a websocket request."
  [track-process-error-metrics-fn {:keys [out] :as request} descriptor exception]
  (log/error exception "error in processing websocket request")
  (track-process-error-metrics-fn descriptor)
  (async/go
    (async/>! out (utils/exception->response request exception))
    (async/close! out)))
