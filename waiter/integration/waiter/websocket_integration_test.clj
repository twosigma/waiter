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
(ns waiter.websocket-integration-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [qbits.jet.client.websocket :as ws-client]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]
            [waiter.websocket :as websocket])
  (:import (java.net HttpCookie)
           (java.nio ByteBuffer)
           (org.eclipse.jetty.websocket.api UpgradeException UpgradeRequest)
           (org.eclipse.jetty.websocket.client WebSocketClient)
           (qbits.jet.client.websocket Connection)
           (qbits.jet.websocket WebSocket)))

(def ^:const default-timeout-period (-> 4 t/minutes t/in-millis))

(defn- ws-url [waiter-url endpoint]
  (str "ws://" waiter-url endpoint))

(defn- add-auth-cookie [request auth-cookie-value]
  (-> request (.getCookies) (.add (HttpCookie. "x-waiter-auth" auth-cookie-value))))

(let [websocket-client (WebSocketClient.)]
  (defn- websocket-client-factory [] websocket-client))

(defn- update-max-message-sizes
  [^WebSocketClient websocket-client ws-max-binary-message-size ws-max-text-message-size]
  (doto (.getPolicy websocket-client)
    (.setMaxBinaryMessageSize ws-max-binary-message-size)
    (.setMaxTextMessageSize ws-max-text-message-size)))

(defn connection->ctrl-data
  "Retrieves the data on the ctrl channel."
  [^Connection connection]
  (when-let [^WebSocket ws (some-> connection :socket)]
    (when-let [ctrl-chan (.-ctrl ws)]
      (async/<!! ctrl-chan))))

(deftest ^:parallel ^:integration-fast test-request-auth-failure
  (testing-using-waiter-url
    (let [connect-success-promise (promise)
          connection (ws-client/connect!
                       (websocket-client-factory)
                       (ws-url waiter-url "/websocket-unauth")
                       (fn [{:keys [out]}]
                         (deliver connect-success-promise :success)
                         (async/close! out)))
          [close-code error] (connection->ctrl-data connection)]
      (is (= :qbits.jet.websocket/error close-code))
      (is (instance? UpgradeException error))
      (is (str/includes? (.getMessage error) "Unexpected HTTP Response Status Code: 403 Forbidden"))
      (is (not (realized? connect-success-promise))))))

(deftest ^:parallel ^:integration-fast test-request-auth-success
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          ws-response-atom (atom [])
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "waiter_ws_test"
                           "x-waiter-name" (rand-name))]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)
              connection (ws-client/connect!
                           (websocket-client-factory)
                           (ws-url waiter-url "/websocket-auth")
                           (fn [{:keys [in out]}]
                             (async/go
                               (async/>! out "request-info")
                               (swap! ws-response-atom conj (async/<! in))
                               (swap! ws-response-atom conj (async/<! in))
                               (deliver response-promise :done)
                               (async/close! out)))
                           {:middleware (fn [_ ^UpgradeRequest request]
                                          (websocket/add-headers-to-upgrade-request! request waiter-headers)
                                          (add-auth-cookie request auth-cookie-value))})
              [close-code error] (connection->ctrl-data connection)]
          (is (= :qbits.jet.websocket/close close-code))
          (is (= websocket-1000-normal error))
          (is (= :done (deref response-promise default-timeout-period :timed-out))))
        (log/info "websocket responses:" @ws-response-atom)
        (is (= "Connected to kitchen" (first @ws-response-atom)) (str @ws-response-atom))
        (let [{:keys [headers]} (-> @ws-response-atom second str json/read-str walk/keywordize-keys)
              {:keys [upgrade x-cid x-waiter-auth-principal]} headers]
          (is x-cid)
          (is (= upgrade "websocket"))
          (is (= x-waiter-auth-principal (retrieve-username))))
        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-fast test-request-auth-disabled
  (testing-using-waiter-url
    (let [ws-response-atom (atom [])
          token (str "token-" (rand-name))
          token-description (assoc (kitchen-request-headers :prefix "")
                              :authentication "disabled"
                              :metric-group "waiter_ws_test"
                              :name (rand-name)
                              :permitted-user "*"
                              :run-as-user (retrieve-username)
                              :token token)
          waiter-headers {"x-waiter-token" token}]
      (try
        (let [token-response (post-token waiter-url token-description)]
          (assert-response-status token-response http-200-ok)
          (try
            (let [response-promise (promise)
                  connection (ws-client/connect!
                               (websocket-client-factory)
                               (ws-url waiter-url "/websocket-auth")
                               (fn [{:keys [in out]}]
                                 (async/go
                                   (async/>! out "request-info")
                                   (swap! ws-response-atom conj (async/<! in))
                                   (swap! ws-response-atom conj (async/<! in))
                                   (deliver response-promise :done)
                                   (async/close! out)))
                               {:middleware (fn [_ ^UpgradeRequest request]
                                              (websocket/add-headers-to-upgrade-request! request waiter-headers))})
                  [close-code error] (connection->ctrl-data connection)]
              (is (= :qbits.jet.websocket/close close-code))
              (is (= websocket-1000-normal error))
              (is (= :done (deref response-promise default-timeout-period :timed-out))))
            (log/info "websocket responses:" @ws-response-atom)
            (is (= "Connected to kitchen" (first @ws-response-atom)) (str @ws-response-atom))
            (let [{:keys [headers]} (-> @ws-response-atom second str json/read-str walk/keywordize-keys)
                  {:keys [upgrade x-cid x-waiter-auth-principal]} headers]
              (is x-cid)
              (is (= upgrade "websocket"))
              (is (nil? x-waiter-auth-principal)))
            (finally
              (delete-service waiter-url waiter-headers))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-request-authentication-and-on-the-fly-headers
  (testing-using-waiter-url
    (let [token (str "token-" (rand-name))
          token-description (assoc (kitchen-request-headers :prefix "")
                              :authentication "disabled"
                              :metric-group "waiter_ws_test"
                              :name (rand-name)
                              :permitted-user "*"
                              :run-as-user (retrieve-username)
                              :token token)]
      (try
        (let [token-response (post-token waiter-url token-description)]
          (assert-response-status token-response http-200-ok)

          (let [connect-success-promise (promise)
                waiter-headers {"x-waiter-concurrency-level" 300
                                "x-waiter-token" token}
                connection (ws-client/connect!
                             (websocket-client-factory)
                             (ws-url waiter-url "/websocket-unauth")
                             (fn [{:keys [out]}]
                               (deliver connect-success-promise :success)
                               (async/close! out))
                             {:middleware (fn [_ ^UpgradeRequest request]
                                            (websocket/add-headers-to-upgrade-request! request waiter-headers))})
                [close-code error] (connection->ctrl-data connection)]
            (is (= :qbits.jet.websocket/error close-code))
            (is (instance? UpgradeException error))
            (is (str/includes? (.getMessage error) "Unexpected HTTP Response Status Code: 400 Bad Request"))
            (is (not (realized? connect-success-promise))))

          (let [connect-success-promise (promise)
                waiter-headers {"x-waiter-authentication" "standard"
                                "x-waiter-token" token}
                connection (ws-client/connect!
                             (websocket-client-factory)
                             (ws-url waiter-url "/websocket-unauth")
                             (fn [{:keys [out]}]
                               (deliver connect-success-promise :success)
                               (async/close! out))
                             {:middleware (fn [_ ^UpgradeRequest request]
                                            (websocket/add-headers-to-upgrade-request! request waiter-headers))})
                [close-code error] (connection->ctrl-data connection)]
            (is (= :qbits.jet.websocket/error close-code))
            (is (instance? UpgradeException error))
            (is (str/includes? (.getMessage error) "Unexpected HTTP Response Status Code: 400 Bad Request"))
            (is (not (realized? connect-success-promise)))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-request-auth-success-single-subprotocol
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          ws-response-atom (atom [])
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "waiter_ws_test"
                           "x-waiter-name" (rand-name))]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)
              connection (ws-client/connect!
                           (websocket-client-factory)
                           (ws-url waiter-url "/websocket-auth")
                           (fn [{:keys [in out]}]
                             (async/go
                               (async/>! out "request-info")
                               (swap! ws-response-atom conj (async/<! in))
                               (swap! ws-response-atom conj (async/<! in))
                               (deliver response-promise :done)
                               (async/close! out)))
                           {:middleware (fn [_ ^UpgradeRequest request]
                                          (websocket/add-headers-to-upgrade-request! request waiter-headers)
                                          (add-auth-cookie request auth-cookie-value))
                            :subprotocols ["Chat-1.0"]})
              [close-code error] (connection->ctrl-data connection)]
          (is (= :qbits.jet.websocket/close close-code))
          (is (= websocket-1000-normal error))
          (is (= :done (deref response-promise default-timeout-period :timed-out))))
        (log/info "websocket responses:" @ws-response-atom)
        (is (= "Connected to kitchen" (first @ws-response-atom)) (str @ws-response-atom))
        (let [{:keys [headers]} (-> @ws-response-atom second str json/read-str walk/keywordize-keys)
              {:keys [sec-websocket-protocol upgrade x-cid x-waiter-auth-principal]} headers]
          (is x-cid)
          (is (= upgrade "websocket"))
          (is (= x-waiter-auth-principal (retrieve-username)))
          (is (= "Chat-1.0" sec-websocket-protocol)))
        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-fast test-request-auth-success-multiple-subprotocols
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          ws-response-atom (atom [])
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "waiter_ws_test"
                           "x-waiter-name" (rand-name))]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)
              ctrl (async/chan)]
          (ws-client/connect!
            (websocket-client-factory)
            (ws-url waiter-url "/websocket-auth")
            (fn [{:keys [out]}]
              (async/go
                (deliver response-promise :unexpected)
                (async/close! out)))
            {:ctrl (constantly ctrl)
             :middleware (fn [_ ^UpgradeRequest request]
                           (websocket/add-headers-to-upgrade-request! request waiter-headers)
                           (add-auth-cookie request auth-cookie-value))
             :subprotocols ["Chat-1.0" "Chat-2.0"]})
          (let [[ctrl-response _] (async/<!! ctrl)]
            (deliver response-promise
                     (if (= :qbits.jet.websocket/error ctrl-response) :failed :success)))
          (is (= :failed (deref response-promise default-timeout-period :timed-out))))
        (log/info "websocket responses:" @ws-response-atom)
        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-fast test-last-request-time
  (testing-using-waiter-url
    (let [waiter-settings (waiter-settings waiter-url)
          metrics-sync-interval-ms (get-in waiter-settings [:metrics-config :metrics-sync-interval-ms])
          inter-request-interval-ms (+ metrics-sync-interval-ms 1000)
          auth-cookie-value (auth-cookie waiter-url)
          waiter-headers (assoc (kitchen-request-headers)
                           :x-waiter-metric-group "waiter_ws_test"
                           :x-waiter-name (rand-name))
          _ (make-kitchen-request waiter-url waiter-headers :method :get)
          {:keys [headers service-id] :as canary-response}
          (make-request-with-debug-info waiter-headers #(make-kitchen-request waiter-url % :method :get))
          _ (assert-response-status canary-response http-200-ok)
          first-request-time-header (-> (get headers "x-waiter-request-date")
                                      (du/str-to-date du/formatter-rfc822))
          num-iterations 5]
      (is (pos? metrics-sync-interval-ms))
      (with-service-cleanup
        service-id
        (is auth-cookie-value)
        (is (pos? (.getMillis first-request-time-header)))
        (let [response-promise (promise)
              connect-start-time-ms (System/currentTimeMillis)
              connect-end-time-ms-atom (atom connect-start-time-ms)
              connection (ws-client/connect!
                           (websocket-client-factory)
                           (ws-url waiter-url "/websocket-auth")
                           (fn [{:keys [in out]}]
                             (async/go
                               (log/info "websocket request connected")
                               (async/<! in)
                               (reset! connect-end-time-ms-atom (System/currentTimeMillis))
                               (dotimes [n num-iterations]
                                 (async/<! (async/timeout inter-request-interval-ms))
                                 (async/>! out (str "hello-" n))
                                 (async/<! in))
                               (log/info "closing channels")
                               (async/close! out)
                               (deliver response-promise :done)))
                           {:middleware (fn [_ ^UpgradeRequest request]
                                          (websocket/add-headers-to-upgrade-request! request waiter-headers)
                                          (add-auth-cookie request auth-cookie-value))})
              [close-code error] (connection->ctrl-data connection)]
          (is (= :qbits.jet.websocket/close close-code))
          (is (= websocket-1000-normal error))
          (is (= :done (deref response-promise (* 2 num-iterations inter-request-interval-ms) :timed-out)))
          (Thread/sleep (* 3 metrics-sync-interval-ms))
          (let [connection-duration-ms (- @connect-end-time-ms-atom connect-start-time-ms)
                websocket-duration-ms (* num-iterations inter-request-interval-ms)
                minimum-last-request-time-duration-ms (+ connection-duration-ms websocket-duration-ms)
                minimum-last-request-time (t/plus first-request-time-header (t/millis minimum-last-request-time-duration-ms))
                service-last-request-time (service-id->last-request-time waiter-url service-id)]
            (is (pos? (.getMillis service-last-request-time)))
            (is (or (t/before? minimum-last-request-time service-last-request-time)
                    (t/equal? minimum-last-request-time service-last-request-time))
                (str [minimum-last-request-time service-last-request-time]))))))))

(deftest ^:parallel ^:integration-fast ^:explicit test-request-socket-timeout
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          send-success-after-timeout-atom (atom true)
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "waiter_ws_test"
                           "x-waiter-name" (rand-name))]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)
              connection (ws-client/connect!
                           (websocket-client-factory)
                           (ws-url waiter-url "/websocket-timeout")
                           (fn [{:keys [in out]}]
                             (async/go
                               (async/>! out "hello")
                               (async/<! in) ;; kitchen message
                               (async/<! in) ;; hello response
                               (Thread/sleep 5000)
                               (reset! send-success-after-timeout-atom (async/>! out "should-be-closed"))
                               (deliver response-promise :done)
                               (async/close! out)))
                           {:middleware (fn [_ ^UpgradeRequest request]
                                          (let [headers (assoc waiter-headers
                                                          "x-waiter-async-request-timeout" "1000"
                                                          "x-waiter-timeout" "1000")]
                                            (websocket/add-headers-to-upgrade-request! request headers))
                                          (add-auth-cookie request auth-cookie-value))})
              [close-code error] (connection->ctrl-data connection)]
          (is (= :qbits.jet.websocket/close close-code))
          (is (= websocket-1011-server-error error))
          (is (= :done (deref response-promise default-timeout-period :timed-out))))
        (is (not @send-success-after-timeout-atom))
        (finally
          (delete-service waiter-url waiter-headers))))))

; FAIL in (test-request-instance-death) (websocket_integration_test.clj:296)
; test-request-instance-death
; expected: [:qbits.jet.websocket/close 1006 "Disconnected"]
;   actual: [:qbits.jet.websocket/error #error {
;  :cause "Idle timeout expired: 120000/120000 ms"
(deftest ^:parallel ^:integration-slow ^:explicit test-request-instance-death
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          send-success-after-timeout-atom (atom true)
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "waiter_ws_test"
                           "x-waiter-name" (rand-name)
                           "x-waiter-max-instances" "1"
                           "x-waiter-min-instances" "1"
                           "x-waiter-concurrency-level" "20")]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)
              ctrl-promise (promise)
              client http1-client
              websocket-client (websocket-client-factory)]
          (doto (.getPolicy websocket-client)
            (.setAsyncWriteTimeout (-> 1 t/minutes t/in-millis))
            (.setIdleTimeout (-> 2 t/minutes t/in-millis)))
          (ws-client/connect!
            websocket-client
            (ws-url waiter-url "/websocket-timeout")
            (fn [{:keys [ctrl in out]}]
              (async/go
                (deliver ctrl-promise (async/<! ctrl)))
              (async/go
                (async/>! out "hello")
                (async/<! in) ;; kitchen message
                (async/<! in) ;; hello response
                ;; cause the backend to die
                (make-request waiter-url "/die" :headers waiter-headers :verbose true :client client)
                (Thread/sleep 5000)
                ;; expect no response back, and that the input channel will be closed
                (async/>! out "data-with-no-response")
                (reset! send-success-after-timeout-atom (async/<! in))
                (deliver response-promise :done)))
            {:middleware (fn [_ ^UpgradeRequest request]
                           (let [headers (assoc waiter-headers
                                           "x-waiter-async-request-timeout" "20000"
                                           "x-waiter-timeout" "20000")]
                             (websocket/add-headers-to-upgrade-request! request headers))
                           (add-auth-cookie request auth-cookie-value))})
          (is (= [:qbits.jet.websocket/close websocket-1006-abnormal "Disconnected"]
                 (deref ctrl-promise default-timeout-period :timed-out)))
          (is (= :done (deref response-promise default-timeout-period :timed-out))))
        (is (not @send-success-after-timeout-atom))
        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-slow test-message-size-received-from-backend-exceeds-supported-max
  (testing-using-waiter-url
    (let [^WebSocketClient websocket-client (websocket-client-factory)
          waiter-settings (waiter-settings waiter-url)
          {:keys [ws-max-binary-message-size ws-max-text-message-size]} (:websocket-config waiter-settings)
          ws-max-binary-message-size' (+ 2048 ws-max-binary-message-size) ;; 2K larger than what Waiter supports
          ws-max-text-message-size' (+ 2048 ws-max-text-message-size)
          auth-cookie-value (auth-cookie waiter-url)
          process-mem 1024
          waiter-headers (-> (kitchen-request-headers)
                           (assoc :x-waiter-mem process-mem
                                  :x-waiter-metric-group "waiter_ws_test"
                                  :x-waiter-name (rand-name))
                           (update :x-waiter-cmd
                                   (fn [cmd] (str cmd ;; on-the-fly doesn't support x-waiter-env
                                                  " --ws-max-binary-message-size " ws-max-binary-message-size'
                                                  " --ws-max-text-message-size " ws-max-text-message-size'))))
          middleware (fn middleware [_ ^UpgradeRequest request]
                       (websocket/add-headers-to-upgrade-request! request waiter-headers)
                       (add-auth-cookie request auth-cookie-value))]
      (update-max-message-sizes websocket-client ws-max-binary-message-size' ws-max-text-message-size')
      (is auth-cookie-value)
      (try

        ;; avoiding testing chars as it causes kitchen to run out of memory
        (testing "large binary response"
          (let [response-promise (promise)
                backend-data-promise (promise)
                ctrl-data-promise (promise)]
            (ws-client/connect!
              websocket-client
              (ws-url waiter-url "/websocket-byte-stream")
              (fn [{:keys [in out ctrl]}]
                (async/go
                  (async/>! out "first-message")
                  (async/<! in) ;; kitchen message
                  (async/<! in) ;; hello response
                  (async/>! out (str "bytes-" (+ ws-max-text-message-size 1024))) ;; 1K larger than what Waiter supports
                  (let [backend-response (async/<! in)]
                    (deliver backend-data-promise backend-response))
                  (async/close! out)
                  (let [ctrl-data (async/<! ctrl)]
                    (deliver ctrl-data-promise ctrl-data))
                  (deliver response-promise :done)))
              {:middleware middleware})

            (is (= :done (deref response-promise default-timeout-period :timed-out)))
            (is (nil? (deref backend-data-promise 100 :timed-out)))
            (let [[message-key close-code close-message] (deref ctrl-data-promise 100 [:timed-out])]
              (is (= :qbits.jet.websocket/close message-key))
              (is (= websocket-1011-server-error close-code))
              (is (str/includes? (str close-message) "exceeds maximum size")))))

        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-fast test-request-stream-bytes-and-string
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          uncorrupted-data-streamed-atom (atom false)
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "waiter_ws_test"
                           "x-waiter-name" (rand-name))]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)
              ^WebSocketClient websocket-client (websocket-client-factory)
              message-length 2000000 ;; jetty default is 65536
              max-message-length (+ 1024 message-length)]
          (update-max-message-sizes websocket-client max-message-length max-message-length)
          (ws-client/connect!
            websocket-client
            (ws-url waiter-url "/websocket-stream")
            (fn [{:keys [in out]}]
              (async/go
                (async/>! out "hello")
                (async/<! in) ;; kitchen message
                (async/<! in) ;; hello response
                (async/>! out (str "chars-" message-length))
                (let [backend-string (async/<! in)]
                  (async/>! out (.getBytes (str backend-string) "utf-8"))
                  (let [^ByteBuffer backend-bytes (async/<! in)
                        bytes-string (some-> backend-bytes (.array) (String. "utf-8"))]
                    (reset! uncorrupted-data-streamed-atom
                            (and (= message-length (count backend-string)) (= backend-string bytes-string)))))
                (async/>! out "exit")
                (async/<! in) ;; connection closed
                (deliver response-promise :done)
                (async/close! out)))
            {:middleware (fn [_ ^UpgradeRequest request]
                           (websocket/add-headers-to-upgrade-request! request waiter-headers)
                           (add-auth-cookie request auth-cookie-value))})
          (is (= :done (deref response-promise default-timeout-period :timed-out))))
        (is @uncorrupted-data-streamed-atom)
        (finally
          (delete-service waiter-url waiter-headers))))))

(defn- request-streaming-helper
  [waiter-url waiter-headers auth-cookie-value all-iteration-result-atom websocket-client]
  "Helper function that streams text data back and forth between the client and server.
   It verifies that data sent is what it received back.
   Any mismatch is reported as a failed assertion on the data being streamed by Waiter."
  (try
    (let [correlation-id (str "test-request-parallel-streaming-" (utils/unique-identifier))
          iteration-promise (promise)
          streaming-status-promise (promise)]
      (ws-client/connect!
        websocket-client
        (ws-url waiter-url "/websocket-streaming")
        (fn [{:keys [in out]}]
          (async/go
            (async/>! out "hello")
            (async/<! in) ;; kitchen message
            (async/<! in) ;; hello response
            (dotimes [n 5]
              (let [data-size (+ 20000 (rand-int 1000))]
                (async/>! out (str "chars-" data-size))
                (let [backend-string (async/<! in)]
                  (async/>! out (.getBytes (str backend-string) "utf-8"))
                  (let [^ByteBuffer backend-bytes (async/<! in)
                        bytes-string (-> backend-bytes (.array) (String. "utf-8"))
                        same-string-streamed (and (= data-size (count backend-string)) (= backend-string bytes-string))]
                    (when (not same-string-streamed)
                      (log/error correlation-id "had a mismatch in streamed data in iteration" n)
                      (deliver streaming-status-promise :failed))))))
            (async/>! out "exit")
            (async/<! in) ;; connection closed
            (async/close! out)
            (deliver streaming-status-promise :success)
            (deliver iteration-promise @streaming-status-promise)))
        {:middleware (fn [_ ^UpgradeRequest request]
                       (websocket/add-headers-to-upgrade-request! request (assoc waiter-headers "x-cid" correlation-id))
                       (add-auth-cookie request auth-cookie-value))})
      (let [iteration-result (deref iteration-promise default-timeout-period :timed-out)]
        (swap! all-iteration-result-atom assoc correlation-id iteration-result)))
    (catch Exception e
      (log/error e "error in executing websocket request for test")
      (is false (str "websocket streaming iteration threw an error:" (.getMessage e))))))

;FAIL in (test-request-parallel-streaming) (websocket_integration_test.clj:476)
;test-request-parallel-streaming
;{:1006 11}
;expected: 12
;actual: 11
;lein test :only waiter.websocket-integration-test/test-request-parallel-streaming
;FAIL in (test-request-parallel-streaming) (websocket_integration_test.clj:477)
;test-request-parallel-streaming
;Only in a: {:waiting-to-stream 0}
;Only in b: {:waiting-to-stream 1}
(deftest ^:parallel ^:integration-slow ^:explicit test-request-parallel-streaming
  ;; streams requests in parallel and verifies all bytes were transferred correctly and
  ;; they were closed with status codes 1000 and 1006 (due to an issue with jet closing requests).
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          _ (is auth-cookie-value)
          iteration-results-atom (atom {})
          concurrency-level 3
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "waiter_ws_test"
                           "x-waiter-name" (rand-name)
                           "x-waiter-concurrency-level" concurrency-level
                           "x-waiter-scale-up-factor" 0.99
                           "x-waiter-scale-down-factor" 0.001)
          service-id (retrieve-service-id waiter-url waiter-headers)
          num-threads 4
          iterations-per-thread 3
          num-requests (* num-threads iterations-per-thread)
          websocket-client (websocket-client-factory)]
      (with-service-cleanup
        service-id
        (parallelize-requests
          num-threads iterations-per-thread
          (fn test-request-parallel-streaming-task []
            (request-streaming-helper waiter-url waiter-headers auth-cookie-value iteration-results-atom websocket-client)))
        (let [iteration-results @iteration-results-atom]
          (is (every? #{:success :timed-out} (vals iteration-results)) (str iteration-results))
          (is (= num-requests (count iteration-results)))
          (is (<= (* 0.75 num-requests) (->> iteration-results vals (filter #{:success}) count)) (str iteration-results)))
        (is (pos? (num-instances waiter-url service-id)))
        (Thread/sleep 1000) ;; allow metrics to be sync-ed
        (let [service-data (service-settings waiter-url service-id :query-params {"include" "metrics"})
              request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])
              response-status (get-in service-data [:metrics :aggregate :counters :response-status])]
          (is (= num-requests (reduce + (vals (select-keys response-status [:1000 :1006])))) (str response-status))
          (is (= {:total num-requests :waiting-for-available-instance 0 :waiting-to-stream 0}
                 (select-keys request-counts [:total :waiting-for-available-instance :waiting-to-stream]))
              (str request-counts))
          (let [num-timed-out (->> @iteration-results-atom vals (filter #{:timed-out}) count)]
            (is (<= (:outstanding request-counts) num-timed-out) (str request-counts))
            (is (<= (:streaming request-counts) num-timed-out) (str request-counts))))))))