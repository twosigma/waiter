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
(ns waiter.websocket-integration-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [qbits.jet.client.websocket :as ws-client]
            [waiter.client-tools :refer :all]
            [waiter.utils :as utils]
            [waiter.websocket :as websocket])
  (:import (java.net HttpCookie)
           (org.eclipse.jetty.websocket.api UpgradeException UpgradeRequest)
           (org.eclipse.jetty.websocket.client WebSocketClient)))

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

(deftest ^:parallel ^:integration-fast test-request-auth-failure
  (testing-using-waiter-url
    (let [connect-success-promise (promise)
          connection (ws-client/connect!
                       (websocket-client-factory)
                       (ws-url waiter-url "/websocket-unauth")
                       (fn [{:keys [out]}]
                         (deliver connect-success-promise :success)
                         (async/close! out)))
          ctrl-chan (.ctrl (:socket connection))
          [close-code error] (async/<!! ctrl-chan)]
      (is (= :qbits.jet.websocket/error close-code))
      (is (instance? UpgradeException error))
      (is (= "403 Unauthorized" (.getMessage error)))
      (is (not (realized? connect-success-promise))))))

(deftest ^:parallel ^:integration-fast test-request-auth-success
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          ws-response-atom (atom [])
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "test-ws-support"
                           "x-waiter-name" (rand-name))]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)]
          (ws-client/connect!
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
          (is (= :done (deref response-promise (-> 2 t/minutes t/in-millis) :timed-out))))
        (is (= "Connected to kitchen" (first @ws-response-atom)))
        (let [{:keys [headers]} (-> @ws-response-atom second str json/read-str walk/keywordize-keys)
              {:keys [upgrade x-cid x-waiter-auth-principal]} headers]
          (is x-cid)
          (is (= upgrade "websocket"))
          (is (= x-waiter-auth-principal (retrieve-username))))
        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-fast test-request-socket-timeout
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          send-success-after-timeout-atom (atom true)
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "test-ws-support"
                           "x-waiter-name" (rand-name))]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)]
          (ws-client/connect!
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
          (is (= :done (deref response-promise (-> 2 t/minutes t/in-millis) :timed-out))))
        (is (not @send-success-after-timeout-atom))
        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-fast test-request-instance-death
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          send-success-after-timeout-atom (atom true)
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "test-ws-support"
                           "x-waiter-name" (rand-name)
                           "x-waiter-max-instances" "1"
                           "x-waiter-concurrency-level" "20")]
      (is auth-cookie-value)
      (try
        (let [response-promise (promise)
              client http-client]
          (ws-client/connect!
            (websocket-client-factory)
            (ws-url waiter-url "/websocket-timeout")
            (fn [{:keys [in out]}]
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
          (is (= :done (deref response-promise (-> 2 t/minutes t/in-millis) :timed-out))))
        (is (not @send-success-after-timeout-atom))
        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-slow test-fail-to-stream-large-responses
  (testing-using-waiter-url
    (let [^WebSocketClient websocket-client (websocket-client-factory)
          waiter-settings (waiter-settings waiter-url)
          {:keys [ws-max-binary-message-size ws-max-text-message-size]} (:websocket-config waiter-settings)
          ws-max-binary-message-size' (+ 2048 ws-max-binary-message-size)
          ws-max-text-message-size' (+ 2048 ws-max-text-message-size)
          auth-cookie-value (auth-cookie waiter-url)
          waiter-headers (-> (kitchen-request-headers)
                             (assoc :x-waiter-mem 1024
                                    :x-waiter-metric-group "test-ws-support"
                                    :x-waiter-name (rand-name))
                             (update :x-waiter-cmd
                                     (fn [cmd] (str cmd ;; on-the-fly doesn't support x-waiter-env
                                                    " --mem 1024M"
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
                  (async/>! out (str "bytes-" (+ ws-max-text-message-size 1024)))
                  (let [backend-response (async/<! in)]
                    (deliver backend-data-promise backend-response))
                  (async/close! out)
                  (let [ctrl-data (async/<! ctrl)]
                    (deliver ctrl-data-promise ctrl-data))
                  (deliver response-promise :done)))
              {:middleware middleware})

            (is (= :done (deref response-promise (-> 3 t/minutes t/in-millis) :timed-out)))
            (is (nil? (deref backend-data-promise 100 :timed-out)))
            (let [[message-key close-code close-message] (deref ctrl-data-promise 100 [:timed-out])]
              (is (= :qbits.jet.websocket/close message-key))
              (is (= 1011 close-code))
              (is (str/includes? (str close-message) "exceeds maximum size")))))

        (finally
          (delete-service waiter-url waiter-headers))))))

(deftest ^:parallel ^:integration-fast test-request-stream-bytes-and-string
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          uncorrupted-data-streamed-atom (atom false)
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "test-ws-support"
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
                  (let [backend-bytes (async/<! in)
                        bytes-string (-> backend-bytes (.array) (String. "utf-8"))]
                    (reset! uncorrupted-data-streamed-atom
                            (and (= message-length (count backend-string)) (= backend-string bytes-string)))))
                (deliver response-promise :done)
                (async/close! out)))
            {:middleware (fn [_ ^UpgradeRequest request]
                           (websocket/add-headers-to-upgrade-request! request waiter-headers)
                           (add-auth-cookie request auth-cookie-value))})
          (is (= :done (deref response-promise (-> 2 t/minutes t/in-millis) :timed-out))))
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
              (let [data-size (+ 20000 (rand-int 200000))]
                (async/>! out (str "chars-" data-size))
                (let [backend-string (async/<! in)]
                  (async/>! out (.getBytes (str backend-string) "utf-8"))
                  (let [backend-bytes (async/<! in)
                        bytes-string (-> backend-bytes (.array) (String. "utf-8"))
                        same-string-streamed (and (= data-size (count backend-string)) (= backend-string bytes-string))]
                    (when (not same-string-streamed)
                      (log/error correlation-id "had a mismatch in streamed data in iteration" n)
                      (deliver streaming-status-promise :failed))))))
            (async/close! out)
            (deliver streaming-status-promise :success)
            (deliver iteration-promise @streaming-status-promise)))
        {:middleware (fn [_ ^UpgradeRequest request]
                       (websocket/add-headers-to-upgrade-request! request (assoc waiter-headers "x-cid" correlation-id))
                       (add-auth-cookie request auth-cookie-value))})
      (let [iteration-result (deref iteration-promise (-> 2 t/minutes t/in-millis) :timed-out)]
        (swap! all-iteration-result-atom assoc correlation-id iteration-result)))
    (catch Exception e
      (log/error e "error in executing websocket request for test")
      (is false (str "websocket streaming iteration threw an error:" (.getMessage e))))))

; Marked explicit due to flaky failures
(deftest ^:parallel ^:integration-fast ^:explicit test-request-parallel-streaming
  (testing-using-waiter-url
    (let [auth-cookie-value (auth-cookie waiter-url)
          _ (is auth-cookie-value)
          all-iteration-result-atom (atom {})
          concurrency-level 5
          waiter-headers (assoc (kitchen-request-headers)
                           "x-waiter-metric-group" "test-ws-support"
                           "x-waiter-name" (rand-name)
                           "x-waiter-concurrency-level" concurrency-level
                           "x-waiter-scale-up-factor" 0.99
                           "x-waiter-scale-down-factor" 0.001)
          service-id (retrieve-service-id waiter-url waiter-headers)
          num-threads 6
          iterations-per-thread 3
          num-requests (* num-threads iterations-per-thread)
          websocket-client (websocket-client-factory)]
      (try
        (parallelize-requests
          num-threads iterations-per-thread
          (fn []
            (request-streaming-helper waiter-url waiter-headers auth-cookie-value all-iteration-result-atom
                                      websocket-client)))
        (is (= num-requests (count @all-iteration-result-atom)))
        (is (every? #(= :success (val %1)) @all-iteration-result-atom) (str @all-iteration-result-atom))
        (let [expected-instances (-> (/ num-threads concurrency-level) Math/ceil int)]
          (is (= expected-instances (num-instances waiter-url service-id))))
        (Thread/sleep 1000) ;; allow metrics to be sync-ed
        (let [service-data (service-settings waiter-url service-id)
              request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
          (is (= num-requests (get-in service-data [:metrics :aggregate :counters :response-status :1000])))
          (is (= {:outstanding 0, :streaming 0, :successful num-requests, :total num-requests, :waiting-for-available-instance 0, :waiting-to-stream 0}
                 (select-keys request-counts [:outstanding :streaming :successful :total :waiting-for-available-instance :waiting-to-stream]))))
        (finally
          (delete-service waiter-url service-id))))))
