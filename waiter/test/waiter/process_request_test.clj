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
(ns waiter.process-request-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [waiter.core :refer :all]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.descriptor :as descriptor]
            [waiter.process-request :refer :all]
            [waiter.statsd :as statsd]
            [waiter.test-helpers :refer :all])
  (:import (java.io ByteArrayOutputStream InputStream IOException)
           (java.nio ByteBuffer)
           (java.util Arrays)
           (java.util.concurrent CountDownLatch LinkedBlockingQueue ThreadLocalRandom ThreadPoolExecutor TimeUnit)
           (org.eclipse.jetty.client HttpClient)))

(deftest test-prepare-request-properties
  (let [test-cases (list
                     {:name "test-prepare-request-properties:nil-inputs"
                      :input {:request-properties nil :waiter-headers nil}
                      :expected {:async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:empty-nil-inputs"
                      :input {:request-properties {} :waiter-headers nil}
                      :expected {:async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:nil-empty-inputs"
                      :input {:request-properties nil :waiter-headers {}}
                      :expected {:async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:empty-inputs"
                      :input {:request-properties {} :waiter-headers {}}
                      :expected {:async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:missing-timeout-header-1"
                      :input {:request-properties {:fie "foe"} :waiter-headers {:foo "bar"}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:missing-timeout-header-2"
                      :input {:request-properties {:fie "foe", :initial-socket-timeout-ms 100, :streaming-timeout-ms 200}
                              :waiter-headers {:foo "bar"}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms 100, :queue-timeout-ms nil, :streaming-timeout-ms 200}
                      }
                     {:name "test-prepare-request-properties:invalid-timeout-header"
                      :input {:request-properties {:fie "foe", :initial-socket-timeout-ms 100, :queue-timeout-ms 150, :streaming-timeout-ms nil}
                              :waiter-headers {"timeout" "bar"}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms 100, :queue-timeout-ms 150, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:negative-timeout-header"
                      :input {:request-properties {:fie "foe", :initial-socket-timeout-ms 100}
                              :waiter-headers {"timeout" -50}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms 100, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-timeout-header"
                      :input {:request-properties {:fie "foe", :initial-socket-timeout-ms 100, :streaming-timeout-ms 200}
                              :waiter-headers {"timeout" 50, "streaming-timeout" 250}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms 50, :queue-timeout-ms nil, :streaming-timeout-ms 250}
                      }
                     {:name "test-prepare-request-properties:invalid-queue-timeout-header"
                      :input {:request-properties {:fie "foe", :queue-timeout-ms 150}
                              :waiter-headers {"queue-timeout" "bar"}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms 150, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:negative-queue-timeout-header"
                      :input {:request-properties {:fie "foe", :queue-timeout-ms 150}
                              :waiter-headers {"queue-timeout" -50}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms 150, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-queue-timeout-header"
                      :input {:request-properties {:fie "foe", :queue-timeout-ms 150, :streaming-timeout-ms nil}
                              :waiter-headers {"queue-timeout" 50}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms nil, :queue-timeout-ms 50, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-both-timeout-headers"
                      :input {:request-properties {:fie "foe", :initial-socket-timeout-ms 100, :queue-timeout-ms 150}
                              :waiter-headers {"timeout" 75, "queue-timeout" 50}}
                      :expected {:fie "foe", :async-check-interval-ms nil, :async-request-timeout-ms nil, :initial-socket-timeout-ms 75, :queue-timeout-ms 50, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:invalid-async-headers"
                      :input {:request-properties {:fie "foe", :async-check-interval-ms 100, :async-request-timeout-ms 200}
                              :waiter-headers {"async-check-interval" -50, "async-request-timeout" "foo"}}
                      :expected {:fie "foe", :async-check-interval-ms 100, :async-request-timeout-ms 200, :initial-socket-timeout-ms nil, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-async-headers"
                      :input {:request-properties {:fie "foe", :async-check-interval-ms 100, :async-request-timeout-ms 200}
                              :waiter-headers {"async-check-interval" 50, "async-request-timeout" 250}}
                      :expected {:fie "foe", :async-check-interval-ms 50, :async-request-timeout-ms 250, :initial-socket-timeout-ms nil, :queue-timeout-ms nil, :streaming-timeout-ms nil}
                      })]
    (doseq [{:keys [name input expected]} test-cases]
      (testing (str "Test " name)
        (let [actual (prepare-request-properties (:request-properties input)
                                                 (pc/map-keys #(str headers/waiter-header-prefix %) (:waiter-headers input)))]
          (is (= expected actual)))))))

(deftest test-stream-http-response-configure-idle-timeout
  (let [idle-timeout-atom (atom nil)
        output-stream (ByteArrayOutputStream.)]
    (with-redefs [set-idle-timeout! (fn [in-output-stream idle-timeout-ms]
                                      (is (= output-stream in-output-stream))
                                      (reset! idle-timeout-atom idle-timeout-ms))]
      (let [body (async/chan)
            confirm-live-connection (fn [] :nothing)
            request-abort-callback (fn [_] :nothing)
            resp-chan (async/chan)
            streaming-timeout-ms 100
            instance-request-properties {:output-buffer-size 1000000 :streaming-timeout-ms streaming-timeout-ms}
            reservation-status-promise (promise)
            request-state-chan (async/chan)
            metric-group nil
            waiter-debug-enabled? nil
            service-id "service-id"
            abort-request-chan (async/chan)
            error-chan (async/chan)
            response {:abort-request-chan abort-request-chan, :body body, :error-chan error-chan}]

        (async/close! error-chan)
        (async/close! body)

        (stream-http-response
          response confirm-live-connection request-abort-callback resp-chan instance-request-properties
          reservation-status-promise request-state-chan metric-group waiter-debug-enabled?
          (metrics/stream-metric-map service-id))

        (loop []
          (let [message (async/<!! resp-chan)]
            (when (function? message)
              (message output-stream))
            (when message
              (recur))))

        (is (= streaming-timeout-ms @idle-timeout-atom))
        (is (= :success @reservation-status-promise))
        (is (nil? (async/<!! request-state-chan)))))))

(defn- stream-response
  "Calls stream-http-response with statsd/inc! redefined to simply store the count of
  response bytes that it gets called with, and returns these counts in a vector"
  [bytes]
  (let [bytes-reported (atom [])]
    (with-redefs [statsd/inc! (fn [_ metric value]
                                (when (= metric "response_bytes")
                                  (swap! bytes-reported conj value)))]
      (let [body (async/chan)
            confirm-live-connection (fn [] :nothing)
            request-abort-callback (fn [_] :nothing)
            resp-chan (async/chan)
            instance-request-properties {:output-buffer-size 1000000 :streaming-timeout-ms 100}
            reservation-status-promise (promise)
            request-state-chan (async/chan)
            metric-group nil
            waiter-debug-enabled? nil
            service-id "service-id"
            abort-request-chan (async/chan)
            error-chan (async/chan)
            response {:abort-request-chan abort-request-chan, :body body, :error-chan error-chan}]
        (async/go-loop [bytes-streamed 0]
          (if (= bytes-streamed bytes)
            (do
              (async/close! error-chan)
              (async/close! body))
            (let [bytes-to-stream (min 1024 (- bytes bytes-streamed))]
              (async/>! body (byte-array (repeat bytes-to-stream 0)))
              (recur (+ bytes-streamed bytes-to-stream)))))
        (stream-http-response response confirm-live-connection request-abort-callback resp-chan instance-request-properties
                              reservation-status-promise request-state-chan metric-group
                              waiter-debug-enabled? (metrics/stream-metric-map service-id))
        (loop []
          (let [message (async/<!! resp-chan)]
            (when message
              (recur))))))
    @bytes-reported))

(deftest test-stream-http-response
  (testing "Streaming the response"
    (testing "should throttle calls to statsd to report bytes streamed"
      (let [bytes-reported (stream-response 1)]
        (is (= 1 (count bytes-reported)))
        (is (= [1] bytes-reported)))

      (let [bytes-reported (stream-response (* 1024 977))]
        (is (= 1 (count bytes-reported)))
        (is (= [1000448] bytes-reported)))

      (let [bytes-reported (stream-response (-> 1024 (* 977) (inc)))]
        (is (= 2 (count bytes-reported)))
        (is (= [1000448 1] bytes-reported)))

      (let [bytes-reported (stream-response 10000000)]
        (is (= 10 (count bytes-reported)))
        (is (= 10000000 (reduce + bytes-reported)))
        (is (= [1000448 1000448 1000448 1000448 1000448 1000448 1000448 1000448 1000448 995968] bytes-reported))))))

(deftest test-extract-async-request-response-data
  (testing "202-missing-location-header"
    (is (nil?
          (extract-async-request-response-data
            {:status 202 :headers {}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "200-not-async"
    (is (nil?
          (extract-async-request-response-data
            {:status 200, :headers {"location" "/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "303-not-async"
    (is (nil?
          (extract-async-request-response-data
            {:status 303, :headers {"location" "/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "404-not-async"
    (is (nil?
          (extract-async-request-response-data
            {:status 404, :headers {"location" "/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "202-absolute-location"
    (is (= {:location "/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status 202 :headers {"location" "/result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-relative-location-1"
    (is (= {:location "/query/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status 202 :headers {"location" "../result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-relative-location-2"
    (is (= {:location "/query/for/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status 202 :headers {"location" "result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-relative-location-two-levels"
    (is (= {:location "/result/location" :query-string "p=q&r=s|t"}
           (extract-async-request-response-data
             {:status 202 :headers {"location" "../../result/location?p=q&r=s|t"}}
             "http://www.example.com:1234/query/for/status?u=v&w=x|y|z"))))
  (testing "202-absolute-url-same-host-port"
    (is (= {:location "/retrieve/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status 202 :headers {"location" "http://www.example.com:1234/retrieve/result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-absolute-url-different-host"
    (is (nil?
          (extract-async-request-response-data
            {:status 202 :headers {"location" "http://www.example2.com:1234/retrieve/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "202-absolute-url-different-port"
    (is (nil?
          (extract-async-request-response-data
            {:status 202 :headers {"location" "http://www.example.com:5678/retrieve/result/location"}}
            "http://www.example.com:1234/query/for/status")))))

(deftest test-stream-http-request
  (let [make-input-stream (fn [read-index-atom input-bytes delayed-read]
                            (let [delay-latch (atom true)]
                              (proxy [InputStream] []
                                (available []
                                  (if (and delayed-read @delay-latch)
                                    (do
                                      (reset! delay-latch false)
                                      0)
                                    (->> input-bytes
                                         (drop @read-index-atom)
                                         (map count)
                                         (reduce +))))
                                (read [target-bytes]
                                  (reset! delay-latch true)
                                  (cond
                                    (= @read-index-atom (count input-bytes))
                                    (do
                                      (swap! read-index-atom inc)
                                      -1)
                                    (< @read-index-atom (count input-bytes))
                                    (let [source-bytes (nth input-bytes @read-index-atom)
                                          num-source-bytes (count source-bytes)]
                                      (when (> num-source-bytes (count target-bytes))
                                        (throw (IOException.
                                                 (str "Insufficient space in target bytes "
                                                      {:available (count target-bytes)
                                                       :required num-source-bytes}))))
                                      (swap! read-index-atom inc)
                                      (System/arraycopy source-bytes 0 target-bytes 0 num-source-bytes)
                                      num-source-bytes)
                                    :else
                                    (throw (IOException. "Input stream has no more data")))))))
        make-error-handler-fn (fn [body-ch exception-atom]
                                (fn [ex]
                                  (reset! exception-atom ex)
                                  (async/close! body-ch)))
        byte-buffer->byte-array (fn [^ByteBuffer byte-buffer]
                                  (let [result-array (byte-array (.remaining byte-buffer))]
                                    (.get byte-buffer result-array 0 (count result-array))
                                    result-array))
        streaming-complete-fn (constantly true)
        streaming-timeout-ms 3000]

    (testing "successful read - single task"
      (let [executor (ThreadPoolExecutor. 1 1 1 TimeUnit/MINUTES (LinkedBlockingQueue.))
            service-id (str "test-service-id-" (System/nanoTime))
            metric-group service-id
            input-bytes (for [n (range 5)]
                          (let [bytes (byte-array (-> n inc (* 100)))]
                            (.nextBytes (ThreadLocalRandom/current) bytes)
                            bytes))
            read-index-atom (atom 0)
            input-stream (make-input-stream read-index-atom input-bytes false)
            body-ch (async/chan 2000)
            exception-atom (atom nil)
            error-handler-fn (make-error-handler-fn body-ch exception-atom)]

        (stream-http-request executor service-id metric-group error-handler-fn streaming-complete-fn
                             streaming-timeout-ms input-stream body-ch 0)

        (let [num-fragments-read-atom (atom 0)]
          (doseq [^bytes source-bytes input-bytes]
            (when-let [read-buffer (async/<!! body-ch)]
              (swap! num-fragments-read-atom inc)
              (is (Arrays/equals source-bytes ^bytes (byte-buffer->byte-array read-buffer)))))
          (is (= (count input-bytes) @num-fragments-read-atom)))
        (is (nil? (async/<!! body-ch)))
        (is (nil? @exception-atom))
        (let [histogram (metrics/service-histogram service-id "request-size")]
          (is (= 1 (histograms/number-recorded histogram)))
          (is (= (reduce + (map count input-bytes)) (histograms/largest histogram))))
        (is (= 1 (.getTaskCount executor)))
        (.shutdown executor)))

    (testing "successful read - multiple tasks"
      (let [executor (ThreadPoolExecutor. 1 1 1 TimeUnit/MINUTES (LinkedBlockingQueue.))
            service-id (str "test-service-id-" (System/nanoTime))
            metric-group service-id
            input-bytes (for [n (range 5)]
                          (let [bytes (byte-array (-> n inc (* 100)))]
                            (.nextBytes (ThreadLocalRandom/current) bytes)
                            bytes))
            read-index-atom (atom 0)
            input-stream (make-input-stream read-index-atom input-bytes true)
            body-ch (async/chan 2000)
            exception-atom (atom nil)
            error-handler-fn (make-error-handler-fn body-ch exception-atom)]

        (stream-http-request executor service-id metric-group error-handler-fn streaming-complete-fn
                             streaming-timeout-ms input-stream body-ch 0)

        (let [num-fragments-read-atom (atom 0)]
          (doseq [^bytes source-bytes input-bytes]
            (when-let [read-buffer (async/<!! body-ch)]
              (swap! num-fragments-read-atom inc)
              (is (Arrays/equals source-bytes ^bytes (byte-buffer->byte-array read-buffer)))))
          (is (= (count input-bytes) @num-fragments-read-atom)))
        (is (nil? (async/<!! body-ch)))
        (is (nil? @exception-atom))
        (let [histogram (metrics/service-histogram service-id "request-size")]
          (is (= 1 (histograms/number-recorded histogram)))
          (is (= (reduce + (map count input-bytes)) (histograms/largest histogram))))
        (is (= (count input-bytes) (.getTaskCount executor)))
        (.shutdown executor)))

    (testing "unsuccessful read - single task"
      (let [executor (ThreadPoolExecutor. 1 1 1 TimeUnit/MINUTES (LinkedBlockingQueue.))
            service-id (str "test-service-id-" (System/nanoTime))
            metric-group service-id
            input-bytes (for [n (range 5)]
                          (let [bytes (byte-array (-> n inc (* 10000)))]
                            (.nextBytes (ThreadLocalRandom/current) bytes)
                            bytes))
            read-index-atom (atom 0)
            input-stream (make-input-stream read-index-atom input-bytes false)
            body-ch (async/chan 2000)
            exception-atom (atom nil)
            error-handler-fn (make-error-handler-fn body-ch exception-atom)]

        (stream-http-request executor service-id metric-group error-handler-fn streaming-complete-fn
                             streaming-timeout-ms input-stream body-ch 0)

        (let [num-fragments-read-atom (atom 0)]
          (doseq [^bytes source-bytes input-bytes]
            (when-let [read-buffer (async/<!! body-ch)]
              (swap! num-fragments-read-atom inc)
              (is (Arrays/equals source-bytes ^bytes (byte-buffer->byte-array read-buffer)))))
          (is (= 3 @num-fragments-read-atom)))
        (is (nil? (async/<!! body-ch)))
        (is (not (nil? @exception-atom)))
        (is (= "Insufficient space in target bytes {:available 32768, :required 40000}"
               (some-> @exception-atom .getMessage)))
        (let [histogram (metrics/service-histogram service-id "request-size")]
          (is (= 1 (histograms/number-recorded histogram)))
          (is (= (reduce + (map count (take 3 input-bytes))) (histograms/largest histogram))))
        (is (zero? (.getTaskCount executor)))
        (.shutdown executor)))

    (testing "unsuccessful read - multiple tasks"
      (let [executor (ThreadPoolExecutor. 1 1 1 TimeUnit/MINUTES (LinkedBlockingQueue.))
            service-id (str "test-service-id-" (System/nanoTime))
            metric-group service-id
            input-bytes (for [n (range 40)]
                          (let [bytes (byte-array (-> n inc (* 1000)))]
                            (.nextBytes (ThreadLocalRandom/current) bytes)
                            bytes))
            read-index-atom (atom 0)
            input-stream (make-input-stream read-index-atom input-bytes true)
            body-ch (async/chan 2000)
            exception-atom (atom nil)
            error-handler-fn (make-error-handler-fn body-ch exception-atom)]

        (stream-http-request executor service-id metric-group error-handler-fn streaming-complete-fn
                             streaming-timeout-ms input-stream body-ch 0)

        (let [num-fragments-read-atom (atom 0)]
          (doseq [^bytes source-bytes input-bytes]
            (when-let [read-buffer (async/<!! body-ch)]
              (swap! num-fragments-read-atom inc)
              (is (Arrays/equals source-bytes ^bytes (byte-buffer->byte-array read-buffer)))))
          (is (= 32 @num-fragments-read-atom)))
        (is (nil? (async/<!! body-ch)))
        (is (not (nil? @exception-atom)))
        (is (= "Insufficient space in target bytes {:available 32768, :required 33000}"
               (some-> @exception-atom .getMessage)))
        (let [histogram (metrics/service-histogram service-id "request-size")]
          (is (= 1 (histograms/number-recorded histogram)))
          (is (= (reduce + (map count (take 32 input-bytes))) (histograms/largest histogram))))
        (is (= 32 (.getTaskCount executor)))
        (.shutdown executor)))

    (testing "successful read - single task - no buffer space"
      (let [executor (ThreadPoolExecutor. 1 1 1 TimeUnit/MINUTES (LinkedBlockingQueue.))
            service-id (str "test-service-id-" (System/nanoTime))
            metric-group service-id
            input-bytes (for [n (range 4000)]
                          (let [bytes (byte-array (-> n inc))]
                            (.nextBytes (ThreadLocalRandom/current) bytes)
                            bytes))
            read-index-atom (atom 0)
            input-stream (make-input-stream read-index-atom input-bytes false)
            body-ch (async/chan 3)
            exception-atom (atom nil)
            error-handler-fn (make-error-handler-fn body-ch exception-atom)]

        (stream-http-request executor service-id metric-group error-handler-fn streaming-complete-fn
                             streaming-timeout-ms input-stream body-ch 0)

        (is (wait-for #(async-protocols/closed? body-ch)))
        (let [num-fragments-read-atom (atom 0)]
          (doseq [^bytes source-bytes input-bytes]
            (when-let [read-buffer (async/<!! body-ch)]
              (swap! num-fragments-read-atom inc)
              (is (Arrays/equals source-bytes ^bytes (byte-buffer->byte-array read-buffer)))))
          (is (not= (count input-bytes) @num-fragments-read-atom)))
        (is (nil? (async/<!! body-ch)))
        (is (not (nil? @exception-atom)))
        (is (str/includes? (str (some-> @exception-atom .getMessage)) "unable to stream request bytes"))
        (let [histogram (metrics/service-histogram service-id "request-size")]
          (is (= 1 (histograms/number-recorded histogram)))
          (is (> (reduce + (map count input-bytes)) (histograms/largest histogram))))
        (is (zero? (.getTaskCount executor)))
        (.shutdown executor)))

    (testing "successful read - multiple tasks - queue limit reached"
      (let [queue-capacity 4
            executor (ThreadPoolExecutor. 1 1 1 TimeUnit/MINUTES (LinkedBlockingQueue. queue-capacity))
            service-id (str "test-service-id-" (System/nanoTime))
            metric-group service-id
            input-bytes (for [n (range 5)]
                          (let [bytes (byte-array (-> n inc (* 100)))]
                            (.nextBytes (ThreadLocalRandom/current) bytes)
                            bytes))
            read-index-atom (atom 0)
            input-stream (make-input-stream read-index-atom input-bytes true)
            body-ch (async/chan 3)
            exception-atom (atom nil)
            error-handler-fn (make-error-handler-fn body-ch exception-atom)]

        (let [sleep-duration 10000
              latch (CountDownLatch. 1)
              runnable-task (fn []
                              (.countDown latch)
                              (Thread/sleep sleep-duration))]
          (.execute executor runnable-task)
          (.await latch)
          (dotimes [_ queue-capacity]
            (.execute executor (constantly true))))

        (stream-http-request executor service-id metric-group error-handler-fn streaming-complete-fn
                             streaming-timeout-ms input-stream body-ch 0)

        (let [num-fragments-read-atom (atom 0)]
          (doseq [^bytes source-bytes input-bytes]
            (when-let [read-buffer (async/<!! body-ch)]
              (swap! num-fragments-read-atom inc)
              (is (Arrays/equals source-bytes ^bytes (byte-buffer->byte-array read-buffer)))))
          (is (= 1 @num-fragments-read-atom)))
        (is (not (nil? @exception-atom)))
        (is (str/includes? (str (some-> @exception-atom .getMessage)) "Too many concurrent requests on router"))
        (let [histogram (metrics/service-histogram service-id "request-size")]
          (is (= 1 (histograms/number-recorded histogram)))
          (is (= (count (first input-bytes)) (histograms/largest histogram))))

        (.shutdown executor)))))

(deftest test-make-request
  (let [instance {:service-id "test-service-id", :host "example.com", :port 8080}
        backend-proto "http"
        request {:authorization/principal "test-user@test.com"
                 :authorization/user "test-user"
                 :body "body"}
        request-properties {:connection-timeout-ms 123456, :initial-socket-timeout-ms 654321}
        passthrough-headers {"accept" "text/html"
                             "accept-charset" "ISO-8859-1,utf-8;q=0.7,*;q=0.7"
                             "accept-language" "en-us"
                             "accept-encoding" "gzip,deflate"
                             "authorization" "test-user-authorization"
                             "cache-control" "no-cache"
                             "connection" "keep-alive"
                             "content-length" "12341234"
                             "content-MD5" "Q2hlY2sgSW50ZWdyaXR5IQ=="
                             "content-type" "application/x-www-form-urlencoded"
                             "cookie" "$Version=1; Skin=new;"
                             "date" "Tue, 15 Nov 2015 08:12:31 GMT"
                             "expect" "100-continue"
                             "expires" "2200-08-09"
                             "forwarded" "for=192.0.2.43, for=198.51.100.17"
                             "from" "user@example.com"
                             "host" "www.test-source.com"
                             "if-match" "737060cd8c284d8af7ad3082f209582d"
                             "if-modified-since" "Sat, 29 Oct 2015 19:43:31 GMT"
                             "keep-alive" "300"
                             "origin" "http://example.example.com"
                             "pragma" "no-cache"
                             "proxy-authenticate" "proxy-authenticate value"
                             "proxy-authorization" "proxy-authorization value"
                             "proxy-connection" "keep-alive"
                             "referer" "http://www.test-referer.com"
                             "te" "trailers, deflate"
                             "trailers" "trailer-name-1, trailer-name-2"
                             "transfer-encoding" "trailers, deflate"
                             "upgrade" "HTTP/2.0, HTTPS/1.3, IRC/6.9, RTA/x11, websocket"
                             "user-agent" "Test/App"
                             "x-cid" "239874623hk2er7908245"
                             "x-forwarded-for" "client1, proxy1, proxy2"
                             "x-http-method-override" "DELETE"}
        end-route "/end-route"
        app-password "test-password"
        executor (ThreadPoolExecutor. 1 1 1 TimeUnit/MINUTES (LinkedBlockingQueue.))]
    (let [expected-endpoint "http://example.com:8080/end-route"
          make-basic-auth-fn (fn make-basic-auth-fn [endpoint username password]
                               (is (= expected-endpoint endpoint))
                               (is (= username "waiter"))
                               (is (= app-password password))
                               (Object.))
          service-id->password-fn (fn service-id->password-fn [service-id]
                                    (is (= "test-service-id" service-id))
                                    app-password)
          http-clients {:http1-client (http/client)}
          http-request-mock-factory (fn [passthrough-headers request-method-fn-call-counter proto-version]
                                      (fn [^HttpClient _ request-config]
                                        (swap! request-method-fn-call-counter inc)
                                        (is (= expected-endpoint (:url request-config)))
                                        (is (= :bytes (:as request-config)))
                                        (is (:auth request-config))
                                        (is (= "body" (:body request-config)))
                                        (is (= 654321 (:idle-timeout request-config)))
                                        (is (= (-> passthrough-headers
                                                   (dissoc "expect" "authorization"
                                                           "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                                                           "te" "trailers" "transfer-encoding" "upgrade")
                                                   (merge {"x-waiter-auth-principal" "test-user"
                                                           "x-waiter-authenticated-principal" "test-user@test.com"}))
                                               (:headers request-config)))
                                        (is (= proto-version (:version request-config)))))]
      (testing "make-request:headers:HTTP/1.0"
        (let [proto-version "HTTP/1.0"
              request-method-fn-call-counter (atom 0)
              request-control-chan (async/promise-chan)]
          (with-redefs [http/request (http-request-mock-factory passthrough-headers request-method-fn-call-counter proto-version)]
            (make-request executor http-clients make-basic-auth-fn service-id->password-fn instance request request-properties
                          passthrough-headers end-route nil backend-proto proto-version request-control-chan)
            (is (= 1 @request-method-fn-call-counter)))))

      (testing "make-request:headers:HTTP/2.0"
        (let [proto-version "HTTP/2.0"
              request-method-fn-call-counter (atom 0)
              request-control-chan (async/promise-chan)]
          (with-redefs [http/request (http-request-mock-factory passthrough-headers request-method-fn-call-counter proto-version)]
            (make-request executor http-clients make-basic-auth-fn service-id->password-fn instance request request-properties
                          passthrough-headers end-route nil backend-proto proto-version request-control-chan)
            (is (= 1 @request-method-fn-call-counter)))))

      (testing "make-request:headers-long-content-length"
        (let [proto-version "HTTP/1.0"
              request-method-fn-call-counter (atom 0)
              passthrough-headers (assoc passthrough-headers "content-length" "1234123412341234")
              statsd-inc-call-value (promise)
              request-control-chan (async/promise-chan)]
          (with-redefs [http/request (http-request-mock-factory passthrough-headers request-method-fn-call-counter proto-version)
                        statsd/inc!
                        (fn [metric-group metric value]
                          (is (nil? metric-group))
                          (is (= "request_content_length" metric))
                          (deliver statsd-inc-call-value value))]
            (make-request executor http-clients make-basic-auth-fn service-id->password-fn instance request request-properties
                          passthrough-headers end-route nil backend-proto proto-version request-control-chan)
            (is (= 1 @request-method-fn-call-counter))
            (is (= 1234123412341234 (deref statsd-inc-call-value 0 :statsd-inc-not-called)))))))

    (.shutdown executor)))

(deftest test-wrap-suspended-service
  (testing "returns error for suspended app"
    (let [handler (wrap-suspended-service (fn [_] {:status 200}))
          request {:descriptor {:service-id "service-id-1"
                                :suspended-state {:suspended true
                                                  :last-updated-by "test-user"
                                                  :time (t/now)}}}
          {:keys [status body]} (handler request)]
      (is (= 503 status))
      (is (str/includes? body "Service has been suspended"))
      (is (str/includes? body "test-user"))))

  (testing "passes apps by default"
    (let [handler (wrap-suspended-service (fn [_] {:status 200}))
          request {}
          {:keys [status]} (handler request)]
      (is (= 200 status)))))

(deftest test-wrap-too-many-requests
  (testing "returns error for too many requests"
    (let [service-id "my-service"
          counter (metrics/service-counter service-id "request-counts" "waiting-for-available-instance")]
      (counters/clear! counter)
      (counters/inc! counter 10)
      (let [handler (wrap-too-many-requests (fn [_] {:status 200}))
            request {:descriptor {:service-id service-id
                                  :service-description {"max-queue-length" 5}}}
            {:keys [status body]} (handler request)]
        (is (= 503 status))
        (is (str/includes? body "Max queue length")))))

  (testing "passes service with fewer requests"
    (let [service-id "ok-service"
          counter (metrics/service-counter service-id "request-counts" "waiting-for-available-instance")]
      (counters/clear! counter)
      (counters/inc! counter 3)
      (let [handler (wrap-too-many-requests (fn [_] {:status 200}))
            request {:descriptor {:service-id service-id
                                  :service-description {"max-queue-length" 10}}}
            {:keys [status]} (handler request)]
        (is (= 200 status))))))

(deftest test-redirect-on-process-error
  (let [request->descriptor-fn (fn [_]
                                 (throw
                                   (ex-info "Test exception" {:type :service-description-error
                                                              :issue {"run-as-user" "missing-required-key"}
                                                              :x-waiter-headers {"queue-length" 100}})))
        start-new-service-fn (constantly nil)
        fallback-state-atom (atom {})
        handler (descriptor/wrap-descriptor (fn [_] {:status 200}) request->descriptor-fn start-new-service-fn fallback-state-atom)]
    (testing "with-query-params"
      (let [request {:headers {"host" "www.example.com:1234"}, :query-string "a=b&c=d", :uri "/path"}
            {:keys [headers status]} (handler request)]
        (is (= 303 status))
        (is (= "/waiter-consent/path?a=b&c=d" (get headers "location")))))

    (testing "with-query-params-and-default-port"
      (let [request {:headers {"host" "www.example.com"}, :query-string "a=b&c=d", :uri "/path"}
            {:keys [headers status]} (handler request)]
        (is (= 303 status))
        (is (= "/waiter-consent/path?a=b&c=d" (get headers "location")))))

    (testing "without-query-params"
      (let [request {:headers {"host" "www.example.com:1234"}, :uri "/path"}
            {:keys [headers status]} (handler request)]
        (is (= 303 status))
        (is (= "/waiter-consent/path" (get headers "location")))))))

(deftest test-no-redirect-on-process-error
  (let [request->descriptor-fn (fn [_] (throw (Exception. "Exception message")))
        start-new-service-fn (constantly nil)
        fallback-state-atom (atom {})
        handler (descriptor/wrap-descriptor (fn [_] {:status 200}) request->descriptor-fn start-new-service-fn fallback-state-atom)
        request {}
        {:keys [body headers status]} (handler request)]
    (is (= 500 status))
    (is (nil? (get headers "location")))
    (is (= "text/plain" (get headers "content-type")))
    (is (str/includes? (str body) "Internal error"))))

(deftest test-message-reaches-user-on-process-error
  (let [request->descriptor-fn (fn [_] (throw (ex-info "Error message for user" {:status 404})))
        start-new-service-fn (constantly nil)
        fallback-state-atom (atom {})
        handler (descriptor/wrap-descriptor (fn [_] {:status 200}) request->descriptor-fn start-new-service-fn fallback-state-atom)
        request {}
        {:keys [body headers status]} (handler request)]
    (is (= 404 status))
    (is (= "text/plain" (get headers "content-type")))
    (is (str/includes? (str body) "Error message for user"))))

(deftest test-determine-priority
  (let [position-generator-atom (atom 100)]
    (is (nil? (determine-priority position-generator-atom nil)))
    (is (nil? (determine-priority position-generator-atom {})))
    (is (nil? (determine-priority position-generator-atom {"foo" 1})))
    (is (= [1 -101] (determine-priority position-generator-atom {"x-waiter-priority" 1})))
    (is (= [2 -102] (determine-priority position-generator-atom {"x-waiter-priority" "2"})))
    (is (= [4 -103] (determine-priority position-generator-atom {"x-waiter-foo" "2", "x-waiter-priority" "4"})))
    (is (nil? (determine-priority position-generator-atom {"priority" 1})))
    (is (= 103 @position-generator-atom))))
