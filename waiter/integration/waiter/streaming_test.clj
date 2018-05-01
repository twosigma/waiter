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
(ns waiter.streaming-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all])
  (:import (java.io IOException)
           (java.net HttpURLConnection)))

(deftest ^:parallel ^:integration-fast test-streaming
  (testing-using-waiter-url
    (let [file-size-double 690606.0
          file-path "test-files/big.txt"
          service-name (rand-name)
          post-body (slurp file-path)
          headers {:x-waiter-name service-name, :x-kitchen-echo "true"}
          make-request #(make-kitchen-request waiter-url headers :body post-body)
          response (make-request)
          metrics-sync-interval-ms (get-in (waiter-settings waiter-url) [:metrics-config :metrics-sync-interval-ms])
          service-id (retrieve-service-id waiter-url (:request-headers response))
          epsilon 0.01]
      (dotimes [_ 49] (make-request))
      ; wait to allow metrics to be aggregated
      (let [sleep-period (max (* 20 metrics-sync-interval-ms) 10000)]
        (log/debug "sleeping for" sleep-period "ms")
        (Thread/sleep sleep-period))
      ; assert request response size metrics
      (let [service-settings (service-settings waiter-url service-id)
            _ (log/info "metrics" (get service-settings :metrics))
            request-size-histogram (get-in service-settings [:metrics :aggregate :histograms :request-size])
            response-size-histogram (get-in service-settings [:metrics :aggregate :histograms :response-size])]
        (is (= 50 (get-in request-size-histogram [:count] 0))
            (str "request-size-histogram: " request-size-histogram))
        (is (->> (- file-size-double (get-in request-size-histogram [:value :0.0] 0.0)) (double) (Math/abs) (>= epsilon))
            (str "request-size-histogram: " request-size-histogram))
        (is (= 50 (get-in response-size-histogram [:count]))
            (str "response-size-histogram: " response-size-histogram))
        (is (->> (- file-size-double (get-in response-size-histogram [:value :0.0] 0.0)) (double) (Math/abs) (>= epsilon))
            (str "response-size-histogram: " response-size-histogram)))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-large-request
  (testing-using-waiter-url
    (let [request-body (apply str (take 2000000 (repeat "hello")))
         headers {:x-waiter-name (rand-name), :x-kitchen-echo true}
         {:keys [body request-headers] :as response} (make-kitchen-request waiter-url headers :body request-body)]
      (assert-response-status response 200)
      (is (= (count request-body) (count body)))
      (delete-service waiter-url (retrieve-service-id waiter-url request-headers)))))

(defn- perform-streaming-timeout-test
  [waiter-url data-size-in-bytes service-name streaming-timeout-fn streaming-timeout-limit-fn
   chunk-delay-fn chunk-size-fn assertion-fn]
  (let [path "/chunked"
        request-url (str HTTP-SCHEME waiter-url path)
        correlation-id (rand-name)
        _ (log/info "request-url =" request-url ", correlation-id =" correlation-id)
        streaming-timeout-ms (get-in (waiter-settings waiter-url) [:instance-request-properties :streaming-timeout-ms])
        streaming-timeout-limit-ms (streaming-timeout-limit-fn streaming-timeout-ms)
        kitchen-request-headers (merge (kitchen-request-headers)
                                       {:x-cid correlation-id
                                        :x-kitchen-response-size data-size-in-bytes
                                        :x-kitchen-chunk-size (chunk-size-fn data-size-in-bytes)
                                        :x-kitchen-chunk-delay (chunk-delay-fn streaming-timeout-ms)
                                        :x-waiter-debug true
                                        :x-waiter-name service-name})
        ^HttpURLConnection url-connection (open-url-connection request-url :post kitchen-request-headers)]
    (when-let [request-streaming-timeout (streaming-timeout-fn streaming-timeout-ms)]
      (.setRequestProperty url-connection "x-waiter-streaming-timeout" (str request-streaming-timeout)))
    (let [service-id (-> url-connection
                         (.getHeaderField "X-Waiter-Backend-Id")
                         (instance-id->service-id))
          input-stream (.getInputStream url-connection)
          data-byte-array (byte-array 300000)
          sleep-iteration 5]
      (try
        (log/info "Reading from request")
        (loop [iteration 0
               bytes-read 0]
          (when (= iteration sleep-iteration)
            (log/info "Sleeping for" streaming-timeout-limit-ms "ms")
            (Thread/sleep streaming-timeout-limit-ms))
          (let [bytes-read-iter (.read input-stream data-byte-array)
                bytes-read-so-far (if (neg? bytes-read-iter) bytes-read (+ bytes-read-iter bytes-read))]
            (if (pos? bytes-read-iter)
              (do
                (when (or (= iteration (dec sleep-iteration)) (zero? (mod iteration 100)))
                  (log/info (str "Iteration-" iteration) "bytes-read-iter:" bytes-read-iter ", bytes-read-total:" bytes-read-so-far))
                (recur (inc iteration) bytes-read-so-far))
              (do
                (log/info (str "Iteration-" iteration) "response size:" bytes-read-so-far)
                (assertion-fn bytes-read-so-far data-size-in-bytes)))))
        (log/info "Done reading from request")
        (finally
          (.close input-stream)
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-slow test-streaming-timeout-on-default-settings
  (testing-using-waiter-url
    (log/info "Streaming test timeout with default settings")
    (let [service-name (rand-name)]
      (is (thrown-with-msg? IOException #"Premature EOF"
                            (perform-streaming-timeout-test
                              waiter-url
                              (* 40 1000 1000)
                              service-name
                              (constantly nil)
                              (fn [streaming-timeout-ms]
                                (* streaming-timeout-ms 8))
                              (constantly 0)
                              (fn [data-size-in-bytes] (quot data-size-in-bytes (* 4 1000)))
                              (fn [total-bytes-read data-size-in-bytes]
                                (is (< total-bytes-read data-size-in-bytes)))))))))

(deftest ^:parallel ^:integration-fast test-successful-streaming-with-custom-settings
  (testing-using-waiter-url
    (log/info "Streaming test success with default settings")
    (let [service-name (rand-name)]
      (perform-streaming-timeout-test
        waiter-url
        (* 40 1000 1000)
        service-name
        (fn [streaming-timeout-ms]
          (int (* 2 streaming-timeout-ms)))
        (fn [streaming-timeout-ms]
          streaming-timeout-ms)
        (constantly 1)
        (fn [data-size-in-bytes] (quot data-size-in-bytes (* 4 1000)))
        (fn [total-bytes-read data-size-in-bytes]
          (is (= total-bytes-read data-size-in-bytes)))))))

; Marked explicit due to:
; FAIL in (test-streaming-timeout-with-custom-settings)
; expected: (< total-bytes-read data-size-in-bytes)
;   actual: (not (< 40000000 40000000))
(deftest ^:parallel ^:integration-slow ^:explicit test-streaming-timeout-with-custom-settings
  (testing-using-waiter-url
    (log/info "Streaming test timeout with default settings")
    (let [service-name (rand-name)]
      (is (thrown-with-msg? IOException #"Premature EOF"
                            (perform-streaming-timeout-test
                              waiter-url
                              (* 40 1000 1000)
                              service-name
                              (fn [streaming-timeout-ms]
                                (int (* 0.75 streaming-timeout-ms)))
                              (fn [streaming-timeout-ms]
                                (* streaming-timeout-ms 6))
                              (constantly 0)
                              (fn [data-size-in-bytes] (quot data-size-in-bytes (* 4 1000)))
                              (fn [total-bytes-read data-size-in-bytes]
                                (is (< total-bytes-read data-size-in-bytes)))))))))
