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
(ns waiter.streaming-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all])
  (:import (java.net HttpURLConnection URL)))

(deftest ^:parallel ^:integration-fast test-streaming
  (testing-using-waiter-url
    (let [file-path "test-files/big.txt"
          service-name (rand-name)
          post-body (slurp file-path)
          body-size (count (.getBytes post-body "utf-8"))
          metrics-sync-interval-ms (get-in (waiter-settings waiter-url) [:metrics-config :metrics-sync-interval-ms])
          headers {:x-cid (str service-name "-canary")
                   :x-kitchen-echo "true"
                   :x-waiter-name service-name}
          {:keys [request-headers] :as canary-response} (make-kitchen-request waiter-url headers)
          service-id (retrieve-service-id waiter-url request-headers)
          epsilon 0.01
          num-streaming-requests 50]
      (assert-response-status canary-response 200)
      (dotimes [n num-streaming-requests]
        (let [request-cid (str service-name "-iter" n)
              headers (assoc headers
                        :content-length body-size
                        :content-type "application/octet-stream"
                        :x-cid request-cid)
              {:keys [body] :as response} (make-kitchen-request waiter-url headers :body post-body)]
          (is (= 200 (:status response)) (str "Request correlation-id: " request-cid))
          (is (.equals post-body body) (str response)))) ;; avoids printing the post-body when assertion fails
      ; wait to allow metrics to be aggregated
      (let [sleep-period (max (* 20 metrics-sync-interval-ms) 10000)]
        (log/debug "sleeping for" sleep-period "ms")
        (Thread/sleep sleep-period))
      ; assert request response size metrics
      (let [service-settings (service-settings waiter-url service-id)
            _ (log/info "metrics" (get service-settings :metrics))
            aggregate-metrics (get-in service-settings [:metrics :aggregate])
            request-counts (get-in aggregate-metrics [:counters :request-counts])
            request-size-histogram (get-in aggregate-metrics [:histograms :request-size])
            response-size-histogram (get-in aggregate-metrics [:histograms :response-size])]
        (is (zero? (:outstanding request-counts)) (-> aggregate-metrics))
        (is (zero? (:streaming request-counts)))
        (is (= (inc num-streaming-requests) (:successful request-counts)))
        (is (= (inc num-streaming-requests) (:total request-counts)))
        ;; there is no content length on the canary request
        (is (= num-streaming-requests (get request-size-histogram :count 0))
            (str "request-size-histogram: " request-size-histogram))
        (is (->> (get-in request-size-histogram [:value :1.0] 0.0) (- body-size) Math/abs (>= epsilon))
            (str "request-size-histogram: " request-size-histogram))
        (is (= (inc num-streaming-requests) (get response-size-histogram :count))
            (str "response-size-histogram: " response-size-histogram))
        ;; all (canary and streaming) response sizes are logged
        (is (->> (get-in response-size-histogram [:value :1.0] 0.0) (- body-size) Math/abs (>= epsilon))
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
        ^HttpURLConnection url-connection (-> request-url (URL.) (.openConnection))
        streaming-timeout-ms (get-in (waiter-settings waiter-url) [:instance-request-properties :streaming-timeout-ms])
        streaming-timeout-limit-ms (streaming-timeout-limit-fn streaming-timeout-ms)
        kitchen-request-headers (merge (kitchen-request-headers)
                                       {:x-cid correlation-id
                                        :x-kitchen-response-size data-size-in-bytes
                                        :x-kitchen-chunk-size (chunk-size-fn data-size-in-bytes)
                                        :x-kitchen-chunk-delay (chunk-delay-fn streaming-timeout-ms)
                                        :x-waiter-debug true
                                        :x-waiter-name service-name})]
    (doto url-connection
      (.setRequestMethod "POST")
      (.setUseCaches false)
      (.setDoInput true)
      (.setDoOutput true))
    (doseq [[key value] kitchen-request-headers]
      (.setRequestProperty url-connection (name key) (str value)))
    (when-let [request-streaming-timeout (streaming-timeout-fn streaming-timeout-ms)]
      (.setRequestProperty url-connection "x-waiter-streaming-timeout" (str request-streaming-timeout)))
    (let [service-id (-> url-connection
                         (.getHeaderField "x-waiter-backend-id")
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
