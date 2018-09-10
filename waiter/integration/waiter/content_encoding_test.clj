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
(ns waiter.content-encoding-test
  (:require [clj-http.client :as clj-http]
            [clj-http.conn-mgr :as conn-mgr]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [qbits.jet.client.http :as http]
            [waiter.util.client-tools :refer :all])
  (:import (java.io EOFException InputStreamReader)
           (org.apache.http ConnectionClosedException)))

(deftest ^:parallel ^:integration-fast test-support-success-chunked-gzip-response
  (testing-using-waiter-url
    (log/info "Test successful chunked gzip response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testgzipsuccesschunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [response-size 2000000
              fail-after-size 8000000
              req-headers (assoc service-headers
                            :x-waiter-debug true
                            :x-kitchen-chunked true
                            :x-kitchen-response-size response-size
                            :x-kitchen-fail-after fail-after-size)
              {:keys [headers] :as response} (make-request waiter-url "/gzip" :headers req-headers)]
          (assert-response-status response 200)
          (is (= (get headers "content-type") "text/plain"))
          (is (= (get headers "content-encoding") "gzip"))
          ;; ideally (is (= (get headers "Transfer-Encoding") "chunked"))
          (is (nil? (get headers "Content-Length")))
          (is (not (nil? (get headers "x-cid"))))
          (let [{:keys [body] :as response}
                (make-request waiter-url "/gzip" :headers req-headers :decompress-body true :verbose true)
                body-length (count (bytes (byte-array (map (comp byte int) (str body)))))]
            (assert-response-status response 200)
            (is (= response-size body-length))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-support-failed-chunked-gzip-response
  (testing-using-waiter-url
    (log/info "Test failed chunked gzip response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testgzipfailedchunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [req-headers (assoc service-headers
                            :x-waiter-debug true
                            :x-kitchen-chunked true
                            :x-kitchen-response-size 1024
                            :x-kitchen-fail-after 50)
              url (str HTTP-SCHEME waiter-url "/gzip")]
          (is (thrown?
                EOFException
                (clj-http/get url {:headers req-headers
                                   :decompress-body true
                                   :spnego-auth use-spnego}))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-support-success-gzip-response
  (testing-using-waiter-url
    (log/info "Test successful gzip response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testgzipsuccessunchunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [response-size 2000000
              fail-after-size 8000000
              req-headers (assoc service-headers
                            :x-waiter-debug true
                            :x-kitchen-chunked false
                            :x-kitchen-response-size response-size
                            :x-kitchen-fail-after fail-after-size)
              {:keys [headers] :as response} (make-request waiter-url "/gzip" :headers req-headers :verbose true)]
          (assert-response-status response 200)
          (is (= (get headers "content-type") "text/plain"))
          (is (= (get headers "content-encoding") "gzip"))
          (is (nil? (get headers "Transfer-Encoding")))
          (is (not (nil? (get headers "content-length"))))
          (is (not (nil? (get headers "x-cid"))))
          (let [{:keys [body] :as response}
                (make-request waiter-url "/gzip" :headers req-headers :decompress-body true :verbose true)
                body-length (count (bytes (byte-array (map (comp byte int) (str body)))))]
            (assert-response-status response 200)
            (is (= response-size body-length))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-support-failed-gzip-response
  (testing-using-waiter-url
    (log/info "Test failed gzip response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testgzipfailedunchunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [req-headers (assoc service-headers
                            :x-waiter-debug true
                            :x-kitchen-chunked false
                            :x-kitchen-response-size 1024
                            :x-kitchen-fail-after 50)]
          (is (thrown?
                ConnectionClosedException
                (clj-http/get (str HTTP-SCHEME waiter-url "/gzip") {:headers req-headers
                                                                    :decompress-body true
                                                                    :spnego-auth use-spnego}))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-support-success-chunked-response
  (testing-using-waiter-url
    (log/info "Test successful chunked plain response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testsuccesschunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [req-headers (assoc service-headers
                            :x-kitchen-response-size 100000
                            :x-kitchen-fail-after 200000)
              {:keys [body headers] :as response} (make-request waiter-url "/chunked" :headers req-headers :verbose true)
              body-length (count (bytes (byte-array (map (comp byte int) (str body)))))]
          (assert-response-status response 200)
          (is (== 100000 body-length))
          (is (= (get headers "content-type") "text/plain"))
          (is (nil? (get headers "Content-Encoding")))
          (is (nil? (get headers "Content-Length")))
          (is (not (nil? (get headers "x-cid")))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-support-failed-chunked-response
  (testing-using-waiter-url
    (log/info "Test truncated chunked plain response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testfailedchunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [req-headers (assoc service-headers
                            :x-kitchen-response-size 100000
                            :x-kitchen-fail-after 5000)
              {:keys [body headers] :as response} (make-request waiter-url "/chunked" :headers req-headers :verbose true)
              body-length (count (bytes (byte-array (map (comp byte int) (str body)))))]
          (assert-response-status response 200)
          (is (< body-length 100000))
          (is (= (get headers "content-type") "text/plain"))
          (is (nil? (get headers "Content-Encoding")))
          (is (nil? (get headers "Content-Length")))
          (is (not (nil? (get headers "x-cid")))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-support-success-unchunked-response
  (testing-using-waiter-url
    (log/info "Test successful plain response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testsuccessunchunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [req-headers (assoc service-headers
                            :x-kitchen-response-size 100000
                            :x-kitchen-fail-after 200000)
              {:keys [body headers] :as response} (make-request waiter-url "/unchunked" :headers req-headers :verbose true)
              body-length (count (bytes (byte-array (map (comp byte int) (str body)))))]
          (assert-response-status response 200)
          (is (== 100000 body-length))
          (is (= (get headers "content-type") "text/plain"))
          (is (nil? (get headers "Content-Encoding")))
          (is (not (nil? (get headers "content-length"))))
          (is (not (nil? (get headers "x-cid")))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-support-failed-unchunked-response
  (testing-using-waiter-url
    (log/info "Test truncated failed plain response")
    (let [service-headers (assoc (kitchen-request-headers) :x-waiter-name (rand-name "testfailedunchunked"))
          {:keys [service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))]
      (try
        (let [req-headers (assoc service-headers
                            :x-kitchen-response-size 100000
                            :x-kitchen-fail-after 5000)]
          (is (thrown? ConnectionClosedException
                       (clj-http/get (str HTTP-SCHEME waiter-url "/unchunked") {:headers req-headers
                                                                                :spnego-auth use-spnego}))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-terminate-chunked-request
  (testing-using-waiter-url
    (let [data-length 1200000
          chunk-size 1000
          chunk-delay 100 ; wait 100 ms between each chunk, entire stream takes ~2 minutes
          headers (merge (kitchen-request-headers) {:x-waiter-name (rand-name "testterminatechunkedrequest")
                                                    ; ensure that each router has at least two slots available
                                                    :x-waiter-concurrency-level 20
                                                    :x-kitchen-chunk-delay chunk-delay
                                                    :x-kitchen-chunk-size chunk-size
                                                    :x-kitchen-response-size data-length})
          get-state (fn []
                      (let [{:keys [body]} (make-request waiter-url "/kitchen-state" :headers headers)]
                        (log/info "Body returned from /kitchen-state:" body)
                        (json/read-str body)))
          connection-manager (conn-mgr/make-regular-conn-manager {})
          {:keys [body service-id]}
          (make-request-with-debug-info headers
                                        #(clj-http/get (str "http://" waiter-url "/chunked")
                                                       {:headers (clojure.walk/stringify-keys %)
                                                        :suppress-connection-close true
                                                        :decompress-body false
                                                        :spnego-auth true
                                                        :as :stream
                                                        :connection-manager connection-manager}))]
      (is (= 1 (get (get-state) "pending-http-requests")))
      ; Client eagerly terminates the request
      (.shutdown connection-manager)
      ; Wait up to 10 seconds for the connection to get cleaned up. If waiter consumes the entire stream it will take
      ; much longer.
      (is (wait-for #(= 0 (get (get-state) "pending-http-requests")) :interval 1 :timeout 10))
      (let [body-data (-> body (InputStreamReader.) slurp)]
        (is (not= data-length (-> body-data str count)) "Waiter streamed entire response!"))
      (delete-service waiter-url service-id))))
