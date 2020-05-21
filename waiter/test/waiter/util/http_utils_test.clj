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
(ns waiter.util.http-utils-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [qbits.jet.client.http :as http]
            [waiter.status-codes :refer :all]
            [waiter.util.http-utils :refer :all]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-http-request
  (testing "successful-response"
    (let [http-client (Object.)
          expected-body {:foo "bar"
                         :fee {:fie {:foe "fum"}}
                         :solve 42}]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (utils/clj->json expected-body))
                                     (async/>!! response-chan {:body body-chan, :status http-200-ok})
                                     response-chan))]
        (is (= expected-body (http-request http-client "some-url"))))))

  (testing "error-in-response"
    (let [http-client (Object.)
          expected-exception (IllegalStateException. "Test Exception")]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)]
                                     (async/>!! response-chan {:error expected-exception})
                                     response-chan))]
        (is (thrown-with-msg? IllegalStateException #"Test Exception"
                              (http-request http-client "some-url"))))))

  (testing "non-2XX-response-without-throw-exceptions"
    (let [http-client (Object.)
          expected-body {:foo "bar"
                         :fee {:fie {:foe "fum"}}
                         :solve 42}]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (utils/clj->json expected-body))
                                     (async/>!! response-chan {:body body-chan, :status http-400-bad-request})
                                     response-chan))]
        (is (= expected-body (http-request http-client "some-url" :throw-exceptions false))))))

  (testing "non-2XX-response-with-throw-exceptions"
    (let [http-client (Object.)]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (utils/clj->json {:error :response}))
                                     (async/>!! response-chan {:body body-chan, :status http-400-bad-request})
                                     response-chan))]
        (try
          (http-request http-client "some-url")
          (is false "exception not thrown")
          (catch ExceptionInfo ex
            (is (= {:body {:error "response"}
                    :status http-400-bad-request}
                   (ex-data ex)))))))))

(deftest test-backend-protocol->http-version
  (is (= "HTTP/2.0" (backend-protocol->http-version "h2")))
  (is (= "HTTP/2.0" (backend-protocol->http-version "h2c")))
  (is (= "HTTP/1.1" (backend-protocol->http-version "http")))
  (is (= "HTTP/1.1" (backend-protocol->http-version "https"))))

(deftest test-backend-proto->scheme
  (is (= "http" (backend-proto->scheme "http")))
  (is (= "http" (backend-proto->scheme "h2c")))
  (is (= "https" (backend-proto->scheme "https")))
  (is (= "https" (backend-proto->scheme "h2")))
  (is (= "ws" (backend-proto->scheme "ws")))
  (is (= "wss" (backend-proto->scheme "wss")))
  (is (= "zzz" (backend-proto->scheme "zzz"))))

(deftest test-grpc?
  (is (grpc? {"content-type" "application/grpc"} "HTTP/2.0"))
  (is (not (grpc? {"content-type" "application/grpc"} "HTTP/1.1")))
  (is (not (grpc? {"content-type" "application/grpc"} nil)))
  (is (not (grpc? nil "HTTP/2.0")))
  (is (not (grpc? {"content-type" "application/xml"} "HTTP/2.0")))
  (is (not (grpc? {"content-type" "text/grpc"} "HTTP/2.0")))
  (is (not (grpc? {"content-type" "text/grpc"} "HTTP/1.1")))
  (is (not (grpc? {"accept" "application/grpc"} "HTTP/2.0"))))

(deftest test-service-unavailable?
  (is (service-unavailable? {:client-protocol "HTTP/0.9"} {:status http-503-service-unavailable}))
  (is (service-unavailable? {:client-protocol "HTTP/1.0"} {:status http-503-service-unavailable}))
  (is (service-unavailable? {:client-protocol "HTTP/1.1"} {:status http-503-service-unavailable}))
  (is (service-unavailable? {:client-protocol "HTTP/2.0"} {:status http-503-service-unavailable}))

  (is (not (service-unavailable? {:client-protocol "HTTP/0.9"} {:status http-502-bad-gateway})))
  (is (not (service-unavailable? {:client-protocol "HTTP/1.0"} {:status http-502-bad-gateway})))
  (is (not (service-unavailable? {:client-protocol "HTTP/1.1"} {:status http-502-bad-gateway})))
  (is (not (service-unavailable? {:client-protocol "HTTP/2.0"} {:status http-502-bad-gateway})))

  (is (not (service-unavailable? {:client-protocol "HTTP/0.9"} {:status http-401-unauthorized})))
  (is (not (service-unavailable? {:client-protocol "HTTP/1.0"} {:status http-401-unauthorized})))
  (is (not (service-unavailable? {:client-protocol "HTTP/1.1"} {:status http-401-unauthorized})))
  (is (not (service-unavailable? {:client-protocol "HTTP/2.0"} {:status http-401-unauthorized})))

  (is (service-unavailable?
        {:client-protocol "HTTP/2.0" :headers {"content-type" "application/grpc"}}
        {:headers {"grpc-status" "14"} :status http-200-ok}))
  (is (service-unavailable?
        {:client-protocol "HTTP/2.0" :headers {"content-type" "application/grpc"}}
        {:headers {"grpc-status" "14"} :status http-500-internal-server-error}))
  (is (not (service-unavailable?
             {:client-protocol "HTTP/2.0" :headers {"content-type" "application/grpc"}}
             {:headers {"grpc-status" "12"} :status http-200-ok})))
  (is (not (service-unavailable?
             {:client-protocol "HTTP/2.0" :headers {"content-type" "application/xml"}}
             {:headers {"grpc-status" "14"} :status http-200-ok})))
  (is (not (service-unavailable?
             {:client-protocol "HTTP/1.1" :headers {"content-type" "application/grpc"}}
             {:headers {"grpc-status" "14"} :status http-200-ok})))
  (is (not (service-unavailable?
             {:client-protocol "HTTP/2.0" :headers {"content-type" "application/grpc"}}
             {:headers {"grpc-status" "13"} :status http-200-ok})))
  (is (not (service-unavailable?
             {:client-protocol "HTTP/2.0" :headers {"content-type" "application/grpc"}}
             {:headers {"grpc-status" 14} :status http-200-ok}))))
