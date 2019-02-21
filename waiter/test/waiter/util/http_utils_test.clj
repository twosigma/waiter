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
            [waiter.util.http-utils :refer :all]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (org.eclipse.jetty.http HttpVersion)))

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
                                     (async/>!! response-chan {:body body-chan, :status 200})
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
                                     (async/>!! response-chan {:body body-chan, :status 400})
                                     response-chan))]
        (is (= expected-body (http-request http-client "some-url" :throw-exceptions false))))))

  (testing "non-2XX-response-with-throw-exceptions"
    (let [http-client (Object.)]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (utils/clj->json {:error :response}))
                                     (async/>!! response-chan {:body body-chan, :status 400})
                                     response-chan))]
        (try
          (http-request http-client "some-url")
          (is false "exception not thrown")
          (catch ExceptionInfo ex
            (is (= {:body "{\"error\":\"response\"}" :status 400} (ex-data ex)))))))))

(deftest test-determine-backend-protocol-version
  (doseq [client-protocol ["HTTP/0.9" "HTTP/1.0" "HTTP/1.1" "HTTP/2.0"]]
    (is (= "HTTP/2.0" (determine-backend-protocol-version "h2c" client-protocol)))
    (is (= (if (= "HTTP/2.0" client-protocol) "HTTP/1.1" client-protocol)
           (determine-backend-protocol-version "h2" client-protocol))))
  (doseq [client-protocol ["HTTP/0.9" "HTTP/1.0" "HTTP/1.1"]]
    (is (= client-protocol (determine-backend-protocol-version "http" client-protocol)))
    (is (= client-protocol (determine-backend-protocol-version "https" client-protocol))))
  (is (= "HTTP/1.1" (determine-backend-protocol-version "http" "HTTP/2.0")))
  (is (= "HTTP/1.1" (determine-backend-protocol-version "https" "HTTP/2.0"))))

(deftest test-backend-proto->scheme
  (is (= "http" (backend-proto->scheme "http")))
  (is (= "http" (backend-proto->scheme "h2c")))
  (is (= "https" (backend-proto->scheme "https")))
  (is (= "h2" (backend-proto->scheme "h2")))
  (is (= "ws" (backend-proto->scheme "ws")))
  (is (= "wss" (backend-proto->scheme "wss")))
  (is (= "zzz" (backend-proto->scheme "zzz"))))

(deftest test-protocol->http-version
  (is (nil? (protocol->http-version nil)))
  (is (nil? (protocol->http-version "")))
  (is (nil? (protocol->http-version "Http/0.9")))
  (is (= HttpVersion/HTTP_0_9 (protocol->http-version "HTTP/0.9")))
  (is (nil? (protocol->http-version "http/1.0")))
  (is (= HttpVersion/HTTP_1_0 (protocol->http-version "HTTP/1.0")))
  (is (= HttpVersion/HTTP_1_1 (protocol->http-version "HTTP/1.1")))
  (is (= HttpVersion/HTTP_2 (protocol->http-version "HTTP/2.0")))
  (is (nil? (protocol->http-version "HTTP/2.1")))
  (is (nil? (protocol->http-version "HTTP/3.0"))))
