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
            [clojure.string :as str]
            [clojure.test :refer :all]
            [metrics.counters :as counters]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [waiter.core :refer :all]
            [waiter.descriptor :as descriptor]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.process-request :refer :all]
            [waiter.statsd :as statsd]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.util.utils :as utils])
  (:import (java.io ByteArrayOutputStream IOException)
           (java.net ConnectException SocketTimeoutException)
           (java.util.concurrent TimeoutException)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.io EofException)
           (org.eclipse.jetty.websocket.api UpgradeException)))

(deftest test-prepare-request-properties
  (let [test-cases (list
                     {:name "test-prepare-request-properties:nil-inputs"
                      :input {:request-properties nil :waiter-headers nil}
                      :expected {:async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:empty-nil-inputs"
                      :input {:request-properties {} :waiter-headers nil}
                      :expected {:async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:nil-empty-inputs"
                      :input {:request-properties nil :waiter-headers {}}
                      :expected {:async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:empty-inputs"
                      :input {:request-properties {} :waiter-headers {}}
                      :expected {:async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:missing-timeout-header-1"
                      :input {:request-properties {:fie "foe"} :waiter-headers {:foo "bar"}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:missing-timeout-header-2"
                      :input {:request-properties {:fie "foe" :initial-socket-timeout-ms 100 :streaming-timeout-ms 200}
                              :waiter-headers {:foo "bar"}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 100 :queue-timeout-ms nil :streaming-timeout-ms 200}
                      }
                     {:name "test-prepare-request-properties:invalid-timeout-header"
                      :input {:request-properties {:fie "foe" :initial-socket-timeout-ms 100 :queue-timeout-ms 150 :streaming-timeout-ms nil}
                              :waiter-headers {"timeout" "bar"}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 100 :queue-timeout-ms 150 :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:negative-timeout-header"
                      :input {:request-properties {:fie "foe" :initial-socket-timeout-ms 100}
                              :waiter-headers {"timeout" -50}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 100 :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-timeout-header"
                      :input {:request-properties {:fie "foe" :initial-socket-timeout-ms 100 :streaming-timeout-ms 200}
                              :waiter-headers {"timeout" 50 "streaming-timeout" 250}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 50 :queue-timeout-ms nil :streaming-timeout-ms 250}
                      }
                     {:name "test-prepare-request-properties:invalid-queue-timeout-header"
                      :input {:request-properties {:fie "foe" :queue-timeout-ms 150}
                              :waiter-headers {"queue-timeout" "bar"}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms 150 :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:negative-queue-timeout-header"
                      :input {:request-properties {:fie "foe" :queue-timeout-ms 150}
                              :waiter-headers {"queue-timeout" -50}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms 150 :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-queue-timeout-header"
                      :input {:request-properties {:fie "foe" :queue-timeout-ms 150 :streaming-timeout-ms nil}
                              :waiter-headers {"queue-timeout" 50}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms nil :queue-timeout-ms 50 :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-both-timeout-headers"
                      :input {:request-properties {:fie "foe" :initial-socket-timeout-ms 100 :queue-timeout-ms 150}
                              :waiter-headers {"queue-timeout" 50 "timeout" 75}}
                      :expected {:fie "foe" :async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 75 :queue-timeout-ms 50 :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:invalid-async-headers"
                      :input {:request-properties {:fie "foe" :async-check-interval-ms 100 :async-request-timeout-ms 200}
                              :waiter-headers {"async-check-interval" -50 "async-request-timeout" "foo"}}
                      :expected {:fie "foe" :async-check-interval-ms 100 :async-request-timeout-ms 200 :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:valid-async-headers"
                      :input {:request-properties {:fie "foe" :async-check-interval-ms 100 :async-request-timeout-ms 200}
                              :waiter-headers {"async-check-interval" 50 "async-request-timeout" 250}}
                      :expected {:fie "foe" :async-check-interval-ms 50 :async-request-timeout-ms 250 :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:too-large-async-request-timeout-header"
                      :input {:request-properties {:fie "foe" :async-check-interval-ms 100 :async-request-max-timeout-ms 100000 :async-request-timeout-ms 200}
                              :waiter-headers {"async-request-timeout" 101000}}
                      :expected {:fie "foe" :async-check-interval-ms 100 :async-request-max-timeout-ms 100000 :async-request-timeout-ms 100000 :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:too-large-async-request-timeout-header"
                      :input {:request-properties {:fie "foe" :async-check-interval-ms 100 :async-request-max-timeout-ms 100000 :async-request-timeout-ms 200}
                              :waiter-headers {"async-request-timeout" 101000}}
                      :expected {:fie "foe" :async-check-interval-ms 100 :async-request-max-timeout-ms 100000 :async-request-timeout-ms 100000 :initial-socket-timeout-ms nil :queue-timeout-ms nil :streaming-timeout-ms nil}
                      }
                     {:name "test-prepare-request-properties:empty-request-properties-with-token-defaults"
                      :input {:request-properties {:initial-socket-timeout-ms 100 :queue-timeout-ms 150 :streaming-timeout-ms nil}
                              :token-metadata {"queue-timeout-ms" 300000 "socket-timeout-ms" 900000 "streaming-timeout-ms" 20000}
                              :waiter-headers nil}
                      :expected {:async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 900000 :queue-timeout-ms 300000 :streaming-timeout-ms 20000}
                      }
                     {:name "test-prepare-request-properties:empty-request-properties-with-token-defaults-and-some-headers"
                      :input {:request-properties {:initial-socket-timeout-ms 100 :queue-timeout-ms 150 :streaming-timeout-ms nil}
                              :token-metadata {"queue-timeout-ms" 300000 "socket-timeout-ms" 900000 "streaming-timeout-ms" 20000}
                              :waiter-headers {"queue-timeout" 50 "timeout" 75}}
                      :expected {:async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 75 :queue-timeout-ms 50 :streaming-timeout-ms 20000}
                      }
                     {:name "test-prepare-request-properties:empty-request-properties-with-token-defaults-and-most-timeout-headers"
                      :input {:request-properties {:initial-socket-timeout-ms 100 :queue-timeout-ms 150 :streaming-timeout-ms nil}
                              :token-metadata {"queue-timeout-ms" 300000 "socket-timeout-ms" 900000 "streaming-timeout-ms" 20000}
                              :waiter-headers {"queue-timeout" 50 "streaming-timeout" 95 "timeout" 75}}
                      :expected {:async-check-interval-ms nil :async-request-timeout-ms nil :initial-socket-timeout-ms 75 :queue-timeout-ms 50 :streaming-timeout-ms 95}
                      })]
    (doseq [{:keys [name input expected]} test-cases]
      (testing (str "Test " name)
        (let [{:keys [request-properties token-metadata waiter-headers]} input
              waiter-headers' (pc/map-keys #(str headers/waiter-header-prefix %) waiter-headers)
              actual (prepare-request-properties request-properties waiter-headers' token-metadata)]
          (is (= expected actual)))))))

(deftest test-prepare-grpc-compliant-request-properties
  (let [request-properties-base {:async-check-interval-ms 1010
                                 :async-request-timeout-ms 1020
                                 :initial-socket-timeout-ms 1030
                                 :queue-timeout-ms 1040
                                 :streaming-timeout-ms 1050}]
    (doseq [{:keys [backend-proto request-headers request-properties-expected token-metadata] :as test-case}
            [{:backend-proto "h2c"
              :request-headers {"x-waiter-async-check-interval" "2100"
                                "x-waiter-async-request-timeout" "2200"
                                "x-waiter-timeout" "2300"
                                "x-waiter-queue-timeout" "2400"
                                "x-waiter-streaming-timeout" "2500"}
              :request-properties-expected {:async-request-timeout-ms 2200
                                            :async-check-interval-ms 2100
                                            :initial-socket-timeout-ms 2300
                                            :queue-timeout-ms 2400
                                            :streaming-timeout-ms 2500}}
             {:backend-proto "h2c"
              :request-headers {}
              :request-properties-expected {}}
             {:backend-proto "h2c"
              :request-headers {"x-waiter-queue-timeout" "2000"}
              :request-properties-expected {:queue-timeout-ms 2000}}
             {:backend-proto "h2c"
              :request-headers {"grpc-timeout" "3S"}
              :request-properties-expected {:queue-timeout-ms 1040}}
             {:backend-proto "h2c"
              :request-headers {"grpc-timeout" "3S"
                                "x-waiter-queue-timeout" "2000"}
              :request-properties-expected {:queue-timeout-ms 2000}}
             {:backend-proto "http"
              :request-headers {"content-type" "application/grpc"
                                "grpc-timeout" "3S"}
              :request-properties-expected {:queue-timeout-ms 1040}}
             {:backend-proto "h2c"
              :request-headers {"content-type" "application/grpc"
                                "grpc-timeout" "3S"}
              :request-properties-expected {:queue-timeout-ms 3100}}
             {:backend-proto "h2c"
              :request-headers {"content-type" "application/grpc"
                                "grpc-timeout" "3S"
                                "x-waiter-queue-timeout" "2000"}
              :request-properties-expected {:queue-timeout-ms 2000}}
             {:backend-proto "h2c"
              :request-headers {"content-type" "application/grpc"
                                "grpc-timeout" "3S"
                                "x-waiter-queue-timeout" "2000"}
              :request-properties-expected {:initial-socket-timeout-ms 900000
                                            :queue-timeout-ms 2000
                                            :streaming-timeout-ms 20000}
              :token-metadata {"queue-timeout-ms" 300000
                               "socket-timeout-ms" 900000
                               "streaming-timeout-ms" 20000}}]]
      (let [{:keys [passthrough-headers waiter-headers]} (headers/split-headers request-headers)
            request-properties-actual  (prepare-grpc-compliant-request-properties
                                         request-properties-base backend-proto passthrough-headers waiter-headers token-metadata)]
        (is (= (merge request-properties-base request-properties-expected) request-properties-actual) (str test-case))))))

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
          response confirm-live-connection request-abort-callback false resp-chan instance-request-properties
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
  [bytes skip-streaming?]
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
        (when skip-streaming?
          (Thread/sleep 100)
          (async/close! resp-chan))
        (stream-http-response response confirm-live-connection request-abort-callback skip-streaming? resp-chan
                              instance-request-properties reservation-status-promise request-state-chan metric-group
                              waiter-debug-enabled? (metrics/stream-metric-map service-id))
        (loop []
          (let [message (async/<!! resp-chan)]
            (when message
              (recur))))))
    @bytes-reported))

(deftest test-stream-http-response
  (testing "Streaming the response"
    (testing "skipping should stream no bytes"
      (let [bytes-reported (stream-response 2048 true)]
        (is (zero? (count bytes-reported)))))

    (testing "should throttle calls to statsd to report bytes streamed"
      (let [bytes-reported (stream-response 1 false)]
        (is (= 1 (count bytes-reported)))
        (is (= [1] bytes-reported)))

      (let [bytes-reported (stream-response (* 1024 977) false)]
        (is (= 1 (count bytes-reported)))
        (is (= [1000448] bytes-reported)))

      (let [bytes-reported (stream-response (-> 1024 (* 977) (inc)) false)]
        (is (= 2 (count bytes-reported)))
        (is (= [1000448 1] bytes-reported)))

      (let [bytes-reported (stream-response 10000000 false)]
        (is (= 10 (count bytes-reported)))
        (is (= 10000000 (reduce + bytes-reported)))
        (is (= [1000448 1000448 1000448 1000448 1000448 1000448 1000448 1000448 1000448 995968] bytes-reported))))))

(deftest test-extract-async-request-response-data
  (testing "202-missing-location-header"
    (is (nil?
          (extract-async-request-response-data
            {:status http-202-accepted :headers {}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "200-not-async"
    (is (nil?
          (extract-async-request-response-data
            {:status http-200-ok, :headers {"location" "/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "303-not-async"
    (is (nil?
          (extract-async-request-response-data
            {:status http-303-see-other, :headers {"location" "/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "404-not-async"
    (is (nil?
          (extract-async-request-response-data
            {:status http-404-not-found, :headers {"location" "/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "202-absolute-location"
    (is (= {:location "/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status http-202-accepted :headers {"location" "/result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-relative-location-1"
    (is (= {:location "/query/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status http-202-accepted :headers {"location" "../result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-relative-location-2"
    (is (= {:location "/query/for/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status http-202-accepted :headers {"location" "result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-relative-location-two-levels"
    (is (= {:location "/result/location" :query-string "p=q&r=s|t"}
           (extract-async-request-response-data
             {:status http-202-accepted :headers {"location" "../../result/location?p=q&r=s|t"}}
             "http://www.example.com:1234/query/for/status?u=v&w=x|y|z"))))
  (testing "202-absolute-url-same-host-port"
    (is (= {:location "/retrieve/result/location" :query-string nil}
           (extract-async-request-response-data
             {:status http-202-accepted :headers {"location" "http://www.example.com:1234/retrieve/result/location"}}
             "http://www.example.com:1234/query/for/status"))))
  (testing "202-absolute-url-different-host"
    (is (nil?
          (extract-async-request-response-data
            {:status http-202-accepted :headers {"location" "http://www.example2.com:1234/retrieve/result/location"}}
            "http://www.example.com:1234/query/for/status"))))
  (testing "202-absolute-url-different-port"
    (is (nil?
          (extract-async-request-response-data
            {:status http-202-accepted :headers {"location" "http://www.example.com:5678/retrieve/result/location"}}
            "http://www.example.com:1234/query/for/status")))))

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
                             "cookie" "$Version=1; Skin=new; x-waiter-auth=foo"
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
        app-password "test-password"]
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
                                                   (assoc "cookie" "$Version=1; Skin=new")
                                                   (dissoc "expect" "authorization"
                                                           "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                                                           "te" "trailers" "transfer-encoding" "upgrade")
                                                   (merge {"x-waiter-auth-principal" "test-user"
                                                           "x-waiter-authenticated-principal" "test-user@test.com"}))
                                               (:headers request-config)))
                                        (is (= proto-version (:version request-config)))))]
      (testing "make-request:headers:HTTP/1.0"
        (let [proto-version "HTTP/1.0"
              request-method-fn-call-counter (atom 0)]
          (with-redefs [http/request (http-request-mock-factory passthrough-headers request-method-fn-call-counter proto-version)]
            (make-request http-clients make-basic-auth-fn service-id->password-fn instance request request-properties
                          passthrough-headers end-route nil backend-proto proto-version)
            (is (= 1 @request-method-fn-call-counter)))))

      (testing "make-request:headers:HTTP/2.0"
        (let [proto-version "HTTP/2.0"
              request-method-fn-call-counter (atom 0)]
          (with-redefs [http/request (http-request-mock-factory passthrough-headers request-method-fn-call-counter proto-version)]
            (make-request http-clients make-basic-auth-fn service-id->password-fn instance request request-properties
                          passthrough-headers end-route nil backend-proto proto-version)
            (is (= 1 @request-method-fn-call-counter)))))

      (testing "make-request:headers-long-content-length"
        (let [proto-version "HTTP/1.0"
              request-method-fn-call-counter (atom 0)
              passthrough-headers (assoc passthrough-headers "content-length" "1234123412341234")
              statsd-inc-call-value (promise)]
          (with-redefs [http/request (http-request-mock-factory passthrough-headers request-method-fn-call-counter proto-version)
                        statsd/inc!
                        (fn [metric-group metric value]
                          (is (nil? metric-group))
                          (is (= "request_content_length" metric))
                          (deliver statsd-inc-call-value value))]
            (make-request http-clients make-basic-auth-fn service-id->password-fn instance request request-properties
                          passthrough-headers end-route nil backend-proto proto-version)
            (is (= 1 @request-method-fn-call-counter))
            (is (= 1234123412341234 (deref statsd-inc-call-value 0 :statsd-inc-not-called)))))))))

(deftest test-wrap-suspended-service
  (testing "returns error for suspended app"
    (let [handler (wrap-suspended-service (fn [_] {:status http-200-ok}))
          request {:descriptor {:service-id "service-id-1"
                                :suspended-state {:suspended true
                                                  :last-updated-by "test-user"
                                                  :time (t/now)}}}
          {:keys [status body]} (handler request)]
      (is (= http-503-service-unavailable status))
      (is (str/includes? body "Service has been suspended"))
      (is (str/includes? body "test-user"))))

  (testing "passes apps by default"
    (let [handler (wrap-suspended-service (fn [_] {:status http-200-ok}))
          request {}
          {:keys [status]} (handler request)]
      (is (= http-200-ok status)))))

(deftest test-wrap-maintenance-mode
  (testing "returns 503 for token in maintenance mode with custom message"
    (let [handler (wrap-maintenance-mode (fn [_] {:status http-200-ok}))
          maintenance-message "test maintenance message"
          request {:waiter-discovery {:token-metadata {"maintenance" {"message" maintenance-message}}
                                      :token "token"
                                      :waiter-headers {}}}
          {:keys [status body]} (handler request)]
      (is (= http-503-service-unavailable status))
      (is (str/includes? body maintenance-message))))

  (testing "passes apps by default"
    (let [handler (wrap-maintenance-mode (fn [_] {:status http-200-ok}))
          request {:waiter-discovery {:token-metadata {}
                                      :token "token"
                                      :waiter-headers {}}}
          {:keys [status]} (handler request)]
      (is (= http-200-ok status)))))

(deftest test-wrap-maintenance-mode-acceptor
  (testing "returns 503 for token in maintenance mode with custom message"
    (let [handler (wrap-maintenance-mode-acceptor (fn [_] (is false "Not supposed to call this handler") true))
          maintenance-message "test maintenance message"
          upgrade-response (reified-upgrade-response)
          request {:upgrade-response upgrade-response
                   :waiter-discovery {:token-metadata {"maintenance" {"message" maintenance-message}}
                                      :token "token"
                                      :waiter-headers {}}}
          response-status (handler request)]
      (is (= http-503-service-unavailable response-status))
      (is (= http-503-service-unavailable (.getStatusCode upgrade-response)))
      (is (str/includes? (.getStatusReason upgrade-response) maintenance-message))))

  (testing "passes apps by default"
    (let [handler (wrap-maintenance-mode-acceptor (fn [_] true))
          request {:waiter-discovery {:token-metadata {}
                                      :token "token"
                                      :waiter-headers {}}}
          success? (handler request)]
      (is (true? success?)))))

(deftest test-wrap-too-many-requests
  (testing "returns error for too many requests"
    (let [service-id "my-service"
          counter (metrics/service-counter service-id "request-counts" "waiting-for-available-instance")]
      (counters/clear! counter)
      (counters/inc! counter 10)
      (let [handler (wrap-too-many-requests (fn [_] {:status http-200-ok}))
            request {:descriptor {:service-id service-id
                                  :service-description {"max-queue-length" 5}}}
            {:keys [status body]} (handler request)]
        (is (= http-503-service-unavailable status))
        (is (str/includes? body "Max queue length")))))

  (testing "passes service with fewer requests"
    (let [service-id "ok-service"
          counter (metrics/service-counter service-id "request-counts" "waiting-for-available-instance")]
      (counters/clear! counter)
      (counters/inc! counter 3)
      (let [handler (wrap-too-many-requests (fn [_] {:status http-200-ok}))
            request {:descriptor {:service-id service-id
                                  :service-description {"max-queue-length" 10}}}
            {:keys [status]} (handler request)]
        (is (= http-200-ok status))))))

(deftest test-redirect-on-process-error
  (let [request->descriptor-fn (fn [_]
                                 (throw
                                   (ex-info "Test exception" {:type :service-description-error
                                                              :issue {"run-as-user" "missing-required-key"}
                                                              :x-waiter-headers {"queue-length" 100}})))
        service-invocation-authorized? (constantly true)
        start-new-service-fn (constantly nil)
        fallback-state-atom (atom {})
        handler (descriptor/wrap-descriptor (fn [_] {:status http-200-ok}) request->descriptor-fn service-invocation-authorized?
                                            start-new-service-fn fallback-state-atom utils/exception->response)]
    (testing "with-query-params"
      (let [request {:headers {"host" "www.example.com:1234"}, :query-string "a=b&c=d", :uri "/path"}
            {:keys [headers status]} (handler request)]
        (is (= http-303-see-other status))
        (is (= "/waiter-consent/path?a=b&c=d" (get headers "location")))))

    (testing "with-query-params-and-default-port"
      (let [request {:headers {"host" "www.example.com"}, :query-string "a=b&c=d", :uri "/path"}
            {:keys [headers status]} (handler request)]
        (is (= http-303-see-other status))
        (is (= "/waiter-consent/path?a=b&c=d" (get headers "location")))))

    (testing "without-query-params"
      (let [request {:headers {"host" "www.example.com:1234"}, :uri "/path"}
            {:keys [headers status]} (handler request)]
        (is (= http-303-see-other status))
        (is (= "/waiter-consent/path" (get headers "location")))))))

(deftest test-no-redirect-on-process-error
  (let [request->descriptor-fn (fn [_] (throw (Exception. "Exception message")))
        service-invocation-authorized? (constantly true)
        start-new-service-fn (constantly nil)
        fallback-state-atom (atom {})
        handler (descriptor/wrap-descriptor (fn [_] {:status http-200-ok}) request->descriptor-fn service-invocation-authorized?
                                            start-new-service-fn fallback-state-atom utils/exception->response)
        request {}
        {:keys [body headers status]} (handler request)]
    (is (= http-500-internal-server-error status))
    (is (nil? (get headers "location")))
    (is (= "text/plain" (get headers "content-type")))
    (is (str/includes? (str body) "Internal error"))))

(deftest test-message-reaches-user-on-process-error
  (let [request->descriptor-fn (fn [_] (throw (ex-info "Error message for user" {:status http-404-not-found})))
        service-invocation-authorized? (constantly true)
        start-new-service-fn (constantly nil)
        fallback-state-atom (atom {})
        handler (descriptor/wrap-descriptor (fn [_] {:status http-200-ok}) request->descriptor-fn service-invocation-authorized?
                                            start-new-service-fn fallback-state-atom utils/exception->response)
        request {}
        {:keys [body headers status]} (handler request)]
    (is (= http-404-not-found status))
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

(deftest test-classify-error
  (with-redefs [utils/message name]
    (is (= [:generic-error "Test Exception" http-500-internal-server-error error-image-500-internal-server-error "clojure.lang.ExceptionInfo"]
           (classify-error "test-classify-error" (ex-info "Test Exception" {:source :test}))))
    (is (= [:test-error "Test Exception" http-500-internal-server-error error-image-500-internal-server-error "clojure.lang.ExceptionInfo"]
           (classify-error "test-classify-error" (ex-info "Test Exception" {:error-cause :test-error :source :test}))))
    (is (= [:generic-error "Test 401 Exception" http-401-unauthorized nil "clojure.lang.ExceptionInfo"]
           (classify-error "test-classify-error" (ex-info "Test 401 Exception" {:source :test :status http-401-unauthorized}))))
    (is (= [:generic-error "Test 400 Exception" http-400-bad-request error-image-400-bad-request "clojure.lang.ExceptionInfo"]
           (classify-error "test-classify-error" (ex-info "Test 400 Exception" {:source :test :status http-400-bad-request}))))
    (is (= [:generic-error "Test 500 Exception" http-500-internal-server-error error-image-500-internal-server-error "clojure.lang.ExceptionInfo"]
           (classify-error "test-classify-error" (ex-info "Test 500 Exception" {:source :test :status http-500-internal-server-error}))))
    (is (= [:generic-error "Test Exception" http-400-bad-request error-image-400-bad-request "clojure.lang.ExceptionInfo"]
           (classify-error "test-classify-error" (ex-info "Test Exception" {:source :test :status http-400-bad-request :waiter/error-image error-image-400-bad-request}))))
    (is (= [:instance-error "backend-request-failed" http-502-bad-gateway error-image-502-connection-failed "java.io.IOException"]
           (classify-error "test-classify-error" (ex-info "Test Exception" {:source :test :status http-400-bad-request} (IOException. "Test")))))
    (is (= [:instance-error "backend-request-failed" http-502-bad-gateway error-image-502-connection-failed "java.io.IOException"]
           (classify-error "test-classify-error" (IOException. "Test"))))
    (is (= [:client-error "Client action means stream is no longer needed" http-400-bad-request error-image-400-bad-request "java.io.IOException"]
           (classify-error "test-classify-error" (ex-info "Test Exception" {:source :test :status http-400-bad-request} (IOException. "cancel_stream_error")))))
    (is (= [:client-error "Client action means stream is no longer needed" http-400-bad-request error-image-400-bad-request "java.io.IOException"]
           (classify-error "test-classify-error" (IOException. "cancel_stream_error"))))
    (let [exception (IOException. "internal_error")]
      (->> (into-array StackTraceElement
                       [(StackTraceElement. "org.eclipse.jetty.http2.client.http.HttpReceiverOverHTTP2" "onReset" "HttpReceivedOverHTTP2.java" 169)
                        (StackTraceElement. "org.eclipse.jetty.http2.api.Stream$Listener" "onReset" "Stream.java" 177)
                        (StackTraceElement. "org.eclipse.jetty.http2.HTTP2Stream" "notifyReset" "HTTP2Stream.java" 574)])
           (.setStackTrace exception))
      (is (= [:client-error "Client send invalid data to HTTP/2 backend" http-400-bad-request error-image-400-bad-request "java.io.IOException"]
             (classify-error "test-classify-error" exception))))
    (is (= [:instance-error "backend-request-failed" http-502-bad-gateway error-image-502-connection-failed "java.io.IOException"]
           (classify-error "test-classify-error" (IOException. "internal_error"))))
    (is (= [:server-eagerly-closed "Connection eagerly closed by server" http-400-bad-request error-image-400-bad-request "java.io.IOException"]
           (classify-error "test-classify-error" (IOException. "no_error"))))
    (is (= [:client-error "Connection unexpectedly closed while streaming request" http-400-bad-request error-image-400-bad-request "org.eclipse.jetty.io.EofException"]
           (classify-error "test-classify-error" (ex-info "Test Exception" {:source :test :status http-400-bad-request} (EofException. "Test")))))
    (is (= [:client-eagerly-closed "Connection eagerly closed by client" http-400-bad-request error-image-400-bad-request "org.eclipse.jetty.io.EofException"]
           (classify-error "test-classify-error" (ex-info "Test Exception" {:source :test :status http-400-bad-request} (EofException. "reset")))))
    (is (= [:client-eagerly-closed "Connection eagerly closed by client" http-400-bad-request error-image-400-bad-request "org.eclipse.jetty.io.EofException"]
           (classify-error "test-classify-error" (EofException. "reset"))))
    (is (= [:instance-error "backend-request-timed-out" http-504-gateway-timeout error-image-504-gateway-timeout "java.util.concurrent.TimeoutException"]
           (classify-error "test-classify-error" (TimeoutException. "timeout"))))
    (is (= [:client-error "Timeout receiving bytes from client" http-408-request-timeout error-image-408-request-timeout "java.util.concurrent.TimeoutException"]
           (let [timeout-exception (TimeoutException. "timeout")]
             (.addSuppressed timeout-exception (Throwable. "HttpInput idle timeout"))
             (classify-error "test-classify-error" timeout-exception))))
    (is (= [:client-error "Failed to upgrade to websocket connection" http-400-bad-request error-image-400-bad-request "org.eclipse.jetty.websocket.api.UpgradeException"]
           (classify-error "test-classify-error" (UpgradeException. nil http-400-bad-request "websocket upgrade failed"))))
    (is (= [:instance-error "backend-connect-error" http-502-bad-gateway error-image-502-connection-failed "java.net.ConnectException"]
           (classify-error "test-classify-error" (ConnectException. "Connection refused"))))
    (is (= [:instance-error "backend-connect-error" http-502-bad-gateway error-image-502-connection-failed "java.net.SocketTimeoutException"]
           (classify-error "test-classify-error" (SocketTimeoutException. "Connect Timeout"))))
    (is (= [:instance-error "backend-request-failed" http-502-bad-gateway error-image-502-connection-failed "java.net.SocketTimeoutException"]
           (classify-error "test-classify-error" (SocketTimeoutException. "Connection refused"))))
    (is (= [:instance-error "backend-request-failed" http-502-bad-gateway error-image-502-connection-failed "java.lang.Exception"]
           (classify-error "test-classify-error" (Exception. "Test Exception"))))))
