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
(ns waiter.process-request-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [metrics.counters :as counters]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [waiter.core :refer :all]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.descriptor :as descriptor]
            [waiter.process-request :refer :all]
            [waiter.statsd :as statsd])
  (:import (org.eclipse.jetty.client HttpClient)))

(defn request
  [resource request-method & params]
  {:request-method request-method :uri resource :params (first params)})

(deftest test-request->endpoint-without-headers
  (let [legacy-endpoints #{"/secrun"}
        passthrough-endpoints #{"/foo" "/baz/bar" "/load/balancer" "/auto/scale/1/2/3"}
        waiter-headers {}
        test-endpoints (clojure.set/union legacy-endpoints passthrough-endpoints)]
    (doseq [item test-endpoints]
      (testing (str "Test retrieve endpoint without headers: " item)
        (let [dummy-request (request item :post {:a 1 :b 2})
              expected-endpoint (if (contains? legacy-endpoints item) "/req" item)]
          (is (= expected-endpoint
                 (request->endpoint dummy-request waiter-headers))))))))

(deftest test-request->endpoint-with-headers
  (let [legacy-endpoints #{"/secrun"}
        passthrough-endpoints #{"/foo" "/baz/bar" "/load/balancer" "/auto/scale/1/2/3"}
        custom-legacy-endpoint "/custom/endpoint"
        waiter-headers {(str headers/waiter-header-prefix "endpoint-path") custom-legacy-endpoint}
        test-endpoints (clojure.set/union legacy-endpoints passthrough-endpoints)]
    (doseq [item test-endpoints]
      (testing (str "Test retrieve endpoint with headers: " item)
        (let [dummy-request (request item :post {:a 1 :b 2})
              expected-endpoint (if (contains? legacy-endpoints item) custom-legacy-endpoint item)]
          (is (= expected-endpoint
                 (request->endpoint dummy-request waiter-headers))))))))

(deftest test-request->endpoint-with-headers-and-query-string
  (let [legacy-endpoints #{"/secrun"}
        passthrough-endpoints #{"/foo" "/baz/bar" "/load/balancer" "/auto/scale/1/2/3"}
        custom-legacy-endpoint "/custom/endpoint"
        waiter-headers {(str headers/waiter-header-prefix "endpoint-path") custom-legacy-endpoint}
        test-endpoints (clojure.set/union legacy-endpoints passthrough-endpoints)]
    (doseq [item test-endpoints]
      (testing (str "Test retrieve endpoint with headers and query string: " item)
        (let [dummy-request (assoc (request item :post {:a 1 :b 2}) :query-string "foo=bar&baz=1234")
              expected-endpoint (if (contains? legacy-endpoints item) custom-legacy-endpoint item)]
          (is (= expected-endpoint
                 (request->endpoint dummy-request waiter-headers))))))))

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

(deftest test-inspect-for-202-async-request-response
  (letfn [(execute-inspect-for-202-async-request-response
            [endpoint request response]
            (let [reservation-status-promise (promise)
                  post-process-data (atom {})
                  post-process-async-request-response-fn
                  (fn [_ _ _ _ auth-user _ _ location query-string]
                    (reset! post-process-data
                            {:auth-user (:username auth-user) :location location :query-string query-string}))]
              (inspect-for-202-async-request-response
                response post-process-async-request-response-fn {} "service-id" "metric-group" {}
                endpoint request {} reservation-status-promise)
              (deliver reservation-status-promise :not-async)
              (assoc @post-process-data :result @reservation-status-promise)))]
    (testing "202-missing-location-header"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202 :headers {}}))))
    (testing "200-not-async"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 200, :headers {"location" "/result/location"}}))))
    (testing "303-not-async"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 303, :headers {"location" "/result/location"}}))))
    (testing "404-not-async"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 404, :headers {"location" "/result/location"}}))))
    (testing "202-absolute-location"
      (is (= {:auth-user "test-user" :location "/result/location" :query-string nil :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202 :headers {"location" "/result/location"}}))))
    (testing "202-relative-location-1"
      (is (= {:auth-user "test-user" :location "/query/result/location" :query-string nil :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202 :headers {"location" "../result/location"}}))))
    (testing "202-relative-location-2"
      (is (= {:auth-user "test-user" :location "/query/for/result/location" :query-string nil :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202 :headers {"location" "result/location"}}))))
    (testing "202-relative-location-two-levels"
      (is (= {:auth-user "test-user" :location "/result/location" :query-string "p=q&r=s|t" :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status?u=v&w=x|y|z"
               {:authorization/user "test-user" :query-string "a=b&c=d|e"}
               {:status 202 :headers {"location" "../../result/location?p=q&r=s|t"}}))))
    (testing "202-absolute-url-same-host-port"
      (is (= {:auth-user "test-user" :location "/retrieve/result/location" :query-string nil :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202 :headers {"location" "http://www.example.com:1234/retrieve/result/location"}}))))
    (testing "202-absolute-url-different-host"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202 :headers {"location" "http://www.example2.com:1234/retrieve/result/location"}}))))
    (testing "202-absolute-url-different-port"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202 :headers {"location" "http://www.example.com:5678/retrieve/result/location"}}))))))

(deftest test-make-request
  (let [instance {:service-id "test-service-id", :host "example.com", :port 8080, :protocol "proto"}
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
    (testing "make-request:headers"
      (let [expected-endpoint "proto://example.com:8080/end-route"
            make-basic-auth-fn (fn make-basic-auth-fn [endpoint username password]
                                 (is (= expected-endpoint endpoint))
                                 (is (= username "waiter"))
                                 (is (= app-password password))
                                 (Object.))
            service-id->password-fn (fn service-id->password-fn [service-id]
                                      (is (= "test-service-id" service-id))
                                      app-password)
            http-client (http/client)
            request-method-fn-call-counter (atom 0)]
        (with-redefs [http/request
                      (fn [^HttpClient _ request-config]
                        (swap! request-method-fn-call-counter inc)
                        (is (= expected-endpoint (:url request-config)))
                        (is (= :bytes (:as request-config)))
                        (is (:auth request-config))
                        (is (= "body" (:body request-config)))
                        (is (= 654321 (:idle-timeout request-config)))
                        (is (= (-> (dissoc passthrough-headers "expect" "authorization"
                                           "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                                           "te" "trailers" "transfer-encoding" "upgrade")
                                   (merge {"x-waiter-auth-principal" "test-user"
                                           "x-waiter-authenticated-principal" "test-user@test.com"}))
                               (:headers request-config))))]
          (make-request http-client make-basic-auth-fn service-id->password-fn instance request request-properties passthrough-headers end-route nil)
          (is (= 1 @request-method-fn-call-counter)))))))

(deftest test-wrap-suspended-service
  (testing "returns error for suspended app"
    (let [handler (-> (fn [_] {:status 200})
                      wrap-suspended-service)
          request {:descriptor {:service-id "service-id-1"
                                :suspended-state {:suspended true
                                                  :last-updated-by "test-user"
                                                  :time (t/now)}}}
          {:keys [status body]} (handler request)]
      (is (= 503 status))
      (is (str/includes? body "Service has been suspended"))
      (is (str/includes? body "test-user"))))

  (testing "passes apps by default"
    (let [handler (-> (fn [_] {:status 200})
                      wrap-suspended-service)
          request {}
          {:keys [status]} (handler request)]
      (is (= 200 status)))))

(deftest test-wrap-too-many-requests
  (testing "returns error for too many requests"
    (let [service-id "my-service"
          counter (metrics/service-counter service-id "request-counts" "waiting-for-available-instance")]
      (counters/clear! counter)
      (counters/inc! counter 10)
      (let [handler (-> (fn [_] {:status 200})
                        wrap-too-many-requests)
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
      (let [handler (-> (fn [_] {:status 200})
                        wrap-too-many-requests)
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
        handler (-> (fn [_] {:status 200})
                    (descriptor/wrap-descriptor request->descriptor-fn start-new-service-fn fallback-state-atom))]
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
        handler (-> (fn [_] {:status 200})
                    (descriptor/wrap-descriptor request->descriptor-fn start-new-service-fn fallback-state-atom))
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
        handler (-> (fn [_] {:status 200})
                    (descriptor/wrap-descriptor request->descriptor-fn start-new-service-fn fallback-state-atom))
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
