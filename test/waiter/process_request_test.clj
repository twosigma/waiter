;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.process-request-test
  (:require [clj-http.client]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [metrics.counters :as counters]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [waiter.core :refer :all]
            [waiter.headers :as headers]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.process-request :refer :all]
            [waiter.service-description :as sd]
            [waiter.statsd :as statsd]
            [waiter.token :as token])
  (:import clojure.lang.ExceptionInfo
           org.eclipse.jetty.client.HttpClient))

(defn request
  [resource request-method & params]
  {:request-method request-method :uri resource :params (first params)})

(deftest test-http-request-method-get
  (testing "Test request method: get"
    (let [dummy-request (request "/run" :get {:a 1 :b 2})]
      (is (= http/get
             (request->http-method-fn dummy-request))))))

(deftest test-http-request-method-post
  (testing "Test request method: post"
    (let [dummy-request (request "/run" :post {:a 1 :b 2})]
      (is (= http/post
             (request->http-method-fn dummy-request))))))

(deftest test-http-request-method-put
  (testing "Test request method: put"
    (let [dummy-request (request "/run" :put {:a 1 :b 2})]
      (is (= http/put
             (request->http-method-fn dummy-request))))))

(deftest test-http-request-method-delete
  (testing "Test request method: delete"
    (let [dummy-request (request "/run" :delete {:a 1 :b 2})]
      (is (= http/delete
             (request->http-method-fn dummy-request))))))

(deftest test-http-request-method-head
  (testing "Test request method: head"
    (let [dummy-request (request "/run" :head {:a 1 :b 2})]
      (is (= http/head
             (request->http-method-fn dummy-request))))))

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
              expected-endpoint (if (contains? legacy-endpoints item) custom-legacy-endpoint (str item "?foo=bar&baz=1234"))]
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

(deftest test-request-authorized?
  (let [test-cases
        (list
          {:name "request-authorized?:missing-permission-and-user"
           :input-data {:user nil, :waiter-headers {"foo" "bar"}}
           :expected false
           }
          {:name "request-authorized?:missing-permitted-but-valid-user"
           :input-data {:user "test-user", :waiter-headers {"foo" "bar"}}
           :expected false
           }
          {:name "request-authorized?:unauthorized-user"
           :input-data {:user "test-user", :waiter-headers {"permitted-user" "another-user"}}
           :expected false
           }
          {:name "request-authorized?:authorized-user-match"
           :input-data {:user "test-user", :waiter-headers {"permitted-user" "test-user"}}
           :expected true
           }
          {:name "request-authorized?:authorized-user-any"
           :input-data {:user "test-user", :waiter-headers {"permitted-user" token/ANY-USER}}
           :expected true
           }
          {:name "request-authorized?:authorized-user-any-with-missing-user"
           :input-data {:user nil, :waiter-headers {"permitted-user" token/ANY-USER}}
           :expected true
           })]
    (doseq [test-case test-cases]
      (testing (str "Test " (:name test-case))
        (is (= (:expected test-case)
               (request-authorized?
                 (get-in test-case [:input-data :user])
                 (get-in test-case [:input-data :waiter-headers "permitted-user"]))))))))

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
            resp-chan (async/chan)
            instance-request-properties {:streaming-timeout-ms 100}
            reservation-status-promise (promise)
            request-state-chan (async/chan)
            metric-group nil
            waiter-debug-enabled nil
            service-id "service-id"
            error-chan (async/chan)]
        (async/go-loop [bytes-streamed 0]
          (if (= bytes-streamed bytes)
            (do
              (async/close! error-chan)
              (async/close! body))
            (let [bytes-to-stream (min 1024 (- bytes bytes-streamed))]
              (async/>! body (byte-array (repeat bytes-to-stream 0)))
              (recur (+ bytes-streamed bytes-to-stream)))))
        (stream-http-response body error-chan confirm-live-connection resp-chan instance-request-properties
                              reservation-status-promise request-state-chan metric-group
                              waiter-debug-enabled (stream-metric-map service-id))
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
                  (fn [_ _ _ auth-user _ _ location _]
                    (reset! post-process-data {:auth-user auth-user, :location location}))]
              (inspect-for-202-async-request-response
                post-process-async-request-response-fn {} "service-id" "metric-group" {}
                endpoint request {} response (atom {}) reservation-status-promise)
              (deliver reservation-status-promise :not-async)
              (assoc @post-process-data :result @reservation-status-promise)))]
    (testing "202-missing-location-header"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {}}))))
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
      (is (= {:auth-user "test-user", :location "/result/location", :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {"location" "/result/location"}}))))
    (testing "202-relative-location-1"
      (is (= {:auth-user "test-user", :location "/query/result/location", :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {"location" "../result/location"}}))))
    (testing "202-relative-location-2"
      (is (= {:auth-user "test-user", :location "/query/for/result/location", :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {"location" "result/location"}}))))
    (testing "202-relative-location-two-levels"
      (is (= {:auth-user "test-user", :location "/result/location", :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {"location" "../../result/location"}}))))
    (testing "202-absolute-url-same-host-port"
      (is (= {:auth-user "test-user", :location "/retrieve/result/location", :result :success-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {"location" "http://www.example.com:1234/retrieve/result/location"}}))))
    (testing "202-absolute-url-different-host"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {"location" "http://www.example2.com:1234/retrieve/result/location"}}))))
    (testing "202-absolute-url-different-port"
      (is (= {:result :not-async}
             (execute-inspect-for-202-async-request-response
               "http://www.example.com:1234/query/for/status"
               {:authorization/user "test-user"}
               {:status 202, :headers {"location" "http://www.example.com:5678/retrieve/result/location"}}))))))

(deftest test-make-request
  (let [instance {:service-id "test-service-id", :host "example.com", :port 8080}
        request {:authorization/user "test-user"
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
      (with-redefs [request->http-method-fn
                    (fn [_]
                      (fn [^HttpClient _ endpoint request-config]
                        (is (= "http://example.com:8080/end-route" endpoint))
                        (is (= :bytes (:as request-config)))
                        (is (:auth request-config))
                        (is (= "body" (:body request-config)))
                        (is (= 654321 (:idle-timeout request-config)))
                        (is (= (-> (dissoc passthrough-headers "content-length" "expect" "authorization"
                                           "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                                           "te" "trailers" "transfer-encoding" "upgrade")
                                   (merge {"x-waiter-auth-principal" "test-user"}))
                               (:headers request-config)))))]
        (make-request instance (http/client) request request-properties passthrough-headers end-route app-password nil (constantly nil))))))

(deftest test-request->descriptor
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        can-run-as? #(= %1 %2)
        waiter-hostname "waiter-hostname.app.example.com"]
    (testing "missing user in request"
      (with-redefs [sd/request->descriptor (fn [& _] {:service-description {}
                                                      :service-preauthorized false})]
        (let [service-description-defaults {}
              service-id-prefix "service-prefix-"
              request {}]
          (is (thrown-with-msg? ExceptionInfo #"kerberos auth failed"
                                (request->descriptor service-description-defaults service-id-prefix
                                                     kv-store waiter-hostname can-run-as? []
                                                     (sd/->DefaultServiceDescriptionBuilder) request))))))
    (testing "not preauthorized service and different user"
      (with-redefs [sd/request->descriptor (fn [& _] {:service-description {"run-as-user" "ruser"}
                                                      :service-preauthorized false})]
        (let [service-description-defaults {}
              service-id-prefix "service-prefix-"
              request {:authorization/user "tuser"}]
          (is (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service"
                                (request->descriptor service-description-defaults service-id-prefix
                                                     kv-store waiter-hostname can-run-as? []
                                                     (sd/->DefaultServiceDescriptionBuilder) request))))))
    (testing "not permitted to run service"
      (with-redefs [sd/request->descriptor (fn [& _] {:service-description {"run-as-user" "ruser", "permitted-user" "puser"}
                                                      :service-preauthorized false})]
        (let [service-description-defaults {}
              service-id-prefix "service-prefix-"
              request {:authorization/user "ruser"}]
          (is (thrown-with-msg? ExceptionInfo #"This user isn't allowed to invoke this service"
                                (request->descriptor service-description-defaults service-id-prefix
                                                     kv-store waiter-hostname can-run-as? []
                                                     (sd/->DefaultServiceDescriptionBuilder) request))))))
    (testing "preauthorized service, not permitted to run service"
      (with-redefs [sd/request->descriptor (fn [& _] {:service-description {"run-as-user" "ruser", "permitted-user" "puser"}
                                                      :service-preauthorized true})]
        (let [service-description-defaults {}
              service-id-prefix "service-prefix-"
              request {:authorization/user "tuser"}]
          (is (thrown-with-msg? ExceptionInfo #"This user isn't allowed to invoke this service"
                                (request->descriptor service-description-defaults service-id-prefix
                                                     kv-store waiter-hostname can-run-as? []
                                                     (sd/->DefaultServiceDescriptionBuilder) request))))))
    (testing "preauthorized service, permitted to run service"
      (with-redefs [sd/request->descriptor (fn [& _] {:service-description {"run-as-user" "ruser", "permitted-user" "tuser"}
                                                      :service-preauthorized true})]
        (let [service-description-defaults {}
              service-id-prefix "service-prefix-"
              request {:authorization/user "tuser"}]
          (is (not (nil? (request->descriptor service-description-defaults service-id-prefix
                                              kv-store waiter-hostname can-run-as? []
                                              (sd/->DefaultServiceDescriptionBuilder) request)))))))
    (testing "not preauthorized service, permitted to run service"
      (with-redefs [sd/request->descriptor (fn [& _] {:service-description {"run-as-user" "tuser", "permitted-user" "tuser"}
                                                      :service-preauthorized false})]
        (let [service-description-defaults {}
              service-id-prefix "service-prefix-"
              request {:authorization/user "tuser"}]
          (is (not (nil? (request->descriptor service-description-defaults service-id-prefix
                                              kv-store waiter-hostname can-run-as? []
                                              (sd/->DefaultServiceDescriptionBuilder) request)))))))))

(deftest test-handle-suspended-service
  (testing "returns error for suspended app"
    (let [{:keys [status body]} (handle-suspended-service nil {:suspended-state {:suspended true
                                                                                 :last-updated-by "suser"
                                                                                 :time (t/now)}})]
      (is (= 503 status))
      (is (str/includes? body "Service has been suspended"))
      (is (str/includes? body "suser"))))
  (testing "passes apps by default"
    (is (nil? (handle-suspended-service nil {})))))

(deftest test-handle-too-many-requests
  (testing "returns error for too many requests"
    (let [service-id "myservice"
          counter (metrics/service-counter service-id "request-counts" "waiting-for-available-instance")]
      (counters/clear! counter)
      (counters/inc! counter 10)
      (let [{:keys [status body]} (handle-too-many-requests nil {:service-id service-id
                                                                 :service-description {"max-queue-length" 5}})]
        (is (= 503 status))
        (is (str/includes? body "Max queue length")))))
  (testing "passes service with fewer requests"
    (let [service-id "okservice"
          counter (metrics/service-counter service-id "request-counts" "waiting-for-available-instance")]
      (counters/clear! counter)
      (counters/inc! counter 3)
      (is (nil? (handle-too-many-requests nil {:service-id service-id
                                               :service-description {"max-queue-length" 10}}))))))
