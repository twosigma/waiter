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
(ns kitchen.core-test
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [kitchen.core :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (java.nio ByteBuffer)
           (java.util Arrays)
           (org.apache.commons.codec.binary Base64)))

(deftest bad-status-handler-test
  (testing "bad status handler"
    (testing "should return intended status code"
      (let [{:keys [status]} (http-handler {:query-params {"status" "400"}
                                            :uri "/bad-status"})]
        (is (= 400 status))
      (let [{:keys [status]} (http-handler {:query-params {"status" "401"}
                                            :uri "/bad-status"})]
        (is (= 401 status)))
      (let [{:keys [status]} (http-handler {:query-params {"status" "402"}
                                            :uri "/bad-status"})]
        (is (= 402 status)))))))

(deftest sleep-handler-test
  (testing "sleep handler"
    (testing "should sleep for intended number of milliseconds"
      (let [{:keys [body status]} (http-handler {:query-params {"sleep-ms" "500"}
                                                 :uri "/sleep"})]
        (is (= 400 status))
        (is (= "Slept for 500 ms" body)))
      (let [{:keys [body status]} (http-handler {:query-params {"sleep-ms" "300" "status" "403"}
                                                 :uri "/sleep"})]
        (is (= 403 status))
        (is (= "Slept for 300 ms" body))))))

(deftest default-handler-test
  (testing "Default handler"
    (testing "should echo request body when x-kitchen-echo is present"
      (let [handle-echo #(default-handler {:headers {"x-kitchen-echo" true} :body %})]
        (is (= {:body "foo" :status 200} (handle-echo "foo")))
        (is (= {:body "bar" :status 200} (handle-echo "bar")))))
    (testing "should set cookies when x-kitchen-cookies is present"
      (let [handle-cookies #(sort (get-in (default-handler {:headers {"x-kitchen-cookies" %}}) [:headers "Set-Cookie"]))]
        (is (= ["a=b" "c=d"] (handle-cookies "a=b,c=d")))
        (is (= ["a=b" "c=d" "e=f"] (handle-cookies "a=b,c=d,e=f")))))
    (testing "should throw exception when x-kitchen-throw is present"
      (let [handle-throw #(default-handler {:headers {"x-kitchen-throw" true}})]
        (is (thrown-with-msg? ExceptionInfo #"Instructed by header to throw" (handle-throw)))))))

(deftest parse-cookies-header-test
  (testing "Parsing of the cookies header"
    (testing "should handle well-formed values"
      (is (= {"session-id" {:value "session-id-hash"}} (parse-cookies "session-id=session-id-hash")))
      (is (= {"a" {:value "b"}, "c" {:value "d"}} (parse-cookies "a=b,c=d"))))
    (testing "should handle nil"
      (is (= nil (parse-cookies nil))))))

(deftest handler-test
  (testing "Top-level handler"
    (testing "should not let exceptions flow out"
      (is (= 500 (:status (http-handler {:uri "/foo", :headers {"x-kitchen-throw" true}})))))))

(deftest pi-handler-test
  (testing "pi handler"
    (testing "should return expected fields"
      (let [{:keys [body]} (http-handler {:uri "/pi"
                                          :form-params {"iterations" "100"
                                                        "threads" "2"}})
            {:strs [iterations inside pi-estimate]} (json/read-str body)]
        (is (and iterations inside pi-estimate))))))

(deftest test-async-request
  (testing "async-request:expected-workflow"
    (let [request {:headers {"x-kitchen-delay-ms" "2000", "x-kitchen-store-async-response-ms" "1000"}
                   :uri "/async/request"}
          {:keys [body headers status]} (http-handler request)]
      (is (= 202 status))
      (is (= "text/plain" (get headers "Content-Type")))
      (let [request-id (get headers "x-kitchen-request-id")]
        (is (= (str "/async/status?request-id=" request-id) (get headers "Location")))
        (is (= (str "Accepted request " request-id) body))
        (let [request-metadata (get @async-requests request-id)
              {:keys [complete-handle gc-handle process-handle]} (:channels request-metadata)]
          (is (every? identity [request-metadata complete-handle gc-handle process-handle]) (str request-metadata))
          (is (= :complete (async/<!! process-handle)))
          (is (:done (get @async-requests request-id)) (str @async-requests))
          (is (= :complete (async/<!! gc-handle)))
          (is (nil? (get @async-requests request-id)))
          (is (= :complete (async/<!! complete-handle))))
        (is (nil? (get @async-requests request-id))))))

  (testing "async-request:exlcude-location-and-expires-header"
    (let [request {:headers {"x-kitchen-delay-ms" "2000", "x-kitchen-store-async-response-ms" "1",
                             "x-kitchen-exclude-headers" "expires,location"}
                   :uri "/async/request"}
          {:keys [body headers status]} (http-handler request)]
      (is (= 202 status))
      (is (= "text/plain" (get headers "Content-Type")))
      (let [request-id (get headers "x-kitchen-request-id")]
        (is (nil? (get headers "Expires")))
        (is (nil? (get headers "Location")))
        (is (= (str "Accepted request " request-id) body))
        (let [request-metadata (get @async-requests request-id)
              {:keys [complete-handle]} (:channels request-metadata)]
          (is (= :complete (async/<!! complete-handle))))
        (is (nil? (get @async-requests request-id)))))))

(deftest test-async-status-handler
  (let [request-id "my-test-request-id"]
    (swap! async-requests assoc request-id {:request-id request-id})

    (testing "missing-request-id"
      (let [request {:request-method :get
                     :query-params {}
                     :uri "/async/status"}
            {:keys [body headers status]} (http-handler request)]
        (is (= 400 status))
        (is (= "text/plain" (get headers "Content-Type")))
        (is (= "Missing request-id" body))))

    (testing "unknown-request-id"
      (let [request {:request-method :get
                     :query-params {"request-id" (str request-id "-unknown")}
                     :uri "/async/status"}
            {:keys [body headers status]} (http-handler request)]
        (is (= 410 status))
        (is (= "text/plain" (get headers "Content-Type")))
        (is (= (str "No data found for request-id " request-id "-unknown") body))))

    (testing "pending-request-id"
      (let [request {:request-method :get
                     :query-params {"request-id" request-id}
                     :uri "/async/status"}
            {:keys [body headers status]} (http-handler request)]
        (is (= 200 status))
        (is (= "text/plain" (get headers "Content-Type")))
        (is (str "Still processing request-id " request-id) body)))

    (testing "completed-request-id"
      (swap! async-requests assoc-in [request-id :done] true)
      (let [request {:request-method :get
                     :query-params {"request-id" request-id}
                     :uri "/async/status"}
            {:keys [body headers status]} (http-handler request)]
        (is (= 303 status))
        (is (= "text/plain" (get headers "Content-Type")))
        (is (= (str "/async/result?request-id=" request-id) (get headers "Location")))
        (is (str "Processing complete for request-id " request-id) body)))

    (swap! async-requests dissoc request-id)))

(deftest test-async-result-handler
  (let [request-id "my-test-request-id"]
    (swap! async-requests assoc request-id {:request-id request-id})
    (let [request {:headers {}
                   :query-params {"request-id" request-id}
                   :uri "/async/result"}
          {:keys [body headers status]} (http-handler request)]
      (is (= 200 status))
      (is (= "application/json" (get headers "Content-Type")))
      (is (= {"result" {"request-id" "my-test-request-id"}} (json/read-str body)))
      (is (nil? (get @async-requests request-id))))
    (swap! async-requests dissoc request-id)))

(deftest test-websocket-handler
  (let [out (async/chan)
        in (async/chan)
        request {:headers {"foo" "bar", "lorem" "ipsum"}
                 :in in
                 :out out
                 :request-method :get
                 :uri "/uri"}
        websocket-handler (websocket-handler-factory {:ws-max-binary-message-size 32768
                                                      :ws-max-text-message-size 32768})]
    (reset! async-requests {})
    (reset! pending-http-requests 0)
    (reset! pending-ws-requests 0)
    (reset! total-http-requests 0)
    (reset! total-ws-requests 0)
    (websocket-handler request)
    (is (= "Connected to kitchen" (async/<!! out)))

    (testing "echo support"
      (let [echo-message "echo-me"]
        (async/>!! in echo-message)
        (is (= echo-message (async/<!! out)))))

    (testing "request-info"
      (async/>!! in "request-info")
      (let [response (-> (async/<!! out) json/read-str)]
        (is (= {"headers" {"foo" "bar", "lorem" "ipsum"}
                "request-method" "get"
                "uri" "/uri"}
               response))))

    (testing "kitchen-state"
      (async/>!! in "kitchen-state")
      (let [response (-> (async/<!! out) json/read-str)]
        (is (= response {"async-requests" {},
                         "pending-http-requests" 0, "total-http-requests" 0,
                         "pending-ws-requests" 1, "total-ws-requests" 1}))))

    (testing "chars-10000"
      (async/>!! in "chars-10000")
      (let [response (async/<!! out)]
        (is (string? response))
        (is (= 10000 (count response)))))

    (testing "bytes-10000"
      (async/>!! in "bytes-10000")
      (let [response-data (async/<!! out)]
        (is (= (Class/forName "[B") (.getClass response-data)))
        (is (= 10000 (count response-data)))))

    (testing "raw-bytes"
      (let [byte-data (byte-array (take 100000 (cycle (range 2121))))
            byte-buffer (ByteBuffer/wrap byte-data)
            _ (async/>!! in byte-buffer)
            response-data (async/<!! out)]
        (is (not (identical? byte-data response-data)))
        (is (Arrays/equals ^bytes byte-data ^bytes response-data))))

    (testing "exit"
      (async/>!! in "exit")
      (is (= "bye" (async/<!! out)))
      (is (nil? (async/<!! out))))

    (async/close! in)
    (async/close! out)))

(deftest test-basic-auth
  (testing "no username and password"
    (let [handler (basic-auth-middleware nil nil (constantly {:status 200}))]
      (is (= {:status 200} (handler {:uri "/handler"})))))
  (let [handler (basic-auth-middleware "waiter" "test" (constantly {:status 200}))
        auth (str "Basic " (String. (Base64/encodeBase64 (.getBytes "waiter:test"))))
        bad-auth (str "Basic " (String. (Base64/encodeBase64 (.getBytes "waiter:badtest"))))]

    (testing "Do not authenticate /status, /bad-status, or /sleep"
      (is (= {:status 200} (handler {:uri "/status"})))
      (is (= {:status 200} (handler {:uri "/bad-status"})))
      (is (= {:status 200} (handler {:uri "/sleep"}))))

    (testing "Return 401 on missing auth"
      (is (= 401 (:status (handler {:uri "/handler", :headers {}})))))

    (testing "Return 401 on bad authentication"
      (is (= 401 (:status (handler {:uri "/handler", :headers {"authorization" bad-auth}})))))

    (testing "Successful authentication"
      (is (= {:status 200} (handler {:uri "/handler", :headers {"authorization" auth}}))))))

(deftest test-correlation-id-middleware
  (testing "no cid in request/response"
    (let [cid-atom (atom nil)
          test-response {:body :test-correlation-id-middleware-helper}
          handler (fn test-correlation-id-middleware-helper
                    [{:keys [headers request-method]}]
                    (is (get headers "x-cid"))
                    (reset! cid-atom (get headers "x-cid"))
                    (is (get headers "x-kitchen-request-id"))
                    (is (= {"foo" "bar", "lorem" "ipsum"} (select-keys headers ["foo" "lorem"])))
                    (is (= :get request-method))
                    test-response)
          request {:headers {"foo" "bar", "lorem" "ipsum"}
                   :request-method :get}
          actual-response ((correlation-id-middleware handler) request)]
      (is (= (assoc-in test-response [:headers "x-cid"] @cid-atom)
             actual-response))))

  (testing "cid in request, but not in response"
    (let [correlation-id (str "cid-" (rand-int 10000))
          test-response {:body :test-correlation-id-middleware-helper}
          handler (fn test-correlation-id-middleware-helper
                    [{:keys [headers request-method]}]
                    (is (= correlation-id (get headers "x-cid")))
                    (is (get headers "x-kitchen-request-id"))
                    (is (= {"foo" "bar", "lorem" "ipsum"} (select-keys headers ["foo" "lorem"])))
                    (is (= :get request-method))
                    test-response)
          request {:headers {"foo" "bar", "lorem" "ipsum", "x-cid" correlation-id}
                   :request-method :get}
          actual-response ((correlation-id-middleware handler) request)]
      (is (= (assoc-in test-response [:headers "x-cid"] correlation-id)
             actual-response))))

  (testing "cid in response, but not in request"
    (let [correlation-id (str "cid-" (rand-int 10000))
          test-response {:body :test-correlation-id-middleware-helper
                         :headers {"humpty" "dumpty", "x-cid" correlation-id}}
          handler (fn test-correlation-id-middleware-helper
                    [{:keys [headers request-method]}]
                    (is (not= (get headers "x-cid") correlation-id))
                    (is (get headers "x-kitchen-request-id"))
                    (is (= {"foo" "bar", "lorem" "ipsum"} (select-keys headers ["foo" "lorem"])))
                    (is (= :get request-method))
                    test-response)
          request {:headers {"foo" "bar", "lorem" "ipsum"}
                   :request-method :get}
          actual-response ((correlation-id-middleware handler) request)]
      (is (= test-response actual-response))))

  (testing "different cid in request and response"
    (let [correlation-id-1 (str "cid-req-" (rand-int 10000))
          correlation-id-2 (str "cid-res-" (rand-int 10000))
          test-response {:body :test-correlation-id-middleware-helper
                         :headers {"humpty" "dumpty", "x-cid" correlation-id-2}}
          handler (fn test-correlation-id-middleware-helper
                    [{:keys [headers request-method]}]
                    (is (= (get headers "x-cid") correlation-id-1))
                    (is (get headers "x-kitchen-request-id"))
                    (is (= {"foo" "bar", "lorem" "ipsum"} (select-keys headers ["foo" "lorem"])))
                    (is (= :get request-method))
                    test-response)
          request {:headers {"foo" "bar", "lorem" "ipsum", "x-cid" correlation-id-1}
                   :request-method :get}
          actual-response ((correlation-id-middleware handler) request)]
      (is (= test-response actual-response)))))
