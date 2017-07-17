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
                 :request-method :get}]
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
                "request-method" "get"}
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
      (let [response-data (async/<!! out)
            expected-data (byte-array (take 10000 (cycle (range 103))))]
        (is (Arrays/equals ^bytes expected-data ^bytes response-data))))

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

    (testing "Do not authenticate /status"
      (is (= {:status 200} (handler {:uri "/status"}))))

    (testing "Return 401 on missing auth"
      (is (= 401 (:status (handler {:uri "/handler", :headers {}})))))

    (testing "Return 401 on bad authentication"
      (is (= 401 (:status (handler {:uri "/handler", :headers {"authorization" bad-auth}})))))

    (testing "Successful authentication"
      (is (= {:status 200} (handler {:uri "/handler", :headers {"authorization" auth}}))))))

(deftest test-correlation-id-middleware
  (let [handler (fn test-correlation-id-middleware-helper
                  [{:keys [headers request-method]}]
                  (is (get headers "x-cid"))
                  (is (get headers "x-kitchen-request-id"))
                  (is (= {"foo" "bar", "lorem" "ipsum"} (select-keys headers ["foo" "lorem"])))
                  (is (= :get request-method))
                  :test-correlation-id-middleware-helper)
        request {:headers {"foo" "bar", "lorem" "ipsum"}
                 :request-method :get}]
    (is (= :test-correlation-id-middleware-helper ((correlation-id-middleware handler) request)))))
