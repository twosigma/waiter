(ns kitchen.core-test
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [kitchen.core :refer :all])
  (:import (clojure.lang ExceptionInfo)))

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
      (is (= 500 (:status (handler {:uri "/foo", :headers {"x-kitchen-throw" true}})))))))

(deftest pi-handler-test
  (testing "pi handler"
    (testing "should return expected fields"
      (let [{:keys [body]} (handler {:uri "/pi"
                                     :form-params {"iterations" "100"
                                                   "threads" "2"}})
            {:strs [iterations inside pi-estimate]} (json/read-str body)]
        (is (and iterations inside pi-estimate))))))

(deftest test-async-request
  (testing "async-request:expected-workflow"
    (let [request {:headers {"x-kitchen-delay-ms" "2000", "x-kitchen-store-async-response-ms" "1000"}
                   :uri "/async/request"}
          {:keys [body headers status]} (handler request)]
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
          {:keys [body headers status]} (handler request)]
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
            {:keys [body headers status]} (handler request)]
        (is (= 400 status))
        (is (= "text/plain" (get headers "Content-Type")))
        (is (= "Missing request-id" body))))

    (testing "unknown-request-id"
      (let [request {:request-method :get
                     :query-params {"request-id" (str request-id "-unknown")}
                     :uri "/async/status"}
            {:keys [body headers status]} (handler request)]
        (is (= 410 status))
        (is (= "text/plain" (get headers "Content-Type")))
        (is (= (str "No data found for request-id " request-id "-unknown") body))))

    (testing "pending-request-id"
      (let [request {:request-method :get
                     :query-params {"request-id" request-id}
                     :uri "/async/status"}
            {:keys [body headers status]} (handler request)]
        (is (= 200 status))
        (is (= "text/plain" (get headers "Content-Type")))
        (is (str "Still processing request-id " request-id) body)))

    (testing "completed-request-id"
      (swap! async-requests assoc-in [request-id :done] true)
      (let [request {:request-method :get
                     :query-params {"request-id" request-id}
                     :uri "/async/status"}
            {:keys [body headers status]} (handler request)]
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
          {:keys [body headers status]} (handler request)]
      (is (= 200 status))
      (is (= "application/json" (get headers "Content-Type")))
      (is (= {"result" {"request-id" "my-test-request-id"}} (json/read-str body)))
      (is (nil? (get @async-requests request-id))))
    (swap! async-requests dissoc request-id)))

