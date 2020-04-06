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
(ns waiter.health-check-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [waiter.util.client-tools :refer :all]
            [waiter.util.http-utils :as hu]))

(defn assert-ping-response
  [waiter-url health-check-protocol idle-timeout service-id response]
  (assert-response-status response 200)
  (let [{:keys [ping-response service-description service-state]}
        (some-> response :body json/read-str walk/keywordize-keys)]
    (is (seq service-description) (str service-description))
    (is (= (service-id->service-description waiter-url service-id) service-description))
    (if (nil? idle-timeout)
      (do
        (is (= "received-response" (get ping-response :result)) (str ping-response))
        (is (= (hu/backend-protocol->http-version health-check-protocol)
               (get-in ping-response [:headers :x-kitchen-protocol-version]))
            (str ping-response))
        (is (= "get" (get-in ping-response [:headers :x-kitchen-request-method])) (str ping-response))
        (is (:exists? service-state) (str service-state))
        (is (= service-id (:service-id service-state)) (str service-state))
        (is (contains? #{"Running" "Starting"} (:status service-state)) (str service-state)))
      (do
        (is (= "timed-out" (get ping-response :result)) (str ping-response))
        (is (= {:exists? true :healthy? false :service-id service-id :status "Starting"} service-state))))))

(defn run-ping-service-test
  [waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index]
  (let [headers (cond-> {:accept "application/json"
                         :x-waiter-cmd command
                         :x-waiter-debug true
                         :x-waiter-health-check-url "/status?include=request-info"
                         :x-waiter-name (rand-name)}
                  backend-proto (assoc :x-waiter-backend-proto backend-proto)
                  health-check-port-index (assoc :x-waiter-health-check-port-index health-check-port-index)
                  health-check-proto (assoc :x-waiter-health-check-proto health-check-proto)
                  idle-timeout (assoc :x-waiter-timeout idle-timeout)
                  num-ports (assoc :x-waiter-ports num-ports))
        {:keys [headers] :as response} (make-kitchen-request waiter-url headers :method :post :path "/waiter-ping")
        service-id (get headers "x-waiter-service-id")
        health-check-protocol (or health-check-proto backend-proto "http")]
    (with-service-cleanup
      service-id
      (assert-ping-response waiter-url health-check-protocol idle-timeout service-id response))))

(deftest ^:parallel ^:integration-fast test-basic-ping-service
  (testing-using-waiter-url
    (let [idle-timeout nil
          command (kitchen-cmd "-p $PORT0")
          backend-proto nil
          health-check-proto nil
          num-ports nil
          health-check-port-index nil]
      (run-ping-service-test waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index))))

(deftest ^:parallel ^:integration-fast test-ping-http-http-port0-timeout
  (testing-using-waiter-url
    (let [idle-timeout 20000
          command (kitchen-cmd "-p $PORT0 --start-up-sleep-ms 600000")
          backend-proto "http"
          health-check-proto "http"
          num-ports nil
          health-check-port-index nil]
      (run-ping-service-test waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index))))

(deftest ^:parallel ^:integration-fast test-ping-http-http-port0
  (testing-using-waiter-url
    (let [command (kitchen-cmd "-p $PORT0")
          backend-proto "http"
          health-check-proto "http"
          num-ports nil
          health-check-port-index nil
          idle-timeout nil]
      (run-ping-service-test waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index))))

(deftest ^:parallel ^:integration-fast test-ping-http-http-port2
  (testing-using-waiter-url
    (let [command (kitchen-cmd "-p $PORT2")
          backend-proto "http"
          health-check-proto "http"
          num-ports 3
          health-check-port-index 2
          idle-timeout nil]
      (run-ping-service-test waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index))))

(deftest ^:parallel ^:integration-fast test-ping-h2c-http-port0
  (testing-using-waiter-url
    (let [command (kitchen-cmd "-p $PORT0")
          backend-proto "h2c"
          health-check-proto "http"
          num-ports nil
          health-check-port-index nil
          idle-timeout nil]
      (run-ping-service-test waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index))))

(deftest ^:parallel ^:integration-fast test-ping-h2c-http-port2
  (testing-using-waiter-url
    (let [command (kitchen-cmd "-p $PORT2")
          backend-proto "h2c"
          health-check-proto "http"
          num-ports 3
          health-check-port-index 2
          idle-timeout nil]
      (run-ping-service-test waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index))))

(deftest ^:parallel ^:integration-fast test-ping-with-fallback-enabled
  (testing-using-waiter-url
    (let [token (rand-name)
          request-headers {:x-waiter-debug true
                           :x-waiter-token token}
          fallback-period-secs 300
          backend-proto "http"
          token-description-1 (-> (kitchen-request-headers :prefix "")
                                (assoc :backend-proto backend-proto
                                       :fallback-period-secs fallback-period-secs
                                       :health-check-url "/status?include=request-info"
                                       :idle-timeout-mins 1
                                       :name (str token "-v1")
                                       :permitted-user "*"
                                       :run-as-user (retrieve-username)
                                       :token token
                                       :version "version-1"))]
      (try
        (assert-response-status (post-token waiter-url token-description-1) 200)
        (let [ping-response-1 (make-request waiter-url "/waiter-ping" :headers request-headers)
              service-id-1 (get-in ping-response-1 [:headers "x-waiter-service-id"])]
          (with-service-cleanup
            service-id-1
            (assert-ping-response waiter-url backend-proto nil service-id-1 ping-response-1)
            (let [token-description-2 (assoc token-description-1 :name (str token "-v2") :version "version-2")
                  _ (assert-response-status (post-token waiter-url token-description-2) 200)
                  ping-response-2 (make-request waiter-url "/waiter-ping" :headers request-headers)
                  service-id-2 (get-in ping-response-2 [:headers "x-waiter-service-id"])]
              (is (not= service-id-1 service-id-2))
              (with-service-cleanup
                service-id-2
                (assert-ping-response waiter-url backend-proto nil service-id-2 ping-response-2)))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-ping-for-run-as-requester
  (testing-using-waiter-url
    (let [token (rand-name)
          request-headers {:x-waiter-debug true
                           :x-waiter-token token}
          backend-proto "http"
          token-description (-> (kitchen-request-headers :prefix "")
                              (assoc :backend-proto backend-proto
                                     :health-check-url "/status?include=request-info"
                                     :idle-timeout-mins 1
                                     :name token
                                     :permitted-user "*"
                                     :run-as-user "*"
                                     :token token
                                     :version "version-foo"))]
      (try
        (assert-response-status (post-token waiter-url token-description) 200)
        (let [ping-response (make-request waiter-url "/waiter-ping" :headers request-headers)
              service-id (get-in ping-response [:headers "x-waiter-service-id"])]
          (with-service-cleanup
            service-id
            (assert-ping-response waiter-url backend-proto nil service-id ping-response)
            (let [{:keys [permitted-user run-as-user]} (service-id->service-description waiter-url service-id)
                  current-user (retrieve-username)]
              (is (= current-user permitted-user))
              (is (= current-user run-as-user)))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-temporarily-unhealthy-instance
  (testing-using-waiter-url
    (let [{:keys [cookies instance-id request-headers service-id] :as canary-response}
          (make-request-with-debug-info
            {:x-waiter-cmd (kitchen-cmd "--enable-status-change -p $PORT0")
             :x-waiter-concurrency-level 128
             :x-waiter-health-check-interval-secs 5
             :x-waiter-health-check-max-consecutive-failures 10
             :x-waiter-min-instances 1
             :x-waiter-name (rand-name)}
            #(make-kitchen-request waiter-url % :path "/hello"))
          check-filtered-instances (fn [target-url healthy-filter-fn]
                                     (let [instance-ids (->> (active-instances target-url service-id :cookies cookies)
                                                          (healthy-filter-fn :healthy?)
                                                          (map :id))]
                                       (and (= 1 (count instance-ids))
                                            (= instance-id (first instance-ids)))))]
      (assert-response-status canary-response 200)
      (with-service-cleanup
        service-id
        (doseq [[_ router-url] (routers waiter-url)]
          (is (wait-for #(check-filtered-instances router-url filter)))
          (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
        (let [request-headers (assoc request-headers
                                :x-kitchen-default-status-timeout 20000
                                :x-kitchen-default-status-value 400)
              response (make-kitchen-request waiter-url request-headers :path "/hello")]
          (assert-response-status response 400))
        (doseq [[_ router-url] (routers waiter-url)]
          (is (wait-for #(check-filtered-instances router-url remove)))
          (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
        (let [response (make-kitchen-request waiter-url request-headers :path "/hello")]
          (assert-response-status response 200))
        (doseq [[_ router-url] (routers waiter-url)]
          (is (wait-for #(check-filtered-instances router-url filter)))
          (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))))))

(deftest ^:parallel ^:integration-fast test-standard-health-check-authentication
  (testing-using-waiter-url
    (when-not (using-marathon? waiter-url)
      (let [token (rand-name)
            request-headers {:x-waiter-debug true
                             :x-waiter-token token}
            backend-proto "http"
            token-description (-> (kitchen-request-headers :prefix "")
                                (assoc :backend-proto backend-proto
                                       :cmd (kitchen-cmd "--enable-health-check-authentication -p $PORT0")
                                       :health-check-authentication "standard"
                                       :health-check-interval-secs 5
                                       :health-check-max-consecutive-failures 4
                                       :health-check-url "/status"
                                       :idle-timeout-mins 1
                                       :name token
                                       :permitted-user "*"
                                       :run-as-user "*"
                                       :token token
                                       :version "version-foo"))]
        (try
          (assert-response-status (post-token waiter-url token-description) 200)
          (let [ping-response (make-request waiter-url "/waiter-ping" :headers request-headers :method :get)
                service-id (get-in ping-response [:headers "x-waiter-service-id"])]
            (is service-id (str ping-response))
            (with-service-cleanup
              service-id
              (let [backend-response (-> ping-response :body json/read-str walk/keywordize-keys :ping-response)]
                (assert-response-status backend-response 200)
                (is (= (str "Hello " (retrieve-username)) (-> backend-response :body str))))))
          (finally
            (delete-token-and-assert waiter-url token)))))))
