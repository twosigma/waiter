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
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [waiter.util.client-tools :refer :all]
            [waiter.util.http-utils :as hu]))

(defn run-ping-service-test
  [waiter-url idle-timeout command backend-proto health-check-proto num-ports health-check-port-index]
  (let [headers (cond-> {:accept "application/json"
                         :x-waiter-cmd command
                         :x-waiter-debug true
                         :x-waiter-env-kitchen_auth_disabled "1"
                         :x-waiter-health-check-url "/request-info"
                         :x-waiter-name (rand-name)}
                  backend-proto (assoc :x-waiter-backend-proto backend-proto)
                  health-check-port-index (assoc :x-waiter-health-check-port-index health-check-port-index)
                  health-check-proto (assoc :x-waiter-health-check-proto health-check-proto)
                  idle-timeout (assoc :x-waiter-timeout idle-timeout)
                  num-ports (assoc :x-waiter-ports num-ports))
        {:keys [headers] :as response} (make-kitchen-request waiter-url headers :method :post :path "/waiter-ping")
        service-id (get headers "x-waiter-service-id")]
    (with-service-cleanup
      service-id
      (assert-response-status response 200)
      (let [{:keys [ping-response service-state]}
            (-> (some-> response :body json/read-str)
              (update-in ["ping-response" "body"] #(some-> % json/read-str))
              walk/keywordize-keys)]
        (if (nil? idle-timeout)
          (let [health-check-protocol (or health-check-proto backend-proto "http")]
            (assert-response-status ping-response 200)
            (is (= (hu/backend-protocol->http-version health-check-protocol)
                   (get-in ping-response [:body :protocol-version]))
                (str ping-response))
            (is (= "get" (get-in ping-response [:body :request-method])) (str ping-response))
            (is (= {:exists? true :healthy? true :status "Running"} service-state)))
          (do
            (is (= "Health check request timed out!"
                   (get-in ping-response [:body :message]))
                (str ping-response))
            (is (= {:exists? true :healthy? false :status "Starting"} service-state))))))))

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
