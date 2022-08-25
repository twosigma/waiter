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
(ns waiter.liveness-check-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-invalid-backend-proto-liveness-check-proto-combo
  (testing-using-waiter-url
   (let [supported-protocols #{"http" "https" "h2c" "h2"}]
     (doseq [backend-proto supported-protocols
             liveness-check-proto (disj supported-protocols backend-proto)
             liveness-check-port-index [nil 0]]
       (let [request-headers (cond-> {:x-waiter-backend-proto backend-proto
                                      :x-waiter-liveness-check-proto liveness-check-proto
                                      :x-waiter-name (rand-name)}
                               liveness-check-port-index (assoc :x-waiter-liveness-check-port-index liveness-check-port-index))
             {:keys [body] :as response} (make-kitchen-request waiter-url request-headers)
             error-msg (str "The backend-proto (" backend-proto ") and liveness check proto (" liveness-check-proto
                            ") must match when liveness-check-port-index is zero")]
         (assert-response-status response http-400-bad-request)
         (is (str/includes? (str body) error-msg)))))))

(deftest ^:parallel ^:integration-fast test-temporarily-unready-instance
  (testing-using-waiter-url
   (when (using-k8s? waiter-url)
     (let [{:keys [cookies instance-id request-headers service-id] :as canary-response}
           (make-request-with-debug-info
            {:x-waiter-cmd (kitchen-cmd "--enable-status-change -p $PORT0")
             :x-waiter-concurrency-level 128
             :x-waiter-grace-period-secs 5
             :x-waiter-health-check-interval-secs 5
             :x-waiter-health-check-max-consecutive-failures 10
             :x-waiter-health-check-url "/default-status"           ; depends on kitchen's default-status-value
             :x-waiter-liveness-check-interval-secs 5
             :x-waiter-liveness-check-max-consecutive-failures 2
             :x-waiter-liveness-check-url "/bad-status?status=200"  ; always 200
             :x-waiter-max-instances 1
             :x-waiter-min-instances 1
             :x-waiter-name (rand-name)}
            #(make-kitchen-request waiter-url % :path "/hello"))
           check-filtered-instances (fn [target-url healthy-filter-fn]
                                      (let [instance-ids (->> (active-instances target-url service-id :cookies cookies)
                                                              (healthy-filter-fn :healthy?)
                                                              (map :id))]
                                        (and (= 1 (count instance-ids))
                                             (= instance-id (first instance-ids)))))]
       (assert-response-status canary-response http-200-ok)
       (with-service-cleanup
         service-id
         ; wait for instance to show as healthy on all routers
         (doseq [[_ router-url] (routers waiter-url)]
           (is (wait-for #(check-filtered-instances router-url filter)))
           (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
         ; set default-status-value to 400 to trigger failing health checks
         (let [request-headers (assoc request-headers
                                      :x-kitchen-default-status-timeout 45000
                                      :x-kitchen-default-status-value http-400-bad-request)
               response (make-request-with-debug-info request-headers #(make-kitchen-request waiter-url % :path "/hello"))]
           (assert-response-status response http-400-bad-request)
           (assert-backend-response response)
           (is (= instance-id (:instance-id response))))
         ; wait for instance to show as unhealthy on all routers
         (doseq [[_ router-url] (routers waiter-url)]
           (is (wait-for #(check-filtered-instances router-url remove) :timeout 60 :interval 5))
           (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
         ; issue a request that will be queued until service reports as healthy (waiting on default-status-timeout)
         (let [response (make-kitchen-request waiter-url request-headers :path "/hello")]
           (assert-response-status response http-200-ok))
         ; verify instance shows as healthy on all routers
         (doseq [[_ router-url] (routers waiter-url)]
           (is (wait-for #(check-filtered-instances router-url filter)))
           (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
         ; verify instance did not experience restarts
         (doseq [[_ router-url] (routers waiter-url)]
           (let [instances (active-instances router-url service-id :cookies cookies)]
             (is (= 1 (count instances)))
             (let [instance (first instances)]
               (is (zero? (:k8s/restart-count instance)))
               (is (= instance-id (:id instance)))))))))))

(deftest ^:parallel ^:integration-fast test-liveness-triggered-restart
  (testing-using-waiter-url
   (when (using-k8s? waiter-url)
     (let [{:keys [cookies instance-id request-headers service-id] :as canary-response}
           (make-request-with-debug-info
            {:x-waiter-cmd (kitchen-cmd "--enable-status-change -p $PORT0")
             :x-waiter-concurrency-level 128
             :x-waiter-grace-period-secs 5
             :x-waiter-health-check-interval-secs 5
             :x-waiter-health-check-max-consecutive-failures 10
             :x-waiter-health-check-url "/bad-status?status=200"  ; always 200
             :x-waiter-liveness-check-interval-secs 5
             :x-waiter-liveness-check-max-consecutive-failures 2
             :x-waiter-liveness-check-url "/default-status"       ; depends on kitchen's default-status-value
             :x-waiter-max-instances 1
             :x-waiter-min-instances 1
             :x-waiter-name (rand-name)}
            #(make-kitchen-request waiter-url % :path "/hello"))
           check-filtered-instances (fn [target-url healthy-filter-fn]
                                      (let [instance-ids (->> (active-instances target-url service-id :cookies cookies)
                                                              (healthy-filter-fn :healthy?)
                                                              (map :id))]
                                        (and (= 1 (count instance-ids))
                                             (= instance-id (first instance-ids)))))]
       (assert-response-status canary-response http-200-ok)
       (with-service-cleanup
         service-id
         ; wait for instance to show as healthy on all routers
         (doseq [[_ router-url] (routers waiter-url)]
           (is (wait-for #(check-filtered-instances router-url filter)))
           (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
         ; set default-status-value to 400 to trigger failing liveness checks
         (let [request-headers (assoc request-headers
                                      :x-kitchen-default-status-timeout 600000
                                      :x-kitchen-default-status-value http-400-bad-request)
               response (make-request-with-debug-info request-headers #(make-kitchen-request waiter-url % :path "/hello"))]
           (assert-response-status response http-400-bad-request)
           (assert-backend-response response)
           (is (= instance-id (:instance-id response))))
         ; wait for service to respond 200 (instance should restart before the default-status-timeout above)
         (is (wait-for #(= http-200-ok (:status (make-kitchen-request waiter-url request-headers :path "/hello")))
                       :timeout 120))
         ; verify instance did experience restarts
         (doseq [[_ router-url] (routers waiter-url)]
           (is (wait-for #(let [instances (active-instances router-url service-id :cookies cookies)]
                             (and (= 1 (count instances))
                                  (let [instance (first instances)]
                                    (and (pos? (:k8s/restart-count instance))
                                         (not= instance-id (:id instance))))))
                         :timeout 120))))))))

(deftest ^:parallel ^:integration-fast test-liveness-recovers-without-restart
  (testing-using-waiter-url
   (when (using-k8s? waiter-url)
     (let [{:keys [cookies instance-id request-headers service-id] :as canary-response}
           (make-request-with-debug-info
            {:x-waiter-cmd (kitchen-cmd "--enable-status-change -p $PORT0")
             :x-waiter-concurrency-level 128
             :x-waiter-grace-period-secs 5
             :x-waiter-health-check-interval-secs 5
             :x-waiter-health-check-max-consecutive-failures 10
             :x-waiter-health-check-url "/bad-status?status=200"  ; always 200
             :x-waiter-liveness-check-interval-secs 5
             :x-waiter-liveness-check-max-consecutive-failures 36
             :x-waiter-liveness-check-url "/default-status"       ; depends on kitchen's default-status-value
             :x-waiter-max-instances 1
             :x-waiter-min-instances 1
             :x-waiter-name (rand-name)}
            #(make-kitchen-request waiter-url % :path "/hello"))
           check-filtered-instances (fn [target-url healthy-filter-fn]
                                      (let [instance-ids (->> (active-instances target-url service-id :cookies cookies)
                                                              (healthy-filter-fn :healthy?)
                                                              (map :id))]
                                        (and (= 1 (count instance-ids))
                                             (= instance-id (first instance-ids)))))]
       (assert-response-status canary-response http-200-ok)
       (with-service-cleanup
         service-id
         ; wait for instance to show as healthy on all routers
         (doseq [[_ router-url] (routers waiter-url)]
           (is (wait-for #(check-filtered-instances router-url filter)))
           (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
         ; set default-status-value to 400 to trigger failing liveness checks
         (let [request-headers (assoc request-headers
                                      :x-kitchen-default-status-timeout 20000
                                      :x-kitchen-default-status-value http-400-bad-request)
               response (make-request-with-debug-info request-headers #(make-kitchen-request waiter-url % :path "/hello"))]
           (assert-response-status response http-400-bad-request)
           (assert-backend-response response)
           (is (= instance-id (:instance-id response))))
         ; wait for service to respond 200 (instance should recover after the default-status-timeout above)
         (is (wait-for #(= http-200-ok (:status (make-kitchen-request waiter-url request-headers :path "/hello")))
                       :timeout 35))
         ; verify instance did not experience restarts
         (doseq [[_ router-url] (routers waiter-url)]
           (let [instances (active-instances router-url service-id :cookies cookies)]
             (is (= 1 (count instances)))
             (let [instance (first instances)]
               (is (zero? (:k8s/restart-count instance)))
               (is (= instance-id (:id instance)))))))))))

(deftest ^:parallel ^:integration-fast test-liveness-explicit-proto-conflict-default-port-index
  (testing-using-waiter-url
   (when (using-k8s? waiter-url)
     (let [canary-response
           (make-request-with-debug-info
            {:x-waiter-cmd (kitchen-cmd "--enable-status-change -p $PORT0")
             :x-waiter-liveness-check-proto "https"
             :x-waiter-name (rand-name)}
            #(make-kitchen-request waiter-url % :path "/hello"))]
       (assert-response-status canary-response http-400-bad-request)))))
