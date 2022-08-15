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

;; (deftest ^:parallel ^:integration-fast test-temporarily-unhealthy-instance
;;   (testing-using-waiter-url
;;    (when (using-k8s? waiter-url)
;;      (let [{:keys [cookies instance-id request-headers service-id] :as canary-response}
;;            (make-request-with-debug-info
;;             {:x-waiter-cmd (kitchen-cmd "--enable-status-change -p $PORT0")
;;              :x-waiter-concurrency-level 128
;;              :x-waiter-health-check-interval-secs 5
;;              :x-waiter-health-check-max-consecutive-failures 10
;;              :x-waiter-health-check-url "/status-400"
;;              :x-waiter-liveness-check-url "/status"
;;              :x-waiter-min-instances 1
;;              :x-waiter-name (rand-name)}
;;             #(make-kitchen-request waiter-url % :path "/hello"))
;;            check-filtered-instances (fn [target-url healthy-filter-fn]
;;                                       (let [instance-ids (->> (active-instances target-url service-id :cookies cookies)
;;                                                               (healthy-filter-fn :healthy?)
;;                                                               (map :id))]
;;                                         (and (= 1 (count instance-ids))
;;                                              (= instance-id (first instance-ids)))))]
;;        (assert-response-status canary-response http-200-ok)
;;        (with-service-cleanup
;;          service-id
;;          (doseq [[_ router-url] (routers waiter-url)]
;;            (is (wait-for #(check-filtered-instances router-url filter)))
;;            (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
;;          (let [request-headers (assoc request-headers
;;                                       :x-kitchen-default-status-timeout 20000
;;                                       :x-kitchen-default-status-value http-400-bad-request)
;;                response (make-kitchen-request waiter-url request-headers :path "/hello")]
;;            (assert-response-status response http-400-bad-request))
;;          (doseq [[_ router-url] (routers waiter-url)]
;;            (is (wait-for #(check-filtered-instances router-url remove)))
;;            (is (= 1 (count (active-instances router-url service-id :cookies cookies)))))
;;          (let [response (make-kitchen-request waiter-url request-headers :path "/hello")]
;;            (assert-response-status response http-200-ok))
;;          (doseq [[_ router-url] (routers waiter-url)]
;;            (is (wait-for #(check-filtered-instances router-url filter)))
;;            (is (= 1 (count (active-instances router-url service-id :cookies cookies))))))))))
