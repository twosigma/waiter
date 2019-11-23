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
(ns waiter.statsd-support-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-header-metric-group
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-metric-group "waiter_test_foo"}
          {:keys [service-id] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          value (:metric-group (response->service-description waiter-url response))]
      (assert-response-status response 200)
      (is (= "waiter_test_foo" value))
      (delete-service waiter-url service-id))))

(defn statsd-enabled?
  [waiter-url]
  (not= "disabled" (:statsd (waiter-settings waiter-url))))

(deftest ^:parallel ^:integration-fast test-statsd-instance-metrics-aggregation
  (testing-using-waiter-url
    (when (statsd-enabled? waiter-url)
      (let
        [router-id->router-url (routers waiter-url)
         {:keys [cookies]} (make-request waiter-url "/waiter-auth")
         metric-group (str "test-" (rand-int 3000000))
         headers {:x-waiter-concurrency-level (count router-id->router-url)
                  :x-waiter-name (rand-name)
                  :x-waiter-metric-group metric-group}
         {:keys [service-id] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
        (assert-response-status response 200)
        (with-service-cleanup
          service-id
          ;; ensure all routers know about the service
          (assert-response-status response 200)
          ;; verify metric groups when statsd is enabled
          (doseq [[_ router-url] router-id->router-url]
            (let [response (make-request-with-debug-info headers #(make-kitchen-request router-url % :cookies cookies))]
              (assert-response-status response 200)
              (is (= service-id (:service-id response)))))
          (let [{:keys [metric-group]} (response->service-description waiter-url response)
                metric-group-keyword (keyword metric-group)
                {:keys [sync-instances-interval-ms]} (get (waiter-settings waiter-url) :statsd)]
            (wait-for
              (fn statsd-instance-metrics-predicate []
                (let [metric-group-gauges (-> waiter-url statsd-state :state :gauge metric-group-keyword)]
                  (log/info metric-group "counts gauges:" metric-group-gauges)
                  (every? #(contains? metric-group-gauges %)
                          [:cpus :instances.failed :instances.healthy :instances.unhealthy :mem])))
              :interval 1
              :timeout (-> sync-instances-interval-ms (quot 1000) (* 2)))))))))

(deftest ^:parallel ^:integration-fast test-statsd-disabled
  (testing-using-waiter-url
    (if (statsd-enabled? waiter-url)
      (log/info "Skipping statsd disabled assertion because statsd is turned on by Waiter")
      (let [headers {:x-waiter-name (rand-name)}
            cookies (all-cookies waiter-url)
            make-request (fn [url]
                           (make-request-with-debug-info headers #(make-kitchen-request url % :cookies cookies)))
            {:keys [router-id service-id status] :as response} (make-request waiter-url)]
        (assert-response-status response 200)
        (with-service-cleanup
          service-id
          (when (= 200 status)
            (let [router-url (router-endpoint waiter-url router-id)
                  cancellation-token (atom false)
                  background-requests (future
                                        (while (not @cancellation-token)
                                          (is (= 200 (:status (make-request router-url)))))
                                        (log/debug "Done sending background requests"))]
              (try
                (let [state (router-statsd-state waiter-url router-id)]
                  (log/debug "State after request:" state)
                  (is (= {} state)))
                (finally
                  (reset! cancellation-token true)
                  @background-requests)))))))))
