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
(ns waiter.metrics-sync-integration-test
  (:require [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.utils :as utils]))

(defmacro assert-invalid-body
  "Asserts that sending the provided body results in the expected-msg being inside the response body. This is used
  to confirm waiter is validating the /metrics/external endpoint properly."
  [waiter-url req-body expected-msg]
  `(let [waiter-url# ~waiter-url
         req-body# ~req-body
         string-req-body# (utils/clj->json req-body#)
         expected-msg# ~expected-msg
         res# (make-request waiter-url# "/metrics/external" :method :post :body string-req-body#)
         res-body# (:body res#)]
     (assert-response-status res# http-400-bad-request)
     (is (.contains res-body# expected-msg#))))

(deftest ^:parallel ^:integration-fast test-external-metrics-validate
  (testing-using-waiter-url
    (let [cluster-name (retrieve-cluster-name waiter-url)
          metrics-payload
          {"cluster" cluster-name
           "service-metrics"
           {"s1" {"i1" {"updated-at" "2022-05-31T14:50:44.956Z"
                        "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                   "active-request-count" 0}}}}}]

      (testing "method must be POST"
        (let [{:keys [body status]} (make-request waiter-url "/metrics/external" :method :get)]
          (is (= status http-400-bad-request))
          (is (.contains body "Invalid request method. Only POST is supported.") body)))

      (testing "wrong cluster in payload causes a 400 response"
        (let [expected-msg "Metrics are for a different cluster."]
          (assert-invalid-body waiter-url (assoc-in metrics-payload ["cluster"] "different-cluster") expected-msg)))

      (testing "updated-at must be an ISO timestamp"
        (let [expected-msg "Invalid 's1.i1.updated-at' field. Must be ISO-8601 time."]
          (assert-invalid-body
            ; wrong time format
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "2022-05-31") expected-msg)
          (assert-invalid-body
            ; missing milliseconds
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "2022-05-31T02:50:44Z") expected-msg)
          (assert-invalid-body
            ; missing 'Z' character
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "2022-05-31T14:50:44.956") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "not-iso-string") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] 5) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "") expected-msg)))

      (testing "last-request-time must be an ISO timestamp"
        (let [expected-msg "Invalid 's1.i1.metrics.last-request-time' field. Must be ISO-8601 time."]
          (assert-invalid-body
            ; wrong time format
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "2022-05-31") expected-msg)
          (assert-invalid-body
            ; missing milliseconds
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "2022-05-31T02:50:44Z") expected-msg)
          (assert-invalid-body
            ; missing 'Z' character
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "2022-05-31T14:50:44.956") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "not-iso-string") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] 5) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "") expected-msg)))

      (testing "active-request-count must be a non negative integer"
        (let [expected-msg "Invalid 's1.i1.metrics.active-request-count' field. Must be non-negative integer."]
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "active-request-count"] -1) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "active-request-count"] "test") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "active-request-count"] "") expected-msg))))))

(defn send-metrics-and-assert-expected-metrics
  "Send metrics-payload to the waiter-url and assert that each router reports the expected-metrics as well as never
  reports any metrics for the expected-nil-keys-list."
  [waiter-url routers cookies metrics-payload expected-metrics expected-nil-keys-list & {:keys [fail-eagerly-on-nil-keys]
                                                                                         :or {fail-eagerly-on-nil-keys true}}]
  (let [metrics-response (make-request waiter-url "/metrics/external" :method :post :body (utils/clj->json metrics-payload)
                                       :headers {:content-type "application/json"})]
    (assert-response-status metrics-response http-200-ok)
    (is (= {"no-op" false}
           (-> metrics-response
               :body
               try-parse-json)))
    (log/info "expected metrics from routers:" {:expected-nil-keys-list expected-nil-keys-list
                                                :expected-metrics expected-metrics})
    (is (wait-for
          (fn []
            (every?
              (fn router-has-expected-metrics?-fn [[_ router-url]]
                (let [metrics-state-response (make-request router-url "/state/router-metrics" :cookies cookies
                                                           :headers {:content-type "application/json"})
                      actual-metrics (-> metrics-state-response
                                         :body
                                         try-parse-json
                                         (get-in ["state" "external-metrics"]))]
                  (log/info "metrics for router:" {:cur-metrics actual-metrics
                                                   :router router-url})

                  ; We expect these requests to succeed ALL the time, and if failed, we consider the wait-for assertion
                  ; to fail.
                  (assert-response-status metrics-state-response http-200-ok)

                  (let [provided-key-values-are-nil? (every? nil? (map #(get-in actual-metrics %) expected-nil-keys-list))
                        ; When deep diffing the expected metrics and the actual metrics returned. We expect that the actual
                        ; metrics is a super map of the expected metrics. This is because the expected metrics keys->values
                        ; should all be in the actual metrics
                        [only-in-expected only-in-actual in-both] (data/diff expected-metrics actual-metrics)]
                    (log/info "actual metrics from routers:" {:expected-nil-keys-values (map #(get-in actual-metrics %) expected-nil-keys-list)
                                                              :in-both in-both
                                                              :only-in-expected only-in-expected
                                                              :only-in-actual only-in-actual
                                                              :router-url router-url})
                    (when fail-eagerly-on-nil-keys
                      (is provided-key-values-are-nil? "Expected metrics to not have metrics for provided keys."))
                    (and
                      provided-key-values-are-nil?
                      (nil? only-in-expected)
                      (= in-both expected-metrics)))))
              routers))
          :interval 1 :timeout 5)
        "All Waiter routers never reported the expected metrics.")))

(deftest ^:parallel ^:integration-slow test-external-metrics-updates-metrics-syncer
  (testing-using-waiter-url
    (let [cluster-name (retrieve-cluster-name waiter-url)
          routers (routers waiter-url)]
      (testing "Empty body results in no-op"
        (let [req-body {"cluster" cluster-name}
              {:keys [body] :as response} (make-request waiter-url "/metrics/external"
                                                        :method :post
                                                        :body (utils/clj->json req-body)
                                                        :headers {:content-type "application/json"})]
          (assert-response-status response http-200-ok)
          (is (= {"no-op" true} (try-parse-json (str body))))))

      (testing "Metrics payload with only irrelevant external metrics will result in a no-op"
        (let [; s1 and i1 are never going to be an actual service-id or instance-id on the waiter routers. These metrics
              ; are expected to be filtered out.
              req-body {"cluster" cluster-name
                        "service-metrics" {"s1" {"i1" {"updated-at" "2022-05-31T14:50:44.956Z"
                                                       "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                                                  "active-request-count" 0}}}}}
              {:keys [body] :as response} (make-request waiter-url "/metrics/external"
                                                        :method :post
                                                        :body (utils/clj->json req-body)
                                                        :headers {:content-type "application/json"})]
          (assert-response-status response http-200-ok)
          (is (= {"no-op" true} (try-parse-json (str body))))))

      (let [; make sure raven does send external metrics for these services
            extra-headers {:content-type "application/json"
                           :x-waiter-env-raven_export_metrics "false"}
            extra-headers-1 (assoc extra-headers :x-waiter-name (rand-name))
            {:keys [cookies] :as canary-response-1} (make-request-with-debug-info extra-headers-1 #(make-kitchen-request waiter-url %))
            instance-id-1 (:instance-id canary-response-1)
            service-id-1 (:service-id canary-response-1)]
        (assert-response-status canary-response-1 http-200-ok)
        (with-service-cleanup
          service-id-1
          (let [extra-headers-2 (assoc extra-headers :x-waiter-name (rand-name))
                canary-response-2 (make-request-with-debug-info extra-headers-2 #(make-kitchen-request waiter-url %))
                instance-id-2 (:instance-id canary-response-2)
                service-id-2 (:service-id canary-response-2)]
            (assert-response-status canary-response-2 http-200-ok)
            (with-service-cleanup
              service-id-2
              (let [metrics-payload
                    {"cluster" cluster-name
                     "service-metrics"
                     {service-id-1 {instance-id-1 {"updated-at" "3000-05-31T14:50:44.956Z"
                                                   "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                                              "active-request-count" 1}}}
                      service-id-2 {instance-id-2 {"updated-at" "3000-05-31T14:50:44.956Z"
                                                   "metrics" {"last-request-time" "2022-05-31T14:00:50.103Z"
                                                              "active-request-count" 2
                                                              "extra-metadata-should-not-be-filtered" "any-value"}}}}}
                    expected-metrics (get metrics-payload "service-metrics")]

                (testing "Sending external metrics for multiple instances updates existing metrics. Extra metadata should not be filtered out."
                  (send-metrics-and-assert-expected-metrics waiter-url routers cookies metrics-payload expected-metrics []))

                (testing "Strictly later updated-at timestamp for instance metrics are stored. Unknown instances are ignored."
                  (let [metrics-payload-instance-1-updated
                        {"cluster" cluster-name
                         "service-metrics"
                         {service-id-1 {"this-instance-is-gibberish" {"updated-at" "3001-05-31T14:50:44.956Z"
                                                                      "metrics" {"last-request-time" "2022-05-01T14:50:44.956Z"
                                                                                 "active-request-count" 0}}
                                        instance-id-1 {"updated-at" "3001-05-31T14:50:44.956Z"
                                                       "metrics" {"last-request-time" "2022-05-01T14:50:44.956Z"
                                                                  "active-request-count" 0}}}
                          service-id-2 {"another-fake-instance-id" {"updated-at" "3000-05-31T14:50:44.800Z"
                                                                    "metrics" {"last-request-time" "2022-06-01T14:00:50.103Z"
                                                                               "active-request-count" 0}}

                                        ; instance-id-2 is attempting to update with stale metrics
                                        instance-id-2 {"updated-at" "3000-05-31T14:50:44.800Z"
                                                       "metrics" {"last-request-time" "2022-06-01T14:00:50.103Z"
                                                                  "active-request-count" 0}}}}}

                        ; We only expect instance-id-1 to get updated because it has a later 'updated-at' timestamp
                        ; compared to previous stored while instance-id-2 has a stale timestamp and should be discarded
                        expected-metrics-instance-1-updated
                        (assoc-in expected-metrics [service-id-1 instance-id-1]
                                  (get-in metrics-payload-instance-1-updated ["service-metrics" service-id-1 instance-id-1]))

                        ; We expect these keys list have nil values on each router
                        expected-nil-keys-list [[service-id-1 "this-instance-is-gibberish"]
                                                [service-id-2 "another-fake-instance-id"]]]
                    (send-metrics-and-assert-expected-metrics
                      waiter-url routers cookies metrics-payload-instance-1-updated expected-metrics-instance-1-updated
                      expected-nil-keys-list)))

                (testing "After service is killed, external metrics for that service should be discarded when new metrics are sent"
                  (let [metrics-payload-instance-1-updated
                        {"cluster" cluster-name
                         "service-metrics"
                         {service-id-1 {instance-id-1 {"updated-at" "3002-05-31T14:50:44.956Z"
                                                       "metrics" {"last-request-time" "2022-05-01T14:50:44.956Z"
                                                                  "active-request-count" 0}}}}}
                        expected-metrics-instance-1-updated (get metrics-payload-instance-1-updated "service-metrics")

                        ; service-id-2 was killed, and should not be tracked in external metrics
                        expected-nil-keys-list [[service-id-2 instance-id-2]]]
                    (delete-service waiter-url service-id-2)

                    ; wait for service to no longer be tracked by routers
                    (async/<!! (async/timeout 3000))
                    (send-metrics-and-assert-expected-metrics
                      waiter-url routers cookies metrics-payload-instance-1-updated expected-metrics-instance-1-updated
                      expected-nil-keys-list :fail-eagerly-on-nil-keys false)))))))))))
