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
(ns waiter.metrics-syncer
  (:require [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.utils :as utils]))

(defmacro assert-invalid-body
  "Asserts that sending the provided body results in the expected-msg being inside the response body. This is used
  to confirm waiter is validating the /instance-metrics endpoint properly."
  [waiter-url req-body expected-msg]
  `(let [waiter-url# ~waiter-url
         req-body# ~req-body
         string-req-body# (utils/clj->json req-body#)
         expected-msg# ~expected-msg
         res# (make-request waiter-url# "/instance-metrics" :method :post :body string-req-body#)
         res-body# (:body res#)]
     (assert-response-status res# http-400-bad-request)
     (is (.contains res-body# expected-msg#))))

(deftest ^:parallel ^:integration-fast test-instance-metrics-validate
  (testing-using-waiter-url
    (let [metrics-payload {"s1" {"i1" {"updated-at" "2022-05-31T14:50:44.956Z"
                                              "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                                         "active-request-count" 0}}}}]

      (testing "method must be POST"
        (let [{:keys [body status]} (make-request waiter-url "/instance-metrics" :method :get)]
          (is (= status http-400-bad-request))
          (is (.contains body "Invalid request method. Only POST is supported.") body)))

      (testing "updated-at must be an ISO timestamp"
        (let [expected-msg "Invalid 's1.i1.updated-at' field. Must be ISO-8601 time."]
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "updated-at"] "not-iso-string") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "updated-at"] 5) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "updated-at"] "") expected-msg)))

      (testing "last-request-time must be an ISO timestamp"
        (let [expected-msg "Invalid 's1.i1.metrics.last-request-time' field. Must be ISO-8601 time."]
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "metrics" "last-request-time"] "not-iso-string") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "metrics" "last-request-time"] 5) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "metrics" "last-request-time"] "") expected-msg)))

      (testing "active-request-count must be a non negative integer"
        (let [expected-msg "Invalid 's1.i1.metrics.active-request-count' field. Must be non-negative integer."]
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "metrics" "active-request-count"] -1) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "metrics" "active-request-count"] "test") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["s1" "i1" "metrics" "active-request-count"] "") expected-msg))))))

(deftest ^:parallel ^:integration-slow test-instance-metrics-updates-metrics-syncer
  (testing-using-waiter-url
    (let [routers (routers waiter-url)]
      (testing "Empty body results in no-op"
        (let [{:keys [body] :as response} (make-request waiter-url "/instance-metrics" :method :post :body "{}")]
          (assert-response-status response http-200-ok)
          (is (= {"no-op" true} (try-parse-json (str body))))))

      (testing "Metrics payload with only irrelevant external metrics will result in a no-op"
        (let [
              ; s1.i1 are never going to be an actual service-id or instance-id on the waiter routers. These metrics are expected
              ; to be filtered out.
              req-body {"s1" {"i1" {"updated-at" "2022-05-31T14:50:44.956Z"
                                    "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                               "active-request-count" 0}}}}
              {:keys [body] :as response} (make-request waiter-url "/instance-metrics" :method :post :body (utils/clj->json req-body))]
          (assert-response-status response http-200-ok)
          (is (= {"no-op" true} (try-parse-json (str body))))))

      (testing "Sending external metrics for multiple instances and services updates the routers external metrics"
        )

      (testing "Irrelevant external metrics for instances not tracked by the router are discarded")

      (testing "Irrelevant external metrics for services not tracked by the router are discarded")

      (testing "Sending stale external metrics does not result in an update")

      (testing "Externally killing a service results in the external metrics for that service to be discorded"))))
