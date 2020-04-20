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
(ns waiter.response-headers-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]))

(defn- make-debug-kitchen-request
  [waiter-url headers]
  (make-request-with-debug-info headers #(make-kitchen-request waiter-url %)))

(deftest ^:parallel ^:integration-fast test-response-headers
  (testing-using-waiter-url
    (let [extra-headers {:x-waiter-name (rand-name)}
          {:keys [request-headers]} (make-kitchen-request waiter-url extra-headers)
          service-id (retrieve-service-id waiter-url request-headers)]
      (with-service-cleanup
        service-id
        (testing "Basic response headers test using endpoint"
          (let [{:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers :debug false)]
            (assert-response-status response http-200-ok)
            (is (every? #(not (str/blank? (get headers %))) required-response-headers) (str "Response headers: " headers))
            (is (every? #(str/blank? (get headers %)) (retrieve-debug-response-headers waiter-url)) (str headers))))

        (testing "Router-Id in response headers test using endpoint"
          (let [{:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers :debug true)]
            (assert-response-status response http-200-ok)
            (is (every? #(not (str/blank? (get headers %)))
                        (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
                (str headers))))

        (testing "Basic response headers with CID included test using endpoint"
          (let [test-cid "1234567890"
                extra-headers (assoc extra-headers :x-cid test-cid)
                {:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers :debug false)]
            (assert-response-status response http-200-ok)
            (is (= test-cid (get headers "x-cid")) (str headers))
            (is (every? #(not (str/blank? (get headers %))) required-response-headers) (str headers))
            (is (every? #(str/blank? (get headers %)) (retrieve-debug-response-headers waiter-url)) (str headers))))))))
