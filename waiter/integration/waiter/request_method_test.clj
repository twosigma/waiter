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
(ns waiter.request-method-test
  (:require [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-request-method
  (testing-using-waiter-url
    (let [lorem-ipsum "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
          service-name (rand-name)
          headers {:x-waiter-name service-name, :x-kitchen-echo "true"}
          {:keys [service-id] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
      (assert-response-status response 200)
      (with-service-cleanup
        service-id
        (is (= lorem-ipsum (:body (make-kitchen-request waiter-url headers :body lorem-ipsum :method :get))))
        (is (= lorem-ipsum (:body (make-kitchen-request waiter-url headers :body lorem-ipsum :method :post))))
        (is (= lorem-ipsum (:body (make-kitchen-request waiter-url headers :body lorem-ipsum :method :put))))))))
