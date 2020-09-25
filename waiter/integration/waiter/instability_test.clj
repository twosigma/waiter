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
(ns waiter.instability-test
  (:require [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-oom-instability
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-mem 100}
          response (make-request-with-debug-info headers #(make-kitchen-request waiter-url % :path "/oom-instability"))]
      (wait-for #(= "not-enough-memory" ((((service-state waiter-url (response->service-id response))
                                            :state) :responder-state) :instability-issue)))
      (assert-response-status response http-502-bad-gateway)
      (is (= "not-enough-memory" ((((service-state waiter-url (response->service-id response))
                                     :state) :responder-state) :instability-issue))))))
