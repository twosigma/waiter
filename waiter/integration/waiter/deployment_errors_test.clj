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
(ns waiter.deployment-errors-test
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]))

(defn get-router->service-state
  "Retrieves service state for a service."
  [waiter-url service-id]
  (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]
    (->> (routers waiter-url)
         (map (fn [[_ router-url]]
                (service-state router-url service-id :cookies cookies)))
         (pc/map-from-vals :router-id))))

(defn formatted-service-state
  "Generates a formatted representation of service state."
  [waiter-url service-id]
  (str "router->service-state:\n" (with-out-str
                                    (pprint/pprint
                                      (get-router->service-state waiter-url service-id)))))

(defn deployment-error->str
  "Converts a deployment error keyword to the associated error message."
  [waiter-url deployment-error]
  (str "Deployment error: " (-> (waiter-settings waiter-url)
                                :messages
                                deployment-error)))

(defmacro assert-deployment-error
  "Asserts that the given response has a status of 503 and body with the error message
  associated with the deployment error."
  [response deployment-error]
  `(let [response# ~response
         body# (:body response#)
         service-id# (response->service-id response#)]
     (assert-response-status response# http-503-service-unavailable)
     (is (str/includes? body# (deployment-error->str ~'waiter-url ~deployment-error))
         (formatted-service-state ~'waiter-url service-id#))
     (testing "status is reported as failing"
       (is
         (wait-for
           (fn []
             (let [service-settings# (service-settings ~'waiter-url service-id#)]
               (= "Failing" (get service-settings# :status))))
           :interval 2 :timeout 30)))))

(deftest ^:parallel ^:integration-slow test-invalid-health-check-response
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-grace-period-secs 45
                   ; health check endpoint always returns status 402
                   :x-waiter-health-check-url "/bad-status?status=402"
                   :x-waiter-health-check-interval-secs 5
                   :x-waiter-health-check-max-consecutive-failures 1
                   :x-waiter-queue-timeout 600000}
          response (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
      (with-service-cleanup
        (response->service-id response)
        (assert-deployment-error response :invalid-health-check-response)))))

(deftest ^:parallel ^:integration-slow test-cannot-connect
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; nothing to connect to
                   :x-waiter-cmd "sleep 3600"
                   :x-waiter-grace-period-secs 15
                   :x-waiter-health-check-interval-secs 5
                   :x-waiter-health-check-max-consecutive-failures 1
                   :x-waiter-queue-timeout 600000}
          response (make-request-with-debug-info headers #(make-shell-request waiter-url %))]
      (with-service-cleanup
        (response->service-id response)
        (assert-deployment-error response :cannot-connect)))))

(deftest ^:parallel ^:integration-slow test-health-check-timed-out
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; health check endpoint sleeps for 300000 ms (= 5 minutes)
                   :x-waiter-health-check-url "/sleep?sleep-ms=300000&status=400"
                   :x-waiter-grace-period-secs 45
                   :x-waiter-health-check-interval-secs 5
                   :x-waiter-health-check-max-consecutive-failures 1
                   :x-waiter-queue-timeout 600000}
          response (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
      (with-service-cleanup
        (response->service-id response)
        (assert-deployment-error response :health-check-timed-out)))))

(deftest ^:parallel ^:integration-fast test-health-check-requires-authentication
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-health-check-url "/bad-status?status=401"}
          response (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
      (with-service-cleanup
        (response->service-id response)
        (assert-deployment-error response :health-check-requires-authentication)))))

; Marked explicit because not all servers use cgroups to limit memory (this error is not reproducible on testing platforms)
(deftest ^:parallel ^:integration-fast ^:explicit test-not-enough-memory
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-mem 1}
          response (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
      (with-service-cleanup
        (response->service-id response)
        (assert-deployment-error response :not-enough-memory)))))

(deftest ^:parallel ^:integration-fast test-bad-startup-command
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; misspelled command (invalid prefix asdf)
                   :x-waiter-cmd (str "asdf" (kitchen-cmd "-p $PORT0"))}
          response (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
      (with-service-cleanup
        (response->service-id response)
        (assert-deployment-error response :bad-startup-command)))))
