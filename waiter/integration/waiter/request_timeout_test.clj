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
(ns waiter.request-timeout-test
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [waiter.util.client-tools :refer :all])
  (:import java.util.concurrent.CountDownLatch))

(defmacro assert-failed-request [service-name response-body start-time-ms timeout-period-sec unhealthy-app?]
  `(let [end-time-ms# (System/currentTimeMillis)
         elapsed-time-secs# (t/in-seconds (t/millis (- end-time-ms# ~start-time-ms)))]
     (is (>= elapsed-time-secs# ~timeout-period-sec))
     (is (str/includes? ~response-body (str "After " ~timeout-period-sec " seconds, no instance available to handle request.")))
     (when ~unhealthy-app?
       (is (str/includes? ~response-body "Check that your service is able to start properly!")))
     (is (and (str/includes? ~response-body "outstanding-requests")
              (not (str/includes? ~response-body "outstanding-requests 0"))))
     (is (str/includes? ~response-body "requests-waiting-to-stream 0"))
     (is (str/includes? ~response-body "waiting-for-available-instance 1"))
     (is (str/includes? ~response-body ~service-name))
     (log/info "Ran assertions on timed-out request")))

(defn- make-request-verbose
  "Makes a request with the provided headers and cookies, and with verbose logging"
  [waiter-url request-headers cookies]
  (let [response (make-request waiter-url "/req"
                               :cookies cookies
                               :headers request-headers
                               :method :post
                               :verbose true)
        service-name (str (get-in response [:request-headers :x-waiter-name]))]
    (log/info "response:" (:body response))
    (assoc response :service-name service-name)))

(defn- make-request-and-assert-timeout
  [waiter-url request-headers start-time-ms timeout-period-sec & {:keys [cookies] :or {cookies {}}}]
  (let [{:keys [body service-name] :as response} (make-request-verbose waiter-url request-headers cookies)]
    (assert-response-status response 503)
    (assert-failed-request service-name body start-time-ms timeout-period-sec true)))

(defn- make-successful-request
  [waiter-url request-headers & {:keys [cookies endpoint] :or {cookies {}, endpoint "/req"}}]
  (let [response (make-request-with-debug-info
                   request-headers
                   #(make-request waiter-url endpoint
                                  :cookies cookies
                                  :headers %
                                  :method :post
                                  :verbose true))]
    (log/info "response: " (:body response))
    response))

(deftest ^:parallel ^:integration-fast test-backend-request-errors
  (testing-using-waiter-url
    (let [service-name (rand-name)
          headers {:x-waiter-name service-name}
          {:keys [service-id] :as first-response}
          (make-request-with-debug-info headers #(make-kitchen-request waiter-url % :method :get))]
      (assert-response-status first-response 200)
      (with-service-cleanup
        service-id
        (testing "backend request timeout"
          (let [timeout-period 2000
                extra-headers {:x-kitchen-delay-ms (+ 2000 timeout-period)
                               :x-waiter-debug "true"
                               :x-waiter-name service-name
                               :x-waiter-timeout timeout-period}
                {:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers)
                response-body (:body response)
                error-message (-> (waiter-settings waiter-url) :messages :backend-request-timed-out)]
            (assert-response-status response 504)
            (is error-message)
            (is (str/includes? response-body error-message))
            (is (not (str/blank? (get headers "server"))))
            (is (not (str/blank? (get headers "x-waiter-backend-id"))))
            (is (not (str/blank? (get headers "x-waiter-backend-host"))))
            (is (not (str/blank? (get headers "x-waiter-backend-port"))))
            (is (not (str/blank? (get headers "x-waiter-backend-proto"))))
            (is (not (str/blank? (get headers "x-cid"))))))

        (testing "backend request failed"
          (let [extra-headers {:x-kitchen-delay-ms 5000 ; sleep long enough to die before response
                               :x-waiter-debug "true"
                               :x-waiter-name service-name}
                {:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers :path "/die")
                response-body (:body response)
                error-message (-> (waiter-settings waiter-url) :messages :backend-request-failed)]
            (assert-response-status response 502)
            (is error-message)
            (is (str/includes? response-body error-message))
            (is (not (str/blank? (get headers "server"))))
            (is (not (str/blank? (get headers "x-waiter-backend-id"))))
            (is (not (str/blank? (get headers "x-waiter-backend-host"))))
            (is (not (str/blank? (get headers "x-waiter-backend-port"))))
            (is (not (str/blank? (get headers "x-waiter-backend-proto"))))
            (is (not (str/blank? (get headers "x-cid"))))))))))

; Marked explicit because it's flaky
(deftest ^:parallel ^:integration-fast ^:explicit test-request-queue-timeout-slow-start-app
  (testing-using-waiter-url
    (let [timeout-period-sec 10
          timeout-period-ms (t/in-millis (t/seconds timeout-period-sec))
          start-time-ms (System/currentTimeMillis)
          start-up-sleep-ms (* 2 timeout-period-ms)
          request-headers (walk/stringify-keys
                            (merge (kitchen-request-headers)
                                   {:x-waiter-name (rand-name)
                                    :x-waiter-cmd (kitchen-cmd (str "-p $PORT0 --start-up-sleep-ms " start-up-sleep-ms))
                                    :x-waiter-queue-timeout timeout-period-ms}))]
      (make-request-and-assert-timeout waiter-url request-headers start-time-ms timeout-period-sec)
      (delete-service waiter-url request-headers))))

; Marked explicit because it's flaky
; (is (str/includes? ~response-body "Check that your service is able to start properly!"))) fails
; instead, we get Waiter Error 503  After 10 seconds, no instance is available to handle request
(deftest ^:parallel ^:integration-slow ^:explicit test-request-queue-timeout-faulty-app
  (testing-using-waiter-url
    (log/info (str "request-queue-timeout-faulty-app: if we can't get an instance quickly (should take " (colored-time "~1 minute") ")"))
    (let [timeout-period-sec 10
          start-time-ms (System/currentTimeMillis)
          request-headers (walk/stringify-keys
                            {:x-waiter-name (rand-name)
                             :x-waiter-cpus 1
                             :x-waiter-mem 100
                             :x-waiter-version "a-version"
                             :x-waiter-cmd-type "shell"
                             :x-waiter-cmd "sleep 5"
                             :x-waiter-health-check-url "/status"
                             :x-waiter-queue-timeout (t/in-millis (t/seconds timeout-period-sec))})]
      (make-request-and-assert-timeout waiter-url request-headers start-time-ms timeout-period-sec)
      (delete-service waiter-url request-headers))))

; Marked explicit because it's flaky
(deftest ^:parallel ^:integration-slow ^:explicit test-request-queue-timeout-max-instances-limit
  ;; Limits the service to a single instance and then makes a long requests to it on each router.
  ;; Then we make a request with a small timeout and make sure it actually times out.
  (testing-using-waiter-url
    (let [router-id->router-url (routers waiter-url)
          num-routers (count router-id->router-url)
          long-request-period-ms (* 3 5000)
          long-request-started-latch (CountDownLatch. num-routers)
          long-request-ended-latch (CountDownLatch. num-routers)
          make-request-fn (fn make-request-fn [target-url extra-headers cookies]
                            (make-request-with-debug-info
                              extra-headers #(make-kitchen-request target-url % :cookies cookies)))
          extra-headers {:x-waiter-cmd (kitchen-cmd "-p $PORT0")
                         :x-waiter-concurrency-level num-routers
                         :x-waiter-max-instances 1
                         :x-waiter-min-instances 1
                         :x-waiter-name (rand-name)}
          {:keys [cookies instance-id service-id] :as response} (make-request-fn waiter-url extra-headers nil)]
      (assert-response-status response 200)
      (log/info "canary instance-id:" instance-id)
      (with-service-cleanup
        service-id
        (doseq [[_ router-url] (seq router-id->router-url)]
          (launch-thread
            (.countDown long-request-started-latch)
            (let [extra-headers (assoc extra-headers :x-kitchen-delay-ms long-request-period-ms)
                  response (make-request-fn router-url extra-headers cookies)]
              (assert-response-status response 200)
              (log/info "long request instance-id:" (:instance-id response))
              (is (= instance-id (:instance-id response))))
            (.countDown long-request-ended-latch)))
        (.await long-request-started-latch)
        (let [timeout-period-ms (/ long-request-period-ms 3)
              timeout-period-secs (-> timeout-period-ms t/millis t/in-seconds)
              extra-headers (assoc extra-headers :x-waiter-queue-timeout timeout-period-ms)
              _ (Thread/sleep timeout-period-ms)
              {:keys [body] :as response} (make-request-fn waiter-url extra-headers cookies)]
          (assert-response-status response 503)
          (is (str/includes?
                (str body)
                (str "After " timeout-period-secs " seconds, no instance available to handle request.")))
          (is (str/includes? (str body) service-id))
          (is (str/blank? (:instance-id response))))
        (.await long-request-ended-latch)))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-grace-period-with-tokens
  (testing-using-waiter-url
    (let [grace-period (t/minutes 2)
          startup-delay-ms (-> grace-period t/in-millis (* 0.75) long)
          token (rand-name)]
      (try
        (log/info "Creating token for" token)
        (let [response
              (post-token waiter-url {:cmd (kitchen-cmd (str "--port $PORT0 --start-up-sleep-ms "
                                                             startup-delay-ms))
                                      :version "not-used"
                                      :cpus 1
                                      :mem 1024
                                      :health-check-url "/status"
                                      :permitted-user "*"
                                      :token token
                                      :name (str "test" token)
                                      :grace-period-secs (t/in-seconds grace-period)
                                      :cmd-type "shell"})]
          (assert-response-status response 200))
        (log/info "Making request for" token)
        (let [{:keys [status service-id] :as response} (make-request-with-debug-info
                                                         {:x-waiter-token token}
                                                         #(make-request waiter-url "/req" :headers %))]
          (assert-response-status response 200)
          (when (= 200 status)
            (log/info "Verifying app grace period for" token)
            (let [instance-acquired-delay-ms (-> response
                                                 :headers
                                                 (get "x-waiter-get-available-instance-ns" -1)
                                                 Long/parseLong
                                                 (quot 1000000))] ; truncated nanos->millis
              (is (<= startup-delay-ms instance-acquired-delay-ms)
                  (str "Healthy instance was found in just " instance-acquired-delay-ms " ms (too short)")))
            (when (can-query-for-grace-period? waiter-url)
              (is (= (t/in-seconds grace-period) (service-id->grace-period waiter-url service-id)))))
          (delete-service waiter-url service-id))
        (finally
          (delete-token-and-assert waiter-url token))))))
