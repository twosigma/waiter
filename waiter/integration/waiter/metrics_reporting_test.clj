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
(ns waiter.metrics-reporting-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du])
  (:import [java.net ServerSocket]))

(defn- get-graphite-reporter-state
  [waiter-url]
  (let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)]
    (assert-response-status response 200)
    (-> body str try-parse-json (get-in ["state" "graphite"]))))

(defmacro ^:private wait-for-period
  [fun]
  `(wait-for ~fun :interval ~'wait-for-delay :timeout (* ~'period-ms 2) :unit-multiplier 1))

(defmacro ^:private check-events-within-period
  [successful?]
  (let [last-event-time (if successful? 'last-reporting-time 'last-connect-failed-time)
        last-event-time-ms (symbol (str last-event-time "-ms"))
        next-last-event-time-ms (symbol (str "next-" last-event-time-ms))]
    `(let [{:strs [~'last-report-successful ~last-event-time]} (get-graphite-reporter-state ~'waiter-url)
           _# (is (= ~'last-report-successful ~successful?))
           _# (is ~last-event-time)
           ~last-event-time-ms (-> ~last-event-time du/str-to-date .getMillis)
           ~next-last-event-time-ms (wait-for-period #(let [~next-last-event-time-ms (-> ~'waiter-url
                                                                                       get-graphite-reporter-state
                                                                                       (get ~(str last-event-time))
                                                                                       du/str-to-date
                                                                                       .getMillis)]
                                                       (if (not= ~next-last-event-time-ms ~last-event-time-ms)
                                                         ~next-last-event-time-ms nil)))]
       (is ~next-last-event-time-ms)
       (when ~next-last-event-time-ms
         (is (< (Math/abs (- ~next-last-event-time-ms ~last-event-time-ms ~'period-ms)) 100))))))

(deftest ^:parallel ^:integration-fast test-graphite-metrics-reporting
  (testing-using-waiter-url
    (let [{:keys [graphite]} (get-in (waiter-settings waiter-url) [:metrics-config :reporters])]
      (when graphite
        (let [{:keys [period-ms port]} graphite
              wait-for-delay (/ period-ms 2)]
          (is (wait-for-period #(-> waiter-url get-graphite-reporter-state (get "last-report-successful") some?)))
          (check-events-within-period false)
          (let [ctrl (async/chan)]
            (async/go (with-open [server-socket (ServerSocket. port)
                                  socket (.accept server-socket)]
                        (async/<! ctrl)))
            (wait-for-period #(-> waiter-url get-graphite-reporter-state (get "last-report-successful")))
            (check-events-within-period true)
            (async/>!! ctrl true)))))))