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
(ns waiter.reporters-integration-test
  (:require [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]))

(defn- get-graphite-reporter-state
  [waiter-url cookies]
  (let [{:keys [body] :as response} (make-request waiter-url "/state/codahale-reporters" :method :get :cookies cookies)]
    (assert-response-status response http-200-ok)
    (-> body str try-parse-json (get-in ["state" "graphite"]))))

(defn- retrieve-graphite-reporter-last-event-time-ms
  [router-url cookies]
  (let [state (get-graphite-reporter-state router-url cookies)
        {:strs [last-connect-failed-time last-flush-failed-time last-reporting-time last-send-failed-time]} state
        last-event-time (reduce utils/nil-safe-max [last-connect-failed-time last-flush-failed-time last-reporting-time last-send-failed-time])]
    (some-> last-event-time du/str-to-date .getMillis)))

(defn- wait-for-period
  [period-ms fun]
  (let [wait-for-delay (/ period-ms 2)]
    (wait-for fun :interval wait-for-delay :timeout (* period-ms 2) :unit-multiplier 1)))

(deftest ^:parallel ^:integration-fast test-graphite-metrics-reporting
  (testing-using-waiter-url
    (let [cookies (all-cookies waiter-url)]
      (doseq [router-url (vals (routers waiter-url))]
        (let [{:keys [graphite]} (get-in (waiter-settings router-url :cookies cookies) [:metrics-config :codahale-reporters])]
          (when graphite
            (let [{:keys [period-ms]} graphite]
              (is (wait-for-period period-ms #(-> (get-graphite-reporter-state router-url cookies)
                                                (get "last-report-successful")
                                                some?)))
              (let [last-event-time-ms (retrieve-graphite-reporter-last-event-time-ms router-url cookies)
                    _ (is last-event-time-ms)
                    next-last-event-time-ms (wait-for-period
                                              period-ms
                                              #(let [next-last-event-time-ms (retrieve-graphite-reporter-last-event-time-ms router-url cookies)]
                                                 (when (not= next-last-event-time-ms last-event-time-ms)
                                                   next-last-event-time-ms)))
                    ;; expected precision for system "sleep" calls. a sleep call will sleep the right duration within 500 ms.
                    sleep_precision 2000]
                (is next-last-event-time-ms)
                (when next-last-event-time-ms
                  (is (< (Math/abs (- next-last-event-time-ms last-event-time-ms period-ms)) sleep_precision)))))))))))
