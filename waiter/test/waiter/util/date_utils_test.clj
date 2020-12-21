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
(ns waiter.util.date-utils-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [waiter.util.date-utils :refer :all]))

(deftest test-start-timer-task
  (let [num-iterations 8
        interval-ms 100
        tolerance-ms 20
        interval-period (t/millis interval-ms)]
    (doseq [{:keys [fixed-delay? iteration-interval sleep-ms test-name]}
            [{:fixed-delay? nil :iteration-interval (+ interval-ms 50) :sleep-ms 50 :test-name "fixed-delay-implicit"}
             {:fixed-delay? true :iteration-interval (+ interval-ms 50) :sleep-ms 50 :test-name "fixed-delay-explicit"}
             {:fixed-delay? false :iteration-interval interval-ms :sleep-ms 50 :test-name "fixed-rate-explicit"}
             {:fixed-delay? false :iteration-interval 150 :sleep-ms 150 :test-name "fixed-rate-explicit-slow"}]]
      (testing test-name
        (let [call-times-atom (atom [])
              callback-fn (fn []
                            (Thread/sleep sleep-ms)
                            (swap! call-times-atom conj (System/currentTimeMillis)))
              cancel-handle (cond
                              (true? fixed-delay?)
                              (start-timer-task interval-period callback-fn :fixed-delay? true)
                              (false? fixed-delay?)
                              (start-timer-task interval-period callback-fn :fixed-delay? false)
                              :else
                              (start-timer-task interval-period callback-fn))]
          (Thread/sleep (* (+ iteration-interval tolerance-ms) num-iterations))
          (cancel-handle)
          (let [invocation-times @call-times-atom
                invocation-diffs (map -
                                      (drop 1 invocation-times)
                                      (drop-last 1 invocation-times))]
            (is (>= (+ num-iterations 2) (count invocation-times) num-iterations))
            (is (every? #(>= % (- interval-ms tolerance-ms)) invocation-diffs)
                (str {:invocation-diffs invocation-diffs
                      :invocation-times invocation-times}))))))))
