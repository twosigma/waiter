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
(ns waiter.test-metrics-test
  (:require [clojure.test :refer :all]))

(deftest test-2-assertions
  (testing "assertion 1"
    (is (= 2 (+ 1 1))))
  (testing "assertion 2"
    (is (= 4 (+ 2 2)))))

; failing test needs to be commented out when actually committed
;(deftest test-2-assertions-fail
;  (dotimes [_ 2]
;    (testing "assertion 1"
;      (is (= 3 (+ 1 1))))
;    (testing "assertion 2"
;      (is (= 5 (+ 2 2))))))

(deftest test-2-assertions-skip
  (dotimes [_ 0]
    (testing "assertion 1"
      (is (= 3 (+ 1 1))))
    (testing "assertion 2"
      (is (= 5 (+ 2 2))))))

(deftest test-2-slow-assertions
  (testing "assertion 1"
    (Thread/sleep 300)
    (is (= 2 (+ 1 1))))
  (testing "assertion 2"
    (Thread/sleep 300)
    (is (= 4 (+ 2 2)))))
