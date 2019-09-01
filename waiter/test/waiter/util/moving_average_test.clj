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
(ns waiter.util.moving-average-test
  (:require [clojure.test :refer :all]
            [waiter.util.moving-average :refer :all]))

(deftest test-sliding-window-moving-average
  (let [moving-average-atom (atom (sliding-window-moving-average 20 1.0))]
    (is (= 1.00 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 1)
    (is (= 1.00 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 0)
    (is (= 0.95 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 0)
    (is (= 0.90 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 0)
    (is (= 0.85 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 0)
    (is (= 0.80 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 1)
    (is (= 0.80 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 0)
    (is (= 0.75 (get-average @moving-average-atom)))
    (swap! moving-average-atom insert-value 0)
    (is (= 0.70 (get-average @moving-average-atom)))))

(defrecord FixedMovingAverage [average]
  MovingAverage
  (insert-value [this _] this)
  (get-average [_] average))

(defn- create-bounded-and-get-average
  [fixed-average max-average min-average]
  (get-average
    (bounded-moving-average
      (->FixedMovingAverage fixed-average)
      max-average min-average)))

(deftest test-bounded-moving-average
  (let [max-average 1.0
        min-average 0.2]
    (is (= max-average (create-bounded-and-get-average 1.2 max-average min-average)))
    (is (= max-average (create-bounded-and-get-average 1.1 max-average min-average)))
    (is (= 1.0 (create-bounded-and-get-average 1.0 max-average min-average)))
    (is (= 0.9 (create-bounded-and-get-average 0.9 max-average min-average)))
    (is (= 0.8 (create-bounded-and-get-average 0.8 max-average min-average)))
    (is (= 0.7 (create-bounded-and-get-average 0.7 max-average min-average)))
    (is (= 0.6 (create-bounded-and-get-average 0.6 max-average min-average)))
    (is (= 0.5 (create-bounded-and-get-average 0.5 max-average min-average)))
    (is (= 0.4 (create-bounded-and-get-average 0.4 max-average min-average)))
    (is (= 0.3 (create-bounded-and-get-average 0.3 max-average min-average)))
    (is (= 0.2 (create-bounded-and-get-average 0.2 max-average min-average)))
    (is (= min-average (create-bounded-and-get-average 0.1 max-average min-average)))
    (is (= min-average (create-bounded-and-get-average 0.0 max-average min-average)))))
