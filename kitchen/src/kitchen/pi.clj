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
(ns kitchen.pi
  (:import (java.util.concurrent ThreadLocalRandom)))

(defn inside-circle
  "Count the number of times out of iterations where
  a random point ([0,1],[0,1]) is inside the circle
  defined by x^2 + y^2 = 1"
  [iterations]
  (let [random (ThreadLocalRandom/current)]
    (count (filter #(<= % 1)
                   (take iterations
                         (repeatedly #(let [x (. random nextDouble)
                                            y (. random nextDouble)]
                                        (+ (* x x) (* y y)))))))))

(defn estimate-pi
  "Estimate pi using Monte Carlo"
  [iterations-per-thread threads]
  (let [inside (reduce + (pmap (fn [_] (inside-circle iterations-per-thread)) (range threads)))
        total-iterations (* iterations-per-thread threads)]
    {:inside inside
     :iterations total-iterations
     :pi-estimate (* 4 (/ inside total-iterations))}))

