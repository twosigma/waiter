;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
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

