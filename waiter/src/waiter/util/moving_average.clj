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
(ns waiter.util.moving-average
  (:require [clojure.string :as str]))

(defprotocol MovingAverage
  "Data structure that computes moving averages."
  (register-tick [_] "Registers a tick")
  (insert-value [_ value] "Update the moving average with `value` and return a new MovingAverage instance.")
  (get-average [_] "Returns the moving average represented by this instance."))

(defrecord ConstantMovingAverage [current-average]
  MovingAverage
  (register-tick [_] nil)
  (insert-value [this _] this)
  (get-average [_] current-average))

(defn constant-moving-average
  [current-average]
  (->ConstantMovingAverage current-average))

(defrecord ExponentialMovingAverage [average-atom backoff-count-atom start-backoff
                                     base-backoff backoff-factor max-backoff]
  MovingAverage
  (register-tick [_]
    (swap! backoff-count-atom dec)
    (when (<= @backoff-count-atom 0)
      (reset! average-atom 1.0)))
  (insert-value [_ value]
    (let [backoff-amount (if (pos? value)
                           (* start-backoff (- 1.0 value))
                           (min max-backoff (* start-backoff backoff-factor)))
          start-backoff' (if (pos? value) base-backoff backoff-amount)
          new-average (if (pos? value) 1.0 0.0)]
      (->ExponentialMovingAverage (atom new-average) (atom start-backoff') start-backoff'
                                  base-backoff backoff-factor max-backoff)))
  (get-average [_]
    @average-atom))

(defn exponential-moving-average
  [base-backoff backoff-factor max-backoff]
  (->ExponentialMovingAverage (atom 1.0) (atom 0) 0
                              base-backoff backoff-factor max-backoff))

(defrecord WeightedMovingAverage [weight current-average]
  MovingAverage
  (register-tick [_] nil)
  (insert-value
    [_ value]
    (let [new-average (/ (+ value (* weight current-average))
                         (inc weight))]
      (->WeightedMovingAverage weight new-average)))

  (get-average [_] current-average))

(defn weighted-moving-average
  [weight current-average]
  (->WeightedMovingAverage weight current-average))

;; Computes the simple moving average over a defined window size.
(defrecord SlidingWindowMovingAverage [window-size data data-sum]
  MovingAverage
  (register-tick [_] nil)
  (insert-value
    [_ value]
    (if (< (count data) window-size)
      (->SlidingWindowMovingAverage
        window-size
        (conj data value)
        (+ data-sum value))
      (->SlidingWindowMovingAverage
        window-size
        (conj (vec (rest data)) value)
        (- (+ data-sum value) (first data)))))

  (get-average [_] (double (/ data-sum (count data)))))

(defn sliding-window-moving-average
  "Returns an instance of SlidingWindowMovingAverage that computes the simple moving average over a defined window size."
  [window-size initial-average]
  {:pre [(pos? window-size)]}
  (let [data (vec (repeat window-size initial-average))
        data-sum (* window-size initial-average)]
    (->SlidingWindowMovingAverage window-size data data-sum)))

;; Wraps a moving average with bounds for the maximum and minimum value for the average.
(defrecord BoundedMovingAverage [wrapped-moving-average max-average min-average]
  MovingAverage
  (register-tick [_] (register-tick wrapped-moving-average))
  (insert-value
    [_ value]
    (->BoundedMovingAverage
      (insert-value wrapped-moving-average value)
      max-average min-average))

  (get-average [_]
    (-> (get-average wrapped-moving-average)
      (min max-average)
      (max min-average))))

(defn bounded-moving-average
  "Returns an instance of BoundedMovingAverage that bounds the average retuned by the wrapped bounder average."
  [inner-moving-average max-average min-average]
  {:pre [(>= max-average min-average)]}
  (->BoundedMovingAverage inner-moving-average max-average min-average))

(clojure.test/deftest test-work-stealing-moving-average
  (doseq [recipient-mode ["stable" "whimsical"]]
    (println)
    (doseq [mode ["current-implementation"
                  "exponential-moving-average-1.0-1.2-10.0"
                  "exponential-moving-average-1.0-1.5-10.0"
                  "exponential-moving-average-1.0-2.0-10.0"
                  "exponential-moving-average-1.0-1.2-20.0"
                  "exponential-moving-average-1.0-1.5-20.0"
                  "exponential-moving-average-1.0-2.0-20.0"
                  "exponential-moving-average-1.0-1.2-40.0"
                  "exponential-moving-average-1.0-1.5-40.0"
                  "exponential-moving-average-1.0-2.0-40.0"
                  "simple-moving-average-0.00"
                  "simple-moving-average-0.10"
                  "simple-moving-average-0.20"
                  "simple-moving-average-0.30"
                  "simple-moving-average-0.40"
                  "simple-moving-average-0.50"
                  "simple-moving-average-0.60"
                  "weighted-moving-average-0.10"
                  "weighted-moving-average-0.20"
                  "weighted-moving-average-0.30"
                  "weighted-moving-average-0.40"
                  "weighted-moving-average-0.50"
                  "weighted-moving-average-0.70"
                  "weighted-moving-average-0.90"
                  "weighted-moving-average-0.95"
                  "weighted-moving-average-0.99"]]
      (let [offer-accepted-times (atom [])
            pending-request-iters 20 ;; a request that needs help shows up every pending-request-iters iterations
            offers-made-count (atom 0)
            helped-request-count (atom 0)
            request-delay-count (atom 0)
            max-iterations 100000
            initial-moving-average (cond
                                     (= mode "current-implementation")
                                     (constant-moving-average 1.0)
                                     (str/starts-with? mode "exponential-moving-average-")
                                     (let [[max-backoff backoff-factor base-backoff]
                                           (->> (str/split mode #"-")
                                             reverse
                                             (take 3)
                                             (map #(Double/parseDouble %)))]
                                       (exponential-moving-average base-backoff backoff-factor max-backoff))
                                     (str/starts-with? mode "simple-moving-average-")
                                     (let [min-value (Double/parseDouble (subs mode (- (count mode) 4)))]
                                       (bounded-moving-average (sliding-window-moving-average 100 1) 1 min-value))
                                     (str/starts-with? mode "weighted-moving-average-")
                                     (let [weight (Double/parseDouble (subs mode (- (count mode) 4)))]
                                       (weighted-moving-average weight 1)))]
        (loop [iteration 0
               moving-average initial-moving-average]
          (register-tick moving-average)
          (if (= iteration max-iterations)
            (println
              (format "%50s" (str mode ":" recipient-mode))
              {:iteration iteration
               :moving-average (format "%6.4f" (double (get-average moving-average)))
               :offers-made (format "%6d" @offers-made-count)
               :request-delay-average (if (and (= recipient-mode "stable") (pos? @helped-request-count))
                                        (format "%7.2f" (double (/ @request-delay-count @helped-request-count)))
                                        "    n/a")
               :requests-helped (format "%4d" @helped-request-count)
               :success-ratio (format "%6.4f" (double (double (/ @helped-request-count @offers-made-count))))}
              (take 20 @offer-accepted-times))
            (let [threshold (get-average moving-average)
                  pending-requests (double (/ iteration pending-request-iters))
                  helped-requests @helped-request-count
                  unhelped-requests (- pending-requests helped-requests)
                  requires-help? (> unhelped-requests 1)
                  send-help? (and requires-help? (< (rand) threshold))
                  help-accepted? (if (= recipient-mode "stable")
                                   (pos? (dec unhelped-requests))
                                   (< (rand) (double (/ 1 pending-request-iters))))
                  moving-average' (if send-help?
                                    (insert-value moving-average (if help-accepted? 1 0))
                                    moving-average)]
              (when send-help?
                (swap! offers-made-count inc))
              (when (and send-help? help-accepted?)
                (swap! offer-accepted-times conj iteration)
                (swap! helped-request-count inc)
                (swap! request-delay-count + (- iteration (* @helped-request-count pending-request-iters))))
              (comment
                println
                {:iteration iteration
                 :moving-average (get-average moving-average')
                 :offers-made @offers-made-count
                 :requests-helped @helped-request-count})
              (recur (inc iteration)
                     moving-average'))))))))