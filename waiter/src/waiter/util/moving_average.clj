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
(ns waiter.util.moving-average)

(defprotocol MovingAverage
  "Data structure that computes moving averages."
  (insert-value [_ value] "Update the moving average with `value` and return a new MovingAverage instance.")
  (get-average [_] "Returns the moving average represented by this instance."))

;; Computes the simple moving average over a defined window size.
(defrecord SlidingWindowMovingAverage [window-size data data-sum]
  MovingAverage
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
