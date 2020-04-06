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
(ns waiter.util.date-utils
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log])
  (:import (org.joda.time DateTime ReadablePeriod)))

(def formatter-iso8601 (:date-time f/formatters))
(def formatter-rfc822 (:rfc822 f/formatters))

(defn date-to-str
  ([^DateTime date-time]
   (date-to-str date-time formatter-iso8601))
  ([^DateTime date-time formatter]
   (when date-time
     (f/unparse
       (f/with-zone formatter t/utc)
       (.withZone date-time t/utc)))))

(defn str-to-date
  (^DateTime [date-str]
   (str-to-date date-str formatter-iso8601))
  (^DateTime [date-str formatter]
   (try
     (f/parse
       (f/with-zone formatter t/utc)
       date-str)
     (catch Exception ex
       (log/error "unable to parse" date-str "with formatter" formatter)
       (throw ex)))))

(defn str-to-date-safe
  "nil-safe str-to-date call"
  (^DateTime [date-str]
   (str-to-date-safe date-str formatter-iso8601))
  (^DateTime [date-str formatter]
    (when date-str (str-to-date date-str formatter))))

(defn time-seq
  "Returns a sequence of date-time values growing over specific period.
  Takes as input the starting value and the growing value, returning a
  lazy infinite sequence."
  [start ^ReadablePeriod period]
  (iterate (fn [^DateTime t] (.plus t period)) start))

(defn start-timer-task
  "Executes the callback function sequentially at specified intervals on an core.async thread.
   Returns a function that will cancel the timer when called."
  [interval-period callback-fn & {:keys [delay-ms] :or {delay-ms 0}}]
  (let [error-handler (fn start-timer-task-error-handler [ex]
                        (log/error ex (str "Exception in timer task.")))
        interval-ms (t/in-millis interval-period)
        cancel-promise (promise)]
    (async/go
      (when (pos? delay-ms)
        (async/<! (async/timeout delay-ms)))
      (loop []
        (when-not (realized? cancel-promise)
          (try
            (callback-fn)
            (catch Throwable th
              (error-handler th)))
          (async/<! (async/timeout interval-ms))
          (recur))))
    (fn cancel-timer-task []
      (deliver cancel-promise ::cancel))))

(defn older-than? [current-time duration {:keys [started-at]}]
  (if (and duration started-at)
    (t/after? current-time (t/plus started-at duration))
    false))

(defn interval->nanoseconds
  "Convert a time interval to a duration in nanoseconds."
  [interval]
  ;; clj-time.core doesn't have a in-nanos function.
  ;; We shouldn't need to worry about overflows here if our intervals are < 200 years.
  (-> interval t/in-millis (* 1e6)))

(defn max-time
  "Returns the max time of the two input time instances."
  [t1 t2]
  (cond
    (nil? t1) t2
    (nil? t2) t1
    :else (if (t/after? t1 t2)
            t1
            t2)))