;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.util.date-utils
  (:require [chime :as chime]
            [clj-time.core :as t]
            [clj-time.format :as f]
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

(defn time-seq
  "Returns a sequence of date-time values growing over specific period.
  Takes as input the starting value and the growing value, returning a
  lazy infinite sequence."
  [start ^ReadablePeriod period]
  (iterate (fn [^DateTime t] (.plus t period)) start))

(defn start-timer-task
  "Executes the callback functions sequentially as specified intervals. Returns
  a function that will cancel the timer when called."
  [interval-period callback-fn & {:keys [delay-ms] :or {delay-ms 0}}]
  (chime/chime-at
    (time-seq (t/plus (t/now) (t/millis delay-ms)) interval-period)
    (fn [_] (callback-fn))
    {:error-handler (fn [ex] (log/error ex (str "Exception in timer task.")))}))

(defn older-than? [current-time duration {:keys [started-at]}]
  (if (and duration started-at)
    (t/after? current-time (t/plus started-at duration))
    false))


