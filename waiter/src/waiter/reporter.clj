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
(ns waiter.reporter
  (:require [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [metrics.core :as metrics]
            [schema.core :as s]
            [waiter.schema :as schema]
            [waiter.util.date-utils :as du])
  (:import (com.codahale.metrics ConsoleReporter Counter Gauge Histogram Metered MetricFilter MetricRegistry
                                 ScheduledReporter Snapshot Timer Clock)
           (com.codahale.metrics.graphite Graphite GraphiteReporter GraphiteSender PickledGraphite)
           (java.io IOException PrintStream)
           java.net.InetSocketAddress
           (java.text DecimalFormat)
           java.util.concurrent.TimeUnit))

(defprotocol CodahaleReporter
  "A reporter for codahale metrics"

  (close! [this]
    "Stops the reporter and performs any cleanup")

  (report [this]
    "Forces the reporter to report current metrics")

  (start [this]
    "Start reporting metrics")

  (state [this]
    "Returns the state of this reporter"))

(defn- filter-regex->metric-filter
  "Create instance of com.codahale.metrics.MetricFilter from a regex"
  [filter-regex]
  (reify MetricFilter (matches [_ name _] (some? (re-matches filter-regex name)))))

(defn- scheduled-reporter->codahale-reporter
  "Create a CodahaleReporter from a com.codahale.metrics.ScheduledReporter"
  [^ScheduledReporter scheduled-reporter state-atom period-ms]
  (reify CodahaleReporter
    (close! [_] (.close scheduled-reporter) (swap! state-atom assoc :run-state :closed))
    (report [_] (.report scheduled-reporter))
    (start [_]
      (.start scheduled-reporter period-ms TimeUnit/MILLISECONDS)
      (swap! state-atom assoc :run-state :started))
    (state [_] @state-atom)))

(defn validate-console-reporter-config
  "Validates ConsoleReporter settings and sets defaults"
  [config]
  (s/validate {(s/required-key :filter-regex) s/Regex
               (s/required-key :period-ms) schema/positive-int
               s/Any s/Any} config))

(defn make-console-reporter
  "Creates a ConsoleReporter for codahale metrics"
  ([filter-regex] (make-console-reporter filter-regex nil))
  ([filter-regex ^PrintStream output]
   (let [state-atom (atom {:run-state :created})
         console-reporter (-> (cond-> (ConsoleReporter/forRegistry metrics/default-registry)
                                      output (.outputTo output))
                              (.filter (filter-regex->metric-filter filter-regex))
                              (.convertRatesTo TimeUnit/SECONDS)
                              (.convertDurationsTo TimeUnit/SECONDS)
                              (.build))]
     [console-reporter state-atom])))

(defn console-reporter
  "Creates and starts a ConsoleReporter for codahale metrics"
  [config]
  (let [{:keys [filter-regex period-ms]} (validate-console-reporter-config config)
        [console-reporter state-atom] (make-console-reporter filter-regex)
        codahale-reporter (scheduled-reporter->codahale-reporter console-reporter state-atom period-ms)]
    (start codahale-reporter)
    codahale-reporter))

(defn validate-graphite-reporter-config
  "Validates GraphiteReporter settings and sets defaults"
  [config]
  (s/validate {(s/required-key :filter-regex) s/Regex
               (s/required-key :host) s/Str
               (s/required-key :period-ms) schema/positive-int
               (s/required-key :pickled?) s/Bool
               (s/required-key :prefix) s/Str
               (s/required-key :port) schema/positive-int
               s/Any s/Any}
              (merge {:pickled? true} config)))

(defn- make-reporting-name
  "Concatenates elements to form a dotted name for reporting, eliding any null values or empty strings."
  [prefix & args]
  (MetricRegistry/name prefix (into-array args)))

(let [df (DecimalFormat. "#.####################")]
  (defn- format-value
    "Convert double to string to send to graphite server"
    [value]
    (.format df value)))

(defn- report-meter
  "Report values from a codahale meter"
  [prefix name ^Metered meter timestamp ^GraphiteSender graphite]
  (let [send #(.send graphite (make-reporting-name prefix name %1) (format-value (%2 meter)) timestamp)]
    (send "count" #(.getCount %))
    (send "m1_rate" #(.getOneMinuteRate %))
    (send "m5_rate" #(.getFiveMinuteRate %))
    (send "m15_rate" #(.getFifteenMinuteRate %))
    (send "mean_rate" #(.getMeanRate %))))

(defn- report-snapshot
  "Report values from a codahale snapshot"
  [prefix name ^Snapshot snapshot timestamp ^GraphiteSender graphite]
  (let [send #(.send graphite (make-reporting-name prefix name %1) (format-value (%2 snapshot)) timestamp)]
    (send "max" #(.getMax %))
    (send "mean" #(.getMean %))
    (send "min" #(.getMin %))
    (send "stddev" #(.getStdDev %))
    (send "p50" #(.getMedian %))
    (send "p75" #(.get75thPercentile %))
    (send "p95" #(.get95thPercentile %))
    (send "p98" #(.get98thPercentile %))
    (send "p99" #(.get99thPercentile %))
    (send "p999" #(.get999thPercentile %))))

(defn- report-timer
  "Report values from a codahale timer"
  [prefix name ^Timer timer timestamp ^GraphiteSender graphite]
  (let [snapshot (.getSnapshot timer)]
    (report-snapshot prefix name snapshot timestamp graphite)
    (report-meter prefix name timer timestamp graphite)))

(defn- report-histogram
  "Report values from a codahale histogram"
  [prefix name ^Histogram histogram timestamp ^GraphiteSender graphite]
  (let [snapshot (.getSnapshot histogram)]
    (.send graphite (make-reporting-name prefix name "count") (format-value (.getCount histogram)) timestamp)
    (report-snapshot prefix name snapshot timestamp graphite)))

(defn- report-counter
  "Report values from a codahale counter"
  [prefix name ^Counter counter timestamp ^GraphiteSender graphite]
  (.send graphite (make-reporting-name prefix name "count") (format-value (.getCount counter)) timestamp))

(defn- report-gauge
  "Report values from a codahale gauge"
  [prefix name ^Gauge gauge timestamp ^GraphiteSender graphite]
  (let [value (.getValue gauge)]
    (when value
      (.send graphite (make-reporting-name prefix name) (str value) timestamp))))

(defn- report-to-graphite
  "Report values from a codahale MetricRegistry"
  [^MetricRegistry registry prefix ^MetricFilter filter ^GraphiteSender graphite]
  (let [timestamp (.getTime (Clock/defaultClock))
        report (fn [reporting-fn values-map]
                 (doseq [[k v] values-map]
                   (reporting-fn prefix k v timestamp graphite)))]
    (try
      (when-not (.isConnected graphite)
        (.connect graphite))
      (report report-gauge (.getGauges registry filter))
      (report report-counter (.getCounters registry filter))
      (report report-histogram (.getHistograms registry filter))
      (report report-meter (.getMeters registry filter))
      (report report-timer (.getTimers registry filter))
      (.flush graphite)
      (catch IOException e
        (try
          (.close graphite)
          (catch IOException e
            (log/warn "Could not close GraphiteSender:" (.getMessage e))))
        (throw e)))))

(defn make-graphite-reporter
  "Creates a GraphiteReporter for metrics"
  ([period-ms filter-regex prefix host port pickled?]
   (let [addr (InetSocketAddress. ^String host ^int port)
         ^GraphiteSender graphite (if pickled?
                                    (PickledGraphite. addr)
                                    (Graphite. addr))]
     (make-graphite-reporter period-ms filter-regex prefix graphite)))
  ([period-ms filter-regex prefix ^GraphiteSender graphite]
   (let [state-atom (atom {:run-state :created})
         update-state (fn [event last-report-successful?]
                        (swap! state-atom assoc
                               event (t/now)
                               :failed-writes-to-server (.getFailures graphite)
                               :last-report-successful last-report-successful?))
         try-operation (fn [operation f failure-event]
                         (try (f)
                              (catch Exception e
                                (log/warn "GraphiteSender failed to" operation "with error:" (.getMessage e))
                                (update-state failure-event false)
                                (throw e))))
         graphite-wrapper (reify GraphiteSender
                            (connect [_]
                              (try-operation "connect" #(.connect graphite) :last-connect-failed-time))
                            (send [_ name value timestamp]
                              (try-operation "send" #(.send graphite name value timestamp) :last-send-failed-time))
                            (flush [_]
                              (try-operation "flush" #(.flush graphite) :last-flush-failed-time)
                              (update-state :last-reporting-time true))
                            (isConnected [_] (.isConnected graphite))
                            (getFailures [_] (.getFailures graphite))
                            (close [_] (.close graphite)))
         filter (filter-regex->metric-filter filter-regex)
         period-ms-period (t/millis period-ms)]
     (reify CodahaleReporter
       (close! [_] (.close graphite-wrapper) (swap! state-atom assoc :run-state :closed))
       (report [_] (report-to-graphite metrics/default-registry prefix filter graphite-wrapper))
       (start [_] (du/start-timer-task period-ms-period #(report _)))
       (state [_] @state-atom)))))

(defn graphite-reporter
  "Creates and starts a GraphiteReporter for metrics"
  [config]
  (let [{:keys [filter-regex host period-ms pickled? port prefix]} (validate-graphite-reporter-config config)
        codahale-reporter (make-graphite-reporter period-ms filter-regex prefix host port pickled?)]
    (start codahale-reporter)
    codahale-reporter))