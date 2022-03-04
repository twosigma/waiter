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
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.core :as mc]
            [schema.core :as s]
            [waiter.metrics :as metrics]
            [waiter.schema :as schema]
            [waiter.util.date-utils :as du])
  (:import (com.codahale.metrics Clock ConsoleReporter MetricFilter MetricRegistry ScheduledReporter)
           (com.codahale.metrics.graphite Graphite GraphiteSender PickledGraphite)
           (java.io IOException PrintStream)
           (java.net InetSocketAddress)
           (java.text DecimalFormat)
           (java.util.concurrent TimeUnit)
           (org.coursera.metrics.datadog DatadogReporter)
           (org.coursera.metrics.datadog.transport Transport Transport$Request UdpTransport$Builder Transport$Request)))

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
  "Create a CodahaleReporter from a com.codahale.metrics.ScheduledReporter.
   The reporter name has to be passed in explicitly because ScheduledReporter does not provide a method to access the name."
  [^ScheduledReporter scheduled-reporter reporter-name state-atom period-ms]
  (reify CodahaleReporter
    (close! [_]
      (log/info "closing scheduled reporter" reporter-name)
      (.close scheduled-reporter)
      (swap! state-atom assoc :run-state :closed))
    (report [_]
      (.report scheduled-reporter))
    (start [_]
      (log/info "starting scheduled reporter" reporter-name)
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
         console-reporter (-> (cond-> (ConsoleReporter/forRegistry mc/default-registry)
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
        codahale-reporter (scheduled-reporter->codahale-reporter console-reporter "console-reporter" state-atom period-ms)]
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
               (s/optional-key :refresh-interval-ms) schema/non-negative-int
               s/Any s/Any}
              (merge {:pickled? true} config)))

(let [df (DecimalFormat. "#.####################")]
  (defn- format-value
    "Convert numbers to string to send to graphite server"
    [value]
    (if (number? value)
      (.format df value)
      (str value))))

(defn- report-to-graphite-helper
  "Recursively traverse metrics map and send to graphite"
  [prefix map-or-value ^GraphiteSender graphite timestamp]
  (if (map? map-or-value)
    (doseq [[k v] map-or-value]
      (report-to-graphite-helper (str prefix "." (str k)) v graphite timestamp))
    (.send graphite prefix (format-value map-or-value) timestamp)))

(defn- report-to-graphite
  "Report values from a codahale MetricRegistry"
  [^MetricRegistry registry prefix ^MetricFilter filter ^GraphiteSender graphite]
  (let [timestamp (/ (.getTime (Clock/defaultClock)) 1000) ;; Graphite expects timestamp in seconds
        map (metrics/metric-registry->metric-filter->metric-map
              registry filter :map-keys-fn #(str/replace (str %) "." "_"))]
    (try
      (when-not (.isConnected graphite)
        (log/info "Connecting to graphite server")
        (.connect graphite))
      (log/info "Sending metrics to graphite server")
      (report-to-graphite-helper prefix map graphite timestamp)
      (.flush graphite)
      (catch IOException e
        (try
          (.close graphite)
          (catch Throwable e
            (log/warn "Could not close GraphiteSender:" (.getMessage e))))
        (throw e)))))

(defn refresh-graphite-instance
  "Refreshes the Graphite instance inside the atom to pick up any updates to the host IP address."
  [graphite-atom host port pickled?]
  (let [socket-address (InetSocketAddress. ^String host ^int port)
        ^GraphiteSender graphite (if pickled?
                                   (PickledGraphite. socket-address)
                                   (Graphite. socket-address))]
    (log/info "graphite reporter address is" socket-address)
    (reset! graphite-atom graphite)))

(defn make-graphite-reporter
  "Creates a GraphiteReporter for metrics.
   When refresh-interval-ms is positive the graphite server host is refreshed periodically to pick up any IP address changes."
  ([period-ms filter-regex prefix host port pickled? refresh-interval-ms]
   (let [graphite-atom (atom nil)
         retrieve-graphite-instance (fn retrieve-graphite-instance [] (deref graphite-atom))]
     (refresh-graphite-instance graphite-atom host port pickled?)
     (when (pos? refresh-interval-ms)
       (log/info "refreshing graphite host ip every" refresh-interval-ms "ms")
       (du/start-timer-task (t/millis refresh-interval-ms) #(refresh-graphite-instance graphite-atom host port pickled?)
                            :delay-ms refresh-interval-ms))
     (make-graphite-reporter period-ms filter-regex prefix retrieve-graphite-instance)))
  ([period-ms filter-regex prefix retrieve-graphite-instance]
   (let [state-atom (atom {:run-state :created})
         update-state (fn [event last-report-successful?]
                        (swap! state-atom assoc
                               event (t/now)
                               :failed-writes-to-server (.getFailures (retrieve-graphite-instance))
                               :last-report-successful last-report-successful?))
         try-operation (fn [operation f failure-event]
                         (try (f)
                              (catch Exception e
                                (log/warn "GraphiteSender failed to" operation "with error:" (.getMessage e))
                                (update-state failure-event false)
                                (throw e))))
         graphite-wrapper (reify GraphiteSender
                            (connect [_]
                              (try-operation "connect" #(.connect (retrieve-graphite-instance)) :last-connect-failed-time))
                            (send [_ name value timestamp]
                              (try-operation "send" #(.send (retrieve-graphite-instance) name value timestamp) :last-send-failed-time))
                            (flush [_]
                              (try-operation "flush" #(.flush (retrieve-graphite-instance)) :last-flush-failed-time)
                              (update-state :last-reporting-time true))
                            (isConnected [_] (.isConnected (retrieve-graphite-instance)))
                            (getFailures [_] (.getFailures (retrieve-graphite-instance)))
                            (close [_] (.close (retrieve-graphite-instance))))
         filter (filter-regex->metric-filter filter-regex)
         period-ms-period (t/millis period-ms)]
     (reify CodahaleReporter
       (close! [_]
         (.close graphite-wrapper)
         (swap! state-atom assoc :run-state :closed))
       (report [_]
         (report-to-graphite mc/default-registry prefix filter graphite-wrapper))
       (start [_]
         (du/start-timer-task period-ms-period #(report _)))
       (state [_]
         @state-atom)))))

(defn graphite-reporter
  "Creates and starts a GraphiteReporter for metrics"
  [config]
  (let [valid-config (validate-graphite-reporter-config config)
        {:keys [filter-regex host period-ms pickled? port prefix refresh-interval-ms] :or {refresh-interval-ms 600000}} valid-config
        codahale-reporter (make-graphite-reporter period-ms filter-regex prefix host port pickled? refresh-interval-ms)]
    (start codahale-reporter)
    codahale-reporter))

(defn validate-datadog-reporter-config
  "Validates DatadogReporter settings and sets defaults."
  [config]
  (s/validate {(s/required-key :filter-regex) s/Regex
               (s/required-key :host) s/Str
               (s/required-key :period-ms) schema/positive-int
               (s/required-key :port) schema/positive-int
               (s/required-key :prefix) s/Str
               (s/required-key :retrying-lookup) s/Bool
               (s/required-key :tags) [schema/non-empty-string]
               s/Any s/Any}
              config))

(defn make-datadog-transport
  "Creates the transport layer for pushing metrics to datadog."
  [state-atom reporter-name host port retrying-lookup]
  (let [udp-transport (-> (UdpTransport$Builder.)
                        (.withPort port)
                        (.withRetryingLookup retrying-lookup)
                        (.withStatsdHost host)
                        (.build))]
    (reify Transport
      (prepare [_]
        (let [udp-request (.prepare udp-transport)]
          (reify Transport$Request
            (addCounter [_ c]
              (.addCounter udp-request c))
            (addGauge [_ g]
              (.addGauge udp-request g))
            (send [_]
              (try
                (.send udp-request)
                (swap! state-atom assoc :last-send-success-time (t/now))
                (catch Exception ex
                  (log/warn ex "failed to send metrics for" reporter-name)
                  (swap! state-atom assoc :last-send-failed-time (t/now))
                  (throw ex))))))))))

(defn make-datadog-reporter
  "Creates a DatadogReporter for metrics.
   When retrying-lookup is true, the datadog server host is refreshed periodically to pick up any IP address changes."
  [period-ms filter-regex prefix host port tags retrying-lookup]
  (let [state-atom (atom {:run-state :created})
        registry mc/default-registry
        reporter-name "datadog-reporter"
        transport (make-datadog-transport state-atom reporter-name host port retrying-lookup)
        filter (filter-regex->metric-filter filter-regex)
        datadog-reporter (-> (DatadogReporter/forRegistry registry)
                           (.filter filter)
                           (.withHost host)
                           (.withPrefix prefix)
                           (.withTags tags)
                           (.withTransport transport)
                           (.build))]
    (scheduled-reporter->codahale-reporter datadog-reporter reporter-name state-atom period-ms)))

(defn datadog-reporter
  "Creates and starts a DatadogReporter for metrics"
  [config]
  (let [valid-config (validate-datadog-reporter-config config)
        {:keys [filter-regex host period-ms port prefix retrying-lookup tags]} valid-config
        codahale-reporter (make-datadog-reporter period-ms filter-regex prefix host port tags retrying-lookup)]
    (start codahale-reporter)
    codahale-reporter))
