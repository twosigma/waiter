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
            [waiter.schema :as schema])
  (:import (com.codahale.metrics ConsoleReporter MetricFilter ScheduledReporter)
           (com.codahale.metrics.graphite Graphite GraphiteReporter GraphiteSender PickledGraphite)
           java.io.PrintStream
           java.net.InetSocketAddress
           java.util.concurrent.TimeUnit))

(defprotocol CodahaleReporter
  "A reporter for codahale metrics"

  (close! [this]
    "Stops the reporter and performs any cleanup")

  (state [this]
    "Returns the state of this reporter"))

(defn- make-metrics-filter
  "Create instance of com.codahale.metrics.MetricFilter from a regex"
  [filter-regex]
  (reify MetricFilter (matches [_ name _] (some? (re-matches filter-regex name)))))

(defn- make-codahale-reporter
  "Create a CodahaleReporter from a com.codahale.metrics.ScheduledReporter"
  [^ScheduledReporter scheduled-reporter state-atom period-ms]
  (.start scheduled-reporter period-ms TimeUnit/MILLISECONDS)
  (swap! state-atom assoc :run-state :started)
  (reify CodahaleReporter
    (close! [_] (.close scheduled-reporter) (swap! state-atom assoc :run-state :closed))
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
                              (.filter (make-metrics-filter filter-regex))
                              (.convertRatesTo TimeUnit/SECONDS)
                              (.convertDurationsTo TimeUnit/MILLISECONDS)
                              (.build))]
     [console-reporter state-atom])))

(defn console-reporter
  "Creates and starts a ConsoleReporter for codahale metrics"
  [config]
  (let [{:keys [filter-regex period-ms]} (validate-console-reporter-config config)
        [console-reporter state-atom] (make-console-reporter filter-regex)]
    (make-codahale-reporter console-reporter state-atom period-ms)))

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

(defn make-graphite-reporter
  "Creates a GraphiteReporter for metrics"
  ([filter-regex prefix host port pickled?]
   (let [addr (InetSocketAddress. ^String host ^int port)
         ^GraphiteSender graphite (if pickled?
                                    (PickledGraphite. addr)
                                    (Graphite. addr))]
     (make-graphite-reporter filter-regex prefix graphite)))
  ([filter-regex prefix ^GraphiteSender graphite]
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
         graphite-reporter (-> (GraphiteReporter/forRegistry metrics/default-registry)
                               (.filter (make-metrics-filter filter-regex))
                               (.prefixedWith prefix)
                               (.convertRatesTo TimeUnit/SECONDS)
                               (.convertDurationsTo TimeUnit/MILLISECONDS)
                               (.build ^GraphiteSender graphite-wrapper))]
     [graphite-reporter state-atom])))

(defn graphite-reporter
  "Creates and starts a GraphiteReporter for metrics"
  [config]
  (let [{:keys [filter-regex host period-ms pickled? port prefix]} (validate-graphite-reporter-config config)
        [graphite-reporter state-atom] (make-graphite-reporter filter-regex prefix host port pickled?)]
    (make-codahale-reporter graphite-reporter state-atom period-ms)))
