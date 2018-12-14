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
            [metrics.core :as metrics]
            [schema.core :as s]
            [waiter.schema :as schema]
            [clojure.tools.logging :as log])
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
  [filter-regex]
  (reify MetricFilter (matches [_ name _] (some? (re-matches filter-regex name)))))

(defn make-console-reporter
  "Creates a ConsoleReporter for codahale metrics"
  ([filter-regex] (make-console-reporter filter-regex nil))
  ([filter-regex ^PrintStream output]
   (cond-> (ConsoleReporter/forRegistry metrics/default-registry)
           output (.outputTo output)
           filter-regex (.filter (make-metrics-filter filter-regex))
           true (-> (.convertRatesTo TimeUnit/SECONDS)
                    (.convertDurationsTo TimeUnit/MILLISECONDS)
                    (.build)))))

(defn- start-console-reporter
  "Starts a ConsoleReporter for codahale metrics. Useful for testing."
  [^ConsoleReporter console-reporter period-ms]
  (.start console-reporter period-ms TimeUnit/MILLISECONDS))

(defn init-console-reporter
  "Creates and starts a ConsoleReporter for codahale metrics"
  [config]
  (let [{:keys [period-ms filter-regex]} (s/validate {(s/required-key :period-ms) schema/positive-int
                                                      (s/optional-key :filter-regex) s/Regex
                                                      s/Any s/Any} config)
        ^ConsoleReporter console-reporter (make-console-reporter filter-regex)]
    (start-console-reporter console-reporter period-ms)
    console-reporter))

(let [state (atom nil)]
  (defn make-graphite-reporter
    "Creates a GraphiteReporter for metrics"
    [filter-regex prefix host port pickled?]
    (let [addr (InetSocketAddress. ^String host ^int port)
          graphite (if pickled?
                     (PickledGraphite. addr)
                     (Graphite. addr))
          graphite-wrapper (reify GraphiteSender
                             (connect [_] (.connect graphite))
                             (send [_ a b c] (.send graphite a b c))
                             (flush [_]
                               (.flush graphite)
                               (reset! state {
                                              :last-reporting-time (t/now)
                                              }))
                             (isConnected [_] (.isConnected graphite))
                             (getFailures [_] (.getFailures graphite)))
          graphite-reporter (cond-> (GraphiteReporter/forRegistry metrics/default-registry)
                        filter-regex (.filter (make-metrics-filter filter-regex))
                        true (-> (.prefixedWith prefix)
                                 (.convertRatesTo TimeUnit/SECONDS)
                                 (.convertDurationsTo TimeUnit/MILLISECONDS)
                                 (.build ^GraphiteSender graphite-wrapper)))]
      (reify xxx)))

  (defn- start-graphite-reporter
    "Starts a GraphiteReporter for metrics. Useful for testing."
    [^GraphiteReporter graphite-reporter period-ms]
    (.start graphite-reporter period-ms TimeUnit/MILLISECONDS))

  (defn init-graphite-reporter
    "Creates and starts a GraphiteReporter for metrics"
    [config]
    (let [{:keys [period-ms filter-regex prefix host port pickled?]}
          (s/validate {(s/required-key :period-ms) schema/positive-int
                       (s/optional-key :filter-regex) s/Regex
                       (s/required-key :host) s/Str
                       (s/optional-key :prefix) s/Str
                       (s/required-key :port) schema/positive-int
                       (s/required-key :pickled?) s/Bool
                       s/Any s/Any}
                      (merge {:pickled? true} config))
          ^GraphiteReporter graphite-reporter (make-graphite-reporter filter-regex prefix host port pickled?)]
      (start-graphite-reporter graphite-reporter period-ms)
      graphite-reporter))

  (defn graphite-state
    "Returns the current state of the graphite metrics reporter"
    []
    @state))
