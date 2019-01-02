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

(defn- make-codahale-reporter
  [^ScheduledReporter scheduled-reporter state period-ms]
  (.start scheduled-reporter period-ms TimeUnit/MILLISECONDS)
  (reify CodahaleReporter
    (close! [_] (.close scheduled-reporter) (reset! state :closed))
    (state [_] @state)))


(defn validate-console-reporter-config
  "Validates ConsoleReporter settings and sets defaults"
  [config]
  (s/validate {(s/required-key :period-ms) schema/positive-int
               (s/optional-key :filter-regex) s/Regex
               s/Any s/Any} config))

(defn make-console-reporter
  "Creates a ConsoleReporter for codahale metrics"
  ([filter-regex] (make-console-reporter filter-regex nil))
  ([filter-regex ^PrintStream output]
   (let [state (atom {})
         console-reporter (cond-> (ConsoleReporter/forRegistry metrics/default-registry)
                                  output (.outputTo output)
                                  filter-regex (.filter (make-metrics-filter filter-regex))
                                  true (-> (.convertRatesTo TimeUnit/SECONDS)
                                           (.convertDurationsTo TimeUnit/MILLISECONDS)
                                           (.build)))]
     [console-reporter state])))

(defn console-reporter
  "Creates and starts a ConsoleReporter for codahale metrics"
  [config]
  (let [{:keys [period-ms filter-regex]} (validate-console-reporter-config config)
        [console-reporter state] (make-console-reporter filter-regex)]
    (make-codahale-reporter console-reporter state period-ms)))



(defn validate-graphite-reporter-config
  "Validates GraphiteReporter settings and sets defaults"
  [config]
  (s/validate {(s/required-key :period-ms) schema/positive-int
               (s/optional-key :filter-regex) s/Regex
               (s/required-key :host) s/Str
               (s/optional-key :prefix) s/Str
               (s/required-key :port) schema/positive-int
               (s/required-key :pickled?) s/Bool
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
   (let [state (atom {})
         graphite-wrapper (reify GraphiteSender
                            (connect [_]
                              (try (.connect graphite)
                                   (catch Exception e
                                     (swap! state #(merge % {
                                                             :last-connect-failed-time (t/now)
                                                             :failed-writes-to-server (.getFailures graphite)
                                                             :last-report-successful false
                                                             }))
                                     (throw e))))
                            (send [_ a b c]
                              (try (.send graphite a b c)
                                   (catch Exception e
                                     (swap! state #(merge % {
                                                             :last-send-failed-time (t/now)
                                                             :failed-writes-to-server (.getFailures graphite)
                                                             :last-report-successful false
                                                             }))
                                     (throw e))))
                            (flush [_]
                              (try
                                (.flush graphite)
                                (catch Exception e
                                  (swap! state #(merge % {
                                                          :last-flush-failed-time (t/now)
                                                          :failed-writes-to-server (.getFailures graphite)
                                                          :last-report-successful false
                                                          }))
                                  (throw e)))
                              (swap! state #(merge % {
                                                      :last-reporting-time (t/now)
                                                      :failed-writes-to-server (.getFailures graphite)
                                                      :last-report-successful true
                                                      })))
                            (isConnected [_] (.isConnected graphite))
                            (getFailures [_] (.getFailures graphite))
                            (close [_] (.close graphite)))
         graphite-reporter (cond-> (GraphiteReporter/forRegistry metrics/default-registry)
                                   filter-regex (.filter (make-metrics-filter filter-regex))
                                   true (-> (.prefixedWith prefix)
                                            (.convertRatesTo TimeUnit/SECONDS)
                                            (.convertDurationsTo TimeUnit/MILLISECONDS)
                                            (.build ^GraphiteSender graphite-wrapper)))]
     [graphite-reporter state])))


(defn graphite-reporter
  "Creates and starts a GraphiteReporter for metrics"
  [config]
  (let [{:keys [period-ms filter-regex prefix host port pickled?]} (validate-graphite-reporter-config config)
        [graphite-reporter state] (make-graphite-reporter filter-regex prefix host port pickled?)]
    (make-codahale-reporter graphite-reporter state period-ms)))
