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
(ns waiter.statsd
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [waiter.metrics :as metrics]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (clojure.lang PersistentQueue)
           (java.net InetAddress DatagramPacket DatagramSocket)))

(defn sanitize
  "Replaces all non-alphanumeric / dash /
  underscore characters with an underscore
  to clean them up for use in Statsd"
  [s]
  (str/replace s #"[^a-zA-Z\d\-_]" "_"))

(let [config (atom nil)
      socket-agent (agent nil)
      environment (atom nil)
      cluster (atom nil)
      server (atom nil)
      publish-interval-ms (atom 0)
      histogram-max-size (atom 10000)]

  (defn init-configuration
    "Initializes the Statsd socket agent and the config map"
    [host port & opts]
    (send socket-agent #(or % (DatagramSocket.)))
    (swap! config #(or % (merge {:host (InetAddress/getByName host)
                                 :port (if (integer? port) port (Integer/parseInt port))}
                                (apply hash-map opts)))))

  (defn send-packet
    "Sends a single UDP packet with the payload returned by
    the format-stat-fn function"
    [^DatagramSocket socket format-stat-fn]
    (try
      (let [^String stat (format-stat-fn)]
        (doto socket (.send (DatagramPacket.
                              ^bytes (.getBytes stat)
                              ^Integer (count stat)
                              ^InetAddress (:host @config)
                              ^Integer (:port @config)))))
      (catch Throwable e
        (log/warn e "Error attempting to send statsd packet using socket" socket)
        socket)))

  (defn- send-stat
    [format-stat-fn]
    (send-off socket-agent send-packet format-stat-fn))

  (defn- publish
    [content-fn]
    (send-stat #(str (:prefix @config) (content-fn))))

  (defn metric-path
    "Formats the complete Statsd metric path, e.g. metric_group.prod.prod1.server.blarg"
    [root-node metric]
    (str (or root-node "unknown") "." @environment "." @cluster "." @server "." metric))

  (defn metric-path-without-server
    "Formats the complete Statsd metric path without the server component,
    e.g. metric_group.prod.prod1.blarg"
    [root-node metric]
    (str (or root-node "unknown") "." @environment "." @cluster "." metric))

  (defn- increment
    "Publishes a Statsd counter metric"
    [key-fn value]
    (publish #(format "%s:%s|c" (key-fn) value)))

  (defn timing
    "Publishes a Statsd timing metric"
    [key-fn value]
    (publish #(format "%s:%d|ms" (key-fn) value)))

  (defn gauge
    "Publishes a Statsd gauge metric"
    [key-fn value]
    (publish #(format "%s:%.1f|g" (key-fn) (float value))))

  (defn- unique
    "Publishes a Statsd set metric"
    [metric-group metric value]
    (publish #(format "%s:%d|s" (metric-path-without-server metric-group metric) value)))

  (let [aggregation-agent (agent {})]

    (defn teardown
      "Clears all fields"
      []
      (swap! environment (constantly nil))
      (swap! cluster (constantly nil))
      (swap! config (constantly nil))
      (when-let [error (agent-error socket-agent)]
        (log/warn error "Socket agent is in error")
        (restart-agent socket-agent nil))
      (send socket-agent (constantly nil))
      (await socket-agent)
      (send aggregation-agent (constantly {}))
      (await aggregation-agent))

    (defn- publish-value
      [[[metric-group metric {:keys [publish-fn]}] value]]
      (try
        (publish-fn metric-group metric value)
        (catch Exception e
          (log/error e "Error publishing to Statsd:" metric-group metric value))))

    (defn- save-on-publish
      [[[_ _ {:keys [delete-on-publish?-fn]}] value]]
      (not (delete-on-publish?-fn value)))

    (defn publish-aggregated-values
      "Publishes a Statsd metric for each locally cached value, and
      returns the new map of cached values, after deleting metrics
      whose :delete-on-publish?-fn indicate that they should be deleted"
      [values-map]
      (try
        (dorun (map publish-value values-map))
        (utils/filterm save-on-publish values-map)
        (catch Exception e
          (log/error e "Error publishing aggregated values to Statsd")
          {})))

    (defn- trigger-publish
      "Triggers publishing of all values currently aggregated,
      where the metric type's :publish-fn dictates how each
      aggregated value should get published."
      []
      (log/debug "Triggering publish of all cached Statsd metrics")
      (send aggregation-agent publish-aggregated-values))

    (defn setup
      "Initializes the statsd library with all fields needed for production code use.
      Returns a function that will cancel the metric-publishing timer when called."
      [{:keys [host port] :as config-map}]
      (let [config-env (:environment config-map)
            config-cluster (:cluster config-map)
            config-server (:server config-map)
            config-publish-interval-ms (:publish-interval-ms config-map)
            config-histogram-max-size (:histogram-max-size config-map)]
        (when (not-every? nil? [@config @socket-agent @environment @cluster])
          (throw (UnsupportedOperationException. "Statsd has already been setup")))
        (when (not-any? nil? [host port config-env config-cluster])
          (reset! environment (sanitize config-env))
          (reset! cluster (sanitize config-cluster))
          (reset! server (sanitize config-server))
          (reset! publish-interval-ms config-publish-interval-ms)
          (init-configuration host port :prefix "waiter.")
          (when config-histogram-max-size
            (reset! histogram-max-size config-histogram-max-size))
          (when (> config-publish-interval-ms 0)
            (du/start-timer-task (t/millis config-publish-interval-ms) trigger-publish
                                 :delay-ms config-publish-interval-ms)))))

    (defn add-value
      "Given the current map of [metric-group metric metric-type] -> value(s), adds the value to the map
      such that the new map continues to contain the aggregation of values seen for that metric-group,
      metric, and type. The method used for aggregating values differs by metric type, and is passed to
      this function as the metric-type's :aggregate-fn. For example, sets aggregate using conj, whereas
      gauge-deltas aggregate using addition."
      [current-map metric-group metric value {:keys [aggregate-fn seed-fn] :as metric-type}]
      (when (and @config @socket-agent @environment @cluster @server)
        (let [existing-aggregation (get current-map [metric-group metric metric-type])]
          (if existing-aggregation
            (assoc current-map [metric-group metric metric-type] (aggregate-fn existing-aggregation value metric-group))
            (assoc current-map [metric-group metric metric-type] (seed-fn value))))))

    (defn percentile
      "Calculates the p-th percentile of the values in coll
      (where 0 < p <= 100), using the Nearest Rank method:

      https://en.wikipedia.org/wiki/Percentile#The_Nearest_Rank_method

      Assumes that coll is sorted (see percentiles below for context)"
      [coll p]
      (if (or (empty? coll) (not (number? p)) (<= p 0) (> p 100))
        nil
        (nth coll
             (-> p
                 (/ 100)
                 (* (count coll))
                 (Math/ceil)
                 (dec)))))

    (defn percentiles
      "Calculates the p-th percentiles of the values in coll for
      each p in p-list (where 0 < p <= 100), and returns a map of
      p -> value"
      [coll & p-list]
      (let [sorted (sort coll)]
        (into {} (map (fn [p] [p (percentile sorted p)]) p-list))))

    (defn set-publish
      "Publishes a Statsd set metric for each value in values-set"
      [metric-group metric values-set]
      (dorun (map #(unique metric-group metric %) values-set)))

    (defn- gauge-delta-publish
      "Publishes a Statsd gauge metric for value"
      [metric-group metric value]
      (gauge #(metric-path metric-group metric) value))

    (defn- gauge-publish
      "Publishes a Statsd gauge metric, without the server in the
      metric path, for value"
      [metric-group metric value]
      (gauge #(metric-path-without-server metric-group metric) value))

    (defn counter-publish
      "Publishes a Statsd counter metric for value"
      [metric-group metric value]
      (increment #(metric-path metric-group metric) value))

    (defn histo-publish
      "Publishes a Statsd timer metric for each of the 50th, 75th, 95th,
      and 100th percentiles of the values that have been locally aggregated"
      [metric-group metric values]
      (dorun
        (map (fn [[k v]]
               (gauge #(metric-path metric-group (str metric "_p" k)) v))
             (percentiles values 50 75 95 99 100))))

    (defn bounded-conj
      "Like conj, except it will start to pop after the count reaches @histogram-max-size.
      We use this with PersistentQueues, which conj onto the rear and pop from the front,
      to get sliding queue behavior for histograms."
      [queue value metric-group]
      (let [queue' (conj queue value)]
        (if (> (count queue') @histogram-max-size)
          (do
            (counters/inc! (metrics/waiter-counter "statsd" "histo-values-dropped" metric-group))
            (pop queue'))
          queue')))

    (def set-metric
      "Before sending to Statsd, we need to convert to an integer,
      so we take the hashCode here. We aggregate sets by union-ing."
      {:seed-fn (fn [^Object value] #{(.hashCode value)})
       :aggregate-fn (fn [values-set ^Object value _] (conj values-set (.hashCode value)))
       :publish-fn set-publish
       :delete-on-publish?-fn (constantly true)
       :metric-type :set})

    (def gauge-delta-metric
      "Gauge deltas aggregate by addition (like counters), but we
      only want to delete on publish when the value is 0."
      {:seed-fn identity
       :aggregate-fn (fn [sum value _] (+ sum value))
       :publish-fn gauge-delta-publish
       :delete-on-publish?-fn zero?
       :metric-type :gauge-delta})

    (def gauge-metric
      "Gauges aggregate by replacing the old value with the new one"
      {:seed-fn identity
       :aggregate-fn (fn [_ value _] value)
       :publish-fn gauge-publish
       :delete-on-publish?-fn (constantly true)
       :metric-type :gauge})

    (def counter-metric
      "Counters aggregate by addition (like gauge deltas), but we always want
      to delete on publish"
      {:seed-fn identity
       :aggregate-fn (fn [sum value _] (+ sum value))
       :publish-fn counter-publish
       :delete-on-publish?-fn (constantly true)
       :metric-type :counter})

    (def histo-metric
      "Histograms aggregate by concatenating"
      {:seed-fn (fn [value] (-> PersistentQueue/EMPTY (conj value)))
       :aggregate-fn bounded-conj
       :publish-fn histo-publish
       :delete-on-publish?-fn (constantly true)
       :metric-type :histo})

    (defn unique!
      "Records a set metric, for counting unique occurrences of a thing"
      [metric-group metric value]
      (send aggregation-agent add-value metric-group metric value set-metric))

    (defn gauge-delta!
      "Records a gauge metric, accepting deltas"
      [metric-group metric value]
      (send aggregation-agent add-value metric-group metric value gauge-delta-metric))

    (defn gauge!
      "Records a gauge metric, accepting current values"
      [metric-group metric value]
      (send aggregation-agent add-value metric-group metric value gauge-metric))

    (defn inc!
      "Records a counter metric"
      ([metric-group metric]
       (inc! metric-group metric 1))
      ([metric-group metric value]
       (send aggregation-agent add-value metric-group metric value counter-metric)))

    (defn histo!
      "Records a histogram metric, ultimately represented in Statsd as a gauge"
      [metric-group metric value]
      (send aggregation-agent add-value metric-group metric value histo-metric))

    (defn await-agents
      "Blocks until the aggregation and socket agents have processed all pending messages"
      []
      (await aggregation-agent)
      (await socket-agent))

    (defn drain
      "Triggers a publish of all locally aggregated metrics and then awaits agents"
      []
      (let [exception (agent-error socket-agent)]
        (if exception
          (throw exception)
          (do
            (log/debug "Draining Statsd metrics")
            (trigger-publish)
            (await-agents)))))

    (defn state
      "Returns the current state of the aggregation-agent using nested maps, so that
      consumers can easily convert to JSON, which doesn't allow arrays as keys"
      []
      (reduce (fn [m [[metric-group metric metric-type] value]]
                (assoc-in m [(:metric-type metric-type) metric-group metric] value))
              {}
              @aggregation-agent))))

(defn publish-metric-group->counts
  "Publishes a gauge for the number of cpus, mem, healthy instances, unhealthy instances, and
   failed instances corresponding to each metric group in the provided map."
  [metric-group->counts]
  (dorun (map (fn [[metric-group {:keys [healthy-instances unhealthy-instances failed-instances cpus mem]}]]
                (gauge! metric-group "instances.healthy" healthy-instances)
                (gauge! metric-group "instances.unhealthy" unhealthy-instances)
                (gauge! metric-group "instances.failed" failed-instances)
                (gauge! metric-group "cpus" cpus)
                (gauge! metric-group "mem" mem))
              metric-group->counts)))

(defn merge-service-state
  "Merges the current map of resources by metric group with service data from a router state update message."
  [service-id->service-description-fn service-id healthy-instances unhealthy-instances failed-instances
   metric-group->counts]
  (try
    (if-let [{:strs [metric-group cpus mem]} (service-id->service-description-fn service-id)]
      (let [healthy (count healthy-instances)
            unhealthy (count unhealthy-instances)
            failed (count failed-instances)
            active (+ healthy unhealthy)
            counts {:healthy-instances healthy
                    :unhealthy-instances unhealthy
                    :failed-instances failed
                    :cpus (* active (or cpus 0))
                    :mem (* active (or mem 0))}]
        (merge-with #(merge-with + %1 %2) metric-group->counts {metric-group counts}))
      (do
        (log/warn "no service description found for service id" service-id)
        metric-group->counts))
    (catch Throwable e
      (log/error e "error processing service metrics for" service-id)
      metric-group->counts)))

(defn router-state->metric-group->counts
  "Converts messages from the router state update to a map of metric-group -> [healthy unhealthy failed],
   where each element in the array is the count of instances in that metric group with that status."
  [service-id->service-description-fn
   {:keys [all-available-service-ids service-id->failed-instances service-id->healthy-instances service-id->unhealthy-instances]}]
  (loop [[service-id & remaining-service-ids] (seq all-available-service-ids)
         metric-group->counts {}]
    (if-not service-id
      metric-group->counts
      (let [failed-instances (get service-id->failed-instances service-id)
            healthy-instances (get service-id->healthy-instances service-id)
            unhealthy-instances (get service-id->unhealthy-instances service-id)]
        (recur remaining-service-ids
               (merge-service-state
                 service-id->service-description-fn service-id healthy-instances unhealthy-instances failed-instances
                 metric-group->counts))))))

(defn process-router-state
  "Publishes all gauges produced from the provided scheduler messages"
  [router-state service-id->service-description-fn]
  (try
    (->> router-state
         (router-state->metric-group->counts service-id->service-description-fn)
         publish-metric-group->counts)
    (catch Throwable e
      (log/error e "error processing router state"))))

(defn start-service-instance-metrics-publisher
  "Launches a timer task, running at intervals of sync-instances-interval-ms milliseconds,
   to continuously publish stats based on router state updates."
  [service-id->service-description-fn query-state-fn sync-instances-interval-ms]
  (log/info "service-instance-metrics-publisher starting")
  (let [cancel-fn (du/start-timer-task
                    (t/millis sync-instances-interval-ms)
                    (fn run-instance-metrics-publisher []
                      (log/info "service-instance-metrics-publisher publishing router state")
                      (-> (query-state-fn)
                          (process-router-state service-id->service-description-fn))))]
    (fn cancel-instance-metrics-publisher []
      (log/info "service-instance-metrics-publisher stopping")
      (cancel-fn))))
