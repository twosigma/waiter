;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.metrics-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [metrics.core :as mc]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.async-utils :as au]
            [waiter.core :as core]
            [waiter.metrics :refer :all]
            [waiter.test-helpers :as test-helpers]
            [waiter.utils :as utils])
  (:import (com.codahale.metrics MetricFilter)
           (org.joda.time DateTime)))

(deftest test-compress-strings
  (is (= ["a"] (compress-strings ["a"] ".")))
  (is (= ["a.b"] (compress-strings ["a" "b"] ".")))
  (is (= ['a ".b"] (compress-strings ['a "b"] ".")))
  (is (= ["a." 'b] (compress-strings ["a" 'b] ".")))
  (is (= ["a.b.c"] (compress-strings ["a" "b" "c"] ".")))
  (is (= ["a." 'b ".c"] (compress-strings ["a" 'b "c"] ".")))
  (is (= ['a ".b.c"] (compress-strings ['a "b" "c"] ".")))
  (is (= ["a.b." 'c] (compress-strings ["a" "b" 'c] ".")))
  (is (= ['a "." 'b] (compress-strings ['a 'b] ".")))
  (is (= ['a "." 'b "." 'c] (compress-strings ['a 'b 'c] ".")))
  (is (= ['a "." 'b ".c"] (compress-strings ['a 'b "c"] ".")))
  (is (= ["a." 'b "." 'c] (compress-strings ["a" 'b 'c] ".")))
  (is (= ["a." 'b "." 'c "." 'd] (compress-strings ["a" 'b 'c 'd] "."))))

(deftest test-join-with-concat
  (is (= `(.concat "a" "b") (join-by-concat ["a" "b"])))
  (is (= `(.concat (.concat "a" "b") "c") (join-by-concat ["a" "b" "c"]))))

(deftest test-metric-name
  (is (= '(.concat (.concat "a." b) ".c") (metric-name ["a" 'b "c"])))
  (is (= '(.concat (.concat "a.b." c) ".d") (metric-name ["a" "b" 'c "d"])))
  (is (= '(.concat (.concat "a." b) ".c.d") (metric-name ["a" 'b "c" "d"]))))

(deftest test-service-counter
  (let [all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true))]
    (.removeMatching mc/default-registry all-metrics-match-filter)
    (service-counter "service-id" "foo")
    (service-counter "service-id" "foo" "bar")
    (service-counter "service-id" "fee" "fie")
    (is (every? #(str/starts-with? % "services.service-id.") (.getNames mc/default-registry)))
    (is (= 3 (count (.getCounters mc/default-registry all-metrics-match-filter))))
    (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest test-service-histogram
  (let [all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true))]
    (.removeMatching mc/default-registry all-metrics-match-filter)
    (service-histogram "service-id" "foo")
    (service-histogram "service-id" "foo" "bar")
    (service-histogram "service-id" "fee" "fie")
    (is (every? #(str/starts-with? % "services.service-id.") (.getNames mc/default-registry)))
    (is (= 3 (count (.getHistograms mc/default-registry all-metrics-match-filter))))
    (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest test-service-timer
  (let [all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true))]
    (.removeMatching mc/default-registry all-metrics-match-filter)
    (service-timer "service-id" "foo")
    (service-timer "service-id" "foo" "bar")
    (service-timer "service-id" "fee" "fie")
    (is (every? #(str/starts-with? % "services.service-id.") (.getNames mc/default-registry)))
    (is (= 3 (count (.getTimers mc/default-registry all-metrics-match-filter))))
    (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest test-service-meter
  (let [all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true))]
    (.removeMatching mc/default-registry all-metrics-match-filter)
    (service-meter "service-id" "foo")
    (service-meter "service-id" "foo" "bar")
    (service-meter "service-id" "fee" "fie")
    (is (every? #(str/starts-with? % "services.service-id.") (.getNames mc/default-registry)))
    (is (= 3 (count (.getMeters mc/default-registry all-metrics-match-filter))))
    (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest test-waiter-counter
  (let [all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true))]
    (.removeMatching mc/default-registry all-metrics-match-filter)
    (waiter-counter "core" "foo")
    (waiter-counter "core" "foo" "bar")
    (waiter-counter "core" "fee" "fie")
    (is (every? #(str/starts-with? % "waiter.core.") (.getNames mc/default-registry)))
    (is (= 3 (count (.getCounters mc/default-registry all-metrics-match-filter))))
    (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest test-waiter-meter
  (let [all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true))]
    (.removeMatching mc/default-registry all-metrics-match-filter)
    (waiter-meter "core" "foo")
    (waiter-meter "core" "foo" "bar")
    (waiter-meter "core" "fee" "fie")
    (is (every? #(str/starts-with? % "waiter.core.") (.getNames mc/default-registry)))
    (is (= 3 (count (.getMeters mc/default-registry all-metrics-match-filter))))
    (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest test-waiter-timer
  (let [all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true))]
    (.removeMatching mc/default-registry all-metrics-match-filter)
    (waiter-timer "core" "foo")
    (waiter-timer "core" "foo" "bar")
    (waiter-timer "core" "fee" "fie")
    (is (every? #(str/starts-with? % "waiter.core.") (.getNames mc/default-registry)))
    (is (= 3 (count (.getTimers mc/default-registry all-metrics-match-filter))))
    (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest test-update-counter
  (let [test-cases [{:name "nil-inputs", :input {:old nil, :new nil}, :expected 0}
                    {:name "one-empty-inputs", :input {:old nil, :new []}, :expected 0}
                    {:name "one-non-empty-input-a", :input {:old nil, :new [:a :b]}, :expected 2}
                    {:name "one-non-empty-input-b", :input {:old [:a :b], :new nil}, :expected -2}
                    {:name "non-empty-inputs-a", :input {:old [:a :b], :new [:b :c :d :e]}, :expected 2}
                    {:name "non-empty-inputs-b", :input {:old [:a :b :c], :new [:d]}, :expected -2}
                    {:name "non-empty-inputs-c", :input {:old [:a :b], :new [:b :c :d :e], :initial 3}, :expected 5}
                    {:name "non-empty-inputs-d", :input {:old [:a :b :c], :new [:d], :initial 3}, :expected 1}]]
    (doseq [test-case test-cases]
      (let [{:keys [name input expected]} test-case
            title ["waiter" "test" name]
            counter (counters/counter title)]
        (testing (str "Test " name)
          (if (:initial input)
            (do
              (counters/clear! counter)
              (counters/inc! counter (:initial input))))
          (update-counter counter (:old input) (:new input))
          (let [actual-value (counters/value counter)]
            (is (= expected actual-value))))))))

(deftest test-reset-counter
  (let [test-cases [{:name "nil-inputs", :initial nil, :input nil, :expected 0}
                    {:name "new-counter-input", :initial nil, :input 3, :expected 3}
                    {:name "old-counter-input-less", :initial 7, :input 3, :expected 3}
                    {:name "old-counter-input-more", :initial 7, :input 11, :expected 11}
                    {:name "old-counter-nil-input", :initial 2, :input nil, :expected 0}]]
    (doseq [{:keys [name initial input expected]} test-cases]
      (testing (str "Test " name)
        (let [title ["waiter" "test" name]
              the-counter (counters/counter title)]
          (when initial
            (counters/clear! the-counter)
            (counters/inc! the-counter initial))
          (reset-counter the-counter input)
          (let [actual-value (counters/value the-counter)]
            (is (= expected actual-value))))))))

(deftest test-get-service-metrics
  (let [metrics-registry mc/default-registry
        create-metrics (fn [service-id]
                         (histograms/update! (histograms/histogram ["services" service-id "test-histogram1"]) (hash service-id))
                         (histograms/update! (histograms/histogram ["services" service-id "instance" "test-histogram2"]) (hash service-id))
                         (timers/start-stop-time! (timers/timer ["services" service-id "test-response-duration"]))
                         (timers/start-stop-time! (timers/timer ["services" service-id "nested" "test-response-duration"]))
                         (meters/mark! (meters/meter ["services" service-id "test-throughput"]))
                         (meters/mark! (meters/meter ["services" service-id "nested" "test-throughput"]))
                         (counters/inc! (counters/counter ["services" service-id "foo" "bar"]))
                         (counters/inc! (counters/counter ["services" service-id "outstanding" "fum"])))
        retrieve-service-metrics (fn [service-id] (-> (get-service-metrics service-id) (get-in ["services" service-id])))
        all-service-metrics-available? (fn [metrics]
                                         (every? #(get-in metrics %)
                                                 [["test-histogram1"]
                                                  ["instance" "test-histogram2"]
                                                  ["test-response-duration"]
                                                  ["nested" "test-response-duration"]
                                                  ["test-throughput"]
                                                  ["nested" "test-throughput"]
                                                  ["foo" "bar"]
                                                  ["outstanding" "fum"]]))]
    (testing "retrieving metrics for specified services"
      (.removeMatching metrics-registry (reify MetricFilter (matches [_ _ _] true)))
      (is (zero? (count (.getMetrics metrics-registry))))
      (create-metrics "test-service-1")
      (is (all-service-metrics-available? (retrieve-service-metrics "test-service-1")))
      (is (zero? (count (retrieve-service-metrics "test-service-2"))))
      (is (zero? (count (retrieve-service-metrics "test-service-3"))))
      (create-metrics "test-service-2")
      (create-metrics "test-service-3")
      (is (all-service-metrics-available? (retrieve-service-metrics "test-service-1")))
      (is (all-service-metrics-available? (retrieve-service-metrics "test-service-2")))
      (is (all-service-metrics-available? (retrieve-service-metrics "test-service-3")))
      (.removeMatching metrics-registry (reify MetricFilter (matches [_ _ _] true))))))

(deftest test-get-waiter-metrics
  (let [metrics-registry mc/default-registry
        create-metrics (fn [prefix]
                         (histograms/update! (histograms/histogram [prefix "test-histogram1"]) (hash prefix))
                         (histograms/update! (histograms/histogram [prefix "instance" "test-histogram2"]) (hash prefix))
                         (timers/start-stop-time! (timers/timer [prefix "test-response-duration"]))
                         (timers/start-stop-time! (timers/timer [prefix "nested" "test-response-duration"]))
                         (meters/mark! (meters/meter [prefix "test-throughput"]))
                         (meters/mark! (meters/meter [prefix "nested" "test-throughput"]))
                         (counters/inc! (counters/counter [prefix "foo" "bar"]))
                         (counters/inc! (counters/counter [prefix "outstanding" "fum"])))
        all-waiter-metrics-available? (fn [metrics]
                                        (every? #(get-in metrics (into ["waiter"] %))
                                                [["test-histogram1"]
                                                 ["instance" "test-histogram2"]
                                                 ["test-response-duration"]
                                                 ["nested" "test-response-duration"]
                                                 ["test-throughput"]
                                                 ["nested" "test-throughput"]
                                                 ["foo" "bar"]
                                                 ["outstanding" "fum"]]))]
    (testing "retrieving metrics for specified services"
      (.removeMatching metrics-registry (reify MetricFilter (matches [_ _ _] true)))
      (is (zero? (count (.getMetrics metrics-registry))))
      (create-metrics "services.test-service-1")
      (create-metrics "waiter")
      (create-metrics "services.test-service-2")
      (is (all-waiter-metrics-available? (get-waiter-metrics)))
      (.removeMatching metrics-registry (reify MetricFilter (matches [_ _ _] true))))))

(deftest test-aggregate-router-data
  (let [router->metrics {"router-a" {"counters" {"instance-counts" {"a" 10, "b" 20}
                                                 "request-counts" {"total" 20}
                                                 "item-a" 100}
                                     "histogram" {"quantile-a" {"count" 10
                                                                "value" {"0.0" 10, "0.25" 40, "0.5" 10, "0.75" 10}}
                                                  "nested" {"quantile-b" {"count" 30
                                                                          "value" {"0.0" 10, "0.25" 10, "0.5" 10, "0.75" 10}}}}
                                     "meters" {"request-rate" {"count" 20, "value" 20}
                                               "response-rate" {"count" 10, "value" 20}}
                                     "timers" {"nested" {"inner" {"quantile-c" {"count" 30
                                                                                "value" {"0.0" 10, "0.25" 10, "0.5" 10, "0.75" 10}}}}}
                                     "metrics-version" 1}
                         "router-b" {"counters" {"instance-counts" {"a" 30, "b" 70}
                                                 "request-counts" {"total" 30}
                                                 "item-a" 200}
                                     "histogram" {"quantile-a" {"count" 10
                                                                "value" {"0.0" 30, "0.25" 40, "0.5" 30, "0.75" 10}}
                                                  "nested" {"quantile-b" {"count" 10
                                                                          "value" {"0.0" 30, "0.25" 20, "0.5" 30, "0.75" 10}}}}
                                     "meters" {"request-rate" {"count" 30, "value" 10}
                                               "response-rate" {"count" 40, "value" 20}}
                                     "timers" {"nested" {"inner" {"quantile-b" {"count" 10
                                                                                "value" {"0.0" 30, "0.25" 20, "0.5" 30, "0.75" 10}}}}}
                                     "metrics-version" 2}
                         "router-c" {"counters" {"instance-counts" {"a" 10, "b" 50}
                                                 "request-counts" {"total" 40}
                                                 "item-a" 300}
                                     "histogram" {"quantile-a" {"count" 20
                                                                "value" {"0.0" 20, "0.25" 80, "0.5" 30, "0.75" 20}}
                                                  "nested" {"quantile-b" {"count" 10
                                                                          "value" {"0.0" 20, "0.25" 10, "0.5" 30, "0.75" 20}}}}
                                     "timers" {"nested" {"inner" {"quantile-b" {"count" 10
                                                                                "value" {"0.0" 20, "0.25" 10, "0.5" 30, "0.75" 20}}}}}
                                     "metrics-version" 8}
                         "router-d" {"counters" {"request-counts" {"total" 10}}
                                     "meters" {"request-rate" {"count" 25, "value" 10}
                                               "response-rate" {"count" 50, "value" 20}}
                                     }}
        expected {"counters" {"item-a" 600
                              "request-counts" {"total" 100}}
                  "histogram" {"quantile-a" {"count" 40
                                             "value" {"0.0" 20, "0.25" 60, "0.5" 25, "0.75" 15}}
                               "nested" {"quantile-b" {"count" 50
                                                       "value" {"0.0" 16, "0.25" 12, "0.5" 18, "0.75" 12}}}}
                  "meters" {"request-rate" {"count" 75, "value" 40}
                            "response-rate" {"count" 100, "value" 60}}
                  "timers" {"nested" {"inner" {"quantile-b" {"count" 20
                                                             "value" {"0.0" 25, "0.25" 15, "0.5" 30, "0.75" 15}}
                                               "quantile-c" {"count" 30
                                                             "value" {"0.0" 10, "0.25" 10, "0.5" 10, "0.75" 10}}}}}
                  :routers-sent-requests-to 4}
        actual (aggregate-router-data router->metrics)]
    (is (= expected actual))))

(deftest test-remove-and-check-metrics-except-outstanding
  (let [metrics-registry mc/default-registry
        create-metrics (fn [service-id]
                         (histograms/update! (histograms/histogram ["services" service-id "test-histogram1"]) (hash service-id))
                         (histograms/update! (histograms/histogram ["services" service-id "instance" "test-histogram2"]) (hash service-id))
                         (timers/start-stop-time! (timers/timer ["services" service-id "test-response-duration"]))
                         (timers/start-stop-time! (timers/timer ["services" service-id "nested" "test-response-duration"]))
                         (meters/mark! (meters/meter ["services" service-id "test-throughput"]))
                         (meters/mark! (meters/meter ["services" service-id "nested" "test-throughput"]))
                         (counters/inc! (counters/counter ["services" service-id "foo" "bar"]))
                         (counters/inc! (counters/counter ["services" service-id "outstanding" "fum"])))
        metrics-per-service 8
        has-metrics-except-outstanding? (fn has-metrics-except-outstanding? [service-id]
                                          (let [metric-filter (reify MetricFilter
                                                                (matches [_ name _]
                                                                  (and (str/includes? name service-id)
                                                                       (not (str/includes? name "outstanding")))))]
                                            (or (not-empty (.getCounters metrics-registry metric-filter))
                                                (not-empty (.getHistograms metrics-registry metric-filter))
                                                (not-empty (.getMeters metrics-registry metric-filter))
                                                (not-empty (.getTimers metrics-registry metric-filter)))))]
    (testing "Delete metrics for specified services"
      (.removeMatching metrics-registry (reify MetricFilter (matches [_ _ _] true)))
      (is (zero? (count (.getMetrics metrics-registry))))
      (create-metrics "test-service-1")
      (create-metrics "test-service-2")
      (create-metrics "test-service-3")
      (is (= (* 3 metrics-per-service) (count (.getMetrics metrics-registry))))
      (create-metrics "test-service-2")
      (is (= (* 3 metrics-per-service) (count (.getMetrics metrics-registry))))
      (create-metrics "test-service-1")
      (is (= (* 3 metrics-per-service) (count (.getMetrics metrics-registry))))
      (is (has-metrics-except-outstanding? "test-service-1"))
      (is (has-metrics-except-outstanding? "test-service-2"))
      (is (has-metrics-except-outstanding? "test-service-3"))
      (remove-metrics-except-outstanding metrics-registry "test-service-1")
      (is (= (+ (* 1 1) (* 2 metrics-per-service)) (count (.getMetrics metrics-registry))))
      (is (not (has-metrics-except-outstanding? "test-service-1")))
      (is (has-metrics-except-outstanding? "test-service-2"))
      (is (has-metrics-except-outstanding? "test-service-3"))
      (remove-metrics-except-outstanding metrics-registry "test-service-2")
      (is (= (+ (* 2 1) (* 1 metrics-per-service)) (count (.getMetrics metrics-registry))))
      (is (not (has-metrics-except-outstanding? "test-service-1")))
      (is (not (has-metrics-except-outstanding? "test-service-2")))
      (is (has-metrics-except-outstanding? "test-service-3"))
      (.removeMatching metrics-registry (reify MetricFilter (matches [_ _ _] true))))))

(deftest test-transient-metrics-data-producer
  (let [service-id->metrics-atom (atom {})
        service-id->metrics-fn (fn service-id->metrics-fn [] @service-id->metrics-atom)
        create-metrics (fn [service-id]
                         (let [metrics-data {"outstanding" (rand-int 10), "total" (+ 10 (rand-int 20))}]
                           (swap! service-id->metrics-atom assoc service-id metrics-data)))
        service-id->metrics-chan (au/latest-chan)
        metrics-gc-interval-ms 10
        {:keys [exit-chan query-chan]}
        (transient-metrics-data-producer service-id->metrics-chan service-id->metrics-fn {:metrics-gc-interval-ms metrics-gc-interval-ms})
        await-iteration-execution
        (fn await-iteration-execution-fn []
          (let [retrieve-iteration (fn []
                                     (let [response-chan (async/promise-chan)
                                           _ (async/>!! query-chan {:response-chan response-chan})
                                           {:keys [iteration]} (async/<!! response-chan)]
                                       iteration))
                initial-iteration (retrieve-iteration)]
            (test-helpers/wait-for #(> (retrieve-iteration) initial-iteration)
                                   :interval metrics-gc-interval-ms
                                   :unit-multiplier 1)))]
    (testing "Transient Data producer"
      (reset! service-id->metrics-atom {})
      (create-metrics "test-service-1")
      (create-metrics "test-service-2")
      (await-iteration-execution)
      (let [service->metrics (async/<!! service-id->metrics-chan)]
        (is (= 2 (count service->metrics)))
        (is (contains? service->metrics "test-service-1"))
        (is (contains? service->metrics "test-service-2")))
      (create-metrics "test-service-3")
      (await-iteration-execution)
      (let [service->metrics (async/<!! service-id->metrics-chan)]
        (is (= 3 (count service->metrics)))
        (is (contains? service->metrics "test-service-1"))
        (is (contains? service->metrics "test-service-2"))
        (is (contains? service->metrics "test-service-3")))
      (reset! service-id->metrics-atom {})
      (await-iteration-execution)
      (let [service->metrics (async/<!! service-id->metrics-chan)]
        (is (zero? (count service->metrics))))
      (create-metrics "test-service-1")
      (await-iteration-execution)
      (let [service->metrics (async/<!! service-id->metrics-chan)]
        (is (= 1 (count service->metrics)))
        (is (contains? service->metrics "test-service-1")))
      (async/>!! exit-chan :exit)
      (reset! service-id->metrics-atom {}))))

(deftest test-transient-metrics-gc
  (let [leader? (constantly true)
        state-store (atom {})
        read-state-fn (fn [_] @state-store)
        write-state-fn (fn [_ state] (reset! state-store state))
        deleted-services-atom (atom #{})
        available-services-atom (atom #{"service-remove0-1", "service-keep-2", "service-keep-3", "service-remove1-4",
                                        "service-remove2-5-will-be-alive"})
        remove-target-atom (atom "remove0")
        service-id->metrics-chan-counter (atom 0)
        test-start-time (t/now)
        clock (fn [] (t/plus test-start-time (t/minutes @service-id->metrics-chan-counter)))
        local-metrics-fn (fn [service-id] {"name" service-id})
        local-metrics-agent (agent (pc/map-from-keys local-metrics-fn @available-services-atom))
        service-gc-go-routine (partial core/service-gc-go-routine read-state-fn write-state-fn leader? clock)]
    (with-redefs [remove-metrics-except-outstanding (fn [_ service-id]
                                                      (swap! deleted-services-atom conj service-id)
                                                      (swap! available-services-atom (fn [old-val] (remove #{service-id} old-val))))]
      (let [exit-flag-atom (atom false)
            await-fn (fn [counter-val]
                       (while (< @service-id->metrics-chan-counter counter-val)))]
        (testing "transient-metrics-gc"
          (let [transient-metrics-timeout-ms 10
                metrics-gc-interval-ms 1
                scheduler-state-chan (async/chan)
                result-chans (transient-metrics-gc scheduler-state-chan local-metrics-agent service-gc-go-routine
                                                   {:transient-metrics-timeout-ms transient-metrics-timeout-ms
                                                    :metrics-gc-interval-ms metrics-gc-interval-ms})
                service-id->metrics-chan (:service-id->metrics-chan result-chans)]
            (async/thread
              (while (not @exit-flag-atom)
                (let [available-service-ids (remove #(str/includes? % (str @remove-target-atom)) @available-services-atom)
                      scheduler-messages [[:update-available-apps {:available-apps available-service-ids}]]]
                  (async/>!! scheduler-state-chan scheduler-messages)))
              (async/close! scheduler-state-chan))
            (async/thread
              (while (not @exit-flag-atom)
                (try
                  (let [service->metrics (zipmap @available-services-atom
                                                 (map (fn [service-id]
                                                        (if (str/includes? service-id "remove")
                                                          {"outstanding" 0, "total" 100}
                                                          {"outstanding" 11, "total" 100}))
                                                      @available-services-atom))]
                    (swap! service-id->metrics-chan-counter inc)
                    (async/>!! service-id->metrics-chan service->metrics)
                    (Thread/sleep 5))
                  (catch Exception _ (comment "Ignore"))))
              (async/close! service-id->metrics-chan))
            (await-fn (+ 20 @service-id->metrics-chan-counter))
            (is (= 1 (count @deleted-services-atom)))
            (is (contains? @deleted-services-atom "service-remove0-1"))
            (reset! remove-target-atom "remove1")
            (await-fn (+ 20 @service-id->metrics-chan-counter))
            (is (= 2 (count @deleted-services-atom)))
            (let [expected-deleted-services ["service-remove0-1" "service-remove1-4"]]
              (is (every? #(contains? @deleted-services-atom %) expected-deleted-services)
                  (str "Expected delete to include: " expected-deleted-services ", actual: " @deleted-services-atom)))
            (reset! exit-flag-atom true)
            (await local-metrics-agent)
            (is (= (pc/map-from-keys local-metrics-fn ["service-keep-2" "service-keep-3" "service-remove2-5-will-be-alive"])
                   @local-metrics-agent))
            (async/>!! (:exit result-chans) :exit)))))))

(deftest test-with-timer!
  (testing "Timing an operation"
    (testing "should send the same elapsed time to Coda Hale and callback"
      (mc/remove-all-metrics)
      (let [elapsed-nanos (atom nil)]
        (is (nil? (with-timer!
                    (service-timer "service-id" "metric")
                    (fn [nanos] (reset! elapsed-nanos nanos))
                    (utils/sleep 100))))
        (let [nanos (timers/sample (service-timer "service-id" "metric"))]
          (is (= 1 (count nanos)))
          (is (< 95000000 (first nanos)))
          (is (= (first nanos) @elapsed-nanos)))))))

(deftest test-get-core-metrics
  (let [local-metrics-agent (agent {"s1" {"last-request-time" 1000}
                                    "s2" {"last-request-time" 2000}
                                    "s3" {"last-request-time" 3000}})]

    (.removeMatching mc/default-registry MetricFilter/ALL)
    (counters/inc! (service-counter "s1" "request-counts" "outstanding") 100)
    (counters/inc! (service-counter "s1" "request-counts" "total") 200)
    (counters/inc! (service-counter "s1" "work-stealing" "received-from" "in-flight") 2)
    (counters/inc! (service-counter "s2" "instance-counts" "slots-available") 10)
    (counters/inc! (service-counter "s2" "instance-counts" "slots-in-use") 5)
    (counters/inc! (service-counter "s2" "request-counts" "outstanding") 150)
    (counters/inc! (service-counter "s2" "request-counts" "total") 250)

    (is (= {"s1" {"last-request-time" 1000, "outstanding" 100, "slots-received" 2, "total" 200},
            "s2" {"last-request-time" 2000, "outstanding" 150, "slots-available" 10, "slots-in-use" 5, "total" 250}}
           (get-core-metrics local-metrics-agent)))))

(deftest test-update-last-request-time
  (let [time-1 (DateTime. 1000)
        time-2 (DateTime. 2000)
        time-3 (DateTime. 3000)
        time-4 (DateTime. 2500)]
    (is (= {"foo" {"last-request-time" (.getMillis time-2)}}
           (update-last-request-time {} "foo" time-2)))
    (is (= {"bar" {"last-request-time" (.getMillis time-4)}, "foo" {"last-request-time" (.getMillis time-2)}}
           (update-last-request-time
             {"bar" {"last-request-time" (.getMillis time-4)}, "foo" {"last-request-time" (.getMillis time-1)}}
             "foo" time-2)))
    (is (= {"bar" {"last-request-time" (.getMillis time-2)}, "foo" {"last-request-time" (.getMillis time-3)}}
           (update-last-request-time
             {"bar" {"last-request-time" (.getMillis time-2)}, "foo" {"last-request-time" (.getMillis time-3)}}
             "foo" time-2)))
    (is (= {"bar" {"last-request-time" (.getMillis time-2)}, "foo" {"last-request-time" (.getMillis time-4)}}
           (update-last-request-time
             {"bar" {"last-request-time" (.getMillis time-2)}, "foo" {"last-request-time" (.getMillis time-4)}}
             "foo" time-2)))))

(deftest test-cleanup-local-metrics
  (let [time-2 2000
        time-4 4000]
    (is (= {"bar" {"last-request-time" time-4}}
           (cleanup-local-metrics
             {"bar" {"last-request-time" time-4}, "foo" {"last-request-time" time-2}} "cid" "foo")))
    (is (= {"bar" {"last-request-time" time-4}, "foo" {"last-request-time" time-2}}
           (cleanup-local-metrics
             {"bar" {"last-request-time" time-4}, "foo" {"last-request-time" time-2}} "cid" "baz")))))
