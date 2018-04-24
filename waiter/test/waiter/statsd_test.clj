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
(ns waiter.statsd-test
  (require [clojure.core.async :as async]
           [clojure.test :refer :all]
           [clojure.tools.logging :as log]
           [waiter.statsd :as statsd]
           [waiter.test-helpers :refer :all])
  (:import (clojure.lang PersistentQueue)))

(defn- teardown
  []
  (statsd/teardown))

(defn- teardown-setup
  [& opts]
  (teardown)
  (statsd/setup (merge {:host "localhost"
                        :port 1234
                        :environment "env"
                        :cluster "cluster"
                        :server "server"
                        :publish-interval-ms 0}
                       (apply hash-map opts)))
  (statsd/await-agents))

(defmacro capture-packets
  [& body]
  `(let [packets# (atom [])]
     (teardown-setup)
     (with-redefs [statsd/send-packet (fn [socket# f#]
                                        (log/debug "Packet" (f#))
                                        (swap! packets# #(conj %1 (f#)))
                                        socket#)]
       ~@body
       (statsd/drain))
     (teardown)
     @packets#))

(deftest test-metric-path
  (testing "Metric path generation"
    (testing "should include environment, cluster, and server names"
      (teardown-setup)
      (is (= "something.env.cluster.server.amazing" (statsd/metric-path "something" "amazing")))
      (teardown-setup :environment "abc" :cluster "def" :server "ghi")
      (is (= "something.abc.def.ghi.amazing" (statsd/metric-path "something" "amazing")))
      (teardown))))

(deftest test-setup
  (testing "Statsd setup process"
    (testing "should throw if called multiple times"
      (teardown-setup)
      (is (thrown? UnsupportedOperationException (statsd/setup {})))
      (teardown))))

(deftest test-start-scheduler-metrics-publisher
  (testing "Scheduler metrics publishing loop"
    (testing "should terminate via exit channel"
      (let [exit-chan (async/chan 1)
            return-chan (statsd/start-scheduler-metrics-publisher (async/chan) exit-chan nil)]
        (is (async/>!! exit-chan :foo))
        (async/alt!!
          return-chan
          ([returned] (is (nil? returned)))

          (async/timeout 1000)
          ([_] (throw (Exception. "Timed out waiting for publisher to terminate"))))))))

(deftest test-scheduler-messages->instance-counts-by-metric-group
  (testing "Conversion of scheduler messages to instance counts by metric group"
    (testing "should produce aggregate instance counts by metric group"
      (let [messages [[:update-available-services {}]
                      [:update-app-instances {:service-id :fee
                                              :healthy-instances [:i]
                                              :unhealthy-instances [:i :i]
                                              :failed-instances [:i :i :i]}]
                      [:update-app-instances {:service-id :fie
                                              :healthy-instances [:i]
                                              :unhealthy-instances [:i :i]
                                              :failed-instances [:i :i :i]}]
                      [:update-app-instances {:service-id :foe
                                              :healthy-instances [:i :i :i]
                                              :unhealthy-instances [:i :i :i]
                                              :failed-instances [:i :i :i]}]
                      [:update-app-instances {:service-id :fum
                                              :healthy-instances [:i :i :i]
                                              :unhealthy-instances [:i :i :i]
                                              :failed-instances [:i :i :i]}]
                      [:update-app-instances {:service-id :qux
                                              :healthy-instances [:i :i :i]
                                              :unhealthy-instances [:i :i :i]
                                              :failed-instances [:i :i :i]}]
                      [:update-app-instances {:service-id :eek
                                              :healthy-instances [:i]
                                              :unhealthy-instances [:i :i]
                                              :failed-instances [:i :i :i]}]]
            service-id->service-description #(% {:fee {"metric-group" "foo", "cpus" 0.1, "mem" 128}
                                                 :fie {"metric-group" "bar", "cpus" 0.2, "mem" 256}
                                                 :foe {"metric-group" "bar", "cpus" 0.4, "mem" 512}
                                                 :fum {"metric-group" "baz", "cpus" 0.8, "mem" 1024}
                                                 :qux {"metric-group" "baz", "cpus" 1.6, "mem" 2048}
                                                 :eek {"metric-group" "baz", "cpus" 3.2, "mem" 4096}})]
        (is (= {"foo" {:healthy-instances 1
                       :unhealthy-instances 2
                       :failed-instances 3
                       :cpus (* 3 0.1)
                       :mem 384}
                "bar" {:healthy-instances 4
                       :unhealthy-instances 5
                       :failed-instances 6
                       :cpus (+ (* 3 0.2) (* 6 0.4))
                       :mem 3840}
                "baz" {:healthy-instances 7
                       :unhealthy-instances 8
                       :failed-instances 9
                       :cpus (+ (* 6 0.8) (* 6 1.6) (* 3 3.2))
                       :mem 30720}}
               (statsd/scheduler-messages->instance-counts-by-metric-group messages service-id->service-description)))))

    (testing "should be resilient to empty service description"
      (is (= {nil {:healthy-instances 1, :unhealthy-instances 2, :failed-instances 3, :cpus 0, :mem 0}}
             (statsd/scheduler-messages->instance-counts-by-metric-group
               [[:update-app-instances {:service-id :fee
                                        :healthy-instances [:i]
                                        :unhealthy-instances [:i :i]
                                        :failed-instances [:i :i :i]}]]
               (constantly {})))))))

(deftest test-process-update-app-instances-message
  (testing "Processing of an :update-app-instances scheduler message"

    (testing "should not let exceptions bubble out"
      (let [misbehaving-fn (fn [_] (throw (Exception. "I'm misbehaving")))]
        (is (= {} (statsd/process-update-app-instances-message {} {} misbehaving-fn)))
        (is (= {"foo" {:healthy-instances 1, :unhealthy-instances 2, :failed-instances 3, :cpus 0.5, :mem 384}}
               (statsd/process-update-app-instances-message
                 {"foo" {:healthy-instances 1, :unhealthy-instances 2, :failed-instances 3, :cpus 0.5, :mem 384}}
                 {:service-id "bar"}
                 misbehaving-fn)))))

    (testing "should not include instance counts when service description is nil"
      (is (= {} (statsd/process-update-app-instances-message {}
                                                             {:service-id :fee
                                                              :healthy-instances [:i]
                                                              :unhealthy-instances [:i :i]
                                                              :failed-instances [:i :i :i]}
                                                             (constantly nil)))))))

(deftest test-process-scheduler-messages
  (testing "Processing a batch of scheduler messages"
    (testing "should not let exceptions bubble out"
      (with-redefs [statsd/publish-instance-counts-by-metric-group (fn [_] (throw (Exception. "I'm misbehaving")))]
        (statsd/process-scheduler-messages [] (constantly {}))))))

(deftest test-publish-instance-counts-by-metric-group
  (testing "Publishing instance counts"
    (testing "should send a gauge for each of healthy, unhealthy, failed"
      (teardown-setup)
      (let [metrics (atom [])]
        (with-redefs [statsd/add-value (fn [_ metric-group metric value metric-type]
                                         (swap! metrics #(conj %1 [metric-group metric value metric-type]))
                                         {})]
          (statsd/publish-instance-counts-by-metric-group {"foo" {:healthy-instances 1
                                                                  :unhealthy-instances 2
                                                                  :failed-instances 3
                                                                  :cpus (* 3 0.1)
                                                                  :mem 384}
                                                           "bar" {:healthy-instances 4
                                                                  :unhealthy-instances 5
                                                                  :failed-instances 6
                                                                  :cpus (+ (* 3 0.2) (* 6 0.4))
                                                                  :mem 3840}
                                                           "baz" {:healthy-instances 7
                                                                  :unhealthy-instances 8
                                                                  :failed-instances 9
                                                                  :cpus (+ (* 6 0.8) (* 6 1.6) (* 3 3.2))
                                                                  :mem 30720}}))
        (statsd/drain)
        (is (= 15 (count @metrics)))
        (is (= ["foo" "instances.healthy" 1 statsd/gauge-metric] (nth @metrics 0)))
        (is (= ["foo" "instances.unhealthy" 2 statsd/gauge-metric] (nth @metrics 1)))
        (is (= ["foo" "instances.failed" 3 statsd/gauge-metric] (nth @metrics 2)))
        (is (= ["foo" "cpus" (* 3 0.1) statsd/gauge-metric] (nth @metrics 3)))
        (is (= ["foo" "mem" 384 statsd/gauge-metric] (nth @metrics 4)))
        (is (= ["bar" "instances.healthy" 4 statsd/gauge-metric] (nth @metrics 5)))
        (is (= ["bar" "instances.unhealthy" 5 statsd/gauge-metric] (nth @metrics 6)))
        (is (= ["bar" "instances.failed" 6 statsd/gauge-metric] (nth @metrics 7)))
        (is (= ["bar" "cpus" (+ (* 3 0.2) (* 6 0.4)) statsd/gauge-metric] (nth @metrics 8)))
        (is (= ["bar" "mem" 3840 statsd/gauge-metric] (nth @metrics 9)))
        (is (= ["baz" "instances.healthy" 7 statsd/gauge-metric] (nth @metrics 10)))
        (is (= ["baz" "instances.unhealthy" 8 statsd/gauge-metric] (nth @metrics 11)))
        (is (= ["baz" "instances.failed" 9 statsd/gauge-metric] (nth @metrics 12)))
        (is (= ["baz" "cpus" (+ (* 6 0.8) (* 6 1.6) (* 3 3.2)) statsd/gauge-metric] (nth @metrics 13)))
        (is (= ["baz" "mem" 30720 statsd/gauge-metric] (nth @metrics 14))))
      (teardown))))

(deftest test-unique!
  (testing "Adding a value to a set"
    (testing "should aggregate and send packets appropriately"
      (let [packets (capture-packets
                      (is (statsd/unique! "testing" "foo" "one"))
                      (is (statsd/unique! "testing" "foo" "two"))
                      (is (statsd/unique! "testing" "foo" "three")))]
        (is (= 3 (count packets)))
        (is (some #{(str "waiter.testing.env.cluster.foo:" (.hashCode "one") "|s")} packets))
        (is (some #{(str "waiter.testing.env.cluster.foo:" (.hashCode "two") "|s")} packets))
        (is (some #{(str "waiter.testing.env.cluster.foo:" (.hashCode "three") "|s")} packets))))))

(deftest test-gauge!
  (testing "Setting a gauge"
    (testing "should aggregate and send packets appropriately"
      (let [packets (capture-packets
                      (is (statsd/gauge! "testing" "foo" 1))
                      (is (statsd/gauge! "testing" "foo" 2))
                      (is (statsd/gauge! "testing" "foo" 3)))]
        (is (= 1 (count packets)))
        (is (= "waiter.testing.env.cluster.foo:3.0|g" (first packets)))))))

(deftest test-gauge-delta!
  (testing "Setting a gauge delta"
    (testing "should aggregate and send packets appropriately"
      (let [packets (capture-packets
                      (is (statsd/gauge-delta! "testing" "foo" +2))
                      (is (statsd/gauge-delta! "testing" "foo" -1))
                      (is (statsd/gauge-delta! "testing" "foo" -1)))]
        (is (= 1 (count packets)))
        (is (= "waiter.testing.env.cluster.server.foo:0.0|g" (first packets)))))))

(deftest test-inc!
  (testing "Incrementing a counter"
    (testing "should aggregate and send packets appropriately"
      (let [packets (capture-packets
                      (is (statsd/inc! "testing" "foo" 1))
                      (is (statsd/inc! "testing" "foo" 2))
                      (is (statsd/inc! "testing" "foo" 3)))]
        (is (= 1 (count packets)))
        (is (= "waiter.testing.env.cluster.server.foo:6|c" (first packets)))))))

(deftest test-histo!
  (testing "Setting a histogram value"

    (testing "should aggregate and send packets appropriately"
      (let [packets (capture-packets (dotimes [n 100] (statsd/histo! "testing" "foo" (inc n))))]
        (is (= 5 (count packets)))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p50:50.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p75:75.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p95:95.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p99:99.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p100:100.0|g")} packets))))

    (testing "should use sliding window behavior"
      (let [packets (capture-packets (dotimes [n 100000] (statsd/histo! "testing" "foo" (inc n))))]
        (is (= 5 (count packets)))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p50:95000.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p75:97500.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p95:99500.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p99:99900.0|g")} packets))
        (is (some #{(str "waiter.testing.env.cluster.server.foo_p100:100000.0|g")} packets))))))

(deftest test-publish-aggregated-values
  (testing "Publishing of locally aggregated values"

    (testing "should delete on publish as dictated by metric types"
      (teardown-setup)
      (is (= {["metric-group" "gauge-delta-1" statsd/gauge-delta-metric] 56}
             (statsd/publish-aggregated-values {["metric-group" "set" statsd/set-metric] #{1 2 3 4}
                                                ["metric-group" "gauge-delta-1" statsd/gauge-delta-metric] 56
                                                ["metric-group" "gauge-delta-2" statsd/gauge-delta-metric] 0
                                                ["metric-group" "gauge" statsd/gauge-metric] 78
                                                ["metric-group" "counter" statsd/counter-metric] 9})))
      (teardown))

    (testing "should call publish function for each locally aggregated metric"
      (teardown-setup)
      (let [published (atom [])
            metric-type {:publish-fn (fn [mg m v] (swap! published #(conj %1 [mg m v])))
                         :delete-on-publish?-fn (constantly true)}]
        (is (= {} (statsd/publish-aggregated-values {["foo" "bar" metric-type] "baz"
                                                     ["fee" "fie" metric-type] "foe"
                                                     ["one" "two" metric-type] "three"})))
        (is (= [["foo" "bar" "baz"]
                ["fee" "fie" "foe"]
                ["one" "two" "three"]] @published)))
      (teardown))))

(deftest test-set-publish
  (testing "Publishing of a set metric"
    (testing "should send a packet for each unique value"
      (let [packets (capture-packets (is (nil? (statsd/set-publish "foo" "bar" #{1 2 3 4 5}))))]
        (is (= 5 (count packets)))
        (is (some #{"waiter.foo.env.cluster.bar:1|s"} packets))
        (is (some #{"waiter.foo.env.cluster.bar:2|s"} packets))
        (is (some #{"waiter.foo.env.cluster.bar:3|s"} packets))
        (is (some #{"waiter.foo.env.cluster.bar:4|s"} packets))
        (is (some #{"waiter.foo.env.cluster.bar:5|s"} packets))))))

(deftest test-percentile
  (testing "Calculating percentile"

    (testing "should handle the happy path"
      (is (= 1 (statsd/percentile [1] 10)))
      (is (= 1 (statsd/percentile [1] 50)))
      (is (= 1 (statsd/percentile [1] 100)))
      (is (= 1 (statsd/percentile [1 2] 10)))
      (is (= 1 (statsd/percentile [1 2] 50)))
      (is (= 2 (statsd/percentile [1 2] 51)))
      (is (= 2 (statsd/percentile [1 2] 100)))
      (is (= 1 (statsd/percentile [1 2 3] (* 100 (/ 1 3)))))
      (is (= 2 (statsd/percentile [1 2 3] (* 100 (/ 2 3)))))
      (is (= 3 (statsd/percentile [1 2 3] (* 100 (/ 3 3)))))
      (is (= 20 (statsd/percentile [15 20 35 40 50] 30)))
      (is (= 20 (statsd/percentile [15 20 35 40 50] 40)))
      (is (= 35 (statsd/percentile [15 20 35 40 50] 50)))
      (is (= 50 (statsd/percentile [15 20 35 40 50] 100)))
      (is (= 7 (statsd/percentile [3 6 7 8 8 10 13 15 16 20] 25)))
      (is (= 8 (statsd/percentile [3 6 7 8 8 10 13 15 16 20] 50)))
      (is (= 15 (statsd/percentile [3 6 7 8 8 10 13 15 16 20] 75)))
      (is (= 20 (statsd/percentile [3 6 7 8 8 10 13 15 16 20] 100)))
      (is (= 7 (statsd/percentile [3 6 7 8 8 9 10 13 15 16 20] 25)))
      (is (= 9 (statsd/percentile [3 6 7 8 8 9 10 13 15 16 20] 50)))
      (is (= 15 (statsd/percentile [3 6 7 8 8 9 10 13 15 16 20] 75)))
      (is (= 20 (statsd/percentile [3 6 7 8 8 9 10 13 15 16 20] 100))))

    (testing "should handle edge cases"
      (is (nil? (statsd/percentile nil 50)))
      (is (nil? (statsd/percentile [] 50)))
      (is (nil? (statsd/percentile [1] 0)))
      (is (nil? (statsd/percentile [1] 100.1)))
      (is (nil? (statsd/percentile [1] nil)))
      (is (thrown? Exception (statsd/percentile {:foo 1 :bar 2} 50))))))

(deftest test-gauge
  (testing "Publishing a gauge"

    (testing "should support ratios"
      (let [packets (capture-packets (statsd/gauge (constantly "foo") (/ 45401903349 4)))]
        (is (= 1 (count packets)))
        (is (= "waiter.foo:11350475776.0|g" (first packets)))))

    (testing "should always call float on value"
      (let [packets (capture-packets (statsd/gauge (constantly "foo") 123))]
        (is (= 1 (count packets)))
        (is (= "waiter.foo:123.0|g" (first packets)))))))

(deftest test-percentiles
  (testing "Calculating multiple percentiles on one collection"
    (testing "should handle un-ordered collections"
      (is (= {25 7, 50 9, 75 15, 100 20}
             (statsd/percentiles [8 9 13 15 20 6 7 10 16 8 3] 25 50 75 100))))))

(deftest test-state
  (testing "Retrieving the aggregated state"
    (testing "should return a nested map grouped by type"
      (teardown-setup)
      (statsd/inc! "metric-group-1" "metric-1")
      (statsd/inc! "metric-group-1" "metric-2")
      (statsd/inc! "metric-group-2" "metric-2")
      (statsd/gauge! "metric-group-1" "metric-1" 12345)
      (statsd/gauge! "metric-group-1" "metric-2" 67890)
      (statsd/gauge! "metric-group-2" "metric-2" 67890)
      (statsd/gauge-delta! "metric-group-1" "metric-1" +2)
      (statsd/gauge-delta! "metric-group-1" "metric-2" -2)
      (statsd/gauge-delta! "metric-group-2" "metric-2" -2)
      (statsd/histo! "metric-group-1" "metric-1" 111)
      (statsd/histo! "metric-group-1" "metric-1" 222)
      (statsd/histo! "metric-group-1" "metric-1" 333)
      (statsd/histo! "metric-group-1" "metric-2" 444)
      (statsd/histo! "metric-group-1" "metric-2" 555)
      (statsd/histo! "metric-group-1" "metric-2" 666)
      (statsd/histo! "metric-group-2" "metric-2" 444)
      (statsd/histo! "metric-group-2" "metric-2" 555)
      (statsd/histo! "metric-group-2" "metric-2" 666)
      (statsd/unique! "metric-group-1" "metric-1" 12)
      (statsd/unique! "metric-group-1" "metric-1" 34)
      (statsd/unique! "metric-group-1" "metric-1" 56)
      (statsd/unique! "metric-group-1" "metric-2" 78)
      (statsd/unique! "metric-group-1" "metric-2" 90)
      (statsd/unique! "metric-group-2" "metric-2" 78)
      (statsd/unique! "metric-group-2" "metric-2" 90)
      (statsd/await-agents)
      (is (= {:counter {"metric-group-1" {"metric-1" 1, "metric-2" 1}
                        "metric-group-2" {"metric-2" 1}}
              :gauge {"metric-group-1" {"metric-1" 12345, "metric-2" 67890}
                      "metric-group-2" {"metric-2" 67890}}
              :gauge-delta {"metric-group-1" {"metric-1" 2, "metric-2" -2}
                            "metric-group-2" {"metric-2" -2}}
              :histo {"metric-group-1" {"metric-1" [111 222 333], "metric-2" [444 555 666]}
                      "metric-group-2" {"metric-2" [444 555 666]}}
              :set {"metric-group-1" {"metric-1" #{12 34 56}, "metric-2" #{78 90}}
                    "metric-group-2" {"metric-2" #{78 90}}}}
             (statsd/state))))))

(deftest test-bounded-conj
  (is (= [1] (statsd/bounded-conj PersistentQueue/EMPTY 1 "metric-grp")))
  (is (= (range 0 10000) (reduce #(statsd/bounded-conj %1 %2 "metric-grp") PersistentQueue/EMPTY (range 0 10000))))
  (is (= (range 1 10001) (reduce #(statsd/bounded-conj %1 %2 "metric-grp") PersistentQueue/EMPTY (range 0 10001))))
  (is (= (range 10000 20000) (reduce #(statsd/bounded-conj %1 %2 "metric-grp") PersistentQueue/EMPTY (range 0 20000)))))
