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
(ns waiter.reporters-test
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]] ;; not using :refer :all because clojure.test has "report" that conflicts with waiter.reporter/report
            [metrics.counters :as counters]
            [waiter.metrics :as metrics]
            [waiter.test-helpers :refer :all]
            [waiter.reporter :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (com.codahale.metrics ConsoleReporter)
           (com.codahale.metrics.graphite GraphiteSender)
           (java.io PrintStream ByteArrayOutputStream)))

(deftest console-reporter-bad-schema
  (is (thrown-with-msg? ExceptionInfo #"period-ms missing-required-key"
                        (validate-console-reporter-config {:extra-key 444}))))

(deftest console-reporter-good-schema
  (validate-console-reporter-config {:extra-key 444 :filter-regex #".*" :period-ms 300}))

(deftest graphite-reporter-bad-schema
  (is (thrown-with-msg? ExceptionInfo #"host missing-required-key"
                        (validate-graphite-reporter-config {:extra-key 444}))))

(deftest graphite-reporter-bad-schema-2
  (is (thrown-with-msg? ExceptionInfo #":port \(not \(pos\?"
                        (validate-graphite-reporter-config {:period-ms 300 :host "localhost" :port -7777}))))

(deftest graphite-reporter-bad-schema-3
  (is (thrown-with-msg? ExceptionInfo #":period-ms \(not \(integer\?"
                        (validate-graphite-reporter-config {:period-ms "five" :host "localhost" :port 7777}))))

(deftest graphite-reporter-good-schema
  (validate-graphite-reporter-config {:extra-key 444 :filter-regex #".*" :period-ms 300 :prefix "" :host "localhost" :port 7777}))

(defn make-printstream []
  (let [os (ByteArrayOutputStream.)
        ps (PrintStream. os)]
    {:ps ps :out #(.toString os "UTF8")}))

(deftest console-reporter-wildcard-filter
  (with-isolated-registry
    (metrics/service-counter "service-id" "foo")
    (metrics/service-counter "service-id" "foo" "bar")
    (metrics/service-counter "service-id" "fee" "fie")
    (counters/inc! (metrics/service-counter "service-id" "foo" "bar") 100)
    (let [{:keys [ps out]} (make-printstream)
          [console-reporter state] (make-console-reporter #".*" ps)]
      (is (instance? ConsoleReporter console-reporter))
      (.report console-reporter)
      (is (= "
-- Counters --------------------------------------------------------------------
services.service-id.counters.fee.fie
             count = 0
services.service-id.counters.foo
             count = 0
services.service-id.counters.foo.bar
             count = 100"
             (->> (out)
                  (str/split-lines)
                  (drop 1)
                  (str/join "\n"))))
      (is (= {:run-state :created} @state)))))

(deftest console-reporter-filter
  (with-isolated-registry
    (metrics/service-counter "service-id" "foo")
    (metrics/service-counter "service-id" "foo" "bar")
    (metrics/service-counter "service-id" "fee" "fie")
    (counters/inc! (metrics/service-counter "service-id" "foo" "bar") 100)
    (let [{:keys [ps out]} (make-printstream)
          [console-reporter state] (make-console-reporter #"^.*fee.*" ps)]
      (is (instance? ConsoleReporter console-reporter))
      (.report console-reporter)
      (is (= "
-- Counters --------------------------------------------------------------------
services.service-id.counters.fee.fie
             count = 0"
             (->> (out)
                  (str/split-lines)
                  (drop 1)
                  (str/join "\n"))))
      (is (= {:run-state :created} @state)))))

;; maximum expected test duration. used for fuzzy timestamp comparison
(def max-test-duration-ms 5000)

(deftest graphite-reporter-wildcard-filter
  (with-isolated-registry
    (metrics/service-counter "service-id" "foo")
    (metrics/service-counter "service-id" "foo" "bar")
    (metrics/service-counter "service-id" "fee" "fie")
    (metrics/service-histogram "service-id" "fum")
    (counters/inc! (metrics/service-counter "service-id" "foo" "bar") 100)
    (let [actual-values (atom {})
          time (t/now)
          graphite (reify GraphiteSender
                     (flush [_])
                     (getFailures [_] 0)
                     (isConnected [_] true)
                     (send [_ name value timestamp] (swap! actual-values assoc name value "timestamp" timestamp)))
          codahale-reporter (make-graphite-reporter 0 #".*" "prefix" graphite)]
      (is (satisfies? CodahaleReporter codahale-reporter))
      (with-redefs [t/now (constantly time)]
        (report codahale-reporter))
      (is (= #{"prefix.services.service-id.counters.fee.fie"
               "prefix.services.service-id.counters.foo"
               "prefix.services.service-id.counters.foo.bar"
               "prefix.services.service-id.histograms.fum.count"
               "prefix.services.service-id.histograms.fum.value.0_0"
               "prefix.services.service-id.histograms.fum.value.0_25"
               "prefix.services.service-id.histograms.fum.value.0_5"
               "prefix.services.service-id.histograms.fum.value.0_75"
               "prefix.services.service-id.histograms.fum.value.0_95"
               "prefix.services.service-id.histograms.fum.value.0_99"
               "prefix.services.service-id.histograms.fum.value.0_999"
               "prefix.services.service-id.histograms.fum.value.1_0"
               "timestamp"}
             (set (keys @actual-values))))
      (is (= (get @actual-values "prefix.services.service-id.counters.fee.fie") "0"))
      (is (= (get @actual-values "prefix.services.service-id.counters.foo") "0"))
      (is (= (get @actual-values "prefix.services.service-id.counters.foo.bar") "100"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.count") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.0_0") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.0_25") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.0_5") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.0_75") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.0_95") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.0_99") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.0_999") "0"))
      (is (= (get @actual-values "prefix.services.service-id.histograms.fum.value.1_0") "0"))
      (is (< (Math/abs (- (* 1000 (get @actual-values "timestamp")) (System/currentTimeMillis))) max-test-duration-ms))
      (is (= {:run-state :created
              :last-reporting-time time
              :failed-writes-to-server 0
              :last-report-successful true} (state codahale-reporter))))))

(deftest graphite-reporter-filter
  (with-isolated-registry
    (metrics/service-counter "service-id" "foo")
    (metrics/service-counter "service-id" "foo" "bar")
    (metrics/service-counter "service-id" "fee" "fie")
    (metrics/service-histogram "service-id" "fum")
    (counters/inc! (metrics/service-counter "service-id" "foo" "bar") 100)
    (let [actual-values (atom {})
          time (t/now)
          graphite (reify GraphiteSender
                     (flush [_])
                     (getFailures [_] 0)
                     (isConnected [_] true)
                     (send [_ name value timestamp] (swap! actual-values assoc name value "timestamp" timestamp)))
          codahale-reporter (make-graphite-reporter 0 #"^.*fee.*" "prefix" graphite)]
      (is (satisfies? CodahaleReporter codahale-reporter))
      (with-redefs [t/now (constantly time)]
        (report codahale-reporter))
      (is (= #{"prefix.services.service-id.counters.fee.fie"
               "timestamp"}
             (set (keys @actual-values))))
      (is (= (get @actual-values "prefix.services.service-id.counters.fee.fie") "0"))
      (is (< (Math/abs (- (* 1000 (get @actual-values "timestamp")) (System/currentTimeMillis))) max-test-duration-ms))
      (is (= {:run-state :created
              :last-reporting-time time
              :failed-writes-to-server 0
              :last-report-successful true} (state codahale-reporter))))))

(deftest graphite-reporter-wildcard-filter-exception
  (with-isolated-registry
    (metrics/service-counter "service-id" "foo")
    (metrics/service-counter "service-id" "foo" "bar")
    (metrics/service-counter "service-id" "fee" "fie")
    (counters/inc! (metrics/service-counter "service-id" "foo" "bar") 100)
    (let [actual-values (atom #{})
          time (t/now)
          graphite (reify GraphiteSender
                     (flush [_])
                     (getFailures [_] 0)
                     (isConnected [_] true)
                     (send [_ _ _ _] (throw (ex-info "test" {}))))
          codahale-reporter (make-graphite-reporter 0 #".*" "prefix" graphite)]
      (is (satisfies? CodahaleReporter codahale-reporter))
      (with-redefs [t/now (constantly time)]
        (is (thrown-with-msg? ExceptionInfo #"^test$"
                              (report codahale-reporter))))
      (is (= #{} @actual-values))
      (is (= {:run-state :created
              :last-send-failed-time time
              :failed-writes-to-server 0
              :last-report-successful false} (state codahale-reporter))))))