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
            [clojure.test :refer :all]
            [metrics.core :as mc]
            [metrics.counters :as counters]
            [waiter.metrics :refer :all]
            [waiter.reporter :as r])
  (:import (clojure.lang ExceptionInfo)
           (com.codahale.metrics MetricFilter MetricRegistry ConsoleReporter)
           (com.codahale.metrics.graphite GraphiteReporter GraphiteSender)
           (java.io PrintStream ByteArrayOutputStream)))

(def ^:private all-metrics-match-filter (reify MetricFilter (matches [_ _ _] true)))

(defmacro with-isolated-registry
  [& body]
  `(with-redefs [mc/default-registry (MetricRegistry.)]
     (.removeMatching mc/default-registry all-metrics-match-filter)
     (do ~@body)
     (.removeMatching mc/default-registry all-metrics-match-filter)))

(deftest console-reporter-bad-schema
  (is (thrown-with-msg? ExceptionInfo #"period-ms missing-required-key"
                        (r/validate-console-reporter-config {:extra-key 444}))))
(deftest console-reporter-good-schema
  (r/validate-console-reporter-config {:period-ms 300 :extra-key 444}))

(deftest graphite-reporter-bad-schema
  (is (thrown-with-msg? ExceptionInfo #"host missing-required-key"
                        (r/validate-graphite-reporter-config {:extra-key 444}))))
(deftest graphite-reporter-bad-schema-2
  (is (thrown-with-msg? ExceptionInfo #":port \(not \(pos\?"
                        (r/validate-graphite-reporter-config {:period-ms 300 :host "localhost" :port -7777}))))
(deftest graphite-reporter-bad-schema-3
  (is (thrown-with-msg? ExceptionInfo #":period-ms \(not \(integer\?"
                        (r/validate-graphite-reporter-config {:period-ms "five" :host "localhost" :port 7777}))))
(deftest graphite-reporter-good-schema
  (r/validate-graphite-reporter-config {:period-ms 300 :extra-key 444 :host "localhost" :port 7777}))

(defn make-printstream []
  (let [os (ByteArrayOutputStream.)
        ps (PrintStream. os)]
    {:ps ps :out #(.toString os "UTF8")}))

(deftest console-reporter-no-filter
  (with-isolated-registry
    (service-counter "service-id" "foo")
    (service-counter "service-id" "foo" "bar")
    (service-counter "service-id" "fee" "fie")
    (counters/inc! (service-counter "service-id" "foo" "bar") 100)
    (let [{:keys [ps out]} (make-printstream)
          [console-reporter state] (r/make-console-reporter nil ps)]
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
                  (clojure.string/split-lines)
                  (drop 1)
                  (clojure.string/join "\n"))))
      (is (= {:run-state :created} @state)))))

(deftest console-reporter-filter
  (with-isolated-registry
    (service-counter "service-id" "foo")
    (service-counter "service-id" "foo" "bar")
    (service-counter "service-id" "fee" "fie")
    (counters/inc! (service-counter "service-id" "foo" "bar") 100)
    (let [{:keys [ps out]} (make-printstream)
          [console-reporter state] (r/make-console-reporter #"^.*fee.*" ps)]
      (is (instance? ConsoleReporter console-reporter))
      (.report console-reporter)
      (is (= "
-- Counters --------------------------------------------------------------------
services.service-id.counters.fee.fie
             count = 0"
             (->> (out)
                  (clojure.string/split-lines)
                  (drop 1)
                  (clojure.string/join "\n"))))
      (is (= {:run-state :created} @state)))))

(deftest graphite-reporter-no-filter
  (with-isolated-registry
    (service-counter "service-id" "foo")
    (service-counter "service-id" "foo" "bar")
    (service-counter "service-id" "fee" "fie")
    (counters/inc! (service-counter "service-id" "foo" "bar") 100)
    (let [actual-values (atom #{})
          time (t/now)
          graphite (reify GraphiteSender
                     (flush [_])
                     (getFailures [_] 0)
                     (isConnected [_] true)
                     (send [_ name value _] (swap! actual-values #(conj % (str name value)))))
          [graphite-reporter state] (r/make-graphite-reporter nil "prefix" graphite)]
      (is (instance? GraphiteReporter graphite-reporter))
      (with-redefs [t/now (fn [] time)]
        (.report graphite-reporter))
      (is (= #{"prefix.services.service-id.counters.fee.fie.count0"
               "prefix.services.service-id.counters.foo.count0"
               "prefix.services.service-id.counters.foo.bar.count100"}
             @actual-values))
      (is (= {:run-state :created
              :last-reporting-time time
              :failed-writes-to-server 0
              :last-report-successful true} @state)))))

(deftest graphite-reporter-filter
  (with-isolated-registry
    (service-counter "service-id" "foo")
    (service-counter "service-id" "foo" "bar")
    (service-counter "service-id" "fee" "fie")
    (counters/inc! (service-counter "service-id" "foo" "bar") 100)
    (let [actual-values (atom #{})
          time (t/now)
          graphite (reify GraphiteSender
                     (flush [_])
                     (getFailures [_] 0)
                     (isConnected [_] true)
                     (send [_ name value _] (swap! actual-values #(conj % (str name value)))))
          [graphite-reporter state] (r/make-graphite-reporter #"^.*fee.*" "prefix" graphite)]
      (is (instance? GraphiteReporter graphite-reporter))
      (with-redefs [t/now (fn [] time)]
        (.report graphite-reporter))
      (is (= #{"prefix.services.service-id.counters.fee.fie.count0"}
             @actual-values))
      (is (= {:run-state :created
              :last-reporting-time time
              :failed-writes-to-server 0
              :last-report-successful true} @state)))))

(deftest graphite-reporter-no-filter-exception
  (with-isolated-registry
    (service-counter "service-id" "foo")
    (service-counter "service-id" "foo" "bar")
    (service-counter "service-id" "fee" "fie")
    (counters/inc! (service-counter "service-id" "foo" "bar") 100)
    (let [actual-values (atom #{})
          time (t/now)
          graphite (reify GraphiteSender
                     (flush [_])
                     (getFailures [_] 0)
                     (isConnected [_] true)
                     (send [_ _ _ _] (throw (ex-info "test" {}))))
          [graphite-reporter state] (r/make-graphite-reporter nil "prefix" graphite)]
      (is (instance? GraphiteReporter graphite-reporter))
      (with-redefs [t/now (fn [] time)]
        (is (thrown-with-msg? ExceptionInfo #"^test$"
                            (.report graphite-reporter))))
      (is (= #{} @actual-values))
      (is (= {:run-state :created
              :last-send-failed-time time
              :failed-writes-to-server 0
              :last-report-successful false} @state)))))