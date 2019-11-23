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
(ns waiter.latency-test
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all]))

(use-fixtures :once statsd-test-fixture)

(defn- headers->options [headers]
  (flatten (map (fn [[key val]] ["-H" (str (name key) ": " val)]) headers)))

(deftest test-headers->options
  (testing "Conversion of headers to command-line options"
    (testing "should stringify keys"
      (is (= ["-H" "x-waiter-foo: bar" "-H" "x-waiter-baz: qux"]
             (headers->options {:x-waiter-foo "bar" :x-waiter-baz "qux"}))))
    (testing "should not escape $"
      (is (= ["-H" "x-waiter-cmd: foo -p $PORT0"]
             (headers->options {:x-waiter-cmd "foo -p $PORT0"}))))))

(defn- parse-int [re s]
  (let [match (re-find re s)]
    (when match (read-string (second match)))))

(defn apache-bench [waiter-url endpoint headers concurrency-level total-requests]
  (let [header-options (headers->options headers)
        auth-cookie (auth-cookie waiter-url)
        _ (log/debug "Auth cookie:" auth-cookie)
        apache-bench-dir (System/getenv "APACHE_BENCH_DIR")
        _ (is (some? apache-bench-dir) "Please define the APACHE_BENCH_DIR environment variable")
        command (concat [(str apache-bench-dir "/ab")
                         "-c" (str concurrency-level)
                         "-n" (str total-requests)]
                        header-options
                        ["-H" (str "Cookie: x-waiter-auth=" auth-cookie ";")
                         (str "http://" waiter-url endpoint)])
        _ (log/info command)
        result (apply shell/sh command)
        out (:out result)
        complete-requests (parse-int #"Complete requests:\s+(\d+)" out)
        failed-requests (parse-int #"Failed requests:\s+(\d+)" out)
        write-errors (parse-int #"Write errors:\s+(\d+)" out)
        latency-50% (parse-int #"50%\s+(\d+)" out)
        latency-66% (parse-int #"66%\s+(\d+)" out)
        latency-75% (parse-int #"75%\s+(\d+)" out)
        latency-80% (parse-int #"80%\s+(\d+)" out)
        latency-90% (parse-int #"90%\s+(\d+)" out)
        non-2xx-responses (parse-int #"Non-2xx responses:\s+(\d+)" out)]
    (log/info out)
    (assoc result :complete-requests complete-requests
                  :failed-requests failed-requests
                  :write-errors write-errors
                  :latency-50% latency-50%
                  :latency-66% latency-66%
                  :latency-75% latency-75%
                  :latency-80% latency-80%
                  :latency-90% latency-90%
                  :non-2xx-responses (or non-2xx-responses 0))))

(deftest ^:perf test-request-latency-apache-bench
  (testing-using-waiter-url
    (when (using-marathon? waiter-url) ; TODO: this performance test only works on marathon
      (let [max-instances (Integer/parseInt (or (System/getenv "WAITER_TEST_REQUEST_LATENCY_MAX_INSTANCES") "50"))
            _ (log/info "using max-instances =" max-instances)
            client-concurrency-level 300
            waiter-concurrency-level 4
            total-requests 100000
            name (rand-name)
            extra-headers {:x-waiter-max-instances max-instances
                           :x-waiter-min-instances 1
                           :x-waiter-name name
                           :x-waiter-scale-down-factor 0.001
                           :x-waiter-scale-up-factor 0.999
                           :x-waiter-concurrency-level waiter-concurrency-level
                           :x-waiter-metric-group "stress_test"}
            headers (merge (kitchen-request-headers) extra-headers)
            endpoint "/endpoint"
            _ (log/info (str "Making canary request..."))
            canary-response (make-request waiter-url endpoint :headers headers)
            _ (assert-response-status canary-response 200)
            service-id (retrieve-service-id waiter-url (:request-headers canary-response))
            run-apache-bench #(apache-bench waiter-url endpoint headers client-concurrency-level %)
            warm-up-requests (/ total-requests 10)
            warm-up-run (run-apache-bench warm-up-requests)]
        (with-service-cleanup
          service-id
          (is (= 0 (:exit warm-up-run)) (:err warm-up-run))
          (is (= 0 (:failed-requests warm-up-run)))
          (is (or (= 0 (:write-errors warm-up-run)) (nil? (:write-errors warm-up-run))))
          (is (= 0 (:non-2xx-responses warm-up-run)))
          (is (= warm-up-requests (:complete-requests warm-up-run)))
          (if (= warm-up-requests (:non-2xx-responses warm-up-run))
            (log/error "All" warm-up-requests "requests had non-2xx responses, bailing out of perf test")
            (let [running-max-instances #(= max-instances (num-marathon-tasks-running waiter-url service-id))
                  running-close-to-max-instances #(<= (int (* 0.90 max-instances)) (num-marathon-tasks-running waiter-url service-id))
                  warmed-up (wait-for running-max-instances :interval 1 :timeout 60)]
              (if warmed-up
                (let [timing-run (run-apache-bench total-requests)]
                  (is (running-close-to-max-instances))
                  (is (= 0 (:exit timing-run)) (:err timing-run))
                  (is (> (/ 1 1000) (/ (:failed-requests timing-run) total-requests)))
                  (is (or (= 0 (:write-errors timing-run)) (nil? (:write-errors timing-run))))
                  (is (= 0 (:non-2xx-responses warm-up-run)))
                  (is (= total-requests (:complete-requests timing-run)))
                  (log/info "Number of async-threads Waiter is using =" (:async-threads (waiter-settings waiter-url)))
                  (statsd-timing "test_request_latency_apache_bench" waiter-url "latency_50" (:latency-50% timing-run))
                  (statsd-timing "test_request_latency_apache_bench" waiter-url "latency_66" (:latency-66% timing-run))
                  (statsd-timing "test_request_latency_apache_bench" waiter-url "latency_75" (:latency-75% timing-run))
                  (statsd-timing "test_request_latency_apache_bench" waiter-url "latency_80" (:latency-80% timing-run))
                  (statsd-timing "test_request_latency_apache_bench" waiter-url "latency_90" (:latency-90% timing-run)))
                (log/warn "Failed to warm-up properly")))))))))
