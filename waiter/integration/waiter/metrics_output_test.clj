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
(ns waiter.metrics-output-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [waiter.schema :as schema]
            [waiter.util.client-tools :refer :all]))

(def quantile-metric-schema
  {(s/required-key "count") schema/non-negative-num
   (s/required-key "value") {(s/required-key "0.0") schema/non-negative-num
                             (s/required-key "0.25") schema/non-negative-num
                             (s/required-key "0.5") schema/non-negative-num
                             (s/required-key "0.75") schema/non-negative-num
                             (s/required-key "0.95") schema/non-negative-num
                             s/Str s/Any}})

(def rate-metric-schema
  {(s/required-key "count") schema/non-negative-num
   (s/required-key "value") schema/non-negative-num})

(def gauge-metric-schema
  {(s/required-key "value") schema/non-negative-num})

(def counted-gauge-metric-schema
  {(s/required-key "count") gauge-metric-schema})

(def service-metrics-schema
  {(s/required-key "counters") {(s/required-key "request-counts") {(s/required-key "outstanding") schema/non-negative-num
                                                                   (s/required-key "successful") schema/positive-num
                                                                   (s/required-key "total") schema/positive-num
                                                                   (s/required-key "streaming") schema/non-negative-num
                                                                   (s/required-key "waiting-for-available-instance") schema/non-negative-num
                                                                   (s/required-key "waiting-to-stream") schema/non-negative-num
                                                                   s/Str s/Any}
                                (s/required-key "response-status") {(s/required-key "200") schema/non-negative-num
                                                                    s/Str s/Any}
                                s/Str s/Any}
   (s/required-key "histograms") {(s/required-key "iterations-to-find-available-instance") quantile-metric-schema
                                  (s/required-key "timing-out-pipeline-buffer-size") quantile-metric-schema
                                  s/Str s/Any}
   (s/required-key "meters") {(s/required-key "response-status-rate") {(s/required-key "200") rate-metric-schema
                                                                       s/Str s/Any}
                              (s/required-key "request-rate") rate-metric-schema
                              s/Str s/Any}
   (s/required-key "timers") {(s/required-key "backend-response") quantile-metric-schema
                              (s/required-key "get-available-instance") quantile-metric-schema
                              (s/required-key "get-task") quantile-metric-schema
                              (s/required-key "process") quantile-metric-schema
                              (s/required-key "reserve-instance") quantile-metric-schema
                              (s/required-key "stream") quantile-metric-schema
                              (s/required-key "update-responder-state") quantile-metric-schema
                              s/Str s/Any}
   s/Str s/Any})

(def jvm-metrics-schema
  {(s/required-key "attribute") s/Any
   (s/required-key "file") s/Any
   (s/required-key "gc") s/Any
   (s/required-key "memory") s/Any
   (s/required-key "thread") {(s/required-key "blocked") counted-gauge-metric-schema
                              (s/required-key "count") gauge-metric-schema
                              (s/required-key "daemon") counted-gauge-metric-schema
                              (s/required-key "deadlock") counted-gauge-metric-schema
                              (s/required-key "deadlocks") s/Any
                              (s/required-key "new") counted-gauge-metric-schema
                              (s/required-key "runnable") counted-gauge-metric-schema
                              (s/required-key "terminated") counted-gauge-metric-schema
                              (s/required-key "timed_waiting") counted-gauge-metric-schema
                              (s/required-key "waiting") counted-gauge-metric-schema
                              s/Str s/Any}
   s/Str s/Any})

(def waiter-metrics-schema
  {(s/required-key "autoscaler") s/Any
   (s/required-key "core") s/Any
   (s/optional-key "gc") s/Any
   (s/required-key "requests") s/Any
   (s/required-key "state") s/Any
   s/Str s/Any})

(defmacro assert-metrics-output
  [metrics-data metrics-schema]
  `(is (nil? (s/check ~metrics-schema ~metrics-data))
       (str ~metrics-data)))

(deftest ^:parallel ^:integration-fast test-metrics-output
  (testing-using-waiter-url
    (let [router->endpoint (routers waiter-url)
          router-urls (vec (vals router->endpoint))
          service-id (rand-name)
          headers {:x-waiter-name service-id}
          {:keys [service-id cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          num-requests 100]
      ; make requests to the app from various routers to make sure we have metrics
      (dotimes [n num-requests]
        (let [router-url (nth router-urls (mod n (count router-urls)))]
          (make-kitchen-request router-url headers :cookies cookies)))

      ; ensure each router has had a chance to publish its local metrics
      (let [waiter-settings (waiter-settings waiter-url)
            metrics-sync-interval-ms (get-in waiter-settings [:metrics-config :metrics-sync-interval-ms] 1)]
        (Thread/sleep (max (* 10 metrics-sync-interval-ms) 10000)))

      (doall (map (fn [router-id]
                    (let [router-url (str (get router->endpoint router-id))
                          metrics-json-response (make-request router-url "/metrics")
                          metrics-response (try-parse-json (:body metrics-json-response))
                          service-metrics (get-in metrics-response ["services" service-id])]
                      (log/info "asserting jvm metrics output for" router-url)
                      (assert-metrics-output (get metrics-response "jvm") jvm-metrics-schema)
                      (log/info "asserting service metrics output for" router-url)
                      (assert-metrics-output service-metrics service-metrics-schema)
                      (log/info "asserting waiter metrics output for" router-url)
                      (assert-metrics-output (get metrics-response "waiter") waiter-metrics-schema)))
                  (keys router->endpoint)))

      (let [apps-response (service-settings waiter-url service-id :keywordize-keys false)
            routers->metrics (get-in apps-response ["metrics" "routers"])
            aggregate-metrics (get-in apps-response ["metrics" "aggregate"])]
        (when (get apps-response "error-messages")
          (log/info "error messages from /apps:" (get apps-response "error-messages")))
        (is (pos? (count routers->metrics)))
        (doseq [[router-id metrics] routers->metrics]
          (log/info "asserting /apps output for" router-id)
          (assert-metrics-output metrics service-metrics-schema))
        (log/info "asserting aggregate /apps output")
        (assert-metrics-output aggregate-metrics service-metrics-schema)
        (is (number? (get aggregate-metrics "routers-sent-requests-to")))
        (is (>= (get-in aggregate-metrics ["counters" "request-counts" "total"]) num-requests)))

      (delete-service waiter-url service-id))))

(defmacro get-percentile-value
  "Look up percentile from metric, also asserting that it's present.
   The percentile argument should be a string (e.g., \"1.0\" or \"0.95\")."
  [metric p]
  `(let [p# ~p
         p-value# (get-in ~metric ["value" p#])]
     (is (number? p-value#) (str "missing p" p# " value"))
     p-value#))

(defn- n-healthy-instances-observed?
  "Returns true if the launch-metrics state reflects at least $n$ healthy
   instances in the instance counts for the given service on the given router."
  [router-url cookies service-id n]
  (-> (make-request router-url "/state/launch-metrics" :cookies cookies :verbose true)
      :body
      try-parse-json
      (get-in ["state" "service-id->launch-tracker" service-id "instance-counts" "healthy"] 0)
      (>= n)))

(defn- tracked-service-id?
  "Returns true if the launch-metrics state exists for the given service on the given router."
  [router-url cookies service-id]
  (-> (make-request router-url "/state/launch-metrics" :cookies cookies :verbose true)
      :body
      try-parse-json
      (get-in ["state" "service-id->launch-tracker" service-id])
      some?))

(deftest ^:parallel ^:integration-slow test-launch-metrics-output
  (testing-using-waiter-url
    (let [waiter-settings (waiter-settings waiter-url)
          metrics-sync-interval-ms (get-in waiter-settings [:metrics-config :metrics-sync-interval-ms])
          router->endpoint (routers waiter-url)
          router-urls (vals router->endpoint)
          service-name (rand-name)
          sleep-millis 20000
          min-startup-seconds 10 ; 20s +/- 10s for 2 polls with 5s granularity
          max-startup-seconds 60 ; the service shouldn't take more than a minute to become healthy
          instance-count 2
          req-headers {:x-waiter-cmd (kitchen-cmd (str "-p $PORT0 --start-up-sleep-ms " sleep-millis))
                       :x-waiter-cmd-type "shell"
                       :x-waiter-min-instances instance-count
                       :x-waiter-name service-name}
          {:keys [cookies headers request-headers service-id] :as first-response}
          (make-request-with-debug-info req-headers #(make-kitchen-request waiter-url % :method :get))]
      (with-service-cleanup
        service-id
        ; ensure the first request succeded before continuing with testing
        (assert-response-status first-response 200)
        ; on each router, check that the launch-metrics are present and have sane values
        (doseq [[router-id router-url] router->endpoint]
          (is (wait-for #(n-healthy-instances-observed? router-url cookies service-id instance-count)
                        :interval 1 :timeout (* 2 max-startup-seconds)))
          (let [metrics-response (->> "/metrics"
                                      (make-request router-url)
                                      :body
                                      try-parse-json)
                service-launch-metrics (get-in metrics-response ["services" service-id "timers" "launch-overhead"])
                service-scheduling-metric (get service-launch-metrics "schedule-time")
                service-startup-metric (get service-launch-metrics "startup-time")
                waiter-scheduling-metric (get-in metrics-response ["waiter" "launch-overhead" "timers" "schedule-time"])]
            (testing "all launch metrics present"
              (is (every? some? [service-scheduling-metric service-startup-metric waiter-scheduling-metric])))
            (testing "expected launch-metric instance counts"
              (is (== instance-count
                      (get service-scheduling-metric "count")))
              (is (<= instance-count
                      (get waiter-scheduling-metric "count"))))
            (testing "reasonable values for current service's startup-time metrics"
              (is (<= min-startup-seconds
                      (get-percentile-value service-startup-metric "1.0")
                      max-startup-seconds)))
            (testing "reasonable values for scheduling-time metrics"
              (is (<= (get-percentile-value waiter-scheduling-metric "0.0")
                      (get-percentile-value service-scheduling-metric "0.0")))
              (is (<= (get-percentile-value service-scheduling-metric "1.0")
                      (get-percentile-value waiter-scheduling-metric "1.0")))))))
      (testing "launch trackers are removed when service is deleted"
        (doseq [[router-id router-url] router->endpoint]
          (is (wait-for #(not (tracked-service-id? router-url cookies service-id))
                        :interval 1 :timeout 10))
          (log/info "Cleaned up" service-id "state on router" router-id))))))
