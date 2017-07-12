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
(ns waiter.metrics-output-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [waiter.client-tools :refer :all]
            [waiter.schema :as schema]))

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
                          metrics-response (json/read-str (:body metrics-json-response))
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
