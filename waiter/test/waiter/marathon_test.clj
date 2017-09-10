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
(ns waiter.marathon-test
  (:require [clj-http.client :as http]
            [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [marathonclj.rest.apps :as apps]
            [slingshot.slingshot :as ss]
            [waiter.marathon :refer :all]
            [waiter.scheduler :as scheduler]
            [waiter.utils :as utils])
  (:import waiter.marathon.MarathonScheduler))

(deftest test-response-data->service-instances
  (let [test-cases (list {
                          :name "response-data->service-instances no response"
                          :marathon-response nil
                          :expected-response {:active-instances []
                                              :failed-instances []
                                              :killed-instances []}},
                         {
                          :name "response-data->service-instances empty response"
                          :marathon-response {}
                          :expected-response {:active-instances []
                                              :failed-instances []
                                              :killed-instances []}},
                         {
                          :name "response-data->service-instances empty-app response"
                          :marathon-response {:app {}}
                          :expected-response {:active-instances []
                                              :failed-instances []
                                              :killed-instances []}},
                         {
                          :name "response-data->service-instances valid response with task failure"
                          :marathon-response {
                                              :app {
                                                    :id "test-app-1234",
                                                    :instances 3,
                                                    :healthChecks [{:path "/health", :portIndex 0, :protocol "HTTPS", :timeoutSeconds 10}],
                                                    :lastTaskFailure {
                                                                      :appId "test-app-1234",
                                                                      :host "10.141.141.10",
                                                                      :message "Abnormal executor termination",
                                                                      :state "TASK_FAILED",
                                                                      :taskId "test-app-1234.D",
                                                                      :timestamp "2014-09-12T232341.711Z",
                                                                      :version "2014-09-12T232821.737Z"},
                                                    :tasks [
                                                            {
                                                             :appId "test-app-1234",
                                                             :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                                   :firstSuccess "2014-09-13T002028.101Z",
                                                                                   :lastFailure nil, :lastSuccess "2014-09-13T002507.506Z",
                                                                                   :taskId "test-app-1234.A"}],
                                                             :host "10.141.141.11",
                                                             :id "/test-app-1234.A",
                                                             :ports [31045],
                                                             :stagedAt "2014-09-12T232828.594Z",
                                                             :startedAt "2014-09-13T002446.959Z",
                                                             :version "2014-09-12T232821.737Z"},
                                                            {
                                                             :appId "/test-app-1234",
                                                             :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                                   :firstSuccess "2014-09-13T002028.101Z",
                                                                                   :lastFailure nil, :lastSuccess "2014-09-13T002507.508Z",
                                                                                   :taskId "test-app-1234.B"}],
                                                             :host "10.141.141.12",
                                                             :id "/test-app-1234.B",
                                                             :ports [31234],
                                                             :stagedAt "2014-09-12T232822.587Z",
                                                             :startedAt "2014-09-13T002456.965Z",
                                                             :version "2014-09-12T232821.737Z"},
                                                            {
                                                             :appId "/test-app-1234",
                                                             :healthCheckResults [{:alive false, :consecutiveFailures 10,
                                                                                   :firstSuccess "2014-09-13T002028.101Z",
                                                                                   :lastFailure "2014-09-13T002507.508Z", :lastSuccess nil,
                                                                                   :taskId "test-app-1234.C"}],
                                                             :host "10.141.141.13",
                                                             :id "test-app-1234.C",
                                                             :ports [41234 12321 90384 56463],
                                                             :stagedAt "2014-09-12T232822.587Z",
                                                             :startedAt "2014-09-14T002446.965Z",
                                                             :version "2014-09-12T232821.737Z"}],}}

                          :expected-response {:active-instances (list
                                                                  (scheduler/make-ServiceInstance
                                                                    {:extra-ports [],
                                                                     :healthy? true,
                                                                     :host "10.141.141.11",
                                                                     :id "test-app-1234.A",
                                                                     :log-directory nil,
                                                                     :message nil,
                                                                     :port 31045,
                                                                     :protocol "https",
                                                                     :service-id "test-app-1234",
                                                                     :started-at "2014-09-13T002446.959Z"}),
                                                                  (scheduler/make-ServiceInstance
                                                                    {:extra-ports [],
                                                                     :healthy? true,
                                                                     :host "10.141.141.12",
                                                                     :id "test-app-1234.B",
                                                                     :log-directory nil,
                                                                     :message nil,
                                                                     :port 31234,
                                                                     :protocol "https",
                                                                     :service-id "test-app-1234",
                                                                     :started-at "2014-09-13T002456.965Z"}),
                                                                  (scheduler/make-ServiceInstance
                                                                    {:extra-ports [12321 90384 56463],
                                                                     :healthy? false,
                                                                     :host "10.141.141.13",
                                                                     :id "test-app-1234.C",
                                                                     :log-directory nil,
                                                                     :message nil,
                                                                     :port 41234,
                                                                     :protocol "https",
                                                                     :service-id "test-app-1234",
                                                                     :started-at "2014-09-14T002446.965Z"}))
                                              :failed-instances (list
                                                                  (scheduler/make-ServiceInstance
                                                                    {:extra-ports [],
                                                                     :healthy? false,
                                                                     :host "10.141.141.10",
                                                                     :id "test-app-1234.D",
                                                                     :log-directory nil,
                                                                     :message "Abnormal executor termination",
                                                                     :port 0,
                                                                     :service-id "test-app-1234",
                                                                     :started-at "2014-09-12T232341.711Z"}))
                                              :killed-instances []}},
                         {
                          :name "response-data->service-instances valid response without task failure"
                          :marathon-response {
                                              :framework-id "F123445"
                                              :app {
                                                    :id "test-app-1234",
                                                    :instances 3,
                                                    :healthChecks [{:path "/health", :portIndex 0, :protocol "HTTP", :timeoutSeconds 10}],
                                                    :tasks [
                                                            {
                                                             :appId "/test-app-1234",
                                                             :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                                   :firstSuccess "2014-09-13T002028.101Z",
                                                                                   :lastFailure nil, :lastSuccess "2014-09-13T002507.506Z",
                                                                                   :taskId "test-app-1234.A"}],
                                                             :host "10.141.141.11",
                                                             :id "test-app-1234.A",
                                                             :ports [31045],
                                                             :stagedAt "2014-09-12T232828.594Z",
                                                             :startedAt "2014-09-13T002446.959Z",
                                                             :version "2014-09-12T232821.737Z",
                                                             :slaveId "S234842"},
                                                            {
                                                             :appId "test-app-1234",
                                                             :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                                   :firstSuccess "2014-09-13T002028.101Z",
                                                                                   :lastFailure nil, :lastSuccess "2014-09-13T002507.508Z",
                                                                                   :taskId "test-app-1234.B"}],
                                                             :host "10.141.141.12",
                                                             :id "test-app-1234.B",
                                                             :ports [31234],
                                                             :stagedAt "2014-09-12T232822.587Z",
                                                             :startedAt "2014-09-13T002446.965Z",
                                                             :version "2014-09-12T232821.737Z"},
                                                            {
                                                             :appId "/test-app-1234",
                                                             :healthCheckResults [{:alive false, :consecutiveFailures 10,
                                                                                   :firstSuccess "2014-09-13T002028.101Z",
                                                                                   :lastFailure "2014-09-13T002507.508Z", :lastSuccess nil,
                                                                                   :taskId "/test-app-1234.C"}],
                                                             :host "10.141.141.13",
                                                             :id "test-app-1234.C",
                                                             :ports [41234],
                                                             :stagedAt "2014-09-12T232822.587Z",
                                                             :startedAt "2014-09-13T002446.965Z",
                                                             :version "2014-09-12T232821.737Z",
                                                             :slaveId "S651616"}],}}

                          :expected-response {:active-instances (list
                                                                  (scheduler/make-ServiceInstance
                                                                    {:extra-ports [],
                                                                     :healthy? true,
                                                                     :host "10.141.141.11",
                                                                     :id "test-app-1234.A",
                                                                     :log-directory "/slave-dir/S234842/frameworks/F123445/executors/test-app-1234.A/runs/latest",
                                                                     :message nil,
                                                                     :port 31045,
                                                                     :protocol "http",
                                                                     :service-id "test-app-1234",
                                                                     :started-at "2014-09-13T002446.959Z"}),
                                                                  (scheduler/make-ServiceInstance
                                                                    {:extra-ports [],
                                                                     :healthy? true,
                                                                     :host "10.141.141.12",
                                                                     :id "test-app-1234.B",
                                                                     :log-directory nil,
                                                                     :message nil,
                                                                     :port 31234,
                                                                     :protocol "http",
                                                                     :service-id "test-app-1234",
                                                                     :started-at "2014-09-13T002446.965Z"}),
                                                                  (scheduler/make-ServiceInstance
                                                                    {:extra-ports [],
                                                                     :healthy? false,
                                                                     :host "10.141.141.13",
                                                                     :id "test-app-1234.C",
                                                                     :log-directory "/slave-dir/S651616/frameworks/F123445/executors/test-app-1234.C/runs/latest",
                                                                     :message nil,
                                                                     :port 41234,
                                                                     :protocol "http",
                                                                     :service-id "test-app-1234",
                                                                     :started-at "2014-09-13T002446.965Z"}))
                                              :failed-instances []
                                              :killed-instances []}})]
    (doseq [{:keys [expected-response marathon-response name]} test-cases]
      (testing (str "Test " name)
        (let [framework-id (:framework-id marathon-response)
              service-id->failed-instances-transient-store (atom {})
              actual-response (response-data->service-instances
                                marathon-response
                                [:app]
                                (fn [] framework-id)
                                "/slave-dir"
                                service-id->failed-instances-transient-store)]
          (is (= expected-response actual-response) (str name))
          (scheduler/preserve-only-killed-instances-for-services! [])
          (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store []))))))

(deftest test-response-data->service->service-instances
  (let [input [{:id "test-app-1234"
                :instances 2
                :healthChecks [{:path "/ping", :portIndex 0, :protocol "HTTPS", :timeoutSeconds 10}]
                :tasks [{:appId "/test-app-1234"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T002028.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.506Z"
                                               :taskId "test-app-1234.A"}]
                         :host "10.141.141.11"
                         :id "test-app-1234.A"
                         :ports [31045]
                         :stagedAt "2014-09-12T232828.594Z"
                         :startedAt "2014-09-13T002446.959Z"
                         :version "2014-09-12T232821.737Z"}
                        {:appId "test-app-1234"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T002028.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.508Z"
                                               :taskId "test-app-1234.B"}]
                         :host "10.141.141.12"
                         :id "test-app-1234.B"
                         :ports [31234]
                         :stagedAt "2014-09-12T232822.587Z"
                         :startedAt "2014-09-13T002446.965Z"
                         :version "2014-09-12T232821.737Z"}]
                :tasksRunning 2
                :tasksHealthy 2
                :tasksUnhealthy 0
                :tasksStaged 0}
               {:id "test-app-6789"
                :instances 3
                :lastTaskFailure {:appId "test-app-6789"
                                  :host "10.141.141.10"
                                  :message "Abnormal executor termination"
                                  :state "TASK_FAILED"
                                  :taskId "test-app-6789.D"
                                  :timestamp "2014-09-12T232341.711Z"
                                  :version "2014-09-12T232821.737Z"}
                :healthChecks [{:path "/health", :portIndex 0, :protocol "HTTP", :timeoutSeconds 10}]
                :tasks [{:appId "test-app-6789"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T002028.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.506Z"
                                               :taskId "test-app-6789.A"}]
                         :host "10.141.141.11"
                         :id "/test-app-6789.A"
                         :ports [31045]
                         :stagedAt "2014-09-12T232828.594Z"
                         :startedAt "2014-09-13T002446.959Z"
                         :version "2014-09-12T232821.737Z"}
                        {:appId "/test-app-6789"
                         :host "10.141.141.12"
                         :id "/test-app-6789.B"
                         :ports [36789]
                         :stagedAt "2014-09-12T232822.587Z"
                         :startedAt "2014-09-13T002456.965Z"
                         :version "2014-09-12T232821.737Z"}
                        {:appId "/test-app-6789"
                         :healthCheckResults [{:alive false
                                               :consecutiveFailures 10
                                               :firstSuccess "2014-09-13T002028.101Z"
                                               :lastFailure "2014-09-13T002507.508Z"
                                               :lastSuccess nil
                                               :taskId "test-app-6789.C"}]
                         :host "10.141.141.13"
                         :id "test-app-6789.C"
                         :ports [46789]
                         :stagedAt "2014-09-12T232822.587Z"
                         :startedAt "2014-09-14T002446.965Z"
                         :version "2014-09-12T232821.737Z"}]}]
        expected (assoc {}
                   (scheduler/make-Service {:id "test-app-1234"
                                            :instances 2
                                            :task-count 2
                                            :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
                   {:active-instances
                    (list
                      (scheduler/make-ServiceInstance
                        {:id "test-app-1234.A"
                         :service-id "test-app-1234"
                         :healthy? true
                         :host "10.141.141.11"
                         :port 31045
                         :protocol "https"
                         :started-at "2014-09-13T002446.959Z"})
                      (scheduler/make-ServiceInstance
                        {:id "test-app-1234.B"
                         :service-id "test-app-1234"
                         :healthy? true
                         :host "10.141.141.12"
                         :port 31234
                         :protocol "https"
                         :started-at "2014-09-13T002446.965Z"}))
                    :failed-instances []
                    :killed-instances []}
                   (scheduler/make-Service {:id "test-app-6789", :instances 3, :task-count 3})
                   {:active-instances
                    (list
                      (scheduler/make-ServiceInstance
                        {:id "test-app-6789.A"
                         :service-id "test-app-6789"
                         :healthy? true
                         :host "10.141.141.11"
                         :port 31045
                         :protocol "http"
                         :started-at "2014-09-13T002446.959Z"})
                      (scheduler/make-ServiceInstance
                        {:id "test-app-6789.B"
                         :service-id "test-app-6789"
                         :healthy? nil
                         :host "10.141.141.12"
                         :port 36789
                         :protocol "http"
                         :started-at "2014-09-13T002456.965Z"})
                      (scheduler/make-ServiceInstance
                        {:id "test-app-6789.C"
                         :service-id "test-app-6789"
                         :healthy? false
                         :host "10.141.141.13"
                         :port 46789
                         :protocol "http"
                         :started-at "2014-09-14T002446.965Z"}))
                    :failed-instances
                    (list
                      (scheduler/make-ServiceInstance
                        {:id "test-app-6789.D"
                         :service-id "test-app-6789"
                         :healthy? false
                         :host "10.141.141.10"
                         :port 0
                         :started-at "2014-09-12T232341.711Z"
                         :message "Abnormal executor termination"}))
                    :killed-instances []})
        service-id->failed-instances-transient-store (atom {})
        actual (response-data->service->service-instances input (fn [] nil) nil
                                                          service-id->failed-instances-transient-store)]
    (is (= expected actual))
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])))

(deftest test-service-id->failed-instances-transient-store
  (let [faled-instance-response-fn (fn [service-id instance-id]
                                     {:appId service-id,
                                      :host (str "10.141.141." instance-id),
                                      :message "Abnormal executor termination",
                                      :state (str instance-id "failed"),
                                      :taskId (str service-id "." instance-id),
                                      :timestamp "2014-09-12T232341.711Z",
                                      :version "2014-09-12T232821.737Z"})
        framework-id "framework-id"
        health-check-url "/status"
        slave-directory "/slave"
        common-extractor-fn (fn [instance-id marathon-task-response]
                              (let [{:keys [appId host message slaveId]} marathon-task-response]
                                (cond-> {:service-id appId
                                         :host host
                                         :health-check-path health-check-url}
                                        (and framework-id slaveId)
                                        (assoc :log-directory
                                               (str slave-directory "/" slaveId "/frameworks/" framework-id
                                                    "/executors/" instance-id "/runs/latest"))
                                        message
                                        (assoc :message (str/trim message)))))
        service-id-1 "test-service-id-failed-instances-1"
        service-id-2 "test-service-id-failed-instances-2"
        service-id->failed-instances-transient-store (atom {})]
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "B") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "B") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "C") common-extractor-fn)
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "D") common-extractor-fn)
    (is (= 4 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "B") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "A") common-extractor-fn)
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "C") common-extractor-fn)
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "D") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "E") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "F") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "G") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "H") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1 (faled-instance-response-fn service-id-1 "I") common-extractor-fn)
    (is (= 9 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2 (faled-instance-response-fn service-id-2 "X") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2 (faled-instance-response-fn service-id-2 "Y") common-extractor-fn)
    (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2 (faled-instance-response-fn service-id-2 "Z") common-extractor-fn)
    (remove-failed-instances-for-service! service-id->failed-instances-transient-store service-id-1)
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [service-id-2])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))))

(deftest test-retrieve-log-url
  (let [instance-id "service-id-1.instance-id-2"
        host "www.example.com"
        mesos-slave-port 5051]
    (with-redefs [http/get (fn [url _]
                             (is (every? #(str/includes? url %) [host "5051" "state.json"]))
                             (let [response-body "
                                   {
                                    \"frameworks\": [{
                                                   \"role\": \"marathon\",
                                                   \"completed_executors\": [{
                                                                            \"id\": \"service-id-1.instance-id-1\",
                                                                            \"directory\": \"/path/to/instance1/directory\"
                                                                            }],
                                                   \"executors\": [{
                                                                  \"id\": \"service-id-1.instance-id-2\",
                                                                  \"directory\": \"/path/to/instance2/directory\"
                                                                  }]
                                                   }]
                                    }"]
                               {:body response-body}))]
      (is (= "/path/to/instance2/directory" (retrieve-log-url {} mesos-slave-port instance-id host))))))

(deftest test-retrieve-directory-content-from-host
  (let [service-id "service-id-1"
        instance-id "service-id-1.instance-id-2"
        host "www.example.com"
        mesos-slave-port 5051
        directory "/path/to/instance2/directory"]
    (with-redefs [http/get (fn [url _]
                             (is (every? #(str/includes? url %) [host "5051" "files/browse?path="]))
                             (let [response-body "
                                   [{\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil1\", \"size\": 1000},
                                    {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir2\", \"size\": 2000},
                                    {\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil3\", \"size\": 3000},
                                    {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir4\", \"size\": 4000}]"]
                               {:body response-body}))]
      (let [expected-result (list {:name "fil1"
                                   :size 1000
                                   :type "file"
                                   :url "http://www.example.com:5051/files/download?path=/path/to/instance2/directory/fil1"}
                                  {:name "dir2"
                                   :size 2000
                                   :type "directory"
                                   :path "/path/to/instance2/directory/dir2"}
                                  {:name "fil3"
                                   :size 3000
                                   :type "file"
                                   :url "http://www.example.com:5051/files/download?path=/path/to/instance2/directory/fil3"}
                                  {:name "dir4"
                                   :size 4000
                                   :type "directory"
                                   :path "/path/to/instance2/directory/dir4"})]
        (is (= expected-result (retrieve-directory-content-from-host {} mesos-slave-port service-id instance-id host directory)))))))

(deftest test-marathon-descriptor
  (let [service-id->password-fn (fn [service-id] (str service-id "-password"))]
    (testing "basic-test-with-defaults"
      (let [expected {:id "test-service-1"
                      :labels {:source "waiter"
                               :user "test-user"}
                      :env {"WAITER_USERNAME" "waiter"
                            "WAITER_PASSWORD" "test-service-1-password"
                            "HOME" "/home/path/test-user"
                            "LOGNAME" "test-user"
                            "USER" "test-user"
                            "FOO" "bar"
                            "BAZ" "quux"}
                      :cmd "test-command"
                      :cpus 1
                      :disk nil
                      :mem 1536
                      :healthChecks [{:protocol "HTTP"
                                      :path "/status"
                                      :gracePeriodSeconds 111
                                      :intervalSeconds 10
                                      :portIndex 0
                                      :timeoutSeconds 20
                                      :maxConsecutiveFailures 5}]
                      :backoffFactor 2
                      :ports [0 0]
                      :user "test-user"}
            home-path-prefix "/home/path/"
            service-id "test-service-1"
            service-description {"backend-proto" "http"
                                 "cmd" "test-command"
                                 "cpus" 1
                                 "mem" 1536
                                 "run-as-user" "test-user"
                                 "ports" 2
                                 "restart-backoff-factor" 2
                                 "grace-period-secs" 111
                                 "env" {"FOO" "bar"
                                        "BAZ" "quux"}}
            actual (marathon-descriptor home-path-prefix service-id->password-fn
                                        {:service-id service-id, :service-description service-description})]
        (is (= expected actual))))))

(deftest test-kill-instance-last-force-kill-time-store
  (let [current-time (t/now)
        service-id "service-1"
        instance-id "service-1.A"
        make-marathon-scheduler #(->MarathonScheduler {} 5051 (fn [] nil) "/slave/directory" "/home/path/"
                                                      (atom {}) %1 %2 (constantly true))
        successful-kill-result {:instance-id instance-id :killed? true :service-id service-id}
        failed-kill-result {:instance-id instance-id :killed? false :service-id service-id}]
    (with-redefs [t/now (fn [] current-time)]

      (testing "normal-kill"
        (let [service-id->kill-info-store (atom {})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        successful-kill-result)]
            (is (= successful-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {} @service-id->kill-info-store)))))

      (testing "failed-kill"
        (let [service-id->kill-info-store (atom {})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        failed-kill-result)]
            (is (= failed-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {service-id {:kill-failing-since current-time}} @service-id->kill-info-store)))))

      (testing "not-yet-forced"
        (let [service-id->kill-info-store (atom {service-id {:kill-failing-since (t/minus current-time (t/millis 500))}})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        failed-kill-result)]
            (is (= failed-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {service-id {:kill-failing-since (t/minus current-time (t/millis 500))}} @service-id->kill-info-store)))))

      (testing "forced-kill"
        (let [service-id->kill-info-store (atom {service-id {:kill-failing-since (t/minus current-time (t/millis 1500))}})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force true, :scale true} params))
                                                        successful-kill-result)]
            (is (= successful-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {} @service-id->kill-info-store))))))))

(deftest test-service-id->state
  (let [service-id "service-id"
        marathon-scheduler (->MarathonScheduler {} 5051 (fn [] nil) "/slave/directory" "/home/path/"
                                                (atom {service-id [:failed-instances]})
                                                (atom {service-id :kill-call-info})
                                                100 (constantly true))
        state (scheduler/service-id->state marathon-scheduler service-id)]
    (is (= {:failed-instances [:failed-instances], :killed-instances [], :kill-info :kill-call-info} state))))

(deftest test-killed-instances-transient-store
  (let [current-time (t/now)
        current-time-str (utils/date-to-str current-time)
        marathon-scheduler (->MarathonScheduler {} 5051 (fn [] nil) "/slave/directory" "/home/path/"
                                                (atom {}) (atom {}) 60000 (constantly true))
        make-instance (fn [service-id instance-id]
                        {:id instance-id
                         :service-id service-id})]
    (with-redefs [apps/kill-task (fn [service-id instance-id scale-key scale-value force-key force-value]
                                   (is (= [scale-key scale-value force-key force-value]
                                          ["scale" "true" "force" "false"]))
                                   {:service-id service-id, :instance-id instance-id, :killed? true, :deploymentId "12982340972"})
                  t/now (fn [] current-time)]
      (testing "tracking-instance-killed"

        (scheduler/preserve-only-killed-instances-for-services! [])

        (is (:killed? (scheduler/kill-instance marathon-scheduler (make-instance "service-1" "service-1.A"))))
        (is (:killed? (scheduler/kill-instance marathon-scheduler (make-instance "service-2" "service-2.A"))))
        (is (:killed? (scheduler/kill-instance marathon-scheduler (make-instance "service-1" "service-1.C"))))
        (is (:killed? (scheduler/kill-instance marathon-scheduler (make-instance "service-1" "service-1.B"))))

        (is (= [{:id "service-1.A", :service-id "service-1", :killed-at current-time-str}
                {:id "service-1.B", :service-id "service-1", :killed-at current-time-str}
                {:id "service-1.C", :service-id "service-1", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-1")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (is (:killed? (scheduler/kill-instance marathon-scheduler (make-instance "service-3" "service-3.A"))))
        (is (:killed? (scheduler/kill-instance marathon-scheduler (make-instance "service-3" "service-3.B"))))
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [{:id "service-3.A", :service-id "service-3", :killed-at current-time-str}
                {:id "service-3.B", :service-id "service-3", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-2")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [{:id "service-3.A", :service-id "service-3", :killed-at current-time-str}
                {:id "service-3.B", :service-id "service-3", :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/preserve-only-killed-instances-for-services! [])
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))))))

(deftest test-max-failed-instances-cache
  (let [current-time (t/now)
        current-time-str (utils/date-to-str current-time)
        service-id->failed-instances-transient-store (atom {})
        common-extractor-fn (constantly {:service-id "service-1"})]
    (testing "test-max-failed-instances-cache"
      (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
      (doseq [n (range 10 50)]
        (parse-and-store-failed-instance!
          service-id->failed-instances-transient-store
          "service-1"
          {:taskId (str "service-1." n)
           :timestamp current-time-str}
          common-extractor-fn))
      (let [actual-failed-instances (set (service-id->failed-instances service-id->failed-instances-transient-store "service-1"))]
        (is (= 10 (count actual-failed-instances)))
        (doseq [n (range 40 50)]
          (is (contains? actual-failed-instances
                         (scheduler/make-ServiceInstance
                           {:id (str "service-1." n)
                            :service-id "service-1"
                            :started-at current-time-str
                            :healthy? false
                            :port 0}))
              (str "Failed instances does not contain instance service-1." n)))))))

(deftest test-marathon-scheduler
  (testing "Creating a MarathonScheduler"

    (testing "should throw on invalid configuration"
      (is (thrown? Throwable (marathon-scheduler {:framework-id-ttl 900000
                                                  :home-path-prefix "/home/"
                                                  :http-options {:conn-timeout 10000
                                                                 :socket-timeout 10000}
                                                  :mesos-slave-port 5051
                                                  :slave-directory "/foo"
                                                  :url nil})))
      (is (thrown? Throwable (marathon-scheduler {:framework-id-ttl 900000
                                                  :home-path-prefix "/home/"
                                                  :http-options {:conn-timeout 10000
                                                                 :socket-timeout 10000}
                                                  :mesos-slave-port 5051
                                                  :slave-directory ""
                                                  :url "url"})))
      (is (thrown? Throwable (marathon-scheduler {:framework-id-ttl 900000
                                                  :home-path-prefix "/home/"
                                                  :http-options {:conn-timeout 10000
                                                                 :socket-timeout 10000}
                                                  :mesos-slave-port 0
                                                  :slave-directory "/foo"
                                                  :url "url"})))
      (is (thrown? Throwable (marathon-scheduler {:framework-id-ttl 900000
                                                  :home-path-prefix "/home/"
                                                  :http-options {}
                                                  :mesos-slave-port 5051
                                                  :slave-directory "/foo"
                                                  :url "url"})))
      (is (thrown? Throwable (marathon-scheduler {:framework-id-ttl 900000
                                                  :home-path-prefix nil
                                                  :http-options {:conn-timeout 10000
                                                                 :socket-timeout 10000}
                                                  :mesos-slave-port 5051
                                                  :slave-directory "/foo"
                                                  :url "url"})))
      (is (thrown? Throwable (marathon-scheduler {:framework-id-ttl 0
                                                  :home-path-prefix "/home/"
                                                  :http-options {:conn-timeout 10000
                                                                 :socket-timeout 10000}
                                                  :mesos-slave-port 5051
                                                  :slave-directory "/foo"
                                                  :url "url"}))))

    (testing "should work with valid configuration"
      (is (instance? MarathonScheduler
                     (marathon-scheduler {:home-path-prefix "/home/"
                                          :http-options {:conn-timeout 10000
                                                         :socket-timeout 10000}
                                          :force-kill-after-ms 60000
                                          :framework-id-ttl 900000
                                          :url "url"}))))))

(deftest test-process-kill-instance-request
  (let [service-id "test-service-id"
        instance-id "instance-id"]
    (testing "successful-delete"
      (with-redefs [apps/kill-task (fn [in-service-id in-instance-id scale-key scale-value force-key force-value]
                                     (is (= service-id in-service-id))
                                     (is (= instance-id in-instance-id))
                                     (is (= [scale-key scale-value force-key force-value] ["scale" "true" "force" "false"]))
                                     {:deploymentId "12982340972"})]
        (is (= {:instance-id instance-id :killed? true :message "Successfully killed instance" :service-id service-id, :status 200}
               (process-kill-instance-request service-id instance-id {})))))

    (testing "unsuccessful-delete"
      (with-redefs [apps/kill-task (fn [in-service-id in-instance-id scale-key scale-value force-key force-value]
                                     (is (= service-id in-service-id))
                                     (is (= instance-id in-instance-id))
                                     (is (= [scale-key scale-value force-key force-value] ["scale" "true" "force" "false"]))
                                     {:failed true})]
        (is (= {:instance-id instance-id :killed? false :message "Unable to kill instance" :service-id service-id, :status 500}
               (process-kill-instance-request service-id instance-id {})))))

    (testing "deployment-conflict"
      (with-redefs [apps/kill-task (fn [in-service-id in-instance-id scale-key scale-value force-key force-value]
                                     (is (= service-id in-service-id))
                                     (is (= instance-id in-instance-id))
                                     (is (= [scale-key scale-value force-key force-value] ["scale" "true" "force" "false"]))
                                     (ss/throw+ {:status 409}))]
        (is (= {:instance-id instance-id :killed? false :message "Locked by one or more deployments" :service-id service-id, :status 409}
               (process-kill-instance-request service-id instance-id {})))))

    (testing "marathon-404"
      (with-redefs [apps/kill-task (fn [in-service-id in-instance-id scale-key scale-value force-key force-value]
                                     (is (= service-id in-service-id))
                                     (is (= instance-id in-instance-id))
                                     (is (= [scale-key scale-value force-key force-value] ["scale" "true" "force" "false"]))
                                     (ss/throw+ {:body "Not Found", :status 404}))]
        (is (= {:instance-id instance-id :killed? false :message "Not Found" :service-id service-id, :status 404}
               (process-kill-instance-request service-id instance-id {})))))

    (testing "exception-while-killing"
      (with-redefs [apps/kill-task (fn [in-service-id in-instance-id scale-key scale-value force-key force-value]
                                     (is (= service-id in-service-id))
                                     (is (= instance-id in-instance-id))
                                     (is (= [scale-key scale-value force-key force-value] ["scale" "true" "force" "false"]))
                                     (throw (Exception. "exception from test")))]
        (is (= {:instance-id instance-id :killed? false :message "exception from test" :service-id service-id, :status 500}
               (process-kill-instance-request service-id instance-id {})))))))

(deftest test-delete-app
  (let [scheduler (->MarathonScheduler {} nil nil nil nil (atom {}) (atom {}) nil nil)]

    (with-redefs [apps/delete-app (constantly {:deploymentId 12345})]
      (is (= {:result :deleted
              :message "Marathon deleted with deploymentId 12345"}
             (scheduler/delete-app scheduler "foo"))))

    (with-redefs [apps/delete-app (constantly {})]
      (is (= {:result :error
              :message "Marathon did not provide deploymentId for delete request"}
             (scheduler/delete-app scheduler "foo"))))

    (with-redefs [apps/delete-app (fn [_] (ss/throw+ {:status 404}))]
      (is (= {:result :no-such-service-exists
              :message "Marathon reports service does not exist"}
             (scheduler/delete-app scheduler "foo"))))))
