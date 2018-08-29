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
(ns waiter.scheduler.marathon-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [slingshot.slingshot :as ss]
            [waiter.scheduler.marathon :refer :all]
            [waiter.mesos.marathon :as marathon]
            [waiter.mesos.mesos :as mesos]
            [waiter.scheduler :as scheduler]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import waiter.scheduler.marathon.MarathonScheduler))

(deftest test-response-data->service-instances
  (let [test-cases (list {
                          :name "response-data->service-instances no response"
                          :marathon-response nil
                          :expected-response {:active-instances []
                                              :failed-instances []}
                          :service-id->service-description {}},
                         {
                          :name "response-data->service-instances empty response"
                          :marathon-response {}
                          :expected-response {:active-instances []
                                              :failed-instances []}
                          :service-id->service-description {}},
                         {
                          :name "response-data->service-instances empty-app response"
                          :marathon-response {:app {}}
                          :expected-response {:active-instances []
                                              :failed-instances []}
                          :service-id->service-description {}},
                         {
                          :name "response-data->service-instances valid response with task failure"
                          :marathon-response
                          {
                           :app {
                                 :id "test-app-1234",
                                 :instances 3,
                                 :healthChecks [{:path "/health", :portIndex 0, :protocol "N/A", :timeoutSeconds 10}],
                                 :lastTaskFailure {
                                                   :appId "test-app-1234",
                                                   :host "10.141.141.10",
                                                   :message "Abnormal executor termination",
                                                   :state "TASK_FAILED",
                                                   :taskId "test-app-1234.D",
                                                   :timestamp "2014-09-12T23:23:41.711Z",
                                                   :version "2014-09-12T23:28:21.737Z"},
                                 :tasks [
                                         {
                                          :appId "test-app-1234",
                                          :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                :firstSuccess "2014-09-13T00:20:28.101Z",
                                                                :lastFailure nil, :lastSuccess "2014-09-13T002507.506Z",
                                                                :taskId "test-app-1234.A"}],
                                          :host "10.141.141.11",
                                          :id "/test-app-1234.A",
                                          :ports [31045],
                                          :stagedAt "2014-09-12T23:28:28.594Z",
                                          :startedAt "2014-09-13T00:24:46.959Z",
                                          :version "2014-09-12T23:28:21.737Z"},
                                         {
                                          :appId "/test-app-1234",
                                          :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                :firstSuccess "2014-09-13T00:20:28.101Z",
                                                                :lastFailure nil, :lastSuccess "2014-09-13T002507.508Z",
                                                                :taskId "test-app-1234.B"}],
                                          :host "10.141.141.12",
                                          :id "/test-app-1234.B",
                                          :ports [31234],
                                          :stagedAt "2014-09-12T23:28:22.587Z",
                                          :startedAt "2014-09-13T00:24:56.965Z",
                                          :version "2014-09-12T23:28:21.737Z"},
                                         {
                                          :appId "/test-app-1234",
                                          :healthCheckResults [{:alive false, :consecutiveFailures 10,
                                                                :firstSuccess "2014-09-13T00:20:28.101Z",
                                                                :lastFailure "2014-09-13T002507.508Z", :lastSuccess nil,
                                                                :taskId "test-app-1234.C"}],
                                          :host "10.141.141.13",
                                          :id "test-app-1234.C",
                                          :ports [41234 12321 90384 56463],
                                          :stagedAt "2014-09-12T23:28:22.587Z",
                                          :startedAt "2014-09-14T00:24:46.965Z",
                                          :version "2014-09-12T23:28:21.737Z"}],}}

                          :expected-response
                          {:active-instances (list
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
                                                  :started-at (du/str-to-date "2014-09-13T00:24:46.959Z" formatter-marathon)}),
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
                                                  :started-at (du/str-to-date "2014-09-13T00:24:56.965Z" formatter-marathon)}),
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
                                                  :started-at (du/str-to-date "2014-09-14T00:24:46.965Z" formatter-marathon)}))
                           :failed-instances (list
                                               (scheduler/make-ServiceInstance
                                                 {:extra-ports [],
                                                  :healthy? false,
                                                  :host "10.141.141.10",
                                                  :id "test-app-1234.D",
                                                  :log-directory nil,
                                                  :message "Abnormal executor termination",
                                                  :port 0,
                                                  :protocol "https",
                                                  :service-id "test-app-1234",
                                                  :started-at (du/str-to-date "2014-09-12T23:23:41.711Z" formatter-marathon)}))}
                          :service-id->service-description {"test-app-1234" {"backend-proto" "https"}}},
                         {
                          :name "response-data->service-instances valid response without task failure"
                          :marathon-response
                          {
                           :framework-id "F123445"
                           :app {
                                 :id "test-app-1234",
                                 :instances 3,
                                 :healthChecks [{:path "/health", :portIndex 0, :protocol "HTTP", :timeoutSeconds 10}],
                                 :tasks [
                                         {
                                          :appId "/test-app-1234",
                                          :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                :firstSuccess "2014-09-13T00:20:28.101Z",
                                                                :lastFailure nil, :lastSuccess "2014-09-13T002507.506Z",
                                                                :taskId "test-app-1234.A"}],
                                          :host "10.141.141.11",
                                          :id "test-app-1234.A",
                                          :ports [31045],
                                          :stagedAt "2014-09-12T23:28:28.594Z",
                                          :startedAt "2014-09-13T00:24:46.959Z",
                                          :version "2014-09-12T23:28:21.737Z",
                                          :slaveId "S234842"},
                                         {
                                          :appId "test-app-1234",
                                          :healthCheckResults [{:alive true, :consecutiveFailures 0,
                                                                :firstSuccess "2014-09-13T00:20:28.101Z",
                                                                :lastFailure nil, :lastSuccess "2014-09-13T002507.508Z",
                                                                :taskId "test-app-1234.B"}],
                                          :host "10.141.141.12",
                                          :id "test-app-1234.B",
                                          :ports [31234],
                                          :stagedAt "2014-09-12T23:28:22.587Z",
                                          :startedAt "2014-09-13T00:24:46.965Z",
                                          :version "2014-09-12T23:28:21.737Z"},
                                         {
                                          :appId "/test-app-1234",
                                          :healthCheckResults [{:alive false, :consecutiveFailures 10,
                                                                :firstSuccess "2014-09-13T00:20:28.101Z",
                                                                :lastFailure "2014-09-13T002507.508Z", :lastSuccess nil,
                                                                :taskId "/test-app-1234.C"}],
                                          :host "10.141.141.13",
                                          :id "test-app-1234.C",
                                          :ports [41234],
                                          :stagedAt "2014-09-12T23:28:22.587Z",
                                          :startedAt "2014-09-13T00:24:46.965Z",
                                          :version "2014-09-12T23:28:21.737Z",
                                          :slaveId "S651616"}],}}

                          :expected-response
                          {:active-instances (list
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
                                                  :started-at (du/str-to-date "2014-09-13T00:24:46.959Z" formatter-marathon)}),
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
                                                  :started-at (du/str-to-date "2014-09-13T00:24:46.965Z" formatter-marathon)}),
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
                                                  :started-at (du/str-to-date "2014-09-13T00:24:46.965Z" formatter-marathon)}))
                           :failed-instances []}
                          :service-id->service-description {"test-app-1234" {"backend-proto" "http"}}})]
    (doseq [{:keys [expected-response marathon-response name service-id->service-description]} test-cases]
      (testing (str "Test " name)
        (let [framework-id (:framework-id marathon-response)
              service-id->failed-instances-transient-store (atom {})
              actual-response (response-data->service-instances
                                marathon-response
                                [:app]
                                (fn [] framework-id)
                                {:slave-directory "/slave-dir"}
                                service-id->failed-instances-transient-store
                                service-id->service-description)]
          (is (= expected-response actual-response) (str name))
          (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store []))))))

(deftest test-response-data->service->service-instances
  (let [input [{:id "test-app-1234"
                :instances 2
                :healthChecks [{:path "/ping", :portIndex 0, :protocol "HTTPS", :timeoutSeconds 10}]
                :tasks [{:appId "/test-app-1234"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.506Z"
                                               :taskId "test-app-1234.A"}]
                         :host "10.141.141.11"
                         :id "test-app-1234.A"
                         :ports [31045]
                         :stagedAt "2014-09-12T23:28:28.594Z"
                         :startedAt "2014-09-13T00:24:46.959Z"
                         :version "2014-09-12T23:28:21.737Z"}
                        {:appId "test-app-1234"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.508Z"
                                               :taskId "test-app-1234.B"}]
                         :host "10.141.141.12"
                         :id "test-app-1234.B"
                         :ports [31234]
                         :stagedAt "2014-09-12T23:28:22.587Z"
                         :startedAt "2014-09-13T00:24:46.965Z"
                         :version "2014-09-12T23:28:21.737Z"}]
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
                                  :timestamp "2014-09-12T23:23:41.711Z"
                                  :version "2014-09-12T23:28:21.737Z"}
                :healthChecks [{:path "/health", :portIndex 0, :protocol "HTTP", :timeoutSeconds 10}]
                :tasks [{:appId "test-app-6789"
                         :healthCheckResults [{:alive true
                                               :consecutiveFailures 0
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure nil
                                               :lastSuccess "2014-09-13T002507.506Z"
                                               :taskId "test-app-6789.A"}]
                         :host "10.141.141.11"
                         :id "/test-app-6789.A"
                         :ports [31045]
                         :stagedAt "2014-09-12T23:28:28.594Z"
                         :startedAt "2014-09-13T00:24:46.959Z"
                         :version "2014-09-12T23:28:21.737Z"}
                        {:appId "/test-app-6789"
                         :host "10.141.141.12"
                         :id "/test-app-6789.B"
                         :ports [36789]
                         :stagedAt "2014-09-12T23:28:22.587Z"
                         :startedAt "2014-09-13T00:24:56.965Z"
                         :version "2014-09-12T23:28:21.737Z"}
                        {:appId "/test-app-6789"
                         :healthCheckResults [{:alive false
                                               :consecutiveFailures 10
                                               :firstSuccess "2014-09-13T00:20:28.101Z"
                                               :lastFailure "2014-09-13T002507.508Z"
                                               :lastSuccess nil
                                               :taskId "test-app-6789.C"}]
                         :host "10.141.141.13"
                         :id "test-app-6789.C"
                         :ports [46789]
                         :stagedAt "2014-09-12T23:28:22.587Z"
                         :startedAt "2014-09-14T00:24:46.965Z"
                         :version "2014-09-12T23:28:21.737Z"}]}]
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
                         :started-at (du/str-to-date "2014-09-13T00:24:46.959Z" formatter-marathon)})
                      (scheduler/make-ServiceInstance
                        {:id "test-app-1234.B"
                         :service-id "test-app-1234"
                         :healthy? true
                         :host "10.141.141.12"
                         :port 31234
                         :protocol "https"
                         :started-at (du/str-to-date "2014-09-13T00:24:46.965Z" formatter-marathon)}))
                    :failed-instances []}
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
                         :started-at (du/str-to-date "2014-09-13T00:24:46.959Z" formatter-marathon)})
                      (scheduler/make-ServiceInstance
                        {:id "test-app-6789.B"
                         :service-id "test-app-6789"
                         :healthy? nil
                         :host "10.141.141.12"
                         :port 36789
                         :protocol "http"
                         :started-at (du/str-to-date "2014-09-13T00:24:56.965Z" formatter-marathon)})
                      (scheduler/make-ServiceInstance
                        {:id "test-app-6789.C"
                         :service-id "test-app-6789"
                         :healthy? false
                         :host "10.141.141.13"
                         :port 46789
                         :protocol "http"
                         :started-at (du/str-to-date "2014-09-14T00:24:46.965Z" formatter-marathon)}))
                    :failed-instances
                    (list
                      (scheduler/make-ServiceInstance
                        {:id "test-app-6789.D"
                         :service-id "test-app-6789"
                         :healthy? false
                         :host "10.141.141.10"
                         :port 0
                         :protocol "http"
                         :started-at (du/str-to-date "2014-09-12T23:23:41.711Z" formatter-marathon)
                         :message "Abnormal executor termination"}))})
        service-id->failed-instances-transient-store (atom {})
        service-id->service-description {"test-app-1234" {"backend-proto" "https"}
                                         "test-app-6789" {"backend-proto" "http"}}
        actual (response-data->service->service-instances
                 input (fn [] nil) nil service-id->failed-instances-transient-store service-id->service-description)]
    (is (= expected actual))
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])))

(deftest test-service-id->failed-instances-transient-store
  (let [faled-instance-response-fn (fn [service-id instance-id]
                                     {:appId service-id,
                                      :host (str "10.141.141." instance-id),
                                      :message "Abnormal executor termination",
                                      :state (str instance-id "failed"),
                                      :taskId (str service-id "." instance-id),
                                      :timestamp "2014-09-12T23:23:41.711Z",
                                      :version "2014-09-12T23:28:21.737Z"})
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
        mesos-api (Object.)]
    (with-redefs [mesos/get-agent-state
                  (fn [in-mesos-api in-host]
                    (is (= mesos-api in-mesos-api))
                    (is (= host in-host))
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
                      (-> response-body json/read-str walk/keywordize-keys)))]
      (is (= "/path/to/instance2/directory" (mesos/retrieve-log-url mesos-api instance-id host "marathon"))))))

(deftest test-retrieve-directory-content-from-host
  (let [host "www.example.com"
        mesos-slave-port 5051
        directory "/path/to/instance2/directory"
        mesos-api (mesos/api-factory (Object.) {} mesos-slave-port directory)]
    (with-redefs [mesos/list-directory-content
                  (fn [in-mesos-api in-host in-directory]
                    (is (= mesos-api in-mesos-api))
                    (is (= host in-host))
                    (is (= directory in-directory))
                    (let [response-body "
                                   [{\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil1\", \"size\": 1000},
                                    {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir2\", \"size\": 2000},
                                    {\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil3\", \"size\": 3000},
                                    {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir4\", \"size\": 4000}]"]
                      (-> response-body json/read-str walk/keywordize-keys)))]
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
        (is (= expected-result (mesos/retrieve-directory-content-from-host mesos-api host directory)))))))

(deftest test-marathon-descriptor
  (let [service-id->password-fn (fn [service-id] (str service-id "-password"))]
    (testing "basic-test-with-defaults"
      (let [expected {:id "test-service-1"
                      :labels {:source "waiter"
                               :user "test-user"}
                      :env {"BAZ" "quux"
                            "FOO" "bar"
                            "HOME" "/home/path/test-user"
                            "LOGNAME" "test-user"
                            "USER" "test-user"
                            "WAITER_CPUS" "1"
                            "WAITER_MEM_MB" "1536"
                            "WAITER_PASSWORD" "test-service-1-password"
                            "WAITER_SERVICE_ID" "test-service-1"
                            "WAITER_USERNAME" "waiter"}
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
                                 "health-check-interval-secs" 10
                                 "health-check-max-consecutive-failures" 5
                                 "env" {"FOO" "bar"
                                        "BAZ" "quux"}}
            actual (marathon-descriptor home-path-prefix service-id->password-fn
                                        {:service-id service-id, :service-description service-description})]
        (is (= expected actual))))))

(defn- create-marathon-scheduler
  [& {:as marathon-config}]
  (-> {:force-kill-after-ms 1000
       :home-path-prefix "/home/path/"
       :is-waiter-app?-fn (constantly true)
       :marathon-api {}
       :mesos-api {}
       :retrieve-framework-id-fn (constantly nil)
       :retrieve-syncer-state-fn (constantly {})
       :service-id->failed-instances-transient-store (atom {})
       :service-id->kill-info-store (atom {})
       :service-id->out-of-sync-state-store (atom {})
       :service-id->password-fn #(str % ".password")
       :service-id->service-description (constantly nil)
       :sync-deployment-maintainer-atom (atom nil)}
      (merge marathon-config)
      map->MarathonScheduler))

(deftest test-kill-instance-last-force-kill-time-store
  (let [current-time (t/now)
        service-id "service-1"
        instance-id "service-1.A"
        make-marathon-scheduler #(create-marathon-scheduler :service-id->kill-info-store %1 :force-kill-after-ms %2)
        successful-kill-result {:instance-id instance-id :killed? true :service-id service-id}
        failed-kill-result {:instance-id instance-id :killed? false :service-id service-id}]
    (with-redefs [t/now (fn [] current-time)]

      (testing "normal-kill"
        (let [service-id->kill-info-store (atom {})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        successful-kill-result)]
            (is (= successful-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {} @service-id->kill-info-store)))))

      (testing "failed-kill"
        (let [service-id->kill-info-store (atom {})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        failed-kill-result)]
            (is (= failed-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {service-id {:kill-failing-since current-time}} @service-id->kill-info-store)))))

      (testing "not-yet-forced"
        (let [service-id->kill-info-store (atom {service-id {:kill-failing-since (t/minus current-time (t/millis 500))}})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force false, :scale true} params))
                                                        failed-kill-result)]
            (is (= failed-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {service-id {:kill-failing-since (t/minus current-time (t/millis 500))}} @service-id->kill-info-store)))))

      (testing "forced-kill"
        (let [service-id->kill-info-store (atom {service-id {:kill-failing-since (t/minus current-time (t/millis 1500))}})
              marathon-scheduler (make-marathon-scheduler service-id->kill-info-store 1000)]
          (with-redefs [process-kill-instance-request (fn [_ in-service-id in-instance-id params]
                                                        (is (= service-id in-service-id))
                                                        (is (= instance-id in-instance-id))
                                                        (is (= {:force true, :scale true} params))
                                                        successful-kill-result)]
            (is (= successful-kill-result (scheduler/kill-instance marathon-scheduler {:id instance-id, :service-id service-id})))
            (is (= {} @service-id->kill-info-store))))))))

(deftest test-service-id->state
  (let [service-id "service-id"
        marathon-scheduler (create-marathon-scheduler
                             :service-id->failed-instances-transient-store (atom {service-id [:failed-instances]})
                             :service-id->kill-info-store (atom {service-id :kill-call-info}))
        state (scheduler/service-id->state marathon-scheduler service-id)]
    (is (= {:failed-instances [:failed-instances]
            :kill-info :kill-call-info
            :out-of-sync-state nil
            :syncer {}}
           state))))

(deftest test-max-failed-instances-cache
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time formatter-marathon)
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
                            :started-at (du/str-to-date current-time-str formatter-marathon)
                            :healthy? false
                            :port 0}))
              (str "Failed instances does not contain instance service-1." n)))))))

(deftest test-marathon-scheduler
  (testing "Creating a MarathonScheduler"
    (let [context {:is-waiter-app?-fn (constantly nil)
                   :leader?-fn (constantly nil)
                   :scheduler-name "marathon"
                   :scheduler-state-chan (async/chan 4)
                   :scheduler-syncer-interval-secs 5
                   :service-id->password-fn (constantly nil)
                   :service-id->service-description-fn (constantly nil)
                   :start-scheduler-syncer-fn (constantly nil)}
          scheduler-config {:force-kill-after-ms 60000
                            :framework-id-ttl 900000
                            :home-path-prefix "/home/"
                            :http-options {:conn-timeout 10000 :socket-timeout 10000}
                            :mesos-slave-port 5051
                            :slave-directory "/foo"
                            :sync-deployment {:interval-ms 15000
                                              :timeout-cycles 4}
                            :url "url"}
          valid-config (merge context scheduler-config)
          create-marathon-scheduler (fn create-marathon-scheduler [config]
                                      (let [result (marathon-scheduler config)
                                            {:keys [sync-deployment-maintainer-atom]} result]
                                        ((deref sync-deployment-maintainer-atom))
                                        result))]

      (testing "should throw on invalid configuration"
        (is (thrown? Throwable (create-marathon-scheduler (assoc valid-config :framework-id-ttl 0))))
        (is (thrown? Throwable (create-marathon-scheduler (assoc valid-config :home-path-prefix nil))))
        (is (thrown? Throwable (create-marathon-scheduler (assoc valid-config :http-options {}))))
        (is (thrown? Throwable (create-marathon-scheduler (assoc valid-config :mesos-slave-port 0))))
        (is (thrown? Throwable (create-marathon-scheduler (assoc valid-config :slave-directory ""))))
        (is (thrown? Throwable (create-marathon-scheduler (assoc valid-config :sync-deployment {:interval-ms 0}))))
        (is (thrown? Throwable (create-marathon-scheduler (assoc valid-config :url nil)))))

      (testing "should work with valid configuration"
        (is (instance? MarathonScheduler (create-marathon-scheduler valid-config)))
        (is (instance? MarathonScheduler (create-marathon-scheduler (dissoc valid-config :force-kill-after-ms))))))))

(deftest test-process-kill-instance-request
  (let [marathon-api (Object.)
        service-id "test-service-id"
        instance-id "instance-id"]
    (testing "successful-delete"
      (with-redefs [marathon/kill-task (fn [in-marathon-api in-service-id in-instance-id scale-value force-value]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         (is (= instance-id in-instance-id))
                                         (is (= [scale-value force-value] [true false]))
                                         {:deploymentId "12982340972"})]
        (is (= {:instance-id instance-id :killed? true :message "Successfully killed instance" :service-id service-id, :status 200}
               (process-kill-instance-request marathon-api service-id instance-id {})))))

    (testing "unsuccessful-delete"
      (with-redefs [marathon/kill-task (fn [in-marathon-api in-service-id in-instance-id scale-value force-value]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         (is (= instance-id in-instance-id))
                                         (is (= [scale-value force-value] [true false]))
                                         {:failed true})]
        (is (= {:instance-id instance-id :killed? false :message "Unable to kill instance" :service-id service-id, :status 500}
               (process-kill-instance-request marathon-api service-id instance-id {})))))

    (testing "deployment-conflict"
      (with-redefs [marathon/kill-task (fn [in-marathon-api in-service-id in-instance-id scale-value force-value]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         (is (= instance-id in-instance-id))
                                         (is (= [scale-value force-value] [true false]))
                                         (ss/throw+ {:status 409}))]
        (is (= {:instance-id instance-id :killed? false :message "Locked by one or more deployments" :service-id service-id, :status 409}
               (process-kill-instance-request marathon-api service-id instance-id {})))))

    (testing "marathon-404"
      (with-redefs [marathon/kill-task (fn [in-marathon-api in-service-id in-instance-id scale-value force-value]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         (is (= instance-id in-instance-id))
                                         (is (= [scale-value force-value] [true false]))
                                         (ss/throw+ {:body "Not Found", :status 404}))]
        (is (= {:instance-id instance-id :killed? false :message "Not Found" :service-id service-id, :status 404}
               (process-kill-instance-request marathon-api service-id instance-id {})))))

    (testing "exception-while-killing"
      (with-redefs [marathon/kill-task (fn [in-marathon-api in-service-id in-instance-id scale-value force-value]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         (is (= instance-id in-instance-id))
                                         (is (= [scale-value force-value] [true false]))
                                         (throw (Exception. "exception from test")))]
        (is (= {:instance-id instance-id :killed? false :message "exception from test" :service-id service-id, :status 500}
               (process-kill-instance-request marathon-api service-id instance-id {})))))))

(deftest test-delete-service
  (let [scheduler (create-marathon-scheduler)]

    (with-redefs [marathon/delete-app (constantly {:deploymentId 12345})]
      (is (= {:result :deleted
              :message "Marathon deleted with deploymentId 12345"}
             (scheduler/delete-service scheduler "foo"))))

    (with-redefs [marathon/delete-app (constantly {})]
      (is (= {:result :error
              :message "Marathon did not provide deploymentId for delete request"}
             (scheduler/delete-service scheduler "foo"))))

    (with-redefs [marathon/delete-app (fn [_ _] (ss/throw+ {:status 404}))]
      (is (= {:result :no-such-service-exists
              :message "Marathon reports service does not exist"}
             (scheduler/delete-service scheduler "foo"))))))

(deftest test-extract-deployment-info
  (let [marathon-api (Object.)
        prepare-response (fn [body]
                           (let [body-chan (async/promise-chan)]
                             (async/>!! body-chan body)
                             {:body body-chan}))]
    (with-redefs [marathon/get-deployments (constantly [{:affectedApps ["/waiter-app-1234"] :id "1234" :version "v1234"}
                                                        {:affectedApps ["/waiter-app-4567"] :id "4567" :version "v4567"}
                                                        {:affectedApps ["/waiter-app-3829"] :id "3829" :version "v3829"}
                                                        {:affectedApps ["/waiter-app-4321"] :id "4321" :version "v4321"}])]
      (testing "no deployments entry"
        (let [response (prepare-response "{\"message\": \"App is locked by one or more deployments.\"}")]
          (is (not (extract-deployment-info marathon-api response)))))

      (testing "no deployments listed"
        (let [response (prepare-response "{\"deployments\": [],
                                           \"message\": \"App is locked by one or more deployments.\"}")]
          (is (not (extract-deployment-info marathon-api response)))))

      (testing "single deployment"
        (let [response (prepare-response "{\"deployments\": [{\"id\":\"1234\"}],
                                           \"message\": \"App is locked by one or more deployments.\"}")]
          (is (= [{:affectedApps ["/waiter-app-1234"] :id "1234" :version "v1234"}]
                 (extract-deployment-info marathon-api response)))))

      (testing "multiple deployments"
        (let [response {:body "{\"deployments\": [{\"id\":\"1234\"}, {\"id\":\"3829\"}],
                                \"message\": \"App is locked by one or more deployments.\"}"}]
          (is (= [{:affectedApps ["/waiter-app-1234"] :id "1234" :version "v1234"}
                  {:affectedApps ["/waiter-app-3829"] :id "3829" :version "v3829"}]
                 (extract-deployment-info marathon-api response)))))

      (testing "multiple deployments, one without info"
        (let [response (prepare-response "{\"deployments\": [{\"id\":\"1234\"}, {\"id\":\"3829\"}, {\"id\":\"9876\"}],
                                           \"message\": \"App is locked by one or more deployments.\"}")]
          (is (= [{:affectedApps ["/waiter-app-1234"] :id "1234" :version "v1234"}
                  {:affectedApps ["/waiter-app-3829"] :id "3829" :version "v3829"}]
                 (extract-deployment-info marathon-api response))))))))

(deftest test-extract-service-deployment-info
  (let [marathon-api (Object.)]
    (with-redefs [marathon/get-deployments (constantly [{:affectedApps ["/waiter-app-1234"] :id "1234a" :version "v1234a"}
                                                        {:affectedApps ["/waiter-app-4567"] :id "4567" :version "v4567"}
                                                        {:affectedApps ["/waiter-app-3829"] :id "3829" :version "v3829"}
                                                        {:affectedApps ["/waiter-app-4321"] :id "4321" :version "v4321"}
                                                        {:affectedApps ["/waiter-app-1234"] :id "1234b" :version "v1234b"}])]
      (testing "no deployments entry"
        (is (nil? (extract-service-deployment-info marathon-api "missing-service-id"))))

      (testing "matching single deployment"
        (is (= {:affectedApps ["/waiter-app-4567"] :id "4567" :version "v4567"}
               (extract-service-deployment-info marathon-api "waiter-app-4567"))))

      (testing "matching multiple deployments"
        (is (= {:affectedApps ["/waiter-app-1234"] :id "1234a" :version "v1234a"}
               (extract-service-deployment-info marathon-api "waiter-app-1234")))))))

(deftest test-scale-service
  (let [marathon-api (Object.)
        marathon-scheduler (create-marathon-scheduler :force-kill-after-ms 60000 :marathon-api marathon-api)
        service-id "test-service-id"]

    (testing "unforced scale of service"
      (let [updated-invoked-promise (promise)
            instances 30]
        (with-redefs [extract-service-deployment-info (fn [in-marathon-api in-service-id]
                                                        (is (= marathon-api in-marathon-api))
                                                        (is (= service-id in-service-id))
                                                        (is false))
                      marathon/get-app (fn [in-marathon-api in-service-id]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         {:app {:cmd "tc" :cpus 2 :id service-id :instances 15 :mem 4
                                                :tasks (->> (fn [] {:appId service-id
                                                                    :id (str service-id "." (rand-int 1000))})
                                                            (repeatedly 15))}})
                      marathon/update-app (fn [in-marathon-api in-service-id descriptor]
                                            (is (= marathon-api in-marathon-api))
                                            (is (= service-id in-service-id))
                                            (is (= {:cmd "tc" :cpus 2 :id service-id :instances instances :mem 4} descriptor))
                                            (deliver updated-invoked-promise :invoked))]
          (scheduler/scale-service marathon-scheduler service-id instances false)
          (is (= :invoked (deref updated-invoked-promise 0 :not-invoked))))))

    (testing "forced scale of service - fewer instances"
      (let [updated-invoked-promise (promise)
            deleted-deployment-promise (promise)
            instances 10
            deployment-id "d-1234"]
        (with-redefs [extract-service-deployment-info (fn [in-marathon-api in-service-id]
                                                        (is (= marathon-api in-marathon-api))
                                                        (is (= service-id in-service-id))
                                                        {:id deployment-id})
                      marathon/delete-deployment (fn [in-marathon-api in-deployment-id]
                                                   (is (= marathon-api in-marathon-api))
                                                   (is (= deployment-id in-deployment-id))
                                                   (deliver deleted-deployment-promise :invoked))
                      marathon/get-app (fn [in-marathon-api in-service-id]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         {:app {:cmd "tc" :cpus 2 :id service-id :instances 25 :mem 4
                                                :tasks (->> (fn [] {:appId service-id
                                                                    :id (str service-id "." (rand-int 1000))})
                                                            (repeatedly 15))}})
                      marathon/update-app (fn [in-marathon-api in-service-id descriptor]
                                            (is (= marathon-api in-marathon-api))
                                            (is (= service-id in-service-id))
                                            (is (= {:cmd "tc" :cpus 2 :id service-id :instances 15 :mem 4} descriptor))
                                            (deliver updated-invoked-promise :invoked))]
          (scheduler/scale-service marathon-scheduler service-id instances true)
          (is (= :invoked (deref deleted-deployment-promise 0 :not-invoked)))
          (is (= :invoked (deref updated-invoked-promise 0 :not-invoked))))))

    (testing "forced scale of service - more instances"
      (let [updated-invoked-promise (promise)
            deleted-deployment-promise (promise)
            instances 20
            deployment-id "d-1234"]
        (with-redefs [extract-service-deployment-info (fn [in-marathon-api in-service-id]
                                                        (is (= marathon-api in-marathon-api))
                                                        (is (= service-id in-service-id))
                                                        {:id deployment-id})
                      marathon/delete-deployment (fn [in-marathon-api in-deployment-id]
                                                   (is (= marathon-api in-marathon-api))
                                                   (is (= deployment-id in-deployment-id))
                                                   (deliver deleted-deployment-promise :invoked))
                      marathon/get-app (fn [in-marathon-api in-service-id]
                                         (is (= marathon-api in-marathon-api))
                                         (is (= service-id in-service-id))
                                         {:app {:cmd "tc" :cpus 2 :id service-id :instances 25 :mem 4
                                                :tasks (->> (fn [] {:appId service-id
                                                                    :id (str service-id "." (rand-int 1000))})
                                                            (repeatedly 15))}})
                      marathon/update-app (fn [in-marathon-api in-service-id descriptor]
                                            (is (= marathon-api in-marathon-api))
                                            (is (= service-id in-service-id))
                                            (is (= {:cmd "tc" :cpus 2 :id service-id :instances instances :mem 4} descriptor))
                                            (deliver updated-invoked-promise :invoked))]
          (scheduler/scale-service marathon-scheduler service-id instances true)
          (is (= :invoked (deref deleted-deployment-promise 0 :not-invoked)))
          (is (= :invoked (deref updated-invoked-promise 0 :not-invoked))))))))

(defmacro run-sync-deployment-maintainer-iteration
  [leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout]
  `(->>
     (trigger-sync-deployment-maintainer-iteration
       ~leader?-fn @~service-id->out-of-sync-state-store ~marathon-scheduler ~trigger-timeout)
     (reset! ~service-id->out-of-sync-state-store)))

(let [marathon-api {:identifier (str "marathon-api-" (rand-int 10000))}
      make-marathon-scheduler (fn [service-id->out-of-sync-state-store]
                                {:identifier (str "marathon-scheduler-" (rand-int 10000))
                                 :is-waiter-app?-fn (fn [id] (str/starts-with? id "ws-"))
                                 :marathon-api marathon-api
                                 :service-id->out-of-sync-state-store service-id->out-of-sync-state-store})
      interval-ms 2000
      timeout-cycles 3
      interval-timeout (t/millis interval-ms)
      trigger-timeout (t/millis (* interval-ms timeout-cycles))
      initial-state {}
      make-app-entry (fn [deployment-ids service-id instances task-ids]
                       {:deployments (map (fn [deployment-id] [{:id deployment-id}]) deployment-ids)
                        :id (str "/" service-id)
                        :instances instances
                        :tasks (map (fn [task-id] [{:id task-id}]) task-ids)})]

  (deftest test-sync-deployment-maintainer-no-pending-sync-deployment
    (let [current-time (t/now)]
      (with-redefs [marathon/get-apps
                    (fn [in-marathon-api in-query-params]
                      (is (= marathon-api in-marathon-api))
                      (is (= {"embed" ["apps.deployments" "apps.tasks"]} in-query-params))
                      {:apps [(make-app-entry [] "ts-s1" 4 ["ts-s1.t1"])
                              (make-app-entry [] "ws-s1" 1 ["ws-s1.t1"])
                              (make-app-entry [] "ws-s2" 2 ["ws-s2.t1" "ws-s2.t2"])]})
                    t/now (constantly current-time)]
        (let [leader?-fn (constantly true)
              service-id->out-of-sync-state-store (atom initial-state)
              marathon-scheduler (make-marathon-scheduler service-id->out-of-sync-state-store)]
          (run-sync-deployment-maintainer-iteration
            leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
          (is (= {:last-update-time current-time
                  :service-id->out-of-sync-state {}}
                 @service-id->out-of-sync-state-store))))))

  (deftest test-sync-deployment-maintainer-ignore-service-with-pending-deployment
    (let [current-time (t/now)]
      (with-redefs [marathon/get-apps
                    (fn [in-marathon-api in-query-params]
                      (is (= marathon-api in-marathon-api))
                      (is (= {"embed" ["apps.deployments" "apps.tasks"]} in-query-params))
                      {:apps [(make-app-entry [] "ts-s1" 4 ["ts-s1.t1"])
                              (make-app-entry [] "ws-s1" 1 ["ws-s1.t1"])
                              (make-app-entry ["s2a.d12"] "ws-s2a" 2 ["ws-s2a.t1"])
                              (make-app-entry [] "ws-s2a" 2 ["ws-s2a.t1"])]})
                    t/now (constantly current-time)]
        (let [leader?-fn (constantly true)
              service-id->out-of-sync-state-store (atom initial-state)
              marathon-scheduler (make-marathon-scheduler service-id->out-of-sync-state-store)]
          (run-sync-deployment-maintainer-iteration
            leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
          (is (= {:last-update-time current-time
                  :service-id->out-of-sync-state
                  {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time current-time}}}
                 @service-id->out-of-sync-state-store))))))

  (deftest test-sync-deployment-maintainer-leadership-changes
    (let [time-0 (t/now)
          current-time-atom (atom time-0)]
      (with-redefs [marathon/get-apps
                    (fn [in-marathon-api in-query-params]
                      (is (= marathon-api in-marathon-api))
                      (is (= {"embed" ["apps.deployments" "apps.tasks"]} in-query-params))
                      {:apps [(make-app-entry [] "ts-s1" 4 ["ts-s1.t1"])
                              (make-app-entry [] "ws-s1" 1 ["ws-s1.t1"])
                              (make-app-entry [] "ws-s2a" 2 ["ws-s2a.t1"])]})
                    t/now (fn [] (deref current-time-atom))]
        (let [leader-atom (atom true)
              leader?-fn (fn [] (deref leader-atom))
              service-id->out-of-sync-state-store (atom initial-state)
              marathon-scheduler (make-marathon-scheduler service-id->out-of-sync-state-store)]
          ;; check we ignore sync-ed services
          (run-sync-deployment-maintainer-iteration
            leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
          (is (= {:last-update-time time-0
                  :service-id->out-of-sync-state
                  {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-0}}}
                 @service-id->out-of-sync-state-store))

          ;; check we reset state when we lose leadership
          (let [time-1 (t/plus time-0 interval-timeout)]
            (reset! leader-atom false)
            (reset! current-time-atom time-1)
            (run-sync-deployment-maintainer-iteration
              leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
            (is (= {:last-update-time time-1
                    :service-id->out-of-sync-state {}}
                   @service-id->out-of-sync-state-store))


            ;; check we update state when we regain leadership
            (let [time-2 (t/plus time-1 interval-timeout)]
              (reset! leader-atom true)
              (reset! current-time-atom time-2)
              (run-sync-deployment-maintainer-iteration
                leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
              (is (= {:last-update-time time-2
                      :service-id->out-of-sync-state
                      {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-2}}}
                     @service-id->out-of-sync-state-store))))))))

  (deftest test-sync-deployment-maintainer-forget-previous-out-of-sync-services
    (let [time-0 (t/now)
          current-time-atom (atom time-0)
          app-entries-atom (atom [(make-app-entry [] "ts-s1" 4 ["ts-s1.t1"])
                                  (make-app-entry [] "ws-s1" 1 ["ws-s1.t1"])])]
      (with-redefs [marathon/get-apps
                    (fn [in-marathon-api in-query-params]
                      (is (= marathon-api in-marathon-api))
                      (is (= {"embed" ["apps.deployments" "apps.tasks"]} in-query-params))
                      {:apps (deref app-entries-atom)})
                    t/now (fn [] (deref current-time-atom))]
        (let [leader-atom (atom true)
              leader?-fn (fn [] (deref leader-atom))
              service-id->out-of-sync-state-store (atom initial-state)
              marathon-scheduler (make-marathon-scheduler service-id->out-of-sync-state-store)]

          ;; check we ignore sync-ed services
          (run-sync-deployment-maintainer-iteration
            leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
          (is (= {:last-update-time time-0
                  :service-id->out-of-sync-state {}}
                 @service-id->out-of-sync-state-store))

          ;; check we detect out-of-sync services
          (let [time-1 (t/plus time-0 interval-timeout)]
            (swap! app-entries-atom conj (make-app-entry [] "ws-s2a" 2 ["ws-s2a.t1"]))
            (swap! app-entries-atom conj (make-app-entry [] "ws-s2b" 2 ["ws-s2b.t1"]))
            (swap! app-entries-atom conj (make-app-entry [] "ws-s2c" 2 ["ws-s2c.t1"]))
            (reset! current-time-atom time-1)
            (run-sync-deployment-maintainer-iteration
              leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
            (is (= {:last-update-time time-1
                    :service-id->out-of-sync-state
                    {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}
                     "ws-s2b" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}
                     "ws-s2c" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}}}
                   @service-id->out-of-sync-state-store))

            ;; check we forget previously out-of-sync services
            (let [time-2 (t/plus time-1 interval-timeout)]
              (swap! app-entries-atom (fn [app-entries] (remove #(= "/ws-s2b" (:id %)) app-entries)))
              (swap! app-entries-atom (fn [app-entries] (remove #(= "/ws-s2c" (:id %)) app-entries)))
              (swap! app-entries-atom conj (make-app-entry [] "ws-s2b" 3 ["ws-s2b.t1" "ws-s2b.t2" "ws-s2b.t3"]))
              (reset! current-time-atom time-2)
              (run-sync-deployment-maintainer-iteration
                leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
              (is (= {:last-update-time time-2
                      :service-id->out-of-sync-state
                      {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}}}
                     @service-id->out-of-sync-state-store))))))))

  (deftest test-sync-deployment-maintainer-detect-new-out-of-sync-deployment
    (let [time-0 (t/now)
          current-time-atom (atom time-0)
          app-entries-atom (atom [(make-app-entry [] "ts-s1" 4 ["ts-s1.t1"])
                                  (make-app-entry [] "ws-s1" 1 ["ws-s1.t1"])])]
      (with-redefs [marathon/get-apps
                    (fn [in-marathon-api in-query-params]
                      (is (= marathon-api in-marathon-api))
                      (is (= {"embed" ["apps.deployments" "apps.tasks"]} in-query-params))
                      {:apps (deref app-entries-atom)})
                    t/now (fn [] (deref current-time-atom))]
        (let [leader-atom (atom true)
              leader?-fn (fn [] (deref leader-atom))
              service-id->out-of-sync-state-store (atom initial-state)
              marathon-scheduler (make-marathon-scheduler service-id->out-of-sync-state-store)]

          ;; check we ignore sync-ed services
          (run-sync-deployment-maintainer-iteration
            leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
          (is (= {:last-update-time time-0
                  :service-id->out-of-sync-state {}}
                 @service-id->out-of-sync-state-store))

          ;; check we detect out-of-sync services
          (let [time-1 (t/plus time-0 interval-timeout)]
            (swap! app-entries-atom conj (make-app-entry [] "ws-s2a" 2 ["ws-s2a.t1"]))
            (reset! current-time-atom time-1)
            (run-sync-deployment-maintainer-iteration
              leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
            (is (= {:last-update-time time-1
                    :service-id->out-of-sync-state
                    {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}}}
                   @service-id->out-of-sync-state-store))

            ;; check we detect new out-of-sync services
            (let [time-2 (t/plus time-1 interval-timeout)]
              (swap! app-entries-atom conj (make-app-entry [] "ws-s4a" 4 ["ws-s4a.t1" "ws-s4a.t2"]))
              (reset! current-time-atom time-2)
              (run-sync-deployment-maintainer-iteration
                leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
              (is (= {:last-update-time time-2
                      :service-id->out-of-sync-state
                      {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}
                       "ws-s4a" {:data {:instances-requested 4 :instances-scheduled 2} :last-modified-time time-2}}}
                     @service-id->out-of-sync-state-store))))))))

  (deftest test-sync-deployment-maintainer-some-pending-sync-deployments
    (let [time-0 (t/now)
          time-1 (t/plus time-0 interval-timeout)
          time-2 (t/plus time-1 interval-timeout)
          time-3 (t/plus time-2 interval-timeout)
          time-4 (t/plus time-3 interval-timeout)
          time-5 (t/plus time-4 interval-timeout)
          time-6 (t/plus time-5 interval-timeout)
          time-7 (t/plus time-6 interval-timeout)
          current-time-atom (atom time-6)
          scheduler-operations-atom (atom [])
          app-entries-atom (atom [(make-app-entry [] "ts-s1" 4 ["ts-s1.t1"])
                                  (make-app-entry [] "ws-s1" 1 ["ws-s1.t1"])
                                  (make-app-entry [] "ws-s2a" 2 ["ws-s2a.t1"])
                                  (make-app-entry [] "ws-s4a" 4 ["ws-s4a.t1" "ws-s4a.t2"])])]
      (with-redefs [marathon/get-apps
                    (fn [in-marathon-api in-query-params]
                      (is (= marathon-api in-marathon-api))
                      (is (= {"embed" ["apps.deployments" "apps.tasks"]} in-query-params))
                      {:apps (deref app-entries-atom)})
                    scheduler/scale-service (fn [_ in-service-id in-target in-force]
                                              (swap! scheduler-operations-atom conj [in-service-id in-target in-force]))
                    t/now (fn [] (deref current-time-atom))]
        (let [leader-atom (atom true)
              leader?-fn (fn [] (deref leader-atom))
              service-id->out-of-sync-state-store
              (atom {:last-update-time time-6
                     :service-id->out-of-sync-state
                     {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}
                      "ws-s4a" {:data {:instances-requested 4 :instances-scheduled 2} :last-modified-time time-3}}})
              marathon-scheduler (make-marathon-scheduler service-id->out-of-sync-state-store)]

          (testing "do not trigger scale on 'active' out-of-sync services"
            (reset! current-time-atom time-4)
            (run-sync-deployment-maintainer-iteration
              leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
            (is (= {:last-update-time time-4
                    :service-id->out-of-sync-state
                    {"ws-s2a" {:data {:instances-requested 2 :instances-scheduled 1} :last-modified-time time-1}
                     "ws-s4a" {:data {:instances-requested 4 :instances-scheduled 2} :last-modified-time time-3}}}
                   @service-id->out-of-sync-state-store)))

          (testing "trigger scale on out-of-sync services"
            (reset! current-time-atom time-5)
            (run-sync-deployment-maintainer-iteration
              leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
            (is (= {:last-update-time time-5
                    :service-id->out-of-sync-state
                    {"ws-s4a" {:data {:instances-requested 4 :instances-scheduled 2} :last-modified-time time-3}}}
                   @service-id->out-of-sync-state-store))
            (is (= [["ws-s2a" 1 false]] (deref scheduler-operations-atom))))

          (testing "trigger scale remaining out-of-sync services"

            (swap! app-entries-atom (fn [app-entries] (remove #(= "/ws-s2a" (:id %)) app-entries)))
            (reset! current-time-atom time-6)
            (run-sync-deployment-maintainer-iteration
              leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
            (is (= {:last-update-time time-6
                    :service-id->out-of-sync-state
                    {"ws-s4a" {:data {:instances-requested 4 :instances-scheduled 2} :last-modified-time time-3}}}
                   @service-id->out-of-sync-state-store))
            (is (= [["ws-s2a" 1 false]] (deref scheduler-operations-atom)))

            (reset! current-time-atom time-7)
            (run-sync-deployment-maintainer-iteration
              leader?-fn service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
            (is (= {:last-update-time time-7
                    :service-id->out-of-sync-state {}}
                   @service-id->out-of-sync-state-store))
            (is (= [["ws-s2a" 1 false] ["ws-s4a" 2 false]] (deref scheduler-operations-atom)))))))))

(deftest test-start-new-service-wrapper
  (let [marathon-api (Object.)
        service-id "test-service-id"
        marathon-descriptor {:reference (Object.)}
        deployment-error-response (->> {:deployments [{:id "d-1234"}]
                                        :message "App is locked by one or more deployments."}
                                       utils/clj->json
                                       (assoc {:status 409} :body))
        create-app-error-factory (fn [error-call-limit create-call-counter]
                                   (fn [in-marathon-api in-marathon-descriptor]
                                     (is (= marathon-api in-marathon-api))
                                     (is (= marathon-descriptor in-marathon-descriptor))
                                     (swap! create-call-counter inc)
                                     (if (> @create-call-counter error-call-limit)
                                       :success
                                       (ss/throw+ deployment-error-response))))
        delete-deployment-factory (fn [deleted-deployments]
                                    (fn [in-marathon-api deployment-id]
                                      (is (= marathon-api in-marathon-api))
                                      (swap! deleted-deployments conj deployment-id)))
        newish-version (du/date-to-str (t/minus (t/now) (t/minutes 1)) formatter-marathon)
        outdated-version (du/date-to-str (t/minus (t/now) (t/minutes 15)) formatter-marathon)]

    (testing "service starts successfully"
      (let [create-call-counter (atom 0)]
        (with-redefs [marathon/create-app (create-app-error-factory 0 create-call-counter)]
          (let [result (start-new-service-wrapper marathon-api service-id marathon-descriptor)]
            (is (= :success result))
            (is (= 1 @create-call-counter))))))

    (testing "service starts successfully despite StopApplication deployment"
      (let [create-call-counter (atom 0)
            deleted-deployments (atom [])]
        (with-redefs [marathon/create-app (create-app-error-factory 1 create-call-counter)
                      marathon/delete-deployment (delete-deployment-factory deleted-deployments)
                      marathon/get-deployments (fn [in-marathon-api]
                                                 (is (= marathon-api in-marathon-api))
                                                 [{:affectedApps ["/test-service-id"]
                                                   :currentActions [{:action "StopApplication"
                                                                     :app "/test-service-id"}]
                                                   :id "d-1234"
                                                   :version outdated-version}])]
          (is (= :success (start-new-service-wrapper marathon-api service-id marathon-descriptor)))
          (is (= 2 @create-call-counter))
          (is (= ["d-1234"] @deleted-deployments)))))

    (testing "service starts fails due to repeated recent StopApplication deployment"
      (let [create-call-counter (atom 0)]
        (with-redefs [marathon/create-app (create-app-error-factory 10 create-call-counter)
                      marathon/get-deployments (fn [in-marathon-api]
                                                 (is (= marathon-api in-marathon-api))
                                                 [{:affectedApps ["/test-service-id"]
                                                   :currentActions [{:action "StopApplication"
                                                                     :app "/test-service-id"}]
                                                   :id "d-1234"
                                                   :version newish-version}])]
          (is (nil? (start-new-service-wrapper marathon-api service-id marathon-descriptor)))
          (is (= 1 @create-call-counter)))))

    (testing "service starts fails due to repeated outdated StopApplication deployment"
      (let [create-call-counter (atom 0)
            deleted-deployments (atom [])]
        (with-redefs [marathon/create-app (create-app-error-factory 10 create-call-counter)
                      marathon/delete-deployment (delete-deployment-factory deleted-deployments)
                      marathon/get-deployments (fn [in-marathon-api]
                                                 (is (= marathon-api in-marathon-api))
                                                 [{:affectedApps ["/test-service-id"]
                                                   :currentActions [{:action "StopApplication"
                                                                     :app "/test-service-id"}]
                                                   :id "d-1234"
                                                   :version outdated-version}])]
          (is (nil? (start-new-service-wrapper marathon-api service-id marathon-descriptor)))
          (is (= 2 @create-call-counter))
          (is (= ["d-1234"] @deleted-deployments)))))

    (testing "service starts fails due to recent StopApplication deployments"
      (let [create-call-counter (atom 0)]
        (with-redefs [marathon/create-app (create-app-error-factory 10 create-call-counter)
                      marathon/get-deployments (fn [in-marathon-api]
                                                 (is (= marathon-api in-marathon-api))
                                                 [{:affectedApps ["/test-service-id"]
                                                   :currentActions [{:action "ScaleApplication"
                                                                     :app "/test-service-id"}]
                                                   :id "d-1234"
                                                   :version newish-version}])]
          (is (nil? (start-new-service-wrapper marathon-api service-id marathon-descriptor)))
          (is (= 1 @create-call-counter)))))

    (testing "service starts fails due to repeated conflict deployments"
      (let [create-call-counter (atom 0)]
        (with-redefs [marathon/create-app (create-app-error-factory 10 create-call-counter)
                      marathon/get-deployments (fn [in-marathon-api]
                                                 (is (= marathon-api in-marathon-api))
                                                 [{:affectedApps ["/test-service-id"]
                                                   :currentActions [{:action "ScaleApplication"
                                                                     :app "/test-service-id"}]
                                                   :id "d-1234"
                                                   :version newish-version}])]
          (is (nil? (start-new-service-wrapper marathon-api service-id marathon-descriptor)))
          (is (= 1 @create-call-counter)))))))
