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
(ns waiter.scheduler.kubernetes-test
  (:require [clojure.core.async :as async]
            [clojure.data]
            [clojure.pprint]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.config :as config]
            [waiter.scheduler :as scheduler]
            [waiter.scheduler.kubernetes :refer :all]
            [waiter.util.client-tools :as ct]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (waiter.scheduler Service ServiceInstance)
           (waiter.scheduler.kubernetes KubernetesScheduler)))

(defmacro throw-exception
  "Throw a RuntimeException with the stack trace suppressed.
   Suppressing the stack trace helps keep our test logs cleaner."
  []
  `(throw
     (doto (RuntimeException.)
       (.setStackTrace (make-array StackTraceElement 0)))))

(def ^:const default-pod-suffix-length 5)

(def dummy-scheduler-default-namespace "waiter")

(defn- make-dummy-scheduler
  ([service-ids] (make-dummy-scheduler service-ids {}))
  ([service-ids args]
   (->
     {:authorizer {:kind :default
                   :default {:factory-fn 'waiter.authorization/noop-authorizer}}
      :daemon-state (atom nil)
      :cluster-name "waiter"
      :fileserver {:port 9090
                   :scheme "http"}
      :log-bucket-sync-secs 60
      :log-bucket-url "http://waiter.example.com:8888/waiter-service-logs"
      :max-patch-retries 5
      :max-name-length 63
      :pod-base-port 8080
      :pod-sigkill-delay-secs 3
      :pod-suffix-length default-pod-suffix-length
      :replicaset-api-version "extensions/v1beta1"
      :replicaset-spec-builder-fn #(waiter.scheduler.kubernetes/default-replicaset-builder
                                     %1 %2 %3
                                     {:container-init-commands ["waiter-k8s-init"]
                                      :default-namespace dummy-scheduler-default-namespace
                                      :default-container-image "twosigma/waiter-test-apps:latest"})
      :service-id->failed-instances-transient-store (atom {})
      :service-id->password-fn #(str "password-" %)
      :service-id->service-description-fn (pc/map-from-keys (constantly {"health-check-port-index" 0
                                                                         "run-as-user" "myself"})
                                                            service-ids)
      :scheduler-name "dummy-scheduler"
      :watch-state (atom nil)}
     (merge args)
     (update-in [:authorizer] utils/create-component)
     map->KubernetesScheduler)))

(def dummy-service-description
  {"backend-proto" "http"
   "cmd" "foo"
   "cpus" 1.2
   "grace-period-secs" 7
   "health-check-interval-secs" 10
   "health-check-max-consecutive-failures" 2
   "health-check-port-index" 0
   "mem" 1024
   "min-instances" 1
   "ports" 2
   "run-as-user" "myself"})

(defn- sanitize-k8s-service-records
  "Walks data structure to remove extra fields added by Kubernetes from Service and ServiceInstance records."
  [walkable-collection]
  (walk/postwalk
    (fn sanitizer [x]
      (cond
        (instance? Service x)
        (dissoc x :k8s/app-name :k8s/namespace)
        (instance? ServiceInstance x)
        (dissoc x :k8s/app-name :k8s/namespace :k8s/pod-name :k8s/restart-count :k8s/user)
        :else x))
    walkable-collection))

(defmacro assert-data-equal
  [expected actual]
  `(let [expected# ~expected
         actual# ~actual]
     (when-not (= expected# actual#)
       (clojure.pprint/pprint
         (clojure.data/diff expected# actual#)))
     (is (= expected# actual#))))

(deftest replicaset-spec-health-check-port-index
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [service-description (assoc dummy-service-description "health-check-port-index" 2 "ports" 3)
          scheduler (make-dummy-scheduler ["test-service-id"]
                                          {:service-id->service-description-fn (constantly service-description)})
          replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" service-description)]
      (is (= {:waiter/port-count "3"
              :waiter/service-id "test-service-id"}
             (get-in replicaset-spec [:spec :template :metadata :annotations]))))))

(deftest replicaset-spec-namespace
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [service-id "test-service-id"]
      (testing "Default namespace"
        (let [scheduler (make-dummy-scheduler [service-id]
                                              {:service-id->service-description-fn (constantly dummy-service-description)})
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id dummy-service-description)]
          (is (nil? (scheduler/validate-service scheduler service-id)))
          (is (= dummy-scheduler-default-namespace (get-in replicaset-spec [:metadata :namespace])))))
      (testing "Valid custom namespace"
        (let [target-namespace "myself"
              service-description (assoc dummy-service-description "namespace" target-namespace)
              scheduler (make-dummy-scheduler [service-id]
                                              {:service-id->service-description-fn (constantly service-description)})
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description)]
          (is (nil? (scheduler/validate-service scheduler service-id)))
          (is (= target-namespace (get-in replicaset-spec [:metadata :namespace]))))))))

(deftest replicaset-spec-no-image
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler (make-dummy-scheduler ["test-service-id"])
          replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" dummy-service-description)]
      (is (= "twosigma/waiter-test-apps:latest" (get-in replicaset-spec [:spec :template :spec :containers 0 :image]))))))

(deftest replicaset-spec-custom-image
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler (make-dummy-scheduler ["test-service-id"])
          replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id"
                            (assoc dummy-service-description "image" "custom/image"))]
      (is (= "custom/image" (get-in replicaset-spec [:spec :template :spec :containers 0 :image]))))))

(deftest test-service-id->k8s-app-name
  (let [base-scheduler-spec {:pod-suffix-length default-pod-suffix-length}
        long-app-name (str/join (repeat 200 \A))
        sample-uuid "e8b625cc83c411e8974c38d5474b213d"
        short-app-name "myapp"
        short-sample-uuid (str (subs sample-uuid 0 8)
                               (subs sample-uuid 24))
        waiter-service-prefix "waiter-service-id-prefix-"]
    (let [{:keys [max-name-length] :as long-name-scheduler}
          (assoc base-scheduler-spec :max-name-length 128)]
      (testing "Very long name length limit maps long service-id through unmodified"
        (let [service-id (str "this_name_is_longer_than_64_chars_but_should_be_unmodified_by_the_mapping_function-"
                              sample-uuid)]
          (is (= service-id (service-id->k8s-app-name long-name-scheduler service-id)))))
      (testing "Very long name length limit maps long service-id through without prefix"
        (let [service-id (str waiter-service-prefix
                              "this_name_is_longer_than_64_chars_but_should_be_unmodified_by_the_mapping_function-"
                              sample-uuid)]
          (is (= (subs service-id (count waiter-service-prefix))
                 (service-id->k8s-app-name long-name-scheduler service-id)))))
      (testing "Very long name length limit maps truncates service name, but keeps full uuid"
        (let [service-id (str waiter-service-prefix long-app-name "-" sample-uuid)]
          (is (= (str (subs long-app-name 0 (- max-name-length 1 32 1 default-pod-suffix-length))
                      "-" sample-uuid)
                 (service-id->k8s-app-name long-name-scheduler service-id))))))
    (let [{:keys [max-name-length] :as short-name-scheduler}
          (assoc base-scheduler-spec :max-name-length 32)]
      (assert (== 32 (count sample-uuid)))
      (assert (== 16 (count short-sample-uuid)))
      (testing "Short name length limit with short app name only shortens uuid"
        (let [service-id (str short-app-name "-" sample-uuid)]
          (is (= (str short-app-name "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id)))))
      (testing "Short name length limit with short app name only removes prefix, shortens uuid"
        (let [service-id (str waiter-service-prefix short-app-name "-" sample-uuid)]
          (is (= (str short-app-name "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id)))))
      (testing "Short name length limit with long app name truncates app name, removes prefix, and shortens uuid"
        (let [service-id (str waiter-service-prefix long-app-name "-" sample-uuid)]
          (is (= (str (subs long-app-name 0 (- max-name-length 1 16 1 default-pod-suffix-length))
                      "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id))))))))

(defn- reset-scheduler-watch-state!
  ([scheduler rs-response]
   (reset-scheduler-watch-state! scheduler rs-response nil))
  ([{:keys [watch-state] :as scheduler} rs-response pods-response]
   (let [dummy-resource-uri "http://ignored-uri"
         rs-state (global-rs-state-query
                    scheduler {:api-request-fn (constantly rs-response)} dummy-resource-uri)
         pods-state (global-pods-state-query
                      scheduler {:api-request-fn (constantly pods-response)} dummy-resource-uri)
         global-state (merge rs-state pods-state)]
     (reset! watch-state global-state))))

(deftest test-scheduler-get-services
  (let [test-cases
        [{:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items []}
          :expected-result nil}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-1234"
                               :namespace "myself"
                               :labels {:app "test-app-1234"
                                        :waiter-cluster "waiter"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-1234"}
                               :annotations {:waiter/service-id "test-app-1234"}}
                    :spec {:replicas 2
                           :selector {:matchLabels {:app "test-app-1234"
                                                    :waiter-cluster "waiter"
                                                    :waiter/cluster "waiter"}}}
                    :status {:replicas 2
                             :readyReplicas 2
                             :availableReplicas 2}}
                   {:metadata {:name "test-app-6789"
                               :namespace "myself"
                               :labels {:app "test-app-6789"
                                        :waiter-cluster "waiter"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-6789"}
                               :annotations {:waiter/service-id "test-app-6789"}}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-6789"
                                                    :waiter-cluster "waiter"
                                                    :waiter/cluster "waiter"}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 2
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-1234"
                                    :instances 2
                                    :task-count 2
                                    :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
           (scheduler/make-Service {:id "test-app-6789" :instances 3 :task-count 3
                                    :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})]}
         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-abcd"
                               :namespace "myself"
                               :labels {:app "test-app-abcd"
                                        :waiter-cluster "waiter"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-abcd"}
                               :annotations {:waiter/service-id "test-app-abcd"}}
                    :spec {:replicas 2
                           :selector {:matchLabels {:app "test-app-abcd"
                                                    :waiter-cluster "waiter"
                                                    :waiter/cluster "waiter"}}}
                    :status {:replicas 2
                             :readyReplicas 2
                             :availableReplicas 2}}
                   {:mismatched-replicaset "should be ignored"}
                   {:metadata {:name "test-app-wxyz"
                               :namespace "myself"
                               :labels {:app "test-app-wxyz"
                                        :waiter-cluster "waiter"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-wxyz"}
                               :annotations {:waiter/service-id "test-app-wxyz"}}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-wxyz"
                                                    :waiter-cluster "waiter"
                                                    :waiter/cluster "waiter"}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 2
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-abcd"
                                    :instances 2
                                    :task-count 2
                                    :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
           (scheduler/make-Service {:id "test-app-wxyz" :instances 3 :task-count 3
                                    :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-4321"
                               :namespace "myself"
                               :labels {:app "test-app-4321"
                                        :waiter-cluster "waiter"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-4321"}
                               :annotations {:waiter/service-id "test-app-4321"}}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-4321"
                                                    :waiter-cluster "waiter"
                                                    :waiter/cluster "waiter"}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 1
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-4321" :instances 3 :task-count 3
                                    :task-stats {:running 2 :healthy 1 :unhealthy 1 :staged 1}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "extensions/v1beta1"
           :items [{:metadata {:name "test-app-9999"
                               :namespace "myself"
                               :labels {:app "test-app-9999"
                                        :waiter-cluster "waiter"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-9999"}
                               :annotations {:waiter/service-id "test-app-9999"}}
                    :spec {:replicas 0
                           :selector {:matchLabels {:app "test-app-9999"
                                                    :waiter-cluster "waiter"
                                                    :waiter/cluster "waiter"}}}
                    :status {:replicas 0
                             :readyReplicas 0
                             :availableReplicas 0}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-9999"
                                    :instances 0
                                    :task-count 0
                                    :task-stats {:running 0, :healthy 0, :unhealthy 0, :staged 0}})]}]]
    (log/info "Expecting Key error due to bad :mismatched-replicaset in API server response...")
    (doseq [{:keys [api-server-response expected-result]} test-cases]
      (let [dummy-scheduler (make-dummy-scheduler ["test-app-1234" "test-app-6789"])
            _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
            actual-result (->> dummy-scheduler
                               scheduler/get-services
                               sanitize-k8s-service-records)]
        (assert-data-equal expected-result actual-result)))))

(deftest test-scheduler-get-service->instances
  (let [rs-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :items [{:metadata {:name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/service-id "test-app-1234"}}
                  :spec {:replicas 2}
                  :status {:replicas 2
                           :readyReplicas 2
                           :availableReplicas 2}}
                 {:metadata {:name "test-app-6789"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/service-id "test-app-6789"}}
                  :spec {:replicas 3}
                  :status {:replicas 3
                           :readyReplicas 1
                           :availableReplicas 2
                           :unavailableReplicas 1}}]}

        pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :items [{:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"
                                      :waiter/user "myself"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-6789-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.13"
                           :startTime "2014-09-13T00:24:35Z"
                           :containerStatuses [{:name "test-app-6789"
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-6789-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.14"
                           :startTime "2014-09-13T00:24:37Z"
                           :containerStatuses [{:name "test-app-6789"
                                                :lastState {:terminated {:exitCode 255
                                                                         :reason "Error"
                                                                         :startedAt "2014-09-13T00:24:36Z"}}
                                                :restartCount 1}]}}
                 {:metadata {:name "test-app-6789-abcd3"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.15"
                           :startTime "2014-09-13T00:24:38Z"
                           :containerStatuses [{:name "test-app-6789"
                                                :restartCount 0}]}}]}

        expected (hash-map
                   (scheduler/make-Service {:id "test-app-1234"
                                            :instances 2
                                            :task-count 2
                                            :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
                   {:active-instances
                    [(scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.11"
                        :id "test-app-1234.test-app-1234-abcd1-0"
                        :log-directory "/home/myself/r0"
                        :port 8080
                        :service-id "test-app-1234"
                        :started-at (du/str-to-date "2014-09-13T00:24:46Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.12"
                        :id "test-app-1234.test-app-1234-abcd2-0"
                        :log-directory "/home/myself/r0"
                        :port 8080
                        :service-id "test-app-1234"
                        :started-at (du/str-to-date "2014-09-13T00:24:47Z" k8s-timestamp-format)})]
                    :failed-instances []}

                   (scheduler/make-Service {:id "test-app-6789" :instances 3 :task-count 3
                                            :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})
                   {:active-instances
                    [(scheduler/make-ServiceInstance
                       {:healthy? true
                        :host "10.141.141.13"
                        :id "test-app-6789.test-app-6789-abcd1-0"
                        :log-directory "/home/myself/r0"
                        :port 8080
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:35Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? false
                        :host "10.141.141.14"
                        :id "test-app-6789.test-app-6789-abcd2-1"
                        :log-directory "/home/myself/r1"
                        :port 8080
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:37Z" k8s-timestamp-format)})
                     (scheduler/make-ServiceInstance
                       {:healthy? false
                        :host "10.141.141.15"
                        :id "test-app-6789.test-app-6789-abcd3-0"
                        :log-directory "/home/myself/r0"
                        :port 8080
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:38Z" k8s-timestamp-format)})]
                    :failed-instances
                    [(scheduler/make-ServiceInstance
                       {:exit-code 255
                        :healthy? false
                        :host "10.141.141.14"
                        :id "test-app-6789.test-app-6789-abcd2-0"
                        :log-directory "/home/myself/r0"
                        :port 8080
                        :service-id "test-app-6789"
                        :started-at (du/str-to-date "2014-09-13T00:24:36Z" k8s-timestamp-format)})]})
        dummy-scheduler (make-dummy-scheduler ["test-app-1234" "test-app-6789"])
        _ (reset-scheduler-watch-state! dummy-scheduler rs-response pods-response)
        actual (->> dummy-scheduler
                    get-service->instances
                    sanitize-k8s-service-records)]
    (assert-data-equal expected actual)))

(deftest test-kill-instance
  (let [service-id "test-kill-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/namespace "myself"})
        instance-id "instance-id"
        instance (scheduler/make-ServiceInstance
                   {:extra-ports []
                    :healthy? true
                    :host "10.141.141.10"
                    :id instance-id
                    :log-directory "/home/myself/r0"
                    :k8s/namespace "myself"
                    :port 8080
                    :service-id service-id
                    :started-at (du/str-to-date "2014-09-13T00:24:56Z" k8s-timestamp-format)})
        dummy-scheduler (make-dummy-scheduler [service-id])
        api-server-response {:kind "ReplicaSetList"
                             :apiVersion "extensions/v1beta1"
                             :items [{:metadata {:name service-id
                                                 :namespace "myself"
                                                 :labels {:app service-id
                                                          :waiter-cluster "waiter"
                                                          :waiter/cluster "waiter"
                                                          :waiter/service-hash service-id}
                                                 :annotations {:waiter/service-id service-id}}
                                      :spec {:replicas 1}
                                      :status {:replicas 1
                                               :readyReplicas 1
                                               :availableReplicas 1}}]}
        _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
        partial-expected {:instance-id instance-id :killed? false :service-id service-id}]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-delete"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :killed? true
                   :message "Successfully killed instance"
                   :status 200)
                 actual))))
      (testing "unsuccessful-delete: no such instance"
        (let [error-msg "Instance not found"
              actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status 404})))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :message error-msg
                   :status 404)
                 actual))))
      (testing "successful-delete: terminated, but had patch conflict"
        (log/info "Expecting 409 patch-conflict error when deleting service instance...")
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :patch)
                                                   (ss/throw+ {:status 409})))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :killed? true
                   :message "Successfully killed instance"
                   :status 200)
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                   :message "Error while killing instance"
                   :status 500)
                 actual)))))))

(deftest test-scheduler-service-exists?
  (let [service-id "test-app-1234"
        empty-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :items []}
        non-empty-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :items [{:metadata {:name service-id
                             :namespace "myself"
                             :labels {:app service-id
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash service-id}
                             :annotations {:waiter/service-id service-id}}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app service-id
                                                  :waiter-cluster "waiter"
                                                  :waiter/cluster "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 2
                           :availableReplicas 2}}]}
        test-cases [{:api-server-response nil
                     :expected-result false}
                    {:api-server-response {}
                     :expected-result false}
                    {:api-server-response empty-response
                     :expected-result false}
                    {:api-server-response non-empty-response
                     :expected-result true}]]
    (doseq [{:keys [api-server-response expected-result]} test-cases]
      (let [dummy-scheduler (make-dummy-scheduler [service-id])
            _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
            actual-result (scheduler/service-exists? dummy-scheduler service-id)]
        (is (= expected-result actual-result))))))

(deftest test-create-app
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [service-id "test-service-id"
          service {:service-id service-id}
          descriptor {:service-description dummy-service-description
                      :service-id service-id}
          dummy-scheduler (make-dummy-scheduler [service-id])]
      (testing "unsuccessful-create: app already exists"
        (let [actual (with-redefs [service-id->service (constantly service)]
                       (scheduler/create-service-if-new dummy-scheduler descriptor))]
          (is (nil? actual))))
      (with-redefs [service-id->service (constantly nil)]
        (testing "unsuccessful-create: service creation conflict (already running)"
          (let [actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                   (ss/throw+ {:status 409}))]
                         (scheduler/create-service-if-new dummy-scheduler descriptor))]
            (is (nil? actual))))
        (testing "unsuccessful-create: internal error"
          (let [actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                   (throw-exception))]
                         (scheduler/create-service-if-new dummy-scheduler descriptor))]
            (is (nil? actual))))
        (testing "successful create"
          (let [actual (with-redefs [api-request (constantly service)
                                     replicaset->Service identity]
                         (scheduler/create-service-if-new dummy-scheduler descriptor))]
            (is (= service actual))))))))

(deftest test-keywords-in-replicaset-spec
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (testing "namespaced keywords in annotation keys and values correctly converted"
      (let [service-id "test-service-id"
            descriptor {:service-description dummy-service-description
                        :service-id service-id}
            dummy-scheduler (-> (make-dummy-scheduler [service-id])
                                (update :replicaset-spec-builder-fn
                                        (fn [base-spec-builder-fn]
                                          (fn [scheduler service-id context]
                                            (-> (base-spec-builder-fn scheduler service-id context)
                                                (assoc-in [:metadata :annotations] {:waiter/x :waiter/y}))))))]
        (let [spec-json (with-redefs [api-request (fn [_ _ & {:keys [body]}] body)
                                      replicaset->Service identity]
                          (create-service descriptor dummy-scheduler))]
          (is (str/includes? spec-json "\"annotations\":{\"waiter/x\":\"waiter/y\"}")))))))

(deftest test-delete-service
  (let [service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/app-name service-id :k8s/namespace "myself"})
        dummy-scheduler (make-dummy-scheduler [service-id])]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-delete"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message (str "Kubernetes deleted ReplicaSet for " service-id)
                  :result :deleted}
                 actual))))
      (testing "unsuccessful-delete: service not found"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status 404})))]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message "Kubernetes reports service does not exist"
                  :result :no-such-service-exists}
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message "Internal error while deleting service"
                  :result :error}
                 actual)))))))

(deftest test-scale-service
  (let [instances' 4
        service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/app-name service-id :k8s/namespace "myself"})
        service-state (atom {:service-id->service {service-id service}})
        dummy-scheduler (assoc (make-dummy-scheduler [service-id]) :watch-state service-state)]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-scale"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success true
                  :status 200
                  :result :scaled
                  :message (str "Scaled to " instances')}
                 actual))))
      (testing "unsuccessful-scale: service not found"
        (let [actual (with-redefs [service-id->service (constantly nil)]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status 404
                  :result :no-such-service-exists
                  :message "Failed to scale missing service"}
                 actual))))
      (testing "unsuccessful-scale: patch conflict"
        (let [actual (with-redefs [api-request (fn [& _] (ss/throw+ {:status 409}))]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status 409
                  :result :conflict
                  :message "Scaling failed due to repeated patch conflicts"}
                 actual))))
      (testing "unsuccessful-scale: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status 500
                  :result :failed
                  :message "Error while scaling waiter service"}
                 actual)))))))

(deftest test-retrieve-directory-content
  ;; Killed pod
  (let [service-id "service-id"
        instance-id "service-id.instance-id-321"
        path "/x/a"
        expected [{:name "bar",
                   :size 4,
                   :type "file",
                   :url
                   "http://waiter.example.com:8888/waiter-service-logs/myself/service-id/instance-id/r321/x/a/bar"}
                  {:name "baz",
                   :size 4,
                   :type "file",
                   :url
                   "http://waiter.example.com:8888/waiter-service-logs/myself/service-id/instance-id/r321/x/a/baz"}
                  {:name "hello.txt",
                   :size 6,
                   :type "file",
                   :url
                   "http://waiter.example.com:8888/waiter-service-logs/myself/service-id/instance-id/r321/x/a/hello.txt"}
                  {:name "aa",
                   :path "/myself/service-id/instance-id/r321/x/a/aa",
                   :type "directory"}
                  {:name "bb",
                   :path "/myself/service-id/instance-id/r321/x/a/bb",
                   :type "directory"}]
        xml-response "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>waiter-service-logs</Name><Prefix>myself/service-id/instance-id/r321/x/a/</Prefix><Marker/><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated><Contents><Key>myself/service-id/instance-id/r321/x/a/bar</Key><LastModified>2019-01-15T16:01:46.824Z</LastModified><ETag>&quot;c157a79031e1c40f85931829bc5fc552&quot;</ETag><Size>4</Size><Owner><ID>http://acs.amazonaws.com/groups/global/AllUsers</ID><DisplayName>undefined</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>myself/service-id/instance-id/r321/x/a/baz</Key><LastModified>2019-01-15T16:01:46.995Z</LastModified><ETag>&quot;258622b1688250cb619f3c9ccaefb7eb&quot;</ETag><Size>4</Size><Owner><ID>http://acs.amazonaws.com/groups/global/AllUsers</ID><DisplayName>undefined</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>myself/service-id/instance-id/r321/x/a/hello.txt</Key><LastModified>2019-01-15T16:01:46.950Z</LastModified><ETag>&quot;b1946ac92492d2347c6235b4d2611184&quot;</ETag><Size>6</Size><Owner><ID>http://acs.amazonaws.com/groups/global/AllUsers</ID><DisplayName>undefined</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><CommonPrefixes><Prefix>myself/service-id/instance-id/r321/x/a/aa/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>myself/service-id/instance-id/r321/x/a/bb/</Prefix></CommonPrefixes></ListBucketResult>"
        dummy-scheduler (make-dummy-scheduler [service-id])
        host "host.local"]
    (testing "s3 logs from killed pod"
      (let [actual (with-redefs [hu/http-request (constantly xml-response)]
                     (scheduler/retrieve-directory-content
                       dummy-scheduler service-id instance-id host path))]
        (is (= expected actual)))))

  ;; Live pod
  (let [service-id "test-service-id"
        instance-id "test-service-id.instance-id-0"
        instance-base-dir "/r0"
        pod-name "instance-id"
        host "host.local"
        path "/some/path/"
        {:keys [watch-state] :as dummy-scheduler} (make-dummy-scheduler [service-id])
        port (get-in dummy-scheduler [:fileserver :port])
        make-file (fn [file-name size]
                    {:url (str "http://" host ":" port instance-base-dir path file-name)
                     :name file-name
                     :size size
                     :type "file"})
        make-dir (fn [dir-name]
                   {:path (str path dir-name)
                    :name dir-name
                    :type "directory"})
        strip-links (partial mapv #(dissoc % :url))
        inputs [{:description "empty directory"
                 :expected []}
                {:description "single file"
                 :expected [(make-file "foo" 1)]}
                {:description "single directory"
                 :expected [(make-dir "foo")]}
                {:description "lots of stuff"
                 :expected [(make-file "a" 10)
                            (make-file "b" 30)
                            (make-file "c" 40)
                            (make-file "d" 50)
                            (make-dir "w")
                            (make-dir "x")
                            (make-dir "y")
                            (make-dir "z")]}]]
    (swap! watch-state assoc-in [:service-id->pod-id->pod service-id pod-name] ::pod)
    (doseq [{:keys [description expected]} inputs]
      (testing description
        (let [actual (with-redefs [hu/http-request (constantly (strip-links expected))]
                       (scheduler/retrieve-directory-content
                         dummy-scheduler service-id instance-id host path))]
          (is (= expected actual)))))))

(defn test-auth-refresher
  "Test implementation of the authentication action-fn"
  [{:keys [refresh-value] :as context}]
  refresh-value)

(deftest test-kubernetes-scheduler
  (let [context {:is-waiter-service?-fn (constantly nil)
                 :leader?-fn (constantly nil)
                 :scheduler-name "kubernetes"
                 :scheduler-state-chan (async/chan 4)
                 :scheduler-syncer-interval-secs 5
                 :service-id->password-fn (constantly nil)
                 :service-id->service-description-fn (constantly nil)
                 :start-scheduler-syncer-fn (constantly nil)}
        custom-options {:a 1 :b "two"}
        k8s-config {:authentication nil
                    :authorizer {:kind :default
                                 :default {:factory-fn 'waiter.authorization/noop-authorizer}}
                    :cluster-name "waiter"
                    :custom-options custom-options
                    :fileserver {:port 9090
                                 :scheme "http"}
                    :watch-state (atom nil)
                    :http-options {:conn-timeout 10000
                                   :socket-timeout 10000}
                    :log-bucket-sync-secs 60
                    :log-bucket-url nil
                    :max-patch-retries 5
                    :max-name-length 63
                    :pod-base-port 8080
                    :pod-sigkill-delay-secs 3
                    :pod-suffix-length default-pod-suffix-length
                    :replicaset-api-version "extensions/v1beta1"
                    :replicaset-spec-builder {:factory-fn 'waiter.scheduler.kubernetes/default-replicaset-builder
                                              :container-init-commands ["waiter-k8s-init"]
                                              :default-container-image "twosigma/waiter-test-apps:latest"}
                    :url "http://127.0.0.1:8001"}
        base-config (merge context k8s-config)]
    (with-redefs [start-pods-watch! (constantly nil)
                  start-replicasets-watch! (constantly nil)]
      (testing "Creating a KubernetesScheduler"

        (testing "should throw on invalid configuration"
          (testing "bad url"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url nil))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url ""))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url "localhost")))))
          (testing "bad http options"
            (is (thrown? Throwable (kubernetes-scheduler (update-in base-config [:http-options :conn-timeout] 0))))
            (is (thrown? Throwable (kubernetes-scheduler (update-in base-config [:http-options :socket-timeout] 0)))))
          (testing "bad max conflict retries"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :max-patch-retries -1)))))
          (testing "bad max name length"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :max-name-length 0)))))
          (testing "bad ReplicaSet spec factory function"
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] nil))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] "not a symbol"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] :not-a-symbol))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] 'not.a.namespace/not-a-fn)))))
          (testing "bad base port number"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port -1))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port "8080"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port 1234567890)))))

          (testing "bad pod termination grace period"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs -1))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs "10"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs 1200))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs 1234567890))))))

        (testing "should work with valid configuration"
          (is (instance? KubernetesScheduler (kubernetes-scheduler base-config))))

        (testing "should retain custom plugin options"
          (is (= custom-options (-> base-config kubernetes-scheduler :custom-options))))

        (testing "periodic auth-refresh task"
          (let [kill-task-fn (atom (constantly nil))
                orig-start-auth-renewer start-auth-renewer
                secret-value "secret-value"]
            (try
              (with-redefs [start-auth-renewer #(reset! kill-task-fn (apply orig-start-auth-renewer %&))]
                (is (instance? KubernetesScheduler (kubernetes-scheduler (assoc base-config :authentication {:action-fn `test-auth-refresher
                                                                                                             :refresh-delay-mins 1
                                                                                                             :refresh-value secret-value})))))
              (is (ct/wait-for #(= secret-value @k8s-api-auth-str) :interval 1 :timeout 10))
              (finally
                (@kill-task-fn)))))

        (testing "validate service - normal"
          (scheduler/validate-service
            (kubernetes-scheduler (assoc base-config :service-id->service-description-fn (constantly {}))) nil))
        (testing "validate service - test that image can be set"
          (scheduler/validate-service
            (kubernetes-scheduler (assoc base-config
                                    :service-id->service-description-fn
                                    (constantly {"image" "twosigma/waiter-test-apps"}))) nil))))))

(deftest test-start-k8s-watch!
  (let [service-id "test-app-1234"
        rs-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/service-id "test-app-1234"}}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app "test-app-1234"
                                                  :waiter-cluster "waiter"
                                                  :waiter/cluster "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 1
                           :availableReplicas 2}}]}

        rs-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter-cluster "waiter"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 2
                            :availableReplicas 2}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"}
                   :spec {:replicas 3
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter-cluster "waiter"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 3
                            :readyReplicas 2
                            :availableReplicas 3}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1003"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter-cluster "waiter"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 1
                            :availableReplicas 2}}}]

        pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :restartCount 0}]}}]}

        pods-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234-abcd2"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.12"
                            :startTime "2014-09-13T00:24:47Z"
                            :containerStatuses [{:name "test-app-1234"
                                                 :ready true
                                                 :restartCount 0}]}}}
         {:type "ADDED"
          :object {:metadata {:name "test-app-1234-abcd3"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.13"
                            :startTime "2014-09-13T00:24:48Z"
                            :containerStatuses [{:name "test-app-1234"
                                                 :restartCount 0}]}}}
         {:type "DELETED"
          :object {:metadata {:name "test-app-1234-abcd1"
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1003"}}}]

        watch-update-signals (vec (repeatedly (count rs-watch-updates) promise))

        make-watch-stream (fn [updates signals]
                            (assert (== (count updates) (count signals)))
                            (map (fn [data signal] @signal data)
                                 (concat updates ["hang after last update"])
                                 (concat signals [(promise)])))

        pods-watch-stream (make-watch-stream pods-watch-updates watch-update-signals)
        rs-watch-stream (make-watch-stream rs-watch-updates watch-update-signals)

        {:keys [watch-state] :as dummy-scheduler} (make-dummy-scheduler ["test-app-1234"])

        rs-watch-thread (start-replicasets-watch!
                          dummy-scheduler
                          {:api-request-fn (constantly rs-response)
                           :exit-on-error? false
                           :streaming-api-request-fn (constantly rs-watch-stream)})

        pods-watch-thread (start-pods-watch!
                            dummy-scheduler
                            {:api-request-fn (constantly pods-response)
                             :exit-on-error? false
                             :streaming-api-request-fn (constantly pods-watch-stream)})

        get-instance (fn [{:keys [watch-state]} index]
                       (let [pod-id (str "test-app-1234-abcd" index)
                             pod (get-in @watch-state [:service-id->pod-id->pod service-id pod-id])]
                         (when pod
                           (pod->ServiceInstance pod))))
        wait-for-version (fn [version-tag value]
                           (ct/wait-for
                             #(= value
                                 (get-in @watch-state [:rs-metadata :version version-tag])
                                 (get-in @watch-state [:pods-metadata :version version-tag]))
                             :interval 500 :timeout 5000 :unit-multiplier 1))]

    ;; Verify base state
    (is (wait-for-version :snapshot 1000))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (some? inst2))
      (is (not (:healthy? inst2)))
      (is (nil? inst3)))

    ;; Verify state after update 1:
    ;; instance 2 should now be healthy
    (deliver (get watch-update-signals 0) true)
    (is (wait-for-version :watch 1001))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 2))
      (is (== staged unhealthy 0))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (nil? inst3)))

    ;; Verify state after update 2:
    ;; instance 3 should now be available
    (deliver (get watch-update-signals 1) true)
    (is (wait-for-version :watch 1002))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 3))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Verify state after update 3:
    ;; instance 1 should now be gone
    (deliver (get watch-update-signals 2) true)
    (is (wait-for-version :watch 1003))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (nil? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Kill the watch threads
    (.stop rs-watch-thread)
    (.stop pods-watch-thread)))

(deftest test-start-k8s-watch-with-retries
  (let [service-id "test-app-1234"

        rs-response
        {:kind "ReplicaSetList"
         :apiVersion "extensions/v1beta1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/service-id "test-app-1234"}}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app "test-app-1234"
                                                  :waiter-cluster "waiter"
                                                  :waiter/cluster "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 1
                           :availableReplicas 2}}]}

        rs-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter-cluster "waiter"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 2
                            :availableReplicas 2}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"}
                   :spec {:replicas 3
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter-cluster "waiter"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 3
                            :readyReplicas 2
                            :availableReplicas 3}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1004"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter-cluster "waiter"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 1
                            :availableReplicas 2}}}]

        pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :restartCount 0}]}}]}

        pods-response'
        {:kind "PodList"
         :apiVersion "v1"
         :metadata {:resourceVersion "1003"}
         :items [{:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd3"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter-cluster "waiter"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :resourceVersion "1002"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.13"
                           :startTime "2014-09-13T00:24:48Z"
                           :containerStatuses [{:name "test-app-1234"
                                                :restartCount 0}]}}]}

        pods-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234-abcd2"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.12"
                            :startTime "2014-09-13T00:24:47Z"
                            :containerStatuses [{:name "test-app-1234"
                                                 :ready true
                                                 :restartCount 0}]}}}
         {:type "ADDED"
          :object {:metadata {:name "test-app-1234-abcd3"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter-cluster "waiter"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.13"
                            :startTime "2014-09-13T00:24:48Z"
                            :containerStatuses [{:name "test-app-1234"
                                                 :restartCount 0}]}}}
         {:type "DELETED"
          :object {:metadata {:name "test-app-1234-abcd1"
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1004"}}}]

        watch-update-signals (vec (repeatedly (count rs-watch-updates) promise))

        make-watch-stream (fn [updates signals]
                            (assert (== (count updates) (count signals)))
                            (map (fn [data signal] @signal data)
                                 (concat updates ["hang after last update"])
                                 (concat signals [(promise)])))

        {:keys [watch-state] :as dummy-scheduler} (make-dummy-scheduler ["test-app-1234"])

        ;; replicasets have a single uninterrupted stream of updates
        rs-watch-stream (make-watch-stream rs-watch-updates watch-update-signals)

        rs-watch-thread (start-replicasets-watch!
                          dummy-scheduler
                          {:api-request-fn (constantly rs-response)
                           :exit-on-error? false
                           :streaming-api-request-fn (constantly rs-watch-stream)})

        ;; pods global queries are called twice, and return a different value each time
        pods-global-update-signal (promise)
        pods-global-query-results (.iterator
                                    (cons pods-response
                                          (lazy-seq
                                            (list (do @pods-global-update-signal
                                                      pods-response')))))
        pods-global-query-fn (fn pods-global-query-fn [& _]
                               (.next pods-global-query-results))

        ;; pods watch streams get interrupted after every update,
        ;; which lets us test the watch-retries behavior here
        pods-watch-query-count (atom 0)
        pods-watch-stream (make-watch-stream pods-watch-updates watch-update-signals)
        pods-watch-query-fn (fn pods-watch-query-fn [_ watch-url]
                              (swap! pods-watch-query-count inc)
                              (let [last-resource-version (->> watch-url
                                                               (re-find #"(?<=[&?]resourceVersion=)\d+")
                                                               Long/parseLong)]
                                (->> pods-watch-stream
                                     (drop-while #(-> (get-in % [:object :metadata :resourceVersion])
                                                      Long/parseLong
                                                      (<= last-resource-version)))
                                     (take 1))))

        pods-watch-thread (start-pods-watch!
                            dummy-scheduler
                            {:api-request-fn pods-global-query-fn
                             :exit-on-error? false
                             :streaming-api-request-fn pods-watch-query-fn
                             :watch-retries 1})

        get-instance (fn [{:keys [watch-state]} index]
                       (let [pod-id (str "test-app-1234-abcd" index)
                             pod (get-in @watch-state [:service-id->pod-id->pod service-id pod-id])]
                         (when pod
                           (pod->ServiceInstance pod))))
        wait-for-version (fn [resource version-tag value]
                           (ct/wait-for
                             #(= value
                                 (get-in @watch-state [resource :version version-tag]))
                             :interval 500 :timeout 5000 :unit-multiplier 1))]

    ;; Verify base state
    (is (wait-for-version :rs-metadata :snapshot 1000))
    (is (wait-for-version :pods-metadata :snapshot 1000))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (some? inst2))
      (is (not (:healthy? inst2)))
      (is (nil? inst3)))

    ;; Verify state after update 1:
    ;; instance 2 should now be healthy
    (deliver (get watch-update-signals 0) true)
    (is (wait-for-version :rs-metadata :watch 1001))
    (is (wait-for-version :pods-metadata :watch 1001))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 2))
      (is (== staged unhealthy 0))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (nil? inst3)))

    ;; Verify state after update 2:
    ;; instance 3 should now be available
    (deliver (get watch-update-signals 1) true)
    (is (wait-for-version :rs-metadata :watch 1002))
    (is (wait-for-version :pods-metadata :watch 1002))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 3))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Verify state after second global pod query (same as update 2 state)
    (deliver pods-global-update-signal true)
    (is (wait-for-version :pods-metadata :snapshot 1003))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 3))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Verify state after update 3:
    ;; instance 1 should now be gone
    (deliver (get watch-update-signals 2) true)
    (is (wait-for-version :rs-metadata :watch 1004))
    (is (wait-for-version :pods-metadata :watch 1004))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (nil? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    (is (== @pods-watch-query-count 4))

    ;; Kill the watch threads
    (.stop rs-watch-thread)
    (.stop pods-watch-thread)))


(deftest test-compute-image
  (let [image-aliases {"to-resolve" "resolved"}]
    (testing "no aliases"
      (is (= "an/image" (compute-image "an/image" nil nil)))
      (is (= "an/image" (compute-image nil "an/image" nil))))
    (testing "aliases, alias found"
      (is (= "resolved" (compute-image "to-resolve" nil image-aliases)))
      (is (= "resolved" (compute-image nil "to-resolve" image-aliases))))
    (testing "aliases, alias not found"
      (is (= "an/image" (compute-image "an/image" nil image-aliases)))
      (is (= "an/image" (compute-image nil "an/image" image-aliases))))))
