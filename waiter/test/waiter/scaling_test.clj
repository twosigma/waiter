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
(ns waiter.scaling-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [full.async :refer (<?? <? go-try)]
            [waiter.correlation-id :as cid]
            [waiter.mocks :refer :all]
            [waiter.scaling :refer :all]
            [waiter.scheduler :as scheduler]
            [waiter.test-helpers :refer :all])
  (:import (java.util.concurrent CountDownLatch Executors)))

(defn- retrieve-state-fn
  "Helper function to query for state on the query-chan"
  ([query-chan]
   (retrieve-state-fn query-chan {}))
  ([query-chan message-map]
   (let [response-chan (async/promise-chan)]
     (async/>!! query-chan (assoc message-map :response-chan response-chan))
     (async/<!! response-chan))))

(deftest test-service-scaling-multiplexer
  (let [scaling-executor-factory (fn [_] {:executor-chan (async/chan 100)})]
    (testing "create-new-service"
      (let [service-id "test-service-id"
            {:keys [executor-multiplexer-chan query-chan]} (service-scaling-multiplexer scaling-executor-factory {})]
        (async/>!! executor-multiplexer-chan {:service-id service-id :scale-amount 1})
        (let [multiplexer-state (retrieve-state-fn query-chan)
              executor-chan (get-in multiplexer-state [service-id :executor-chan])]
          (is (contains? multiplexer-state service-id))
          (is executor-chan)
          (async/>!! executor-chan :test-data)
          (is (= {:service-id service-id :scale-amount 1} (async/<!! executor-chan)))
          (is (= :test-data (async/<!! executor-chan))))))

    (testing "send-to-existing-service"
      (let [service-id "test-service-id"
            service-executor-chan (async/chan 10)
            {:keys [executor-multiplexer-chan query-chan]}
            (service-scaling-multiplexer scaling-executor-factory {service-id {:executor-chan service-executor-chan}})]
        (async/>!! executor-multiplexer-chan {:service-id service-id :scale-amount 1})
        (let [multiplexer-state (retrieve-state-fn query-chan)
              executor-chan (get-in multiplexer-state [service-id :executor-chan])]
          (is (contains? multiplexer-state service-id))
          (is (= service-executor-chan executor-chan))
          (async/>!! executor-chan :test-data)
          (is (= {:service-id service-id :scale-amount 1} (async/<!! executor-chan)))
          (is (= :test-data (async/<!! executor-chan))))))

    (testing "remove-existing-service"
      (let [service-id "test-service-id"
            service-executor-chan (async/chan 10)
            {:keys [executor-multiplexer-chan query-chan]}
            (service-scaling-multiplexer scaling-executor-factory {service-id {:executor-chan service-executor-chan}})]
        (async/>!! executor-multiplexer-chan {:service-id service-id})
        (let [multiplexer-state (retrieve-state-fn query-chan)]
          (is (not (contains? multiplexer-state service-id)))
          (async/>!! service-executor-chan :test-data)
          (is (nil? (async/<!! service-executor-chan))))))

    (testing "create-another-service"
      (let [service-id-1 "test-service-id-1"
            service-id-2 "test-service-id-2"
            service-executor-chan (async/chan 10)
            {:keys [executor-multiplexer-chan query-chan]}
            (service-scaling-multiplexer scaling-executor-factory {service-id-1 {:executor-chan service-executor-chan}})]
        (async/>!! executor-multiplexer-chan {:service-id service-id-2 :scale-amount 1})
        (let [multiplexer-state (retrieve-state-fn query-chan)
              executor-chan (get-in multiplexer-state [service-id-2 :executor-chan])]
          (is (contains? multiplexer-state service-id-2))
          (is executor-chan)
          (async/>!! service-executor-chan :test-data-1)
          (async/>!! executor-chan :test-data-2)
          (is (= {:service-id service-id-2 :scale-amount 1} (async/<!! executor-chan)))
          (is (= :test-data-2 (async/<!! executor-chan)))
          (is (= :test-data-1 (async/<!! service-executor-chan))))))

    (testing "query-state"
      (let [service-id-1 "test-service-id-1"
            service-id-2 "test-service-id-2"
            service-query-chan (async/chan 10)
            initial-state {service-id-1 {:query-chan service-query-chan}}
            {:keys [query-chan]}
            (service-scaling-multiplexer scaling-executor-factory initial-state)]
        (async/go
          (let [{:keys [response-chan]} (async/<! service-query-chan)]
            (async/>! response-chan (str "data-for-" service-id-1))))
        (is (= initial-state (retrieve-state-fn query-chan {})))
        (is (= (str "data-for-" service-id-1) (retrieve-state-fn query-chan {:service-id service-id-1})))
        (is (= :no-data-available (retrieve-state-fn query-chan {:service-id service-id-2})))))))

(deftest test-kill-instance-handler
  (let [current-time (t/now)]
    (with-redefs [t/now (fn [] current-time)]
      (let [test-service-id "test-service-id"
            src-router-id "src-router-id"
            inter-kill-request-wait-time-ms 10
            timeout-config {:blacklist-backoff-base-time-ms 10000
                            :inter-kill-request-wait-time-ms inter-kill-request-wait-time-ms
                            :max-blacklist-time-ms 60000}
            scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
            make-scheduler (fn [operation-tracker-atom]
                             (reify scheduler/ServiceScheduler
                               (scale-service [_ service-id scale-to-instances force]
                                 (swap! operation-tracker-atom conj [:scale-service service-id scale-to-instances])
                                 (is (= test-service-id service-id))
                                 (is (false? force))
                                 (when (neg? scale-to-instances)
                                   (throw (Exception. "throwing exception as required by test")))
                                 (is (pos? scale-to-instances))
                                 true)
                               (kill-instance [_ {:keys [id message service-id success-flag]}]
                                 (swap! operation-tracker-atom conj [:kill-instance id service-id success-flag])
                                 (is id)
                                 (is (= test-service-id service-id))
                                 {:instance-id id, :killed? success-flag, :message message, :service-id service-id,
                                  :status (if success-flag 200 404)})))
            peers-acknowledged-blacklist-requests-fn (fn [{:keys [service-id]} short-circuit? blacklist-period-ms reason]
                                                       (if (= (:blacklist-backoff-base-time-ms timeout-config) blacklist-period-ms)
                                                         (do
                                                           (is short-circuit?)
                                                           (is (= :prepare-to-kill reason)))
                                                         (do
                                                           (is (not short-circuit?))
                                                           (is (= :killed reason))))
                                                       (is (= test-service-id service-id))
                                                       true)]
        (testing "successfully-kill-instance"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)]
            (let [instance-1 {:id "instance-1", :message "Killed", :service-id test-service-id, :success-flag true}]
              (mock-reservation-system
                instance-rpc-chan
                [(fn [[{:keys [reason]} response-chan]]
                   (is (= :kill-instance reason))
                   (async/>!! response-chan instance-1))
                 (fn [[instance result]]
                   (is (= instance-1 instance))
                   (is (= :killed (:status result))))])
              (let [killed-instance-promise (promise)
                    notify-instance-killed-fn (fn [instance] (deliver killed-instance-promise instance))
                    correlation-id (str "test-cid-" (rand-int 10000))
                    response-chan
                    (cid/with-correlation-id
                      correlation-id
                      (kill-instance-handler
                        notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn
                        scheduler populate-maintainer-chan! timeout-config scheduler-interactions-thread-pool
                        {:basic-authentication {:src-router-id src-router-id} :route-params {:service-id test-service-id}}))
                    {:keys [body headers status]} (async/<!! response-chan)]
                (is (= 200 status))
                (is (= (assoc expected-json-response-headers "x-cid" correlation-id) headers))
                (is (= {:kill-response {:instance-id "instance-1", :killed? true, :message "Killed", :service-id test-service-id, :status 200},
                        :service-id test-service-id, :source-router-id src-router-id, :success true}
                       (walk/keywordize-keys (json/read-str body))))
                (is (= [[:kill-instance "instance-1" "test-service-id" true]] @scheduler-operation-tracker-atom))
                (is (= instance-1 (deref killed-instance-promise 10 :not-killed)))))))

        (testing "no-instance-to-kill-no-message-404"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)]
            (mock-reservation-system
              instance-rpc-chan
              [(fn [[{:keys [reason]} response-chan]]
                 (is (= :kill-instance reason))
                 (async/>!! response-chan :instance-unavailable))])
            (let [killed-instance-promise (promise)
                  notify-instance-killed-fn (fn [instance] (deliver killed-instance-promise instance))
                  correlation-id (str "test-cid-" (rand-int 10000))
                  response-chan
                  (cid/with-correlation-id
                    correlation-id
                    (kill-instance-handler
                      notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn
                      scheduler populate-maintainer-chan! timeout-config scheduler-interactions-thread-pool
                      {:basic-authentication {:src-router-id src-router-id} :route-params {:service-id test-service-id}}))
                  {:keys [body headers status]} (async/<!! response-chan)]
              (is (= 404 status))
              (is (= (assoc expected-json-response-headers "x-cid" correlation-id) headers))
              (is (= {:kill-response {:message "no-instance-killed", :status 404}, :service-id test-service-id,
                      :source-router-id src-router-id, :success false}
                     (walk/keywordize-keys (json/read-str body))))
              (is (empty? @scheduler-operation-tracker-atom))
              (is (= :not-killed (deref killed-instance-promise 100 :not-killed))))))

        (testing "no-instance-to-kill-with-message-409"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)]
            (let [instance-1 {:id "instance-1", :message "Failure message", :service-id test-service-id, :success-flag false}]
              (mock-reservation-system
                instance-rpc-chan
                [(fn [[{:keys [reason]} response-chan]]
                   (is (= :kill-instance reason))
                   (async/>!! response-chan instance-1))
                 (fn [[instance result]]
                   (is (= instance-1 instance))
                   (is (= :not-killed (:status result))))]))
            (let [killed-instance-promise (promise)
                  notify-instance-killed-fn (fn [instance] (deliver killed-instance-promise instance))
                  correlation-id (str "test-cid-" (rand-int 10000))
                  response-chan
                  (cid/with-correlation-id
                    correlation-id
                    (kill-instance-handler
                      notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn
                      scheduler populate-maintainer-chan! timeout-config scheduler-interactions-thread-pool
                      {:basic-authentication {:src-router-id src-router-id} :route-params {:service-id test-service-id}}))
                  {:keys [body headers status]} (async/<!! response-chan)]
              (is (= 404 status))
              (is (= (assoc expected-json-response-headers "x-cid" correlation-id) headers))
              (is (= {:kill-response {:instance-id "instance-1", :killed? false, :message "Failure message",
                                      :service-id test-service-id, :status 404},
                      :service-id test-service-id, :source-router-id src-router-id, :success false}
                     (walk/keywordize-keys (json/read-str body))))
              (is (= [[:kill-instance "instance-1" "test-service-id" false]] @scheduler-operation-tracker-atom))
              (is (= :not-killed (deref killed-instance-promise 100 :not-killed))))))

        (.shutdown scheduler-interactions-thread-pool)))))

(deftest test-compute-scale-amount-restricted-by-quanta
  (is (= 1 (compute-scale-amount-restricted-by-quanta {"cpus" 10 "mem" 1024} {:cpus 32 :mem 4608} 1)))
  (is (= 3 (compute-scale-amount-restricted-by-quanta {"cpus" 10 "mem" 1024} {:cpus 32 :mem 4608} 3)))
  (is (= 4 (compute-scale-amount-restricted-by-quanta {"cpus" 5 "mem" 1024} {:cpus 32 :mem 4608} 8)))
  (is (= 3 (compute-scale-amount-restricted-by-quanta {"cpus" 10 "mem" 1024} {:cpus 32 :mem 4608} 8)))
  (is (= 2 (compute-scale-amount-restricted-by-quanta {"cpus" 10 "mem" 2048} {:cpus 32 :mem 4608} 8)))
  (is (= 9 (compute-scale-amount-restricted-by-quanta {"cpus" 2 "mem" 512} {:cpus 32 :mem 4608} 10)))
  (is (= 1 (compute-scale-amount-restricted-by-quanta {"cpus" 64 "mem" 512} {:cpus 32 :mem 4608} 10))))

(deftest test-service-scaling-executor
  (let [current-time (t/now)]
    (with-redefs [t/now (fn [] current-time)]
      (let [test-service-id "test-service-id"
            inter-kill-request-wait-time-ms 10
            timeout-config {:blacklist-backoff-base-time-ms 10000
                            :inter-kill-request-wait-time-ms inter-kill-request-wait-time-ms
                            :max-blacklist-time-ms 60000}
            make-scheduler (fn [operation-tracker-atom]
                             (reify scheduler/ServiceScheduler
                               (scale-service [_ service-id scale-to-instances force]
                                 (swap! operation-tracker-atom conj [:scale-service service-id scale-to-instances force])
                                 (is (= test-service-id service-id))
                                 (when (neg? scale-to-instances)
                                   (throw (Exception. "throwing exception as required by test")))
                                 (is (pos? scale-to-instances))
                                 true)
                               (kill-instance [_ {:keys [id service-id success-flag]}]
                                 (swap! operation-tracker-atom conj [:kill-instance id service-id success-flag])
                                 (is id)
                                 (is (= test-service-id service-id))
                                 {:instance-id id, :killed? success-flag, :service-id service-id})))
            peers-acknowledged-blacklist-requests-fn (fn [{:keys [service-id]} short-circuit? blacklist-period-ms reason]
                                                       (if (= (:blacklist-backoff-base-time-ms timeout-config) blacklist-period-ms)
                                                         (do
                                                           (is short-circuit?)
                                                           (is (= :prepare-to-kill reason)))
                                                         (do
                                                           (is (not short-circuit?))
                                                           (is (= :killed reason))))
                                                       (is (= test-service-id service-id))
                                                       true)
            delegate-instance-kill-request-fn (fn [service-id]
                                                (is (= test-service-id service-id))
                                                false)
            notify-instance-killed-fn (fn [instance] (throw (ex-info "Unexpected call" {:instance instance})))
            service-id->service-description-fn (constantly {"cpus" 10 "mem" 1024})
            quanta-constraints {:cpus 64 :mem 100000}
            equilibrium-state {}
            make-scaling-message (fn [service-id scale-amount scale-to-instances task-count total-instances response-chan]
                                   {:correlation-id (first *testing-contexts*)
                                    :service-id service-id, :scale-amount scale-amount, :scale-to-instances scale-to-instances,
                                    :task-count task-count, :total-instances total-instances, :response-chan response-chan})
            run-service-scaling-executor (fn [scheduler populate-maintainer-chan! scale-service-thread-pool &
                                              {:keys [delegate-instance-kill-request-fn
                                                      notify-instance-killed-fn
                                                      peers-acknowledged-blacklist-requests-fn]
                                               :or {delegate-instance-kill-request-fn delegate-instance-kill-request-fn
                                                    notify-instance-killed-fn notify-instance-killed-fn
                                                    peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn}}]
                                           (service-scaling-executor
                                             notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn delegate-instance-kill-request-fn
                                             service-id->service-description-fn scheduler populate-maintainer-chan! quanta-constraints
                                             timeout-config scale-service-thread-pool test-service-id))]
        (testing "basic-equilibrium-with-no-scaling"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                peers-acknowledged-blacklist-requests-fn (fn [_ _ _ _] (throw (Exception. "unexpected call")))
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)]
            (mock-reservation-system instance-rpc-chan [])
            (async/>!! executor-chan {:correlation-id (first *testing-contexts*) :service-id test-service-id, :scale-amount 0})
            (is (= equilibrium-state (retrieve-state-fn query-chan)))
            (async/>!! exit-chan :exit)
            (.shutdown scale-service-thread-pool)))

        (testing "scale-up:pending"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                peers-acknowledged-blacklist-requests-fn (fn [_ _ _ _] (throw (Exception. "unexpected call")))
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)]
            (mock-reservation-system instance-rpc-chan [])
            (async/>!! executor-chan (make-scaling-message test-service-id 10 30 25 30 nil))
            (is (= equilibrium-state (retrieve-state-fn query-chan)))
            (is (empty? @scheduler-operation-tracker-atom))
            (async/>!! exit-chan :exit)
            (.shutdown scale-service-thread-pool)))

        (testing "scale-up:trigger:above-quanta"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                peers-acknowledged-blacklist-requests-fn (fn [_ _ _ _] (throw (Exception. "unexpected call")))
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)]
            (mock-reservation-system instance-rpc-chan [])
            (async/>!! executor-chan (make-scaling-message test-service-id 10 30 25 20 nil))
            (is (= equilibrium-state (retrieve-state-fn query-chan)))
            (is (= [[:scale-service "test-service-id" 26 false]] @scheduler-operation-tracker-atom))
            (async/>!! exit-chan :exit)
            (.shutdown scale-service-thread-pool)))

        (testing "scale-up:trigger:below-quanta"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                peers-acknowledged-blacklist-requests-fn (fn [_ _ _ _] (throw (Exception. "unexpected call")))
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)]
            (mock-reservation-system instance-rpc-chan [])
            (async/>!! executor-chan (make-scaling-message test-service-id 4 24 22 20 nil))
            (is (= equilibrium-state (retrieve-state-fn query-chan)))
            (is (= [[:scale-service "test-service-id" 24 false]] @scheduler-operation-tracker-atom))
            (async/>!! exit-chan :exit)
            (.shutdown scale-service-thread-pool)))

        (testing "scale-force:trigger"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                peers-acknowledged-blacklist-requests-fn (fn [_ _ _ _] (throw (Exception. "unexpected call")))
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)]
            (mock-reservation-system instance-rpc-chan [])
            (async/>!! executor-chan (make-scaling-message test-service-id -5 25 20 20 nil))
            (is (= equilibrium-state (retrieve-state-fn query-chan)))
            (is (= [[:scale-service "test-service-id" 25 true]] @scheduler-operation-tracker-atom))
            (async/>!! exit-chan :exit)
            (.shutdown scale-service-thread-pool)))

        (testing "scale-down:no-instance-globally"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                response-chan (async/promise-chan)
                latch (CountDownLatch. 1)
                delegate-instance-kill-request-fn (fn [service-id]
                                                    (is (= test-service-id service-id))
                                                    (.countDown latch)
                                                    false)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :delegate-instance-kill-request-fn delegate-instance-kill-request-fn)]
            (mock-reservation-system
              instance-rpc-chan
              [(fn [[{:keys [reason]} response-chan]]
                 (is (= :kill-instance reason))
                 (async/>!! response-chan :no-instance-available))])
            (async/>!! executor-chan (make-scaling-message test-service-id -1 30 31 31 response-chan))
            (.await latch)
            (is (= equilibrium-state (retrieve-state-fn query-chan)))
            (async/>!! response-chan :nothing-killed-locally)
            (is (= :nothing-killed-locally (async/<!! response-chan)))
            (is (empty? @scheduler-operation-tracker-atom))
            (async/>!! exit-chan :exit)
            (.shutdown scale-service-thread-pool)))

        (testing "scale-down:delegated-instance-kill"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                response-chan (async/promise-chan)
                latch (CountDownLatch. 1)
                delegate-instance-kill-request-fn (fn [service-id]
                                                    (is (= test-service-id service-id))
                                                    (.countDown latch)
                                                    true)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :delegate-instance-kill-request-fn delegate-instance-kill-request-fn)]
            (mock-reservation-system
              instance-rpc-chan
              [(fn [[{:keys [reason]} response-chan]]
                 (is (= :kill-instance reason))
                 (async/>!! response-chan :no-instance-available))])
            (async/>!! executor-chan (make-scaling-message test-service-id -1 30 31 31 response-chan))
            (.await latch)
            (is (= (assoc equilibrium-state :last-scale-down-time current-time)
                   (retrieve-state-fn query-chan)))
            (async/>!! response-chan :nothing-killed-locally)
            (is (= :nothing-killed-locally (async/<!! response-chan)))
            (is (empty? @scheduler-operation-tracker-atom))
            (async/>!! exit-chan :exit)
            (.shutdown scale-service-thread-pool)))

        (testing "scale-down:one-instance"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                notify-instance-killed-fn (fn [{:keys [id service-id]}]
                                            (is (= "instance-1" id))
                                            (is (= test-service-id service-id)))
                response-chan (async/promise-chan)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :notify-instance-killed-fn notify-instance-killed-fn)]
            (let [instance-1 {:id "instance-1", :service-id test-service-id, :success-flag true}]
              (mock-reservation-system
                instance-rpc-chan
                [(fn [[{:keys [reason]} response-chan]]
                   (is (= :kill-instance reason))
                   (async/>!! response-chan instance-1))
                 (fn [[instance result]]
                   (is (= instance-1 instance))
                   (is (= :killed (:status result))))])
              (async/>!! executor-chan (make-scaling-message test-service-id -1 30 31 31 response-chan))
              (is (= {:instance-id (:id instance-1), :killed? true, :service-id test-service-id}
                     (async/<!! response-chan)))
              (is (= (assoc equilibrium-state :last-scale-down-time current-time)
                     (retrieve-state-fn query-chan)))
              (is (= [[:kill-instance "instance-1" "test-service-id" true]]
                     @scheduler-operation-tracker-atom))
              (async/>!! exit-chan :exit)
              (.shutdown scale-service-thread-pool))))

        (testing "scale-down:kill-vetoed-then-no-instance"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                response-chan (async/promise-chan)
                peers-acknowledged-blacklist-requests-fn
                (fn [{:keys [id]} short-circuit? blacklist-period-ms reason]
                  (is (= "instance-1" id))
                  (is short-circuit?)
                  (is (= (:blacklist-backoff-base-time-ms timeout-config) blacklist-period-ms))
                  (is (= :prepare-to-kill reason))
                  false)
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)
                latch (CountDownLatch. 1)]
            (let [instance-1 {:id "instance-1", :service-id test-service-id, :success-flag true}]
              (mock-reservation-system
                instance-rpc-chan
                [(fn [[{:keys [reason]} response-chan]]
                   (is (= :kill-instance reason))
                   (async/>!! response-chan instance-1))
                 (fn [[instance result]]
                   (is (= instance-1 instance))
                   (is (= :not-killed (:status result))))
                 (fn [[{:keys [reason]} response-chan exclude-ids-set]]
                   (is (= :kill-instance reason))
                   (is (= #{"instance-1"} exclude-ids-set))
                   (.countDown latch)
                   (async/>!! response-chan :no-matching-instance-found))])
              (async/>!! executor-chan (make-scaling-message test-service-id -2 30 32 32 response-chan))
              (.await latch)
              (is (= equilibrium-state (retrieve-state-fn query-chan)))
              (async/>!! response-chan :nothing-killed-locally)
              (is (= :nothing-killed-locally (async/<!! response-chan)))
              (is (empty? @scheduler-operation-tracker-atom))
              (async/>!! exit-chan :exit)
              (.shutdown scale-service-thread-pool))))

        (testing "scale-down:kill-vetoed-first-then-kill-next-instance"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                response-chan (async/promise-chan)
                notify-instance-killed-fn (fn [{:keys [id service-id]}]
                                            (is (= "instance-2" id))
                                            (is (= test-service-id service-id)))
                peers-acknowledged-blacklist-requests-fn (fn [{:keys [id]} _ _ _] (not= "instance-1" id))
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :notify-instance-killed-fn notify-instance-killed-fn
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)
                latch (CountDownLatch. 1)]
            (let [instance-1 {:id "instance-1", :service-id test-service-id, :success-flag true}
                  instance-2 {:id "instance-2", :service-id test-service-id, :success-flag true}]
              (mock-reservation-system
                instance-rpc-chan
                [(fn [[{:keys [reason]} response-chan]]
                   (is (= :kill-instance reason))
                   (async/>!! response-chan instance-1))
                 (fn [[instance result]]
                   (is (= instance-1 instance))
                   (is (= :not-killed (:status result))))
                 (fn [[{:keys [reason]} response-chan]]
                   (is (= :kill-instance reason))
                   (async/>!! response-chan instance-2))
                 (fn [[instance result]]
                   (is (= instance-2 instance))
                   (is (= :killed (:status result)))
                   (.countDown latch))])
              (async/>!! executor-chan (make-scaling-message test-service-id -2 30 32 32 response-chan))
              (.await latch)
              (is (= (assoc equilibrium-state :last-scale-down-time current-time)
                     (retrieve-state-fn query-chan)))
              (is (= {:instance-id (:id instance-2), :killed? true, :service-id test-service-id}
                     (async/<!! response-chan)))
              (is (= [[:kill-instance "instance-2" "test-service-id" true]]
                     @scheduler-operation-tracker-atom))
              (async/>!! exit-chan :exit)
              (.shutdown scale-service-thread-pool))))

        (testing "scale-down:one-veto-and-one-failure"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                response-chan (async/promise-chan)
                peers-acknowledged-blacklist-requests-fn (fn [{:keys [id]} _ _ _] (not= "instance-1" id))
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :peers-acknowledged-blacklist-requests-fn peers-acknowledged-blacklist-requests-fn)
                latch (CountDownLatch. 1)]
            (let [instance-1 {:id "instance-1", :service-id test-service-id, :success-flag true}
                  instance-2 {:id "instance-2", :service-id test-service-id, :success-flag false}]
              (mock-reservation-system
                instance-rpc-chan
                [(fn [[{:keys [reason]} response-chan exclude-ids-set]]
                   (is (= :kill-instance reason))
                   (is (= #{} exclude-ids-set))
                   (async/>!! response-chan instance-1))
                 (fn [[instance result]]
                   (is (= instance-1 instance))
                   (is (= :not-killed (:status result))))
                 (fn [[{:keys [reason]} response-chan exclude-ids-set]]
                   (is (= :kill-instance reason))
                   (is (= #{"instance-1"} exclude-ids-set))
                   (async/>!! response-chan instance-2))
                 (fn [[instance result]]
                   (is (= instance-2 instance))
                   (is (= :not-killed (:status result)))
                   (.countDown latch))])
              (async/>!! executor-chan (make-scaling-message test-service-id -3 30 33 33 response-chan))
              (.await latch)
              (is (= {:instance-id (:id instance-2), :killed? false, :service-id test-service-id}
                     (async/<!! response-chan)))
              (is (= equilibrium-state (retrieve-state-fn query-chan)))
              (is (= [[:kill-instance "instance-2" "test-service-id" false]]
                     @scheduler-operation-tracker-atom))
              (async/>!! exit-chan :exit)
              (.shutdown scale-service-thread-pool))))

        (testing "scale-down:two-instances"
          (let [instance-rpc-chan (async/chan 1)
                populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
                scheduler-operation-tracker-atom (atom [])
                scheduler (make-scheduler scheduler-operation-tracker-atom)
                scale-service-thread-pool (Executors/newFixedThreadPool 2)
                response-chan (async/promise-chan)
                notify-instance-killed-fn (fn [{:keys [id service-id]}]
                                            (is (= "instance-1" id))
                                            (is (= test-service-id service-id)))
                {:keys [executor-chan exit-chan query-chan]}
                (run-service-scaling-executor
                  scheduler populate-maintainer-chan! scale-service-thread-pool
                  :notify-instance-killed-fn notify-instance-killed-fn)]
            (let [instance-1 {:id "instance-1", :service-id test-service-id, :success-flag true}]
              (mock-reservation-system
                instance-rpc-chan
                [(fn [[{:keys [reason]} response-chan exclude-ids-set]]
                   (is (= :kill-instance reason))
                   (is (= #{} exclude-ids-set))
                   (async/>!! response-chan instance-1))
                 (fn [[instance result]] (is (= instance-1 instance))
                   (is (= :killed (:status result))))])
              (async/>!! executor-chan (make-scaling-message test-service-id -2 30 32 32 response-chan))
              (is (= {:instance-id (:id instance-1), :killed? true, :service-id test-service-id}
                     (async/<!! response-chan)))
              (is (= (assoc equilibrium-state :last-scale-down-time current-time)
                     (retrieve-state-fn query-chan)))
              (is (= [[:kill-instance "instance-1" "test-service-id" true]]
                     @scheduler-operation-tracker-atom))
              (async/>!! exit-chan :exit)
              (.shutdown scale-service-thread-pool))))))))

(deftest test-apply-scaling
  (let [executor-multiplexer-chan (async/chan 10)]
    (apply-scaling! executor-multiplexer-chan "test-service-id"
                    {:total-instances 10
                     :task-count 10
                     :scale-to-instances 12
                     :scale-amount 2
                     :outstanding-requests 12})
    (async/>!! executor-multiplexer-chan :test-data)
    (let [channel-data (async/<!! executor-multiplexer-chan)]
      (is (every? #(contains? channel-data %)
                  [:correlation-id :scale-amount :scale-to-instances :service-id :task-count :total-instances])))
    (is (= :test-data (async/<!! executor-multiplexer-chan)))))

(deftest scale-services-test
  (let [config {"min-instances" 1
                "max-instances" 10}
        service-id->scaling-states-atom (atom {})
        update-service-scale-state! (fn [service-id scaling-state]
                                      (swap! service-id->scaling-states-atom
                                             update service-id (fnil conj []) scaling-state))
        ; assert that we are applying scaling
        apply-scaling (fn [service-id {:keys [scale-to-instances scale-amount]}]
                        (case service-id
                          ; outstanding requests
                          "app1" (do
                                   (is (= 5 scale-amount))
                                   (is (= 10 scale-to-instances))
                                   10)
                          ; min instances is 5
                          "app3" (do
                                   (is (= 5 scale-amount))
                                   (is (= 5 scale-to-instances))
                                   5)
                          ; max instances is 10
                          "app4" (do
                                   (is (= -5 scale-amount))
                                   (is (= 10 scale-to-instances))
                                   10)
                          ; max instances is 20
                          "app5" (do
                                   (is (= 7 scale-amount))
                                   (is (= 21 scale-to-instances))
                                   18)))
        ; simple scaling function that targets outstanding-requests
        test-scale-service (fn [{:strs [min-instances max-instances]}
                                {:keys [expired-instances total-instances outstanding-requests]}]
                             (let [target-instances (max min-instances (min max-instances outstanding-requests))
                                   scale-to-instances (+ target-instances expired-instances)]
                               {:scale-to-instances scale-to-instances
                                :target-instances target-instances
                                :scale-amount (- scale-to-instances total-instances)}))
        max-expired-unhealthy-instances-to-consider 2
        result (scale-services
                 ;; service-ids
                 ["app1" "app2" "app3" "app4" "app5" "app7"]
                 ;; service-id->service-description
                 {"app1" (merge config {})
                  "app2" (merge config {})
                  "app3" (merge config {"min-instances" 5})
                  "app4" (merge config {"max-instances" 10})
                  "app5" (merge config {"max-instances" 20}) ;; scale past max instances to replace expired
                  "app6" (merge config {})
                  "app7" (merge config {})}
                 ;; service-id->outstanding-requests
                 {"app1" 10
                  "app2" 5
                  "app3" 0
                  "app4" 15
                  "app5" 12
                  "app6" 10
                  "app7" 10}
                 ;; service-id->scale-state
                 {"app1" {:scale-amount 1 :target-instances 5}
                  "app2" {:scale-amount 0 :target-instances 5}
                  "app3" {:scale-amount 0 :target-instances 0}
                  "app4" {:scale-amount 0 :target-instances 10}
                  "app5" {:scale-amount 0 :target-instances 12}
                  "app6" {:scale-amount 0 :target-instances 10}
                  "app7" {:target-instances 10}}
                 apply-scaling
                 update-service-scale-state!
                 5
                 test-scale-service
                 ;; service-id->router-state
                 {"app1" {:healthy-instances 5 :task-count 5 :expired-healthy-instances 0 :expired-unhealthy-instances 0}
                  "app2" {:healthy-instances 5 :task-count 5 :expired-healthy-instances 0 :expired-unhealthy-instances 0}
                  "app3" {:healthy-instances 0 :task-count 0 :expired-healthy-instances 0 :expired-unhealthy-instances 0}
                  "app4" {:healthy-instances 15 :task-count 15 :expired-healthy-instances 0 :expired-unhealthy-instances 0}
                  ;; scale past the max instances of 20 to replace 9 (7 + 2) of the expired instances
                  "app5" {:healthy-instances 10 :task-count 14 :expired-healthy-instances 7 :expired-unhealthy-instances 4}
                  "app6" {:healthy-instances 5 :task-count 5 :expired-healthy-instances 0 :expired-unhealthy-instances 0}
                  "app7" {:healthy-instances 10 :task-count 10 :expired-healthy-instances 0 :expired-unhealthy-instances 0}}
                 ;; service-id->scheduler-state
                 {"app1" {:instances 5 :task-count 5}
                  "app2" {:instances 5 :task-count 5}
                  "app3" {:instances 0 :task-count 0}
                  "app4" {:instances 15 :task-count 15}
                  "app5" {:instances 14 :task-count 14}
                  "app6" {:instances 5 :task-count 5}
                  "app7" {:instances 10 :task-count 10}}
                 max-expired-unhealthy-instances-to-consider)]
    (is (= {:target-instances 10, :scale-to-instances 10, :scale-amount 5} (get result "app1")))
    (is (= {:target-instances 5, :scale-to-instances 5, :scale-amount 0} (get result "app2")))
    (is (= {:target-instances 5, :scale-to-instances 5, :scale-amount 5} (get result "app3")))
    (is (= {:target-instances 10, :scale-to-instances 10, :scale-amount -5} (get result "app4")))
    (is (= {:target-instances 12, :scale-to-instances 21, :scale-amount 7} (get result "app5")))
    (is (nil? (get result "app6")))
    (is (= {:target-instances 10, :scale-to-instances 10, :scale-amount 0} (get result "app7")))
    (is (= {"app3" [:scale-up] "app4" [:scale-down] "app5" [:scale-up] "app7" [:stable]}
           @service-id->scaling-states-atom))))

(deftest normalize-factor-test
  (is (= 0. (normalize-factor 0.5 0)))
  (is (= 0.5 (normalize-factor 0.5 1)))
  (is (= 0.75 (normalize-factor 0.5 2))))

(defn scales-like
  [expected-scale-amount expected-scale-to-instances expected-target-instances
   config total-instances outstanding-requests target-instances healthy-instances expired-instances]
  (let [epsilon 1e-2
        {:keys [scale-amount scale-to-instances target-instances]}
        (scale-service config {:total-instances total-instances
                               :outstanding-requests outstanding-requests
                               :target-instances target-instances
                               :healthy-instances healthy-instances
                               :expired-instances expired-instances})]
    (is (> epsilon (Math/abs (double (- scale-amount expected-scale-amount))))
        (str (last *testing-contexts*) ": scale-amount=" scale-amount
             " expected-scale-amount=" expected-scale-amount))
    (is (= scale-to-instances expected-scale-to-instances)
        (str (last *testing-contexts*) ": scale-to-instances=" scale-to-instances
             " expected-scale-to-instances=" expected-scale-to-instances))
    (is (> epsilon (Math/abs (double (- target-instances expected-target-instances))))
        (str (last *testing-contexts*) ": target-instances=" target-instances
             " expected-target-instances=" expected-target-instances))))

(deftest scale-service-test
  (let [jitter-threshold 0.9
        default-scaling {"concurrency-level" 1
                         "expired-instance-restart-rate" 0.1
                         "scale-factor" 1
                         "scale-up-factor" 0.5
                         "scale-down-factor" 0.5
                         "min-instances" 1
                         "max-instances" 50
                         "jitter-threshold" jitter-threshold
                         "scale-ticks" 1}
        fast-scaling (assoc default-scaling
                       "scale-up-factor" 0.999
                       "scale-down-factor" 0.999)
        epsilon 1e-2]
    (testing "scale whole way"
      (scales-like 5 10 10, fast-scaling 5 10 5 5 0))
    (testing "don't scale up if there are enough instances for outstanding requests"
      (scales-like 0 5 7, (assoc default-scaling "scale-up-factor" 0.999) 5 4 10 5 0))
    (testing "scale half way"
      (scales-like 5 15 15, default-scaling 10 20 10 10 0))
    (testing "scale three-quarters way (two ticks @ 50% each)"
      (scales-like 15 15 15, (assoc default-scaling "scale-ticks" 2) 0 20 0 0 0))
    (testing "don't scale above max"
      (scales-like 0 50 50, default-scaling 50 100 50 50 0))
    (testing "don't scale below min"
      (scales-like 0 1 1, default-scaling 1 0 1 1 0))
    (testing "scale down to max"
      (scales-like -10 50 50, default-scaling 60 100 60 60 0))
    (testing "scale up to min"
      (scales-like 1 1 1, default-scaling 0 0 0 0 0))
    (testing "prevent jitter: threshold not met scaling up"
      (scales-like 0 1 (inc (- jitter-threshold epsilon)), (assoc default-scaling "scale-up-factor" (- jitter-threshold epsilon)) 1 2 1.0 1 0))
    (testing "prevent jitter: threshold met scaling up"
      (scales-like 1 2 (inc (+ jitter-threshold epsilon)), (assoc default-scaling "scale-up-factor" (+ jitter-threshold epsilon)) 1 2 1.0 1 0))
    (testing "prevent jitter: threshold not met scaling down"
      (scales-like 0 2 (inc (* 2 epsilon)), (assoc default-scaling "scale-down-factor" (- 1 (* 2 epsilon))) 2 1 2.0 2 0))
    (testing "prevent jitter: threshold met scaling down"
      (scales-like -1 1 1, (assoc default-scaling "scale-down-factor" 1) 2 1 2.0 2 0))
    (testing "epsilon test"
      (scales-like -1 1 1, default-scaling 2 1 1.01 1 0))
    (testing "scale-factor"
      (scales-like 10 20 20, (assoc default-scaling "scale-factor" 0.5) 10 60 10 10 0))
    (testing "concurrency-level-2"
      (scales-like 10 20 20, (assoc default-scaling "concurrency-level" 2) 10 60 10 10 0))
    (testing "concurrency-level-2-fast"
      (scales-like 20 30 29.98, (assoc fast-scaling "concurrency-level" 2) 10 60 10 10 0))
    (testing "concurrency-level-5"
      (scales-like 4 6 6, (assoc default-scaling "concurrency-level" 5) 2 50 2 2 0))
    (testing "concurrency-level-5-fast"
      (scales-like 8 10 10, (assoc fast-scaling "concurrency-level" 5) 2 50 2 2 0))
    (testing "concurrency-level-and-scale-factor-fast-A"
      (scales-like 2 3 3, (assoc fast-scaling "concurrency-level" 2 "scale-factor" 0.1) 1 45 1 1 0))
    (testing "concurrency-level-and-scale-factor-fast-B"
      (scales-like 1 2 2, (assoc fast-scaling "concurrency-level" 5 "scale-factor" 0.166666666) 1 45 1 1 0))
    (testing "concurrency-level-and-scale-factor-fast-C1"
      (scales-like 1 2 2, (assoc fast-scaling "concurrency-level" 5 "scale-factor" 0.1) 1 95 1 1 0))
    (testing "concurrency-level-and-scale-factor-fast-C2"
      (scales-like 2 3 3, (assoc fast-scaling "concurrency-level" 5 "scale-factor" 0.1) 1 150 1 1 0))
    (testing "scale up with expired instance"
      (scales-like 1 2 1, default-scaling 1 0 1.2 1 1))
    (testing "scale up additional instance with queued requests and expiring"
      (scales-like 2 3 2, fast-scaling 1 2 0.9 1 1))
    (testing "scale down with expired instances"
      (scales-like -4 1 1, fast-scaling 5 0 5.3 5 5))
    (testing "scale down without expired instances"
      (scales-like -2 8 7.919, (assoc default-scaling "scale-down-factor" 0.001 "scale-up-factor" 0.1) 10 0 7.927 9 0)
      (scales-like -1 8 7.919, (assoc default-scaling "scale-down-factor" 0.001 "scale-up-factor" 0.1) 9 0 7.927 8 0))
    (testing "scale down does not affect unhealthy instance"
      (scales-like 0 2 1, default-scaling 2 0 1.3 1 1))
    (testing "scale down replacement instance for expired"
      (scales-like -1 1 1, default-scaling 2 0 1.1 2 1))
    (testing "apply expired scale factor"
      (scales-like 2 22 19.95, default-scaling 20 20 19.9 20 20)
      (scales-like 20 40 20, (assoc default-scaling "expired-instance-restart-rate" 1) 20 20 20 20 20)
      (scales-like 0 1 1, (assoc default-scaling "expired-instance-restart-rate" 0) 1 0 1 1 1))
    (testing "scale up when there is at least one expired instance"
      (scales-like 1 2 1, (assoc default-scaling "expired-instance-restart-rate" 1) 1 1 1 0 1)
      (scales-like 3 6 5, (assoc default-scaling "expired-instance-restart-rate" 1) 3 5 5 2 1))))

(let [leader?-fn (constantly true)
      instance-killer-multiplexer-fn (fn [_])
      service-id->service-description (fn [id] {:service-id id
                                                "min-instances" 1})
      timeout-interval-ms 10000
      scale-service-fn (fn [_ state]
                         (case (int (:total-instances state))
                           2
                           {:scale-to-instances 3
                            :target-instances 3
                            :scale-amount 1}
                           3
                           {:scale-to-instances 4
                            :target-instances 4
                            :scale-amount 2}
                           4
                           {:scale-to-instances 0
                            :target-instances 0
                            :scale-amount -4}))
      max-expired-unhealthy-instances-to-consider 2
      start-autoscaler-goroutine (fn start-autoscaler-goroutine [initial-state scheduler-data scheduler-interactions-thread-pool]
                                   (let [metrics-chan (async/chan 1)
                                         service-id->metrics-fn (fn service-id->metrics-fn []
                                                                  (let [[value channel] (async/alts!! [metrics-chan (async/timeout 10)])]
                                                                    (when (= metrics-chan channel) value)))
                                         state-chan (async/chan 1)
                                         state-chan-reader (async/chan 1)
                                         state-mult (async/mult state-chan)
                                         initial-timeout-chan (async/chan 1)
                                         scheduler (reify scheduler/ServiceScheduler
                                                     (get-services [_] scheduler-data)
                                                     (scale-service [_ _ _ _] {}))
                                         update-service-scale-state! (constantly true)
                                         autoscaler-chans-map
                                         (autoscaler-goroutine (assoc initial-state
                                                                 :previous-cycle-start-time (t/minus (t/now) (t/seconds 10))
                                                                 :timeout-chan initial-timeout-chan)
                                                               leader?-fn service-id->metrics-fn instance-killer-multiplexer-fn scheduler
                                                               timeout-interval-ms scale-service-fn service-id->service-description state-mult
                                                               scheduler-interactions-thread-pool max-expired-unhealthy-instances-to-consider
                                                               update-service-scale-state!)]
                                     (async/tap state-mult state-chan-reader)
                                     (merge autoscaler-chans-map
                                            {:initial-timeout-chan initial-timeout-chan
                                             :metrics-chan metrics-chan
                                             :state-chan state-chan
                                             :state-chan-reader state-chan-reader})))]

  (deftest test-autoscaler-goroutine-populate-initial-state
    (let [scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
          {:keys [exit initial-timeout-chan metrics-chan query query-service-state-fn state-chan]}
          (start-autoscaler-goroutine {} [] scheduler-interactions-thread-pool)
          _ (async/>!! state-chan {})
          service-id "service1"
          _ (async/>!! metrics-chan {service-id {"outstanding" 2}})
          _ (async/>!! initial-timeout-chan :timeout)
          query-response (async/chan)
          _ (async/>!! query {:response-chan query-response :service-id service-id})
          scaler-state (async/<!! query-response)]
      (is (= {:outstanding-requests 2} scaler-state))
      (is (= {:outstanding-requests 2} (query-service-state-fn {:service-id service-id})))
      (async/>!! exit :kill)
      (.shutdown scheduler-interactions-thread-pool)))

  (deftest test-autoscaler-goroutine-remove-deleted-services
    (let [deleted-services-atom (atom #{})
          query-state-fn (fn query-state-fn [query-chan service-id]
                           (let [query-response (async/chan)]
                             (async/>!! query-chan {:service-id service-id, :response-chan query-response})
                             (async/<!! query-response)))]
      (with-redefs [apply-scaling! (fn [_ service-id scaling-data]
                                     (is (empty? scaling-data))
                                     (swap! deleted-services-atom conj service-id))]
        (let [scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
              {:keys [exit query state-chan state-chan-reader]} (start-autoscaler-goroutine {} [] scheduler-interactions-thread-pool)
              _ (async/>!! state-chan {:service-id->expired-instances {"s1" [], "s2" [], "s3" [], "s4" [], "s5" []}
                                       :service-id->healthy-instances {"s1" [], "s2" [], "s3" [], "s4" [], "s5" []}
                                       :service-id->unhealthy-instances {"s1" [], "s2" [], "s3" [], "s4" [], "s5" []}})
              _ (async/<!! state-chan-reader) ;; ensure delivery from mult
              _ (query-state-fn query "s1")
              _ (async/>!! state-chan {:service-id->expired-instances {"s1" [], "s2" [], "s3" []}
                                       :service-id->healthy-instances {"s1" [], "s2" [], "s3" []}
                                       :service-id->unhealthy-instances {"s1" [], "s2" [], "s3" []}})
              _ (async/<!! state-chan-reader)
              _ (query-state-fn query "s1")]
          (is (= #{"s4" "s5"} @deleted-services-atom))
          (async/>!! exit :kill)
          (.shutdown scheduler-interactions-thread-pool)))))

  (deftest test-autoscaler-goroutine-first-run-of-scaler
    (let [service-id "service1"
          scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
          {:keys [exit initial-timeout-chan metrics-chan query query-service-state-fn state-chan state-chan-reader]}
          (start-autoscaler-goroutine {} [{:id service-id :instances 2 :task-count 2}] scheduler-interactions-thread-pool)]
      (async/>!! state-chan {:service-id->healthy-instances {service-id [{:id "instance-1"}]}
                             :service-id->unhealthy-instances {service-id [{:id "instance-2"}]}
                             :service-id->expired-instances {service-id [{:healthy? true :id "instance-1"}]}})
      (async/<!! state-chan-reader) ;; ensure delivery from mult
      (async/>!! metrics-chan {service-id {"outstanding" 2}})
      (async/>!! initial-timeout-chan :timeout)
      (let [query-response (async/promise-chan)
            expected-state {:expired-healthy-instances 1
                            :expired-unhealthy-instances 0
                            :healthy-instances 1
                            :instances 2
                            :outstanding-requests 2
                            :task-count 2}]
        (async/>!! query {:response-chan query-response :service-id service-id})
        (is (= expected-state (select-keys (async/<!! query-response) (keys expected-state))))
        (is (= expected-state (select-keys (query-service-state-fn {:service-id service-id}) (keys expected-state)))))
      (async/>!! exit :kill)
      (.shutdown scheduler-interactions-thread-pool)))

  (deftest test-autoscaler-goroutine-scaler-does-not-scale-during-pending-scaling-operation
    (let [service-id "service1"
          scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
          {:keys [exit initial-timeout-chan metrics-chan query query-service-state-fn]}
          (start-autoscaler-goroutine {:global-state {service-id {"outstanding" 2}}
                                       :service-id->scale-state {service-id {:target-instances 2 :scale-to-instances 2 :scale-amount 0}}
                                       :service-id->router-state {service-id {:healthy-instances 1
                                                                              :expired-healthy-instances 1
                                                                              :expired-unhealthy-instances 0}}
                                       :service-id->scheduler-state {service-id {:instances 2 :task-count 2}}}
                                      [{:id service-id :instances 2 :task-count 3}]
                                      scheduler-interactions-thread-pool)]
      (async/>!! metrics-chan {service-id {"outstanding" 2}})
      (async/>!! initial-timeout-chan :timeout)
      (let [query-response (async/promise-chan)
            expected-state {:expired-healthy-instances 1
                            :expired-unhealthy-instances 0
                            :healthy-instances 1
                            :instances 2
                            :outstanding-requests 2
                            :target-instances 3
                            :task-count 3}]
        (async/>!! query {:response-chan query-response :service-id service-id})
        (is (= expected-state (select-keys (async/<!! query-response) (keys expected-state))))
        (is (= expected-state (select-keys (query-service-state-fn {:service-id service-id}) (keys expected-state)))))
      (async/>!! exit :kill)
      (.shutdown scheduler-interactions-thread-pool)))

  (deftest test-autoscaler-goroutine-scaler-scale-up-after-pending-scaling-operation-completes
    (let [service-id "service1"
          scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
          {:keys [exit initial-timeout-chan metrics-chan query query-service-state-fn]}
          (start-autoscaler-goroutine {:global-state {service-id {"outstanding" 2}}
                                       :service-id->scale-state {service-id {:target-instances 2 :scale-to-instances 2 :scale-amount 0}}
                                       :service-id->router-state {service-id {:healthy-instances 1
                                                                              :expired-healthy-instances 1
                                                                              :expired-unhealthy-instances 0}}
                                       :service-id->scheduler-state {service-id {:instances 2 :task-count 3}}}
                                      [{:id service-id :instances 3 :task-count 3}]
                                      scheduler-interactions-thread-pool)]
      (async/>!! metrics-chan {service-id {"outstanding" 2}})
      (async/>!! initial-timeout-chan :timeout)
      (let [query-response (async/promise-chan)
            expected-state {:expired-healthy-instances 1
                            :expired-unhealthy-instances 0
                            :healthy-instances 1
                            :instances 3
                            :outstanding-requests 2
                            :target-instances 4
                            :task-count 3}]
        (async/>!! query {:response-chan query-response :service-id service-id})
        (is (= expected-state (select-keys (async/<!! query-response) (keys expected-state))))
        (is (= expected-state (select-keys (query-service-state-fn {:service-id service-id}) (keys expected-state)))))
      (async/>!! exit :kill)
      (.shutdown scheduler-interactions-thread-pool)))

  (deftest test-autoscaler-goroutine-scaler-process-state-update
    (let [service-id "service1"
          scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
          {:keys [exit metrics-chan query query-service-state-fn state-chan state-chan-reader]}
          (start-autoscaler-goroutine {:global-state {service-id {"outstanding" 2}}
                                       :service-id->scale-state {service-id {:target-instances 4 :scale-to-instances 4 :scale-amount 2}}
                                       :service-id->router-state {service-id {:healthy-instances 2
                                                                              :expired-healthy-instances 0
                                                                              :expired-unhealthy-instances 1}}
                                       :service-id->scheduler-state {service-id {:instances 3 :task-count 3}}}
                                      [{:id service-id :instances 3 :task-count 3}]
                                      scheduler-interactions-thread-pool)]
      (async/>!! state-chan {:service-id->healthy-instances {service-id [{:healthy? true :id "instance-1"}
                                                                         {:healthy? true :id "instance-3"}]}
                             :service-id->unhealthy-instances {service-id [{:id "instance-2"}]}
                             :service-id->expired-instances {service-id [{:id "instance-2"}]}})
      (async/<!! state-chan-reader) ;; ensure delivery from mult
      (async/>!! metrics-chan {service-id {"outstanding" 2}})
      (let [query-response (async/promise-chan)
            expected-state {:expired-healthy-instances 0
                            :expired-unhealthy-instances 1
                            :healthy-instances 2
                            :instances 3
                            :outstanding-requests 2
                            :target-instances 4
                            :task-count 3}]
        (async/>!! query {:response-chan query-response :service-id service-id})
        (is (= expected-state (select-keys (async/<!! query-response) (keys expected-state))))
        (is (= expected-state (select-keys (query-service-state-fn {:service-id service-id}) (keys expected-state)))))
      (async/>!! exit :kill)
      (.shutdown scheduler-interactions-thread-pool)))

  (deftest test-autoscaler-goroutine-scaler-process-state-update-expired-instances
    (let [service-id "service1"
          scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
          {:keys [exit metrics-chan query query-service-state-fn state-chan state-chan-reader]}
          (start-autoscaler-goroutine {:global-state {service-id {"outstanding" 2}}
                                       :service-id->scale-state {service-id {:target-instances 4 :scale-to-instances 4 :scale-amount 2}}
                                       :service-id->router-state {service-id {:healthy-instances 4
                                                                              :expired-healthy-instances 3
                                                                              :expired-unhealthy-instances 2}}
                                       :service-id->scheduler-state {service-id {:instances 3 :task-count 3}}}
                                      [{:id service-id :instances 3 :task-count 3}]
                                      scheduler-interactions-thread-pool)]
      (async/>!! state-chan {:service-id->healthy-instances {service-id [{:healthy? true :id "i1"}
                                                                         {:healthy? true :id "i3"}
                                                                         {:healthy? true :id "i5"}
                                                                         {:healthy? true :id "i7"}]}
                             :service-id->unhealthy-instances {service-id [{:id "i2"} {:id "i4"} {:id "i6"} {:id "i8"}]}
                             :service-id->expired-instances {service-id [{:healthy? true :id "i1"}
                                                                         {:id "i2"}
                                                                         {:healthy? true :id "i3"}
                                                                         {:id "i4"}
                                                                         {:healthy? true :id "i5"}]}})
      (async/<!! state-chan-reader) ;; ensure delivery from mult
      (async/>!! metrics-chan {service-id {"outstanding" 2}})
      (let [query-response (async/promise-chan)
            expected-state {:expired-healthy-instances 3
                            :expired-unhealthy-instances 2
                            :healthy-instances 4
                            :instances 3
                            :outstanding-requests 2
                            :target-instances 4
                            :task-count 3}]
        (async/>!! query {:response-chan query-response :service-id service-id})
        (is (= expected-state (select-keys (async/<!! query-response) (keys expected-state))))
        (is (= expected-state (select-keys (query-service-state-fn {:service-id service-id}) (keys expected-state)))))
      (async/>!! exit :kill)
      (.shutdown scheduler-interactions-thread-pool)))

  (deftest test-autoscaler-goroutine-scaler-scales-down-before-scale-up-is-completed
    (let [service-id "service1"
          scheduler-interactions-thread-pool (Executors/newFixedThreadPool 1)
          {:keys [exit initial-timeout-chan metrics-chan query query-service-state-fn state-chan state-chan-reader]}
          (start-autoscaler-goroutine {:global-state {service-id {"outstanding" 2}}
                                       :service-id->scale-state {service-id {:target-instances 4 :scale-to-instances 4 :scale-amount 2}}
                                       :service-id->router-state {service-id {:healthy-instances 2
                                                                              :expired-healthy-instances 1
                                                                              :expired-unhealthy-instances 0}}
                                       :service-id->scheduler-state {service-id {:instances 3 :task-count 3}}}
                                      [{:id service-id :instances 4 :task-count 3}]
                                      scheduler-interactions-thread-pool)]
      (async/>!! state-chan {:service-id->healthy-instances {service-id [{:id "instance-1"} {:id "instance-3"}]}
                             :service-id->unhealthy-instances {service-id [{:id "instance-2"}]}
                             :service-id->expired-instances {service-id [{:healthy? true :id "instance-1"}]}})
      (async/<!! state-chan-reader) ;; ensure delivery from mult
      (async/>!! metrics-chan {service-id {"outstanding" 2}})
      (async/>!! initial-timeout-chan :timeout)
      (let [query-response (async/promise-chan)
            expected-state {:expired-healthy-instances 1
                            :expired-unhealthy-instances 0
                            :healthy-instances 2
                            :instances 4
                            :outstanding-requests 2
                            :target-instances 0
                            :task-count 3}]
        (async/>!! query {:response-chan query-response :service-id service-id})
        (is (= expected-state (select-keys (async/<!! query-response) (keys expected-state))))
        (is (= expected-state (select-keys (query-service-state-fn {:service-id service-id}) (keys expected-state)))))
      (async/>!! exit :kill)
      (.shutdown scheduler-interactions-thread-pool))))
