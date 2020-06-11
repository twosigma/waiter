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
(ns waiter.work-stealing-test
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers]
            [waiter.util.semaphore :as semaphore]
            [waiter.test-helpers :refer :all]
            [waiter.util.utils :as utils]
            [waiter.work-stealing :refer :all]))

(defn- make-metrics
  [{:keys [outstanding slots-available slots-in-use slots-received]
    :or {outstanding 0, slots-available 0, slots-in-use 0, slots-received 0}}]
  {"outstanding" outstanding
   "slots-available" slots-available
   "slots-in-use" slots-in-use
   "slots-received" slots-received})

(deftest test-compute-help-required
  (is (= -6 (compute-help-required {"outstanding" 6, "slots-available" 2, "slots-in-use" 10, "slots-received" 0})))
  (is (= -10 (compute-help-required {"outstanding" 6, "slots-available" 2, "slots-in-use" 14, "slots-received" 0})))
  (is (zero? (compute-help-required {"slots-received" 0, "outstanding" 10, "slots-available" 10, "slots-in-use" 0})))
  (is (zero? (compute-help-required {"slots-received" 0, "outstanding" 14, "slots-available" 10, "slots-in-use" 4})))

  (is (= 13 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 10, "slots-received" 0})))
  (is (= 9 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 14, "slots-received" 0})))
  (is (= 15 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 0, "slots-received" 0})))
  (is (= 11 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 4, "slots-received" 0})))

  (is (= 1 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 10, "slots-received" 12})))
  (is (= -3 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 14, "slots-received" 12})))
  (is (= 3 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 0, "slots-received" 12})))
  (is (= -1 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 4, "slots-received" 12}))))

(deftest test-help-required?
  (is (false? (help-required? {"outstanding" 6, "slots-available" 2, "slots-in-use" 10, "slots-received" 0})))
  (is (false? (help-required? {"outstanding" 6, "slots-available" 2, "slots-in-use" 14, "slots-received" 0})))
  (is (false? (help-required? {"outstanding" 10, "slots-available" 10, "slots-in-use" 0, "slots-received" 0})))
  (is (false? (help-required? {"outstanding" 14, "slots-available" 10, "slots-in-use" 4, "slots-received" 0})))

  (is (false? (help-required? {"outstanding" 25, "slots-available" 1, "slots-in-use" 10, "slots-received" 0})))
  (is (true? (help-required? {"outstanding" 25, "slots-available" 0, "slots-in-use" 14, "slots-received" 0})))
  (is (false? (help-required? {"outstanding" 25, "slots-available" 10, "slots-in-use" 0, "slots-received" 0})))
  (is (true? (help-required? {"outstanding" 25, "slots-available" 0, "slots-in-use" 4, "slots-received" 0})))

  (is (false? (help-required? {"outstanding" 25, "slots-available" 2, "slots-in-use" 10, "slots-received" 12})))
  (is (false? (help-required? {"outstanding" 25, "slots-available" 2, "slots-in-use" 14, "slots-received" 12})))
  (is (false? (help-required? {"outstanding" 25, "slots-available" 10, "slots-in-use" 0, "slots-received" 12})))
  (is (false? (help-required? {"outstanding" 25, "slots-available" 10, "slots-in-use" 4, "slots-received" 12}))))

(deftest test-router-id->metrics->router-id->help-required
  (testing "nil-input"
    (is (= {}
           (router-id->metrics->router-id->help-required nil))))
  (testing "empty-input"
    (is (= {}
           (router-id->metrics->router-id->help-required {}))))
  (testing "router-missing-data"
    (is (= {}
           (router-id->metrics->router-id->help-required {"router-1" {}}))))
  (testing "router-not-requiring-help"
    (is (= {}
           (router-id->metrics->router-id->help-required
             {"router-1" (make-metrics {:outstanding 10, :slots-available 20})
              "router-2" (make-metrics {:slots-available 20})}))))
  (testing "router-requiring-help"
    (is (= {"router-1" 10}
           (router-id->metrics->router-id->help-required
             {"router-1" (make-metrics {:outstanding 10})
              "router-2" (make-metrics {:outstanding 10 :slots-available 5})}))))
  (testing "multiple-routers-requiring-help"
    (is (= {"router-1" 10, "router-2A" 3, "router-7A" 5}
           (router-id->metrics->router-id->help-required
             {"router-0" (make-metrics {:outstanding 10, :slots-available 20})
              "router-1" (make-metrics {:outstanding 10})
              "router-2A" (make-metrics {:outstanding 3})
              "router-2B" (make-metrics {:outstanding 5, :slots-available 2})
              "router-3" (make-metrics {:outstanding 2, :slots-available 5})
              "router-4" (make-metrics {})
              "router-5" (make-metrics {:outstanding 10, :slots-available 20, :slots-received 15})
              "router-6" (make-metrics {:outstanding 20, :slots-available 10, :slots-received 15})
              "router-7A" (make-metrics {:outstanding 20, :slots-available 0, :slots-received 15})
              "router-7B" (make-metrics {:outstanding 40, :slots-available 20, :slots-received 15})})))))

(defmacro check-work-stealing-balancer-query-state [query-chan expected-result]
  `(let [response-chan# (async/chan 1)
         _# (async/>!! ~query-chan {:response-chan response-chan#})
         raw-result# (async/<!! response-chan#)
         query-result# (select-keys raw-result# (keys ~expected-result))]
     (when (not= ~expected-result query-result#)
       (println (first *testing-vars*))
       (println "Expected: " (utils/deep-sort-map ~expected-result))
       (println "Actual:   " (utils/deep-sort-map query-result#)))
     (is (= ~expected-result query-result#))))

(let [router-id "test-router-id"
      service-id "test-service-id"
      metrics-atom (atom nil)
      max-permits 100
      service-id->router-id->metrics (fn [in-service-id] (is (= service-id in-service-id)) @metrics-atom)]

  (defn- make-request-id [iteration loop-counter]
    (str service-id "." router-id ".ws" iteration ".offer" loop-counter))

  (defn- populate-request-id->workstealer [result-map iteration loop-counter router-id instance-id]
    (let [request-id (make-request-id iteration loop-counter)]
      (assoc result-map request-id {:cid request-id
                                    :instance {:id instance-id}
                                    :request-id request-id
                                    :target-router-id router-id})))

  (deftest test-work-stealing-balancer-initialization
    (let [initial-state {:iteration 10 :request-id->work-stealer {"req-1" {:instance {}}}}
          offer-help-fn (fn [_ _] nil)
          release-instance-fn (fn [_] nil)
          reserve-instance-fn (fn [_ response-chan] (async/>!! response-chan :no-instance-found))
          _ (reset! metrics-atom nil)
          custom-timeout-chan (async/chan 1)
          timeout-chan-factory (constantly custom-timeout-chan)
          offers-allowed-semaphore (semaphore/create-semaphore max-permits)
          {:keys [exit-chan query-chan]}
          (work-stealing-balancer initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn
                                  release-instance-fn offer-help-fn offers-allowed-semaphore router-id service-id)]
      (reset! metrics-atom {"router-1" (make-metrics {:outstanding 10 :slots-available 20})})
      (check-work-stealing-balancer-query-state
        query-chan
        (assoc initial-state :global-offers {:allowed max-permits :available max-permits}))
      (async/>!! exit-chan :exit)))

  (deftest test-work-stealing-balancer-no-routers-require-help
    (let [initial-state {:iteration 10 :request-id->work-stealer {}}
          offer-help-fn (fn [_ _] nil)
          release-instance-fn (fn [_] nil)
          reserve-instance-counter (atom 0)
          reserve-instance-fn (fn [_ response-chan]
                                (swap! reserve-instance-counter inc)
                                (async/>!! response-chan :no-instance-found))
          _ (reset! metrics-atom nil)
          custom-timeout-chan (async/chan 1)
          timeout-chan-factory (constantly custom-timeout-chan)
          offers-allowed-semaphore (semaphore/create-semaphore max-permits)
          {:keys [exit-chan query-chan]}
          (work-stealing-balancer initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn
                                  release-instance-fn offer-help-fn offers-allowed-semaphore router-id service-id)]
      (reset! metrics-atom {"router-1" (make-metrics {:outstanding 10 :slots-available 20})
                            "router-2" (make-metrics {:slots-available 20})})
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available max-permits}
         :iteration 10
         :request-id->work-stealer {}
         :slots {:offerable 0 :offered 0}})
      (is (zero? (deref reserve-instance-counter)))
      (async/>!! exit-chan :exit)))

  (deftest test-work-stealing-balancer-no-instance-available
    (let [initial-state {:iteration 10 :request-id->work-stealer {}}
          offer-help-fn (fn [_ _] nil)
          reserve-instance-counter (atom 0)
          reserve-instance-fn (fn [_ response-chan]
                                (swap! reserve-instance-counter inc)
                                (async/>!! response-chan :no-instance-found))
          release-instance-fn (fn [_] nil)
          _ (reset! metrics-atom nil)
          custom-timeout-chan (async/chan 1)
          timeout-chan-factory (constantly custom-timeout-chan)
          offers-allowed-semaphore (semaphore/create-semaphore max-permits)
          {:keys [exit-chan query-chan]}
          (work-stealing-balancer initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn
                                  release-instance-fn offer-help-fn offers-allowed-semaphore router-id service-id)]

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 15})
                            "router-1" (make-metrics {:outstanding 10})
                            "router-2" (make-metrics {:slots-available 20})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available max-permits}
         :iteration 11 :request-id->work-stealer {} :slots {:offerable 5 :offered 0}})
      (is (= 1 @reserve-instance-counter))
      (async/>!! exit-chan :exit)))

  (deftest test-work-stealing-balancer-single-instance-available-reservation-and-release
    (let [available-slots 1
          initial-state {:iteration 10 :request-id->work-stealer {}}
          request-id->cleanup-chan-atom (atom {})
          offer-help-fn (fn [{:keys [request-id]} cleanup-chan]
                          (swap! request-id->cleanup-chan-atom assoc request-id cleanup-chan))
          response-callback (fn [request-id response-status]
                              (async/>!! (get @request-id->cleanup-chan-atom request-id)
                                         {:request-id request-id :status response-status}))
          released-instances-atom (atom #{})
          release-instance-fn (fn [reservation-result]
                                (swap! released-instances-atom conj (get-in reservation-result [:instance :id])))
          reserve-instance-counter (atom 0)
          reserve-instance-fn (fn [_ response-chan]
                                (swap! reserve-instance-counter inc)
                                (if (<= @reserve-instance-counter available-slots)
                                  (async/>!! response-chan {:id (str "test-instance-id-" @reserve-instance-counter)})
                                  (async/>!! response-chan :no-instance-found)))
          _ (reset! metrics-atom nil)
          custom-timeout-chan (async/chan 1)
          timeout-chan-factory (constantly custom-timeout-chan)
          offers-allowed-semaphore (semaphore/create-semaphore max-permits)
          {:keys [exit-chan query-chan]}
          (work-stealing-balancer initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn
                                  release-instance-fn offer-help-fn offers-allowed-semaphore router-id service-id)]

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 15})
                            "router-1" (make-metrics {:outstanding 10})
                            "router-2" (make-metrics {:slots-available 20})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (dec max-permits)}
         :iteration 11
         :request-id->work-stealer (populate-request-id->workstealer {} 10 0 "router-1" "test-instance-id-1")
         :slots {:offerable 5 :offered 1}})

      (is (pos? @reserve-instance-counter))
      (is (contains? @request-id->cleanup-chan-atom "test-service-id.test-router-id.ws10.offer0"))

      (response-callback "test-service-id.test-router-id.ws10.offer0" :success)
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available max-permits}
         :iteration 14
         :request-id->work-stealer {}
         :slots {:offerable 5 :offered 0}})
      (is (contains? @released-instances-atom "test-instance-id-1"))

      (async/>!! exit-chan :exit)))

  (deftest test-work-stealing-balancer-multiple-instances-available-incremental-reservation
    (let [available-slots 7
          initial-state {:iteration 10 :request-id->work-stealer {}}
          request-id->cleanup-chan-atom (atom {})
          offer-help-fn (fn [{:keys [request-id]} cleanup-chan]
                          (swap! request-id->cleanup-chan-atom assoc request-id cleanup-chan))
          released-instances-atom (atom #{})
          release-instance-fn (fn [reservation-result]
                                (swap! released-instances-atom conj (get-in reservation-result [:instance :id])))
          reserve-instance-counter (atom 0)
          reserve-instance-fn (fn [_ response-chan]
                                (swap! reserve-instance-counter inc)
                                (if (<= @reserve-instance-counter available-slots)
                                  (async/>!! response-chan {:id (str "test-instance-id-" @reserve-instance-counter)})
                                  (async/>!! response-chan :no-instance-found)))
          _ (reset! metrics-atom nil)
          custom-timeout-chan (async/chan 1)
          timeout-chan-factory (constantly custom-timeout-chan)
          offers-allowed-semaphore (semaphore/create-semaphore max-permits)
          {:keys [exit-chan query-chan]}
          (work-stealing-balancer initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn
                                  release-instance-fn offer-help-fn offers-allowed-semaphore router-id service-id)]

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 15})
                            "router-1" (make-metrics {:outstanding 1})
                            "router-2" (make-metrics {:outstanding 1})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (- max-permits 2)}
         :iteration 11
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 0 "router-2" "test-instance-id-1")
                                     (populate-request-id->workstealer 10 1 "router-1" "test-instance-id-2"))
         :slots {:offerable 5 :offered 2}})

      (is (= 2 @reserve-instance-counter))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 0)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 1)))

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 13})
                            "router-1" (make-metrics {:outstanding 0})
                            "router-2" (make-metrics {:outstanding 0})
                            "router-3" (make-metrics {:outstanding 4})
                            "router-4" (make-metrics {:outstanding 3})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (- max-permits 5)}
         :iteration 13
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 0 "router-2" "test-instance-id-1")
                                     (populate-request-id->workstealer 10 1 "router-1" "test-instance-id-2")
                                     (populate-request-id->workstealer 12 0 "router-3" "test-instance-id-3")
                                     (populate-request-id->workstealer 12 1 "router-4" "test-instance-id-4")
                                     (populate-request-id->workstealer 12 2 "router-3" "test-instance-id-5"))
         :slots {:offerable 3 :offered 5}})

      (is (= 5 @reserve-instance-counter))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 0)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 1)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 12 0)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 12 1)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 12 2)))

      (async/>!! exit-chan :exit)))

  (deftest test-work-stealing-balancer-multiple-instances-available-reservation-and-release
    (let [available-slots 7
          initial-state {:iteration 10 :request-id->work-stealer {}}
          request-id->cleanup-chan-atom (atom {})
          offer-help-fn (fn [{:keys [request-id]} cleanup-chan]
                          (swap! request-id->cleanup-chan-atom assoc request-id cleanup-chan))
          response-callback (fn [request-id response-status]
                              (async/>!! (get @request-id->cleanup-chan-atom request-id)
                                         {:request-id request-id :status response-status}))
          released-instances-atom (atom #{})
          release-instance-fn (fn [reservation-result]
                                (swap! released-instances-atom conj (get-in reservation-result [:instance :id])))
          reserve-instance-counter (atom 0)
          reserve-instance-fn (fn [_ response-chan]
                                (swap! reserve-instance-counter inc)
                                (if (<= @reserve-instance-counter available-slots)
                                  (async/>!! response-chan {:id (str "test-instance-id-" @reserve-instance-counter)})
                                  (async/>!! response-chan :no-instance-found)))
          _ (reset! metrics-atom nil)
          custom-timeout-chan (async/chan 1)
          timeout-chan-factory (constantly custom-timeout-chan)
          offers-allowed-semaphore (semaphore/create-semaphore max-permits)
          {:keys [exit-chan query-chan]}
          (work-stealing-balancer initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn
                                  release-instance-fn offer-help-fn offers-allowed-semaphore router-id service-id)]

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 15})
                            "router-1" (make-metrics {:outstanding 4})
                            "router-2" (make-metrics {:outstanding 3})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (- max-permits 5)}
         :iteration 11
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 0 "router-1" "test-instance-id-1")
                                     (populate-request-id->workstealer 10 1 "router-2" "test-instance-id-2")
                                     (populate-request-id->workstealer 10 2 "router-1" "test-instance-id-3")
                                     (populate-request-id->workstealer 10 3 "router-2" "test-instance-id-4")
                                     (populate-request-id->workstealer 10 4 "router-1" "test-instance-id-5"))
         :slots {:offerable 5 :offered 5}})

      (is (= 5 @reserve-instance-counter))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 0)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 1)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 2)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 3)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 4)))

      (response-callback (make-request-id 10 0) :success)
      (response-callback (make-request-id 10 2) :success)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (- max-permits 3)}
         :iteration 14
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 1 "router-2" "test-instance-id-2")
                                     (populate-request-id->workstealer 10 3 "router-2" "test-instance-id-4")
                                     (populate-request-id->workstealer 10 4 "router-1" "test-instance-id-5"))
         :slots {:offerable 5 :offered 3}})
      (is (contains? @released-instances-atom "test-instance-id-1"))
      (is (not (contains? @released-instances-atom "test-instance-id-2")))
      (is (contains? @released-instances-atom "test-instance-id-3"))
      (is (not (contains? @released-instances-atom "test-instance-id-4")))
      (is (not (contains? @released-instances-atom "test-instance-id-5")))
      (is (not (contains? @released-instances-atom "test-instance-id-6")))
      (is (not (contains? @released-instances-atom "test-instance-id-7")))
      (is (not (contains? @released-instances-atom "test-instance-id-8")))

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 15})
                            "router-3" (make-metrics {:outstanding 10})
                            "router-4" (make-metrics {:outstanding 30})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (- max-permits 5)}
         :iteration 16
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 1 "router-2" "test-instance-id-2")
                                     (populate-request-id->workstealer 10 3 "router-2" "test-instance-id-4")
                                     (populate-request-id->workstealer 10 4 "router-1" "test-instance-id-5")
                                     (populate-request-id->workstealer 15 0 "router-4" "test-instance-id-6")
                                     (populate-request-id->workstealer 15 1 "router-4" "test-instance-id-7"))
         :slots {:offerable 5 :offered 5}})

      (response-callback (make-request-id 10 1) :success)
      (response-callback (make-request-id 10 3) :success)
      (response-callback (make-request-id 15 1) :success)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (- max-permits 2)}
         :iteration 20
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 4 "router-1" "test-instance-id-5")
                                     (populate-request-id->workstealer 15 0 "router-4" "test-instance-id-6"))
         :slots {:offerable 5 :offered 2}})

      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available (- max-permits 2)}
         :iteration 22
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 4 "router-1" "test-instance-id-5")
                                     (populate-request-id->workstealer 15 0 "router-4" "test-instance-id-6"))
         :slots {:offerable 5 :offered 2}})
      (is (contains? @released-instances-atom "test-instance-id-1"))
      (is (contains? @released-instances-atom "test-instance-id-2"))
      (is (contains? @released-instances-atom "test-instance-id-3"))
      (is (contains? @released-instances-atom "test-instance-id-4"))
      (is (not (contains? @released-instances-atom "test-instance-id-5")))
      (is (not (contains? @released-instances-atom "test-instance-id-6")))
      (is (contains? @released-instances-atom "test-instance-id-7"))
      (is (not (contains? @released-instances-atom "test-instance-id-8")))

      (async/>!! exit-chan :exit)))

  (deftest test-work-stealing-balancer-offers-limited-by-max-work-stealing-offers
    (let [available-slots 7
          max-permits 2
          initial-state {:iteration 10 :request-id->work-stealer {}}
          request-id->cleanup-chan-atom (atom {})
          offer-help-fn (fn [{:keys [request-id]} cleanup-chan]
                          (swap! request-id->cleanup-chan-atom assoc request-id cleanup-chan))
          response-callback (fn [request-id response-status]
                              (let [response-chan (get @request-id->cleanup-chan-atom request-id)]
                                (async/>!! response-chan {:request-id request-id :status response-status})))
          released-instances-atom (atom #{})
          release-instance-fn (fn [reservation-result]
                                (swap! released-instances-atom conj (get-in reservation-result [:instance :id])))
          reserve-instance-counter (atom 0)
          reserve-instance-fn (fn [_ response-chan]
                                (swap! reserve-instance-counter inc)
                                (if (<= @reserve-instance-counter available-slots)
                                  (async/>!! response-chan {:id (str "test-instance-id-" @reserve-instance-counter)})
                                  (async/>!! response-chan :no-instance-found)))
          _ (reset! metrics-atom nil)
          custom-timeout-chan (async/chan 1)
          timeout-chan-factory (constantly custom-timeout-chan)
          offers-allowed-semaphore (semaphore/create-semaphore max-permits)
          {:keys [exit-chan query-chan]}
          (work-stealing-balancer initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn
                                  release-instance-fn offer-help-fn offers-allowed-semaphore router-id service-id)]

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 15})
                            "router-1" (make-metrics {:outstanding 4})
                            "router-2" (make-metrics {:outstanding 3})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available 0}
         :iteration 11
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 0 "router-1" "test-instance-id-1")
                                     (populate-request-id->workstealer 10 1 "router-2" "test-instance-id-2"))
         :slots {:offerable 5 :offered 2}})

      (is (= 2 @reserve-instance-counter))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 0)))
      (is (contains? @request-id->cleanup-chan-atom (make-request-id 10 1)))
      (is (not (contains? @request-id->cleanup-chan-atom (make-request-id 10 2))))
      (is (not (contains? @request-id->cleanup-chan-atom (make-request-id 10 3))))
      (is (not (contains? @request-id->cleanup-chan-atom (make-request-id 10 4))))

      (response-callback (make-request-id 10 0) :success)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available 1}
         :iteration 13
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 1 "router-2" "test-instance-id-2"))
         :slots {:offerable 5 :offered 1}})
      (is (contains? @released-instances-atom "test-instance-id-1"))
      (is (not (contains? @released-instances-atom "test-instance-id-2")))
      (is (not (contains? @released-instances-atom "test-instance-id-3")))
      (is (not (contains? @released-instances-atom "test-instance-id-4")))
      (is (not (contains? @released-instances-atom "test-instance-id-5")))

      (reset! metrics-atom {router-id (make-metrics {:outstanding 10 :slots-available 15})
                            "router-3" (make-metrics {:outstanding 10})
                            "router-4" (make-metrics {:outstanding 30})})
      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available 0}
         :iteration 15
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 10 1 "router-2" "test-instance-id-2")
                                     (populate-request-id->workstealer 14 0 "router-4" "test-instance-id-3"))
         :slots {:offerable 5 :offered 2}})

      (response-callback (make-request-id 10 1) :success)
      (check-work-stealing-balancer-query-state
        query-chan {:iteration 17
                    :request-id->work-stealer
                    (-> {}
                      (populate-request-id->workstealer 14 0 "router-4" "test-instance-id-3"))
                    :slots {:offerable 5 :offered 1}})

      (async/>!! custom-timeout-chan :custom-timeout)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available 0}
         :iteration 19
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 14 0 "router-4" "test-instance-id-3")
                                     (populate-request-id->workstealer 18 0 "router-4" "test-instance-id-4"))
         :slots {:offerable 5 :offered 2}})
      (is (contains? @released-instances-atom "test-instance-id-1"))
      (is (contains? @released-instances-atom "test-instance-id-2"))
      (is (not (contains? @released-instances-atom "test-instance-id-3")))
      (is (not (contains? @released-instances-atom "test-instance-id-4")))
      (is (not (contains? @released-instances-atom "test-instance-id-5")))

      (response-callback (make-request-id 18 0) :success)
      (check-work-stealing-balancer-query-state
        query-chan
        {:global-offers {:allowed max-permits :available 1}
         :iteration 21
         :request-id->work-stealer (-> {}
                                     (populate-request-id->workstealer 14 0 "router-4" "test-instance-id-3"))
         :slots {:offerable 5 :offered 1}})
      (is (contains? @released-instances-atom "test-instance-id-1"))
      (is (contains? @released-instances-atom "test-instance-id-2"))
      (is (not (contains? @released-instances-atom "test-instance-id-3")))
      (is (contains? @released-instances-atom "test-instance-id-4"))
      (is (not (contains? @released-instances-atom "test-instance-id-5")))

      (async/>!! exit-chan :exit))))

(deftest test-start-work-stealing-balancer-offer-help-fn
  (let [offer-help-fn-atom (atom nil)
        router-id "test-router-id"
        target-router-id "target-router-id"
        service-id "test-service-id"
        request-id "test-request-id"
        instance-rpc-chan (async/chan 100)
        populate-maintainer-chan! (make-populate-maintainer-chan! instance-rpc-chan)
        reserve-timeout-ms 1000
        offer-help-interval-ms 1000
        offer-idle-timeout-ms 1800000
        service-id->router-id->metrics {}
        make-inter-router-requests-fn-factory (fn [status response-map]
                                                (fn [endpoint & {:keys [acceptable-router? config body method]}]
                                                  (is (= "work-stealing" endpoint))
                                                  (is (acceptable-router? target-router-id))
                                                  (is (not (acceptable-router? router-id)))
                                                  (is (= {:idle-timeout offer-idle-timeout-ms} config))
                                                  (is (= (walk/stringify-keys
                                                           {:request-id request-id
                                                            :router-id router-id
                                                            :service-id service-id
                                                            :target-router-id target-router-id})
                                                         (json/read-str body)))
                                                  (is (= :post method))
                                                  (let [response-chan (async/promise-chan)
                                                        body-chan (async/promise-chan)]
                                                    (async/>!! body-chan (utils/clj->json response-map))
                                                    (async/>!! response-chan {:body body-chan :status status})
                                                    {target-router-id response-chan})))]
    (with-redefs [work-stealing-balancer
                  (fn [_ _ _ _ _ offer-help-fn _ _ _]
                    (reset! offer-help-fn-atom offer-help-fn))]

      (testing "2XX response - missing status"
        (let [offers-allowed-semaphore (semaphore/create-semaphore 10)
              make-inter-router-requests-fn (make-inter-router-requests-fn-factory http-200-ok {})]
          (start-work-stealing-balancer
            populate-maintainer-chan! reserve-timeout-ms offer-help-interval-ms offer-idle-timeout-ms
            offers-allowed-semaphore service-id->router-id->metrics make-inter-router-requests-fn router-id service-id)
          (is @offer-help-fn-atom)
          (let [offer-help-fn @offer-help-fn-atom
                reservation-parameters {:request-id request-id :target-router-id target-router-id}
                cleanup-chan (async/chan 1)]
            (offer-help-fn reservation-parameters cleanup-chan)
            (is (= {:request-id request-id :status :work-stealing-error} (async/<!! cleanup-chan))))))

      (testing "2XX response - success"
        (let [offers-allowed-semaphore (semaphore/create-semaphore 10)
              make-inter-router-requests-fn (make-inter-router-requests-fn-factory http-200-ok {:response-status "successful"})]
          (start-work-stealing-balancer
            populate-maintainer-chan! reserve-timeout-ms offer-help-interval-ms offer-idle-timeout-ms
            offers-allowed-semaphore service-id->router-id->metrics make-inter-router-requests-fn router-id service-id)
          (is @offer-help-fn-atom)
          (let [offer-help-fn @offer-help-fn-atom
                reservation-parameters {:request-id request-id :target-router-id target-router-id}
                cleanup-chan (async/chan 1)]
            (offer-help-fn reservation-parameters cleanup-chan)
            (is (= {:request-id request-id :status :successful} (async/<!! cleanup-chan))))))

      (testing "2XX response - failure"
        (let [offers-allowed-semaphore (semaphore/create-semaphore 10)
              make-inter-router-requests-fn (make-inter-router-requests-fn-factory http-200-ok {:response-status "failure"})]
          (start-work-stealing-balancer
            populate-maintainer-chan! reserve-timeout-ms offer-help-interval-ms offer-idle-timeout-ms
            offers-allowed-semaphore service-id->router-id->metrics make-inter-router-requests-fn router-id service-id)
          (is @offer-help-fn-atom)
          (let [offer-help-fn @offer-help-fn-atom
                reservation-parameters {:request-id request-id :target-router-id target-router-id}
                cleanup-chan (async/chan 1)]
            (offer-help-fn reservation-parameters cleanup-chan)
            (is (= {:request-id request-id :status :failure} (async/<!! cleanup-chan))))))

      (testing "4XX response - failure"
        (let [offers-allowed-semaphore (semaphore/create-semaphore 10)
              make-inter-router-requests-fn (make-inter-router-requests-fn-factory http-400-bad-request {:response-status "failure"})]
          (start-work-stealing-balancer
            populate-maintainer-chan! reserve-timeout-ms offer-help-interval-ms offer-idle-timeout-ms
            offers-allowed-semaphore service-id->router-id->metrics make-inter-router-requests-fn router-id service-id)
          (is @offer-help-fn-atom)
          (let [offer-help-fn @offer-help-fn-atom
                reservation-parameters {:request-id request-id :target-router-id target-router-id}
                cleanup-chan (async/chan 1)]
            (offer-help-fn reservation-parameters cleanup-chan)
            (is (= {:request-id request-id :status :failure} (async/<!! cleanup-chan))))))

      (testing "5XX response - failure"
        (let [offers-allowed-semaphore (semaphore/create-semaphore 10)
              make-inter-router-requests-fn (make-inter-router-requests-fn-factory http-500-internal-server-error {:response-status "failure"})]
          (start-work-stealing-balancer
            populate-maintainer-chan! reserve-timeout-ms offer-help-interval-ms offer-idle-timeout-ms
            offers-allowed-semaphore service-id->router-id->metrics make-inter-router-requests-fn router-id service-id)
          (is @offer-help-fn-atom)
          (let [offer-help-fn @offer-help-fn-atom
                reservation-parameters {:request-id request-id :target-router-id target-router-id}
                cleanup-chan (async/chan 1)]
            (offer-help-fn reservation-parameters cleanup-chan)
            (is (= {:request-id request-id :status :failure} (async/<!! cleanup-chan)))))))))
