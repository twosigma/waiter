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
(ns waiter.service-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [full.async :as fa]
            [waiter.mocks :refer :all]
            [waiter.service :refer :all]
            [waiter.util.cache-utils :as cu])
  (:import (java.util.concurrent Future Executors)))

(defn- mock-blacklisting-instance
  [instance-rpc-chan test-service-id callback-fn]
  (async/thread
    (try
      (let [{:keys [method response-chan service-id]} (async/<!! instance-rpc-chan)
            blacklist-chan (async/chan 1)]
        (is (= :blacklist method))
        (is (= test-service-id service-id))
        (async/>!! response-chan blacklist-chan)
        (callback-fn (async/<!! blacklist-chan)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)))))

(deftest test-blacklist-instance
  (testing "basic blacklist"
    (let [instance-rpc-chan (async/chan 1)
          service-id "test-id"
          instance-id "test-id.12345"
          instance {:service-id service-id :id instance-id}
          custom-blacklist-period-ms 5000
          response-chan (async/promise-chan)]
      (mock-blacklisting-instance
        instance-rpc-chan
        service-id
        (fn [[{:keys [cid blacklist-period-ms instance-id]} _]]
          (is (= (:id instance) instance-id))
          (is (not (nil? cid)))
          (is (= custom-blacklist-period-ms blacklist-period-ms))))
      (blacklist-instance-go instance-rpc-chan service-id instance-id custom-blacklist-period-ms response-chan)
      (async/<!! (async/timeout 10)))))

(defn- mock-offering-instance
  [instance-rpc-chan test-service-id callback-fn]
  (async/thread
    (try
      (let [{:keys [method response-chan service-id]} (async/<!! instance-rpc-chan)
            offer-chan (async/chan 1)]
        (is (= :offer method))
        (is (= test-service-id service-id))
        (async/>!! response-chan offer-chan)
        (callback-fn (async/<!! offer-chan)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)
        (is false "Unexpected exception")))))

(deftest test-offer-instance
  (testing "basic offer"
    (let [instance-rpc-chan (async/chan 1)
          router-id "router-id"
          service-id "test-id"
          instance-id "test-id.12345"
          instance {:service-id service-id, :id instance-id}
          response-chan (async/promise-chan)]
      (mock-offering-instance
        instance-rpc-chan
        service-id
        (fn [offer-params]
          (is (= instance (:instance offer-params)))
          (is (= response-chan (:response-chan offer-params)))
          (is (= router-id (:router-id offer-params)))))
      (offer-instance-go instance-rpc-chan service-id {:instance instance
                                                       :response-chan response-chan
                                                       :router-id router-id})
      (async/<!! (async/timeout 10)))))

(deftest test-query-maintainer-channel-map-with-timeout!
  (testing "basic query-state success"
    (let [instance-rpc-chan (async/chan 1)
          test-service-id "test-id"
          result-query-chan (async/chan 1)]
      (async/thread
        (let [{:keys [method response-chan service-id]} (async/<!! instance-rpc-chan)]
          (is (= :query-key method))
          (is (= test-service-id service-id))
          (is (not (nil? response-chan)))
          (async/>!! response-chan result-query-chan)))
      (async/<!! ;; wait for the go-block to complete execution
        (async/go
          (let [result-chan (query-maintainer-channel-map-with-timeout! instance-rpc-chan test-service-id 1000 :query-key)]
            (is (= result-query-chan result-chan)))))))
  (testing "basic query-state timeout"
    (let [instance-rpc-chan (async/chan 1)
          test-service-id "test-id"]
      (async/thread
        (let [{:keys [method response-chan service-id]} (async/<!! instance-rpc-chan)]
          (is (= :query-key method))
          (is (= test-service-id service-id))
          (is (not (nil? response-chan)))))
      (async/<!! ;; wait for the go-block to complete execution
        (async/go
          (let [result-chan (query-maintainer-channel-map-with-timeout! instance-rpc-chan test-service-id 100 :query-key)]
            (is (= {:message "Request timed-out!"} result-chan))))))))

(defn- mock-query-state-instance
  [instance-rpc-chan test-service-id callback-fn]
  (async/thread
    (try
      (let [{:keys [method response-chan service-id]} (async/<!! instance-rpc-chan)
            query-state-chan (async/chan 1)]
        (is (= :query-state method))
        (is (= test-service-id service-id))
        (async/>!! response-chan query-state-chan)
        (callback-fn (async/<!! query-state-chan)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)))))

(deftest test-query-state-instance
  (testing "basic query-state"
    (let [instance-rpc-chan (async/chan 1)
          in-service-id "test-id"
          response-chan (async/promise-chan)]
      (mock-query-state-instance
        instance-rpc-chan
        in-service-id
        (fn [{:keys [cid response-chan service-id]}]
          (is (= in-service-id service-id))
          (is (not (nil? cid)))
          (is (not (nil? response-chan)))))
      (query-instance-go instance-rpc-chan in-service-id response-chan)
      (async/<!! (async/timeout 10)))))

(deftest test-release-instance
  (testing "basic release"
    (let [instance-rpc-chan (async/chan 1)
          instance {:service-id "test-id" :id "test-id.12345"}
          reservation-result {:status :success, :cid "CID"}]
      (mock-reservation-system instance-rpc-chan
                               [(fn [[inst reservation-result]]
                                  (is (= instance inst))
                                  (is (= :success (:status reservation-result))))])
      (release-instance-go instance-rpc-chan instance reservation-result)
      ; Let mock propogate
      (async/<!! (async/timeout 10)))))

(deftest test-get-rand-inst
  (testing "basic get"
    (let [instance-rpc-chan (async/chan 1)
          instance {:service-id "test-id" :id "test-id.12345"}
          service-id "test-id"]
      (mock-reservation-system instance-rpc-chan [(fn [[_ resp-chan]] (async/>!! resp-chan instance))])
      (is (= instance
             (fa/<?? (async/go (get-rand-inst instance-rpc-chan service-id {:reason :serve-request} nil 100))))))))

(deftest test-start-new-service
  (let [make-cache-fn (fn [threshold ttl]
                        (cu/cache-factory {:threshold threshold :ttl ttl}))
        start-service-thread-pool (Executors/newFixedThreadPool 20)
        scheduler (Object.)
        descriptor {:service-id "test-service-id"}]

    (testing "start-new-service"
      (let [cache (make-cache-fn 100 20)
            start-called-atom (atom false)
            start-fn (fn [] (reset! start-called-atom (not @start-called-atom)))
            start-service-result (start-new-service scheduler descriptor cache start-service-thread-pool :start-fn start-fn)]
        (is (not (nil? start-service-result)))
        (.get ^Future start-service-result)
        (is @start-called-atom)))

    (testing "service-already-starting"
      (let [cache (make-cache-fn 100 1000)
            start-called-atom (atom false)
            start-fn (fn [] (reset! start-called-atom (not @start-called-atom)))]
        (let [start-service-result-1 (start-new-service scheduler descriptor cache start-service-thread-pool :start-fn start-fn)]
          (is (not (nil? start-service-result-1)))
          (.get ^Future start-service-result-1)
          (is @start-called-atom))
        (let [start-service-result-2 (start-new-service scheduler descriptor cache start-service-thread-pool :start-fn start-fn)]
          (is (nil? start-service-result-2)))))

    (testing "service-starting-after-cache-eviction"
      (let [cache (make-cache-fn 100 20)
            start-called-atom (atom 0)]
        (let [start-fn (fn [] (reset! start-called-atom 1))
              start-service-result-1 (start-new-service scheduler descriptor cache start-service-thread-pool :start-fn start-fn)]
          (is (not (nil? start-service-result-1)))
          (.get ^Future start-service-result-1)
          (is (= 1 @start-called-atom)))
        (Thread/sleep 30)
        (let [start-fn (fn [] (reset! start-called-atom 2))
              start-service-result-2 (start-new-service scheduler descriptor cache start-service-thread-pool :start-fn start-fn)]
          (is (not (nil? start-service-result-2)))
          (.get ^Future start-service-result-2))
        (is (= 2 @start-called-atom))))

    (testing "tens-of-services-starting-simultaneously"
      (let [cache (make-cache-fn 100 15000)
            start-called-atom (atom {})
            individual-call-result-atom (atom {})]
        (doall
          (pmap
            (fn [n]
              (let [service-num (rand-int 50)
                    service-id (str "test-service-id-" service-num)
                    descriptor {:service-id service-id}
                    start-fn (fn [] (swap! start-called-atom assoc service-id (not (get @start-called-atom service-id))))
                    start-service-result (start-new-service scheduler descriptor cache start-service-thread-pool :start-fn start-fn)]
                (if-not (nil? start-service-result)
                  (do
                    (.get ^Future start-service-result)
                    (swap! individual-call-result-atom assoc n (get @start-called-atom service-id)))
                  (swap! individual-call-result-atom assoc n true))))
            (range 1 1000)))
        (is (and (pos? (count @start-called-atom))
                 (every? (fn [[_ value]] (true? value)) @start-called-atom)))
        (is (and (pos? (count @individual-call-result-atom))
                 (every? (fn [[_ value]] (true? value)) @individual-call-result-atom)))))))
