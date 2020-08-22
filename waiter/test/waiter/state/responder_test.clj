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
(ns waiter.state.responder-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [digest]
            [metrics.counters :as counters]
            [plumbing.core :as pc]
            [waiter.metrics :as metrics]
            [waiter.state.responder :refer :all]
            [waiter.util.utils :as utils])
  (:import (clojure.lang PersistentQueue)
           (org.joda.time DateTime)))

(deftest test-find-instance-to-offer-with-concurrency-level-1
  (let [make-instance (fn [id] {:id (str "inst-" id), :started-at (DateTime. (* id 1000000))})
        instance-1 (make-instance 1)
        instance-2 (make-instance 2)
        instance-3 (make-instance 3)
        instance-4 (make-instance 4)
        instance-5 (make-instance 5)
        instance-6 (make-instance 6)
        instance-7 (make-instance 7)
        instance-8 (make-instance 8)
        current-time (t/now)
        healthy-instance-combo [instance-2 instance-3 instance-5 instance-6 instance-8]
        healthy-instance-ids (map :id healthy-instance-combo)
        unhealthy-instance-combo [instance-1 instance-4 instance-7]
        unhealthy-instance-ids (map :id unhealthy-instance-combo)
        all-instance-combo (concat healthy-instance-combo unhealthy-instance-combo)
        all-sorted-instance-ids (sort (map :id all-instance-combo))
        instance-id->state-fn #(merge
                                 (into {} (map (fn [instance-id] [instance-id {:slots-assigned 1, :slots-used 0, :status-tags #{:healthy}}]) %1))
                                 (into {} (map (fn [instance-id] [instance-id {:slots-assigned 0, :slots-used 0, :status-tags #{:unhealthy}}]) %2)))
        all-id->instance (pc/map-from-vals :id all-instance-combo)
        lingering-request-threshold-ms 60000
        time-active (->> (- lingering-request-threshold-ms 1000) (t/millis) (t/minus current-time))
        time-linger (->> (+ lingering-request-threshold-ms 1000) (t/millis) (t/minus current-time))
        test-cases (list
                     {:name "find-instance-to-offer:serving-with-no-healthy-instances"
                      :expected nil
                      :reason :serve-request
                      :id->instance {}
                      :instance-id->state (instance-id->state-fn [] [])}
                     {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-instances"
                      :expected [instance-2]
                      :reason :serve-request
                      :instance-id->state (instance-id->state-fn healthy-instance-ids [])}
                     {:name "find-instance-to-offer:serving-healthy-unejected-instance-with-no-unhealthy-instances"
                      :expected [instance-3]
                      :reason :serve-request
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :ejected))}
                     {:name "find-instance-to-offer:serving-healthy-unejected-instance-with-no-unhealthy-instances:limited-sorted-instance-ids"
                      :expected [instance-5]
                      :reason :serve-request
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :ejected))
                      :sorted-instance-ids (drop 3 all-sorted-instance-ids)}
                     {:name "find-instance-to-offer:serving-healthy-unejected-instance-with-no-unhealthy-instances:limited-sorted-instance-ids-2"
                      :expected [instance-6]
                      :reason :serve-request
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :ejected))
                      :sorted-instance-ids (drop 5 all-sorted-instance-ids)}
                     {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-instances:exclude-ejected-locked-and-killed"
                      :expected [instance-6]
                      :reason :serve-request
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :ejected)
                                            (update-in ["inst-3" :status-tags] conj :killed)
                                            (update-in ["inst-5" :status-tags] conj :locked))}
                     {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-instances-but-all-excluded"
                      :expected nil
                      :reason :serve-request
                      :instance-id->state (instance-id->state-fn healthy-instance-ids [])
                      :exclude-ids-set (set (concat healthy-instance-ids unhealthy-instance-ids))}
                     (let [exclude-ids-set #{"inst-1" "inst-2" "inst-7" "inst-8"}]
                       {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-but-excluded-instances"
                        :expected [instance-3]
                        :reason :serve-request
                        :instance-id->state (instance-id->state-fn healthy-instance-ids [])
                        :exclude-ids-set exclude-ids-set})
                     {:name "find-instance-to-offer:serving-healthy-instance-with-some-unhealthy-instances"
                      :expected [instance-2]
                      :reason :serve-request
                      :instance-id->state (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)}
                     (let [exclude-ids-set #{"inst-1" "inst-2" "inst-3" "inst-7" "inst-8"}]
                       {:name "find-instance-to-offer:serving-healthy-instance-with-some-unhealthy-and-excluded-instances"
                        :expected [instance-5]
                        :reason :serve-request
                        :instance-id->state (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                        :exclude-ids-set exclude-ids-set})
                     (let [exclude-ids-set (set healthy-instance-ids)]
                       {:name "find-instance-to-offer:exclude-all-healthy-instances"
                        :expected [nil]
                        :reason :serve-request
                        :instance-id->state (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                        :exclude-ids-set exclude-ids-set})
                     {:expected [instance-5]
                      :name "find-instance-to-offer:select-oldest-healthy-live-instance"
                      :reason :serve-request
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-3" :status-tags] conj :expired)
                                            (update-in ["inst-8" :status-tags] conj :expired))}
                     {:expected [instance-8]
                      :name "find-instance-to-offer:select-youngest-healthy-expired-instance"
                      :reason :serve-request
                      :instance-id->state (->> (instance-id->state-fn healthy-instance-ids [])
                                            (pc/map-vals #(update % :status-tags conj :expired)))}
                     {:name "find-instance-to-offer:killing-with-no-instances"
                      :expected nil
                      :reason :kill-instance
                      :id->instance {}
                      :instance-id->state (instance-id->state-fn [] [])}
                     {:name "find-instance-to-offer:killing-youngest-healthy-instance-with-no-unhealthy-instances"
                      :expected [instance-8]
                      :reason :kill-instance
                      :instance-id->state (instance-id->state-fn healthy-instance-ids [])}
                     {:name "find-instance-to-offer:killing-oldest-healthy-instance-with-no-unhealthy-instances"
                      :expected [instance-2]
                      :load-balancing :youngest
                      :reason :kill-instance
                      :instance-id->state (instance-id->state-fn healthy-instance-ids [])}
                     {:name "find-instance-to-offer:killing-oldest-healthy-instance-with-no-unhealthy-but-excluded-instances"
                      :expected [instance-6]
                      :reason :kill-instance
                      :instance-id->state (instance-id->state-fn healthy-instance-ids [])
                      :exclude-ids-set #{"inst-1" "inst-2" "inst-7" "inst-8"}
                      }
                     {:name "find-instance-to-offer:killing-youngest-healthy-instance-with-no-unhealthy-but-excluded-instances"
                      :expected [instance-3]
                      :load-balancing :youngest
                      :reason :kill-instance
                      :instance-id->state (instance-id->state-fn healthy-instance-ids [])
                      :exclude-ids-set #{"inst-1" "inst-2" "inst-7" "inst-8"}
                      }
                     {:name "find-instance-to-offer:killing-healthy-instance-with-no-unhealthy-but-excluded-instances:exclude-busy"
                      :expected [instance-5]
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-6"]
                                                       assoc :slots-assigned 2 :slots-used 1))
                      :exclude-ids-set #{"inst-1" "inst-2" "inst-7" "inst-8"}}
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances"
                      :expected [instance-7]
                      :reason :kill-instance
                      :instance-id->state (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)}
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances:exclude-busy"
                      :expected [instance-4]
                      :reason :kill-instance
                      :id->instance all-id->instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                                            (update-in ["inst-7"]
                                                       assoc :slots-assigned 0 :slots-used 1))}
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances:exclude-killed"
                      :expected [instance-4]
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                                            (update-in ["inst-7" :status-tags] conj :killed))}
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances:exclude-killed-include-ejected"
                      :expected [instance-4]
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                                            (update-in ["inst-4" :status-tags] conj :ejected)
                                            (update-in ["inst-7" :status-tags] conj :killed))}
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-and-excluded-instances"
                      :expected [instance-4]
                      :reason :kill-instance
                      :instance-id->state (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                      :exclude-ids-set #{"inst-1" "inst-2" "inst-7" "inst-8"}}
                     {:name "find-instance-to-offer:killing-healthy-ejected-instance-with-no-unhealthy-instances"
                      :expected [instance-8]
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-8" :status-tags] conj :ejected))}
                     {:name "find-instance-to-offer:killing-healthy-instance-with-no-unhealthy-instances:exclude-locked-and-killed"
                      :expected [instance-2]
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :ejected)
                                            (update-in ["inst-3" :status-tags] conj :killed)
                                            (update-in ["inst-8" :status-tags] conj :locked))}
                     {:name "find-instance-to-offer:killing-healthy-ejected-instance-with-no-unhealthy-instances:exclude-locked-and-killed"
                      :expected [instance-6]
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-3" :status-tags] conj :killed)
                                            (update-in ["inst-8" :status-tags] conj :locked))}
                     {:expected [instance-2]
                      :name "find-instance-to-offer:get-youngest-unhealthy-in-presence-of-expired-and-unhealthy-instances"
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-5" :status-tags] conj :expired))}
                     {:expected [instance-2]
                      :name "find-instance-to-offer:select-idle-expired-instance-with-oldest-load-balancing"
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired))}
                     {:expected [instance-2]
                      :load-balancing :youngest
                      :name "find-instance-to-offer:select-idle-expired-instance-with-youngest-load-balancing"
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired))}
                     {:expected [instance-6]
                      :name "find-instance-to-offer:oldest-idle-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-linger}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-2"] assoc :slots-used 1)
                                            (update-in ["inst-6" :status-tags] conj :expired)
                                            (update-in ["inst-8" :status-tags] conj :expired))}
                     {:expected nil
                      :name "find-instance-to-offer:no-healthy-instance-in-presence-of-busy-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-active}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-2"] assoc :slots-used 1))}
                     {:expected [instance-2]
                      :name "find-instance-to-offer:no-healthy-instance-in-presence-of-lingering-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-linger}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-2"] assoc :slots-used 1))}
                     {:expected [instance-6]
                      :name "find-instance-to-offer:oldest-idle-expired-instance-in-presence-of-lingering-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-linger}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-6" :status-tags] conj :expired)
                                            (update-in ["inst-8" :status-tags] conj :expired)
                                            (update-in ["inst-2"] assoc :slots-used 1))}
                     {:exclude-ids-set #{"inst-1" "inst-2" "inst-3"}
                      :expected [instance-6]
                      :name "find-instance-to-offer:oldest-acceptable-idle-expired-instance-in-presence-of-lingering-expired-instance"
                      :reason :kill-instance
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-6" :status-tags] conj :expired)
                                            (update-in ["inst-8" :status-tags] conj :expired))}
                     {:expected [instance-2]
                      :name "find-instance-to-offer:oldest-expired-instance-in-presence-of-lingering-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request :time time-linger}}
                                                                "inst-6" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request :time time-linger}}
                                                                "inst-8" {"req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request :time time-linger}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-6" :status-tags] conj :expired)
                                            (update-in ["inst-8" :status-tags] conj :expired)
                                            (update-in ["inst-2"] assoc :slots-used 1)
                                            (update-in ["inst-6"] assoc :slots-used 1)
                                            (update-in ["inst-8"] assoc :slots-used 1))}
                     {:expected [instance-6]
                      :name "find-instance-to-offer:youngest-healthy-ejected-instance-in-presence-of-busy-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-active}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids [])
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-2"] assoc :slots-used 1)
                                            (update-in ["inst-5" :status-tags] conj :ejected)
                                            (update-in ["inst-6" :status-tags] conj :ejected))}
                     {:expected [instance-7]
                      :name "find-instance-to-offer:youngest-unhealthy-instance-in-presence-of-busy-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-active}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-2"] assoc :slots-used 1))}
                     {:expected [instance-4]
                      :name "find-instance-to-offer:youngest-unhealthy-unlocked-instance-in-presence-of-busy-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-active}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-7" :status-tags] conj :locked)
                                            (update-in ["inst-2"] assoc :slots-used 1))}
                     {:expected [instance-4]
                      :name "find-instance-to-offer:youngest-unhealthy-live-instance-in-presence-of-busy-expired-instance"
                      :reason :kill-instance
                      :instance-id->request-id->use-reason-map {"inst-2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-active}}}
                      :instance-id->state (-> (instance-id->state-fn healthy-instance-ids unhealthy-instance-ids)
                                            (update-in ["inst-2" :status-tags] conj :expired)
                                            (update-in ["inst-7" :status-tags] conj :killed)
                                            (update-in ["inst-2"] assoc :slots-used 1))}

                     {:expected [instance-3]
                      :name "find-instance-to-offer:only-healthy-and-unknown-ejected-instance"
                      :reason :kill-instance
                      :id->instance (pc/map-from-vals :id [instance-2 instance-3])
                      :instance-id->request-id->use-reason-map {}
                      :instance-id->state (-> (instance-id->state-fn (map :id [instance-2 instance-3 instance-5]) [])
                                            (update-in ["inst-5" :status-tags] (constantly #{:ejected}))
                                            (update-in ["inst-5"] assoc :slots-assigned 0))}
                     )]
    (doseq [{:keys [exclude-ids-set expected id->instance instance-id->request-id->use-reason-map
                    instance-id->state load-balancing name reason sorted-instance-ids]} test-cases]
      (testing (str "Test " name)
        (with-redefs [t/now (constantly current-time)]
          (let [exclude-ids-set (or exclude-ids-set #{})
                id->instance (or id->instance all-id->instance)
                load-balancing (or load-balancing :oldest)
                sorted-instance-ids (or sorted-instance-ids
                                        (let [expired-instance-ids (->> instance-id->state
                                                                     (filter (fn [[_ state]] (expired? state)))
                                                                     (map first)
                                                                     set)]
                                          (->> (keys instance-id->state)
                                            (map id->instance)
                                            (sort-instances-for-processing expired-instance-ids)
                                            (map :id))))
                acceptable-instance-id? (fn [instance-id] (not (contains? exclude-ids-set instance-id)))
                instance-id->request-id->use-reason-map (or instance-id->request-id->use-reason-map {})
                actual (if (= :kill-instance reason)
                         (find-killable-instance id->instance instance-id->state acceptable-instance-id?
                                                 instance-id->request-id->use-reason-map load-balancing
                                                 lingering-request-threshold-ms)
                         (find-available-instance sorted-instance-ids id->instance instance-id->state acceptable-instance-id? first))]
            (when (or (and (nil? expected) (not (nil? actual)))
                      (and expected (not-any? #(= actual %) expected)))
              (println name)
              (doseq [[k v] (into (sorted-map) instance-id->state)] (println "  " k "=>" v))
              (println "  Expected: " expected ", Actual: " actual))
            (when (nil? expected)
              (is (nil? actual)))
            (when-not (nil? expected)
              (is (not (contains? exclude-ids-set (:id actual))))
              (if (= 1 (count expected))
                (is (= (first expected) actual))
                (is (some #(= actual %) expected))))))))))

(let [service-id "s1"
      {:keys [eject-backoff-base-time-ms lingering-request-threshold-ms max-eject-time-ms] :as timeout-config}
      {:eject-backoff-base-time-ms 10000.0 :lingering-request-threshold-ms 60000 :max-eject-time-ms 100000}
      id-counter (atom 0)]

  (defn- make-queue [items]
    (apply conj (PersistentQueue/EMPTY) items))

  (defn- retrieve-channel-config []
    {:eject-instance-chan (async/chan 1)
     :exit-chan (async/chan 1)
     :kill-instance-chan (async/chan 1)
     :query-state-chan (async/chan 1)
     :release-instance-chan (async/chan 1)
     :reserve-instance-chan (async/chan 1)
     :scaling-state-chan (async/chan 1)
     :uneject-instance-chan (async/chan 10)
     :update-state-chan (async/chan 1)
     :work-stealing-chan (async/chan 1)})

  (defn- assert-instance-counters [slot-counters-map]
    (doseq [[counter-name expected-counter-value] (seq slot-counters-map)]
      (let [actual-counter-value (counters/value (metrics/service-counter service-id "instance-counts" counter-name))]
        (when (not= expected-counter-value actual-counter-value)
          (println (first *testing-vars*) ":" counter-name "expected:" expected-counter-value "actual:" actual-counter-value))
        (is (= expected-counter-value actual-counter-value)
            (str "Mismatch in " counter-name " counter value. Expected: " expected-counter-value " Actual: " actual-counter-value)))))

  (defn- check-state-fn [query-state-chan expected-state]
    (Thread/sleep 1) ; allow previous chaneel messages to get processed
    (let [query-state-response-chan (async/promise-chan)]
      (async/>!! query-state-chan {:cid "cid" :response-chan query-state-response-chan :service-id service-id})
      (let [actual-state (async/<!! query-state-response-chan)
            check-fn (fn [item-key]
                       (let [expected (item-key expected-state)
                             actual (item-key actual-state)]
                         (when (contains? expected-state item-key)
                           (when (not= expected actual)
                             (let [sanitize-data-fn (fn [data]
                                                      (cond->> data
                                                        (map? data) (into (sorted-map))
                                                        (instance? PersistentQueue data) (vec)))]
                               (println (first *testing-vars*) ":" (name item-key))
                               (println "Expected: " (sanitize-data-fn expected))
                               (println "Actual:   " (sanitize-data-fn actual))))
                           (is (= expected actual) (str "Checking: " (name item-key))))))]
        (check-fn :deployment-error)
        (check-fn :id->instance)
        (check-fn :instance-id->eject-expiry-time)
        (check-fn :instance-id->request-id->use-reason-map)
        (check-fn :instance-id->consecutive-failures)
        (check-fn :instance-id->state)
        (check-fn :load-balancing)
        (check-fn :request-id->work-stealer)
        (check-fn :sorted-instance-ids)
        (check-fn :work-stealing-queue)
        (let [expected-counter-map (cond-> {}
                                     (:instance-id->eject-expiry-time expected-state)
                                     (assoc "ejected" (count (:instance-id->eject-expiry-time expected-state)))
                                     (:instance-id->state expected-state)
                                     (merge (let [[slots-assigned slots-used slots-available] (compute-slots-values (:instance-id->state expected-state))]
                                              {"slots-assigned" slots-assigned "slots-available" slots-available "slots-in-use" slots-used})))]
          (log/info "expected counters" expected-counter-map)
          (assert-instance-counters expected-counter-map))
        actual-state)))

  (defn- update-slot-state-fn
    ([slot-state instance-id slots-assigned slots-used]
     (update-slot-state-fn slot-state instance-id slots-assigned slots-used #{:healthy}))
    ([slot-state instance-id slots-assigned slots-used status-tags]
     (assoc slot-state instance-id {:slots-assigned slots-assigned
                                    :slots-used slots-used
                                    :status-tags status-tags})))

  (defn- check-reserve-request-instance-fn [request-instance-chan expected-result &
                                            {:keys [exclude-ids-set expect-deadlock]
                                             :or {exclude-ids-set #{} expect-deadlock false}}]
    (swap! id-counter inc)
    (let [reserve-instance-response-chan (async/promise-chan)]
      (async/>!! request-instance-chan [{:cid (str "cid-" @id-counter)
                                         :reason :serve-request
                                         :request-id (str "req-" @id-counter)}
                                        reserve-instance-response-chan
                                        exclude-ids-set])
      (let [reserved-result (if expect-deadlock
                              (async/alt!! reserve-instance-response-chan ([instance] instance)
                                           (async/timeout 500) :no-matching-instance-found)
                              (async/<!! reserve-instance-response-chan))]
        (if (keyword? expected-result)
          (is (= expected-result reserved-result))
          (let [actual-result (select-keys reserved-result [:id])]
            (when (not= {:id expected-result} actual-result)
              (println (first *testing-contexts*) "check-request-instance-fn:"
                       "Expected: " {:id expected-result} "Actual:   " actual-result))
            (is (= {:id expected-result} actual-result) (str "Error in requesting instance for cid-" @id-counter)))))))

  (defn- check-kill-request-instance-fn [request-instance-chan expected-result &
                                         {:keys [exclude-ids-set expect-deadlock]
                                          :or {exclude-ids-set #{} expect-deadlock false}}]
    (swap! id-counter inc)
    (let [kill-instance-response-chan (async/promise-chan)]
      (async/>!! request-instance-chan [{:cid (str "cid-" @id-counter)
                                         :reason :kill-instance
                                         :request-id (str "req-" @id-counter)}
                                        kill-instance-response-chan
                                        exclude-ids-set])
      (let [reserved-result (if expect-deadlock
                              (async/alt!! kill-instance-response-chan ([instance] instance)
                                           (async/timeout 500) :no-matching-instance-found)
                              (async/<!! kill-instance-response-chan))]
        (if (keyword? expected-result)
          (is (= expected-result reserved-result))
          (let [expected-result {:id expected-result}
                actual-result (select-keys reserved-result [:id])]
            (when (not= expected-result actual-result)
              (println (first *testing-contexts*) "check-request-instance-fn:"
                       "Expected: " expected-result "Actual:   " actual-result))
            (is (= expected-result actual-result) (str "Error in requesting instance for cid-" @id-counter)))))))

  (defn- make-work-stealing-offer [work-stealing-chan router-id instance-id]
    (let [response-chan (async/promise-chan)]
      (swap! id-counter inc)
      (async/>!! work-stealing-chan {:cid (str "cid-" @id-counter)
                                     :instance {:id instance-id}
                                     :response-chan response-chan
                                     :router-id router-id})
      response-chan))

  (defn- make-work-stealing-data [cid instance-id response-chan router-id]
    {:cid cid :instance {:id instance-id} :response-chan response-chan :router-id router-id})

  (defn- release-instance-fn [release-instance-chan instance-id id status]
    (async/>!! release-instance-chan [{:id instance-id} {:cid (str "cid-" id) :request-id (str "req-" id) :status status}]))

  (defn- check-eject-instance-fn [eject-instance-chan instance-id expected-result]
    (let [eject-instance-response-chan (async/promise-chan)]
      (async/>!! eject-instance-chan [{:instance-id instance-id
                                           :eject-period-ms eject-backoff-base-time-ms
                                           :cid "cid"}
                                          eject-instance-response-chan])
      (let [response (async/<!! eject-instance-response-chan)]
        (when (not= expected-result response)
          (println "Expected:" expected-result " actual:" response))
        (is (= expected-result response))
        response)))

  (defn- launch-service-chan-responder [id-counter-value initial-state]
    (reset! id-counter id-counter-value)
    (let [initial-state (utils/assoc-if-absent initial-state :load-balancing :oldest)
          channel-config (retrieve-channel-config)
          trigger-uneject-process-atom (atom {})
          trigger-uneject-process-fn (fn [_ instance-id eject-period-ms _]
                                           (swap! trigger-uneject-process-atom assoc instance-id eject-period-ms))]
      (let [slots-assigned-counter (metrics/service-counter service-id "instance-counts" "slots-assigned")
            slots-available-counter (metrics/service-counter service-id "instance-counts" "slots-available")
            slots-in-use-counter (metrics/service-counter service-id "instance-counts" "slots-in-use")
            ejected-instance-counter (metrics/service-counter service-id "instance-counts" "ejected")
            in-use-instance-counter (metrics/service-counter service-id "instance-counts" "in-use")
            work-stealing-received-in-flight-counter (metrics/service-counter service-id "work-stealing" "received-from" "in-flight")]
        (update-slots-metrics (:instance-id->state initial-state) slots-assigned-counter slots-available-counter slots-in-use-counter)
        (metrics/reset-counter ejected-instance-counter (count (:instance-id->eject-expiry-time initial-state)))
        (metrics/reset-counter in-use-instance-counter (count (:instance-id->request-id->use-reason-map initial-state)))
        (metrics/reset-counter work-stealing-received-in-flight-counter
                               (+ (count (:work-stealing-queue initial-state)) (count (:request-id->work-stealer initial-state)))))
      ;; start the service-chan-responder
      (start-service-chan-responder service-id trigger-uneject-process-fn timeout-config channel-config initial-state)
      (assoc channel-config :trigger-uneject-process-atom trigger-uneject-process-atom)))

  (let [instance-h1 {:id "s1.h1" :started-at (DateTime. 10000)}
        instance-h2 {:id "s1.h2" :started-at (DateTime. 20000)}
        instance-h3 {:id "s1.h3" :started-at (DateTime. 30000)}
        instance-h4 {:id "s1.h4" :started-at (DateTime. 40000)}
        instance-h5 {:id "s1.h5" :started-at (DateTime. 50000)}
        instance-h6 {:id "s1.h6" :started-at (DateTime. 60000)}
        instance-u1 {:id "s1.u1" :started-at (DateTime. 11000)}
        instance-u2 {:id "s1.u2" :started-at (DateTime. 21000)}
        instance-u3 {:id "s1.u3" :started-at (DateTime. 31000)}
        id->instance-data {"s1.h1" instance-h1
                           "s1.h2" instance-h2
                           "s1.h3" instance-h3
                           "s1.h4" instance-h4
                           "s1.h5" instance-h5
                           "s1.h6" instance-h6
                           "s1.u1" instance-u1
                           "s1.u2" instance-u2
                           "s1.u3" instance-u3}]

    (deftest test-start-service-chan-responder-simple-state-updates
      (let [{:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3]
                            :unhealthy-instances [instance-u1 instance-u2 instance-u3]
                            :starting-instances [instance-u3]
                            :expired-instances [instance-h1 instance-h3]
                            :my-instance->slots {instance-h1 1 instance-h2 1 instance-h3 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.h2" 1 0)
                                                                (update-slot-state-fn "s1.h3" 1 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                                                (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy})
                                                                (update-slot-state-fn "s1.u3" 0 0 #{:starting :unhealthy}))
                                          :load-balancing :oldest
                                          :sorted-instance-ids ["s1.u1" "s1.h2" "s1.u2" "s1.u3"
                                                                "s1.h3" "s1.h1"]})
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3 instance-h4 instance-h5]
                            :unhealthy-instances [instance-u1 instance-u3] ; drop s1.u2 from update
                            :starting-instances [] ; remove s1.u3 from starting
                            :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.u1"]
                            :my-instance->slots {instance-h1 2 instance-h2 1 instance-h3 2 instance-h4 2}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 2 0 #{:healthy})
                                                                (update-slot-state-fn "s1.h2" 1 0)
                                                                (update-slot-state-fn "s1.h3" 2 0)
                                                                (update-slot-state-fn "s1.h4" 2 0)
                                                                (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                                                (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                                          :load-balancing :oldest
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["s1.h1" "s1.u1" "s1.h2" "s1.h3"
                                                                "s1.u3" "s1.h4" "s1.h5"]
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-deployment-errors ; tests to make sure deployment errors are updated correctly
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        (doseq [deployment-error [:authentication-required :bad-startup-command :health-check-misconfigured :not-enough-memory nil]]
          ; update state and verify whether state changes are reflected correctly
          (let [update-state {:deployment-error deployment-error
                              :healthy-instances []
                              :unhealthy-instances [instance-u1 instance-u2 instance-u3]
                              :starting-instances []
                              :expired-instances []
                              :my-instance->slots {}}]
            (async/>!! update-state-chan [update-state (t/now)]))
          (check-state-fn query-state-chan {:deployment-error deployment-error
                                            :instance-id->eject-expiry-time {}
                                            :instance-id->request-id->use-reason-map {}
                                            :instance-id->consecutive-failures {}
                                            :instance-id->state (-> {}
                                                                  (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                                            :load-balancing :oldest
                                            :request-id->work-stealer {}
                                            :sorted-instance-ids ["s1.u1" "s1.u2" "s1.u3"]
                                            :work-stealing-queue (make-queue [])})
          ; attempt to reserve an instances
          (if deployment-error
            (check-reserve-request-instance-fn reserve-instance-chan deployment-error) ; chanel should be open only when there are deployment errors
            (check-reserve-request-instance-fn reserve-instance-chan :no-matching-instance-found :expect-deadlock true))
          (check-state-fn query-state-chan {:deployment-error deployment-error
                                            :instance-id->eject-expiry-time {}
                                            :instance-id->request-id->use-reason-map {}
                                            :instance-id->consecutive-failures {}
                                            :instance-id->state (-> {}
                                                                  (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                                            :load-balancing :oldest
                                            :request-id->work-stealer {}
                                            :work-stealing-queue (make-queue [])}))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-exclude-expired-instance
      (let [{:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3]
                            :unhealthy-instances [instance-u1 instance-u2]
                            :expired-instances [instance-h1 instance-h3]
                            :my-instance->slots {instance-h1 1 instance-h2 1 instance-h3 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.h2" 1 0)
                                                                (update-slot-state-fn "s1.h3" 1 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                                                (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy}))
                                          :load-balancing :oldest
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["s1.u1" "s1.h2" "s1.u2" "s1.h3" "s1.h1"]
                                          :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h4 instance-h5]
                            :unhealthy-instances [instance-u1] ; drop s1.u2 from update
                            :sorted-instance-ids ["s1.h1" "s1.u1" "s1.h2" "s1.h4" "s1.h5"]
                            :my-instance->slots {instance-h1 2 instance-h2 1 instance-h4 2}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 2 0 #{:healthy})
                                                                (update-slot-state-fn "s1.h2" 1 0)
                                                                (update-slot-state-fn "s1.h4" 2 0)
                                                                (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy}))
                                          :load-balancing :oldest
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["s1.h1" "s1.u1" "s1.h2" "s1.h4" "s1.h5"]
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-eject-expired-instance
      (let [{:keys [exit-chan query-state-chan update-state-chan eject-instance-chan trigger-uneject-process-atom uneject-instance-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3]
                            :unhealthy-instances [instance-u1]
                            :expired-instances [instance-h1 instance-h3]
                            :my-instance->slots {instance-h1 1 instance-h2 1 instance-h3 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.h2" 1 0)
                                                                (update-slot-state-fn "s1.h3" 1 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy}))
                                          :load-balancing :oldest
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["s1.h2" ; healthy
                                                                "s1.u1" ; unhealthy
                                                                "s1.h1" "s1.h3" ; expired
                                                                ]
                                          :work-stealing-queue (make-queue [])})
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (check-eject-instance-fn eject-instance-chan "s1.h1" :ejected)
            (check-eject-instance-fn eject-instance-chan "s1.h3" :ejected)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {"s1.h1" (t/plus start-time (t/millis eject-backoff-base-time-ms))
                                                                  "s1.h3" (t/plus start-time (t/millis eject-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {}
                             :instance-id->consecutive-failures {}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 0 #{:ejected :expired :healthy})
                                                   (update-slot-state-fn "s1.h2" 1 0)
                                                   (update-slot-state-fn "s1.h3" 1 0 #{:ejected :expired :healthy})
                                                   (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy}))})
            (check-eject-instance-fn eject-instance-chan "s1.h4" :ejected)
            (let [update-state {:healthy-instances [instance-h1 instance-h2]
                                :unhealthy-instances [instance-u1]
                                :expired-instances [instance-h1 instance-h4]
                                :my-instance->slots {instance-h1 1 instance-h2 1}}]
              (async/>!! update-state-chan [update-state (t/now)]))
            (let [expiry-time (t/plus start-time (t/millis eject-backoff-base-time-ms))]
              (check-state-fn query-state-chan
                              {:instance-id->eject-expiry-time {"s1.h1" expiry-time
                                                                    "s1.h3" expiry-time
                                                                    "s1.h4" expiry-time}
                               :instance-id->request-id->use-reason-map {}
                               :instance-id->consecutive-failures {}
                               :instance-id->state (-> {}
                                                     (update-slot-state-fn "s1.h1" 1 0 #{:ejected :expired :healthy})
                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                     (update-slot-state-fn "s1.h3" 0 0 #{:ejected})
                                                     (update-slot-state-fn "s1.h4" 0 0 #{:ejected :expired})
                                                     (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy}))})
              (is (= {"s1.h1" eject-backoff-base-time-ms
                      "s1.h3" eject-backoff-base-time-ms
                      "s1.h4" eject-backoff-base-time-ms}
                     @trigger-uneject-process-atom)))
            ; clear the eject buffer
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-eject-time-ms))))
              (async/>!! uneject-instance-chan {:instance-id "s1.h1"})
              (async/>!! uneject-instance-chan {:instance-id "s1.h3"})
              (async/>!! uneject-instance-chan {:instance-id "s1.h4"}))
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {}
                             :instance-id->request-id->use-reason-map {}
                             :instance-id->consecutive-failures {}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 0 #{:expired :healthy})
                                                   (update-slot-state-fn "s1.h2" 1 0)
                                                   (update-slot-state-fn "s1.h3" 0 0 #{})
                                                   (update-slot-state-fn "s1.h4" 0 0 #{:expired})
                                                   (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy}))
                             :work-stealing-queue (make-queue [])}))
          (let [update-state {:healthy-instances [instance-h2]
                              :unhealthy-instances []
                              :expired-instances []
                              :my-instance->slots {instance-h2 1}}]
            (async/>!! update-state-chan [update-state (t/now)]))
          (check-state-fn query-state-chan
                          {:instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (update-slot-state-fn {} "s1.h2" 1 0)})
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-simple-state-updates-with-reserved-kill
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3]
                            :unhealthy-instances []
                            :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3"]
                            :expired-instances [instance-h2]
                            :my-instance->slots {instance-h1 1 instance-h2 2 instance-h3 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0)
                                                                (update-slot-state-fn "s1.h2" 2 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.h3" 1 0))})
        (check-kill-request-instance-fn kill-instance-chan "s1.h2")
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"s1.h2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :kill-instance}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0)
                                                                (update-slot-state-fn "s1.h2" 2 0 #{:expired :healthy :locked})
                                                                (update-slot-state-fn "s1.h3" 1 0))
                                          :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3 instance-h4 instance-h5]
                            :unhealthy-instances []
                            :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5"]
                            :expired-instances [instance-h2]
                            :my-instance->slots {instance-h1 1 instance-h2 4 instance-h3 4}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"s1.h2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :kill-instance}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0)
                                                                (update-slot-state-fn "s1.h2" 4 0 #{:expired :healthy :locked})
                                                                (update-slot-state-fn "s1.h3" 4 0))
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3 instance-h4 instance-h5]
                            :unhealthy-instances []
                            :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5"]
                            :expired-instances [instance-h2]
                            :my-instance->slots {instance-h1 1 instance-h2 4 instance-h3 2}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"s1.h2" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :kill-instance}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0)
                                                                (update-slot-state-fn "s1.h2" 4 0 #{:expired :healthy :locked})
                                                                (update-slot-state-fn "s1.h3" 2 0))})
        (release-instance-fn release-instance-chan "s1.h2" 1 :not-killed)
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 1 0)
                                                                (update-slot-state-fn "s1.h2" 4 0 #{:expired :healthy})
                                                                (update-slot-state-fn "s1.h3" 2 0))
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-reserve-instances
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan]}
            (launch-service-chan-responder 0 {:id->instance id->instance-data
                                              :instance-id->eject-expiry-time {}
                                              :instance-id->request-id->use-reason-map {}
                                              :instance-id->consecutive-failures {}
                                              :instance-id->state (-> {}
                                                                    (update-slot-state-fn "s1.h1" 2 0)
                                                                    (update-slot-state-fn "s1.h2" 1 0)
                                                                    (update-slot-state-fn "s1.h3" 2 0)
                                                                    (update-slot-state-fn "s1.h4" 2 0))
                                              :request-id->work-stealer {}
                                              :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4"]
                                              :work-stealing-queue (make-queue [])})]
        ; reserve a few instances
        (doseq [instance-id ["s1.h1" "s1.h1" "s1.h2" "s1.h3" "s1.h3" "s1.h4"]]
          (check-reserve-request-instance-fn reserve-instance-chan instance-id))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                             "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                                    "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                                    "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                             "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                                    "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 2 2)
                                                                (update-slot-state-fn "s1.h2" 1 1)
                                                                (update-slot-state-fn "s1.h3" 2 2)
                                                                (update-slot-state-fn "s1.h4" 2 1))
                                          :load-balancing :oldest
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-reserve-expired-instance
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 0 {:id->instance id->instance-data
                                              :instance-id->eject-expiry-time {}
                                              :instance-id->request-id->use-reason-map {}
                                              :instance-id->consecutive-failures {}
                                              :instance-id->state (-> {}
                                                                    (update-slot-state-fn "s1.h1" 2 0)
                                                                    (update-slot-state-fn "s1.h2" 1 0))})]
        (let [update-state {:healthy-instances [instance-h1 instance-h2]
                            :unhealthy-instances []
                            :expired-instances [instance-h2]
                            :my-instance->slots {instance-h1 2 instance-h2 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        ; reserve expired instance
        (doseq [instance-id ["s1.h1" "s1.h1" "s1.h2"]]
          (check-reserve-request-instance-fn reserve-instance-chan instance-id))
        (check-state-fn query-state-chan {:instance-id->eject-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                             "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                                    "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                (update-slot-state-fn "s1.h1" 2 2)
                                                                (update-slot-state-fn "s1.h2" 1 1 #{:expired :healthy}))
                                          :load-balancing :oldest
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-slot-state-consistency
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 6 {:id->instance id->instance-data
                                              :instance-id->eject-expiry-time {}
                                              :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                                 "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                                        "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                                        "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                                 "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                                        "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}}
                                              :instance-id->consecutive-failures {}
                                              :instance-id->state (-> {}
                                                                    (update-slot-state-fn "s1.h1" 2 2)
                                                                    (update-slot-state-fn "s1.h2" 1 1)
                                                                    (update-slot-state-fn "s1.h3" 2 2)
                                                                    (update-slot-state-fn "s1.h4" 2 1))
                                              :request-id->work-stealer {}
                                              :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4"]
                                              :work-stealing-queue (make-queue [])})]
        ; give fewer slots to some instances and verify the slot state does not change
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3]
                            :unhealthy-instances [instance-u1 instance-u2]
                            :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u1" "s1.u2"]
                            :my-instance->slots {instance-h1 1 instance-h2 1 instance-h3 8}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (doseq [instance-id ["s1.h3" "s1.h3"]]
          (check-reserve-request-instance-fn reserve-instance-chan instance-id))
        ; trying to kill an instance will always give the same unhealthy instance we rely on state updates to lose the unhealthy instance
        (check-kill-request-instance-fn kill-instance-chan "s1.u2")
        (release-instance-fn release-instance-chan "s1.u2" 9 :not-killed)
        (check-state-fn query-state-chan {}) ;; ensure the release is executed
        (check-kill-request-instance-fn kill-instance-chan "s1.u2")
        (check-kill-request-instance-fn kill-instance-chan "s1.u1" :exclude-ids-set #{"s1.u2"})
        ; check that state is still as expected
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                            "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                   "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                   "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                            "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                            "req-7" {:cid "cid-7" :request-id "req-7" :reason :serve-request}
                                                                            "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}
                                                                   "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}
                                                                   "s1.u1" {"req-11" {:cid "cid-11" :request-id "req-11" :reason :kill-instance}}
                                                                   "s1.u2" {"req-10" {:cid "cid-10" :request-id "req-10" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 2)
                                               (update-slot-state-fn "s1.h2" 1 1)
                                               (update-slot-state-fn "s1.h3" 8 4)
                                               (update-slot-state-fn "s1.h4" 0 1 #{})
                                               (update-slot-state-fn "s1.u1" 0 0 #{:locked :unhealthy})
                                               (update-slot-state-fn "s1.u2" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-killed-reservation
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan trigger-uneject-process-atom uneject-instance-chan]}
            (launch-service-chan-responder 11 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                                  "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                                         "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                                         "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                                  "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                                  "req-7" {:cid "cid-7" :request-id "req-7" :reason :serve-request}
                                                                                                  "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}
                                                                                         "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}
                                                                                         "s1.u2" {"req-10" {:cid "cid-10" :request-id "req-10" :reason :kill-instance}}
                                                                                         "s1.u1" {"req-11" {:cid "cid-11" :request-id "req-11" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 2)
                                                                     (update-slot-state-fn "s1.h2" 1 1)
                                                                     (update-slot-state-fn "s1.h3" 8 4)
                                                                     (update-slot-state-fn "s1.h4" 0 1 #{})
                                                                     (update-slot-state-fn "s1.u1" 0 0 #{:locked :unhealthy})
                                                                     (update-slot-state-fn "s1.u2" 0 0 #{:locked :unhealthy}))
                                               :request-id->work-stealer {}
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u1" "s1.u2"]
                                               :work-stealing-queue (make-queue [])})]
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (release-instance-fn release-instance-chan "s1.u2" 10 :killed)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {"s1.u2" (t/plus start-time (t/millis max-eject-time-ms))}
                             :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                       "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                       "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                "req-7" {:cid "cid-7" :request-id "req-7" :reason :serve-request}
                                                                                "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}
                                                                       "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}
                                                                       "s1.u1" {"req-11" {:cid "cid-11" :request-id "req-11" :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"s1.u2" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 2)
                                                   (update-slot-state-fn "s1.h2" 1 1)
                                                   (update-slot-state-fn "s1.h3" 8 4)
                                                   (update-slot-state-fn "s1.h4" 0 1 #{})
                                                   (update-slot-state-fn "s1.u1" 0 0 #{:locked :unhealthy})
                                                   (update-slot-state-fn "s1.u2" 0 0 #{:ejected :killed :unhealthy}))
                             :load-balancing :oldest
                             :request-id->work-stealer {}
                             :work-stealing-queue (make-queue [])})
            (is (= {"s1.u2" max-eject-time-ms}
                   @trigger-uneject-process-atom))
            (release-instance-fn release-instance-chan "s1.u1" 11 :killed)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {"s1.u1" (t/plus start-time (t/millis max-eject-time-ms))
                                                                  "s1.u2" (t/plus start-time (t/millis max-eject-time-ms))}
                             :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                       "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                       "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                "req-7" {:cid "cid-7" :request-id "req-7" :reason :serve-request}
                                                                                "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}
                                                                       "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}}
                             :instance-id->consecutive-failures {"s1.u1" 1 "s1.u2" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 2)
                                                   (update-slot-state-fn "s1.h2" 1 1)
                                                   (update-slot-state-fn "s1.h3" 8 4)
                                                   (update-slot-state-fn "s1.h4" 0 1 #{})
                                                   (update-slot-state-fn "s1.u1" 0 0 #{:ejected :killed :unhealthy})
                                                   (update-slot-state-fn "s1.u2" 0 0 #{:ejected :killed :unhealthy}))
                             :request-id->work-stealer {}
                             :work-stealing-queue (make-queue [])})
            (is (= {"s1.u1" max-eject-time-ms
                    "s1.u2" max-eject-time-ms}
                   @trigger-uneject-process-atom))
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (+ 1000000 max-eject-time-ms))))
              ; no more unhealthy instances to kill all healthy instances are busy
              (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found :expect-deadlock true)
              (async/>!! uneject-instance-chan {:instance-id "s1.u1"})
              (async/>!! uneject-instance-chan {:instance-id "s1.u2"}))
            ; ensure eject was cleared due to expiry of period
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {}
                             :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                       "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                       "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                "req-7" {:cid "cid-7" :request-id "req-7" :reason :serve-request}
                                                                                "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}
                                                                       "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}}
                             :instance-id->consecutive-failures {"s1.u1" 1 "s1.u2" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 2)
                                                   (update-slot-state-fn "s1.h2" 1 1)
                                                   (update-slot-state-fn "s1.h3" 8 4)
                                                   (update-slot-state-fn "s1.h4" 0 1 #{})
                                                   (update-slot-state-fn "s1.u1" 0 0 #{:killed :unhealthy})
                                                   (update-slot-state-fn "s1.u2" 0 0 #{:killed :unhealthy}))
                             :load-balancing :oldest
                             :request-id->work-stealer {}
                             :work-stealing-queue (make-queue [])}))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-failed-eject
      (let [{:keys [eject-instance-chan exit-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                                  "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                                         "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                                         "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                                  "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                                  "req-7" {:cid "cid-7" :request-id "req-7" :reason :serve-request}
                                                                                                  "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}
                                                                                         "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"s1.u2" 1 "s1.u1" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 2)
                                                                     (update-slot-state-fn "s1.h2" 1 1)
                                                                     (update-slot-state-fn "s1.h3" 8 4)
                                                                     (update-slot-state-fn "s1.h4" 0 1 #{})
                                                                     (update-slot-state-fn "s1.u1" 0 0 #{:killed :unhealthy})
                                                                     (update-slot-state-fn "s1.u2" 0 0 #{:killed :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u1" "s1.u2"]})]
        ; try ejecting an instance in-use
        (check-eject-instance-fn eject-instance-chan "s1.h1" :in-use)
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-success-instances
      (let [{:keys [exit-chan query-state-chan release-instance-chan update-state-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}
                                                                                                  "req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request}}
                                                                                         "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                                         "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                                  "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                                  "req-7" {:cid "cid-7" :request-id "req-7" :reason :serve-request}
                                                                                                  "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}
                                                                                         "s1.h4" {"req-6" {:cid "cid-6" :request-id "req-6" :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"s1.u2" 1 "s1.u1" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 2)
                                                                     (update-slot-state-fn "s1.h2" 1 1)
                                                                     (update-slot-state-fn "s1.h3" 8 4)
                                                                     (update-slot-state-fn "s1.h4" 0 1 #{})
                                                                     (update-slot-state-fn "s1.u1" 0 0 #{:killed :unhealthy})
                                                                     (update-slot-state-fn "s1.u2" 0 0 #{:killed :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u1" "s1.u2"]})]
        ; update state with newer unhealthy-instance
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h3 instance-h4 instance-h5]
                            :unhealthy-instances [instance-u3]
                            :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.u3"]
                            :my-instance->slots {instance-h1 1 instance-h2 1 instance-h3 8}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        ; release a few slots and check state
        (release-instance-fn release-instance-chan "s1.h1" 2 :success)
        (release-instance-fn release-instance-chan "s1.h2" 3 :success)
        (release-instance-fn release-instance-chan "s1.h3" 7 :success)
        (release-instance-fn release-instance-chan "s1.h4" 6 :success)
        ; call release with faulty arguments
        (release-instance-fn release-instance-chan "s1.h3" 107 :success)
        (release-instance-fn release-instance-chan "s1.h4" 106 :success)
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                   "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                            "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                            "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}}
                         :instance-id->consecutive-failures {"s1.u1" 1 "s1.u2" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 1)
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 8 3)
                                               (update-slot-state-fn "s1.h4" 0 0 #{:healthy}) ;; since the response was a success
                                               (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                         :load-balancing :oldest
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-make-in-use-instance-unhealthy
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan update-state-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                                         "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                                  "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                                  "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"s1.u2" 1 "s1.u1" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 1)
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 3)
                                                                     (update-slot-state-fn "s1.h4" 0 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u1" "s1.u2" "s1.u3"]})]
        ; s1.h3 now becomes unhealthy
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h4 instance-h5]
                            :unhealthy-instances [instance-h3 instance-u3]
                            :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.u3"]
                            :my-instance->slots {instance-h1 1 instance-h2 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                   "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                            "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                            "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 1)
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 0 3 #{:unhealthy})
                                               (update-slot-state-fn "s1.h4" 0 0 #{:healthy}) ;; since the response was a success
                                               (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                         :load-balancing :oldest
                         :work-stealing-queue (make-queue [])})
        ; release requests for s1.h3
        (release-instance-fn release-instance-chan "s1.h3" 5 :success)
        (release-instance-fn release-instance-chan "s1.h3" 8 :success)
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                   "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 1)
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 0 1 #{:unhealthy})
                                               (update-slot-state-fn "s1.h4" 0 0 #{:healthy}) ;; since the response was a success
                                               (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        ; s1.h3 should not show up in a kill-instance reservation
        (check-kill-request-instance-fn kill-instance-chan "s1.u3")
        (check-kill-request-instance-fn kill-instance-chan "s1.h4")
        (check-kill-request-instance-fn kill-instance-chan "s1.h2")
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                   "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :kill-instance}}
                                                                   "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}}
                                                                   "s1.h4" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 1)
                                               (update-slot-state-fn "s1.h2" 1 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.h3" 0 1 #{:unhealthy})
                                               (update-slot-state-fn "s1.h4" 0 0 #{:healthy :locked}) ;; since the response was a success
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-failed-instances
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan trigger-uneject-process-atom uneject-instance-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                                         "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                                                  "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}
                                                                                                  "req-8" {:cid "cid-8" :request-id "req-8" :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"s1.u2" 1 "s1.u1" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 1)
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 3)
                                                                     (update-slot-state-fn "s1.h4" 0 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u3"]})]
        ; fail remaining requests except 2 and explicitly eject another instance
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (release-instance-fn release-instance-chan "s1.h1" 1 :instance-error)
            (release-instance-fn release-instance-chan "s1.h3" 4 :instance-busy)
            (release-instance-fn release-instance-chan "s1.h3" 8 :instance-busy)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {"s1.h1" (t/plus start-time (t/millis eject-backoff-base-time-ms))
                                                                  "s1.h3" (t/plus start-time (t/millis (* (Math/pow 2 (dec 2)) eject-backoff-base-time-ms)))}
                             :instance-id->request-id->use-reason-map {"s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}}
                             :instance-id->consecutive-failures {"s1.h1" 1 "s1.h3" 2 "s1.u1" 1 "s1.u2" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 0 #{:ejected :healthy})
                                                   (update-slot-state-fn "s1.h2" 1 0)
                                                   (update-slot-state-fn "s1.h3" 8 1 #{:ejected :healthy})
                                                   (update-slot-state-fn "s1.h4" 0 0)
                                                   (update-slot-state-fn "s1.u3" 0 0 #{:unhealthy}))})
            (is (= {"s1.h1" eject-backoff-base-time-ms
                    "s1.h3" (* (Math/pow 2 (dec 2)) eject-backoff-base-time-ms)}
                   @trigger-uneject-process-atom))
            (do
              ; kill unhealthy instance
              (check-kill-request-instance-fn kill-instance-chan "s1.u3")
              (reset! current-time-atom (t/plus start-time (t/millis (* 2 max-eject-time-ms))))
              (async/>!! uneject-instance-chan {:instance-id "s1.h1"})
              (async/>!! uneject-instance-chan {:instance-id "s1.h3"})
              (check-state-fn query-state-chan
                              {:instance-id->eject-expiry-time {}
                               :instance-id->request-id->use-reason-map {"s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.h3" 2 "s1.u1" 1 "s1.u2" 1}
                               :instance-id->state (-> {}
                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                     (update-slot-state-fn "s1.h3" 8 1)
                                                     (update-slot-state-fn "s1.h4" 0 0)
                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                               :load-balancing :oldest
                               :request-id->work-stealer {}
                               :work-stealing-queue (make-queue [])})
              (release-instance-fn release-instance-chan "s1.u3" 13 :killed)))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-kill-known-healthy-instance:even-with-no-slots
      (let [{:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 13 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.u2" 1
                                                                                   "s1.u1" 1
                                                                                   "s1.h1" 1
                                                                                   "s1.h3" 2
                                                                                   "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 1)
                                                                     (update-slot-state-fn "s1.h4" 0 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u3"]})]
        ; kill a healthy instance and clear the eject buffer
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-eject-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "s1.h4")))
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                   "s1.h4" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.h3" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 0)
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 8 1)
                                               (update-slot-state-fn "s1.h4" 0 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-healthy-instance
      (let [{:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 13 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.u2" 1
                                                                                   "s1.u1" 1
                                                                                   "s1.h1" 1
                                                                                   "s1.h3" 2
                                                                                   "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 1)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        ; kill a healthy instance and clear the eject buffer
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-eject-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "s1.h2")))
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h2" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}
                                                                   "s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.h3" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 0)
                                               (update-slot-state-fn "s1.h2" 1 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.h3" 8 1)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-expired-instance-from-other-router
      (let [{:keys [exit-chan kill-instance-chan query-state-chan update-state-chan]}
            (->> {:id->instance id->instance-data
                  :instance-id->eject-expiry-time {}
                  :instance-id->request-id->use-reason-map {"s1.h1" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}}
                  :instance-id->consecutive-failures {"s1.u2" 1
                                                      "s1.u1" 1
                                                      "s1.h1" 1
                                                      "s1.u3" 1}
                  :instance-id->state (-> {}
                                        (update-slot-state-fn "s1.h1" 1 1)
                                        (update-slot-state-fn "s1.h2" 1 0)
                                        (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy})
                                        (update-slot-state-fn "s1.u3" 0 0 #{:starting :unhealthy}))}
              (launch-service-chan-responder 13))]
        ; s1.h3 now becomes expired
        (let [update-state {:healthy-instances [instance-h1 instance-h2]
                            :unhealthy-instances [instance-u2 instance-u3]
                            :starting-instances [instance-u3]
                            :expired-instances [instance-h3 instance-h4]
                            :my-instance->slots {instance-h1 1 instance-h2 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:id->instance (select-keys id->instance-data #{"s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u2" "s1.u3"})
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}}
                         :instance-id->consecutive-failures {"s1.u2" 1
                                                             "s1.h1" 1
                                                             "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 1)
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 0 0 #{:expired})
                                               (update-slot-state-fn "s1.h4" 0 0 #{:expired})
                                               (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy})
                                               (update-slot-state-fn "s1.u3" 0 0 #{:starting :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.u2" "s1.u3" "s1.h4" "s1.h3"]
                         :work-stealing-queue (make-queue [])})
        ; s1.u2 should be killed because it is not starting
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-eject-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "s1.u2")))
        ; s1.u3 becomes healthy
        (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-u3]
                            :expired-instances [instance-h3]
                            :my-instance->slots {instance-h1 1 instance-h2 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        ; expired instance should be killed
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-eject-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "s1.h3")))
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.u2" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}
                                                                   "s1.h3" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :kill-instance}}
                                                                   "s1.h1" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                   }
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 1)
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 0 0 #{:expired :locked})
                                               (update-slot-state-fn "s1.u2" 0 0 #{:locked})
                                               (update-slot-state-fn "s1.u3" 0 0))})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-clear-failures
      (let [{:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}
                                                                                         "s1.h2" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.u2" 1
                                                                                   "s1.u1" 1
                                                                                   "s1.h1" 1
                                                                                   "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                                     (update-slot-state-fn "s1.h2" 1 0 #{:healthy :locked})
                                                                     (update-slot-state-fn "s1.h3" 8 1)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        ; successful release should clear out the failures counter
        (release-instance-fn release-instance-chan "s1.h3" 5 :success)
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h2" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 0)
                                               (update-slot-state-fn "s1.h2" 1 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.h3" 8 0)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-without-killing
      (let [{:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h2" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                                     (update-slot-state-fn "s1.h2" 1 0 #{:healthy :locked})
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        ; release instance without killing
        (release-instance-fn release-instance-chan "s1.h2" 14 :not-killed)
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 0)
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 8 0)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-eject-owned-instance
      (let [{:keys [eject-instance-chan exit-chan query-state-chan release-instance-chan trigger-uneject-process-atom uneject-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :load-balancing :oldest
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        ; try ejecting an instance successfully
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (check-eject-instance-fn eject-instance-chan "s1.h2" :ejected)
            ; repeated call should also succeed
            (check-eject-instance-fn eject-instance-chan "s1.h2" :ejected)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {"s1.h2" (t/plus start-time (t/millis eject-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 0)
                                                   (update-slot-state-fn "s1.h2" 1 0 #{:ejected :healthy})
                                                   (update-slot-state-fn "s1.h3" 8 0)
                                                   (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))})
            (is (= {"s1.h2" eject-backoff-base-time-ms}
                   @trigger-uneject-process-atom))
            ; clear the eject buffer with a dummy state call
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-eject-time-ms))))
              (async/>!! uneject-instance-chan {:instance-id "s1.h2"}))
            ; dummy release call should not throw an error
            (release-instance-fn release-instance-chan "s1.hUnknown" 114 :success)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {}
                             :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 0)
                                                   (update-slot-state-fn "s1.h2" 1 0)
                                                   (update-slot-state-fn "s1.h3" 8 0)
                                                   (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))}))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-eject-external-instance
      (let [{:keys [eject-instance-chan exit-chan query-state-chan release-instance-chan trigger-uneject-process-atom uneject-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :load-balancing :oldest
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        ; try ejecting an external instance successfully
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (check-eject-instance-fn eject-instance-chan "s1.h8" :ejected)
            ; repeated call should also succeed
            (check-eject-instance-fn eject-instance-chan "s1.h9" :ejected)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {"s1.h8" (t/plus start-time (t/millis eject-backoff-base-time-ms))
                                                                  "s1.h9" (t/plus start-time (t/millis eject-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 0)
                                                   (update-slot-state-fn "s1.h2" 1 0 #{:healthy})
                                                   (update-slot-state-fn "s1.h3" 8 0)
                                                   (update-slot-state-fn "s1.h8" 0 0 #{:ejected})
                                                   (update-slot-state-fn "s1.h9" 0 0 #{:ejected})
                                                   (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))})
            (is (= {"s1.h8" eject-backoff-base-time-ms
                    "s1.h9" eject-backoff-base-time-ms}
                   @trigger-uneject-process-atom))
            ; clear the eject buffer with a dummy state call
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-eject-time-ms))))
              (async/>!! uneject-instance-chan {:instance-id "s1.h8"})
              (async/>!! uneject-instance-chan {:instance-id "s1.h9"}))
            ; dummy release call should not throw an error
            (release-instance-fn release-instance-chan "s1.hUnknown" 114 :success)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {}
                             :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 1 0)
                                                   (update-slot-state-fn "s1.h2" 1 0)
                                                   (update-slot-state-fn "s1.h3" 8 0)
                                                   (update-slot-state-fn "s1.h8" 0 0 #{})
                                                   (update-slot-state-fn "s1.h9" 0 0 #{})
                                                   (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                             :load-balancing :oldest
                             :work-stealing-queue (make-queue [])}))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-cause-unowned-instance-eject
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 1 0)
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        ; eject an instance which gets remove from state it should not introduce any errors
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h1")
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h2")
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                   "s1.h2" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 1 1)
                                               (update-slot-state-fn "s1.h2" 1 1)
                                               (update-slot-state-fn "s1.h3" 8 0)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [instance-h2 instance-h3]
                            :unhealthy-instances []
                            :sorted-instance-ids ["s1.h2" "s1.h3"]
                            :my-instance->slots {instance-h2 1 instance-h3 8}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                   "s1.h2" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 0 1 #{})
                                               (update-slot-state-fn "s1.h2" 1 1)
                                               (update-slot-state-fn "s1.h3" 8 0)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-cause-instance-eject
      (let [{:keys [exit-chan query-state-chan release-instance-chan trigger-uneject-process-atom uneject-instance-chan]}
            (launch-service-chan-responder 16 {:instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                                         "s1.h2" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 0 1 #{})
                                                                     (update-slot-state-fn "s1.h2" 1 1)
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (release-instance-fn release-instance-chan "s1.h1" 15 :instance-error)
            (check-state-fn query-state-chan
                            {:instance-id->eject-expiry-time {"s1.h1" (t/plus start-time (t/millis (* (Math/pow 2 (dec 2)) eject-backoff-base-time-ms)))}
                             :instance-id->request-id->use-reason-map {"s1.h2" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                       "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"s1.h1" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                             :instance-id->state (-> {}
                                                   (update-slot-state-fn "s1.h1" 0 0 #{:ejected})
                                                   (update-slot-state-fn "s1.h2" 1 1)
                                                   (update-slot-state-fn "s1.h3" 8 0)
                                                   (update-slot-state-fn "s1.u3" 0 0 #{:locked}))})
            (is (= {"s1.h1" (* (Math/pow 2 (dec 2)) eject-backoff-base-time-ms)}
                   @trigger-uneject-process-atom))
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-eject-time-ms))))
              (async/>!! uneject-instance-chan {:instance-id "s1.h1"}))
            (testing "check that releasing instance still works as expected"
              (release-instance-fn release-instance-chan "s1.h2" 16 :success)
              (check-state-fn query-state-chan
                              {:instance-id->eject-expiry-time {}
                               :instance-id->request-id->use-reason-map {"s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                               :instance-id->consecutive-failures {"s1.h1" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                               :instance-id->state (-> {}
                                                     (update-slot-state-fn "s1.h1" 0 0 #{})
                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked}))
                               :load-balancing :oldest
                               :request-id->work-stealer {}
                               :work-stealing-queue (make-queue [])}))
            (async/>!! exit-chan :exit)))))

    (deftest test-start-service-chan-responder-locked-healthy-instance-not-used-to-service-request
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        ; locked s1.h1 should not be used to service a request
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h2")
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h3")
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h3")
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                   "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                   "s1.h3" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}
                                                                            "req-17" {:cid "cid-17" :request-id "req-17" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.h2" 1 1)
                                               (update-slot-state-fn "s1.h3" 8 2)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-instance-cleanup-during-state-update
      (let [{:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 14 {:instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                                         "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                                         "s1.h3" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}
                                                                                                  "req-17" {:cid "cid-17" :request-id "req-17" :reason :serve-request}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1
                                                                                   "s1.u1" 1
                                                                                   "s1.u2" 1
                                                                                   "s1.u3" 1
                                                                                   "s1.z1" 1} ; s1.z1 should be removed
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                                                     (update-slot-state-fn "s1.h2" 1 1)
                                                                     (update-slot-state-fn "s1.h3" 8 2)
                                                                     (update-slot-state-fn "s1.h4" 5 0)
                                                                     (update-slot-state-fn "s1.h5" 4 0)
                                                                     (update-slot-state-fn "s1.h6" 7 0)
                                                                     (update-slot-state-fn "s1.h7" 0 0 #{:ejected})
                                                                     (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                                                     (update-slot-state-fn "s1.u2" 0 0 #{:killed :unhealthy})
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy})
                                                                     (update-slot-state-fn "s1.u4" 0 0 #{:killed :unhealthy})
                                                                     (update-slot-state-fn "s1.u5" 0 0 #{:unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.h6" "s1.h7"
                                                                     "s1.u1" "s1.u2" "s1.u3" "s1.u4" "s1.u5"]})]
        (let [update-state {:healthy-instances [instance-h2 instance-h3 instance-h5 instance-h6]
                            :unhealthy-instances [instance-u1 instance-u2]
                            :my-instance->slots {instance-h2 1 instance-h3 8 instance-h5 9 instance-h6 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                   "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                   "s1.h3" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}
                                                                            "req-17" {:cid "cid-17" :request-id "req-17" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1
                                                             "s1.u1" 1
                                                             "s1.u2" 1
                                                             "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 0 0 #{:locked})
                                               (update-slot-state-fn "s1.h2" 1 1)
                                               (update-slot-state-fn "s1.h3" 8 2)
                                               (update-slot-state-fn "s1.h5" 9 0)
                                               (update-slot-state-fn "s1.h6" 1 0)
                                               (update-slot-state-fn "s1.h7" 0 0 #{:ejected})
                                               (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                               (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy})
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-offer-workstealing-instance-promptly-rejected
      (let [{:keys [exit-chan query-state-chan work-stealing-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]})]
        (counters/clear! (metrics/service-counter service-id "request-counts" "outstanding")) ;; clear the counter to zero
        (let [response-chan-1 (make-work-stealing-offer work-stealing-chan "test-router" "s1.h1") ;; offer a known instance
              _ (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding") 2) ;; fewer outstanding requests than available slots
              response-chan-2 (make-work-stealing-offer work-stealing-chan "test-router" "s1.h2") ;; offer a known instance
              response-chan-3 (make-work-stealing-offer work-stealing-chan "test-router" "s1.h4") ;; offer an unknown instance]
              ]
          (check-state-fn query-state-chan
                          {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                     "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                                 (update-slot-state-fn "s1.h2" 1 0)
                                                 (update-slot-state-fn "s1.h3" 8 0)
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                           :load-balancing :oldest
                           :request-id->work-stealer {}
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                           :work-stealing-queue (make-queue [])})
          (is (= :promptly-rejected (async/<!! response-chan-1)))
          (is (= :promptly-rejected (async/<!! response-chan-2)))
          (is (= :promptly-rejected (async/<!! response-chan-3))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-offer-workstealing-instance-accepted
      (let [initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-11" {:cid "cid-11" :request-id "req-11" :reason :kill-instance}}
                                                                     "s1.h2" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :serve-request}}
                                                                     "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 1 0 #{:healthy :locked})
                                                 (update-slot-state-fn "s1.h2" 1 1)
                                                 (update-slot-state-fn "s1.h3" 0 0)
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]}
            {:keys [exit-chan query-state-chan work-stealing-chan]}
            (launch-service-chan-responder 14 initial-state)]
        (counters/clear! (metrics/service-counter service-id "request-counts" "outstanding")) ;; clear the counter to zero
        (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding") 20) ;; more outstanding requests than available slots
        (let [response-chan-1 (make-work-stealing-offer work-stealing-chan "test-router-1" "s1.h1") ;; offer a known instance
              response-chan-2 (make-work-stealing-offer work-stealing-chan "test-router-2" "s1.h2") ;; offer a known instance
              response-chan-3 (make-work-stealing-offer work-stealing-chan "test-router-1" "s1.h4") ;; offer an unknown instance
              ]
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :request-id->work-stealer {}
                                   :work-stealing-queue
                                   (make-queue [(make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-offer-workstealing-instance-rejected
      (let [initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                     "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                                 (update-slot-state-fn "s1.h2" 1 0)
                                                 (update-slot-state-fn "s1.h3" 8 0)
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]}
            {:keys [exit-chan query-state-chan work-stealing-chan]}
            (launch-service-chan-responder 14 initial-state)]
        (counters/clear! (metrics/service-counter service-id "request-counts" "outstanding")) ;; clear the counter to zero
        (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding") 20) ;; more outstanding requests than available slots
        (do
          (make-work-stealing-offer work-stealing-chan "test-router-1" "s1.h1") ;; offer a known instance
          (make-work-stealing-offer work-stealing-chan "test-router-2" "s1.h2") ;; offer a known instance
          (make-work-stealing-offer work-stealing-chan "test-router-1" "s1.h4") ;; offer an unknown instance
          (check-state-fn query-state-chan initial-state))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-workstealing-ensure-rejects-during-exit
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            response-chan-3 (async/chan 1)
            test-instance-id->state (-> {}
                                      (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                      (update-slot-state-fn "s1.h2" 1 0)
                                      (update-slot-state-fn "s1.h3" 8 0)
                                      (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan]}
            (launch-service-chan-responder 17 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                                               :request-id->work-stealer {}
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})]
        (async/>!! exit-chan :exit)
        (is (= :rejected (async/<!! response-chan-1)))
        (is (= :rejected (async/<!! response-chan-2)))
        (is (= :rejected (async/<!! response-chan-3)))))

    (deftest test-start-service-chan-responder-workstealing-instances-rejected
      ;; more outstanding requests than available slots
      (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 20)
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            response-chan-3 (async/chan 1)
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 17 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state (-> {}
                                                                     (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                                                     (update-slot-state-fn "s1.h2" 1 0)
                                                                     (update-slot-state-fn "s1.h3" 8 0)
                                                                     (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                                               :request-id->work-stealer {}
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})]
        ;; more available slots than outstanding requests
        (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 5)
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 8 0)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                           (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})
        (is (= 2 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-1)))
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 8 0)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})
        (is (= 1 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-2)))
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state (-> {}
                                               (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                               (update-slot-state-fn "s1.h2" 1 0)
                                               (update-slot-state-fn "s1.h3" 8 0)
                                               (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [])})
        (is (zero? (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-3)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-workstealing-instances-used
      ;; more outstanding requests than available slots
      (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 20)
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            response-chan-3 (async/chan 1)
            test-instance-id->state (-> {}
                                      (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                      (update-slot-state-fn "s1.h2" 1 0)
                                      (update-slot-state-fn "s1.h3" 8 0)
                                      (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan reserve-instance-chan]}
            (launch-service-chan-responder 17 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                                               :request-id->work-stealer {}
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})]
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h1") ;; use work-stealing instance despite it being locked
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                            "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                           (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h2")
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                            "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                   "s1.h2" {"req-19" {:cid "cid-19" :request-id "req-19" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))

        (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 5)
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                            "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                   "s1.h2" {"req-19" {:cid "cid-19" :request-id "req-19" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [])})
        (is (= 2 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-3)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-workstealing-instances-used-when-slots-are-unavailable
      ;; more outstanding requests than available slots
      (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 20)
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            response-chan-3 (async/chan 1)
            test-instance-id->state (-> {}
                                      (update-slot-state-fn "s1.h1" 1 0 #{:healthy :locked})
                                      (update-slot-state-fn "s1.h2" 1 1)
                                      (update-slot-state-fn "s1.h3" 1 2)
                                      (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan reserve-instance-chan]}
            (launch-service-chan-responder 17 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                                         "s1.h2" {"req-08" {:cid "cid-08" :request-id "req-08" :reason :serve-request}}
                                                                                         "s1.h3" {"req-09" {:cid "cid-09" :request-id "req-09" :reason :serve-request}
                                                                                                  "req-10" {:cid "cid-10" :request-id "req-10" :reason :serve-request}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :load-balancing :oldest
                                               :request-id->work-stealer {}
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})]
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h1") ;; use work-stealing instance even when no slots are available
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                            "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                   "s1.h2" {"req-08" {:cid "cid-08" :request-id "req-08" :reason :serve-request}}
                                                                   "s1.h3" {"req-09" {:cid "cid-09" :request-id "req-09" :reason :serve-request}
                                                                            "req-10" {:cid "cid-10" :request-id "req-10" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")
                                                           (make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})
        (check-reserve-request-instance-fn reserve-instance-chan "s1.h2")
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                            "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                   "s1.h2" {"req-08" {:cid "cid-08" :request-id "req-08" :reason :serve-request}
                                                                            "req-19" {:cid "cid-19" :request-id "req-19" :reason :serve-request}}
                                                                   "s1.h3" {"req-09" {:cid "cid-09" :request-id "req-09" :reason :serve-request}
                                                                            "req-10" {:cid "cid-10" :request-id "req-10" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "s1.h4" response-chan-3 "test-router-1")])})
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))

        (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 5)
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                            "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                   "s1.h2" {"req-08" {:cid "cid-08" :request-id "req-08" :reason :serve-request}
                                                                            "req-19" {:cid "cid-19" :request-id "req-19" :reason :serve-request}}
                                                                   "s1.h3" {"req-09" {:cid "cid-09" :request-id "req-09" :reason :serve-request}
                                                                            "req-10" {:cid "cid-10" :request-id "req-10" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [])})
        (is (= 2 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-3)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-workstealing-instances-released
      ;; more outstanding requests than available slots
      (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 20)
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            test-instance-id->state (-> {}
                                      (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                      (update-slot-state-fn "s1.h2" 1 0)
                                      (update-slot-state-fn "s1.h3" 8 0)
                                      (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 19 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                                                  "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                                         "s1.h2" {"req-19" {:cid "cid-19" :request-id "req-19" :reason :serve-request}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                                               :request-id->work-stealer {"req-18" {:cid "cid-18" :instance instance-h1 :response-chan response-chan-1 :router-id "test-router-1"}
                                                                          "req-19" {:cid "cid-19" :instance instance-h2 :response-chan response-chan-2 :router-id "test-router-2"}}
                                               :work-stealing-queue (make-queue [])})]
        (release-instance-fn release-instance-chan "s1.h2" 19 :success)
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                            "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {"req-18" {:cid "cid-18" :instance instance-h1 :response-chan response-chan-1 :router-id "test-router-1"}}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [])})
        (is (= 1 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :success (async/<!! response-chan-2)))
        (release-instance-fn release-instance-chan "s1.h1" 18 :success)
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->eject-expiry-time {}
                         :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}}
                                                                   "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"s1.u2" 1 "s1.u1" 1 "s1.u3" 1} ;; h1 loses its failure entry
                         :instance-id->state test-instance-id->state
                         :load-balancing :oldest
                         :request-id->work-stealer {}
                         :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                         :work-stealing-queue (make-queue [])})
        (is (zero? (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :success (async/<!! response-chan-1)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-workstealing-instances-release-with-instance-error
      (counters/clear! (metrics/service-counter service-id "request-counts" "outstanding")) ;; clear the counter to zero
      (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding") 20) ;; more outstanding requests than available slots
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            test-instance-id->state (-> {}
                                      (update-slot-state-fn "s1.h1" 4 0 #{:healthy :locked})
                                      (update-slot-state-fn "s1.h2" 1 0)
                                      (update-slot-state-fn "s1.h3" 8 0)
                                      (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 19 {:id->instance id->instance-data
                                               :instance-id->eject-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                                                  "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                                         "s1.h2" {"req-19" {:cid "cid-19" :request-id "req-19" :reason :serve-request}}
                                                                                         "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"s1.h1" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :load-balancing :oldest
                                               :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")
                                                                          "req-19" (make-work-stealing-data "cid-16" "s1.h2" response-chan-2 "test-router-2")}
                                               :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                                               :work-stealing-queue (make-queue [])})]
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] current-time)]
            (release-instance-fn release-instance-chan "s1.h2" 19 :instance-error)
            (check-state-fn query-state-chan
                            {:id->instance id->instance-data
                             :instance-id->eject-expiry-time {"s1.h2" (t/plus current-time (t/millis eject-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {"s1.h1" {"req-12" {:cid "cid-12" :request-id "req-12" :reason :kill-instance}
                                                                                "req-18" {:cid "cid-18" :request-id "req-18" :reason :serve-request}}
                                                                       "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"s1.h1" 1 "s1.h2" 1 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                             :instance-id->state (update-slot-state-fn test-instance-id->state "s1.h2" 1 0 #{:ejected :healthy})
                             :load-balancing :oldest
                             :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "s1.h1" response-chan-1 "test-router-1")}
                             :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]
                             :work-stealing-queue (make-queue [])})
            (is (= :instance-error (async/<!! response-chan-2))))
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 8 max-eject-time-ms))))]
            (check-state-fn query-state-chan nil)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-async-request-assigned-instance
      (let [initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                     "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                     "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                              "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}}
                           :instance-id->consecutive-failures {"s1.h3" 2 "s1.u2" 1 "s1.u1" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 1 1)
                                                 (update-slot-state-fn "s1.h2" 1 1)
                                                 (update-slot-state-fn "s1.h3" 8 2)
                                                 (update-slot-state-fn "s1.u1" 0 0 #{:killed :unhealthy})
                                                 (update-slot-state-fn "s1.u2" 0 0 #{:killed :unhealthy}))
                           :load-balancing :oldest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u1" "s1.u2"]}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 10 initial-state)]
        ; release a success async
        (release-instance-fn release-instance-chan "s1.h3" 4 :success-async)
        (check-state-fn query-state-chan
                        (-> initial-state
                          (assoc-in [:instance-id->request-id->use-reason-map "s1.h3" "req-4" :variant] :async-request)
                          (utils/dissoc-in [:instance-id->consecutive-failures "s1.h3"])))
        ; now-release the async-request assigned instance
        (release-instance-fn release-instance-chan "s1.h3" 4 :success)
        (check-state-fn query-state-chan
                        (-> initial-state
                          (utils/dissoc-in [:instance-id->request-id->use-reason-map "s1.h3" "req-4"])
                          (utils/dissoc-in [:instance-id->consecutive-failures "s1.h3"])
                          (update-in [:instance-id->state] update-slot-state-fn "s1.h3" 8 1)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-async-request-work-stealing-instance
      (let [response-chan-1 (async/chan 4)
            response-chan-2 (async/chan 1)
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request}}
                                                                     "s1.h2" {"req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request}}
                                                                     "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}
                                                                              "req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request}}}
                           :instance-id->consecutive-failures {"s1.h3" 2 "s1.u2" 1 "s1.u1" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 1 1)
                                                 (update-slot-state-fn "s1.h2" 1 1)
                                                 (update-slot-state-fn "s1.h3" 8 1)
                                                 (update-slot-state-fn "s1.u1" 0 0 #{:killed :unhealthy})
                                                 (update-slot-state-fn "s1.u2" 0 0 #{:killed :unhealthy}))
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.u1" "s1.u2"]
                           :request-id->work-stealer {"req-4" (make-work-stealing-data "cid-4" "s1.h2" response-chan-1 "test-router-1")}
                           :work-stealing-queue (make-queue [(make-work-stealing-data "cid-7" "s1.h4" response-chan-2 "test-router-1")])}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 10 initial-state)]
        ; release a success async it also triggers release of the work-stealing head
        (release-instance-fn release-instance-chan "s1.h3" 4 :success-async)
        (check-state-fn query-state-chan
                        (-> initial-state
                          (assoc-in [:instance-id->request-id->use-reason-map "s1.h3" "req-4" :variant] :async-request)
                          (utils/dissoc-in [:instance-id->consecutive-failures "s1.h3"])
                          (assoc :work-stealing-queue (make-queue []))))
        ; no writes on response channel
        (is (async/>!! response-chan-1 :dummy-data))
        (is (= :dummy-data (async/<!! response-chan-1)))
        ; now-release the async-request work-stealing instance
        (release-instance-fn release-instance-chan "s1.h3" 4 :success)
        (check-state-fn query-state-chan
                        (-> initial-state
                          (utils/dissoc-in [:instance-id->request-id->use-reason-map "s1.h3" "req-4"])
                          (utils/dissoc-in [:instance-id->consecutive-failures "s1.h3"])
                          (utils/dissoc-in [:request-id->work-stealer "req-4"])
                          (assoc :work-stealing-queue (make-queue []))))
        (is (= :success (async/<!! response-chan-1))) ; work-stealing instance released successfully
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-successfully-release-ejected-instance
      (let [initial-state {:instance-id->eject-expiry-time {"s1.h1" (t/plus (t/now) (t/millis 100000))}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                     "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                     "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"s1.h1" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 2 1 #{:ejected :healthy})
                                                 (update-slot-state-fn "s1.h2" 2 1 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 8 0)
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked}))}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check that releasing instance still works as expected"
          (release-instance-fn release-instance-chan "s1.h1" 16 :success)
          (check-state-fn query-state-chan
                          (-> initial-state
                            (utils/dissoc-in [:instance-id->eject-expiry-time "s1.h1"])
                            (utils/dissoc-in [:instance-id->request-id->use-reason-map "s1.h1" "req-16"])
                            (utils/dissoc-in [:instance-id->consecutive-failures "s1.h1"])
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h1" 2 0)))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-successfully-release-borrowed-ejected-instance
      (let [initial-state {:instance-id->eject-expiry-time {"s1.h1" (t/plus (t/now) (t/millis 100000))}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                     "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                     "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"s1.h1" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 0 1 #{:ejected :healthy})
                                                 (update-slot-state-fn "s1.h2" 2 1 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 8 0)
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked}))}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check that releasing instance still works as expected"
          (release-instance-fn release-instance-chan "s1.h1" 16 :success)
          (check-state-fn query-state-chan
                          (-> initial-state
                            (utils/dissoc-in [:instance-id->eject-expiry-time "s1.h1"])
                            (utils/dissoc-in [:instance-id->request-id->use-reason-map "s1.h1" "req-16"])
                            (utils/dissoc-in [:instance-id->consecutive-failures "s1.h1"])
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h1" 0 0)))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-slot-counts-for-locked-and-ejected-instance
      (let [initial-state {:instance-id->eject-expiry-time {"s1.h1" (t/plus (t/now) (t/millis 100000))}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                     "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                     "s1.h4" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}
                                                                     "s1.h5" {"req-10" {:cid "cid-10" :request-id "req-10" :reason :serve-request}
                                                                              "req-11" {:cid "cid-11" :request-id "req-11" :reason :serve-request}}}
                           :instance-id->consecutive-failures {"s1.h1" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 2 1 #{:ejected :healthy})
                                                 (update-slot-state-fn "s1.h2" 3 1 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 7 0)
                                                 (update-slot-state-fn "s1.h4" 11 0 #{:locked})
                                                 (update-slot-state-fn "s1.h5" 0 2))}
            {:keys [exit-chan query-state-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (check-state-fn query-state-chan initial-state)
        (assert-instance-counters {"slots-assigned" 23 "slots-available" 9 "slots-in-use" 4})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-successfully-release-work-stealing-instance-success-async
      (let [response-chan-1 (async/chan 4)
            initial-state {:instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                     "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                     "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}}}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 0 1 #{:ejected :healthy})
                                                 (update-slot-state-fn "s1.h2" 2 1 #{:healthy})
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked}))
                           :request-id->work-stealer {"req-4" (make-work-stealing-data "cid-4" "s1.h3" response-chan-1 "test-router-1")}}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check releasing instance with :success-async"
          (release-instance-fn release-instance-chan "s1.h3" 4 :success-async)
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc-in [:instance-id->request-id->use-reason-map "s1.h3" "req-4" :variant] :async-request)
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h3" 0 0 #{}))))
          (async/>!! response-chan-1 :not-response)
          (is (= :not-response (async/<!! response-chan-1))))

        (testing "check releasing instance with :success after :success-async"
          (release-instance-fn release-instance-chan "s1.h3" 4 :success)
          (check-state-fn query-state-chan
                          (-> initial-state
                            (utils/dissoc-in [:instance-id->request-id->use-reason-map "s1.h3" "req-4"])
                            (utils/dissoc-in [:request-id->work-stealer "req-4"])
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h3" 0 0 #{}))))
          (async/>!! response-chan-1 :not-response)
          (is (= :success (async/<!! response-chan-1))))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-no-instances-assigned-hence-none-available-for-kill
      (let [initial-state {:instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state {}
                           :request-id->work-stealer {}}
            {:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check kill when no instances assigned"
          (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found)
          (check-state-fn query-state-chan initial-state))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-no-slots-assigned-hence-none-available-for-kill
      (let [initial-state {:instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 0 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h2" 0 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h5" 0 0 #{:healthy})
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked}))
                           :load-balancing :oldest
                           :request-id->work-stealer {}}
            {:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check kill when no slots assigned"
          (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found)
          (check-state-fn query-state-chan initial-state))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-no-idle-instance-available-for-kill
      (let [response-chan-1 (async/chan 4)
            initial-state {:instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                     "s1.h2" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :serve-request}
                                                                              "req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                     "s1.h3" {"req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request}}}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 0 1 #{:ejected :healthy})
                                                 (update-slot-state-fn "s1.h2" 2 2 #{:healthy})
                                                 (update-slot-state-fn "s1.h5" 0 0 #{:healthy})
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked}))
                           :request-id->work-stealer {"req-4" (make-work-stealing-data "cid-4" "s1.h3" response-chan-1 "test-router-1")}}
            {:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 20 initial-state)]

        (testing "check kill when no idle assigned instances"
          (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found)
          (check-state-fn query-state-chan initial-state))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-ejected-instance-cleanup
      (let [current-time (t/now)
            initial-state {:instance-id->eject-expiry-time {"s1.h1" (t/minus current-time (t/millis 1000))
                                                                "s1.h2" (t/minus current-time (t/millis 2000))
                                                                "s1.h3" (t/minus current-time (t/millis 3000))
                                                                "s1.h4" (t/plus current-time (t/millis 10000))}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                                                     "s1.h2" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}
                                                                     "s1.h4" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance}}
                                                                     "s1.h5" {"req-10" {:cid "cid-10" :request-id "req-10" :reason :serve-request}
                                                                              "req-11" {:cid "cid-11" :request-id "req-11" :reason :serve-request}}}
                           :instance-id->consecutive-failures {"s1.h1" 2 "s1.u1" 1 "s1.u2" 1 "s1.u3" 1}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 2 1 #{:ejected :healthy})
                                                 (update-slot-state-fn "s1.h2" 3 1 #{:ejected :healthy})
                                                 (update-slot-state-fn "s1.h3" 7 0 #{:ejected})
                                                 (update-slot-state-fn "s1.h4" 11 0 #{:ejected :locked})
                                                 (update-slot-state-fn "s1.h5" 0 2))}
            {:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (check-state-fn query-state-chan initial-state)

        (testing "check uneject cleanup during state update"
          (let [update-state {:healthy-instances [instance-h1 instance-h2 instance-h5 instance-h6]
                              :unhealthy-instances [instance-u1 instance-u2]
                              :my-instance->slots {instance-h1 5 instance-h2 2 instance-h3 8
                                                   instance-h4 2 instance-h5 1}}]
            (async/>!! update-state-chan [update-state current-time]))

          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :instance-id->eject-expiry-time {"s1.h4" (t/plus current-time (t/millis 10000))}
                                   :instance-id->consecutive-failures {"s1.h1" 2 "s1.u1" 1 "s1.u2" 1}
                                   :instance-id->state (-> {}
                                                         (update-slot-state-fn "s1.h1" 5 1 #{:healthy})
                                                         (update-slot-state-fn "s1.h2" 2 1 #{:healthy})
                                                         (update-slot-state-fn "s1.h3" 8 0 #{})
                                                         (update-slot-state-fn "s1.h4" 2 0 #{:ejected :locked})
                                                         (update-slot-state-fn "s1.h5" 1 2 #{:healthy})
                                                         (update-slot-state-fn "s1.u1" 0 0 #{:unhealthy})
                                                         (update-slot-state-fn "s1.u2" 0 0 #{:unhealthy}))))))


        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-work-stealing-reject
      (let [response-chan-1 (async/promise-chan)
            response-chan-2 (async/promise-chan)
            initial-state {:instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {"s1.h1" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (update-slot-state-fn {} "s1.h1" 1 1 #{:healthy})
                           :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "s1.h4" response-chan-1 "test-router-1")
                                                             (make-work-stealing-data "cid-18" "s1.h5" response-chan-2 "test-router-2")])}
            {:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 20 initial-state)]

        (testing "trigger cleanup of work-stealing queue when releasing instance"
          (release-instance-fn release-instance-chan "s1.h1" 16 :success)
          (check-state-fn query-state-chan (-> initial-state
                                             (assoc :instance-id->request-id->use-reason-map {}
                                                    :instance-id->state (-> {}
                                                                          (update-slot-state-fn "s1.h1" 1 0 #{:healthy}))
                                                    :work-stealing-queue (make-queue [(make-work-stealing-data "cid-18" "s1.h5" response-chan-2 "test-router-2")]))))

          (async/>!! response-chan-1 :from-test)
          (is (= :rejected (async/<!! response-chan-1))))

        (testing "trigger cleanup of work-stealing queue when attempting to kill instance"
          (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found)
          (check-state-fn query-state-chan (-> initial-state
                                             (assoc :instance-id->request-id->use-reason-map {}
                                                    :instance-id->state (-> {}
                                                                          (update-slot-state-fn "s1.h1" 1 0 #{:healthy}))
                                                    :work-stealing-queue (make-queue []))))

          (async/>!! response-chan-2 :from-test)
          (is (= :rejected (async/<!! response-chan-2))))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-expired-instance-busy-with-all-outdated-requests
      (let [current-time (t/now)
            time-0 (->> (- lingering-request-threshold-ms 1000) (t/millis) (t/minus current-time))
            time-1 (->> (+ lingering-request-threshold-ms 1000) (t/millis) (t/minus current-time))
            time-2 (->> (+ lingering-request-threshold-ms 2000) (t/millis) (t/minus current-time))
            time-3 (->> (+ lingering-request-threshold-ms 3000) (t/millis) (t/minus current-time))
            instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-1}}
                                                     "s1.h2" {"req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request :time time-2}
                                                              "req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request :time time-3}}
                                                     "s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request :time time-2}}
                                                     "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance :time time-0}}}
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map instance-id->request-id->use-reason-map
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 1 1 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.h2" 1 2 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.h3" 8 1 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                           :load-balancing :oldest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.u3"]}
            {:keys [exit-chan kill-instance-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]
        ; kill a healthy instance and clear the eject buffer
        (with-redefs [t/now (fn [] current-time)]
          (check-kill-request-instance-fn kill-instance-chan "s1.h1"))
        (->> (-> initial-state
               (update-in
                 [:instance-id->request-id->use-reason-map "s1.h1"]
                 (fn [request-id->use-reason-map]
                   (assoc request-id->use-reason-map
                     "req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance})))
               (update :instance-id->state
                       (fn [instance-id->state]
                         (-> instance-id->state
                           (update-slot-state-fn "s1.h1" 1 1 #{:expired :healthy :locked})))))
          (check-state-fn query-state-chan))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-expired-instance-busy-with-some-outdated-requests
      (let [current-time (t/now)
            time-0 (->> (- lingering-request-threshold-ms 1000) (t/millis) (t/minus current-time))
            time-1 (->> (- lingering-request-threshold-ms 1000) (t/millis) (t/minus current-time))
            time-2 (->> (+ lingering-request-threshold-ms 2000) (t/millis) (t/minus current-time))
            time-3 (->> (+ lingering-request-threshold-ms 3000) (t/millis) (t/minus current-time))
            instance-id->request-id->use-reason-map {"s1.h0" {}
                                                     "s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-0}}
                                                     "s1.h2" {"req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request :time time-0}
                                                              "req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request :time time-3}}
                                                     "s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request :time time-2}}
                                                     "s1.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance :time time-1}}}
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map instance-id->request-id->use-reason-map
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h0" 1 0 #{:healthy}) ;; idle healthy instance
                                                 (update-slot-state-fn "s1.h1" 1 1 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.h2" 1 2 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.h3" 8 1 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.u3" 0 0 #{:locked :unhealthy}))
                           :load-balancing :oldest
                           :sorted-instance-ids ["s1.h0" "s1.h1" "s1.h2" "s1.h3" "s1.u3"]}
            {:keys [exit-chan kill-instance-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]
        ; kill a healthy instance and clear the eject buffer
        (with-redefs [t/now (fn [] current-time)]
          (check-kill-request-instance-fn kill-instance-chan "s1.h3"))
        (->> (-> initial-state
               (update-in
                 [:instance-id->request-id->use-reason-map "s1.h3"]
                 (fn [request-id->use-reason-map]
                   (assoc request-id->use-reason-map
                     "req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance})))
               (update :instance-id->state
                       (fn [instance-id->state]
                         (-> instance-id->state
                           (update-slot-state-fn "s1.h3" 8 1 #{:expired :healthy :locked})))))
          (check-state-fn query-state-chan))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-eject-expired-instance
      (let [current-time (t/now)
            time-0 (->> (+ lingering-request-threshold-ms 20000) (t/millis) (t/minus current-time))
            time-1 (->> (+ lingering-request-threshold-ms 10000) (t/millis) (t/minus current-time))
            time-2 (->> (- lingering-request-threshold-ms 10000) (t/millis) (t/minus current-time))
            time-3 (->> (- lingering-request-threshold-ms 20000) (t/millis) (t/minus current-time))
            instance-id->request-id->use-reason-map {"s1.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-0}
                                                              "req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request :time time-1}}
                                                     "s1.h2" {"req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request :time time-2}
                                                              "req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request :time time-3}}
                                                     "s1.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request :time time-2}}}
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map instance-id->request-id->use-reason-map
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 4 2 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.h2" 4 2 #{:expired :healthy})
                                                 (update-slot-state-fn "s1.h3" 8 1 #{:expired :healthy}))
                           :load-balancing :oldest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3"]}
            {:keys [eject-instance-chan exit-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]
        ; try ejecting an instance
        (with-redefs [t/now (fn [] current-time)]
          (testing "eject with lingering and active requests"
            (check-eject-instance-fn eject-instance-chan "s1.h2" :in-use)
            (check-state-fn query-state-chan initial-state)

            (check-eject-instance-fn eject-instance-chan "s1.h3" :in-use)
            (check-state-fn query-state-chan initial-state))

          (testing "eject with only lingering requests"
            (check-eject-instance-fn eject-instance-chan "s1.h1" :ejected)
            (->> (-> initial-state
                   (update :instance-id->eject-expiry-time assoc "s1.h1" (t/plus current-time (t/millis eject-backoff-base-time-ms)))
                   (update :instance-id->state update-slot-state-fn "s1.h1" 4 2 #{:ejected :expired :healthy}))
              (check-state-fn query-state-chan)))

          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-kill-healthy-instance-in-presence-of-ejected
      (let [current-time (t/now)
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h2" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 0 0 #{:ejected}))
                           :load-balancing :oldest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3"]}
            {:keys [kill-instance-chan exit-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]

        (with-redefs [t/now (fn [] current-time)]
          (check-kill-request-instance-fn kill-instance-chan "s1.h3")

          (->> (-> initial-state
                 (assoc :instance-id->request-id->use-reason-map
                        {"s1.h3" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}})
                 (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h3" 0 0 #{:ejected :locked})))
            (check-state-fn query-state-chan))

          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-kill-healthy-instance-in-presence-of-unknown-ejected
      (let [current-time (t/now)
            initial-state {:id->instance (dissoc id->instance-data "s1.h3")
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h2" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 0 0 #{:ejected}))
                           :load-balancing :oldest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3"]}
            {:keys [kill-instance-chan exit-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]

        (with-redefs [t/now (fn [] current-time)]
          (check-kill-request-instance-fn kill-instance-chan "s1.h2")

          (->> (-> initial-state
                 (assoc :instance-id->request-id->use-reason-map
                        {"s1.h2" {"req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance}}})
                 (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h2" 3 0 #{:healthy :locked})))
            (check-state-fn query-state-chan))

          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-scaling-state-updates-oldest-load-balancing
      (let [current-time (t/now)
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h2" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 0 0 #{:ejected}))
                           :load-balancing :oldest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.h6" "s1.u1" "s1.u2" "s1.u3"]}
            {:keys [exit-chan query-state-chan scaling-state-chan]} (launch-service-chan-responder 13 initial-state)]

        (with-redefs [t/now (fn [] current-time)]
          (async/>!! scaling-state-chan {:scaling-state :scale-up})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :scale-down})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan initial-state)

          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-scaling-state-updates-random-load-balancing
      (let [current-time (t/now)
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h2" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 0 0 #{:ejected}))
                           :load-balancing :random
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.h6" "s1.u1" "s1.u2" "s1.u3"]}
            {:keys [exit-chan query-state-chan scaling-state-chan]} (launch-service-chan-responder 13 initial-state)]

        (with-redefs [t/now (fn [] current-time)]
          (async/>!! scaling-state-chan {:scaling-state :scale-up})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :scale-down})
          (check-state-fn query-state-chan (assoc initial-state :load-balancing :youngest))
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan initial-state)

          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-scaling-state-updates-youngest-load-balancing
      (let [current-time (t/now)
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h2" 3 0 #{:healthy})
                                                 (update-slot-state-fn "s1.h3" 0 0 #{:ejected}))
                           :load-balancing :youngest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.h6" "s1.u1" "s1.u2" "s1.u3"]}
            {:keys [exit-chan query-state-chan scaling-state-chan]} (launch-service-chan-responder 13 initial-state)]

        (with-redefs [t/now (fn [] current-time)]
          (async/>!! scaling-state-chan {:scaling-state :scale-up})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :scale-down})
          (check-state-fn query-state-chan initial-state)
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan initial-state)

          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-load-balancing-random
      (let [current-time (t/now)
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 2 0)
                                                 (update-slot-state-fn "s1.h2" 2 0)
                                                 (update-slot-state-fn "s1.h3" 1 0)
                                                 (update-slot-state-fn "s1.h4" 1 0))
                           :load-balancing :random
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.h6" "s1.u1" "s1.u2" "s1.u3"]}
            {:keys [exit-chan query-state-chan reserve-instance-chan scaling-state-chan]}
            (launch-service-chan-responder 14 initial-state)]

        (with-redefs [t/now (fn [] current-time)
                      rand-nth (fn [coll] (nth coll (-> coll count (/ 2) int)))]

          ;; keeps traffic distribution mode to random
          (async/>!! scaling-state-chan {:scaling-state :scale-up})
          (check-state-fn query-state-chan initial-state)

          ;; reserves last (random) available instance
          (check-reserve-request-instance-fn reserve-instance-chan "s1.h3")
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :instance-id->request-id->use-reason-map
                                   {"s1.h3" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}})
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h3" 1 1))))


          ;; keeps traffic distribution mode to random
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :instance-id->request-id->use-reason-map
                                   {"s1.h3" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}})
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h3" 1 1))))

          ;; reserves last (random) available instance
          (check-reserve-request-instance-fn reserve-instance-chan "s1.h2")
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :instance-id->request-id->use-reason-map
                                   {"s1.h2" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                    "s1.h3" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}})
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h2" 2 1))
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h3" 1 1))))

          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-load-balancing-youngest
      (let [current-time (t/now)
            initial-state {:id->instance id->instance-data
                           :instance-id->eject-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                 (update-slot-state-fn "s1.h1" 2 0)
                                                 (update-slot-state-fn "s1.h2" 2 0)
                                                 (update-slot-state-fn "s1.h3" 2 0)
                                                 (update-slot-state-fn "s1.h4" 1 0))
                           :load-balancing :youngest
                           :sorted-instance-ids ["s1.h1" "s1.h2" "s1.h3" "s1.h4" "s1.h5" "s1.h6" "s1.u1" "s1.u2" "s1.u3"]}
            {:keys [exit-chan query-state-chan reserve-instance-chan scaling-state-chan]}
            (launch-service-chan-responder 14 initial-state)]

        (with-redefs [t/now (fn [] current-time)]

          ;; keeps traffic distribution mode to random
          (async/>!! scaling-state-chan {:scaling-state :scale-up})
          (check-state-fn query-state-chan initial-state)

          ;; reserves last (random) available instance
          (check-reserve-request-instance-fn reserve-instance-chan "s1.h4")
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :instance-id->request-id->use-reason-map
                                   {"s1.h4" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}})
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h4" 1 1))))


          ;; keeps traffic distribution mode to random
          (async/>!! scaling-state-chan {:scaling-state :stable})
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :instance-id->request-id->use-reason-map
                                   {"s1.h4" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}})
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h4" 1 1))))

          ;; reserves last (random) available instance
          (check-reserve-request-instance-fn reserve-instance-chan "s1.h3")
          (check-state-fn query-state-chan
                          (-> initial-state
                            (assoc :instance-id->request-id->use-reason-map
                                   {"s1.h3" {"req-16" {:cid "cid-16" :request-id "req-16" :reason :serve-request}}
                                    "s1.h4" {"req-15" {:cid "cid-15" :request-id "req-15" :reason :serve-request}}})
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h3" 2 1))
                            (update-in [:instance-id->state] #(update-slot-state-fn %1 "s1.h4" 1 1))))

          (async/>!! exit-chan :exit))))))

(deftest test-trigger-uneject-process
  (let [correlation-id "test-correlation-id"
        test-instance-id "test-instance-id"
        uneject-instance-chan (async/chan 1)
        eject-period-ms 200
        current-time (t/now)]
    (with-redefs [t/now (fn [] current-time)]
      (trigger-uneject-process correlation-id test-instance-id eject-period-ms uneject-instance-chan))
    (let [{:keys [instance-id]} (async/<!! uneject-instance-chan)
          received-time (t/now)]
      (is (= test-instance-id instance-id))
      (is (not (t/before? received-time (t/plus current-time (t/millis eject-period-ms))))))))
