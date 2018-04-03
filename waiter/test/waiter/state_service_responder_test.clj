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
(ns waiter.state-service-responder-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [digest]
            [metrics.counters :as counters]
            [waiter.metrics :as metrics]
            [waiter.state :refer :all]
            [waiter.utils :as utils])
  (:import clojure.lang.PersistentQueue))

(let [service-id "testabcd"
      {:keys [blacklist-backoff-base-time-ms expired-instance-timeout-ms max-blacklist-time-ms] :as timeout-config}
      {:blacklist-backoff-base-time-ms 10000.0 :expired-instance-timeout-ms 60000 :max-blacklist-time-ms 100000}
      id-counter (atom 0)]

  (defn- make-queue [items]
    (apply conj (PersistentQueue/EMPTY) items))

  (defn- retrieve-channel-config []
    {:exit-chan (async/chan 1)
     :blacklist-instance-chan (async/chan 1)
     :kill-instance-chan (async/chan 1)
     :query-state-chan (async/chan 1)
     :release-instance-chan (async/chan 1)
     :reserve-instance-chan (async/chan 1)
     :unblacklist-instance-chan (async/chan 10)
     :update-state-chan (async/chan 1)
     :work-stealing-chan (async/chan 1)})

  (defn- assert-instance-counters [slot-counters-map]
    (doseq [[counter-name expected-counter-value] (seq slot-counters-map)]
      (let [actual-counter-value (counters/value (metrics/service-counter service-id "instance-counts" counter-name))]
        (when (not= expected-counter-value actual-counter-value)
          (println (first *testing-vars*) ":" counter-name "expected:" expected-counter-value "actual:" actual-counter-value))
        (is (= expected-counter-value actual-counter-value)
            (str "Mismatch in" counter-name "counter value, expected: " expected-counter-value ", actual: " actual-counter-value)))))

  (defn- check-state-fn [query-state-chan expected-state]
    (Thread/sleep 1) ; allow previous chaneel messages to get processed
    (let [query-state-response-chan (async/promise-chan)]
      (async/>!! query-state-chan {:cid "cid", :response-chan query-state-response-chan, :service-id service-id})
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
        (check-fn :instance-id->blacklist-expiry-time)
        (check-fn :instance-id->request-id->use-reason-map)
        (check-fn :instance-id->consecutive-failures)
        (check-fn :instance-id->state)
        (check-fn :request-id->work-stealer)
        (check-fn :sorted-instance-ids)
        (check-fn :work-stealing-queue)
        (let [expected-counter-map (cond-> {}
                                           (:instance-id->blacklist-expiry-time expected-state)
                                           (assoc "blacklisted" (count (:instance-id->blacklist-expiry-time expected-state)))
                                           (:instance-id->state expected-state)
                                           (merge (let [[slots-assigned slots-used slots-available] (compute-slots-values (:instance-id->state expected-state))]
                                                    {"slots-assigned" slots-assigned, "slots-available" slots-available, "slots-in-use" slots-used})))]
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
                                             :or {exclude-ids-set #{}, expect-deadlock false}}]
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
          (do
            (when (not (= {:id expected-result} reserved-result))
              (print (first *testing-contexts*) "check-request-instance-fn")
              (print ": expected: " {:id expected-result})
              (println ", actual:   " reserved-result))
            (is (= {:id expected-result} reserved-result) (str "Error in requesting instance for cid-" @id-counter)))))))

  (defn- check-kill-request-instance-fn [request-instance-chan expected-result &
                                         {:keys [exclude-ids-set expect-deadlock]
                                          :or {exclude-ids-set #{}, expect-deadlock false}}]
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
          (let [expected-result {:id expected-result}]
            (when (not (= expected-result reserved-result))
              (print (first *testing-contexts*) "check-request-instance-fn")
              (print ": expected: " expected-result)
              (println ", actual:   " reserved-result))
            (is (= expected-result reserved-result) (str "Error in requesting instance for cid-" @id-counter)))))))

  (defn- make-work-stealing-offer [work-stealing-chan router-id instance-id]
    (let [response-chan (async/promise-chan)]
      (swap! id-counter inc)
      (async/>!! work-stealing-chan {:cid (str "cid-" @id-counter)
                                     :instance {:id instance-id}
                                     :response-chan response-chan
                                     :router-id router-id})
      response-chan))

  (defn- make-work-stealing-data [cid instance-id response-chan router-id]
    {:cid cid, :instance {:id instance-id}, :response-chan response-chan, :router-id router-id})

  (defn- release-instance-fn [release-instance-chan instance-id id status]
    (async/>!! release-instance-chan [{:id instance-id} {:cid (str "cid-" id), :request-id (str "req-" id), :status status}]))

  (defn- check-blacklist-instance-fn [blacklist-instance-chan instance-id expected-result]
    (let [blacklist-instance-response-chan (async/promise-chan)]
      (async/>!! blacklist-instance-chan [{:instance-id instance-id
                                           :blacklist-period-ms blacklist-backoff-base-time-ms
                                           :cid "cid"}
                                          blacklist-instance-response-chan])
      (let [response (async/<!! blacklist-instance-response-chan)]
        (when (not= expected-result response)
          (println "Expected:" expected-result ", actual:" response))
        (is (= expected-result response))
        response)))

  (defn- launch-service-chan-responder [id-counter-value initial-state]
    (reset! id-counter id-counter-value)
    (let [channel-config (retrieve-channel-config)
          trigger-unblacklist-process-atom (atom {})
          trigger-unblacklist-process-fn (fn [_ instance-id blacklist-period-ms _]
                                           (swap! trigger-unblacklist-process-atom assoc instance-id blacklist-period-ms))]
      (let [slots-assigned-counter (metrics/service-counter service-id "instance-counts" "slots-assigned")
            slots-available-counter (metrics/service-counter service-id "instance-counts" "slots-available")
            slots-in-use-counter (metrics/service-counter service-id "instance-counts" "slots-in-use")
            blacklisted-instance-counter (metrics/service-counter service-id "instance-counts" "blacklisted")
            in-use-instance-counter (metrics/service-counter service-id "instance-counts" "in-use")
            work-stealing-received-in-flight-counter (metrics/service-counter service-id "work-stealing" "received-from" "in-flight")]
        (update-slots-metrics (:instance-id->state initial-state) slots-assigned-counter slots-available-counter slots-in-use-counter)
        (metrics/reset-counter blacklisted-instance-counter (count (:instance-id->blacklist-expiry-time initial-state)))
        (metrics/reset-counter in-use-instance-counter (count (:instance-id->request-id->use-reason-map initial-state)))
        (metrics/reset-counter work-stealing-received-in-flight-counter
                               (+ (count (:work-stealing-queue initial-state)) (count (:request-id->work-stealer initial-state)))))
      ;; start the service-chan-responder
      (start-service-chan-responder service-id trigger-unblacklist-process-fn timeout-config channel-config initial-state)
      (assoc channel-config :trigger-unblacklist-process-atom trigger-unblacklist-process-atom)))
  (let [id->instance-data {"testabcd.h1" {:id "testabcd.h1"},
                           "testabcd.h2" {:id "testabcd.h2"},
                           "testabcd.h3" {:id "testabcd.h3"},
                           "testabcd.h4" {:id "testabcd.h4"},
                           "testabcd.h5" {:id "testabcd.h5"},
                           "testabcd.u1" {:id "testabcd.u1"},
                           "testabcd.u2" {:id "testabcd.u2"},
                           "testabcd.u3" {:id "testabcd.u3"}}]
    (deftest test-start-service-chan-responder-simple-state-updates
      (let [{:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}]
                            :unhealthy-instances [{:id "testabcd.u1"}, {:id "testabcd.u2"}, {:id "testabcd.u3"}]
                            :starting-instances [{:id "testabcd.u3"}]
                            :expired-instances [{:id "testabcd.h1"}, {:id "testabcd.h3"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1, {:id "testabcd.h3"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.h2" 1 0)
                                                                  (update-slot-state-fn "testabcd.h3" 1 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "testabcd.u3" 0 0 #{:starting :unhealthy}))
                                          :sorted-instance-ids ["testabcd.h2" ; healthy
                                                                "testabcd.u1" "testabcd.u2" "testabcd.u3" ; unhealthy
                                                                "testabcd.h1" "testabcd.h3" ; expired
                                                                ]})
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}, {:id "testabcd.h4"}, {:id "testabcd.h5"}]
                            :unhealthy-instances [{:id "testabcd.u1"}, {:id "testabcd.u3"}] ; drop testabcd.u2 from update
                            :starting-instances [] ; remove testabcd.u3 from starting
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.h5" "testabcd.u1"]
                            :my-instance->slots {{:id "testabcd.h1"} 2, {:id "testabcd.h2"} 1, {:id "testabcd.h3"} 2, {:id "testabcd.h4"} 2}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 2 0 #{:healthy})
                                                                  (update-slot-state-fn "testabcd.h2" 1 0)
                                                                  (update-slot-state-fn "testabcd.h3" 2 0)
                                                                  (update-slot-state-fn "testabcd.h4" 2 0)
                                                                  (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.h5" ; healthy
                                                                "testabcd.u1" "testabcd.u3" ; unhealthy
                                                                ]
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-deployment-errors ; tests to make sure deployment errors are updated correctly
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        (doseq [deployment-error [:authentication-required :bad-startup-command :health-check-misconfigured :not-enough-memory nil]]
          ; update state and verify whether state changes are reflected correctly
          (let [update-state {:deployment-error deployment-error
                              :healthy-instances []
                              :unhealthy-instances [{:id "testabcd.u1"}, {:id "testabcd.u2"}, {:id "testabcd.u3"}]
                              :starting-instances []
                              :expired-instances []
                              :my-instance->slots {}}]
            (async/>!! update-state-chan [update-state (t/now)]))
          (check-state-fn query-state-chan {:deployment-error deployment-error
                                            :instance-id->blacklist-expiry-time {}
                                            :instance-id->request-id->use-reason-map {}
                                            :instance-id->consecutive-failures {}
                                            :instance-id->state (-> {}
                                                                    (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                                    (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy})
                                                                    (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                                            :request-id->work-stealer {}
                                            :sorted-instance-ids ["testabcd.u1" "testabcd.u2" "testabcd.u3"]
                                            :work-stealing-queue (make-queue [])})
          ; attempt to reserve an instances
          (if deployment-error
            (check-reserve-request-instance-fn reserve-instance-chan deployment-error) ; chanel should be open only when there are deployment errors
            (check-reserve-request-instance-fn reserve-instance-chan :no-matching-instance-found :expect-deadlock true))
          (check-state-fn query-state-chan {:deployment-error deployment-error
                                            :instance-id->blacklist-expiry-time {}
                                            :instance-id->request-id->use-reason-map {}
                                            :instance-id->consecutive-failures {}
                                            :instance-id->state (-> {}
                                                                    (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                                    (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy})
                                                                    (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                                            :request-id->work-stealer {}
                                            :work-stealing-queue (make-queue [])}))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-exclude-expired-instance
      (let [{:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}]
                            :unhealthy-instances [{:id "testabcd.u1"}, {:id "testabcd.u2"}]
                            :expired-instances [{:id "testabcd.h1"}, {:id "testabcd.h3"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1, {:id "testabcd.h3"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.h2" 1 0)
                                                                  (update-slot-state-fn "testabcd.h3" 1 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                                  (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy}))
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["testabcd.h2" ; healthy
                                                                "testabcd.u1" "testabcd.u2" ; unhealthy
                                                                "testabcd.h1" "testabcd.h3" ; expired
                                                                ]
                                          :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h4"}, {:id "testabcd.h5"}]
                            :unhealthy-instances [{:id "testabcd.u1"}] ; drop testabcd.u2 from update
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h4" "testabcd.h5" "testabcd.u1"]
                            :my-instance->slots {{:id "testabcd.h1"} 2, {:id "testabcd.h2"} 1, {:id "testabcd.h4"} 2}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 2 0 #{:healthy})
                                                                  (update-slot-state-fn "testabcd.h2" 1 0)
                                                                  (update-slot-state-fn "testabcd.h4" 2 0)
                                                                  (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy}))
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h4" "testabcd.h5" ; healthy
                                                                "testabcd.u1" ; unhealthy
                                                                ]
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-blacklist-expired-instance
      (let [{:keys [exit-chan query-state-chan update-state-chan blacklist-instance-chan trigger-unblacklist-process-atom unblacklist-instance-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}]
                            :unhealthy-instances [{:id "testabcd.u1"}]
                            :expired-instances [{:id "testabcd.h1"}, {:id "testabcd.h3"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1, {:id "testabcd.h3"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.h2" 1 0)
                                                                  (update-slot-state-fn "testabcd.h3" 1 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy}))
                                          :request-id->work-stealer {}
                                          :sorted-instance-ids ["testabcd.h2" ; healthy
                                                                "testabcd.u1" ; unhealthy
                                                                "testabcd.h1" "testabcd.h3" ; expired
                                                                ]
                                          :work-stealing-queue (make-queue [])})
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h1" :blacklisted)
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h3" :blacklisted)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {"testabcd.h1" (t/plus start-time (t/millis blacklist-backoff-base-time-ms))
                                                                  "testabcd.h3" (t/plus start-time (t/millis blacklist-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {}
                             :instance-id->consecutive-failures {}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 0 #{:blacklisted :expired :healthy})
                                                     (update-slot-state-fn "testabcd.h2" 1 0)
                                                     (update-slot-state-fn "testabcd.h3" 1 0 #{:blacklisted :expired :healthy})
                                                     (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy}))})
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h4" :blacklisted)
            (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}]
                                :unhealthy-instances [{:id "testabcd.u1"}]
                                :expired-instances [{:id "testabcd.h1"}, {:id "testabcd.h4"}]
                                :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1}}]
              (async/>!! update-state-chan [update-state (t/now)]))
            (let [expiry-time (t/plus start-time (t/millis blacklist-backoff-base-time-ms))]
              (check-state-fn query-state-chan
                              {:instance-id->blacklist-expiry-time {"testabcd.h1" expiry-time
                                                                    "testabcd.h3" expiry-time
                                                                    "testabcd.h4" expiry-time}
                               :instance-id->request-id->use-reason-map {}
                               :instance-id->consecutive-failures {}
                               :instance-id->state (-> {}
                                                       (update-slot-state-fn "testabcd.h1" 1 0 #{:blacklisted :expired :healthy})
                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                       (update-slot-state-fn "testabcd.h3" 0 0 #{:blacklisted})
                                                       (update-slot-state-fn "testabcd.h4" 0 0 #{:blacklisted :expired})
                                                       (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy}))})
              (is (= {"testabcd.h1" blacklist-backoff-base-time-ms
                      "testabcd.h3" blacklist-backoff-base-time-ms
                      "testabcd.h4" blacklist-backoff-base-time-ms}
                     @trigger-unblacklist-process-atom)))
            ; clear the blacklist buffer
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-blacklist-time-ms))))
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h1"})
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h3"})
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h4"}))
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {}
                             :instance-id->request-id->use-reason-map {}
                             :instance-id->consecutive-failures {}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 0 #{:expired :healthy})
                                                     (update-slot-state-fn "testabcd.h2" 1 0)
                                                     (update-slot-state-fn "testabcd.h3" 0 0 #{})
                                                     (update-slot-state-fn "testabcd.h4" 0 0 #{:expired})
                                                     (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy}))
                             :work-stealing-queue (make-queue [])}))
          (let [update-state {:healthy-instances [{:id "testabcd.h2"}]
                              :unhealthy-instances []
                              :expired-instances []
                              :my-instance->slots {{:id "testabcd.h2"} 1}}]
            (async/>!! update-state-chan [update-state (t/now)]))
          (check-state-fn query-state-chan
                          {:instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h2" 1 0))})
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-simple-state-updates-with-reserved-kill
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan update-state-chan]}
            (launch-service-chan-responder 0 {})]
        ; update state and verify whether state changes are reflected correctly
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}]
                            :unhealthy-instances []
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3"]
                            :expired-instances [{:id "testabcd.h2"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 2, {:id "testabcd.h3"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0)
                                                                  (update-slot-state-fn "testabcd.h2" 2 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.h3" 1 0))})
        (check-kill-request-instance-fn kill-instance-chan "testabcd.h2")
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"testabcd.h2" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :kill-instance}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0)
                                                                  (update-slot-state-fn "testabcd.h2" 2 0 #{:expired :healthy :locked})
                                                                  (update-slot-state-fn "testabcd.h3" 1 0))
                                          :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}, {:id "testabcd.h4"}, {:id "testabcd.h5"}]
                            :unhealthy-instances []
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.h5"]
                            :expired-instances [{:id "testabcd.h2"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 4, {:id "testabcd.h3"} 4}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"testabcd.h2" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :kill-instance}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0)
                                                                  (update-slot-state-fn "testabcd.h2" 4 0 #{:expired :healthy :locked})
                                                                  (update-slot-state-fn "testabcd.h3" 4 0))
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}, {:id "testabcd.h4"}, {:id "testabcd.h5"}]
                            :unhealthy-instances []
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.h5"]
                            :expired-instances [{:id "testabcd.h2"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 4, {:id "testabcd.h3"} 2}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"testabcd.h2" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :kill-instance}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0)
                                                                  (update-slot-state-fn "testabcd.h2" 4 0 #{:expired :healthy :locked})
                                                                  (update-slot-state-fn "testabcd.h3" 2 0))})
        (release-instance-fn release-instance-chan "testabcd.h2" 1 :not-killed)
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 1 0)
                                                                  (update-slot-state-fn "testabcd.h2" 4 0 #{:expired :healthy})
                                                                  (update-slot-state-fn "testabcd.h3" 2 0))
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-reserve-instances
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan]}
            (launch-service-chan-responder 0 {:id->instance id->instance-data
                                              :instance-id->blacklist-expiry-time {}
                                              :instance-id->request-id->use-reason-map {}
                                              :instance-id->consecutive-failures {}
                                              :instance-id->state (-> {}
                                                                      (update-slot-state-fn "testabcd.h1" 2 0)
                                                                      (update-slot-state-fn "testabcd.h2" 1 0)
                                                                      (update-slot-state-fn "testabcd.h3" 2 0)
                                                                      (update-slot-state-fn "testabcd.h4" 2 0))
                                              :request-id->work-stealer {}
                                              :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4"]
                                              :work-stealing-queue (make-queue [])})]
        ; reserve a few instances
        (doseq [instance-id ["testabcd.h1" "testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h3" "testabcd.h4"]]
          (check-reserve-request-instance-fn reserve-instance-chan instance-id))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                                   "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                                    "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                                    "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                                   "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                                    "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 2 2)
                                                                  (update-slot-state-fn "testabcd.h2" 1 1)
                                                                  (update-slot-state-fn "testabcd.h3" 2 2)
                                                                  (update-slot-state-fn "testabcd.h4" 2 1))
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-reserve-expired-instance
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 0 {:id->instance id->instance-data
                                              :instance-id->blacklist-expiry-time {}
                                              :instance-id->request-id->use-reason-map {}
                                              :instance-id->consecutive-failures {}
                                              :instance-id->state (-> {}
                                                                      (update-slot-state-fn "testabcd.h1" 2 0)
                                                                      (update-slot-state-fn "testabcd.h2" 1 0))})]
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}]
                            :unhealthy-instances []
                            :expired-instances [{:id "testabcd.h2"}]
                            :my-instance->slots {{:id "testabcd.h1"} 2, {:id "testabcd.h2"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        ; reserve expired instance
        (doseq [instance-id ["testabcd.h1" "testabcd.h1" "testabcd.h2"]]
          (check-reserve-request-instance-fn reserve-instance-chan instance-id))
        (check-state-fn query-state-chan {:instance-id->blacklist-expiry-time {}
                                          :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                                   "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                                    "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}}
                                          :instance-id->consecutive-failures {}
                                          :instance-id->state (-> {}
                                                                  (update-slot-state-fn "testabcd.h1" 2 2)
                                                                  (update-slot-state-fn "testabcd.h2" 1 1 #{:expired :healthy}))
                                          :request-id->work-stealer {}
                                          :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-slot-state-consistency
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 6 {:id->instance id->instance-data
                                              :instance-id->blacklist-expiry-time {}
                                              :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                                       "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                                        "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                                        "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                                       "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                                        "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}}
                                              :instance-id->consecutive-failures {}
                                              :instance-id->state (-> {}
                                                                      (update-slot-state-fn "testabcd.h1" 2 2)
                                                                      (update-slot-state-fn "testabcd.h2" 1 1)
                                                                      (update-slot-state-fn "testabcd.h3" 2 2)
                                                                      (update-slot-state-fn "testabcd.h4" 2 1))
                                              :request-id->work-stealer {}
                                              :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4"]
                                              :work-stealing-queue (make-queue [])})]
        ; give fewer slots to some instances and verify the slot state does not change
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}]
                            :unhealthy-instances [{:id "testabcd.u1"}, {:id "testabcd.u2"}]
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u1" "testabcd.u2"]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1, {:id "testabcd.h3"} 8}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (doseq [instance-id ["testabcd.h3" "testabcd.h3"]]
          (check-reserve-request-instance-fn reserve-instance-chan instance-id))
        ; trying to kill an instance will always give the same unhealthy instance, we rely on state updates to lose the unhealthy instance
        (check-kill-request-instance-fn kill-instance-chan "testabcd.u2")
        (release-instance-fn release-instance-chan "testabcd.u2" 9 :not-killed)
        (check-state-fn query-state-chan {}) ;; ensure the release is executed
        (check-kill-request-instance-fn kill-instance-chan "testabcd.u2")
        (check-kill-request-instance-fn kill-instance-chan "testabcd.u1" :exclude-ids-set #{"testabcd.u2"})
        ; check that state is still as expected
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                  "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                  "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                  "req-7" {:cid "cid-7", :request-id "req-7", :reason :serve-request}
                                                                                  "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}
                                                                   "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}
                                                                   "testabcd.u1" {"req-11" {:cid "cid-11", :request-id "req-11", :reason :kill-instance}}
                                                                   "testabcd.u2" {"req-10" {:cid "cid-10", :request-id "req-10", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 2)
                                                 (update-slot-state-fn "testabcd.h2" 1 1)
                                                 (update-slot-state-fn "testabcd.h3" 8 4)
                                                 (update-slot-state-fn "testabcd.h4" 0 1 #{})
                                                 (update-slot-state-fn "testabcd.u1" 0 0 #{:locked :unhealthy})
                                                 (update-slot-state-fn "testabcd.u2" 0 0 #{:locked :unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-killed-reservation
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan trigger-unblacklist-process-atom unblacklist-instance-chan]}
            (launch-service-chan-responder 11 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                                        "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                                         "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                                         "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                                        "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                                        "req-7" {:cid "cid-7", :request-id "req-7", :reason :serve-request}
                                                                                                        "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}
                                                                                         "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}
                                                                                         "testabcd.u2" {"req-10" {:cid "cid-10", :request-id "req-10", :reason :kill-instance}}
                                                                                         "testabcd.u1" {"req-11" {:cid "cid-11", :request-id "req-11", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 2)
                                                                       (update-slot-state-fn "testabcd.h2" 1 1)
                                                                       (update-slot-state-fn "testabcd.h3" 8 4)
                                                                       (update-slot-state-fn "testabcd.h4" 0 1 #{})
                                                                       (update-slot-state-fn "testabcd.u1" 0 0 #{:locked :unhealthy})
                                                                       (update-slot-state-fn "testabcd.u2" 0 0 #{:locked :unhealthy}))
                                               :request-id->work-stealer {}
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u1" "testabcd.u2"]
                                               :work-stealing-queue (make-queue [])})]
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (release-instance-fn release-instance-chan "testabcd.u2" 10 :killed)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {"testabcd.u2" (t/plus start-time (t/millis max-blacklist-time-ms))}
                             :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                      "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                       "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                       "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                      "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                      "req-7" {:cid "cid-7", :request-id "req-7", :reason :serve-request}
                                                                                      "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}
                                                                       "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}
                                                                       "testabcd.u1" {"req-11" {:cid "cid-11", :request-id "req-11", :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"testabcd.u2" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 2)
                                                     (update-slot-state-fn "testabcd.h2" 1 1)
                                                     (update-slot-state-fn "testabcd.h3" 8 4)
                                                     (update-slot-state-fn "testabcd.h4" 0 1 #{})
                                                     (update-slot-state-fn "testabcd.u1" 0 0 #{:locked :unhealthy})
                                                     (update-slot-state-fn "testabcd.u2" 0 0 #{:blacklisted :killed :unhealthy}))
                             :request-id->work-stealer {}
                             :work-stealing-queue (make-queue [])})
            (is (= {"testabcd.u2" max-blacklist-time-ms}
                   @trigger-unblacklist-process-atom))
            (release-instance-fn release-instance-chan "testabcd.u1" 11 :killed)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {"testabcd.u1" (t/plus start-time (t/millis max-blacklist-time-ms))
                                                                  "testabcd.u2" (t/plus start-time (t/millis max-blacklist-time-ms))}
                             :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                      "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                       "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                       "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                      "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                      "req-7" {:cid "cid-7", :request-id "req-7", :reason :serve-request}
                                                                                      "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}
                                                                       "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}}
                             :instance-id->consecutive-failures {"testabcd.u1" 1, "testabcd.u2" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 2)
                                                     (update-slot-state-fn "testabcd.h2" 1 1)
                                                     (update-slot-state-fn "testabcd.h3" 8 4)
                                                     (update-slot-state-fn "testabcd.h4" 0 1 #{})
                                                     (update-slot-state-fn "testabcd.u1" 0 0 #{:blacklisted :killed :unhealthy})
                                                     (update-slot-state-fn "testabcd.u2" 0 0 #{:blacklisted :killed :unhealthy}))
                             :request-id->work-stealer {}
                             :work-stealing-queue (make-queue [])})
            (is (= {"testabcd.u1" max-blacklist-time-ms
                    "testabcd.u2" max-blacklist-time-ms}
                   @trigger-unblacklist-process-atom))
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (+ 1000000 max-blacklist-time-ms))))
              ; no more unhealthy instances to kill, all healthy instances are busy
              (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found :expect-deadlock true)
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.u1"})
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.u2"}))
            ; ensure blacklist was cleared due to expiry of period
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {}
                             :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                      "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                       "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                       "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                      "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                      "req-7" {:cid "cid-7", :request-id "req-7", :reason :serve-request}
                                                                                      "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}
                                                                       "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}}
                             :instance-id->consecutive-failures {"testabcd.u1" 1, "testabcd.u2" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 2)
                                                     (update-slot-state-fn "testabcd.h2" 1 1)
                                                     (update-slot-state-fn "testabcd.h3" 8 4)
                                                     (update-slot-state-fn "testabcd.h4" 0 1 #{})
                                                     (update-slot-state-fn "testabcd.u1" 0 0 #{:killed :unhealthy})
                                                     (update-slot-state-fn "testabcd.u2" 0 0 #{:killed :unhealthy}))
                             :request-id->work-stealer {}
                             :work-stealing-queue (make-queue [])}))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-failed-blacklist
      (let [{:keys [blacklist-instance-chan exit-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}
                                                                                                        "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}}
                                                                                         "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}}
                                                                                         "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                                        "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                                        "req-7" {:cid "cid-7", :request-id "req-7", :reason :serve-request}
                                                                                                        "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}
                                                                                         "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"testabcd.u2" 1, "testabcd.u1" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 2)
                                                                       (update-slot-state-fn "testabcd.h2" 1 1)
                                                                       (update-slot-state-fn "testabcd.h3" 8 4)
                                                                       (update-slot-state-fn "testabcd.h4" 0 1 #{})
                                                                       (update-slot-state-fn "testabcd.u1" 0 0 #{:killed :unhealthy})
                                                                       (update-slot-state-fn "testabcd.u2" 0 0 #{:killed :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u1" "testabcd.u2"]})]
        ; try blacklisting an instance in-use
        (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h1" :in-use)
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-success-instances
      (let [{:keys [exit-chan query-state-chan release-instance-chan update-state-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data,
                                               :instance-id->blacklist-expiry-time {},
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request},
                                                                                                        "req-2" {:cid "cid-2", :request-id "req-2", :reason :serve-request}},
                                                                                         "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}},
                                                                                         "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request},
                                                                                                        "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request},
                                                                                                        "req-7" {:cid "cid-7", :request-id "req-7", :reason :serve-request},
                                                                                                        "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}},
                                                                                         "testabcd.h4" {"req-6" {:cid "cid-6", :request-id "req-6", :reason :serve-request}}},
                                               :instance-id->consecutive-failures {"testabcd.u2" 1, "testabcd.u1" 1},
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 2)
                                                                       (update-slot-state-fn "testabcd.h2" 1 1)
                                                                       (update-slot-state-fn "testabcd.h3" 8 4)
                                                                       (update-slot-state-fn "testabcd.h4" 0 1 #{})
                                                                       (update-slot-state-fn "testabcd.u1" 0 0 #{:killed :unhealthy})
                                                                       (update-slot-state-fn "testabcd.u2" 0 0 #{:killed :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u1" "testabcd.u2"]})]
        ; update state with newer unhealthy-instance
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h3"}, {:id "testabcd.h4"}, {:id "testabcd.h5"}]
                            :unhealthy-instances [{:id "testabcd.u3"}]
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.h5" "testabcd.u3"]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1, {:id "testabcd.h3"} 8}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        ; release a few slots and check state
        (release-instance-fn release-instance-chan "testabcd.h1" 2 :success)
        (release-instance-fn release-instance-chan "testabcd.h2" 3 :success)
        (release-instance-fn release-instance-chan "testabcd.h3" 7 :success)
        (release-instance-fn release-instance-chan "testabcd.h4" 6 :success)
        ; call release with faulty arguments
        (release-instance-fn release-instance-chan "testabcd.h3" 107 :success)
        (release-instance-fn release-instance-chan "testabcd.h4" 106 :success)
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                  "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                  "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}}
                         :instance-id->consecutive-failures {"testabcd.u1" 1, "testabcd.u2" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 1)
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 8 3)
                                                 (update-slot-state-fn "testabcd.h4" 0 0 #{:healthy}) ;; since the response was a success
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-make-in-use-instance-unhealthy
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan update-state-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}}
                                                                                         "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                                        "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                                        "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"testabcd.u2" 1, "testabcd.u1" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 1)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 3)
                                                                       (update-slot-state-fn "testabcd.h4" 0 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u1" "testabcd.u2" "testabcd.u3"]})]
        ; testabcd.h3 now becomes unhealthy
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h4"}, {:id "testabcd.h5"}]
                            :unhealthy-instances [{:id "testabcd.h3"}, {:id "testabcd.u3"}]
                            :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.h5" "testabcd.u3"]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                  "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                  "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 1)
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 0 3 #{:unhealthy})
                                                 (update-slot-state-fn "testabcd.h4" 0 0 #{:healthy}) ;; since the response was a success
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                         :work-stealing-queue (make-queue [])})
        ; release requests for testabcd.h3
        (release-instance-fn release-instance-chan "testabcd.h3" 5 :success)
        (release-instance-fn release-instance-chan "testabcd.h3" 8 :success)
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 1)
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 0 1 #{:unhealthy})
                                                 (update-slot-state-fn "testabcd.h4" 0 0 #{:healthy}) ;; since the response was a success
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        ; testabcd.h3 should not show up in a kill-instance reservation
        (check-kill-request-instance-fn kill-instance-chan "testabcd.u3")
        (check-kill-request-instance-fn kill-instance-chan "testabcd.h4")
        (check-kill-request-instance-fn kill-instance-chan "testabcd.h2")
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :kill-instance}}
                                                                   "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}}
                                                                   "testabcd.h4" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :kill-instance}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 1)
                                                 (update-slot-state-fn "testabcd.h2" 1 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.h3" 0 1 #{:unhealthy})
                                                 (update-slot-state-fn "testabcd.h4" 0 0 #{:healthy :locked}) ;; since the response was a success
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-failed-instances
      (let [{:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan trigger-unblacklist-process-atom unblacklist-instance-chan]}
            (launch-service-chan-responder 12 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}}
                                                                                         "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}
                                                                                                        "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}
                                                                                                        "req-8" {:cid "cid-8", :request-id "req-8", :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"testabcd.u2" 1, "testabcd.u1" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 1)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 3)
                                                                       (update-slot-state-fn "testabcd.h4" 0 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u3"]})]
        ; fail remaining requests except 2 and explicitly blacklist another instance
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (release-instance-fn release-instance-chan "testabcd.h1" 1 :instance-error)
            (release-instance-fn release-instance-chan "testabcd.h3" 4 :instance-busy)
            (release-instance-fn release-instance-chan "testabcd.h3" 8 :instance-busy)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {"testabcd.h1" (t/plus start-time (t/millis blacklist-backoff-base-time-ms))
                                                                  "testabcd.h3" (t/plus start-time (t/millis (* (Math/pow 2 (dec 2)) blacklist-backoff-base-time-ms)))}
                             :instance-id->request-id->use-reason-map {"testabcd.h3" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}}
                             :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.h3" 2, "testabcd.u1" 1, "testabcd.u2" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 0 #{:blacklisted :healthy})
                                                     (update-slot-state-fn "testabcd.h2" 1 0)
                                                     (update-slot-state-fn "testabcd.h3" 8 1 #{:blacklisted :healthy})
                                                     (update-slot-state-fn "testabcd.h4" 0 0)
                                                     (update-slot-state-fn "testabcd.u3" 0 0 #{:unhealthy}))})
            (is (= {"testabcd.h1" blacklist-backoff-base-time-ms
                    "testabcd.h3" (* (Math/pow 2 (dec 2)) blacklist-backoff-base-time-ms)}
                   @trigger-unblacklist-process-atom))
            (do
              ; kill unhealthy instance
              (check-kill-request-instance-fn kill-instance-chan "testabcd.u3")
              (reset! current-time-atom (t/plus start-time (t/millis (* 2 max-blacklist-time-ms))))
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h1"})
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h3"})
              (check-state-fn query-state-chan
                              {:instance-id->blacklist-expiry-time {}
                               :instance-id->request-id->use-reason-map {"testabcd.h3" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.h3" 2, "testabcd.u1" 1, "testabcd.u2" 1}
                               :instance-id->state (-> {}
                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                       (update-slot-state-fn "testabcd.h3" 8 1)
                                                       (update-slot-state-fn "testabcd.h4" 0 0)
                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                               :request-id->work-stealer {}
                               :work-stealing-queue (make-queue [])})
              (release-instance-fn release-instance-chan "testabcd.u3" 13 :killed)))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-kill-known-healthy-instance:even-with-no-slots
      (let [{:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 13 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h3" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.u2" 1
                                                                                   "testabcd.u1" 1
                                                                                   "testabcd.h1" 1
                                                                                   "testabcd.h3" 2
                                                                                   "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 1)
                                                                       (update-slot-state-fn "testabcd.h4" 0 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u3"]})]
        ; kill a healthy instance and clear the blacklist buffer
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-blacklist-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "testabcd.h4")))
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h3" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                   "testabcd.h4" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :kill-instance}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.h3" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 0)
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 8 1)
                                                 (update-slot-state-fn "testabcd.h4" 0 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-healthy-instance
      (let [{:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 13 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h3" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.u2" 1
                                                                                   "testabcd.u1" 1
                                                                                   "testabcd.h1" 1
                                                                                   "testabcd.h3" 2
                                                                                   "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 1)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        ; kill a healthy instance and clear the blacklist buffer
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-blacklist-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "testabcd.h2")))
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h2" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :kill-instance}}
                                                                   "testabcd.h3" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.h3" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 0)
                                                 (update-slot-state-fn "testabcd.h2" 1 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.h3" 8 1)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-expired-instance-from-other-router
      (let [{:keys [exit-chan kill-instance-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 13 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}}
                                               :instance-id->consecutive-failures {"testabcd.u2" 1
                                                                                   "testabcd.u1" 1
                                                                                   "testabcd.h1" 1
                                                                                   "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 1)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy})
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:starting :unhealthy}))})]
        ; testabcd.h3 now becomes expired
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}]
                            :unhealthy-instances [{:id "testabcd.u2"} {:id "testabcd.u3"}]
                            :starting-instances [{:id "testabcd.u3"}]
                            :expired-instances [{:id "testabcd.h3"}, {:id "testabcd.h4"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:id->instance (select-keys id->instance-data #{"testabcd.h1", "testabcd.h2", "testabcd.h3", "testabcd.h4", "testabcd.u2", "testabcd.u3"})
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}}
                         :instance-id->consecutive-failures {"testabcd.u2" 1
                                                             "testabcd.h1" 1
                                                             "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 1)
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 0 0 #{:expired})
                                                 (update-slot-state-fn "testabcd.h4" 0 0 #{:expired})
                                                 (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy})
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:starting :unhealthy}))
                         :request-id->work-stealer {}
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.u2" "testabcd.u3" "testabcd.h3" "testabcd.h4"]
                         :work-stealing-queue (make-queue [])})
        ; testabcd.u2 should be killed because it is not starting
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-blacklist-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "testabcd.u2")))
        ; testabcd.u3 becomes healthy
        (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.u3"}]
                            :expired-instances [{:id "testabcd.h3"}]
                            :my-instance->slots {{:id "testabcd.h1"} 1, {:id "testabcd.h2"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        ; expired instance should be killed
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 4 max-blacklist-time-ms))))]
            (check-kill-request-instance-fn kill-instance-chan "testabcd.h3")))
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.u2" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :kill-instance}}
                                                                   "testabcd.h3" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :kill-instance}}
                                                                   "testabcd.h1" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}
                                                                   }
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 1)
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 0 0 #{:expired :locked})
                                                 (update-slot-state-fn "testabcd.u2" 0 0 #{:locked})
                                                 (update-slot-state-fn "testabcd.u3" 0 0))})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-clear-failures
      (let [{:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data,
                                               :instance-id->blacklist-expiry-time {},
                                               :instance-id->request-id->use-reason-map {"testabcd.h3" {"req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}},
                                                                                         "testabcd.h2" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :kill-instance}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}},
                                               :instance-id->consecutive-failures {"testabcd.u2" 1
                                                                                   "testabcd.u1" 1
                                                                                   "testabcd.h1" 1
                                                                                   "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0 #{:healthy :locked})
                                                                       (update-slot-state-fn "testabcd.h3" 8 1)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        ; successful release should clear out the failures counter
        (release-instance-fn release-instance-chan "testabcd.h3" 5 :success)
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h2" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :kill-instance}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 0)
                                                 (update-slot-state-fn "testabcd.h2" 1 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.h3" 8 0)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-without-killing
      (let [{:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data,
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h2" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :kill-instance}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0 #{:healthy :locked})
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        ; release instance without killing
        (release-instance-fn release-instance-chan "testabcd.h2" 14 :not-killed)
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 0)
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 8 0)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-blacklist-owned-instance
      (let [{:keys [blacklist-instance-chan exit-chan query-state-chan release-instance-chan trigger-unblacklist-process-atom unblacklist-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        ; try blacklisting an instance successfully
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h2" :blacklisted)
            ; repeated call should also succeed
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h2" :blacklisted)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {"testabcd.h2" (t/plus start-time (t/millis blacklist-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 0)
                                                     (update-slot-state-fn "testabcd.h2" 1 0 #{:blacklisted :healthy})
                                                     (update-slot-state-fn "testabcd.h3" 8 0)
                                                     (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))})
            (is (= {"testabcd.h2" blacklist-backoff-base-time-ms}
                   @trigger-unblacklist-process-atom))
            ; clear the blacklist buffer with a dummy state call
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-blacklist-time-ms))))
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h2"}))
            ; dummy release call should not throw an error
            (release-instance-fn release-instance-chan "testabcd.hUnknown" 114 :success)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {}
                             :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 0)
                                                     (update-slot-state-fn "testabcd.h2" 1 0)
                                                     (update-slot-state-fn "testabcd.h3" 8 0)
                                                     (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))}))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-blacklist-external-instance
      (let [{:keys [blacklist-instance-chan exit-chan query-state-chan release-instance-chan trigger-unblacklist-process-atom unblacklist-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        ; try blacklisting an external instance successfully
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h8" :blacklisted)
            ; repeated call should also succeed
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h9" :blacklisted)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {"testabcd.h8" (t/plus start-time (t/millis blacklist-backoff-base-time-ms))
                                                                  "testabcd.h9" (t/plus start-time (t/millis blacklist-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 0)
                                                     (update-slot-state-fn "testabcd.h2" 1 0 #{:healthy})
                                                     (update-slot-state-fn "testabcd.h3" 8 0)
                                                     (update-slot-state-fn "testabcd.h8" 0 0 #{:blacklisted})
                                                     (update-slot-state-fn "testabcd.h9" 0 0 #{:blacklisted})
                                                     (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))})
            (is (= {"testabcd.h8" blacklist-backoff-base-time-ms
                    "testabcd.h9" blacklist-backoff-base-time-ms}
                   @trigger-unblacklist-process-atom))
            ; clear the blacklist buffer with a dummy state call
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-blacklist-time-ms))))
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h8"})
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h9"}))
            ; dummy release call should not throw an error
            (release-instance-fn release-instance-chan "testabcd.hUnknown" 114 :success)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {}
                             :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 1 0)
                                                     (update-slot-state-fn "testabcd.h2" 1 0)
                                                     (update-slot-state-fn "testabcd.h3" 8 0)
                                                     (update-slot-state-fn "testabcd.h8" 0 0 #{})
                                                     (update-slot-state-fn "testabcd.h9" 0 0 #{})
                                                     (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                             :work-stealing-queue (make-queue [])}))
          (async/>!! exit-chan :exit))))

    (deftest test-start-service-chan-responder-cause-unowned-instance-blacklist
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan update-state-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 1 0)
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        ; blacklist an instance which gets remove from state, it should not introduce any errors
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h1")
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h2")
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 1 1)
                                                 (update-slot-state-fn "testabcd.h2" 1 1)
                                                 (update-slot-state-fn "testabcd.h3" 8 0)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (let [update-state {:healthy-instances [{:id "testabcd.h2"}, {:id "testabcd.h3"}]
                            :unhealthy-instances []
                            :sorted-instance-ids ["testabcd.h2" "testabcd.h3"]
                            :my-instance->slots {{:id "testabcd.h2"} 1, {:id "testabcd.h3"} 8}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 0 1 #{})
                                                 (update-slot-state-fn "testabcd.h2" 1 1)
                                                 (update-slot-state-fn "testabcd.h3" 8 0)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-cause-instance-blacklist
      (let [{:keys [exit-chan query-state-chan release-instance-chan trigger-unblacklist-process-atom unblacklist-instance-chan]}
            (launch-service-chan-responder 16 {:instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                                         "testabcd.h2" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 0 1 #{})
                                                                       (update-slot-state-fn "testabcd.h2" 1 1)
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        (let [start-time (t/now)
              current-time-atom (atom start-time)]
          (with-redefs [t/now (fn [] @current-time-atom)]
            (release-instance-fn release-instance-chan "testabcd.h1" 15 :instance-error)
            (check-state-fn query-state-chan
                            {:instance-id->blacklist-expiry-time {"testabcd.h1" (t/plus start-time (t/millis (* (Math/pow 2 (dec 2)) blacklist-backoff-base-time-ms)))}
                             :instance-id->request-id->use-reason-map {"testabcd.h2" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                       "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"testabcd.h1" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                             :instance-id->state (-> {}
                                                     (update-slot-state-fn "testabcd.h1" 0 0 #{:blacklisted})
                                                     (update-slot-state-fn "testabcd.h2" 1 1)
                                                     (update-slot-state-fn "testabcd.h3" 8 0)
                                                     (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))})
            (is (= {"testabcd.h1" (* (Math/pow 2 (dec 2)) blacklist-backoff-base-time-ms)}
                   @trigger-unblacklist-process-atom))
            (do
              (reset! current-time-atom (t/plus start-time (t/millis (* 8 max-blacklist-time-ms))))
              (async/>!! unblacklist-instance-chan {:instance-id "testabcd.h1"}))
            (testing "check that releasing instance still works as expected"
              (release-instance-fn release-instance-chan "testabcd.h2" 16 :success)
              (check-state-fn query-state-chan
                              {:instance-id->blacklist-expiry-time {}
                               :instance-id->request-id->use-reason-map {"testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                               :instance-id->consecutive-failures {"testabcd.h1" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                               :instance-id->state (-> {}
                                                       (update-slot-state-fn "testabcd.h1" 0 0 #{})
                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))
                               :request-id->work-stealer {}
                               :work-stealing-queue (make-queue [])}))
            (async/>!! exit-chan :exit)))))

    (deftest test-start-service-chan-responder-locked-healthy-instance-not-used-to-service-request
      (let [{:keys [exit-chan query-state-chan reserve-instance-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        ; locked testabcd.h1 should not be used to service a request
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h2")
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h3")
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h3")
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                   "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}
                                                                                  "req-17" {:cid "cid-17", :request-id "req-17", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.h2" 1 1)
                                                 (update-slot-state-fn "testabcd.h3" 8 2)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-instance-cleanup-during-state-update
      (let [{:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 14 {:instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                                         "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                                         "testabcd.h3" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}
                                                                                                        "req-17" {:cid "cid-17", :request-id "req-17", :reason :serve-request}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1
                                                                                   "testabcd.u1" 1
                                                                                   "testabcd.u2" 1
                                                                                   "testabcd.u3" 1
                                                                                   "testabcd.z1" 1} ; testabcd.z1 should be removed
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                                       (update-slot-state-fn "testabcd.h2" 1 1)
                                                                       (update-slot-state-fn "testabcd.h3" 8 2)
                                                                       (update-slot-state-fn "testabcd.h4" 5 0)
                                                                       (update-slot-state-fn "testabcd.h5" 4 0)
                                                                       (update-slot-state-fn "testabcd.h6" 7 0)
                                                                       (update-slot-state-fn "testabcd.h7" 0 0 #{:blacklisted})
                                                                       (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                                       (update-slot-state-fn "testabcd.u2" 0 0 #{:killed :unhealthy})
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy})
                                                                       (update-slot-state-fn "testabcd.u4" 0 0 #{:killed :unhealthy})
                                                                       (update-slot-state-fn "testabcd.u5" 0 0 #{:unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.h5" "testabcd.h6" "testabcd.h7"
                                                                     "testabcd.u1" "testabcd.u2" "testabcd.u3" "testabcd.u4" "testabcd.u5"]})]
        (let [update-state {:healthy-instances [{:id "testabcd.h2"}, {:id "testabcd.h3"}, {:id "testabcd.h5"}, {:id "testabcd.h6"}]
                            :unhealthy-instances [{:id "testabcd.u1"}, {:id "testabcd.u2"}]
                            :my-instance->slots {{:id "testabcd.h2"} 1, {:id "testabcd.h3"} 8, {:id "testabcd.h5"} 9, {:id "testabcd.h6"} 1}}]
          (async/>!! update-state-chan [update-state (t/now)]))
        (check-state-fn query-state-chan
                        {:instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                   "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}
                                                                                  "req-17" {:cid "cid-17", :request-id "req-17", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1
                                                             "testabcd.u1" 1
                                                             "testabcd.u2" 1
                                                             "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 0 0 #{:locked})
                                                 (update-slot-state-fn "testabcd.h2" 1 1)
                                                 (update-slot-state-fn "testabcd.h3" 8 2)
                                                 (update-slot-state-fn "testabcd.h5" 9 0)
                                                 (update-slot-state-fn "testabcd.h6" 1 0)
                                                 (update-slot-state-fn "testabcd.h7" 0 0 #{:blacklisted})
                                                 (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                 (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy})
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-offer-workstealing-instance-promptly-rejected
      (let [{:keys [exit-chan query-state-chan work-stealing-chan]}
            (launch-service-chan-responder 14 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]})]
        (counters/clear! (metrics/service-counter service-id "request-counts" "outstanding")) ;; clear the counter to zero
        (let [response-chan-1 (make-work-stealing-offer work-stealing-chan "test-router" "testabcd.h1") ;; offer a known instance
              _ (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding") 2) ;; fewer outstanding requests than available slots
              response-chan-2 (make-work-stealing-offer work-stealing-chan "test-router" "testabcd.h2") ;; offer a known instance
              response-chan-3 (make-work-stealing-offer work-stealing-chan "test-router" "testabcd.h4") ;; offer an unknown instance]
              ]
          (check-state-fn query-state-chan
                          {:id->instance id->instance-data
                           :instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                     "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                   (update-slot-state-fn "testabcd.h2" 1 0)
                                                   (update-slot-state-fn "testabcd.h3" 8 0)
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                           :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                           :request-id->work-stealer {}
                           :work-stealing-queue (make-queue [])})
          (is (= :promptly-rejected (async/<!! response-chan-1)))
          (is (= :promptly-rejected (async/<!! response-chan-2)))
          (is (= :promptly-rejected (async/<!! response-chan-3))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-offer-workstealing-instance-accepted
      (let [initial-state {:id->instance id->instance-data
                           :instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-11" {:cid "cid-11", :request-id "req-11", :reason :kill-instance}}
                                                                     "testabcd.h2" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :serve-request}}
                                                                     "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 1 0 #{:healthy :locked})
                                                   (update-slot-state-fn "testabcd.h2" 1 1)
                                                   (update-slot-state-fn "testabcd.h3" 0 0)
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                           :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]}
            {:keys [exit-chan query-state-chan work-stealing-chan]}
            (launch-service-chan-responder 14 initial-state)]
        (counters/clear! (metrics/service-counter service-id "request-counts" "outstanding")) ;; clear the counter to zero
        (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding") 20) ;; more outstanding requests than available slots
        (let [response-chan-1 (make-work-stealing-offer work-stealing-chan "test-router-1" "testabcd.h1") ;; offer a known instance
              response-chan-2 (make-work-stealing-offer work-stealing-chan "test-router-2" "testabcd.h2") ;; offer a known instance
              response-chan-3 (make-work-stealing-offer work-stealing-chan "test-router-1" "testabcd.h4") ;; offer an unknown instance
              ]
          (check-state-fn query-state-chan
                          (-> initial-state
                              (assoc :request-id->work-stealer {}
                                     :work-stealing-queue
                                     (make-queue [(make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                  (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                  (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-offer-workstealing-instance-rejected
      (let [initial-state {:id->instance id->instance-data
                           :instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                     "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                   (update-slot-state-fn "testabcd.h2" 1 0)
                                                   (update-slot-state-fn "testabcd.h3" 8 0)
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                           :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]}
            {:keys [exit-chan query-state-chan work-stealing-chan]}
            (launch-service-chan-responder 14 initial-state)]
        (counters/clear! (metrics/service-counter service-id "request-counts" "outstanding")) ;; clear the counter to zero
        (counters/inc! (metrics/service-counter service-id "request-counts" "outstanding") 20) ;; more outstanding requests than available slots
        (do
          (make-work-stealing-offer work-stealing-chan "test-router-1" "testabcd.h1") ;; offer a known instance
          (make-work-stealing-offer work-stealing-chan "test-router-2" "testabcd.h2") ;; offer a known instance
          (make-work-stealing-offer work-stealing-chan "test-router-1" "testabcd.h4") ;; offer an unknown instance
          (check-state-fn query-state-chan initial-state))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-workstealing-ensure-rejects-during-exit
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            response-chan-3 (async/chan 1)
            test-instance-id->state (-> {}
                                        (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                        (update-slot-state-fn "testabcd.h2" 1 0)
                                        (update-slot-state-fn "testabcd.h3" 8 0)
                                        (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan]}
            (launch-service-chan-responder 17 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                                               :request-id->work-stealer {}
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})]
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
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state (-> {}
                                                                       (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                                       (update-slot-state-fn "testabcd.h2" 1 0)
                                                                       (update-slot-state-fn "testabcd.h3" 8 0)
                                                                       (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                                               :request-id->work-stealer {}
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})]
        ;; more available slots than outstanding requests
        (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 5)
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 8 0)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                           (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})
        (is (= 2 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-1)))
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 8 0)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})
        (is (= 1 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-2)))
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state (-> {}
                                                 (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                                 (update-slot-state-fn "testabcd.h2" 1 0)
                                                 (update-slot-state-fn "testabcd.h3" 8 0)
                                                 (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {}
                         :work-stealing-queue (make-queue [])})
        (is (= 0 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :rejected (async/<!! response-chan-3)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-workstealing-instances-used
      ;; more outstanding requests than available slots
      (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 20)
      (let [response-chan-1 (async/chan 1)
            response-chan-2 (async/chan 1)
            response-chan-3 (async/chan 1)
            test-instance-id->state (-> {}
                                        (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                        (update-slot-state-fn "testabcd.h2" 1 0)
                                        (update-slot-state-fn "testabcd.h3" 8 0)
                                        (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan reserve-instance-chan]}
            (launch-service-chan-responder 17 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                                               :request-id->work-stealer {}
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})]
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h1") ;; use work-stealing instance despite it being locked
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                  "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")}
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                           (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h2")
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                  "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-19" {:cid "cid-19", :request-id "req-19", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")}
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))

        (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 5)
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                  "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-19" {:cid "cid-19", :request-id "req-19", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")}
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
                                        (update-slot-state-fn "testabcd.h1" 1 0 #{:healthy :locked})
                                        (update-slot-state-fn "testabcd.h2" 1 1)
                                        (update-slot-state-fn "testabcd.h3" 1 2)
                                        (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan reserve-instance-chan]}
            (launch-service-chan-responder 17 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                                         "testabcd.h2" {"req-08" {:cid "cid-08", :request-id "req-08", :reason :serve-request}}
                                                                                         "testabcd.h3" {"req-09" {:cid "cid-09", :request-id "req-09", :reason :serve-request}
                                                                                                        "req-10" {:cid "cid-10", :request-id "req-10", :reason :serve-request}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                                               :request-id->work-stealer {}
                                               :work-stealing-queue (make-queue [(make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                                                 (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                                                 (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})]
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h1") ;; use work-stealing instance even when no slots are available
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                  "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-08" {:cid "cid-08", :request-id "req-08", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-09" {:cid "cid-09", :request-id "req-09", :reason :serve-request}
                                                                                  "req-10" {:cid "cid-10", :request-id "req-10", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")}
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")
                                                           (make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})
        (check-reserve-request-instance-fn reserve-instance-chan "testabcd.h2")
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                  "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-08" {:cid "cid-08", :request-id "req-08", :reason :serve-request}
                                                                                  "req-19" {:cid "cid-19", :request-id "req-19", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-09" {:cid "cid-09", :request-id "req-09", :reason :serve-request}
                                                                                  "req-10" {:cid "cid-10", :request-id "req-10", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")}
                         :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "testabcd.h4" response-chan-3 "test-router-1")])})
        (is (= 3 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))

        (metrics/reset-counter (metrics/service-counter service-id "request-counts" "outstanding") 5)
        (async/>!! release-instance-chan [:dummy "data"]) ;; dummy request to trigger work-stealing node clearing
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                  "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                   "testabcd.h2" {"req-08" {:cid "cid-08", :request-id "req-08", :reason :serve-request}
                                                                                  "req-19" {:cid "cid-19", :request-id "req-19", :reason :serve-request}}
                                                                   "testabcd.h3" {"req-09" {:cid "cid-09", :request-id "req-09", :reason :serve-request}
                                                                                  "req-10" {:cid "cid-10", :request-id "req-10", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                    "req-19" (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")}
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
                                        (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                        (update-slot-state-fn "testabcd.h2" 1 0)
                                        (update-slot-state-fn "testabcd.h3" 8 0)
                                        (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 19 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                                        "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                                         "testabcd.h2" {"req-19" {:cid "cid-19", :request-id "req-19", :reason :serve-request}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                                               :request-id->work-stealer {"req-18" {:cid "cid-18", :instance {:id "testabcd.h1"}, :response-chan response-chan-1, :router-id "test-router-1"}
                                                                          "req-19" {:cid "cid-19", :instance {:id "testabcd.h2"}, :response-chan response-chan-2, :router-id "test-router-2"}}
                                               :work-stealing-queue (make-queue [])})]
        (release-instance-fn release-instance-chan "testabcd.h2" 19 :success)
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                  "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {"req-18" {:cid "cid-18", :instance {:id "testabcd.h1"}, :response-chan response-chan-1, :router-id "test-router-1"}}
                         :work-stealing-queue (make-queue [])})
        (is (= 1 (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))))
        (is (= :success (async/<!! response-chan-2)))
        (release-instance-fn release-instance-chan "testabcd.h1" 18 :success)
        (check-state-fn query-state-chan
                        {:id->instance id->instance-data
                         :instance-id->blacklist-expiry-time {}
                         :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}}
                                                                   "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                         :instance-id->consecutive-failures {"testabcd.u2" 1, "testabcd.u1" 1, "testabcd.u3" 1} ;; h1 loses its failure entry
                         :instance-id->state test-instance-id->state
                         :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                         :request-id->work-stealer {}
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
                                        (update-slot-state-fn "testabcd.h1" 4 0 #{:healthy :locked})
                                        (update-slot-state-fn "testabcd.h2" 1 0)
                                        (update-slot-state-fn "testabcd.h3" 8 0)
                                        (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 19 {:id->instance id->instance-data
                                               :instance-id->blacklist-expiry-time {}
                                               :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                                        "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                                         "testabcd.h2" {"req-19" {:cid "cid-19", :request-id "req-19", :reason :serve-request}}
                                                                                         "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                                               :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                                               :instance-id->state test-instance-id->state
                                               :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                                               :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")
                                                                          "req-19" (make-work-stealing-data "cid-16" "testabcd.h2" response-chan-2 "test-router-2")}
                                               :work-stealing-queue (make-queue [])})]
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] current-time)]
            (release-instance-fn release-instance-chan "testabcd.h2" 19 :instance-error)
            (check-state-fn query-state-chan
                            {:id->instance id->instance-data
                             :instance-id->blacklist-expiry-time {"testabcd.h2" (t/plus current-time (t/millis blacklist-backoff-base-time-ms))}
                             :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-12" {:cid "cid-12", :request-id "req-12", :reason :kill-instance}
                                                                                      "req-18" {:cid "cid-18", :request-id "req-18", :reason :serve-request}}
                                                                       "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                             :instance-id->consecutive-failures {"testabcd.h1" 1, "testabcd.h2" 1, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                             :instance-id->state (-> test-instance-id->state
                                                     (update-slot-state-fn "testabcd.h2" 1 0 #{:blacklisted :healthy}))
                             :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]
                             :request-id->work-stealer {"req-18" (make-work-stealing-data "cid-15" "testabcd.h1" response-chan-1 "test-router-1")}
                             :work-stealing-queue (make-queue [])})
            (is (= :instance-error (async/<!! response-chan-2))))
          (with-redefs [t/now (fn [] (t/plus current-time (t/millis (* 8 max-blacklist-time-ms))))]
            (check-state-fn query-state-chan nil)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-async-request-assigned-instance
      (let [initial-state {:id->instance id->instance-data,
                           :instance-id->blacklist-expiry-time {},
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}},
                                                                     "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}},
                                                                     "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request},
                                                                                    "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}},
                           :instance-id->consecutive-failures {"testabcd.h3" 2, "testabcd.u2" 1, "testabcd.u1" 1},
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 1 1)
                                                   (update-slot-state-fn "testabcd.h2" 1 1)
                                                   (update-slot-state-fn "testabcd.h3" 8 2)
                                                   (update-slot-state-fn "testabcd.u1" 0 0 #{:killed :unhealthy})
                                                   (update-slot-state-fn "testabcd.u2" 0 0 #{:killed :unhealthy}))
                           :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u1" "testabcd.u2"]}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 10 initial-state)]
        ; release a success async
        (release-instance-fn release-instance-chan "testabcd.h3" 4 :success-async)
        (check-state-fn query-state-chan
                        (-> initial-state
                            (assoc-in [:instance-id->request-id->use-reason-map "testabcd.h3" "req-4" :variant] :async-request)
                            (utils/dissoc-in [:instance-id->consecutive-failures "testabcd.h3"])))
        ; now-release the async-request assigned instance
        (release-instance-fn release-instance-chan "testabcd.h3" 4 :success)
        (check-state-fn query-state-chan
                        (-> initial-state
                            (utils/dissoc-in [:instance-id->request-id->use-reason-map "testabcd.h3" "req-4"])
                            (utils/dissoc-in [:instance-id->consecutive-failures "testabcd.h3"])
                            (update-in [:instance-id->state] update-slot-state-fn "testabcd.h3" 8 1)))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-release-async-request-work-stealing-instance
      (let [response-chan-1 (async/chan 4)
            response-chan-2 (async/chan 1)
            initial-state {:id->instance id->instance-data,
                           :instance-id->blacklist-expiry-time {},
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1", :request-id "req-1", :reason :serve-request}},
                                                                     "testabcd.h2" {"req-3" {:cid "cid-3", :request-id "req-3", :reason :serve-request}},
                                                                     "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request},
                                                                                    "req-5" {:cid "cid-5", :request-id "req-5", :reason :serve-request}}},
                           :instance-id->consecutive-failures {"testabcd.h3" 2, "testabcd.u2" 1, "testabcd.u1" 1},
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 1 1)
                                                   (update-slot-state-fn "testabcd.h2" 1 1)
                                                   (update-slot-state-fn "testabcd.h3" 8 1)
                                                   (update-slot-state-fn "testabcd.u1" 0 0 #{:killed :unhealthy})
                                                   (update-slot-state-fn "testabcd.u2" 0 0 #{:killed :unhealthy}))
                           :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.h4" "testabcd.u1" "testabcd.u2"]
                           :request-id->work-stealer {"req-4" (make-work-stealing-data "cid-4" "testabcd.h2" response-chan-1 "test-router-1")}
                           :work-stealing-queue (make-queue [(make-work-stealing-data "cid-7" "testabcd.h4" response-chan-2 "test-router-1")])}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 10 initial-state)]
        ; release a success async, it also triggers release of the work-stealing head
        (release-instance-fn release-instance-chan "testabcd.h3" 4 :success-async)
        (check-state-fn query-state-chan
                        (-> initial-state
                            (assoc-in [:instance-id->request-id->use-reason-map "testabcd.h3" "req-4" :variant] :async-request)
                            (utils/dissoc-in [:instance-id->consecutive-failures "testabcd.h3"])
                            (assoc :work-stealing-queue (make-queue []))))
        ; no writes on response channel
        (is (async/>!! response-chan-1 :dummy-data))
        (is (= :dummy-data (async/<!! response-chan-1)))
        ; now-release the async-request work-stealing instance
        (release-instance-fn release-instance-chan "testabcd.h3" 4 :success)
        (check-state-fn query-state-chan
                        (-> initial-state
                            (utils/dissoc-in [:instance-id->request-id->use-reason-map "testabcd.h3" "req-4"])
                            (utils/dissoc-in [:instance-id->consecutive-failures "testabcd.h3"])
                            (utils/dissoc-in [:request-id->work-stealer "req-4"])
                            (assoc :work-stealing-queue (make-queue []))))
        (is (= :success (async/<!! response-chan-1))) ; work-stealing instance released successfully
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-successfully-release-blacklisted-instance
      (let [initial-state {:instance-id->blacklist-expiry-time {"testabcd.h1" (t/plus (t/now) (t/millis 100000))}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                     "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                     "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"testabcd.h1" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 2 1 #{:blacklisted :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 2 1 #{:healthy})
                                                   (update-slot-state-fn "testabcd.h3" 8 0)
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check that releasing instance still works as expected"
          (release-instance-fn release-instance-chan "testabcd.h1" 16 :success)
          (check-state-fn query-state-chan
                          (-> initial-state
                              (utils/dissoc-in [:instance-id->blacklist-expiry-time "testabcd.h1"])
                              (utils/dissoc-in [:instance-id->request-id->use-reason-map "testabcd.h1" "req-16"])
                              (utils/dissoc-in [:instance-id->consecutive-failures "testabcd.h1"])
                              (update-in [:instance-id->state] #(update-slot-state-fn %1 "testabcd.h1" 2 0)))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-successfully-release-borrowed-blacklisted-instance
      (let [initial-state {:instance-id->blacklist-expiry-time {"testabcd.h1" (t/plus (t/now) (t/millis 100000))}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                     "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                     "testabcd.u3" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}}
                           :instance-id->consecutive-failures {"testabcd.h1" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 0 1 #{:blacklisted :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 2 1 #{:healthy})
                                                   (update-slot-state-fn "testabcd.h3" 8 0)
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check that releasing instance still works as expected"
          (release-instance-fn release-instance-chan "testabcd.h1" 16 :success)
          (check-state-fn query-state-chan
                          (-> initial-state
                              (utils/dissoc-in [:instance-id->blacklist-expiry-time "testabcd.h1"])
                              (utils/dissoc-in [:instance-id->request-id->use-reason-map "testabcd.h1" "req-16"])
                              (utils/dissoc-in [:instance-id->consecutive-failures "testabcd.h1"])
                              (update-in [:instance-id->state] #(update-slot-state-fn %1 "testabcd.h1" 0 0)))))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-slot-counts-for-locked-and-blacklisted-instance
      (let [initial-state {:instance-id->blacklist-expiry-time {"testabcd.h1" (t/plus (t/now) (t/millis 100000))}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                     "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                     "testabcd.h4" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}
                                                                     "testabcd.h5" {"req-10" {:cid "cid-10", :request-id "req-10", :reason :serve-request}
                                                                                    "req-11" {:cid "cid-11", :request-id "req-11", :reason :serve-request}}}
                           :instance-id->consecutive-failures {"testabcd.h1" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 2 1 #{:blacklisted :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 3 1 #{:healthy})
                                                   (update-slot-state-fn "testabcd.h3" 7 0)
                                                   (update-slot-state-fn "testabcd.h4" 11 0 #{:locked})
                                                   (update-slot-state-fn "testabcd.h5" 0 2))}
            {:keys [exit-chan query-state-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (check-state-fn query-state-chan initial-state)
        (assert-instance-counters {"slots-assigned" 23, "slots-available" 9, "slots-in-use" 4})
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-successfully-release-work-stealing-instance-success-async
      (let [response-chan-1 (async/chan 4)
            initial-state {:instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                     "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                     "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}}}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 0 1 #{:blacklisted :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 2 1 #{:healthy})
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))
                           :request-id->work-stealer {"req-4" (make-work-stealing-data "cid-4" "testabcd.h3" response-chan-1 "test-router-1")}}
            {:keys [exit-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check releasing instance with :success-async"
          (release-instance-fn release-instance-chan "testabcd.h3" 4 :success-async)
          (check-state-fn query-state-chan
                          (-> initial-state
                              (assoc-in [:instance-id->request-id->use-reason-map "testabcd.h3" "req-4" :variant] :async-request)
                              (update-in [:instance-id->state] #(update-slot-state-fn %1 "testabcd.h3" 0 0 #{}))))
          (async/>!! response-chan-1 :not-response)
          (is (= :not-response (async/<!! response-chan-1))))

        (testing "check releasing instance with :success after :success-async"
          (release-instance-fn release-instance-chan "testabcd.h3" 4 :success)
          (check-state-fn query-state-chan
                          (-> initial-state
                              (utils/dissoc-in [:instance-id->request-id->use-reason-map "testabcd.h3" "req-4"])
                              (utils/dissoc-in [:request-id->work-stealer "req-4"])
                              (update-in [:instance-id->state] #(update-slot-state-fn %1 "testabcd.h3" 0 0 #{}))))
          (async/>!! response-chan-1 :not-response)
          (is (= :success (async/<!! response-chan-1))))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-no-instances-assigned-hence-none-available-for-kill
      (let [initial-state {:instance-id->blacklist-expiry-time {}
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
      (let [initial-state {:instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 0 0 #{:healthy})
                                                   (update-slot-state-fn "testabcd.h2" 0 0 #{:healthy})
                                                   (update-slot-state-fn "testabcd.h5" 0 0 #{:healthy})
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))
                           :request-id->work-stealer {}}
            {:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (testing "check kill when no slots assigned"
          (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found)
          (check-state-fn query-state-chan initial-state))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-no-idle-instance-available-for-kill
      (let [response-chan-1 (async/chan 4)
            initial-state {:instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                     "testabcd.h2" {"req-14" {:cid "cid-14", :request-id "req-14", :reason :serve-request}
                                                                                    "req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                     "testabcd.h3" {"req-4" {:cid "cid-4", :request-id "req-4", :reason :serve-request}}}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 0 1 #{:blacklisted :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 2 2 #{:healthy})
                                                   (update-slot-state-fn "testabcd.h5" 0 0 #{:healthy})
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked}))
                           :request-id->work-stealer {"req-4" (make-work-stealing-data "cid-4" "testabcd.h3" response-chan-1 "test-router-1")}}
            {:keys [exit-chan kill-instance-chan query-state-chan]}
            (launch-service-chan-responder 20 initial-state)]

        (testing "check kill when no idle assigned instances"
          (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found)
          (check-state-fn query-state-chan initial-state))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-blacklisted-instance-cleanup
      (let [current-time (t/now)
            initial-state {:instance-id->blacklist-expiry-time {"testabcd.h1" (t/minus current-time (t/millis 1000))
                                                                "testabcd.h2" (t/minus current-time (t/millis 2000))
                                                                "testabcd.h3" (t/minus current-time (t/millis 3000))
                                                                "testabcd.h4" (t/plus current-time (t/millis 10000))}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}
                                                                     "testabcd.h2" {"req-15" {:cid "cid-15", :request-id "req-15", :reason :serve-request}}
                                                                     "testabcd.h4" {"req-13" {:cid "cid-13", :request-id "req-13", :reason :kill-instance}}
                                                                     "testabcd.h5" {"req-10" {:cid "cid-10", :request-id "req-10", :reason :serve-request}
                                                                                    "req-11" {:cid "cid-11", :request-id "req-11", :reason :serve-request}}}
                           :instance-id->consecutive-failures {"testabcd.h1" 2, "testabcd.u1" 1, "testabcd.u2" 1, "testabcd.u3" 1}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 2 1 #{:blacklisted :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 3 1 #{:blacklisted :healthy})
                                                   (update-slot-state-fn "testabcd.h3" 7 0 #{:blacklisted})
                                                   (update-slot-state-fn "testabcd.h4" 11 0 #{:blacklisted :locked})
                                                   (update-slot-state-fn "testabcd.h5" 0 2))}
            {:keys [exit-chan query-state-chan update-state-chan]}
            (launch-service-chan-responder 16 initial-state)]

        (check-state-fn query-state-chan initial-state)

        (testing "check unblacklist cleanup during state update"
          (let [update-state {:healthy-instances [{:id "testabcd.h1"}, {:id "testabcd.h2"}, {:id "testabcd.h5"}, {:id "testabcd.h6"}]
                              :unhealthy-instances [{:id "testabcd.u1"}, {:id "testabcd.u2"}]
                              :my-instance->slots {{:id "testabcd.h1"} 5, {:id "testabcd.h2"} 2, {:id "testabcd.h3"} 8,
                                                   {:id "testabcd.h4"} 2, {:id "testabcd.h5"} 1}}]
            (async/>!! update-state-chan [update-state current-time]))

          (check-state-fn query-state-chan
                          (-> initial-state
                              (assoc :instance-id->blacklist-expiry-time {"testabcd.h4" (t/plus current-time (t/millis 10000))}
                                     :instance-id->consecutive-failures {"testabcd.h1" 2, "testabcd.u1" 1, "testabcd.u2" 1}
                                     :instance-id->state (-> {}
                                                             (update-slot-state-fn "testabcd.h1" 5 1 #{:healthy})
                                                             (update-slot-state-fn "testabcd.h2" 2 1 #{:healthy})
                                                             (update-slot-state-fn "testabcd.h3" 8 0 #{})
                                                             (update-slot-state-fn "testabcd.h4" 2 0 #{:blacklisted :locked})
                                                             (update-slot-state-fn "testabcd.h5" 1 2 #{:healthy})
                                                             (update-slot-state-fn "testabcd.u1" 0 0 #{:unhealthy})
                                                             (update-slot-state-fn "testabcd.u2" 0 0 #{:unhealthy}))))))


        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-work-stealing-reject
      (let [response-chan-1 (async/promise-chan)
            response-chan-2 (async/promise-chan)
            initial-state {:instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map {"testabcd.h1" {"req-16" {:cid "cid-16", :request-id "req-16", :reason :serve-request}}}
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 1 1 #{:healthy}))
                           :work-stealing-queue (make-queue [(make-work-stealing-data "cid-17" "testabcd.h4" response-chan-1 "test-router-1")
                                                             (make-work-stealing-data "cid-18" "testabcd.h5" response-chan-2 "test-router-2")])}
            {:keys [exit-chan kill-instance-chan query-state-chan release-instance-chan]}
            (launch-service-chan-responder 20 initial-state)]

        (testing "trigger cleanup of work-stealing queue when releasing instance"
          (release-instance-fn release-instance-chan "testabcd.h1" 16 :success)
          (check-state-fn query-state-chan (-> initial-state
                                               (assoc :instance-id->request-id->use-reason-map {}
                                                      :instance-id->state (-> {}
                                                                              (update-slot-state-fn "testabcd.h1" 1 0 #{:healthy}))
                                                      :work-stealing-queue (make-queue [(make-work-stealing-data "cid-18" "testabcd.h5" response-chan-2 "test-router-2")]))))

          (async/>!! response-chan-1 :from-test)
          (is (= :rejected (async/<!! response-chan-1))))

        (testing "trigger cleanup of work-stealing queue when attempting to kill instance"
          (check-kill-request-instance-fn kill-instance-chan :no-matching-instance-found)
          (check-state-fn query-state-chan (-> initial-state
                                               (assoc :instance-id->request-id->use-reason-map {}
                                                      :instance-id->state (-> {}
                                                                              (update-slot-state-fn "testabcd.h1" 1 0 #{:healthy}))
                                                      :work-stealing-queue (make-queue []))))

          (async/>!! response-chan-2 :from-test)
          (is (= :rejected (async/<!! response-chan-2))))

        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-expired-instance-busy-with-all-outdated-requests
      (let [current-time (t/now)
            time-0 (->> (- expired-instance-timeout-ms 1000) (t/millis) (t/minus current-time))
            time-1 (->> (+ expired-instance-timeout-ms 1000) (t/millis) (t/minus current-time))
            time-2 (->> (+ expired-instance-timeout-ms 2000) (t/millis) (t/minus current-time))
            time-3 (->> (+ expired-instance-timeout-ms 3000) (t/millis) (t/minus current-time))
            instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-1}}
                                                     "testabcd.h2" {"req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request :time time-2}
                                                                    "req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request :time time-3}}
                                                     "testabcd.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request :time time-2}}
                                                     "testabcd.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance :time time-0}}}
            initial-state {:id->instance id->instance-data
                           :instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map instance-id->request-id->use-reason-map
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 1 1 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 1 2 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.h3" 8 1 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                           :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]}
            {:keys [exit-chan kill-instance-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]
        ; kill a healthy instance and clear the blacklist buffer
        (with-redefs [t/now (fn [] current-time)]
          (check-kill-request-instance-fn kill-instance-chan "testabcd.h1"))
        (->> (-> initial-state
                 (update-in
                   [:instance-id->request-id->use-reason-map "testabcd.h1"]
                   (fn [request-id->use-reason-map]
                     (assoc request-id->use-reason-map
                       "req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance})))
                 (update :instance-id->state
                         (fn [instance-id->state]
                           (-> instance-id->state
                               (update-slot-state-fn "testabcd.h1" 1 1 #{:expired :healthy :locked})))))
             (check-state-fn query-state-chan))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-kill-expired-instance-busy-with-some-outdated-requests
      (let [current-time (t/now)
            time-0 (->> (- expired-instance-timeout-ms 1000) (t/millis) (t/minus current-time))
            time-1 (->> (- expired-instance-timeout-ms 1000) (t/millis) (t/minus current-time))
            time-2 (->> (+ expired-instance-timeout-ms 2000) (t/millis) (t/minus current-time))
            time-3 (->> (+ expired-instance-timeout-ms 3000) (t/millis) (t/minus current-time))
            instance-id->request-id->use-reason-map {"testabcd.h0" {}
                                                     "testabcd.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-0}}
                                                     "testabcd.h2" {"req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request :time time-0}
                                                                    "req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request :time time-3}}
                                                     "testabcd.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request :time time-2}}
                                                     "testabcd.u3" {"req-13" {:cid "cid-13" :request-id "req-13" :reason :kill-instance :time time-1}}}
            initial-state {:id->instance id->instance-data
                           :instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map instance-id->request-id->use-reason-map
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h0" 1 0 #{:healthy}) ;; idle healthy instance
                                                   (update-slot-state-fn "testabcd.h1" 1 1 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 1 2 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.h3" 8 1 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.u3" 0 0 #{:locked :unhealthy}))
                           :sorted-instance-ids ["testabcd.h0" "testabcd.h1" "testabcd.h2" "testabcd.h3" "testabcd.u3"]}
            {:keys [exit-chan kill-instance-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]
        ; kill a healthy instance and clear the blacklist buffer
        (with-redefs [t/now (fn [] current-time)]
          (check-kill-request-instance-fn kill-instance-chan "testabcd.h3"))
        (->> (-> initial-state
                 (update-in
                   [:instance-id->request-id->use-reason-map "testabcd.h3"]
                   (fn [request-id->use-reason-map]
                     (assoc request-id->use-reason-map
                       "req-14" {:cid "cid-14" :request-id "req-14" :reason :kill-instance})))
                 (update :instance-id->state
                         (fn [instance-id->state]
                           (-> instance-id->state
                               (update-slot-state-fn "testabcd.h3" 8 1 #{:expired :healthy :locked})))))
             (check-state-fn query-state-chan))
        (async/>!! exit-chan :exit)))

    (deftest test-start-service-chan-responder-blacklist-expired-instance
      (let [current-time (t/now)
            time-0 (->> (+ expired-instance-timeout-ms 20000) (t/millis) (t/minus current-time))
            time-1 (->> (+ expired-instance-timeout-ms 10000) (t/millis) (t/minus current-time))
            time-2 (->> (- expired-instance-timeout-ms 10000) (t/millis) (t/minus current-time))
            time-3 (->> (- expired-instance-timeout-ms 20000) (t/millis) (t/minus current-time))
            instance-id->request-id->use-reason-map {"testabcd.h1" {"req-1" {:cid "cid-1" :request-id "req-1" :reason :serve-request :time time-0}
                                                                    "req-4" {:cid "cid-4" :request-id "req-4" :reason :serve-request :time time-1}}
                                                     "testabcd.h2" {"req-2" {:cid "cid-2" :request-id "req-2" :reason :serve-request :time time-2}
                                                                    "req-3" {:cid "cid-3" :request-id "req-3" :reason :serve-request :time time-3}}
                                                     "testabcd.h3" {"req-5" {:cid "cid-5" :request-id "req-5" :reason :serve-request :time time-2}}}
            initial-state {:id->instance id->instance-data
                           :instance-id->blacklist-expiry-time {}
                           :instance-id->request-id->use-reason-map instance-id->request-id->use-reason-map
                           :instance-id->consecutive-failures {}
                           :instance-id->state (-> {}
                                                   (update-slot-state-fn "testabcd.h1" 4 2 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.h2" 4 2 #{:expired :healthy})
                                                   (update-slot-state-fn "testabcd.h3" 8 1 #{:expired :healthy}))
                           :sorted-instance-ids ["testabcd.h1" "testabcd.h2" "testabcd.h3"]}
            {:keys [blacklist-instance-chan exit-chan query-state-chan]} (launch-service-chan-responder 13 initial-state)]
        ; try blacklisting an instance
        (with-redefs [t/now (fn [] current-time)]
          (testing "blacklist with lingering and active requests"
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h2" :in-use)
            (check-state-fn query-state-chan initial-state)

            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h3" :in-use)
            (check-state-fn query-state-chan initial-state))

          (testing "blacklist with only lingering requests"
            (check-blacklist-instance-fn blacklist-instance-chan "testabcd.h1" :blacklisted)
            (->> (-> initial-state
                     (update :instance-id->blacklist-expiry-time assoc "testabcd.h1" (t/plus current-time (t/millis blacklist-backoff-base-time-ms)))
                     (update :instance-id->state update-slot-state-fn "testabcd.h1" 4 2  #{:blacklisted :expired :healthy}))
                 (check-state-fn query-state-chan)))

          (async/>!! exit-chan :exit))))))
