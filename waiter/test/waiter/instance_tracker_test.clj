(ns waiter.instance-tracker-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [waiter.instance-tracker :refer :all]
            [waiter.util.async-utils :as au])
  (:import (org.joda.time DateTime)))

(let [current-time (t/now)]
  (defn- clock [] current-time)

  (defn- clock-millis [] (.getMillis ^DateTime (clock))))

(defrecord TestInstanceFailureHandler [handler-state]

  InstanceEventHandler

  (handle-instances-event! [this instances-event]
    (swap! (:handler-state this)
           (fn [{:keys [handle-instances-event!] :as handler-state}]
             (assoc handler-state
               :handle-instances-event! (concat handle-instances-event! [[instances-event]])))))

  (state [this include-flags]
    (swap! (:handler-state this)
           (fn [{:keys [state] :as handler-state}]
             (assoc handler-state :state (concat state [include-flags]))))))

(defn stop-instance-tracker!!
  [go-chan exit-chan]
  (async/>!! exit-chan :exit)
  (async/<!! go-chan))

(defn get-latest-instance-tracker-state!!
  [query-chan]
  (let [temp-chan (async/promise-chan)]
    (async/>!! query-chan {:include-flags #{"id->failed-date" "id->failed-instance" "instance-failure-handler"}
                           :response-chan temp-chan})
    (async/<!! temp-chan)))

(deftest test-instance-tracker
  (let [router-state-1 {:service-id->failed-instances {"service-id-1" [{:id "s1.instance-id-1"}]}}
        router-state-2 {:service-id->failed-instances {}}
        router-state-3 {:service-id->failed-instances {"service-id-1" [{:id "s1.instance-id-1" :field "value changed"}]}}
        router-state-4 {:service-id->failed-instances {"service-id-2" [{:id "s2.instance-id-1"}
                                                                       {:id "s2.instance-id-2"}
                                                                       {:id "s2.instance-id-3"}]}}]

    (testing "instance-tracker daemon determines new failed instances from initial router-state"
      (let [ev-handler-call-history-atom (atom {:handle-instances-event! [] :state []})
            ev-handler (TestInstanceFailureHandler. ev-handler-call-history-atom)
            router-state-chan (au/latest-chan)
            {:keys [exit-chan go-chan query-chan]} (start-instance-tracker clock router-state-chan ev-handler)]

        ; instance-tracker considers initial failed instances as new failed instances because they are not persisted
        (async/>!! router-state-chan router-state-1)
        (let [{:keys [id->failed-instance]} (get-latest-instance-tracker-state!! query-chan)
              {:keys [handle-instances-event!]} @ev-handler-call-history-atom]
          (is (= {"s1.instance-id-1" {:id "s1.instance-id-1"}}
                 id->failed-instance))
          (is (= [[{:new-failed-instances [{:id "s1.instance-id-1"}]}]]
                 handle-instances-event!)))

        (stop-instance-tracker!! go-chan exit-chan)))

    (testing "instance-tracker daemon determines new failed instances from router-state changes"
      (let [ev-handler-call-history-atom (atom {:handle-instances-event! [] :state []})
            ev-handler (TestInstanceFailureHandler. ev-handler-call-history-atom)
            router-state-chan (au/latest-chan)
            {:keys [exit-chan go-chan query-chan]} (start-instance-tracker clock router-state-chan ev-handler)]

        ; assert no new failed instances
        (async/>!! router-state-chan router-state-2)
        (let [{:keys [id->failed-instance]} (get-latest-instance-tracker-state!! query-chan)
              {:keys [handle-instances-event!]} @ev-handler-call-history-atom]
          (is (= {} id->failed-instance))
          (is (= [] handle-instances-event!)))

        ; assert new failed instances
        (async/>!! router-state-chan router-state-1)
        (let [{:keys [id->failed-instance]} (get-latest-instance-tracker-state!! query-chan)
              {:keys [handle-instances-event!]} @ev-handler-call-history-atom]
          (is (= {"s1.instance-id-1" {:id "s1.instance-id-1"}}
                 id->failed-instance))
          (is (= [[{:new-failed-instances [{:id "s1.instance-id-1"}]}]]
                 handle-instances-event!)))

        (stop-instance-tracker!! go-chan exit-chan)))

    (testing "instance-tracker daemon determines new failed instances based on instance id changes"
      (let [ev-handler-call-history-atom (atom {:handle-instances-event! [] :state []})
            ev-handler (TestInstanceFailureHandler. ev-handler-call-history-atom)
            router-state-chan (au/latest-chan)
            {:keys [exit-chan go-chan query-chan]} (start-instance-tracker clock router-state-chan ev-handler)]

        ; initial new failed instances
        (async/>!! router-state-chan router-state-1)
        (let [{:keys [id->failed-instance]} (get-latest-instance-tracker-state!! query-chan)
              {:keys [handle-instances-event!]} @ev-handler-call-history-atom]
          (is (= {"s1.instance-id-1" {:id "s1.instance-id-1"}}
                 id->failed-instance))
          (is (= [[{:new-failed-instances [{:id "s1.instance-id-1"}]}]]
                 handle-instances-event!)))

        ; no new failed instances, just field changed
        (async/>!! router-state-chan router-state-3)
        (let [{:keys [id->failed-instance]} (get-latest-instance-tracker-state!! query-chan)
              {:keys [handle-instances-event!]} @ev-handler-call-history-atom]
          (is (= {"s1.instance-id-1" {:id "s1.instance-id-1" :field "value changed"}}
                 id->failed-instance))
          ; assert history did not change
          (is (= [[{:new-failed-instances [{:id "s1.instance-id-1"}]}]]
                 handle-instances-event!)))

        (stop-instance-tracker!! go-chan exit-chan)))

    (testing "instance-tracker daemon determines multiple new failed instances based changes"
      (let [ev-handler-call-history-atom (atom {:handle-instances-event! [] :state []})
            ev-handler (TestInstanceFailureHandler. ev-handler-call-history-atom)
            router-state-chan (au/latest-chan)
            {:keys [exit-chan go-chan query-chan]} (start-instance-tracker clock router-state-chan ev-handler)]

        ; initial new failed instances
        (async/>!! router-state-chan router-state-1)
        (let [{:keys [id->failed-instance]} (get-latest-instance-tracker-state!! query-chan)
              {:keys [handle-instances-event!]} @ev-handler-call-history-atom]
          (is (= {"s1.instance-id-1" {:id "s1.instance-id-1"}}
                 id->failed-instance))
          (is (= [[{:new-failed-instances [{:id "s1.instance-id-1"}]}]]
                 handle-instances-event!)))

        ; multiple new failed instances
        (async/>!! router-state-chan router-state-4)
        (let [{:keys [id->failed-instance]} (get-latest-instance-tracker-state!! query-chan)
              {:keys [handle-instances-event!]} @ev-handler-call-history-atom]
          (is (= {"s2.instance-id-1" {:id "s2.instance-id-1"}
                  "s2.instance-id-2" {:id "s2.instance-id-2"}
                  "s2.instance-id-3" {:id "s2.instance-id-3"}}
                 id->failed-instance))
          (is (= 2 (count handle-instances-event!)))
          (is (= #{{:id "s2.instance-id-1"}
                   {:id "s2.instance-id-2"}
                   {:id "s2.instance-id-3"}}
                 (some-> handle-instances-event! second first :new-failed-instances set))))

        (stop-instance-tracker!! go-chan exit-chan)))))