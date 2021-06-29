(ns waiter.instance-tracker-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [waiter.instance-tracker :refer :all]
            [waiter.scheduler :as scheduler]
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
    (async/>!! query-chan {:include-flags #{"id->failed-date" "id->failed-instance" "id->healthy-instance"
                                            "instance-failure-handler"}
                           :response-chan temp-chan})
    (async/<!! temp-chan)))

(defn- create-watch-chans
  [x]
  (for [_ (range x)]
    (async/chan (async/sliding-buffer 1024))))

(defn add-watch-chans [tokens-watch-channels-update-chan watch-chans]
  (doseq [chan watch-chans]
    (async/put! tokens-watch-channels-update-chan chan)))

(defn remove-watch-chans [watch-chans]
  (doseq [chan watch-chans]
    (async/close! chan)))

(defmacro assert-channels-no-new-message
  [chans timeout-ms]
  `(let [chans# ~chans
         timeout-ms# ~timeout-ms
         timeout-chan# (async/timeout timeout-ms#)
         [msg# res-chan#] (-> chans#
                              (conj timeout-chan#)
                              (async/alts!! :priority true))]
     (is (= res-chan# timeout-chan#)
         (str "Expected no message from channel instead got: " msg#))))

(defmacro assert-channels-next-message-with-fn
  [chans msg-fn]
  `(let [chans# ~chans
         msg-fn# ~msg-fn
         res# (for [chan# chans#] (async/<!! chan#))]
     (is (every? msg-fn# res#))))

(defmacro assert-channels-next-event
  "Assert that list of channels next event"
  [chans msg]
  `(let [chans# ~chans
         msg# ~msg]
     (assert-channels-next-message-with-fn chans# #(and (:id %) (= (dissoc % :id) msg#)))))

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

        (stop-instance-tracker!! go-chan exit-chan)))

    (testing "instance-tracker daemon increments watch-chan count when adding a new watch-chan"
      (let [ev-handler-call-history-atom (atom {:handle-instances-event! [] :state []})
            ev-handler (TestInstanceFailureHandler. ev-handler-call-history-atom)
            router-state-chan (au/latest-chan)
            {:keys [exit-chan go-chan instance-watch-channels-update-chan query-chan]}
            (start-instance-tracker clock router-state-chan ev-handler)]
        (is (= (:watch-count (get-latest-instance-tracker-state!! query-chan))
               0))
        (add-watch-chans instance-watch-channels-update-chan (create-watch-chans 10))
        (is (= (:watch-count (get-latest-instance-tracker-state!! query-chan))
               10))
        (stop-instance-tracker!! go-chan exit-chan)))

    (testing "instance-tracker does not send empty events to watch-chans"
      (let [ev-handler-call-history-atom (atom {:handle-instances-event! [] :state []})
            ev-handler (TestInstanceFailureHandler. ev-handler-call-history-atom)
            router-state-chan (au/latest-chan)
            watch-chans (create-watch-chans 10)
            {:keys [exit-chan go-chan instance-watch-channels-update-chan query-chan]}
            (start-instance-tracker clock router-state-chan ev-handler)]
        (add-watch-chans instance-watch-channels-update-chan watch-chans)
        (assert-channels-next-event watch-chans {:object {:healthy-instances {:updated []}}
                                                 :type :initial})
        (async/>!! router-state-chan {:service-id->failed-instances {}
                                      :service-id->healthy-instances {}})
        (assert-channels-no-new-message watch-chans 1000)
        (stop-instance-tracker!! go-chan exit-chan)))

    (testing "instance-tracker daemon determines updated healthy instances if a field changed"
      (let [ev-handler-call-history-atom (atom {:handle-instances-event! [] :state []})
            ev-handler (TestInstanceFailureHandler. ev-handler-call-history-atom)
            router-state-chan (au/latest-chan)
            watch-chans (create-watch-chans 10)
            started-at (t/minus (clock) (t/hours 1))
            inst-1-original (scheduler/->ServiceInstance "s1.i1" "s1" started-at nil nil #{} nil "host" 123 [] "/log" "test")
            inst-1-changed (scheduler/->ServiceInstance "s1.i1" "s1" started-at nil nil #{} nil "different-host" 123 [] "/log" "test")
            {:keys [exit-chan go-chan instance-watch-channels-update-chan]}
            (start-instance-tracker clock router-state-chan ev-handler)]
        (async/>!! router-state-chan {:service-id->failed-instances {}
                                      :service-id->healthy-instances {"s1" [inst-1-original]}})
        (add-watch-chans instance-watch-channels-update-chan watch-chans)
        (assert-channels-next-event watch-chans {:object {:healthy-instances {:updated [inst-1-original]}}
                                                 :type :initial})
        (async/put! router-state-chan {:service-id->failed-instances {}
                                       :service-id->healthy-instances {"s1" [inst-1-changed]}})
        (assert-channels-next-event watch-chans {:object {:healthy-instances {:updated [inst-1-changed]}}
                                                 :type :events})
        (stop-instance-tracker!! go-chan exit-chan)))))
