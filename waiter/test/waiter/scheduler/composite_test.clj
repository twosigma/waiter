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
(ns waiter.scheduler.composite-test
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [waiter.scheduler :as scheduler]
            [waiter.scheduler.composite :refer :all]))

(deftest test-process-invalid-services
  (let [delete-service-atom (atom [])
        scheduler (reify scheduler/ServiceScheduler
                    (delete-service [_ service-id] (swap! delete-service-atom conj service-id)))]

    (process-invalid-services scheduler ["foobar-service1" "feefie-service2"])

    (is (= ["foobar-service1" "feefie-service2"] @delete-service-atom))))

(defn- compute-service
  [service-id]
  (scheduler/make-Service {:id service-id :instance 2}))

(defn- compute-service-instances
  [service-id]
  [(scheduler/make-ServiceInstance {:id (str service-id ".inst1") :service-id service-id})
   (scheduler/make-ServiceInstance {:id (str service-id ".inst2") :service-id service-id})])

(defn- compute-service->instances
  [service-ids]
  (->> service-ids
       (pc/map-from-keys compute-service-instances)
       (pc/map-keys compute-service)))

(defrecord TestScheduler [scheduler-id service-ids]

  scheduler/ServiceScheduler

  (get-services [_]
    (map compute-service service-ids))

  (kill-instance [_ instance]
    {:identifier (:id instance) :operation :kill :scheduler-id scheduler-id})

  (service-exists? [_ service-id]
    {:identifier service-id :operation :exists :scheduler-id scheduler-id})

  (create-service-if-new [_ descriptor]
    {:identifier (:service-id descriptor) :operation :create :scheduler-id scheduler-id})

  (delete-service [_ service-id]
    {:identifier service-id :operation :delete :scheduler-id scheduler-id})

  (scale-service [_ service-id target-instances force]
    {:identifier (str service-id ":" target-instances ":" force) :operation :scale :scheduler-id scheduler-id})

  (retrieve-directory-content [_ service-id instance-id host directory]
    {:identifier (str service-id ":" instance-id ":" host ":" directory) :operation :directory :scheduler-id scheduler-id})

  (service-id->state [_ service-id]
    {:identifier service-id :operation :service-state :scheduler-id scheduler-id})

  (state [_]
    {:operation :scheduler-state :scheduler-id scheduler-id}))

(defn create-test-scheduler
  [{:keys [scheduler-id service-ids service-id->service-description-fn service-id->password-fn]}]
  (is service-id->service-description-fn)
  (is service-id->password-fn)
  (->TestScheduler scheduler-id service-ids))

(deftest test-initialize-component-schedulers
  (let [components {:lorem {:factory-fn 'create-test-scheduler
                            :scheduler-id "scheduler-lorem"
                            :service-ids #{"s1-a" "s1-b" "s1-c"}}
                    :ipsum {:factory-fn 'create-test-scheduler
                            :scheduler-id "scheduler-ipsum"
                            :service-ids #{"s2-a" "s2-b" "s2-c" "s2-d" "s2-e"}}}
        config {:components components
                :service-id->password-fn (fn [service-id] (str service-id ".password"))
                :service-id->service-description-fn (fn [service-id] {"name" service-id})}
        actual (initialize-component-schedulers config)]
    (is (every? #(and (contains? % :scheduler) (contains? % :scheduler-state-chan)) (vals actual)))
    (is (= (->> components
                (pc/map-keys name)
                (pc/map-vals #(select-keys % [:scheduler-id :service-ids])))
           (->> actual
                (pc/map-vals :scheduler)
                (pc/map-vals #(select-keys % [:scheduler-id :service-ids])))))))

(deftest test-start-scheduler-state-aggregator
  (let [scheduler-state-chan (async/chan 10)
        fee-scheduler-state-chan (async/chan)
        fie-scheduler-state-chan (async/chan)
        foe-scheduler-state-chan (async/chan)
        scheduler-id->scheduler-state-chan {"fee" fee-scheduler-state-chan
                                            "fie" fie-scheduler-state-chan
                                            "foe" foe-scheduler-state-chan}
        {:keys [query-state-fn result-chan]} (start-scheduler-state-aggregator scheduler-state-chan scheduler-id->scheduler-state-chan)]
    (let [fee-available-1 {:available-service-ids #{"fee-s1" "fee-s2"} :healthy-service-ids #{"fee-s2"} :scheduler-sync-time 10000}
          fie-available-1 {:available-service-ids #{"fie-s1" "fie-s2"} :healthy-service-ids #{"fie-s2"} :scheduler-sync-time 11000}
          fee-available-2 {:available-service-ids #{"fee-s2"} :healthy-service-ids #{"fee-s2"} :scheduler-sync-time 12000}
          foe-available-1 {:available-service-ids #{"foe-s1"} :healthy-service-ids #{"foe-s1"} :scheduler-sync-time 13000}]

      (testing "first message from scheduler"
        (async/>!! fee-scheduler-state-chan [[:update-available-services fee-available-1]
                                             [:update-service-instances "fee-s1"]
                                             [:update-service-instances "fee-s2"]])
        (is (= [[:update-available-services fee-available-1]
                [:update-service-instances "fee-s1"]
                [:update-service-instances "fee-s2"]]
               (async/<!! scheduler-state-chan)))
        (is (= {:scheduler-id->scheduler-messages {"fee" [[:update-available-services fee-available-1]
                                                          [:update-service-instances "fee-s1"]
                                                          [:update-service-instances "fee-s2"]]}
                :scheduler-id->scheduler-state-chan scheduler-id->scheduler-state-chan}
               (query-state-fn))))

      (testing "messages from two schedulers"
        (async/>!! fie-scheduler-state-chan [[:update-available-services fie-available-1]
                                             [:update-service-instances "fie-s1"]
                                             [:update-service-instances "fie-s2"]])
        (is (= [[:update-available-services
                 {:available-service-ids #{"fee-s1" "fee-s2" "fie-s1" "fie-s2"}
                  :healthy-service-ids #{"fee-s2" "fie-s2"}
                  :scheduler-sync-time 11000}]
                [:update-service-instances "fee-s1"]
                [:update-service-instances "fee-s2"]
                [:update-service-instances "fie-s1"]
                [:update-service-instances "fie-s2"]]
               (async/<!! scheduler-state-chan)))
        (is (= {:scheduler-id->scheduler-messages {"fee" [[:update-available-services fee-available-1]
                                                          [:update-service-instances "fee-s1"]
                                                          [:update-service-instances "fee-s2"]]
                                                   "fie" [[:update-available-services fie-available-1]
                                                          [:update-service-instances "fie-s1"]
                                                          [:update-service-instances "fie-s2"]]}
                :scheduler-id->scheduler-state-chan scheduler-id->scheduler-state-chan}
               (query-state-fn))))

      (testing "update from first scheduler"
        (async/>!! fee-scheduler-state-chan [[:update-available-services fee-available-2]
                                             [:update-service-instances "fee-s2"]])
        (is (= [[:update-available-services
                 {:available-service-ids #{"fee-s2" "fie-s1" "fie-s2"}
                  :healthy-service-ids #{"fee-s2" "fie-s2"}
                  :scheduler-sync-time 12000}]
                [:update-service-instances "fee-s2"]
                [:update-service-instances "fie-s1"]
                [:update-service-instances "fie-s2"]]
               (async/<!! scheduler-state-chan)))
        (is (= {:scheduler-id->scheduler-messages {"fee" [[:update-available-services fee-available-2]
                                                          [:update-service-instances "fee-s2"]]
                                                   "fie" [[:update-available-services fie-available-1]
                                                          [:update-service-instances "fie-s1"]
                                                          [:update-service-instances "fie-s2"]]}
                :scheduler-id->scheduler-state-chan scheduler-id->scheduler-state-chan}
               (query-state-fn))))

      (testing "messages from three schedulers"
        (async/>!! foe-scheduler-state-chan [[:update-available-services foe-available-1]
                                             [:update-service-instances "foe-s1"]])
        (is (= [[:update-available-services
                 {:available-service-ids #{"fee-s2" "fie-s1" "fie-s2" "foe-s1"}
                  :healthy-service-ids #{"fee-s2" "fie-s2" "foe-s1"}
                  :scheduler-sync-time 13000}]
                [:update-service-instances "fee-s2"]
                [:update-service-instances "fie-s1"]
                [:update-service-instances "fie-s2"]
                [:update-service-instances "foe-s1"]]
               (async/<!! scheduler-state-chan)))
        (is (= {:scheduler-id->scheduler-messages {"fee" [[:update-available-services fee-available-2]
                                                          [:update-service-instances "fee-s2"]]
                                                   "fie" [[:update-available-services fie-available-1]
                                                          [:update-service-instances "fie-s1"]
                                                          [:update-service-instances "fie-s2"]]
                                                   "foe" [[:update-available-services foe-available-1]
                                                          [:update-service-instances "foe-s1"]]}
                :scheduler-id->scheduler-state-chan scheduler-id->scheduler-state-chan}
               (query-state-fn))))

      (testing "first scheduler goes away"
        (async/close! fee-scheduler-state-chan)
        (is (= [[:update-available-services
                 {:available-service-ids #{"fie-s1" "fie-s2" "foe-s1"}
                  :healthy-service-ids #{"fie-s2" "foe-s1"}
                  :scheduler-sync-time 13000}]
                [:update-service-instances "fie-s1"]
                [:update-service-instances "fie-s2"]
                [:update-service-instances "foe-s1"]]
               (async/<!! scheduler-state-chan))))

      (testing "second scheduler goes away"
        (async/close! foe-scheduler-state-chan)
        (is (= [[:update-available-services
                 {:available-service-ids #{"fie-s1" "fie-s2"}
                  :healthy-service-ids #{"fie-s2"}
                  :scheduler-sync-time 11000}]
                [:update-service-instances "fie-s1"]
                [:update-service-instances "fie-s2"]]
               (async/<!! scheduler-state-chan))))

      (testing "third scheduler goes away"
        (async/close! fie-scheduler-state-chan)
        (is (= [[:update-available-services
                 {:available-service-ids #{}
                  :healthy-service-ids #{}
                  :scheduler-sync-time 0}]]
               (async/<!! scheduler-state-chan)))

        (testing "start-scheduler-state-aggregator go block must complete"
          (is (nil? (async/<!! result-chan)))
          (is (= {:scheduler-id->scheduler-messages {}
                  :scheduler-id->scheduler-state-chan {}}
                 (query-state-fn))))))))

(deftest test-composite-scheduler
  (let [scheduler-state-chan (async/chan)
        component-channel (async/chan 10)]
    (with-redefs [async/chan (constantly component-channel)]
      (let [service-id->scheduler-id #(let [[scheduler name] (str/split (str %) #"-" 2)]
                                        (when name
                                          scheduler))
            service-id->service-description-fn (fn [service-id]
                                                 (when-let [scheduler-id (service-id->scheduler-id service-id)]
                                                   {"scheduler" scheduler-id}))
            service-id->password-fn (constantly "password")
            scheduler-config {:components {:lorem {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                                                   :scheduler-id "lorem"
                                                   :service-ids ["lorem-fie" "lorem-foe" "ipsum-bar"]}
                                           :ipsum {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                                                   :scheduler-id "ipsum"
                                                   :service-ids ["ipsum-fee" "ipsum-foo" "ipsum-fuu"]}}
                              :scheduler-state-chan scheduler-state-chan
                              :service-description-defaults {"scheduler" "lorem"}
                              :service-id->service-description-fn service-id->service-description-fn
                              :service-id->password-fn service-id->password-fn}
            all-service-ids ["lorem-fie" "lorem-foe" "ipsum-fee" "ipsum-foo" "ipsum-fuu"]
            composite-scheduler (create-composite-scheduler scheduler-config)]

        (is composite-scheduler)
        (is (fn? (:query-aggregator-state-fn composite-scheduler)))

        (testing "scheduler resolution"
          (let [service-id->scheduler (:service-id->scheduler composite-scheduler)]
            (doseq [service-id all-service-ids]
              (is (= (service-id->scheduler-id service-id) (-> service-id service-id->scheduler :scheduler-id))))
            (testing "default"
              (is (nil? (service-id->scheduler-id "foo")))
              (is (= (-> scheduler-config :service-description-defaults (get "scheduler"))
                     (-> "foo" service-id->scheduler :scheduler-id))))))

        (testing "get-services"
          (is (= (map compute-service all-service-ids)
                 (scheduler/get-services composite-scheduler))))

        (testing "kill-instance"
          (doseq [service-id all-service-ids]
            (is (= {:identifier (str service-id ".instance-id")
                    :operation :kill
                    :scheduler-id (service-id->scheduler-id service-id)}
                   (->> {:id (str service-id ".instance-id") :service-id service-id}
                        (scheduler/kill-instance composite-scheduler))))))

        (testing "service-exists?"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :exists
                    :scheduler-id (service-id->scheduler-id service-id)}
                   (scheduler/service-exists? composite-scheduler service-id)))))

        (testing "create-service-if-new"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :create
                    :scheduler-id (service-id->scheduler-id service-id)}
                   (scheduler/create-service-if-new composite-scheduler {:service-id service-id})))))

        (testing "delete-service"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :delete
                    :scheduler-id (service-id->scheduler-id service-id)}
                   (scheduler/delete-service composite-scheduler service-id)))))

        (testing "scale-service"
          (doseq [service-id all-service-ids]
            (is (= {:identifier (str service-id ":5:false")
                    :operation :scale
                    :scheduler-id (service-id->scheduler-id service-id)}
                   (scheduler/scale-service composite-scheduler service-id 5 false)))))

        (testing "retrieve-directory-content"
          (doseq [service-id all-service-ids]
            (is (= {:identifier (str service-id ":i:h:d")
                    :operation :directory
                    :scheduler-id (service-id->scheduler-id service-id)}
                   (scheduler/retrieve-directory-content composite-scheduler service-id "i" "h" "d")))))

        (testing "service-id->state"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :service-state
                    :scheduler-id (service-id->scheduler-id service-id)}
                   (scheduler/service-id->state composite-scheduler service-id)))))

        (testing "state"
          (is (= {:aggregator {:scheduler-id->scheduler-messages {}
                               :scheduler-id->scheduler-state-chan {"ipsum" component-channel
                                                                    "lorem" component-channel}}
                  :components {"ipsum" {:operation :scheduler-state :scheduler-id "ipsum"}
                              "lorem" {:operation :scheduler-state :scheduler-id "lorem"}}}
                 (scheduler/state composite-scheduler))))))

    (async/close! component-channel)
    (async/close! scheduler-state-chan)))
