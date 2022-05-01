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
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [waiter.scheduler :as scheduler]
            [waiter.scheduler.composite :refer :all]
            [waiter.test-helpers :as test-helpers])
  (:import (clojure.lang ExceptionInfo)))

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

(defrecord TestScheduler [scheduler-name service-ids]

  scheduler/ServiceScheduler

  (get-services [_]
    (map compute-service service-ids))

  (kill-instance [_ instance]
    {:identifier (:id instance) :operation :kill :scheduler-name scheduler-name})

  (service-exists? [_ service-id]
    {:identifier service-id :operation :exists :scheduler-name scheduler-name})

  (create-service-if-new [_ descriptor]
    {:identifier (:service-id descriptor) :operation :create :scheduler-name scheduler-name})

  (delete-service [_ service-id]
    {:identifier service-id :operation :delete :scheduler-name scheduler-name})

  (deployment-error-config [_ service-id]
    {:scheduler-name scheduler-name})

  (scale-service [_ service-id target-instances force]
    {:identifier (str service-id ":" target-instances ":" force) :operation :scale :scheduler-name scheduler-name})

  (retrieve-directory-content [_ service-id instance-id host directory]
    {:identifier (str service-id ":" instance-id ":" host ":" directory) :operation :directory :scheduler-name scheduler-name})

  (service-id->state [_ service-id]
    {:identifier service-id :operation :service-state :scheduler-name scheduler-name})

  (state [_ _]
    {:operation :scheduler-state :scheduler-name scheduler-name}))

(deftest test-service-id+scheduler-parameter->scheduler
  (let [service-id->service-description-fn {"bar" {"scheduler" "lorem"}
                                            "baz" {"name" "no-scheduler"}
                                            "foo" {"scheduler" "ipsum"}}
        scheduler-id->scheduler {"lorem" "lorem-scheduler"}
        default-scheduler :lorem
        selector-fn (create-scheduler-parameter-based-selector
                      {:default-scheduler default-scheduler
                       :scheduler-id->scheduler scheduler-id->scheduler
                       :service-id->service-description-fn service-id->service-description-fn})]
    (is (= "lorem-scheduler" (selector-fn "bar")))
    (is (= "lorem-scheduler" (selector-fn "baz")))
    (is (thrown-with-msg? ExceptionInfo #"No matching scheduler found!" (selector-fn "foo")))))

(deftest test-service-id+some-image-parameter->scheduler
  (let [service-id->service-description-fn {"bar" {"image" "i1"}
                                            "baz" {"name" "no-image"}}
        scheduler-id->scheduler {"lorem" "lorem-scheduler"
                                 "ipsum" "ipsum-scheduler"}
        default-scheduler :lorem
        image-scheduler :ipsum
        selector-fn (create-some-image-parameter-based-selector
                      {:default-scheduler default-scheduler
                       :image-scheduler image-scheduler
                       :scheduler-id->scheduler scheduler-id->scheduler
                       :service-id->service-description-fn service-id->service-description-fn})]
    (is (= "ipsum-scheduler" (selector-fn "bar")))
    (is (= "lorem-scheduler" (selector-fn "baz")))))

(defn create-test-scheduler
  [{:keys [scheduler-name service-ids service-id->service-description-fn service-id->password-fn]}]
  (is service-id->service-description-fn)
  (is service-id->password-fn)
  (->TestScheduler scheduler-name service-ids))

(deftest test-initialize-component-schedulers
  (let [components {:lorem {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                            :scheduler-name "scheduler-lorem"
                            :service-ids #{"s1-a" "s1-b" "s1-c"}}
                    :ipsum {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                            :scheduler-name "scheduler-ipsum"
                            :service-ids #{"s2-a" "s2-b" "s2-c" "s2-d" "s2-e"}}}
        config {:components components
                :default-scheduler :ipsum
                :service-id->password-fn (fn [service-id] (str service-id ".password"))
                :service-id->service-description-fn (fn [service-id] {"name" service-id})}
        actual (initialize-component-schedulers config)]
    (is (every? #(and (contains? % :scheduler) (contains? % :scheduler-state-chan)) (vals actual)))
    (is (= (->> components
                (pc/map-keys name)
                (pc/map-vals #(select-keys % [:scheduler-name :service-ids])))
           (->> actual
                (pc/map-vals :scheduler)
                (pc/map-vals #(select-keys % [:scheduler-name :service-ids])))))))

(deftest test-start-scheduler-state-aggregator
  (with-redefs [t/now (constantly (tc/from-long 8000))]
    (let [scheduler-state-chan (async/chan 10)
          fee-scheduler-state-chan (async/chan)
          fie-scheduler-state-chan (async/chan)
          foe-scheduler-state-chan (async/chan)
          scheduler-id->state-chan {"fee" fee-scheduler-state-chan
                                    "fie" fie-scheduler-state-chan
                                    "foe" foe-scheduler-state-chan}
          {:keys [query-state-fn result-chan]} (start-scheduler-state-aggregator scheduler-state-chan scheduler-id->state-chan)]
      (let [fee-available-1 {:available-service-ids #{"fee-s1" "fee-s2"} :healthy-service-ids #{"fee-s2"} :scheduler-sync-time (tc/from-long 10000)}
            fie-available-1 {:available-service-ids #{"fie-s1" "fie-s2"} :healthy-service-ids #{"fie-s2"} :scheduler-sync-time (tc/from-long 11000)}
            fee-available-2 {:available-service-ids #{"fee-s2"} :healthy-service-ids #{"fee-s2"} :scheduler-sync-time (tc/from-long 12000)}
            foe-available-1 {:available-service-ids #{"foe-s1"} :healthy-service-ids #{"foe-s1"} :scheduler-sync-time (tc/from-long 13000)}]

        (testing "first message from scheduler"
          (async/>!! fee-scheduler-state-chan [[:update-available-services fee-available-1]
                                               [:update-service-instances "fee-s1"]
                                               [:update-service-instances "fee-s2"]])
          (is (= [[:update-available-services fee-available-1]
                  [:update-service-instances "fee-s1"]
                  [:update-service-instances "fee-s2"]]
                 (async/<!! scheduler-state-chan)))
          (is (test-helpers/wait-for
                #(= {:scheduler-id->state-chan scheduler-id->state-chan
                     :scheduler-id->sync-time {"fee" (tc/from-long 10000)}
                     :scheduler-id->type->messages {"fee" {:update-available-services [[:update-available-services fee-available-1]]
                                                           :update-service-instances [[:update-service-instances "fee-s1"]
                                                                                      [:update-service-instances "fee-s2"]]}}}
                    (query-state-fn)))))

        (testing "messages from two schedulers"
          (async/>!! fie-scheduler-state-chan [[:update-available-services fie-available-1]
                                               [:update-service-instances "fie-s1"]
                                               [:update-service-instances "fie-s2"]])
          (is (= [[:update-available-services
                   {:available-service-ids #{"fee-s1" "fee-s2" "fie-s1" "fie-s2"}
                    :healthy-service-ids #{"fee-s2" "fie-s2"}
                    :scheduler-sync-time (tc/from-long 11000)}]
                  [:update-service-instances "fee-s1"]
                  [:update-service-instances "fee-s2"]
                  [:update-service-instances "fie-s1"]
                  [:update-service-instances "fie-s2"]]
                 (async/<!! scheduler-state-chan)))
          (is (test-helpers/wait-for
                #(= {:scheduler-id->state-chan scheduler-id->state-chan
                     :scheduler-id->sync-time {"fee" (tc/from-long 10000)
                                               "fie" (tc/from-long 11000)}
                     :scheduler-id->type->messages {"fee" {:update-available-services [[:update-available-services fee-available-1]]
                                                           :update-service-instances [[:update-service-instances "fee-s1"]
                                                                                      [:update-service-instances "fee-s2"]]}
                                                    "fie" {:update-available-services [[:update-available-services fie-available-1]]
                                                           :update-service-instances [[:update-service-instances "fie-s1"]
                                                                                      [:update-service-instances "fie-s2"]]}}}
                    (query-state-fn)))))

        (testing "update from first scheduler"
          (async/>!! fee-scheduler-state-chan [[:update-available-services fee-available-2]
                                               [:update-service-instances "fee-s2"]])
          (is (= [[:update-available-services
                   {:available-service-ids #{"fee-s2" "fie-s1" "fie-s2"}
                    :healthy-service-ids #{"fee-s2" "fie-s2"}
                    :scheduler-sync-time (tc/from-long 12000)}]
                  [:update-service-instances "fee-s2"]
                  [:update-service-instances "fie-s1"]
                  [:update-service-instances "fie-s2"]]
                 (async/<!! scheduler-state-chan)))
          (is (test-helpers/wait-for
                #(= {:scheduler-id->state-chan scheduler-id->state-chan
                     :scheduler-id->sync-time {"fee" (tc/from-long 12000)
                                               "fie" (tc/from-long 11000)}
                     :scheduler-id->type->messages {"fee" {:update-available-services [[:update-available-services fee-available-2]]
                                                           :update-service-instances [[:update-service-instances "fee-s2"]]}
                                                    "fie" {:update-available-services [[:update-available-services fie-available-1]]
                                                           :update-service-instances [[:update-service-instances "fie-s1"]
                                                                                      [:update-service-instances "fie-s2"]]}}}
                    (query-state-fn)))))

        (testing "messages from three schedulers"
          (async/>!! foe-scheduler-state-chan [[:update-available-services foe-available-1]
                                               [:update-service-instances "foe-s1"]])
          (is (= [[:update-available-services
                   {:available-service-ids #{"fee-s2" "fie-s1" "fie-s2" "foe-s1"}
                    :healthy-service-ids #{"fee-s2" "fie-s2" "foe-s1"}
                    :scheduler-sync-time (tc/from-long 13000)}]
                  [:update-service-instances "fee-s2"]
                  [:update-service-instances "fie-s1"]
                  [:update-service-instances "fie-s2"]
                  [:update-service-instances "foe-s1"]]
                 (async/<!! scheduler-state-chan)))
          (is (test-helpers/wait-for
                #(= {:scheduler-id->state-chan scheduler-id->state-chan
                     :scheduler-id->sync-time {"fee" (tc/from-long 12000)
                                               "fie" (tc/from-long 11000)
                                               "foe" (tc/from-long 13000)}
                     :scheduler-id->type->messages {"fee" {:update-available-services [[:update-available-services fee-available-2]]
                                                           :update-service-instances [[:update-service-instances "fee-s2"]]}
                                                    "fie" {:update-available-services [[:update-available-services fie-available-1]]
                                                           :update-service-instances [[:update-service-instances "fie-s1"]
                                                                                      [:update-service-instances "fie-s2"]]}
                                                    "foe" {:update-available-services [[:update-available-services foe-available-1]]
                                                           :update-service-instances [[:update-service-instances "foe-s1"]]}}}
                    (query-state-fn)))))

        (testing "first scheduler goes away"
          (async/close! fee-scheduler-state-chan)
          (is (= [[:update-available-services
                   {:available-service-ids #{"fie-s1" "fie-s2" "foe-s1"}
                    :healthy-service-ids #{"fie-s2" "foe-s1"}
                    :scheduler-sync-time (tc/from-long 13000)}]
                  [:update-service-instances "fie-s1"]
                  [:update-service-instances "fie-s2"]
                  [:update-service-instances "foe-s1"]]
                 (async/<!! scheduler-state-chan))))

        (testing "second scheduler goes away"
          (async/close! foe-scheduler-state-chan)
          (is (= [[:update-available-services
                   {:available-service-ids #{"fie-s1" "fie-s2"}
                    :healthy-service-ids #{"fie-s2"}
                    :scheduler-sync-time (tc/from-long 13000)}]
                  [:update-service-instances "fie-s1"]
                  [:update-service-instances "fie-s2"]]
                 (async/<!! scheduler-state-chan))))

        (testing "third scheduler goes away"
          (async/close! fie-scheduler-state-chan)
          (is (= [[:update-available-services
                   {:available-service-ids #{}
                    :healthy-service-ids #{}
                    :scheduler-sync-time (tc/from-long 13000)}]]
                 (async/<!! scheduler-state-chan)))

          (testing "start-scheduler-state-aggregator go block must complete"
            (is (nil? (async/<!! result-chan)))
            (is (test-helpers/wait-for
                  #(= {:scheduler-id->state-chan {}
                       :scheduler-id->sync-time {"fee" (tc/from-long 12000)
                                                 "fie" (tc/from-long 11000)
                                                 "foe" (tc/from-long 13000)}
                       :scheduler-id->type->messages {}}
                      (query-state-fn))))))))))

(deftest test-create-composite-scheduler-with-selectors
  (let [scheduler-state-chan (async/chan)
        default-scheduler :lorem
        service-id->password-fn (constantly "password")
        service-id->service-description-fn (constantly {})
        scheduler-config {:components {:lorem {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                                               :scheduler-name "lorem"
                                               :service-ids ["lorem-fie" "lorem-foe" "ipsum-bar"]}
                                       :ipsum {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                                               :scheduler-name "ipsum"
                                               :service-ids ["ipsum-fee" "ipsum-foo" "ipsum-fuu"]}}
                          :custom-components {:foo {:bar :baz}}
                          :default-scheduler default-scheduler
                          :scheduler-state-chan scheduler-state-chan
                          :service-id->password-fn service-id->password-fn
                          :service-id->service-description-fn service-id->service-description-fn}]

    (testing "missing selector context"
      (let [composite-scheduler (create-composite-scheduler scheduler-config)]
        (is composite-scheduler)
        (is (fn? (:query-aggregator-state-fn composite-scheduler)))))

    (testing "provides custom-components to service-id->scheduler-fn"
      (let [provided-context-atom (atom nil)]
        (with-redefs [create-some-image-parameter-based-selector
                      (fn [context] (reset! provided-context-atom context))]
          (let [scheduler-config (assoc scheduler-config
                                   :selector-context {:factory-fn 'waiter.scheduler.composite/create-some-image-parameter-based-selector
                                                      :image-scheduler :ipsum})
                composite-scheduler (create-composite-scheduler scheduler-config)]
            (is (some? composite-scheduler))
            (is (some? @provided-context-atom))
            (is (= (:custom-components scheduler-config)
                   (:custom-components @provided-context-atom)))))))

    (testing "using scheduler-parameter selector context"
      (let [old-create-scheduler-parameter-based-selector create-scheduler-parameter-based-selector
            function-called-atom (atom nil)]
        (with-redefs [create-scheduler-parameter-based-selector
                      (fn [context]
                        (reset! function-called-atom true)
                        (old-create-scheduler-parameter-based-selector context))]
          (let [scheduler-config (assoc scheduler-config
                                   :selector-context {:factory-fn 'waiter.scheduler.composite/create-scheduler-parameter-based-selector})
                composite-scheduler (create-composite-scheduler scheduler-config)]
            (is composite-scheduler)
            (is @function-called-atom))))

      (let [scheduler-config (assoc scheduler-config
                               :default-scheduler :foo
                               :selector-context {:factory-fn 'waiter.scheduler.composite/create-scheduler-parameter-based-selector})]
        (is (thrown-with-msg? AssertionError #"Assert failed"
                              (create-composite-scheduler scheduler-config)))))

    (testing "using image-parameter selector context"
      (let [old-create-some-image-parameter-based-selector create-some-image-parameter-based-selector
            function-called-atom (atom nil)]
        (with-redefs [create-some-image-parameter-based-selector
                      (fn [context]
                        (reset! function-called-atom true)
                        (old-create-some-image-parameter-based-selector context))]
          (let [scheduler-config (assoc scheduler-config
                                   :selector-context {:factory-fn 'waiter.scheduler.composite/create-some-image-parameter-based-selector
                                                      :image-scheduler :ipsum})
                composite-scheduler (create-composite-scheduler scheduler-config)]
            (is composite-scheduler)
            (is @function-called-atom))))

      (let [scheduler-config (assoc scheduler-config
                               :selector-context {:factory-fn 'waiter.scheduler.composite/create-some-image-parameter-based-selector
                                                  :image-scheduler :lorem})]
        (is (thrown-with-msg? AssertionError #"Assert failed"
                              (create-composite-scheduler scheduler-config))))

      (let [scheduler-config (-> scheduler-config
                               (dissoc :default-scheduler)
                               (assoc :selector-context {:factory-fn 'waiter.scheduler.composite/create-some-image-parameter-based-selector
                                                         :image-scheduler :ipsum}))]
        (is (thrown-with-msg? AssertionError #"Assert failed"
                              (create-composite-scheduler scheduler-config)))))))

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
                                                   :scheduler-name "lorem"
                                                   :service-ids ["lorem-fie" "lorem-foe" "ipsum-bar"]}
                                           :ipsum {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                                                   :scheduler-name "ipsum"
                                                   :service-ids ["ipsum-fee" "ipsum-foo" "ipsum-fuu"]}}
                              :scheduler-state-chan scheduler-state-chan
                              :service-id->service-description-fn service-id->service-description-fn
                              :service-id->password-fn service-id->password-fn}
            all-service-ids ["lorem-fie" "lorem-foe" "ipsum-fee" "ipsum-foo" "ipsum-fuu"]
            composite-scheduler (create-composite-scheduler scheduler-config)]

        (is composite-scheduler)
        (is (fn? (:query-aggregator-state-fn composite-scheduler)))

        (testing "scheduler resolution"
          (let [service-id->scheduler (:service-id->scheduler composite-scheduler)]
            (doseq [service-id all-service-ids]
              (is (= (service-id->scheduler-id service-id) (-> service-id service-id->scheduler :scheduler-name))))))

        (testing "get-services"
          (is (= (map compute-service all-service-ids)
                 (scheduler/get-services composite-scheduler))))

        (testing "kill-instance"
          (doseq [service-id all-service-ids]
            (is (= {:identifier (str service-id ".instance-id")
                    :operation :kill
                    :scheduler-name (service-id->scheduler-id service-id)}
                   (->> {:id (str service-id ".instance-id") :service-id service-id}
                        (scheduler/kill-instance composite-scheduler))))))

        (testing "service-exists?"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :exists
                    :scheduler-name (service-id->scheduler-id service-id)}
                   (scheduler/service-exists? composite-scheduler service-id)))))

        (testing "create-service-if-new"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :create
                    :scheduler-name (service-id->scheduler-id service-id)}
                   (scheduler/create-service-if-new composite-scheduler {:service-id service-id})))))

        (testing "delete-service"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :delete
                    :scheduler-name (service-id->scheduler-id service-id)}
                   (scheduler/delete-service composite-scheduler service-id)))))

        (testing "deployment-error-config"
          (doseq [service-id all-service-ids]
            (is (= {:scheduler-name (service-id->scheduler-id service-id)}
                   (scheduler/deployment-error-config composite-scheduler service-id)))))

        (testing "scale-service"
          (doseq [service-id all-service-ids]
            (is (= {:identifier (str service-id ":5:false")
                    :operation :scale
                    :scheduler-name (service-id->scheduler-id service-id)}
                   (scheduler/scale-service composite-scheduler service-id 5 false)))))

        (testing "retrieve-directory-content"
          (doseq [service-id all-service-ids]
            (is (= {:identifier (str service-id ":i:h:d")
                    :operation :directory
                    :scheduler-name (service-id->scheduler-id service-id)}
                   (scheduler/retrieve-directory-content composite-scheduler service-id "i" "h" "d")))))

        (testing "service-id->state"
          (doseq [service-id all-service-ids]
            (is (= {:identifier service-id
                    :operation :service-state
                    :scheduler-name (service-id->scheduler-id service-id)}
                   (scheduler/service-id->state composite-scheduler service-id)))))

        (testing "state"
          (is (= {:aggregator {:scheduler-id->state-chan {"ipsum" component-channel
                                                          "lorem" component-channel}
                               :scheduler-id->sync-time {}
                               :scheduler-id->type->messages {}}
                  :components {"ipsum" {:operation :scheduler-state :scheduler-name "ipsum"}
                               "lorem" {:operation :scheduler-state :scheduler-name "lorem"}}
                  :supported-include-params ["aggregator" "components"]
                  :type "CompositeScheduler"}
                 (scheduler/state composite-scheduler #{"aggregator" "components"}))))))

    (async/close! component-channel)
    (async/close! scheduler-state-chan)))
