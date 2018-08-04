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
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [waiter.scheduler :as scheduler]
            [waiter.scheduler.composite :refer :all]))

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

  (get-service->instances [_]
    (compute-service->instances service-ids))

  (get-services [_]
    (map compute-service service-ids))

  (get-instances [_ service-id]
    (compute-service-instances service-id))

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

(deftest test-composite-scheduler
  (let [service-id->scheduler-id #(let [[scheduler name] (str/split (str %) #"-" 2)]
                                    (when name
                                      scheduler))
        service-id->service-description-fn (fn [service-id]
                                             (when-let [scheduler-id (service-id->scheduler-id service-id)]
                                               {"scheduler" scheduler-id}))
        service-id->password-fn (constantly "password")
        scheduler-config {:components {:lorem {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                                               :scheduler-id "lorem"
                                               :service-ids ["lorem-fie" "lorem-foe"]}
                                       :ipsum {:factory-fn 'waiter.scheduler.composite-test/create-test-scheduler
                                               :scheduler-id "ipsum"
                                               :service-ids ["ipsum-fee" "ipsum-foo" "ipsum-fuu"]}}
                          :default "lorem"
                          :service-id->service-description-fn service-id->service-description-fn
                          :service-id->password-fn service-id->password-fn}
        all-service-ids ["lorem-fie" "lorem-foe" "ipsum-fee" "ipsum-foo" "ipsum-fuu"]
        composite-scheduler (create-composite-scheduler scheduler-config)]

    (is composite-scheduler)

    (testing "scheduler resolution"
      (let [service-id->scheduler (:service-id->scheduler composite-scheduler)]
        (doseq [service-id all-service-ids]
          (is (= (service-id->scheduler-id service-id) (-> service-id service-id->scheduler :scheduler-id))))
        (testing "default"
          (is (nil? (service-id->scheduler-id "foo")))
          (is (= (:default scheduler-config) (-> "foo" service-id->scheduler :scheduler-id))))))

    (testing "get-service->instances"
      (is (= (compute-service->instances all-service-ids)
             (scheduler/get-service->instances composite-scheduler))))

    (testing "get-services"
      (is (= (map compute-service all-service-ids)
             (scheduler/get-services composite-scheduler))))

    (testing "get-instances"
      (doseq [service-id all-service-ids]
        (is (= (compute-service-instances service-id)
               (scheduler/get-instances composite-scheduler service-id)))))

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
      (is (= {"ipsum" {:operation :scheduler-state :scheduler-id "ipsum"}
              "lorem" {:operation :scheduler-state :scheduler-id "lorem"}}
             (scheduler/state composite-scheduler))))))
