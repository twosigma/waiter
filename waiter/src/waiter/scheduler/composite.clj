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
(ns waiter.scheduler.composite
  (:require [clojure.string :as str]
            [plumbing.core :as pc]
            [schema.core :as s]
            [waiter.scheduler :as scheduler]
            [waiter.util.utils :as utils]))

(defrecord CompositeScheduler [service-id->scheduler scheduler-id->scheduler]

  scheduler/ServiceScheduler

  (get-service->instances [_]
    (->> (vals scheduler-id->scheduler)
         (map #(future (scheduler/get-service->instances %)))
         (map deref)
         (apply merge)))

  (get-services [_]
    (->> (vals scheduler-id->scheduler)
         (map #(future (scheduler/get-services %)))
         (map deref)
         (apply concat)))

  (get-instances [_ service-id]
    (-> service-id
        service-id->scheduler
        (scheduler/get-instances service-id)))

  (kill-instance [_ instance]
    (-> instance
        :service-id
        service-id->scheduler
        (scheduler/kill-instance instance)))

  (service-exists? [_ service-id]
    (-> service-id
        service-id->scheduler
        (scheduler/service-exists? service-id)))

  (create-service-if-new [_ descriptor]
    (-> descriptor
        :service-id
        service-id->scheduler
        (scheduler/create-service-if-new descriptor)))

  (delete-service [_ service-id]
    (-> service-id
        service-id->scheduler
        (scheduler/delete-service service-id)))

  (scale-service [_ service-id target-instances force]
    (-> service-id
        service-id->scheduler
        (scheduler/scale-service service-id target-instances force)))

  (retrieve-directory-content [_ service-id instance-id host directory]
    (-> service-id
        service-id->scheduler
        (scheduler/retrieve-directory-content service-id instance-id host directory)))

  (service-id->state [_ service-id]
    (-> service-id
        service-id->scheduler
        (scheduler/service-id->state service-id)))

  (state [_]
    (pc/map-vals scheduler/state scheduler-id->scheduler)))

(defn service-id->scheduler
  "Resolves the scheduler for a given service-id using the scheduler defined in the description.
   If the scheduler is not specified, the default scheduler is returned."
  [service-id->service-description-fn scheduler-id->scheduler default-scheduler-id service-id]
  (let [service-description (service-id->service-description-fn service-id)
        scheduler-id (get service-description "scheduler" default-scheduler-id)]
    (if-let [scheduler (scheduler-id->scheduler scheduler-id)]
      scheduler
      (throw (ex-info "Invalid scheduler specified!"
                      {:available-schedulers (-> scheduler-id->scheduler keys sort)
                       :service-id service-id
                       :specified-scheduler scheduler-id})))))

(defn invoke-component-factory
  "Creates a component based on the factory-fn specified in the component-config."
  [context {:keys [factory-fn] :as component-config}]
  (if factory-fn
    (if-let [resolved-fn (utils/resolve-symbol factory-fn)]
      (resolved-fn (merge context component-config))
      (throw (ex-info "Unable to resolve factory function" (assoc component-config :ns (namespace factory-fn)))))
    (throw (ex-info "No :factory-fn specified" component-config))))

(def component-schema
  {s/Keyword {(s/required-key :factory-fn) s/Symbol
              s/Keyword s/Any}})

(defn initialize-component-schedulers
  "Initializes individual component schedulers."
  [{:keys [components] :as config}]
  {:pre [(not-empty components)]}
  (s/validate component-schema components)
  (let [context (dissoc config :components :default)]
    (->> components
         (pc/map-vals #(invoke-component-factory context %))
         (pc/map-keys name))))

(defn create-composite-scheduler
  "Creates and starts composite scheduler with components using their respective factory functions."
  [{:keys [default service-id->service-description-fn] :as config}]
  {:pre [(not (str/blank? default))]}
  (let [scheduler-id->scheduler (initialize-component-schedulers config)
        service-id->scheduler-fn (fn service-id->scheduler-fn [service-id]
                                   (service-id->scheduler
                                     service-id->service-description-fn scheduler-id->scheduler default service-id))]
    (->CompositeScheduler service-id->scheduler-fn scheduler-id->scheduler)))
