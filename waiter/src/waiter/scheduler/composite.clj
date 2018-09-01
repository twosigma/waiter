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
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [schema.core :as s]
            [waiter.scheduler :as scheduler]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]))

(defn process-invalid-services
  "Deletes the provided services on the specified scheduler."
  [scheduler service-ids]
  (log/info "found" (count service-ids) "misplaced services in" scheduler)
  (doseq [service-id service-ids]
    (log/info "deleting misplaced service" service-id "in" scheduler)
    (scheduler/delete-service scheduler service-id))
  (log/info "deleted" (count service-ids) "misplaced services in" scheduler))

(defn- retrieve-services
  "Retrieves the services for services that are configured to be running on the specified scheduler.
   If any service is found (e.g. due to default scheduler being changed) which does not belong in the
   specified scheduler, it is promptly deleted."
  [scheduler service-id->scheduler]
  (let [services (scheduler/get-services scheduler)
        valid-service? #(-> % :id service-id->scheduler (= scheduler))
        {valid-services true invalid-services false} (group-by valid-service? services)]
    (when (seq invalid-services)
      (->> invalid-services
           (map :id)
           (process-invalid-services scheduler)))
    valid-services))

(defrecord CompositeScheduler [service-id->scheduler scheduler-id->scheduler query-aggregator-state-fn]

  scheduler/ServiceScheduler

  (get-services [_]
    (->> (vals scheduler-id->scheduler)
         (pmap #(retrieve-services % service-id->scheduler))
         (reduce into [])))

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
    {:aggregator (query-aggregator-state-fn)
     :components (pc/map-vals scheduler/state scheduler-id->scheduler)}))

(defn service-id->scheduler
  "Resolves the scheduler for a given service-id using the scheduler defined in the description."
  [service-id->service-description-fn scheduler-id->scheduler default-scheduler service-id]
  (let [service-description (service-id->service-description-fn service-id)
        default-scheduler-id (when default-scheduler (name default-scheduler))
        scheduler-id (get service-description "scheduler" default-scheduler-id)]
    (if-let [scheduler (scheduler-id->scheduler scheduler-id)]
      scheduler
      (throw (ex-info "No matching scheduler found!"
                      {:available-schedulers (-> scheduler-id->scheduler keys sort)
                       :service-id service-id
                       :specified-scheduler scheduler-id})))))

(defn invoke-component-factory
  "Creates a component based on the factory-fn specified in the component-config."
  [context {:keys [factory-fn] :as component-config}]
  (if-let [resolved-fn (utils/resolve-symbol factory-fn)]
    (resolved-fn (merge context component-config))
    (throw (ex-info "Unable to resolve factory function" (assoc component-config :ns (namespace factory-fn))))))

(def component-schema
  {s/Keyword {(s/required-key :factory-fn) s/Symbol
              s/Keyword s/Any}})

(defn- initialize-component
  "Initializes a component scheduler with its own scheduler-state-chan.
   Returns a map containing the scheduler and scheduler-state-chan."
  [context component-key component-config]
  (let [component-name (name component-key)
        component-state-chan (au/latest-chan)
        component-context (assoc context
                            :scheduler-name component-name
                            :scheduler-state-chan component-state-chan)]
    {:scheduler (invoke-component-factory component-context component-config)
     :scheduler-state-chan component-state-chan}))

(defn initialize-component-schedulers
  "Initializes individual component schedulers."
  [{:keys [components default-scheduler] :as config}]
  {:pre [(seq components)
         (or (nil? default-scheduler)
             (contains? components default-scheduler))]}
  (s/validate component-schema components)
  (let [context (dissoc config :components)]
    (->> (keys components)
         (pc/map-from-keys (fn [component-key]
                             (->> (get components component-key)
                                  (initialize-component context component-key))))
         (pc/map-keys name))))

(defn start-scheduler-state-aggregator
  "Aggregates scheduler messages from multiple schedulers; aggregates them; and forwards it along to the
   scheduler-state-chan whenever there is a message received on the component scheduler-state-chan."
  [scheduler-state-chan scheduler-id->state-chan]
  (log/info "starting scheduler state aggregator for schedulers:" (keys scheduler-id->state-chan))
  (let [scheduler-state-chan->scheduler-id (set/map-invert scheduler-id->state-chan)
        scheduler-state-aggregator-atom (atom {})
        start-time (t/now)]
    {:query-state-fn (fn query-state-fn [] @scheduler-state-aggregator-atom)
     :result-chan
     (async/go
       (loop [{:keys [scheduler-id->state-chan scheduler-id->sync-time scheduler-id->type->messages] :as current-state}
              {:scheduler-id->state-chan scheduler-id->state-chan
               :scheduler-id->sync-time {}
               :scheduler-id->type->messages {}}]
         (reset! scheduler-state-aggregator-atom current-state)
         (if-not (seq scheduler-id->state-chan)
           (log/info "exiting scheduler state aggregator as all components scheduler-state-chan have been closed")
           (let [[scheduler-messages selected-chan] (async/alts! (vals scheduler-id->state-chan))
                 scheduler-id (scheduler-state-chan->scheduler-id selected-chan)
                 scheduler-chan-closed? (nil? scheduler-messages)]
             (if scheduler-chan-closed?
               (log/info scheduler-id "state chan has been closed")
               (log/info "received" (count scheduler-messages) "scheduler messages from" scheduler-id))
             (let [type->messages (group-by first scheduler-messages)
                   scheduler-id->type->messages' (if scheduler-chan-closed?
                                                   (dissoc scheduler-id->type->messages scheduler-id)
                                                   (assoc scheduler-id->type->messages
                                                     scheduler-id type->messages))
                   scheduler-id->state-chan' (cond-> scheduler-id->state-chan
                                               scheduler-chan-closed? (dissoc scheduler-id))
                   scheduler-id->sync-time' (cond-> scheduler-id->sync-time
                                              (not scheduler-chan-closed?)
                                              (assoc scheduler-id
                                                     (-> type->messages
                                                         :update-available-services
                                                         first
                                                         second
                                                         :scheduler-sync-time)))
                   available-services-messages (->> (vals scheduler-id->type->messages')
                                                    (mapcat :update-available-services))
                   services-message (->> available-services-messages
                                         (map #(select-keys (second %) [:available-service-ids :healthy-service-ids]))
                                         (apply merge-with set/union)
                                         (merge {:available-service-ids #{}
                                                 :healthy-service-ids #{}
                                                 :scheduler-sync-time (reduce du/max-time start-time (vals scheduler-id->sync-time'))}))
                   instances-messages (->> (vals scheduler-id->type->messages')
                                           (mapcat :update-service-instances)
                                           doall)
                   composite-scheduler-messages (conj instances-messages
                                                      [:update-available-services services-message])]
               (log/info "sending" (-> services-message :available-service-ids count) "services along scheduler-state-chan")
               (async/>! scheduler-state-chan composite-scheduler-messages)
               (recur (assoc current-state
                        :scheduler-id->state-chan scheduler-id->state-chan'
                        :scheduler-id->sync-time scheduler-id->sync-time'
                        :scheduler-id->type->messages scheduler-id->type->messages')))))))}))

(defn create-composite-scheduler
  "Creates and starts composite scheduler with components using their respective factory functions."
  [{:keys [default-scheduler scheduler-state-chan service-id->service-description-fn] :as config}]
  (let [scheduler-id->component (initialize-component-schedulers config)
        scheduler-id->scheduler (pc/map-vals :scheduler scheduler-id->component)
        scheduler-id->state-chan (pc/map-vals :scheduler-state-chan scheduler-id->component)
        service-id->scheduler-fn (fn service-id->scheduler-fn [service-id]
                                   (service-id->scheduler
                                     service-id->service-description-fn scheduler-id->scheduler default-scheduler service-id))
        {:keys [query-state-fn]} (start-scheduler-state-aggregator scheduler-state-chan scheduler-id->state-chan)]
    (->CompositeScheduler service-id->scheduler-fn scheduler-id->scheduler query-state-fn)))
