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
(ns waiter.state.ejection-expiry
  (:require [clojure.set :as set]
            [plumbing.core :as pc]))

(defrecord EjectionExpiryTracker
  [failure-threshold service-id->instance-ids-atom])

(defn tracker-state
  "Returns the state of the tracker."
  [{:keys [failure-threshold service-id->instance-ids-atom]} include-flags]
  (cond-> {:failure-threshold failure-threshold
           :supported-include-params ["service-details"]}
    (contains? include-flags "service-details")
    (assoc :service-id->instance-ids (pc/map-vals vec (deref service-id->instance-ids-atom)))))

(defn instance-expired?
  "Returns true if the instance is being tracked as expired due to consecutively failed requests."
  [{:keys [service-id->instance-ids-atom]} service-id instance-id]
  (contains? (get @service-id->instance-ids-atom service-id) instance-id))

(defn select-services!
  "Stops tracking services not in the provided service-ids."
  [{:keys [service-id->instance-ids-atom]} service-ids]
  (swap! service-id->instance-ids-atom select-keys service-ids))

(defn- update-service-id->instance-ids
  "Helper function to clear empty entries from service-id->instance-ids map."
  [service-id->instance-ids service-id new-instance-ids]
  (if (empty? new-instance-ids)
    (dissoc service-id->instance-ids service-id)
    (assoc service-id->instance-ids service-id new-instance-ids)))

(defn select-instances!
  "Stops tracking services not in the provided service-ids."
  [{:keys [service-id->instance-ids-atom]} service-id instance-ids]
  (swap! service-id->instance-ids-atom
         (fn inner-select-instances! [service-id->instance-ids]
           (let [cur-instance-ids (get service-id->instance-ids service-id)
                 new-instance-ids (set/intersection cur-instance-ids (set instance-ids))]
             (update-service-id->instance-ids service-id->instance-ids service-id new-instance-ids)))))

(defn track-consecutive-failures!
  "Determines if the provided instance should be tracked for expiry based on whether the consecutive failures
   has reached the configured failure threshold."
  [{:keys [failure-threshold service-id->instance-ids-atom]} service-id instance-id consecutive-failures]
  (swap! service-id->instance-ids-atom
         (fn inner-track-consecutive-failures! [service-id->instance-ids]
           (let [cur-instance-ids (get service-id->instance-ids service-id)
                 transform (if (and consecutive-failures (>= consecutive-failures failure-threshold)) conj disj)
                 new-instance-ids (transform (or cur-instance-ids #{}) instance-id)]
             (update-service-id->instance-ids service-id->instance-ids service-id new-instance-ids)))))
