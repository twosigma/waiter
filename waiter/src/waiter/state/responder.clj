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
(ns waiter.state.responder
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [metrics.core]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]
            [waiter.work-stealing :as work-stealing])
  (:import clojure.lang.PersistentQueue))

(defn- healthy?
  "Predicate on instances containing the :healthy status tag and optionally the :expired status tag."
  [{:keys [status-tags]}]
  (or (= #{:healthy} status-tags)
      (= #{:healthy :expired} status-tags)))

(defn expired?
  "Predicate on instances containing the :expired status tag."
  [{:keys [status-tags]}]
  (contains? status-tags :expired))

(defn- slots-available?
  "Predicate on healthy instances with available slots greater than used slots."
  [{:keys [slots-assigned slots-used] :as state}]
  (and (healthy? state) (> slots-assigned slots-used)))

(defn- only-lingering-requests?
  "Returns truthy value if the instance is idle or processing expired requests based on the earliest-request-threshold-time"
  [request-id->use-reason-map earliest-request-threshold-time]
  (every? #(t/before? % earliest-request-threshold-time)
          (->> request-id->use-reason-map
            vals
            (map :time))))

(defn- killable?
  "Only the following instances can be killed:
   - without slots used and not killed|locked, or
   - expired and idle or processing lingering requests.
   If there are expired instances, only choose healthy instances that are blacklisted or expired.
   If there are expired and starting instances, only kill unhealthy instances that are not starting."
  [request-id->use-reason-map earliest-request-threshold-time expired-instances? starting-instances?
   {:keys [slots-used status-tags] :as state}]
  (and (not-any? #(contains? status-tags %) [:killed :locked])
       (or (zero? slots-used)
           (and (expired? state)
                (only-lingering-requests? request-id->use-reason-map earliest-request-threshold-time)))
       (or (not (and expired-instances? (contains? status-tags :healthy)))
           (some #(contains? status-tags %) [:blacklisted :expired]))
       (or (not (and expired-instances? starting-instances?))
           (and (contains? status-tags :unhealthy)
                (not (contains? status-tags :starting))))))

(defn- find-max
  "Uses the provided comparison function to find the max element in collection."
  [comparison-fn collection]
  (reduce #(if (pos? (comparison-fn %1 %2)) %1 %2) (first collection) (rest collection)))

(defn- expired-instances?
  "Returns truthy value if there is some instance in an expired state."
  [instance-id->state]
  (some (fn [[_ {:keys [status-tags]}]] (contains? status-tags :expired)) instance-id->state))

(defn- starting-instances?
  "Returns truthy value if there is some instance in an starting state."
  [instance-id->state]
  (some (fn [[_ {:keys [status-tags]}]] (contains? status-tags :starting)) instance-id->state))

(defn find-killable-instance
  "While killing instances choose using the following logic:
   if there are expired and starting instances, only kill unhealthy instances that are not starting;
   choose instances in following order:
   - choose amongst the oldest idle expired instances
   - choose among expired instances with lingering requests
   - choose amongst the idle unhealthy instances
   - choose amongst the idle blacklisted instances
   - choose amongst the idle youngest healthy instances."
  [id->instance instance-id->state acceptable-instance-id? instance-id->request-id->use-reason-map
   load-balancing lingering-request-threshold-ms]
  (let [earliest-request-threshold-time (t/minus (t/now) (t/millis lingering-request-threshold-ms))
        instance-id-state-pair->categorizer-vec (fn [[instance-id {:keys [slots-used status-tags] :as state}]]
                                                  ; most important goes first
                                                  (vector
                                                    (zero? slots-used)
                                                    (contains? status-tags :expired)
                                                    (contains? status-tags :unhealthy)
                                                    (contains? status-tags :blacklisted)
                                                    (cond-> (or (some-> instance-id
                                                                  id->instance
                                                                  :started-at
                                                                  tc/to-long)
                                                                0)
                                                      ; invert sorting for expired instances or youngest load balancing
                                                      (or (expired? state)
                                                          (= :youngest load-balancing)) (unchecked-negate))))
        instance-id-comparator #(let [category-comparison (compare
                                                            (instance-id-state-pair->categorizer-vec %1)
                                                            (instance-id-state-pair->categorizer-vec %2))]
                                  (if (zero? category-comparison)
                                    (scheduler/instance-comparator
                                      (get id->instance (first %1))
                                      (get id->instance (first %2)))
                                    category-comparison))
        has-expired-instances (expired-instances? instance-id->state)
        has-starting-instances (starting-instances? instance-id->state)]
    (some->> instance-id->state
      (filter (fn [[instance-id _]] (and (acceptable-instance-id? instance-id)
                                         (contains? id->instance instance-id))))
      (filter (fn [[instance-id state]]
                (-> (instance-id->request-id->use-reason-map instance-id)
                  (killable? earliest-request-threshold-time has-expired-instances has-starting-instances state))))
      (find-max instance-id-comparator)
      first ; extract the instance-id
      id->instance)))

(defn find-available-instance
  "For servicing requests, choose the _oldest_ live healthy instance with available slots.
   If such an instance does not exist, choose the _youngest_ expired healthy instance with available slots."
  [sorted-instance-ids id->instance instance-id->state acceptable-instance-id? select-fn]
  (->> sorted-instance-ids
    (filter acceptable-instance-id?)
    (filter (fn [instance-id]
              (let [state (instance-id->state instance-id)]
                (and (not (nil? state))
                     (slots-available? state)
                     (healthy? state)))))
    select-fn
    id->instance))

(defn sort-instances-for-processing
  "Sorts two service instances, expired instances show up last after healthy instances.
   The comparison order is: expired, started-at, and finally id.\n   "
  [expired-instance-ids instances]
  (let [instance->feature-vector (memoize
                                   (fn [{:keys [id started-at]}]
                                     [(contains? expired-instance-ids id)
                                      (cond-> (or (some-> started-at .getMillis) 0)
                                        ; invert sorting for expired instances
                                        (contains? expired-instance-ids id)
                                        (unchecked-negate))
                                      id]))
        instance-comparator (fn [i1 i2]
                              (compare (instance->feature-vector i1) (instance->feature-vector i2)))]
    (sort instance-comparator instances)))

(defn- state->slots-available
  "Computes the slots available from the instance state."
  [{:keys [slots-assigned slots-used status-tags]}]
  (if (some #(contains? status-tags %1) #{:blacklisted, :killed, :locked})
    0
    (max 0 (int (- slots-assigned slots-used)))))

(defn compute-slots-values
  "Computes the aggregate values for slots assigned, available and in-use using data available in `instance-id->state`.
   Returns the aggregated vector [slots-assigned slots-used slots-available]."
  [instance-id->state]
  (reduce (fn [[slots-assigned-accum slots-used-accum slots-available-accum]
               {:keys [slots-assigned slots-used] :as instance-state}]
            [(+ slots-assigned-accum slots-assigned)
             (+ slots-used-accum slots-used)
             (+ slots-available-accum (state->slots-available instance-state))])
          [0 0 0]
          (vals instance-id->state)))

(defn- sanitize-instance-state
  "Ensures non-nil value for instance-state."
  [instance-state]
  (or instance-state {:slots-assigned 0, :slots-used 0, :status-tags #{}}))

(defn- update-slot-state
  "Updates the `slots-used` field in `instance-id->state` using the `slots-used-fn` function."
  [current-state instance-id slots-used-fn slots-in-use-counter slots-available-counter]
  (update-in current-state [:instance-id->state instance-id]
                           (fn [{:keys [slots-assigned slots-used] :as old-state}]
                             (let [new-slots-used (slots-used-fn slots-assigned slots-used)
                                   new-state (assoc old-state :slots-used new-slots-used)]
                               (counters/inc! slots-in-use-counter (- new-slots-used slots-used))
                               (counters/inc! slots-available-counter (- (state->slots-available new-state) (state->slots-available old-state)))
                               new-state))))

(defn- update-status-tag
  "Updates the status tags in the instance state and the slots-available-counter."
  [{:keys [status-tags] :as old-state} status-tag-fn slots-available-counter]
  (let [new-state (assoc old-state :status-tags (status-tag-fn status-tags))]
    (counters/inc! slots-available-counter (- (state->slots-available new-state) (state->slots-available old-state)))
    new-state))

(defn- complete-work-stealing-offer!
  "Completes a work-stealing offer by sending `response-status` in the response channel."
  [service-id {:keys [cid instance response-chan router-id] :as work-stealing-offer}
   response-status work-stealing-received-in-flight-counter]
  (when work-stealing-offer
    (cid/cdebug cid "work-stealing instance" (:id instance) "from" router-id "has status" response-status)
    (counters/dec! work-stealing-received-in-flight-counter)
    (let [release-counter-name (if (#{:rejected :promptly-rejected} response-status) "rejects" "releases")]
      (counters/inc! (metrics/service-counter service-id "work-stealing" "received-from" router-id release-counter-name)))
    (async/go (async/>! response-chan response-status))))

(defn- handle-exit-request
  [service-id {:keys [work-stealing-queue] :as current-state} work-stealing-received-in-flight-counter data]
  (if (= :exit data)
    (do
      (log/info "cleaning up work-stealing offers before exiting")
      ; cleanup state by rejecting any outstanding work-stealing offers, return nil since :exit was sent
      (doseq [work-stealing-offer (vec work-stealing-queue)]
        (complete-work-stealing-offer! service-id work-stealing-offer :rejected work-stealing-received-in-flight-counter)))
    current-state))

(defn update-slots-metrics
  "Resets the counters for the slots assigned, available and in-use using data available in `instance-id->state`."
  [instance-id->state slots-assigned-counter slots-available-counter slots-in-use-counter]
  (let [[slots-assigned slots-used slots-available] (compute-slots-values instance-id->state)]
    (metrics/reset-counter slots-assigned-counter slots-assigned)
    (metrics/reset-counter slots-available-counter slots-available)
    (metrics/reset-counter slots-in-use-counter slots-used)))

(defn- handle-update-state-request
  [{:keys [instance-id->state instance-id->consecutive-failures] :as current-state} data update-responder-state-timer
   update-responder-state-meter slots-assigned-counter slots-available-counter slots-in-use-counter]
  (timers/start-stop-time!
    update-responder-state-timer
    (when-let [[{:keys [healthy-instances unhealthy-instances my-instance->slots expired-instances starting-instances deployment-error instability-issue]} _] data]
      ; instances that are expired *might also* appear in healthy-instances, depending on whether
      ; or not this router has been assigned the expired instance
      ; stated differently, healthy-instances contains only instances that are both healthy
      ; *and* assigned to this router, which is different from how unhealthy-instances and
      ; expired-instances work
      ; expired instances always contains *all* expired instances for the service
      (let [healthy-instance-ids (set (map :id healthy-instances))
            unhealthy-instance-ids (set (map :id unhealthy-instances))
            my-instance-id->slots (pc/map-keys :id my-instance->slots)
            expired-instance-ids (set (map :id expired-instances))
            starting-instance-ids (set (map :id starting-instances))
            all-instance-ids (set (concat (keys my-instance-id->slots)
                                          (keys instance-id->state)
                                          unhealthy-instance-ids
                                          expired-instance-ids))
            sorted-instance-ids (->> (concat healthy-instances unhealthy-instances expired-instances)
                                  (set)
                                  (sort-instances-for-processing expired-instance-ids)
                                  (map :id))
            instance-id->state' (persistent!
                                  (reduce
                                    (fn [acc instance-id]
                                      (let [slots-assigned (get my-instance-id->slots instance-id 0)
                                            slots-used (get-in instance-id->state [instance-id :slots-used] 0)
                                            ; do not change blacklisted/locked state
                                            status-tags (let [existing-status-tags (get-in instance-id->state [instance-id :status-tags])
                                                              was-locked? (:locked existing-status-tags)
                                                              was-blacklisted? (:blacklisted existing-status-tags)
                                                              healthy? (contains? healthy-instance-ids instance-id)
                                                              unhealthy? (contains? unhealthy-instance-ids instance-id)
                                                              expired? (contains? expired-instance-ids instance-id)
                                                              starting? (contains? starting-instance-ids instance-id)]
                                                          (persistent!
                                                            (cond-> (transient #{})
                                                              healthy? (conj! :healthy)
                                                              unhealthy? (conj! :unhealthy)
                                                              expired? (conj! :expired)
                                                              starting? (conj! :starting)
                                                              (or healthy? unhealthy?) (disj! :killed)
                                                              was-locked? (conj! :locked)
                                                              was-blacklisted? (conj! :blacklisted))))]
                                        ; cleanup instances who have no useful state
                                        (if (and (zero? slots-assigned)
                                                 (zero? slots-used)
                                                 (not-any? #(contains? #{:blacklisted, :expired, :healthy, :locked, :unhealthy} %) status-tags))
                                          acc
                                          (assoc! acc instance-id {:slots-assigned slots-assigned
                                                                   :slots-used slots-used
                                                                   :status-tags status-tags}))))
                                    (transient {})
                                    all-instance-ids))
            id->instance' (persistent!
                            (reduce
                              (fn [acc {:keys [id] :as instance}]
                                (assoc! acc id instance))
                              (transient {})
                              (concat healthy-instances unhealthy-instances expired-instances)))
            instance-id->consecutive-failures' (into {} (filter (fn [[instance-id _]]
                                                                  (contains? all-instance-ids instance-id))
                                                                instance-id->consecutive-failures))]
        (update-slots-metrics instance-id->state' slots-assigned-counter slots-available-counter slots-in-use-counter)
        (meters/mark! update-responder-state-meter)
        (assoc current-state
          :deployment-error deployment-error
          :id->instance id->instance'
          :instance-id->state instance-id->state'
          :instance-id->consecutive-failures instance-id->consecutive-failures'
          :instability-issue instability-issue
          :sorted-instance-ids sorted-instance-ids)))))

(defn handle-work-stealing-offer
  "Handles a work-stealing offer.
   It returns a map with the following keys: current-state', response-chan, response.
   The entry for response may be nil if there's no immediate response."
  [{:keys [work-stealing-queue] :as current-state} service-id slots-in-use-counter slots-available-counter
   work-stealing-received-in-flight-counter requests-outstanding-counter {:keys [cid instance response-chan router-id] :as data}]
  (cid/cdebug cid "received work-stealing instance" (:id instance) "from" router-id)
  (counters/inc! (metrics/service-counter service-id "work-stealing" "received-from" router-id "offers"))
  (if (work-stealing/help-required?
        {"outstanding" (counters/value requests-outstanding-counter)
         "slots-available" (counters/value slots-available-counter)
         "slots-in-use" (counters/value slots-in-use-counter)
         "slots-received" (counters/value work-stealing-received-in-flight-counter)})
    (do
      (cid/cdebug cid "accepting work-stealing instance" (:id instance) "from" router-id)
      (counters/inc! work-stealing-received-in-flight-counter)
      {:current-state' (assoc current-state :work-stealing-queue (conj work-stealing-queue data))})
    (do
      (cid/cdebug cid "promptly rejecting work-stealing instance" (:id instance) "from" router-id)
      (counters/inc! (metrics/service-counter service-id "work-stealing" "received-from" router-id "rejects"))
      {:current-state' current-state
       :response-chan response-chan
       :response :promptly-rejected})))

(defn handle-reserve-instance-request
  "Handles a reserve request.
   Work-stealing offers are used with higher priority to enable releasing it quickly when the request is done.
   Instances from the available slots are looked up only when there are no work-stealing offers,
   this is expected to be the common case."
  [{:keys [deployment-error id->instance instance-id->state load-balancing request-id->work-stealer
           sorted-instance-ids work-stealing-queue] :as current-state}
   service-id update-slot-state-fn [{:keys [cid request-id] :as reason-map} resp-chan exclude-ids-set _]]
  (if deployment-error ; if a deployment error is associated with the state, return the error immediately instead of an instance
    {:current-state' current-state
     :response-chan resp-chan
     :response deployment-error}
    (if-let [{:keys [instance router-id] :as work-stealer-data} (first work-stealing-queue)]
      ; using instance received via work-stealing
      (let [instance-id (:id instance)]
        (cid/cdebug (str cid "|" (:cid work-stealer-data)) "using work-stealing instance" instance-id)
        (counters/inc! (metrics/service-counter service-id "work-stealing" "received-from" router-id "accepts"))
        {:current-state' (-> current-state
                           (assoc-in [:instance-id->request-id->use-reason-map instance-id request-id] reason-map)
                           (assoc :request-id->work-stealer (assoc request-id->work-stealer request-id work-stealer-data)
                                  :work-stealing-queue (pop work-stealing-queue)))
         :response-chan resp-chan
         :response instance})
      ; lookup available slots from router's pre-allocated instances
      (let [acceptable-instance-id? #(not (contains? exclude-ids-set %))
            select-fn (cond
                        (= :oldest load-balancing) first
                        (= :random load-balancing) rand-nth
                        (= :youngest load-balancing) last)
            {instance-id :id :as instance-to-offer}
            (find-available-instance sorted-instance-ids id->instance instance-id->state acceptable-instance-id? select-fn)]
        (if instance-to-offer
          {:current-state' (-> current-state
                             (assoc-in [:instance-id->request-id->use-reason-map instance-id request-id] reason-map)
                             (update-slot-state-fn instance-id #(inc %2)))
           :response-chan resp-chan
           :response instance-to-offer}
          {:current-state' current-state
           :response-chan resp-chan
           :response :no-matching-instance-found})))))

(defn handle-kill-instance-request
  "Handles a kill request."
  [{:keys [id->instance instance-id->request-id->use-reason-map instance-id->state load-balancing] :as current-state}
   update-status-tag-fn lingering-request-threshold-ms [{:keys [request-id] :as reason-map} resp-chan exclude-ids-set _]]
  (let [acceptable-instance-id? #(not (contains? exclude-ids-set %))
        instance (find-killable-instance id->instance instance-id->state acceptable-instance-id?
                                         instance-id->request-id->use-reason-map load-balancing
                                         lingering-request-threshold-ms)]
    (if instance
      (let [instance-id (:id instance)]
        {:current-state' (-> current-state
                           (assoc-in [:instance-id->request-id->use-reason-map instance-id request-id] reason-map)
                           ; mark instance as locked if it is going to be killed.
                           (update-in [:instance-id->state instance-id] update-status-tag-fn #(conj % :locked)))
         :response-chan resp-chan
         :response instance})
      {:current-state' current-state
       :response-chan resp-chan
       :response :no-matching-instance-found})))

(defn handle-release-instance-request
  "Handles a release instance request."
  [{:keys [instance-id->consecutive-failures instance-id->request-id->use-reason-map request-id->work-stealer] :as current-state} service-id
   update-slot-state-fn update-status-tag-fn update-state-by-blacklisting-instance-fn update-instance-id->blacklist-expiry-time-fn
   work-stealing-received-in-flight-counter max-blacklist-time-ms blacklist-backoff-base-time-ms max-backoff-exponent
   [{instance-id :id :as instance-to-release} {:keys [cid request-id status] :as reservation-result}]]
  (let [{:keys [reason] :as reason-map} (get-in instance-id->request-id->use-reason-map [instance-id request-id])
        work-stealing-data (get request-id->work-stealer request-id)]
    (if (nil? reason-map)
      current-state ;; no processing required if the request-id cannot be found
      (let [current-state'
            (if (not= status :success-async)
              (cond-> (-> current-state
                        (update-in [:instance-id->request-id->use-reason-map] utils/dissoc-in [instance-id request-id])
                        (update-in [:instance-id->state instance-id] sanitize-instance-state))
                ; instance received from work-stealing, do not change slot state
                (nil? work-stealing-data) (update-slot-state-fn instance-id #(cond-> %2 (not= :kill-instance reason) (-> (dec) (max 0))))
                ; mark instance as no longer locked.
                (nil? work-stealing-data) (update-in [:instance-id->state instance-id] update-status-tag-fn #(disj % :locked))
                ; clear work-stealing entry
                work-stealing-data (update-in [:request-id->work-stealer] dissoc request-id))
              (-> current-state
                (assoc-in [:instance-id->request-id->use-reason-map instance-id request-id :variant] :async-request)
                (update-in [:instance-id->state instance-id] sanitize-instance-state)))]
        (when-not (= status :success-async)
          (when work-stealing-data
            (complete-work-stealing-offer! service-id work-stealing-data status work-stealing-received-in-flight-counter)))
        (if (#{:instance-error :killed :instance-busy} status)
          ; mark as blacklisted and track the instance
          (let [consecutive-failures (inc (get instance-id->consecutive-failures instance-id 0))
                expiry-time-ms (min max-blacklist-time-ms
                                    (if (= :killed status)
                                      max-blacklist-time-ms
                                      (* blacklist-backoff-base-time-ms
                                         (Math/pow 2 (min max-backoff-exponent (dec consecutive-failures))))))]
            (cid/cinfo cid instance-id "with status" status "has" consecutive-failures "consecutive failures")
            (-> current-state'
              ; mark instance as blacklisted and killed based on status.
              (update-in [:instance-id->state instance-id] update-status-tag-fn #(cond-> (conj % :blacklisted) (= :killed status) (conj :killed)))
              ; track the blacklist expiry time
              (update-state-by-blacklisting-instance-fn cid instance-id expiry-time-ms)
              ; track the consecutive failure count
              (update-in [:instance-id->consecutive-failures] #(assoc % instance-id consecutive-failures))))
          ; else: clear out any failure records if instance has successfully processed a request
          (-> current-state'
            (update-instance-id->blacklist-expiry-time-fn #(if (= :not-killed status) % (dissoc % instance-id)))
            (update-in [:instance-id->consecutive-failures] #(if (= :not-killed status) % (dissoc % instance-id)))
            (update-in [:instance-id->state instance-id] update-status-tag-fn #(disj % :blacklisted))))))))

(defn handle-blacklist-request
  "Handle a request to blacklist an instance."
  [{:keys [instance-id->request-id->use-reason-map instance-id->state] :as current-state}
   update-status-tag-fn update-state-by-blacklisting-instance-fn lingering-request-threshold-ms
   [{:keys [instance-id blacklist-period-ms cid]} response-chan]]
  (cid/with-correlation-id
    cid
    (log/info "attempt to blacklist" instance-id "which has"
              (count (get instance-id->request-id->use-reason-map instance-id)) "uses with state:"
              (get instance-id->state instance-id))
    ;; cannot blacklist if the instance is not killable
    (let [instance-not-allowed? (and (contains? instance-id->state instance-id)
                                     (let [request-id->use-reason-map (instance-id->request-id->use-reason-map instance-id)
                                           earliest-request-threshold-time (t/minus (t/now) (t/millis lingering-request-threshold-ms))
                                           has-expired-instances (expired-instances? instance-id->state)
                                           has-starting-instances (starting-instances? instance-id->state)
                                           state (instance-id->state instance-id)]
                                       (not
                                         (killable? request-id->use-reason-map earliest-request-threshold-time
                                                    has-expired-instances has-starting-instances state))))
          response-code (if instance-not-allowed? :in-use :blacklisted)]
      {:current-state' (if (= :blacklisted response-code)
                         (-> current-state
                           ; mark instance as blacklisted and set the expiry time
                           (update-in [:instance-id->state instance-id] sanitize-instance-state)
                           (update-in [:instance-id->state instance-id] update-status-tag-fn #(conj % :blacklisted))
                           (update-state-by-blacklisting-instance-fn cid instance-id blacklist-period-ms))
                         current-state)
       :response-chan response-chan
       :response response-code})))

(defn- update-in-with-log-instance
  [mp log-body event-type log-level key-sequence update-fn func]
  (scheduler/log-service-instance log-body event-type log-level)
  (update-in mp key-sequence update-fn func))

(defn- unblacklist-instance
  [{:keys [id->instance instance-id->state] :as current-state} update-instance-id->blacklist-expiry-time-fn update-status-tag-fn
   instance-id expiry-time]
  (log/info "unblacklisting instance" instance-id "as blacklist expired at" expiry-time)
  (cond-> (update-instance-id->blacklist-expiry-time-fn current-state #(dissoc % instance-id))
    (contains? instance-id->state instance-id)
    (update-in-with-log-instance (get id->instance instance-id) :readmit :info [:instance-id->state instance-id] update-status-tag-fn #(disj % :blacklisted))))

(defn- expiry-time-reached?
  "Returns true if current-time is greater than or equal to expiry-time."
  [expiry-time current-time]
  (and expiry-time (not (t/after? expiry-time current-time))))

(defn- handle-unblacklist-request
  "Handle a request to unblacklist an instance."
  [{:keys [instance-id->blacklist-expiry-time] :as current-state} update-status-tag-fn
   update-instance-id->blacklist-expiry-time-fn {:keys [instance-id]}]
  (let [expiry-time (get instance-id->blacklist-expiry-time instance-id)]
    (if (expiry-time-reached? expiry-time (t/now))
      (unblacklist-instance current-state update-instance-id->blacklist-expiry-time-fn update-status-tag-fn
                            instance-id expiry-time)
      current-state)))

(defn- handle-unblacklist-cleanup-request
  "Handle cleanup of expired blacklisted instances."
  [{:keys [instance-id->blacklist-expiry-time] :as current-state} cleanup-time update-status-tag-fn
   update-instance-id->blacklist-expiry-time-fn]
  (let [expired-instance-id->expiry-time (->> instance-id->blacklist-expiry-time
                                           (filter #(expiry-time-reached? (val %) cleanup-time))
                                           (into {}))]
    (if (seq expired-instance-id->expiry-time)
      (letfn [(unblacklist-instance-fn [new-state [instance-id expiry-time]]
                (unblacklist-instance new-state update-instance-id->blacklist-expiry-time-fn update-status-tag-fn
                                      instance-id expiry-time))]
        (log/warn "found" (count expired-instance-id->expiry-time) "instances blacklisted beyond time of" cleanup-time)
        (reduce unblacklist-instance-fn current-state expired-instance-id->expiry-time))
      current-state)))

(defn- handle-scaling-state-request
  "Handle scaling-state update."
  [current-state service-id default-load-balancing scaling-state]
  (let [load-balancing (if (and (= :scale-down scaling-state)
                                (= :random default-load-balancing))
                         :youngest
                         default-load-balancing)]
    (log/info service-id "traffic distribution mode is now" load-balancing
              "as scaling mode is" scaling-state)
    (assoc current-state :load-balancing load-balancing)))

(defn release-unneeded-work-stealing-offers!
  "Releases the head of the work-stealing queue if no help is required."
  [current-state service-id slots-in-use-counter slots-available-counter work-stealing-received-in-flight-counter
   requests-outstanding-counter]
  (update-in current-state
    [:work-stealing-queue]
    (fn [work-stealing-queue]
      (when work-stealing-queue
        (if (work-stealing/help-required?
              {"outstanding" (counters/value requests-outstanding-counter)
               "slots-available" (counters/value slots-available-counter)
               "slots-in-use" (counters/value slots-in-use-counter)
               "slots-received" (counters/value work-stealing-received-in-flight-counter)})
          work-stealing-queue
          (let [offer (first work-stealing-queue)]
            (log/info "service-chan-responder deleting a work-stealing offer since help deemed unnecessary")
            (complete-work-stealing-offer! service-id offer :rejected work-stealing-received-in-flight-counter)
            (pop work-stealing-queue)))))))

(defn start-service-chan-responder
  "go block that maintains the available instances for one service.
  Instances are reserved using the reserve-instance-chan,
  instances are released using the release-instance-chan,
  instances are blacklisted using the blacklist-instance-chan,
  updated state is passed into the block through the update-state-chan,
  state queries are passed into the block through the query-state-chan."
  [service-id trigger-unblacklist-process-fn
   {:keys [blacklist-backoff-base-time-ms lingering-request-threshold-ms max-blacklist-time-ms]}
   {:keys [blacklist-instance-chan exit-chan kill-instance-chan query-state-chan release-instance-chan
           reserve-instance-chan scaling-state-chan unblacklist-instance-chan update-state-chan work-stealing-chan]}
   initial-state]
  {:pre [(some? (:load-balancing initial-state))]}
  (when (some nil? (vals initial-state))
    (throw (ex-info "Initial state contains nil values!" initial-state)))
  (let [max-backoff-exponent (->> (/ (Math/log max-blacklist-time-ms) (Math/log blacklist-backoff-base-time-ms))
                               Math/exp
                               inc
                               (max 1)
                               int)
        responder-timer (metrics/service-timer service-id "service-chan-responder-iteration")
        responder-reserve-timer (metrics/service-timer service-id "service-chan-responder-reserve")
        responder-release-timer (metrics/service-timer service-id "service-chan-responder-release")
        responder-kill-timer (metrics/service-timer service-id "service-chan-responder-kill")
        responder-update-timer (metrics/service-timer service-id "service-chan-responder-update")
        responder-blacklist-timer (metrics/service-timer service-id "service-chan-responder-blacklist")
        responder-unblacklist-timer (metrics/service-timer service-id "service-chan-responder-unblacklist")
        slots-available-counter (metrics/service-counter service-id "instance-counts" "slots-available")
        slots-assigned-counter (metrics/service-counter service-id "instance-counts" "slots-assigned")
        slots-in-use-counter (metrics/service-counter service-id "instance-counts" "slots-in-use")
        update-responder-state-timer (metrics/service-timer service-id "update-responder-state")
        update-responder-state-meter (metrics/service-meter service-id "update-responder-state")
        blacklisted-instance-counter (metrics/service-counter service-id "instance-counts" "blacklisted")
        in-use-instance-counter (metrics/service-counter service-id "instance-counts" "in-use")
        requests-outstanding-counter (metrics/service-counter service-id "request-counts" "outstanding")
        work-stealing-received-in-flight-counter (metrics/service-counter service-id "work-stealing" "received-from" "in-flight")
        update-slot-state-fn #(update-slot-state %1 %2 %3 slots-in-use-counter slots-available-counter)
        update-status-tag-fn #(update-status-tag %1 %2 slots-available-counter)
        update-instance-id->blacklist-expiry-time-fn
        (fn update-instance-id->blacklist-expiry-time-fn [current-state transform-fn]
          (update-in current-state [:instance-id->blacklist-expiry-time]
                                   (fn inner-update-instance-id->blacklist-expiry-time-fn [instance-id->blacklist-expiry-time]
                                     (let [instance-id->blacklist-expiry-time' (transform-fn instance-id->blacklist-expiry-time)]
                                       (metrics/reset-counter blacklisted-instance-counter (count instance-id->blacklist-expiry-time'))
                                       instance-id->blacklist-expiry-time'))))
        update-state-by-blacklisting-instance-fn
        (fn update-state-by-blacklisting-instance-fn [current-state correlation-id instance-id expiry-time-ms]
          (let [actual-expiry-time (t/plus (t/now) (t/millis expiry-time-ms))]
            (cid/cinfo correlation-id "blacklisting instance" instance-id "for" expiry-time-ms "ms.")
            (scheduler/log-service-instance (assoc (get (:id->instance current-state) instance-id) :blacklist-period-ms expiry-time-ms) :eject :info)
            (trigger-unblacklist-process-fn correlation-id instance-id expiry-time-ms unblacklist-instance-chan)
            (update-instance-id->blacklist-expiry-time-fn current-state #(assoc % instance-id actual-expiry-time))))
        default-load-balancing (:load-balancing initial-state)]
    (async/go
      (try
        (log/info "service-chan-responder started for" service-id "with initial state:" initial-state)
        ; `instance-id->blacklist-expiry-time` maintains a list of recently 'killed' or 'erroneous' instances (via the :killed or :instance-error status while releasing an instance).
        ; Such instances are guaranteed not to be served up as an available instance until sufficient time has elapsed since their last use.
        ; Killed instances are blacklisted for max-blacklist-time-ms milliseconds.
        ; Erroneous instances are blacklisted using an exponential delay based on number of successive failures and blacklist-backoff-base-time-ms.
        ; If inside this period the instance gets actually killed, it will no longer show up as a live instance from the scheduler.
        ; After the time period has elapsed and the instance is not actually killed, it will start showing up in the state.
        ; This can possibly happen as either the previous kill request for that instance backend has failed or the temporary error at the instance has gone away.
        ; status-tags inside instance-id->state contains only
        ; :healthy|:unhealthy => instance is known to be (un)healthy from state updates
        ; :blacklisted => instance was blacklisted based on its response
        ; :killed => instance was marked as successfully killed
        ; :locked => instance has been locked and cannot be used to satify any other request (e.g. used for a kill request)
        ; :expired => instance has exceeded its age and is being prepared to be replaced
        ; :starting => instance is starting up and has the potential to be healthy
        (loop [{:keys [deployment-error instance-id->state timer-context work-stealing-queue]
                :as current-state}
               (merge {:deployment-error nil
                       :id->instance {}
                       :instability-issue nil
                       :instance-id->blacklist-expiry-time {}
                       :instance-id->consecutive-failures {}
                       :instance-id->request-id->use-reason-map {}
                       :instance-id->state {}
                       :load-balancing default-load-balancing
                       :request-id->work-stealer {}
                       :sorted-instance-ids []
                       :timer-context (timers/start responder-timer)
                       :work-stealing-queue (PersistentQueue/EMPTY)}
                      initial-state)]
          (if-let [new-state
                   (let [slots-available? (some slots-available? (vals instance-id->state))
                         ;; `reserve-instance-chan` must be highest priority channel as we want reserve calls
                         ; to be handled before release calls. This allows instances from work-stealing offers
                         ; to be used preferentially. `exit-chan` and `query-state-chan` must be lowest priority
                         ; to facilitate unit testing.
                         chans (concat [update-state-chan scaling-state-chan]
                                       (when (or slots-available? (seq work-stealing-queue) deployment-error)
                                         [reserve-instance-chan])
                                       [release-instance-chan blacklist-instance-chan unblacklist-instance-chan kill-instance-chan
                                        work-stealing-chan query-state-chan exit-chan])
                         [data chan-selected] (async/alts! chans :priority true)]
                     (cond->
                       ;; first obtain new state by pre-processing based on selected channel
                       (condp = chan-selected
                         exit-chan
                         (handle-exit-request service-id current-state work-stealing-received-in-flight-counter data)

                         work-stealing-chan
                         (let [{:keys [current-state' response-chan response]}
                               (handle-work-stealing-offer
                                 current-state service-id slots-in-use-counter slots-available-counter
                                 work-stealing-received-in-flight-counter requests-outstanding-counter data)]
                           (when response
                             (async/>! response-chan response))
                           current-state')

                         reserve-instance-chan
                         (let [{:keys [current-state' response-chan response]}
                               (timers/start-stop-time!
                                 responder-reserve-timer
                                 (handle-reserve-instance-request current-state service-id update-slot-state-fn data))]
                           (async/>! response-chan response)
                           current-state')

                         kill-instance-chan
                         (let [{:keys [current-state' response-chan response]}
                               (timers/start-stop-time!
                                 responder-kill-timer
                                 (handle-kill-instance-request current-state update-status-tag-fn lingering-request-threshold-ms data))]
                           (async/>! response-chan response)
                           current-state')

                         release-instance-chan
                         (timers/start-stop-time!
                           responder-release-timer
                           (handle-release-instance-request
                             current-state service-id update-slot-state-fn update-status-tag-fn
                             update-state-by-blacklisting-instance-fn update-instance-id->blacklist-expiry-time-fn
                             work-stealing-received-in-flight-counter max-blacklist-time-ms
                             blacklist-backoff-base-time-ms max-backoff-exponent data))

                         scaling-state-chan
                         (let [{:keys [scaling-state]} data]
                           (handle-scaling-state-request current-state service-id default-load-balancing scaling-state))

                         query-state-chan
                         (let [{:keys [cid response-chan service-id]} data]
                           (cid/cinfo cid "returning current state of" service-id)
                           (async/put! response-chan (dissoc current-state :timer-context))
                           current-state)

                         update-state-chan
                         (timers/start-stop-time!
                           responder-update-timer
                           (let [[_ data-time] data]
                             (-> current-state
                               (handle-update-state-request
                                 data update-responder-state-timer update-responder-state-meter slots-assigned-counter
                                 slots-available-counter slots-in-use-counter)
                               ;; cleanup items from blacklist map in-case they have not been cleaned
                               (handle-unblacklist-cleanup-request
                                 data-time update-status-tag-fn update-instance-id->blacklist-expiry-time-fn))))

                         blacklist-instance-chan
                         (let [{:keys [current-state' response-chan response]}
                               (timers/start-stop-time!
                                 responder-blacklist-timer
                                 (handle-blacklist-request
                                   current-state update-status-tag-fn update-state-by-blacklisting-instance-fn
                                   lingering-request-threshold-ms data))]
                           (async/put! response-chan response)
                           current-state')

                         unblacklist-instance-chan
                         (timers/start-stop-time!
                           responder-unblacklist-timer
                           (handle-unblacklist-request
                             current-state update-status-tag-fn update-instance-id->blacklist-expiry-time-fn data)))

                       ;; cleanup items from work-stealing queue one at a time if they are not needed
                       (and (seq work-stealing-queue)
                            (not (contains? #{query-state-chan, reserve-instance-chan, work-stealing-chan} chan-selected)))
                       (release-unneeded-work-stealing-offers!
                         service-id slots-in-use-counter slots-available-counter
                         work-stealing-received-in-flight-counter requests-outstanding-counter)))]
            (do
              (metrics/reset-counter in-use-instance-counter (count (:instance-id->request-id->use-reason-map new-state)))
              (timers/stop timer-context)
              (recur (assoc new-state :timer-context (timers/start responder-timer))))
            (log/info "service-chan-responder shutting down for" service-id)))
        (catch Exception e
          (log/error e "Fatal error in service-chan-responder for" service-id)
          (System/exit 1))))))

(defn trigger-unblacklist-process
  "Launches a go-block that sends a message to unblacklist-instance-chan to unblacklist instance-id after blacklist-period-ms have elapsed."
  [correlation-id instance-id blacklist-period-ms unblacklist-instance-chan]
  (async/go
    (try
      (async/<! (async/timeout blacklist-period-ms))
      (cid/cinfo correlation-id "requesting instance" instance-id "to be unblacklisted")
      (async/>! unblacklist-instance-chan {:instance-id instance-id})
      (catch Throwable th
        (log/error th "unexpected error inside trigger-unblacklist-process"
                   {:blacklist-period-ms blacklist-period-ms :cid correlation-id :instance-id instance-id})))))

(defn prepare-and-start-service-chan-responder
  "Starts the service channel responder."
  [service-id service-description instance-request-properties blacklist-config]
  (log/debug "[prepare-and-start-service-chan-responder] starting" service-id)
  (let [{:keys [lingering-request-threshold-ms queue-timeout-ms]} instance-request-properties
        timeout-request-fn (fn [service-id c [reason-map resp-chan _ request-queue-timeout-ms]]
                             (async/go
                               (let [timeout-amount-ms (or request-queue-timeout-ms queue-timeout-ms)
                                     timeout (async/timeout timeout-amount-ms)]
                                 (async/alt!
                                   c ([_] :item-handled)
                                   timeout ([_]
                                            (cid/cinfo (:cid reason-map) "timeout in handling"
                                                       (assoc-in (update-in reason-map [:time] du/date-to-str)
                                                                 [:state :timeout]
                                                                 (metrics/retrieve-local-stats-for-service service-id)))
                                            (async/put! c {:id reason-map
                                                           :resp-chan resp-chan}))))))
        in (async/chan 1024)
        out (au/timing-out-pipeline
              (str service-id ":timing-out-pipeline")
              (metrics/service-histogram service-id "timing-out-pipeline-buffer-size")
              in
              1024
              first
              (fn priority-fn [[{:keys [priority]} & _]] priority)
              (partial timeout-request-fn service-id)
              (fn [e]
                (log/error e "Error in service-chan timing-out-pipeline")))
        channel-map {:reserve-instance-chan-in in
                     :reserve-instance-chan out
                     :kill-instance-chan (async/chan 1024)
                     :release-instance-chan (async/chan 1024)
                     :blacklist-instance-chan (async/chan 1024)
                     :query-state-chan (async/chan 1024)
                     :scaling-state-chan (au/latest-chan)
                     :unblacklist-instance-chan (async/chan 1024)
                     :update-state-chan (au/latest-chan)
                     :work-stealing-chan (async/chan 1024)
                     :exit-chan (async/chan 1)}]
    (let [timeout-config (-> (select-keys blacklist-config [:blacklist-backoff-base-time-ms :max-blacklist-time-ms])
                           (assoc :lingering-request-threshold-ms lingering-request-threshold-ms))
          {:strs [load-balancing]} service-description
          load-balancing (keyword load-balancing)
          initial-state {:load-balancing load-balancing}]
      (start-service-chan-responder service-id trigger-unblacklist-process timeout-config channel-map initial-state))
    channel-map))
