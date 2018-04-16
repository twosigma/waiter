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
(ns waiter.state
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [digest]
            [metrics.core]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.discovery :as discovery]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.util.async-utils :as au]
            [waiter.util.utils :as utils]
            [waiter.work-stealing :as work-stealing])
  (:import clojure.lang.PersistentQueue))

;; Router state maintainers

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
  [id->instance instance-id->state acceptable-instance-id? instance-id->request-id->use-reason-map lingering-request-threshold-ms]
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
                                                            ; invert sorting for expired instances
                                                            (expired? state) (unchecked-negate))))
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
  [sorted-instance-ids id->instance instance-id->state acceptable-instance-id?]
  (->> sorted-instance-ids
       (filter acceptable-instance-id?)
       (filter (fn [instance-id]
                 (let [state (instance-id->state instance-id)]
                   (and (not (nil? state))
                        (slots-available? state)
                        (healthy? state)))))
       first
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
    (when-let [[{:keys [healthy-instances unhealthy-instances my-instance->slots expired-instances starting-instances deployment-error]} _] data]
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
  [{:keys [deployment-error id->instance instance-id->state request-id->work-stealer sorted-instance-ids work-stealing-queue] :as current-state}
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
      (let [{instance-id :id :as instance-to-offer}
            (find-available-instance sorted-instance-ids id->instance instance-id->state #(not (contains? exclude-ids-set %)))]
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
  [{:keys [id->instance instance-id->request-id->use-reason-map instance-id->state] :as current-state}
   update-status-tag-fn lingering-request-threshold-ms [{:keys [request-id] :as reason-map} resp-chan exclude-ids-set _]]
  (let [acceptable-instance-id? #(not (contains? exclude-ids-set %))
        instance (find-killable-instance id->instance instance-id->state acceptable-instance-id?
                                         instance-id->request-id->use-reason-map lingering-request-threshold-ms)]
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
  (cid/with-correlation-id cid
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

(defn- unblacklist-instance
  [{:keys [instance-id->state] :as current-state} update-instance-id->blacklist-expiry-time-fn update-status-tag-fn
   instance-id expiry-time]
  (log/info "unblacklisting instance" instance-id "as blacklist expired at" expiry-time)
  (cond-> (update-instance-id->blacklist-expiry-time-fn current-state #(dissoc % instance-id))
          (contains? instance-id->state instance-id)
          (update-in [:instance-id->state instance-id] update-status-tag-fn #(disj % :blacklisted))))

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
           reserve-instance-chan unblacklist-instance-chan update-state-chan work-stealing-chan]}
   initial-state]
  (when (some nil? (vals initial-state))
    (throw (ex-info "Initial state contains nil values!" initial-state)))
  (let [max-backoff-exponent (int (max 1 (inc (/ (Math/log max-blacklist-time-ms) (Math/log blacklist-backoff-base-time-ms)))))
        responder-timer (metrics/service-timer service-id "service-chan-responder-iteration")
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
            (trigger-unblacklist-process-fn correlation-id instance-id expiry-time-ms unblacklist-instance-chan)
            (update-instance-id->blacklist-expiry-time-fn current-state #(assoc % instance-id actual-expiry-time))))]
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
        (loop [{:keys [deployment-error
                       id->instance
                       instance-id->blacklist-expiry-time
                       instance-id->consecutive-failures
                       instance-id->request-id->use-reason-map
                       instance-id->state
                       request-id->work-stealer
                       sorted-instance-ids
                       timer-context
                       work-stealing-queue]
                :as current-state}
               (merge {:deployment-error nil
                       :id->instance {}
                       :instance-id->blacklist-expiry-time {}
                       :instance-id->consecutive-failures {}
                       :instance-id->request-id->use-reason-map {}
                       :instance-id->state {}
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
                         chans (concat [update-state-chan]
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
                               (handle-reserve-instance-request current-state service-id update-slot-state-fn data)]
                           (async/>! response-chan response)
                           current-state')

                         kill-instance-chan
                         (let [{:keys [current-state' response-chan response]}
                               (handle-kill-instance-request current-state update-status-tag-fn lingering-request-threshold-ms data)]
                           (async/>! response-chan response)
                           current-state')

                         release-instance-chan
                         (handle-release-instance-request current-state service-id update-slot-state-fn update-status-tag-fn
                                                          update-state-by-blacklisting-instance-fn update-instance-id->blacklist-expiry-time-fn
                                                          work-stealing-received-in-flight-counter max-blacklist-time-ms
                                                          blacklist-backoff-base-time-ms max-backoff-exponent data)

                         query-state-chan
                         (let [{:keys [cid response-chan service-id]} data]
                           (cid/cinfo cid "returning current state of" service-id)
                           (->> (dissoc current-state :timer-context)
                                (async/put! response-chan))
                           current-state)

                         update-state-chan
                         (let [[_ data-time] data]
                           (-> current-state
                               (handle-update-state-request
                                 data update-responder-state-timer update-responder-state-meter slots-assigned-counter
                                 slots-available-counter slots-in-use-counter)
                               ;; cleanup items from blacklist map in-case they have not been cleaned
                               (handle-unblacklist-cleanup-request
                                 data-time update-status-tag-fn update-instance-id->blacklist-expiry-time-fn)))

                         blacklist-instance-chan
                         (let [{:keys [current-state' response-chan response]}
                               (handle-blacklist-request
                                 current-state update-status-tag-fn update-state-by-blacklisting-instance-fn
                                 lingering-request-threshold-ms data)]
                           (async/put! response-chan response)
                           current-state')

                         unblacklist-instance-chan
                         (handle-unblacklist-request current-state update-status-tag-fn update-instance-id->blacklist-expiry-time-fn data))

                       ;; cleanup items from work-stealing queue one at a time if they are not needed
                       (and (seq work-stealing-queue)
                            (not (contains? #{query-state-chan, reserve-instance-chan, work-stealing-chan} chan-selected)))
                       (release-unneeded-work-stealing-offers! service-id slots-in-use-counter slots-available-counter
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
  [service-id instance-request-properties blacklist-config]
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
                                                        (assoc-in (update-in reason-map [:time] utils/date-to-str)
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
                     :unblacklist-instance-chan (async/chan 1024)
                     :update-state-chan (au/latest-chan)
                     :work-stealing-chan (async/chan 1024)
                     :exit-chan (async/chan 1)}]
    (let [timeout-config (-> (select-keys blacklist-config [:blacklist-backoff-base-time-ms :max-blacklist-time-ms])
                             (assoc :lingering-request-threshold-ms lingering-request-threshold-ms))]
      (start-service-chan-responder service-id trigger-unblacklist-process timeout-config channel-map {}))
    channel-map))

(defn close-update-state-channel
  "Closes the update-state channelfor the responder"
  [service-id {:keys [update-state-chan]}]
  (log/warn "[close-update-state-channel] closing update-state-chan of" service-id)
  (async/go (async/close! update-state-chan)))

(defn start-service-chan-maintainer
  "go block to maintain the mapping from service-id to chans to communicate with
   service-chan-responders.

   Services are started by invoking `start-service` with the `service-id` and the
   returned result is stored as the channel-map.

   Services are destroyed (when the service is missing in state updates) by invoking
   `(remove-service service-id channel-map)`.

   Requests for service-chan-responder channels is passed into request-chan
      It is expected that messages on the channel have a map with keys
      `:cid`, `:method`, `:response-chan`, and `:service-id`.
      The `method` should be either `:blacklist`, `:kill`, `:query-state`, `:reserve`, or `release`.
      The `service-id` is the service for the request and
      `response-chan` will have the corresponding channel placed onto it by invoking
      `(retrieve-channel channel-map method)`.

   Updated state for the router is passed into `state-chan` and then propogated to the
   service-chan-responders.

   `query-service-maintainer-chan` return the current state of the maintainer."
  [initial-state request-chan state-chan query-service-maintainer-chan
   start-service remove-service retrieve-channel]
  (let [exit-chan (async/chan 1)
        update-state-timer (metrics/waiter-timer "state" "service-chan-maintainer" "update-state")]
    (async/go
      (try
        (loop [service-id->channel-map (or (:service-id->channel-map initial-state) {})
               last-state-update-time (:last-state-update-time initial-state)]
          (let [new-state
                (async/alt!
                  exit-chan
                  ([message]
                    (log/warn "stopping service-chan-maintainer")
                    (when (not= :exit message)
                      [service-id->channel-map last-state-update-time]))

                  state-chan
                  ([router-state]
                    (timers/start-stop-time!
                      update-state-timer
                      (let [{:keys [service-id->my-instance->slots service-id->unhealthy-instances service-id->expired-instances
                                    service-id->starting-instances service-id->deployment-error time]} router-state
                            incoming-service-ids (set (keys service-id->my-instance->slots))
                            known-service-ids (set (keys service-id->channel-map))
                            new-service-ids (set/difference incoming-service-ids known-service-ids)
                            removed-service-ids (set/difference known-service-ids incoming-service-ids)
                            service-id->channel-map' (select-keys service-id->channel-map incoming-service-ids)
                            new-chans (map start-service new-service-ids) ; create new responders
                            service-id->channel-map'' (if (seq new-chans)
                                                        (apply assoc service-id->channel-map' (interleave new-service-ids new-chans))
                                                        service-id->channel-map')]
                        ;; Destroy deleted responders
                        (doseq [service-id removed-service-ids]
                          (log/warn "[service-chan-maintainer] removing" service-id)
                          (remove-service service-id (service-id->channel-map service-id)))
                        ;; Update state for responders
                        (doseq [service-id (keys service-id->channel-map'')]
                          (let [my-instance->slots (get service-id->my-instance->slots service-id)
                                healthy-instances (keys my-instance->slots)
                                unhealthy-instances (get service-id->unhealthy-instances service-id)
                                expired-instances (get service-id->expired-instances service-id)
                                starting-instances (get service-id->starting-instances service-id)
                                deployment-error (get service-id->deployment-error service-id)
                                update-state-chan (retrieve-channel (get service-id->channel-map'' service-id) :update-state)]
                            (if (or healthy-instances unhealthy-instances)
                              (async/put! update-state-chan
                                          (let [update-state {:healthy-instances healthy-instances
                                                              :unhealthy-instances unhealthy-instances
                                                              :expired-instances expired-instances
                                                              :starting-instances starting-instances
                                                              :my-instance->slots my-instance->slots
                                                              :deployment-error deployment-error}]
                                            [update-state time]))
                              (async/put! update-state-chan [{} time]))))
                        [service-id->channel-map'' time])))

                  request-chan
                  ([message]
                    (let [{:keys [cid method response-chan service-id]} message]
                      (cid/cdebug cid "[service-chan-maintainer]" service-id "received request of type" method)
                      (let [channel-map (get service-id->channel-map service-id)]
                        (if channel-map
                          (let [method-chan (retrieve-channel channel-map method)]
                            (async/put! response-chan method-chan))
                          (do
                            (cid/cdebug cid "[service-chan-maintainer] no channel map found for" service-id)
                            (counters/inc! (metrics/service-counter service-id "maintainer" "not-found"))
                            (async/close! response-chan)))
                        [service-id->channel-map last-state-update-time])))

                  query-service-maintainer-chan
                  ([message]
                    (let [{:keys [service-id response-chan]} message]
                      (log/info "[service-chan-maintainer] state has been queried for" service-id)
                      (if (str/blank? service-id)
                        (async/put! response-chan {:last-state-update-time last-state-update-time
                                                   :service-id->channel-map service-id->channel-map})
                        (async/put! response-chan {:last-state-update-time last-state-update-time
                                                   :maintainer-chan-available (contains? service-id->channel-map service-id)}))
                      [service-id->channel-map last-state-update-time]))
                  :priority true)]
            (when new-state
              (recur (first new-state) (second new-state)))))
        (catch Exception e
          (log/error e "Fatal error in service-chan-maintainer")
          (System/exit 1))))
    {:exit-chan exit-chan}))

(defn md5-hash-function
  "Computes the md5 hash after concatenating the input strings."
  [router-id instance-id]
  (digest/md5 (str router-id instance-id)))

(defn build-instance-id->sorted-site-hash
  "Uses the provided hash-fn to prepare the sorted [router-id hash-value] list for all instances.

  Example:
  (def router-ids ['ab12' 'cd34' 'fg56' 'hi78'])
  (def instances [{:id 'a1'} {:id 'b2'} {:id 'c3'} {:id 'd4'} {:id 'e5'} {:id 'f6'} {:id 'g7'}])

  => (build-instance-id->sorted-site-hash router-ids instances md5-hash-function)
  {'a1' (['fg56' 247803843] ['hi78' 913429638] ['ab12' 1533979899] ['cd34' 1973192373]),
   'b2' (['hi78' 1029199499] ['cd34' 1406840036] ['fg56' 1847691010] ['ab12' 1956020785]),
   'c3' (['fg56' 228374035] ['ab12' 1831374624] ['hi78' 1936252602] ['cd34' 2059229285]),
   'd4' (['ab12' 100811858] ['cd34' 1151899184] ['fg56' 1749946111] ['hi78' 1882240006]),
   'e5' (['ab12' -553989504] ['cd34' 51409049] ['fg56' 1045133553] ['hi78' 1238647152]),
   'f6' (['cd34' 1468257634] ['ab12' 2006147975] ['hi78' 2075255005] ['fg56' 2138275348]),
   'g7' (['cd34' 1343876473] ['hi78' 1452603434] ['ab12' 1884303260] ['fg56' 2023634393])}
  "
  [router-ids instances hash-fn]
  (zipmap
    (map :id instances)
    (map (fn [instance] ; prepare the sorted [router-id hash-value] list for a given instance
           (let [router-id->instance-hashes (zipmap router-ids (map (fn [router-site] (hash-fn router-site (:id instance))) router-ids))]
             (sort-by second
                      (map (fn [router-id] [router-id (get router-id->instance-hashes router-id)]) router-ids))))
         instances)))

(defn- consistent-hash-distribution
  "Implements the Highest Random Weight (HRW) consistent hashing.
   The basic idea is to give each instance a score (a weight) for each router, and assign the instance to the least scoring router.
   The output is a mapping of the router-id to list of list of instance-ids (LLII).
   The first list in the LLII represents those instance ids for which the router scored lowest.
   The second list in the LLII represents those instance ids for which the router scored second-lowest and so on.


   Example:
   router-ids [ab12 cd34 fg56])
   instances [{:id a1} {:id b2} {:id c3} {:id d4} {:id e5} {:id f6}]

   => (consistent-hash-distribution router-ids instances md5-hash-function)
   {ab12 ((a1 e5 f6) (b2 c3) (d4)),
    cd34 ((b2) (a1 d4 f6) (c3 e5)),
    fg56 ((c3 d4) (e5) (a1 b2 f6))}

   router-ids [ab12 cd34 fg56])
   instances [{:id a1} {:id b2} {:id c3} {:id d4} {:id e5} {:id f6} {:id g7}]

   => (consistent-hash-distribution router-ids instances md5-hash-function)
   {ab12 ((a1 e5 f6) (b2 c3) (d4 g7)),
    cd34 ((b2 g7) (a1 d4 f6) (c3 e5)),
    fg56 ((c3 d4) (e5 g7) (a1 b2 f6))}
  "
  [router-ids instances hash-fn]
  (let [instance-id->sorted-site-hash (build-instance-id->sorted-site-hash router-ids instances hash-fn)]
    (pc/map-from-keys
      (fn [router-id]
        (let [instance-ids-with-rank-fn
              (fn [rank] (filter
                           (fn [instance-id]
                             (let [sorted-site-hash (get instance-id->sorted-site-hash instance-id)]
                               (= router-id (first (nth sorted-site-hash rank)))))
                           (map :id (scheduler/sort-instances instances))))]
          (map instance-ids-with-rank-fn (range (count router-ids)))))
      router-ids)))

(defn- update-instance-id->available-slots
  "Updates the available slots by 'subtracting' the slots taken in `instance-id->taken-slots`"
  [instance-id->available-slots instance-id->taken-slots]
  (pc/map-from-keys (fn [instance-id]
                      (let [initial-slots (get instance-id->available-slots instance-id 0)
                            taken-slots (get instance-id->taken-slots instance-id 0)]
                        (max (- initial-slots taken-slots) 0)))
                    (keys instance-id->available-slots)))

(defn allocate-from-available-slots
  "Allocates a total of `target-slots` from available slots on instances.
   It tries to allocate slots from as many instances as possible.
   If there is an imbalance in slots allocated from an instance, it uses the traversal order of `sorted-instance-ids` to prefer instances.

   For example, assuming instances are sorted by name:
     target-slots: 12 and
     initial-instance-id->available-slots: {a 2, b 5, c 0, d 3, e 2, f 4, g 0}
   will produce a result of
     {a 2, b 3, d 3, e 2, f 2}
   as b and d are 'preferred' over f."
  [target-slots initial-instance-id->available-slots sorted-instance-ids]
  (let [target-slots (or target-slots 0)
        assign-slot-fn (fn [instance-id->assigned-slots instance-id]
                         (if (contains? instance-id->assigned-slots instance-id)
                           (update-in instance-id->assigned-slots [instance-id] inc)
                           (assoc instance-id->assigned-slots instance-id 1)))]
    (let [num-instances-with-available-slots (count (filter pos? (vals initial-instance-id->available-slots)))
          min-slots-from-each-instance
          (if (zero? num-instances-with-available-slots) 0 (quot target-slots num-instances-with-available-slots))
          initial-instance-id->assigned-slots
          (pc/map-from-keys #(min (get initial-instance-id->available-slots %) min-slots-from-each-instance)
                            (keys initial-instance-id->available-slots))
          remaining-instance-id->available-slots
          (update-instance-id->available-slots initial-instance-id->available-slots initial-instance-id->assigned-slots)]
      (loop [num-slots-to-consume (- target-slots (reduce + (vals initial-instance-id->assigned-slots)))
             num-slots-available (reduce + (vals remaining-instance-id->available-slots))
             instance-id->assigned-slots (utils/filterm #(pos? (val %)) initial-instance-id->assigned-slots)
             instance-id->available-slots remaining-instance-id->available-slots
             [instance-id & remaining-instance-ids] (cycle sorted-instance-ids)]
        (cond
          ; base case: done assigning slots
          (zero? num-slots-to-consume) instance-id->assigned-slots
          ; we've exhausted all slots but we still need to assign some slots to the router (e.g. 1 slot for 3 routers case)
          (zero? num-slots-available) (->> (take num-slots-to-consume sorted-instance-ids)
                                           (reduce (fn [instance-id->assigned-slots instance-id]
                                                     (if (contains? instance-id->assigned-slots instance-id)
                                                       instance-id->assigned-slots
                                                       (assign-slot-fn instance-id->assigned-slots instance-id)))
                                                   instance-id->assigned-slots))
          ; consume a slot from the currently preferred instance
          :else (let [slot-available? (pos? (get instance-id->available-slots instance-id 0))
                      instance-id->assigned-slots' (cond-> instance-id->assigned-slots
                                                           slot-available? (assign-slot-fn instance-id))
                      instance-id->available-slots' (cond-> instance-id->available-slots
                                                            slot-available? (update-in [instance-id] dec))]
                  (recur (cond-> num-slots-to-consume slot-available? (dec))
                         (cond-> num-slots-available slot-available? (dec))
                         instance-id->assigned-slots'
                         instance-id->available-slots'
                         remaining-instance-ids)))))))

(defn evenly-distribute-slots-across-routers
  "Performs distribution of slots across all routers.
   Total slots assigned to each router is distributed as evenly as possible.

   During imbalance while picking slots from instances, each router preferntially uses its higher ranked instances.
   Routers are processed in a custom sorted order to ensure the distribution yields deterministic results."
  [instances router-id->ranked-instance-ids concurrency-level]
  (let [concurrency-level (or concurrency-level 1)
        router-ids (keys router-id->ranked-instance-ids)
        id->instance (zipmap (map :id instances) instances)
        num-instances (count instances) ; max number of instances any router can 'own' used as our base for comparison values
        router-id->ranked-instance-value (fn [router-id]
                                           (reduce (fn [acc instance-ids] (+ (* num-instances acc) (count instance-ids)))
                                                   0
                                                   (get router-id->ranked-instance-ids router-id)))
        sorted-router-ids (sort (fn [id1 id2]
                                  ; sort routers by instances they own and then by their names to minimize shuffling while sharing
                                  (let [v1 (router-id->ranked-instance-value id1)
                                        v2 (router-id->ranked-instance-value id2)]
                                    (if (= v1 v2)
                                      (compare id1 id2)
                                      (compare v2 v1))))
                                router-ids)]
    (loop [counter 0
           [router-id & sorted-router-ids'] sorted-router-ids ; Important: ensure routers are processed in their sorted order
           instance-id->available-slots (pc/map-from-keys (constantly concurrency-level) (map :id instances))
           router-id->instances->slots {}]
      (if router-id
        (let [router-target-slots (max 1 (int (Math/ceil (/ (reduce + (vals instance-id->available-slots)) (inc (count sorted-router-ids'))))))
              sorted-instance-ids (reduce (fn [accum instance-ids]
                                            ; rotate the instance-ids to ensure load-balancing of preferences in same priority level
                                            (let [shift (if (empty? instance-ids) 0 (mod counter (count instance-ids)))]
                                              (concat accum (concat (drop shift instance-ids) (take shift instance-ids)))))
                                          [] (get router-id->ranked-instance-ids router-id))
              my-instance-id->slots (allocate-from-available-slots router-target-slots instance-id->available-slots sorted-instance-ids)
              instance-ids->available-slots' (update-instance-id->available-slots instance-id->available-slots my-instance-id->slots)
              router-ids->instances->slots' (assoc router-id->instances->slots
                                              router-id (pc/map-keys #(get id->instance %) my-instance-id->slots))]
          (recur (inc counter) sorted-router-ids' instance-ids->available-slots' router-ids->instances->slots'))
        router-id->instances->slots))))

(defn distribute-slots-across-routers
  "Performs distribution of slots across all routers as per the preferetial (first ranked) instances.
   All slots (the concurrency-level) for each instance is assigned to exactly one router.
   This may lead to imbalance in assignment of instances to routers but avoids shuffling of instances during scaling.
   We can rely on work-stealing to address the imbalance."
  [instances router-id->ranked-instance-ids concurrency-level]
  (let [id->instance (zipmap (map :id instances) instances)]
    (pc/map-vals (fn [ranked-instance-ids]
                   (->> ranked-instance-ids
                        (first)
                        (map #(get id->instance %))
                        (map (fn [instance] [instance concurrency-level]))
                        (into {})))
                 router-id->ranked-instance-ids)))

(defn distribute-slots-using-consistent-hash-distribution
  "Initially, perform a consistent hash distribution by invoking consistent-hash-distribution.
   Then it performs a round of load-balancing to ensure slots are evenly distributed across all routers.

   The hash order is used to define an 'affinity' for an instance to a router.
   During imbalance in slot distribution, a router will prefer slots from instances with higher affinity."
  [router-ids instances hash-fn concurrency-level distribution-scheme]
  (let [router-id->ranked-instance-ids (consistent-hash-distribution router-ids instances hash-fn)]
    (if (= "balanced" distribution-scheme)
      (evenly-distribute-slots-across-routers instances router-id->ranked-instance-ids concurrency-level)
      (distribute-slots-across-routers instances router-id->ranked-instance-ids concurrency-level))))

(defn- compute-service-id->my-instance->slots
  "Computes the slot distribution of instances on a given router."
  [router-id router-id->http-endpoint service-id->service-description-fn service-id->healthy-instances]
  (let [service-id->router->instance->slots
        (->> service-id->healthy-instances
             (map (fn [[service-id instances]]
                    (let [{:strs [concurrency-level distribution-scheme]} (service-id->service-description-fn service-id)
                          router-id->instance->slots (distribute-slots-using-consistent-hash-distribution
                                                       (keys router-id->http-endpoint) instances md5-hash-function concurrency-level distribution-scheme)]
                      (log/info "instance distribution of" service-id "is" (pc/map-vals #(pc/map-keys :id %) router-id->instance->slots))
                      [service-id router-id->instance->slots])))
             (into {}))]
    (pc/map-vals #(get % router-id) service-id->router->instance->slots)))

(defn- update-router-instance-counts
  "Updates the local instance-count metrics."
  [{:keys [service-id->expired-instances service-id->healthy-instances service-id->my-instance->slots service-id->unhealthy-instances]
    :as candidate-state}]
  (doseq [[service-id my-instances] service-id->my-instance->slots]
    (metrics/reset-counter (metrics/service-counter service-id "instance-counts" "healthy") (count (get service-id->healthy-instances service-id)))
    (metrics/reset-counter (metrics/service-counter service-id "instance-counts" "unhealthy") (count (get service-id->unhealthy-instances service-id)))
    (metrics/reset-counter (metrics/service-counter service-id "instance-counts" "expired") (count (get service-id->expired-instances service-id)))
    (metrics/reset-counter (metrics/service-counter service-id "instance-counts" "my-instances") (count my-instances)))
  candidate-state)

(defn- update-router-state
  "Update the instance maintainer state to reflect the new services, instances and router list.
   Avoids recomputation of instance distribution for services that have not changed from last state update."
  [router-id {:keys [iteration service-id->my-instance->slots] :as current-state} candidate-new-state service-id->service-description-fn]
  (timers/start-stop-time!
    (metrics/waiter-timer "state" "update-router-state")
    (let [service-id->healthy-instances (:service-id->healthy-instances current-state)
          service-id->healthy-instances' (:service-id->healthy-instances candidate-new-state)
          router-id->http-endpoint (:routers current-state)
          router-id->http-endpoint' (:routers candidate-new-state)
          routers-changed? (not= router-id->http-endpoint router-id->http-endpoint')
          delta-service-id->healthy-instances (utils/filterm
                                                (fn [[service-id healthy-instances]]
                                                  (or routers-changed?
                                                      (not= healthy-instances (service-id->healthy-instances service-id))))
                                                service-id->healthy-instances')
          _ (log/info "avoiding distribution computation of" (- (count service-id->healthy-instances') (count delta-service-id->healthy-instances))
                      "out of" (count service-id->healthy-instances') "services.")
          delta-service-id->my-instance->slots (compute-service-id->my-instance->slots
                                                 router-id router-id->http-endpoint service-id->service-description-fn delta-service-id->healthy-instances)
          service-id->my-instance->slots' (into (select-keys service-id->my-instance->slots (keys service-id->healthy-instances'))
                                                delta-service-id->my-instance->slots)]
      (-> candidate-new-state
          (assoc :iteration (inc iteration)
                 :service-id->my-instance->slots service-id->my-instance->slots')
          update-router-instance-counts))))

(defn get-deployment-error
  "Returns appropriate deployment error for a service based on its instances, or nil if no such errors."
  [healthy-instances unhealthy-instances failed-instances {:keys [min-failed-instances min-hosts]}]
  (when (empty? healthy-instances)
    (let [failed-instance-hosts (set (map :host failed-instances))
          has-failed-instances? (and (>= (count failed-instances) min-failed-instances)
                                     (>= (count failed-instance-hosts) min-hosts))
          has-unhealthy-instances? (not-empty unhealthy-instances)
          first-unhealthy-status (-> unhealthy-instances first :health-check-status)
          first-exit-code (-> failed-instances first :exit-code)
          failed-instance-flags (map :flags failed-instances)
          all-instances-flagged-with? (fn [flag] (every? #(contains? % flag) failed-instance-flags))
          no-instances-flagged-with? (fn [flag] (every? #(not (contains? % flag)) failed-instance-flags))
          all-instances-exited-similarly? (and first-exit-code (every? #(= first-exit-code %) (map :exit-code failed-instances)))]
      (cond
        (and has-failed-instances? all-instances-exited-similarly?) :bad-startup-command
        (and has-failed-instances? (all-instances-flagged-with? :memory-limit-exceeded)) :not-enough-memory
        (and has-failed-instances? (no-instances-flagged-with? :has-connected)) :cannot-connect
        (and has-failed-instances? (no-instances-flagged-with? :has-responded)) :health-check-timed-out
        (and has-failed-instances? (all-instances-flagged-with? :never-passed-health-checks)) :invalid-health-check-response
        (and has-unhealthy-instances? (= first-unhealthy-status 401)) :health-check-requires-authentication))))

(defn start-router-state-maintainer
  "Start the instance state maintainer.
   Maintains the state of the router as well as the state of marathon
   and the existence of other routers. Acts as the central access point
   for modifying this data for the router."
  [scheduler-state-chan router-chan router-id exit-chan service-id->service-description-fn deployment-error-config]
  (let [state-chan (async/chan)
        router-state-push-chan (au/latest-chan)]
    {:state-chan state-chan
     :router-state-push-mult (async/mult router-state-push-chan)
     :go-chan
     (async/go
       (try
         (loop [{:keys [iteration routers] :as current-state}
                {:service-id->healthy-instances {}
                 :service-id->unhealthy-instances {}
                 :service-id->my-instance->slots {} ; updated in update-router-state
                 :service-id->expired-instances {}
                 :service-id->starting-instances {}
                 :service-id->failed-instances {}
                 :service-id->deployment-error {}
                 :iteration 0
                 :routers []
                 :time (t/now)}]
           (let [next-state
                 (async/alt!
                   exit-chan
                   ([message]
                     (log/warn "Stopping router-state-maintainer")
                     (when (not= :exit message)
                       (throw (ex-info "Stopping router-state maintainer" {:time (t/now), :reason message}))))

                   scheduler-state-chan
                   ([scheduler-messages]
                     (cid/with-correlation-id
                       (str "router-state-maintainer-" iteration)
                       (loop [{:keys [service-id->healthy-instances service-id->unhealthy-instances service-id->my-instance->slots
                                      service-id->expired-instances service-id->starting-instances service-id->failed-instances
                                      service-id->deployment-error] :as loop-state} current-state
                              [[message-type message-data] & remaining] scheduler-messages]
                         (log/trace "scheduler-state-chan received, type:" message-type)
                         (let [loop-state'
                               (case message-type
                                 :update-available-apps
                                 (let [{:keys [available-apps scheduler-sync-time]} message-data
                                       available-service-ids (into #{} available-apps)
                                       services-without-instances (remove #(contains? service-id->my-instance->slots %) available-service-ids)
                                       service-id->healthy-instances' (select-keys service-id->healthy-instances available-service-ids)
                                       service-id->unhealthy-instances' (select-keys service-id->unhealthy-instances available-service-ids)
                                       service-id->expired-instances' (select-keys service-id->expired-instances available-service-ids)
                                       service-id->starting-instances' (select-keys service-id->starting-instances available-service-ids)
                                       service-id->failed-instances' (select-keys service-id->failed-instances available-service-ids)
                                       service-id->deployment-error' (select-keys service-id->deployment-error available-service-ids)]
                                   (when (or (not= service-id->healthy-instances service-id->healthy-instances')
                                             (not= service-id->unhealthy-instances service-id->unhealthy-instances')
                                             (seq services-without-instances))
                                     (log/info "update-available-apps:"
                                               (count service-id->healthy-instances') "services with healthy instances and"
                                               (count services-without-instances) "services without instances:"
                                               (vec services-without-instances)))
                                   (assoc loop-state
                                     :service-id->healthy-instances service-id->healthy-instances'
                                     :service-id->unhealthy-instances service-id->unhealthy-instances'
                                     :service-id->expired-instances service-id->expired-instances'
                                     :service-id->starting-instances service-id->starting-instances'
                                     :service-id->failed-instances service-id->failed-instances'
                                     :service-id->deployment-error service-id->deployment-error'
                                     :time scheduler-sync-time))

                                 :update-app-instances
                                 (let [{:keys [service-id healthy-instances unhealthy-instances failed-instances scheduler-sync-time]} message-data
                                       service-id->healthy-instances' (assoc service-id->healthy-instances service-id healthy-instances)
                                       service-id->unhealthy-instances' (assoc service-id->unhealthy-instances service-id unhealthy-instances)
                                       service-description (service-id->service-description-fn service-id)
                                       grace-period-secs (t/seconds (int (get service-description "grace-period-secs")))
                                       expiry-mins-int (int (get service-description "instance-expiry-mins"))
                                       expiry-mins (t/minutes expiry-mins-int)
                                       expired-instances (filter #(and (pos? expiry-mins-int)
                                                                       (utils/older-than? scheduler-sync-time expiry-mins %))
                                                                 healthy-instances)
                                       starting-instances (filter #(not (utils/older-than? scheduler-sync-time grace-period-secs %)) unhealthy-instances)
                                       service-id->expired-instances' (assoc service-id->expired-instances service-id expired-instances)
                                       service-id->starting-instances' (assoc service-id->starting-instances service-id starting-instances)
                                       service-id->failed-instances' (assoc service-id->failed-instances service-id failed-instances)
                                       deployment-error (get-deployment-error healthy-instances unhealthy-instances failed-instances deployment-error-config)
                                       service-id->deployment-error' (if deployment-error
                                                                       (assoc service-id->deployment-error service-id deployment-error)
                                                                       (dissoc service-id->deployment-error service-id))]
                                   (when (or (not= (get service-id->healthy-instances service-id) healthy-instances)
                                             (not= (get service-id->unhealthy-instances service-id) unhealthy-instances))
                                     (let [curr-instance-ids (set (map :id healthy-instances))
                                           prev-instance-ids (set (map :id (get service-id->healthy-instances service-id)))
                                           new-instance-ids (filterv (complement prev-instance-ids) curr-instance-ids)
                                           rem-instance-ids (filterv (complement curr-instance-ids) prev-instance-ids)
                                           unhealthy-instance-ids (mapv :id (get service-id->unhealthy-instances' service-id))]
                                       (log/info "update-healthy-instances:" service-id "has"
                                                 (count healthy-instances) "healthy instance(s) and"
                                                 (count unhealthy-instance-ids) "unhealthy instance(s)."
                                                 (if (seq new-instance-ids) (str "New healthy instances: " new-instance-ids ".") "")
                                                 (if (seq rem-instance-ids) (str "Removed healthy instances: " rem-instance-ids ".") "")
                                                 (if (seq unhealthy-instance-ids) (str "Unhealthy instances: " unhealthy-instance-ids ".") ""))))
                                   (assoc loop-state
                                     :service-id->healthy-instances service-id->healthy-instances'
                                     :service-id->unhealthy-instances service-id->unhealthy-instances'
                                     :service-id->expired-instances service-id->expired-instances'
                                     :service-id->starting-instances service-id->starting-instances'
                                     :service-id->failed-instances service-id->failed-instances'
                                     :service-id->deployment-error service-id->deployment-error'
                                     :time scheduler-sync-time))

                                 ; default value
                                 (do
                                   (log/warn "scheduler-state-chan unknown message type=" message-type)
                                   loop-state))]
                           (if (nil? remaining)
                             (let [new-state (update-router-state router-id current-state loop-state' service-id->service-description-fn)]
                               (when (not= (select-keys current-state [:service-id->healthy-instances :service-id->unhealthy-instances
                                                                       :service-id->expired-instances :service-id->starting-instances
                                                                       :service-id->failed-instances :service-id->deployment-error])
                                           (select-keys new-state [:service-id->healthy-instances :service-id->unhealthy-instances
                                                                   :service-id->expired-instances :service-id->starting-instances
                                                                   :service-id->failed-instances :service-id->deployment-error]))
                                 ; propagate along router-state-push-chan only when state changes
                                 (async/put! router-state-push-chan new-state))
                               new-state)
                             (recur loop-state' remaining))))))

                   router-chan
                   ([data]
                     (cid/with-correlation-id
                       (str "router-state-maintainer-" iteration)
                       (let [router-id->endpoint-url data
                             new-state
                             (if (not= routers router-id->endpoint-url)
                               (let [candidate-state (assoc current-state :routers router-id->endpoint-url)
                                     current-routers (-> current-state :routers keys vec sort)
                                     new-routers (-> router-id->endpoint-url keys vec sort)]
                                 (log/info "peer router info changed from" current-routers "to" new-routers)
                                 (metrics/update-counter (metrics/waiter-counter "core" "number-of-routers") routers router-id->endpoint-url)
                                 (update-router-state router-id current-state candidate-state service-id->service-description-fn))
                               current-state)]
                         (async/put! router-state-push-chan new-state)
                         new-state)))

                   [[state-chan current-state]]
                   (assoc current-state :router-id router-id))]
             (if next-state
               (recur next-state)
               (log/info "Stopping router-state-maintainer as next state is nil"))))
         (catch Exception e
           (log/error e "Fatal error in router-state-maintainer!")
           (System/exit 1))))}))

;; External state queries

(defn retrieve-peer-routers
  "Query `discovery` for the list of currently running routers and populates it into `router-chan`."
  [discovery router-chan]
  (log/trace "querying discovery for routers")
  (async/>!! router-chan (discovery/router-id->endpoint-url discovery "http" "")))

(defn start-router-syncer
  "Starts loop to query discovery service for the list of currently running routers and
   sends the list to the router state maintainer."
  [discovery router-chan router-syncer-interval-ms router-syncer-delay-ms]
  (log/info "Starting router syncer")
  (utils/start-timer-task
    (t/millis router-syncer-interval-ms)
    #(retrieve-peer-routers discovery router-chan)
    :delay-ms router-syncer-delay-ms))
