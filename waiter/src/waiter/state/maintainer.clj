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
(ns waiter.state.maintainer
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [digest]
            [metrics.counters :as counters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]))

(defn retrieve-maintainer-channel
  "Retrieves the channel mapped to the provided method."
  [channel-map method]
  (let [maintainer-chan-keyword :maintainer-chan-map
        method-chan (case method
                      :eject [maintainer-chan-keyword :eject-instance-chan]
                      :kill [maintainer-chan-keyword :kill-instance-chan]
                      :offer [maintainer-chan-keyword :work-stealing-chan]
                      :query-state [maintainer-chan-keyword :query-state-chan]
                      :query-work-stealing [:work-stealing-chan-map :query-chan]
                      :release [maintainer-chan-keyword :release-instance-chan]
                      :reserve [maintainer-chan-keyword :reserve-instance-chan-in]
                      :scaling-state [maintainer-chan-keyword :scaling-state-chan]
                      :update-state [maintainer-chan-keyword :update-state-chan])]
    (get-in channel-map method-chan)))

(defn close-update-state-channel
  "Closes the update-state channel for the responder"
  [service-id {:keys [update-state-chan]}]
  (log/warn "[close-update-state-channel] closing update-state-chan of" service-id)
  (async/go (async/close! update-state-chan)))

(defn retrieve-service-method-chan
  "Retrieves the channel mapped to the provided service and method."
  [{:keys [service-id->channel-map]} retrieve-channel service-id method]
  (if-let [channel-map (get service-id->channel-map service-id)]
    (do
      (counters/inc! (metrics/service-counter service-id "maintainer" "fast-path" "hit"))
      (retrieve-channel channel-map method))
    (do
      (counters/inc! (metrics/service-counter service-id "maintainer" "fast-path" "miss"))
      nil)))

(defn forward-request-to-instance-rpc-chan!
  "Forwards the request to the instance-rpc-chan which will eventually populate/close response-chan.
   If the forwarding fails, closes the input response-chan immediately."
  [instance-rpc-chan {:keys [cid response-chan] :as request-map}]
  (when-not (try
              (async/put! instance-rpc-chan request-map)
              (catch Throwable th
                (cid/cerror cid th "error in forwarding request to instance-rpc-chan")))
    (cid/cinfo cid "put! on instance-rpc-chan unsuccessful, closing response-chan")
    (async/close! response-chan)))

(defn start-service-chan-maintainer
  "go block to maintain the mapping from service-id to channels to communicate with
   service-chan-responders.

   Services are started by invoking `start-service` with the `service-id` and the
   returned result is stored as the channel-map.

   Services are destroyed (when the service is missing in state updates) by invoking
   `(remove-service service-id channel-map)`.

   Requests for service-chan-responder channels is passed into instance-rpc-chan
   It is expected that messages on the channel have a map with keys
      `:cid`, `:method`, `:response-chan`, and `:service-id`.
   The `method` should be either `:eject`, `:kill`, `:query-state`, `:reserve`,
      `:release`, or `:scaling-state`.
   The `service-id` is the service for the request and `response-chan` will have the
       corresponding channel placed onto it by invoking `(retrieve-channel channel-map method)`.
   The returned `populate-maintainer-chan!` function can be used as a fast-path to bypass
       a message on instance-rpc-chan and instead retrieve the channel by directly invoking
       `(retrieve-channel channel-map method)`.

   Updated state for the router is passed into `state-chan` and then propagated to the
   service-chan-responders.

   `query-service-maintainer-chan` return the current state of the maintainer."
  [initial-state state-chan query-service-maintainer-chan
   start-service remove-service retrieve-channel]
  (let [instance-rpc-chan (async/chan 1024)
        exit-chan (async/chan 1)
        state-atom (atom {:last-update-time (t/now)
                          :service-id->channel-map nil})
        update-state-timer (metrics/waiter-timer "state" "service-chan-maintainer" "update-state")
        populate-maintainer-chan! (fn populate-maintainer-chan!
                                    [{:keys [method response-chan service-id] :as request-map}]
                                    (if-let [result-chan (retrieve-service-method-chan @state-atom retrieve-channel service-id method)]
                                      (async/put! response-chan result-chan)
                                      (forward-request-to-instance-rpc-chan! instance-rpc-chan request-map)))]
    (async/go
      (try
        (loop [service-id->channel-map (or (:service-id->channel-map initial-state) {})
               last-state-update-time (:last-state-update-time initial-state)]
          (swap!
            state-atom
            assoc
            :last-update-time last-state-update-time
            :service-id->channel-map service-id->channel-map)
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
                     (try
                       (let [{:keys [service-id->my-instance->slots service-id->unhealthy-instances service-id->expired-instances
                                     service-id->starting-instances service-id->deployment-error service-id->instability-issue time]} router-state
                             incoming-service-ids (set (keys service-id->my-instance->slots))
                             known-service-ids (set (keys service-id->channel-map))
                             new-service-ids (set/difference incoming-service-ids known-service-ids)
                             removed-service-ids (set/difference known-service-ids incoming-service-ids)
                             service-id->channel-map' (select-keys service-id->channel-map incoming-service-ids)
                             new-chans (map #(start-service populate-maintainer-chan! %) new-service-ids) ; create new responders
                             service-id->channel-map'' (if (seq new-chans)
                                                         (apply assoc service-id->channel-map' (interleave new-service-ids new-chans))
                                                         service-id->channel-map')]
                         ;; Destroy deleted responders
                         (doseq [service-id removed-service-ids]
                           (log/warn "[service-chan-maintainer] removing" service-id)
                           (remove-service service-id (service-id->channel-map service-id)))
                         ;; Update state for responders
                         (doseq [service-id (keys service-id->channel-map'')]
                           (try
                             (let [my-instance->slots (get service-id->my-instance->slots service-id)
                                   healthy-instances (keys my-instance->slots)
                                   unhealthy-instances (get service-id->unhealthy-instances service-id)
                                   expired-instances (get service-id->expired-instances service-id)
                                   starting-instances (get service-id->starting-instances service-id)
                                   deployment-error (get service-id->deployment-error service-id)
                                   instability-issue (get service-id->instability-issue service-id)
                                   update-state-chan (retrieve-channel (get service-id->channel-map'' service-id) :update-state)]
                               (if (or healthy-instances unhealthy-instances)
                                 (async/put! update-state-chan
                                             (let [update-state {:healthy-instances healthy-instances
                                                                 :unhealthy-instances unhealthy-instances
                                                                 :expired-instances expired-instances
                                                                 :starting-instances starting-instances
                                                                 :my-instance->slots my-instance->slots
                                                                 :deployment-error deployment-error
                                                                 :instability-issue instability-issue}]
                                               [update-state time]))
                                 (async/put! update-state-chan [{} time])))
                             (catch Exception ex
                               (log/error ex "[service-chan-maintainer] error in updating router state for" service-id)
                               (throw ex))))
                         [service-id->channel-map'' time])
                       (catch Exception ex
                         (log/error ex "[service-chan-maintainer] error in updating router state")
                         (throw ex)))))

                  instance-rpc-chan
                  ([message]
                   (let [{:keys [cid method response-chan service-id]} message]
                     (cid/cdebug cid "[service-chan-maintainer]" service-id "received request of type" method)
                     (try
                       (if-let [channel-map (get service-id->channel-map service-id)]
                         (let [method-chan (retrieve-channel channel-map method)]
                           (async/put! response-chan method-chan))
                         (do
                           (cid/cdebug cid "[service-chan-maintainer] no channel map found for" service-id)
                           (counters/inc! (metrics/service-counter service-id "maintainer" "not-found"))
                           (async/close! response-chan)))
                       [service-id->channel-map last-state-update-time]
                       (catch Exception ex
                         (log/error ex "[service-chan-maintainer] error processing request" message)
                         (throw ex)))))

                  query-service-maintainer-chan
                  ([message]
                   (let [{:keys [service-id response-chan]} message]
                     (log/info "[service-chan-maintainer] state has been queried for" service-id)
                     (when-not (au/chan? response-chan)
                       (log/error "[service-chan-maintainer] invalid response channel" response-chan)
                       (throw (ex-info "invalid response channel" {:message message})))
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
          (log/error e "fatal error in service-chan-maintainer")
          (System/exit 1))))
    {:exit-chan exit-chan
     :instance-rpc-chan instance-rpc-chan
     :query-state-fn (fn query-service-chan-maintainer-state []
                       (-> @state-atom
                         (update :service-id->channel-map keys)
                         (set/rename-keys {:service-id->channel-map :service-ids})))
     :populate-maintainer-chan! populate-maintainer-chan!}))

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
          (zero? num-slots-available) (reduce (fn [instance-id->assigned-slots instance-id]
                                                (if (contains? instance-id->assigned-slots instance-id)
                                                  instance-id->assigned-slots
                                                  (assign-slot-fn instance-id->assigned-slots instance-id)))
                                              instance-id->assigned-slots
                                              (take num-slots-to-consume sorted-instance-ids))
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
                                                 router-id router-id->http-endpoint' service-id->service-description-fn delta-service-id->healthy-instances)
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
          all-instances-exited-similarly? (and first-exit-code (every? #(= first-exit-code %) (map :exit-code failed-instances)))
          deployment-error (cond
                             (and has-failed-instances? all-instances-exited-similarly?) :bad-startup-command
                             (and has-failed-instances? (all-instances-flagged-with? :memory-limit-exceeded)) :not-enough-memory
                             (and has-failed-instances? (no-instances-flagged-with? :has-connected)) :cannot-connect
                             (and has-failed-instances? (no-instances-flagged-with? :has-responded)) :health-check-timed-out
                             (and has-failed-instances? (all-instances-flagged-with? :never-passed-health-checks)) :invalid-health-check-response
                             (and has-unhealthy-instances? (= first-unhealthy-status http-401-unauthorized)) :health-check-requires-authentication)]
      (when deployment-error
        (log/debug "computed deployment error"
                   {:deployment-error deployment-error
                    :instances {:failed (map :id failed-instances)
                                :unhealthy (map :id unhealthy-instances)}}))
      deployment-error)))

(defn get-instability-issue
  "Returns appropriate instability issue (only oom for now) for a service based on its instances, or nil if no such issues."
  [failed-instances]
  (when (not-empty failed-instances)
    (let [failed-instances-flags (map :flags failed-instances)
          some-instances-flagged-with? (fn [flag] (some #(contains? % flag) failed-instances-flags))]
      (cond
        (some-instances-flagged-with? :memory-limit-exceeded) :not-enough-memory))))

(defn start-router-state-maintainer
  "Start the instance state maintainer.
   Maintains the state of the router as well as the state of marathon and the existence of other routers.
   Acts as the central access point for modifying this data for the router.
   Exposes the state of the router via a `query-state-fn` no-args function that is returned."
  [scheduler-state-chan router-chan router-id exit-chan service-id->service-description-fn
   refresh-service-descriptions-fn service-id->deployment-error-config-fn default-deployment-error-config]
  (cid/with-correlation-id
    "router-state-maintainer"
    (let [killed-instances-to-keep 10
          state-atom (atom {:all-available-service-ids #{}
                            :iteration 0
                            :routers []
                            :service-id->deployment-error {}
                            :service-id->expired-instances {}
                            :service-id->failed-instances {}
                            :service-id->healthy-instances {}
                            :service-id->instability-issue {}
                            :service-id->instance-counts {}
                            :service-id->killed-instances {}
                            :service-id->my-instance->slots {} ; updated in update-router-state
                            :service-id->starting-instances {}
                            :service-id->unhealthy-instances {}
                            :time (t/now)})
          router-state-push-chan (au/latest-chan)
          query-chan (async/chan)
          kill-notification-chan (async/chan 16)
          go-chan
          (async/go
            (try
              (loop [{:keys [routers] :as current-state} @state-atom]
                (reset! state-atom current-state)
                (let [next-state
                      (async/alt!
                        exit-chan
                        ([message]
                         (log/warn "Stopping router-state-maintainer")
                         (when (not= :exit message)
                           (throw (ex-info "Stopping router-state maintainer" {:time (t/now), :reason message}))))

                        scheduler-state-chan
                        ([scheduler-messages]
                         (loop [{:keys [service-id->healthy-instances service-id->unhealthy-instances service-id->my-instance->slots
                                        service-id->expired-instances service-id->starting-instances service-id->failed-instances
                                        service-id->instance-counts service-id->deployment-error service-id->instability-issue
                                        service-id->killed-instances] :as loop-state} current-state
                                [[message-type message-data] & remaining] scheduler-messages]
                           (log/trace "scheduler-state-chan received, type:" message-type)
                           (let [loop-state'
                                 (case message-type
                                   :update-available-services
                                   (let [{:keys [available-service-ids scheduler-sync-time]} message-data
                                         all-available-service-ids' (refresh-service-descriptions-fn available-service-ids)
                                         services-without-instances (remove #(contains? service-id->my-instance->slots %) all-available-service-ids')
                                         service-id->healthy-instances' (select-keys service-id->healthy-instances all-available-service-ids')
                                         service-id->killed-instances' (select-keys service-id->killed-instances all-available-service-ids')
                                         service-id->unhealthy-instances' (select-keys service-id->unhealthy-instances all-available-service-ids')
                                         service-id->expired-instances' (select-keys service-id->expired-instances all-available-service-ids')
                                         service-id->starting-instances' (select-keys service-id->starting-instances all-available-service-ids')
                                         service-id->failed-instances' (select-keys service-id->failed-instances all-available-service-ids')
                                         service-id->instance-counts' (select-keys service-id->instance-counts all-available-service-ids')
                                         service-id->deployment-error' (select-keys service-id->deployment-error all-available-service-ids')
                                         service-id->instability-issue' (select-keys service-id->instability-issue all-available-service-ids')]
                                     (when (or (not= service-id->healthy-instances service-id->healthy-instances')
                                               (not= service-id->unhealthy-instances service-id->unhealthy-instances')
                                               (seq services-without-instances))
                                       (log/info "update-available-services:"
                                                 (count service-id->healthy-instances') "services with healthy instances and"
                                                 (count services-without-instances) "services without instances:"
                                                 (vec services-without-instances)))
                                     (assoc loop-state
                                       :all-available-service-ids all-available-service-ids'
                                       :service-id->healthy-instances service-id->healthy-instances'
                                       :service-id->unhealthy-instances service-id->unhealthy-instances'
                                       :service-id->expired-instances service-id->expired-instances'
                                       :service-id->starting-instances service-id->starting-instances'
                                       :service-id->failed-instances service-id->failed-instances'
                                       :service-id->instance-counts service-id->instance-counts'
                                       :service-id->deployment-error service-id->deployment-error'
                                       :service-id->instability-issue service-id->instability-issue'
                                       :service-id->killed-instances service-id->killed-instances'
                                       :time scheduler-sync-time))

                                   :update-service-instances
                                   (let [{:keys [service-id healthy-instances unhealthy-instances failed-instances instance-counts scheduler-sync-time]} message-data
                                         service-id->healthy-instances' (assoc service-id->healthy-instances service-id healthy-instances)
                                         service-id->unhealthy-instances' (assoc service-id->unhealthy-instances service-id unhealthy-instances)
                                         service-description (service-id->service-description-fn service-id)]
                                     (if-not (seq service-description)
                                       (do
                                         (log/info "skipping instances of unknown service" service-id)
                                         loop-state)
                                       (let [grace-period-secs (t/seconds (int (get service-description "grace-period-secs")))
                                             expiry-mins-int (int (get service-description "instance-expiry-mins"))
                                             expiry-mins (t/minutes expiry-mins-int)
                                             instance-expired? (fn instance-expired? [instance]
                                                                 (or (contains? (:flags instance) :expired)
                                                                     (and (pos? expiry-mins-int)
                                                                          (du/older-than? scheduler-sync-time expiry-mins instance))))
                                             expired-healthy-instances (filter instance-expired? healthy-instances)
                                             expired-unhealthy-instances (filter instance-expired? unhealthy-instances)
                                             expired-instances (concat expired-healthy-instances expired-unhealthy-instances)
                                             starting-instances (filter #(not (du/older-than? scheduler-sync-time grace-period-secs %)) unhealthy-instances)
                                             service-id->expired-instances' (assoc service-id->expired-instances service-id expired-instances)
                                             service-id->starting-instances' (assoc service-id->starting-instances service-id starting-instances)
                                             service-id->failed-instances' (assoc service-id->failed-instances service-id failed-instances)
                                             service-id->instance-counts' (assoc service-id->instance-counts service-id instance-counts)
                                             deployment-error-config (merge default-deployment-error-config
                                                                            (service-id->deployment-error-config-fn service-id))
                                             deployment-error (get-deployment-error healthy-instances unhealthy-instances failed-instances deployment-error-config)
                                             service-id->deployment-error' (if deployment-error
                                                                             (assoc service-id->deployment-error service-id deployment-error)
                                                                             (dissoc service-id->deployment-error service-id))
                                             instability-issue (get-instability-issue failed-instances)
                                             service-id->instability-issue' (if instability-issue
                                                                              (assoc service-id->instability-issue service-id instability-issue)
                                                                              (dissoc service-id->instability-issue service-id))]
                                         (when (or (not= (get service-id->healthy-instances service-id) healthy-instances)
                                                   (not= (get service-id->unhealthy-instances service-id) unhealthy-instances))
                                           (let [curr-instances (set healthy-instances)
                                                 prev-instances (set (get service-id->healthy-instances service-id))
                                                 new-instances (filter (complement prev-instances) curr-instances)
                                                 rem-instances (filter (complement curr-instances) prev-instances)
                                                 new-instance-ids (mapv :id new-instances)
                                                 rem-instance-ids (mapv :id rem-instances)
                                                 prev-unhealthy-instances (set (get service-id->unhealthy-instances service-id))
                                                 new-unhealthy-instances (filter (complement prev-unhealthy-instances) unhealthy-instances)
                                                 unhealthy-instance-ids (mapv :id (get service-id->unhealthy-instances' service-id))]
                                             (doseq [rem-instance rem-instances]
                                               (scheduler/log-service-instance rem-instance :remove :info))
                                             (doseq [new-healthy-instance new-instances]
                                               (scheduler/log-service-instance new-healthy-instance :healthy :info))
                                             (doseq [new-unhealthy-instance new-unhealthy-instances]
                                               (scheduler/log-service-instance new-unhealthy-instance :unhealthy :info))
                                             (log/info "update-healthy-instances:" service-id "has"
                                                       {:num-expired-healthy-instances (count expired-healthy-instances)
                                                        :num-expired-unhealthy-instances (count expired-unhealthy-instances)
                                                        :num-healthy-instances (count healthy-instances)
                                                        :num-unhealthy-instance-ids (count unhealthy-instance-ids)
                                                        :service-id service-id}
                                                       (if (seq new-instance-ids) (str "New healthy instances: " new-instance-ids ".") "")
                                                       (if (seq rem-instance-ids) (str "Removed healthy instances: " rem-instance-ids ".") "")
                                                       (if (seq unhealthy-instance-ids) (str "Unhealthy instances: " unhealthy-instance-ids ".") ""))))
                                         (when (not= (get service-id->expired-instances service-id) expired-instances)
                                            (let [cur-exp-instances (set expired-instances)
                                                  old-exp-instances (set (get service-id->expired-instances service-id))
                                                  delta-exp-instances (filter (complement old-exp-instances) cur-exp-instances)]
                                              (doseq [expired-instance delta-exp-instances]
                                                (scheduler/log-service-instance expired-instance :expire :info))))
                                         (assoc loop-state
                                           :service-id->deployment-error service-id->deployment-error'
                                           :service-id->expired-instances service-id->expired-instances'
                                           :service-id->failed-instances service-id->failed-instances'
                                           :service-id->healthy-instances service-id->healthy-instances'
                                           :service-id->instability-issue service-id->instability-issue'
                                           :service-id->instance-counts service-id->instance-counts'
                                           :service-id->starting-instances service-id->starting-instances'
                                           :service-id->unhealthy-instances service-id->unhealthy-instances'
                                           :time scheduler-sync-time))))

                                   ; default value
                                   (do
                                     (log/warn "scheduler-state-chan unknown message type=" message-type)
                                     loop-state))]
                             (if (nil? remaining)
                               (let [new-state (update-router-state router-id current-state loop-state' service-id->service-description-fn)
                                     relevant-keys [:service-id->deployment-error :service-id->expired-instances :service-id->failed-instances
                                                    :service-id->healthy-instances :service-id->instability-issue :service-id->instance-counts
                                                    :service-id->starting-instances :service-id->unhealthy-instances]]
                                 (when (not= (select-keys current-state relevant-keys)
                                             (select-keys new-state relevant-keys))
                                   ; propagate along router-state-push-chan only when state changes
                                   (async/put! router-state-push-chan new-state))
                                 new-state)
                               (recur loop-state' remaining)))))

                        kill-notification-chan
                        ([message]
                         (let [{{:keys [id service-id] :as instance} :instance} message]
                           (log/info "tracking killed instance" id "of service" service-id)
                           (-> current-state
                             (update-in [:service-id->killed-instances service-id]
                                        (fn [killed-instances]
                                          (vec
                                            (take
                                              killed-instances-to-keep
                                              (conj
                                                (filterv (fn [i] (not= (:id i) id)) killed-instances)
                                                instance))))))))

                        query-chan
                        ([response-chan]
                         (async/put! response-chan current-state)
                         current-state)

                        router-chan
                        ([data]
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
                           new-state)))]
                  (if next-state
                    (recur next-state)
                    (log/info "Stopping router-state-maintainer as next state is nil"))))
              (catch Exception e
                (log/error e "Fatal error in router-state-maintainer!")
                (System/exit 1))))]
      {:go-chan go-chan
       :notify-instance-killed-fn (fn notify-router-state-maintainer-of-instance-killed [instance]
                                    (log/info "received notification of killed instance" (:id instance))
                                    (async/go (async/>! kill-notification-chan {:instance instance})))
       :query-chan query-chan
       :query-state-fn (fn router-state-maintainer-query-state-fn []
                         (assoc @state-atom :router-id router-id))
       :router-state-push-mult (async/mult router-state-push-chan)})))
