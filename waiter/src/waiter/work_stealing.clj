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
(ns waiter.work-stealing
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [plumbing.core :as pc]
            [waiter.metrics :as metrics]
            [waiter.service :as service]
            [waiter.util.async-utils :as au]
            [waiter.util.semaphore :as semaphore]
            [waiter.util.utils :as utils]))

(defn compute-help-required
  "Computes the number of slots (requests that can be made to instances) of help required at a router given the values for:
     outstanding: the number of outstanding requests at the router;
     slots-available: the number of slots available (where available = not in use and not blacklisted) from those assigned
                      to the router by the distribution algorithm;
     slots-in-use: the number of slots used by the router from those that were assigned to it by the distribution
                   algorithm at some point in time, it may include slots from instances that the router no longer owns; and
     slots-received: the number of slots received as help from other routers via work-stealing.
   The slots-in-use allows us to account for instances being used by a router that it no longer owns.
   If the function returns positive, say +x, it means the router needs x slots of help to service requests.
   If the function returns zero, it means the router does not need help.
   If the function returns negative, say -x, then the router needs no help and has x extra unused slots that were
   either assigned to it by the distribution algorithm or received from work-stealing offers."
  [{:strs [outstanding slots-available slots-in-use slots-received]
    :or {outstanding 0, slots-available 0, slots-in-use 0, slots-received 0}}]
  (- outstanding (+ slots-in-use slots-available slots-received)))

(defn help-required?
  "Determines whether a given router needs help based on the values of:
     outstanding: the number of outstanding requests at the router;
     slots-available: the number of slots available (where available = not in use and not blacklisted) from those assigned
                      to the router by the distribution algorithm;
     slots-in-use: the number of slots used by the router from those that were assigned to it by the distribution
                   algorithm at some point in time, it may include slots from instances that the router no longer owns; and
     slots-received: the number of slots received as help from other routers via work-stealing.
   It returns true if there are no slots available and `compute-help-required` returns a positive value."
  [{:strs [slots-available] :or {slots-available 0} :as metrics-map}]
  (and (zero? slots-available)
       (pos? (compute-help-required metrics-map))))

(defn router-id->metrics->router-id->help-required
  "Converts the router->metrics map to a router->help-required map.
   Only routers which are deemed to need help are included in the result map."
  [router->metrics]
  (->> router->metrics
       (pc/map-vals (fn [metrics]
                      (when (help-required? metrics)
                        (compute-help-required metrics))))
       (utils/filterm (fn [[_ help-required]]
                        (and help-required (pos? help-required))))))

(defn- make-work-stealing-offers
  "Makes work-stealing offers to victim routers when the current router has idle slots.
   Routers which are more heavily loaded preferentially receive help offers."
  [label offer-help-fn reserve-instance-fn {:keys [iteration] :as current-state} offerable-slots
   router-id->help-required cleanup-chan offers-allowed-semaphore router-id service-id]
  (async/go
    (loop [counter 0
           iteration-state current-state
           router-id->help-required router-id->help-required]
      (let [iter-label (str label ".iter" iteration)]
        (if (and (< counter offerable-slots)
                 (seq router-id->help-required)
                 ;; acquiring the semaphore must be the last operation
                 (semaphore/try-acquire! offers-allowed-semaphore))
          (let [request-id (str service-id "." router-id ".ws" iteration ".offer" counter)
                reservation-parameters {:cid request-id, :request-id request-id}
                response-chan (async/promise-chan)
                _ (reserve-instance-fn reservation-parameters response-chan)
                {instance-id :id :as instance} (async/<! response-chan)]
            (if instance-id
              (let [target-router-id (-> (juxt val key)
                                         (sort-by router-id->help-required)
                                         last
                                         key)
                    help-required (get router-id->help-required target-router-id)
                    offer-parameters (assoc reservation-parameters
                                       :instance instance
                                       :target-router-id target-router-id)]
                (log/info iter-label (str "item" counter) "offering" instance-id "to" target-router-id)
                (counters/inc! (metrics/service-counter service-id "work-stealing" "sent-to" target-router-id "offers"))
                (counters/inc! (metrics/service-counter service-id "work-stealing" "sent-to" "total"))
                (counters/inc! (metrics/service-counter service-id "work-stealing" "sent-to" "in-flight"))
                (offer-help-fn offer-parameters cleanup-chan)
                (recur (inc counter)
                       (assoc-in iteration-state [:request-id->work-stealer request-id] offer-parameters)
                       (if (= 1 help-required)
                         (dissoc router-id->help-required target-router-id)
                         (update-in router-id->help-required [target-router-id] dec))))
              (do
                (when (pos? counter)
                  (log/info iter-label "no more instances to offer, offered" counter "of" offerable-slots "slots"))
                ;; release the semaphore, no instances were available
                (semaphore/release! offers-allowed-semaphore)
                iteration-state)))
          (do
            (if (pos? counter)
              (log/info iter-label "exhausted help offers, offered" counter "of" offerable-slots "slots"))
            iteration-state))))))

(defn work-stealing-balancer
  "go block to execute the work-stealing load balancer for a given service.
   `initial-state` is used to initialize the state of the maintainer.
   `timeout-chan-factory` is used to create timeout channels to determine when the next work-stealing round is triggered.
   `service-id->router-id->metrics` is used to retrieve router-id->metrics.
   `reserve-instance-fn` is a helper method to retrieve an idle instance slot using provided reservation parameters.
   `release-instance-fn` is a helper method to release a previously reserved instance slot.
   `offer-help-fn` has signature (fn [reservation-parameters cleanup-chan] ...) where cleanup chan receives the
                   map {:request-id request-id, :status response-status}.
   `router-id` the id of the current router.
   `service-id` refers to the id of the service whose load-balancer is being executed.

   Returns a map containing the query-chan and exit-chan:
   `query-chan` returns the current state of the load-balancer.
   `exit-chan` triggers the go-block to exit."
  [initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn release-instance-fn offer-help-fn
   offers-allowed-semaphore router-id service-id]
  (let [exit-chan (au/latest-chan)
        query-chan (async/chan 16)
        cleanup-chan (async/chan 1024)
        label (str "work-stealing-balancer-" service-id)]
    (async/go
      (try
        (loop [{:keys [iteration request-id->work-stealer timeout-chan] :as current-state}
               (merge {:iteration 0
                       :request-id->work-stealer {}
                       :timeout-chan (timeout-chan-factory)}
                      initial-state)]
          (when current-state
            (when-let [new-state
                       (async/alt!
                         exit-chan
                         ([data]
                           (log/info label "exit channel received" data)
                           (when (not= :exit data)
                             current-state))

                         cleanup-chan
                         ([data]
                           (let [{:keys [request-id status]} data]
                             (if-let [{:keys [target-router-id] :as work-stealer} (get request-id->work-stealer request-id)]
                               (do
                                 (log/info label "releasing instance reserved for request" request-id)
                                 (release-instance-fn (assoc work-stealer :status status))
                                 (semaphore/release! offers-allowed-semaphore)
                                 (counters/inc! (metrics/service-counter service-id "work-stealing" "sent-to" target-router-id "releases"))
                                 (counters/dec! (metrics/service-counter service-id "work-stealing" "sent-to" "in-flight"))
                                 (assoc current-state :request-id->work-stealer (dissoc request-id->work-stealer request-id)))
                               current-state)))

                         timeout-chan
                         ([_]
                           (let [router-id->metrics (service-id->router-id->metrics service-id)
                                 _ (log/trace label "received metrics from" (count router-id->metrics) "routers")
                                 offerable-slots (-> (router-id->metrics router-id)
                                                     compute-help-required
                                                     unchecked-negate)
                                 router-id->help-required (-> router-id->metrics
                                                              (dissoc router-id)
                                                              (router-id->metrics->router-id->help-required))]
                             (log/trace label "can make up to" offerable-slots "work-stealing offers this iteration")
                             (-> (if (and (pos? offerable-slots)
                                          (seq router-id->help-required))
                                   (async/<!
                                     (make-work-stealing-offers
                                       label offer-help-fn reserve-instance-fn current-state offerable-slots
                                       router-id->help-required cleanup-chan offers-allowed-semaphore router-id service-id))
                                   (do
                                     (log/debug label "no work-stealing offers this iteration"
                                                {:global-offers (semaphore/state offers-allowed-semaphore)
                                                 :metrics (router-id->metrics router-id)
                                                 :slots {:offerable offerable-slots
                                                         :offered (count request-id->work-stealer)}})
                                     current-state))
                                 (assoc :timeout-chan (timeout-chan-factory)))))

                         query-chan
                         ([data]
                           (let [{:keys [response-chan]} data
                                 router-id->metrics (service-id->router-id->metrics service-id)
                                 offerable-slots (-> (router-id->metrics router-id)
                                                     compute-help-required
                                                     unchecked-negate)
                                 router-id->help-required (-> router-id->metrics
                                                              (dissoc router-id)
                                                              (router-id->metrics->router-id->help-required))]
                             (log/info label "state has been queried")
                             (async/put! response-chan (-> current-state
                                                           (dissoc :timeout-chan)
                                                           (assoc :global-offers (semaphore/state offers-allowed-semaphore)
                                                                  :router-id->help-required router-id->help-required
                                                                  :router-id->metrics router-id->metrics
                                                                  :slots {:offerable offerable-slots
                                                                          :offered (count request-id->work-stealer)})))
                             current-state))

                         :priority true)]
              (recur (assoc new-state :iteration (inc iteration))))))
        (log/info label "exiting.")
        (catch Exception e
          (log/error e label "terminating work-stealing load balancing."))))
    {:exit-chan exit-chan
     :query-chan query-chan}))

(defn start-work-stealing-balancer
  "Starts the work-stealing balancer for all services."
  [populate-maintainer-chan! reserve-timeout-ms offer-help-interval-ms offers-allowed-semaphore
   service-id->router-id->metrics make-inter-router-requests-fn router-id service-id]
  (log/info "starting work-stealing balancer for" service-id)
  (letfn [(reserve-instance-fn
            [reservation-parameters response-chan]
            (async/go
              (try
                (let [instance (-> (service/get-rand-inst
                                     populate-maintainer-chan!
                                     service-id
                                     (assoc reservation-parameters
                                       :reason :work-stealing
                                       :time (t/now))
                                     #{}
                                     reserve-timeout-ms)
                                   (or :no-matching-instance-found))]
                  (async/>! response-chan instance))
                (catch Exception e
                  (log/error e "Error in reserving instance")
                  (async/>! response-chan :no-matching-instance-found)))))
          (release-instance-fn
            [{:keys [instance status] :as reservation-summary}]
            (counters/inc! (metrics/waiter-counter "work-stealing" "offer" (str "response-" (name status))))
            (meters/mark! (metrics/waiter-meter "work-stealing" "offer" "response-rate"))
            (service/release-instance-go
              populate-maintainer-chan!
              instance
              (select-keys reservation-summary [:cid :request-id :status])))
          (offer-help-fn
            [{:keys [request-id target-router-id] :as reservation-parameters} cleanup-chan]
            (async/go
              (let [response-result-promise (promise)
                    default-response-result "work-stealing-error"]
                (try
                  (counters/inc! (metrics/waiter-counter "work-stealing" "offer" "request"))
                  (meters/mark! (metrics/waiter-meter "work-stealing" "offer" "send-rate"))
                  (let [{:keys [body error headers status] :as inter-router-response}
                        (some-> (make-inter-router-requests-fn "work-stealing"
                                                               :acceptable-router? #(= target-router-id %)
                                                               :body (-> reservation-parameters
                                                                       (assoc :router-id router-id
                                                                              :service-id service-id)
                                                                       (utils/clj->json-response)
                                                                       :body)
                                                               :method :post)
                          (get target-router-id)
                          async/<!)
                        _ (when error
                            (throw error))
                        response-result (if (and inter-router-response body)
                                          (-> body
                                              async/<! ;; rely on http client library to close the body
                                              str
                                              json/read-str
                                              walk/keywordize-keys
                                              :response-status
                                              (or default-response-result))
                                          (do
                                            (log/info "no inter-router response from" target-router-id
                                                      {:cid (get headers "x-cid") :status status})
                                            default-response-result))]
                    (deliver response-result-promise response-result))
                  (catch Exception e
                    (counters/inc! (metrics/service-counter service-id "work-stealing" "sent-to" target-router-id "errors"))
                    (deliver response-result-promise default-response-result)
                    (log/error e "unsuccessful attempt to offer work-stealing help" reservation-parameters))
                  (finally
                    (deliver response-result-promise "success")
                    (let [offer-response-status (keyword @response-result-promise)]
                      (async/>! cleanup-chan {:request-id request-id :status offer-response-status})))))))
          (timeout-chan-factory
            []
            (async/timeout offer-help-interval-ms))]
    (work-stealing-balancer {} timeout-chan-factory service-id->router-id->metrics reserve-instance-fn release-instance-fn
                            offer-help-fn offers-allowed-semaphore router-id service-id)))
