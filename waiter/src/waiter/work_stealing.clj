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
(ns waiter.work-stealing
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metrics.counters :as counters]
            [plumbing.core :as pc]
            [waiter.async-utils :as au]
            [waiter.metrics :as metrics]
            [waiter.service :as service]
            [waiter.utils :as utils]))

(defn router-id->metrics->router-id->help-required
  "Converts the router->metrics map to a router->help-required map.
   Only routers which are deemed to need help are included in the result map."
  [router->metrics]
  (->> router->metrics
       (pc/map-vals (fn [metrics]
                      (when (utils/help-required? metrics)
                        (utils/compute-help-required metrics))))
       (utils/filterm (fn [[_ help-required]]
                        (and help-required (pos? help-required))))))

(defn- make-work-stealing-offers
  "Makes work-stealing offers to victim routers when the current router has idle slots.
   Routers which are more heavily loaded preferentially receive help offers."
  [label offer-help-fn reserve-instance-fn {:keys [iteration] :as current-state} offerable-slots
   router-id->help-required cleanup-chan router-id service-id]
  (async/go
    (loop [counter 0
           iteration-state current-state
           router-id->help-required router-id->help-required]
      (let [iter-label (str label ".iter" iteration)]
        (if (and (< counter offerable-slots) (seq router-id->help-required))
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
  [initial-state timeout-chan-factory service-id->router-id->metrics reserve-instance-fn release-instance-fn offer-help-fn router-id service-id]
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
                                 (counters/inc! (metrics/service-counter service-id "work-stealing" "sent-to" target-router-id "releases"))
                                 (counters/dec! (metrics/service-counter service-id "work-stealing" "sent-to" "in-flight"))
                                 (assoc current-state :request-id->work-stealer (dissoc request-id->work-stealer request-id)))
                               current-state)))

                         timeout-chan
                         ([_]
                           (let [router-id->metrics (service-id->router-id->metrics service-id)
                                 _ (log/trace label "received metrics from" (count router-id->metrics) "routers")
                                 offerable-slots (-> (router-id->metrics router-id)
                                                     (utils/compute-help-required)
                                                     (unchecked-negate))
                                 offered-slots (count request-id->work-stealer)
                                 router-id->help-required (-> router-id->metrics
                                                              (dissoc router-id)
                                                              (router-id->metrics->router-id->help-required))]
                             (log/trace label "can make up to" offerable-slots "work-stealing offers this iteration")
                             (-> (if (and (pos? offerable-slots)
                                          (seq router-id->help-required))
                                   (async/<!
                                     (make-work-stealing-offers
                                       label offer-help-fn reserve-instance-fn current-state offerable-slots
                                       router-id->help-required cleanup-chan router-id service-id))
                                   (do
                                     (log/debug label "no work-stealing offers this iteration"
                                                {:metrics (router-id->metrics router-id)
                                                 :slots {:offerable offerable-slots
                                                         :offered offered-slots}})
                                     current-state))
                                 (assoc :timeout-chan (timeout-chan-factory)))))

                         query-chan
                         ([data]
                           (let [{:keys [response-chan]} data
                                 router-id->metrics (service-id->router-id->metrics service-id)
                                 offered-slots (count request-id->work-stealer)
                                 offerable-slots (-> (router-id->metrics router-id)
                                                     (utils/compute-help-required)
                                                     (unchecked-negate))
                                 router-id->help-required (-> router-id->metrics
                                                              (dissoc router-id)
                                                              (router-id->metrics->router-id->help-required))]
                             (log/info label "state has been queried")
                             (async/put! response-chan (-> current-state
                                                           (dissoc :timeout-chan)
                                                           (assoc :router-id->help-required router-id->help-required
                                                                  :router-id->metrics router-id->metrics
                                                                  :slots {:offerable offerable-slots
                                                                          :offered offered-slots})))
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
  [instance-rpc-chan reserve-timeout-ms offer-help-interval-ms service-id->router-id->metrics
   make-inter-router-requests-fn router-id service-id]
  (log/info "starting work-stealing balancer for" service-id)
  (letfn [(reserve-instance-fn
            [reservation-parameters response-chan]
            (async/go
              (try
                (let [instance (-> (service/get-rand-inst
                                     instance-rpc-chan
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
            [{:keys [instance] :as reservation-summary}]
            (service/release-instance-go
              instance-rpc-chan
              instance
              (select-keys reservation-summary [:cid, :request-id, :status])))
          (offer-help-fn
            [{:keys [request-id target-router-id] :as reservation-parameters} cleanup-chan]
            (async/go
              (let [response-result-promise (promise)
                    response->body (fn [{:keys [body error]}]
                                     (if error
                                       (throw error)
                                       body))
                    default-response-result "work-stealing-error"]
                (try
                  (let [{:keys [headers status] :as inter-router-response}
                        (-> (make-inter-router-requests-fn "work-stealing"
                                                           :acceptable-router? #(= target-router-id %)
                                                           :body (-> reservation-parameters
                                                                     (assoc :router-id router-id
                                                                            :service-id service-id)
                                                                     (utils/map->json-response)
                                                                     :body)
                                                           :method :post)
                            (get target-router-id)
                            async/<!)
                        response-result (if (and inter-router-response (<= 200 status 299))
                                          (-> inter-router-response
                                              response->body
                                              async/<! ;; rely on http client library to close the body
                                              str
                                              json/read-str
                                              walk/keywordize-keys
                                              (get :response-status default-response-result))
                                          (do
                                            (log/info "no inter-router response from" target-router-id
                                                      {:cid (get headers "x-cid"), :status status})
                                            default-response-result))]
                    (deliver response-result-promise response-result))
                  (catch Exception e
                    (counters/inc! (metrics/service-counter service-id "work-stealing" "sent-to" target-router-id "errors"))
                    (deliver response-result-promise default-response-result)
                    (log/error e "unsuccessful attempt to offer work-stealing help" reservation-parameters))
                  (finally
                    (deliver response-result-promise "success")
                    (let [offer-response-status (keyword @response-result-promise)]
                      (async/>! cleanup-chan {:request-id request-id, :status offer-response-status})))))))
          (timeout-chan-factory
            []
            (async/timeout offer-help-interval-ms))]
    (work-stealing-balancer {} timeout-chan-factory service-id->router-id->metrics reserve-instance-fn release-instance-fn
                            offer-help-fn router-id service-id)))
