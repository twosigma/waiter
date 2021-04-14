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
(ns waiter.service
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.statsd :as statsd]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.cache-utils :as cu]
            [waiter.util.utils :as utils])
  (:import java.util.concurrent.ExecutorService))

(def ^:const status-check-path "/status")

(def ^:const deployment-error-prefix "Deployment error: ")

;;; Service instance ejecting, work-stealing, access and creation

;; Attempt to eject instances
(defmacro eject-instance!
  "Sends a rpc to the router state to eject the given instance.
   Throws an exception if a eject channel cannot be found for the specfied service."
  [populate-maintainer-chan! service-id instance-id eject-period-ms response-chan]
  `(let [response-chan# (async/promise-chan)]
     (log/info "Requesting eject channel for" ~service-id)
     (->> {:cid (cid/get-correlation-id)
           :method :eject
           :response-chan response-chan#
           :service-id ~service-id}
          (~populate-maintainer-chan!))
     (if-let [eject-chan# (async/<! response-chan#)]
       (do
         (log/info "Received eject channel, making eject request.")
         (when-not (au/offer! eject-chan# [{:instance-id ~instance-id
                                                :eject-period-ms ~eject-period-ms
                                                :cid (cid/get-correlation-id)}
                                               ~response-chan])
           (throw (ex-info "Unable to put instance-id on eject chan."
                           {:instance-id ~instance-id, :service-id ~service-id}))))
       (do
         (log/error "Unable to find eject chan for service" ~service-id)
         (throw (ex-info "Service not found" {:instance-id ~instance-id
                                              :service-id ~service-id
                                              :status http-400-bad-request}))))))

(defn eject-instance-go
  "Sends a rpc to the router state to eject the lock on the given instance."
  [populate-maintainer-chan! service-id instance-id eject-period-ms response-chan]
  (async/go
    (try
      (eject-instance! populate-maintainer-chan! service-id instance-id eject-period-ms response-chan)
      (catch Exception e
        (log/error e "Error while ejecting instance" instance-id)))))

;; Offer instances obtained via work-stealing mechanism
(defmacro offer-instance!
  "Sends a rpc to the proxy state to offer the given instance.
   Throws an exception if a work-stealing channel cannot be found for the specified service."
  [populate-maintainer-chan! service-id offer-params]
  `(let [offer-params# ~offer-params
         response-chan# (async/promise-chan)]
     (log/debug "Requesting offer channel for" ~service-id)
     (->> {:cid (cid/get-correlation-id)
           :method :offer
           :response-chan response-chan#
           :service-id ~service-id}
          (~populate-maintainer-chan!))
     (if-let [work-stealing-chan# (async/<! response-chan#)]
       (do
         (log/info "received offer channel, making offer request.")
         (when-not (au/offer! work-stealing-chan# offer-params#)
           (log/error "unable to put instance on work-stealing-chan"
                      {:offer-params offer-params# :service-id ~service-id})
           (when-let [offer-response-chan# (:response-chan offer-params#)]
             (async/put! offer-response-chan# :channel-put-failed))))
       (do
         (log/info "unable to find work-stealing-chan"
                   {:offer-params offer-params# :service-id ~service-id})
         (when-let [offer-response-chan# (:response-chan offer-params#)]
           (async/put! offer-response-chan# :channel-not-found))))))

(defn offer-instance-go
  "Sends a rpc to the proxy state to offer the lock on the given instance."
  [populate-maintainer-chan! service-id offer-params]
  (async/go
    (try
      (offer-instance! populate-maintainer-chan! service-id offer-params)
      (catch Exception e
        (log/error e "Error while offering instance to service" {:service-id service-id, :offer-params offer-params})))))

;; Query Service State
(defmacro query-maintainer-channel-map!
  "Sends a rpc to retrieve the channel on which to query the state of the given service."
  [populate-maintainer-chan! service-id response-chan query-type]
  `(->> {:cid (cid/get-correlation-id)
         :method ~query-type
         :response-chan ~response-chan
         :service-id ~service-id}
        (~populate-maintainer-chan!)))

(defmacro query-maintainer-channel-map-with-timeout!
  "Sends a rpc to retrieve the channel on which to query the state of the given service."
  [populate-maintainer-chan! service-id timeout-ms query-type]
  `(let [response-chan# (async/promise-chan)]
     (query-maintainer-channel-map! ~populate-maintainer-chan! ~service-id response-chan# ~query-type)
     (async/alt!
       response-chan# ([result-channel#] result-channel#)
       (async/timeout ~timeout-ms) ([ignore#] {:message "Request timed-out!"})
       :priority true)))

(defmacro query-instance!
  "Sends a rpc to the router state to query the state of the given service.
   Throws an exception if a query channel cannot be found for the specfied service."
  [populate-maintainer-chan! service-id response-chan]
  `(let [response-chan# (async/promise-chan)]
     (query-maintainer-channel-map! ~populate-maintainer-chan! ~service-id response-chan# :query-state)
     (if-let [query-state-chan# (async/<! response-chan#)]
       (when-not (au/offer! query-state-chan# {:cid (cid/get-correlation-id)
                                               :response-chan ~response-chan
                                               :service-id ~service-id})
         (throw (ex-info "Unable to put instance on query-state-chan for service"
                         {:service-id ~service-id})))
       (do
         (log/error "Unable to find query-state-chan for service" ~service-id)
         (throw (ex-info "Unable to find query-state-chan for service"
                         {:service-id ~service-id}))))))

(defn query-instance-go
  "Sends a rpc to the router state to query the state of the given service."
  [populate-maintainer-chan! service-id response-chan]
  (async/go
    (try
      (query-instance! populate-maintainer-chan! service-id response-chan)
      (catch Exception e
        (log/error e "Error while querying state of" service-id)))))

;; Reserve and Release Instances
(defmacro release-instance!
  "Sends a rpc to the router state to release the lock on the given instance.
   Throws an exception if a release channel cannot be found for the specified service."
  [populate-maintainer-chan! instance reservation-result]
  `(let [response-chan# (async/promise-chan)
         service-id# (scheduler/instance->service-id ~instance)]
     (->> {:cid (cid/get-correlation-id)
           :method :release
           :response-chan response-chan#
           :service-id service-id#}
          (~populate-maintainer-chan!))
     (if-let [release-chan# (async/<! response-chan#)]
       (when-not (au/offer! release-chan# [~instance ~reservation-result])
         (throw (ex-info "Unable to put instance on release-chan."
                         {:instance ~instance})))
       (do
         (throw (ex-info "Unable to find release-chan."
                         {:instance ~instance :service-id service-id#}))))))

(defn release-instance-go
  "Sends a rpc to the router state to release the lock on the given instance."
  [populate-maintainer-chan! instance reservation-result]
  (async/go
    (try
      (release-instance! populate-maintainer-chan! instance reservation-result)
      (catch Exception e
        (log/error e "Error while releasing instance" instance)))))

(defmacro notify-scaling-state!
  "Sends a rpc to the router state to notify the scaling mode of a service.
   Throws an exception if a release channel cannot be found for the specified service."
  [populate-maintainer-chan! service-id scaling-state]
  `(let [response-chan# (async/promise-chan)
         service-id# ~service-id
         scaling-state# ~scaling-state]
     (->> {:cid (cid/get-correlation-id)
           :method :scaling-state
           :response-chan response-chan#
           :service-id service-id#}
       (~populate-maintainer-chan!))
     (if-let [release-chan# (async/<! response-chan#)]
       (when-not (au/offer! release-chan# {:scaling-state scaling-state#})
         (throw (ex-info "Unable to put scaling-state on release-chan."
                         {:scaling-state scaling-state# :service-id service-id#})))
       (do
         (throw (ex-info "Unable to find release-chan."
                         {:scaling-state scaling-state# :service-id service-id#}))))))

(defn notify-scaling-state-go
  "Sends a rpc to the router state to notify the scaling mode of a service."
  [populate-maintainer-chan! service-id scaling-state]
  (async/go
    (try
      (notify-scaling-state! populate-maintainer-chan! service-id scaling-state)
      (catch Exception e
        (log/error e "Error while notifying scaling mode" service-id scaling-state)))))

(defmacro get-rand-inst
  "Requests a random instance from the service-chan-responder.

   Will return an instance if the service exists and an instance is available,
   will return nil if the service is unknown or no instances are available"
  [populate-maintainer-chan! service-id reason-map exclude-ids-set timeout-in-millis]
  `(timers/start-stop-time!
     (metrics/service-timer ~service-id "get-task")
     (let [response-chan# (async/promise-chan)
           instance-resp-chan# (async/chan)
           method# (case (:reason ~reason-map)
                     :serve-request :reserve
                     :work-stealing :reserve
                     :kill-instance :kill)]
       ;;TODO: handle back pressure
       (->> {:cid (cid/get-correlation-id)
             :method method#
             :response-chan response-chan#
             :service-id ~service-id}
            (~populate-maintainer-chan!))
       (when-let [service-chan# (async/<! response-chan#)]
         (log/debug "found reservation channel for" ~service-id)
         (timers/start-stop-time!
           (metrics/service-timer ~service-id "reserve-instance")
           (when-not (au/offer! service-chan# [~reason-map instance-resp-chan# ~exclude-ids-set ~timeout-in-millis])
             (throw (ex-info "Unable to request an instance."
                             {:status http-503-service-unavailable
                              :service-id ~service-id
                              :reason-map ~reason-map})))
           (async/<! instance-resp-chan#))))))

(defn get-available-instance
  "Starts a `clojure.core.async/go` block to query the router state to get an
   available instance to send a request. It will continue to query the state until
   an instance is available."
  [populate-maintainer-chan! service-id {:keys [cid] :as reason-map} service-not-found-fn queue-timeout-ms metric-group]
  (async/go
    (try
      (counters/inc! (metrics/service-counter service-id "request-counts" "waiting-for-available-instance"))
      (statsd/gauge-delta! metric-group "request_waiting_for_instance" +1)
      (let [expiry-time (t/plus (t/now) (t/millis queue-timeout-ms))]
        (loop [iterations 1]
          (let [instance (get-rand-inst populate-maintainer-chan! service-id reason-map #{} queue-timeout-ms)]
            (if-not (nil? (:id instance)) ; instance is nil or :no-matching-instance-found
              (do
                (histograms/update!
                  (metrics/service-histogram service-id "iterations-to-find-available-instance")
                  iterations)
                instance)
              (if (and instance (not= instance :no-matching-instance-found))
                ; instance is a deployment error if it (1) does not have an :id tag, (2) is not nil, and (3) does not equal :no-matching-instance-found
                (let [{:keys [service-deployment-error-details service-deployment-error-msg]} instance
                      {:keys [error-map error-message]}
                      (cond->
                        {:error-map {:log-level :info
                                     :service-id service-id
                                     :status http-503-service-unavailable}
                         :error-message (utils/message instance)}
                        (and service-deployment-error-msg service-deployment-error-details)
                        (-> (assoc :error-message service-deployment-error-msg)
                            (update :error-map #(merge service-deployment-error-details %))))]
                  (ex-info (str deployment-error-prefix error-message) error-map))
                (if-not (t/before? (t/now) expiry-time)
                  (do
                    ;; No instances were started in a reasonable amount of time
                    (meters/mark! (metrics/service-meter service-id "no-available-instance-timeout"))
                    (statsd/inc! metric-group "no_instance_timeout")
                    (let [outstanding-requests (counters/value (metrics/service-counter service-id "request-counts" "outstanding"))
                          requests-waiting-to-stream (counters/value (metrics/service-counter service-id "request-counts" "waiting-to-stream"))
                          waiting-for-available-instance (counters/value (metrics/service-counter service-id "request-counts" "waiting-for-available-instance"))
                          healthy-instances (counters/value (metrics/service-counter service-id "instance-counts" "healthy"))
                          unhealthy-instances (counters/value (metrics/service-counter service-id "instance-counts" "unhealthy"))
                          failed-instances (counters/value (metrics/service-counter service-id "instance-counts" "failed"))]
                      (ex-info (str "After " (t/in-seconds (t/millis queue-timeout-ms))
                                    " seconds, no instance available to handle request."
                                    (when (and (zero? healthy-instances) (or (pos? unhealthy-instances) (pos? failed-instances)))
                                      " Check that your service is able to start properly!")
                                    (when (and (pos? outstanding-requests) (pos? healthy-instances))
                                      " Check that your service is able to scale properly!"))
                               {:service-id service-id
                                :outstanding-requests outstanding-requests
                                :requests-waiting-to-stream requests-waiting-to-stream
                                :waiting-for-available-instance waiting-for-available-instance
                                :slots-assigned (counters/value (metrics/service-counter service-id "instance-counts" "slots-assigned"))
                                :slots-available (counters/value (metrics/service-counter service-id "instance-counts" "slots-available"))
                                :slots-in-use (counters/value (metrics/service-counter service-id "instance-counts" "slots-in-use"))
                                :work-stealing-offers-received (counters/value (metrics/service-counter service-id "work-stealing" "received-from" "in-flight"))
                                :work-stealing-offers-sent (counters/value (metrics/service-counter service-id "work-stealing" "sent-to" "in-flight"))
                                :status http-503-service-unavailable})))
                  (do
                    (cid/with-correlation-id cid (service-not-found-fn))
                    (async/<! (async/timeout 1500))
                    (recur (inc iterations)))))))))
      (catch Throwable e
        (cid/cerror cid e "Error in get-available-instance")
        e)
      (finally
        (counters/dec! (metrics/service-counter service-id "request-counts" "waiting-for-available-instance"))
        (statsd/gauge-delta! metric-group "request_waiting_for_instance" -1)))))

;; Create service helpers

(defn start-new-service
  "Sends a call to the scheduler to start a service with the descriptor.
   Cached to prevent too many duplicate requests going to the scheduler."
  [scheduler {:keys [service-id] :as descriptor} start-service-cache ^ExecutorService start-service-thread-pool
   & {:keys [pre-start-fn start-fn] :or {pre-start-fn nil, start-fn nil}}]
  (let [my-value (Object.)
        cache-value (cu/cache-get-or-load start-service-cache service-id
                                          (fn []
                                            (log/info "setting" service-id "to" my-value "in start service cache")
                                            my-value))]
    (if (identical? my-value cache-value)
      (let [correlation-id (cid/get-correlation-id)
            start-fn (or start-fn
                         (fn new-service-start-fn []
                           (try
                             (when pre-start-fn
                               (pre-start-fn))
                             (scheduler/create-service-if-new scheduler descriptor)
                             (catch Exception e
                               (log/warn e "error starting new service")))))]
        (.submit start-service-thread-pool
                 ^Runnable (fn [] (cid/with-correlation-id correlation-id (start-fn)))))
      (log/info service-id "has been started on another thread" cache-value))))

(defn resolve-service-status
  "Determines the service status at any point in time.
   A service can be one of the following states:
   - Starting: the service has no healthy instances and is starting one up,
   - Running: the service is running successfully with at least one healthy instance,
   - Failing: the service has instances failing to start,
   - Inactive: the service has no scheduled and running tasks."
  [deployment-error {:keys [healthy requested scheduled] :or {healthy 0 requested 0 scheduled 0}}]
  (cond
    deployment-error :service-state-failing
    (and (zero? requested) (zero? scheduled)) :service-state-inactive
    (zero? healthy) :service-state-starting
    :else :service-state-running))

(defn retrieve-service-status-label
  "Returns the status of the specified service."
  [service-id {:keys [service-id->deployment-error service-id->instance-counts]}]
  (let [deployment-error (get service-id->deployment-error service-id)
        instance-counts (get service-id->instance-counts service-id)]
    (utils/message (resolve-service-status deployment-error instance-counts))))
