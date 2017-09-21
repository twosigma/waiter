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
(ns waiter.service
  (:require [clj-time.core :as t]
            [clojure.core.cache :as cache]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [waiter.async-utils :as au]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.statsd :as statsd]
            [waiter.utils :as utils])
  (:import java.util.concurrent.ExecutorService))

(def ^:const status-check-path "/status")

(defn extract-health-check-url
  "Extract the health-check-url from App Info"
  [app-info default-url]
  (let [health-check-data-list (get-in app-info [:app :healthChecks])
        health-check-data (first health-check-data-list)]
    (:path health-check-data default-url)))

(defn annotate-tasks-with-health-url
  "Introduce the health-check-url into the task metadata"
  [app-info]
  (let [health-check-url (extract-health-check-url app-info status-check-path)
        map-assoc-health-check-func (fn [x] (map #(assoc % :health-check-url health-check-url) x))]
    (update-in app-info [:app :tasks] map-assoc-health-check-func)))

;;; Service instance blacklisting, work-stealing, access and creation

;; Attempt to blacklist instances
(defmacro blacklist-instance!
  "Sends a rpc to the router state to blacklist the given instance.
   Throws an exception if a blacklist channel cannot be found for the specfied service."
  [instance-rpc-chan service-id instance-id blacklist-period-ms response-chan]
  `(let [chan-resp-chan# (async/chan)]
     (log/info "Requesting blacklist channel for" ~service-id)
     (async/put! ~instance-rpc-chan [:blacklist ~service-id (cid/get-correlation-id) chan-resp-chan#])
     (if-let [blacklist-chan# (async/<! chan-resp-chan#)]
       (do
         (log/info "Received blacklist channel, making blacklist request.")
         (when-not (au/offer! blacklist-chan# [{:instance-id ~instance-id
                                                :blacklist-period-ms ~blacklist-period-ms
                                                :cid (cid/get-correlation-id)}
                                               ~response-chan])
           (throw (ex-info "Unable to put instance-id on blacklist chan."
                           {:instance-id ~instance-id, :service-id ~service-id}))))
       (do
         (log/error "Unable to find blacklist chan for service" ~service-id)
         (throw (ex-info "Service not found" {:instance-id ~instance-id
                                              :service-id ~service-id
                                              :status 400}))))))

(defn blacklist-instance-go
  "Sends a rpc to the router state to blacklist the lock on the given instance."
  [instance-rpc-chan service-id instance-id blacklist-period-ms response-chan]
  (async/go
    (try
      (blacklist-instance! instance-rpc-chan service-id instance-id blacklist-period-ms response-chan)
      (catch Exception e
        (log/error e "Error while blacklisting instance" instance-id)))))

;; Offer instances obtained via work-stealing mechanism
(defmacro offer-instance!
  "Sends a rpc to the proxy state to offer the given instance.
   Throws an exception if a work-stealing channel cannot be found for the specfied service."
  [instance-rpc-chan service-id offer-params]
  `(let [chan-resp-chan# (async/chan)]
     (log/debug "Requesting offer channel for" ~service-id)
     (async/put! ~instance-rpc-chan [:offer ~service-id (cid/get-correlation-id) chan-resp-chan#])
     (if-let [work-stealing-chan# (async/<! chan-resp-chan#)]
       (do
         (log/info "Received offer channel, making offer request.")
         (when-not (au/offer! work-stealing-chan# ~offer-params)
           (throw (ex-info "Unable to put instance on work-stealing-chan."
                           {:offer-params ~offer-params, :service-id ~service-id}))))
       (do
         (log/error "Unable to find work-stealing-chan for service" ~service-id)
         (throw (ex-info "Unable to find work-stealing-chan."
                         {:offer-params ~offer-params, :service-id ~service-id}))))))

(defn offer-instance-go
  "Sends a rpc to the proxy state to offer the lock on the given instance."
  [instance-rpc-chan service-id offer-params]
  (async/go
    (try
      (offer-instance! instance-rpc-chan service-id offer-params)
      (catch Exception e
        (log/error e "Error while offering instance to service" {:service-id service-id, :offer-params offer-params})))))

;; Query Service State
(defmacro query-maintainer-channel-map!
  "Sends a rpc to retrieve the channel on which to query the state of the given service."
  [instance-rpc-chan service-id response-chan query-type]
  `(async/put! ~instance-rpc-chan [~query-type ~service-id (cid/get-correlation-id) ~response-chan]))

(defmacro query-maintainer-channel-map-with-timeout!
  "Sends a rpc to retrieve the channel on which to query the state of the given service."
  [instance-rpc-chan service-id timeout-ms query-type]
  `(let [chan-resp-chan# (async/chan)]
     (query-maintainer-channel-map! ~instance-rpc-chan ~service-id chan-resp-chan# ~query-type)
     (async/alt!
       chan-resp-chan# ([result-channel#] result-channel#)
       (async/timeout ~timeout-ms) ([ignore#] {:message "Request timed-out!"})
       :priority true)))

(defmacro query-instance!
  "Sends a rpc to the router state to query the state of the given service.
   Throws an exception if a query channel cannot be found for the specfied service."
  [instance-rpc-chan service-id response-chan]
  `(let [chan-resp-chan# (async/chan)]
     (query-maintainer-channel-map! ~instance-rpc-chan ~service-id chan-resp-chan# :query-state)
     (if-let [query-state-chan# (async/<! chan-resp-chan#)]
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
  [instance-rpc-chan service-id response-chan]
  (async/go
    (try
      (query-instance! instance-rpc-chan service-id response-chan)
      (catch Exception e
        (log/error e "Error while querying state of" service-id)))))

;; Reserve and Release Instances
(defmacro release-instance!
  "Sends a rpc to the router state to release the lock on the given instance.
   Throws an exception if a release channel cannot be found for the specfied service."
  [instance-rpc-chan instance reservation-result]
  `(let [chan-resp-chan# (async/chan)
         service-id# (scheduler/instance->service-id ~instance)]
     (async/put! ~instance-rpc-chan [:release service-id# (cid/get-correlation-id) chan-resp-chan#])
     (if-let [release-chan# (async/<! chan-resp-chan#)]
       (when-not (au/offer! release-chan# [~instance ~reservation-result])
         (throw (ex-info "Unable to put instance on release-chan."
                         {:instance ~instance})))
       (do
         (log/error "Unable to find release-chan for service" service-id#)
         (throw (ex-info "Unable to find release-chan."
                         {:instance ~instance}))))))

(defn release-instance-go
  "Sends a rpc to the router state to release the lock on the given instance."
  [instance-rpc-chan instance reservation-result]
  (async/go
    (try
      (release-instance! instance-rpc-chan instance reservation-result)
      (catch Exception e
        (log/error e "Error while releasing instance" instance)))))

(defmacro get-rand-inst
  "Requests a random instance from the service-chan-responder.

   Will return an instance if the service exists and an instance is available,
   will return nil if the service is unknown or no instances are available"
  [instance-rpc-chan service-id reason-map exclude-ids-set timeout-in-millis]
  `(timers/start-stop-time!
     (metrics/service-timer ~service-id "get-task")
     (let [chan-resp-chan# (async/chan)
           instance-resp-chan# (async/chan)
           method# (case (:reason ~reason-map)
                     :serve-request :reserve
                     :work-stealing :reserve
                     :kill-instance :kill)]
       ;;TODO: handle back pressure
       (async/put! ~instance-rpc-chan [method# ~service-id (cid/get-correlation-id) chan-resp-chan#])
       (when-let [service-chan# (async/<! chan-resp-chan#)]
         (log/debug "found reservation channel for" ~service-id)
         (timers/start-stop-time!
           (metrics/service-timer ~service-id "reserve-instance")
           (when-not (au/offer! service-chan# [~reason-map instance-resp-chan# ~exclude-ids-set ~timeout-in-millis])
             (throw (ex-info "Unable to request an instance."
                             {:status 503
                              :service-id ~service-id
                              :reason-map ~reason-map})))
           (async/<! instance-resp-chan#))))))

(defn get-available-instance
  "Starts a `clojure.core.async/go` block to query the router state to get an
   available instance to send a request. It will continue to query the state until
   an instance is available."
  [instance-rpc-chan service-id reason-map app-not-found-fn queue-timeout-ms metric-group add-debug-header-into-response!]
  (async/go
    (cid/with-correlation-id
      (:cid reason-map)
      (try
        (metrics/with-timer!
          (metrics/service-timer service-id "get-available-instance")
          (fn [nanos]
            (add-debug-header-into-response! "X-Waiter-Get-Available-Instance-ns" nanos)
            (statsd/histo! metric-group "get_instance" nanos))
          (counters/inc! (metrics/service-counter service-id "request-counts" "waiting-for-available-instance"))
          (statsd/gauge-delta! metric-group "request_waiting_for_instance" +1)
          (let [expiry-time (t/plus (t/now) (t/millis queue-timeout-ms))]
            (loop [iterations 1]
              (let [instance (get-rand-inst instance-rpc-chan service-id reason-map #{} queue-timeout-ms)]
                (if-not (nil? (:id instance)) ; instance is nil or :no-matching-instance-found
                  (do
                    (histograms/update!
                      (metrics/service-histogram service-id "iterations-to-find-available-instance")
                      iterations)
                    instance)
                  (if (and instance (not= instance :no-matching-instance-found))
                    ; instance is a deployment error if it (1) does not have an :id tag, (2) is not nil, and (3) does not equal :no-matching-instance-found
                    (ex-info (str "Deployment error: " (utils/message instance)) {:service-id service-id :status 503})
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
                                    :status 503})))
                      (do
                        (app-not-found-fn)
                        (async/<! (async/timeout 1500))
                        (recur (inc iterations))))))))))
        (catch Exception e
          (log/error e "Error in get-available-instance")
          e)
        (finally
          (counters/dec! (metrics/service-counter service-id "request-counts" "waiting-for-available-instance"))
          (statsd/gauge-delta! metric-group "request_waiting_for_instance" -1))))))

;; Create service helpers

(defn start-new-service
  "Sends a call to the scheduler to start an app with the descriptor.
   Cached to prevent too many duplicate requests going to the scheduler."
  [scheduler service-id->password-fn descriptor cache-atom ^ExecutorService start-app-threadpool
   & {:keys [pre-start-fn start-fn] :or {pre-start-fn nil, start-fn nil}}]
  (let [cache-key (:service-id descriptor)]
    (when-not (cache/has? @cache-atom cache-key)
      (let [my-value (Object.)
            cache (swap! cache-atom
                         (fn [c]
                           (if (cache/has? c cache-key)
                             (cache/hit c cache-key)
                             (cache/miss c cache-key my-value))))
            cache-value (cache/lookup cache cache-key)]
        (when (identical? my-value cache-value)
          (let [correlation-id (cid/get-correlation-id)
                start-fn (or start-fn
                             (fn []
                               (try
                                 (when pre-start-fn
                                   (pre-start-fn))
                                 (scheduler/create-app-if-new scheduler service-id->password-fn descriptor)
                                 (catch Exception e
                                   (log/warn e "Error starting new app")))))]
            (.submit start-app-threadpool
                     ^Runnable (fn [] (cid/with-correlation-id correlation-id (start-fn))))))))))
