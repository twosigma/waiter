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
(ns waiter.descriptor
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.timers :as timers]
            [waiter.metrics :as metrics]
            [waiter.middleware :as middleware]
            [waiter.service-description :as sd]
            [waiter.token :as token]
            [waiter.util.async-utils :as au])
  (:import [org.joda.time DateTime]))

(defn fallback-maintainer
  "Long running daemon process that listens for scheduler state updates and triggers changes in the
   fallback state. It also responds to queries for the fallback state."
  [scheduler-state-chan fallback-state-atom]
  (let [exit-chan (au/latest-chan)
        query-chan (async/chan 32)
        channels [exit-chan scheduler-state-chan query-chan]]
    (async/go
      (loop [{:keys [available-service-ids healthy-service-ids]
              :or {available-service-ids #{}
                   healthy-service-ids #{}} :as current-state}
             @fallback-state-atom]
        (let [next-state
              (try
                (let [[message selected-chan] (async/alts! channels :priority true)]
                  (condp = selected-chan
                    exit-chan
                    (if (= :exit message)
                      (log/warn "stopping fallback-maintainer")
                      (do
                        (log/info "received unknown message, not stopping fallback-maintainer" {:message message})
                        current-state))

                    query-chan
                    (let [{:keys [response-chan service-id]} message]
                      (->> (if service-id
                             {:available (contains? available-service-ids service-id)
                              :healthy (contains? healthy-service-ids service-id)
                              :service-id service-id}
                             {:state current-state})
                           (async/>! response-chan))
                      current-state)

                    scheduler-state-chan
                    (let [{:keys [available-service-ids healthy-service-ids]}
                          (some (fn [[message-type message-data]]
                                  (when (= :update-available-services message-type)
                                    message-data))
                                message)
                          current-state' (assoc current-state
                                           :available-service-ids available-service-ids
                                           :healthy-service-ids healthy-service-ids)]
                      (reset! fallback-state-atom current-state')
                      current-state')))
                (catch Exception e
                  (log/error e "error in fallback-maintainer")
                  current-state))]
          (when next-state
            (recur next-state)))))
    {:exit-chan exit-chan
     :query-chan query-chan}))

(defn service-exists?
  "Returns true if the requested service-id exists as per the state in the fallback-state."
  [fallback-state service-id]
  (-> fallback-state :available-service-ids (contains? service-id)))

(defn service-healthy?
  "Returns true if the requested service-id has a healthy instance as per the state in the fallback-state."
  [fallback-state service-id]
  (-> fallback-state :healthy-service-ids (contains? service-id)))

(defn descriptor->service-fallback-period-secs
  "Retrieves the service-fallback-period-secs for the given descriptor."
  [descriptor]
  (or (get-in descriptor [:waiter-headers "x-waiter-service-fallback-period-secs"])
      (get-in descriptor [:sources :service-fallback-period-secs] 0)))

(defn retrieve-fallback-descriptor
  "Computes the fallback descriptor with a healthy instance based on the provided descriptor.
   Fallback descriptors can only be computed for token-based descriptors.
   The amount of history lookup for fallback descriptor candidates is limited by search-history-length.
   Also, the fallback descriptor needs to be inside the fallback period to be returned."
  [descriptor->previous-descriptor search-history-length fallback-state
   request-time descriptor]
  (when (-> descriptor :sources :token-sequence seq)
    (let [{{:keys [token->token-data]} :sources} descriptor
          service-fallback-period-secs (descriptor->service-fallback-period-secs descriptor)]
      (when (and (pos? service-fallback-period-secs)
                 (let [most-recently-modified-token (sd/retrieve-most-recently-modified-token token->token-data)
                       token-last-update-time (get-in token->token-data [most-recently-modified-token "last-update-time"] 0)]
                   (->> (t/seconds service-fallback-period-secs)
                        (t/plus (DateTime. token-last-update-time))
                        (t/before? request-time))))
        (loop [iteration 1
               loop-descriptor descriptor]
          (when (<= iteration search-history-length)
            (when-let [previous-descriptor (descriptor->previous-descriptor loop-descriptor)]
              (let [{:keys [service-id]} previous-descriptor]
                (if (service-healthy? fallback-state service-id)
                  (do
                    (log/info (str "iteration-" iteration) (:service-id descriptor) "falling back to" service-id)
                    previous-descriptor)
                  (do
                    (log/debug (str "iteration-" iteration) "skipping" service-id "as the fallback service"
                               {:available (service-exists? fallback-state service-id)
                                :healthy (service-healthy? fallback-state service-id)})
                    (recur (inc iteration) previous-descriptor)))))))))))

(defn wrap-fallback
  "Redirects users to a healthy fallback service when the current service has not started or does not have healthy instances."
  [handler descriptor->previous-descriptor-fn start-new-service-fn assoc-run-as-user-approved?
   search-history-length fallback-state-atom]
  (fn wrap-fallback-handler [{:keys [descriptor request-time] :as request}]
    (let [{:keys [service-id]} descriptor
          fallback-state @fallback-state-atom
          handler (middleware/wrap-assoc handler :latest-service-id service-id)]
      (if (service-healthy? fallback-state service-id)
        (handler request)
        (let [auth-user (:authorization/user request)
              service-approved? (fn service-approved? [service-id] (assoc-run-as-user-approved? request service-id))
              descriptor->previous-descriptor (fn [descriptor] (descriptor->previous-descriptor-fn service-approved? auth-user descriptor))]
          (if-let [fallback-descriptor (retrieve-fallback-descriptor
                                         descriptor->previous-descriptor search-history-length fallback-state request-time descriptor)]
            (let [fallback-service-id (:service-id fallback-descriptor)
                  new-handler (middleware/wrap-merge handler {:descriptor fallback-descriptor :latest-service-id service-id})]
              (when-not (service-exists? fallback-state service-id)
                (log/info "starting" service-id "before causing request to fallback to" fallback-service-id)
                (start-new-service-fn descriptor))
              (counters/inc! (metrics/service-counter service-id "request-counts" "fallback" "source"))
              (counters/inc! (metrics/service-counter fallback-service-id "request-counts" "fallback" "target"))
              (new-handler request))
            (do
              (log/info "no fallback service found for" service-id)
              (handler request))))))))

(defn request-authorized?
  "Takes the request w/ kerberos auth info & the app headers, and returns true if the user is allowed to use "
  [user permitted-user]
  (log/debug "validating:" (str "permitted=" permitted-user) (str "actual=" user))
  (or (= token/ANY-USER permitted-user)
      (= ":any" (str permitted-user)) ; support ":any" for backwards compatibility
      (and (not (nil? permitted-user)) (= user permitted-user))))

(let [request->descriptor-timer (metrics/waiter-timer "core" "request->descriptor")]
  (defn request->descriptor
    "Extract the service descriptor from a request.
     It also performs the necessary authorization."
    [service-description-defaults token-defaults service-id-prefix kv-store waiter-hostnames can-run-as?
     metric-group-mappings service-description-builder assoc-run-as-user-approved? request]
    (timers/start-stop-time!
      request->descriptor-timer
      (let [auth-user (:authorization/user request)
            service-approved? (fn service-approved? [service-id] (assoc-run-as-user-approved? request service-id))
            {:keys [service-authentication-disabled service-description service-preauthorized] :as descriptor}
            (sd/request->descriptor
              service-description-defaults token-defaults service-id-prefix kv-store waiter-hostnames
              request metric-group-mappings service-description-builder service-approved?)
            {:strs [run-as-user permitted-user]} service-description]
        (when-not (or service-authentication-disabled
                      service-preauthorized
                      (and auth-user (can-run-as? auth-user run-as-user)))
          (throw (ex-info "Authenticated user cannot run service"
                          {:authenticated-user auth-user
                           :run-as-user run-as-user
                           :status 403})))
        (when-not (request-authorized? auth-user permitted-user)
          (throw (ex-info "This user isn't allowed to invoke this service"
                          {:authenticated-user auth-user
                           :service-description service-description
                           :status 403})))
        descriptor))))
