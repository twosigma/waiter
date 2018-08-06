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
(ns waiter.descriptor
  (:require [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [try-let :as tl]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.middleware :as middleware]
            [waiter.service-description :as sd]
            [waiter.token :as token]
            [waiter.util.async-utils :as au]
            [waiter.util.utils :as utils]))

(def instability-state-atom (atom {:instability-service-ids->replacement {}}))

(defn service-exists?
  "Returns true if the requested service-id exists as per the state in the fallback-state."
  [fallback-state service-id]
  (-> fallback-state :available-service-ids (contains? service-id)))

(defn service-healthy?
  "Returns true if the requested service-id has a healthy instance as per the state in the fallback-state."
  [fallback-state service-id]
  (-> fallback-state :healthy-service-ids (contains? service-id)))

(defn service-instable?
  "Returns true if the requested service-id exists as per the state in the instable-state."
  [instable-state service-id]
  (-> instable-state :instability-service-ids (contains? service-id)))

(defn missing-run-as-user?
  "Returns true if the exception is due to a missing run-as-user validation on the service description."
  [exception]
  (let [{:keys [issue type x-waiter-headers]} (ex-data exception)]
    (and (= :service-description-error type)
         (map? issue)
         (= 1 (count issue))
         (= "missing-required-key" (str (get issue "run-as-user")))
         (-> (keys x-waiter-headers)
             (set)
             (set/intersection sd/on-the-fly-service-description-keys)
             (empty?)))))

(defn wrap-descriptor
  "Adds the descriptor to the request/response.
  Redirects users in the case of missing user/run-as-requestor."
  [handler request->descriptor-fn start-new-service-fn fallback-state-atom]
  (fn [request]
    (tl/try-let [request-descriptor (request->descriptor-fn request)]
      (let [{:keys [descriptor latest-descriptor]} request-descriptor
            fallback-service-id (:service-id descriptor)
            latest-service-id (:service-id latest-descriptor)
            handler (middleware/wrap-merge handler {:descriptor descriptor :latest-service-id latest-service-id})]
        (when (not= latest-service-id fallback-service-id)
          (counters/inc! (metrics/service-counter latest-service-id "request-counts" "fallback" "source"))
          (counters/inc! (metrics/service-counter fallback-service-id "request-counts" "fallback" "target"))
          (when-not (service-exists? @fallback-state-atom latest-service-id)
            (log/info "starting" latest-service-id "before causing request to fallback to" fallback-service-id)
            (start-new-service-fn latest-descriptor)))
        (handler request))
      (catch Exception e
        (if (missing-run-as-user? e)
          (let [{:keys [query-string uri]} request
                location (str "/waiter-consent" uri (when (not (str/blank? query-string)) (str "?" query-string)))]
            (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "redirect"))
            (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "redirect"))
            {:headers {"location" location} :status 303})
          (do
            ; For consistency with historical data, count errors looking up the descriptor as a "process error"
            (meters/mark! (metrics/waiter-meter "core" "process-errors"))
            (utils/exception->response e request)))))))

(defn instability-maintainer
  "Long running daemon process that listens for scheduler state updates and triggers changes in the
   intstability state."
  [router-state-chan]
  (let [exit-chan (au/latest-chan)
        query-chan (async/chan 32)
        channels [exit-chan router-state-chan query-chan]]
    (async/go
      (loop [{:keys [instability-service-ids->replacement]
              :or {instability-service-ids->replacement {}} :as current-state}
             @instability-state-atom]
        (let [next-state
              (try
                (let [[message selected-chan] (async/alts! channels :priority true)]
                  (condp = selected-chan
                    exit-chan
                    (if (= :exit message)
                      (log/warn "stopping instability-maintainer")
                      (do
                        (log/info "received unknown message, not stopping instability-maintainer" {:message message})
                        current-state))

                    query-chan
                    (let [{:keys [response-chan service-id]} message]
                      (->> (if service-id
                             {:instable (contains? instability-service-ids->replacement service-id)
                              :has-replacement (not (= (get instability-service-ids->replacement service-id) nil))
                              :service-id service-id}
                             {:state current-state})
                           (async/>! response-chan))
                      current-state)

                    router-state-chan
                    (let [instable-service-ids (set (keys (message :service-id->instability-issue)))
                          instability-service-ids->replacement (apply array-map (mapcat (fn [x] [x nil]) instable-service-ids))
                          current-state' (merge-with set/union current-state
                                                     {:instability-service-ids->replacement instability-service-ids->replacement})]
                      (reset! instability-state-atom current-state')
                      current-state')))
                (catch Exception e
                  (log/error e "error in instability-maintainer")
                  current-state))]
          (when next-state
            (recur next-state)))))
    {:exit-chan exit-chan
     :query-chan query-chan}))

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

(defn descriptor->fallback-period-secs
  "Retrieves the fallback-period-secs for the given descriptor."
  [descriptor]
  (or (get-in descriptor [:waiter-headers "x-waiter-fallback-period-secs"])
      (get-in descriptor [:sources :fallback-period-secs] 0)))

(defn retrieve-fallback-descriptor
  "Computes the fallback descriptor with a healthy instance based on the provided descriptor.
   Fallback descriptors can only be computed for token-based descriptors.
   The amount of history lookup for fallback descriptor candidates is limited by search-history-length.
   Also, the fallback descriptor needs to be inside the fallback period to be returned."
  [descriptor->previous-descriptor search-history-length fallback-state request-time descriptor]
  (when (-> descriptor :sources :token-sequence seq)
    (let [{{:keys [token->token-data]} :sources} descriptor
          fallback-period-secs (descriptor->fallback-period-secs descriptor)]
      (when (and (pos? fallback-period-secs)
                 (let [most-recently-modified-token (sd/retrieve-most-recently-modified-token token->token-data)
                       token-last-update-time (get-in token->token-data [most-recently-modified-token "last-update-time"] 0)]
                   (->> (t/seconds fallback-period-secs)
                        (t/plus (tc/from-long token-last-update-time))
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

(defn compute-instability-replacement
  "Long running daemon process that computes replacements for instable services if they do not have one yet."
  [kv-store service-description-defaults metric-group-mappings]
  (let [nm (reduce (fn [m [service-id descriptor]]
                     (assoc m service-id (if (nil? descriptor) (sd/service-id->service-description kv-store service-id service-description-defaults metric-group-mappings) descriptor)))
                   {}
                   (@instability-state-atom :instability-service-ids->replacement))]
    (reset! instability-state-atom {:instability-service-ids->replacement nm})))

(defn retrieve-instability-descriptor
  "Computer the instability descriptor with 10% greater memory based on the provided descriptor."
  [descriptor]
  (let [add-mem (int (* 0.1 (get ((descriptor :sources) :headers) "mem")))]
    (update-in descriptor [:sources :headers "mem"] + add-mem)))

(defn resolve-descriptor
  "Resolves the descriptor that should be used based on available healthy services.
   Returns the provided latest-descriptor if it references a service with healthy instances or no valid fallback
   descriptor can be found (i.e. no descriptor in the search history which has healthy instances).
   Resolves to a different fallback descriptor which describes a services with healthy instances when the latest descriptor
   represents a service that has not started or does not have healthy instances."
  [descriptor->previous-descriptor search-history-length request-time fallback-state instability-state latest-descriptor]
  (let [{:keys [service-id]} latest-descriptor]
    (if (service-instable? instability-state service-id)
      (or (retrieve-instability-descriptor latest-descriptor)
          (do
            (log/info "no instability replacement service found for" (:service-id latest-descriptor))
            latest-descriptor))
      (if (service-healthy? fallback-state service-id)
        latest-descriptor
        (or (retrieve-fallback-descriptor
              descriptor->previous-descriptor search-history-length fallback-state request-time latest-descriptor)
            (do
              (log/info "no fallback service found for" (:service-id latest-descriptor))
              latest-descriptor))))))

(defn request-authorized?
  "Takes the request w/ kerberos auth info & the app headers, and returns true if the user is allowed to use "
  [user permitted-user]
  (log/debug "validating:" (str "permitted=" permitted-user) (str "actual=" user))
  (or (= token/ANY-USER permitted-user)
      (= ":any" (str permitted-user)) ; support ":any" for backwards compatibility
      (and (not (nil? permitted-user)) (= user permitted-user))))

(defn compute-descriptor
  "Creates the service descriptor from the request.
   The result map contains the following elements:
   {:keys [waiter-headers passthrough-headers sources service-id service-description core-service-description suspended-state]}"
  [service-description-defaults token-defaults service-id-prefix kv-store waiter-hostnames request metric-group-mappings
   service-description-builder assoc-run-as-user-approved?]
  (let [current-request-user (get request :authorization/user)]
    (-> (headers/split-headers (:headers request))
        (sd/merge-service-description-sources kv-store waiter-hostnames service-description-defaults token-defaults)
        (sd/merge-service-description-and-id kv-store service-id-prefix current-request-user metric-group-mappings
                                             service-description-builder assoc-run-as-user-approved?)
        (sd/merge-suspended kv-store))))

(defn descriptor->previous-descriptor
  "Creates the service descriptor from the request.
   The result map contains the following elements:
   {:keys [waiter-headers passthrough-headers sources service-id service-description core-service-description suspended-state]}"
  [kv-store service-id-prefix token-defaults metric-group-mappings service-description-builder service-approved? username
   {:keys [sources] :as descriptor}]
  (when-let [token-sequence (-> sources :token-sequence seq)]
    (let [{:keys [token->token-data]} sources
          previous-token (->> token->token-data
                              (pc/map-vals (fn [token-data] (get token-data "previous")))
                              sd/retrieve-most-recently-modified-token)
          previous-token-data (get-in token->token-data [previous-token "previous"])]
      (when (seq previous-token-data)
        (let [new-sources (->> (assoc token->token-data previous-token previous-token-data)
                               (sd/compute-service-description-template-from-tokens token-defaults token-sequence)
                               (merge sources))]
          (-> (select-keys descriptor [:passthrough-headers :waiter-headers])
              (assoc :sources new-sources)
              (sd/merge-service-description-and-id
                kv-store service-id-prefix username metric-group-mappings service-description-builder service-approved?)
              (sd/merge-suspended kv-store)))))))

(let [request->descriptor-timer (metrics/waiter-timer "core" "request->descriptor")]
  (defn request->descriptor
    "Extract the service descriptor from a request.
     It also performs the necessary authorization."
    [assoc-run-as-user-approved? can-run-as? fallback-state-atom kv-store metric-group-mappings
     search-history-length service-description-builder service-description-defaults service-id-prefix token-defaults
     waiter-hostnames {:keys [request-time] :as request}]
    (timers/start-stop-time!
      request->descriptor-timer
      (let [auth-user (:authorization/user request)
            service-approved? (fn service-approved? [service-id] (assoc-run-as-user-approved? request service-id))
            latest-descriptor (compute-descriptor
                                service-description-defaults token-defaults service-id-prefix kv-store waiter-hostnames
                                request metric-group-mappings service-description-builder service-approved?)
            descriptor->previous-descriptor
            (fn descriptor->previous-descriptor-fn
              [descriptor]
              (descriptor->previous-descriptor
                kv-store service-id-prefix token-defaults metric-group-mappings service-description-builder
                service-approved? auth-user descriptor))
            fallback-state @fallback-state-atom
            instability-state @instability-state-atom
            descriptor (resolve-descriptor
                         descriptor->previous-descriptor search-history-length request-time fallback-state instability-state latest-descriptor)
            {:keys [service-authentication-disabled service-description service-preauthorized]} descriptor
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
        {:descriptor descriptor
         :latest-descriptor latest-descriptor}))))
