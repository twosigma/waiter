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
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [full.async :refer [go-try]]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [try-let :as tl]
            [waiter.headers :as headers]
            [waiter.metrics :as metrics]
            [waiter.middleware :as middleware]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.token :as token]
            [waiter.util.async-utils :as au]
            [waiter.util.utils :as utils]))

(defn service-exists?
  "Returns true if the requested service-id exists as per the state in the fallback-state."
  [fallback-state service-id]
  (-> fallback-state :available-service-ids (contains? service-id)))

(defn service-healthy?
  "Returns true if the requested service-id has a healthy instance as per the state in the fallback-state."
  [fallback-state service-id]
  (-> fallback-state :healthy-service-ids (contains? service-id)))

(defn retrieve-fallback-state-for
  "Returns the fallback state for the provided service-ids."
  [{:keys [available-service-ids healthy-service-ids]} service-ids]
  {:available-service-ids (set/intersection available-service-ids service-ids)
   :healthy-service-ids (set/intersection healthy-service-ids service-ids)})

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
  [handler request->descriptor-fn service-invocation-authorized?  start-new-service-fn fallback-state-atom]
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
            (let [auth-user (:authorization/user request)]
              (if (service-invocation-authorized? auth-user latest-descriptor)
                (do
                  (log/info "starting" latest-service-id "before causing request to fallback to" fallback-service-id)
                  (start-new-service-fn latest-descriptor))
                (log/info "not starting" latest-service-id "as" auth-user "is not authorized to make a request to the service")))))
        (handler request))
      (catch Exception e
        (if (missing-run-as-user? e)
          (let [{:keys [query-string uri]} request
                location (str "/waiter-consent" uri (when-not (str/blank? query-string) (str "?" query-string)))]
            (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "redirect"))
            (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "redirect"))
            (log/info "redirecting to consent page as run-as-user is missing" (.getMessage e))
            (utils/attach-waiter-source {:headers {"location" location} :status http-303-see-other}))
          (do
            ; For consistency with historical data, count errors looking up the descriptor as a "process error"
            (meters/mark! (metrics/waiter-meter "core" "process-errors"))
            (utils/exception->response e request)))))))

(defn- log-service-changes
  "Logs changes to the tracker service ids."
  [new-service-ids old-service-ids qualifier]
  (when-let [old-ids-delta (seq (set/difference old-service-ids new-service-ids))]
    (log/info "no longer" qualifier "services:" old-ids-delta))
  (when-let [new-ids-delta (seq (set/difference new-service-ids old-service-ids))]
    (log/info "newly" qualifier "services:" new-ids-delta)))

(defn fallback-maintainer
  "Long running daemon process that listens for scheduler state updates and triggers changes in the
   fallback state. It also responds to queries for the fallback state."
  [router-state-chan fallback-state-atom]
  (let [exit-chan (au/latest-chan)
        query-chan (async/chan 32)
        channels [exit-chan router-state-chan query-chan]]
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
                      (async/>! response-chan
                                (if service-id
                                  {:available (contains? available-service-ids service-id)
                                   :healthy (contains? healthy-service-ids service-id)
                                   :service-id service-id}
                                  {:state current-state}))
                      current-state)

                    router-state-chan
                    (let [{:keys [all-available-service-ids service-id->healthy-instances]} message
                          healthy-service-ids' (->> service-id->healthy-instances
                                                    (filter #(-> % second seq))
                                                    (map first)
                                                    set)
                          current-state' (assoc current-state
                                           :available-service-ids all-available-service-ids
                                           :healthy-service-ids healthy-service-ids')]
                      (reset! fallback-state-atom current-state')
                      (log-service-changes all-available-service-ids available-service-ids "available")
                      (log-service-changes healthy-service-ids' healthy-service-ids "healthy")
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

(defn retrieve-most-recently-updated-component-entry
  "Retrieves the most recently updated component entry from the descriptor."
  [{:keys [component->previous-descriptor-fns] :as descriptor}]
  (apply
    max-key
    (fn [[_ {:keys [retrieve-last-update-time]}]]
      (retrieve-last-update-time descriptor))
    (seq component->previous-descriptor-fns)))

(defn retrieve-fallback-descriptor
  "Computes the fallback descriptor with a healthy instance based on the provided descriptor.
   Fallback descriptors can only be computed for token-based descriptors.
   The amount of history lookup for fallback descriptor candidates is limited by search-history-length.
   Also, the fallback descriptor needs to be inside the fallback period to be returned."
  [descriptor->previous-descriptor search-history-length fallback-state request-time
   {:keys [component->previous-descriptor-fns] :as descriptor}]
  (when (seq component->previous-descriptor-fns)
    (let [current-service-id (:service-id descriptor)
          fallback-period-secs (descriptor->fallback-period-secs descriptor)]
      (when (pos? fallback-period-secs)
        (let [[component {:keys [retrieve-last-update-time]}] (retrieve-most-recently-updated-component-entry descriptor)
              component-last-update-time (retrieve-last-update-time descriptor)]
          (if (->> (t/seconds fallback-period-secs)
                (t/plus (tc/from-long component-last-update-time))
                (t/before? request-time))
            (loop [iteration 1
                   loop-descriptor descriptor
                   attempted-service-ids #{}]
              (if (<= iteration search-history-length)
                (if-let [previous-descriptor (descriptor->previous-descriptor loop-descriptor)]
                  (let [{:keys [service-id]} previous-descriptor]
                    (if (service-healthy? fallback-state service-id)
                      (do
                        (log/info (str "iteration-" iteration) current-service-id "falling back to" service-id)
                        previous-descriptor)
                      (recur (inc iteration)
                             previous-descriptor
                             (conj attempted-service-ids service-id))))
                  (log/info "no fallback service found for" current-service-id "after entire history lookup"
                            {:attempted-service-ids attempted-service-ids
                             :fallback-state (retrieve-fallback-state-for fallback-state attempted-service-ids)}))
                (log/info "no fallback found for" current-service-id "after exhausting search history length"
                          {:attempted-service-ids attempted-service-ids
                           :fallback-state (retrieve-fallback-state-for fallback-state attempted-service-ids)})))
            (log/info "fallback period expired for" current-service-id
                      {:component component
                       :last-update-time component-last-update-time})))))))

(defn resolve-descriptor
  "Resolves the descriptor that should be used based on available healthy services.
   Returns the provided latest-descriptor if it references a service with healthy instances or no valid fallback
   descriptor can be found (i.e. no descriptor in the search history which has healthy instances).
   Resolves to a different fallback descriptor which describes a services with healthy instances when the latest descriptor
   represents a service that has not started or does not have healthy instances."
  [descriptor->previous-descriptor search-history-length request-time fallback-state latest-descriptor]
  (let [{:keys [service-id]} latest-descriptor]
    (if (service-healthy? fallback-state service-id)
      latest-descriptor
      (or (retrieve-fallback-descriptor
            descriptor->previous-descriptor search-history-length fallback-state request-time latest-descriptor)
          (do
            (log/info "no fallback service found for" (:service-id latest-descriptor))
            latest-descriptor)))))

(defn request-authorized?
  "Returns true if the user is allowed to use "
  [user permitted-user]
  (log/debug "validating:" (str "permitted=" permitted-user) (str "actual=" user))
  (or (= token/ANY-USER permitted-user)
      (= ":any" (str permitted-user)) ; support ":any" for backwards compatibility
      (and (not (nil? permitted-user)) (= user permitted-user))))

(defn- compute-token-source-previous-descriptor
  "Computes the previous descriptor using token sources."
  [attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id
   {:keys [component->previous-descriptor-fns sources] :as descriptor}]
  (when-let [token-sequence (-> sources :token-sequence seq)]
    (let [{:keys [token->token-data]} sources
          previous-token (->> token->token-data
                           (pc/map-vals (fn [token-data] (get token-data "previous")))
                           sd/retrieve-most-recently-modified-token)
          previous-token-data (get-in token->token-data [previous-token "previous"])]
      (when (seq previous-token-data)
        (let [new-sources (->> (assoc token->token-data previous-token previous-token-data)
                            (sd/compute-service-description-template-from-tokens
                              attach-service-defaults-fn attach-token-defaults-fn token-sequence)
                            (merge sources))
              token-previous-descriptor-fns (get component->previous-descriptor-fns :token)]
          (-> (select-keys descriptor [:passthrough-headers :waiter-headers])
            (assoc :component->previous-descriptor-fns {:token token-previous-descriptor-fns}
                   :sources new-sources)
            (build-service-description-and-id false)))))))

(defn attach-token-fallback-source
  "Attaches the helper functions map to retrieve previous descriptor using tokens into the
   [:component->previous-descriptor-fns :token] key in the provided descriptor.
   The map contains the following keys: :retrieve-last-update-time and :retrieve-previous-descriptor"
  [descriptor attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id]
  (cond-> descriptor
    (-> descriptor :sources :token-sequence seq)
    (assoc-in [:component->previous-descriptor-fns :token]
              {:retrieve-last-update-time sd/retrieve-most-recently-modified-token-update-time
               :retrieve-previous-descriptor (fn retrieve-most-recent-token-update-descriptor [descriptor]
                                               (compute-token-source-previous-descriptor
                                                 attach-service-defaults-fn attach-token-defaults-fn
                                                 build-service-description-and-id descriptor))})))

(defn compute-descriptor
  "Creates the service descriptor from the request.
   The result map contains the following elements:
   {:keys [waiter-headers passthrough-headers sources service-id service-description core-service-description suspended-state]}"
  [attach-service-defaults-fn attach-token-defaults-fn service-id-prefix kv-store waiter-hostnames request
   service-description-builder assoc-run-as-user-approved?]
  (let [current-request-user (get request :authorization/user)
        build-service-description-and-id-helper (sd/make-build-service-description-and-id-helper
                                                  kv-store service-id-prefix current-request-user
                                                  service-description-builder assoc-run-as-user-approved?)
        descriptor
        (-> (headers/split-headers (:headers request))
          (sd/merge-service-description-sources
            kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn)
          (attach-token-fallback-source
            attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper)
          (build-service-description-and-id-helper true))]
    (when-let [throwable (sd/validate-service-description kv-store service-description-builder descriptor)]
      (throw throwable))
    descriptor))

(defn descriptor->previous-descriptor
  "Creates a valid previous version of the descriptor from the provided descriptor.
   The result map contains the following elements:
   {:keys [component->previous-descriptor-fns core-service-description passthrough-headers
           service-id service-description sources suspended-state waiter-headers]}"
  [kv-store service-description-builder descriptor]
  (loop [{:keys [component->previous-descriptor-fns] :as descriptor} descriptor]
    (when-let [component-entry (and (seq component->previous-descriptor-fns)
                                    (retrieve-most-recently-updated-component-entry descriptor))]
      (let [component (key component-entry)
            {:keys [retrieve-previous-descriptor]} (val component-entry)]
        (if-let [previous-descriptor (retrieve-previous-descriptor descriptor)]
          (if (or (:error previous-descriptor)
                  (sd/validate-service-description kv-store service-description-builder previous-descriptor))
            (recur (dissoc previous-descriptor :error))
            (do
              (log/info (:service-id descriptor) "has previous descriptor with service-id"
                        (:service-id previous-descriptor) "computed using" component)
              previous-descriptor))
          (recur (utils/dissoc-in descriptor [:component->previous-descriptor-fns component])))))))

(defn perform-authorization-checks
  "Performs authorization checks on whether the currently authenticated user is allowed to invoke the service."
  [can-run-as? auth-user {:keys [service-authentication-disabled service-description service-id service-preauthorized]}]
  (let [{:strs [run-as-user permitted-user]} service-description]
    (when-not (or service-authentication-disabled
                  service-preauthorized
                  (and auth-user (can-run-as? auth-user run-as-user)))
      (throw (ex-info "Authenticated user cannot run service"
                      {:authenticated-user auth-user
                       :log-level :warn
                       :run-as-user run-as-user
                       :service-description service-description
                       :service-id service-id
                       :status http-403-forbidden})))
    (when-not (request-authorized? auth-user permitted-user)
      (throw (ex-info "This user isn't allowed to invoke this service"
                      {:authenticated-user auth-user
                       :log-level :warn
                       :service-description service-description
                       :service-id service-id
                       :status http-403-forbidden})))))

(defn service-invocation-authorized?
  "Predicate variant of the perform-authorization-checks function.
   Returns true if user is allowed to invoke the service, false otherwise."
  [can-run-as?-fn auth-user descriptor]
  (try
    (perform-authorization-checks can-run-as?-fn auth-user descriptor)
    true
    (catch Exception ex
      (let [{:keys [log-level] :or {log-level :info} :as error-data} (ex-data ex)
            error-message (ex-message ex)]
        (log/logp log-level error-message (select-keys error-data [:authenticated-user :service-id])))
      false)))

(let [request->descriptor-timer (metrics/waiter-timer "core" "request->descriptor")]
  (defn request->descriptor
    "Extract the service descriptor from a request.
     It also performs the necessary authorization."
    [assoc-run-as-user-approved? can-run-as? attach-service-defaults-fn attach-token-defaults-fn fallback-state-atom
     kv-store search-history-length service-description-builder service-id-prefix waiter-hostnames
     {:keys [request-time] :as request}]
    (timers/start-stop-time!
      request->descriptor-timer
      (let [auth-user (:authorization/user request)
            service-approved? (fn service-approved? [service-id] (assoc-run-as-user-approved? request service-id))
            latest-descriptor (compute-descriptor
                                attach-service-defaults-fn attach-token-defaults-fn service-id-prefix kv-store
                                waiter-hostnames request service-description-builder service-approved?)
            descriptor->previous-descriptor
            (fn descriptor->previous-descriptor-fn
              [descriptor]
              (descriptor->previous-descriptor kv-store service-description-builder descriptor))
            fallback-state @fallback-state-atom
            descriptor (resolve-descriptor
                         descriptor->previous-descriptor search-history-length request-time fallback-state latest-descriptor)]
        ;; authorization checks for both the latest service as well as the fallback service
        (perform-authorization-checks can-run-as? auth-user descriptor)
        {:descriptor descriptor
         :latest-descriptor latest-descriptor}))))

(defn await-service-goal-fallback-state-locally
  "Polls fallback-state-atom locally and returns true if the goal state was reached and false if timeout is reached"
  [fallback-state-atom service-id timeout sleep-duration goal]
  (async/go
    (let [goal-reached-predicate (case goal
                                   "deleted" #(false? (service-exists? % service-id))
                                   "healthy" #(true? (service-healthy? % service-id))
                                   "exist" #(true? (service-exists? % service-id)))]
      (loop [time-left-ms timeout]
        (let [fallback-state @fallback-state-atom
              goal-reached? (goal-reached-predicate fallback-state)]
          (if (or goal-reached? (not (pos? time-left-ms)))
            goal-reached?
            (do
              (async/<! (async/timeout (min sleep-duration time-left-ms)))
              (recur (- time-left-ms sleep-duration)))))))))

(defn await-service-goal-fallback-state
  "Polls local router and other routers and returns map router-id->success?"
  [fallback-state-atom make-inter-router-requests-fn router-id service-id timeout sleep-duration goal]
  (go-try
    (let [router-id->response-chan (assoc
                                     (make-inter-router-requests-fn (str "apps/" service-id "/await/" goal)
                                                                    :method :get
                                                                    :config {:query-string (str "timeout=" timeout)})
                                     router-id (async/go
                                                 {:body (async/go
                                                          (json/write-str
                                                            {:success? (async/<! (await-service-goal-fallback-state-locally
                                                                                  fallback-state-atom service-id timeout sleep-duration goal))}))}))]
      (loop [router-id->success? {}
             [[router-id response-chan] & remaining] (seq router-id->response-chan)]
        (if (and router-id response-chan)
          (recur
            (assoc router-id->success? router-id (some-> response-chan
                                            async/<!
                                            :body
                                            async/<!
                                            utils/try-parse-json
                                            (get "success?")))
            remaining)
          router-id->success?)))))
