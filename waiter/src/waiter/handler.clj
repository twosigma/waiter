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
(ns waiter.handler
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [comb.template :as template]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [full.async :refer [<?]]
            [plumbing.core :as pc]
            [ring.middleware.multipart-params :as multipart-params]
            [waiter.async-request :as async-req]
            [waiter.auth.jwt :as jwt]
            [waiter.authorization :as authz]
            [waiter.correlation-id :as cid]
            [waiter.descriptor :as descriptor]
            [waiter.headers :as headers]
            [waiter.interstitial :as interstitial]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.service-description :as sd]
            [waiter.statsd :as statsd]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.semaphore :as semaphore]
            [waiter.util.utils :as utils])
  (:import (java.io InputStream)))

(defn async-make-request-helper
  "Helper function that returns a function that can invoke make-request-fn."
  [http-clients instance-request-properties make-basic-auth-fn service-id->password-fn prepare-request-properties-fn make-request-fn]
  (fn async-make-request-fn [instance {:keys [headers] :as request} end-route metric-group backend-proto]
    (let [{:keys [passthrough-headers waiter-headers]} (headers/split-headers headers)
          instance-request-properties (prepare-request-properties-fn instance-request-properties waiter-headers)
          proto-version (hu/backend-protocol->http-version backend-proto)]
      (make-request-fn http-clients make-basic-auth-fn service-id->password-fn instance request
                       instance-request-properties passthrough-headers end-route metric-group
                       backend-proto proto-version))))

(defn- async-make-http-request
  "Helper function for async status/result handlers."
  [counter-name make-http-request-fn service-id->service-description-fn
   {:keys [route-params uri] :as request}]
  (let [{:keys [host location port request-id router-id service-id]} route-params]
    (when-not (and host location port request-id router-id service-id)
      (throw (ex-info "Missing host, location, port, request-id, router-id or service-id in uri"
                      {:log-level :info :route-params route-params :status http-400-bad-request :uri uri})))
    (counters/inc! (metrics/service-counter service-id "request-counts" counter-name))
    (let [{:strs [backend-proto metric-group] :as service-description} (service-id->service-description-fn service-id)
          instance (scheduler/make-ServiceInstance {:host host :port port :service-id service-id})
          _ (log/info request-id counter-name "relative location is" location)
          response-chan (make-http-request-fn instance request location metric-group backend-proto)]
      (async/go
        (-> (async/<! response-chan)
          (assoc :descriptor {:service-description service-description
                              :service-id service-id}
                 :instance instance))))))

(defn complete-async-handler
  "Completes execution of an async request by propagating a termination message to the request monitor system."
  [async-request-terminate-fn {:keys [route-params uri] {:keys [src-router-id]} :basic-authentication :as request}]
  (try
    (let [{:keys [request-id service-id]} route-params]
      (when (str/blank? service-id)
        (throw (ex-info "No service-id specified" {:log-level :info :src-router-id src-router-id :status http-400-bad-request :uri uri})))
      (when (str/blank? request-id)
        (throw (ex-info "No request-id specified" {:log-level :info :src-router-id src-router-id :status http-400-bad-request :uri uri})))
      (let [succeeded (async-request-terminate-fn request-id)]
        (utils/clj->json-response {:request-id request-id, :success succeeded})))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn async-result-handler
  "Result handler for async requests. Supports any http method.
   The router delegates the call to the backend and it notifies the 'host' router to treat the async request as complete."
  [async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn
   {:keys [request-method route-params] :as request}]
  (async/go
    (try
      (let [{:keys [request-id router-id service-id]} route-params
            {:keys [error status] :as backend-response}
            (async/<! (async-make-http-request "async-result" make-http-request-fn service-id->service-description-fn request))]
        (log/info "http" request-method "returned status" status)
        (async-trigger-terminate-fn router-id service-id request-id)
        (when error (throw error))
        backend-response)
      (catch Exception ex
        (log/error ex "error in retrieving result of async request")
        (utils/exception->response ex request)))))

(defn async-status-handler
  "Status handler for async requests. Supports both delete and get http methods.
   The status checks are 'host' router, i.e. the router that processed the original async request, independent.
   The router delegates the call to the backend and intercepts the response to know when the async request has completed.
   If the router determines that the request has completed, it notifies the 'host' router to treat the async request as complete."
  [async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn
   {:keys [request-method route-params] :as request}]
  (async/go
    (try
      (let [{:keys [host location port request-id router-id service-id]} route-params
            {:keys [error status] :as backend-response}
            (async/<! (async-make-http-request "async-status" make-http-request-fn service-id->service-description-fn request))]
        (when error (throw error))
        (let [{:strs [backend-proto]} (service-id->service-description-fn service-id)
              endpoint (scheduler/end-point-url backend-proto host port location)
              location-header (get-in backend-response [:headers "location"])
              location-url (async-req/normalize-location-header endpoint location-header)
              relative-location? (str/starts-with? (str location-url) "/")]
          (when (or (and (= request-method :get)
                         (or (and (= http-303-see-other status) (not relative-location?))
                             (= http-410-gone status)))
                    (and (= request-method :delete)
                         (or (= http-200-ok status)
                             (= http-204-no-content status))))
            (async-trigger-terminate-fn router-id service-id request-id))
          (if (and (= request-method :get) (= http-303-see-other status) relative-location?)
            (let [result-location (async-req/route-params->uri "/waiter-async/result/" (assoc route-params :location location-url))]
              (assoc-in backend-response [:headers "location"] result-location))
            backend-response)))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn eject-instance
  [notify-instance-killed-fn populate-maintainer-chan! request]
  (async/go
    (try
      (let [{:strs [instance period-in-ms reason] :as request-body-map} (-> request ru/json-request :body)
            instance-id (get instance "id")
            service-id (get instance "service-id")]
        (if (or (str/blank? reason)
                (str/blank? instance-id)
                (str/blank? service-id)
                (or (not (integer? period-in-ms)) (neg? period-in-ms)))
          (throw (ex-info "Must provide the service-id, the instance id, the reason, and a positive period"
                          {:input-data request-body-map
                           :log-level :info
                           :status http-400-bad-request}))
          (let [response-chan (async/promise-chan)
                _ (service/eject-instance! populate-maintainer-chan! service-id instance-id period-in-ms response-chan)
                _ (log/info "Waiting for response from eject channel...")
                response-code (async/alt!
                                response-chan ([code] code)
                                (async/timeout (-> 30 t/seconds t/in-millis)) ([_] :timeout)
                                :priority true)
                successful? (= response-code :ejected)]
            (cid/cloghelper
              (if successful? :info :warn)
              "eject" instance-id "of" service-id "response:" (name response-code))
            (if successful?
              (do
                (when (= "killed" reason)
                  (let [instance (-> instance
                                     walk/keywordize-keys
                                     (update :started-at (fn [started-at]
                                                           (when started-at
                                                             (du/str-to-date started-at)))))]
                    (notify-instance-killed-fn instance)))
                (utils/clj->json-response {:instance-id instance-id
                                           :eject-period period-in-ms}))
              (let [response-status (if (= :in-use response-code) http-423-locked http-503-service-unavailable)]
                (utils/clj->json-response {:message "Unable to eject instance."
                                           :instance-id instance-id
                                           :reason response-code}
                                          :status response-status))))))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn get-ejected-instances
  "Return the ejected instances for a given service-id at this router."
  [populate-maintainer-chan! service-id request]
  (async/go
    (try
      (when (str/blank? service-id)
        (throw (ex-info "Missing service-id" {:log-level :info :status http-400-bad-request})))
      (let [response-chan (async/promise-chan)
            _ (service/query-instance! populate-maintainer-chan! service-id response-chan)
            _ (log/info "Waiting for response from query-state channel...")
            current-state (async/alt!
                            response-chan ([state] state)
                            (async/timeout (-> 30 t/seconds t/in-millis)) ([_] {})
                            :priority true)
            ejected-instances (vec (keys (:instance-id->eject-expiry-time current-state)))]
        (log/info service-id "has" (count ejected-instances) "ejected instance(s).")
        (utils/clj->json-response {:ejected-instances ejected-instances}))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn metrics-request-handler
  "Retrieves the codahale metrics for a service-id present at this router."
  [request]
  (try
    (let [request-params (-> request ru/query-params-request :query-params)
          exclude-services (utils/request-flag request-params "exclude-services")
          service-id (get request-params "service-id" nil)
          include-jvm-metrics (utils/request-flag request-params "include-jvm-metrics")
          metrics (cond
                    exclude-services (metrics/get-waiter-metrics)
                    (and (not exclude-services) service-id) (metrics/get-service-metrics service-id)
                    include-jvm-metrics (metrics/get-jvm-metrics)
                    :else (metrics/get-metrics (metrics/contains-metrics-filter (str (get request-params "name")))))]
      (utils/clj->streaming-json-response metrics))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn service-id-handler
  "Retrieves the service-id of the service specified by the request."
  [{:keys [descriptor] :as request} kv-store store-service-description-fn]
  (try
    (let [{:keys [service-id core-service-description]} descriptor]
      (when (not= core-service-description (sd/fetch-core kv-store service-id))
        ; eagerly store the service description for this service-id
        (store-service-description-fn descriptor))
      (utils/attach-waiter-source
        {:body service-id
         :status http-200-ok }))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn- str->filter-fn
  "Returns a name-filtering function given a user-provided name filter string"
  [name]
  (let [pattern (-> (str name)
                    (str/replace #"\." "\\\\.")
                    (str/replace #"\*+" ".*")
                    re-pattern)]
    #(re-matches pattern %)))

(defn- strs->filter-fn
  "Returns a name-filtering function that matches on any of the given sequence of user-provided names as filter string."
  [names]
  (let [filter-fns (map str->filter-fn names)]
    (fn [value] (some #(%1 value) filter-fns))))

(defn- retrieve-scaling-state
  "Retrieves the scaling state for the service from the autoscaler state."
  [query-autoscaler-state-fn service-id]
  (some-> (query-autoscaler-state-fn)
    :service-id->scale-state
    (get service-id)
    :scale-amount
    utils/scale-amount->scaling-state))

(defn param-value->filter-fn
  "Accepts a single string or a sequence of strings as input.
   Creates the filter function that does substring match for the input string or any string in the sequence."
  [param-value]
  (if (string? param-value)
    (str->filter-fn param-value)
    (strs->filter-fn param-value)))

(defn query-params->service-description-filter-predicate
  "Creates the filter function for service descriptions that matches every parameter provided in the request-params map."
  [request-params]
  (let [service-description-params (select-keys request-params sd/service-parameter-keys)
        param-predicates (map (fn [[param-name param-value]]
                                (let [param-predicate (param-value->filter-fn param-value)]
                                  (fn [service-description]
                                    (param-predicate (str (get service-description param-name))))))
                              (seq service-description-params))]
    (fn [service-description]
      (or (empty? param-predicates)
          (every? #(%1 service-description) param-predicates)))))

(defn compute-resource-usage
  "Computes the resources used to a service."
  [{:strs [cpus mem]} instance-count]
  {:cpus (* instance-count cpus)
   :mem (* instance-count mem)})

(defn list-services-handler
  "Retrieves the list of services viewable by the currently logged in user.
   A service is viewable by the run-as-user or a waiter super-user."
  [entitlement-manager query-state-fn query-autoscaler-state-fn prepend-waiter-url service-id->service-description-fn
   service-id->metrics-fn service-id->references-fn service-id->source-tokens-entries-fn request]
  (let [{:keys [all-available-service-ids service-id->healthy-instances service-id->unhealthy-instances] :as global-state} (query-state-fn)]
    (let [{:strs [run-as-user token token-version] :as request-params} (-> request ru/query-params-request :query-params)
          service-description-filter-predicate (query-params->service-description-filter-predicate request-params)
          auth-user (get request :authorization/user)
          viewable-service-ids (filter
                                 (fn [service-id]
                                   (let [service-description (service-id->service-description-fn service-id :effective? true)
                                         source-tokens (-> (service-id->source-tokens-entries-fn service-id) vec flatten)]
                                     (and (service-description-filter-predicate service-description)
                                          ;; legacy behavior:
                                          ;; when run-as-user param is provided filtered by the run-as-user using
                                          ;; the service-description-filter-predicate predicate above,
                                          ;; else list only services managed by current user.
                                          (or (seq run-as-user)
                                              (authz/manage-service? entitlement-manager auth-user service-id service-description))
                                          (or (str/blank? token)
                                              (let [filter-fn (str->filter-fn token)]
                                                (->> source-tokens
                                                  (map #(get % "token"))
                                                  (some filter-fn))))
                                          (or (str/blank? token-version)
                                              (let [filter-fn (str->filter-fn token-version)]
                                                (->> source-tokens
                                                  (map #(get % "version"))
                                                  (some filter-fn)))))))
                                 (sort all-available-service-ids))
          retrieve-instance-counts (fn retrieve-instance-counts [service-id]
                                     {:healthy-instances (-> service-id->healthy-instances (get service-id) count)
                                      :unhealthy-instances (-> service-id->unhealthy-instances (get service-id) count)})
          service-id->metrics (service-id->metrics-fn)
          include-effective-parameters? (or (utils/request-flag request-params "effective-parameters")
                                            (utils/param-contains? request-params "include" "effective-parameters"))
          include-references? (utils/param-contains? request-params "include" "references")
          response-data (map
                          (fn service-id->service-info [service-id]
                            (let [scaling-state (retrieve-scaling-state query-autoscaler-state-fn service-id)
                                  core-service-description (service-id->service-description-fn service-id :effective? false)
                                  source-tokens-entries (service-id->source-tokens-entries-fn service-id)
                                  instance-counts-map (retrieve-instance-counts service-id)
                                  instance-count (reduce + (vals instance-counts-map))
                                  effective-service-description (service-id->service-description-fn service-id :effective? true)
                                  resource-usage (compute-resource-usage effective-service-description instance-count)]
                              (cond->
                                {:instance-counts instance-counts-map
                                 :last-request-time (get-in service-id->metrics [service-id "last-request-time"])
                                 :resource-usage resource-usage
                                 :service-id service-id
                                 :service-description core-service-description
                                 :status (service/retrieve-service-status-label service-id global-state)
                                 :url (prepend-waiter-url (str "/apps/" service-id))}
                                include-effective-parameters?
                                (assoc :effective-parameters effective-service-description)
                                include-references?
                                (assoc :references (seq (service-id->references-fn service-id)))
                                scaling-state
                                (assoc :scaling-state scaling-state)
                                (seq source-tokens-entries)
                                (assoc :source-tokens source-tokens-entries))))
                          viewable-service-ids)]
      (utils/clj->streaming-json-response response-data))))

(defn delete-service-handler
  "Deletes the service from the scheduler (after authorization checks)."
  [router-id service-id core-service-description scheduler allowed-to-manage-service?-fn scheduler-interactions-thread-pool
   make-inter-router-requests-fn fallback-state-atom request]
  (let [{:strs [timeout] :or {timeout "0"}} (-> request ru/query-params-request :query-params)
        timeout (utils/parse-int timeout)
        auth-user (get request :authorization/user)
        run-as-user (get core-service-description "run-as-user")]
    (when (nil? timeout)
      (throw
        (ex-info "timeout must be an integer"
                 {:current-user auth-user
                  :existing-owner run-as-user
                  :log-level :info
                  :service-id service-id
                  :status http-400-bad-request
                  :timeout timeout})))
    (if-not (allowed-to-manage-service?-fn service-id auth-user)
      (throw
        (ex-info "User not allowed to delete service"
                 {:current-user auth-user
                  :existing-owner run-as-user
                  :log-level :info
                  :service-id service-id
                  :status http-403-forbidden}))
      (async/go
        (try
          (let [{:keys [error result]} (async/<!
                                         (au/execute
                                           (fn delete-service-task []
                                             (scheduler/delete-service scheduler service-id))
                                           scheduler-interactions-thread-pool))]
            (if error
              (utils/exception->response error request)
              (let [goal "deleted"
                    sleep-duration-ms 100
                    delete-result (:result result)
                    response-status (case delete-result
                                      :deleted http-200-ok
                                      :no-such-service-exists http-404-not-found
                                      http-400-bad-request)
                    should-poll? (and (= http-200-ok response-status)
                                      (pos? timeout))
                    routers-agree (when should-poll?
                                    (<?
                                      (descriptor/await-service-goal-fallback-state fallback-state-atom make-inter-router-requests-fn router-id service-id timeout sleep-duration-ms goal)))
                    response-body-map (cond-> {:service-id service-id,
                                               :success (= http-200-ok response-status)}
                                              should-poll? (assoc :routers-agree routers-agree)
                                              true (merge result))]
                (utils/clj->json-response response-body-map :status response-status))))
          (catch Throwable ex
            (log/error ex "error while deleting service" service-id)
            (utils/exception->response ex request)))))))

(defn generate-log-url
  "Generates the log url for an instance"
  [prepend-waiter-url {:keys [directory host id service-id]}]
  (prepend-waiter-url (str "/apps/" service-id "/logs?instance-id=" id "&host=" host
                           (when directory (str "&directory=" directory)))))

(defn- assoc-log-url
  "Appends the :log-url field for an instance"
  [generate-log-url-fn service-instance]
  (assoc service-instance :log-url (generate-log-url-fn service-instance)))

(defn- get-service-instances
  "Retrieve a {:active-instances [...] :failed-instances [...] :killed-instances [...]} map of
   scheduler/ServiceInstance records for the given service-id.
   The active-instances should not be assumed to be healthy (or live)."
  [global-state service-id]
  (let [{:keys [service-id->failed-instances service-id->healthy-instances service-id->killed-instances
                service-id->unhealthy-instances]} global-state]
    {:active-instances (concat (get service-id->healthy-instances service-id)
                               (get service-id->unhealthy-instances service-id))
     :failed-instances (get service-id->failed-instances service-id)
     :killed-instances (get service-id->killed-instances service-id)}))

(defn- get-current-for-tokens
  [source-token-entries token->token-hash]
  (reduce (fn [acc source-tokens]
            (let [current-for-tokens (map (fn [{:strs [token version]}]
                                            (let [current-version (token->token-hash token)]
                                              (= version current-version)))
                                          source-tokens)]
              (if (every? true? current-for-tokens)
                (into acc (map (fn [t] (get t "token")) source-tokens))
                acc)))
          []
          source-token-entries))

(defn- get-service-handler
  "Returns details about the service such as the service description, metrics, instances, etc."
  [router-id service-id core-service-description kv-store generate-log-url-fn make-inter-router-requests-fn
   service-id->service-description-fn service-id->source-tokens-entries-fn service-id->references-fn
   query-state-fn query-autoscaler-state-fn service-id->metrics-fn token->token-hash request]
  (let [global-state (query-state-fn)
        service-instance-maps (try
                                (let [process-instances-fn
                                      (fn process-instances-fn [instances instances-to-keep]
                                        (cond->> instances
                                          true (scheduler/sort-instances)
                                          (integer? instances-to-keep) (take instances-to-keep)
                                          true (map #(assoc-log-url generate-log-url-fn %))))
                                      active-instances-to-keep nil
                                      inactive-instances-to-keep 10]
                                  (-> (get-service-instances global-state service-id)
                                    (update :active-instances process-instances-fn active-instances-to-keep)
                                    (update :failed-instances process-instances-fn inactive-instances-to-keep)
                                    (update :killed-instances process-instances-fn inactive-instances-to-keep)))
                                (catch Exception e
                                  (log/error e "Error in retrieving instances for" service-id)))
        request-params (-> request ru/query-params-request :query-params)
        include-metrics? (utils/param-contains? request-params "include" "metrics")
        router->metrics (when include-metrics?
                          (try
                            (let [router-id->response-chan (make-inter-router-requests-fn (str "metrics?service-id=" service-id) :method :get)
                                  router-id->response (-> (pc/map-vals (fn [chan] (update (async/<!! chan) :body async/<!!))
                                                                       router-id->response-chan)
                                                          (assoc router-id (-> (metrics/get-service-metrics service-id)
                                                                               (utils/clj->json-response))))
                                  response->service-metrics (fn response->metrics [{:keys [body]}]
                                                              (try
                                                                (let [metrics (json/read-str (str body))]
                                                                  (get-in metrics ["services" service-id]))
                                                                (catch Exception e
                                                                  (log/error e "unable to retrieve metrics from response" (str body)))))
                                  router->service-metrics (pc/map-vals response->service-metrics router-id->response)]
                              (utils/filterm val router->service-metrics))
                            (catch Exception e
                              (log/error e "Error in retrieving router metrics for" service-id))))
        aggregate-metrics-map (when include-metrics?
                                (try
                                  (metrics/aggregate-router-codahale-metrics (or router->metrics {}))
                                  (catch Exception e
                                    (log/error e "Error in aggregating router metrics for" service-id))))
        service-description-overrides (try
                                        (sd/service-id->overrides kv-store service-id :refresh true)
                                        (catch Exception e
                                          (log/error e "Error in retrieving service description overrides for" service-id)))
        service-suspended-state (try
                                  (sd/service-id->suspended-state kv-store service-id :refresh true)
                                  (catch Exception e
                                    (log/error e "Error in retrieving service suspended state for" service-id)))
        source-tokens-entries (service-id->source-tokens-entries-fn service-id)
        current-for-tokens (get-current-for-tokens source-tokens-entries token->token-hash)
        include-effective-parameters? (or (utils/request-flag request-params "effective-parameters")
                                          (utils/param-contains? request-params "include" "effective-parameters"))
        include-references? (utils/param-contains? request-params "include" "references")
        last-request-time (get-in (service-id->metrics-fn) [service-id "last-request-time"])
        scaling-state (retrieve-scaling-state query-autoscaler-state-fn service-id)
        effective-service-description (service-id->service-description-fn service-id :effective? true)
        num-active-instances (count (:active-instances service-instance-maps))
        resource-usage (compute-resource-usage effective-service-description num-active-instances)
        result-map (cond-> {:num-routers (count router->metrics)
                            :resource-usage resource-usage
                            :router-id router-id
                            :status (service/retrieve-service-status-label service-id global-state)}
                     (and (not-empty core-service-description) include-effective-parameters?)
                     (assoc :effective-parameters effective-service-description)
                     (not-empty service-instance-maps)
                     (assoc :instances service-instance-maps
                            :num-active-instances num-active-instances)
                     last-request-time
                     (assoc :last-request-time last-request-time)
                     (not-empty aggregate-metrics-map)
                     (assoc-in [:metrics :aggregate] aggregate-metrics-map)
                     (not-empty router->metrics)
                     (assoc-in [:metrics :routers] router->metrics)
                     include-references?
                     (assoc :references (seq (service-id->references-fn service-id)))
                     scaling-state
                     (assoc :scaling-state scaling-state)
                     (not-empty core-service-description)
                     (assoc :service-description core-service-description)
                     (not-empty (or (:overrides service-description-overrides) {}))
                     (assoc :service-description-overrides service-description-overrides)
                     (:time service-suspended-state)
                     (assoc :service-suspended-state service-suspended-state)
                     (seq source-tokens-entries)
                     (assoc :source-tokens source-tokens-entries)
                     (seq current-for-tokens)
                     (assoc :current-for-tokens current-for-tokens))]
    (utils/clj->streaming-json-response result-map)))

(defn service-handler
  "Handles the /apps/<service-id> requests.
   It supports the following request methods:
     :delete deletes the service from the scheduler (after authorization checks).
     :get returns details about the service such as the service description, metrics, instances, etc."
  [router-id service-id scheduler kv-store allowed-to-manage-service?-fn generate-log-url-fn make-inter-router-requests-fn
   service-id->service-description-fn service-id->source-tokens-entries-fn service-id->references-fn query-state-fn
   query-autoscaler-state-fn service-id->metrics-fn scheduler-interactions-thread-pool token->token-hash fallback-state-atom request]
  (try
    (when-not service-id
      (throw (ex-info "Missing service-id" {:log-level :info :status http-400-bad-request})))
    (let [core-service-description (sd/fetch-core kv-store service-id :refresh true)]
      (if (empty? core-service-description)
        (throw (ex-info "Service not found" {:log-level :info :service-id service-id :status http-404-not-found}))
        (case (:request-method request)
          :delete (delete-service-handler router-id service-id core-service-description scheduler allowed-to-manage-service?-fn
                                          scheduler-interactions-thread-pool make-inter-router-requests-fn fallback-state-atom request)
          :get (get-service-handler router-id service-id core-service-description kv-store generate-log-url-fn
                                    make-inter-router-requests-fn service-id->service-description-fn
                                    service-id->source-tokens-entries-fn service-id->references-fn
                                    query-state-fn query-autoscaler-state-fn
                                    service-id->metrics-fn token->token-hash request))))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn- trigger-service-refresh
  "Makes inter-router calls to refresh service caches in kv-store."
  [make-inter-router-requests-fn service-id]
  (make-inter-router-requests-fn (str "apps/" service-id "/refresh") :method :get))

(defn suspend-or-resume-service-handler
  "Suspend/Resume a service after performing run-as-user/waiter-user validation."
  [kv-store allowed-to-manage-service? make-inter-router-requests-fn service-id mode request]
  (try
    (when (str/blank? service-id)
      (throw (ex-info "Missing service-id" {:log-level :info :status http-400-bad-request})))
    ; throw exception if no service description for service-id exists
    (sd/fetch-core kv-store service-id :refresh true :nil-on-missing? false)
    (let [auth-user (get request :authorization/user)
          mode-str (name mode)]
      (log/info auth-user "wants to" mode-str " " service-id)
      (if (allowed-to-manage-service? service-id auth-user)
        (do
          (cond
            (= :suspend mode) (sd/suspend-service kv-store service-id auth-user)
            (= :resume mode) (sd/resume-service kv-store service-id auth-user)
            :else (log/info "unsupported mode:" mode-str))
          (let [success (contains? #{:suspend :resume} mode)]
            ; refresh state on routers
            (trigger-service-refresh make-inter-router-requests-fn service-id)
            (utils/clj->json-response {:action mode-str
                                       :service-id service-id
                                       :success success})))
        (throw (ex-info (str auth-user " not allowed to modify " service-id)
                        {:auth-user auth-user
                         :log-level :info
                         :service-id service-id
                         :status http-403-forbidden}))))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn override-service-handler
  "Handles overrides for a service."
  [kv-store allowed-to-manage-service? make-inter-router-requests-fn service-id {:keys [request-method] :as request}]
  (when (str/blank? service-id)
    (throw (ex-info "Missing service-id" {:log-level :info :status http-400-bad-request})))
  ; throw exception if no service description for service-id exists
  (sd/fetch-core kv-store service-id :refresh true :nil-on-missing? false)
  (let [auth-user (get request :authorization/user)]
    (case request-method
      :delete
      (do
        (log/info auth-user "wants to delete overrides for" service-id)
        (if (allowed-to-manage-service? service-id auth-user)
          (do
            (sd/clear-service-description-overrides kv-store service-id auth-user)
            (trigger-service-refresh make-inter-router-requests-fn service-id)
            (utils/clj->json-response {:service-id service-id, :success true}))
          (throw (ex-info (str auth-user " not allowed to override " service-id)
                          {:auth-user auth-user :log-level :info :service-id service-id, :status http-403-forbidden}))))

      :get
      (if (allowed-to-manage-service? service-id auth-user)
        (-> (sd/service-id->overrides kv-store service-id :refresh true)
            (assoc :service-id service-id)
            utils/clj->json-response)
        (throw (ex-info (str auth-user " not allowed view override information of " service-id)
                        {:auth-user auth-user :log-level :info :service-id service-id :status http-403-forbidden})))

      :post
      (do
        (log/info auth-user "wants to update overrides for" service-id)
        (if (allowed-to-manage-service? service-id auth-user)
          (do
            (let [service-description-overrides (-> request ru/json-request :body)]
              (sd/store-service-description-overrides kv-store service-id auth-user service-description-overrides))
            (trigger-service-refresh make-inter-router-requests-fn service-id)
            (utils/clj->json-response {:service-id service-id, :success true}))
          (throw (ex-info (str auth-user " not allowed to override " service-id)
                          {:auth-user auth-user :log-level :info :service-id service-id :status http-403-forbidden}))))

      (throw (ex-info "Unsupported request method" {:log-level :info :request-method request-method :status http-405-method-not-allowed})))))

(defn service-view-logs-handler
  "Redirects user to the log directory on the instance host."
  [scheduler service-id generate-log-url-fn request]
  (try
    (let [{:strs [instance-id host directory]} (-> request ru/query-params-request :query-params)
          _ (when-not instance-id
              (throw (ex-info "Missing instance-id parameter" {:log-level :info :status http-400-bad-request})))
          _ (when-not host
              (throw (ex-info "Missing host parameter" {:log-level :info :status http-400-bad-request})))
          directory-content (map (fn [{:keys [path type] :as entry}]
                                   (if (= type "file")
                                     entry
                                     (-> (dissoc entry :path)
                                         (assoc :url (generate-log-url-fn {:directory path
                                                                           :host host
                                                                           :id instance-id
                                                                           :service-id service-id})))))
                                 (scheduler/retrieve-directory-content scheduler service-id instance-id host directory))]
      (utils/clj->json-response (vec directory-content)))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn service-await-handler
  "Polls fallback-state-atom until timeout is reached or service does not exist"
  [fallback-state-atom {{:keys [service-id goal-state]} :route-params
                        {:keys [src-router-id]} :basic-authentication
                        :keys [request-method]
                        :as request}]
  (async/go
    (try
      (log/info service-id "service-await-handler triggered by router" src-router-id)
      (case request-method
        :get (let [{:strs [timeout sleep-duration] :or {sleep-duration "100"}} (-> request ru/query-params-request :query-params)
                   _ (when (nil? timeout)
                       (throw (ex-info "timeout is a required query parameter"
                                       {:log-level :info
                                        :request-method request-method
                                        :status http-400-bad-request})))
                   parsed-timeout (utils/parse-int timeout)
                   parsed-sleep-duration (utils/parse-int sleep-duration)]
               (when (or (nil? parsed-sleep-duration) (nil? parsed-timeout))
                 (throw (ex-info "timeout and sleep-duration must be integers"
                                 {:log-level :info
                                  :request-method request-method
                                  :status http-400-bad-request
                                  :timeout timeout
                                  :sleep-duration sleep-duration})))
               (utils/clj->json-response {:success? (async/<!
                                                      (descriptor/await-service-goal-fallback-state-locally
                                                        fallback-state-atom service-id parsed-timeout parsed-sleep-duration goal-state))
                                          :service-id service-id}))
        (utils/exception->response (ex-info "Only GET supported" {:log-level :info
                                              :request-method request-method
                                              :status http-405-method-not-allowed})
                                   request))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn work-stealing-handler
  "Handles work-stealing offers of instances for load-balancing work on the current router."
  [populate-maintainer-chan! request]
  (async/go
    (try
      (meters/mark! (metrics/waiter-meter "work-stealing" "request-rate"))
      (let [{:keys [cid instance request-id router-id service-id] :as request-body-map}
            (-> request ru/json-request :body walk/keywordize-keys)]
        (log/info "received work-stealing offer" (:id instance) "of" service-id "from" router-id)
        (if-not (and cid instance request-id router-id service-id)
          (throw (ex-info "Missing one of cid, instance, request-id, router-id or service-id"
                          (assoc request-body-map :log-level :info :status http-400-bad-request)))
          (let [response-chan (async/promise-chan)
                offer-params {:cid cid
                              :instance (scheduler/make-ServiceInstance instance)
                              :request-id request-id
                              :response-chan response-chan
                              :router-id router-id
                              :service-id service-id}]
            (meters/mark! (metrics/service-meter service-id "work-stealing" "request-rate"))
            (service/offer-instance! populate-maintainer-chan! service-id offer-params)
            (let [response-status (async/<! response-chan)
                  http-status (condp = response-status
                                :channel-not-found http-404-not-found
                                :channel-put-failed http-500-internal-server-error
                                :client-error http-400-bad-request
                                :generic-error http-500-internal-server-error
                                :instance-busy http-503-service-unavailable
                                :instance-error http-502-bad-gateway
                                http-200-ok)]
              (counters/inc! (metrics/waiter-counter "work-stealing" "response" (name response-status)))
              (meters/mark! (metrics/waiter-meter "work-stealing" "response-rate" (name response-status)))
              (counters/inc! (metrics/service-counter service-id "work-stealing" "response" (name response-status)))
              (meters/mark! (metrics/service-meter service-id "work-stealing" "response-rate" (name response-status)))
              (utils/clj->json-response (-> offer-params
                                          (select-keys [:cid :request-id :router-id :service-id])
                                          (assoc :response-status response-status))
                                        :status http-status)))))
      (catch Exception ex
        (counters/inc! (metrics/waiter-counter "work-stealing" "response" "error"))
        (meters/mark! (metrics/waiter-meter "work-stealing" "response-rate" "error"))
        (utils/exception->response ex request)))))

(defn- retrieve-channel-state
  "Retrieves the state reported on the channel."
  [state-chan]
  (async/go
    (let [timeout-chan (-> 30 t/seconds t/in-millis async/timeout)]
      (first (async/alts! [state-chan timeout-chan] :priority true)))))

(defn get-router-state
  "Outputs the state of the router as json."
  [router-id query-state-fn request]
  (try
    (let [{:keys [routers]} (query-state-fn)
          host (get-in request [:headers "host"])
          scheme (some-> request utils/request->scheme name)
          make-url (fn make-url [path]
                     (str (when scheme (str scheme "://")) host "/state/" path))]
      (utils/clj->streaming-json-response {:details (->> ["autoscaler" "autoscaling-multiplexer" "codahale-reporters" "fallback"
                                                          "gc-broken-services" "gc-services" "gc-transient-metrics" "interstitial"
                                                          "jwt-auth-server" "kv-store" "launch-metrics" "leader" "local-usage"
                                                          "maintainer" "router-metrics" "scheduler" "service-description-builder"
                                                          "service-maintainer" "statsd" "work-stealing"]
                                                         (pc/map-from-keys make-url))
                                           :router-id router-id
                                           :routers routers}))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn get-chan-latest-state-handler
  "Outputs the latest state of the channel."
  [router-id query-state-fn request]
  (try
    (log/info (str "Waiting for response from state channel"))
    (let [data (query-state-fn)
          state (or data {:message "No data available"})]
      (utils/clj->streaming-json-response {:router-id router-id :state state}))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn get-query-chan-state-handler
  "Outputs the state by sending a standard query to the channel."
  [router-id query-chan request]
  (async/go
    (try
      (let [timeout-chan (-> 30 t/seconds t/in-millis async/timeout)
            response-chan (async/promise-chan)]
        (async/>! query-chan {:cid (cid/get-correlation-id) :response-chan response-chan})
        (log/info (str "Waiting for response from query channel"))
        (let [[data _] (async/alts! [response-chan timeout-chan] :priority true)
              state (or data {:message "Request timeout"})]
          (utils/clj->streaming-json-response {:router-id router-id :state state})))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn- get-function-state
  "Outputs the state obtained by invoking `retrieve-state-fn`."
  [retrieve-state-fn router-id request]
  (try
    (utils/clj->streaming-json-response {:router-id router-id :state (retrieve-state-fn)})
    (catch Exception ex
      (utils/exception->response ex request))))

(defn get-query-fn-state
  "Outputs the state retrieved by invoking the query-state-fn."
  [router-id query-state-fn request]
  (get-function-state query-state-fn router-id request))

(defn get-jwt-auth-server-state
  "Outputs the JWT auth server state."
  [router-id auth-server request]
  (let [{:strs [include]} (-> request ru/query-params-request :query-params)
        include-flags (if (string? include) #{include} (set include))
        query-state-fn (if (nil? auth-server)
                         (constantly :disabled)
                         #(jwt/retrieve-server-state auth-server include-flags))]
    (get-function-state query-state-fn router-id request)))

(defn get-kv-store-state
  "Outputs the kv-store state."
  [router-id kv-store request]
  (get-function-state #(kv/state kv-store) router-id request))

(defn get-local-usage-state
  "Outputs the local metrics agent state."
  [router-id local-usage-agent request]
  (get-function-state #(identity @local-usage-agent) router-id request))

(defn get-leader-state
  "Outputs the leader state."
  [router-id leader?-fn leader-id-fn request]
  (get-function-state
    (fn leader-state-fn []
      {:leader? (leader?-fn)
       :leader-id (leader-id-fn)})
    router-id request))

(defn get-router-metrics-state
  "Outputs the router metrics state."
  [router-id router-metrics-state-fn request]
  (get-function-state router-metrics-state-fn router-id request))

(defn get-scheduler-state
  "Outputs the scheduler state."
  [router-id scheduler request]
  (let [{:strs [include]} (-> request ru/query-params-request :query-params)
        include-flags (if (string? include) #{include} (set include))]
    (get-function-state #(scheduler/state scheduler include-flags) router-id request)))

(defn get-statsd-state
  "Outputs the statsd state."
  [router-id request]
  (get-function-state statsd/state router-id request))

(defn get-work-stealing-state
  "Outputs the global work-stealing state."
  [offers-allowed-semaphore router-id request]
  (get-function-state
    (fn compute-work-stealing-state []
      (let [request-params (-> request ru/query-params-request :query-params)]
        {:global-offers (semaphore/state offers-allowed-semaphore)
         :metrics (cond-> (metrics/get-metrics (metrics/conjunctive-metrics-filter
                                                 (metrics/prefix-metrics-filter "waiter")
                                                 (metrics/contains-metrics-filter "work-stealing")))
                    (utils/param-contains? request-params "include" "services")
                    (merge (metrics/get-metrics (metrics/conjunctive-metrics-filter
                                                  (metrics/prefix-metrics-filter "services")
                                                  (metrics/contains-metrics-filter "work-stealing")))))}))
    router-id request))

(defn get-service-state
  "Retrieves the state for a particular service on the router."
  [router-id enable-work-stealing-support? populate-maintainer-chan! local-usage-agent service-id query-sources request]
  (async/go
    (try
      (if (str/blank? service-id)
        (throw (ex-info "Missing service-id" {:log-level :info :status http-400-bad-request}))
        (let [timeout-ms (-> 10 t/seconds t/in-millis)
              _ (log/info "waiting for response from query-state channel...")
              responder-state-chan (service/query-maintainer-channel-map-with-timeout!
                                     populate-maintainer-chan! service-id timeout-ms :query-state)
              _ (log/info "waiting for response from query-work-stealing channel...")
              work-stealing-state (if (enable-work-stealing-support? service-id)
                                    (service/query-maintainer-channel-map-with-timeout!
                                      populate-maintainer-chan! service-id timeout-ms :query-work-stealing)
                                    :disabled)
              local-usage-state (get @local-usage-agent service-id)
              query-params {:cid (cid/get-correlation-id) :service-id service-id}
              [query-chans initial-result]
              (loop [[[entry-key entry-value] & remaining]
                     (concat query-sources
                             [[:responder-state responder-state-chan]
                              [:work-stealing-state work-stealing-state]])
                     query-chans {}
                     initial-result {:local-usage local-usage-state}]
                (if entry-key
                  (cond
                    (or (map? entry-value) (keyword? entry-value))
                    (recur remaining
                           query-chans
                           (assoc initial-result entry-key entry-value))
                    (fn? entry-value)
                    (recur remaining
                           query-chans
                           (assoc initial-result entry-key (entry-value query-params)))
                    :else
                    (recur remaining
                           (assoc query-chans entry-key entry-value)
                           initial-result))
                  [query-chans initial-result]))
              query-chans-state (loop [[[key query-response-or-chan] & remaining] (seq query-chans)
                                       result initial-result]
                                  (if (and key query-response-or-chan)
                                    (let [response-chan (async/promise-chan)
                                          timeout-chan (async/timeout timeout-ms)
                                          _ (->> (assoc query-params :response-chan response-chan)
                                                 (async/>! query-response-or-chan))
                                          _ (log/info "waiting on response from" key "channel")
                                          [data _] (async/alts! [response-chan timeout-chan] :priority true)
                                          state (or data {:message "Request timeout"})]
                                      (recur remaining (assoc result key state)))
                                    result))]
          (utils/clj->streaming-json-response {:router-id router-id, :state query-chans-state})))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn acknowledge-consent-handler
  "Processes the acknowledgment to launch a service as the auth-user.
   It triggers storing of the x-waiter-consent cookie on the client."
  [token->service-description-template token->token-metadata service-description->service-id
   consent-cookie-value add-encoded-cookie consent-expiry-days {:keys [request-method] :as request}]
  (try
    (when-not (= :post request-method)
      (throw (ex-info "Only POST supported" {:log-level :info :request-method request-method :status http-405-method-not-allowed})))
    (let [{:keys [headers params] :as request} (multipart-params/multipart-params-request request)
          {:strs [host origin referer x-requested-with]} headers
          {:strs [mode service-id] :as params} params]
      (when-not (str/blank? origin)
        (when-not (utils/same-origin request)
          (throw (ex-info "Origin is not the same as the host"
                          {:host host :log-level :info :origin origin :status http-400-bad-request}))))
      (when (and (not (str/blank? origin)) (not (str/blank? referer)))
        (when-not (str/starts-with? referer origin)
          (throw (ex-info "Referer does not start with origin"
                          {:origin origin :log-level :info :referer referer :status http-400-bad-request}))))
      (when-not (= x-requested-with "XMLHttpRequest")
        (throw (ex-info "Header x-requested-with does not match expected value"
                        {:actual x-requested-with :expected "XMLHttpRequest" :log-level :info :status http-400-bad-request})))
      (when-not (and mode (contains? #{"service" "token"} mode))
        (throw (ex-info "Missing or invalid mode" (assoc params :log-level :info :status http-400-bad-request))))
      (when (= "service" mode)
        (when-not service-id
          (throw (ex-info "Missing service-id" (assoc params :log-level :info :status http-400-bad-request)))))
      (let [token (utils/authority->host host)
            service-description-template (token->service-description-template token)]
        (when-not (seq service-description-template)
          (throw (ex-info "Unable to load description for token" {:log-level :info :status http-400-bad-request :token token})))
        (when (= "service" mode)
          (let [auth-user (:authorization/user request)
                computed-service-id (-> service-description-template
                                        (sd/assoc-run-as-requester-fields auth-user)
                                        service-description->service-id)]
            (when-not (= service-id computed-service-id)
              (log/error "computed" computed-service-id ", but user[" auth-user "] provided" service-id "for" token)
              (throw (ex-info "Invalid service-id for specified token" (assoc params :log-level :info :status http-400-bad-request))))))
        (let [token-metadata (token->token-metadata token)
              cookie-name "x-waiter-consent"
              cookie-value (consent-cookie-value mode service-id token token-metadata)]
          (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "approve-success"))
          (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "approve-success"))
          (-> {:body (str "Added cookie " cookie-name), :headers {}, :status http-200-ok }
            (utils/attach-waiter-source)
            (add-encoded-cookie cookie-name cookie-value consent-expiry-days)))))
    (catch Exception ex
      (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "approve-error"))
      (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "approve-error"))
      (utils/exception->response ex request))))

(let [consent-template-fn
      (template/fn
        [{:keys [auth-user consent-expiry-days service-description-template service-id target-url token]}]
        (slurp (io/resource "web/consent.html")))]
  (defn render-consent-template
    "Render the consent html page."
    [context]
    (consent-template-fn context)))

(defn request-consent-handler
  "Displays the consent form and requests approval from user. The content is rendered from consent.html.
   Approval form is submitted using AJAX and the user is then redirected to the target url that triggered a redirect to this form."
  [token->service-description-template service-description->service-id consent-expiry-days
   {:keys [headers query-string request-method request-time route-params] :as request}]
  (try
    (when-not (= :get request-method)
      (throw (ex-info "Only GET supported" {:log-level :info :request-method request-method :status http-405-method-not-allowed})))
    (let [host-header (get headers "host")
          token (utils/authority->host host-header)
          {:keys [path]} route-params
          {:strs [interstitial-secs] :as service-description-template} (token->service-description-template token)]
      (when-not (seq service-description-template)
        (throw (ex-info "Unable to load description for token" {:log-level :info :status http-404-not-found :token token})))
      (let [auth-user (:authorization/user request)
            service-id (-> service-description-template
                           (sd/assoc-run-as-requester-fields auth-user)
                           service-description->service-id)
            query-string' (str (when-not (str/blank? query-string) query-string)
                               (when (some-> interstitial-secs pos?)
                                 ;; add the bypass query param only if interstitial is enabled
                                 (str (when-not (str/blank? query-string) "&")
                                      (interstitial/request-time->interstitial-param-string request-time))))]
        (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "form-render"))
        (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "form-render"))
        (utils/attach-waiter-source
          {:body (render-consent-template
                   {:auth-user auth-user
                    :consent-expiry-days consent-expiry-days
                    :service-description-template service-description-template
                    :service-id service-id
                    :target-url (str (name (utils/request->scheme request)) "://" host-header "/" path
                                     (when-not (str/blank? query-string') (str "?" query-string')))
                    :token token})
           :headers {"content-type" "text/html"}
           :status http-200-ok })))
    (catch Exception ex
      (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "form-error"))
      (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "form-error"))
      (utils/exception->response ex request))))

(let [html-fn (template/fn [{:keys [cid host hostname message port support-info timestamp]}]
                           (slurp (io/resource "web/welcome.html")))]
  (defn- render-welcome-html
    "Renders welcome html"
    [context]
    (html-fn context)))

(let [text-fn (template/fn [{:keys [cid host hostname message port support-info timestamp]}]
                           (slurp (io/resource "web/welcome.txt")))]
  (defn- render-welcome-text
    "Renders welcome text"
    [context]
    (text-fn context)))

(defn welcome-handler
  "Response with a welcome page."
  [{:keys [host hostname port support-info]} {:keys [request-method request-time] :as request}]
  (let [welcome-info {:cid (cid/get-correlation-id)
                      :host host
                      :hostname hostname
                      :message "Welcome to Waiter"
                      :port port
                      :support-info support-info
                      :timestamp (du/date-to-str request-time)}
        content-type (utils/request->content-type request)]
    (try
      (case request-method
        :get (utils/attach-waiter-source
               {:body (case content-type
                        "application/json" (utils/clj->json welcome-info)
                        "text/html" (render-welcome-html welcome-info)
                        "text/plain" (render-welcome-text welcome-info))
                :headers {"content-type" content-type}})
        (throw (ex-info "Only GET supported" {:log-level :info :status http-405-method-not-allowed})))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn not-found-handler
  "Responds with a handler indicating a resource isn't found."
  [request]
  (utils/exception->response (ex-info (utils/message :not-found) {:log-level :info :status http-404-not-found}) request))

(defn status-handler
  "Responds with an 'ok' status.
   Includes representation of request if requested using the include=request-info query param."
  [{:keys [body trailers-fn] :as request}]
  (try
    (when (instance? InputStream body)
      (log/info "consuming request body before rendering response")
      (slurp body))
    (let [request-params (-> request ru/query-params-request :query-params)
          include-request-info (utils/param-contains? request-params "include" "request-info")]
      (-> (cond-> {:status "ok"}
            include-request-info
            (assoc
              :request-info
              (let [request-keys [:character-encoding :client-protocol :content-length :content-type :headers
                                  :internal-protocol :query-string :request-id :request-method :request-time :router-id
                                  :scheme :uri]
                    trailers (when trailers-fn (trailers-fn))]
                (cond-> (-> (select-keys request request-keys)
                            (update :headers headers/truncate-header-values))
                  (seq trailers)
                  (assoc :trailers (headers/truncate-header-values trailers))))))
          utils/clj->json-response))
    (catch Throwable th
      (utils/exception->response th request))))

(defn display-profiles-handler
  "Responds with a list of profiles"
  [profile->defaults request]
  (try
    (utils/clj->json-response
      (map (fn [[profile defaults]]
             {:defaults defaults
              :name profile})
           (seq profile->defaults)))
    (catch Throwable th
      (utils/exception->response th request))))