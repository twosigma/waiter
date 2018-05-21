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
            [plumbing.core :as pc]
            [ring.middleware.multipart-params :as multipart-params]
            [waiter.async-request :as async-req]
            [waiter.authorization :as authz]
            [waiter.correlation-id :as cid]
            [waiter.headers :as headers]
            [waiter.interstitial :as interstitial]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.service-description :as sd]
            [waiter.statsd :as statsd]
            [waiter.util.date-utils :as du]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils]))

(defn make-auth-user-map
  "Creates a map containing the username and principal from a request"
  [request]
  {:principal (:authorization/principal request)
   :username (:authorization/user request)})

(defn async-make-request-helper
  "Helper function that returns a function that can invoke make-request-fn."
  [http-client instance-request-properties make-basic-auth-fn service-id->password-fn prepare-request-properties-fn make-request-fn]
  (fn async-make-request-fn [instance {:keys [headers] :as request} end-route metric-group]
    (let [{:keys [passthrough-headers waiter-headers]} (headers/split-headers headers)
          instance-request-properties (prepare-request-properties-fn instance-request-properties waiter-headers)]
      (make-request-fn http-client make-basic-auth-fn service-id->password-fn instance request
                       instance-request-properties passthrough-headers end-route metric-group))))

(defn- async-make-http-request
  "Helper function for async status/result handlers."
  [counter-name make-http-request-fn service-id->service-description-fn
   {:keys [route-params uri] :as request}]
  (let [{:keys [host location port request-id router-id service-id]} route-params]
    (when-not (and host location port request-id router-id service-id)
      (throw (ex-info "Missing host, location, port, request-id, router-id or service-id in uri"
                      {:route-params route-params, :uri uri, :status 400})))
    (counters/inc! (metrics/service-counter service-id "request-counts" counter-name))
    (let [{:strs [backend-proto metric-group]} (service-id->service-description-fn service-id)
          instance (scheduler/make-ServiceInstance {:host host :port port :protocol backend-proto :service-id service-id})
          _ (log/info request-id counter-name "relative location is" location)]
      (make-http-request-fn instance request location metric-group))))

(defn complete-async-handler
  "Completes execution of an async request by propagating a termination message to the request monitor system."
  [async-request-terminate-fn {:keys [route-params uri] {:keys [src-router-id]} :basic-authentication :as request}]
  (try
    (let [{:keys [request-id service-id]} route-params]
      (when (str/blank? service-id)
        (throw (ex-info "No service-id specified" {:src-router-id src-router-id, :status 400, :uri uri})))
      (when (str/blank? request-id)
        (throw (ex-info "No request-id specified" {:src-router-id src-router-id, :status 400, :uri uri})))
      (let [succeeded (async-request-terminate-fn request-id)]
        (utils/map->json-response {:request-id request-id, :success succeeded})))
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
              endpoint (scheduler/end-point-url {:host host :port port :protocol backend-proto} location)
              location-header (get-in backend-response [:headers "location"])
              location-url (async-req/normalize-location-header endpoint location-header)
              relative-location? (str/starts-with? (str location-url) "/")]
          (when (or (and (= request-method :get)
                         (or (and (= 303 status) (not relative-location?))
                             (= 410 status)))
                    (and (= request-method :delete)
                         (or (= 200 status)
                             (= 204 status))))
            (async-trigger-terminate-fn router-id service-id request-id))
          (if (and (= request-method :get) (= 303 status) relative-location?)
            (let [result-location (async-req/route-params->uri "/waiter-async/result/" (assoc route-params :location location-url))]
              (assoc-in backend-response [:headers "location"] result-location))
            backend-response)))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn blacklist-instance
  [instance-rpc-chan request]
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
                           :status 400}))
          (let [response-chan (async/promise-chan)
                _ (service/blacklist-instance! instance-rpc-chan service-id instance-id period-in-ms response-chan)
                _ (log/info "Waiting for response from blacklist channel...")
                response-code (async/alt!
                                response-chan ([code] code)
                                (async/timeout (-> 30 t/seconds t/in-millis)) ([_] :timeout)
                                :priority true)
                successful? (= response-code :blacklisted)]
            (cid/cloghelper
              (if successful? :info :warn)
              "Blacklist" instance-id "of" service-id "response:" (name response-code))
            (if successful?
              (do
                (when (= "killed" reason)
                  (-> instance
                      walk/keywordize-keys
                      (update :started-at (fn [started-at]
                                            (when started-at
                                              (du/str-to-date started-at))))
                      scheduler/process-instance-killed!))
                (utils/map->json-response {:instance-id instance-id
                                           :blacklist-period period-in-ms}))
              (let [response-status (if (= :in-use response-code) 423 503)]
                (utils/map->json-response {:message "Unable to blacklist instance."
                                           :instance-id instance-id
                                           :reason response-code}
                                          :status response-status))))))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn get-blacklisted-instances
  "Return the blacklisted instances for a given service-id at this router."
  [instance-rpc-chan service-id request]
  (async/go
    (try
      (when (str/blank? service-id)
        (throw (ex-info "Missing service-id" {:status 400})))
      (let [response-chan (async/promise-chan)
            _ (service/query-instance! instance-rpc-chan service-id response-chan)
            _ (log/info "Waiting for response from query-state channel...")
            current-state (async/alt!
                            response-chan ([state] state)
                            (async/timeout (-> 30 t/seconds t/in-millis)) ([_] {})
                            :priority true)
            blacklisted-instances (vec (keys (:instance-id->blacklist-expiry-time current-state)))]
        (log/info service-id "has" (count blacklisted-instances) "blacklisted instance(s).")
        (utils/map->json-response {:blacklisted-instances blacklisted-instances}))
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
                    :else (metrics/get-metrics))]
      (utils/map->streaming-json-response metrics))
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
      {:body service-id
       :status 200})
    (catch Exception ex
      (utils/exception->response ex request))))

(defn list-services-handler
  "Retrieves the list of services viewable by the currently logged in user.
   A service is viewable by the run-as-user or a waiter super-user."
  [entitlement-manager state-chan prepend-waiter-url service-id->service-description-fn service-id->metrics-fn request]
  (let [timeout-ms 30000
        current-state (async/alt!!
                        state-chan ([state-data] state-data)
                        (async/timeout timeout-ms) ([_] :timeout)
                        :priority true)]
    (if (= :timeout current-state)
      (throw (ex-info "Query for service state timed out" {:status 503}))
      (let [request-params (-> request ru/query-params-request :query-params)
            auth-user (get request :authorization/user)
            run-as-user-param (get request-params "run-as-user")
            viewable-services (filter
                                #(let [{:strs [run-as-user] :as service-description} (service-id->service-description-fn % :effective? false)]
                                   (and service-description
                                        (if run-as-user-param
                                          (= run-as-user run-as-user-param)
                                          (authz/manage-service? entitlement-manager auth-user % service-description))))
                                (->> (concat (keys (:service-id->healthy-instances current-state))
                                             (keys (:service-id->unhealthy-instances current-state)))
                                     (apply sorted-set)))
            retrieve-instance-counts (fn retrieve-instance-counts [service-id]
                                       {:healthy-instances (count (get-in current-state [:service-id->healthy-instances service-id]))
                                        :unhealthy-instances (count (get-in current-state [:service-id->unhealthy-instances service-id]))})
            service-id->metrics (service-id->metrics-fn)
            include-effective-parameters? (utils/request-flag request-params "effective-parameters")
            response-data (map
                            (fn service-id->service-info [service-id]
                              (let [service-description (service-id->service-description-fn service-id :effective? false)]
                                (cond->
                                  {:instance-counts (retrieve-instance-counts service-id)
                                   :last-request-time (get-in service-id->metrics [service-id "last-request-time"])
                                   :service-id service-id
                                   :service-description service-description
                                   :url (prepend-waiter-url (str "/apps/" service-id))}
                                  include-effective-parameters? (assoc :effective-parameters
                                                                       (service-id->service-description-fn
                                                                         service-id :effective? true)))))
                            viewable-services)]
        (utils/map->streaming-json-response response-data)))))

(defn delete-service-handler
  "Deletes the service from the scheduler (after authorization checks)."
  [service-id core-service-description scheduler allowed-to-manage-service?-fn request]
  (let [auth-user (get request :authorization/user)
        run-as-user (get core-service-description "run-as-user")]
    (when-not (allowed-to-manage-service?-fn service-id auth-user)
      (throw (ex-info "User not allowed to delete service"
                      {:existing-owner run-as-user
                       :current-user auth-user
                       :service-id service-id
                       :status 403})))
    (let [delete-result (scheduler/delete-app scheduler service-id)
          response-status (case (:result delete-result)
                            :deleted 200
                            :no-such-service-exists 404
                            400)
          response-body-map (merge {:success (= 200 response-status), :service-id service-id} delete-result)]
      (utils/map->json-response response-body-map :status response-status))))

(defn generate-log-url
  "Generates the log url for an instance"
  [prepend-waiter-url {:keys [directory host id service-id]}]
  (prepend-waiter-url (str "/apps/" service-id "/logs?instance-id=" id "&host=" host
                           (when directory (str "&directory=" directory)))))

(defn- assoc-log-url
  "Appends the :log-url field for an instance"
  [generate-log-url-fn service-instance]
  (assoc service-instance :log-url (generate-log-url-fn service-instance)))

(defn- get-service-handler
  "Returns details about the service such as the service description, metrics, instances, etc."
  [router-id service-id core-service-description scheduler kv-store generate-log-url-fn make-inter-router-requests-fn]
  (let [service-instance-maps (try
                                (let [assoc-log-url-to-instances
                                      (fn assoc-log-url-to-instances [instances]
                                        (when (not-empty instances)
                                          (map #(assoc-log-url generate-log-url-fn %) instances)))]
                                  (-> (scheduler/get-instances scheduler service-id)
                                      (update-in [:active-instances] assoc-log-url-to-instances)
                                      (update-in [:failed-instances] assoc-log-url-to-instances)
                                      (update-in [:killed-instances] assoc-log-url-to-instances)))
                                (catch Exception e
                                  (log/error e "Error in retrieving instances for" service-id)))
        router->metrics (try
                          (let [router->response (-> (make-inter-router-requests-fn (str "metrics?service-id=" service-id) :method :get)
                                                     (assoc router-id (-> (metrics/get-service-metrics service-id)
                                                                          (utils/map->json-response))))
                                response->service-metrics (fn response->metrics [{:keys [body]}]
                                                            (try
                                                              (let [metrics (json/read-str (str body))]
                                                                (get-in metrics ["services" service-id]))
                                                              (catch Exception e
                                                                (log/error e "unable to retrieve metrics from response" (str body)))))
                                router->service-metrics (pc/map-vals response->service-metrics router->response)]
                            (utils/filterm val router->service-metrics))
                          (catch Exception e
                            (log/error e "Error in retrieving router metrics for" service-id)))
        aggregate-metrics-map (try
                                (metrics/aggregate-router-codahale-metrics (or router->metrics {}))
                                (catch Exception e
                                  (log/error e "Error in aggregating router metrics for" service-id)))
        service-description-overrides (try
                                        (sd/service-id->overrides kv-store service-id :refresh true)
                                        (catch Exception e
                                          (log/error e "Error in retrieving service description overrides for" service-id)))
        service-suspended-state (try
                                  (sd/service-id->suspended-state kv-store service-id :refresh true)
                                  (catch Exception e
                                    (log/error e "Error in retrieving service suspended state for" service-id)))
        result-map (walk/stringify-keys
                     (cond-> {:router-id router-id, :num-routers (count router->metrics)}
                             (not-empty service-instance-maps)
                             (assoc :instances service-instance-maps
                                    :num-active-instances (count (:active-instances service-instance-maps)))
                             (not-empty aggregate-metrics-map)
                             (assoc-in [:metrics :aggregate] aggregate-metrics-map)
                             (not-empty router->metrics)
                             (assoc-in [:metrics :routers] router->metrics)
                             (not-empty core-service-description)
                             (assoc :service-description core-service-description)
                             (not-empty (or (:overrides service-description-overrides) {}))
                             (assoc :service-description-overrides service-description-overrides)
                             (:time service-suspended-state)
                             (assoc :service-suspended-state service-suspended-state)))
        sorted-result-map (utils/deep-sort-map result-map)]
    (utils/map->streaming-json-response sorted-result-map)))

(defn service-handler
  "Handles the /apps/<service-id> requests.
   It supports the following request methods:
     :delete deletes the service from the scheduler (after authorization checks).
     :get returns details about the service such as the service description, metrics, instances, etc."
  [router-id service-id scheduler kv-store allowed-to-manage-service?-fn generate-log-url-fn make-inter-router-requests-fn request]
  (try
    (when (not service-id)
      (throw (ex-info "Missing service-id" {:status 400})))
    (let [core-service-description (sd/fetch-core kv-store service-id :refresh true)]
      (if (empty? core-service-description)
        (throw (ex-info "Service not found" {:status 404 :service-id service-id}))
        (case (:request-method request)
          :delete (delete-service-handler service-id core-service-description scheduler allowed-to-manage-service?-fn request)
          :get (get-service-handler router-id service-id core-service-description scheduler kv-store
                                    generate-log-url-fn make-inter-router-requests-fn))))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn- trigger-service-refresh
  "Makes interrouter calls to refresh service caches in kv-store."
  [make-inter-router-requests-fn service-id]
  (make-inter-router-requests-fn (str "apps/" service-id "/refresh") :method :get))

(defn suspend-or-resume-service-handler
  "Suspend/Resume a service after performing run-as-user/waiter-user validation."
  [kv-store allowed-to-manage-service? make-inter-router-requests-fn service-id mode request]
  (try
    (when (str/blank? service-id)
      (throw (ex-info "Missing service-id" {:status 400})))
    ; throw exception if no service description for service-id exists
    (sd/fetch-core kv-store service-id :nil-on-missing? false)
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
            (utils/map->json-response {:action mode-str
                                       :service-id service-id
                                       :success success})))
        (throw (ex-info (str auth-user " not allowed to modify " service-id)
                        {:auth-user auth-user
                         :service-id service-id
                         :status 403}))))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn override-service-handler
  "Handles overrides for a service."
  [kv-store allowed-to-manage-service? make-inter-router-requests-fn service-id {:keys [request-method] :as request}]
  (when (str/blank? service-id)
    (throw (ex-info "Missing service-id" {:status 400})))
  ; throw exception if no service description for service-id exists
  (sd/fetch-core kv-store service-id :nil-on-missing? false)
  (let [auth-user (get request :authorization/user)]
    (case request-method
      :delete
      (do
        (log/info auth-user "wants to delete overrides for" service-id)
        (if (allowed-to-manage-service? service-id auth-user)
          (do
            (sd/clear-service-description-overrides kv-store service-id auth-user)
            (trigger-service-refresh make-inter-router-requests-fn service-id)
            (utils/map->json-response {:service-id service-id, :success true}))
          (throw (ex-info (str auth-user " not allowed to override " service-id)
                          {:auth-user auth-user, :service-id service-id, :status 403}))))

      :get
      (if (allowed-to-manage-service? service-id auth-user)
        (-> (sd/service-id->overrides kv-store service-id :refresh true)
            (assoc :service-id service-id)
            utils/map->json-response)
        (throw (ex-info (str auth-user " not allowed view override information of " service-id)
                        {:auth-user auth-user, :service-id service-id, :status 403})))

      :post
      (do
        (log/info auth-user "wants to update overrides for" service-id)
        (if (allowed-to-manage-service? service-id auth-user)
          (do
            (let [service-description-overrides (-> request ru/json-request :body)]
              (sd/store-service-description-overrides kv-store service-id auth-user service-description-overrides))
            (trigger-service-refresh make-inter-router-requests-fn service-id)
            (utils/map->json-response {:service-id service-id, :success true}))
          (throw (ex-info (str auth-user " not allowed to override " service-id)
                          {:auth-user auth-user, :service-id service-id, :status 403}))))

      (throw (ex-info "Unsupported request method" {:request-method request-method, :status 405})))))

(defn service-view-logs-handler
  "Redirects user to the log directory on the slave"
  [scheduler service-id generate-log-url-fn request]
  (try
    (let [{:strs [instance-id host directory]} (-> request ru/query-params-request :query-params)
          _ (when-not instance-id
              (throw (ex-info "Missing instance-id parameter" {:status 400})))
          _ (when-not host
              (throw (ex-info "Missing host parameter" {:status 400})))
          directory-content (map (fn [{:keys [path type] :as entry}]
                                   (if (= type "file")
                                     entry
                                     (-> (dissoc entry :path)
                                         (assoc :url (generate-log-url-fn {:directory path
                                                                           :host host
                                                                           :id instance-id
                                                                           :service-id service-id})))))
                                 (scheduler/retrieve-directory-content scheduler service-id instance-id host directory))]
      (utils/map->json-response (vec directory-content)))
    (catch Exception ex
      (utils/exception->response ex request))))

(defn work-stealing-handler
  "Handles work-stealing offers of instances for load-balancing work on the current router."
  [instance-rpc-chan request]
  (async/go
    (try
      (let [{:keys [cid instance request-id router-id service-id] :as request-body-map}
            (-> request ru/json-request :body walk/keywordize-keys)]
        (log/info "received work-stealing offer" (:id instance) "of" service-id "from" router-id)
        (if-not (and cid instance request-id router-id service-id)
          (throw (ex-info "Missing one of cid, instance, request-id, router-id or service-id"
                          (assoc request-body-map :status 400)))
          (let [response-chan (async/promise-chan)
                offer-params {:cid cid
                              :instance (scheduler/make-ServiceInstance instance)
                              :request-id request-id
                              :response-chan response-chan
                              :router-id router-id
                              :service-id service-id}]
            (service/offer-instance! instance-rpc-chan service-id offer-params)
            (let [response-status (async/<! response-chan)]
              (utils/map->json-response (assoc (select-keys offer-params [:cid :request-id :router-id :service-id])
                                          :response-status response-status))))))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn- retrieve-channel-state
  "Retrieves the state reported on the channel."
  [state-chan]
  (async/go
    (let [timeout-chan (-> 30 t/seconds t/in-millis async/timeout)]
      (first (async/alts! [state-chan timeout-chan] :priority true)))))

(defn get-router-state
  "Outputs the state of the router as json."
  [router-id state-chan request]
  (async/go
    (try
      (let [routers (-> state-chan retrieve-channel-state async/<! :routers)
            host (get-in request [:headers "host"])
            scheme (some-> request utils/request->scheme name)
            make-url (fn make-url [path]
                       (str (when scheme (str scheme "://")) host "/state/" path))]
        (-> {:details (->> ["fallback" "interstitial" "kv-store" "leader" "local-usage" "maintainer"
                            "router-metrics" "scheduler" "statsd"]
                           (pc/map-from-keys make-url))
             :router-id router-id
             :routers routers}
            (utils/map->streaming-json-response)))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn get-chan-latest-state-handler
  "Outputs the latest state of the channel."
  [router-id state-chan request]
  (async/go
    (try
      (log/info (str "Waiting for response from state channel"))
      (let [data (-> state-chan retrieve-channel-state async/<!)
            state (or data {:message "Request timeout"})]
        (-> {:router-id router-id :state state}
            (utils/map->streaming-json-response)))
      (catch Exception ex
        (utils/exception->response ex request)))))

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
          (-> {:router-id router-id :state state}
              (utils/map->streaming-json-response))))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn- get-function-state
  "Outputs the state obtained by invoking `retrieve-state-fn`."
  [retrieve-state-fn router-id request]
  (try
    (-> {:router-id router-id
         :state (retrieve-state-fn)}
        (utils/map->streaming-json-response))
    (catch Exception ex
      (utils/exception->response ex request))))

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
  (-> (fn leader-state-fn []
        {:leader? (leader?-fn)
         :leader-id (leader-id-fn)})
      (get-function-state router-id request)))

(defn get-router-metrics-state
  "Outputs the router metrics state."
  [router-id router-metrics-state-fn request]
  (get-function-state router-metrics-state-fn router-id request))

(defn get-statsd-state
  "Outputs the statsd state."
  [router-id request]
  (get-function-state statsd/state router-id request))

(defn get-service-state
  "Retrieves the state for a particular service on the router."
  [router-id instance-rpc-chan local-usage-agent service-id query-chans request]
  (async/go
    (try
      (if (str/blank? service-id)
        (throw (ex-info "Missing service-id" {:status 400}))
        (let [timeout-ms (-> 10 t/seconds t/in-millis)
              _ (log/info "waiting for response from query-state channel...")
              responder-state-chan
              (service/query-maintainer-channel-map-with-timeout! instance-rpc-chan service-id timeout-ms :query-state)
              _ (log/info "waiting for response from query-work-stealing channel...")
              work-stealing-state-chan
              (service/query-maintainer-channel-map-with-timeout! instance-rpc-chan service-id timeout-ms :query-work-stealing)
              local-usage-state (get @local-usage-agent service-id)
              [query-chans initial-result]
              (loop [[[entry-key entry-value] & remaining] [[:responder-state responder-state-chan]
                                                            [:work-stealing-state work-stealing-state-chan]]
                     query-chans query-chans
                     initial-result {:local-usage local-usage-state}]
                (if entry-key
                  (if (map? entry-value)
                    (recur remaining query-chans (assoc initial-result entry-key entry-value))
                    (recur remaining (assoc query-chans entry-key entry-value) initial-result))
                  [query-chans initial-result]))
              query-chans-state (loop [[[key query-response-or-chan] & remaining] (seq query-chans)
                                       result initial-result]
                                  (if (and key query-response-or-chan)
                                    (let [state (let [response-chan (async/promise-chan)]
                                                  (async/>! query-response-or-chan
                                                            {:cid (cid/get-correlation-id)
                                                             :response-chan response-chan
                                                             :service-id service-id})
                                                  (log/info (str "Waiting on response on " key " channel"))
                                                  (async/alt!
                                                    response-chan ([state] state)
                                                    (async/timeout timeout-ms) ([_] {:message "Request timeout"})))]
                                      (recur remaining (assoc result key state)))
                                    result))]
          (utils/map->streaming-json-response {:router-id router-id, :state (utils/deep-sort-map query-chans-state)})))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn acknowledge-consent-handler
  "Processes the acknowledgment to launch a service as the auth-user.
   It triggers storing of the x-waiter-consent cookie on the client."
  [token->service-description-template token->token-metadata service-description->service-id
   consent-cookie-value add-encoded-cookie consent-expiry-days {:keys [request-method] :as request}]
  (try
    (when-not (= :post request-method)
      (throw (ex-info "Only POST supported" {:request-method request-method, :status 405})))
    (let [{:keys [headers params] :as request} (multipart-params/multipart-params-request request)
          {:strs [host origin referer x-requested-with]} headers
          {:strs [mode service-id] :as params} params]
      (when-not (str/blank? origin)
        (when-not (utils/same-origin request)
          (throw (ex-info "Origin is not the same as the host"
                          {:host host
                           :origin origin
                           :status 400}))))
      (when (and (not (str/blank? origin)) (not (str/blank? referer)))
        (when-not (str/starts-with? referer origin)
          (throw (ex-info "Referer does not start with origin"
                          {:origin origin
                           :referer referer
                           :status 400}))))
      (when-not (= x-requested-with "XMLHttpRequest")
        (throw (ex-info "Header x-requested-with does not match expected value"
                        {:actual x-requested-with
                         :expected "XMLHttpRequest"
                         :status 400})))
      (when-not (and mode (contains? #{"service" "token"} mode))
        (throw (ex-info "Missing or invalid mode" (assoc params :status 400))))
      (when (= "service" mode)
        (when-not service-id
          (throw (ex-info "Missing service-id" (assoc params :status 400)))))
      (let [token (utils/authority->host host)
            service-description-template (token->service-description-template token)]
        (when-not (seq service-description-template)
          (throw (ex-info "Unable to load description for token" {:token token :status 400})))
        (when (= "service" mode)
          (let [auth-user (:authorization/user request)
                computed-service-id (-> service-description-template
                                        (sd/assoc-run-as-requester-fields auth-user)
                                        service-description->service-id)]
            (when-not (= service-id computed-service-id)
              (log/error "computed" computed-service-id ", but user[" auth-user "] provided" service-id "for" token)
              (throw (ex-info "Invalid service-id for specified token" (assoc params :status 400))))))
        (let [token-metadata (token->token-metadata token)
              cookie-name "x-waiter-consent"
              cookie-value (consent-cookie-value mode service-id token token-metadata)]
          (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "approve-success"))
          (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "approve-success"))
          (-> {:body (str "Added cookie " cookie-name), :headers {}, :status 200}
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
      (throw (ex-info "Only GET supported" {:request-method request-method, :status 405})))
    (let [host-header (get headers "host")
          token (utils/authority->host host-header)
          {:keys [path]} route-params
          {:strs [interstitial-secs] :as service-description-template} (token->service-description-template token)]
      (when-not (seq service-description-template)
        (throw (ex-info "Unable to load description for token" {:token token :status 404})))
      (let [auth-user (:authorization/user request)
            service-id (-> service-description-template
                           (sd/assoc-run-as-requester-fields auth-user)
                           service-description->service-id)
            query-string' (str (when (not (str/blank? query-string)) query-string)
                               (when (some-> interstitial-secs pos?)
                                 ;; add the bypass query param only if interstitial is enabled
                                 (str (when (not (str/blank? query-string)) "&")
                                      (interstitial/request-time->interstitial-param-string request-time))))]
        (counters/inc! (metrics/waiter-counter "auto-run-as-requester" "form-render"))
        (meters/mark! (metrics/waiter-meter "auto-run-as-requester" "form-render"))
        {:body (render-consent-template
                 {:auth-user auth-user
                  :consent-expiry-days consent-expiry-days
                  :service-description-template service-description-template
                  :service-id service-id
                  :target-url (str (name (utils/request->scheme request)) "://" host-header "/" path
                                   (when (not (str/blank? query-string')) (str "?" query-string')))
                  :token token})
         :headers {"content-type" "text/html"}
         :status 200}))
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
        :get {:body (case content-type
                      "application/json" (json/write-str welcome-info)
                      "text/html" (render-welcome-html welcome-info)
                      "text/plain" (render-welcome-text welcome-info))
              :headers {"content-type" content-type}}
        (throw (ex-info "Only GET supported" {:status 405})))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn not-found-handler
  "Responds with a handler indicating a resource isn't found."
  [request]
  (utils/exception->response (ex-info (utils/message :not-found) {:status 404}) request))
