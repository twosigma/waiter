;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
;; This namespace will act as an asylum for floating handlers.
(ns waiter.handler
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metrics.counters :as counters]
            [plumbing.core :as pc]
            [ring.middleware.params :as ring-params]
            [waiter.async-request :as async-req]
            [waiter.correlation-id :as cid]
            [waiter.headers :as headers]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.service-description :as sd]
            [waiter.statsd :as statsd]
            [waiter.utils :as utils]))

(defn- async-make-http-request
  "Helper function for async status/result handlers."
  [counter-name make-http-request-fn {:keys [body headers query-string request-method route-params uri] :as request}]
  (let [{:keys [host location port request-id router-id service-id]} route-params]
    (when-not (and host location port request-id router-id service-id)
      (throw (ex-info "Missing host, location, port, request-id, router-id or service-id in uri!"
                      {:route-params route-params, :uri uri, :status 400})))
    (counters/inc! (metrics/service-counter service-id "request-counts" counter-name))
    (let [target-location (scheduler/end-point-url {:host host, :port port}
                                                   (cond-> location (not (str/blank? query-string)) (str "?" query-string)))
          _ (log/info request-id counter-name "location is" target-location)
          auth-user (:authorization/user request)
          {:keys [passthrough-headers]} (headers/split-headers headers)]
      (make-http-request-fn service-id target-location auth-user request-method passthrough-headers body))))

(defn complete-async-handler
  "Completes execution of an async request by propagating a termination message to the request monitor system."
  [async-request-terminate-fn src-router-id {:keys [route-params uri]}]
  (try
    (let [{:keys [request-id service-id]} route-params]
      (when (str/blank? service-id)
        (throw (ex-info "No service-id specified!" {:src-router-id src-router-id, :status 400, :uri uri})))
      (when (str/blank? request-id)
        (throw (ex-info "No request-id specified!" {:src-router-id src-router-id, :status 400, :uri uri})))
      (let [succeeded (async-request-terminate-fn request-id)]
        (utils/map->json-response {:request-id request-id, :success succeeded})))
    (catch Exception ex
      (utils/exception->json-response ex))))

(defn async-result-handler
  "Result handler for async requests. Supports any http method.
   The router delegates the call to the backend and it notifies the 'host' router to treat the async request as complete."
  [async-trigger-terminate-fn make-http-request-fn {:keys [request-method route-params] :as request}]
  (async/go
    (try
      (let [{:keys [request-id router-id service-id]} route-params
            {:keys [error status] :as backend-response}
            (async/<! (async-make-http-request "async-result" make-http-request-fn request))]
        (log/info "http" request-method "returned status" status)
        (async-trigger-terminate-fn router-id service-id request-id)
        (when error (throw error))
        backend-response)
      (catch Exception ex
        (log/error ex "error in retrieving result of async request")
        (utils/exception->json-response ex)))))

(defn async-status-handler
  "Status handler for async requests. Supports both delete and get http methods.
   The status checks are 'host' router, i.e. the router that processed the original async request, independent.
   The router delegates the call to the backend and intercepts the response to know when the async request has completed.
   If the router determines that the request has completed, it notifies the 'host' router to treat the async request as complete."
  [async-trigger-terminate-fn make-http-request-fn {:keys [request-method route-params] :as request}]
  (async/go
    (try
      (let [{:keys [host location port request-id router-id service-id]} route-params
            {:keys [error status] :as backend-response}
            (async/<! (async-make-http-request "async-status" make-http-request-fn request))]
        (when error (throw error))
        (log/info "http" (name request-method) "returned status code" status)
        (let [endpoint (scheduler/end-point-url {:host host, :port port} location)
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
              (log/info request-id "async result absolute url is" result-location)
              (assoc-in backend-response [:headers "location"] result-location))
            backend-response)))
      (catch Exception ex
        (log/error ex "error in querying status of async request")
        (utils/exception->json-response ex)))))

(defn blacklist-instance
  [instance-rpc-chan request]
  (async/go
    (try
      (let [request-body (slurp (:body request))
            {:strs [instance period-in-ms reason] :as request-body-map} (json/read-str request-body)
            instance-id (get instance "id")
            service-id (get instance "service-id")]
        (if (or (str/blank? reason)
                (str/blank? instance-id)
                (str/blank? service-id)
                (or (not (integer? period-in-ms)) (neg? period-in-ms)))
          (utils/map->json-response {:message "Must provide the service-id, the instance id, the reason, and a positive period"
                                     :input-data request-body-map}
                                    :status 400)
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
                  (scheduler/process-instance-killed! (walk/keywordize-keys instance)))
                (utils/map->json-response {:instance-id instance-id
                                           :blacklist-period period-in-ms}))
              (let [response-status (if (= :in-use response-code) 423 503)]
                (utils/map->json-response {:message "Unable to blacklist instance."
                                           :instance-id instance-id
                                           :reason response-code}
                                          :status response-status))))))
      (catch Exception e
        (utils/exception->response "Blacklisting instance failed." e :status 500)))))

(defn get-blacklisted-instances
  "Return the blacklisted instances for a given service-id at this router."
  [instance-rpc-chan service-id]
  (async/go
    (try
      (when (str/blank? service-id)
        (throw (ex-info "Missing service-id!" {})))
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
      (catch Exception e
        (utils/map->json-response {:error (utils/exception->strs e)} :status 400)))))

(defn metrics-request-handler
  "Retrieves the codahale metrics for a service-id present at this router."
  [request]
  (try
    (let [request-params (:params (ring-params/params-request request))
          exclude-services (Boolean/parseBoolean (get request-params "exclude-services" "false"))
          service-id (get request-params "service-id" nil)
          metrics (cond
                    exclude-services (metrics/get-waiter-metrics)
                    (and (not exclude-services) service-id) (metrics/get-service-metrics service-id)
                    :else (metrics/get-metrics))]
      (utils/map->json-response metrics))
    (catch Exception e
      (utils/exception->json-response e :status 500))))

(defn service-name-handler
  "Retrieves the app-name of the service specified by the request."
  [request request->descriptor-fn kv-store store-service-description-fn]
  (try
    (let [{:keys [service-id core-service-description]} (request->descriptor-fn request)]
      (when (not= core-service-description (sd/fetch-core kv-store service-id))
        ; eagerly store the service description for this service-id
        (store-service-description-fn service-id core-service-description))
      {:body service-id
       :status 200})
    (catch Exception e
      (utils/exception->response "Error locating app name" e))))

(defn list-services-handler
  "Retrieves the list of services viewable by the currently logged in user.
   A service is viewable by the run-as-user or a waiter super-user."
  [can-run-as? state-chan prepend-waiter-url service-id->service-description-fn request]
  (try
    (let [timeout-ms 30000
          current-state (async/alt!!
                          state-chan ([state-data] state-data)
                          (async/timeout timeout-ms) ([_] :timeout)
                          :priority true)]
      (if (= :timeout current-state)
        (utils/map->json-response {"message" "Timeout in retrieving services"})
        (let [request-params (:params (ring-params/params-request request))
              auth-user (get request :authorization/user)
              run-as-user-filter (get request-params "run-as-user" auth-user)
              viewable-services (filter
                                  #(let [service-description (service-id->service-description-fn % :effective? false)]
                                     (can-run-as? run-as-user-filter (get service-description "run-as-user")))
                                  (->> (concat (keys (:service-id->healthy-instances current-state))
                                               (keys (:service-id->unhealthy-instances current-state)))
                                       (apply sorted-set)))
              retrieve-instance-counts (fn retrieve-instance-counts [service-id]
                                         {:healthy-instances (count (get-in current-state [:service-id->healthy-instances service-id]))
                                          :unhealthy-instances (count (get-in current-state [:service-id->unhealthy-instances service-id]))})
              include-effective-parameters? (utils/request-flag request-params "effective-parameters")
              response-data (map
                              (fn service-id->service-info [service-id]
                                (let [service-description (service-id->service-description-fn service-id :effective? false)]
                                  (cond->
                                    {:service-id service-id
                                     :service-description service-description
                                     :instance-counts (retrieve-instance-counts service-id)
                                     :url (prepend-waiter-url (str "/apps/" service-id))}
                                    include-effective-parameters? (assoc :effective-parameters
                                                                         (service-id->service-description-fn
                                                                           service-id :effective? true)))))
                              viewable-services)]
          (utils/map->json-response response-data))))
    (catch Exception e
      (utils/exception->response "Error retrieving services" e))))

(defn delete-service-handler
  "Deletes the service from the scheduler (after authorization checks)."
  [service-id core-service-description scheduler can-run-as? request]
  (let [auth-user (get request :authorization/user)
        run-as-user (get core-service-description "run-as-user")]
    (when-not (can-run-as? auth-user run-as-user)
      (throw (ex-info "User not allowed to delete service"
                      {:existing-owner run-as-user
                       :current-user auth-user
                       :service-id service-id
                       :status 403})))
    (let [delete-result (scheduler/delete-app scheduler service-id)
          response-status (cond
                            (:deploymentId delete-result) 200
                            (= :no-such-service-exists (:result delete-result)) 404
                            :else 400)
          response-body-map (merge {:success (= 200 response-status), :service-id service-id} delete-result)]
      (utils/map->json-response response-body-map :status response-status))))

(defn generate-log-url
  "Generates the log url for an instance"
  [prepend-waiter-url {:keys [directory host id service-id]}]
  (prepend-waiter-url (str "/apps/" service-id "/logs?instance-id=" id "&host=" host
                           (when directory (str "&directory=" directory)))))

(defn- assoc-log-url
  "Appends the :log-url field for an instance"
  [prepend-waiter-url service-instance]
  (assoc service-instance :log-url (generate-log-url prepend-waiter-url service-instance)))

(defn- get-service-handler
  "Returns details about the service such as the service description, metrics, instances, etc."
  [router-id service-id core-service-description scheduler kv-store prepend-waiter-url make-inter-router-requests-fn]
  (let [service-instance-maps (try
                                (let [assoc-log-url-to-instances
                                      (fn assoc-log-url-to-instances [instances]
                                        (when (not-empty instances)
                                          (map #(assoc-log-url prepend-waiter-url %1) instances)))]
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
                                (metrics/aggregate-router-data (or router->metrics {}))
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
                             (update-in [:metrics :aggregate] (fn [_] aggregate-metrics-map))
                             (not-empty router->metrics)
                             (update-in [:metrics :routers] (fn [_] router->metrics))
                             (not-empty core-service-description)
                             (assoc :service-description core-service-description)
                             (not-empty (or (:overrides service-description-overrides) {}))
                             (assoc :service-description-overrides service-description-overrides)
                             (:time service-suspended-state)
                             (assoc :service-suspended-state service-suspended-state)))
        sorted-result-map (utils/deep-sort-map result-map)]
    (utils/map->json-response sorted-result-map)))

(defn service-handler
  "Handles the /apps/<service-id> requests.
   It supports the following request methods:
     :delete deletes the service from the scheduler (after authorization checks).
     :get returns details about the service such as the service description, metrics, instances, etc."
  [router-id service-id scheduler kv-store can-run-as? prepend-waiter-url make-inter-router-requests-fn request]
  (try
    (when (not service-id)
      (throw (ex-info (str "Service id is missing!") {})))
    (let [core-service-description (try
                                     (sd/fetch-core kv-store service-id :refresh true)
                                     (catch Exception e
                                       (log/error e "Error in retrieving service description for" service-id)))]
      (if (empty? core-service-description)
        (utils/map->json-response {:message (str "No service description found: " service-id)} :status 404)
        (case (:request-method request)
          :delete (delete-service-handler service-id core-service-description scheduler can-run-as? request)
          :get (get-service-handler router-id service-id core-service-description scheduler kv-store
                                    prepend-waiter-url make-inter-router-requests-fn))))
    (catch Exception e
      (utils/exception->json-response e))))

(defn- trigger-service-refresh
  "Makes interrouter calls to refresh service caches in kv-store."
  [make-inter-router-requests-fn service-id]
  (make-inter-router-requests-fn (str "apps/" service-id "/refresh") :method :get))

(defn suspend-or-resume-service-handler
  "Suspend/Resume a service after performing run-as-user/waiter-user validation."
  [kv-store allowed-to-manage-service? make-inter-router-requests-fn service-id mode request]
  (try
    (when (str/blank? service-id)
      (throw (ex-info "Missing service id!" {})))
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
        (let [message (str auth-user " not allowed to modify " service-id)]
          (log/info message)
          (utils/map->json-response {:message message, :success false} :status 403))))
    (catch Exception e
      (utils/exception->response "Error in suspending/resuming service." e :status 500))))

(defn override-service-handler
  "Handles overrides for a service."
  [kv-store allowed-to-manage-service? make-inter-router-requests-fn service-id request]
  (try
    (when (str/blank? service-id)
      (throw (ex-info "Missing service id!" {})))
    ; throw exception if no service description for service-id exists
    (sd/fetch-core kv-store service-id :nil-on-missing? false)
    (case (:request-method request)
      :delete
      (let [auth-user (get request :authorization/user)]
        (log/info auth-user "wants to delete overrides for" service-id)
        (if (allowed-to-manage-service? service-id auth-user)
          (do
            (sd/clear-service-description-overrides kv-store service-id auth-user)
            (trigger-service-refresh make-inter-router-requests-fn service-id)
            (utils/map->json-response {:service-id service-id, :success true}))
          (let [message (str auth-user " not allowed to override " service-id)]
            (log/info message)
            (utils/map->json-response {:message message, :success false} :status 403))))
      :post
      (let [auth-user (get request :authorization/user)]
        (log/info auth-user "wants to update overrides for" service-id)
        (if (allowed-to-manage-service? service-id auth-user)
          (do
            (let [service-description-overrides (json/read-str (slurp (:body request)))]
              (sd/store-service-description-overrides kv-store service-id auth-user service-description-overrides))
            (trigger-service-refresh make-inter-router-requests-fn service-id)
            (utils/map->json-response {:service-id service-id, :success true}))
          (let [message (str auth-user " not allowed to override " service-id)]
            (log/info message)
            (utils/map->json-response {:message message, :success false} :status 403)))))
    (catch Exception e
      (utils/exception->response (str "Error in modifying overrides for " service-id) e :status 500))))

(defn service-view-logs-handler
  "Redirects user to the log directory on the slave"
  [scheduler service-id prepend-waiter-url request]
  (try
    (let [{:strs [instance-id host directory]} (:params (ring-params/params-request request))
          directory-content (map (fn [{:keys [path type] :as entry}]
                                   (if (= type "file")
                                     entry
                                     (-> (dissoc entry :path)
                                         (assoc :url (generate-log-url prepend-waiter-url
                                                                       {:directory path
                                                                        :host host
                                                                        :id instance-id
                                                                        :service-id service-id})))))
                                 (scheduler/retrieve-directory-content scheduler service-id instance-id host directory))]
      (utils/map->json-response (vec directory-content)))
    (catch Exception e
      (utils/exception->json-response e :status 500))))

(defn work-stealing-handler
  "Handles work-stealing offers of instances for load-balancing work on the current router."
  [instance-rpc-chan request]
  (async/go
    (try
      (let [{:keys [cid instance request-id router-id service-id] :as request-body-map}
            (-> request (:body) (slurp) (json/read-str) (walk/keywordize-keys))]
        (log/info "received work-stealing offer" (:id instance) "of" service-id "from" router-id)
        (if-not (and cid instance request-id router-id service-id)
          (let [ex (ex-info "Missing one of cid, instance, request-id, router-id or service-id!" request-body-map)]
            (utils/map->json-response {:error (utils/exception->strs ex)} :status 400))
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
      (catch Exception e
        (utils/exception->json-response e :status 500)))))

(defn thread-dump-handler
  "Perform health check on the current router.
   It includes tracking state of running threads."
  [retrieve-stale-thread-stack-trace-data request]
  (try
    (let [request-params (:params (ring-params/params-request request))
          excluded-methods (remove str/blank? (str/split (get request-params "excluded-methods" "") #","))
          stale-threshold-ms (-> (get request-params "stale-threshold-ms") (or "0") (Integer/parseInt))
          thread-data (retrieve-stale-thread-stack-trace-data excluded-methods stale-threshold-ms)]
      (utils/map->json-response (utils/deep-sort-map thread-data)))
    (catch Exception e
      (log/error e "Error in retrieving thread dump data")
      (utils/exception->json-response e :status 500))))

(defn get-router-state
  "Outputs the state of the router as json."
  [state-chan router-metrics-state-fn kv-store]
  (try
    (let [timeout-ms 30000
          current-state (async/alt!!
                          state-chan ([state-data] state-data)
                          (async/timeout timeout-ms) ([_] {:message "Request timed out!"})
                          :priority true)]
      (-> current-state
          (assoc :kv-store (kv/state kv-store)
                 :router-metrics-state (router-metrics-state-fn)
                 :statsd (statsd/state))
          (utils/map->json-response)))
    (catch Exception e
      (log/error e "Error getting router state")
      (utils/exception->json-response e :status 500))))

(defn get-service-state
  "Retrieves the state for a particular service on the router."
  [router-id instance-rpc-chan service-id query-chans]
  (if (str/blank? service-id)
    (let [ex (IllegalArgumentException. "Missing service-id!")]
      (utils/map->json-response {:error (utils/exception->strs ex)} :status 400))
    (async/go
      (try
        (let [timeout-ms (-> 10 t/seconds t/in-millis)
              _ (log/info "waiting for response from query-state channel...")
              responder-state-chan (service/query-maintainer-channel-map-with-timeout! instance-rpc-chan service-id timeout-ms :query-state)
              _ (log/info "waiting for response from query-work-stealing channel...")
              work-stealing-state-chan (service/query-maintainer-channel-map-with-timeout! instance-rpc-chan service-id timeout-ms :query-work-stealing)
              [query-chans initial-result]
              (loop [[[entry-key entry-value] & remaining] [[:responder-state responder-state-chan]
                                                            [:work-stealing-state work-stealing-state-chan]]
                     query-chans query-chans
                     initial-result {}]
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
          (utils/map->json-response {:router-id router-id, :state (utils/deep-sort-map query-chans-state)}))
        (catch Exception e
          (utils/map->json-response {:error (utils/exception->strs e)} :status 500))))))

