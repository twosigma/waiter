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
(ns waiter.scheduler.marathon
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.memoize :as memo]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.correlation-id :as cid]
            [waiter.mesos.marathon :as marathon]
            [waiter.mesos.mesos :as mesos]
            [waiter.util.http-utils :as http-utils]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (org.joda.time.format DateTimeFormat)))

(def formatter-marathon (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

(defn- remove-slash-prefix
  "Returns the input string after stripping out any preceding slashes."
  [^String service-id]
  (if (and service-id (str/starts-with? service-id "/")) (subs service-id 1) service-id))

(defn preserve-only-failed-instances-for-services!
  "Removes failed instance entries for services that no longer exist based on `service-ids-to-keep`."
  [service-id->failed-instances-transient-store service-ids-to-keep]
  (swap! service-id->failed-instances-transient-store #(select-keys % service-ids-to-keep)))

(defn remove-failed-instances-for-service!
  "Removes failed instance entries for the specified service."
  [service-id->failed-instances-transient-store service-id]
  (swap! service-id->failed-instances-transient-store #(dissoc % service-id)))

(defn service-id->failed-instances
  "Return the known list of failed service instances for a given service."
  [service-id->failed-instances-transient-store service-id]
  (vec (get @service-id->failed-instances-transient-store service-id [])))

(defn parse-and-store-failed-instance!
  "Parses the failed instance marathon response and adds it to the known set of failed instances."
  [service-id->failed-instances-transient-store service-id failed-marathon-task common-extractor-fn]
  (when failed-marathon-task
    (let [failed-instance (scheduler/make-ServiceInstance
                            (let [instance-id (remove-slash-prefix (:taskId failed-marathon-task))]
                              (merge
                                (common-extractor-fn instance-id failed-marathon-task)
                                {:id instance-id
                                 :started-at (some-> failed-marathon-task :timestamp (du/str-to-date formatter-marathon))
                                 :healthy? false
                                 :port 0})))
          max-instances-to-keep 10]
      (scheduler/add-instance-to-buffered-collection!
        service-id->failed-instances-transient-store max-instances-to-keep service-id failed-instance
        (fn [] #{}) (fn [instances] (-> (scheduler/sort-instances instances) (rest) (set)))))))

(defn- get-deployment-info
  "Extracts the deployments section from the response body if it exists."
  [{:keys [body]}]
  (-> (if (au/chan? body)
        (async/<!! body)
        body)
      str
      json/read-str
      (get "deployments")
      not-empty))

(defn extract-deployment-info
  "If the response body has deployments listed, pings Marathon to get the deployment details."
  [marathon-api response]
  (try
    (when-let [deployments (get-deployment-info response)]
      (let [deployment-ids (->> deployments
                                (map #(get % "id"))
                                set)]
        (->> (marathon/get-deployments marathon-api)
             (filter (fn [{:keys [id]}] (contains? deployment-ids id)))
             vec)))
    (catch Exception e
      (log/error e "unable to extract the deployment info"))))

(defn extract-service-deployment-info
  "Pings Marathon to get the deployment details and return the first deployment that affects service-id."
  [marathon-api service-id]
  (try
    (->> (marathon/get-deployments marathon-api)
         (filter (fn service-deployment-info-filter-pred [{:keys [affectedApps]}]
                   (some (fn service-deployment-info-some-pred [affected-app]
                           (= (remove-slash-prefix affected-app) service-id))
                         affectedApps)))
         first)
    (catch Exception e
      (log/error e "unable to extract the deployment info for" service-id))))

(defn process-kill-instance-request
  "Processes a kill instance request"
  [marathon-api service-id instance-id {:keys [force scale] :or {force false, scale true} :as kill-params}]
  (scheduler/retry-on-transient-server-exceptions
    "kill-instance"
    (letfn [(make-kill-response [killed? message status]
              {:instance-id instance-id, :killed? killed?, :message message, :service-id service-id, :status status})]
      (ss/try+
        (log/debug "killing instance" instance-id "from service" service-id)
        (let [result (marathon/kill-task marathon-api service-id instance-id scale force)
              kill-success? (and result (map? result) (contains? result :deploymentId))]
          (log/info "kill instance" instance-id "result" result)
          (let [message (if kill-success? "Successfully killed instance" "Unable to kill instance")
                status (if kill-success? 200 500)]
            (make-kill-response kill-success? message status)))
        (catch [:status 409] e
          (log/info "kill instance" instance-id "failed as it is locked by one or more deployments"
                    {:deployment-info (extract-deployment-info marathon-api e)
                     :kill-params kill-params})
          (make-kill-response false "Locked by one or more deployments" 409))
        (catch map? {:keys [body status]}
          (log/info "kill instance" instance-id "returned" status body)
          (make-kill-response false (str body) (or status 500)))
        (catch Throwable e
          (log/info e "exception thrown when calling kill-instance")
          (make-kill-response false (str (.getMessage e)) (:status e 500)))))))

(defn response-data->service-instances
  "Extracts the list of instances for a given app from the marathon response."
  [marathon-response service-keys retrieve-framework-id-fn mesos-api service-id->failed-instances-transient-store
   service-id->service-description]
  (let [service-id (remove-slash-prefix (get-in marathon-response (conj service-keys :id)))
        {:strs [backend-proto]} (service-id->service-description service-id)
        framework-id (retrieve-framework-id-fn)
        common-extractor-fn (fn [instance-id marathon-task-response]
                              (let [{:keys [appId host message slaveId]} marathon-task-response
                                    log-directory (mesos/build-sandbox-path mesos-api slaveId framework-id instance-id)]
                                (cond-> {:host host
                                         :protocol backend-proto
                                         :service-id (remove-slash-prefix appId)}
                                        log-directory
                                        (assoc :log-directory log-directory)

                                        message
                                        (assoc :message (str/trim message))

                                        (str/includes? (str message) "Memory limit exceeded:")
                                        (assoc :flags #{:memory-limit-exceeded})

                                        (str/includes? (str message) "Task was killed since health check failed")
                                        (assoc :flags #{:never-passed-health-checks})

                                        (str/includes? (str message) "Command exited with status")
                                        (assoc :exit-code (try (-> message (str/split #"\s+") last Integer/parseInt)
                                                               (catch Throwable _))))))
        healthy?-fn #(let [health-checks (:healthCheckResults %)]
                       (and
                         (and (seq health-checks)
                              (every? :alive health-checks))
                         (every?
                           (fn [hc]
                             (zero? (:consecutiveFailures hc))) health-checks)))
        active-marathon-tasks (get-in marathon-response (conj service-keys :tasks))
        active-instances (map
                           #(scheduler/make-ServiceInstance
                              (let [instance-id (remove-slash-prefix (:id %))]
                                (merge
                                  (common-extractor-fn instance-id %)
                                  {:id instance-id
                                   :started-at (some-> % :startedAt (du/str-to-date formatter-marathon))
                                   :healthy? (healthy?-fn %)
                                   ;; first port must be used for the web server, extra ports can be used freely.
                                   :port (-> % :ports first)
                                   :extra-ports (-> % :ports rest vec)})))
                           active-marathon-tasks)]
    (parse-and-store-failed-instance!
      service-id->failed-instances-transient-store
      service-id (get-in marathon-response (conj service-keys :lastTaskFailure)) common-extractor-fn)
    {:active-instances active-instances
     :failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
     :killed-instances (scheduler/service-id->killed-instances service-id)}))

(defn- app->waiter-service-id
  "Given a Marathon app, returns the Waiter service id for that app"
  [app]
  (remove-slash-prefix (:id app)))

(defn- response->Service
  "Converts the app entry from a marathon response into a Service object"
  [response]
  (scheduler/->Service
    (app->waiter-service-id response)
    (:instances response)
    (count (:tasks response))
    {:running (or (:tasksRunning response) 0)
     :healthy (or (:tasksHealthy response) 0)
     :unhealthy (or (:tasksUnhealthy response) 0)
     :staged (or (:tasksStaged response) 0)}))

(defn response-data->service->service-instances
  "Extracts the list of instances for a given app from the marathon apps-list."
  [apps-list retrieve-framework-id-fn mesos-api service-id->failed-instances-transient-store service-id->service-description]
  (let [service->service-instances (zipmap
                                     (map response->Service apps-list)
                                     (map #(response-data->service-instances
                                             % [] retrieve-framework-id-fn mesos-api
                                             service-id->failed-instances-transient-store
                                             service-id->service-description)
                                          apps-list))]
    (scheduler/preserve-only-killed-instances-for-services! (map :id (keys service->service-instances)))
    (preserve-only-failed-instances-for-services!
      service-id->failed-instances-transient-store (map :id (keys service->service-instances)))
    service->service-instances))

(defn- get-apps
  "Makes a call with hardcoded embed parameters.
   Filters the apps to return only Waiter apps."
  [marathon-api is-waiter-app?-fn query-params]
  (let [apps (marathon/get-apps marathon-api query-params)]
    (filter #(is-waiter-app?-fn (app->waiter-service-id %)) (:apps apps))))

(defn marathon-descriptor
  "Returns the descriptor to be used by Marathon to create new apps."
  [home-path-prefix service-id->password-fn {:keys [service-id service-description]}]
  (let [health-check-url (sd/service-description->health-check-url service-description)
        {:strs [backend-proto cmd cmd-type cpus disk grace-period-secs health-check-interval-secs
                health-check-max-consecutive-failures mem ports restart-backoff-factor run-as-user]} service-description
        home-path (str home-path-prefix run-as-user)]
    (when (= "docker" cmd-type)
      (throw (ex-info "Unsupported command type on service"
                      {:cmd-type cmd-type
                       :service-description service-description
                       :service-id service-id})))
    {:id service-id
     :env (scheduler/environment service-id service-description service-id->password-fn home-path)
     :user run-as-user
     :cmd cmd
     :disk disk
     :mem mem
     :ports (-> ports (repeat 0) vec)
     :cpus cpus
     :healthChecks [{:protocol (str/upper-case backend-proto)
                     :path health-check-url
                     :gracePeriodSeconds grace-period-secs
                     :intervalSeconds health-check-interval-secs
                     :portIndex 0
                     :timeoutSeconds 20
                     :maxConsecutiveFailures health-check-max-consecutive-failures}]
     :backoffFactor restart-backoff-factor
     :labels {:source "waiter"
              :user run-as-user}}))

(defn- start-new-service
  "Helper function to start a service with the specified descriptor."
  [marathon-api service-id marathon-descriptor conflict-handler]
  (ss/try+
    (log/info "Starting new app for" service-id "with descriptor" (dissoc marathon-descriptor :env))
    (scheduler/retry-on-transient-server-exceptions
      (str "create-app-if-new[" service-id "]")
      (marathon/create-app marathon-api marathon-descriptor))
    (catch [:status 409] e
      (conflict-handler {:deployment-info (extract-deployment-info marathon-api e)
                         :descriptor marathon-descriptor
                         :error e}))))

(defn- exception->stop-application-deployment
  "Retrieves the first StopApplication deployment in the exception data."
  [error-data]
  (when (= 409 (some-> error-data :error :status))
    (some->> error-data
             :deployment-info
             (some (fn [{:keys [currentActions] :as deployment}]
                     (and (some (fn [{:keys [action]}]
                                  (= "StopApplication" action))
                                currentActions)
                          deployment))))))

(defn start-new-service-wrapper
  "Starts the service with the specified descriptor.
   If the service is currently under a StopApplication deployment,
   it waits up to stop-application-timeout-ms milliseconds before deleting the deployment
   and starting the service."
  [marathon-api service-id marathon-descriptor]
  (let [conflict-handler-basic
        (fn conflict-handler-basic [error-data]
          (log/warn (ex-info "Conflict status when trying to start app. Is app starting up?"
                             error-data)
                    "Exception starting new app"))
        conflict-handler-retry
        (fn conflict-handler-retry [error-data]
          (if-let [deployment (exception->stop-application-deployment error-data)]
            (let [{:keys [id version]} deployment]
              (log/info "detected StopApplication deployment" {:deployment deployment :service-id service-id})
              (if (-> (du/str-to-date version formatter-marathon)
                      (t/plus (t/minutes 5))
                      (t/before? (t/now)))
                (do
                  (log/info "deleting existing StopApplication deployment" id)
                  (marathon/delete-deployment marathon-api id)
                  (log/info "re-attempting start service" service-id)
                  (start-new-service marathon-api service-id marathon-descriptor conflict-handler-basic))
                (conflict-handler-basic error-data)))
            (conflict-handler-basic error-data)))]
    (start-new-service marathon-api service-id marathon-descriptor conflict-handler-retry)))

(defrecord MarathonScheduler [marathon-api mesos-api retrieve-framework-id-fn
                              home-path-prefix service-id->failed-instances-transient-store
                              service-id->kill-info-store service-id->out-of-sync-state-store
                              service-id->password-fn service-id->service-description
                              force-kill-after-ms is-waiter-app?-fn sync-deployment-maintainer-atom]

  scheduler/ServiceScheduler

  (get-service->instances [_]
    (let [apps (get-apps marathon-api is-waiter-app?-fn {"embed" ["apps.lastTaskFailure" "apps.tasks"]})]
      (response-data->service->service-instances
        apps retrieve-framework-id-fn mesos-api service-id->failed-instances-transient-store
        service-id->service-description)))

  (get-apps [_]
    (map response->Service (get-apps marathon-api is-waiter-app?-fn {"embed" ["apps.lastTaskFailure" "apps.tasks"]})))

  (get-instances [_ service-id]
    (ss/try+
      (scheduler/retry-on-transient-server-exceptions
        (str "get-instances[" service-id "]")
        (let [marathon-response (marathon/get-app marathon-api service-id)]
          (response-data->service-instances
            marathon-response [:app] retrieve-framework-id-fn mesos-api service-id->failed-instances-transient-store
            service-id->service-description)))
      (catch [:status 404] {}
        (log/warn "get-instances: service" service-id "does not exist!"))))

  (kill-instance [_ {:keys [id service-id] :as instance}]
    (let [current-time (t/now)
          {:keys [kill-failing-since] :or {kill-failing-since current-time}}
          (get @service-id->kill-info-store service-id)
          use-force (t/after? current-time (t/plus kill-failing-since (t/millis force-kill-after-ms)))
          _ (when use-force
              (log/info "using force killing" id "as kills have been failing since" (du/date-to-str kill-failing-since)))
          params {:force use-force, :scale true}
          {:keys [killed?] :as kill-result} (process-kill-instance-request marathon-api service-id id params)]
      (if killed?
        (do (swap! service-id->kill-info-store dissoc service-id)
            (scheduler/process-instance-killed! instance))
        (swap! service-id->kill-info-store update-in [service-id :kill-failing-since]
               (fn [existing-time] (or existing-time current-time))))
      kill-result))

  (app-exists? [_ service-id]
    (ss/try+
      (scheduler/suppress-transient-server-exceptions
        (str "app-exists?[" service-id "]")
        (marathon/get-app marathon-api service-id))
      (catch [:status 404] _
        (log/warn "app-exists?: service" service-id "does not exist!"))))

  (create-app-if-new [this descriptor]
    (timers/start-stop-time!
      (metrics/waiter-timer "core" "create-app")
      (let [service-id (:service-id descriptor)
            marathon-descriptor (marathon-descriptor home-path-prefix service-id->password-fn descriptor)]
        (when-not (scheduler/app-exists? this service-id)
          (start-new-service-wrapper marathon-api service-id marathon-descriptor)))))

  (delete-app [_ service-id]
    (ss/try+
      (let [delete-result (scheduler/retry-on-transient-server-exceptions
                            (str "in delete-app[" service-id "]")
                            (log/info "deleting service" service-id)
                            (marathon/delete-app marathon-api service-id))]
        (when delete-result
          (remove-failed-instances-for-service! service-id->failed-instances-transient-store service-id)
          (scheduler/remove-killed-instances-for-service! service-id)
          (swap! service-id->kill-info-store dissoc service-id))
        (if (:deploymentId delete-result)
          {:result :deleted
           :message (str "Marathon deleted with deploymentId " (:deploymentId delete-result))}
          {:result :error
           :message "Marathon did not provide deploymentId for delete request"}))
      (catch [:status 404] {}
        (log/warn "[delete-app] Service does not exist:" service-id)
        {:result :no-such-service-exists
         :message "Marathon reports service does not exist"})
      (catch [:status 409] e
        (log/warn "Marathon deployment conflict while deleting"
                  {:deployment-info (extract-deployment-info marathon-api e)
                   :service-id service-id}))
      (catch [:status 503] {}
        (log/warn "[delete-app] Marathon unavailable (Error 503).")
        (log/debug (:throwable &throw-context) "[delete-app] Marathon unavailable"))))

  (scale-app [_ service-id scale-to-instances force]
    (ss/try+
      (scheduler/suppress-transient-server-exceptions
        (str "in scale-app[" service-id "]")
        (when force
          (when-let [current-deployment (extract-service-deployment-info marathon-api service-id)]
            (log/info "forcefully deleting deployment" current-deployment)
            (marathon/delete-deployment marathon-api (:id current-deployment))))
        (let [old-descriptor (:app (marathon/get-app marathon-api service-id))
              scale-to-instances' (cond-> scale-to-instances
                                          ;; avoid unintentional scale-down in force mode
                                          force (max (-> old-descriptor :tasks count)))
              _ (when (not= scale-to-instances scale-to-instances')
                  (log/info "adjusting scale to instances to" scale-to-instances' "in force mode"))
              new-descriptor (-> (select-keys old-descriptor [:cmd :cpus :id :mem])
                                 (assoc :instances scale-to-instances'))]
          (marathon/update-app marathon-api service-id new-descriptor)))
      (catch [:status 409] e
        (log/warn "Marathon deployment conflict while scaling"
                  {:deployment-info (extract-deployment-info marathon-api e)
                   :service-id service-id}))
      (catch [:status 503] {}
        (log/warn "[scale-app] Marathon unavailable (Error 503).")
        (log/debug (:throwable &throw-context) "[autoscaler] Marathon unavailable"))))

  (retrieve-directory-content [_ service-id instance-id host directory]
    (when (str/blank? service-id) (throw (ex-info (str "Service id is missing!") {})))
    (when (str/blank? instance-id) (throw (ex-info (str "Instance id is missing!") {})))
    (when (str/blank? host) (throw (ex-info (str "Host is missing!") {})))
    (let [log-directory (or directory (mesos/retrieve-log-url mesos-api instance-id host "marathon"))]
      (when (str/blank? log-directory) (throw (ex-info "No directory found for instance!" {})))
      (mesos/retrieve-directory-content-from-host mesos-api host log-directory)))

  (service-id->state [_ service-id]
    {:failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
     :killed-instances (scheduler/service-id->killed-instances service-id)
     :kill-info (get @service-id->kill-info-store service-id)
     :out-of-sync-state (get @service-id->out-of-sync-state-store service-id)})

  (state [_]
    {:service-id->failed-instances-transient-store @service-id->failed-instances-transient-store
     :service-id->kill-info-store @service-id->kill-info-store
     :service-id->out-of-sync-state-store @service-id->out-of-sync-state-store}))

(defn- get-apps-with-deployments
  "Retrieves the apps with the deployment info embedded."
  [{:keys [marathon-api is-waiter-app?-fn]}]
  (get-apps marathon-api is-waiter-app?-fn {"embed" ["apps.deployments" "apps.tasks"]}))

(defn- is-out-of-sync?
  "Returns true if the service does not have pending deployments and the instance and task counts do not match."
  [{:keys [deployments instances tasks]}]
  (and (empty? deployments) (not= instances (count tasks))))

(defn- retrieve-out-of-sync-apps
  "Retrieves the details about out-of-sync services.
   Returns a map keyed with service-id and values of :instances-requested and :instances-scheduled."
  [marathon-scheduler]
  (try
    (->> (get-apps-with-deployments marathon-scheduler)
         (filter is-out-of-sync?)
         (map (fn [{:keys [id instances tasks]}]
                [(remove-slash-prefix id)
                 {:instances-requested instances :instances-scheduled (count tasks)}]))
         (into {}))
    (catch Exception e
      (log/error e "unable to retrieve out-of-sync services"))))

(defn- trigger-sync-deployment!
  "Triggers deployment for out-of-sync service."
  [marathon-scheduler service-id {:keys [instances-scheduled] :as task-data}]
  (try
    (log/info "triggering sync deployment" {:service-id service-id :task-data task-data})
    (scheduler/scale-app marathon-scheduler service-id instances-scheduled false)
    (catch Exception e
      (log/error e "unable to sync marathon deployment for" service-id))))

(defn- compute-out-of-sync-state
  "A helper function that computes the new out-of-sync-state and the trigger service-ids."
  [service-id->out-of-sync-state service-id->instance-count-map current-time trigger-timeout]
  (let [out-of-sync-service-ids (-> service-id->instance-count-map keys set)
        trigger-time (t/minus current-time trigger-timeout)
        trigger-service-ids (->> service-id->out-of-sync-state
                                 (filter (fn out-of-sync-trigger-predicate
                                           [[service-id {:keys [data last-modified-time]}]]
                                           (and (= data (service-id->instance-count-map service-id))
                                                (t/before? last-modified-time trigger-time))))
                                 (map first)
                                 set)]
    (log/info (count out-of-sync-service-ids) "service(s) have out-of-sync deployments:" out-of-sync-service-ids)
    (log/info (count trigger-service-ids) "service(s) qualify for sync deployments:" trigger-service-ids)
    {:service-id->instance-count-map service-id->instance-count-map
     :service-id->out-of-sync-state (->> (set/difference out-of-sync-service-ids trigger-service-ids)
                                         (pc/map-from-keys
                                           (fn compute-out-of-sync-state [service-id]
                                             (let [prev-state (get service-id->out-of-sync-state service-id)
                                                   prev-data (:data prev-state)
                                                   curr-data (service-id->instance-count-map service-id)]
                                               (if (= prev-data curr-data)
                                                 prev-state
                                                 {:data curr-data :last-modified-time current-time})))))
     :trigger-service-ids trigger-service-ids}))

(defn- process-out-of-sync-services!
  "Determines the out-of-sync services and triggers sync deployments on those services if needed.
   As a side-effect, it updates the service-id->out-of-sync-state in the scheduler."
  [{:keys [service-id->out-of-sync-state] :as current-state} marathon-scheduler current-time trigger-timeout]
  (try
    (let [service-id->instance-count-map (retrieve-out-of-sync-apps marathon-scheduler)
          {:keys [service-id->instance-count-map service-id->out-of-sync-state trigger-service-ids]}
          (compute-out-of-sync-state
            service-id->out-of-sync-state service-id->instance-count-map current-time trigger-timeout)]
      (doseq [service-id trigger-service-ids]
        (->> (service-id->instance-count-map service-id)
             (trigger-sync-deployment! marathon-scheduler service-id)))
      (assoc current-state
        :last-update-time current-time
        :service-id->out-of-sync-state service-id->out-of-sync-state))
    (catch Exception e
      (log/error e "unable to process out-of-sync services")
      (assoc current-state :last-update-time current-time))))

(defn trigger-sync-deployment-maintainer-iteration
  "Runs an individual iteration of the sync-deployment-maintainer.
   On non-leader routers it returns the reset state.
   On the leader it processes out-of-sync services and returns the updated state."
  [leader?-fn service-id->out-of-sync-state marathon-scheduler trigger-timeout]
  (if (leader?-fn)
    (let [current-time (t/now)]
      (cid/with-correlation-id
        (str "sync-deployment." (tc/to-long current-time))
        (process-out-of-sync-services! service-id->out-of-sync-state marathon-scheduler current-time trigger-timeout)))
    (assoc service-id->out-of-sync-state
      :last-update-time (t/now)
      :service-id->out-of-sync-state {})))

(defn- start-sync-deployment-maintainer
  "Launches the sync-deployment maintainer which triggers new deployments for services which have a mismatch
   in the counts for requested and scheduled instances."
  [leader?-fn service-id->out-of-sync-state-store marathon-scheduler {:keys [interval-ms timeout-cycles]}]
  (let [trigger-timeout (t/millis (* interval-ms timeout-cycles))
        processing-mutex (atom ::idle)]
    (du/start-timer-task
      (t/millis interval-ms)
      (fn sync-deployment-maintainer-timer-task []
        (if (compare-and-set! processing-mutex ::idle ::busy)
          (try
            (->> (trigger-sync-deployment-maintainer-iteration
                   leader?-fn @service-id->out-of-sync-state-store marathon-scheduler trigger-timeout)
                 (reset! service-id->out-of-sync-state-store))
            (finally
              (reset! processing-mutex ::idle)))
          (log/warn "sync-deployment-maintainer is busy processing previous iteration")))
      :delay-ms interval-ms)))

(defn- retrieve-framework-id
  "Retrieves the framework id of the running marathon instance."
  [marathon-api]
  (utils/log-and-suppress-when-exception-thrown
    "Error in retrieving info from marathon."
    (:frameworkId (marathon/get-info marathon-api))))

(defn marathon-scheduler
  "Returns a new MarathonScheduler with the provided configuration.
   Validates the configuration against marathon-scheduler-schema and throws if it's not valid."
  [{:keys [home-path-prefix http-options force-kill-after-ms framework-id-ttl mesos-slave-port
           slave-directory sync-deployment url
           ;; functions provided in the context
           is-waiter-app?-fn leader?-fn service-id->password-fn service-id->service-description-fn]}]
  {:pre [(not (str/blank? url))
         (or (nil? slave-directory) (not (str/blank? slave-directory)))
         (or (nil? mesos-slave-port) (utils/pos-int? mesos-slave-port))
         (utils/pos-int? framework-id-ttl)
         (utils/pos-int? (:conn-timeout http-options))
         (utils/pos-int? (:socket-timeout http-options))
         (not (str/blank? home-path-prefix))
         (utils/pos-int? (:interval-ms sync-deployment))
         (utils/pos-int? (:timeout-cycles sync-deployment))]}
  (when (or (not slave-directory) (not mesos-slave-port))
    (log/info "scheduler mesos-slave-port or slave-directory is missing, log directory and url support will be disabled"))
  (let [http-client (http-utils/http-client-factory http-options)
        marathon-api (marathon/api-factory http-client http-options url)
        mesos-api (mesos/api-factory http-client http-options mesos-slave-port slave-directory)
        service-id->failed-instances-transient-store (atom {})
        service-id->last-force-kill-store (atom {})
        service-id->out-of-sync-state-store (atom {})
        retrieve-framework-id-fn (memo/ttl #(retrieve-framework-id marathon-api) :ttl/threshold framework-id-ttl)
        sync-deployment-maintainer-atom (atom nil)
        marathon-scheduler (->MarathonScheduler
                             marathon-api mesos-api retrieve-framework-id-fn home-path-prefix
                             service-id->failed-instances-transient-store service-id->last-force-kill-store
                             service-id->out-of-sync-state-store service-id->password-fn
                             service-id->service-description-fn force-kill-after-ms is-waiter-app?-fn
                             sync-deployment-maintainer-atom)
        sync-deployment-maintainer (start-sync-deployment-maintainer
                                     leader?-fn service-id->out-of-sync-state-store marathon-scheduler sync-deployment)]
    (reset! sync-deployment-maintainer-atom sync-deployment-maintainer)
    marathon-scheduler))
