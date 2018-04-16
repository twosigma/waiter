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
(ns waiter.scheduler.marathon
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.memoize :as memo]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [slingshot.slingshot :as ss]
            [waiter.async-utils :as au]
            [waiter.mesos.marathon :as marathon]
            [waiter.mesos.mesos :as mesos]
            [waiter.mesos.utils :as mesos-utils]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.utils :as utils])
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
                                 :started-at (some-> failed-marathon-task :timestamp (utils/str-to-date formatter-marathon))
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
                                set)
            all-marathon-deployments (marathon/get-deployments marathon-api)]
        (->> all-marathon-deployments
             (filter (fn [{:strs [id]}] (contains? deployment-ids id)))
             vec)))
    (catch Exception e
      (log/error e "unable to extract the deployment info"))))

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
                                   :started-at (some-> % :startedAt (utils/str-to-date formatter-marathon))
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
  [marathon-api is-waiter-app?-fn]
  (let [apps (marathon/get-apps marathon-api)]
    (filter #(is-waiter-app?-fn (app->waiter-service-id %)) (:apps apps))))

(defn marathon-descriptor
  "Returns the descriptor to be used by Marathon to create new apps."
  [home-path-prefix service-id->password-fn {:keys [service-id service-description]}]
  (let [health-check-url (sd/service-description->health-check-url service-description)
        {:strs [backend-proto cmd cpus disk grace-period-secs health-check-interval-secs
                health-check-max-consecutive-failures mem ports restart-backoff-factor run-as-user]} service-description
        home-path (str home-path-prefix run-as-user)]
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

(defn retrieve-log-url
  "Retrieve the directory path for the specified instance running on the specified host."
  [mesos-api instance-id host]
  (when (str/blank? instance-id) (throw (ex-info (str "Instance id is missing!") {})))
  (when (str/blank? host) (throw (ex-info (str "Host is missing!") {})))
  (let [response-parsed (mesos/get-agent-state mesos-api host)
        frameworks (concat (:completed_frameworks response-parsed) (:frameworks response-parsed))
        marathon-framework (first (filter #(or (= (:role %) "marathon") (= (:name %) "marathon")) frameworks))
        marathon-executors (concat (:completed_executors marathon-framework) (:executors marathon-framework))
        log-directory (str (:directory (first (filter #(= (:id %) instance-id) marathon-executors))))]
    log-directory))

(defn retrieve-directory-content-from-host
  "Retrieve the content of the directory for the given instance on the specified host"
  [mesos-api service-id instance-id host directory]
  (when (str/blank? service-id) (throw (ex-info (str "Service id is missing!") {})))
  (when (str/blank? instance-id) (throw (ex-info (str "Instance id is missing!") {})))
  (when (str/blank? host) (throw (ex-info (str "Host is missing!") {})))
  (when (str/blank? directory) (throw (ex-info "No directory found for instance!" {})))
  (let [response-parsed (mesos/list-directory-content mesos-api host directory)]
    (map (fn [entry]
           (merge {:name (subs (:path entry) (inc (count directory)))
                   :size (:size entry)}
                  (if (= (:nlink entry) 1)
                    {:type "file"
                     :url (mesos/build-directory-download-link mesos-api host (:path entry))}
                    {:type "directory"
                     :path (:path entry)})))
         response-parsed)))

(defrecord MarathonScheduler [marathon-api mesos-api retrieve-framework-id-fn
                              home-path-prefix service-id->failed-instances-transient-store
                              service-id->kill-info-store service-id->service-description
                              force-kill-after-ms is-waiter-app?-fn]

  scheduler/ServiceScheduler

  (get-apps->instances [_]
    (let [apps (get-apps marathon-api is-waiter-app?-fn)]
      (response-data->service->service-instances
        apps retrieve-framework-id-fn mesos-api service-id->failed-instances-transient-store
        service-id->service-description)))

  (get-apps [_]
    (map response->Service (get-apps marathon-api is-waiter-app?-fn)))

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
              (log/info "using force killing" id "as kills have been failing since" (utils/date-to-str kill-failing-since)))
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

  (create-app-if-new [this service-id->password-fn descriptor]
    (timers/start-stop-time!
      (metrics/waiter-timer "core" "create-app")
      (let [service-id (:service-id descriptor)
            marathon-descriptor (marathon-descriptor home-path-prefix service-id->password-fn descriptor)]
        (when-not (scheduler/app-exists? this service-id)
          (ss/try+
            (log/info "Starting new app for" service-id "with descriptor" (dissoc marathon-descriptor :env))
            (scheduler/retry-on-transient-server-exceptions
              (str "create-app-if-new[" service-id "]")
              (marathon/create-app marathon-api marathon-descriptor))
            (catch [:status 409] e
              (log/warn (ex-info "Conflict status when trying to start app. Is app starting up?"
                                 {:deployment-info (extract-deployment-info marathon-api e)
                                  :descriptor marathon-descriptor
                                  :error e})
                        "Exception starting new app")))))))

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

  (scale-app [_ service-id instances]
    (ss/try+
      (scheduler/suppress-transient-server-exceptions
        (str "in scale-app[" service-id "]")
        (let [old-descriptor (:app (marathon/get-app marathon-api service-id))
              new-descriptor (update-in
                               (select-keys old-descriptor [:id :cmd :mem :cpus :instances])
                               [:instances]
                               (fn [_] instances))]
          (marathon/update-app marathon-api service-id new-descriptor)))
      (catch [:status 409] e
        (log/warn "Marathon deployment conflict while scaling"
                  {:deployment-info (extract-deployment-info marathon-api e)
                   :service-id service-id}))
      (catch [:status 503] {}
        (log/warn "[scale-app] Marathon unavailable (Error 503).")
        (log/debug (:throwable &throw-context) "[autoscaler] Marathon unavailable"))))

  (retrieve-directory-content [_ service-id instance-id host directory]
    (let [log-directory (or directory (retrieve-log-url mesos-api instance-id host))]
      (retrieve-directory-content-from-host mesos-api service-id instance-id host log-directory)))

  (service-id->state [_ service-id]
    {:failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
     :killed-instances (scheduler/service-id->killed-instances service-id)
     :kill-info (get @service-id->kill-info-store service-id)})

  (state [_]
    {:service-id->failed-instances-transient-store @service-id->failed-instances-transient-store
     :service-id->kill-info-store @service-id->kill-info-store}))

(defn- retrieve-framework-id
  "Retrieves the framework id of the running marathon instance."
  [marathon-api]
  (utils/log-and-suppress-when-exception-thrown
    "Error in retrieving info from marathon."
    (:frameworkId (marathon/get-info marathon-api))))

(defn marathon-scheduler
  "Returns a new MarathonScheduler with the provided configuration. Validates the
  configuration against marathon-scheduler-schema and throws if it's not valid."
  [{:keys [home-path-prefix http-options force-kill-after-ms framework-id-ttl mesos-slave-port
           service-id->service-description-fn slave-directory url is-waiter-app?-fn]}]
  {:pre [(not (str/blank? url))
         (or (nil? slave-directory) (not (str/blank? slave-directory)))
         (or (nil? mesos-slave-port) (utils/pos-int? mesos-slave-port))
         (utils/pos-int? framework-id-ttl)
         (utils/pos-int? (:conn-timeout http-options))
         (utils/pos-int? (:socket-timeout http-options))
         (not (str/blank? home-path-prefix))]}
  (when (or (not slave-directory) (not mesos-slave-port))
    (log/info "scheduler mesos-slave-port or slave-directory is missing, log directory and url support will be disabled"))
  (let [http-client (mesos-utils/http-client-factory http-options)
        marathon-api (marathon/api-factory http-client http-options url)
        mesos-api (mesos/api-factory http-client http-options mesos-slave-port slave-directory)
        service-id->failed-instances-transient-store (atom {})
        service-id->last-force-kill-store (atom {})
        retrieve-framework-id-fn (memo/ttl #(retrieve-framework-id marathon-api) :ttl/threshold framework-id-ttl)]
    (->MarathonScheduler marathon-api mesos-api retrieve-framework-id-fn home-path-prefix
                         service-id->failed-instances-transient-store service-id->last-force-kill-store
                         service-id->service-description-fn force-kill-after-ms is-waiter-app?-fn)))
