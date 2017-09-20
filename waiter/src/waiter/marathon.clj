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
(ns waiter.marathon
  (:require [clj-http.client :as http]
            [clj-time.core :as t]
            [clojure.core.memoize :as memo]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [marathonclj.common :as mc]
            [marathonclj.rest.apps :as apps]
            [metrics.timers :as timers]
            [slingshot.slingshot :as ss]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.utils :as utils])
  (:import java.io.StringWriter
           marathonclj.common.Connection))

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
                                 :started-at (str (:timestamp failed-marathon-task))
                                 :healthy? false
                                 :port 0})))
          max-instances-to-keep 10]
      (scheduler/add-instance-to-buffered-collection!
        service-id->failed-instances-transient-store max-instances-to-keep service-id failed-instance
        (fn [] #{}) (fn [instances] (-> (scheduler/sort-instances instances) (rest) (set)))))))

(defmacro swallow-out
  "Like with-out-str, except it swallows anything sent to *out* and returns
  what the body returns. We wrap this around the calls made to marathonclj
  that call pprint on the request to avoid the unwanted output on stdout"
  [& body]
  `(let [sw# (new StringWriter)]
     (try
       (binding [*out* sw#] ~@body)
       (finally
         (.close sw#)))))

(defn process-kill-instance-request
  "Processes a kill instance request"
  [service-id instance-id {:keys [force scale] :or {force false, scale true} :as kill-params}]
  (scheduler/retry-on-transient-server-exceptions
    "kill-instance"
    (letfn [(make-kill-response [killed? message status]
              {:instance-id instance-id, :killed? killed?, :message message, :service-id service-id, :status status})]
      (ss/try+
        (log/debug "killing instance" instance-id "from service" service-id)
        (let [result (swallow-out (apps/kill-task service-id instance-id "scale" (str scale) "force" (str force)))
              kill-success? (and result (map? result) (contains? result :deploymentId))]
          (log/info "kill instance" instance-id "result" result)
          (let [message (if kill-success? "Successfully killed instance" "Unable to kill instance")
                status (if kill-success? 200 500)]
            (make-kill-response kill-success? message status)))
        (catch [:status 409] _
          (log/info "kill instance" instance-id "failed as it is locked by one or more deployments" kill-params)
          (make-kill-response false "Locked by one or more deployments" 409))
        (catch map? {:keys [body status]}
          (log/info "kill instance" instance-id "returned" status body)
          (make-kill-response false (str body) (or status 500)))
        (catch Throwable e
          (log/info e "exception thrown when calling kill-instance")
          (make-kill-response false (str (.getMessage e)) (:status e 500)))))))

(defn response-data->service-instances
  "Extracts the list of instances for a given app from the marathon response."
  [marathon-response service-keys retrieve-framework-id-fn slave-directory service-id->failed-instances-transient-store]
  (let [service-id (remove-slash-prefix (get-in marathon-response (conj service-keys :id)))
        framework-id (retrieve-framework-id-fn)
        common-extractor-fn (fn [instance-id marathon-task-response]
                              (let [{:keys [appId host message slaveId]} marathon-task-response
                                    log-directory (str slave-directory "/" slaveId "/frameworks/" framework-id "/executors/" instance-id "/runs/latest")]
                                (cond-> {:service-id (remove-slash-prefix appId)
                                         :host host}
                                        (and slave-directory framework-id slaveId)
                                        (assoc :log-directory log-directory)

                                        message
                                        (assoc :message (str/trim message))

                                        (str/includes? (str message) "Memory limit exceeded:")
                                        (assoc :flags #{:memory-limit-exceeded})

                                        (str/includes? (str message) "Task was killed since health check failed")
                                        (assoc :flags #{:never-passed-health-checks})

                                        (str/includes? (str message) "Command exited with status")
                                        (assoc :exit-code (try (-> message (str/split #"\s+") last Integer/parseInt)
                                                               (catch Throwable e))))))
        healthy?-fn #(let [health-checks (:healthCheckResults %)]
                       (and
                         (and (seq health-checks)
                              (every? :alive health-checks))
                         (every?
                           (fn [hc]
                             (zero? (:consecutiveFailures hc))) health-checks)))
        protocol (-> marathon-response
                     (get-in (conj service-keys :healthChecks 0 :protocol))
                     str
                     str/lower-case)
        active-marathon-tasks (get-in marathon-response (conj service-keys :tasks))
        active-instances (map
                           #(scheduler/make-ServiceInstance
                              (let [instance-id (remove-slash-prefix (:id %))]
                                (merge
                                  (common-extractor-fn instance-id %)
                                  {:id instance-id
                                   :started-at (str (:startedAt %))
                                   :healthy? (healthy?-fn %)
                                   ;; first port must be used for the web server, extra ports can be used freely.
                                   :port (-> % :ports first)
                                   :extra-ports (-> % :ports rest vec)
                                   :protocol protocol})))
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
  [apps-list retrieve-framework-id-fn slave-directory service-id->failed-instances-transient-store]
  (let [service->service-instances (zipmap
                                     (map response->Service apps-list)
                                     (map #(response-data->service-instances
                                             % [] retrieve-framework-id-fn slave-directory
                                             service-id->failed-instances-transient-store)
                                          apps-list))]
    (scheduler/preserve-only-killed-instances-for-services! (map :id (keys service->service-instances)))
    (preserve-only-failed-instances-for-services!
      service-id->failed-instances-transient-store (map :id (keys service->service-instances)))
    service->service-instances))

(defn- get-apps
  "Makes a call with hardcoded embed parameters.
  marathonclj.rest.apps cannot handle duplicate query params."
  [is-waiter-app?-fn]
  (let [apps (mc/get (mc/url-with-path "v2" "apps?embed=apps.tasks&embed=apps.lastTaskFailure"))]
    (filter #(is-waiter-app?-fn (app->waiter-service-id %)) (:apps apps))))

(defn marathon-descriptor
  "Returns the descriptor to be used by Marathon to create new apps."
  [home-path-prefix service-id->password-fn {:keys [service-id service-description]}]
  (let [health-check-url (sd/service-description->health-check-url service-description)
        {:strs [backend-proto cmd cpus disk grace-period-secs mem ports restart-backoff-factor run-as-user]} service-description
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
                     :intervalSeconds 10
                     :portIndex 0
                     :timeoutSeconds 20
                     :maxConsecutiveFailures 5}]            ; decreased max to expedite feedback loop
     :backoffFactor restart-backoff-factor
     :labels {:source "waiter"
              :user run-as-user}}))

(defn retrieve-log-url
  "Retrieve the directory path for the specified instance running on the specified host."
  [http-options mesos-slave-port instance-id host]
  (when (str/blank? instance-id) (throw (ex-info (str "Instance id is missing!") {})))
  (when (str/blank? host) (throw (ex-info (str "Host is missing!") {})))
  (let [url (str "http://" host ":" mesos-slave-port "/state.json")
        response-body (:body (http/get url (assoc http-options :headers {} :throw-exceptions false)))
        _ (log/info "Mesos state on" host "is" response-body)
        response-parsed (walk/keywordize-keys (json/read-str response-body))
        frameworks (concat (:completed_frameworks response-parsed) (:frameworks response-parsed))
        marathon-framework (first (filter #(= (:role %) "marathon") frameworks))
        marathon-executors (concat (:completed_executors marathon-framework) (:executors marathon-framework))
        log-directory (str (:directory (first (filter #(= (:id %) instance-id) marathon-executors))))]
    log-directory))

(defn retrieve-directory-content-from-host
  "Retrieve the content of the directory for the given instance on the specified host"
  [http-options mesos-slave-port service-id instance-id host directory]
  (when (str/blank? service-id) (throw (ex-info (str "Service id is missing!") {})))
  (when (str/blank? instance-id) (throw (ex-info (str "Instance id is missing!") {})))
  (when (str/blank? host) (throw (ex-info (str "Host is missing!") {})))
  (when (str/blank? directory) (throw (ex-info "No directory found for instance!" {})))
  (let [response (http/get (str "http://" host ":" mesos-slave-port "/files/browse?path=" directory) (assoc http-options :headers {} :throw-exceptions false))
        response-parsed (walk/keywordize-keys (json/read-str (:body response)))]
    (map (fn [entry]
           (merge {:name (subs (:path entry) (inc (count directory)))
                   :size (:size entry)}
                  (if (= (:nlink entry) 1)
                    {:type "file"
                     :url (str "http://" host ":" mesos-slave-port "/files/download?path=" (:path entry))}
                    {:type "directory"
                     :path (:path entry)})))
         response-parsed)))

(defrecord MarathonScheduler [http-options mesos-slave-port retrieve-framework-id-fn
                              slave-directory home-path-prefix service-id->failed-instances-transient-store
                              service-id->kill-info-store force-kill-after-ms is-waiter-app?-fn]

  scheduler/ServiceScheduler

  (get-apps->instances [_]
    (let [apps (get-apps is-waiter-app?-fn)]
      (response-data->service->service-instances
        apps retrieve-framework-id-fn slave-directory
        service-id->failed-instances-transient-store)))

  (get-apps [_]
    (map response->Service (get-apps is-waiter-app?-fn)))

  (get-instances [_ service-id]
    (ss/try+
      (scheduler/retry-on-transient-server-exceptions
        (str "get-instances[" service-id "]")
        (let [marathon-response (apps/get-app service-id)]
          (response-data->service-instances marathon-response [:app] retrieve-framework-id-fn slave-directory
                                            service-id->failed-instances-transient-store)))
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
          {:keys [killed?] :as kill-result} (process-kill-instance-request service-id id params)]
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
        (apps/get-app service-id))
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
              (swallow-out (apps/create-app marathon-descriptor)))
            (catch [:status 409] e
              (log/warn (ex-info "Conflict status when trying to start app. Is app starting up?"
                                 {:descriptor marathon-descriptor
                                  :error e})
                        "Exception starting new app")))))))

  (delete-app [_ service-id]
    (ss/try+
      (let [delete-result (scheduler/retry-on-transient-server-exceptions
                            (str "in delete-app[" service-id "]")
                            (log/info "deleting service" service-id)
                            (swallow-out (apps/delete-app service-id)))]
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
      (catch [:status 409] {}
        (log/warn "Marathon deployment conflict while deleting" service-id))
      (catch [:status 503] {}
        (log/warn "[delete-app] Marathon unavailable (Error 503).")
        (log/debug (:throwable &throw-context) "[delete-app] Marathon unavailable"))))

  (scale-app [_ service-id instances]
    (ss/try+
      (scheduler/suppress-transient-server-exceptions
        (str "in scale-app[" service-id "]")
        (let [old-descriptor (:app (apps/get-app service-id))
              new-descriptor (update-in
                               (select-keys old-descriptor [:id :cmd :mem :cpus :instances])
                               [:instances]
                               (fn [_] instances))]
          (swallow-out (apps/update-app service-id new-descriptor "force" "true"))))
      (catch [:status 409] {}
        (log/warn "Marathon deployment conflict while scaling" service-id))
      (catch [:status 503] {}
        (log/warn "[scale-app] Marathon unavailable (Error 503).")
        (log/debug (:throwable &throw-context) "[autoscaler] Marathon unavailable"))))

  (retrieve-directory-content [_ service-id instance-id host directory]
    (when mesos-slave-port
      (let [log-directory (or directory (retrieve-log-url http-options mesos-slave-port instance-id host))]
        (retrieve-directory-content-from-host http-options mesos-slave-port service-id instance-id host log-directory))))

  (service-id->state [_ service-id]
    {:failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
     :killed-instances (scheduler/service-id->killed-instances service-id)
     :kill-info (get @service-id->kill-info-store service-id)})

  (state [_]
    {:service-id->failed-instances-transient-store @service-id->failed-instances-transient-store
     :service-id->kill-info-store @service-id->kill-info-store}))

(defn- retrieve-framework-id
  "Retrieves the framework id of the running marathon instance."
  []
  (utils/log-and-suppress-when-exception-thrown
    "Error in retrieving info from marathon."
    (:frameworkId (mc/get (mc/url-with-path "v2" "info")))))

(defn marathon-scheduler
  "Returns a new MarathonScheduler with the provided configuration. Validates the
  configuration against marathon-scheduler-schema and throws if it's not valid."
  [{:keys [home-path-prefix http-options force-kill-after-ms framework-id-ttl
           mesos-slave-port slave-directory url is-waiter-app?-fn]}]
  {:pre [(not (str/blank? url))
         (or (nil? slave-directory) (not (str/blank? slave-directory)))
         (or (nil? mesos-slave-port) (utils/pos-int? mesos-slave-port))
         (utils/pos-int? framework-id-ttl)
         (utils/pos-int? (:conn-timeout http-options))
         (utils/pos-int? (:socket-timeout http-options))
         (not (str/blank? home-path-prefix))]}
  (when (or (not slave-directory) (not mesos-slave-port))
    (log/info "scheduler mesos-slave-port or slave-directory is missing, log directory and url support will be disabled"))
  (mc/init! (Connection. url http-options))
  (let [retrieve-framework-id-fn (memo/ttl #(retrieve-framework-id) :ttl/threshold framework-id-ttl)
        service-id->failed-instances-transient-store (atom {})
        service-id->last-force-kill-store (atom {})]
    (->MarathonScheduler http-options mesos-slave-port retrieve-framework-id-fn slave-directory home-path-prefix
                         service-id->failed-instances-transient-store service-id->last-force-kill-store
                         force-kill-after-ms is-waiter-app?-fn)))
