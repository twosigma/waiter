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
(ns waiter.scheduler.cook
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [schema.core :as s]
            [slingshot.slingshot :as ss]
            [waiter.authorization :as authz]
            [waiter.mesos.mesos :as mesos]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.schema :as schema]
            [waiter.util.async-utils :as au]
            [waiter.util.cache-utils :as cu]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as http-utils]
            [waiter.util.utils :as utils])
  (:import (java.util UUID)))

;; Interactions with Cook API

(defn post-jobs
  "Post a job description to Cook."
  [{:keys [http-client impersonate spnego-auth url]} job-description]
  (let [run-as-user (-> job-description :jobs first :labels :user)]
    (http-utils/http-request
      http-client
      (str url "/jobs")
      :accept "application/json"
      :body (utils/clj->json job-description)
      :content-type "application/json"
      :headers (cond-> {} impersonate (assoc "x-cook-impersonate" run-as-user))
      :request-method :post
      :spnego-auth spnego-auth)))

(defn delete-jobs
  "Delete the specified jobs on Cook."
  [{:keys [http-client impersonate spnego-auth url]} run-as-user job-uuids]
  (http-utils/http-request
    http-client
    (str url "/rawscheduler")
    :accept "application/json"
    :content-type "application/json"
    :headers (cond-> {} impersonate (assoc "x-cook-impersonate" run-as-user))
    :query-string {:job job-uuids}
    :request-method :delete
    :spnego-auth spnego-auth))

(defn get-jobs
  "Retrieves running or waiting jobs for the specified user on Cook."
  [{:keys [http-client spnego-auth url]} user states &
   {:keys [end-time search-interval service-id start-time]
    :or {search-interval (t/days 7)}}]
  (let [end-time (or end-time (t/now))
        end-time-str (du/date-to-str end-time)
        start-time (or start-time (t/minus end-time search-interval))
        start-time-str (du/date-to-str start-time)
        jobs (http-utils/http-request
               http-client (str url "/jobs")
               :accept "application/json"
               :content-type "application/json"
               :query-string (cond-> {:end end-time-str
                                      :start start-time-str
                                      :state states
                                      :user user}
                               service-id (assoc :name (str service-id "*")))
               :request-method :get
               :spnego-auth spnego-auth)]
    (filter (fn [job] (= "waiter" (-> job :labels :source))) jobs)))

;; Instance health checks

(let [http-client (http-utils/http-client-factory {:conn-timeout 10000
                                                   :socket-timeout 10000
                                                   :spnego-auth false
                                                   :user-agent "waiter-cook-health-check"})
      ;; TODO make this cache configurable
      healthy-instance-cache (cu/cache-factory {:threshold 5000
                                                :ttl (-> 10 t/seconds t/in-millis)})]
  (defn- instance-healthy?
    "Performs health check if an entry does not exist in the healthy-instance-cache."
    [task-id health-check-url]
    ;; TODO Move health check out of critical path, e.g. by storing a future in the cache
    (let [health-result (cu/cache-get-or-load
                          healthy-instance-cache
                          task-id
                          (fn perform-instance-health-check []
                            (try
                              ;; no exception thrown means we got a 2XX response
                              (log/info "performing health check for" task-id "at" health-check-url)
                              (http-utils/http-request http-client health-check-url)
                              (catch Exception e
                                (log/error e "exception thrown while performing health check"
                                           {:health-check-url health-check-url})
                                false))))]
      ;; Do not track unhealthy instances in the cache
      (when-not health-result
        (cu/cache-evict healthy-instance-cache task-id))
      health-result)))

(defn- job->port
  "Extracts the backend port from the job."
  [job]
  (if-let [backend-port (-> job :labels :backend-port)]
    (Integer/parseInt backend-port)
    (-> job :instances first :ports first)))

(defn job-healthy?
  "Returns true if the specified job has a healthy running instance."
  [job]
  (and (= "running" (-> job :status))
       (= "running" (-> job :instances first :status))
       (let [job-instance (-> job :instances first)
             host (-> job-instance :hostname)
             port (job->port job)
             protocol (-> job :labels :backend-proto)
             health-check-url (-> job :labels :health-check-url)
             task-id (-> job-instance :task_id)]
         (when (and host port protocol health-check-url)
           (instance-healthy? task-id (str protocol "://" host ":" port health-check-url))))))

;; Helper Methods

(let [instance-expiry-adjustment-mins 5]
  (defn create-job-description
    "Create the Cook job description for a service."
    [service-id service-description service-id->password-fn home-path-prefix instance-priority backend-port]
    (let [{:strs [backend-proto cmd cmd-type cpus health-check-url instance-expiry-mins mem name ports
                  run-as-user version]} service-description
          job-uuid (str (UUID/randomUUID)) ;; TODO Use "less random" UUIDs for better Cook cache performance.
          _ (log/info "creating a new job for" service-id "with uuid" job-uuid)
          home-path (str home-path-prefix run-as-user)
          environment (scheduler/environment service-id service-description service-id->password-fn home-path)
          container-mode? (= "docker" cmd-type)
          [_ image-namespace image-name image-label] (when container-mode?
                                                       (re-matches #"(.*)/(.*):(.*)" version))
          container-support-enabled? (and image-namespace image-name image-label)]
      (when container-mode?
        (let [container-data {:cmd-type cmd-type
                              :image-label image-label
                              :image-name image-name
                              :image-namespace image-namespace
                              :service-id service-id
                              :version version}]
          (if container-support-enabled?
            (log/info "container support enabled" container-data)
            (throw (ex-info "to use container support format version as namespace/name:label" container-data)))))
      {:jobs [(cond-> {:application {:name name
                                     :version (or image-label version)}
                       :command cmd
                       :cpus cpus
                       :disable-mea-culpa-retries true
                       :env environment
                       :executor "cook"
                       :labels (cond-> {:backend-proto backend-proto
                                        :health-check-url health-check-url
                                        :service-id service-id
                                        :source "waiter"
                                        :user run-as-user}
                                 backend-port (assoc :backend-port (str backend-port)))
                       :max-retries 1
                       ;; extend max runtime past instance expiry as there may be a delay in killing it
                       :max-runtime (-> instance-expiry-mins
                                        (+ instance-expiry-adjustment-mins)
                                        t/minutes
                                        t/in-millis)
                       :mem mem
                       :name (str service-id "." job-uuid)
                       :ports ports
                       :priority instance-priority
                       :uuid job-uuid}
                container-support-enabled?
                (assoc :container {:docker {:force-pull-image false
                                            :image (str "namespace:" image-namespace ","
                                                        "name:" image-name ","
                                                        "label:" image-label)
                                            :network "HOST"}
                                   :type "docker"}))]})))

(defn create-job
  "Create and start a new Cook job specified by the service-description."
  [cook-api service-id service-description service-id->password-fn home-path-prefix instance-priority
   backend-port]
  (->> (create-job-description service-id service-description service-id->password-fn home-path-prefix
                               instance-priority backend-port)
       (post-jobs cook-api)))

(defn determine-instance-priority
  "Determines the instance priority based on allowed (sorted by highest first) priorities and unavailable priorities."
  [allowed-priorities unavailable-priorities]
  ;; The and clause in some is used to return the priority instead of a boolean.
  (or (some #(and (not (contains? unavailable-priorities %)) %) allowed-priorities)
      (last allowed-priorities)))

(defn launch-jobs
  "Launch num-instances jobs specified by the service-description.
   The instance priority is determined by finding the first unused value in allowed priorities or the lowest priority."
  [cook-api service-id service-description service-id->password-fn
   home-path-prefix num-instances allowed-priorities reserved-priorities backend-port]
  (loop [iteration 0
         unavailable-priorities reserved-priorities]
    (when (< iteration num-instances)
      ;; TODO introduce new sort-position field in ServiceInstance for use in sorting
      (let [instance-priority (determine-instance-priority allowed-priorities unavailable-priorities)]
        (create-job cook-api service-id service-description service-id->password-fn
                    home-path-prefix instance-priority backend-port)
        (recur (inc iteration)
               (conj unavailable-priorities instance-priority))))))

(defn retrieve-jobs
  "Retrieves jobs for the specified user."
  [cook-api search-interval service-id {:strs [run-as-user]}]
  (when run-as-user
    (let [user-jobs (get-jobs cook-api run-as-user ["running" "waiting"]
                              :search-interval search-interval :service-id service-id)]
      (seq user-jobs))))

(defn job->service-instance
  "Converts a Cook Job+Instance to a waiter.scheduler/ServiceInstance"
  [job]
  ;; we expect every job to have a single instance
  (let [job-port (job->port job)
        job-instance (-> job :instances first)
        instance-ports (-> job-instance :ports)]
    (scheduler/make-ServiceInstance
      {:cook/job-name (-> job :name)
       :cook/job-uuid (-> job :uuid)
       :cook/priority (-> job :priority)
       :cook/task-id (-> job-instance :task_id)
       :exit-code (-> job-instance :exit_code)
       :extra-ports (seq (remove #(= job-port %) instance-ports))
       :healthy? (job-healthy? job)
       :host (-> job-instance :hostname)
       ;; Mimic Cook's logic of creating a task name
       :id (str (-> job :name) "_" (-> job :labels :user) "_" (-> job-instance :task_id))
       :log-directory (-> job-instance :sandbox_directory)
       :message (-> job-instance :reason_string)
       :port job-port
       :protocol (-> job :labels :backend-proto)
       :service-id (-> job :labels :service-id)
       :started-at (-> job-instance :start_time tc/from-long)})))

(defn jobs->service
  "Converts jobs belonging to a service to a waiter.scheduler/Service."
  [jobs]
  (let [service-id (-> jobs first :labels :service-id)
        num-jobs (count jobs)
        num-running-jobs (count (filter #(= "running" (:status %)) jobs))
        num-healthy-jobs (count (filter job-healthy? jobs))]
    (scheduler/make-Service
      {:id service-id
       :instances num-jobs
       :task-count num-jobs
       :task-stats {:running num-running-jobs
                    :healthy num-healthy-jobs
                    :unhealthy (- num-running-jobs num-healthy-jobs)
                    :staged (- num-jobs num-running-jobs)}})))

;; Failed instances tracking

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

(defn track-failed-instances
  "Retrieves and stores the failed instances in the specified time range into
   service-id->failed-instances-transient-store.
   No more than max-instances-to-keep failed instances are tracked per service."
  [{:keys [allowed-users cook-api]} service-id->failed-instances-transient-store max-instances-to-keep
   start-time end-time]
  (let [failed-jobs (mapcat #(get-jobs cook-api % ["failed"] :end-time end-time :start-time start-time)
                            allowed-users)
        failed-instances (map job->service-instance failed-jobs)]
    (when (seq failed-instances)
      (log/info "found" (count failed-instances) "failed instances" {:end-time end-time :start-time start-time}))
    (doseq [{:keys [service-id] :as failed-instance} failed-instances]
      (scheduler/add-instance-to-buffered-collection!
        service-id->failed-instances-transient-store max-instances-to-keep service-id failed-instance
        (fn [] #{})
        (fn [instances] (-> (scheduler/sort-instances instances) (rest) (set)))))))

(defn start-track-failed-instances
  "Launches a timer task that tracks failed instances."
  [service-id->failed-instances-transient-store scheduler failed-tracker-interval-ms]
  (let [last-start-time-atom (atom nil)
        max-instances-to-keep 10]
    (du/start-timer-task
      (t/millis failed-tracker-interval-ms)
      (fn track-failed-instances-fn []
        (let [start-time @last-start-time-atom
              end-time (t/now)]
          (track-failed-instances
            scheduler service-id->failed-instances-transient-store max-instances-to-keep start-time end-time)
          (reset! last-start-time-atom end-time)))
      :delay-ms failed-tracker-interval-ms)))

(defn get-service->instances
  "Returns a map of scheduler/Service records -> map of scheduler/ServiceInstance records."
  [cook-api allowed-users search-interval service-id->failed-instances-transient-store]
  (let [all-jobs (mapcat #(get-jobs cook-api % ["running" "waiting"] :search-interval search-interval)
                         allowed-users)
        service-id->jobs (group-by #(-> % :labels :service-id) all-jobs)]
    (->> (keys service-id->jobs)
         (reduce
           (fn [service->instance-distribution service-id]
             (let [jobs (service-id->jobs service-id)]
               (assoc!
                 service->instance-distribution
                 (jobs->service jobs)
                 {:active-instances (map job->service-instance jobs)
                  :failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)})))
           (transient {}))
         (persistent!))))

;; Scheduler Interface and Factory Methods

(defrecord CookScheduler [scheduler-name service-id->password-fn service-id->service-description-fn
                          cook-api allowed-priorities allowed-users backend-port home-path-prefix
                          search-interval service-id->failed-instances-transient-store
                          retrieve-syncer-state-fn authorizer]

  scheduler/ServiceScheduler

  (get-services [_]
    (let [all-jobs (mapcat #(get-jobs cook-api % ["running" "waiting"] :search-interval search-interval)
                           allowed-users)
          service-id->jobs (group-by #(-> % :labels :service-id) all-jobs)]
      (preserve-only-failed-instances-for-services!
        service-id->failed-instances-transient-store (keys service-id->jobs))
      (map
        (fn [service-id]
          (-> service-id service-id->jobs jobs->service))
        (keys service-id->jobs))))

  (kill-instance [this {:keys [cook/job-uuid id service-id] :as instance}]
    (if (scheduler/service-exists? this service-id)
      (let [log-data {:instance-id id :job-uuid job-uuid :service-id service-id}
            success (try
                      (when job-uuid
                        (log/debug "killing" log-data)
                        (let [{:strs [run-as-user]} (service-id->service-description-fn service-id)]
                          (delete-jobs cook-api run-as-user [job-uuid])
                          (log/info "killed" log-data))
                        true)
                      (catch Exception ex
                        (log/error ex "unable to kill" log-data)
                        false))]
        {:killed? success
         :success success
         :result (if success :killed :failed)
         :message (if success
                    (str "Killed " id)
                    (str "Unable to kill " id))})
      {:success false
       :result :no-such-service-exists
       :message (str service-id " does not exist!")}))

  (service-exists? [_ service-id]
    (ss/try+
      (scheduler/suppress-transient-server-exceptions
        (str "service-exists?[" service-id "]")
        (some->> (service-id->service-description-fn service-id)
                 (retrieve-jobs cook-api search-interval service-id)))
      (catch [:status 404] _
        (log/warn "service-exists?: service" service-id "does not exist!"))))

  (create-service-if-new [this {:keys [service-id] :as descriptor}]
    (if-not (scheduler/service-exists? this service-id)
      (timers/start-stop-time!
        (metrics/waiter-timer "scheduler" scheduler-name "create-service")
        (let [success (try
                        (let [{:keys [service-description]} descriptor
                              {:strs [min-instances run-as-user]} service-description]
                          (when-not (contains? allowed-users run-as-user)
                            (throw (ex-info "User not allowed to launch service via cook scheduler"
                                            {:log-level :info :service-id service-id :user run-as-user})))
                          (launch-jobs cook-api service-id service-description service-id->password-fn
                                       home-path-prefix min-instances allowed-priorities #{} backend-port)
                          true)
                        (catch Exception ex
                          (log/error ex "unable to create service" service-id)
                          false))]
          {:message (if success
                      (str "Created " service-id)
                      (str "Unable to create " service-id))
           :result (if success :created :failed)
           :success success}))
      {:message (str service-id " already exists!")
       :result :already-exists
       :success false}))

  (delete-service [this service-id]
    (if (scheduler/service-exists? this service-id)
      (let [success (try
                      (let [{:strs [run-as-user] :as service-description} (service-id->service-description-fn service-id)
                            jobs (retrieve-jobs cook-api search-interval service-id service-description)
                            job-uuids (map :uuid jobs)]
                        (log/info "deleting" (count job-uuids) "jobs for service" service-id)
                        (delete-jobs cook-api run-as-user job-uuids)
                        true)
                      (catch Exception ex
                        (log/error ex "unable to delete all jobs for service" service-id)
                        false))]
        {:message (if success
                    (str "Deleted " service-id)
                    (str "Unable to delete " service-id))
         :result (if success :deleted :failed)
         :success success})
      {:message (str service-id " does not exist!")
       :result :no-such-service-exists
       :success false}))

  (scale-service [this service-id scale-to-instances _]
    (if (scheduler/service-exists? this service-id)
      (let [result (try
                     (let [service-description (service-id->service-description-fn service-id)
                           jobs (retrieve-jobs cook-api search-interval service-id service-description)
                           num-jobs (count jobs)
                           scale-amount (- scale-to-instances num-jobs)]
                       (log/info "scaling" {:num-jobs num-jobs :scale-amount scale-amount :service-id service-id})
                       (if (pos? scale-amount)
                         (let [reserved-priorities (->> jobs (map :priority) set)]
                           (launch-jobs cook-api service-id service-description service-id->password-fn
                                        home-path-prefix scale-amount allowed-priorities reserved-priorities
                                        backend-port)
                           :scaled)
                         :scaling-not-needed))
                     (catch Exception ex
                       (log/error ex "unable to scale service" service-id)
                       :failed))
            success (contains? #{:scaled :scaling-not-needed} result)]
        {:message (if success
                    (str "Scaled " service-id)
                    (str "Unable to scale " service-id))
         :result result
         :success success})
      {:message (str service-id " does not exist!")
       :result :no-such-service-exists
       :success false}))

  (retrieve-directory-content [_ service-id instance-id host directory]
    (when (str/blank? service-id) (throw (ex-info (str "Service id is missing!") {:log-level :info})))
    (when (str/blank? instance-id) (throw (ex-info (str "Instance id is missing!") {:log-level :info})))
    (when (str/blank? host) (throw (ex-info (str "Host is missing!") {:log-level :info})))
    (let [task-id-start-index (str/last-index-of instance-id "_")
          task-id (->> task-id-start-index inc (subs instance-id))
          log-directory (or directory (mesos/retrieve-log-url cook-api task-id host "cook"))]
      (when (str/blank? log-directory) (throw (ex-info "No directory found for instance!" {:log-level :info})))
      (mesos/retrieve-directory-content-from-host cook-api host log-directory)))

  (service-id->state [_ service-id]
    {:failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
     :syncer (retrieve-syncer-state-fn service-id)})

  (state [_]
    {:authorizer (when authorizer (authz/state authorizer))
     :service-id->failed-instances-transient-store @service-id->failed-instances-transient-store
     :syncer (retrieve-syncer-state-fn)})

  (validate-service [_ service-id]
    (let [{:strs [run-as-user]} (service-id->service-description-fn service-id)]
      (authz/check-user authorizer run-as-user service-id))))

(s/defn ^:always-validate create-cook-scheduler
  "Returns a new CookScheduler with the provided configuration."
  [{:keys [allowed-users authorizer backend-port home-path-prefix instance-priorities search-interval-days
           ;; entries from the context
           scheduler-name service-id->password-fn service-id->service-description-fn]}
   cook-api service-id->failed-instances-transient-store retrieve-syncer-state-fn]
  {:pre [(seq allowed-users)
         (schema/contains-kind-sub-map? authorizer)
         (or (nil? backend-port) (pos? backend-port))
         (not (str/blank? home-path-prefix))
         (> (:max instance-priorities) (:min instance-priorities))
         (pos? (:delta instance-priorities))
         (pos? search-interval-days)
         (not (str/blank? scheduler-name))
         (fn? service-id->password-fn)
         (fn? service-id->service-description-fn)]}
  (let [allowed-priorities (range (:max instance-priorities)
                                  (:min instance-priorities)
                                  (unchecked-negate-int (:delta instance-priorities)))
        authorizer (utils/create-component authorizer)
        search-interval (t/days search-interval-days)]
    (->CookScheduler scheduler-name service-id->password-fn service-id->service-description-fn
                     cook-api allowed-priorities allowed-users backend-port home-path-prefix
                     search-interval service-id->failed-instances-transient-store
                     retrieve-syncer-state-fn authorizer)))

(defn cook-scheduler
  "Creates and starts cook scheduler with associated daemons."
  [{:keys [allowed-users failed-tracker-interval-ms http-options impersonate mesos-slave-port search-interval-days url
           ;; entries from the context
           scheduler-name scheduler-state-chan scheduler-syncer-interval-secs start-scheduler-syncer-fn] :as config}]
  {:pre [(seq allowed-users)
         (pos? search-interval-days)
         (not (str/blank? scheduler-name))
         (au/chan? scheduler-state-chan)
         (pos-int? scheduler-syncer-interval-secs)
         (fn? start-scheduler-syncer-fn)]}
  (let [http-client (-> http-options
                        (utils/assoc-if-absent :user-agent "waiter-cook")
                        http-utils/http-client-factory)
        cook-api {:http-client http-client
                  :impersonate impersonate
                  :slave-port mesos-slave-port
                  :spnego-auth (:spnego-auth http-options false)
                  :url url}
        search-interval (t/days search-interval-days)
        service-id->failed-instances-transient-store (atom {})
        get-service->instances-fn
        #(get-service->instances cook-api allowed-users search-interval service-id->failed-instances-transient-store)
        {:keys [retrieve-syncer-state-fn]}
        (start-scheduler-syncer-fn scheduler-name get-service->instances-fn scheduler-state-chan scheduler-syncer-interval-secs)
        scheduler (create-cook-scheduler config cook-api service-id->failed-instances-transient-store retrieve-syncer-state-fn)]
    (start-track-failed-instances service-id->failed-instances-transient-store scheduler failed-tracker-interval-ms)
    scheduler))
