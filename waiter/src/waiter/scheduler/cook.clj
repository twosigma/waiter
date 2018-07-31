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
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [schema.core :as s]
            [slingshot.slingshot :as ss]
            [waiter.mesos.mesos :as mesos]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as http-utils]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.util UUID)))

;; Interactions with Cook API

(defn post-jobs
  "Post a job description to Cook."
  [{:keys [http-client impersonate spnego-auth url]} job-description]
  (let [run-as-user (-> job-description :jobs first :labels :user)]
    (http-utils/http-request
      http-client
      (str url "/jobs")
      :accept "application/json"
      :body (json/write-str job-description)
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
        start-time-str (du/date-to-str start-time)]
    (http-utils/http-request
      http-client
      (str url "/jobs")
      :accept "application/json"
      :content-type "application/json"
      :query-string (cond-> {:end end-time-str
                             :start start-time-str
                             :state states
                             :user user}
                      service-id (assoc :name (str service-id "*")))
      :request-method :get
      :spnego-auth spnego-auth)))

;; Instance health checks

(let [http-client (http-utils/http-client-factory {:conn-timeout 10000
                                                   :socket-timeout 10000
                                                   :spnego-auth false})
      ;; TODO make this cache configurable
      healthy-instance-cache (-> {}
                                 (cache/fifo-cache-factory :threshold 5000)
                                 (cache/ttl-cache-factory :ttl (-> 10 t/seconds t/in-millis))
                                 atom)]
  (defn- instance-healthy?
    "Performs health check if an entry does not exist in the healthy-instance-cache."
    [task-id health-check-url]
    (let [health-result (utils/atom-cache-get-or-load
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
        (utils/atom-cache-evict healthy-instance-cache task-id))
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
    [service-id service-description service-id->password-fn home-path-prefix instance-priority]
    (let [{:strs [backend-proto cmd cpus health-check-url instance-expiry-mins mem metadata name ports
                  run-as-user version]} service-description
          job-uuid (str (UUID/randomUUID))
          home-path (str home-path-prefix run-as-user)
          environment (scheduler/environment service-id service-description service-id->password-fn home-path)
          cook-backend-port (get metadata "cook-backend-port")
          docker-image-label (get metadata "docker-image-label")
          docker-image-name (get metadata "docker-image-name")
          docker-image-namespace (get metadata "docker-image-namespace")]
      (when cook-backend-port
        (try
          (when-not (pos? (Integer/parseInt cook-backend-port))
            (throw (ex-info "cook-backend-port metadata parsed to a non-positive integer"
                            {:cook-backend-port cook-backend-port})))
          (catch ExceptionInfo ex
            (throw ex))
          (catch Exception ex
            (throw (ex-info "cook-backend-port metadata must parse to a positive integer"
                            {:cook-backend-port cook-backend-port} ex)))))
      {:jobs [(cond-> {:application {:name name
                                     :version version}
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
                                 cook-backend-port (assoc :backend-port cook-backend-port))
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
                (and docker-image-namespace docker-image-name docker-image-label)
                (assoc :container {:docker {:force-pull-image false
                                            :image (str "namespace:" docker-image-namespace ","
                                                        "name:" docker-image-name ","
                                                        "label:" docker-image-label)
                                            :network "HOST"}
                                   :type "docker"}))]})))

(defn create-job
  "Create and start a new Cook job specified by the service-description."
  [cook-api service-id service-description service-id->password-fn home-path-prefix instance-priority]
  (log/info "creating a new job for" service-id)
  (->> (create-job-description service-id service-description service-id->password-fn home-path-prefix instance-priority)
       (post-jobs cook-api)))

(defn determine-instance-priority
  "Determines the instance priority based on allowed (sorted by highest first) priorities and unavailable priorities."
  [allowed-priorities unavailable-priorities]
  (or (some #(and (not (contains? unavailable-priorities %)) %) allowed-priorities)
      (last allowed-priorities)))

(defn launch-jobs
  "Launch num-instances jobs specified by the service-description."
  [cook-api service-id service-description service-id->password-fn
   home-path-prefix num-instances allowed-priorities reserved-priorities]
  (loop [iteration 0
         unavailable-priorities reserved-priorities]
    (when (< iteration num-instances)
      ;; TODO introduce new sort-position field in ServiceInstance for use in sorting
      (let [instance-priority (determine-instance-priority allowed-priorities unavailable-priorities)]
        (create-job cook-api service-id service-description service-id->password-fn
                    home-path-prefix instance-priority)
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
        num-running-jobs (count (filter #(= "running" (-> % :status)) jobs))
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

;; Scheduler Interface and Factory Methods

(defrecord CookScheduler [service-id->password-fn service-id->service-description-fn
                          cook-api allowed-priorities allowed-users home-path-prefix
                          search-interval service-id->failed-instances-transient-store]

  scheduler/ServiceScheduler

  (get-apps->instances [_]
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
                    :failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
                    :killed-instances (scheduler/service-id->killed-instances service-id)})))
             (transient {}))
           (persistent!))))

  (get-apps [_]
    (let [all-jobs (mapcat #(get-jobs cook-api % ["running" "waiting"] :search-interval search-interval)
                           allowed-users)
          service-id->jobs (group-by #(-> % :labels :service-id) all-jobs)]
      (preserve-only-failed-instances-for-services!
        service-id->failed-instances-transient-store (keys service-id->jobs))
      (map
        (fn [service-id]
          (-> service-id service-id->jobs jobs->service))
        (keys service-id->jobs))))

  (get-instances [_ service-id]
    (ss/try+
      (scheduler/retry-on-transient-server-exceptions
        (str "get-instances[" service-id "]")
        {:active-instances (some->> (service-id->service-description-fn service-id)
                                    (retrieve-jobs cook-api search-interval service-id)
                                    (map job->service-instance)
                                    doall)
         :failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
         :killed-instances (scheduler/service-id->killed-instances service-id)})
      (catch [:status 404] {}
        (log/warn "get-instances: service" service-id "does not exist!"))))

  (kill-instance [this {:keys [cook/job-uuid id service-id] :as instance}]
    (if (scheduler/app-exists? this service-id)
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
        (when success
          (scheduler/process-instance-killed! instance))
        {:killed? success
         :success success
         :result (if success :killed :failed)
         :message (if success
                    (str "Killed " id)
                    (str "Unable to kill " id))})
      {:success false
       :result :no-such-service-exists
       :message (str service-id " does not exist!")}))

  (app-exists? [_ service-id]
    (ss/try+
      (scheduler/suppress-transient-server-exceptions
        (str "app-exists?[" service-id "]")
        (some->> (service-id->service-description-fn service-id)
                 (retrieve-jobs cook-api search-interval service-id)))
      (catch [:status 404] _
        (log/warn "app-exists?: service" service-id "does not exist!"))))

  (create-app-if-new [this {:keys [service-id] :as descriptor}]
    (if-not (scheduler/app-exists? this service-id)
      (timers/start-stop-time!
        (metrics/waiter-timer "core" "create-app")
        (let [success (try
                        (let [{:keys [service-description]} descriptor
                              {:strs [min-instances run-as-user]} service-description]
                          (when-not (contains? allowed-users run-as-user)
                            (throw (ex-info "User not allowed to launch service via cook scheduler"
                                            {:service-id service-id :user run-as-user})))
                          (launch-jobs cook-api service-id service-description service-id->password-fn
                                       home-path-prefix min-instances allowed-priorities #{})
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

  (delete-app [this service-id]
    (if (scheduler/app-exists? this service-id)
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

  (scale-app [this service-id scale-to-instances _]
    (if (scheduler/app-exists? this service-id)
      (let [result (try
                     (let [service-description (service-id->service-description-fn service-id)
                           jobs (retrieve-jobs cook-api search-interval service-id service-description)
                           num-jobs (count jobs)
                           scale-amount (- scale-to-instances num-jobs)]
                       (log/info "scaling" {:num-jobs num-jobs :scale-amount scale-amount :service-id service-id})
                       (if (pos? scale-amount)
                         (let [reserved-priorities (->> jobs (map :priority) set)]
                           (launch-jobs cook-api service-id service-description service-id->password-fn
                                        home-path-prefix scale-amount allowed-priorities reserved-priorities)
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
    (when (str/blank? service-id) (throw (ex-info (str "Service id is missing!") {})))
    (when (str/blank? instance-id) (throw (ex-info (str "Instance id is missing!") {})))
    (when (str/blank? host) (throw (ex-info (str "Host is missing!") {})))
    (let [task-id-start-index (str/last-index-of instance-id "_")
          task-id (->> task-id-start-index inc (subs instance-id))
          log-directory (or directory (mesos/retrieve-log-url cook-api task-id host "cook"))]
      (when (str/blank? log-directory) (throw (ex-info "No directory found for instance!" {})))
      (mesos/retrieve-directory-content-from-host cook-api host log-directory)))

  (service-id->state [_ service-id]
    {:failed-instances (service-id->failed-instances service-id->failed-instances-transient-store service-id)
     :killed-instances (scheduler/service-id->killed-instances service-id)})

  (state [_]
    {:service-id->failed-instances-transient-store @service-id->failed-instances-transient-store
     :service-id->killed-instances-transient-store @scheduler/service-id->killed-instances-transient-store}))

(s/defn ^:always-validate create-cook-scheduler
  "Returns a new CookScheduler with the provided configuration."
  [{:keys [allowed-users home-path-prefix instance-priorities search-interval-days
           service-id->password-fn service-id->service-description-fn]}
   cook-api service-id->failed-instances-transient-store]
  {:pre [(seq allowed-users)
         (not (str/blank? home-path-prefix))
         (> (:max instance-priorities) (:min instance-priorities))
         (pos? (:delta instance-priorities))
         (pos? search-interval-days)]}
  (let [allowed-priorities (range (:max instance-priorities)
                                  (:min instance-priorities)
                                  (unchecked-negate-int (:delta instance-priorities)))
        search-interval (t/days search-interval-days)]
    (->CookScheduler service-id->password-fn service-id->service-description-fn
                     cook-api allowed-priorities allowed-users home-path-prefix
                     search-interval service-id->failed-instances-transient-store)))

(defn cook-scheduler
  "Creates and starts cook scheduler with associated daemons."
  [{:keys [failed-tracker-interval-ms http-options impersonate mesos-slave-port url] :as config}]
  (let [http-client (http-utils/http-client-factory http-options)
        cook-api {:http-client http-client
                  :impersonate impersonate
                  :slave-port mesos-slave-port
                  :spnego-auth (:spnego-auth http-options false)
                  :url url}
        service-id->failed-instances-transient-store (atom {})
        scheduler (create-cook-scheduler config cook-api service-id->failed-instances-transient-store)]
    (start-track-failed-instances service-id->failed-instances-transient-store scheduler failed-tracker-interval-ms)
    scheduler))
