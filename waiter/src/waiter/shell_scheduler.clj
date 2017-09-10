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
(ns waiter.shell-scheduler
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [schema.core :as s]
            [waiter.scheduler :as scheduler]
            [waiter.schema :as schema]
            [waiter.service-description :as sd]
            [waiter.utils :as utils]
            [metrics.timers :as timers]
            [waiter.metrics :as metrics])
  (:import java.io.File
           java.lang.UNIXProcess
           java.net.ServerSocket
           java.util.ArrayList))

(defn pid
  "Returns the pid of the provided process (if it is a UNIXProcess)"
  [^Process process]
  (try
    (when (= UNIXProcess (type process))
      (let [field (.getDeclaredField (.getClass process) "pid")
            _ (.setAccessible field true)
            pid (.getLong field process)]
        (.setAccessible field false)
        pid))
    (catch Exception e
      (log/error e "error getting pid from" process))))

(defn launch-process
  "Launches a process that executes the specified command with the specified environment for a service.
   The instance is configured to spit out the stdout and stderr in a subdirectory of working-dir-base-path."
  [^String service-id ^String working-dir-base-path ^String command environment]
  (log/info "launching process:" service-id "with work dir" working-dir-base-path
            ", command [" command "], and environment" environment)
  (let [instance-id (str service-id "." (utils/unique-identifier))
        service-dir-path (str working-dir-base-path File/separator service-id)
        run-script-path (str service-dir-path File/separator "run-service.sh")
        working-dir (File. service-dir-path instance-id)
        ;; We use setsid below so that we can later kill by session id (see the
        ;; kill-process function), in case the process we launch spawns children
        ;; of its own.
        ;;
        ;; From https://linux.die.net/man/2/setsid:
        ;; setsid() creates a new session if the calling process is not a process
        ;; group leader. The calling process is the leader of the new session,
        ;; the process group leader of the new process group, and has no
        ;; controlling terminal. The process group ID and session ID of the
        ;; calling process are set to the PID of the calling process. The calling
        ;; process will be the only process in this new process group and in this
        ;; new session.
        pb (ProcessBuilder. (ArrayList. ["setsid" "bash" run-script-path]))
        process-env (.environment pb)]
    (.mkdirs working-dir)
    (spit run-script-path (str "#!/bin/bash"
                               (System/lineSeparator)
                               (System/lineSeparator)
                               command))
    (sh/sh "chmod" "+x" run-script-path)
    (doseq [[env-key env-val] (seq environment)]
      (when-not (nil? env-val)
        (.put process-env env-key env-val)))
    (.directory pb working-dir)
    (.redirectOutput pb (File. working-dir "stdout"))
    (.redirectError pb (File. working-dir "stderr"))
    {:instance-id instance-id
     :process (.start pb)
     :started-at (utils/date-to-str (t/now) (f/formatters :date-time))
     :working-directory (.getAbsolutePath working-dir)}))

(defn release-port!
  "Marks the given port as released (i.e. not reserved) after the given grace period"
  [port->reservation-atom port port-grace-period-ms]
  (let [port-expiry-time (t/plus (t/now) (t/millis port-grace-period-ms))]
    (swap! port->reservation-atom assoc port {:state :in-grace-period-until-expiry
                                              :expiry-time port-expiry-time})))

(defn kill-process!
  "Triggers killing of process and any children processes it spawned"
  [{:keys [:shell-scheduler/process :shell-scheduler/pid extra-ports port] :as instance}
   port->reservation-atom port-grace-period-ms]
  (try
    (log/info "killing process:" instance)
    (.destroyForcibly process)
    (sh/sh "pkill" "-9" "-g" (str pid))
    (release-port! port->reservation-atom port port-grace-period-ms)
    (doseq [port extra-ports]
      (release-port! port->reservation-atom port port-grace-period-ms))
    (catch Throwable e
      (log/error e "error attempting to kill process:" instance))))

(defn port-reserved?
  "Returns true if port is currently reserved"
  [port->reservation-atom port]
  (let [{:keys [state expiry-time] :as reservation} (get @port->reservation-atom port false)]
    (and reservation
         (or (= state :in-use)
             (and (= state :in-grace-period-until-expiry)
                  (t/before? (t/now) expiry-time))))))

(defn port-can-be-used?
  "Returns true if port is not reserved and not reported as in use by the OS"
  [port->reservation-atom port]
  (and (not (port-reserved? port->reservation-atom port)) (utils/port-available? port)))

(defn reserve-port!
  "Returns an available port on the host, from the provided range.
   If no port is available, returns nil."
  [port->reservation-atom port-range]
  (let [pool (range (first port-range) (inc (second port-range)))
        port (first (filter #(port-can-be-used? port->reservation-atom %) pool))]
    (when port
      (swap! port->reservation-atom assoc port {:state :in-use, :expiry-time nil})
      port)))

(defn reserve-ports!
  "Reserves num-ports available ports on the host, from the provided range.
   Throws an exception if num-ports ports are not available."
  [num-ports port->reservation-atom port-range]
  (let [reserved-ports (reduce (fn inner-reserve-ports! [ports _]
                                 (if-let [port (reserve-port! port->reservation-atom port-range)]
                                   (conj ports port)
                                   (reduced ports)))
                               []
                               (range num-ports))]
    (if-not (= num-ports (count reserved-ports))
      (do
        (doall (map #(release-port! port->reservation-atom % 0) reserved-ports))
        (throw (ex-info (str "Unable to reserve " num-ports " ports")
                        {:num-reserved-ports (count reserved-ports)})))
      reserved-ports)))

(defn launch-instance
  "Launches a new process for the given service-id"
  [service-id {:strs [backend-proto cmd ports]} working-dir-base-path environment port->reservation-atom port-range]
  (when-not cmd
    (throw (ex-info "The command to run was not supplied" {:service-id service-id})))
  (let [reserved-ports (reserve-ports! ports port->reservation-atom port-range)
        process-environment (into environment
                                  (map (fn build-port-environment [index port]
                                         [(str "PORT" index) (str port)])
                                       (range 0 (-> reserved-ports count inc))
                                       reserved-ports))
        {:keys [instance-id process started-at working-directory]}
        (launch-process service-id working-dir-base-path cmd process-environment)]
    (scheduler/make-ServiceInstance
      {:id instance-id
       :service-id service-id
       :started-at started-at
       :healthy? nil
       :host (str "127.0.0." (inc (rand-int 10)))
       :port (-> reserved-ports first)
       :extra-ports (-> reserved-ports rest vec)
       :protocol backend-proto
       :log-directory working-directory
       :shell-scheduler/process process
       :shell-scheduler/working-directory working-directory
       :shell-scheduler/pid (pid process)})))

(defn active?
  "Returns true if the given instance is considered active"
  [instance]
  (not (:killed? instance)))

(defn- healthy?
  "Returns true if the given instance is healthy (and active)"
  [instance]
  (and (active? instance) (:healthy? instance)))

(defn- unhealthy?
  "Returns true if the given instance is unhealthy (but active)"
  [instance]
  (and (active? instance) (not (healthy? instance))))

(defn update-task-stats
  "Updates task-count and task-stats based upon the id->instances map for a service."
  [{:keys [service id->instance] :as service-entry}]
  (let [running (->> id->instance vals (filter active?) count)
        healthy (->> id->instance vals (filter healthy?) count)
        unhealthy (->> id->instance vals (filter unhealthy?) count) ]
    (assoc service-entry :service (-> service
                                      (assoc :task-count running)
                                      (assoc :task-stats {:healthy healthy
                                                          :unhealthy unhealthy
                                                          :running running
                                                          :staged 0})))))

(defn launch-service
  "Creates a new service and launches a single new instance for this service"
  [service-id {:strs [mem] :as service-description} service-id->password-fn
   working-dir-base-path port->reservation-atom port-range]
  (let [environment (scheduler/environment service-id service-description service-id->password-fn working-dir-base-path)
        instance (launch-instance service-id service-description working-dir-base-path environment port->reservation-atom port-range)
        service (scheduler/make-Service {:id service-id
                                         :instances 1
                                         :environment environment
                                         :service-description service-description
                                         :shell-scheduler/mem mem})]
    {:service service
     :instance instance}))

(defn create-service
  "Launches a new service and returns the updated id->service map"
  [id->service service-id service-description service-id->password-fn
   work-directory port->reservation-atom port-range completion-promise]
  (try
    (if (contains? id->service service-id)
      (do
        (log/info "service" service-id "already exists")
        (deliver completion-promise :already-exists)
        id->service)
      (do
        (log/info "creating service" service-id ":" service-description)
        (let [{:keys [service instance]}
              (launch-service service-id service-description service-id->password-fn
                              work-directory port->reservation-atom port-range)]
          (deliver completion-promise :created)
          (let [service-entry (-> {:service service 
                                   :id->instance {(:id instance) instance}}
                                  update-task-stats)] 
            (assoc id->service service-id service-entry)))))
    (catch Throwable e
      (log/error e "error attempting to create service" service-id)
      (deliver completion-promise :failed)
      id->service)))

(defn- kill-instance
  "Deletes the instance corresponding to service-id/instance-id and returns the updated id->service map"
  [id->service service-id instance-id port->reservation-atom port-grace-period-ms completion-promise]
  (try
    (if (contains? id->service service-id)
      (let [{:keys [id->instance]} (get id->service service-id)
            {:keys [:shell-scheduler/process] :as instance} (get id->instance instance-id)]
        (if (and instance (active? instance))
          (do
            (log/info "deleting instance" instance-id "process" process)
            (kill-process! instance port->reservation-atom port-grace-period-ms)
            (deliver completion-promise :deleted)
            (-> id->service
                (update-in [service-id :service :instances] dec)
                (assoc-in [service-id :id->instance instance-id :killed?] true)
                (assoc-in [service-id :id->instance instance-id :shell-scheduler/process] nil)))
          (do
            (log/info "instance" instance-id "does not exist")
            (deliver completion-promise :no-such-instance-exists)
            id->service)))
      (do
        (log/info "service" service-id "does not exist")
        (deliver completion-promise :no-such-service-exists)
        id->service))
    (catch Throwable e
      (log/error e "error attempting to delete instance" instance-id)
      (deliver completion-promise :failed)
      id->service)))

(defn- delete-service
  "Deletes the service corresponding to service-id and returns the updated id->service map"
  [id->service service-id port->reservation-atom port-grace-period-ms completion-promise]
  (try
    (if (contains? id->service service-id)
      (do
        (log/info "deleting service" service-id)
        (let [{:keys [id->instance]} (get id->service service-id)]
          (doseq [[_ instance] id->instance]
            (when (active? instance)
              (kill-process! instance port->reservation-atom port-grace-period-ms))))
        (deliver completion-promise :deleted)
        (dissoc id->service service-id))
      (do
        (log/info "service" service-id "does not exist")
        (deliver completion-promise :no-such-service-exists)
        id->service))
    (catch Throwable e
      (log/error e "error attempting to delete service" service-id)
      (deliver completion-promise :failed)
      id->service)))

(defn perform-health-check
  "Runs a synchronous health check against instance and returns true if it was successful"
  [{:keys [port] :as instance} health-check-path http-client]
  (if (pos? port)
    (let [_ (log/debug "running health check against" instance)
          instance-health-check-url (scheduler/health-check-url instance health-check-path)
          {:keys [status error]} (async/<!! (http/get http-client instance-health-check-url))]
      (scheduler/log-health-check-issues instance instance-health-check-url status error)
      (and (not error) (<= 200 status 299)))
    false))

(defn- update-instance-health
  "Runs a health check against instance"
  [instance health-check-url http-client]
  (if (active? instance)
    (assoc instance :healthy? (perform-health-check instance health-check-url http-client))
    instance))

(defn- associate-exit-codes
  "Associates exit codes with exited instances"
  [{:keys [:shell-scheduler/process port] :as instance} port->reservation-atom port-grace-period-ms]
  (if (and (active? instance) (not (.isAlive process)))
    (let [exit-value (.exitValue process)]
      (log/info "instance exited with value" {:instance instance :exit-value exit-value})
      (release-port! port->reservation-atom port port-grace-period-ms)
      (assoc instance :healthy? false
                      :failed? (if (zero? exit-value) false true)
                      :killed? true                          ; does not actually mean killed -- using this to mark inactive
                      :exit-code exit-value))
    instance))

(defn- enforce-grace-period
  "Kills processes for unhealthy instances exceeding their grace period"
  [{:keys [:shell-scheduler/process started-at] :as instance} grace-period-secs port->reservation-atom port-grace-period-ms]
  (if (unhealthy? instance)
    (let [start-time (f/parse (f/formatters :date-time) started-at)
          current-time (t/now)]
      (if (>= (t/in-seconds (t/interval start-time current-time)) grace-period-secs)
        (do (log/info "unhealthy instance exceeded its grace period, killing instance"
                      {:instance instance :start-time start-time :current-time current-time :grace-period-secs grace-period-secs})
            (kill-process! instance port->reservation-atom port-grace-period-ms)
            (assoc instance :failed? true
                            :killed? true
                            :flags #{:never-passed-health-checks}
                            :shell-scheduler/process nil))
        instance))
    instance))

(defn- enforce-instance-limits
  "Kills processes that exceed allocated memory usage"
  [{:keys [:shell-scheduler/process :shell-scheduler/pid] :as instance} mem pid->memory port->reservation-atom port-grace-period-ms]
  (if (and (active? instance) (.isAlive process))
    (let [memory-allocated (* mem 1000)
          memory-used (get pid->memory pid)]
      (if (and memory-used (> memory-used memory-allocated))
        (do (log/info "instance exceeds memory limit, killing instance" {:instance instance :memory-limit memory-allocated :memory-used memory-used})
            (kill-process! instance port->reservation-atom port-grace-period-ms)
            (assoc instance :healthy? false
                            :failed? true
                            :killed? true
                            :flags #{:memory-limit-exceeded}
                            :shell-scheduler/process nil))
        instance))
    instance))

(defn- get-pid->memory
  "Issues and parses the results of a ps shell command and returns a new pid->memory map"
  []
  (try
    (let [ps-output (-> (sh/sh "ps" "-eo" "pid,pgid,rss") :out (str/split #"\n") rest)
          pid->pgid (map #(-> % str/trim (str/split #"\s+") butlast
                              ((fn [[a b]] [(Integer/parseInt a) (Integer/parseInt b)])))
                         ps-output)
          pgid->rss (apply merge-with +
                           (map #(-> % str/trim (str/split #"\s+") rest
                                     ((fn [[a b]] {(Integer/parseInt a) (Integer/parseInt b)})))
                                ps-output))]
      (into {} (for [[pid pgid] pid->pgid] [pid (get pgid->rss pgid)])))
    (catch Throwable e
      (log/error e "error attempting to issue ps command")
      {})))

(defn update-service-health
  "Runs health checks against all active instances of service and returns the updated service-entry"
  [id->service port->reservation-atom port-grace-period-ms http-client]
  (timers/start-stop-time!
    (metrics/waiter-timer "shell-scheduler" "update-health")
    (let [pid->memory (get-pid->memory)
          exit-codes-check #(associate-exit-codes % port->reservation-atom port-grace-period-ms)]
      (loop [remaining-service-entries (vals id->service)
             id->service' {}]
        (if-let [{:keys [service id->instance] :as service-entry} (first remaining-service-entries)]
          (let [{:strs [health-check-url grace-period-secs]} (:service-description service)
                health-check #(update-instance-health % health-check-url http-client)
                limits-check #(enforce-instance-limits % (:shell-scheduler/mem service) pid->memory port->reservation-atom port-grace-period-ms)
                grace-period-check #(enforce-grace-period % grace-period-secs port->reservation-atom port-grace-period-ms)
                id->instance' (pc/map-vals (comp grace-period-check health-check limits-check exit-codes-check) id->instance)
                service-entry' (-> service-entry
                                   (assoc :id->instance id->instance')
                                   update-task-stats)]
            (recur (rest remaining-service-entries) (assoc id->service' (:id service) service-entry')))
          id->service')))))

(defn- start-updating-health
  "Runs health checks against all active instances of all services in a loop"
  [id->service-agent port->reservation-atom port-grace-period-ms timeout-ms http-client]
  (log/info "starting update-health")
  (utils/start-timer-task
    (t/millis timeout-ms)
    (fn []
      (send id->service-agent update-service-health port->reservation-atom port-grace-period-ms http-client))))

(defn- set-service-scale
  "Given the current id->service map, sets the scale of the service-id to the
  requested number of instances and returns a new id->service map"
  [id->service service-id scale-to-instances completion-promise]
  (try
    (if (contains? id->service service-id)
      (let [{:keys [id->instance]} (get id->service service-id)
            current-instances (->> id->instance vals (filter active?) count)]
        (if (> scale-to-instances current-instances)
          (do
            (log/info "setting scale of" service-id "to" scale-to-instances "instances from" current-instances)
            (deliver completion-promise :scaled)
            (assoc-in id->service [service-id :service :instances] scale-to-instances))
          (do
            (log/info "received scale-app call, but current (" current-instances ") >= target (" scale-to-instances ")")
            (deliver completion-promise :scaling-not-needed)
            id->service)))
      (do
        (log/info "service" service-id "does not exist")
        (deliver completion-promise :no-such-service-exists)
        id->service))
    (catch Throwable e
      (log/error e "error attempting to scale service" service-id)
      (deliver completion-promise :failed)
      id->service)))

(defn maintain-instance-scale
  "Relaunches failed instances or otherwise ensures scale"
  [id->service port->reservation-atom port-range]
  (timers/start-stop-time!
    (metrics/waiter-timer "shell-scheduler" "retry-failed-instances")
    (loop [remaining-service-entries (vals id->service)
           id->service' {}]
      (if-let [{:keys [service id->instance] :as service-entry} (first remaining-service-entries)]
        (let [{:keys [id environment service-description]} service
              active-instances (->> id->instance vals (filter active?) count)
              scale-to-instances (:instances service)
              to-launch (- scale-to-instances active-instances)
              launch-new #(let [instance (launch-instance id service-description (get environment "HOME") environment port->reservation-atom port-range)]
                            [(:id instance) instance])
              service-entry' (if (pos? to-launch)
                               (do (log/info "launching new instances to ensure scale" {:current-count active-instances, :target-count scale-to-instances})
                                   (-> service-entry
                                       (assoc :id->instance (merge id->instance (into {} (repeatedly to-launch launch-new))))
                                       update-task-stats))
                               service-entry)]
          (recur (rest remaining-service-entries) (assoc id->service' id service-entry')))
        id->service'))))

(defn- start-retry-failed-instances
  "Relaunches failed instances in a loop"
  [id->service-agent port->reservation-atom port-range timeout-ms]
  (log/info "starting retry-failed-instances")
  (utils/start-timer-task
    (t/millis timeout-ms)
    (fn []
      (send id->service-agent maintain-instance-scale port->reservation-atom port-range))))

(defn- service-entry->instances
  "Converts the given service-entry to a map of shape:

    {:active-instances [...]
     :failed-instances [...]
     :killed-instances [...]}

  after running health checks on all instances of the service."
  [{:keys [service id->instance]}]
  [service
   {:active-instances (filter active? (vals id->instance))
    :failed-instances (filter :failed? (vals id->instance))
    :killed-instances (filter :killed? (vals id->instance))}])

(defn directory-content
  "Returns a sequence of entries, where each entry represents an
  item found in the instance's working directory + relative-dir"
  [{:keys [id->instance]} instance-id relative-dir]
  (let [{:keys [:shell-scheduler/working-directory]} (get id->instance instance-id)
        directory (io/file working-directory relative-dir)
        directory-content (vec (.listFiles directory))]
    (map (fn [^File file]
           (cond-> {:name (.getName file)
                    :size (.length file)
                    :type (if (.isDirectory file) "directory" "file")}
                   (.isDirectory file)
                   (assoc :path (-> file
                                    (.toPath)
                                    (.relativize (.getPath (File. (str working-directory))))
                                    (str)))
                   (.isFile file)
                   (assoc :url (str (.toURL file)))))
         directory-content)))

; The ShellScheduler's shell-agent holds all of the state about which
; services and instances are running (and killed). It is a map of:
;
;   service-id -> {:service service
;                  :id->instance id->instance}
;
; Each service is a waiter.scheduler/Service record, and each instance is a
; waiter.scheduler/ServiceInstance record, with some ShellScheduler-specific
; metadata fields added using keywords namespaced with :shell-scheduler/*:
;
;   :shell-scheduler/mem
;   :shell-scheduler/process
;   :shell-scheduler/working-directory
;   :shell-scheduler/pid
;
(defrecord ShellScheduler [work-directory id->service-agent port->reservation-atom port-grace-period-ms port-range]
  scheduler/ServiceScheduler

  (get-apps->instances [_]
    (let [id->service @id->service-agent]
      (into {} (map (fn [[_ service-entry]]
                      (service-entry->instances service-entry))
                    id->service))))

  (get-apps [_]
    (let [id->service @id->service-agent]
      (map (fn [[_ {:keys [service]}]] service) id->service)))

  (get-instances [_ service-id]
    (let [id->service @id->service-agent
          service-entry (get id->service service-id)]
      (second (service-entry->instances service-entry))))

  (kill-instance [this {:keys [id service-id] :as instance}]
    (if (scheduler/app-exists? this service-id)
      (let [completion-promise (promise)]
        (send id->service-agent kill-instance service-id id
              port->reservation-atom port-grace-period-ms
              completion-promise)
        (let [result (deref completion-promise)
              success (= result :deleted)]
          {:killed? true
           :success success
           :result result
           :message (if success
                      (str "Deleted " id)
                      (str "Unable to delete " id))}))
      {:success false
       :result :no-such-service-exists
       :message (str service-id " does not exist!")}))

  (app-exists? [_ service-id]
    (contains? @id->service-agent service-id))

  (create-app-if-new [this service-id->password-fn {:keys [service-id service-description] :as descriptor}]
    (if-not (scheduler/app-exists? this service-id)
      (let [completion-promise (promise)]
        (send id->service-agent create-service service-id service-description
              service-id->password-fn work-directory port->reservation-atom
              port-range completion-promise)
        (let [result (deref completion-promise)
              success (= result :created)]
          {:success success
           :result result
           :message (if success
                      (str "Created " service-id)
                      (str "Unable to create " service-id))}))
      {:success false
       :result :already-exists
       :message (str service-id " already exists!")}))

  (delete-app [this service-id]
    (if (scheduler/app-exists? this service-id)
      (let [completion-promise (promise)]
        (send id->service-agent delete-service service-id port->reservation-atom port-grace-period-ms completion-promise)
        (let [result (deref completion-promise)
              success (= result :deleted)]
          {:success success
           :result result
           :message (if success
                      (str "Deleted " service-id)
                      (str "Unable to delete " service-id))}))
      {:success false
       :result :no-such-service-exists
       :message (str service-id " does not exist!")}))

  (scale-app [this service-id scale-to-instances]
    (if (scheduler/app-exists? this service-id)
      (let [completion-promise (promise)]
        (send id->service-agent set-service-scale service-id scale-to-instances completion-promise)
        (let [result (deref completion-promise)
              success (= result :scaled)]
          {:success success
           :result result
           :message (if success
                      (str "Scaled " service-id)
                      (str "Unable to scale " service-id))}))
      {:success false
       :result :no-such-service-exists
       :message (str service-id " does not exist!")}))

  (retrieve-directory-content [_ service-id instance-id _ relative-directory]
    (let [id->service @id->service-agent
          service-entry (get id->service service-id)]
      (directory-content service-entry instance-id relative-directory)))

  (service-id->state [_ service-id]
    (let [id->service @id->service-agent
          service-entry (get id->service service-id)]
      service-entry))

  (state [_]
    {:id->service @id->service-agent
     :port->reservation @port->reservation-atom}))

(s/defn ^:always-validate create-shell-scheduler
  "Returns a new ShellScheduler with the provided configuration. Validates the
  configuration against shell-scheduler-schema and throws if it's not valid."
  [{:keys [failed-instance-retry-interval-ms health-check-interval-ms health-check-timeout-ms port-grace-period-ms port-range work-directory]}]
  {:pre [(utils/pos-int? failed-instance-retry-interval-ms)
         (utils/pos-int? health-check-interval-ms)
         (utils/pos-int? health-check-timeout-ms)
         (utils/pos-int? port-grace-period-ms)
         (and (every? utils/pos-int? port-range)
              (= 2 (count port-range))
              (<= (first port-range) (second port-range)))
         (not (str/blank? work-directory))]}
  (let [id->service-agent (agent {})
        port->reservation-atom (atom {})]
    (->ShellScheduler (-> work-directory
                          io/file
                          (.getCanonicalPath))
                      id->service-agent
                      port->reservation-atom
                      port-grace-period-ms
                      port-range)))

(defn shell-scheduler
  "Creates and starts shell scheduler with loops"
  [{:keys [failed-instance-retry-interval-ms health-check-interval-ms health-check-timeout-ms port-grace-period-ms port-range work-directory] :as config}]
  (let [{:keys [id->service-agent port->reservation-atom] :as scheduler} (create-shell-scheduler config)
        http-client (http/client {:connect-timeout health-check-timeout-ms
                                  :idle-timeout health-check-timeout-ms})]
    (start-updating-health id->service-agent port->reservation-atom port-grace-period-ms health-check-interval-ms http-client)
    (start-retry-failed-instances id->service-agent port->reservation-atom port-range failed-instance-retry-interval-ms)
    scheduler))
