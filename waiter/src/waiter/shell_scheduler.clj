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
            [waiter.utils :as utils])
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
  [service-id working-dir-base-path command backend-protocol environment num-ports port->reservation-atom port-range]
  (when-not command
    (throw (ex-info "The command to run was not supplied" {:service-id service-id})))
  (let [reserved-ports (reserve-ports! num-ports port->reservation-atom port-range)
        process-environment (into environment
                                  (map (fn build-port-environment [index port]
                                         [(str "PORT" index) (str port)])
                                       (range 0 (-> reserved-ports count inc))
                                       reserved-ports))
        {:keys [instance-id process started-at working-directory]}
        (launch-process service-id working-dir-base-path command process-environment)]
    (scheduler/make-ServiceInstance
      {:id instance-id
       :service-id service-id
       :started-at started-at
       :healthy? nil
       :host "localhost"
       :port (-> reserved-ports first)
       :extra-ports (-> reserved-ports rest vec)
       :protocol backend-protocol
       :log-directory working-directory
       :shell-scheduler/process process
       :shell-scheduler/working-directory working-directory
       :shell-scheduler/last-health-check-time (t/epoch)
       :shell-scheduler/pid (pid process)})))

(defn launch-service
  "Creates a new service and launches a single new instance for this service"
  [service-id {:strs [backend-proto cmd ports] :as service-description} service-id->password-fn
   working-dir-base-path port->reservation-atom port-range]
  (let [environment (scheduler/environment service-id service-description service-id->password-fn working-dir-base-path)
        instance (launch-instance service-id working-dir-base-path cmd backend-proto environment ports port->reservation-atom port-range)
        service (scheduler/make-Service {:id service-id
                                         :instances 1
                                         :environment environment
                                         :service-description service-description
                                         :task-count 1
                                         :task-stats {:running 1
                                                      :healthy 0
                                                      :unhealthy 0
                                                      :staged 0}})]
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
          (assoc id->service service-id {:service service
                                         :id->instance {(:id instance) instance}}))))
    (catch Throwable e
      (log/error e "error attempting to create service" service-id)
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
          (dorun (pc/map-vals #(kill-process! % port->reservation-atom port-grace-period-ms) id->instance)))
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

(defn active?
  "Returns true if the given instance is considered active"
  [instance]
  (not (:killed instance)))

(defn- healthy?
  "Returns true if the given instance is healthy"
  [instance]
  (:healthy? instance))

(defn- unhealthy?
  "Returns true if the given instance is unhealthy (but active)"
  [instance]
  (and (active? instance) (not (healthy? instance))))

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
  "Runs a health check against instance if enough time has passed since the last one"
  [{:keys [:shell-scheduler/last-health-check-time] :as instance} health-check-url http-client health-check-interval-ms]
  (if (and (active? instance)
           (t/after? (t/now) (t/plus last-health-check-time (t/millis health-check-interval-ms))))
    (-> instance
        (assoc :healthy? (perform-health-check instance health-check-url http-client))
        (assoc :shell-scheduler/last-health-check-time (t/now)))
    instance))

(defn- update-service-health
  "Runs health checks against all instances of service and returns the updated service-entry"
  [{:keys [id->instance service] :as service-entry} health-check-timeout-ms health-check-interval-ms]
  (let [{:strs [health-check-url]} (:service-description service)
        http-client (http/client {:connect-timeout health-check-timeout-ms
                                  :idle-timeout health-check-timeout-ms})
        health-check #(update-instance-health % health-check-url http-client health-check-interval-ms)
        id->instance' (pc/map-vals health-check id->instance)]
    (-> service-entry
        (assoc :id->instance id->instance')
        (assoc-in [:service :task-stats :healthy] (->> id->instance' vals (filter healthy?) count))
        (assoc-in [:service :task-stats :unhealthy] (->> id->instance' vals (filter unhealthy?) count))
        (assoc-in [:service :task-stats :running] (->> id->instance' vals (filter active?) count)))))

(defn- service-entry->instances
  "Converts the given service-entry to a map of shape:

    {:active-instances [...]
     :failed-instances [...]
     :killed-instances [...]}

  after running health checks on all instances of the service."
  [service-entry health-check-timeout-ms health-check-interval-ms]
  (let [{:keys [service id->instance]}
        (update-service-health service-entry health-check-timeout-ms health-check-interval-ms)]
    [service
     {:active-instances (filter active? (vals id->instance))
      :failed-instances (filter :failed (vals id->instance))
      :killed-instances (filter :killed (vals id->instance))}]))

(defn- scale-service
  "Given the current id->service map, scales the service-id to the
  requested number of instances and returns a new id->service map"
  [id->service service-id scale-to-instances port->reservation-atom port-range completion-promise]
  (try
    (if (contains? id->service service-id)
      (let [{:keys [id->instance service] :as service-entry} (get id->service service-id)
            {:keys [environment service-description]} service
            work-directory (get environment "HOME")
            {:strs [backend-proto cmd ports]} service-description
            current-instances (->> id->instance vals (filter active?) count)
            to-launch (- scale-to-instances current-instances)
            launch-new
            #(let [instance (launch-instance service-id work-directory cmd backend-proto environment ports port->reservation-atom port-range)]
               [(:id instance) instance])]
        (if (pos? to-launch)
          (do
            (log/info "scaling" service-id "to" scale-to-instances "instances from" current-instances)
            (deliver completion-promise :scaled)
            (assoc id->service
              service-id
              (-> service-entry
                  (assoc :id->instance (merge id->instance (into {} (repeatedly to-launch launch-new))))
                  (assoc-in [:service :instances] scale-to-instances)
                  (assoc-in [:service :task-count] scale-to-instances)
                  (assoc-in [:service :task-stats :running] scale-to-instances))))
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

(defn- delete-instance
  "Deletes the instance corresponding to service-id/instance-id and returns the updated id->service map"
  [id->service service-id instance-id port->reservation-atom port-grace-period-ms completion-promise]
  (try
    (if (contains? id->service service-id)
      (let [{:keys [id->instance]} (get id->service service-id)
            instance (get id->instance instance-id)]
        (if instance
          (do
            (log/info "deleting instance" instance-id)
            (kill-process! instance port->reservation-atom port-grace-period-ms)
            (deliver completion-promise :deleted)
            (assoc-in id->service
                      [service-id :id->instance instance-id :killed]
                      true))
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
;   :shell-scheduler/process
;   :shell-scheduler/working-directory
;   :shell-scheduler/last-health-check-time
;   :shell-scheduler/pid
;
(defrecord ShellScheduler [work-directory id->service-agent health-check-timeout-ms health-check-interval-ms
                           port->reservation-atom port-grace-period-ms port-range]
  scheduler/ServiceScheduler

  (get-apps->instances [_]
    (let [id->service @id->service-agent]
      (into {}
            (map (fn [[_ service-entry]]
                   (service-entry->instances service-entry health-check-timeout-ms health-check-interval-ms))
                 id->service))))

  (get-apps [_]
    (let [id->service @id->service-agent]
      (map (fn [[_ {:keys [service]}]] service) id->service)))

  (get-instances [_ service-id]
    (let [id->service @id->service-agent
          service-entry (get id->service service-id)]
      (second (service-entry->instances service-entry health-check-timeout-ms health-check-interval-ms))))

  (kill-instance [this {:keys [id service-id] :as instance}]
    (if (scheduler/app-exists? this service-id)
      (let [completion-promise (promise)]
        (send id->service-agent delete-instance service-id id
              port->reservation-atom port-grace-period-ms
              completion-promise)
        (let [result (deref completion-promise)
              success (= result :deleted)]
          {:success success
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

  (scale-app [this service-id instances]
    (if (scheduler/app-exists? this service-id)
      (let [completion-promise (promise)]
        (send id->service-agent scale-service service-id instances
              port->reservation-atom port-range completion-promise)
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

(s/defn ^:always-validate shell-scheduler
  "Returns a new ShellScheduler with the provided configuration. Validates the
  configuration against shell-scheduler-schema and throws if it's not valid."
  [{:keys [health-check-interval-ms health-check-timeout-ms port-grace-period-ms port-range work-directory]}]
  {:pre [(utils/pos-int? health-check-interval-ms)
         (utils/pos-int? health-check-timeout-ms)
         (utils/pos-int? port-grace-period-ms)
         (and (every? utils/pos-int? port-range)
              (= 2 (count port-range))
              (<= (first port-range) (second port-range)))
         (not (str/blank? work-directory))]}
  (->ShellScheduler (-> work-directory
                        io/file
                        (.getCanonicalPath))
                    (agent {})
                    health-check-timeout-ms
                    health-check-interval-ms
                    (atom {})
                    port-grace-period-ms
                    port-range))
