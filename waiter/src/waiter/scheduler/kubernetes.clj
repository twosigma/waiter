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
(ns waiter.scheduler.kubernetes
  (:require [cheshire.core :as cheshire]
            [clj-http.client :as clj-http]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.data.zip.xml :as zx]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.xml :as xml]
            [clojure.zip :as zip]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [schema.core :as s]
            [slingshot.slingshot :as ss]
            [waiter.authorization :as authz]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.schema :as schema]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.cache-utils :as cu]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (java.io InputStreamReader)
           (java.util.concurrent Executors)
           (org.joda.time.format DateTimeFormat)))

(defn authorization-from-environment
  "Sample implementation of the authentication string refresh function.
   Returns a map with :auth-token mapped to a string that can be used as
   the value for the Authorization HTTP header, reading the string from
   the WAITER_K8S_AUTH_STRING environment variable."
  [_]
  (log/info "called waiter.scheduler.kubernetes/authorization-from-environment")
  {:auth-token (System/getenv "WAITER_K8S_AUTH_STRING")})

(def k8s-api-auth-str
  "Atom containing authentication string for the Kubernetes API server.
   This value may be periodically refreshed asynchronously."
  (atom nil))

(def k8s-timestamp-format
  "Kubernetes reports dates in ISO8061 format, sans the milliseconds component."
  (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mm:ss'Z'"))

(def ^:const waiter-primary-container-name "waiter-app")
(def ^:const waiter-fileserver-sidecar-name "waiter-fileserver")
(def ^:const waiter-raven-sidecar-name "waiter-raven-sidecar")

(defn timestamp-str->datetime
  "Parse a Kubernetes API timestamp string."
  [k8s-timestamp-str]
  (du/str-to-date-safe k8s-timestamp-str k8s-timestamp-format))

(defn default-k8s-message-transform
  "Takes k8s api error response and creates a user friendly deployment error message"
  [k8s-error-response]
  (get-in k8s-error-response [:body :message] "Unknown reason - check logs"))

(defn create-service-deployment-error
  "Transforms the deployment error message to be more user friendly"
  [k8s-error-response response->deployment-error-msg-fn]
  {:service-deployment-error-details {:k8s-response-body (get-in k8s-error-response [:body])}
   :service-deployment-error-msg (response->deployment-error-msg-fn k8s-error-response)})

(defn- use-short-service-hash? [k8s-max-name-length]
  ;; This is fairly arbitrary, but if we have at least 48 characters for the app name,
  ;; then we can fit the full 32 character service-id hash, plus a hyphen as a separator,
  ;; and still have 25 characters left for some prefix of the app name.
  ;; If we have fewer than 48 characters, then we'll probably want to shorten the hash.
  (< k8s-max-name-length 48))

(defn service-id->k8s-app-name
  "Shorten a full Waiter service-id to a Kubernetes-compatible application name.
   May return the service-id unmodified if it doesn't violate the
   configured name-length restrictions for this Kubernetes cluster.

   Example:

   (service-id->k8s-app-name
     {:max-name-length 32}
     \"waiter-myapp-e8b625cc83c411e8974c38d5474b213d\")
   ==> \"myapp-e8b625cc474b213d\""
  [{:keys [max-name-length pod-suffix-length] :as scheduler} service-id]
  (let [[_ app-prefix x y z] (re-find #"([^-]+)-(\w{8})(\w+)(\w{8})$" service-id)
        k8s-max-name-length (- max-name-length pod-suffix-length 1)
        suffix (if (use-short-service-hash? k8s-max-name-length)
                 (str \- x z)
                 (str \- x y z))
        prefix-max-length (- k8s-max-name-length (count suffix))
        app-prefix' (cond-> app-prefix
                      (< prefix-max-length (count app-prefix))
                      (subs 0 prefix-max-length))]
    (str app-prefix' suffix)))

(defn service-id->service-hash
  "Extract the 32-char (256-bit) hash string from a Waiter service-id.
   Returns the whole service-id if it's 32 characters or shorter."
  [service-id]
  (let [hash-offset (- (count service-id) 32)]
    (cond-> service-id
      (pos? hash-offset)
      (subs hash-offset))))

(defn quantity->double-value
  "Converts a k8s quantity string to a numeric value.
   A k8s quantity is a fixed-point representation of a number defined at
   https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/"
  [quantity]
  (cond
    (str/ends-with? quantity "Pi") (-> quantity (str/replace "Pi" "") (utils/parse-double) (* 1.125899906842624e15))
    (str/ends-with? quantity "Ti") (-> quantity (str/replace "Ti" "") (utils/parse-double) (* 1.099511627776e12))
    (str/ends-with? quantity "Gi") (-> quantity (str/replace "Gi" "") (utils/parse-double) (* 1.073741824e9))
    (str/ends-with? quantity "Mi") (-> quantity (str/replace "Mi" "") (utils/parse-double) (* 1.048576e6))
    (str/ends-with? quantity "Ki") (-> quantity (str/replace "Ki" "") (utils/parse-double) (* 1.024e3))
    (str/ends-with? quantity "P") (-> quantity (str/replace "P" "") (utils/parse-double) (* 1e15))
    (str/ends-with? quantity "T") (-> quantity (str/replace "T" "") (utils/parse-double) (* 1e12))
    (str/ends-with? quantity "G") (-> quantity (str/replace "G" "") (utils/parse-double) (* 1e9))
    (str/ends-with? quantity "M") (-> quantity (str/replace "M" "") (utils/parse-double) (* 1e6))
    (str/ends-with? quantity "K") (-> quantity (str/replace "K" "") (utils/parse-double) (* 1e3))
    (str/ends-with? quantity "m") (-> quantity (str/replace "m" "") (utils/parse-double) (/ 1e3))
    :else (-> (utils/parse-double quantity))))

(defn replicaset->Service
  "Convert a Kubernetes ReplicaSet JSON response into a Waiter Service record."
  [replicaset-json]
  (try
    (pc/letk
      [[spec
        [:metadata name namespace uid [:annotations waiter/service-id]]
        [:status {replicas 0} {availableReplicas 0} {readyReplicas 0} {unavailableReplicas 0}]] replicaset-json
       ;; for backward compatibility where the revision timestamp is missing we cannot use the destructuring above
       rs-annotations (get-in replicaset-json [:metadata :annotations] nil)
       rs-pod-annotations (get-in replicaset-json [:spec :template :metadata :annotations] nil)
       containers (get-in replicaset-json [:spec :template :spec :containers])
       rs-containers (vec (map :name containers))
       rs-container-resources (mapv (fn [{:keys [name] :as container-spec}]
                                      (let [{:keys [cpu memory]} (get-in container-spec [:resources :requests])]
                                        {:cpus (-> cpu (str) (quantity->double-value))
                                         :mem (-> memory (str) (quantity->double-value) (/ (* 1024 1024)))
                                         :name name}))
                                    containers)
       rs-creation-timestamp (some-> replicaset-json (get-in [:metadata :creationTimestamp]) (timestamp-str->datetime) (du/date-to-str))
       requested (get spec :replicas 0)
       staged (- replicas (+ availableReplicas unavailableReplicas))]
      (scheduler/make-Service
        {:id service-id
         :instances requested
         :k8s/app-name name
         :k8s/container-resources rs-container-resources
         :k8s/containers rs-containers
         :k8s/namespace namespace
         :k8s/replicaset-annotations (dissoc rs-annotations :waiter/service-id)
         :k8s/replicaset-creation-timestamp rs-creation-timestamp
         :k8s/replicaset-pod-annotations (dissoc rs-pod-annotations :waiter/service-id)
         :k8s/replicaset-uid uid
         :task-count replicas
         :task-stats {:healthy readyReplicas
                      :running (- replicas staged)
                      :staged staged
                      :unhealthy (- replicas readyReplicas staged)}}))
    (catch Throwable t
      (log/error t "error converting ReplicaSet to Waiter Service"))))

(defn create-empty-service
  "Creates an instance of an empty service"
  [service-id]
  (scheduler/make-Service
    {:id service-id
     :instances 0
     :task-count 0}))

(defn k8s-object->id
  "Get the id (name) from a ReplicaSet or Pod's metadata"
  [k8s-obj]
  (get-in k8s-obj [:metadata :name]))

(defn k8s-object->namespace
  "Get the namespace from a ReplicaSet or Pod's metadata"
  [k8s-obj]
  (get-in k8s-obj [:metadata :namespace]))

(defn k8s-object->resource-version
  "Get the resource version from a Kubernetes API response object.
   Valid on ReplicaSets, Pods, and watch-update objects."
  [k8s-obj]
  (some-> k8s-obj
    (get-in [:metadata :resourceVersion])
    (Long/parseLong)))

(defn k8s-object->service-id
  "Get the Waiter service-id from a ReplicaSet or Pod's annotations"
  [k8s-obj]
  (get-in k8s-obj [:metadata :annotations :waiter/service-id]))

(defn- pod->instance-id
  "Construct the Waiter instance-id for the given Kubernetes pod incarnation.
   Note that a new Waiter Service Instance is created each time a pod restarts,
   and that we generate a unique instance-id by including the pod's restartCount value."
  [pod restart-count]
  (let [pod-name (k8s-object->id pod)
        service-id (k8s-object->service-id pod)]
    (str service-id \. pod-name \- restart-count)))

(defn- unpack-instance-id
  "Extract the service-id, pod name, and pod restart-number from an instance-id."
  [instance-id]
  (let [[_ service-id pod-name restart-number] (re-find #"^([-a-z0-9]+)\.([-a-z0-9]+)-(\d+)$" instance-id)]
    {:service-id service-id
     :pod-name pod-name
     :restart-number restart-number}))

(defn- log-dir-path [user restart-count]
  "Build log directory path string for a containter run."
  (str "/home/" user "/r" restart-count))

(defn- killed-by-k8s?
  "Determine whether a pod was killed (restarted) by its corresponding Kubernetes liveness checks."
  [{:keys [exitCode reason] :as pod-terminated-info}]
  ;; TODO (#351) - Look at events for messages about liveness probe failures.
  ;; Currently, we assume any SIGKILL (137) or SIGTERM (143) exit code
  ;; with the default "Error" reason string indicates a livenessProbe kill.
  (and (contains? #{137 143} exitCode)
       (= "Error" reason)))

(defn retrieve-container-status
  "Retrieves the status of the specific container in the pod."
  [pod container-name]
  (->> (get-in pod [:status :containerStatuses])
    (filter #(= container-name (:name %)))
    (first)))

(defn- track-failed-instances!
  "Update this KubernetesScheduler's service-id->failed-instances-transient-store
   when a new pod failure is listed in the given pod's lastState container status.
   Note that unique instance-ids are deterministically generated each time the pod is restarted
   by passing the pod's restartCount value to the pod->instance-id function."
  [{:keys [service-id] :as live-instance} {:keys [service-id->failed-instances-transient-store] :as scheduler} pod]
  (try
    (let [primary-container-status (retrieve-container-status pod waiter-primary-container-name)]
      (when-let [newest-failure (get-in primary-container-status [:lastState :terminated])]
        (when-let [restart-count (:restartCount primary-container-status)]
          (let [failure-flags (if (= "OOMKilled" (:reason newest-failure)) #{:memory-limit-exceeded} #{})
                newest-failure-start-time (-> newest-failure :startedAt timestamp-str->datetime)
                newest-failure-id (pod->instance-id pod (dec restart-count))
                failures (-> service-id->failed-instances-transient-store deref (get service-id))]
            (when-not (some #(= (:id %) newest-failure-id) failures)
              (let [newest-failure-instance (cond-> (assoc live-instance
                                                      :flags failure-flags
                                                      :healthy? false
                                                      :id newest-failure-id
                                                      :log-directory (log-dir-path (:k8s/user live-instance)
                                                                                   (dec restart-count))
                                                      :started-at newest-failure-start-time
                                                      :status "Failed")
                                              ;; To match the behavior of the marathon scheduler,
                                              ;; we don't include the exit code in failed instances that were killed by k8s.
                                              (not (killed-by-k8s? newest-failure))
                                              (assoc :exit-code (:exitCode newest-failure)))]
                (scheduler/add-to-store-and-track-failed-instance!
                  service-id->failed-instances-transient-store scheduler/max-failed-instances-to-keep service-id newest-failure-instance)))))))
    (catch Throwable e
      (log/error e "error converting failed pod to waiter service instance" pod)
      (comment "Returning nil on failure."))))

(defn- check-expired
  "Returns true when the pod instance can be classified as expired.
   An instance can be expired for the following reasons:
   - it has restarted too many times (reached the restart-expiry-threshold threshold)
   - the primary container (waiter-apps) has not transitioned to running state in container-running-grace-secs seconds
   - the pod has the waiter/pod-expired=true annotation
   - the replicaset for the pod has been updated by using a newer revision timestamp or the same timestamp with a newer revision version."
  [{:keys [container-running-grace-secs restart-expiry-threshold watch-state] :as scheduler}
   service-id instance-id restart-count {:keys [waiter/pod-expired waiter/revision-timestamp waiter/revision-version]}
   primary-container-status pod-started-at]
  (let [rs-revision-timestamp-path [:service-id->service service-id :k8s/replicaset-annotations :waiter/revision-timestamp]
        watch-state-value @watch-state
        rs-revision-timestamp (get-in watch-state-value rs-revision-timestamp-path)
        rs-revision-version-path [:service-id->service service-id :k8s/replicaset-annotations :waiter/revision-version]
        rs-revision-version (get-in watch-state-value rs-revision-version-path)]
    (cond
      (>= restart-count restart-expiry-threshold)
      (do
        (log/info "instance expired as it reached the restart threshold"
                  {:instance-id instance-id
                   :restart-count restart-count})
        true)
      (and pod-started-at
           (pos? container-running-grace-secs)
           (empty? (:lastState primary-container-status))
           (not (contains? (:state primary-container-status) :running))
           (<= container-running-grace-secs (t/in-seconds (t/interval pod-started-at (t/now)))))
      (do
        (log/info "instance expired as it took too long to transition to running state"
                  {:instance-id instance-id
                   :primary-container-status primary-container-status
                   :started-at pod-started-at})
        true)
      (= "true" pod-expired)
      (do
        (log/info "instance expired based on pod annotation"
                  {:instance-id instance-id
                   :waiter/pod-expired pod-expired})
        true)

      (let [revision-timestamp-comp (compare rs-revision-timestamp revision-timestamp)]
        (or (pos? revision-timestamp-comp)
            (and (zero? revision-timestamp-comp)
                 (or (some? rs-revision-version) (some? revision-version))
                 (try
                   ;; for backward compatibility, default the missing revision version value to zero.
                   (let [rs-revision-version-int (Integer/parseInt (or rs-revision-version "0"))
                         pod-revision-version-int (Integer/parseInt (or revision-version "0"))]
                     (> rs-revision-version-int pod-revision-version-int))
                   (catch Exception ex
                     (log/error ex "error in comparing revision versions" {:pod revision-version :rs rs-revision-version})
                     false)))))
      (do
        (log/info "instance expired based on pod and replicaset revision timestamps"
                  {:instance-id instance-id
                   :revision-timestamp {:pod revision-timestamp :rs rs-revision-timestamp}
                   :revision-version {:pod revision-version :rs rs-revision-version}})
        true))))

;; forward declaration of the hard-delete-service-instance function to avoid reordering a bunch of related functions
(declare hard-delete-service-instance)

(let [kill-restart-threshold-exceeded-pod-thread-pool (Executors/newFixedThreadPool 1)]
  (defn kill-restart-threshold-exceeded-pod
    "Processes killing of a frequently restarting pod (service instance)."
    [scheduler {:keys [k8s/pod-name k8s/restart-count service-id] :as instance}]
    (au/execute
      (fn kill-restart-threshold-exceeded-pod-task []
        (try
          (log/info "deleting frequently restarting pod"
                    {:pod-name pod-name :restart-count restart-count :service-id service-id})
          (hard-delete-service-instance scheduler instance)
          (catch Exception ex
            (log/error ex "error in deleting frequently restarting pod" pod-name))))
      kill-restart-threshold-exceeded-pod-thread-pool)))

(defn port->key
  "Convert port numbers to keywords for use as keys in JSON-friendly maps."
  [port-number]
  ;; Clojure keywords should start with a non-numeric value
  ;; see https://clojure.org/reference/reader#_literals
  (keyword (str "p" port-number)))

(defn retrieve-failing-container-names
  "Retrieves the names of containers that are failing (i.e. in a CrashLoopBackOff)."
  [container-statuses type-filter]
  (for [{:keys [name ready reason type]} container-statuses
        :when (and (= type type-filter)
                   (false? ready)
                   (= "CrashLoopBackOff" reason))]
    name))

(defn pod->ServiceInstance
  "Convert a Kubernetes Pod JSON response into a Waiter Service Instance record."
  [{:keys [api-server-url leader?-fn restart-kill-threshold] :as scheduler} pod]
  (try
    (let [;; waiter-app is the first container we register
          primary-container-status (retrieve-container-status pod waiter-primary-container-name)
          primary-container-restart-count (or (get primary-container-status :restartCount) 0)
          exceeded-restart-kill-threshold? (>= primary-container-restart-count restart-kill-threshold)
          service-id (k8s-object->service-id pod)
          instance-id (pod->instance-id pod primary-container-restart-count)
          node-name (get-in pod [:spec :nodeName])
          pod-labels (get-in pod [:metadata :labels])
          pod-annotations (get-in pod [:metadata :annotations])
          port0 (or (some-> pod-annotations :waiter/service-port (Integer/parseInt))
                    (get-in pod [:spec :containers 0 :ports 0 :containerPort]))
          port->protocol (some-> pod-annotations :waiter/port-onto-protocol (utils/try-parse-json keyword))
          raven-mode (get pod-labels :waiter/raven "disabled")
          run-as-user (or (get-in pod [:metadata :labels :waiter/user])
                          ;; falling back to namespace for legacy pods missing the waiter/user label
                          (k8s-object->namespace pod))
          ;; pod phase documentation: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
          {:keys [phase] :as pod-status} (:status pod)
          init-container-statuses (get pod-status :initContainerStatuses)
          unready-init-containers-statuses (filter (fn init-container-unready? [{:keys [ready restartCount]}]
                                                     (and (false? ready) (integer? restartCount)))
                                                   init-container-statuses)
          app-container-statuses (get pod-status :containerStatuses)
          ;; preserve the application container statuses as the leading entries
          container-statuses (concat (map #(assoc % :type :app) app-container-statuses)
                                     (map #(assoc % :type :init) init-container-statuses))
          ;; uses restart count from init containers when any of them are not ready
          pod-restart-count (if (seq unready-init-containers-statuses)
                              (reduce max (map :restartCount unready-init-containers-statuses))
                              primary-container-restart-count)
          pod-started-at (-> pod (get-in [:status :startTime]) timestamp-str->datetime)
          {:keys [waiter/revision-timestamp waiter/revision-version]} (get-in pod [:metadata :annotations])
          pod-name (k8s-object->id pod)
          primary-container-ready (true? (get primary-container-status :ready))
          healthy? (and primary-container-ready
                        ;; Note that when exceeded-restart-kill-threshold? becomes true, the container *just* restarted and is non-ready,
                        ;; therefore it's impossible to observe a healthy? status between the time the container restarted
                        ;; and the time that the instance was marked permanently unhealthy due to the high restart count.
                        (not exceeded-restart-kill-threshold?))
          pod-container-statuses (for [{:keys [restartCount state] :as status} container-statuses
                                       :let [[state-key {:keys [reason]}] (first state)]]
                                   (do
                                     (when (> (count state) 1)
                                       (log/warn "received multiple states for container:" status))
                                     (cond-> (select-keys status [:image :name :ready :type])
                                       (some? reason) (assoc :reason reason)
                                       (some? restartCount) (assoc :restart-count restartCount)
                                       (some? state-key) (assoc :state state-key))))
          failing-init-containers (retrieve-failing-container-names pod-container-statuses :init)
          failing-app-containers (retrieve-failing-container-names pod-container-statuses :app)
          status-message (cond
                           (seq failing-init-containers) (str "Init container(s) in crash loop: " (str/join ", " failing-init-containers))
                           exceeded-restart-kill-threshold? "Application restarting frequently"
                           (some #(= waiter-primary-container-name %) failing-app-containers) "Application in crash loop"
                           (seq failing-app-containers) (str "Application container(s) in crash loop: " (str/join ", " failing-app-containers))
                           healthy? "Healthy"
                           :else "Unhealthy")
          instance (scheduler/make-ServiceInstance
                     (cond-> {:extra-ports (->> pod-annotations :waiter/port-count Integer/parseInt range next (mapv #(+ port0 %)))
                              :flags (cond-> #{}
                                       (check-expired scheduler service-id instance-id pod-restart-count pod-annotations primary-container-status pod-started-at)
                                       (conj :expired))
                              :healthy? healthy?
                              :host (get-in pod [:status :podIP] scheduler/UNKNOWN-IP)
                              :id instance-id
                              :k8s/api-server-url api-server-url
                              :k8s/app-name (get-in pod [:metadata :labels :app])
                              :k8s/namespace (k8s-object->namespace pod)
                              :k8s/pod-name pod-name
                              :k8s/port->protocol port->protocol
                              :k8s/raven raven-mode
                              :k8s/restart-count primary-container-restart-count
                              :k8s/user run-as-user
                              :log-directory (log-dir-path run-as-user primary-container-restart-count)
                              :port port0
                              :service-id service-id
                              :started-at pod-started-at
                              :status status-message}
                       node-name (assoc :k8s/node-name node-name)
                       phase (assoc :k8s/pod-phase phase)
                       revision-timestamp (assoc :k8s/revision-timestamp revision-timestamp)
                       revision-version (assoc :k8s/revision-version revision-version)
                       (seq pod-container-statuses) (assoc :k8s/container-statuses pod-container-statuses)))]
      (when exceeded-restart-kill-threshold?
        (try
          (if (leader?-fn)
            (kill-restart-threshold-exceeded-pod scheduler instance)
            (log/info "skipping deleting frequently restarting pod on non-leader"
                      {:pod-name pod-name :restart-count primary-container-restart-count :service-id service-id}))
          (catch Throwable e
            (log/error e "error killing frequently restarting pod" pod-name))))
      instance)
    (catch Throwable e
      (log/error e "error converting pod to waiter service instance" pod)
      (comment "Returning nil on failure."))))

(defn- pod-logs-live?
  "Returns true if the given pod has its log fileserver running.
   Assumes that the pod is configured to run with the fileserver container and that the container is at index 1."
  [pod]
  (and (some? pod)
       (-> pod
         (retrieve-container-status waiter-fileserver-sidecar-name)
         (get-in [:state :terminated])
         (nil?))))

(defn streaming-api-request
  "Make a long-lived HTTP request to the Kubernetes API server using the configured authentication.
   If data is provided via :body, the application/json content type is added automatically.
   The response payload (if any) is returned as a lazy seq of parsed JSON objects."
  [url request-options]
  (let [keyword-keys? true
        auth-str @k8s-api-auth-str
        request-options (cond-> (assoc request-options :as :stream)
                          auth-str (assoc :headers {"Authorization" auth-str}))
        {:keys [body error status] :as response} (clj-http/get url request-options)]
    (when error
      (throw error))
    (when-not (hu/status-2XX? status)
      (ss/throw+ response))
    (-> body
      InputStreamReader.
      (cheshire/parsed-seq keyword-keys?))))

(defn streaming-watch-api-request
  "Make a long-lived watch connection to the Kubernetes API server via the streaming-api-request function.
   If a Failure error is found in the response object stream, an exception is thrown."
  [resource-name url request-options]
  (for [update-json (streaming-api-request url request-options)]
    (if (and (= "ERROR" (:type update-json))
             (= "Failure" (-> update-json :object :status)))
      (throw (ex-info "k8s watch connection failed"
                      {:watch-resource resource-name
                       :watch-response update-json}))
      ;; Drop managedFields from response objects when present (much too verbose!)
      ;; https://github.com/kubernetes/kubernetes/issues/90066#issuecomment-626828512
      (utils/dissoc-in update-json [:object :metadata :managedFields]))))

(defn api-request
  "Make an HTTP request to the Kubernetes API server using the configured authentication.
   If data is provided via :body, the application/json content type is added automatically.
   The response payload (if any) is automatically parsed to JSON."
  [url {:keys [http-client scheduler-name]} & {:keys [body content-type request-method] :or {request-method :get} :as options}]
  (scheduler/log "making request to K8s API server:" url request-method body)
  (ss/try+
    (let [auth-str @k8s-api-auth-str
          result (timers/start-stop-time!
                   (metrics/waiter-timer "scheduler" scheduler-name (name request-method))
                   (pc/mapply hu/http-request http-client url
                              :accept "application/json"
                              (cond-> options
                                auth-str (assoc-in [:headers "Authorization"] auth-str)
                                (and (not content-type) body) (assoc :content-type "application/json"))))]
      (scheduler/log "response from K8s API server:" result)
      result)
    (catch [:status http-400-bad-request] response
      (log/error "malformed K8s API request: " url options response)
      (ss/throw+ response))
    (catch [:client http-client] response
      (log/error "request to K8s API server failed: " url options body response)
      (ss/throw+ response))))

(defn- retrieve-service-description
  "Get the corresponding service-description for a service-id."
  [{:keys [service-id->service-description-fn]} service-id]
  (service-id->service-description-fn service-id))

(defn- get-services
  "Get all Waiter Services (reified as ReplicaSets) running in this Kubernetes cluster."
  [{:keys [service-id->deployment-error-cache watch-state]}]
  (let [service-id->service (-> watch-state deref :service-id->service)
        service-id->deployment-error (cu/cache->map service-id->deployment-error-cache)
        service-ids (set (concat (keys service-id->deployment-error) (keys service-id->service)))]
    (map
      (fn [service-id]
        (let [deployment-error (get-in service-id->deployment-error [service-id :data])
              service (or (get service-id->service service-id) (create-empty-service service-id))]
          (cond-> service
            deployment-error (assoc :deployment-error deployment-error))))
      service-ids)))

(defn- get-replicaset-pods
  "Get all Kubernetes pods associated with the given Waiter Service's corresponding ReplicaSet."
  [{:keys [watch-state] :as scheduler} {service-id :id}]
  (-> watch-state deref :service-id->pod-id->pod (get service-id) vals))

(defn- live-pod?
  "Returns true if the pod has started, but has not yet been deleted."
  [pod]
  (nil? (get-in pod [:metadata :deletionTimestamp])))

(defn- get-service-instances!
  "Get all active Waiter Service Instances associated with the given Waiter Service.
   Also updates the service-id->failed-instances-transient-store as a side-effect.
   Pods reporting Failed phase status are treated as failed instances and are excluded from the return value."
  [{:keys [service-id->failed-instances-transient-store] :as scheduler} basic-service-info]
  (let [all-instances (for [pod (get-replicaset-pods scheduler basic-service-info)
                            :when (live-pod? pod)]
                        (when-let [service-instance (pod->ServiceInstance scheduler pod)]
                          (track-failed-instances! service-instance scheduler pod)
                          service-instance))
        {active-instances false failed-instances true}
        (->> all-instances (remove nil?) (group-by #(-> % :k8s/pod-phase (= "Failed")) ))]
    ;; pods with Failed phase are treated as failed instances
    (doseq [{:keys [service-id] :as failed-instance} failed-instances]
      (->> (assoc failed-instance :healthy? false :status "Failed")
        (scheduler/add-to-store-and-track-failed-instance!
          service-id->failed-instances-transient-store scheduler/max-failed-instances-to-keep service-id)))
    ;; returns only pods with non-Failed phase
    (vec active-instances)))

(defn instances-breakdown!
  "Get all Waiter Service Instances associated with the given Waiter Service.
   Grouped by liveness status, i.e.: {:active-instances [...] :failed-instances [...] :killed-instances [...]}"
  [{:keys [service-id->failed-instances-transient-store] :as scheduler} {service-id :id :as basic-service-info}]
  {:active-instances (get-service-instances! scheduler basic-service-info)
   :failed-instances (-> @service-id->failed-instances-transient-store (get service-id) vec)})

(defn- patch-object-json
  "Make a JSON-patch request on a given Kubernetes object."
  [k8s-object-uri ops scheduler]
  (api-request k8s-object-uri scheduler
               :body (utils/clj->json ops)
               :content-type "application/json-patch+json"
               :request-method :patch))

(defn- patch-object-replicas
  "Update the replica count in the given Kubernetes object's spec."
  [k8s-object-uri replicas replicas' scheduler]
  (patch-object-json k8s-object-uri
                     [{:op :test :path "/spec/replicas" :value replicas}
                      {:op :replace :path "/spec/replicas" :value replicas'}]
                     scheduler))

(defn- get-replica-count
  "Query the current requested replica count for the given Kubernetes object."
  [{:keys [watch-state] :as scheduler} service-id]
  (-> watch-state deref :service-id->service (get service-id) :instances))

(defmacro k8s-patch-with-retries
  "Query the current replica count for the given Kubernetes object,
   retrying a limited number of times in the event of an HTTP 409 conflict error."
  [patch-cmd retry-condition retry-cmd]
  `(let [patch-result# (ss/try+
                         ~patch-cmd
                         (catch [:status http-409-conflict] _#
                           (with-meta
                             `conflict
                             {:throw-context ~'&throw-context})))]
     (if (not= `conflict patch-result#)
       patch-result#
       (if ~retry-condition
         ~retry-cmd
         (throw (-> patch-result# meta :throw-context :throwable))))))

(defn- build-replicaset-url
  "Build the URL for the given Waiter Service's ReplicaSet."
  [{:keys [api-server-url replicaset-api-version]} {:keys [k8s/app-name k8s/namespace]}]
  (when (not-any? str/blank? [namespace app-name])
    (str api-server-url "/apis/" replicaset-api-version
         "/namespaces/" namespace "/replicasets/" app-name)))

(defn- scale-service-up-to
  "Scale the number of instances for a given service to a specific number.
   Only used for upward scaling. No-op if it would result in downward scaling."
  [{:keys [max-patch-retries] :as scheduler} {service-id :id :as service} instances']
  (let [replicaset-url (build-replicaset-url scheduler service)]
    (loop [attempt 1
           instances (:instances service)]
      (if (<= instances' instances)
        (log/warn "skipping non-upward scale-up request on" service-id
                  "from" instances "to" instances')
        (k8s-patch-with-retries
          (patch-object-replicas replicaset-url instances instances' scheduler)
          (<= attempt max-patch-retries)
          (recur (inc attempt) (get-replica-count scheduler service-id)))))))

(defn- scale-service-by-delta
  "Scale the number of instances for a given service by a given delta.
   Can scale either upward (positive delta) or downward (negative delta)."
  [{:keys [max-patch-retries] :as scheduler} {service-id :id :as service} instances-delta]
  (let [replicaset-url (build-replicaset-url scheduler service)]
    (loop [attempt 1
           instances (:instances service)]
      (let [instances' (+ instances instances-delta)]
        (k8s-patch-with-retries
          (patch-object-replicas replicaset-url instances instances' scheduler)
          (<= attempt max-patch-retries)
          (recur (inc attempt) (get-replica-count scheduler service-id)))))))

(defn- instance->pod-url
  "Returns the pod api server url."
  [api-server-url {:keys [k8s/namespace k8s/pod-name]}]
  (str api-server-url "/api/v1/namespaces/" namespace "/pods/" pod-name))

(defn hard-delete-service-instance
  "Force kill the Kubernetes pod corresponding to the given Waiter Service Instance.
   Does not adjust ReplicaSet replica count; preventing scheduling of a replacement pod must be ensured by the callee.
   Returns nil on success, but throws on failure."
  [{:keys [api-server-url] :as scheduler} {:keys [service-id] :as instance}]
  (let [pod-url (instance->pod-url api-server-url instance)]
    (try
      (api-request pod-url scheduler :request-method :delete)
      (catch Throwable t
        (log/error t "Error force-killing pod")))))

(defn kill-service-instance
  "Safely kill the Kubernetes pod corresponding to the given Waiter Service Instance.
   Also adjusts the ReplicaSet replica count to prevent a replacement pod from being started.
   Returns nil on success, but throws on failure."
  [{:keys [api-server-url] :as scheduler} {:keys [service-id] :as instance} service]
  ;; SAFE DELETION STRATEGY:
  ;; 1) Delete the target pod with a grace period of 5 minutes
  ;;    Since the target pod is currently in the "Terminating" state,
  ;;    the owner ReplicaSet will not immediately create a replacement pod.
  ;;    Since we never expect the 5 minute period to elapse before step 3 completes,
  ;;    we call this the "soft" delete.
  ;; 2) Scale down the owner ReplicaSet by 1 pod.
  ;;    Since the target pod is still in the "Terminating" state (assuming delay < 5min),
  ;;    the owner ReplicaSet will not immediately delete a different victim pod.
  ;; 3) Delete the target pod. This immediately removes the pod from Kubernetes.
  ;;    The state of the ReplicaSet (desired vs actual pods) should now be consistent.
  ;;    This eager delete overrides the 5-minute delay from above,
  ;;    making this a "hard" delete, using the default grace period set on the pod.
  ;; Note that if it takes more than 5 minutes to get from step 1 to step 2,
  ;; we assume we're already so far out of sync that the possibility of non-atomic scaling
  ;; doesn't hurt us significantly. If it takes more than 5 minutes to get from step 1
  ;; to step 3, then the pod was already deleted, and the force-delete is no longer needed.
  ;; The force-delete can fail with a 404 (object not found), but this operation still succeeds.
  (let [pod-url (instance->pod-url api-server-url instance)]
    ; "soft" delete of the pod (i.e., simply transition the pod to "Terminating" state)
    (api-request pod-url scheduler :request-method :delete
                 :body (utils/clj->json {:kind "DeleteOptions" :apiVersion "v1" :gracePeriodSeconds 300}))
    ; scale down the replicaset to reflect removal of this instance
    (try
      (scale-service-by-delta scheduler service -1)
      (catch Throwable t
        (log/error t "Error while scaling down ReplicaSet after pod termination")))
    ; "hard" delete the pod (i.e., actually kill, allowing the pod's default grace period expires)
    ; (note that the pod's default grace period is different from the 300s period set above)
    (hard-delete-service-instance scheduler instance)
    (comment "Success! Even if the scale-down or force-kill operation failed,
              the pod will be force-killed after the grace period is up.")))

(defn invoke-replicaset-spec-builder-fn
  "Helper function to invoke replicaset-spec-builder-fn and ensure arity is maintained in invocation."
  [{:keys [replicaset-spec-builder-fn] :as scheduler} service-id service-description context]
  (replicaset-spec-builder-fn scheduler service-id service-description context))

(defn create-service
  "Reify a Waiter Service as a Kubernetes ReplicaSet."
  [{:keys [run-as-user-source service-description service-id]}
   {:keys [api-server-url pdb-api-version pdb-spec-builder-fn replicaset-api-version
           response->deployment-error-msg-fn service-id->deployment-error-cache] :as scheduler}]
  (let [{:strs [cmd-type]} service-description]
    (when (= "docker" cmd-type)
      (throw (ex-info "Unsupported command type on service"
                      {:cmd-type cmd-type
                       :service-description service-description
                       :service-id service-id}))))
  (let [rs-spec-builder-context {:run-as-user-source run-as-user-source}
        rs-spec (invoke-replicaset-spec-builder-fn scheduler service-id service-description rs-spec-builder-context)
        request-namespace (k8s-object->namespace rs-spec)
        request-url (str api-server-url "/apis/" replicaset-api-version "/namespaces/" request-namespace "/replicasets")
        response-json
        (ss/try+
          (api-request request-url scheduler :body (utils/clj->json rs-spec) :request-method :post)
          (catch Object response
            ; Don't create deployment error for http-409-conflict (replicaset already exists)
            (when-not (= (:status response) http-409-conflict)
              (let [deployment-error (create-service-deployment-error response response->deployment-error-msg-fn)]
                (log/info "creating deployment error for service" {:deployment-error deployment-error
                                                                   :service-id service-id})
                (cu/cache-put! service-id->deployment-error-cache service-id deployment-error)))
            (ss/throw+ response)))
        {:keys [k8s/replicaset-uid] :as service} (some-> response-json replicaset->Service)]
    (if service
      (cu/cache-evict service-id->deployment-error-cache service-id)
      (throw (ex-info "failed to create service"
                      {:service-description service-description
                       :service-id service-id})))
    (when pdb-spec-builder-fn
      (let [{:strs [min-instances]} service-description]
        (when (> min-instances 1)
          (let [pdb-spec (pdb-spec-builder-fn pdb-api-version rs-spec replicaset-uid)
                request-url (str api-server-url "/apis/" pdb-api-version "/namespaces/" request-namespace "/poddisruptionbudgets")
                with-retries (utils/retry-strategy {:delay-multiplier 1.0 :initial-delay-ms 100 :max-retries 2})]
            (try
              (with-retries
                (fn create-pdb []
                  (api-request request-url scheduler
                               :body (utils/clj->json pdb-spec)
                               :request-method :post)))
              (catch Exception ex
                (log/error ex "unable to create pod disruption budget for service"
                           {:pdb-api-version pdb-api-version :replicaset-uid replicaset-uid :service-id service-id})))))))
    service))

(defn- delete-service
  "Delete the Kubernetes ReplicaSet corresponding to a Waiter Service.
   Owned Pods will be removed asynchronously by the Kubernetes garbage collector."
  [scheduler {:keys [id] :as service}]
  (let [replicaset-url (build-replicaset-url scheduler service)
        kill-json (utils/clj->json
                    {:kind "DeleteOptions" :apiVersion "v1"
                     :propagationPolicy "Background"})]
    (when-not replicaset-url
      (throw (ex-info "could not find service to delete"
                      {:service service
                       :status http-404-not-found})))
    (api-request replicaset-url scheduler :request-method :delete :body kill-json)
    {:message (str "Kubernetes deleted ReplicaSet for " id)
     :result :deleted}))

(defn service-id->service
  "Look up a Waiter Service record via its service-id."
  [{:keys [watch-state] :as scheduler} service-id]
  (-> watch-state deref :service-id->service (get service-id)))

(defn get-service->instances
  "Returns a map of scheduler/Service records -> map of scheduler/ServiceInstance records."
  [scheduler]
  (pc/map-from-keys #(instances-breakdown! scheduler %)
                    (get-services scheduler)))

(defn- retrieve-service-log-bucket-url
  "Retrieves the S3 bucket url where log files should be copied when a waiter service pod is terminated."
  [{:strs [env]} log-bucket-url]
  (get env "WAITER_CONFIG_LOG_BUCKET_URL" log-bucket-url))

(defn retrieve-use-authenticated-health-checks?
  "Computes whether to use authenticated health checks for pods."
  [{:keys [authenticate-health-checks? service-id->service-description-fn]} service-id]
  (or authenticate-health-checks?
      (-> service-id
        (service-id->service-description-fn)
        (scheduler/authenticated-health-check-configured?))))

; The Waiter Scheduler protocol implementation for Kubernetes
(defrecord KubernetesScheduler [api-server-url
                                authenticate-health-checks?
                                authorizer
                                cluster-name
                                container-running-grace-secs
                                custom-options
                                daemon-state
                                determine-replicaset-namespace-fn
                                fileserver
                                http-client
                                leader?-fn
                                log-bucket-url
                                namespace
                                max-patch-retries
                                max-name-length
                                pdb-api-version
                                pdb-spec-builder-fn
                                pod-base-port
                                pod-sigkill-delay-secs
                                pod-suffix-length
                                raven-sidecar
                                replicaset-api-version
                                replicaset-spec-builder-fn
                                response->deployment-error-msg-fn
                                restart-expiry-threshold
                                restart-kill-threshold
                                retrieve-auth-token-state-fn
                                retrieve-syncer-state-fn
                                service-id->deployment-error-cache
                                service-id->failed-instances-transient-store
                                service-id->password-fn
                                service-id->service-description-fn
                                scheduler-name
                                watch-state]
  scheduler/ServiceScheduler

  (get-services [this]
    (get-services this))

  (kill-instance [this {:keys [id service-id] :as instance}]
    (ss/try+
      (let [service (service-id->service this service-id)]
        (kill-service-instance this instance service)
        (scheduler/log-service-instance instance :kill :info)
        {:instance-id id
         :killed? true
         :message "Successfully killed instance"
         :service-id service-id
         :status http-200-ok })
      (catch [:status http-404-not-found] _
        {:instance-id id
         :killed? false
         :message "Instance not found"
         :service-id service-id
         :status http-404-not-found})
      (catch Object ex
        (log/error ex "error while killing instance")
        {:instance-id id
         :killed? false
         :message "Error while killing instance"
         :service-id service-id
         :status http-500-internal-server-error})))

  (service-exists? [this service-id]
    (ss/try+
      (some? (service-id->service this service-id))
      (catch [:status http-404-not-found] _
        (comment "App does not exist."))))

  (create-service-if-new [this {:keys [service-id] :as descriptor}]
    (when-not (scheduler/service-exists? this service-id)
      (ss/try+
        (create-service descriptor this)
        (catch [:status http-409-conflict] _
          (log/error "conflict status when trying to start app. Is app starting up?"
                     descriptor))
        (catch Object ex
          (log/error ex "error starting new app." descriptor)))))

  (delete-service [this service-id]
    (ss/try+
      (let [service (service-id->service this service-id)
            delete-result (delete-service this service)]
        (swap! service-id->failed-instances-transient-store dissoc service-id)
        delete-result)
      (catch [:status http-404-not-found] _
        (log/warn "service does not exist:" service-id)
        {:result :no-such-service-exists
         :message "Kubernetes reports service does not exist"})
      (catch Object ex
        (log/warn ex "internal error while deleting service" {:service-id service-id})
        {:result :error
         :message "Internal error while deleting service"})))

  (deployment-error-config [_ _]
    ;; The min-hosts count currently MUST be 1 on Kubernetes since a failing service's
    ;; container restarts repeatedly within a single Pod, normally not switching hosts.
    {:min-hosts 1})

  (request-protocol [_ {:keys [k8s/port->protocol port] :as instance} port-index service-description]
    (if port->protocol
      (let [result-proto (get port->protocol (port->key (+ port port-index)))]
        (when-not result-proto
          (log/error "protocol not found for instance"
                     {:instance instance
                      :port-index port-index
                      :service-description service-description}))
        result-proto)
      (scheduler/retrieve-protocol port-index service-description)))

  (use-authenticated-health-checks? [this service-id]
    (retrieve-use-authenticated-health-checks? this service-id))

  (scale-service [this service-id scale-to-instances _]
    (ss/try+
      (if-let [service (service-id->service this service-id)]
        (do
          (scale-service-up-to this service scale-to-instances)
          {:success true
           :status http-200-ok
           :result :scaled
           :message (str "Scaled to " scale-to-instances)})
        (do
          (log/error "cannot scale missing service" service-id)
          {:success false
           :status http-404-not-found
           :result :no-such-service-exists
           :message "Failed to scale missing service"}))
      (catch [:status http-409-conflict] _
        {:success false
         :status http-409-conflict
         :result :conflict
         :message "Scaling failed due to repeated patch conflicts"})
      (catch Object ex
        (log/error ex "error while scaling waiter service" service-id)
        {:success false
         :status http-500-internal-server-error
         :result :failed
         :message "Error while scaling waiter service"})))

  (retrieve-directory-content
    [{{:keys [port predicate-fn scheme]} :fileserver :as scheduler}
     service-id instance-id host browse-path]
    (let [{:keys [_ pod-name restart-number]} (unpack-instance-id instance-id)
          instance-base-dir (str "r" restart-number)
          browse-path (if (str/blank? browse-path) "/" browse-path)
          browse-path (cond->
                        browse-path
                        (not (str/ends-with? browse-path "/"))
                        (str "/")
                        (not (str/starts-with? browse-path "/"))
                        (->> (str "/")))
          {:strs [run-as-user] :as service-description} (retrieve-service-description scheduler service-id)
          base-bucket-url (retrieve-service-log-bucket-url service-description log-bucket-url)
          pod (get-in @watch-state [:service-id->pod-id->pod service-id pod-name])]
      (ss/try+
        (cond
          ;; fileserver is disabled on pod: return no logs information
          (not (predicate-fn scheduler service-id service-description nil))
          nil

          ;; the pod is live: try accessing logs through sidecar
          (pod-logs-live? pod)
          (let [target-url (str scheme "://" host ":" port "/" instance-base-dir browse-path)
                result (hu/http-request
                         http-client
                         target-url
                         :accept "application/json")]
            (for [{entry-name :name entry-type :type :as entry} result]
              (if (= "file" entry-type)
                (assoc entry :url (str target-url entry-name))
                (assoc entry :path (str browse-path entry-name)))))

          ;; the pod is not live: try accessing logs through S3
          :else
          (when base-bucket-url
            (let [prefix (str run-as-user "/" service-id "/" pod-name "/" instance-base-dir browse-path)
                  query-string (str "delimiter=/&prefix=" prefix)
                  result (hu/http-request
                           http-client
                           base-bucket-url
                           :query-string query-string
                           ;; Enabling Kerberos/SPNEGO when the bucket is not kerberized does not
                           ;; cause an error, and the extra flag is ignored on non-kerberized systems.
                           :spnego-auth true)
                  xml-listing (-> result .getBytes io/input-stream xml/parse zip/xml-zip)]
              (vec
                (concat
                  (for [f (zx/xml-> xml-listing :Contents)]
                    (let [path (zx/xml1-> f :Key zx/text)
                          size (Long/parseLong (zx/xml1-> f :Size zx/text))
                          basename (subs path (count prefix))]
                      {:name basename :size size :type "file" :url (str base-bucket-url "/" path)}))
                  (distinct
                    (for [d (zx/xml-> xml-listing :CommonPrefixes)]
                      (let [path (zx/xml1-> d :Prefix zx/text)
                            subdir-path (subs path (count prefix))
                            dirname-length (str/index-of subdir-path "/")
                            dirname (subs subdir-path 0 dirname-length)]
                        {:name dirname :path (str "/" prefix dirname) :type "directory"}))))))))
        (catch [:client http-client] response
          (log/error "request to fileserver failed: " response))
        (catch Object ex
          (log/error ex "request to fileserver failed")))))

  (service-id->state [_ service-id]
    {:failed-instances (get @service-id->failed-instances-transient-store service-id)
     :syncer (retrieve-syncer-state-fn service-id)})

  (state [_ include-flags]
    (cond-> {:supported-include-params ["auth-token-renewer" "authorizer" "service-id->failed-instances"
                                        "syncer" "syncer-details" "watch-state" "watch-state-details"]
             :type "KubernetesScheduler"}
      (contains? include-flags "auth-token-renewer")
      (assoc :auth-token-renewer (retrieve-auth-token-state-fn))
      (and authorizer (contains? include-flags "authorizer"))
      (assoc :authorizer (authz/state authorizer))
      (contains? include-flags "service-id->failed-instances")
      (assoc :service-id->failed-instances @service-id->failed-instances-transient-store)
      (or (contains? include-flags "syncer")
          (contains? include-flags "syncer-details"))
      (assoc :syncer (cond-> (retrieve-syncer-state-fn)
                       (not (contains? include-flags "syncer-details"))
                       (dissoc :service-id->health-check-context)))
      (or (contains? include-flags "watch-state")
          (contains? include-flags "watch-state-details"))
      (assoc :watch-state (if (contains? include-flags "watch-state-details")
                            (update @watch-state :service-id->pod-id->pod
                                    (fn [service-id->pod-id->pod]
                                      (pc/map-vals (fn [pod-id->pod]
                                                     (pc/map-vals #(dissoc % :spec) pod-id->pod))
                                                   service-id->pod-id->pod)))
                            (dissoc @watch-state :service-id->pod-id->pod :service-id->service)))))

  (validate-service [this service-id]
    (let [{:strs [run-as-user]} (retrieve-service-description this service-id)]
      (authz/check-user authorizer run-as-user service-id)))

  (compute-instance-usage [this service-id]
    (let [{:strs [cpus mem]} (service-id->service-description-fn service-id)]
      (try
        (let [{:keys [k8s/container-resources]} (service-id->service this service-id)]
          (if (seq container-resources)
            (->> container-resources
              (map #(select-keys % [:cpus :mem]))
              (apply merge-with +)
              (merge {:k8s/num-containers (count container-resources)}))
            {:cpus cpus :mem mem}))
        (catch Exception ex
          (log/error ex "error in computing resource usage" {:service-id service-id})
          {:cpus cpus :mem mem})))))

(defn compute-image
  "Compute the image to use for the service"
  [image default-container-image image-aliases]
  (let [unresolved-image (or image default-container-image)]
    (get image-aliases unresolved-image unresolved-image)))

(defn prepare-health-check-probe
  "Returns the configuration for a basic health check probe."
  [service-id->password-fn service-id authenticate-health-check?
   health-check-scheme health-check-url health-check-port health-check-interval-secs]
  {:httpGet (cond-> {:path health-check-url
                     :port health-check-port
                     :scheme health-check-scheme}
              authenticate-health-check?
              (assoc :httpHeaders
                     (->> (scheduler/retrieve-auth-headers service-id->password-fn service-id)
                       (map (fn [[k v]] {:name k :value v})))))
   :periodSeconds health-check-interval-secs
   :timeoutSeconds 1})

(def ^:const default-raven-env-flag "RAVEN_ENABLED")
(def ^:const default-raven-tls-env-flag "RAVEN_FORCE_INGRESS_TLS")

(defn has-raven-config-in-env?
  "Check if the env has any explicit raven sidecar config."
  [env {:keys [env-vars]}]
  (some #(contains? env %) (mapcat val env-vars)))

(defn raven-env-config-helper
  "Check environment flags to see if raven sidecar should be enabled.
   Used to build other predicate functions by providing defaults.
   Returns the first env var name and value matched
   as {:name var-name :match match-fn-result},
   or default-result if none of the configured env vars are found."
  [env {{:keys [flags features tls-flags]} :env-vars} default-result]
  (merge
    default-result
    ;; sidecar on/off
    (or (first (for [flag flags
                     :let [flag-value (get env flag)]
                     :when (some? flag-value)]
                 {:name flag :match (utils/match-yes-like flag-value)}))
        (first (for [feature features
                     :let [feature-value (get env feature)]
                     :when (some? feature-value)]
                 {:name feature :match feature-value})))
    ;; strict-tls on/off
    (first (for [flag tls-flags
                 :let [flag-value (get env flag)]
                 :when (some? flag-value)]
             {:tls-name flag :tls-match (utils/match-yes-like flag-value)}))))

(defn raven-mode-helper
  "Returns one of the following based on env flags set:
   - nil when Raven is disabled
   - :strict-tls when Raven is enabled with tls enforced on the downstream connection
     (i.e., forced tls ingress on the pod via the Raven sidecar)
   - :enabled when Raven is enabled, but configured to use tls only when the upstream service is also using tls"
  [env raven-sidecar-config default-env-match]
  (let [{:keys [match tls-match]} (raven-env-config-helper env raven-sidecar-config default-env-match)]
    (when match
      (if tls-match :strict-tls :enabled))))

(defn raven-sidecar-opt-in?
  "Returns nil to disable the raven sidecar unless explicitly enabled."
  [{raven-sidecar-config :raven-sidecar} _ {:strs [env]} _]
  (raven-mode-helper env raven-sidecar-config nil))

(defn raven-sidecar-opt-out?
  "Returns :enabled to enable the raven sidecar unless explicitly disabled."
  [{raven-sidecar-config :raven-sidecar} _ {:strs [env]} _]
  (raven-mode-helper env raven-sidecar-config {:match "yes"}))

(defn raven-sidecar-strict-tls-opt-out?
  "Always returns :strict-tls to enable the raven sidecar with TLS, unless explicitly disabled."
  [{raven-sidecar-config :raven-sidecar} _ {:strs [env]} _]
  (raven-mode-helper env raven-sidecar-config {:match "yes" :tls-match "yes"}))

(defn attach-raven-sidecar
  "Attaches raven sidecar to replicaset"
  [replicaset raven-sidecar
   {:strs [backend-proto health-check-port-index health-check-proto] :as service-description}
   base-env service-port port0 force-tls?]
  (update-in replicaset
             [:spec :template :spec :containers]
             conj
             (let [{:keys [cmd resources image]} raven-sidecar
                   raven-base-env (get-in raven-sidecar [:env-vars :defaults])
                   user-env (:env service-description)
                   env-map (-> raven-base-env
                               (merge user-env base-env)
                               (assoc "FORCE_TLS_TERMINATION" (str force-tls?)
                                      "HEALTH_CHECK_PORT_INDEX" (str health-check-port-index)
                                      "HEALTH_CHECK_PROTOCOL" health-check-proto
                                      "PORT0" (str port0)
                                      "SERVICE_PORT" (str service-port)
                                      "SERVICE_PROTOCOL" backend-proto))
                   env (vec (for [[k v] env-map]
                              {:name k :value v}))
                   raven-container {:command cmd
                                    :env env
                                    :image image
                                    :imagePullPolicy "IfNotPresent"
                                    :name waiter-raven-sidecar-name
                                    :ports [{:containerPort service-port}]
                                    :resources {:limits {:memory (str (:mem resources) "Mi")}
                                                :requests {:cpu (str (:cpu resources)) :memory (str (:mem resources) "Mi")}}}]
               raven-container)))

(defn- proto->tls-proto
  "Return equivalent TLS protocol for given protocol"
  [proto]
  (case proto
    "http" "https"
    "h2c" "h2"
    proto))

(def ^:const service-ports-index 0)
(def ^:const proxied-ports-index 1)

(defn get-port-range
  "0th port in a range of up to 10 for a given service-id's hash.
   Note that each container is limited to a contiguous 100 ranges of 10 ports,
   therefore the return values are only unique for range-index [0, 99]."
  [service-id-hash range-index base-port]
  (-> service-id-hash (+ range-index) (mod 100) (* 10) (+ base-port)))

(defn determine-namespace
  "Determines the k8s namespace to use while creating k8s objects for the service."
  [scheduler-namespace default-namespace override-namespace {:strs [namespace run-as-user]}]
  (when-not (or namespace default-namespace override-namespace scheduler-namespace)
    (throw (ex-info "Waiter configuration is missing a default namespace for Kubernetes pods" {})))
  (when (and scheduler-namespace namespace (not= scheduler-namespace namespace))
    (throw (ex-info "service namespace does not match scheduler namespace"
                    {:scheduler-ns scheduler-namespace :service-ns namespace})))
  (or override-namespace
      namespace
      (if (= "*" default-namespace) run-as-user default-namespace)
      scheduler-namespace))

(defn determine-replicaset-namespace
  "Default implementation that determines the namespace to use for a replicaset"
  [{scheduler-namespace :namespace} _ service-description {:keys [default-namespace override-namespace]}]
  (determine-namespace scheduler-namespace default-namespace override-namespace service-description))

(defn retrieve-unique-service-mapping-token
  "Returns thr unique token being used to access a service.
   Returns nil if multiple (partial) tokens are being used to create the service."
  [{:strs [env]}]
  (when-let [waiter-config-token (get env "WAITER_CONFIG_TOKEN")]
    (when (nil? (str/index-of waiter-config-token ","))
      waiter-config-token)))

(defn default-replicaset-builder
  "Factory function which creates a Kubernetes ReplicaSet spec for the given Waiter Service."
  [{:keys [cluster-name determine-replicaset-namespace-fn fileserver pod-base-port pod-sigkill-delay-secs
           replicaset-api-version raven-sidecar service-id->password-fn] :as scheduler}
   service-id
   {:strs [backend-proto cmd cpus grace-period-secs health-check-interval-secs
           health-check-max-consecutive-failures health-check-port-index health-check-proto image
           mem min-instances ports run-as-user termination-grace-period-secs]
    :as service-description}
   {:keys [container-init-commands default-container-image log-bucket-url image-aliases run-as-user-source]
    :as context}]
  (when-not (or image default-container-image)
    (throw (ex-info "Waiter configuration is missing a default image for Kubernetes pods" {})))
  (when (nil? run-as-user-source)
    (throw (ex-info "ReplicaSet spec builder context is missing run-as-user-source" {:context context})))
  (let [rs-namespace (determine-replicaset-namespace-fn scheduler service-id service-description context)
        work-path (str "/home/" run-as-user)
        home-path (str work-path "/latest")
        base-env (scheduler/environment service-id service-description
                                        service-id->password-fn home-path)
        ;; We include the default log-bucket-sync-secs value in the total-sigkill-delay-secs
        ;; delay iff the S3 bucket-url setting is configured for the service.
        base-bucket-url (retrieve-service-log-bucket-url service-description log-bucket-url)
        log-bucket-sync-secs (if base-bucket-url (:log-bucket-sync-secs context) 0)
        configured-pod-sigkill-delay-secs (max termination-grace-period-secs pod-sigkill-delay-secs)
        total-sigkill-delay-secs (+ configured-pod-sigkill-delay-secs log-bucket-sync-secs)
        ;; Make $PORT0 value pseudo-random to ensure clients can't hardcode it.
        ;; Helps maintain compatibility with Marathon, where port assignment is dynamic.
        service-id-hash (hash service-id)
        service-port (get-port-range service-id-hash service-ports-index pod-base-port)
        raven-mode (when-let [raven-sidecar-check-fn (:predicate-fn raven-sidecar)]
                     (raven-sidecar-check-fn scheduler service-id service-description context))
        has-raven? (boolean raven-mode)
        raven-force-downstream-tls? (= :strict-tls raven-mode)
        raven-label (cond
                      (not has-raven?) "disabled"
                      (keyword? raven-mode) (name raven-mode)
                      :else "enabled")
        port0 (if has-raven?
                (get-port-range service-id-hash proxied-ports-index pod-base-port)
                service-port)
        health-check-port (when (pos? health-check-port-index)
                            (+ service-port health-check-port-index))
        ;; Determine actual protocols on the service port and health-check port.
        ;; Note that these may differ from the protocols in the service-description
        ;; if the Raven sidecar is using TLS where the backend process uses clear text.
        actual-backend-proto (cond-> backend-proto raven-force-downstream-tls? proto->tls-proto)
        actual-health-check-proto (if-not health-check-port
                                    actual-backend-proto
                                    (cond-> (or health-check-proto backend-proto)
                                      raven-force-downstream-tls? proto->tls-proto))
        ;; NOTE: the work-stealing handler passes instance objects around as JSON,
        ;; and since the deserializer there assumes that all map keys are keywords,
        ;; these port->protocol map keys must also be keywords.
        port->protocol (cond-> {(port->key service-port) actual-backend-proto}
                         health-check-port
                         (assoc (port->key health-check-port) actual-health-check-proto))
        env (into (cond-> [;; We set these two "MESOS_*" variables to improve interoperability.
                           ;; New clients should prefer using WAITER_SANDBOX.
                           {:name "MESOS_DIRECTORY" :value home-path}
                           {:name "MESOS_SANDBOX" :value home-path}
                           {:name "WAITER_SANDBOX" :value home-path}
                           ;; Number of seconds to wait after receiving a sigterm
                           ;; before sending a sigkill to the user's process.
                           ;; This is handled by the waiter-k8s-init script,
                           ;; separately from the pod's grace period,
                           ;; in order to provide extra time for logs to sync to an s3 bucket.
                           {:name "WAITER_GRACE_SECS" :value (str configured-pod-sigkill-delay-secs)}]
                    has-raven? (conj {:name "WAITER_RAVEN_PORT" :value (str service-port)}))
                  (concat
                    (for [[k v] base-env]
                      {:name k :value v})
                    [{:name "PORT" :value (str port0)}]
                    (for [i (range ports)]
                      {:name (str "PORT" i) :value (str (+ port0 i))})))
        k8s-name (service-id->k8s-app-name scheduler service-id)
        revision-timestamp (du/date-to-str (t/now)) ;; we use a monotonically increasing version string
        revision-version "0"
        authenticate-health-check? (retrieve-use-authenticated-health-checks? scheduler service-id)
        liveness-scheme (-> (or health-check-proto backend-proto) hu/backend-proto->scheme str/upper-case)
        readiness-scheme (-> (or actual-health-check-proto backend-proto) hu/backend-proto->scheme str/upper-case)
        health-check-url (sd/service-description->health-check-url service-description)
        memory (str mem "Mi")
        service-hash (service-id->service-hash service-id)
        fileserver-predicate-fn (-> fileserver :predicate-fn)
        fileserver-enabled? (fileserver-predicate-fn scheduler service-id service-description context)
        fileserver-label (if fileserver-enabled? "enabled" "disabled")
        waiter-config-token (retrieve-unique-service-mapping-token service-description)]
    (cond->
      {:kind "ReplicaSet"
       :apiVersion replicaset-api-version
       :metadata {:annotations (cond-> {:waiter/revision-timestamp revision-timestamp
                                        :waiter/revision-version revision-version
                                        :waiter/run-as-user-source run-as-user-source
                                        ;; Since there are length restrictions on Kubernetes label values,
                                        ;; we store just the 32-char hash portion of the service-id as a searchable label,
                                        ;; but store the full service-id as an annotation.
                                        ;; https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#syntax-and-character-set
                                        ;; https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
                                        :waiter/service-id service-id}
                                 waiter-config-token (assoc :waiter/token waiter-config-token))
                  :labels {:app k8s-name
                           :waiter/cluster cluster-name
                           :waiter/fileserver fileserver-label
                           :waiter/raven raven-label
                           :waiter/service-hash service-hash
                           :waiter/user run-as-user}
                  :name k8s-name
                  :namespace rs-namespace}
       :spec {:replicas min-instances
              :selector {:matchLabels {:app k8s-name
                                       :waiter/user run-as-user}}
              :template {:metadata {:annotations (cond-> {:waiter/port-count (str ports)
                                                          :waiter/port-onto-protocol (utils/clj->json port->protocol)
                                                          :waiter/revision-timestamp revision-timestamp
                                                          :waiter/revision-version revision-version
                                                          :waiter/run-as-user-source run-as-user-source
                                                          :waiter/service-id service-id
                                                          :waiter/service-port (str service-port)}
                                                   waiter-config-token (assoc :waiter/token waiter-config-token))
                                    :labels {:app k8s-name
                                             :waiter/cluster cluster-name
                                             :waiter/fileserver fileserver-label
                                             :waiter/raven raven-label
                                             :waiter/service-hash service-hash
                                             :waiter/user run-as-user}}
                         :spec {;; Service account tokens allow easy access to the k8s api server,
                                ;; but this is only enabled when the replicaset namespace matches the run-as-user.
                                :automountServiceAccountToken (= rs-namespace run-as-user)
                                ;; Note: waiter-app must always be the first container we register
                                :containers [{:command (conj (vec container-init-commands) cmd)
                                              :env env
                                              :image (compute-image image default-container-image image-aliases)
                                              :imagePullPolicy "IfNotPresent"
                                              :name waiter-primary-container-name
                                              :ports [{:containerPort port0}]
                                              :readinessProbe (-> (prepare-health-check-probe
                                                                    service-id->password-fn service-id
                                                                    authenticate-health-check?
                                                                    readiness-scheme health-check-url
                                                                    (+ service-port health-check-port-index)
                                                                    health-check-interval-secs)
                                                                (assoc :failureThreshold 1))
                                              :resources {:limits {:memory memory}
                                                          :requests {:cpu cpus
                                                                     :memory memory}}
                                              :volumeMounts [{:mountPath work-path
                                                              :name "user-home"}]
                                              :workingDir work-path}]
                                :volumes [{:name "user-home"
                                           :emptyDir {}}]
                                :terminationGracePeriodSeconds total-sigkill-delay-secs}}}}
      ;; enable liveness only if positive grace-period-secs is specified
      (pos? grace-period-secs)
      (update-in
        [:spec :template :spec :containers 0]
        assoc :livenessProbe (-> (prepare-health-check-probe
                                   service-id->password-fn service-id
                                   authenticate-health-check?
                                   liveness-scheme health-check-url
                                   (+ port0 health-check-port-index)
                                   health-check-interval-secs)
                               (assoc
                                 ;; We increment the threshold value to match Marathon behavior.
                                 ;; Marathon treats this as a retry count,
                                 ;; whereas Kubernetes treats it as a run count.
                                 :failureThreshold (inc health-check-max-consecutive-failures)
                                 :initialDelaySeconds grace-period-secs)))
      ;; Optional fileserver sidecar container
      ;; fileserver port must be provided and the container must be enabled on the service
      fileserver-enabled?
      (update-in
        [:spec :template :spec :containers]
        conj
        (let [{:keys [cmd image port] {:keys [cpu mem]} :resources} fileserver
              memory (str mem "Mi")
              base-bucket-url (retrieve-service-log-bucket-url service-description log-bucket-url)]
          {:command cmd
           :env (into [{:name "WAITER_FILESERVER_PORT" :value (str port)}
                       {:name "WAITER_GRACE_SECS" :value (str configured-pod-sigkill-delay-secs)}]
                      (concat
                        (for [[k v] base-env
                              :when (str/starts-with? k "WAITER_")]
                          {:name k :value v})
                        (when base-bucket-url
                          [{:name "WAITER_LOG_BUCKET_URL"
                            :value (str base-bucket-url "/" run-as-user "/" service-id)}])))
           :image image
           :imagePullPolicy "IfNotPresent"
           :name waiter-fileserver-sidecar-name
           :ports [{:containerPort port}]
           :resources {:limits {:memory memory}
                       :requests {:cpu cpu :memory memory}}
           :volumeMounts [{:mountPath "/srv/www"
                           :name "user-home"}]}))

      ;; Optional raven sidecar container
      has-raven?
      (attach-raven-sidecar raven-sidecar service-description base-env service-port port0 raven-force-downstream-tls?))))

(defn default-pdb-spec-builder
  "Factory function which creates a Kubernetes PodDisruptionBudget spec for the given ReplicaSet."
  [pdb-api-version rs-spec replicaset-uid]
  (let [rs-api-version (get rs-spec :apiVersion)
        rs-kind (get rs-spec :kind)
        rs-labels (get-in rs-spec [:metadata :labels])
        rs-name (get-in rs-spec [:metadata :name])
        rs-replicas (get-in rs-spec [:spec :replicas])
        rs-selector (get-in rs-spec [:spec :selector])
        service-id (k8s-object->service-id rs-spec)
        min-available (quot (inc rs-replicas) 2)
        pdb-hash (-> replicaset-uid (hash) (mod 9000) (+ 1000))]
    (log/info "creating pod disruption budget"
              {:min-available min-available
               :replicaset-name rs-name
               :service-id service-id})
    {:apiVersion pdb-api-version
     :kind "PodDisruptionBudget"
     :metadata {:annotations {:waiter/service-id service-id}
                :labels rs-labels
                :name (str rs-name "-pdb-" pdb-hash)
                :ownerReferences [{:apiVersion rs-api-version
                                   :blockOwnerDeletion true
                                   :controller false
                                   :kind rs-kind
                                   :name rs-name
                                   :uid replicaset-uid}]}
     :spec {:minAvailable min-available
            :selector rs-selector}}))

(defn start-auth-renewer
  "Initialize the k8s-api-auth-str atom,
   and optionally start a chime to periodically refresh the value."
  [{:keys [action-fn refresh-delay-mins] :as context}]
  {:pre [(or (nil? refresh-delay-mins)
             (pos-int? refresh-delay-mins))
         (symbol? action-fn)]}
  (let [refresh! (-> action-fn utils/resolve-symbol deref)
        auth-renewer-state-atom (atom nil)
        auth-update-task (fn auth-update-task []
                           (let [{:keys [auth-token] :as auth-state} (refresh! context)]
                             (if auth-token
                               (do
                                 (reset! auth-renewer-state-atom
                                         (-> auth-state
                                           (update :auth-token utils/truncate 20)
                                           (assoc :k8s/retrieve-time (t/now))))
                                 (reset! k8s-api-auth-str auth-token))
                               (log/info "auth token renewal failed:" auth-state))))]
    (assert (fn? refresh!) "Refresh function must be a Clojure fn")
    (auth-update-task)
    {:cancel-fn (if refresh-delay-mins
                  (du/start-timer-task
                    (t/minutes refresh-delay-mins)
                    auth-update-task
                    :delay-ms (* 60000 refresh-delay-mins))
                  (constantly nil))
     :query-state-fn (fn query-auth-token-state []
                       @auth-renewer-state-atom)}))


(defn- reset-watch-state!
  "Reset the global state that is used as the basis for applying incremental watch updates."
  [{:keys [watch-state] :as scheduler}
   {:keys [query-fn resource-key resource-url metadata-key] :as options}]
  (let [{:keys [version] :as initial-state} (query-fn scheduler options resource-url)]
    (swap! watch-state assoc
           resource-key (get initial-state resource-key)
           metadata-key {:timestamp {:snapshot (t/now)}
                         :version {:snapshot version}})
    version))

(defn- latest-watch-state-version
  "Get the last resourceVersion seen and stored in our watch-state."
  [{:keys [watch-state]} {:keys [metadata-key]}]
  (let [version-metadata (get-in @watch-state [metadata-key :version])]
    (or (:watch version-metadata)
        (:snapshot version-metadata))))

(def default-watch-options
  "Default options for start-k8s-watch! daemon threads."
  {:api-request-fn api-request
   :connect-timeout-ms (-> 10 t/seconds t/in-millis)
   :exit-on-error? true
   :init-timeout-ms (-> 2 t/minutes t/in-millis)
   :socket-timeout-ms (-> 15 t/minutes t/in-millis)
   :streaming-api-request-fn streaming-watch-api-request
   :watch-retries 0})

(def retry-watch-runner
  "Configured run-with-retries helper for use in our watch threads."
  (utils/retry-strategy {:delay-multiplier 1.5
                         :initial-delay-ms 100
                         :max-delay-ms 60000
                         :max-retries Long/MAX_VALUE}))

(defn- start-k8s-watch!
  "Start a thread to continuously update the watch-state atom based on watched K8s events."
  [{:keys [api-server-url scheduler-name watch-state] :as scheduler}
   {:keys [connect-timeout-ms exit-on-error? resource-name resource-url socket-timeout-ms
           streaming-api-request-fn update-fn watch-retries watch-trigger-chan] :as options}]
  (doto
    (Thread.
      (fn k8s-watch []
        (try
          (log/info "starting" resource-name "watch at" resource-url)
          ;; retry getting state updates forever
          (let [resource-name-lower (str/lower-case resource-name)]
            (while true
              (retry-watch-runner
                (fn k8s-watch-retried-thunk []
                  (try
                    (log/info "prepping" resource-name "watch with global sync")
                    (loop [version (reset-watch-state! scheduler options)
                           iter 0]
                      (log/info "starting" resource-name "watch connection, iteration" iter)
                      (timers/start-stop-time!
                        (metrics/waiter-timer "scheduler" scheduler-name "state-watch")
                        (timers/start-stop-time!
                          (metrics/waiter-timer "scheduler" scheduler-name "state-watch" resource-name-lower)
                          (let [request-options (cond-> {}
                                                  connect-timeout-ms (assoc :conn-timeout connect-timeout-ms)
                                                  socket-timeout-ms (assoc :socket-timeout socket-timeout-ms))
                                watch-url (str resource-url "&watch=true&resourceVersion=" version)]
                            ;; process updates forever (unless there's an exception)
                            (doseq [json-object (streaming-api-request-fn resource-name watch-url request-options)]
                              (when json-object
                                (if (= "ERROR" (:type json-object))
                                  (log/info "received error update in watch" resource-name json-object)
                                  (do
                                    (counters/inc! (metrics/waiter-counter "scheduler" scheduler-name "state-watch" resource-name-lower "update-count"))
                                    (meters/mark! (metrics/waiter-meter "scheduler" scheduler-name "state-watch" resource-name-lower "update-rate"))
                                    (update-fn json-object)
                                    (async/>!! watch-trigger-chan {:object json-object :resource-name resource-name :time (t/now)}))))))))
                      ;; when the watch connection closed normally (i.e., no HTTP error code response),
                      ;; retry the watch (before falling back to the global query again) `watch-retries` times.
                      (when (< iter watch-retries)
                        (when-let [version' (latest-watch-state-version scheduler options)]
                          (recur version' (inc iter)))))
                    (catch Exception e
                      (log/warn "error in" resource-name "state watch thread:" e)
                      (throw e)))))))
          (catch Throwable t
            (when exit-on-error?
              (log/error t "unrecoverable error in" resource-name "state watch thread, terminating waiter.")
              (System/exit 1))))))
    (.setDaemon true)
    (.start)))

(defn- global-state-query
  [scheduler {:keys [api-request-fn]} objects-url]
  (let [{:keys [items] :as response} (api-request-fn objects-url scheduler)
        resource-version (k8s-object->resource-version response)]
    {:items items
     :version resource-version}))

(defn global-pods-state-query
  "Query K8s for all Waiter-managed Pods"
  [scheduler options pods-url]
  (let [{:keys [items version]} (global-state-query scheduler options pods-url)
        service-id->pod-id->pod (->> items
                                  (group-by k8s-object->service-id)
                                  (pc/map-vals (partial pc/map-from-vals k8s-object->id)))]
    {:service-id->pod-id->pod service-id->pod-id->pod
     :version version}))

(defn start-pods-watch!
  "Start a thread to continuously update the watch-state atom based on watched Pod events."
  [{:keys [api-server-url cluster-name namespace watch-state] :as scheduler} options]
  (start-k8s-watch!
    scheduler
    (->
      {:query-fn global-pods-state-query
       :resource-key :service-id->pod-id->pod
       :resource-name "Pods"
       :resource-url (str api-server-url "/api/v1"
                          (when namespace (str "/namespaces/" namespace))
                          "/pods?labelSelector=waiter%2Fcluster=" cluster-name)
       :metadata-key :pods-metadata
       :update-fn (fn pods-watch-update [{pod :object update-type :type}]
                    (let [now (t/now)
                          pod-id (k8s-object->id pod)
                          service-id (k8s-object->service-id pod)
                          version (k8s-object->resource-version pod)
                          old-state @watch-state]
                      (swap! watch-state
                             #(as-> % state
                                (case update-type
                                  "ADDED" (assoc-in state [:service-id->pod-id->pod service-id pod-id] pod)
                                  "MODIFIED" (assoc-in state [:service-id->pod-id->pod service-id pod-id] pod)
                                  "DELETED" (utils/dissoc-in state [:service-id->pod-id->pod service-id pod-id]))
                                (assoc-in state [:pods-metadata :timestamp :watch] now)
                                (assoc-in state [:pods-metadata :version :watch] version)))
                      (let [old-pod (get-in old-state [:service-id->pod-id->pod service-id pod-id])
                            [pod-fields pod-fields'] (data/diff old-pod pod)
                            pod-ns (k8s-object->namespace pod)
                            pod-handle (str pod-ns "/" pod-id)]
                        (scheduler/log "pod state update:" update-type pod-handle version pod-fields "->" pod-fields'))))}
      (merge options))))

(defn global-rs-state-query
  "Query K8s for all Waiter-managed ReplicaSets"
  [scheduler options rs-url]
  (let [{:keys [items version]} (global-state-query scheduler options rs-url)
        service-id->service (->> items
                              (map replicaset->Service)
                              (filter some?)
                              (pc/map-from-vals :id))]
    {:service-id->service service-id->service
     :version version}))

(defn start-replicasets-watch!
  "Start a thread to continuously update the watch-state atom based on watched ReplicaSet events."
  [{:keys [api-server-url cluster-name namespace replicaset-api-version watch-state] :as scheduler} options]
  (start-k8s-watch!
    scheduler
    (->
      {:query-fn global-rs-state-query
       :resource-key :service-id->service
       :resource-name "ReplicaSets"
       :resource-url (str api-server-url "/apis/" replicaset-api-version
                          (when namespace (str "/namespaces/" namespace))
                          "/replicasets?labelSelector=waiter%2Fcluster="
                          cluster-name)
       :metadata-key :rs-metadata
       :update-fn (fn rs-watch-update [{rs :object update-type :type}]
                    (let [now (t/now)
                          {service-id :id :as service} (replicaset->Service rs)
                          version (k8s-object->resource-version rs)]
                      (when service
                        (scheduler/log "rs state update:" update-type version service)
                        (swap! watch-state
                               #(as-> % state
                                  (case update-type
                                    "ADDED" (assoc-in state [:service-id->service service-id] service)
                                    "MODIFIED" (assoc-in state [:service-id->service service-id] service)
                                    "DELETED" (utils/dissoc-in state [:service-id->service service-id]))
                                  (assoc-in state [:rs-metadata :timestamp :watch] now)
                                  (assoc-in state [:rs-metadata :version :watch] version))))))}
      (merge options))))

(defn wait-for-watches
  "Waits until both the Pod and ReplicaSet scheduler watch states are populated.
   Logs and error and returns if this takes longer than watch-init-timeout-ms.
   If watch-init-timeout-ms is zero, then returns immediately."
  [{:keys [watch-state]} {:keys [init-timeout-ms]}]
  (let [sleep-ms 10]
    (when (pos? init-timeout-ms)
      (loop [total-ms-slept 0]
        (cond
          ;; base case: both watches initialized
          (let [current-watch-state @watch-state]
            (and (get-in current-watch-state [:rs-metadata :timestamp :snapshot])
                 (get-in current-watch-state [:pods-metadata :timestamp :snapshot])))
          (log/info "scheduler watches ready after" total-ms-slept "milliseconds")
          ;; recursive case: still waiting for one or both watches
          (<= total-ms-slept init-timeout-ms)
          (do
            (utils/sleep sleep-ms)
            (recur (+ total-ms-slept sleep-ms)))
          ;; base case: init-timeout expired, but the watches still aren't ready
          :else
          (log/error "scheduler watches still not ready after" init-timeout-ms "milliseconds"))))))

(defn fileserver-container-enabled?
  "Returns true when the port is configured on the fileserver configuration."
  [{:keys [fileserver]} _ _ _]
  (-> fileserver :port integer?))

(defn kubernetes-scheduler
  "Returns a new KubernetesScheduler with the provided configuration. Validates the
   configuration against kubernetes-scheduler-schema and throws if it's not valid."
  [{:keys [authenticate-health-checks? authentication authorizer cluster-name container-running-grace-secs custom-options http-options
           determine-replicaset-namespace-fn leader?-fn log-bucket-sync-secs
           log-bucket-url max-patch-retries max-name-length namespace pdb-api-version pdb-spec-builder pod-base-port pod-sigkill-delay-secs
           pod-suffix-length replicaset-api-version response->deployment-error-msg-fn restart-expiry-threshold restart-kill-threshold
           raven-sidecar scheduler-name scheduler-state-chan scheduler-syncer-interval-secs service-id->service-description-fn
           service-id->password-fn start-scheduler-syncer-fn url watch-chan-throttle-interval-ms watch-connect-timeout-ms watch-init-timeout-ms watch-retries watch-socket-timeout-ms]
    {fileserver-port :port fileserver-scheme :scheme :as fileserver} :fileserver
    {:keys [default-namespace] :as replicaset-spec-builder} :replicaset-spec-builder
    {service-id->deployment-error-cache-threshold :threshold service-id->deployment-error-cache-ttl-sec :ttl} :service-id->deployment-error-cache
    :as context}]
  {:pre [(or (nil? authenticate-health-checks?) (boolean? authenticate-health-checks?))
         (schema/contains-kind-sub-map? authorizer)
         (or (zero? container-running-grace-secs) (pos-int? container-running-grace-secs))
         (or (nil? custom-options) (map? custom-options))
         (or (nil? determine-replicaset-namespace-fn) (symbol? determine-replicaset-namespace-fn))
         (or (nil? raven-sidecar) (nil? (s/check schema/valid-raven-sidecar-config raven-sidecar)))
         (or (nil? fileserver-port)
             (and (integer? fileserver-port)
                  (< 0 fileserver-port 65535)))
         (re-matches #"https?" fileserver-scheme)
         (or (-> fileserver :predicate-fn nil?)
             (-> fileserver :predicate-fn symbol?))
         (pos-int? (:socket-timeout http-options))
         (pos-int? (:conn-timeout http-options))
         (fn? leader?-fn)
         (and (number? log-bucket-sync-secs) (<= 0 log-bucket-sync-secs 300))
         (or (nil? log-bucket-url) (some? (io/as-url log-bucket-url)))
         (utils/non-neg-int? max-patch-retries)
         (pos-int? max-name-length)
         (not (str/blank? cluster-name))
         (or (nil? namespace) (not (str/blank? namespace)))
         (or (nil? namespace) (nil? default-namespace) (= namespace default-namespace))
         (or (nil? pdb-api-version) (not (str/blank? pdb-api-version)))
         (or (nil? pdb-spec-builder) (symbol? (:factory-fn pdb-spec-builder)))
         (integer? pod-base-port)
         (< 0 pod-base-port 65527) ; max port is 65535, and we need to reserve up to 10 ports
         (integer? pod-sigkill-delay-secs)
         (<= 0 pod-sigkill-delay-secs 300)
         (pos-int? pod-suffix-length)
         (not (str/blank? replicaset-api-version))
         (symbol? (:factory-fn replicaset-spec-builder))
         (symbol? response->deployment-error-msg-fn)
         (pos-int? restart-expiry-threshold)
         (or (nil? restart-kill-threshold) (pos-int? restart-kill-threshold))
         (or (nil? restart-kill-threshold) (<= restart-expiry-threshold restart-kill-threshold))
         (some? (io/as-url url))
         (not (str/blank? scheduler-name))
         (au/chan? scheduler-state-chan)
         (pos-int? scheduler-syncer-interval-secs)
         (pos-int? service-id->deployment-error-cache-threshold)
         (pos-int? service-id->deployment-error-cache-ttl-sec)
         (fn? service-id->password-fn)
         (fn? service-id->service-description-fn)
         (fn? start-scheduler-syncer-fn)
         (or (nil? watch-chan-throttle-interval-ms) (pos-int? watch-chan-throttle-interval-ms))
         (or (nil? watch-connect-timeout-ms) (integer? watch-connect-timeout-ms))
         (or (nil? watch-init-timeout-ms) (integer? watch-init-timeout-ms))
         (or (nil? watch-retries) (integer? watch-retries))
         (or (nil? watch-socket-timeout-ms) (integer? watch-socket-timeout-ms))]}
  (let [authorizer (utils/create-component authorizer)
        authenticate-health-checks? (if (some? authenticate-health-checks?) authenticate-health-checks? false)
        http-client (-> http-options
                      (utils/assoc-if-absent :client-name "waiter-k8s")
                      (utils/assoc-if-absent :user-agent "waiter-k8s")
                      hu/http-client-factory)
        service-id->deployment-error-cache (cu/cache-factory {:threshold service-id->deployment-error-cache-threshold
                                                              :ttl (-> service-id->deployment-error-cache-ttl-sec t/seconds t/in-millis)})
        service-id->failed-instances-transient-store (atom {})
        replicaset-spec-builder-ctx (assoc replicaset-spec-builder
                                      :log-bucket-sync-secs log-bucket-sync-secs
                                      :log-bucket-url log-bucket-url)
        pdb-api-version (or pdb-api-version "policy/v1beta1")
        pdb-spec-builder-factory-fn (:factory-fn pdb-spec-builder)
        pdb-spec-builder-fn (when pdb-spec-builder-factory-fn
                              (let [f (-> pdb-spec-builder-factory-fn
                                        utils/resolve-symbol
                                        deref)]
                                (assert (fn? f) "PodDisruptionBudget spec function must be a Clojure fn")
                                f))
        replicaset-spec-builder-fn (let [f (-> replicaset-spec-builder
                                             :factory-fn
                                             utils/resolve-symbol
                                             deref)]
                                     (assert (fn? f) "ReplicaSet spec function must be a Clojure fn")
                                     (fn [scheduler service-id service-description in-context]
                                       (let [context (merge replicaset-spec-builder-ctx in-context)]
                                         (f scheduler service-id service-description context))))
        response->deployment-error-msg-fn (-> response->deployment-error-msg-fn utils/resolve-symbol!)
        restart-kill-threshold (or restart-kill-threshold (+ 2 restart-expiry-threshold))
        watch-trigger-chan (au/latest-chan)
        watch-options (cond-> (assoc default-watch-options :watch-trigger-chan watch-trigger-chan)
                        (some? watch-retries) (assoc :watch-retries watch-retries)
                        watch-connect-timeout-ms (assoc :connect-timeout-ms watch-connect-timeout-ms)
                        watch-init-timeout-ms (assoc :init-timeout-ms watch-init-timeout-ms)
                        watch-socket-timeout-ms (assoc :socket-timeout-ms watch-socket-timeout-ms))
        watch-state (atom nil)
        scheduler-promise (promise) ;; resolves circular dependency
        get-service->instances-fn (fn []
                                    (if (realized? scheduler-promise)
                                      (get-service->instances @scheduler-promise)
                                      (do
                                        (log/warn "scheduler instance has not yet been initialized")
                                        {})))
        syncer-timer-chan (scheduler/scheduler-syncer-timer-chan scheduler-syncer-interval-secs)
        watch-chan-throttle-interval-ms (or watch-chan-throttle-interval-ms 1000)
        syncer-trigger-chan (au/throttle-chan watch-chan-throttle-interval-ms [syncer-timer-chan watch-trigger-chan])
        {:keys [retrieve-syncer-state-fn]} (start-scheduler-syncer-fn
                                             scheduler-name
                                             get-service->instances-fn
                                             scheduler-state-chan
                                             syncer-trigger-chan)
        fileserver (update fileserver :predicate-fn (fn [predicate-fn]
                                                      (if (nil? predicate-fn)
                                                        fileserver-container-enabled?
                                                        (utils/resolve-symbol! predicate-fn))))
        raven-sidecar (when raven-sidecar
                        (cond-> (update raven-sidecar :predicate-fn utils/resolve-symbol!)
                          (not (:env-vars raven-sidecar))
                          (assoc :env-vars {:flags [default-raven-env-flag]
                                            :tls-flags [default-raven-tls-env-flag]})))
        determine-replicaset-namespace-fn (if determine-replicaset-namespace-fn
                                            (utils/resolve-symbol! determine-replicaset-namespace-fn)
                                            determine-replicaset-namespace)]

    (let [daemon-state (atom nil)
          auth-renewer (when authentication
                         (start-auth-renewer authentication))
          retrieve-auth-token-state-fn (or (:query-state-fn auth-renewer) (constantly nil))
          scheduler-config {:api-server-url url
                            :authenticate-health-checks? authenticate-health-checks?
                            :authorizer authorizer
                            :cluster-name cluster-name
                            :container-running-grace-secs container-running-grace-secs
                            :custom-options custom-options
                            :daemon-state daemon-state
                            :determine-replicaset-namespace-fn determine-replicaset-namespace-fn
                            :fileserver fileserver
                            :http-client http-client
                            :leader?-fn leader?-fn
                            :log-bucket-url log-bucket-url
                            :max-patch-retries max-patch-retries
                            :max-name-length max-name-length
                            :namespace namespace
                            :pdb-api-version pdb-api-version
                            :pdb-spec-builder-fn pdb-spec-builder-fn
                            :pod-base-port pod-base-port
                            :pod-sigkill-delay-secs pod-sigkill-delay-secs
                            :pod-suffix-length pod-suffix-length
                            :raven-sidecar raven-sidecar
                            :replicaset-api-version replicaset-api-version
                            :replicaset-spec-builder-fn replicaset-spec-builder-fn
                            :response->deployment-error-msg-fn response->deployment-error-msg-fn
                            :restart-expiry-threshold restart-expiry-threshold
                            :restart-kill-threshold restart-kill-threshold
                            :retrieve-auth-token-state-fn retrieve-auth-token-state-fn
                            :retrieve-syncer-state-fn retrieve-syncer-state-fn
                            :scheduler-name scheduler-name
                            :service-id->deployment-error-cache service-id->deployment-error-cache
                            :service-id->failed-instances-transient-store service-id->failed-instances-transient-store
                            :service-id->password-fn service-id->password-fn
                            :service-id->service-description-fn service-id->service-description-fn
                            :watch-state watch-state}
          scheduler (map->KubernetesScheduler scheduler-config)
          _ (deliver scheduler-promise scheduler)
          pod-watch-thread (start-pods-watch! scheduler watch-options)
          rs-watch-thread (start-replicasets-watch! scheduler watch-options)]
      (reset! daemon-state {:pod-watch-daemon pod-watch-thread
                            :rs-watch-daemon rs-watch-thread})
      (assert (every? #(contains? scheduler %) (keys scheduler-config))
              "ensure all fields in scheduler-config are present in KubernetesScheduler")
      (wait-for-watches scheduler watch-options)
      scheduler)))
