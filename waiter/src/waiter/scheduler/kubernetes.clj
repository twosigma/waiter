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
            [clojure.data :as data]
            [clojure.data.zip.xml :as zx]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.xml :as xml]
            [clojure.zip :as zip]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.authorization :as authz]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.schema :as schema]
            [waiter.service-description :as sd]
            [waiter.util.async-utils :as au]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (java.io InputStreamReader)
           (org.joda.time.format DateTimeFormat)))

(defn authorization-from-environment []
  "Sample implementation of the authentication string refresh function.
   Returns a string to be used as the value for the Authorization HTTP header,
   reading the string from the WAITER_K8S_AUTH_STRING environment variable."
  (log/info "called waiter.scheduler.kubernetes/authorization-from-environment")
  (System/getenv "WAITER_K8S_AUTH_STRING"))

(def k8s-api-auth-str
  "Atom containing authentication string for the Kubernetes API server.
   This value may be periodically refreshed asynchronously."
  (atom nil))

(def k8s-timestamp-format
  "Kubernetes reports dates in ISO8061 format, sans the milliseconds component."
  (DateTimeFormat/forPattern "yyyy-MM-dd'T'HH:mm:ss'Z'"))

(defn timestamp-str->datetime
  "Parse a Kubernetes API timestamp string."
  [k8s-timestamp-str]
  (du/str-to-date-safe k8s-timestamp-str k8s-timestamp-format))

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

(defn replicaset->Service
  "Convert a Kubernetes ReplicaSet JSON response into a Waiter Service record."
  [replicaset-json]
  (try
    (pc/letk
      [[spec
        [:metadata name namespace [:annotations waiter/service-id]]
        [:status {replicas 0} {availableReplicas 0} {readyReplicas 0} {unavailableReplicas 0}]]
       replicaset-json
       requested (get spec :replicas 0)
       staged (- replicas (+ availableReplicas unavailableReplicas))]
      (scheduler/make-Service
        {:id service-id
         :instances requested
         :k8s/app-name name
         :k8s/namespace namespace
         :task-count replicas
         :task-stats {:healthy readyReplicas
                      :running (- replicas staged)
                      :staged staged
                      :unhealthy (- replicas readyReplicas staged)}}))
    (catch Throwable t
      (log/error t "error converting ReplicaSet to Waiter Service"))))

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
  ([pod]
   ;; waiter-app is the first container we register, hence first container in the reported status
   (pod->instance-id pod (get-in pod [:status :containerStatuses 0 :restartCount])))
  ([pod restart-count]
   (let [pod-name (k8s-object->id pod)
         service-id (k8s-object->service-id pod)]
     (str service-id \. pod-name \- restart-count))))

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

(defn- track-failed-instances!
  "Update this KubernetesScheduler's service-id->failed-instances-transient-store
   when a new pod failure is listed in the given pod's lastState container status.
   Note that unique instance-ids are deterministically generated each time the pod is restarted
   by passing the pod's restartCount value to the pod->instance-id function."
  [{:keys [service-id] :as live-instance} {:keys [service-id->failed-instances-transient-store] :as scheduler} pod]
  (try
    (when-let [newest-failure (get-in pod [:status :containerStatuses 0 :lastState :terminated])]
      (let [failure-flags (if (= "OOMKilled" (:reason newest-failure)) #{:memory-limit-exceeded} #{})
            newest-failure-start-time (-> newest-failure :startedAt timestamp-str->datetime)
            restart-count (get-in pod [:status :containerStatuses 0 :restartCount])
            newest-failure-id (pod->instance-id pod (dec restart-count))
            failures (-> service-id->failed-instances-transient-store deref (get service-id))]
        (when-not (contains? failures newest-failure-id)
          (let [newest-failure-instance (cond-> (assoc live-instance
                                                       :flags failure-flags
                                                       :healthy? false
                                                       :id newest-failure-id
                                                       :log-directory (log-dir-path (:k8s/user live-instance)
                                                                                    (dec restart-count))
                                                       :started-at newest-failure-start-time)
                                          ;; To match the behavior of the marathon scheduler,
                                          ;; we don't include the exit code in failed instances that were killed by k8s.
                                          (not (killed-by-k8s? newest-failure))
                                          (assoc :exit-code (:exitCode newest-failure)))]
            (swap! service-id->failed-instances-transient-store
                   update-in [service-id] assoc newest-failure-id newest-failure-instance)))))
    (catch Throwable e
      (log/error e "error converting failed pod to waiter service instance" pod)
      (comment "Returning nil on failure."))))

(defn pod->ServiceInstance
  "Convert a Kubernetes Pod JSON response into a Waiter Service Instance record."
  [restart-expiry-threshold pod]
  (try
    (let [port0 (get-in pod [:spec :containers 0 :ports 0 :containerPort])
          ;; waiter-app is the first container we register
          restart-count (get-in pod [:status :containerStatuses 0 :restartCount] 0)
          run-as-user (or (get-in pod [:metadata :labels :waiter/user])
                          ;; falling back to namespace for legacy pods missing the waiter/user label
                          (k8s-object->namespace pod))
          ;; pod phase documentation: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
          {:keys [phase] :as pod-status} (:status pod)
          container-statuses (get pod-status :containerStatuses)]
      (scheduler/make-ServiceInstance
        (cond-> {:extra-ports (->> (get-in pod [:metadata :annotations :waiter/port-count])
                                   Integer/parseInt range next (mapv #(+ port0 %)))
                 :flags (cond-> #{} (>= restart-count restart-expiry-threshold) (conj :expired))
                 :healthy? (get-in pod [:status :containerStatuses 0 :ready] false)
                 :host (get-in pod [:status :podIP])
                 :id (pod->instance-id pod)
                 :k8s/app-name (get-in pod [:metadata :labels :app])
                 :k8s/namespace (k8s-object->namespace pod)
                 :k8s/pod-name (k8s-object->id pod)
                 :k8s/restart-count restart-count
                 :k8s/user run-as-user
                 :log-directory (log-dir-path run-as-user restart-count)
                 :port port0
                 :service-id (k8s-object->service-id pod)
                 :started-at (-> pod
                               (get-in [:status :startTime])
                               (timestamp-str->datetime))}
          phase (assoc :k8s/pod-phase phase)
          (seq container-statuses) (assoc :k8s/container-statuses
                                          (map (fn [{:keys [state] :as status}]
                                                 (when (> (count state) 1)
                                                   (log/warn "received multiple states for container:" status))
                                                 (let [[state {:keys [reason]}] (first state)]
                                                   (cond-> (select-keys status [:image :name :ready])
                                                     (some? reason) (assoc :reason reason)
                                                     (some? state) (assoc :state state))))
                                               container-statuses)))))
    (catch Throwable e
      (log/error e "error converting pod to waiter service instance" pod)
      (comment "Returning nil on failure."))))

(defn- pod-logs-live? [pod]
  "Returns true if the given pod has its log fileserver running."
  (and (some? pod)
       (nil? (get-in pod [:status :containerStatuses 1 :state :terminated]))))

(defn streaming-api-request
  "Make a long-lived HTTP request to the Kubernetes API server using the configured authentication.
   If data is provided via :body, the application/json content type is added automatically.
   The response payload (if any) is returned as a lazy seq of parsed JSON objects."
  [url]
  (let [keyword-keys? true
        auth-str @k8s-api-auth-str
        request-options (cond-> {:as :stream}
                          auth-str (assoc :headers {"Authorization" auth-str}))
        {:keys [body error status] :as response} (clj-http/get url request-options)]
    (when error
      (throw error))
    (when-not (<= 200 status 299)
      (ss/throw+ response))
    (-> body
        InputStreamReader.
        (cheshire/parsed-seq keyword-keys?))))

(defn streaming-watch-api-request
  "Make a long-lived watch connection to the Kubernetes API server via the streaming-api-request function.
   If a Failure error is found in the response object stream, an exception is thrown."
  [resource-name url]
  (for [update-json (streaming-api-request url)]
    (if (and (= "ERROR" (:type update-json))
             (= "Failure" (-> update-json :object :status)))
      (throw (ex-info "k8s watch connection failed"
                      {:watch-resource resource-name
                       :watch-response update-json}))
      update-json)))

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
    (catch [:status 400] response
      (log/error "malformed K8s API request: " url options response))
    (catch [:client http-client] response
      (log/error "request to K8s API server failed: " url options body response)
      (ss/throw+ response))))

(defn- retrieve-service-description
  "Get the correspoinding service-description for a service-id."
  [{:keys [service-id->service-description-fn]} service-id]
  (service-id->service-description-fn service-id))

(defn- get-services
  "Get all Waiter Services (reified as ReplicaSets) running in this Kubernetes cluster."
  [{:keys [watch-state] :as scheduler}]
  (-> watch-state deref :service-id->service vals))

(defn- get-replicaset-pods
  "Get all Kubernetes pods associated with the given Waiter Service's corresponding ReplicaSet."
  [{:keys [watch-state] :as scheduler} {service-id :id}]
  (-> watch-state deref :service-id->pod-id->pod (get service-id) vals))

(defn- live-pod?
  "Returns true if the pod has started, but has not yet been deleted."
  [pod]
  (and (some? (get-in pod [:status :podIP]))
       (nil? (get-in pod [:metadata :deletionTimestamp]))))

(defn- get-service-instances!
  "Get all active Waiter Service Instances associated with the given Waiter Service.
   Also updates the service-id->failed-instances-transient-store as a side-effect."
  [{:keys [restart-expiry-threshold] :as scheduler} basic-service-info]
  (vec (for [pod (get-replicaset-pods scheduler basic-service-info)
             :when (live-pod? pod)]
         (let [service-instance (pod->ServiceInstance restart-expiry-threshold pod)]
           (track-failed-instances! service-instance scheduler pod)
           service-instance))))

(defn instances-breakdown!
  "Get all Waiter Service Instances associated with the given Waiter Service.
   Grouped by liveness status, i.e.: {:active-instances [...] :failed-instances [...] :killed-instances [...]}"
  [{:keys [service-id->failed-instances-transient-store] :as scheduler} {service-id :id :as basic-service-info}]
  {:active-instances (get-service-instances! scheduler basic-service-info)
   :failed-instances (-> @service-id->failed-instances-transient-store (get service-id) vals vec)})

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
                         (catch [:status 409] _#
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
  (when (not-any? string/blank? [namespace app-name])
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

(defn- kill-service-instance
  "Safely kill the Kubernetes pod corresponding to the given Waiter Service Instance.
   Returns nil on success, but throws on failure."
  [{:keys [api-server-url] :as scheduler} {:keys [id k8s/namespace k8s/pod-name service-id] :as instance} service]
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
  (let [pod-url (str api-server-url
                     "/api/v1/namespaces/"
                     namespace
                     "/pods/"
                     pod-name)
        make-kill-response (fn [killed? message status]
                             {:instance-id id :killed? killed?
                              :message message :service-id service-id :status status})]
    ; "soft" delete of the pod (i.e., simply transition the pod to "Terminating" state)
    (api-request pod-url scheduler :request-method :delete
                 :body (utils/clj->json {:kind "DeleteOptions" :apiVersion "v1" :gracePeriodSeconds 300}))
    ; scale down the replicaset to reflect removal of this instance
    (try
      (scale-service-by-delta scheduler service -1)
      (catch Throwable t
        (log/error t "Error while scaling down ReplicaSet after pod termination")))
    ; force-kill the instance (should still be terminating)
    (try
      ; "hard" delete the pod (i.e., actually kill, allowing the pod's default grace period expires)
      ; (note that the pod's default grace period is different from the 300s period set above)
      (api-request pod-url scheduler :request-method :delete)
      (catch Throwable t
        (log/error t "Error force-killing pod")))
    (comment "Success! Even if the scale-down or force-kill operation failed,
              the pod will be force-killed after the grace period is up.")))

(defn create-service
  "Reify a Waiter Service as a Kubernetes ReplicaSet."
  [{:keys [service-description service-id]}
   {:keys [api-server-url replicaset-api-version replicaset-spec-builder-fn] :as scheduler}]
  (let [{:strs [cmd-type]} service-description]
    (when (= "docker" cmd-type)
      (throw (ex-info "Unsupported command type on service"
                      {:cmd-type cmd-type
                       :service-description service-description
                       :service-id service-id}))))
  (let [rs-spec (replicaset-spec-builder-fn scheduler service-id service-description)
        request-namespace (k8s-object->namespace rs-spec)
        request-url (str api-server-url "/apis/" replicaset-api-version "/namespaces/"
                         request-namespace "/replicasets")
        response-json (api-request request-url scheduler
                                   :body (utils/clj->json rs-spec)
                                   :request-method :post)
        service (some-> response-json replicaset->Service)]
    (when-not service
      (throw (ex-info "failed to create service"
                      {:service-description service-description
                       :service-id service-id})))
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
                       :status 404})))
    (api-request replicaset-url scheduler :request-method :delete :body kill-json)
    {:message (str "Kubernetes deleted ReplicaSet for " id)
     :result :deleted}))

(defn service-id->service
  "Look up a Waiter Service record via its service-id."
  [{:keys [watch-state] :as scheduler} service-id]
  (-> watch-state deref :service-id->service (get service-id)))

(defn get-service->instances
  "Returns a map of scheduler/Service records -> map of scheduler/ServiceInstance records."
  [scheduler-config]
  (pc/map-from-keys #(instances-breakdown! scheduler-config %)
                    (get-services scheduler-config)))

; The Waiter Scheduler protocol implementation for Kubernetes
(defrecord KubernetesScheduler [api-server-url
                                authorizer
                                cluster-name
                                custom-options
                                daemon-state
                                fileserver
                                http-client
                                log-bucket-url
                                max-patch-retries
                                max-name-length
                                pod-base-port
                                pod-sigkill-delay-secs
                                pod-suffix-length
                                replicaset-api-version
                                replicaset-spec-builder-fn
                                retrieve-syncer-state-fn
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
        {:instance-id id
         :killed? true
         :message "Successfully killed instance"
         :service-id service-id
         :status 200})
      (catch [:status 404] _
        {:instance-id id
         :killed? false
         :message "Instance not found"
         :service-id service-id
         :status 404})
      (catch Object ex
        (log/error ex "error while killing instance")
        {:instance-id id
         :killed? false
         :message "Error while killing instance"
         :service-id service-id
         :status 500})))

  (service-exists? [this service-id]
    (ss/try+
      (some? (service-id->service this service-id))
      (catch [:status 404] _
        (comment "App does not exist."))))

  (create-service-if-new [this {:keys [service-id] :as descriptor}]
    (when-not (scheduler/service-exists? this service-id)
      (ss/try+
        (create-service descriptor this)
        (catch [:status 409] _
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
      (catch [:status 404] _
        (log/warn "service does not exist:" service-id)
        {:result :no-such-service-exists
         :message "Kubernetes reports service does not exist"})
      (catch Object ex
        (log/warn ex "internal error while deleting service" {:service-id service-id})
        {:result :error
         :message "Internal error while deleting service"})))

  (deployment-error-config [_ _]
    ;; The min-hosts count currently MUST be 1 on Kubernetes since a failing service's
    ;; container restarts repeadly within a single Pod, normally not switching hosts.
    {:min-hosts 1})

  (scale-service [this service-id scale-to-instances _]
    (ss/try+
      (if-let [service (service-id->service this service-id)]
        (do
          (scale-service-up-to this service scale-to-instances)
          {:success true
           :status 200
           :result :scaled
           :message (str "Scaled to " scale-to-instances)})
        (do
          (log/error "cannot scale missing service" service-id)
          {:success false
           :status 404
           :result :no-such-service-exists
           :message "Failed to scale missing service"}))
      (catch [:status 409] _
        {:success false
         :status 409
         :result :conflict
         :message "Scaling failed due to repeated patch conflicts"})
      (catch Object ex
        (log/error ex "error while scaling waiter service" service-id)
        {:success false
         :status 500
         :result :failed
         :message "Error while scaling waiter service"})))

  (retrieve-directory-content
    [{:keys [http-client log-bucket-url watch-state]
      {:keys [port scheme]} :fileserver :as scheduler}
     service-id instance-id host browse-path]
    (let [{:keys [_ pod-name restart-number]} (unpack-instance-id instance-id)
          instance-base-dir (str "r" restart-number)
          browse-path (if (string/blank? browse-path) "/" browse-path)
          browse-path (cond->
                        browse-path
                        (not (string/ends-with? browse-path "/"))
                        (str "/")
                        (not (string/starts-with? browse-path "/"))
                        (->> (str "/")))
          {:strs [run-as-user]} (retrieve-service-description scheduler service-id)
          pod (get-in @watch-state [:service-id->pod-id->pod service-id pod-name])]
      (ss/try+
        (if (pod-logs-live? pod)
          ;; the pod is live: try accessing logs through sidecar
          (when port
            (let [target-url (str scheme "://" host ":" port "/" instance-base-dir browse-path)
                  result (hu/http-request
                           http-client
                           target-url
                           :accept "application/json")]
              (for [{entry-name :name entry-type :type :as entry} result]
                (if (= "file" entry-type)
                  (assoc entry :url (str target-url entry-name))
                  (assoc entry :path (str browse-path entry-name))))))
          ;; the pod is not live: try accessing logs through S3
          (when log-bucket-url
            (let [prefix (str run-as-user "/" service-id "/" pod-name "/" instance-base-dir browse-path)
                  query-string (str "delimiter=/&prefix=" prefix)
                  result (hu/http-request
                           http-client
                           log-bucket-url
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
                      {:name basename :size size :type "file" :url (str log-bucket-url "/" path)}))
                  (distinct
                    (for [d (zx/xml-> xml-listing :CommonPrefixes)]
                      (let [path (zx/xml1-> d :Prefix zx/text)
                            subdir-path (subs path (count prefix))
                            dirname-length (clojure.string/index-of subdir-path "/")
                            dirname (subs subdir-path 0 dirname-length)]
                        {:name dirname :path (str "/" prefix dirname) :type "directory"}))))))))
        (catch [:client http-client] response
          (log/error "request to fileserver failed: " response))
        (catch Object ex
          (log/error ex "request to fileserver failed")))))

  (service-id->state [_ service-id]
    {:failed-instances (vals (get @service-id->failed-instances-transient-store service-id))
     :syncer (retrieve-syncer-state-fn service-id)})

  (state [{:keys [watch-state]}]
    {:authorizer (when authorizer (authz/state authorizer))
     :service-id->failed-instances @service-id->failed-instances-transient-store
     :syncer (retrieve-syncer-state-fn)
     :watch-state @watch-state})

  (validate-service [this service-id]
    (let [{:strs [namespace run-as-user]} (retrieve-service-description this service-id)]
      (authz/check-user authorizer run-as-user service-id))))

(defn compute-image
  "Compute the image to use for the service"
  [image default-container-image image-aliases]
  (let [unresolved-image (or image default-container-image)]
    (get image-aliases unresolved-image unresolved-image)))

(defn default-replicaset-builder
  "Factory function which creates a Kubernetes ReplicaSet spec for the given Waiter Service."
  [{:keys [cluster-name fileserver pod-base-port pod-sigkill-delay-secs
           replicaset-api-version service-id->password-fn] :as scheduler}
   service-id
   {:strs [backend-proto cmd cpus grace-period-secs health-check-interval-secs health-check-max-consecutive-failures
           health-check-port-index health-check-proto image mem min-instances namespace ports run-as-user]
    :as service-description}
   {:keys [container-init-commands default-container-image default-namespace log-bucket-url image-aliases]
    :as context}]
  (when-not (or image default-container-image)
    (throw (ex-info "Waiter configuration is missing a default image for Kubernetes pods" {})))
  (when-not (or namespace default-namespace)
    (throw (ex-info "Waiter configuration is missing a default namespace for Kubernetes pods" {})))
  (let [work-path (str "/home/" run-as-user)
        home-path (str work-path "/latest")
        base-env (scheduler/environment service-id service-description
                                        service-id->password-fn home-path)
        ;; We include the default log-bucket-sync-secs value in the total-sigkill-delay-secs
        ;; delay iff the log-bucket-url setting was given the scheduler config.
        log-bucket-sync-secs (if log-bucket-url (:log-bucket-sync-secs context) 0)
        total-sigkill-delay-secs (+ pod-sigkill-delay-secs log-bucket-sync-secs)
        ;; Make $PORT0 value pseudo-random to ensure clients can't hardcode it.
        ;; Helps maintain compatibility with Marathon, where port assignment is dynamic.
        port0 (-> service-id hash (mod 100) (* 10) (+ pod-base-port))
        health-check-port (+ port0 health-check-port-index)
        env (into [;; We set these two "MESOS_*" variables to improve interoperability.
                   ;; New clients should prefer using WAITER_SANDBOX.
                   {:name "MESOS_DIRECTORY" :value home-path}
                   {:name "MESOS_SANDBOX" :value home-path}
                   {:name "WAITER_SANDBOX" :value home-path}
                   ;; Number of seconds to wait after receiving a sigterm
                   ;; before sending a sigkill to the user's process.
                   ;; This is handled by the waiter-k8s-init script,
                   ;; separately from the pod's grace period,
                   ;; in order to provide extra time for logs to sync to an s3 bucket.
                   {:name "WAITER_GRACE_SECS" :value (str pod-sigkill-delay-secs)}]
                  (concat
                    (for [[k v] base-env]
                      {:name k :value v})
                    [{:name "PORT" :value (str port0)}]
                    (for [i (range ports)]
                      {:name (str "PORT" i) :value (str (+ port0 i))})))
        k8s-name (service-id->k8s-app-name scheduler service-id)
        health-check-scheme (-> (or health-check-proto backend-proto) hu/backend-proto->scheme string/upper-case)
        health-check-url (sd/service-description->health-check-url service-description)
        memory (str mem "Mi")
        service-hash (service-id->service-hash service-id)]
    (cond->
      {:kind "ReplicaSet"
       :apiVersion replicaset-api-version
       :metadata {;; Since there are length restrictions on Kubernetes label values,
                  ;; we store just the 32-char hash portion of the service-id as a searchable label,
                  ;; but store the full service-id as an annotation.
                  ;; https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
                  ;; https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#syntax-and-character-set
                  :annotations {:waiter/service-id service-id}
                  :labels {:app k8s-name
                           ;; TODO - remove waiter-cluster
                           ;; after waiter/cluster is exclusively in use
                           ;; (see GitHub issue #721)
                           :waiter-cluster cluster-name
                           :waiter/cluster cluster-name
                           :waiter/service-hash service-hash
                           :waiter/user run-as-user}
                  :name k8s-name
                  :namespace (or namespace default-namespace)}
       :spec {:replicas min-instances
              :selector {:matchLabels {:app k8s-name
                                       :waiter-cluster cluster-name
                                       :waiter/user run-as-user}}
              :template {:metadata {:annotations {:waiter/port-count (str ports)
                                                  :waiter/service-id service-id}
                                    :labels {:app k8s-name
                                             :waiter-cluster cluster-name
                                             :waiter/cluster cluster-name
                                             :waiter/service-hash service-hash
                                             :waiter/user run-as-user}}
                         :spec {;; Service account tokens allow easy access to the k8s api server,
                                ;; but this is only enabled when the x-waiter-namespace is set explicitly
                                ;; (i.e., don't give arbitrary users access to the default namespace's token).
                                ;; Note that even if the run-as-user matches the default namespace,
                                ;; the token is still not mounted unless the namespace was explicitly set.
                                :automountServiceAccountToken (= namespace run-as-user)
                                ;; Note: waiter-app must always be the first container we register
                                :containers [{:command (conj (vec container-init-commands) cmd)
                                              :env env
                                              :image (compute-image image default-container-image image-aliases)
                                              :imagePullPolicy "IfNotPresent"
                                              :livenessProbe {:httpGet {:path health-check-url
                                                                        :port health-check-port
                                                                        :scheme health-check-scheme}
                                                              ;; We increment the threshold value to match Marathon behavior.
                                                              ;; Marathon treats this as a retry count,
                                                              ;; whereas Kubernetes treats it as a run count.
                                                              :failureThreshold (inc health-check-max-consecutive-failures)
                                                              :initialDelaySeconds grace-period-secs
                                                              :periodSeconds health-check-interval-secs
                                                              :timeoutSeconds 1}
                                              :name "waiter-app"
                                              :ports [{:containerPort port0}]
                                              :readinessProbe {:httpGet {:path health-check-url
                                                                         :port health-check-port
                                                                         :scheme health-check-scheme}
                                                               :failureThreshold 1
                                                               :periodSeconds health-check-interval-secs
                                                               :timeoutSeconds 1}
                                              :resources {:limits {:memory memory}
                                                          :requests {:cpu cpus
                                                                     :memory memory}}
                                              :volumeMounts [{:mountPath work-path
                                                              :name "user-home"}]
                                              :workingDir work-path}]
                                :volumes [{:name "user-home"
                                           :emptyDir {}}]
                                :terminationGracePeriodSeconds total-sigkill-delay-secs}}}}
      ;; Optional fileserver sidecar container
      (integer? (:port fileserver))
      (update-in
        [:spec :template :spec :containers]
        conj
        (let [{:keys [cmd image port] {:keys [cpu mem]} :resources} fileserver
              memory (str mem "Mi")]
          {:command cmd
           :env (into [{:name "WAITER_FILESERVER_PORT" :value (str port)}
                       {:name "WAITER_GRACE_SECS" :value (str pod-sigkill-delay-secs)}]
                      (when log-bucket-url
                        [{:name "WAITER_LOG_BUCKET_URL"
                          :value (str log-bucket-url "/" run-as-user "/" service-id)}]))
           :image image
           :imagePullPolicy "IfNotPresent"
           :name "waiter-fileserver"
           :ports [{:containerPort port}]
           :resources {:limits {:memory memory}
                       :requests {:cpu cpu :memory memory}}
           :volumeMounts [{:mountPath "/srv/www"
                           :name "user-home"}]})))))

(defn start-auth-renewer
  "Initialize the k8s-api-auth-str atom,
   and optionally start a chime to periodically refresh the value."
  [{:keys [action-fn refresh-delay-mins] :as context}]
  {:pre [(or (nil? refresh-delay-mins)
             (pos-int? refresh-delay-mins))
         (symbol? action-fn)]}
  (let [refresh! (-> action-fn utils/resolve-symbol deref)
        auth-update-task (fn auth-update-task []
                           (if-let [auth-str' (refresh! context)]
                             (reset! k8s-api-auth-str auth-str')))]
    (assert (fn? refresh!) "Refresh function must be a Clojure fn")
    (auth-update-task)
    (when refresh-delay-mins
      (du/start-timer-task
        (t/minutes refresh-delay-mins)
        auth-update-task
        :delay-ms (* 60000 refresh-delay-mins)))))


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
   :exit-on-error? true
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
   {:keys [exit-on-error? resource-key resource-name resource-url
           streaming-api-request-fn update-fn watch-retries] :as options}]
  (doto
    (Thread.
      (fn k8s-watch []
        (try
          ;; retry getting state updates forever
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
                      (let [watch-url (str resource-url "&watch=true&resourceVersion=" version)]
                        ;; process updates forever (unless there's an exception)
                        (doseq [json-object (streaming-api-request-fn resource-name watch-url)]
                          (when json-object
                            (if (= "ERROR" (:type json-object))
                              (log/info "received error update in watch" resource-name json-object)
                              (update-fn json-object))))))
                    ;; when the watch connection closed normally (i.e., no HTTP error code response),
                    ;; retry the watch (before falling back to the global query again) `watch-retries` times.
                    (when (< iter watch-retries)
                      (when-let [version' (latest-watch-state-version scheduler options)]
                        (recur version' (inc iter)))))
                  (catch Exception e
                    (log/warn "error in" resource-name "state watch thread:" e)
                    (throw e))))))
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
  [{:keys [api-server-url cluster-name watch-state] :as scheduler} options]
  (start-k8s-watch!
    scheduler
    (->
      {:query-fn global-pods-state-query
       :resource-key :service-id->pod-id->pod
       :resource-name "Pods"
       :resource-url (str api-server-url "/api/v1/pods?labelSelector=waiter-cluster=" cluster-name)
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
  [{:keys [api-server-url cluster-name replicaset-api-version watch-state] :as scheduler} options]
  (start-k8s-watch!
    scheduler
    (->
      {:query-fn global-rs-state-query
       :resource-key :service-id->service
       :resource-name "ReplicaSets"
       :resource-url (str api-server-url "/apis/" replicaset-api-version
                          "/replicasets?labelSelector=waiter-cluster="
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

(defn kubernetes-scheduler
  "Returns a new KubernetesScheduler with the provided configuration. Validates the
   configuration against kubernetes-scheduler-schema and throws if it's not valid."
  [{:keys [authentication authorizer cluster-name custom-options http-options log-bucket-sync-secs
           log-bucket-url max-patch-retries max-name-length pod-base-port pod-sigkill-delay-secs
           pod-suffix-length replicaset-api-version replicaset-spec-builder restart-expiry-threshold scheduler-name
           scheduler-state-chan scheduler-syncer-interval-secs service-id->service-description-fn
           service-id->password-fn start-scheduler-syncer-fn url watch-retries]
    {fileserver-port :port fileserver-scheme :scheme :as fileserver} :fileserver :as context}]
  {:pre [(schema/contains-kind-sub-map? authorizer)
         (or (nil? custom-options) (map? custom-options))
         (or (nil? fileserver-port)
             (and (integer? fileserver-port)
                  (< 0 fileserver-port 65535)))
         (re-matches #"https?" fileserver-scheme)
         (pos-int? (:socket-timeout http-options))
         (pos-int? (:conn-timeout http-options))
         (and (number? log-bucket-sync-secs) (<= 0 log-bucket-sync-secs 300))
         (or (nil? log-bucket-url) (some? (io/as-url log-bucket-url)))
         (utils/non-neg-int? max-patch-retries)
         (pos-int? max-name-length)
         (not (string/blank? cluster-name))
         (integer? pod-base-port)
         (< 0 pod-base-port 65527) ; max port is 65535, and we need to reserve up to 10 ports
         (integer? pod-sigkill-delay-secs)
         (<= 0 pod-sigkill-delay-secs 300)
         (pos-int? pod-suffix-length)
         (not (string/blank? replicaset-api-version))
         (symbol? (:factory-fn replicaset-spec-builder))
         (pos-int? restart-expiry-threshold)
         (some? (io/as-url url))
         (not (string/blank? scheduler-name))
         (au/chan? scheduler-state-chan)
         (pos-int? scheduler-syncer-interval-secs)
         (fn? service-id->password-fn)
         (fn? service-id->service-description-fn)
         (fn? start-scheduler-syncer-fn)
         (or (nil? watch-retries) (integer? watch-retries))]}
  (let [authorizer (utils/create-component authorizer)
        http-client (-> http-options
                      (utils/assoc-if-absent :client-name "waiter-k8s")
                      (utils/assoc-if-absent :user-agent "waiter-k8s")
                      hu/http-client-factory)
        service-id->failed-instances-transient-store (atom {})
        replicaset-spec-builder-ctx (assoc replicaset-spec-builder
                                      :log-bucket-sync-secs log-bucket-sync-secs
                                      :log-bucket-url log-bucket-url)
        replicaset-spec-builder-fn (let [f (-> replicaset-spec-builder
                                               :factory-fn
                                               utils/resolve-symbol
                                               deref)]
                                     (assert (fn? f) "ReplicaSet spec function must be a Clojure fn")
                                     (fn [scheduler service-id service-description]
                                       (f scheduler service-id service-description replicaset-spec-builder-ctx)))
        watch-options (cond-> default-watch-options
                        (some? watch-retries)
                        (assoc :watch-retries watch-retries))
        watch-state (atom nil)
        scheduler-config {:api-server-url url
                          :http-client http-client
                          :cluster-name cluster-name
                          :replicaset-api-version replicaset-api-version
                          :restart-expiry-threshold restart-expiry-threshold
                          :service-id->failed-instances-transient-store service-id->failed-instances-transient-store
                          :watch-state watch-state}
        get-service->instances-fn #(get-service->instances scheduler-config)
        {:keys [retrieve-syncer-state-fn]} (start-scheduler-syncer-fn
                                             scheduler-name
                                             get-service->instances-fn
                                             scheduler-state-chan
                                             scheduler-syncer-interval-secs)]
    (when authentication
      (start-auth-renewer authentication))
    (let [daemon-state (atom nil)
          scheduler (->KubernetesScheduler url
                                           authorizer
                                           cluster-name
                                           custom-options
                                           daemon-state
                                           fileserver
                                           http-client
                                           log-bucket-url
                                           max-patch-retries
                                           max-name-length
                                           pod-base-port
                                           pod-sigkill-delay-secs
                                           pod-suffix-length
                                           replicaset-api-version
                                           replicaset-spec-builder-fn
                                           retrieve-syncer-state-fn
                                           service-id->failed-instances-transient-store
                                           service-id->password-fn
                                           service-id->service-description-fn
                                           scheduler-name
                                           watch-state)
          pod-watch-thread (start-pods-watch! scheduler watch-options)
          rs-watch-thread (start-replicasets-watch! scheduler watch-options)]
      (reset! daemon-state {:pod-watch-daemon pod-watch-thread
                            :rs-watch-daemon rs-watch-thread})
      scheduler)))
