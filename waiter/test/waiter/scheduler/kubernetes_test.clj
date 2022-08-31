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
(ns waiter.scheduler.kubernetes-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data]
            [clojure.data.json :as json]
            [clojure.pprint]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.config :as config]
            [waiter.scheduler :as scheduler]
            [waiter.scheduler.kubernetes :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.cache-utils :as cu]
            [waiter.util.client-tools :as ct]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent CountDownLatch)
           (waiter.scheduler Service ServiceInstance)
           (waiter.scheduler.kubernetes KubernetesScheduler)))

(defmacro throw-exception
  "Throw a RuntimeException with the stack trace suppressed.
   Suppressing the stack trace helps keep our test logs cleaner."
  []
  `(throw
    (doto (RuntimeException.)
      (.setStackTrace (make-array StackTraceElement 0)))))

(def ^:const default-pod-suffix-length 5)

(def dummy-scheduler-default-namespace "waiter")

(defn- make-dummy-scheduler
  ([service-ids] (make-dummy-scheduler service-ids {}))
  ([service-ids {:keys [default-namespace log-bucket-sync-secs log-bucket-url]
                 :or {default-namespace dummy-scheduler-default-namespace
                      log-bucket-sync-secs 60
                      log-bucket-url "http://waiter.example.com:8888/waiter-service-logs"}
                 :as args}]
   (->
    {:api-server-url "https://k8s-api.example/"
     :auth-str-atom (atom nil)
     :authenticate-health-checks? false
     :authorizer {:kind :default
                  :default {:factory-fn 'waiter.authorization/noop-authorizer}}
     :daemon-state (atom nil)
     :determine-replicaset-namespace-fn determine-replicaset-namespace
     :cluster-name "waiter"
     :container-running-grace-secs 120
     :event-fetcher-state (atom nil)
     :fileserver {:port 9090
                  :predicate-fn fileserver-container-enabled?
                  :scheme "http"}
     :k8s-object-key->event-cache (cu/cache-factory {:threshold 50 :ttl (-> 2 t/seconds t/in-millis)})
     :leader?-fn (constantly true)
     :log-bucket-sync-secs log-bucket-sync-secs
     :log-bucket-url log-bucket-url
     :max-patch-retries 5
     :max-name-length 63
     :pdb-api-version "policy/v1beta1"
     :pdb-spec-builder-fn waiter.scheduler.kubernetes/default-pdb-spec-builder
     :pod-base-port 8080
     :pod-sigkill-delay-secs 3
     :pod-suffix-length default-pod-suffix-length
     :replicaset-api-version "apps/v1"
     :replicaset-spec-builder-fn #(waiter.scheduler.kubernetes/default-replicaset-builder
                                   %1 %2 %3
                                   (merge {:container-init-commands ["waiter-k8s-init"]
                                           :default-namespace default-namespace
                                           :default-container-image "twosigma/waiter-test-apps:latest"
                                           :log-bucket-sync-secs log-bucket-sync-secs
                                           :log-bucket-url log-bucket-url}
                                          %4))
     :retrieve-auth-token-state-fn (constantly nil)
     :retrieve-syncer-state-fn (constantly nil)
     :response->deployment-error-msg-fn waiter.scheduler.kubernetes/default-k8s-message-transform
     :restart-expiry-threshold 100
     :restart-kill-threshold 200
     :service-id->deployment-error-cache (cu/cache-factory {:threshold 50 :ttl (-> 2 t/seconds t/in-millis)})
     :service-id->failed-instances-transient-store (atom {})
     :service-id->password-fn #(str "password-" %)
     :service-id->service-description-fn (pc/map-from-keys (constantly {"health-check-port-index" 0
                                                                        "run-as-user" "myself"})
                                                           service-ids)
     :scheduler-name "dummy-scheduler"
     :watch-chan-throttle-interval-ms 1
     :watch-init-timeout-ms 0
     :watch-state (atom nil)}
    (merge args)
    (update-in [:authorizer] utils/create-component)
    map->KubernetesScheduler)))

(def dummy-service-description
  {"backend-proto" "http"
   "cmd" "foo"
   "cpus" 1.2
   "grace-period-secs" 7
   "health-check-interval-secs" 10
   "health-check-max-consecutive-failures" 2
   "health-check-port-index" 0
   "health-check-proto" "http"
   "mem" 1024
   "min-instances" 1
   "ports" 2
   "run-as-user" "myself"
   "termination-grace-period-secs" 0})

(defn- sanitize-k8s-service-records
  "Walks data structure to remove extra fields added by Kubernetes from Service and ServiceInstance records."
  [walkable-collection]
  (walk/postwalk
   (fn sanitizer [x]
     (cond
       (instance? Service x)
       (dissoc x :k8s/app-name :k8s/namespace :k8s/replicaset-uid)
       (instance? ServiceInstance x)
       (dissoc x :k8s/api-server-url :k8s/app-name :k8s/namespace :k8s/pod-name :k8s/port->protocol :k8s/raven :k8s/restart-count :k8s/user)
       :else x))
   walkable-collection))

(defmacro assert-data-equal
  [expected actual]
  `(let [expected# ~expected
         actual# ~actual]
     (when-not (= expected# actual#)
       (clojure.pprint/pprint
        (clojure.data/diff expected# actual#)))
     (is (= expected# actual#))))

(deftest test-replicaset-spec-fileserver-container-and-metadata
  (let [current-time (t/now)]
    (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                  config/retrieve-waiter-principal (constantly "waiter@test.com")
                  t/now (constantly current-time)]
      (doseq [fileserver-enabled [true false]]
        (let [{:strs [run-as-user] :as service-description} (assoc dummy-service-description "health-check-port-index" 2 "ports" 3)
              test-service-id "waiter-testservice123456789"
              scheduler (make-dummy-scheduler
                         [test-service-id]
                         {:fileserver {:port 9090
                                       :predicate-fn (constantly fileserver-enabled)
                                       :scheme "http"}
                          :service-id->service-description-fn (constantly service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler test-service-id service-description rs-spec-builder-context)]
          (is (= {:waiter/revision-timestamp (du/date-to-str current-time)
                  :waiter/revision-version "0"
                  :waiter/run-as-user-source "unknown"
                  :waiter/service-id test-service-id}
                 (get-in replicaset-spec [:metadata :annotations])))
          (is (= {:app test-service-id
                  :waiter/cluster dummy-scheduler-default-namespace
                  :waiter/fileserver (if fileserver-enabled "enabled" "disabled")
                  :waiter/raven "disabled"
                  :waiter/service-hash test-service-id
                  :waiter/user run-as-user}
                 (get-in replicaset-spec [:metadata :labels])))
          (let [fileserver-containers (filter #(= waiter-fileserver-sidecar-name (:name %))
                                              (get-in replicaset-spec [:spec :template :spec :containers]))]
            (if fileserver-enabled
              (is (= 1 (count fileserver-containers)))
              (is (empty? fileserver-containers)))))))))

(deftest test-replicaset-spec-health-check-port-index
  (let [current-time (t/now)]
    (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                  config/retrieve-waiter-principal (constantly "waiter@test.com")
                  t/now (constantly current-time)]
      (let [service-description (assoc dummy-service-description "health-check-port-index" 2 "ports" 3)
            scheduler (make-dummy-scheduler ["test-service-id"]
                                            {:service-id->service-description-fn (constantly service-description)})
            rs-spec-builder-context {:run-as-user-source "unknown"}
            replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" service-description rs-spec-builder-context)]
        (is (= {:waiter/port-count "3"
                :waiter/port-onto-protocol "{\"p8330\":\"http\",\"p8332\":\"http\"}"
                :waiter/revision-timestamp (du/date-to-str current-time)
                :waiter/revision-version "0"
                :waiter/run-as-user-source "unknown"
                :waiter/service-id "test-service-id"
                :waiter/service-port "8330"}
               (get-in replicaset-spec [:spec :template :metadata :annotations])))))))

(deftest test-replicaset-and-pod-annotations-waiter-config-token-env
  (let [current-time (t/now)
        service-id "test-service-id"]
    (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                  config/retrieve-waiter-principal (constantly "waiter@test.com")
                  t/now (constantly current-time)]
      (let [service-description dummy-service-description
            scheduler (make-dummy-scheduler [service-id] {:service-id->service-description-fn (constantly service-description)})
            rs-spec-builder-context {:run-as-user-source "unknown"}
            replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)]
        (is (nil? (get-in replicaset-spec [:metadata :annotations :waiter/token])))
        (is (nil? (get-in replicaset-spec [:spec :template :metadata :annotations :waiter/token]))))

      (doseq [waiter-config-token-env ["foo-bar" "foo.test.com"]]
        (let [service-description (assoc-in dummy-service-description ["env" "WAITER_CONFIG_TOKEN"] waiter-config-token-env)
              scheduler (make-dummy-scheduler [service-id] {:service-id->service-description-fn (constantly service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)]
          (is (= waiter-config-token-env (get-in replicaset-spec [:metadata :annotations :waiter/token])))
          (is (= waiter-config-token-env (get-in replicaset-spec [:spec :template :metadata :annotations :waiter/token])))))

      (let [waiter-config-token-env "foo.test.com,bar.test.com"
            service-description (assoc-in dummy-service-description ["env" "WAITER_CONFIG_TOKEN"] waiter-config-token-env)
            scheduler (make-dummy-scheduler [service-id] {:service-id->service-description-fn (constantly service-description)})
            rs-spec-builder-context {:run-as-user-source "unknown"}
            replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)]
        (is (nil? (get-in replicaset-spec [:metadata :annotations :waiter/token])))
        (is (nil? (get-in replicaset-spec [:spec :template :metadata :annotations :waiter/token])))))))

(defn- make-replica-spec-with-pod-anti-affinity
  "helper function for test-replicaset-spec-pod-anti-affinity"
  [anti-affinity-config-val]
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [service-description (assoc dummy-service-description "health-check-port-index" 2 "ports" 3)
          scheduler (make-dummy-scheduler ["test-service-id"]
                                          {:service-id->service-description-fn (constantly service-description)})
          rs-spec-builder-context {:pod-anti-affinity anti-affinity-config-val
                                   :run-as-user-source "unknown"}
          replicaset-spec ((:replicaset-spec-builder-fn scheduler)
                           scheduler "test-service-id" service-description rs-spec-builder-context)]
      {:anti-affinity-spec (get-in replicaset-spec [:spec :template :spec :affinity :podAntiAffinity])
       :k8s-name (get-in replicaset-spec [:metadata :labels :app])})))

(deftest test-replicaset-spec-pod-anti-affinity
  (let [expected-spec (fn [anti-affinity-type k8s-name]
                        {anti-affinity-type
                         [{:podAffinityTerm {:labelSelector {:matchExpressions [{:key "app"
                                                                                 :operator "In"
                                                                                 :values [k8s-name]}]}
                                             :topologyKey "kubernetes.io/hostname"}
                           :weight 50}]})]
    (testing "preferred pod anti-affinity"
      (let [{:keys [anti-affinity-spec k8s-name]} (make-replica-spec-with-pod-anti-affinity :preferred)]
        (is (= (expected-spec :preferredDuringSchedulingIgnoredDuringExecution k8s-name)
               anti-affinity-spec))))
    (testing "required pod anti-affinity"
      (let [{:keys [anti-affinity-spec k8s-name]} (make-replica-spec-with-pod-anti-affinity :required)]
        (is (= (expected-spec :requiredDuringSchedulingIgnoredDuringExecution k8s-name)
               anti-affinity-spec))))
    (testing "default (nil) pod anti-affinity"
      (let [{:keys [anti-affinity-spec]} (make-replica-spec-with-pod-anti-affinity nil)]
        (is (nil? anti-affinity-spec))))))

(deftest test-replicaset-spec-termination-grace-period-secs
  (let [current-time (t/now)
        rs-spec-builder-context {:run-as-user-source "unknown"}]
    (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                  config/retrieve-waiter-principal (constantly "waiter@test.com")
                  t/now (constantly current-time)]
      (doseq [{:keys [configured-termination-grace-period-secs spec-termination-grace-period-secs]}
              [{:configured-termination-grace-period-secs 0 :spec-termination-grace-period-secs 3}
               {:configured-termination-grace-period-secs 1 :spec-termination-grace-period-secs 3}
               {:configured-termination-grace-period-secs 5 :spec-termination-grace-period-secs 5}
               {:configured-termination-grace-period-secs 15 :spec-termination-grace-period-secs 15}]]
        (let [service-description (assoc dummy-service-description "termination-grace-period-secs" configured-termination-grace-period-secs)
              {:keys [log-bucket-sync-secs replicaset-spec-builder-fn] :as scheduler}
              (make-dummy-scheduler ["test-service-id"]
                                    {:service-id->service-description-fn (constantly service-description)})
              replicaset-spec (replicaset-spec-builder-fn scheduler "test-service-id" service-description rs-spec-builder-context)]
          (is (= (+ log-bucket-sync-secs spec-termination-grace-period-secs)
                 (get-in replicaset-spec [:spec :template :spec :terminationGracePeriodSeconds])))))

      (let [service-description dummy-service-description
            pod-sigkill-delay-secs 10
            {:keys [replicaset-spec-builder-fn] :as scheduler}
            (make-dummy-scheduler ["test-service-id"]
                                  {:log-bucket-sync-secs 10
                                   :log-bucket-url nil
                                   :pod-sigkill-delay-secs pod-sigkill-delay-secs})
            replicaset-spec (replicaset-spec-builder-fn scheduler "test-service-id" service-description rs-spec-builder-context)]
        (is (= pod-sigkill-delay-secs (get-in replicaset-spec [:spec :template :spec :terminationGracePeriodSeconds]))))

      (let [log-bucket-url "http://service.example.com:1234/service-logs"
            service-description (assoc dummy-service-description "env" {"WAITER_CONFIG_LOG_BUCKET_URL" log-bucket-url})
            pod-sigkill-delay-secs 5
            log-bucket-sync-secs 10
            {:keys [replicaset-spec-builder-fn] :as scheduler}
            (make-dummy-scheduler ["test-service-id"]
                                  {:log-bucket-sync-secs log-bucket-sync-secs
                                   :log-bucket-url nil
                                   :pod-sigkill-delay-secs pod-sigkill-delay-secs})
            replicaset-spec (replicaset-spec-builder-fn scheduler "test-service-id" service-description rs-spec-builder-context)]
        (is (= (+ pod-sigkill-delay-secs log-bucket-sync-secs)
               (get-in replicaset-spec [:spec :template :spec :terminationGracePeriodSeconds])))))))

(deftest test-get-port-range
  ;; this condition is critical for our sidecar-proxy logic,
  ;; which reserves a second range of ports by incrementing the hash code,
  ;; and the two ranges must not overlap (or be equal)
  (testing "no two adjacent service-id hashes map to the same port0"
    (let [ports (for [i (range 1000) j (range 100)] (get-port-range i j 0))]
      (is (every? (partial apply not=) (partition 2 1 ports))))))

(deftest test-determine-namespace
  (is (thrown-with-msg? ExceptionInfo #"Waiter configuration is missing a default namespace for Kubernetes pods"
                        (determine-namespace nil nil nil {"run-as-user" "jill"})))
  (is (thrown-with-msg? ExceptionInfo #"service namespace does not match scheduler namespace"
                        (determine-namespace "john" "jack" nil {"namespace" "jane" "run-as-user" "jill"})))
  (is (thrown-with-msg? ExceptionInfo #"service namespace does not match scheduler namespace"
                        (determine-namespace "john" "jack" "jeff" {"namespace" "jane" "run-as-user" "jill"})))

  (is (= "jane" (determine-namespace nil nil nil {"namespace" "jane" "run-as-user" "jill"})))
  (is (= "jeff" (determine-namespace nil nil "jeff" {"namespace" "jane" "run-as-user" "jill"})))
  (is (= "jack" (determine-namespace nil "jack" nil {"run-as-user" "jill"})))
  (is (= "jeff" (determine-namespace nil "jack" "jeff" {"run-as-user" "jill"})))
  (is (= "jill" (determine-namespace nil "*" nil {"run-as-user" "jill"})))
  (is (= "jill" (determine-namespace "john" "*" nil {"run-as-user" "jill"})))
  (is (= "jane" (determine-namespace nil "*" nil {"namespace" "jane" "run-as-user" "jill"})))
  (is (= "john" (determine-namespace "john" nil nil {"run-as-user" "jill"})))
  (is (= "jeff" (determine-namespace "john" nil "jeff" {"run-as-user" "jill"})))
  (is (= "jack" (determine-namespace "john" "jack" nil {"run-as-user" "jill"})))
  (is (= "jeff" (determine-namespace "john" "jack" "jeff" {"run-as-user" "jill"}))))

(defn- get-container-env-map
  "Get an var->value map from a container's environment."
  [{:keys [env]}]
  (->> env (mapv (juxt :name :value)) (into {})))

(deftest test-replicaset-spec-with-raven
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-request-log-request-headers (constantly  #{})
                config/retrieve-request-log-response-headers (constantly #{"content-type" "server"})
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [service-id "proxy-test-service-id"
          custom-raven-flag "MY_RAVEN_FLAG"
          scheduler (make-dummy-scheduler [service-id] {:raven-sidecar {:cmd ["/opt/waiter/raven/bin/raven-start"]
                                                                        :env-vars {:defaults {"PORT0" "P0"
                                                                                              "RAVEN_E1" "V1"}
                                                                                   :flags [custom-raven-flag]}
                                                                        :image "twosigma/waiter-raven"
                                                                        :predicate-fn raven-sidecar-opt-in?
                                                                        :resources {:cpu 0.1 :mem 256}}})
          service-description (assoc dummy-service-description "env" {custom-raven-flag "true"
                                                                      "PORT0" "to-be-overwritten"
                                                                      "SERVICE_PORT" "to-be-overwritten"})
          rs-spec-builder-context {:run-as-user-source "unknown"}
          replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)
          app-container (get-in replicaset-spec [:spec :template :spec :containers 0])
          app-env (get-container-env-map app-container)
          sidecar-container (some
                             #(if (= waiter-raven-sidecar-name (:name %)) %)
                             (get-in replicaset-spec [:spec :template :spec :containers]))
          sidecar-env (get-container-env-map sidecar-container)]

      (testing "raven opt-in flag recognized in service env"
        (is (has-raven-config-in-env? app-env (:raven-sidecar scheduler))))

      (testing "replicaset has waiter/service-port annotation"
        (is (contains? (get-in replicaset-spec [:spec :template :metadata :annotations]) :waiter/service-port)))

      (testing "sidecar container is present in replicaset"
        (is (some? sidecar-container)))

      (testing "sidecar container has unique entries in environment"
        (is (= (count sidecar-env) (-> sidecar-container :env count))))

      (testing "configure default variables are present in environment"
        (is (some #(and (contains? #{"RAVEN_E1"} (:name %)) (= "V1" (:value %))) (:env sidecar-container))))

      (testing "user defined environment variables are correctly overwritten"
        (is (not-any? #(and (contains? #{"PORT0" "SERVICE_PORT"} (:name %))
                            (= "to-be-overwritten" (:value %)))
                      (:env sidecar-container))))

      (testing "request log entries in environment variables"
        (is (not (contains? sidecar-env "RAVEN_LOG_REQUEST_HEADER_NAMES")))
        (is (= "content-type,server" (get sidecar-env "RAVEN_LOG_RESPONSE_HEADER_NAMES"))))

      (testing "waiter/raven label is set"
        (is (= "enabled" (get-in replicaset-spec [:metadata :labels :waiter/raven]))))

      (testing "service-proto, service-port and waiter port0 values and env variables are correct"
        (let [{:keys [pod-base-port]} scheduler
              service-id-hash (hash service-id)
              service-port (get-port-range service-id-hash service-ports-index pod-base-port)
              port0 (get-port-range service-id-hash proxied-ports-index pod-base-port)
              env-service-proto (get sidecar-env "SERVICE_PROTOCOL")
              env-service-port (get sidecar-env "SERVICE_PORT")
              env-port0 (get sidecar-env "PORT0")]
          (is (= "http" env-service-proto))
          (is (= service-port (Integer/parseInt (get-in replicaset-spec [:spec :template :metadata :annotations :waiter/service-port]))))
          (is (= service-port (get-in sidecar-container [:ports 0 :containerPort])))
          (is (= (str service-port) env-service-port))
          (is (= port0 (get-in app-container [:ports 0 :containerPort])))
          (is (= (str port0) env-port0))

          (testing "waiter and raven ports in main container"
            (is (= (str port0) (get app-env "PORT0" "missing")))
            (is (= (str service-port) (get app-env "WAITER_RAVEN_PORT" "missing"))))))

      (testing "waiter/port-count annotation is correct"
        (let [port-count (get service-description "ports")]
          (is (= port-count (-> (get-in replicaset-spec [:spec :template :metadata :annotations :waiter/port-count])
                                (Integer/parseInt))))))

      (testing "resource requests for raven container are correct"
        (let [cpu (get-in sidecar-container [:resources :requests :cpu])
              memory (get-in sidecar-container [:resources :requests :memory])]
          (is (= "0.1" cpu))
          (is (= "256Mi" memory))))

      (testing "resource limits for raven are correct"
        (let [memory-limit (get-in sidecar-container [:resources :limits :memory])]
          (is (= "256Mi" memory-limit))))

      (testing "raven pod container name is correct"
        (is (= waiter-raven-sidecar-name (:name sidecar-container))))

      (testing "raven pod container image is correct"
        (is (= "twosigma/waiter-raven" (:image sidecar-container)))))))

(deftest test-raven-env-flags
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-request-log-request-headers (constantly  #{"content-type" "host"})
                config/retrieve-request-log-response-headers (constantly #{"content-type" "server"})
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [service-id "raven-test-service-id"
          custom-raven-flag "MY_RAVEN_FLAG"
          custom-raven-tls-flag "MY_RAVEN_TLS_FLAG"
          scheduler (make-dummy-scheduler [service-id] {:raven-sidecar {:cmd ["/opt/waiter/raven/bin/raven-start"]
                                                                        :env-vars {:flags [custom-raven-flag]
                                                                                   :tls-flags [custom-raven-tls-flag]}
                                                                        :image "twosigma/waiter-raven"
                                                                        :predicate-fn raven-sidecar-opt-in?
                                                                        :resources {:cpu 0.1 :mem 256}}})]
      (doseq [raven? [true false]
              tls? [true false]]
        (let [service-description (assoc dummy-service-description "env" {custom-raven-flag (str raven?)
                                                                          custom-raven-tls-flag (str tls?)
                                                                          "PORT0" "to-be-overwritten"
                                                                          "SERVICE_PORT" "to-be-overwritten"})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)
              app-container (get-in replicaset-spec [:spec :template :spec :containers 0])
              app-env (get-container-env-map app-container)
              readiness-probe-scheme (get-in app-container [:readinessProbe :httpGet :scheme])
              liveness-probe-scheme (get-in app-container [:livenessProbe :httpGet :scheme])
              raven-container (some
                               #(if (= waiter-raven-sidecar-name (:name %)) %)
                               (get-in replicaset-spec [:spec :template :spec :containers]))]

          (testing "raven opt-in flag recognized in service env"
            (is (has-raven-config-in-env? app-env (:raven-sidecar scheduler))))

          (testing "waiter/raven label is set"
            (let [raven-mode (get-in replicaset-spec [:metadata :labels :waiter/raven])]
              (cond
                (not raven?) (is (= "disabled" raven-mode))
                (not tls?) (is (= "enabled" raven-mode))
                :else (is (= "strict-tls" raven-mode)))))

          (testing "raven container present iff raven is enabled"
            (is (= raven? (some? raven-container)))

            (when raven?
              (testing "request log entries"
                (let [raven-env (get-container-env-map raven-container)]
                  (is (= "content-type,host" (get raven-env "RAVEN_LOG_REQUEST_HEADER_NAMES")))
                  (is (= "content-type,server" (get raven-env "RAVEN_LOG_RESPONSE_HEADER_NAMES")))))))

          (testing "liveness probe protocol is correct (always unproxied)"
            (is (= "HTTP" liveness-probe-scheme)))

          (testing "readiness probe protocol is correct (can be proxied)"
            (if (and raven? tls?)
              (is (= "HTTPS" readiness-probe-scheme))
              (is (= "HTTP" readiness-probe-scheme)))))))))

(deftest test-replicaset-spec-with-raven-health-check
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-request-log-request-headers (constantly  #{"content-type" "host"})
                config/retrieve-request-log-response-headers (constantly #{})
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [service-id "proxy-health-test-service-id"
          scheduler (make-dummy-scheduler [service-id] {:raven-sidecar {:cmd ["/opt/waiter/raven/bin/raven-start"]
                                                                        :env-vars {:flags [default-raven-env-flag]}
                                                                        :image "twosigma/waiter-raven"
                                                                        :predicate-fn raven-sidecar-opt-out?
                                                                        :resources {:cpu 0.1 :mem 256}}})
          service-description (assoc dummy-service-description
                                     "env" {"PORT0" "to-be-overwritten"
                                            "SERVICE_PORT" "to-be-overwritten"}
                                     "health-check-port-index" 5
                                     "ports" 9)
          rs-spec-builder-context {:run-as-user-source "unknown"}
          replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)
          app-container (get-in replicaset-spec [:spec :template :spec :containers 0])
          app-env (get-container-env-map app-container)
          sidecar-container (some
                             #(if (= waiter-raven-sidecar-name (:name %)) %)
                             (get-in replicaset-spec [:spec :template :spec :containers]))
          sidecar-env (get-container-env-map sidecar-container)]

      (testing "raven-sidecar config has default opt-in env flag"
        (is (= [default-raven-env-flag] (get-in scheduler [:raven-sidecar :env-vars :flags]))))

      (testing "raven opt-out worked w/o flag recognized in service env"
        (is (not (has-raven-config-in-env? app-env (:raven-sidecar scheduler)))))

      (testing "replicaset has waiter/raven=enabled label"
        (is (= "enabled" (get-in replicaset-spec [:metadata :labels :waiter/raven]))))

      (testing "replicaset has waiter/service-port annotation"
        (is (contains? (get-in replicaset-spec [:spec :template :metadata :annotations]) :waiter/service-port)))

      (testing "sidecar container is present in replicaset"
        (is (not= nil sidecar-container)))

      (testing "sidecar container has unique entries in environment"
        (is (= (count sidecar-env) (-> sidecar-container :env count))))

      (testing "user defined environment variables are correctly overwritten"
        (is (nil? (some #(when (or
                                (= (:name %) "PORT0")
                                (= (:name %) "SERVICE_PORT"))
                           (= "to-be-overwritten" (:value %))) (:env sidecar-container)))))

      (testing "request log entries in environment variables"
        (is (= "content-type,host" (get sidecar-env "RAVEN_LOG_REQUEST_HEADER_NAMES")))
        (is (not (contains? sidecar-env "RAVEN_LOG_RESPONSE_HEADER_NAMES"))))

      (testing "ports and corresponding and env variables are correct"
        (let [{:keys [pod-base-port]} scheduler
              service-id-hash (hash service-id)
              service-port (get-port-range service-id-hash service-ports-index pod-base-port)
              health-check-port (+ 5 service-port)
              port0 (get-port-range service-id-hash proxied-ports-index pod-base-port)
              port5 (+ 5 port0)
              env-service-port (get sidecar-env "SERVICE_PORT")
              env-health-check-port-index (get sidecar-env "HEALTH_CHECK_PORT_INDEX")
              readiness-probe-port (get-in app-container [:readinessProbe :httpGet :port])
              get-env-port (fn get-env-port [n] (->> n (str "PORT") (get sidecar-env) (Integer/parseInt)))
              liveness-probe-port (get-in app-container [:livenessProbe :httpGet :port])]
          (is (= service-port (Integer/parseInt (get-in replicaset-spec [:spec :template :metadata :annotations :waiter/service-port]))))
          (is (= service-port (get-in sidecar-container [:ports 0 :containerPort])))
          (is (= (str service-port) env-service-port))
          (is (= "5" env-health-check-port-index))
          (is (= readiness-probe-port health-check-port))
          (is (= port0 (get-in app-container [:ports 0 :containerPort])))
          (is (= port0 (get-env-port 0)))
          (is (= (+ port0 1) (get-env-port 1)))
          (is (= (+ port0 2) (get-env-port 2)))
          (is (= (+ port0 3) (get-env-port 3)))
          (is (= (+ port0 4) (get-env-port 4)))
          (is (= (+ port0 5) (get-env-port 5)))
          (is (= liveness-probe-port port5))))

      (testing "waiter/port-count annotation is correct"
        (let [port-count (get service-description "ports")]
          (is (= port-count (-> (get-in replicaset-spec [:spec :template :metadata :annotations :waiter/port-count])
                                (Integer/parseInt))))))

      (testing "resource requests for raven are correct"
        (let [cpu (get-in sidecar-container [:resources :requests :cpu])
              memory (get-in sidecar-container [:resources :requests :memory])]
          (is (= "0.1" cpu))
          (is (= "256Mi" memory))))

      (testing "resource limits for raven are correct"
        (let [memory-limit (get-in sidecar-container [:resources :limits :memory])]
          (is (= "256Mi" memory-limit))))

      (testing "raven pod container name is correct"
        (is (= waiter-raven-sidecar-name (:name sidecar-container))))

      (testing "raven pod container image is correct"
        (is (= "twosigma/waiter-raven" (:image sidecar-container)))))))

(deftest test-replicaset-spec-liveness-and-readiness
  (let [basic-probe {:failureThreshold 1
               :httpGet {:httpHeaders [{:name "x-waiter-request-type" :value "health-check"}]
                         :path "/status"
                         :port 8330
                         :scheme "HTTP"}
               :periodSeconds 10
               :timeoutSeconds 1}]
    (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                  config/retrieve-waiter-principal (constantly "waiter@test.com")]

      (testing "liveness enabled, uses health check config for defaults"
        (let [service-description dummy-service-description
              scheduler (make-dummy-scheduler ["test-service-id"]
                                              {:service-id->service-description-fn (constantly service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" service-description rs-spec-builder-context)
              waiter-app-container (get-in replicaset-spec [:spec :template :spec :containers 0])]
          (is (= basic-probe (:readinessProbe waiter-app-container)))
          (is (= (assoc (:readinessProbe waiter-app-container) :failureThreshold 3 :initialDelaySeconds 7)
                 (:livenessProbe waiter-app-container)))))
      
      (testing "liveness enabled, uses explicit config"
        (let [service-description (assoc dummy-service-description
                                         "liveness-check-interval-secs" 600
                                         "liveness-check-max-consecutive-failures" 36
                                         "liveness-check-port-index" 1
                                         "liveness-check-proto" "https"
                                         "liveness-check-url" "/liveness")
              scheduler (make-dummy-scheduler ["test-service-id"]
                                              {:service-id->service-description-fn (constantly service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" service-description rs-spec-builder-context)
              waiter-app-container (get-in replicaset-spec [:spec :template :spec :containers 0])]
          (is (= basic-probe (:readinessProbe waiter-app-container)))
          (is (= (-> basic-probe
                     (assoc :failureThreshold 37 :initialDelaySeconds 7 :periodSeconds 600)
                     (assoc-in [:httpGet :path] "/liveness")
                     (assoc-in [:httpGet :port] 8331)
                     (assoc-in [:httpGet :scheme] "HTTPS"))
                 (:livenessProbe waiter-app-container)))))

      (testing "liveness disabled"
        (let [service-description (assoc dummy-service-description "grace-period-secs" 0)
              scheduler (make-dummy-scheduler ["test-service-id"]
                                              {:service-id->service-description-fn (constantly service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" service-description rs-spec-builder-context)
              waiter-app-container (get-in replicaset-spec [:spec :template :spec :containers 0])]
          (is (not (contains? waiter-app-container :livenessProbe)))
          (is (= basic-probe (:readinessProbe waiter-app-container))))))))

(deftest test-replicaset-spec-namespace
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [service-id "test-service-id"]
      (testing "Star namespace"
        (let [scheduler (make-dummy-scheduler [service-id]
                                              {:default-namespace "*"
                                               :service-id->service-description-fn (constantly dummy-service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id dummy-service-description rs-spec-builder-context)
              {:strs [run-as-user]} dummy-service-description]
          (is (nil? (scheduler/validate-service scheduler service-id)))
          (is (= run-as-user (get-in replicaset-spec [:metadata :namespace])))
          (is (true? (get-in replicaset-spec [:spec :template :spec :automountServiceAccountToken])))))
      (testing "Default namespace"
        (let [scheduler (make-dummy-scheduler [service-id]
                                              {:service-id->service-description-fn (constantly dummy-service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id dummy-service-description rs-spec-builder-context)]
          (is (nil? (scheduler/validate-service scheduler service-id)))
          (is (= dummy-scheduler-default-namespace (get-in replicaset-spec [:metadata :namespace])))
          (is (false? (get-in replicaset-spec [:spec :template :spec :automountServiceAccountToken]))))
        (testing "service validation"
          (let [rs-spec-builder-context {:run-as-user-source "unknown"}]
            (let [service-description (assoc dummy-service-description "max-instances" 10 "min-instances" 4)
                  scheduler (make-dummy-scheduler [service-id]
                                                  {:service-id->service-description-fn (constantly service-description)})]
              (is (map? ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context))))
            (let [service-description (assoc dummy-service-description "max-instances" 10 "min-instances" 5)
                  scheduler (make-dummy-scheduler [service-id]
                                                  {:service-id->service-description-fn (constantly service-description)})]
              (is (thrown-with-msg? ExceptionInfo #"min-instances \(5\) in the default namespace must be less than or equal to 4"
                                    ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context))))
            (let [service-description (assoc dummy-service-description "max-instances" 10 "min-instances" 5 "namespace" "default")
                  scheduler (make-dummy-scheduler [service-id]
                                                  {:service-id->service-description-fn (constantly service-description)})]
              (is (thrown-with-msg? ExceptionInfo #"min-instances \(5\) in the default namespace must be less than or equal to 4"
                                    ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context))))
            (let [service-description (assoc dummy-service-description "max-instances" 10 "min-instances" 5 "namespace" "myself")
                  scheduler (make-dummy-scheduler [service-id]
                                                  {:service-id->service-description-fn (constantly service-description)})]
              (is (map? ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)))))))
      (testing "Override namespace"
        (let [scheduler (make-dummy-scheduler [service-id]
                                              {:service-id->service-description-fn (constantly dummy-service-description)})
              rs-spec-builder-context {:override-namespace "john"
                                       :run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id dummy-service-description rs-spec-builder-context)]
          (is (nil? (scheduler/validate-service scheduler service-id)))
          (is (= "john" (get-in replicaset-spec [:metadata :namespace])))
          (is (false? (get-in replicaset-spec [:spec :template :spec :automountServiceAccountToken])))))
      (testing "Valid custom namespace"
        (let [target-namespace "myself"
              service-description (assoc dummy-service-description "namespace" target-namespace)
              scheduler (make-dummy-scheduler [service-id]
                                              {:service-id->service-description-fn (constantly service-description)})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)]
          (is (nil? (scheduler/validate-service scheduler service-id)))
          (is (= target-namespace (get-in replicaset-spec [:metadata :namespace])))
          (is (true? (get-in replicaset-spec [:spec :template :spec :automountServiceAccountToken])))))
      (testing "Custom namespace matches scheduler namespace"
        (let [target-namespace "myself"
              service-description (assoc dummy-service-description "namespace" target-namespace)
              scheduler (make-dummy-scheduler [service-id] {:namespace target-namespace})
              rs-spec-builder-context {:run-as-user-source "unknown"}
              replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context)]
          (is (nil? (scheduler/validate-service scheduler service-id)))
          (is (= target-namespace (get-in replicaset-spec [:metadata :namespace])))
          (is (true? (get-in replicaset-spec [:spec :template :spec :automountServiceAccountToken])))))
      (testing "Custom namespace does not match scheduler namespace"
        (let [target-namespace "myself"
              service-description (assoc dummy-service-description "namespace" target-namespace)
              scheduler (make-dummy-scheduler [service-id] {:namespace (str "x" target-namespace)})
              rs-spec-builder-context {:run-as-user-source "unknown"}]
          (is (thrown-with-msg? RuntimeException #"service namespace does not match scheduler namespace"
                                ((:replicaset-spec-builder-fn scheduler) scheduler service-id service-description rs-spec-builder-context))))))))

(deftest test-replicaset-spec-no-image
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [scheduler (make-dummy-scheduler ["test-service-id"])
          rs-spec-builder-context {:run-as-user-source "unknown"}
          replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" dummy-service-description rs-spec-builder-context)]
      (is (= "twosigma/waiter-test-apps:latest" (get-in replicaset-spec [:spec :template :spec :containers 0 :image]))))))

(deftest test-replicaset-spec-custom-image
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [scheduler (make-dummy-scheduler ["test-service-id"])
          rs-spec-builder-context {:run-as-user-source "unknown"}
          service-description (assoc dummy-service-description "image" "custom/image")
          replicaset-spec ((:replicaset-spec-builder-fn scheduler) scheduler "test-service-id" service-description rs-spec-builder-context)]
      (is (= "custom/image" (get-in replicaset-spec [:spec :template :spec :containers 0 :image]))))))

(deftest test-default-pdb-spec-builder
  (doseq [{:keys [min-available replicas]}
          [{:min-available 1 :replicas 1} ;; ideally we do not expect it to be called for 1 replica
           {:min-available 1 :replicas 2}
           {:min-available 2 :replicas 3}
           {:min-available 2 :replicas 4}
           {:min-available 3 :replicas 5}]]
    (let [pdb-api-version "pdb/api-version"
          service-id "test-service-id"
          rs-labels {:l1 "v1" :l2 "v2"}
          rs-selector {:a "b" :c "d"}
          rs-spec {:apiVersion "rs/api-version"
                   :kind "kind/ReplicaSet"
                   :metadata {:annotations {:waiter/service-id service-id}
                              :labels rs-labels
                              :name "replicaset-1234"}
                   :spec {:replicas replicas
                          :selector rs-selector}}
          replicaset-uid "replicaset-1234-uid"
          actual-spec (default-pdb-spec-builder pdb-api-version rs-spec replicaset-uid)
          pdb-hash (-> replicaset-uid (hash) (mod 9000) (+ 1000))
          expected-spec {:apiVersion "pdb/api-version"
                         :kind "PodDisruptionBudget"
                         :metadata {:annotations {:waiter/service-id service-id}
                                    :labels rs-labels
                                    :name (str "replicaset-1234-pdb-" pdb-hash)
                                    :ownerReferences [{:apiVersion "rs/api-version"
                                                       :blockOwnerDeletion true
                                                       :controller false
                                                       :kind "kind/ReplicaSet"
                                                       :name "replicaset-1234"
                                                       :uid "replicaset-1234-uid"}]}
                         :spec {:minAvailable min-available
                                :selector rs-selector}}]
      (is (= expected-spec actual-spec)))))

(deftest test-prepare-health-check-probe
  (with-redefs [scheduler/retrieve-auth-headers (constantly {"Authorization" "Basic foo:bar"})]
    (let [service-id->password-fn (constantly "waiter-password")
          service-id "test-service-id"
          health-check-scheme "http"
          health-check-url "/status"
          health-check-port 80
          health-check-interval-secs 10]
      (is (= {:httpGet {:httpHeaders [{:name "Authorization", :value "Basic foo:bar"}
                                      {:name "x-waiter-request-type" :value "health-check"}]
                        :path health-check-url
                        :port health-check-port
                        :scheme health-check-scheme}
              :periodSeconds health-check-interval-secs
              :timeoutSeconds 1}
             (prepare-health-check-probe
               service-id->password-fn service-id true
               health-check-scheme health-check-url health-check-port health-check-interval-secs)))
      (is (= {:httpGet {:httpHeaders [{:name "x-waiter-request-type" :value "health-check"}]
                        :path health-check-url
                        :port health-check-port
                        :scheme health-check-scheme}
              :periodSeconds health-check-interval-secs
              :timeoutSeconds 1}
             (prepare-health-check-probe
              service-id->password-fn service-id false
              health-check-scheme health-check-url health-check-port health-check-interval-secs))))))

(deftest test-service-id->k8s-app-name
  (let [base-scheduler-spec {:pod-suffix-length default-pod-suffix-length}
        long-app-name (str/join (repeat 200 \A))
        sample-uuid "e8b625cc83c411e8974c38d5474b213d"
        short-app-name "myapp"
        short-sample-uuid (str (subs sample-uuid 0 8)
                               (subs sample-uuid 24))
        waiter-service-prefix "waiter-service-id-prefix-"]
    (let [{:keys [max-name-length] :as long-name-scheduler}
          (assoc base-scheduler-spec :max-name-length 128)]
      (testing "Very long name length limit maps long service-id through unmodified"
        (let [service-id (str "this_name_is_longer_than_64_chars_but_should_be_unmodified_by_the_mapping_function-"
                              sample-uuid)]
          (is (= service-id (service-id->k8s-app-name long-name-scheduler service-id)))))
      (testing "Very long name length limit maps long service-id through without prefix"
        (let [service-id (str waiter-service-prefix
                              "this_name_is_longer_than_64_chars_but_should_be_unmodified_by_the_mapping_function-"
                              sample-uuid)]
          (is (= (subs service-id (count waiter-service-prefix))
                 (service-id->k8s-app-name long-name-scheduler service-id)))))
      (testing "Very long name length limit maps truncates service name, but keeps full uuid"
        (let [service-id (str waiter-service-prefix long-app-name "-" sample-uuid)]
          (is (= (str (subs long-app-name 0 (- max-name-length 1 32 1 default-pod-suffix-length))
                      "-" sample-uuid)
                 (service-id->k8s-app-name long-name-scheduler service-id))))))
    (let [{:keys [max-name-length] :as short-name-scheduler}
          (assoc base-scheduler-spec :max-name-length 32)]
      (assert (== 32 (count sample-uuid)))
      (assert (== 16 (count short-sample-uuid)))
      (testing "Short name length limit with short app name only shortens uuid"
        (let [service-id (str short-app-name "-" sample-uuid)]
          (is (= (str short-app-name "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id)))))
      (testing "Short name length limit with short app name only removes prefix, shortens uuid"
        (let [service-id (str waiter-service-prefix short-app-name "-" sample-uuid)]
          (is (= (str short-app-name "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id)))))
      (testing "Short name length limit with long app name truncates app name, removes prefix, and shortens uuid"
        (let [service-id (str waiter-service-prefix long-app-name "-" sample-uuid)]
          (is (= (str (subs long-app-name 0 (- max-name-length 1 16 1 default-pod-suffix-length))
                      "-" short-sample-uuid)
                 (service-id->k8s-app-name short-name-scheduler service-id))))))))

(defn- reset-scheduler-watch-state!
  ([scheduler rs-response]
   (reset-scheduler-watch-state! scheduler rs-response nil))
  ([{:keys [watch-state] :as scheduler} rs-response pods-response]
   (let [dummy-resource-uri "http://ignored-uri"
         rs-state (global-rs-state-query
                   scheduler {:api-request-fn (constantly rs-response)} dummy-resource-uri)
         pods-state (global-pods-state-query
                     scheduler {:api-request-fn (constantly pods-response)} dummy-resource-uri)
         global-state (merge rs-state pods-state)]
     (reset! watch-state global-state))))

(defn- reset-scheduler-service-id->deployment-error!
  [{:keys [service-id->deployment-error-cache]} service-id->deployment-error]
  (doseq [[service-id _] (cu/cache->map service-id->deployment-error-cache)]
    (cu/cache-evict service-id->deployment-error-cache service-id))
  (doseq [[service-id deployment-error] service-id->deployment-error]
    (cu/cache-get-or-load service-id->deployment-error-cache service-id (constantly deployment-error))))

(deftest test-scheduler-get-services
  (let [test-cases
        [{:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "apps/v1"
           :items []}
          :expected-result []}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "apps/v1"
           :items [{:metadata {:creationTimestamp "2019-08-01T00:00:00Z"
                               :name "test-app-1234"
                               :namespace "myself"
                               :labels {:app "test-app-1234"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-1234"}
                               :annotations {:waiter/service-id "test-app-1234"}
                               :uid "test-app-1234-uid"}
                    :spec {:replicas 2
                           :selector {:matchLabels {:app "test-app-1234"
                                                    :waiter/cluster "waiter"}}
                           :template {:metadata {:annotations {:waiter/service-id "test-app-1234"}}
                                      :spec {:containers [{:name waiter-primary-container-name
                                                           :resources {:requests {:cpu "1" :memory "1Gi"}}}]}}}
                    :status {:replicas 2
                             :readyReplicas 2
                             :availableReplicas 2}}
                   {:metadata {:creationTimestamp "2019-08-05T00:00:00Z"
                               :name "test-app-6789"
                               :namespace "myself"
                               :labels {:app "test-app-6789"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-6789"}
                               :annotations {:waiter/service-id "test-app-6789"}
                               :uid "test-app-6789-uid"}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-6789"
                                                    :waiter/cluster "waiter"}}
                           :template {:metadata {:annotations {:waiter/service-id "test-app-6789"}}
                                      :spec {:containers [{:name waiter-primary-container-name
                                                           :resources {:requests {:cpu "2" :memory "512Mi"}}}]}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 2
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-1234"
                                    :instances 2
                                    :k8s/container-resources [{:cpus 1.0 :mem 1024.0 :name waiter-primary-container-name}]
                                    :k8s/containers [waiter-primary-container-name]
                                    :k8s/replicaset-creation-timestamp "2019-08-01T00:00:00.000Z"
                                    :k8s/replicaset-annotations {}
                                    :k8s/replicaset-pod-annotations {}
                                    :task-count 2
                                    :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
           (scheduler/make-Service {:id "test-app-6789"
                                    :instances 3
                                    :k8s/container-resources [{:cpus 2.0 :mem 512.0 :name waiter-primary-container-name}]
                                    :k8s/containers [waiter-primary-container-name]
                                    :k8s/replicaset-creation-timestamp "2019-08-05T00:00:00.000Z"
                                    :k8s/replicaset-annotations {}
                                    :k8s/replicaset-pod-annotations {}
                                    :task-count 3
                                    :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})]}
         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "apps/v1"
           :items [{:metadata {:creationTimestamp "2019-09-07T00:00:00Z"
                               :name "test-app-abcd"
                               :namespace "myself"
                               :labels {:app "test-app-abcd"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-abcd"}
                               :annotations {:waiter/service-id "test-app-abcd"}
                               :uid "test-app-abcd-uid"}
                    :spec {:replicas 2
                           :selector {:matchLabels {:app "test-app-abcd"
                                                    :waiter/cluster "waiter"}}
                           :template {:metadata {:annotations {:waiter/service-id "test-app-abcd"}}
                                      :spec {:containers [{:name waiter-primary-container-name
                                                           :resources {:requests {:cpu "1.5" :memory "512Mi"}}}]}}}
                    :status {:replicas 2
                             :readyReplicas 2
                             :availableReplicas 2}}
                   {:mismatched-replicaset "should be ignored"}
                   {:metadata {:creationTimestamp "2019-10-15T00:00:00Z"
                               :name "test-app-wxyz"
                               :namespace "myself"
                               :labels {:app "test-app-wxyz"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-wxyz"}
                               :annotations {:waiter/service-id "test-app-wxyz"}
                               :uid "test-app-wxyz-uid"}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-wxyz"
                                                    :waiter/cluster "waiter"}}
                           :template {:metadata {:annotations {:waiter/service-id "test-app-wxyz"}}
                                      :spec {:containers [{:name waiter-primary-container-name
                                                           :resources {:requests {:cpu "1.1" :memory "512Mi"}}}
                                                          {:name waiter-fileserver-sidecar-name
                                                           :resources {:requests {:cpu "0.1" :memory "256Mi"}}}]}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 2
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-abcd"
                                    :instances 2
                                    :k8s/container-resources [{:cpus 1.5 :mem 512.0 :name waiter-primary-container-name}]
                                    :k8s/containers [waiter-primary-container-name]
                                    :k8s/replicaset-creation-timestamp "2019-09-07T00:00:00.000Z"
                                    :k8s/replicaset-annotations {}
                                    :k8s/replicaset-pod-annotations {}
                                    :task-count 2
                                    :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
           (scheduler/make-Service {:id "test-app-wxyz"
                                    :instances 3
                                    :k8s/container-resources [{:cpus 1.1 :mem 512.0 :name waiter-primary-container-name}
                                                              {:cpus 0.1 :mem 256.0 :name waiter-fileserver-sidecar-name}]
                                    :k8s/containers [waiter-primary-container-name waiter-fileserver-sidecar-name]
                                    :k8s/replicaset-creation-timestamp "2019-10-15T00:00:00.000Z"
                                    :k8s/replicaset-annotations {}
                                    :k8s/replicaset-pod-annotations {}
                                    :task-count 3
                                    :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "apps/v1"
           :items [{:metadata {:creationTimestamp "2020-03-04T05:06:07Z"
                               :name "test-app-4321"
                               :namespace "myself"
                               :labels {:app "test-app-4321"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-4321"}
                               :annotations {:waiter/service-id "test-app-4321"}
                               :uid "test-app-4321-uid"}
                    :spec {:replicas 3
                           :selector {:matchLabels {:app "test-app-4321"
                                                    :waiter/cluster "waiter"}}
                           :template {:metadata {:annotations {:waiter/service-id "test-app-4321"}}
                                      :spec {:containers [{:name waiter-primary-container-name
                                                           :resources {:requests {:cpu "1.1" :memory "512Mi"}}}
                                                          {:name waiter-fileserver-sidecar-name
                                                           :resources {:requests {:cpu "0.1" :memory "256Mi"}}}
                                                          {:name waiter-raven-sidecar-name
                                                           :resources {:requests {:cpu "0.2" :memory "128Mi"}}}]}}}
                    :status {:replicas 3
                             :readyReplicas 1
                             :availableReplicas 1
                             :unavailableReplicas 1}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-4321"
                                    :instances 3
                                    :k8s/container-resources [{:cpus 1.1 :mem 512.0 :name waiter-primary-container-name}
                                                              {:cpus 0.1 :mem 256.0 :name waiter-fileserver-sidecar-name}
                                                              {:cpus 0.2 :mem 128.0 :name waiter-raven-sidecar-name}]
                                    :k8s/containers [waiter-primary-container-name waiter-fileserver-sidecar-name waiter-raven-sidecar-name]
                                    :k8s/replicaset-creation-timestamp "2020-03-04T05:06:07.000Z"
                                    :k8s/replicaset-annotations {}
                                    :k8s/replicaset-pod-annotations {}
                                    :task-count 3
                                    :task-stats {:running 2 :healthy 1 :unhealthy 1 :staged 1}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "apps/v1"
           :items [{:metadata {:creationTimestamp "2020-01-02T03:04:05Z"
                               :name "test-app-9999"
                               :namespace "myself"
                               :labels {:app "test-app-9999"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-9999"}
                               :annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"
                                             :waiter/service-id "test-app-9999"}
                               :uid "test-app-9999-uid"}
                    :spec {:replicas 0
                           :selector {:matchLabels {:app "test-app-9999"
                                                    :waiter/cluster "waiter"}}
                           :template {:metadata {:annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"
                                                               :waiter/service-id "test-app-9999"}}
                                      :spec {:containers [{:name waiter-primary-container-name
                                                           :resources {:requests {:cpu "1.1" :memory "512Mi"}}}]}}}
                    :status {:replicas 0
                             :readyReplicas 0
                             :availableReplicas 0}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-9999"
                                    :instances 0
                                    :k8s/container-resources [{:cpus 1.1 :mem 512.0 :name waiter-primary-container-name}]
                                    :k8s/containers [waiter-primary-container-name]
                                    :k8s/replicaset-creation-timestamp "2020-01-02T03:04:05.000Z"
                                    :k8s/replicaset-annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"}
                                    :k8s/replicaset-pod-annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"}
                                    :task-count 0
                                    :task-stats {:running 0, :healthy 0, :unhealthy 0, :staged 0}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "apps/v1"
           :items [{:metadata {:creationTimestamp "2020-01-02T03:04:05Z"
                               :name "test-app-9999"
                               :namespace "myself"
                               :labels {:app "test-app-9999"
                                        :waiter/cluster "waiter"
                                        :waiter/service-hash "test-app-9999"}
                               :annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"
                                             :waiter/revision-version "3"
                                             :waiter/service-id "test-app-9999"}
                               :uid "test-app-9999-uid"}
                    :spec {:replicas 0
                           :selector {:matchLabels {:app "test-app-9999"
                                                    :waiter/cluster "waiter"}}
                           :template {:metadata {:annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"
                                                               :waiter/revision-version "3"
                                                               :waiter/service-id "test-app-9999"}}
                                      :spec {:containers [{:name waiter-primary-container-name
                                                           :resources {:requests {:cpu "1.1" :memory "512Mi"}}}]}}}
                    :status {:replicas 0
                             :readyReplicas 0
                             :availableReplicas 0}}]}
          :expected-result
          [(scheduler/make-Service {:id "test-app-9999"
                                    :instances 0
                                    :k8s/container-resources [{:cpus 1.1 :mem 512.0 :name waiter-primary-container-name}]
                                    :k8s/containers [waiter-primary-container-name]
                                    :k8s/replicaset-creation-timestamp "2020-01-02T03:04:05.000Z"
                                    :k8s/replicaset-annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"
                                                                 :waiter/revision-version "3"}
                                    :k8s/replicaset-pod-annotations {:waiter/revision-timestamp "2020-09-22T20:22:22.000Z"
                                                                     :waiter/revision-version "3"}
                                    :task-count 0
                                    :task-stats {:running 0, :healthy 0, :unhealthy 0, :staged 0}})]}

         {:api-server-response
          {:kind "ReplicaSetList"
           :apiVersion "apps/v1"
           :items []}
          :service-id->deployment-error {"test-app-1234" "K8s API Error: something failed"}
          :expected-result
          [(scheduler/make-Service {:deployment-error "K8s API Error: something failed"
                                    :id "test-app-1234"
                                    :instances 0
                                    :task-count 0
                                    :task-stats {:running 0, :healthy 0, :unhealthy 0, :staged 0}})]}]]
    (log/info "Expecting Key error due to bad :mismatched-replicaset in API server response...")
    (doseq [{:keys [api-server-response expected-result service-id->deployment-error]} test-cases]
      (let [dummy-scheduler (make-dummy-scheduler ["test-app-1234" "test-app-6789"])
            _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
            _ (reset-scheduler-service-id->deployment-error! dummy-scheduler service-id->deployment-error)
            actual-result (->> dummy-scheduler
                               scheduler/get-services
                               sanitize-k8s-service-records)]
        (assert-data-equal expected-result actual-result)))))

(deftest test-scheduler-get-services-with-events
  (let [quota-str "memory=66282Mi, used: memory=198846Mi, limited: memory=200Gi"
        test-date (timestamp-str->datetime "2022-05-23T18:39:12Z")
        w8r-date-str (du/date-to-str test-date)
        test-event-quota {:creation-timestamp test-date
                          :message (str "Error creating: pods \"my-pod\" is forbidden: exceeded quota: pods, requested: " quota-str)
                          :reason "FailedCreate"
                          :type "Warning"}
        details-event (assoc test-event-quota :creation-timestamp w8r-date-str)
        test-cases [{:deployment-errors {}
                     :expected-service (create-empty-service 123)
                     :state-service (create-empty-service 123)
                     :test-name "No deployment errors"}
                    {:deployment-errors {}
                     :expected-service (assoc (create-empty-service 123)
                                              :deployment-error {:service-deployment-error-details {:k8s-event details-event}
                                                                 :service-deployment-error-msg (str "Could not create pod (exceeded quota in namespace test) - requested: " quota-str)
                                                                 :service-deployment-error-source "k8s-event"}
                                              :instances 1
                                              :k8s/events [test-event-quota]
                                              :k8s/namespace "test")
                     :state-service (assoc (create-empty-service 123)
                                           :instances 1
                                           :k8s/events [test-event-quota]
                                           :k8s/namespace "test")
                     :test-name "Event-based deployment error"}
                    {:deployment-errors {123 "broken"}
                     :expected-service (assoc (create-empty-service 123) :deployment-error "broken")
                     :state-service (create-empty-service 123)
                     :test-name "Cache-sourced deployment error"}
                    {:deployment-errors {123 "broken"}
                     :expected-service (assoc (create-empty-service 123)
                                              :deployment-error "broken"
                                              :instances 1
                                              :k8s/events [test-event-quota]
                                              :k8s/namespace "test")
                     :state-service (assoc (create-empty-service 123)
                                           :instances 1
                                           :k8s/events [test-event-quota]
                                           :k8s/namespace "test")
                     :test-name "Favor cache over events for deployment errors"}]]
        (doseq [{:keys [deployment-errors expected-service state-service test-name]} test-cases]
          (testing test-name
            (let [deployment-error-cache (cu/cache-factory {})]
              (doseq [[id deployment-error] deployment-errors]
                (cu/cache-put! deployment-error-cache id deployment-error))
              (let [service-id (:id state-service)
                    state (atom {:service-id->service {service-id state-service}})
                    actual-services (get-services {:service-id->deployment-error-cache deployment-error-cache
                                                   :watch-state state})]
                (is (= 1 (count actual-services)))
                (is (= expected-service (first actual-services)))))))))

(deftest test-scheduler-get-service->instances
  (let [rs-response
        {:kind "ReplicaSetList"
         :apiVersion "apps/v1"
         :items [{:metadata {:creationTimestamp "2020-01-02T03:04:05Z"
                             :name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/revision-timestamp "2020-09-22T20:33:33.000Z"
                                           :waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-uid"}
                  :spec {:replicas 2
                         :template {:metadata {:annotations {:waiter/revision-timestamp "2020-09-22T20:33:33.000Z"
                                                             :waiter/service-id "test-app-1234"}}
                                    :spec {:containers [{:name waiter-primary-container-name
                                                         :resources {:requests {:cpu "1.1" :memory "512Mi"}}}
                                                        {:name waiter-fileserver-sidecar-name
                                                         :resources {:requests {:cpu "0.1" :memory "256Mi"}}}]}}}
                  :status {:replicas 2
                           :readyReplicas 2
                           :availableReplicas 2}}
                 {:metadata {:creationTimestamp "2020-09-08T07:06:05Z"
                             :name "test-app-6789"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/service-id "test-app-6789"}
                             :uid "test-app-6789-uid"}
                  :spec {:replicas 3
                         :template {:metadata {:annotations {:waiter/service-id "test-app-6789"}}
                                    :spec {:containers [{:name waiter-primary-container-name
                                                         :resources {:requests {:cpu "0.6" :memory "400Mi"}}}
                                                        {:name waiter-fileserver-sidecar-name
                                                         :resources {:requests {:cpu "0.4" :memory "200Mi"}}}
                                                        {:name waiter-raven-sidecar-name
                                                         :resources {:requests {:cpu "0.2" :memory "100Mi"}}}]}}}
                  :status {:replicas 3
                           :readyReplicas 1
                           :availableReplicas 2
                           :unavailableReplicas 1}}]}

        pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :items [{:metadata {:name "test-app-1234-abcd0"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"
                                      :waiter/user "myself"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/revision-timestamp "2020-09-22T20:00:00.000Z"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]
                         :nodeName "node-0.k8s.com"}
                  :status {:phase "Pending"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready false
                                                :restartCount 0
                                                :state {:waiting {:reason "ContainerCreating"}}}]}}
                 {:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"
                                      :waiter/user "myself"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/revision-timestamp "2020-09-22T20:11:11.000Z"
                                           :waiter/revision-version "0"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:phase "Running"
                           :podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0
                                                :state {:running {}}}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/revision-timestamp "2020-09-22T20:22:22.000Z"
                                           :waiter/revision-version "1"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]
                         :nodeName "node-2.k8s.com"}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd3"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"
                                      :waiter/user "myself"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/revision-timestamp "2020-09-22T20:11:11.000Z"
                                           :waiter/service-id "test-app-1234"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:phase "Failed"
                           :podIP "10.141.141.13"
                           :startTime "2014-09-13T00:24:13Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0
                                                :state {:running {}}}]}}
                 {:metadata {:name "test-app-6789-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.13"
                           :startTime "2014-09-13T00:24:35Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-6789-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.14"
                           :startTime "2014-09-13T00:24:37Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :lastState {:terminated {:exitCode 255
                                                                         :reason "Error"
                                                                         :startedAt "2014-09-13T00:24:36Z"}}
                                                :restartCount 1}]}}
                 {:metadata {:name "test-app-6789-abcd3"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.15"
                           :startTime "2014-09-13T00:24:38Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-6789-abcd4"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.16"
                           :startTime "2014-09-13T00:24:48Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :restartCount 0}]
                           :initContainerStatuses [{:name "waiter-setup"
                                                    :ready false
                                                    :restartCount 200}]}}
                 {:metadata {:name "test-app-6789-abcd5"
                             :namespace "myself"
                             :labels {:app "test-app-6789"
                                      :waiter/cluster "waiter"
                                      :waiter/user "myself"
                                      :waiter/service-hash "test-app-6789"}
                             :annotations {:waiter/prepared-to-scale-down-at "2020-09-22T20:11:11.000Z"
                                           :waiter/port-count "1"
                                           :waiter/service-id "test-app-6789"}}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.16"
                           :startTime "2014-09-13T00:24:48Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :restartCount 0}]
                           :initContainerStatuses [{:name "waiter-setup"
                                                    :ready false
                                                    :restartCount 200}]}}]}

        expected (hash-map
                  (scheduler/make-Service {:id "test-app-1234"
                                           :instances 2
                                           :k8s/container-resources [{:cpus 1.1 :mem 512.0 :name waiter-primary-container-name}
                                                                     {:cpus 0.1 :mem 256.0 :name waiter-fileserver-sidecar-name}]
                                           :k8s/containers [waiter-primary-container-name waiter-fileserver-sidecar-name]
                                           :k8s/replicaset-creation-timestamp "2020-01-02T03:04:05.000Z"
                                           :k8s/replicaset-annotations {:waiter/revision-timestamp "2020-09-22T20:33:33.000Z"}
                                           :k8s/replicaset-pod-annotations {:waiter/revision-timestamp "2020-09-22T20:33:33.000Z"}
                                           :task-count 2
                                           :task-stats {:running 2, :healthy 2, :unhealthy 0, :staged 0}})
                  {:active-instances
                   [(scheduler/make-ServiceInstance
                     {:flags #{:expired}
                      :healthy? false
                      :host "0.0.0.0"
                      :id "test-app-1234.test-app-1234-abcd0-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :ready false
                                                :reason "ContainerCreating"
                                                :restart-count 0
                                                :state :waiting
                                                :type :app}]
                      :k8s/node-name "node-0.k8s.com"
                      :k8s/pod-phase "Pending"
                      :k8s/revision-timestamp "2020-09-22T20:00:00.000Z"
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-1234"
                      :started-at (du/str-to-date "2014-09-13T00:24:46Z" k8s-timestamp-format)
                      :status "Unhealthy"})
                    (scheduler/make-ServiceInstance
                     {:flags #{:expired}
                      :healthy? true
                      :host "10.141.141.11"
                      :id "test-app-1234.test-app-1234-abcd1-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restart-count 0
                                                :state :running
                                                :type :app}]
                      :k8s/pod-phase "Running"
                      :k8s/revision-timestamp "2020-09-22T20:11:11.000Z"
                      :k8s/revision-version "0"
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-1234"
                      :started-at (du/str-to-date "2014-09-13T00:24:46Z" k8s-timestamp-format)
                      :status "Healthy"})
                    (scheduler/make-ServiceInstance
                     {:flags #{:expired}
                      :healthy? true
                      :host "10.141.141.12"
                      :id "test-app-1234.test-app-1234-abcd2-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restart-count 0
                                                :type :app}]
                      :k8s/node-name "node-2.k8s.com"
                      :k8s/revision-timestamp "2020-09-22T20:22:22.000Z"
                      :k8s/revision-version "1"
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-1234"
                      :started-at (du/str-to-date "2014-09-13T00:24:47Z" k8s-timestamp-format)
                      :status "Healthy"})]
                   :failed-instances
                   [(scheduler/make-ServiceInstance
                     {:flags #{:expired}
                      :healthy? false
                      :host "10.141.141.13"
                      :id "test-app-1234.test-app-1234-abcd3-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restart-count 0
                                                :state :running
                                                :type :app}]
                      :k8s/pod-phase "Failed"
                      :k8s/revision-timestamp "2020-09-22T20:11:11.000Z"
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-1234"
                      :started-at (du/str-to-date "2014-09-13T00:24:13Z" k8s-timestamp-format)
                      :status "Failed"})]}

                  (scheduler/make-Service {:id "test-app-6789"
                                           :instances 3
                                           :k8s/container-resources [{:cpus 0.6 :mem 400.0 :name waiter-primary-container-name}
                                                                     {:cpus 0.4 :mem 200.0 :name waiter-fileserver-sidecar-name}
                                                                     {:cpus 0.2 :mem 100.0 :name waiter-raven-sidecar-name}]
                                           :k8s/containers [waiter-primary-container-name waiter-fileserver-sidecar-name waiter-raven-sidecar-name]
                                           :k8s/replicaset-creation-timestamp "2020-09-08T07:06:05.000Z"
                                           :k8s/replicaset-annotations {}
                                           :k8s/replicaset-pod-annotations {}
                                           :task-count 3
                                           :task-stats {:running 3 :healthy 1 :unhealthy 2 :staged 0}})
                  {:active-instances
                   [(scheduler/make-ServiceInstance
                     {:healthy? true
                      :host "10.141.141.13"
                      :id "test-app-6789.test-app-6789-abcd1-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restart-count 0
                                                :type :app}]
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-6789"
                      :started-at (du/str-to-date "2014-09-13T00:24:35Z" k8s-timestamp-format)
                      :status "Healthy"})
                    (scheduler/make-ServiceInstance
                     {:healthy? false
                      :host "10.141.141.14"
                      :id "test-app-6789.test-app-6789-abcd2-1"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :restart-count 1
                                                :type :app}]
                      :log-directory "/home/myself/r1"
                      :port 8080
                      :service-id "test-app-6789"
                      :started-at (du/str-to-date "2014-09-13T00:24:37Z" k8s-timestamp-format)
                      :status "Unhealthy"})
                    (scheduler/make-ServiceInstance
                     {:healthy? false
                      :host "10.141.141.15"
                      :id "test-app-6789.test-app-6789-abcd3-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :restart-count 0
                                                :type :app}]
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-6789"
                      :started-at (du/str-to-date "2014-09-13T00:24:38Z" k8s-timestamp-format)
                      :status "Unhealthy"})
                    (scheduler/make-ServiceInstance
                     {:flags #{:expired}
                      :healthy? false
                      :host "10.141.141.16"
                      :id "test-app-6789.test-app-6789-abcd4-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :restart-count 0
                                                :type :app}
                                               {:name "waiter-setup"
                                                :ready false
                                                :restart-count 200
                                                :type :init}]
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-6789"
                      :started-at (du/str-to-date "2014-09-13T00:24:48Z" k8s-timestamp-format)
                      :status "Unhealthy"})
                    (scheduler/make-ServiceInstance
                     {:flags #{:expired}
                      :healthy? false
                      :host "10.141.141.16"
                      :id "test-app-6789.test-app-6789-abcd5-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :restart-count 0
                                                :type :app}
                                               {:name "waiter-setup"
                                                :ready false
                                                :restart-count 200
                                                :type :init}]
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :prepared-to-scale-down-at (du/str-to-date "2020-09-22T20:11:11.000Z")
                      :service-id "test-app-6789"
                      :started-at (du/str-to-date "2014-09-13T00:24:48Z" k8s-timestamp-format)
                      :status "Unhealthy"})]
                   :failed-instances
                   [(scheduler/make-ServiceInstance
                     {:exit-code 255
                      :healthy? false
                      :host "10.141.141.14"
                      :id "test-app-6789.test-app-6789-abcd2-0"
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :restart-count 1
                                                :type :app}]
                      :log-directory "/home/myself/r0"
                      :port 8080
                      :service-id "test-app-6789"
                      :started-at (du/str-to-date "2014-09-13T00:24:36Z" k8s-timestamp-format)
                      :status "Failed"})]})
        watch-state-atom (atom {:service-id->service {"test-app-1234" "2020-09-22T20:33:33.000Z"}})
        dummy-scheduler (make-dummy-scheduler ["test-app-1234" "test-app-6789"]
                                              {:container-running-grace-secs 0
                                               :watch-state watch-state-atom})
        _ (reset-scheduler-watch-state! dummy-scheduler rs-response pods-response)
        actual (->> dummy-scheduler
                    get-service->instances
                    sanitize-k8s-service-records)]
    (assert-data-equal expected actual)))

(deftest test-kill-instance
  (let [service-id "test-kill-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/namespace "myself"})
        instance-id "instance-id"
        instance (scheduler/make-ServiceInstance
                  {:extra-ports []
                   :healthy? true
                   :host "10.141.141.10"
                   :id instance-id
                   :log-directory "/home/myself/r0"
                   :k8s/namespace "myself"
                   :port 8080
                   :service-id service-id
                   :started-at (du/str-to-date "2014-09-13T00:24:56Z" k8s-timestamp-format)})
        dummy-scheduler (make-dummy-scheduler [service-id])
        api-server-response {:kind "ReplicaSetList"
                             :apiVersion "apps/v1"
                             :items [{:metadata {:name service-id
                                                 :namespace "myself"
                                                 :labels {:app service-id
                                                          :waiter/cluster "waiter"
                                                          :waiter/service-hash service-id}
                                                 :annotations {:waiter/service-id service-id}
                                                 :uid (str service-id "-uid")}
                                      :spec {:replicas 1}
                                      :status {:replicas 1
                                               :readyReplicas 1
                                               :availableReplicas 1}}]}
        _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
        partial-expected {:instance-id instance-id :killed? false :service-id service-id}]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-delete"
        (let [pod-marked-for-scale-down?-atom (atom false)
              mark-pod-for-scale-down-mock (fn mark-pod-for-scale-down-fn [_ _ _]
                                             (reset! pod-marked-for-scale-down?-atom true))
              actual (with-redefs [api-request (constantly {:status "OK"})
                                   mark-pod-for-scale-down mark-pod-for-scale-down-mock]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :killed? true
                        :message "Successfully killed instance"
                        :status http-200-ok)
                 actual))
          (is (not @pod-marked-for-scale-down?-atom)
              "service is not in bypass, so pod should not be marked for two phase scale down!")))
      (testing "unsuccessful-delete: forbidden"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status http-403-forbidden})))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :message "Error while killing instance"
                        :status http-500-internal-server-error)
                 actual))))
      (testing "unsuccessful-delete: no such instance"
        (let [error-msg "Instance not found"
              actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status http-404-not-found})))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :message error-msg
                        :status http-404-not-found)
                 actual))))
      (testing "successful-delete: terminated, but had patch conflict"
        (log/info "Expecting 409 patch-conflict error when deleting service instance...")
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :patch)
                                                   (ss/throw+ {:status http-409-conflict})))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :killed? true
                        :message "Successfully killed instance"
                        :status http-200-ok)
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :message "Error while killing instance"
                        :status http-500-internal-server-error)
                 actual))))
      (testing "succesful-delete: service is in bypass, instance has not been annotated with 'prepared-to-scale-down-at'"
        (let [service-id->service-description-fn
              (constantly {"metadata" {"waiter-proxy-bypass-opt-in" "true"}})
              dummy-scheduler (assoc dummy-scheduler :service-id->service-description-fn service-id->service-description-fn)
              pod-marked-for-scale-down?-atom (atom false)
              mark-pod-for-scale-down-mock (fn mark-pod-for-scale-down-fn [_ _]
                                             (reset! pod-marked-for-scale-down?-atom true))
              instances-killed?-fn (atom false)
              hard-delete-service-instance-mock (fn hard-delete-service-instance-fn [_ _]
                                                  (reset! instances-killed?-fn true))
              actual (with-redefs [api-request (constantly {:status "OK"})
                                   mark-pod-for-scale-down mark-pod-for-scale-down-mock
                                   hard-delete-service-instance hard-delete-service-instance-mock]
                       (scheduler/kill-instance dummy-scheduler instance))]
          (is (= (assoc partial-expected
                        :killed? false
                        :message "Successfully annotated pod to be prepared for scale down"
                        :status http-200-ok)
                 actual))
          (is @pod-marked-for-scale-down?-atom)
          (is (not @instances-killed?-fn)
              "Instance should not be killed fully, that is handled by the pod cleanup daemon separately.")))

      (testing "succesful-delete: service is in bypass, instance has been annotated with 'prepared-to-scale-down-at'"
        (let [service-id->service-description-fn
              (constantly {"metadata" {"waiter-proxy-bypass-opt-in" "true"}})
              dummy-scheduler (assoc dummy-scheduler :service-id->service-description-fn service-id->service-description-fn)
              instance-prepared-to-scale-down (assoc instance :prepared-to-scale-down-at (t/now))
              pod-marked-for-scale-down?-atom (atom false)
              mark-pod-for-scale-down-mock (fn mark-pod-for-scale-down-fn [_ _]
                                             (reset! pod-marked-for-scale-down?-atom true))
              instances-killed?-fn (atom false)
              hard-delete-service-instance-mock (fn hard-delete-service-instance-fn [_ _]
                                                  (reset! instances-killed?-fn true))
              actual (with-redefs [api-request (constantly {:status "OK"})
                                   mark-pod-for-scale-down mark-pod-for-scale-down-mock
                                   hard-delete-service-instance hard-delete-service-instance-mock]
                       (scheduler/kill-instance dummy-scheduler instance-prepared-to-scale-down))]
          (is (= (assoc partial-expected
                        :killed? true
                        :message "Successfully killed instance in second phase of scale down"
                        :status http-200-ok)
                 actual))
          (is (not @pod-marked-for-scale-down?-atom))
          (is @instances-killed?-fn))))))

(deftest test-scheduler-service-exists?
  (let [service-id "test-app-1234"
        empty-response
        {:kind "ReplicaSetList"
         :apiVersion "apps/v1"
         :items []}
        non-empty-response
        {:kind "ReplicaSetList"
         :apiVersion "apps/v1"
         :items [{:metadata {:name service-id
                             :namespace "myself"
                             :labels {:app service-id
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash service-id}
                             :annotations {:waiter/service-id service-id}
                             :uid (str service-id "-uid")}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app service-id
                                                  :waiter/cluster "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 2
                           :availableReplicas 2}}]}
        test-cases [{:api-server-response nil
                     :expected-result false}
                    {:api-server-response {}
                     :expected-result false}
                    {:api-server-response empty-response
                     :expected-result false}
                    {:api-server-response non-empty-response
                     :expected-result true}]]
    (doseq [{:keys [api-server-response expected-result]} test-cases]
      (let [dummy-scheduler (make-dummy-scheduler [service-id])
            _ (reset-scheduler-watch-state! dummy-scheduler api-server-response)
            actual-result (scheduler/service-exists? dummy-scheduler service-id)]
        (is (= expected-result actual-result))))))

(defmacro assert-service-deployment-error
  [scheduler search-service-id search-deployment-error]
  `(let [scheduler# ~scheduler
         search-service-id# ~search-service-id
         search-deployment-error# ~search-deployment-error
         services# (scheduler/get-services scheduler#)]
     (is (some
          (fn [service#]
            (let [service-id# (:id service#)
                  deployment-error# (get-in service# [:deployment-error :service-deployment-error-msg])]
              (and (= service-id# search-service-id#)
                   (= deployment-error# search-deployment-error#))))
          services#))))

(deftest test-create-app
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (let [service-id "test-service-id"
          service {:service-id service-id}
          descriptor {:run-as-user-source "unknown"
                      :service-description dummy-service-description
                      :service-id service-id}
          dummy-scheduler (make-dummy-scheduler [service-id])]
      (testing "unsuccessful-create: app already exists"
        (let [actual (with-redefs [service-id->service (constantly service)]
                       (scheduler/create-service-if-new dummy-scheduler descriptor))]
          (is (nil? actual))))
      (with-redefs [config/retrieve-waiter-principal (constantly "waiter@test.com")
                    service-id->service (constantly nil)]
        (testing "unsuccessful-create: forbidden"
          (let [actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                   (ss/throw+ {:status http-403-forbidden}))]
                         (scheduler/create-service-if-new dummy-scheduler descriptor))]
            (is (nil? actual))
            (assert-service-deployment-error
             dummy-scheduler (:service-id service) (str "Unknown reason - check logs"))))
        (testing "unsuccessful-create: service creation conflict (already running)"
          (let [actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                   (ss/throw+ {:status http-409-conflict}))]
                         (scheduler/create-service-if-new dummy-scheduler descriptor))]
            (is (nil? actual))
            (assert-service-deployment-error
             dummy-scheduler (:service-id service) (str "Unknown reason - check logs"))))
        (testing "unsuccessful-create: internal error"
          (let [actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                   (throw-exception))]
                         (scheduler/create-service-if-new dummy-scheduler descriptor))]
            (is (nil? actual))
            (assert-service-deployment-error
             dummy-scheduler (:service-id service) (str "Unknown reason - check logs"))))
        (testing "unsuccessful-create: failing k8s deployment error does not persist passed cache ttl"
          (Thread/sleep 2001)
          (is (= [] (scheduler/get-services dummy-scheduler))))
        (testing "unsuccessful-create: message is propagated as deployment-error"
          (let [k8s-error-message "some error message"
                actual (with-redefs [api-request (fn mocked-api-request [& _]
                                                   (ss/throw+ {:status http-400-bad-request
                                                               :body {:message k8s-error-message}}))]
                         (scheduler/create-service-if-new dummy-scheduler descriptor))]
            (is (nil? actual))
            (assert-service-deployment-error
             dummy-scheduler (:service-id service) k8s-error-message)))

        (testing "successful create"
          (let [apis-url "https://k8s-api.example//apis"
                make-api-request (fn [api-calls-atom]
                                   (fn [url _ & {:keys [body request-method] :or {request-method :get}}]
                                     (swap! api-calls-atom conj {:kind (-> body json/read-str (get "kind"))
                                                                 :request-method request-method
                                                                 :url url})
                                     service))]

            (testing "removes previously created deployment error"
              (let [api-calls-atom (atom [])
                    actual (with-redefs [api-request (make-api-request api-calls-atom)
                                         replicaset->Service identity]
                             (scheduler/create-service-if-new dummy-scheduler descriptor))
                    api-calls @api-calls-atom
                    services (scheduler/get-services dummy-scheduler)]
                (is (= 1 (count api-calls)))
                (is (= {:kind "ReplicaSet"
                        :request-method :post
                        :url (str apis-url "/apps/v1/namespaces/waiter/replicasets")}
                       (first api-calls)))
                (is (= service actual))
                (is (= [] services))))

            (testing "without pod disruption budget"
              (let [api-calls-atom (atom [])
                    actual (with-redefs [api-request (make-api-request api-calls-atom)
                                         replicaset->Service identity]
                             (scheduler/create-service-if-new dummy-scheduler descriptor))
                    api-calls @api-calls-atom]
                (is (= 1 (count api-calls)))
                (is (= {:kind "ReplicaSet"
                        :request-method :post
                        :url (str apis-url "/apps/v1/namespaces/waiter/replicasets")}
                       (first api-calls)))
                (is (= service actual)))

              (let [descriptor (assoc-in descriptor [:service-description "min-instances"] 2)
                    api-calls-atom (atom [])
                    dummy-scheduler (make-dummy-scheduler [service-id] {:pdb-spec-builder-fn nil})
                    actual (with-redefs [api-request (make-api-request api-calls-atom)
                                         replicaset->Service identity]
                             (scheduler/create-service-if-new dummy-scheduler descriptor))
                    api-calls @api-calls-atom]
                (is (= 1 (count api-calls)))
                (is (= {:kind "ReplicaSet"
                        :request-method :post
                        :url (str apis-url "/apps/v1/namespaces/waiter/replicasets")}
                       (first api-calls)))
                (is (= service actual))))

            (testing "with pod disruption budget"
              (let [descriptor (assoc-in descriptor [:service-description "min-instances"] 2)
                    api-calls-atom (atom [])
                    actual (with-redefs [api-request (make-api-request api-calls-atom)
                                         replicaset->Service identity]
                             (scheduler/create-service-if-new dummy-scheduler descriptor))
                    api-calls @api-calls-atom]
                (is (= 2 (count api-calls)))
                (is (= {:kind "ReplicaSet"
                        :request-method :post
                        :url (str apis-url "/apps/v1/namespaces/waiter/replicasets")}
                       (first api-calls)))
                (is (= {:kind "PodDisruptionBudget"
                        :request-method :post
                        :url (str apis-url "/policy/v1beta1/namespaces/waiter/poddisruptionbudgets")}
                       (second api-calls)))
                (is (= service actual))))))))))

(deftest test-keywords-in-replicaset-spec
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")
                config/retrieve-waiter-principal (constantly "waiter@test.com")]
    (testing "namespaced keywords in annotation keys and values correctly converted"
      (let [service-id "test-service-id"
            descriptor {:run-as-user-source "unknown"
                        :service-description dummy-service-description
                        :service-id service-id}
            dummy-scheduler (-> (make-dummy-scheduler [service-id])
                                (update :replicaset-spec-builder-fn
                                        (fn [base-spec-builder-fn]
                                          (fn [scheduler service-id service-description context]
                                            (-> (base-spec-builder-fn scheduler service-id service-description context)
                                                (assoc-in [:metadata :annotations] {:waiter/x :waiter/y}))))))]
        (let [spec-json (with-redefs [api-request (fn [_ _ & {:keys [body]}] body)
                                      replicaset->Service identity]
                          (create-service descriptor dummy-scheduler))]
          (is (str/includes? spec-json "\"annotations\":{\"waiter/x\":\"waiter/y\"}")))))))

(deftest test-delete-service
  (let [service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/app-name service-id :k8s/namespace "myself"})
        dummy-scheduler (make-dummy-scheduler [service-id])]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-delete"
        (let [actual (with-redefs [api-request (constantly {:status "OK"})]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message (str "Kubernetes deleted ReplicaSet for " service-id)
                  :result :deleted}
                 actual))))
      (testing "unsuccessful-delete: forbidden"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status http-403-forbidden})))]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message "Internal error while deleting service"
                  :result :error}
                 actual))))
      (testing "unsuccessful-delete: service not found"
        (let [actual (with-redefs [api-request (fn mocked-api-request [_ _ & {:keys [request-method]}]
                                                 (when (= request-method :delete)
                                                   (ss/throw+ {:status http-404-not-found})))]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message "Kubernetes reports service does not exist"
                  :result :no-such-service-exists}
                 actual))))
      (testing "unsuccessful-delete: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/delete-service dummy-scheduler service-id))]
          (is (= {:message "Internal error while deleting service"
                  :result :error}
                 actual)))))))

(deftest test-scale-service
  (let [instances' 4
        service-id "test-service-id"
        service (scheduler/make-Service {:id service-id :instances 1 :k8s/app-name service-id :k8s/namespace "myself"})
        service-state (atom {:service-id->service {service-id service}})
        dummy-scheduler (assoc (make-dummy-scheduler [service-id]) :watch-state service-state)]
    (with-redefs [service-id->service (constantly service)]
      (testing "successful-scale"
        (with-redefs [api-request (fn [url _ & {:keys [body content-type request-method]}]
                                    (is (= "https://k8s-api.example//apis/apps/v1/namespaces/myself/replicasets/test-service-id" url))
                                    (is (= "application/json-patch+json" content-type))
                                    (is (= :patch request-method))
                                    (is (= [{"op" "test" "path" "/spec/replicas" "value" 1}
                                            {"op" "replace" "path" "/spec/replicas" "value" 4}]
                                           (json/read-str (str body))))
                                    {:status "OK"})]
          (testing "integer value"
            (let [actual (scheduler/scale-service dummy-scheduler service-id 4 false)]
              (is (= {:message "Scaled to 4" :result :scaled :status http-200-ok :success true} actual))))
          (testing "double value"
            (let [actual (scheduler/scale-service dummy-scheduler service-id 4.0 false)]
              (is (= {:message "Scaled to 4" :result :scaled :status http-200-ok :success true} actual)))
            (let [actual (scheduler/scale-service dummy-scheduler service-id 4.2 false)]
              (is (= {:message "Scaled to 4" :result :scaled :status http-200-ok :success true} actual)))
            (let [actual (scheduler/scale-service dummy-scheduler service-id 4.8 false)]
              (is (= {:message "Scaled to 4" :result :scaled :status http-200-ok :success true} actual))))))

      (testing "unsuccessful-scale: service not found"
        (let [actual (with-redefs [service-id->service (constantly nil)]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status http-404-not-found
                  :result :no-such-service-exists
                  :message "Failed to scale missing service"}
                 actual))))
      (testing "unsuccessful-scale: forbidden"
        (let [actual (with-redefs [api-request (fn [& _] (ss/throw+ {:status http-403-forbidden}))]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status http-500-internal-server-error
                  :result :failed
                  :message "Error while scaling waiter service"}
                 actual))))
      (testing "unsuccessful-scale: patch conflict"
        (let [actual (with-redefs [api-request (fn [& _] (ss/throw+ {:status http-409-conflict}))]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status http-409-conflict
                  :result :conflict
                  :message "Scaling failed due to repeated patch conflicts"}
                 actual))))
      (testing "unsuccessful-scale: internal error"
        (let [actual (with-redefs [api-request (fn [& _] (throw-exception))]
                       (scheduler/scale-service dummy-scheduler service-id instances' false))]
          (is (= {:success false
                  :status http-500-internal-server-error
                  :result :failed
                  :message "Error while scaling waiter service"}
                 actual)))))))

(deftest test-retrieve-directory-content
  ;; Killed pod
  (let [get-expected-result (fn [log-bucket-url]
                              [{:name "bar",
                                :size 4,
                                :type "file",
                                :url (str log-bucket-url "/myself/service-id/instance-id/r321/x/a/bar")}
                               {:name "baz",
                                :size 4,
                                :type "file",
                                :url (str log-bucket-url "/myself/service-id/instance-id/r321/x/a/baz")}
                               {:name "hello.txt",
                                :size 6,
                                :type "file",
                                :url (str log-bucket-url "/myself/service-id/instance-id/r321/x/a/hello.txt")}
                               {:name "aa",
                                :path "/myself/service-id/instance-id/r321/x/a/aa",
                                :type "directory"}
                               {:name "bb",
                                :path "/myself/service-id/instance-id/r321/x/a/bb",
                                :type "directory"}])]
    (let [service-id "service-id"
          instance-id "service-id.instance-id-321"
          path "/x/a"
          xml-response "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>waiter-service-logs</Name><Prefix>myself/service-id/instance-id/r321/x/a/</Prefix><Marker/><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated><Contents><Key>myself/service-id/instance-id/r321/x/a/bar</Key><LastModified>2019-01-15T16:01:46.824Z</LastModified><ETag>&quot;c157a79031e1c40f85931829bc5fc552&quot;</ETag><Size>4</Size><Owner><ID>http://acs.amazonaws.com/groups/global/AllUsers</ID><DisplayName>undefined</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>myself/service-id/instance-id/r321/x/a/baz</Key><LastModified>2019-01-15T16:01:46.995Z</LastModified><ETag>&quot;258622b1688250cb619f3c9ccaefb7eb&quot;</ETag><Size>4</Size><Owner><ID>http://acs.amazonaws.com/groups/global/AllUsers</ID><DisplayName>undefined</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>myself/service-id/instance-id/r321/x/a/hello.txt</Key><LastModified>2019-01-15T16:01:46.950Z</LastModified><ETag>&quot;b1946ac92492d2347c6235b4d2611184&quot;</ETag><Size>6</Size><Owner><ID>http://acs.amazonaws.com/groups/global/AllUsers</ID><DisplayName>undefined</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><CommonPrefixes><Prefix>myself/service-id/instance-id/r321/x/a/aa/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>myself/service-id/instance-id/r321/x/a/bb/</Prefix></CommonPrefixes></ListBucketResult>"
          host "host.local"]
      (testing "s3 logs from killed pod"
        (testing "with default log bucket url"
          (let [dummy-scheduler (make-dummy-scheduler [service-id])
                expected (get-expected-result (:log-bucket-url dummy-scheduler))]
            (let [actual (with-redefs [hu/http-request (constantly xml-response)]
                           (scheduler/retrieve-directory-content
                            dummy-scheduler service-id instance-id host path))]
              (is (= expected actual)))))

        (testing "with custom log bucket url"
          (let [log-bucket-url "http://service.example.com:1234/service-logs"
                dummy-scheduler (assoc (make-dummy-scheduler [service-id])
                                       :service-id->service-description-fn {service-id {"env" {"WAITER_CONFIG_LOG_BUCKET_URL" log-bucket-url}
                                                                                        "health-check-port-index" 0
                                                                                        "run-as-user" "myself"}})
                expected (get-expected-result log-bucket-url)]
            (let [actual (with-redefs [hu/http-request (constantly xml-response)]
                           (scheduler/retrieve-directory-content
                            dummy-scheduler service-id instance-id host path))]
              (is (= expected actual))))))))

  ;; Live pod
  (let [service-id "test-service-id"
        instance-id "test-service-id.instance-id-0"
        instance-base-dir "/r0"
        pod-name "instance-id"
        host "host.local"
        path "/some/path/"
        {:keys [watch-state] :as dummy-scheduler} (make-dummy-scheduler [service-id])
        port (get-in dummy-scheduler [:fileserver :port])
        make-file (fn [file-name size]
                    {:url (str "http://" host ":" port instance-base-dir path file-name)
                     :name file-name
                     :size size
                     :type "file"})
        make-dir (fn [dir-name]
                   {:path (str path dir-name)
                    :name dir-name
                    :type "directory"})
        strip-links (partial mapv #(dissoc % :url))
        inputs [{:description "empty directory"
                 :expected []}
                {:description "single file"
                 :expected [(make-file "foo" 1)]}
                {:description "single directory"
                 :expected [(make-dir "foo")]}
                {:description "lots of stuff"
                 :expected [(make-file "a" 10)
                            (make-file "b" 30)
                            (make-file "c" 40)
                            (make-file "d" 50)
                            (make-dir "w")
                            (make-dir "x")
                            (make-dir "y")
                            (make-dir "z")]}]]
    (swap! watch-state assoc-in [:service-id->pod-id->pod service-id pod-name] ::pod)
    (doseq [{:keys [description expected]} inputs]
      (testing description
        (let [actual (with-redefs [hu/http-request (constantly (strip-links expected))]
                       (scheduler/retrieve-directory-content
                        dummy-scheduler service-id instance-id host path))]
          (is (= expected actual)))))))

(defn test-auth-refresher
  "Test implementation of the authentication action-fn"
  [{:keys [refresh-value]}]
  {:auth-token refresh-value})

(deftest test-kubernetes-scheduler
  (let [context {:is-waiter-service?-fn (constantly nil)
                 :leader?-fn (constantly true)
                 :scheduler-name "kubernetes"
                 :scheduler-state-chan (async/chan 4)
                 :scheduler-syncer-interval-secs 5
                 :service-id->password-fn (constantly nil)
                 :service-id->service-description-fn (constantly nil)
                 :start-scheduler-syncer-fn (constantly nil)}
        custom-options {:a 1 :b "two"}
        k8s-config {:authentication nil
                    :authorizer {:kind :default
                                 :default {:factory-fn 'waiter.authorization/noop-authorizer}}
                    :cluster-name "waiter"
                    :container-running-grace-secs 120
                    :custom-options custom-options
                    :fileserver {:port 9090
                                 :scheme "http"}
                    :watch-state (atom nil)
                    :http-options {:conn-timeout 10000
                                   :socket-timeout 10000}
                    :k8s-object-key->event-cache {:threshold 5000
                                                  :ttl 60}
                    :kube-context "test-kube-ctx"
                    :log-bucket-sync-secs 60
                    :log-bucket-url nil
                    :max-patch-retries 5
                    :max-name-length 63
                    :pod-base-port 8080
                    :pod-sigkill-delay-secs 3
                    :pod-suffix-length default-pod-suffix-length
                    :replicaset-api-version "apps/v1"
                    :replicaset-spec-builder {:factory-fn 'waiter.scheduler.kubernetes/default-replicaset-builder
                                              :container-init-commands ["waiter-k8s-init"]
                                              :default-container-image "twosigma/waiter-test-apps:latest"}
                    :response->deployment-error-msg-fn 'waiter.scheduler.kubernetes/default-k8s-message-transform
                    :restart-expiry-threshold 2
                    :restart-kill-threshold 8
                    :service-id->deployment-error-cache {:threshold 5000
                                                         :ttl 60}
                    :url "http://127.0.0.1:8001"
                    :watch-init-timeout-ms 0}
        base-config (merge context k8s-config)]
    (with-redefs [start-pods-watch! (constantly nil)
                  start-replicasets-watch! (constantly nil)]
      (testing "Creating a KubernetesScheduler"

        (testing "should throw on invalid configuration"
          (testing "bad url"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url nil))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url ""))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :url "localhost")))))
          (testing "bad http options"
            (is (thrown? Throwable (kubernetes-scheduler (update-in base-config [:http-options :conn-timeout] 0))))
            (is (thrown? Throwable (kubernetes-scheduler (update-in base-config [:http-options :socket-timeout] 0)))))
          (testing "bad max conflict retries"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :max-patch-retries -1)))))
          (testing "bad max name length"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :max-name-length 0)))))
          (testing "bad PodDisruptionBudget api version"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pdb-api-version 1)))))
          (testing "bad PodDisruptionBudget spec factory function"
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:pdb-spec-builder :factory-fn] nil))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:pdb-spec-builder :factory-fn] "not a symbol"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:pdb-spec-builder :factory-fn] :not-a-symbol))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:pdb-spec-builder :factory-fn] 'not.a.namespace/not-a-fn)))))
          (testing "bad ReplicaSet spec factory function"
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] nil))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] "not a symbol"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] :not-a-symbol))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:replicaset-spec-builder :factory-fn] 'not.a.namespace/not-a-fn)))))
          (testing "bad replicaset-spec-builder fileserver predicate-fn"
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:fileserver :predicate-fn] false))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc-in base-config [:fileserver :predicate-fn]
                                                                   'waiter.scheduler.kubernetes-test/does-not-exist?)))))
          (testing "bad base port number"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port -1))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port "8080"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-base-port 1234567890)))))

          (testing "bad pod termination grace period"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs -1))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs "10"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs 1200))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :pod-sigkill-delay-secs 1234567890)))))

          (testing "bad container running grace seconds"
            (is (thrown? Throwable (kubernetes-scheduler (dissoc base-config :container-running-grace-secs))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :container-running-grace-secs -1)))))

          (testing "bad restart-expiry-threshold"
            (is (thrown? Throwable (kubernetes-scheduler (dissoc base-config :restart-expiry-threshold))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :restart-expiry-threshold 100))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :restart-expiry-threshold -1)))))

          (testing "bad restart-kill-threshold"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :restart-kill-threshold "string"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :restart-kill-threshold 1))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :restart-kill-threshold -1)))))

          (testing "bad response->deployment-error-msg-fn"
            (is (thrown? Throwable (kubernetes-scheduler (dissoc base-config :response->deployment-error-msg-fn))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :response->deployment-error-msg-fn 1))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :response->deployment-error-msg-fn "string")))))

          (testing "bad (non-matching) scheduler namespace and default-namespace"
            (let [config (-> base-config
                             (assoc :namespace "y")
                             (assoc-in [:replicaset-spec-builder :default-namespace] "x"))]
              (is (thrown? Throwable (kubernetes-scheduler config)))))

          (testing "star default-namespace"
            (let [config (assoc-in base-config [:replicaset-spec-builder :default-namespace] "*")]
              (is (instance? KubernetesScheduler (kubernetes-scheduler config))))

            (testing "with bad (non-matching) scheduler namespace"
              (let [config (-> base-config
                               (assoc :namespace "y")
                               (assoc-in [:replicaset-spec-builder :default-namespace] "*"))]
                (is (thrown? Throwable (kubernetes-scheduler config))))))

          (testing "good (non-conflicting) scheduler namespace and default-namespace"
            (is (instance? KubernetesScheduler (kubernetes-scheduler (assoc base-config :namespace "y"))))
            (is (instance? KubernetesScheduler (kubernetes-scheduler (assoc base-config :default-namespace "x"))))
            (is (instance? KubernetesScheduler (kubernetes-scheduler (assoc base-config :default-namespace "x" :namespace "x")))))

          (testing "bad service-id->deployment-error-cache-threshold"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :service-id->deployment-error-cache {:threshold 1}))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :service-id->deployment-error-cache {:ttl 1}))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :service-id->deployment-error-cache {:threshold 1
                                                                                                                 :ttl -1}))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :service-id->deployment-error-cache {:threshold -1
                                                                                                                 :ttl 1})))))

          (testing "determine-replicaset-namespace-fn"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :determine-replicaset-namespace-fn "determine-replicaset-namespace"))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :determine-replicaset-namespace-fn determine-replicaset-namespace))))
            (is (instance? KubernetesScheduler (kubernetes-scheduler (assoc base-config :determine-replicaset-namespace-fn 'waiter.scheduler.kubernetes/determine-replicaset-namespace)))))

          (testing "good restart-kill-threshold"
            (is (instance? KubernetesScheduler (kubernetes-scheduler (dissoc base-config :restart-kill-threshold))))
            (is (instance? KubernetesScheduler (kubernetes-scheduler (assoc base-config :restart-kill-threshold 2)))))

          (testing "bad k8s-object-key->event-cache-threshold"
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :k8s-object-key->event-cache {:threshold 1}))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :k8s-object-key->event-cache {:ttl 1}))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :k8s-object-key->event-cache {:threshold 1
                                                                                                          :ttl -1}))))
            (is (thrown? Throwable (kubernetes-scheduler (assoc base-config :k8s-object-key->event-cache {:threshold -1
                                                                                                          :ttl 1}))))))

        (testing "should work with valid configuration"
          (is (instance? KubernetesScheduler (kubernetes-scheduler base-config))))

        (testing "should work with valid fileserver predicate-fn?"
          (let [scheduler (kubernetes-scheduler (assoc-in base-config [:fileserver :predicate-fn]
                                                          'waiter.scheduler.kubernetes/fileserver-container-enabled?))]
            (is (instance? KubernetesScheduler scheduler))))

        (testing "should work with PodDisruptionBudget api version"
          (let [scheduler (kubernetes-scheduler (assoc base-config :pdb-api-version "/policy.pdb-v1"))]
            (is (instance? KubernetesScheduler scheduler))
            (is (= "/policy.pdb-v1" (:pdb-api-version scheduler)))
            (is (nil? (:pdb-spec-builder-fn scheduler)))))
        (testing "should work with PodDisruptionBudget spec factory function"
          (let [config (assoc-in base-config [:pdb-spec-builder :factory-fn] 'waiter.scheduler.kubernetes/default-pdb-spec-builder)
                scheduler (kubernetes-scheduler config)]
            (is (instance? KubernetesScheduler scheduler))
            (is (= "policy/v1beta1" (:pdb-api-version scheduler)))
            (is (fn? (:pdb-spec-builder-fn scheduler)))))

        (testing "should retain custom plugin options"
          (is (= custom-options (-> base-config kubernetes-scheduler :custom-options))))

        (testing "should retain kube-context option"
          (is (= "test-kube-ctx" (-> base-config kubernetes-scheduler :kube-context))))

        (testing "periodic auth-refresh task"
          (let [kill-task-fn (atom (constantly nil))
                orig-start-auth-renewer start-auth-renewer
                secret-value "secret-value"]
            (try
              (with-redefs [start-auth-renewer (fn [auth-str-atom context]
                                                 (let [{:keys [cancel-fn]} (orig-start-auth-renewer auth-str-atom context)]
                                                   (reset! kill-task-fn cancel-fn)))]
                (let [scheduler (kubernetes-scheduler (assoc base-config :authentication {:action-fn `test-auth-refresher
                                                                                          :refresh-delay-mins 1
                                                                                          :refresh-value secret-value}))]
                  (is (instance? KubernetesScheduler scheduler))
                  (is (ct/wait-for #(= secret-value (-> scheduler :auth-str-atom deref)) :interval 1 :timeout 10))))
              (finally
                (@kill-task-fn)))))

        (testing "validate service - normal"
          (scheduler/validate-service
           (kubernetes-scheduler (assoc base-config :service-id->service-description-fn (constantly {}))) nil))
        (testing "validate service - test that image can be set"
          (scheduler/validate-service
           (kubernetes-scheduler (assoc base-config
                                        :service-id->service-description-fn
                                        (constantly {"image" "twosigma/waiter-test-apps"}))) nil))))))

(defn- watch-trigger-chan->resource-version
  [watch-trigger-chan]
  (get-in (async/<!! watch-trigger-chan) [:object :object :metadata :resourceVersion]))

(deftest test-start-k8s-watch!
  (let [service-id "test-app-1234"
        rs-response
        {:kind "ReplicaSetList"
         :apiVersion "apps/v1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-uid"}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app "test-app-1234"
                                                  :waiter/cluster "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 1
                           :availableReplicas 2}}]}

        rs-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"
                              :uid "test-app-1234-uid"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 2
                            :availableReplicas 2}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"
                              :uid "test-app-1234-uid"}
                   :spec {:replicas 3
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 3
                            :readyReplicas 2
                            :availableReplicas 3}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1003"
                              :uid "test-app-1234-uid"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 1
                            :availableReplicas 2}}}]

        pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-abcd1-uid"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]
                         :nodeName "node-1.k8s.com"}
                  :status {:podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-abcd2-uid"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :restartCount 0}]}}]}

        pods-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234-abcd2"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"
                              :uid "test-app-1234-abcd2-uid"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.12"
                            :startTime "2014-09-13T00:24:47Z"
                            :containerStatuses [{:name waiter-primary-container-name
                                                 :ready true
                                                 :restartCount 0}]}}}
         {:type "ADDED"
          :object {:metadata {:name "test-app-1234-abcd3"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"
                              :uid "test-app-1234-abcd3-uid"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.13"
                            :startTime "2014-09-13T00:24:48Z"
                            :containerStatuses [{:name waiter-primary-container-name
                                                 :restartCount 0}]}}}
         {:type "DELETED"
          :object {:metadata {:name "test-app-1234-abcd1"
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1003"}}}]

        watch-update-signals (vec (repeatedly (count rs-watch-updates) promise))

        make-watch-stream (fn [updates signals]
                            (assert (== (count updates) (count signals)))
                            (map (fn [data signal] @signal data)
                                 (concat updates ["hang after last update"])
                                 (concat signals [(promise)])))

        pods-watch-stream (make-watch-stream pods-watch-updates watch-update-signals)
        rs-watch-stream (make-watch-stream rs-watch-updates watch-update-signals)

        {:keys [watch-state] :as dummy-scheduler} (make-dummy-scheduler ["test-app-1234"])
        watch-trigger-chan (au/latest-chan)

        rs-watch-thread (start-replicasets-watch!
                         dummy-scheduler
                         {:api-request-fn (constantly rs-response)
                          :exit-on-error? false
                          :streaming-api-request-fn (constantly rs-watch-stream)
                          :watch-trigger-chan watch-trigger-chan})

        pods-watch-thread (start-pods-watch!
                           dummy-scheduler
                           {:api-request-fn (constantly pods-response)
                            :exit-on-error? false
                            :streaming-api-request-fn (constantly pods-watch-stream)
                            :watch-trigger-chan watch-trigger-chan})

        get-instance (fn [{:keys [watch-state]} index]
                       (let [pod-id (str "test-app-1234-abcd" index)
                             pod (get-in @watch-state [:service-id->pod-id->pod service-id pod-id])]
                         (when pod
                           (pod->ServiceInstance dummy-scheduler pod))))
        wait-for-version (fn [version-tag value]
                           (ct/wait-for
                            #(= value
                                (get-in @watch-state [:rs-metadata :version version-tag])
                                (get-in @watch-state [:pods-metadata :version version-tag]))
                            :interval 500 :timeout 5000 :unit-multiplier 1))]

    ;; Verify base state
    (is (wait-for-version :snapshot 1000))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (some? inst2))
      (is (not (:healthy? inst2)))
      (is (nil? inst3)))

    ;; Verify state after update 1:
    ;; instance 2 should now be healthy
    (deliver (get watch-update-signals 0) true)
    (is (wait-for-version :watch 1001))
    (is (= "1001" (watch-trigger-chan->resource-version watch-trigger-chan)))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 2))
      (is (== staged unhealthy 0))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (nil? inst3)))

    ;; Verify state after update 2:
    ;; instance 3 should now be available
    (deliver (get watch-update-signals 1) true)
    (is (wait-for-version :watch 1002))
    (is (= "1002" (watch-trigger-chan->resource-version watch-trigger-chan)))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 3))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Verify state after update 3:
    ;; instance 1 should now be gone
    (deliver (get watch-update-signals 2) true)
    (is (wait-for-version :watch 1003))
    (is (= "1003" (watch-trigger-chan->resource-version watch-trigger-chan)))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (nil? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Kill the watch threads
    (.stop rs-watch-thread)
    (.stop pods-watch-thread)))

(deftest test-start-k8s-watch-with-retries
  (let [service-id "test-app-1234"

        rs-response
        {:kind "ReplicaSetList"
         :apiVersion "apps/v1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-uid"}
                  :spec {:replicas 2
                         :selector {:matchLabels {:app "test-app-1234"
                                                  :waiter/cluster "waiter"}}}
                  :status {:replicas 2
                           :readyReplicas 1
                           :availableReplicas 2}}]}

        rs-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"
                              :uid "test-app-1234-uid"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 2
                            :availableReplicas 2}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"
                              :uid "test-app-1234-uid"}
                   :spec {:replicas 3
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 3
                            :readyReplicas 2
                            :availableReplicas 3}}}
         {:type "MODIFIED"
          :object {:metadata {:name "test-app-1234"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/service-id "test-app-1234"}
                              :resourceVersion "1004"
                              :uid "test-app-1234-uid"}
                   :spec {:replicas 2
                          :selector {:matchLabels {:app "test-app-1234"
                                                   :waiter/cluster "waiter"}}}
                   :status {:replicas 2
                            :readyReplicas 1
                            :availableReplicas 2}}}]

        pods-response
        {:kind "PodList"
         :apiVersion "v1"
         :metadata {:resourceVersion "1000"}
         :items [{:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-abcd1-uid"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-abcd2-uid"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :restartCount 0}]}}]}

        pods-response'
        {:kind "PodList"
         :apiVersion "v1"
         :metadata {:resourceVersion "1003"}
         :items [{:metadata {:name "test-app-1234-abcd1"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-abcd1-uid"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.11"
                           :startTime "2014-09-13T00:24:46Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd2"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :uid "test-app-1234-abcd2-uid"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.12"
                           :startTime "2014-09-13T00:24:47Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restartCount 0}]}}
                 {:metadata {:name "test-app-1234-abcd3"
                             :namespace "myself"
                             :labels {:app "test-app-1234"
                                      :waiter/cluster "waiter"
                                      :waiter/service-hash "test-app-1234"}
                             :annotations {:waiter/port-count "1"
                                           :waiter/service-id "test-app-1234"}
                             :resourceVersion "1002"
                             :uid "test-app-1234-abcd3-uid"}
                  :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                  :status {:podIP "10.141.141.13"
                           :startTime "2014-09-13T00:24:48Z"
                           :containerStatuses [{:name waiter-primary-container-name
                                                :restartCount 0}]}}]}

        pods-watch-updates
        [{:type "MODIFIED"
          :object {:metadata {:name "test-app-1234-abcd2"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1001"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.12"
                            :startTime "2014-09-13T00:24:47Z"
                            :containerStatuses [{:name waiter-primary-container-name
                                                 :ready true
                                                 :restartCount 0}]}}}
         {:type "ADDED"
          :object {:metadata {:name "test-app-1234-abcd3"
                              :namespace "myself"
                              :labels {:app "test-app-1234"
                                       :waiter/cluster "waiter"
                                       :waiter/service-hash "test-app-1234"}
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1002"}
                   :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
                   :status {:podIP "10.141.141.13"
                            :startTime "2014-09-13T00:24:48Z"
                            :containerStatuses [{:name waiter-primary-container-name
                                                 :restartCount 0}]}}}
         {:type "DELETED"
          :object {:metadata {:name "test-app-1234-abcd1"
                              :annotations {:waiter/port-count "1"
                                            :waiter/service-id "test-app-1234"}
                              :resourceVersion "1004"}}}]

        watch-update-signals (vec (repeatedly (count rs-watch-updates) promise))

        make-watch-stream (fn [updates signals]
                            (assert (== (count updates) (count signals)))
                            (map (fn [data signal] @signal data)
                                 (concat updates ["hang after last update"])
                                 (concat signals [(promise)])))

        {:keys [watch-state] :as dummy-scheduler} (make-dummy-scheduler ["test-app-1234"])
        watch-trigger-chan (au/latest-chan)

        ;; replicasets have a single uninterrupted stream of updates
        rs-watch-stream (make-watch-stream rs-watch-updates watch-update-signals)

        rs-watch-thread (start-replicasets-watch!
                         dummy-scheduler
                         {:api-request-fn (constantly rs-response)
                          :exit-on-error? false
                          :streaming-api-request-fn (constantly rs-watch-stream)
                          :watch-trigger-chan watch-trigger-chan})

        ;; pods global queries are called twice, and return a different value each time
        pods-global-update-signal (promise)
        pods-global-query-results (.iterator
                                   (cons pods-response
                                         (lazy-seq
                                          (list (do @pods-global-update-signal
                                                    pods-response')))))
        pods-global-query-fn (fn pods-global-query-fn [& _]
                               (.next pods-global-query-results))

        ;; pods watch streams get interrupted after every update,
        ;; which lets us test the watch-retries behavior here
        pods-watch-query-count (atom 0)
        pods-watch-stream (make-watch-stream pods-watch-updates watch-update-signals)
        pods-watch-query-fn (fn pods-watch-query-fn [scheduler resource-name watch-url request-options]
                              (is (= "Pods" resource-name))
                              (is (= {:insecure? nil} request-options))
                              (swap! pods-watch-query-count inc)
                              (let [last-resource-version (->> watch-url
                                                               (re-find #"(?<=[&?]resourceVersion=)\d+")
                                                               Long/parseLong)]
                                (->> pods-watch-stream
                                     (drop-while #(-> (get-in % [:object :metadata :resourceVersion])
                                                      Long/parseLong
                                                      (<= last-resource-version)))
                                     (take 1))))

        pods-watch-thread (start-pods-watch!
                           dummy-scheduler
                           {:api-request-fn pods-global-query-fn
                            :exit-on-error? false
                            :streaming-api-request-fn pods-watch-query-fn
                            :watch-retries 1
                            :watch-trigger-chan watch-trigger-chan})

        get-instance (fn [{:keys [watch-state]} index]
                       (let [pod-id (str "test-app-1234-abcd" index)
                             pod (get-in @watch-state [:service-id->pod-id->pod service-id pod-id])]
                         (when pod
                           (pod->ServiceInstance dummy-scheduler pod))))
        wait-for-version (fn [resource version-tag value]
                           (ct/wait-for
                            #(= value
                                (get-in @watch-state [resource :version version-tag]))
                            :interval 500 :timeout 5000 :unit-multiplier 1))]

    ;; Verify base state
    (is (wait-for-version :rs-metadata :snapshot 1000))
    (is (wait-for-version :pods-metadata :snapshot 1000))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (some? inst2))
      (is (not (:healthy? inst2)))
      (is (nil? inst3)))

    ;; Verify state after update 1:
    ;; instance 2 should now be healthy
    (deliver (get watch-update-signals 0) true)
    (is (wait-for-version :rs-metadata :watch 1001))
    (is (wait-for-version :pods-metadata :watch 1001))
    (is (= "1001" (watch-trigger-chan->resource-version watch-trigger-chan)))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 2))
      (is (== staged unhealthy 0))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (nil? inst3)))

    ;; Verify state after update 2:
    ;; instance 3 should now be available
    (deliver (get watch-update-signals 1) true)
    (is (wait-for-version :rs-metadata :watch 1002))
    (is (wait-for-version :pods-metadata :watch 1002))
    (is (= "1002" (watch-trigger-chan->resource-version watch-trigger-chan)))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 3))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Verify state after second global pod query (same as update 2 state)
    (deliver pods-global-update-signal true)
    (is (wait-for-version :pods-metadata :snapshot 1003))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 2))
      (is (== running 3))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (:healthy? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    ;; Verify state after update 3:
    ;; instance 1 should now be gone
    (deliver (get watch-update-signals 2) true)
    (is (wait-for-version :rs-metadata :watch 1004))
    (is (wait-for-version :pods-metadata :watch 1004))
    (is (= "1004" (watch-trigger-chan->resource-version watch-trigger-chan)))
    (let [task-stats (get-in @watch-state [:service-id->service service-id :task-stats])
          {:keys [healthy running staged unhealthy]} task-stats
          inst1 (get-instance dummy-scheduler 1)
          inst2 (get-instance dummy-scheduler 2)
          inst3 (get-instance dummy-scheduler 3)]
      (is (== healthy 1))
      (is (== running 2))
      (is (zero? staged))
      (is (== unhealthy 1))
      (is (nil? inst1))
      (is (:healthy? inst2))
      (is (some? inst3))
      (is (not (:healthy? inst3))))

    (is (== @pods-watch-query-count 4))

    ;; Kill the watch threads
    (.stop rs-watch-thread)
    (.stop pods-watch-thread)))

(deftest test-compute-image
  (let [image-aliases {"to-resolve" "resolved"}]
    (testing "no aliases"
      (is (= "an/image" (compute-image "an/image" nil nil)))
      (is (= "an/image" (compute-image nil "an/image" nil))))
    (testing "aliases, alias found"
      (is (= "resolved" (compute-image "to-resolve" nil image-aliases)))
      (is (= "resolved" (compute-image nil "to-resolve" image-aliases))))
    (testing "aliases, alias not found"
      (is (= "an/image" (compute-image "an/image" nil image-aliases)))
      (is (= "an/image" (compute-image nil "an/image" image-aliases))))))

(deftest test-kill-restart-threshold-exceeded-pod
  (let [num-iterations 16
        expected-call-count num-iterations
        expected-pod-names (set (map #(str "pod-" %) (range num-iterations)))
        call-counter (atom 0)
        killed-pods (atom [])
        scheduler {}
        call-latch (CountDownLatch. expected-call-count)]
    (with-redefs [hard-delete-service-instance (fn [_ {:keys [k8s/pod-name]}]
                                                 (swap! call-counter inc)
                                                 (swap! killed-pods conj pod-name)
                                                 (.countDown call-latch))]
      (doseq [i (range num-iterations)]
        (let [pod-name (str "pod-" i)
              service-id (str "service-" i)
              instance {:k8s/pod-name pod-name
                        :k8s/restart-count 100
                        :service-id service-id}]
          (kill-restart-threshold-exceeded-pod scheduler instance)))
      (if (pos? expected-call-count)
        (.await call-latch)
        (Thread/sleep 10))
      (is (= expected-call-count @call-counter))
      (is (= expected-pod-names (set @killed-pods))))))

(deftest test-pod->ServiceInstance
  (let [api-server-url "https://k8s-api.example/"
        service-id "test-app-1234"
        revision-timestamp-0 "2020-09-22T20:00:00.000Z"
        revision-timestamp-1 "2020-09-22T20:11:11.000Z"
        revision-timestamp-2 "2020-09-22T20:22:22.000Z"
        watch-state-atom (atom {:service-id->service {service-id {:k8s/replicaset-annotations {:waiter/revision-timestamp revision-timestamp-1}}}})
        base-scheduler {:api-server-url api-server-url
                        :container-running-grace-secs 120
                        :restart-expiry-threshold 1000
                        :restart-kill-threshold 2000
                        :watch-state watch-state-atom}
        pod-start-time (t/minus (t/now) (t/seconds 60))
        pod-start-time-k8s-str (du/date-to-str pod-start-time k8s-timestamp-format)
        pod {:metadata {:name "test-app-1234-abcd1"
                        :namespace "myself"
                        :labels {:app service-id
                                 :waiter/cluster "waiter"
                                 :waiter/service-hash service-id}
                        :annotations {:waiter/port-count "1"
                                      :waiter/port-onto-protocol (utils/clj->json {:p1234 "http"})
                                      :waiter/revision-timestamp revision-timestamp-1
                                      :waiter/service-id service-id}}
             :spec {:containers [{:ports [{:containerPort 8080 :protocol "TCP"}]}]}
             :status {:podIP "10.141.141.11"
                      :startTime pod-start-time-k8s-str
                      :containerStatuses [{:name waiter-primary-container-name
                                           :ready true
                                           :restartCount 9}]}}
        instance-map {:exit-code nil
                      :extra-ports []
                      :flags #{}
                      :health-check-status nil
                      :healthy? true
                      :host "10.141.141.11"
                      :id "test-app-1234.test-app-1234-abcd1-9"
                      :log-directory "/home/myself/r9"
                      :message nil
                      :port 8080
                      :service-id service-id
                      :started-at (timestamp-str->datetime pod-start-time-k8s-str)
                      :k8s/api-server-url api-server-url
                      :k8s/app-name service-id
                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                :ready true
                                                :restart-count 9
                                                :type :app}]
                      :k8s/namespace "myself"
                      :k8s/pod-name "test-app-1234-abcd1"
                      :k8s/port->protocol {:p1234 "http"}
                      :k8s/raven "disabled"
                      :k8s/revision-timestamp revision-timestamp-1
                      :k8s/restart-count 9
                      :k8s/user "myself"
                      :status "Healthy"}
        ejected-instance-map (assoc instance-map :flags #{:ejected})
        expired-instance-map (assoc instance-map :flags #{:expired})
        ejected-and-expired-instance-map (assoc instance-map :flags #{:ejected :expired})
        expired-unhealthy-instance-map (assoc expired-instance-map :healthy? false :status "Unhealthy")
        rs-revision-timestamp-path [:service-id->service service-id :k8s/replicaset-annotations :waiter/revision-timestamp]]

    (testing "pod start time from main container status"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
            container-start-time (t/minus (t/now) (t/seconds 30))
            container-start-time-k8s-str (du/date-to-str container-start-time k8s-timestamp-format)
            container-last-finish-time (t/minus (t/now) (t/seconds 32))
            container-last-finish-time-k8s-str (du/date-to-str container-last-finish-time k8s-timestamp-format)]
        (let [pod' (-> pod
                       (assoc-in [:status :containerStatuses 0 :lastState] {:terminated {:finishedAt container-last-finish-time-k8s-str}})
                       (assoc-in [:status :containerStatuses 0 :state] {:running {:startedAt container-start-time-k8s-str}}))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                                :ready true
                                                                :restart-count 9
                                                                :state :running
                                                                :type :app}]
                                      :started-at (timestamp-str->datetime container-start-time-k8s-str))]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance)))
        (let [pod' (-> pod
                       (assoc-in [:status :containerStatuses 0 :state] {:running {:startedAt container-start-time-k8s-str}}))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                                :ready true
                                                                :restart-count 9
                                                                :state :running
                                                                :type :app}]
                                      :started-at (timestamp-str->datetime container-start-time-k8s-str))]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance)))
        (let [pod' (-> pod
                       (assoc-in [:status :containerStatuses 0 :lastState] {:terminated {:finishedAt container-last-finish-time-k8s-str}}))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                      :k8s/container-statuses [{:name waiter-primary-container-name
                                                                :ready true
                                                                :restart-count 9
                                                                :type :app}]
                                      :started-at (timestamp-str->datetime container-last-finish-time-k8s-str))]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance)))))

    (testing "pod init containers"
      (testing "succeeding"
        (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
              pod' (-> pod
                       (assoc-in [:status :initContainerStatuses] [{:name "waiter-setup" :ready true :restartCount 1 :state {:waiting {:reason "CrashLoopBackOff"}}}
                                                                   {:name "waiter-load" :ready true}]))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                           :k8s/container-statuses [{:name waiter-primary-container-name :ready true :restart-count 9 :type :app}
                                                                    {:name "waiter-setup" :ready true :reason "CrashLoopBackOff" :restart-count 1 :state :waiting :type :init}
                                                                    {:name "waiter-load" :ready true :type :init}])]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance))))

      (testing "failing"
        (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
              pod' (-> pod
                       (assoc-in [:status :initContainerStatuses] [{:name "waiter-setup" :ready false :restartCount 5 :state {:waiting {:reason "CrashLoopBackOff"}}}])
                       (assoc-in [:status :containerStatuses] [{:name waiter-primary-container-name :ready false}]))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                           :healthy? false
                                           :id "test-app-1234.test-app-1234-abcd1-0"
                                           :k8s/container-statuses [{:name waiter-primary-container-name :ready false :type :app}
                                                                    {:name "waiter-setup" :ready false :reason "CrashLoopBackOff" :restart-count 5 :state :waiting :type :init}]
                                           :k8s/restart-count 0
                                           :log-directory "/home/myself/r0"
                                           :status "Init container(s) in crash loop: waiter-setup")]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance)))

        (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
              pod' (-> pod
                       (assoc-in [:status :initContainerStatuses] [{:name "waiter-setup" :ready true :restartCount 1 :state {:waiting {:reason "CrashLoopBackOff"}}}
                                                                   {:name "waiter-load" :ready false :restartCount 5 :state {:waiting {:reason "CrashLoopBackOff"}}}])
                       (assoc-in [:status :containerStatuses] [{:name waiter-primary-container-name
                                                                :ready false}]))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                           :healthy? false
                                           :id "test-app-1234.test-app-1234-abcd1-0"
                                           :k8s/container-statuses [{:name waiter-primary-container-name :ready false :type :app}
                                                                    {:name "waiter-setup" :ready true :reason "CrashLoopBackOff" :restart-count 1 :state :waiting :type :init}
                                                                    {:name "waiter-load" :ready false :reason "CrashLoopBackOff" :restart-count 5 :state :waiting :type :init}]
                                           :k8s/restart-count 0
                                           :log-directory "/home/myself/r0"
                                           :status "Init container(s) in crash loop: waiter-load")]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance)))))

    (testing "pod app containers"
      (testing "app sidecar failing"
        (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
              pod' (-> pod
                       (assoc-in [:status :initContainerStatuses] [{:name "waiter-setup" :ready true}
                                                                   {:name "waiter-load" :ready true}])
                       (assoc-in [:status :containerStatuses] [{:name waiter-primary-container-name :ready true :restartCount 9}
                                                               {:name waiter-fileserver-sidecar-name :ready false :restartCount 5 :state {:waiting {:reason "CrashLoopBackOff"}}}]))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                           :k8s/container-statuses [{:name waiter-primary-container-name :ready true :restart-count 9 :type :app}
                                                                    {:name waiter-fileserver-sidecar-name :ready false :reason "CrashLoopBackOff" :restart-count 5 :state :waiting :type :app}
                                                                    {:name "waiter-setup" :ready true :type :init}
                                                                    {:name "waiter-load" :ready true :type :init}]
                                           :status (str "Application container(s) in crash loop: " waiter-fileserver-sidecar-name))]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance))))

      (testing "main container failing"
        (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
              pod' (-> pod
                       (assoc-in [:status :initContainerStatuses] [{:name "waiter-setup" :ready true}
                                                                   {:name "waiter-load" :ready true}])
                       (assoc-in [:status :containerStatuses] [{:name waiter-primary-container-name :ready false :restartCount 9 :state {:waiting {:reason "CrashLoopBackOff"}}}
                                                               {:name waiter-fileserver-sidecar-name :ready true :restartCount 5 :state {:running {}}}]))
              instance (pod->ServiceInstance dummy-scheduler pod')
              expected-instance-map (assoc instance-map
                                           :healthy? false
                                           :k8s/container-statuses [{:name waiter-primary-container-name :ready false :reason "CrashLoopBackOff" :restart-count 9 :state :waiting :type :app}
                                                                    {:name waiter-fileserver-sidecar-name :ready true :restart-count 5 :state :running :type :app}
                                                                    {:name "waiter-setup" :ready true :type :init}
                                                                    {:name "waiter-load" :ready true :type :init}]
                                           :status "Application in crash loop")]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance)))))

    (testing "pod to live instance"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
            instance (pod->ServiceInstance dummy-scheduler pod)]
        (is (= (scheduler/make-ServiceInstance instance-map) instance))))

    (testing "pod with envoy sidecar to instance"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
            pod' (-> pod
                     (assoc-in [:metadata :annotations :waiter/service-port] "8080")
                     (assoc-in [:spec :containers 0 :ports 0 :containerPort] 8081))
            instance (pod->ServiceInstance dummy-scheduler pod')]
        (is (= (scheduler/make-ServiceInstance instance-map) instance))))

    (testing "pod from scheduler with kube context to instance"
      (let [kube-context-name "kube-ctx-1"
            dummy-scheduler (assoc base-scheduler :kube-context kube-context-name)
            instance (pod->ServiceInstance dummy-scheduler pod)
            instance-map' (assoc instance-map :k8s/context kube-context-name)]
        (is (= (scheduler/make-ServiceInstance instance-map') instance))))

    (testing "pod with ejected annotation"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
            pod' (assoc-in pod [:metadata :annotations :waiter/pod-ejected] "true")
            instance (pod->ServiceInstance dummy-scheduler pod')]
        (is (= (scheduler/make-ServiceInstance ejected-instance-map) instance))))

    (testing "pod with expired annotation"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
            pod' (assoc-in pod [:metadata :annotations :waiter/pod-expired] "true")
            instance (pod->ServiceInstance dummy-scheduler pod')]
        (is (= (scheduler/make-ServiceInstance expired-instance-map) instance))))

    (testing "pod with ejected and expired annotation"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 10)
            pod' (-> pod
                     (assoc-in [:metadata :annotations :waiter/pod-ejected] "true")
                     (assoc-in [:metadata :annotations :waiter/pod-expired] "true"))
            instance (pod->ServiceInstance dummy-scheduler pod')]
        (is (= (scheduler/make-ServiceInstance ejected-and-expired-instance-map) instance))))

    (testing "pod to expired instance threshold"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 9)
            instance (pod->ServiceInstance dummy-scheduler pod)]
        (is (= (scheduler/make-ServiceInstance expired-instance-map) instance))))

    (testing "pod to expired instance exceeded threshold"
      (let [dummy-scheduler (assoc base-scheduler :restart-expiry-threshold 5)
            instance (pod->ServiceInstance dummy-scheduler pod)]
        (is (= (scheduler/make-ServiceInstance expired-instance-map) instance))))

    (testing "pod to expired unhealthy instance exceeded kill threshold"
      (let [dummy-scheduler (assoc base-scheduler
                                   :leader?-fn (constantly true)
                                   :restart-expiry-threshold 5
                                   :restart-kill-threshold 8)
            pod (assoc-in pod [:status :containerStatuses 0 :restartCount] 9)
            killed-pod-name-atom (atom nil)
            instance (with-redefs [kill-restart-threshold-exceeded-pod (fn [_ {:keys [k8s/pod-name]}]
                                                                         (reset! killed-pod-name-atom pod-name))]
                       (pod->ServiceInstance dummy-scheduler pod))
            expected-instance-map (assoc expired-unhealthy-instance-map :status "Application restarting frequently")]
        (is (= (scheduler/make-ServiceInstance expected-instance-map) instance))
        (is (= (k8s-object->id pod) @killed-pod-name-atom)))

      (let [dummy-scheduler (assoc base-scheduler
                                   :leader?-fn (constantly false)
                                   :restart-expiry-threshold 5
                                   :restart-kill-threshold 8)
            pod (assoc-in pod [:status :containerStatuses 0 :restartCount] 9)
            killed-pod-name-atom (atom nil)
            instance (with-redefs [kill-restart-threshold-exceeded-pod (fn [_ {:keys [k8s/pod-name]}]
                                                                         (reset! killed-pod-name-atom pod-name))]
                       (pod->ServiceInstance dummy-scheduler pod))
            expected-instance-map (assoc expired-unhealthy-instance-map :status "Application restarting frequently")]
        (is (= (scheduler/make-ServiceInstance expected-instance-map) instance))
        (is (nil? @killed-pod-name-atom))))

    (testing "pod to expired instance exceeded running grace period"
      (let [dummy-scheduler (assoc base-scheduler
                                   :container-running-grace-secs 45
                                   :restart-expiry-threshold 25)
            instance (pod->ServiceInstance dummy-scheduler pod)]
        (is (= (scheduler/make-ServiceInstance expired-instance-map) instance))))

    (testing "previously started pod not expired despite instance exceeded running grace period"
      (let [dummy-scheduler (assoc base-scheduler
                                   :container-running-grace-secs 45
                                   :restart-expiry-threshold 25)
            pod (assoc-in pod [:status :containerStatuses 0 :lastState] {:terminated {}})
            instance (pod->ServiceInstance dummy-scheduler pod)]
        (is (= (scheduler/make-ServiceInstance instance-map) instance))))

    (testing "revision timestamps"
      (testing "missing in both replicaset and pod"
        (let [watch-state (utils/dissoc-in @watch-state-atom rs-revision-timestamp-path)
              dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
              pod (utils/dissoc-in pod [:metadata :annotations :waiter/revision-timestamp])
              instance (pod->ServiceInstance dummy-scheduler pod)]
          (is (= (-> instance-map (dissoc :k8s/revision-timestamp) scheduler/make-ServiceInstance) instance))))

      (testing "present in pod but missing in replicaset"
        (let [watch-state (utils/dissoc-in @watch-state-atom rs-revision-timestamp-path)
              dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
              instance (pod->ServiceInstance dummy-scheduler pod)]
          (is (= (scheduler/make-ServiceInstance instance-map) instance))))

      (testing "present in replicaset but missing in pod"
        (let [watch-state (assoc-in @watch-state-atom rs-revision-timestamp-path revision-timestamp-2)
              dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
              pod (utils/dissoc-in pod [:metadata :annotations :waiter/revision-timestamp])
              instance (pod->ServiceInstance dummy-scheduler pod)]
          (is (= (-> expired-instance-map (dissoc :k8s/revision-timestamp)  scheduler/make-ServiceInstance) instance))))

      (testing "present in both replicaset and pod"
        (testing "with older value in replicaset"
          (let [watch-state (assoc-in @watch-state-atom rs-revision-timestamp-path revision-timestamp-0)
                dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                instance (pod->ServiceInstance dummy-scheduler pod)]
            (is (= (scheduler/make-ServiceInstance instance-map) instance))))

        (testing "with matching value in replicaset"
          (let [watch-state (assoc-in @watch-state-atom rs-revision-timestamp-path revision-timestamp-1)
                dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                instance (pod->ServiceInstance dummy-scheduler pod)]
            (is (= (scheduler/make-ServiceInstance instance-map) instance))))

        (testing "with expired value in pod"
          (let [watch-state (assoc-in @watch-state-atom rs-revision-timestamp-path revision-timestamp-2)
                dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                instance (pod->ServiceInstance dummy-scheduler pod)]
            (is (= (scheduler/make-ServiceInstance expired-instance-map) instance))))))

    (testing "pod with waiter/prepared-to-scale-down-at annotation"
      (testing "annotation is valid ISO-8601 string"
        (let [now (t/now)
              now-str (du/date-to-str now)
              pod' (-> pod
                       (assoc-in [:metadata :annotations :waiter/prepared-to-scale-down-at] now-str))
              instance (pod->ServiceInstance base-scheduler pod')
              expected-instance-map (assoc instance-map :prepared-to-scale-down-at now)]
          (is (= (scheduler/make-ServiceInstance expected-instance-map) instance))))

      (testing "annotation is NOT a valid ISO-8601 string so it is ignored"
        (let [pod' (-> pod
                       (assoc-in [:metadata :annotations :waiter/prepared-to-scale-down-at] "not-a-iso-8601-string"))
              instance (pod->ServiceInstance base-scheduler pod')]
          (is (= (scheduler/make-ServiceInstance instance-map) instance)))))

    (testing "match timestamps with different revision version combinations"
      (let [pod-revision-version-path [:metadata :annotations :waiter/revision-version]
            rs-revision-version-path [:service-id->service service-id :k8s/replicaset-annotations :waiter/revision-version]
            version-instance-map (fn [version] (assoc instance-map :k8s/revision-version version))
            version-expired-instance-map (fn [version] (assoc expired-instance-map :k8s/revision-version version))]
        (testing "missing in both replicaset and pod"
          (let [watch-state @watch-state-atom
                dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                instance (pod->ServiceInstance dummy-scheduler pod)]
            (is (= (scheduler/make-ServiceInstance instance-map) instance))))

        (testing "present in pod but missing in replicaset"
          (let [watch-state @watch-state-atom
                dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                pod (assoc-in pod pod-revision-version-path "0")
                instance (pod->ServiceInstance dummy-scheduler pod)]
            (is (= (scheduler/make-ServiceInstance (version-instance-map "0")) instance)))

          (testing "with negative value in pod"
            (let [watch-state @watch-state-atom
                  dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                  pod (assoc-in pod pod-revision-version-path "-1")
                  instance (pod->ServiceInstance dummy-scheduler pod)]
              (is (= (scheduler/make-ServiceInstance (version-expired-instance-map "-1")) instance)))))

        (testing "present in replicaset but missing in pod"
          (let [watch-state (assoc-in @watch-state-atom rs-revision-version-path "0")
                dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                instance (pod->ServiceInstance dummy-scheduler pod)]
            (is (= (scheduler/make-ServiceInstance instance-map) instance)))

          (testing "with positive value in replicaset"
            (let [watch-state (assoc-in @watch-state-atom rs-revision-version-path "1")
                  dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                  instance (pod->ServiceInstance dummy-scheduler pod)]
              (is (= (scheduler/make-ServiceInstance expired-instance-map) instance)))))

        (testing "present in both replicaset and pod"
          (testing "with older value in replicaset"
            (let [watch-state (assoc-in @watch-state-atom rs-revision-version-path "0")
                  dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                  pod (assoc-in pod pod-revision-version-path "1")
                  instance (pod->ServiceInstance dummy-scheduler pod)]
              (is (= (scheduler/make-ServiceInstance (version-instance-map "1")) instance))))

          (testing "with matching value in replicaset"
            (let [watch-state (assoc-in @watch-state-atom rs-revision-version-path "1")
                  dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                  pod (assoc-in pod pod-revision-version-path "1")
                  instance (pod->ServiceInstance dummy-scheduler pod)]
              (is (= (scheduler/make-ServiceInstance (version-instance-map "1")) instance))))

          (testing "with expired value in pod"
            (let [watch-state (assoc-in @watch-state-atom rs-revision-version-path "2")
                  dummy-scheduler (assoc base-scheduler :watch-state (atom watch-state))
                  pod (assoc-in pod pod-revision-version-path "1")
                  instance (pod->ServiceInstance dummy-scheduler pod)]
              (is (= (scheduler/make-ServiceInstance (version-expired-instance-map "1")) instance)))))))))

(deftest test-service-id->state
  (let [fetcher-state {:last-successful-iteration (du/date-to-str (t/now))}
        service-id "service-id"
        syncer-state-atom (atom {:last-update-time :time
                                 :service-id->health-check-context {}})
        retrieve-syncer-state-fn (partial scheduler/retrieve-syncer-state @syncer-state-atom)
        kubernetes-scheduler (make-dummy-scheduler
                              [service-id]
                              {:event-fetcher-state (atom fetcher-state)
                               :retrieve-syncer-state-fn retrieve-syncer-state-fn
                               :service-id->failed-instances-transient-store (atom {service-id [:failed-instances]})})
        supported-include-params ["auth-token-renewer" "authorizer" "event-fetcher-state" "service-id->failed-instances"
                                  "syncer" "syncer-details" "watch-state" "watch-state-details"]]
    (is (= {:failed-instances [:failed-instances]
            :syncer {:last-update-time :time}}
           (scheduler/service-id->state kubernetes-scheduler service-id)))
    (is (= {:supported-include-params supported-include-params
            :type "KubernetesScheduler"}
           (scheduler/state kubernetes-scheduler #{})))
    (is (= {:supported-include-params supported-include-params
            :syncer {:last-update-time :time}
            :type "KubernetesScheduler"}
           (scheduler/state kubernetes-scheduler #{"syncer"})))
    (is (= {:supported-include-params supported-include-params
            :syncer {:last-update-time :time
                     :service-id->health-check-context {}}
            :type "KubernetesScheduler"}
           (scheduler/state kubernetes-scheduler #{"syncer-details"})))
    (is (= {:authorizer {:type :no-op}
            :service-id->failed-instances {"service-id" [:failed-instances]}
            :supported-include-params supported-include-params
            :syncer {:last-update-time :time
                     :service-id->health-check-context {}}
            :type "KubernetesScheduler"}
           (scheduler/state kubernetes-scheduler #{"authorizer" "service-id->failed-instances" "syncer" "syncer-details"})))))

(deftest test-use-authenticated-health-checks?
  (doseq [{:keys [authenticate-health-checks? expected-result health-check-authentication]}
          [{:authenticate-health-checks? false
            :expected-result true
            :health-check-authentication "standard"}
           {:authenticate-health-checks? false
            :expected-result false
            :health-check-authentication "disabled"}
           {:authenticate-health-checks? true
            :expected-result true
            :health-check-authentication "standard"}
           {:authenticate-health-checks? true
            :expected-result true
            :health-check-authentication "disabled"}]]
    (let [service-id "test-service-id"
          service-id->service-description (constantly {"health-check-authentication" health-check-authentication})
          scheduler (make-dummy-scheduler [service-id]
                                          {:authenticate-health-checks? authenticate-health-checks?
                                           :service-id->service-description-fn service-id->service-description})]
      (is (= expected-result (scheduler/use-authenticated-health-checks? scheduler service-id))))))

(deftest test-quantity->double-value
  (is (= (Math/pow 1024 5) (quantity->double-value "1Pi")))
  (is (= (Math/pow 1024 4) (quantity->double-value "1Ti")))
  (is (= (Math/pow 1024 3) (quantity->double-value "1Gi")))
  (is (= (Math/pow 1024 2) (quantity->double-value "1Mi")))
  (is (= (Math/pow 1024 1) (quantity->double-value "1Ki")))
  (is (= 1e15 (quantity->double-value "1P")))
  (is (= 1e12 (quantity->double-value "1T")))
  (is (= 1e9 (quantity->double-value "1G")))
  (is (= 1e6 (quantity->double-value "1M")))
  (is (= 1e3 (quantity->double-value "1K")))
  (is (= 1e-2 (quantity->double-value "10m")))
  (is (= 1e0 (quantity->double-value "1"))))

(deftest test-compute-instance-usage
  (let [service-id "test-service-id"
        service-id->service-description (constantly {"cpus" 0.2 "mem" 1380})
        {:keys [watch-state] :as scheduler} (make-dummy-scheduler [service-id] {:service-id->service-description-fn service-id->service-description})]
    (swap! watch-state assoc-in [:service-id->service service-id :k8s/container-resources]
           [{:cpus 0.2 :mem 1380.0 :name waiter-primary-container-name}])
    (is (= {:cpus 0.2 :mem 1380.0 :k8s/num-containers 1}
           (scheduler/compute-instance-usage scheduler service-id)))

    (swap! watch-state assoc-in [:service-id->service service-id :k8s/container-resources]
           [{:cpus 0.2 :mem 1380.0 :name waiter-primary-container-name}
            {:cpus 0.2 :mem 100.0 :name waiter-raven-sidecar-name}])
    (is (= {:cpus 0.4 :mem 1480.0 :k8s/num-containers 2}
           (scheduler/compute-instance-usage scheduler service-id)))

    (swap! watch-state assoc-in [:service-id->service service-id :k8s/container-resources]
           [{:cpus 0.2 :mem 1380.0 :name waiter-primary-container-name}
            {:cpus 0.4 :mem 200.0 :name waiter-fileserver-sidecar-name}
            {:cpus 0.2 :mem 100.0 :name waiter-raven-sidecar-name}])
    (is (= {:cpus 0.8 :mem 1680.0 :k8s/num-containers 3}
           (scheduler/compute-instance-usage scheduler service-id)))))

(deftest test-k8s-event->simple-event
  (let [message "Error: cannot find volume"
        reason "Failed"
        timestamp-str "2014-09-13T00:24:56Z"
        type "Warning"]
    (is (= {:creation-timestamp (du/str-to-date timestamp-str k8s-timestamp-format)
            :message message
            :reason reason
            :type type}
           (k8s-event->simple-event {:metadata {:creationTimestamp timestamp-str :name "w8r-myreplicaset-mypod.12345"}
                                     :message message :name "w8r-myreplicaset-mypod" :reason reason :type type})))
    (is (thrown-with-msg? IllegalArgumentException #"Invalid format: \"nope\""
                          (k8s-event->simple-event {:metadata {:creationTimestamp "nope" :name "w8r-myreplicaset-mypod.12345"}
                                                    :message message :name "w8r-myreplicaset-mypod" :reason reason :type type})))
    (is (= {:creation-timestamp nil :message nil :reason nil :type nil}
           (k8s-event->simple-event {:metadata {:name "w8r-myreplicaset-mypod.12345"} :name "w8r-myreplicaset-mypod"})))))

(deftest test-get-cached-events-chan
  (let [cache (cu/cache-factory {})
        fetched-value {:v "fetched-value"}
        fetched-promise (async/promise-chan)
        fetch-fn (constantly fetched-promise)
        known-namespace "known-namespace"
        known-value {:v "known-value"}
        known-object "known-object"
        unknown-namespace "unknown-namespace"
        unknown-object "unknown-object"]
    (async/>!! fetched-promise fetched-value)
    (cu/cache-put! cache {:namespace known-namespace :k8s-object-name known-object} known-value)
    (let [create-key (fn [namespace object-name] {:namespace namespace :k8s-object-name object-name})]
      (let [actual-value (async/<!! (get-cached-events-chan cache (create-key known-namespace known-object) fetch-fn {} {}))]
        (is (true? (:w8r-cached actual-value)))
        (is (= known-value (dissoc actual-value :w8r-cached))))
      (is (= fetched-value (async/<!! (get-cached-events-chan cache (create-key known-namespace unknown-object) fetch-fn {} {}))))
      (is (= fetched-value (async/<!! (get-cached-events-chan cache (create-key unknown-namespace known-object) fetch-fn {} {}))))
      (is (= fetched-value (async/<!! (get-cached-events-chan cache (create-key unknown-namespace unknown-object) fetch-fn {} {})))))))

(deftest test-service-unhealthy?
  (is (true? (service-unhealthy? {:instances 1 :task-count 0 :task-stats {:healthy 0}} 30)))
  (is (true? (service-unhealthy? {:k8s/replicaset-creation-timestamp (du/date-to-str (t/minus (t/now) (t/seconds 60)))
                                  :instances 1 :task-count 0 :task-stats {:healthy 0}} 30)))
  (is (false? (service-unhealthy? {:k8s/replicaset-creation-timestamp (du/date-to-str (t/now))
                                   :instances 1 :task-count 0 :task-stats {:healthy 0}} 30)))
  (is (false? (service-unhealthy? {:instances 0 :task-count 0 :task-stats {:healthy 0}} 30)))
  (is (false? (service-unhealthy? {:instances 1 :task-count 2 :task-stats {:healthy 2}} 30)))
  (is (true? (service-unhealthy? {:instances 1 :task-count 1 :task-stats {:healthy 0}} 30)))
  (is (true? (service-unhealthy? {:k8s/replicaset-creation-timestamp "1978-06-22T11:00:00.000Z"
                                  :instances 1 :task-count 1 :task-stats {:healthy 0}} 30)))
  (is (false? (service-unhealthy? {:k8s/replicaset-creation-timestamp (du/date-to-str (t/now))
                                   :instances 1 :task-count 1 :task-stats {:healthy 0}} 30)))
  (is (false? (service-unhealthy? {:instances 1 :task-count 1 :task-stats {:healthy 1}} 30)))
  (is (false? (service-unhealthy? {:instances 1 :task-count 1 :task-stats {:healthy 2}} 30)))
  (is (false? (service-unhealthy? {:k8s/replicaset-creation-timestamp (du/date-to-str (t/now))
                                   :instances 1 :task-count 1 :task-stats {:healthy 2}} 30))))

(defn- make-pod-with-statuses
  ([statuses]
   (make-pod-with-statuses statuses "1978-06-22T11:00:00Z"))
  ([statuses creation-timestamp-str]
   (let [condition-statuses (if (nil? statuses)
                              nil
                              (vec (map (fn [v] {:status v}) statuses)))]
     {:metadata {:creationTimestamp creation-timestamp-str} :status {:conditions condition-statuses}})))

(deftest test-pod-unhealthy?
  (is (true? (pod-unhealthy? (make-pod-with-statuses nil) 30)))
  (is (false? (pod-unhealthy? (make-pod-with-statuses nil (du/date-to-str (t/now) k8s-timestamp-format)) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses []) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses [nil]) 30)))
  (is (false? (pod-unhealthy? (make-pod-with-statuses ["True"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["False"]) 30)))
  (is (false? (pod-unhealthy? (make-pod-with-statuses ["False"] (du/date-to-str (t/now) k8s-timestamp-format)) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["Unknown"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["False" "False"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["False" "Unknown"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["True" "False"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["False" "True"]) 30)))
  (is (false? (pod-unhealthy? (make-pod-with-statuses ["True" "True"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["True" "False" "Unknown"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["True" "Unknown" "True"]) 30)))
  (is (true? (pod-unhealthy? (make-pod-with-statuses ["False" "True" "True"]) 30))))

(deftest test-start-event-fetcher!
  (let [event-fetcher-state (atom {})
        fetch-events-chan (au/latest-chan)
        namespace "myself"
        service-a-name "service-a"
        service-a-id (str service-a-name "-id")
        service-b-name "service-b"
        service-b-id (str service-b-name "-id")
        service-a (scheduler/make-Service {:id service-a-id :instances 1 :task-count 0 :k8s/app-name service-a-name :k8s/namespace namespace})
        service-b (scheduler/make-Service {:id service-b-id :instances 1 :task-count 0 :k8s/app-name service-b-name :k8s/namespace namespace})
        timestamp-str "2014-09-13T00:24:56Z"
        object-name "w8r-realwork"
        initial-state {:service-id->service {service-a-id service-a service-b-id service-b}}
        simple-event {:creation-timestamp (du/str-to-date timestamp-str k8s-timestamp-format)
                      :message "quota probs"
                      :reason "FailedCreate"
                      :type "Warning"}
        k8s-response {:items [{:metadata {:creationTimestamp timestamp-str :name (str object-name ".123")}
                               :involvedObject {:kind "ReplicaSet" :namespace namespace :name object-name}
                               :message (:message simple-event) :reason (:reason simple-event) :type (:type simple-event)}]
                      :metadata {:resourceVersion (str 123)}}
        k8s-response-chan (async/chan 1)
        options {:api-request-fn (constantly k8s-response-chan)
                 :fetch-events-chan fetch-events-chan}
        {:keys [watch-state] :as dummy-scheduler} (make-dummy-scheduler ["test-app-1234"])]
    ;; Set an initial state
    (async/put! k8s-response-chan k8s-response)
    (swap! watch-state (constantly initial-state))
    (is (nil? (-> @watch-state :service-id->service (get service-a-id) :k8s/events)))
    (is (false? (cu/cache-contains? (:k8s-object-key->event-cache dummy-scheduler) {:namespace namespace :k8s-object-name object-name})))
    (with-redefs [service-unhealthy? (fn [{:keys [id]} _] (= service-a-id id))
                  pod-unhealthy? (constantly false)]
      ;; Start fetcher go block
      (start-event-fetcher! dummy-scheduler options)
      ;; Wait for an update, check it
      (let [{:keys [events object-id]} (async/<!! fetch-events-chan)]
        (is (= service-a-id object-id))
        (is (= 1 (count events)))
        (is (= simple-event (first events)))
        (is (true? (cu/cache-contains? (:k8s-object-key->event-cache dummy-scheduler) {:namespace namespace :k8s-object-name service-a-name})))))))
