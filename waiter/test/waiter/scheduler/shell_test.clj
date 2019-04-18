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
(ns waiter.scheduler.shell-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [qbits.jet.client.http :as http]
            [waiter.config :as config]
            [waiter.scheduler :as scheduler]
            [waiter.scheduler.shell :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import clojure.lang.ExceptionInfo
           java.io.File
           java.util.concurrent.TimeUnit
           waiter.scheduler.shell.ShellScheduler))

(defn work-dir
  "Returns the canonical path for the ./scheduler directory"
  []
  (-> "./scheduler" (io/file) (.getCanonicalPath)))

(defn- create-test-service
  "Creates a new (test) service with command `ls` and the given service-id"
  ([scheduler service-id]
   (create-test-service scheduler service-id {}))
  ([scheduler service-id custom-service-description]
   (let [descriptor {:service-description (merge {"backend-proto" "http"
                                                  "cmd" "ls"
                                                  "concurrency-level" 1
                                                  "cpus" 2
                                                  "grace-period-secs" 10
                                                  "health-check-port-index" 0
                                                  "health-check-url" "/health-check-url"
                                                  "mem" 32
                                                  "ports" 1}
                                                 custom-service-description)
                     :service-id service-id}]
     (scheduler/create-service-if-new scheduler descriptor))))

(defn- task-stats
  "Gets the task-stats for the first service in the given scheduler"
  [scheduler]
  (-> scheduler
      :id->service-agent
      get-service->instances
      keys
      first
      :task-stats))

(defn- ensure-agent-finished
  "Awaits the scheduler's agent"
  [{:keys [id->service-agent]}]
  (await id->service-agent))

(defn- force-update-service-health
  "Forces a call to update-service-health"
  [{:keys [id->service-agent port->reservation-atom scheduler-name] :as scheduler} {:keys [port-grace-period-ms]}]
  (send id->service-agent update-service-health scheduler-name port->reservation-atom port-grace-period-ms nil)
  (ensure-agent-finished scheduler))

(defn- force-maintain-instance-scale
  "Forces a call to maintain-instance-scale"
  [{:keys [id->service-agent port->reservation-atom port-range scheduler-name] :as scheduler}]
  (send id->service-agent maintain-instance-scale scheduler-name port->reservation-atom port-range)
  (ensure-agent-finished scheduler))

(deftest test-update-task-stats
  (testing "1 healthy"
    (let [{:keys [service]} (update-task-stats {:service {}
                                                :id->instance {"foo" {:healthy? true}}})]
      (is (= {:healthy 1
              :unhealthy 0
              :running 1
              :staged 0} (:task-stats service)))
      (is (= 1 (:task-count service)))))
  (testing "1 unhealthy"
    (let [{:keys [service]} (update-task-stats {:service {}
                                                :id->instance {"foo" {:healthy? false}}})]
      (is (= {:healthy 0
              :unhealthy 1
              :running 1
              :staged 0} (:task-stats service)))
      (is (= 1 (:task-count service)))))
  (testing "1 healthy, 1 unhealthy"
    (let [{:keys [service]} (update-task-stats {:service {}
                                                :id->instance {"foo" {:healthy? true}
                                                               "bar" {:healthy? false}}})]
      (is (= {:healthy 1
              :unhealthy 1
              :running 2
              :staged 0} (:task-stats service)))
      (testing "1 healthy, 1 unhealthy, 1 killed"
        (let [{:keys [service]} (update-task-stats {:service {}
                                                    :id->instance {"foo" {:healthy? true}
                                                                   "bar" {:healthy? false}
                                                                   "baz" {:healthy? false :killed? true}}})]
          (is (= {:healthy 1
                  :unhealthy 1
                  :running 2
                  :staged 0} (:task-stats service)))
          (is (= 2 (:task-count service)))))
      (is (= 2 (:task-count service))))))

(deftest test-launch-instance
  (testing "Launching an instance"
    (testing "should throw if cmd is nil"
      (is (thrown-with-msg? ExceptionInfo #"The command to run was not supplied"
                            (launch-instance "foo" {"backend-proto" "http" "ports" 1 "mem" 32} (work-dir) {} nil nil))))

    (testing "should throw if enough ports aren't available"
      (is (thrown-with-msg? ExceptionInfo #"Unable to reserve 4 ports"
                            (launch-instance "bar" {"backend-proto" "http" "cmd" "echo 1" "ports" 4 "mem" 32} (work-dir) {} (atom {}) [5100 5102]))))

    (testing "with multiple ports"
      (let [num-ports 8
            port-range-start 5100
            port-range [port-range-start (+ port-range-start 100)]]
        (with-redefs [launch-process (fn [_ _ command environment]
                                       (is (= "dummy-command" command))
                                       (is (every? #(contains? environment (str "PORT" %)) (range num-ports))))]
          (let [service-description {"backend-proto" "http"
                                     "cmd" "dummy-command"
                                     "health-check-port-index" 0
                                     "mem" 32
                                     "ports" num-ports}
                {:keys [extra-ports port]} (launch-instance "baz" service-description (work-dir) {} (atom {}) port-range)]
            (is (= port-range-start port))
            (is (= (map #(+ % port-range-start) (range 1 num-ports)) extra-ports))))))

    (testing "with custom health check index"
      (let [num-ports 8
            port-range-start 5100
            port-range [port-range-start (+ port-range-start 100)]]
        (with-redefs [launch-process (fn [_ _ command environment]
                                       (is (= "dummy-command" command))
                                       (is (every? #(contains? environment (str "PORT" %)) (range num-ports))))]
          (let [service-description {"backend-proto" "http"
                                     "cmd" "dummy-command"
                                     "health-check-port-index" 3
                                     "mem" 32
                                     "ports" num-ports}
                {:keys [extra-ports port]} (launch-instance "baz" service-description (work-dir) {} (atom {}) port-range)]
            (is (= port-range-start port))
            (is (= (map #(+ % port-range-start) (range 1 num-ports)) extra-ports))))))))

(deftest test-directory-content
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [service-description {"backend-proto" "http"
                               "cmd" "echo Hello, World!"
                               "cpus" 4
                               "mem" 256
                               "ports" 1}
          id->service (create-service {} "foo" service-description (constantly "password")
                                      (work-dir) (atom {}) [0 0] (promise))
          service-entry (get id->service "foo")
          [instance-id instance] (-> service-entry :id->instance first)
          content (directory-content service-entry instance-id "")]
      (.waitFor (:shell-scheduler/process instance) 100 TimeUnit/MILLISECONDS)
      (is (= 2 (count content)))
      (is (some #(= "stdout" (:name %)) content))
      (is (some #(= "stderr" (:name %)) content))
      (is (= "Hello, World!\n" (slurp (:url (first (filter #(= "stdout" (:name %)) content)))))))))

(deftest test-pid
  (testing "Getting the pid of a Process"

    (testing "should return nil if it is not a UNIXProcess"
      (is (nil? (pid (proxy [Process] [])))))

    (testing "should return nil if it catches an Exception"
      (with-redefs [type (fn [_] (throw (ex-info "ERROR!" {})))]
        (is (nil? (pid (:process (launch-process "foo" (work-dir) "ls" {})))))))))

(defn common-scheduler-config
  []
  {:failed-instance-retry-interval-ms 500
   :health-check-interval-ms 500
   :health-check-timeout-ms 1
   :id->service-agent (agent {})
   :port-grace-period-ms 1
   :port-range [10000 11000]
   :retrieve-syncer-state-fn (fn default-retrieve-syncer-state-fn
                               ([] {:syncer-state :global})
                               ([service-id] {:syncer-state service-id}))
   :scheduler-name "shell"
   :service-id->password-fn (fn [service-id] (str service-id ".password"))
   :work-directory (work-dir)})

(deftest test-shell-scheduler
  (testing "Creating a new ShellScheduler"
    (let [valid-config (common-scheduler-config)]

      (testing "should work with valid configuration"
        (is (instance? ShellScheduler (create-shell-scheduler valid-config))))

      (testing "should throw if port range is bogus"
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :port-range [nil nil]))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :port-range [0 nil]))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :port-range [10000 nil]))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :port-range [10000 0]))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :port-range [10000 9999])))))

      (testing "should throw if other config fields are bogus"
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :health-check-timeout-ms 0))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :port-grace-period-ms 0))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :failed-instance-retry-interval-ms 0))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :health-check-interval-ms 0))))
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :work-directory "")))))

      (testing "validate service - normal"
        (scheduler/validate-service
          (create-shell-scheduler (assoc valid-config :service-id->service-description-fn (constantly {}))) nil))
      (testing "validate service - test that image can't be set"
        (is (thrown? Throwable (scheduler/validate-service
                                 (create-shell-scheduler (assoc valid-config
                                                           :service-id->service-description-fn
                                                           (constantly {"image" "twosigma/waiter-test-apps"}))) nil)))))))

(deftest test-reserve-port!
  (testing "Reserving a port"

    (testing "should return the lowest available port in range"
      (with-redefs [utils/port-available? #(= 12345 %)]
        (is (= 12345 (reserve-port! (atom {}) [10000 13000])))))

    (testing "should use :state :in-use to signify a port that is in use"
      (let [port->reservation-atom (atom {})]
        (with-redefs [utils/port-available? (constantly true)]
          (reserve-port! port->reservation-atom [10000 10000]))
        (is (= {10000 {:state :in-use
                       :expiry-time nil}}
               @port->reservation-atom))))))

(deftest test-create-service-if-new
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler (create-shell-scheduler (common-scheduler-config))]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo")))
      (ensure-agent-finished scheduler)
      (is (= {:success false, :result :already-exists, :message "foo already exists!"}
             (create-test-service scheduler "foo")))
      (ensure-agent-finished scheduler)
      (is (= {:success true, :result :deleted, :message "Deleted foo"}
             (scheduler/delete-service scheduler "foo"))))))

(deftest test-delete-service
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler (create-shell-scheduler (common-scheduler-config))]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo")))
      (ensure-agent-finished scheduler)
      (is (scheduler/service-exists? scheduler "foo"))
      (is (= {:success true, :result :deleted, :message "Deleted foo"}
             (scheduler/delete-service scheduler "foo")))
      (ensure-agent-finished scheduler)
      (is (not (scheduler/service-exists? scheduler "foo"))))))

(deftest test-scale-service
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler-config (common-scheduler-config)
          scheduler (create-shell-scheduler scheduler-config)]
      ;; Bogus service
      (is (= {:success false, :result :no-such-service-exists, :message "bar does not exist!"}
             (scheduler/scale-service scheduler "bar" 2 false)))
      (with-redefs [perform-health-check (constantly true)]
        ;; Create service, instances: 1
        (is (= {:success true, :result :created, :message "Created foo"}
               (create-test-service scheduler "foo" {"cmd" "sleep 10000"})))
        (ensure-agent-finished scheduler)
        (is (scheduler/service-exists? scheduler "foo"))
        ;; Scale up, instances: 2
        (is (= {:success true, :result :scaled, :message "Scaled foo"}
               (scheduler/scale-service scheduler "foo" 2 false)))
        (force-maintain-instance-scale scheduler)
        (force-update-service-health scheduler scheduler-config)
        (is (= {:running 2, :healthy 2, :unhealthy 0, :staged 0}
               (task-stats scheduler)))
        ;; No need to scale down, instances: 2
        (is (= {:success false, :result :scaling-not-needed, :message "Unable to scale foo"}
               (scheduler/scale-service scheduler "foo" 1 false)))
        (ensure-agent-finished scheduler)
        ;; Successfully kill one instance, instances: 1
        (let [instance (-> (scheduler/service-id->state scheduler "foo") :id->instance vals first)]
          (is (= {:killed? true, :success true, :result :deleted, :message (str "Deleted " (:id instance))}
                 (scheduler/kill-instance scheduler instance))))
        (force-update-service-health scheduler scheduler-config)
        (is (= {:running 1, :healthy 1, :unhealthy 0, :staged 0}
               (task-stats scheduler)))
        ;; Scale up, instances: 2
        (is (= {:success true, :result :scaled, :message "Scaled foo"}
               (scheduler/scale-service scheduler "foo" 2 false)))
        (force-maintain-instance-scale scheduler)
        (force-update-service-health scheduler scheduler-config)
        (is (= {:running 2, :healthy 2, :unhealthy 0, :staged 0}
               (task-stats scheduler)))))))

(deftest test-kill-instance
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler (create-shell-scheduler (common-scheduler-config))]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo")))
      (ensure-agent-finished scheduler)
      (with-redefs [perform-health-check (constantly true)]
        (let [instance (-> (scheduler/service-id->state scheduler "foo") :id->instance vals first)]
          (is (= {:killed? true, :success true, :result :deleted, :message (str "Deleted " (:id instance))}
                 (scheduler/kill-instance scheduler instance)))
          (is (= {:success true, :result :deleted, :message "Deleted foo"}
                 (scheduler/delete-service scheduler "foo")))
          (ensure-agent-finished scheduler)
          (is (= {:success false, :result :no-such-service-exists, :message "foo does not exist!"}
                 (scheduler/kill-instance scheduler instance))))))))

(deftest test-kill-process
  (testing "Killing a process"
    (testing "should handle exceptions"
      (let [instance (launch-instance "foo" {"backend-proto" "http" "cmd" "ls" "ports" 1 "mem" 32} (work-dir) {} (atom {}) [0 0])]
        (with-redefs [t/plus (fn [_ _] (throw (ex-info "ERROR!" {})))]
          (kill-process! instance (atom {}) 0))))

    (testing "should release ports"
      (let [port-reservation-atom (atom {})
            num-ports 5
            port-range-start 2000
            instance (launch-instance "bar" {"backend-proto" "http" "cmd" "ls" "ports" num-ports "mem" 32} (work-dir) {} port-reservation-atom [port-range-start 3000])]
        (is (= num-ports (count @port-reservation-atom)))
        (is (= (repeat num-ports {:state :in-use, :expiry-time nil})
               (vals @port-reservation-atom)))
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] current-time)]
            (kill-process! instance port-reservation-atom 0)
            (is (= num-ports (count @port-reservation-atom)))
            (is (= (repeat num-ports {:state :in-grace-period-until-expiry, :expiry-time current-time})
                   (vals @port-reservation-atom)))))))))

(deftest test-health-check-instances
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler-config (common-scheduler-config)
          scheduler (create-shell-scheduler scheduler-config)
          health-check-count-atom (atom 0)]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo" {"cmd" "sleep 100000"})))
      (ensure-agent-finished scheduler)
      (with-redefs [http/get (fn [_ _]
                               (swap! health-check-count-atom inc)
                               (let [c (async/chan)]
                                 (async/go (async/>! c {:status 200}))
                                 c))]
        (let [instance (-> (scheduler/service-id->state scheduler "foo") :id->instance vals first)]
          ;; We force a health check to occur
          (force-update-service-health scheduler scheduler-config)
          (is (= 1 @health-check-count-atom))
          ;; Kill the single instance of our service
          (is (= {:killed? true, :success true, :result :deleted, :message (str "Deleted " (:id instance))}
                 (scheduler/kill-instance scheduler instance))))
        (ensure-agent-finished scheduler)
        ;; Force another health check attempt
        (force-update-service-health scheduler scheduler-config)
        ;; But, the health check shouldn't happen because the instance is killed
        (is (= 1 @health-check-count-atom))))))

(deftest test-should-update-task-stats-in-service
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler-config (common-scheduler-config)
          scheduler (create-shell-scheduler scheduler-config)]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo" {"cmd" "sleep 10000"})))
      (with-redefs [perform-health-check (constantly true)]
        (force-update-service-health scheduler scheduler-config)
        (is (= {:running 1, :healthy 1, :unhealthy 0, :staged 0}
               (task-stats scheduler))))
      (with-redefs [perform-health-check (constantly false)]
        (force-update-service-health scheduler scheduler-config)
        (is (= {:running 1, :healthy 0, :unhealthy 1, :staged 0}
               (task-stats scheduler))))
      (is (= {:success true, :result :deleted, :message "Deleted foo"}
             (scheduler/delete-service scheduler "foo"))))))

(deftest test-get-services
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler-config (common-scheduler-config)
          scheduler (create-shell-scheduler scheduler-config)]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo" {"cmd" "sleep 10000"})))
      (is (= {:success true, :result :created, :message "Created bar"}
             (create-test-service scheduler "bar" {"cmd" "sleep 10000"})))
      (is (= {:success true, :result :created, :message "Created baz"}
             (create-test-service scheduler "baz" {"cmd" "sleep 10000"})))
      (ensure-agent-finished scheduler)
      (is (= (map scheduler/map->Service
                  [{:id "foo"
                    :instances 1
                    :task-count 1
                    :task-stats {:running 1, :healthy 0, :unhealthy 1, :staged 0}
                    :environment {"HOME" (work-dir)
                                  "LOGNAME" nil
                                  "USER" nil
                                  "WAITER_CLUSTER" "test-cluster"
                                  "WAITER_CONCURRENCY_LEVEL" "1"
                                  "WAITER_CPUS" "2"
                                  "WAITER_MEM_MB" "32"
                                  "WAITER_PASSWORD" "foo.password"
                                  "WAITER_SERVICE_ID" "foo"
                                  "WAITER_USERNAME" "waiter"}
                    :service-description {"backend-proto" "http"
                                          "cmd" "sleep 10000"
                                          "concurrency-level" 1
                                          "cpus" 2
                                          "grace-period-secs" 10
                                          "health-check-port-index" 0
                                          "health-check-url" "/health-check-url"
                                          "mem" 32
                                          "ports" 1}
                    :shell-scheduler/mem 32}
                   {:id "bar"
                    :instances 1
                    :task-count 1
                    :task-stats {:running 1, :healthy 0, :unhealthy 1, :staged 0}
                    :environment {"HOME" (work-dir)
                                  "LOGNAME" nil
                                  "USER" nil
                                  "WAITER_CLUSTER" "test-cluster"
                                  "WAITER_CONCURRENCY_LEVEL" "1"
                                  "WAITER_CPUS" "2"
                                  "WAITER_MEM_MB" "32"
                                  "WAITER_PASSWORD" "bar.password"
                                  "WAITER_SERVICE_ID" "bar"
                                  "WAITER_USERNAME" "waiter"}
                    :service-description {"backend-proto" "http"
                                          "cmd" "sleep 10000"
                                          "concurrency-level" 1
                                          "cpus" 2
                                          "grace-period-secs" 10
                                          "health-check-port-index" 0
                                          "health-check-url" "/health-check-url"
                                          "mem" 32
                                          "ports" 1}
                    :shell-scheduler/mem 32}
                   {:id "baz"
                    :instances 1
                    :task-count 1
                    :task-stats {:running 1, :healthy 0, :unhealthy 1, :staged 0}
                    :environment {"HOME" (work-dir)
                                  "LOGNAME" nil
                                  "USER" nil
                                  "WAITER_CLUSTER" "test-cluster"
                                  "WAITER_CONCURRENCY_LEVEL" "1"
                                  "WAITER_CPUS" "2"
                                  "WAITER_MEM_MB" "32"
                                  "WAITER_PASSWORD" "baz.password"
                                  "WAITER_SERVICE_ID" "baz"
                                  "WAITER_USERNAME" "waiter"}
                    :service-description {"backend-proto" "http"
                                          "cmd" "sleep 10000"
                                          "concurrency-level" 1
                                          "cpus" 2
                                          "grace-period-secs" 10
                                          "health-check-port-index" 0
                                          "health-check-url" "/health-check-url"
                                          "mem" 32
                                          "ports" 1}
                    :shell-scheduler/mem 32}])
             (scheduler/get-services scheduler)))
      (is (= {:success true, :result :deleted, :message "Deleted foo"}
             (scheduler/delete-service scheduler "foo")))
      (is (= {:success true, :result :deleted, :message "Deleted bar"}
             (scheduler/delete-service scheduler "bar")))
      (is (= {:success true, :result :deleted, :message "Deleted baz"}
             (scheduler/delete-service scheduler "baz"))))))

(deftest test-service-id->state
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler (create-shell-scheduler (common-scheduler-config))
          instance-id "bar"
          fake-pid 1234
          started-at (t/now)
          instance-dir (str (work-dir) "/foo/foo." instance-id)
          port 10000
          expected-state {:service (scheduler/make-Service
                                     {:id "foo"
                                      :instances 1
                                      :task-count 1
                                      :task-stats {:running 1, :healthy 0, :unhealthy 1, :staged 0}
                                      :environment {"HOME" (work-dir)
                                                    "LOGNAME" nil
                                                    "USER" nil
                                                    "WAITER_CLUSTER" "test-cluster"
                                                    "WAITER_CONCURRENCY_LEVEL" "1"
                                                    "WAITER_CPUS" "2"
                                                    "WAITER_MEM_MB" "32"
                                                    "WAITER_PASSWORD" "foo.password"
                                                    "WAITER_SERVICE_ID" "foo"
                                                    "WAITER_USERNAME" "waiter"}
                                      :service-description {"backend-proto" "http"
                                                            "cmd" "ls"
                                                            "concurrency-level" 1
                                                            "cpus" 2
                                                            "grace-period-secs" 10
                                                            "health-check-port-index" 0
                                                            "health-check-url" "/health-check-url"
                                                            "mem" 32
                                                            "ports" 1}
                                      :shell-scheduler/mem 32})
                          :id->instance {"foo.bar"
                                         (scheduler/make-ServiceInstance
                                           {:id "foo.bar"
                                            :service-id "foo"
                                            :started-at started-at
                                            :port port
                                            :log-directory instance-dir
                                            :shell-scheduler/working-directory instance-dir
                                            :shell-scheduler/pid fake-pid})}
                          :syncer {:syncer-state "foo"}}
          process-keys [:id->instance "foo.bar" :shell-scheduler/process]
          host-keys [:id->instance "foo.bar" :host]]
      (with-redefs [pid (constantly fake-pid)
                    utils/unique-identifier (constantly instance-id)
                    t/now (constantly started-at)
                    reserve-port! (constantly port)]
        (is (= {:success true, :result :created, :message "Created foo"}
               (create-test-service scheduler "foo"))))
      (ensure-agent-finished scheduler)
      (let [result (scheduler/service-id->state scheduler "foo")]
        (log/debug "service state:" result)
        (is (= (-> expected-state
                   (assoc-in process-keys (get-in result process-keys))
                   (assoc-in host-keys (get-in result host-keys)))
               result)))
      (is (= {:success true, :result :deleted, :message "Deleted foo"}
             (scheduler/delete-service scheduler "foo"))))))

(deftest test-port-reserved?
  (let [port->reservation-atom (atom {})
        port 50000
        port-grace-period-ms -1000]
    (is (false? (port-reserved? port->reservation-atom port)))
    (with-redefs [utils/port-available? (constantly true)]
      (reserve-port! port->reservation-atom [port port]))
    (is (port-reserved? port->reservation-atom port))
    (release-port! port->reservation-atom port port-grace-period-ms)
    (is (false? (port-reserved? port->reservation-atom port)))))

(deftest test-reserve-ports!
  (testing "successfully reserve all ports"
    (let [port->reservation-atom (atom {})
          port-range [50000 51000]
          reserved-ports (reserve-ports! 20 port->reservation-atom port-range)]
      (is (= (range 50000 50020) reserved-ports))))

  (testing "unable to reserve all ports"
    (let [port->reservation-atom (atom {})
          port-range [50000 50010]]
      (try
        (reserve-ports! 20 port->reservation-atom port-range)
        (is false "reserve-ports! did not throw an exception!")
        (catch Exception ex
          (let [ex-data (ex-data ex)]
            (is (= {:num-reserved-ports 11} ex-data))
            (is (= "Unable to reserve 20 ports" (.getMessage ex)))))))))

(deftest test-retry-failed-instances
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler-config (common-scheduler-config)
          scheduler (create-shell-scheduler scheduler-config)]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo" {"cmd" "exit 1" "mem" 0.1})))
      ;; Instance should get marked as failed
      (force-update-service-health scheduler scheduler-config)
      (let [instances (-> (scheduler/service-id->state scheduler "foo") :id->instance vals)
            failed-instances (filter :failed? instances)]
        (is (= 1 (count failed-instances))))
      ;; Loop should continue to launch additional instances after initial failure
      (force-maintain-instance-scale scheduler)
      (force-update-service-health scheduler scheduler-config)
      (let [instances (-> (scheduler/service-id->state scheduler "foo") :id->instance vals)
            failed-instances (filter :failed? instances)]
        (is (= 2 (count failed-instances))))
      (is (= {:success true, :result :deleted, :message "Deleted foo"}
             (scheduler/delete-service scheduler "foo"))))))

(deftest test-enforce-grace-period
  (with-redefs [config/retrieve-cluster-name (constantly "test-cluster")]
    (let [scheduler-config (common-scheduler-config)
          scheduler (create-shell-scheduler scheduler-config)]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo" {"cmd" "sleep 10000" "grace-period-secs" 0})))
      ;; Instance should be marked as unhealthy and failed
      (with-redefs [perform-health-check (constantly false)]
        (force-update-service-health scheduler scheduler-config))
      (let [instances (-> (scheduler/service-id->state scheduler "foo") :id->instance vals)
            failed-instances (filter :failed? instances)]
        (is (zero? (- (count instances) (count failed-instances))))
        (is (= 1 (count failed-instances)))))))

(deftest test-pid->memory
  (with-redefs [sh/sh (fn [& _]
                        {:out (str "PID  PPID   RSS\n"
                                   "  1     0     1\n"
                                   "  2     1    10\n"
                                   "  3     1   100\n"
                                   "  4     3  1000\n")})]
    (let [pid->memory (get-pid->memory)]
      (is (= 1111 (pid->memory 1)))
      (is (= 10 (pid->memory 2)))
      (is (= 1100 (pid->memory 3)))
      (is (= 1000 (pid->memory 4))))))

(deftest test-kill-orphaned-processes!
  (let [id->service {"s1" {:id->instance {"s1.a" {:killed? true :shell-scheduler/pid 1234}
                                          "s1.b" {:killed? false :shell-scheduler/pid 1212}
                                          "s1.c" {:killed? false :shell-scheduler/pid 1245}}}
                     "s2" {:id->instance {"s2.a" {:killed? true :shell-scheduler/pid 3456}
                                          "s2.b" {:killed? false :shell-scheduler/pid 3434}
                                          "s2.c" {:killed? false :shell-scheduler/pid 3467}
                                          "s2.d" {:killed? false :shell-scheduler/pid 3478}}}
                     "s3" {:id->instance {"s3.a" {:killed? false :shell-scheduler/pid 5656}
                                          "s3.b" {:killed? false :shell-scheduler/pid 5678}}}}
        running-pids #{1212 1245 3434 3467 3478 3489 4567 4578 5656 5678}
        killed-process-group-ids (atom #{})]
    (with-redefs [sh/sh (fn [command arg1 arg2 pgid]
                          (is (= ["pkill" "-9" "-g"] [command arg1 arg2]))
                          (swap! killed-process-group-ids conj pgid))]
      (kill-orphaned-processes! id->service running-pids))
    (is (= #{"3489" "4567" "4578"} @killed-process-group-ids))))

(defn- make-instance
  [service-id instance-id & {:as config}]
  (merge {:exit-code nil
          :extra-ports []
          :flags []
          :health-check-status nil,
          :id instance-id
          :log-directory (str "/tmp/" service-id "/" instance-id)
          :message nil
          :service-id service-id
          :shell-scheduler/working-directory (str "/tmp/" service-id "/" instance-id)}
         config))

(let [id->service {"waiter-kitchen"
                   {:id->instance {"waiter-kitchen.a1" (make-instance
                                                         "waiter-kitchen" "waiter-kitchen.a1"
                                                         :exit-code 1
                                                         :healthy? false
                                                         :host "127.0.0.5"
                                                         :killed? true
                                                         :started-at (du/str-to-date "2018-08-30T15:44:33.136Z")
                                                         :port 10000
                                                         :shell-scheduler/pid 32432
                                                         :failed? true
                                                         :message "Exited with code 1")
                                   "waiter-kitchen.b2" (make-instance
                                                         "waiter-kitchen" "waiter-kitchen.b2"
                                                         :healthy? true
                                                         :host "127.0.0.10"
                                                         :port 10001
                                                         :started-at (du/str-to-date "2018-08-30T15:44:37.393Z")
                                                         :shell-scheduler/pid 76576)
                                   "waiter-kitchen.c3" (make-instance
                                                         "waiter-kitchen" "waiter-kitchen.c3"
                                                         :healthy? true
                                                         :host "127.0.0.8"
                                                         :port 10002
                                                         :started-at (du/str-to-date "2018-08-30T15:45:37.393Z")
                                                         :shell-scheduler/pid 82982)}
                    :service {:environment {"HOME" "/home/waiter"
                                            "LOGNAME" "hiro"
                                            "USER" "hiro"
                                            "WAITER_CPUS" "0.1"
                                            "WAITER_MEM_MB" "256"
                                            "WAITER_PASSWORD" "7e37af"
                                            "WAITER_SERVICE_ID" "waiter-kitchen"
                                            "WAITER_USERNAME" "waiter"}
                              :id "waiter-kitchen"
                              :instances 1
                              :service-description {"allowed-params" [],
                                                    "authentication" "standard",
                                                    "backend-proto" "http",
                                                    "blacklist-on-503" true,
                                                    "cmd" "/opt/bin/kitchen -p $PORT0",
                                                    "cmd-type" "shell",
                                                    "concurrency-level" 1,
                                                    "cpus" 0.1,
                                                    "distribution-scheme" "balanced",
                                                    "env" {},
                                                    "expired-instance-restart-rate" 0.1,
                                                    "grace-period-secs" 120,
                                                    "health-check-interval-secs" 10,
                                                    "health-check-max-consecutive-failures" 5,
                                                    "health-check-url" "/status",
                                                    "idle-timeout-mins" 10,
                                                    "instance-expiry-mins" 7200,
                                                    "interstitial-secs" 0,
                                                    "jitter-threshold" 0.5,
                                                    "max-instances" 500,
                                                    "max-queue-length" 1000000,
                                                    "mem" 256,
                                                    "metadata" {},
                                                    "metric-group" "waiter_kitchen",
                                                    "min-instances" 1,
                                                    "name" "kitchen-app",
                                                    "permitted-user" "hiro",
                                                    "ports" 1,
                                                    "restart-backoff-factor" 2,
                                                    "run-as-user" "hiro",
                                                    "scale-down-factor" 0.001,
                                                    "scale-factor" 1,
                                                    "scale-up-factor" 0.1,
                                                    "version" "v1"}
                              :shell-scheduler/mem 256
                              :task-count 2
                              :task-stats {:healthy 2 :running 2 :staged 0 :unhealthy 0}}}}
      port->reservation {10000 {:expiry-time (du/str-to-date "2018-08-30T15:46:37.374Z")
                                :state :in-grace-period-until-expiry}
                         10001 {:expiry-time nil
                                :state :in-use}}
      work-directory (System/getProperty "user.dir")]

  (deftest test-backup-state
    (let [file-content-atom (atom nil)
          backup-file-path (str work-directory (File/separator) "test-files/test-backup-state.json")]
      (with-redefs [spit (fn [file-name content]
                           (is (= backup-file-path file-name))
                           (reset! file-content-atom content))]
        (backup-state "shell-scheduler" id->service port->reservation backup-file-path))
      (is (= (-> "test-files/shell-scheduler-backup.json" slurp json/read-str (dissoc "time"))
             (-> @file-content-atom json/read-str (dissoc "time"))))))

  (deftest test-restore-state-matching-running-pids
    (let [scheduler-config (assoc (common-scheduler-config) :work-directory work-directory)
          {:keys [id->service-agent port->reservation-atom] :as scheduler} (create-shell-scheduler scheduler-config)]
      (with-redefs [sh/sh (fn [& _] {:out "76576\n82982"})]
        (restore-state scheduler "test-files/shell-scheduler-backup.json"))
      (is (= id->service @id->service-agent))
      (is (= port->reservation @port->reservation-atom))))

  (deftest test-restore-state-with-orphaned-pids
    (let [scheduler-config (assoc (common-scheduler-config) :work-directory work-directory)
          {:keys [id->service-agent port->reservation-atom] :as scheduler} (create-shell-scheduler scheduler-config)]
      (with-redefs [sh/sh (fn [& _] {:out "76576"})]
        (restore-state scheduler "test-files/shell-scheduler-backup.json"))
      (is (= (-> id->service
                 (update-in ["waiter-kitchen" :id->instance "waiter-kitchen.c3"]
                            (fn [instance]
                              (assoc instance
                                :killed? true
                                :message "Process lost after restart")))
                 (update-in ["waiter-kitchen" :service] assoc
                            :task-count 1
                            :task-stats {:healthy 1 :running 1 :staged 0 :unhealthy 0}))
             @id->service-agent))
      (is (= port->reservation @port->reservation-atom)))))
