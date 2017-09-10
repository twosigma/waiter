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
(ns waiter.shell-scheduler-test
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [qbits.jet.client.http :as http]
            [waiter.client-tools :as ct]
            [waiter.scheduler :as scheduler]
            [waiter.shell-scheduler :refer :all]
            [waiter.test-helpers :as th]
            [waiter.utils :as utils])
  (:import clojure.lang.ExceptionInfo
           java.util.concurrent.TimeUnit
           waiter.shell_scheduler.ShellScheduler))

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
                                                  "grace-period-secs" 10
                                                  "mem" 32
                                                  "ports" 1}
                                                 custom-service-description)
                     :service-id service-id}]
     (scheduler/create-app-if-new scheduler (constantly "password") descriptor))))

(defn- task-stats
  "Gets the task-stats for the first service in the given scheduler"
  [scheduler]
  (-> scheduler
      scheduler/get-apps->instances
      keys
      first
      :task-stats))

(defn- ensure-agent-finished
  "Awaits the scheduler's agent"
  [{:keys [id->service-agent]}]
  (await id->service-agent))

(defn- force-update-service-health
  "Forces a call to update-service-health"
  [{:keys [id->service-agent port->reservation-atom] :as scheduler} {:keys [port-grace-period-ms]}]
  (send id->service-agent update-service-health port->reservation-atom port-grace-period-ms nil)
  (ensure-agent-finished scheduler))

(defn- force-maintain-instance-scale
  "Forces a call to maintain-instance-scale"
  [{:keys [id->service-agent port->reservation-atom port-range] :as scheduler}]
  (send id->service-agent maintain-instance-scale port->reservation-atom port-range)
  (ensure-agent-finished scheduler))

(deftest test-update-task-stats
  (testing "1 healthy"
    (let [{:keys [service id->instance]} (update-task-stats {:service {}
                                                             :id->instance {"foo" {:healthy? true}}})]
      (is (= {:healthy 1
              :unhealthy 0
              :running 1
              :staged 0} (:task-stats service)))
      (is (= 1 (:task-count service)))))
  (testing "1 unhealthy"
    (let [{:keys [service id->instance]} (update-task-stats {:service {}
                                                             :id->instance {"foo" {:healthy? false}}})]
      (is (= {:healthy 0
              :unhealthy 1
              :running 1
              :staged 0} (:task-stats service)))
      (is (= 1 (:task-count service)))))
  (testing "1 healthy, 1 unhealthy"
    (let [{:keys [service id->instance]} (update-task-stats {:service {}
                                                             :id->instance {"foo" {:healthy? true}
                                                                            "bar" {:healthy? false}}})]
      (is (= {:healthy 1
              :unhealthy 1
              :running 2
              :staged 0} (:task-stats service)))
   (testing "1 healthy, 1 unhealthy, 1 killed"
    (let [{:keys [service id->instance]} (update-task-stats {:service {}
                                                             :id->instance {"foo" {:healthy? true}
                                                                            "bar" {:healthy? false}
                                                                            "baz" {:healthy? false :killed? true}}})]
      (is (= {:healthy 1
              :unhealthy 1
              :running 2
              :staged 0} (:task-stats service)))
      (is (= 2 (:task-count service)))))   (is (= 2 (:task-count service))))))

(deftest test-launch-instance
  (testing "Launching an instance"
    (testing "should throw if cmd is nil"
      (is (thrown-with-msg? ExceptionInfo #"The command to run was not supplied"
                            (launch-instance "foo" {"backend-proto" "http" "ports" 1 "mem" 32} (work-dir) {} nil nil))))

    (testing "with https backend proto"
      (let [{:keys [protocol]} (launch-instance "foo" {"backend-proto" "https" "cmd" "ls" "ports" 1 "mem" 32} "scheduler" {} (atom {}) [2000 3000])]
        (is (= "https" protocol))))

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
          (let [{:keys [extra-ports port]} (launch-instance "baz" {"backend-proto" "http" "cmd" "dummy-command" "ports" num-ports "mem" 32} (work-dir) {} (atom {}) port-range)]
            (is (= port-range-start port))
            (is (= (map #(+ % port-range-start) (range 1 num-ports)) extra-ports))))))))

(deftest test-directory-content
  (let [service-description {"backend-proto" "http"
                             "cmd" "echo Hello, World!"
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
    (is (= "Hello, World!\n" (slurp (:url (first (filter #(= "stdout" (:name %)) content))))))))

(deftest test-pid
  (testing "Getting the pid of a Process"

    (testing "should return nil if it is not a UNIXProcess"
      (is (nil? (pid (proxy [Process] [])))))

    (testing "should return nil if it catches an Exception"
      (with-redefs [type (fn [_] (throw (ex-info "ERROR!" {})))]
        (is (nil? (pid (:process (launch-process "foo" (work-dir) "ls" {})))))))))

(deftest test-shell-scheduler
  (testing "Creating a new ShellScheduler"
    (let [valid-config {:health-check-timeout-ms 1
                        :port-grace-period-ms 1
                        :port-range [10000 10000]
                        :failed-instance-retry-interval-ms 500
                        :health-check-interval-ms 500
                        :work-directory (work-dir)}]

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
        (is (thrown? Throwable (create-shell-scheduler (assoc valid-config :work-directory ""))))))))

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

(deftest test-create-app-if-new
  (let [scheduler (create-shell-scheduler {:health-check-timeout-ms 1
                                           :port-grace-period-ms 1
                                           :port-range [10000 11000]
                                           :failed-instance-retry-interval-ms 100
                                           :health-check-interval-ms 100
                                           :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (is (= {:success false, :result :already-exists, :message "foo already exists!"}
           (create-test-service scheduler "foo")))
    (is (= {:success true, :result :deleted, :message "Deleted foo"}
           (scheduler/delete-app scheduler "foo")))))

(deftest test-delete-app
  (let [scheduler (create-shell-scheduler {:health-check-timeout-ms 1
                                           :port-grace-period-ms 1
                                           :port-range [10000 11000]
                                           :failed-instance-retry-interval-ms 500
                                           :health-check-interval-ms 500
                                           :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (ensure-agent-finished scheduler)
    (is (scheduler/app-exists? scheduler "foo"))
    (is (= {:success true, :result :deleted, :message "Deleted foo"}
           (scheduler/delete-app scheduler "foo")))
    (ensure-agent-finished scheduler)
    (is (not (scheduler/app-exists? scheduler "foo")))))

(deftest test-scale-app
  (let [scheduler-config {:health-check-timeout-ms 1
                          :port-grace-period-ms 1
                          :port-range [10000 11000]
                          :failed-instance-retry-interval-ms 500
                          :health-check-interval-ms 500
                          :work-directory (work-dir)}
        scheduler (create-shell-scheduler scheduler-config)]
    ;; Bogus service
    (is (= {:success false, :result :no-such-service-exists, :message "bar does not exist!"}
           (scheduler/scale-app scheduler "bar" 2)))
    (with-redefs [perform-health-check (constantly true)]
      ;; Create service, instances: 1
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo" {"cmd" "sleep 10000"})))
      (ensure-agent-finished scheduler)
      (is (scheduler/app-exists? scheduler "foo"))
      ;; Scale up, instances: 2
      (is (= {:success true, :result :scaled, :message "Scaled foo"}
             (scheduler/scale-app scheduler "foo" 2)))
      (force-maintain-instance-scale scheduler)
      (force-update-service-health scheduler scheduler-config)
      (is (= {:running 2, :healthy 2, :unhealthy 0, :staged 0}
             (task-stats scheduler)))
      ;; No need to scale down, instances: 2
      (is (= {:success false, :result :scaling-not-needed, :message "Unable to scale foo"}
             (scheduler/scale-app scheduler "foo" 1)))
      ;; Successfully kill one instance, instances: 1
      (let [instance (first (:active-instances (scheduler/get-instances scheduler "foo")))]
        (is (= {:killed? true, :success true, :result :deleted, :message (str "Deleted " (:id instance))}
               (scheduler/kill-instance scheduler instance))))
      (force-update-service-health scheduler scheduler-config)
      (is (= {:running 1, :healthy 1, :unhealthy 0, :staged 0}
             (task-stats scheduler)))
      ;; Scale up, instances: 2
      (is (= {:success true, :result :scaled, :message "Scaled foo"}
             (scheduler/scale-app scheduler "foo" 2)))
      (force-maintain-instance-scale scheduler)
      (force-update-service-health scheduler scheduler-config)
      (is (= {:running 2, :healthy 2, :unhealthy 0, :staged 0}
             (task-stats scheduler))))))

(deftest test-kill-instance
  (let [scheduler (create-shell-scheduler {:health-check-timeout-ms 1
                                           :port-grace-period-ms 1
                                           :port-range [10000 11000]
                                           :failed-instance-retry-interval-ms 500
                                           :health-check-interval-ms 500
                                           :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (with-redefs [perform-health-check (constantly true)]
      (let [instance (first (:active-instances (scheduler/get-instances scheduler "foo")))]
        (is (= {:killed? true, :success true, :result :deleted, :message (str "Deleted " (:id instance))}
               (scheduler/kill-instance scheduler instance)))
        (is (= {:success true, :result :deleted, :message "Deleted foo"}
               (scheduler/delete-app scheduler "foo")))
        (is (= {:success false, :result :no-such-service-exists, :message "foo does not exist!"}
               (scheduler/kill-instance scheduler instance)))))))

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
        (is (every? #(= {:state :in-use, :expiry-time nil}
                        (get @port-reservation-atom %))
                    (range port-range-start (+ port-range-start num-ports))))
        (let [current-time (t/now)]
          (with-redefs [t/now (fn [] current-time)]
            (kill-process! instance port-reservation-atom 0)
            (is (= num-ports (count @port-reservation-atom)))
            (is (every? #(= {:state :in-grace-period-until-expiry, :expiry-time current-time}
                            (get @port-reservation-atom %))
                        (range port-range-start (+ port-range-start num-ports))))))))))

(deftest test-health-check-instances
  (let [scheduler-config {:health-check-timeout-ms 1
                          :port-grace-period-ms 1
                          :port-range [10000 11000]
                          :failed-instance-retry-interval-ms 500
                          :health-check-interval-ms 500
                          :work-directory (work-dir)}
        scheduler (create-shell-scheduler scheduler-config)
        health-check-count-atom (atom 0)]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo" {"cmd" "sleep 100000"})))
    (with-redefs [http/get (fn [_ _]
                             (swap! health-check-count-atom inc)
                             (let [c (async/chan)]
                               (async/go (async/>! c {:status 200}))
                               c))]
      (let [instance (first (:active-instances (scheduler/get-instances scheduler "foo")))]
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
      (is (= 1 @health-check-count-atom)))))

(deftest test-should-update-task-stats-in-service
  (let [scheduler-config {:health-check-timeout-ms 1
                          :port-grace-period-ms 1
                          :port-range [10000 11000]
                          :failed-instance-retry-interval-ms 500
                          :health-check-interval-ms 500
                          :work-directory (work-dir)}
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
           (scheduler/delete-app scheduler "foo")))))

(deftest test-get-apps
  (let [scheduler-config {:health-check-timeout-ms 1
                          :port-grace-period-ms 1
                          :port-range [10000 11000]
                          :failed-instance-retry-interval-ms 1000
                          :health-check-interval-ms 1000
                          :work-directory (work-dir)}
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
                  :environment {"WAITER_USERNAME" "waiter"
                                "WAITER_PASSWORD" "password"
                                "HOME" (work-dir)
                                "LOGNAME" nil
                                "USER" nil}
                  :service-description {"backend-proto" "http"
                                        "cmd" "sleep 10000"
                                        "grace-period-secs" 10
                                        "mem" 32
                                        "ports" 1}
                  :shell-scheduler/mem 32}
                 {:id "bar"
                  :instances 1
                  :task-count 1
                  :task-stats {:running 1, :healthy 0, :unhealthy 1, :staged 0}
                  :environment {"WAITER_USERNAME" "waiter"
                                "WAITER_PASSWORD" "password"
                                "HOME" (work-dir)
                                "LOGNAME" nil
                                "USER" nil}
                  :service-description {"backend-proto" "http"
                                        "cmd" "sleep 10000"
                                        "grace-period-secs" 10
                                        "mem" 32
                                        "ports" 1}
                  :shell-scheduler/mem 32}
                 {:id "baz"
                  :instances 1
                  :task-count 1
                  :task-stats {:running 1, :healthy 0, :unhealthy 1, :staged 0}
                  :environment {"WAITER_USERNAME" "waiter"
                                "WAITER_PASSWORD" "password"
                                "HOME" (work-dir)
                                "LOGNAME" nil
                                "USER" nil}
                  :service-description {"backend-proto" "http"
                                        "cmd" "sleep 10000"
                                        "grace-period-secs" 10
                                        "mem" 32
                                        "ports" 1}
                  :shell-scheduler/mem 32}])
           (scheduler/get-apps scheduler)))
    (is (= {:success true, :result :deleted, :message "Deleted foo"}
           (scheduler/delete-app scheduler "foo")))
    (is (= {:success true, :result :deleted, :message "Deleted bar"}
           (scheduler/delete-app scheduler "bar")))
    (is (= {:success true, :result :deleted, :message "Deleted baz"}
           (scheduler/delete-app scheduler "baz")))))

(deftest test-service-id->state
  (let [scheduler (create-shell-scheduler {:health-check-timeout-ms 1
                                           :port-grace-period-ms 1
                                           :port-range [10000 10000]
                                           :failed-instance-retry-interval-ms 500
                                           :health-check-interval-ms 500
                                           :work-directory (work-dir)})
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
                                    :environment {"WAITER_USERNAME" "waiter"
                                                  "WAITER_PASSWORD" "password"
                                                  "HOME" (work-dir)
                                                  "LOGNAME" nil
                                                  "USER" nil}
                                    :service-description {"backend-proto" "http"
                                                          "cmd" "ls"
                                                          "grace-period-secs" 10
                                                          "mem" 32
                                                          "ports" 1}
                                    :shell-scheduler/mem 32})
                        :id->instance {"foo.bar"
                                       (scheduler/make-ServiceInstance
                                         {:id "foo.bar"
                                          :service-id "foo"
                                          :started-at (utils/date-to-str started-at (f/formatters :date-time))
                                          :port port
                                          :protocol "http"
                                          :log-directory instance-dir
                                          :shell-scheduler/working-directory instance-dir
                                          :shell-scheduler/pid fake-pid})}}
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
           (scheduler/delete-app scheduler "foo")))))

(deftest test-port-reserved?
  (let [port->reservation-atom (atom {})
        port 10000
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
          port-range [10000 11000]
          reserved-ports (reserve-ports! 20 port->reservation-atom port-range)]
      (is (= (range 10000 10020) reserved-ports))))

  (testing "unable to reserve all ports"
    (let [port->reservation-atom (atom {})
          port-range [10000 10010]]
      (try
        (reserve-ports! 20 port->reservation-atom port-range)
        (is false "reserve-ports! did not throw an exception!")
        (catch Exception ex
          (let [ex-data (ex-data ex)]
            (is (= {:num-reserved-ports 11} ex-data))
            (is (= "Unable to reserve 20 ports" (.getMessage ex)))))))))

(deftest test-retry-failed-instances
  (let [scheduler-config {:health-check-timeout-ms 1
                          :port-grace-period-ms 1
                          :port-range [10000 11000]
                          :failed-instance-retry-interval-ms 500
                          :health-check-interval-ms 500
                          :work-directory (work-dir)}
        scheduler (create-shell-scheduler scheduler-config)]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo" {"cmd" "sleep 10000" "mem" 0.1})))
    ;; Instance should get marked as failed
    (force-update-service-health scheduler scheduler-config)
    (let [instances (scheduler/get-instances scheduler "foo")]
      (is (= 1 (count (:failed-instances instances)))))
    ;; Loop should continue to launch additional instances after initial failure
    (force-maintain-instance-scale scheduler)
    (force-update-service-health scheduler scheduler-config)
    (let [instances (scheduler/get-instances scheduler "foo")]
      (is (= 2 (count (:failed-instances instances)))))
    (is (= {:success true, :result :deleted, :message "Deleted foo"}
           (scheduler/delete-app scheduler "foo")))))

(deftest test-enforce-grace-period
  (let [scheduler-config {:health-check-timeout-ms 1
                          :port-grace-period-ms 1
                          :port-range [10000 11000]
                          :failed-instance-retry-interval-ms 500
                          :health-check-interval-ms 500
                          :work-directory (work-dir)}
        scheduler (create-shell-scheduler scheduler-config)]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo" {"cmd" "sleep 10000" "grace-period-secs" 0})))
    ;; Instance should be marked as unhealthy and failed
    (with-redefs [perform-health-check (constantly false)]
     (force-update-service-health scheduler scheduler-config))
    (let [instances (scheduler/get-instances scheduler "foo")]
      (is (= 0 (count (:active-instances instances))))
      (is (= 1 (count (:failed-instances instances)))))))
