;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
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
            [qbits.jet.client.http :as http]
            [waiter.client-tools :as ct]
            [waiter.scheduler :as scheduler]
            [waiter.shell-scheduler :refer :all]
            [waiter.test-helpers :as th]
            [waiter.utils :as utils])
  (:import clojure.lang.ExceptionInfo
           waiter.shell_scheduler.ShellScheduler))

(defn work-dir
  "Returns the canonical path for the ./scheduler directory"
  []
  (-> "./scheduler" (io/file) (.getCanonicalPath)))

(defn- create-test-service
  "Creates a new (test) service with command `ls` and the given service-id"
  [scheduler service-id]
  (let [descriptor {:service-description {"cmd" "ls"}
                    :service-id service-id}]
    (scheduler/create-app-if-new scheduler (constantly "password") descriptor)))

(defn- task-stats
  "Gets the task-stats for the first service in the given scheduler"
  [scheduler]
  (-> scheduler
      scheduler/get-apps->instances
      keys
      first
      :task-stats))

(deftest test-launch-instance
  (testing "Launching an instance"
    (testing "should throw if cmd is nil"
      (is (thrown-with-msg? ExceptionInfo #"The command to run was not supplied"
                            (launch-instance "foo" "." nil {} nil nil))))))

(deftest test-directory-content
  (let [id->service (create-service {} "foo" {"cmd" "echo Hello, World!"} (constantly "password")
                                    (work-dir) (atom {}) [0 0] (promise))
        service-entry (get id->service "foo")
        instance-id (-> service-entry :id->instance keys first)
        content (directory-content service-entry instance-id "")]
    (is (= 2 (count content)))
    (is (some #(= "stdout" (:name %)) content))
    (is (some #(= "stderr" (:name %)) content))
    (is (th/wait-for (fn []
                       (= "Hello, World!\n" (slurp (:url (first (filter #(= "stdout" (:name %)) content))))))
                     :interval 1))))

(deftest test-pid
  (testing "Getting the pid of a Process"

    (testing "should return nil if it is not a UNIXProcess"
      (is (nil? (pid (proxy [Process] [])))))

    (testing "should return nil if it catches an Exception"
      (with-redefs [type (fn [_] (throw (ex-info "ERROR!" {})))]
        (is (nil? (pid (:process (launch-process "foo" (work-dir) "ls" {})))))))))

(deftest test-shell-scheduler
  (testing "Creating a new ShellScheduler"
    (let [valid-config {:health-check-interval-ms 1
                        :health-check-timeout-ms 1
                        :port-grace-period-ms 1
                        :port-range [10000 10000]
                        :work-directory (work-dir)}]

      (testing "should work with valid configuration"
        (is (instance? ShellScheduler (shell-scheduler valid-config))))

      (testing "should throw if port range is bogus"
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :port-range [nil nil]))))
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :port-range [0 nil]))))
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :port-range [10000 nil]))))
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :port-range [10000 0]))))
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :port-range [10000 9999])))))

      (testing "should throw if other config fields are bogus"
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :health-check-interval-ms 0))))
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :health-check-timeout-ms 0))))
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :port-grace-period-ms 0))))
        (is (thrown? Throwable (shell-scheduler (assoc valid-config :work-directory ""))))))))

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
  (let [scheduler (shell-scheduler {:health-check-interval-ms 1
                                    :health-check-timeout-ms 1
                                    :port-grace-period-ms 1
                                    :port-range [10000 11000]
                                    :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (is (= {:success false, :result :already-exists, :message "foo already exists!"}
           (create-test-service scheduler "foo")))))

(deftest test-delete-app
  (let [scheduler (shell-scheduler {:health-check-interval-ms 1
                                    :health-check-timeout-ms 1
                                    :port-grace-period-ms 1
                                    :port-range [10000 11000]
                                    :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (is (ct/wait-for #(scheduler/app-exists? scheduler "foo") :interval 1))
    (is (= {:success true, :result :deleted, :message "Deleted foo"}
           (scheduler/delete-app scheduler "foo")))
    (is (ct/wait-for #(not (scheduler/app-exists? scheduler "foo")) :interval 1))))

(deftest test-scale-app
  (let [scheduler (shell-scheduler {:work-directory (work-dir)
                                    :port-grace-period-ms 1
                                    :port-range [10000 11000]
                                    :health-check-interval-ms 1
                                    :health-check-timeout-ms 1})]
    ;; Bogus service
    (is (= {:success false, :result :no-such-service-exists, :message "bar does not exist!"}
           (scheduler/scale-app scheduler "bar" 2)))
    (with-redefs [perform-health-check (constantly true)]
      ;; Create service, instances: 1
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo")))
      ;; Scale up, instances: 2
      (is (= {:success true, :result :scaled, :message "Scaled foo"}
             (scheduler/scale-app scheduler "foo" 2)))
      (is (ct/wait-for #(= {:running 2, :healthy 2, :unhealthy 0, :staged 0}
                           (task-stats scheduler))
                       :interval 0.1))
      ;; No need to scale down, instances: 2
      (is (= {:success false, :result :scaling-not-needed, :message "Unable to scale foo"}
             (scheduler/scale-app scheduler "foo" 1)))
      ;; Kill one instance, instances: 1
      (let [instance (first (:active-instances (scheduler/get-instances scheduler "foo")))]
        (is (= {:success true, :result :deleted, :message (str "Deleted " (:id instance))}
               (scheduler/kill-instance scheduler instance))))
      (is (ct/wait-for #(= {:running 1, :healthy 1, :unhealthy 0, :staged 0}
                           (task-stats scheduler))
                       :interval 0.1
                       :timeout 1))
      ;; Scale up, instances: 2
      (is (= {:success true, :result :scaled, :message "Scaled foo"}
             (scheduler/scale-app scheduler "foo" 2)))
      (is (ct/wait-for #(= {:running 2, :healthy 2, :unhealthy 0, :staged 0}
                           (task-stats scheduler))
                       :interval 0.1)))))

(deftest test-kill-instance
  (let [scheduler (shell-scheduler {:health-check-interval-ms 1
                                    :health-check-timeout-ms 1
                                    :port-grace-period-ms 1
                                    :port-range [10000 11000]
                                    :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (with-redefs [perform-health-check (constantly true)]
      (let [instance (first (:active-instances (scheduler/get-instances scheduler "foo")))]
        (is (= {:success true, :result :deleted, :message (str "Deleted " (:id instance))}
               (scheduler/kill-instance scheduler instance)))
        (is (= {:success true, :result :deleted, :message "Deleted foo"}
               (scheduler/delete-app scheduler "foo")))
        (is (= {:success false, :result :no-such-service-exists, :message "foo does not exist!"}
               (scheduler/kill-instance scheduler instance)))))))

(deftest test-kill-process
  (testing "Killing a process"
    (testing "should handle exceptions"
      (let [instance (launch-instance "foo" (work-dir) "ls" {} (atom {}) [0 0])]
        (with-redefs [t/plus (fn [_ _] (throw (ex-info "ERROR!" {})))]
          (kill-process! instance (atom {}) 0))))))

(deftest test-should-not-health-check-killed-instances
  (let [scheduler (shell-scheduler {:health-check-interval-ms 1
                                    :health-check-timeout-ms 1
                                    :port-grace-period-ms 1
                                    :port-range [10000 11000]
                                    :work-directory (work-dir)})
        health-check-count-atom (atom 0)]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (with-redefs [http/get (fn [_ _]
                             (swap! health-check-count-atom inc)
                             (let [c (async/chan)]
                               (async/go (async/>! c {:status 200}))
                               c))]
      (let [instance (first (:active-instances (scheduler/get-instances scheduler "foo")))]
        ;; We expect a health check to have occurred
        (is (= 1 @health-check-count-atom))
        ;; Kill the single instance of our service
        (is (= {:success true, :result :deleted, :message (str "Deleted " (:id instance))}
               (scheduler/kill-instance scheduler instance))))
      ;; Realize the lazy :killed-instances sequence to force a health check attempt
      (dorun (:killed-instances (scheduler/get-instances scheduler "foo")))
      ;; But, the health check shouldn't happen because the instance is killed
      (is (= 1 @health-check-count-atom)))))

(deftest test-should-update-task-stats-in-service
  (let [scheduler (shell-scheduler {:health-check-interval-ms 1
                                    :health-check-timeout-ms 1
                                    :port-grace-period-ms 1
                                    :port-range [10000 11000]
                                    :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (with-redefs [perform-health-check (constantly true)]
      (is (= {:running 1, :healthy 1, :unhealthy 0, :staged 0}
             (task-stats scheduler))))
    (with-redefs [perform-health-check (constantly false)]
      (is (= {:running 1, :healthy 0, :unhealthy 1, :staged 0}
             (task-stats scheduler))))))

(deftest test-get-apps
  (let [scheduler (shell-scheduler {:health-check-interval-ms 1
                                    :health-check-timeout-ms 1
                                    :port-grace-period-ms 1
                                    :port-range [10000 11000]
                                    :work-directory (work-dir)})]
    (is (= {:success true, :result :created, :message "Created foo"}
           (create-test-service scheduler "foo")))
    (is (= {:success true, :result :created, :message "Created bar"}
           (create-test-service scheduler "bar")))
    (is (= {:success true, :result :created, :message "Created baz"}
           (create-test-service scheduler "baz")))
    (is (= (map scheduler/map->Service
                [{:id "foo"
                  :instances 1
                  :task-count 1
                  :task-stats {:running 1, :healthy 0, :unhealthy 0, :staged 0}
                  :environment {"WAITER_USERNAME" "waiter"
                                "WAITER_PASSWORD" "password"
                                "HOME" (work-dir)
                                "LOGNAME" nil
                                "USER" nil}
                  :service-description {"cmd" "ls"}}
                 {:id "bar"
                  :instances 1
                  :task-count 1
                  :task-stats {:running 1, :healthy 0, :unhealthy 0, :staged 0}
                  :environment {"WAITER_USERNAME" "waiter"
                                "WAITER_PASSWORD" "password"
                                "HOME" (work-dir)
                                "LOGNAME" nil
                                "USER" nil}
                  :service-description {"cmd" "ls"}}
                 {:id "baz"
                  :instances 1
                  :task-count 1
                  :task-stats {:running 1, :healthy 0, :unhealthy 0, :staged 0}
                  :environment {"WAITER_USERNAME" "waiter"
                                "WAITER_PASSWORD" "password"
                                "HOME" (work-dir)
                                "LOGNAME" nil
                                "USER" nil}
                  :service-description {"cmd" "ls"}}])
           (scheduler/get-apps scheduler)))))

(deftest test-service-id->state
  (let [scheduler (shell-scheduler {:health-check-interval-ms 1
                                    :health-check-timeout-ms 1
                                    :port-grace-period-ms 1
                                    :port-range [10000 10000]
                                    :work-directory (work-dir)})
        instance-id "bar"
        fake-pid 1234
        started-at (t/now)
        instance-dir (str (work-dir) "/foo/foo." instance-id)
        port 10000]
    (with-redefs [pid (constantly fake-pid)
                  utils/unique-identifier (constantly instance-id)
                  t/now (constantly started-at)
                  reserve-port! (constantly port)]
      (is (= {:success true, :result :created, :message "Created foo"}
             (create-test-service scheduler "foo"))))
    (let [result (scheduler/service-id->state scheduler "foo")]
      (is (= {:service (scheduler/map->Service
                         {:id "foo"
                          :instances 1
                          :task-count 1
                          :task-stats {:running 1, :healthy 0, :unhealthy 0, :staged 0}
                          :environment {"WAITER_USERNAME" "waiter"
                                        "WAITER_PASSWORD" "password"
                                        "HOME" (work-dir)
                                        "LOGNAME" nil
                                        "USER" nil}
                          :service-description {"cmd" "ls"}})
              :id->instance {"foo.bar" (scheduler/map->ServiceInstance
                                         {:id "foo.bar"
                                          :service-id "foo"
                                          :started-at (utils/date-to-str started-at (f/formatters :date-time))
                                          :healthy? nil
                                          :host "localhost"
                                          :port port
                                          :log-directory instance-dir
                                          :message nil
                                          :shell-scheduler/working-directory instance-dir
                                          :shell-scheduler/last-health-check-time (t/epoch)
                                          :shell-scheduler/pid fake-pid
                                          :shell-scheduler/process (get-in result [:id->instance "foo.bar" :shell-scheduler/process])})}}
             result)))))

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
