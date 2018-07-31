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
(ns waiter.scheduler.cook-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.scheduler.cook :refer :all]
            [waiter.mesos.mesos :as mesos]
            [waiter.scheduler :as scheduler]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as http-utils]
            [waiter.util.utils :as utils])
  (:import (java.util UUID)
           (waiter.scheduler.cook CookScheduler)
           [clojure.lang ExceptionInfo]))

(deftest test-post-jobs
  (let [http-client (Object.)
        cook-url "http://cook.localtest.me"
        job-description {:jobs [{:application {:name "test-service"
                                               :version "123456"}
                                 :command "test-command"
                                 :cpus 1
                                 :labels {:user "test-user"}
                                 :mem 1536}]}]
    (testing "impersonation disabled"
      (let [cook-api {:http-client http-client
                      :spnego-auth "test-spnego-auth"
                      :url cook-url}]
        (with-redefs [http-utils/http-request
                      (fn [in-http-client in-request-url & {:as options}]
                        (is (= http-client in-http-client))
                        (is (= (str cook-url "/jobs") in-request-url))
                        (is (= {:accept "application/json"
                                :body (utils/map->json job-description)
                                :content-type "application/json"
                                :headers {}
                                :request-method :post
                                :spnego-auth "test-spnego-auth"}
                               options)))]
          (post-jobs cook-api job-description))))

    (testing "impersonation enabled"
      (let [cook-api {:http-client http-client
                      :impersonate true
                      :spnego-auth "test-spnego-auth"
                      :url cook-url}]
        (with-redefs [http-utils/http-request
                      (fn [in-http-client in-request-url & {:as options}]
                        (is (= http-client in-http-client))
                        (is (= (str cook-url "/jobs") in-request-url))
                        (is (= {:accept "application/json"
                                :body (utils/map->json job-description)
                                :content-type "application/json"
                                :headers {"x-cook-impersonate" "test-user"}
                                :request-method :post
                                :spnego-auth "test-spnego-auth"}
                               options)))]
          (post-jobs cook-api job-description))))))

(deftest test-delete-jobs
  (let [http-client (Object.)
        cook-url "http://cook.localtest.me"
        run-as-user "test-user"
        job-uuids ["uuid-1" "uuid-2" "uuid-3"]]

    (testing "impersonation disabled"
      (let [cook-api {:http-client http-client
                      :spnego-auth "test-spnego-auth"
                      :url cook-url}]
        (with-redefs [http-utils/http-request
                      (fn [in-http-client in-request-url & {:as options}]
                        (is (= http-client in-http-client))
                        (is (= (str cook-url "/rawscheduler") in-request-url))
                        (is (= {:accept "application/json"
                                :content-type "application/json"
                                :headers {}
                                :query-string {:job job-uuids}
                                :request-method :delete
                                :spnego-auth "test-spnego-auth"}
                               options)))]
          (delete-jobs cook-api run-as-user job-uuids))))

    (testing "impersonation enabled"
      (let [cook-api {:http-client http-client
                      :impersonate true
                      :spnego-auth "test-spnego-auth"
                      :url cook-url}]
        (with-redefs [http-utils/http-request
                      (fn [in-http-client in-request-url & {:as options}]
                        (is (= http-client in-http-client))
                        (is (= (str cook-url "/rawscheduler") in-request-url))
                        (is (= {:accept "application/json"
                                :content-type "application/json"
                                :headers {"x-cook-impersonate" "test-user"}
                                :query-string {:job job-uuids}
                                :request-method :delete
                                :spnego-auth "test-spnego-auth"}
                               options)))]
          (delete-jobs cook-api run-as-user job-uuids))))))

(deftest test-get-jobs
  (let [http-client (Object.)
        cook-url "http://cook.localtest.me"
        cook-api {:http-client http-client
                  :spnego-auth "test-spnego-auth"
                  :url cook-url}
        test-user "test-user"
        current-time (t/now)
        service-id "test-service-id"
        http-request-fn-factory (fn [expected-query-string]
                                  (fn [in-http-client in-request-url & {:as options}]
                                    (is (= http-client in-http-client))
                                    (is (= (str cook-url "/jobs") in-request-url))
                                    (is (= {:accept "application/json"
                                            :content-type "application/json"
                                            :query-string expected-query-string
                                            :request-method :get
                                            :spnego-auth "test-spnego-auth"}
                                           options))))]
    (with-redefs [t/now (constantly current-time)]

      (testing "no optional arguments"
        (let [end-time current-time
              search-interval (t/days 7)
              start-time (t/minus end-time search-interval)
              states ["running"]
              query-string {:end (du/date-to-str end-time)
                            :start (du/date-to-str start-time)
                            :state states
                            :user test-user}]
          (with-redefs [http-utils/http-request (http-request-fn-factory query-string)]
            (get-jobs cook-api test-user states))))

      (testing "only search interval argument"
        (let [end-time current-time
              search-interval (t/days 4)
              start-time (t/minus end-time search-interval)
              states ["running"]
              query-string {:end (du/date-to-str end-time)
                            :start (du/date-to-str start-time)
                            :state states
                            :user test-user}]
          (with-redefs [http-utils/http-request (http-request-fn-factory query-string)]
            (get-jobs cook-api test-user states :search-interval search-interval))))

      (testing "only end-time argument"
        (let [end-time (t/minus current-time (t/days 1))
              search-interval (t/days 7)
              start-time (t/minus end-time search-interval)
              states ["running"]
              query-string {:end (du/date-to-str end-time)
                            :start (du/date-to-str start-time)
                            :state states
                            :user test-user}]
          (with-redefs [http-utils/http-request (http-request-fn-factory query-string)]
            (get-jobs cook-api test-user states :end-time end-time))))

      (testing "only service-id argument"
        (let [end-time current-time
              search-interval (t/days 7)
              start-time (t/minus end-time search-interval)
              states ["running"]
              query-string {:end (du/date-to-str end-time)
                            :name (str service-id "*")
                            :start (du/date-to-str start-time)
                            :state states
                            :user test-user}]
          (with-redefs [http-utils/http-request (http-request-fn-factory query-string)]
            (get-jobs cook-api test-user states :service-id service-id))))

      (testing "all arguments"
        (let [end-time current-time
              search-interval (t/days 2)
              start-time (t/minus end-time search-interval)
              states ["running" "waiting"]
              query-string {:end (du/date-to-str end-time)
                            :start (du/date-to-str start-time)
                            :state states
                            :user test-user}]
          (with-redefs [http-utils/http-request (http-request-fn-factory query-string)]
            (get-jobs cook-api test-user states
                      :end-time end-time
                      :start-time start-time)))))))

(deftest test-job-healthy?
  (with-redefs [http-utils/http-request (fn [_ in-health-check-url]
                                          (is (str/starts-with? in-health-check-url "http://www.hostname.com:1234/"))
                                          (when-not (str/includes? in-health-check-url "unhealthy")
                                            in-health-check-url))]
    (is (job-healthy? {:status "running"
                       :instances [{:hostname "www.hostname.com"
                                    :ports [1234]
                                    :status "running"
                                    :task_id (str "task-" (System/nanoTime))}]
                       :labels {:backend-proto "http"
                                :health-check-url "/healthy"}}))

    (is (job-healthy? {:status "running"
                       :instances [{:hostname "www.hostname.com"
                                    :ports [4567]
                                    :status "running"
                                    :task_id (str "task-" (System/nanoTime))}]
                       :labels {:backend-port "1234"
                                :backend-proto "http"
                                :health-check-url "/healthy"}}))

    (is (not (job-healthy? {:status "running"
                            :instances [{:hostname "www.hostname.com"
                                         :ports [1234]
                                         :status "running"
                                         :task_id (str "task-" (System/nanoTime))}]
                            :labels {:backend-proto "http"
                                     :health-check-url "/unhealthy"}})))

    (is (not (job-healthy? {:status "running"
                            :instances [{:hostname "www.hostname.com"
                                         :ports [1234]
                                         :status "failed"
                                         :task_id (str "task-" (System/nanoTime))}]
                            :labels {:backend-proto "http"
                                     :health-check-url "/healthy"}})))

    (is (not (job-healthy? {:status "completed"
                            :instances [{:hostname "www.hostname.com"
                                         :ports [1234]
                                         :status "success"
                                         :task_id (str "task-" (System/nanoTime))}]
                            :labels {:backend-proto "http"
                                     :health-check-url "/healthy"}})))))

(deftest test-create-job-description
  (let [service-id->password-fn (fn [service-id] (str service-id "-password"))
        home-path-prefix "/home/path/"
        service-id "test-service-1"
        service-description {"backend-proto" "http"
                             "cmd" "test-command"
                             "cpus" 1
                             "mem" 1536
                             "run-as-user" "test-user"
                             "ports" 2
                             "restart-backoff-factor" 2
                             "grace-period-secs" 111
                             "health-check-interval-secs" 10
                             "health-check-max-consecutive-failures" 5
                             "health-check-url" "/health-check"
                             "instance-expiry-mins" 3600
                             "env" {"FOO" "bar"
                                    "BAZ" "quux"}
                             "name" "test-service"
                             "version" "123456"}
        instance-priority 75
        expected-job {:application {:name "test-service"
                                    :version "123456"}
                      :command "test-command"
                      :cpus 1
                      :disable-mea-culpa-retries true
                      :env {"BAZ" "quux"
                            "FOO" "bar"
                            "HOME" "/home/path/test-user"
                            "LOGNAME" "test-user"
                            "USER" "test-user"
                            "WAITER_CPUS" "1"
                            "WAITER_MEM_MB" "1536"
                            "WAITER_PASSWORD" "test-service-1-password"
                            "WAITER_SERVICE_ID" "test-service-1"
                            "WAITER_USERNAME" "waiter"}
                      :executor "cook"
                      :labels {:backend-proto "http"
                               :health-check-url "/health-check"
                               :service-id "test-service-1"
                               :source "waiter"
                               :user "test-user"}
                      :max-retries 1
                      :max-runtime 216300000
                      :mem 1536
                      :ports 2
                      :priority 75}]

    (testing "basic-test-with-defaults"
      (let [actual (create-job-description
                     service-id service-description service-id->password-fn home-path-prefix instance-priority)
            job-uuid (-> actual :jobs first :uuid)
            expected {:jobs [(assoc expected-job
                               :name (str "test-service-1." job-uuid)
                               :uuid job-uuid)]}]
        (is (= expected actual))))

    (testing "basic-test-custom-port"
      (let [service-description (assoc service-description
                                  "metadata" {"cook-backend-port" "4567"})
            actual (create-job-description
                     service-id service-description service-id->password-fn home-path-prefix instance-priority)
            job-uuid (-> actual :jobs first :uuid)
            expected {:jobs [(-> expected-job
                                 (assoc :name (str "test-service-1." job-uuid) :uuid job-uuid)
                                 (update :labels assoc :backend-port "4567"))]}]
        (is (= expected actual)))

      (is (thrown-with-msg?
            ExceptionInfo #"cook-backend-port metadata parsed to a non-positive integer"
            (let [service-description (assoc service-description
                                        "metadata" {"cook-backend-port" "0"})
                  actual (create-job-description
                           service-id service-description service-id->password-fn home-path-prefix instance-priority)
                  job-uuid (-> actual :jobs first :uuid)
                  expected {:jobs [(-> expected-job
                                       (assoc :name (str "test-service-1." job-uuid) :uuid job-uuid)
                                       (update :labels assoc :backend-port "4567"))]}]
              (is (= expected actual)))))

      (is (thrown-with-msg?
            ExceptionInfo #"cook-backend-port metadata must parse to a positive integer"
            (let [service-description (assoc service-description
                                        "metadata" {"cook-backend-port" "abcd"})
                  actual (create-job-description
                           service-id service-description service-id->password-fn home-path-prefix instance-priority)
                  job-uuid (-> actual :jobs first :uuid)
                  expected {:jobs [(-> expected-job
                                       (assoc :name (str "test-service-1." job-uuid) :uuid job-uuid)
                                       (update :labels assoc :backend-port "4567"))]}]
              (is (= expected actual))))))

    (testing "basic-test-with-docker-image"
      (let [service-description (assoc service-description
                                  "metadata" {"docker-image-label" "baz"
                                              "docker-image-name" "bar"
                                              "docker-image-namespace" "foo"})
            actual (create-job-description
                     service-id service-description service-id->password-fn home-path-prefix instance-priority)
            job-uuid (-> actual :jobs first :uuid)
            expected {:jobs [(assoc expected-job
                               :container {:docker {:force-pull-image false
                                                    :image "namespace:foo,name:bar,label:baz"
                                                    :network "HOST"}
                                           :type "docker"}
                               :name (str "test-service-1." job-uuid)
                               :uuid job-uuid)]}]
        (is (= expected actual))))))

(deftest test-determine-instance-priority
  (let [allowed-priorities [75 70 65 60 55]]
    (is (= 75 (determine-instance-priority allowed-priorities #{})))
    (is (= 70 (determine-instance-priority allowed-priorities #{75})))
    (is (= 65 (determine-instance-priority allowed-priorities #{75 70})))
    (is (= 70 (determine-instance-priority allowed-priorities #{75 65})))
    (is (= 60 (determine-instance-priority allowed-priorities #{75 70 65 55})))
    (is (= 55 (determine-instance-priority allowed-priorities #{75 70 65 60})))
    (is (= 55 (determine-instance-priority allowed-priorities #{75 70 65 60 55})))))

(deftest test-launch-jobs
  (let [posted-jobs-atom (atom [])]
    (with-redefs [create-job-description (fn [service-id _ _ _ instance-priority]
                                           {:priority instance-priority
                                            :service-id service-id})
                  post-jobs (fn [_ job] (swap! posted-jobs-atom conj job))]
      (let [cook-api (Object.)
            service-id "test-service-id"
            service-description {}
            service-id->password-fn (constantly "password")
            home-path-prefix "/home/path/"
            num-instances 3
            allowed-priorities [75 70 65 60 55]
            reserved-priorities #{75 65}]
        (launch-jobs cook-api service-id service-description service-id->password-fn
                     home-path-prefix num-instances allowed-priorities reserved-priorities)
        (is (= [{:priority 70 :service-id service-id}
                {:priority 60 :service-id service-id}
                {:priority 55 :service-id service-id}]
               @posted-jobs-atom))))))

(deftest test-retrieve-jobs
  (let [search-interval (t/days 1)
        service-id "test-service-id"
        test-user "test-user"]
    (with-redefs [get-jobs (fn [_ user states & {:as options}]
                             (is (= test-user user))
                             (is (= ["running" "waiting"] states))
                             (is (= {:search-interval search-interval :service-id service-id} options))
                             [])]
      (retrieve-jobs (Object.) search-interval service-id {"run-as-user" test-user}))))

(deftest test-job->service-instance
  (with-redefs [job-healthy? (constantly true)]
    (let [job-uuid (str (UUID/randomUUID))
          start-time (System/currentTimeMillis)
          expected-instance {:cook/job-name "job-name"
                             :cook/job-uuid job-uuid
                             :cook/priority 75
                             :cook/task-id "task-id"
                             :exit-code 0
                             :extra-ports [5678 7890]
                             :flags #{}
                             :health-check-status nil
                             :healthy? true
                             :host "hostname.localtest.me"
                             :id "job-name_test-user_task-id"
                             :log-directory "/sandbox/directory"
                             :message "test reason"
                             :port 1234
                             :protocol "http"
                             :service-id "service-id"
                             :started-at (tc/from-long start-time)}
          actual-job {:instances [{:exit_code 0
                                   :hostname "hostname.localtest.me"
                                   :ports [1234 5678 7890]
                                   :reason_string "test reason"
                                   :sandbox_directory "/sandbox/directory"
                                   :start_time start-time
                                   :task_id "task-id"}]
                      :labels {:backend-proto "http"
                               :service-id "service-id"
                               :user "test-user"}
                      :name "job-name"
                      :priority 75
                      :status "running"
                      :uuid job-uuid}]

      (testing "basic job"
        (is (= (scheduler/make-ServiceInstance expected-instance)
               (job->service-instance actual-job))))

      (testing "job with custom port"
        (let [expected-instance (assoc expected-instance
                                  :extra-ports [1234 5678 7890]
                                  :port 4321)
              actual-job (update actual-job :labels assoc :backend-port "4321")]
          (is (= (scheduler/make-ServiceInstance expected-instance)
                 (job->service-instance actual-job))))))))

(deftest test-jobs->service
  (with-redefs [job-healthy? #(str/includes? (:name %) "healthy-")]
    (let [suffix (System/nanoTime)
          jobs [{:labels {:service-id "test-service-id"} :name (str "healthy-1." suffix) :status "running"}
                {:labels {:service-id "test-service-id"} :name (str "healthy-2." suffix) :status "running"}
                {:labels {:service-id "test-service-id"} :name (str "unhealthy." suffix) :status "running"}
                {:labels {:service-id "test-service-id"} :name (str "staging-1." suffix) :status "staging"}]]
      (is (= (scheduler/make-Service
               {:id "test-service-id"
                :instances 4
                :task-count 4
                :task-stats {:healthy 2 :running 3 :staged 1 :unhealthy 1}})
             (jobs->service jobs))))))

(defn- parse-and-store-failed-instance!
  "Parses the failed instance response and adds it to the known set of failed instances."
  [service-id->failed-instances-transient-store service-id failed-instance]
  (when failed-instance
    (let [max-instances-to-keep 10]
      (scheduler/add-instance-to-buffered-collection!
        service-id->failed-instances-transient-store
        max-instances-to-keep
        service-id
        failed-instance
        (fn [] #{})
        (fn [instances] (-> (scheduler/sort-instances instances) (rest) (set)))))))

(deftest test-service-id->failed-instances-transient-store
  (let [health-check-url "/status"
        failed-instance-response-fn (fn [service-id instance-id]
                                      {:health-check-path health-check-url
                                       :host (str "10.141.141." instance-id)
                                       :id (str service-id "." instance-id)
                                       :message "Abnormal executor termination"
                                       :port 0
                                       :service-id service-id
                                       :started-at (du/str-to-date "2014-09-12T23:23:41.711Z")
                                       :version "2014-09-12T23:28:21.737Z"})
        service-id-1 "test-service-id-failed-instances-1"
        service-id-2 "test-service-id-failed-instances-2"
        service-id->failed-instances-transient-store (atom {})]
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "A")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "A")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "B")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "A")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "B")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "C")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "D")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 4 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (scheduler/preserve-only-killed-instances-for-services! [])
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "A")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 1 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "B")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "A")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "A")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 2 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "C")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (->> (failed-instance-response-fn service-id-1 "D")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (->> (failed-instance-response-fn service-id-1 "E")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (->> (failed-instance-response-fn service-id-1 "F")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (->> (failed-instance-response-fn service-id-1 "G")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (->> (failed-instance-response-fn service-id-1 "H")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (->> (failed-instance-response-fn service-id-1 "I")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-1))
    (is (= 9 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))
    (->> (failed-instance-response-fn service-id-2 "X")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2))
    (->> (failed-instance-response-fn service-id-2 "Y")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2))
    (->> (failed-instance-response-fn service-id-2 "Z")
         (parse-and-store-failed-instance! service-id->failed-instances-transient-store service-id-2))
    (remove-failed-instances-for-service! service-id->failed-instances-transient-store service-id-1)
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))
    (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [service-id-2])
    (is (= 0 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-1))))
    (is (= 3 (count (service-id->failed-instances service-id->failed-instances-transient-store service-id-2))))))

(deftest test-max-failed-instances-cache
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time)
        service-id->failed-instances-transient-store (atom {})
        make-instance (fn [n]
                        {:id (str "service-1." n)
                         :service-id "service-1"
                         :started-at (du/str-to-date current-time-str)
                         :healthy? false
                         :port 0})]
    (testing "test-max-failed-instances-cache"
      (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store [])
      (doseq [n (range 10 50)]
        (parse-and-store-failed-instance!
          service-id->failed-instances-transient-store "service-1" (make-instance n)))
      (let [actual-failed-instances (set (service-id->failed-instances service-id->failed-instances-transient-store "service-1"))]
        (is (= 10 (count actual-failed-instances)))
        (doseq [n (range 40 50)]
          (is (contains? actual-failed-instances (make-instance n))
              (str "Failed instances does not contain instance service-1." n)))))))

(deftest test-retrieve-log-url
  (let [host "agent-1.com"
        mesos-api (Object.)]
    (with-redefs [mesos/get-agent-state
                  (fn [in-mesos-api in-host]
                    (is (= mesos-api in-mesos-api))
                    (is (= host in-host))
                    {:frameworks [{:completed_executors [{:directory "/path/to/instance1/directory"
                                                          :id "service-id-1.instance-id-1"}]
                                   :executors [{:directory "/path/to/instance2/directory"
                                                :id "service-id-1.instance-id-2"}]
                                   :role "cook"}
                                  {:completed_executors [{:directory "/marathon/instance1/directory"
                                                          :id "service-id-1.instance-id-1"}]
                                   :executors [{:directory "/marathon/instance2/directory"
                                                :id "service-id-1.instance-id-2"}]
                                   :name "marathon"}
                                  {:completed_executors [{:directory "/path/to/instance3/directory"
                                                          :id "service-id-1.instance-id-3"}]
                                   :executors [{:directory "/path/to/instance4/directory"
                                                :id "service-id-1.instance-id-4"}]
                                   :name "Cook-1.17.4"
                                   :role "*"}]})]
      (is (= "/path/to/instance1/directory"
             (mesos/retrieve-log-url mesos-api "service-id-1.instance-id-1" host "cook")))
      (is (= "/path/to/instance2/directory"
             (mesos/retrieve-log-url mesos-api "service-id-1.instance-id-2" host "cook")))
      (is (= "/path/to/instance3/directory"
             (mesos/retrieve-log-url mesos-api "service-id-1.instance-id-3" host "cook")))
      (is (= "/path/to/instance4/directory"
             (mesos/retrieve-log-url mesos-api "service-id-1.instance-id-4" host "cook"))))))

(defn create-cook-scheduler-helper
  [& {:as cook-config}]
  (-> {:allowed-priorities [60 55 50 45 40]
       :allowed-users #{"test-user"}
       :cook-api {}
       :home-path-prefix "/home/path/"
       :search-interval (t/minutes 10)
       :service-id->failed-instances-transient-store (atom {})
       :service-id->password-fn #(str % ".password")
       :service-id->service-description-fn (constantly {})}
      (merge cook-config)
      map->CookScheduler))

(deftest test-killed-instances-transient-store
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time)
        cook-api (Object.)
        test-user "test-user"
        cook-scheduler (create-cook-scheduler-helper
                         :cook-api cook-api
                         :service-id->service-description-fn (constantly {"run-as-user" test-user}))
        make-instance (fn [service-id instance-id]
                        {:cook/job-uuid instance-id
                         :id instance-id
                         :service-id service-id})]
    (with-redefs [delete-jobs (fn [in-cook-api run-as-user job-uuids]
                                (is (= cook-api in-cook-api))
                                (is (= test-user run-as-user))
                                (is (= 1 (count job-uuids)))
                                {:killed? true})
                  retrieve-jobs (fn [in-cook-api in-search-interval in-service-id _]
                                  (is (= cook-api in-cook-api))
                                  (is in-search-interval)
                                  (str/starts-with? in-service-id "service-"))
                  t/now (fn [] current-time)]
      (testing "tracking-instance-killed"

        (scheduler/preserve-only-killed-instances-for-services! [])

        (is (:killed? (scheduler/kill-instance cook-scheduler (make-instance "service-1" "service-1.A"))))
        (is (:killed? (scheduler/kill-instance cook-scheduler (make-instance "service-2" "service-2.A"))))
        (is (:killed? (scheduler/kill-instance cook-scheduler (make-instance "service-1" "service-1.C"))))
        (is (:killed? (scheduler/kill-instance cook-scheduler (make-instance "service-1" "service-1.B"))))

        (is (= [{:cook/job-uuid "service-1.A" :id "service-1.A" :service-id "service-1" :killed-at current-time-str}
                {:cook/job-uuid "service-1.B" :id "service-1.B" :service-id "service-1" :killed-at current-time-str}
                {:cook/job-uuid "service-1.C" :id "service-1.C" :service-id "service-1" :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:cook/job-uuid "service-2.A" :id "service-2.A" :service-id "service-2" :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-1")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:cook/job-uuid "service-2.A" :id "service-2.A" :service-id "service-2" :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))

        (is (:killed? (scheduler/kill-instance cook-scheduler (make-instance "service-3" "service-3.A"))))
        (is (:killed? (scheduler/kill-instance cook-scheduler (make-instance "service-3" "service-3.B"))))
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [{:cook/job-uuid "service-2.A" :id "service-2.A" :service-id "service-2" :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-2")))
        (is (= [{:cook/job-uuid "service-3.A" :id "service-3.A" :service-id "service-3" :killed-at current-time-str}
                {:cook/job-uuid "service-3.B" :id "service-3.B" :service-id "service-3" :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/remove-killed-instances-for-service! "service-2")
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [{:cook/job-uuid "service-3.A" :id "service-3.A" :service-id "service-3" :killed-at current-time-str}
                {:cook/job-uuid "service-3.B" :id "service-3.B" :service-id "service-3" :killed-at current-time-str}]
               (scheduler/service-id->killed-instances "service-3")))

        (scheduler/preserve-only-killed-instances-for-services! [])
        (is (= [] (scheduler/service-id->killed-instances "service-1")))
        (is (= [] (scheduler/service-id->killed-instances "service-2")))
        (is (= [] (scheduler/service-id->killed-instances "service-3")))))))

(deftest test-get-services->instances
  (let [{:keys [service-id->failed-instances-transient-store] :as scheduler} (create-cook-scheduler-helper)
        suffix (System/nanoTime)
        jobs [{:labels {:service-id "S1"} :name (str "healthy-S1.1." suffix) :status "running"}
              {:labels {:service-id "S1"} :name (str "healthy-S1.2." suffix) :status "running"}
              {:labels {:service-id "S1"} :name (str "unhealthy-S1." suffix) :status "running"}
              {:labels {:service-id "S1"} :name (str "staging-S1.1." suffix) :status "staging"}
              {:labels {:service-id "S2"} :name (str "healthy-S2.1." suffix) :status "running"}
              {:labels {:service-id "S2"} :name (str "staging-S2.1." suffix) :status "staging"}]]
    (with-redefs [job-healthy? #(str/includes? (:name %) "healthy-")
                  get-jobs (fn [cook-api _ states & {:keys [search-interval]}]
                             (is (= (:cook-api scheduler) cook-api))
                             (is (= ["running" "waiting"] states))
                             (is (= (:search-interval scheduler) search-interval))
                             jobs)
                  job->service-instance (fn [{:keys [name]}] {:id name})]

      (scheduler/process-instance-killed! {:id (str "killed-S1.1." suffix) :service-id "S1"})
      (scheduler/add-instance-to-buffered-collection!
        service-id->failed-instances-transient-store 1 "S2"
        {:id (str "failed-S2.1." suffix) :service-id "S2"}
        (fn [] #{})
        (fn [instances] (-> (scheduler/sort-instances instances) (rest) (set))))

      (try
        (let [service-1 (scheduler/make-Service
                          {:id "S1" :instances 4 :task-count 4 :task-stats {:healthy 3 :running 3 :staged 1 :unhealthy 0}})
              service-1-instances {:active-instances [{:id (str "healthy-S1.1." suffix)}
                                                      {:id (str "healthy-S1.2." suffix)}
                                                      {:id (str "unhealthy-S1." suffix)}
                                                      {:id (str "staging-S1.1." suffix)}]
                                   :failed-instances []
                                   :killed-instances (scheduler/service-id->killed-instances "S1")}
              service-2 (scheduler/make-Service
                          {:id "S2" :instances 2 :task-count 2 :task-stats {:healthy 1 :running 1 :staged 1 :unhealthy 0}})
              service-2-instances {:active-instances [{:id (str "healthy-S2.1." suffix)}
                                                      {:id (str "staging-S2.1." suffix)}]
                                   :failed-instances (service-id->failed-instances service-id->failed-instances-transient-store "S2")
                                   :killed-instances []}]
          (is (seq (scheduler/service-id->killed-instances "S1")))
          (is (seq (service-id->failed-instances service-id->failed-instances-transient-store "S2")))

          (is (= {service-1 service-1-instances, service-2 service-2-instances}
                 (scheduler/get-apps->instances scheduler))))
        (finally
          (scheduler/remove-killed-instances-for-service! "S1")
          (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store []))))))

(deftest test-get-service-instances
  (let [{:keys [service-id->failed-instances-transient-store] :as scheduler} (create-cook-scheduler-helper)
        suffix (System/nanoTime)
        jobs [{:labels {:service-id "S1"} :name (str "healthy-S1.1." suffix) :status "running"}
              {:labels {:service-id "S1"} :name (str "healthy-S1.2." suffix) :status "running"}
              {:labels {:service-id "S1"} :name (str "unhealthy-S1." suffix) :status "running"}
              {:labels {:service-id "S1"} :name (str "staging-S1.1." suffix) :status "staging"}]]
    (with-redefs [retrieve-jobs (constantly jobs)
                  job->service-instance (fn [{:keys [name]}] {:id name})]

      (scheduler/process-instance-killed! {:id (str "killed-S1.1." suffix) :service-id "S1"})
      (scheduler/add-instance-to-buffered-collection!
        service-id->failed-instances-transient-store 1 "S1"
        {:id (str "failed-S1.1." suffix) :service-id "S1"}
        (fn [] #{})
        (fn [instances] (-> (scheduler/sort-instances instances) (rest) (set))))
      (try
        (is (seq (scheduler/service-id->killed-instances "S1")))
        (is (seq (service-id->failed-instances service-id->failed-instances-transient-store "S1")))

        (is (= {:active-instances [{:id (str "healthy-S1.1." suffix)}
                                   {:id (str "healthy-S1.2." suffix)}
                                   {:id (str "unhealthy-S1." suffix)}
                                   {:id (str "staging-S1.1." suffix)}]
                :failed-instances (service-id->failed-instances service-id->failed-instances-transient-store "S1")
                :killed-instances (scheduler/service-id->killed-instances "S1")}
               (scheduler/get-instances scheduler "S1")))
        (finally
          (scheduler/remove-killed-instances-for-service! "S1")
          (preserve-only-failed-instances-for-services! service-id->failed-instances-transient-store []))))))

(deftest test-kill-instance
  (let [scheduler (create-cook-scheduler-helper)
        service-id "foo"
        test-instance {:cook/job-uuid "uuid-1" :id "foo.1A" :service-id service-id}
        current-time (t/now)]

    (with-redefs [retrieve-jobs (constantly [{:uuid "uuid-1"}
                                             {:uuid "uuid-2"}
                                             {:uuid "uuid-3"}])
                  t/now (constantly current-time)]
      (with-redefs [delete-jobs (constantly {:deploymentId 12345})]
        (scheduler/preserve-only-killed-instances-for-services! [])
        (is (= {:killed? true :message "Killed foo.1A" :result :killed :success true}
               (scheduler/kill-instance scheduler test-instance)))
        (is (= [(assoc test-instance :killed-at (du/date-to-str current-time))]
               (scheduler/service-id->killed-instances service-id)))
        (scheduler/preserve-only-killed-instances-for-services! []))

      (with-redefs [delete-jobs (constantly {})]
        (scheduler/preserve-only-killed-instances-for-services! [])
        (is (= {:killed? true :message "Killed foo.1A" :result :killed :success true}
               (scheduler/kill-instance scheduler test-instance)))
        (is (= [(assoc test-instance :killed-at (du/date-to-str current-time))]
               (scheduler/service-id->killed-instances service-id)))
        (scheduler/preserve-only-killed-instances-for-services! []))

      (with-redefs [delete-jobs (fn [_ _] (throw (ex-info "Delete error" {:status 400})))]
        (is (= {:killed? false :message "Unable to kill foo.1A" :result :failed :success false}
               (scheduler/kill-instance scheduler test-instance)))
        (is (empty? (scheduler/service-id->killed-instances service-id))))

      (with-redefs [delete-jobs (constantly {})
                    retrieve-jobs (constantly nil)]
        (is (= {:message "foo does not exist!" :result :no-such-service-exists :success false}
               (scheduler/kill-instance scheduler test-instance)))
        (is (empty? (scheduler/service-id->killed-instances service-id)))))))

(deftest test-create-instance
  (let [cook-api (Object.)
        test-user "test-user"
        cook-scheduler (create-cook-scheduler-helper :allowed-users #{test-user} :cook-api cook-api)
        service-id "foo"
        min-instances 4
        descriptor {:service-description {"min-instances" min-instances
                                          "run-as-user" test-user}
                    :service-id service-id}]

    (testing "create service - success"
      (let [updated-invoked-promise (promise)]
        (with-redefs [scheduler/app-exists? (constantly false)
                      launch-jobs (fn [in-cook-api in-service-id _ _ _ num-instances & _]
                                    (deliver updated-invoked-promise :invoked)
                                    (is (= cook-api in-cook-api))
                                    (is (= service-id in-service-id))
                                    (is (= min-instances num-instances))
                                    true)]
          (is (= {:message "Created foo" :result :created :success true}
                 (scheduler/create-app-if-new cook-scheduler descriptor)))
          (is (= :invoked (deref updated-invoked-promise 0 :not-invoked))))))

    (testing "create service - failure"
      (let [updated-invoked-promise (promise)]
        (with-redefs [scheduler/app-exists? (constantly false)
                      launch-jobs (fn [in-cook-api in-service-id _ _ _ num-instances & _]
                                    (deliver updated-invoked-promise :invoked)
                                    (is (= cook-api in-cook-api))
                                    (is (= service-id in-service-id))
                                    (is (= min-instances num-instances))
                                    (throw (ex-info "Failed" {})))]
          (is (= {:message "Unable to create foo" :result :failed :success false}
                 (scheduler/create-app-if-new cook-scheduler descriptor)))
          (is (= :invoked (deref updated-invoked-promise 0 :not-invoked))))))

    (testing "create service - service exists"
      (with-redefs [retrieve-jobs (fn [_ _ in-service-id & _] (= service-id in-service-id))]
        (is (scheduler/app-exists? cook-scheduler service-id))
        (is (= {:message "foo already exists!" :result :already-exists :success false}
               (scheduler/create-app-if-new cook-scheduler descriptor)))))))

(deftest test-delete-service
  (with-redefs [retrieve-jobs (constantly [{:uuid "uuid-1"}
                                           {:uuid "uuid-2"}
                                           {:uuid "uuid-3"}])]
    (let [scheduler (create-cook-scheduler-helper)]

      (with-redefs [delete-jobs (constantly {:deploymentId 12345})]
        (is (= {:message "Deleted foo" :result :deleted :success true}
               (scheduler/delete-app scheduler "foo"))))

      (with-redefs [delete-jobs (constantly {})]
        (is (= {:message "Deleted foo" :result :deleted :success true}
               (scheduler/delete-app scheduler "foo"))))

      (with-redefs [delete-jobs (fn [_ _] (throw (ex-info "Delete error" {:status 400})))]
        (is (= {:message "Unable to delete foo" :result :failed :success false}
               (scheduler/delete-app scheduler "foo"))))

      (with-redefs [delete-jobs (constantly {})
                    retrieve-jobs (constantly nil)]
        (is (= {:message "foo does not exist!" :result :no-such-service-exists :success false}
               (scheduler/delete-app scheduler "foo")))))))

(deftest test-scale-service
  (let [cook-api (Object.)
        cook-scheduler (create-cook-scheduler-helper :cook-api cook-api)
        service-id "test-service-id"]

    (testing "scale of service - no such service"
      (let [updated-invoked-promise (promise)
            instances 5]
        (with-redefs [retrieve-jobs (fn [in-cook-api _ in-service-id _]
                                      (is (= cook-api in-cook-api))
                                      (is (= service-id in-service-id))
                                      nil)
                      launch-jobs (fn [in-cook-api in-service-id & _]
                                    (deliver updated-invoked-promise :invoked)
                                    (is (= cook-api in-cook-api))
                                    (is (= service-id in-service-id))
                                    true)]
          (is (= {:message "test-service-id does not exist!" :result :no-such-service-exists :success false}
                 (scheduler/scale-app cook-scheduler service-id instances false)))
          (is (= :not-invoked (deref updated-invoked-promise 0 :not-invoked))))))

    (testing "scale of service - no-op"
      (let [updated-invoked-promise (promise)
            instances 5]
        (with-redefs [retrieve-jobs (fn [in-cook-api _ in-service-id _]
                                      (is (= cook-api in-cook-api))
                                      (is (= service-id in-service-id))
                                      (for [n (range 10)]
                                        {:cook/job-uuid (str "uuid-" n)
                                         :id (str service-id "." n)
                                         :priority n
                                         :service-id service-id}))
                      launch-jobs (fn [in-cook-api in-service-id _ _ _ extra-instances & _]
                                    (deliver updated-invoked-promise :invoked)
                                    (is (= cook-api in-cook-api))
                                    (is (= service-id in-service-id))
                                    (is (= 20 extra-instances))
                                    true)]
          (is (= {:message "Scaled test-service-id" :result :scaling-not-needed :success true}
                 (scheduler/scale-app cook-scheduler service-id instances false)))
          (is (= :not-invoked (deref updated-invoked-promise 0 :not-invoked))))))

    (testing "scale of service - success"
      (let [updated-invoked-promise (promise)
            instances 30]
        (with-redefs [retrieve-jobs (fn [in-cook-api _ in-service-id _]
                                      (is (= cook-api in-cook-api))
                                      (is (= service-id in-service-id))
                                      (for [n (range 10)]
                                        {:cook/job-uuid (str "uuid-" n)
                                         :id (str service-id "." n)
                                         :priority n
                                         :service-id service-id}))
                      launch-jobs (fn [in-cook-api in-service-id _ _ _ extra-instances & _]
                                    (deliver updated-invoked-promise :invoked)
                                    (is (= cook-api in-cook-api))
                                    (is (= service-id in-service-id))
                                    (is (= 20 extra-instances))
                                    true)]
          (is (= {:message "Scaled test-service-id" :result :scaled :success true}
                 (scheduler/scale-app cook-scheduler service-id instances false)))
          (is (= :invoked (deref updated-invoked-promise 0 :not-invoked))))))

    (testing "scale of service - fail"
      (let [updated-invoked-promise (promise)
            instances 30]
        (with-redefs [retrieve-jobs (fn [in-cook-api _ in-service-id _]
                                      (is (= cook-api in-cook-api))
                                      (is (= service-id in-service-id))
                                      (for [n (range 10)]
                                        {:cook/job-uuid (str "uuid-" n)
                                         :id (str service-id "." n)
                                         :priority n
                                         :service-id service-id}))
                      launch-jobs (fn [in-cook-api in-service-id _ _ _ extra-instances & _]
                                    (deliver updated-invoked-promise :invoked)
                                    (is (= cook-api in-cook-api))
                                    (is (= service-id in-service-id))
                                    (is (= 20 extra-instances))
                                    (throw (ex-info "Launch failed!" {})))]
          (is (= {:message "Unable to scale test-service-id" :result :failed :success false}
                 (scheduler/scale-app cook-scheduler service-id instances false)))
          (is (= :invoked (deref updated-invoked-promise 0 :not-invoked))))))))

(deftest test-service-id->state
  (let [service-id "service-id"
        cook-scheduler (create-cook-scheduler-helper
                         :service-id->failed-instances-transient-store (atom {service-id [:failed-instances]}))]
    (scheduler/preserve-only-killed-instances-for-services! [])
    (is (= {:failed-instances [:failed-instances]
            :killed-instances []}
           (scheduler/service-id->state cook-scheduler service-id)))
    (is (= {:service-id->failed-instances-transient-store {"service-id" [:failed-instances]}
            :service-id->killed-instances-transient-store {}}
           (scheduler/state cook-scheduler)))))

(deftest test-cook-scheduler
  (testing "Creating a CookScheduler"
    (let [valid-config {:allowed-users #{"test-user"}
                        :cook-api {}
                        :home-path-prefix "/home/path/"
                        :instance-priorities {:delta 5
                                              :max 70
                                              :min 30}
                        :search-interval-days 10
                        :service-id->password-fn #(str % ".password")
                        :service-id->service-description-fn (constantly {})}
          cook-api (Object.)
          service-id->failed-instances-transient-store (atom {})
          create-cook-scheduler-helper (fn create-cook-scheduler-helper [config]
                                         (create-cook-scheduler config cook-api service-id->failed-instances-transient-store))]

      (testing "should throw on invalid configuration"
        (is (thrown? Throwable (create-cook-scheduler-helper (assoc valid-config :allowed-users #{}))))
        (is (thrown? Throwable (create-cook-scheduler-helper (assoc valid-config :home-path-prefix nil))))
        (is (thrown? Throwable (create-cook-scheduler-helper (assoc valid-config :instance-priorities {}))))
        (is (thrown? Throwable (create-cook-scheduler-helper (assoc valid-config :search-interval-days 0)))))

      (testing "should work with valid configuration"
        (is (instance? CookScheduler (create-cook-scheduler-helper valid-config)))))))
