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
(ns waiter.basic-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [waiter.interstitial :as interstitial]
            [waiter.service-description :as sd]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (java.io ByteArrayInputStream)
           (java.net URLEncoder)))

(deftest ^:parallel ^:integration-fast test-basic-functionality
  (testing-using-waiter-url
    (let [{:keys [service-id request-headers]}
          (make-request-with-debug-info
            {:x-waiter-name (rand-name)}
            #(make-kitchen-request waiter-url % :path "/hello"))]

      (let [service-settings (service-settings waiter-url service-id)]
        (testing "instances are non-null"
          (is (get-in service-settings [:instances :active-instances]))
          (is (get-in service-settings [:instances :failed-instances]))
          (is (get-in service-settings [:instances :killed-instances]))))

      (testing "status is reported"
        (is (wait-for #(= "Running" (get (service-settings waiter-url service-id) :status)) :interval 2 :timeout 30)
            (str "Service status is " (get (service-settings waiter-url service-id) :status))))

      (testing "explicitly specifying default parameter resolves to different service"
        (let [{:keys [service-description-defaults]} (waiter-settings waiter-url)
              request-headers (walk/stringify-keys request-headers)]
          (doseq [[k v] service-description-defaults]
            (let [parameter-key (str "x-waiter-" (name k))]
              (when-not (or (contains? request-headers parameter-key)
                            (map? v)
                            (vector? v))
                (let [new-request-headers (assoc request-headers parameter-key v)
                      new-service-id (retrieve-service-id waiter-url new-request-headers)]
                  (is (not= service-id new-service-id)
                      (str {:new-parameter [k v] :request-headers request-headers}))))))))

      (testing "empty-body"
        (log/info "Basic test for empty body in request")
        (let [request-headers (assoc request-headers :accept "text/plain")
              {:keys [body headers]} (make-kitchen-request waiter-url request-headers :path "/request-info")
              body-json (json/read-str (str body))]
          (is (= "application/json" (get headers "content-type")) (str headers))
          (is (every? #(get-in body-json ["headers" %]) ["authorization" "x-cid" "x-waiter-auth-principal"]) (str body))
          (is (nil? (get-in body-json ["headers" "content-type"])) (str body))
          (is (= "0" (get-in body-json ["headers" "content-length"])) (str body))
          (is (= "text/plain" (get-in body-json ["headers" "accept"])) (str body))))

      (testing "string-body"
        (let [body-content "Hello.World.Lorem.Ipsum"]
          (log/info "Basic test for string body in request")
          (let [request-headers (assoc request-headers :accept "text/plain")
                {:keys [body headers]} (make-kitchen-request waiter-url request-headers :body body-content :path "/request-info")
                body-json (json/read-str (str body))]
            (is (= "application/json" (get headers "content-type")) (str headers))
            (is (every? #(get-in body-json ["headers" %]) ["authorization" "x-cid" "x-waiter-auth-principal"]) (str body))
            (is (nil? (get-in body-json ["headers" "content-type"])) (str body))
            (is (= (-> body-content count str) (get-in body-json ["headers" "content-length"])) (str body))
            (is (= "text/plain" (get-in body-json ["headers" "accept"])) (str body)))

          (let [request-headers (assoc request-headers :accept "text/plain" :content-type "text/plain")
                {:keys [body headers]} (make-kitchen-request waiter-url request-headers :body body-content :path "/request-info")
                body-json (json/read-str (str body))]
            (is (= "application/json" (get headers "content-type")) (str headers))
            (is (every? #(get-in body-json ["headers" %]) ["authorization" "x-cid" "x-waiter-auth-principal"]) (str body))
            (is (= "text/plain" (get-in body-json ["headers" "content-type"])) (str body))
            (is (= (-> body-content count str) (get-in body-json ["headers" "content-length"])) (str body))
            (is (= "text/plain" (get-in body-json ["headers" "accept"])) (str body)))

          (let [request-headers (assoc request-headers :x-kitchen-content-type "text/plain" :x-kitchen-echo "true")
                {:keys [body headers]} (make-kitchen-request waiter-url request-headers :body body-content :path "/echo-data")]
            (is (= "text/plain" (get headers "content-type")) (str headers))
            (is (= body-content (str body)) (str body)))

          (let [request-headers (assoc request-headers :x-kitchen-content-type "text/foo-bar" :x-kitchen-echo "true")
                {:keys [body headers]} (make-kitchen-request waiter-url request-headers :body body-content :path "/echo-data")]
            (is (= "text/foo-bar" (get headers "content-type")) (str headers))
            (is (= body-content (str body)) (str body)))))

      (testing "query-string with special characters"
        (log/info "Basic test for query-string with special characters")
        (let [bad-query-string "q=~`!@$%^&*()_-+={}[]|:;'<>,.?&foo=%12jhsdf"
              {:keys [body] :as response} (make-kitchen-request
                                            waiter-url
                                            (assoc request-headers :accept "application/json")
                                            :path "/request-info"
                                            :query-params bad-query-string)]
          (assert-response-status response 200)
          (is (= bad-query-string (get (json/read-str body) "query-string"))))

        (log/info "Basic test for query-string with encoded characters")
        (let [bad-query-string (str "q=" (URLEncoder/encode "~`!@$%^&*()_-+={}[]|:;'<>,.?&foo=%12jhsdf"))
              {:keys [body] :as response} (make-kitchen-request
                                            waiter-url
                                            (assoc request-headers :accept "application/json")
                                            :path "/request-info"
                                            :query-params bad-query-string)]
          (assert-response-status response 200)
          (is (= bad-query-string (get (json/read-str body) "query-string")))))

      (testing "http methods"
        (log/info "Basic test for empty body in request")
        (testing "http method: HEAD"
          (let [response (make-kitchen-request waiter-url request-headers :method :head :path "/request-info")]
            (assert-response-status response 200)
            (is (str/blank? (:body response)))))
        (doseq [request-method [:delete :copy :get :move :patch :post :put]]
          (testing (str "http method: " (-> request-method name str/upper-case))
            (let [{:keys [body] :as response}
                  (make-kitchen-request waiter-url request-headers :method request-method :path "/request-info")
                  body-json (json/read-str (str body))]
              (assert-response-status response 200)
              (is (= (name request-method) (get body-json "request-method")))))))

      (testing "content headers"
        (let [request-length 100000
              long-request (apply str (repeat request-length "a"))
              plain-resp (make-kitchen-request
                           waiter-url request-headers
                           :path "/request-info"
                           :body long-request)
              chunked-resp (make-kitchen-request
                             waiter-url
                             request-headers
                             :path "/request-info"
                             ; force a chunked request
                             :body (ByteArrayInputStream. (.getBytes long-request)))
              plain-body-json (json/read-str (str (:body plain-resp)))
              chunked-body-json (json/read-str (str (:body chunked-resp)))]
          (is (= (str request-length) (get-in plain-body-json ["headers" "content-length"])))
          (is (nil? (get-in plain-body-json ["headers" "transfer-encoding"])))
          (is (= "chunked" (get-in chunked-body-json ["headers" "transfer-encoding"])))
          (is (nil? (get-in chunked-body-json ["headers" "content-length"])))))

      (testing "large header"
        (let [all-chars (map char (range 33 127))
              random-string (fn [n] (reduce str (take n (repeatedly #(rand-nth all-chars)))))
              make-request (fn [header-size]
                             (log/info "making request with header size" header-size)
                             (make-kitchen-request waiter-url
                                                   (assoc request-headers :x-kitchen-long-string
                                                                          (random-string header-size))))]
          (let [response (make-request 2000)]
            (assert-response-status response 200))
          (let [response (make-request 4000)]
            (assert-response-status response 200))
          (let [response (make-request 8000)]
            (assert-response-status response 200))
          (let [response (make-request 16000)]
            (assert-response-status response 200))
          (let [response (make-request 20000)]
            (assert-response-status response 200))
          (let [response (make-request 24000)]
            (assert-response-status response 200))))

      (testing "https-redirect header is a no-op"
        (let [request-headers (-> request-headers
                                  (assoc "x-waiter-https-redirect" "true")
                                  (dissoc "x-cid"))
              endpoint "/request-info"]

          (testing "get request"
            (let [{:keys [headers] :as response}
                  (make-kitchen-request waiter-url request-headers :method :get :path endpoint)]
              (assert-response-status response 200)
              (is (not (str/starts-with? (str (get headers "server")) "waiter")) (str "headers:" headers))))

          (testing "post request"
            (let [{:keys [headers] :as response}
                  (make-kitchen-request waiter-url request-headers :method :post :path endpoint)]
              (assert-response-status response 200)
              (is (not (str/starts-with? (str (get headers "server")) "waiter")) (str "headers:" headers))))))

      (testing "metric group should be waiter_kitchen_test"
        (is (= "waiter_kitchen_test" (service-id->metric-group waiter-url service-id))
            (str "Invalid metric group for " service-id)))

      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-basic-logs
  (testing-using-waiter-url
    (let [waiter-headers {:x-waiter-name (rand-name)}
          {:keys [cookies service-id]} (make-request-with-debug-info waiter-headers #(make-kitchen-request waiter-url %))]
      (assert-service-on-all-routers waiter-url service-id cookies)
      (let [active-instances (get-in (service-settings waiter-url service-id :cookies cookies)
                                     [:instances :active-instances])
            log-url (:log-url (first active-instances))
            _ (log/debug "Log Url:" log-url)
            make-request-fn (fn [url] (make-request url "" :verbose true))
            {:keys [body] :as logs-response} (make-request-fn log-url)
            _ (assert-response-status logs-response 200)
            _ (log/debug "Response body:" body)
            log-files-list (walk/keywordize-keys (json/read-str body))
            stdout-file-link (:url (first (filter #(= (:name %) "stdout") log-files-list)))
            stderr-file-link (:url (first (filter #(= (:name %) "stderr") log-files-list)))]
        (is (every? #(str/includes? body %) ["stderr" "stdout"])
            (str "Directory listing is missing entries: stderr and stdout, got response: " logs-response))
        (when (using-marathon? waiter-url)
          (is (str/includes? body service-id)
              (str "Directory listing is missing entries: " service-id
                   ": got response: " logs-response)))
        (doseq [file-link [stderr-file-link stdout-file-link]]
          (if (str/starts-with? (str file-link) "http")
            (assert-response-status (make-request-fn file-link) 200)
            (log/warn "test-basic-logs did not verify file link:" stdout-file-link))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-basic-backoff-config
  (let [path "/req"]
    (testing-using-waiter-url
      (log/info (str "Basic backoff config test using endpoint: " path))
      (let [{:keys [service-id] :as response}
            (make-request-with-debug-info
              {:x-waiter-name (rand-name)
               :x-waiter-restart-backoff-factor 2.5}
              #(make-kitchen-request waiter-url % :path path))
            service-settings (service-settings waiter-url service-id)]
        (assert-response-status response 200)
        (is (= 2.5 (get-in service-settings [:service-description :restart-backoff-factor])))
        (delete-service waiter-url service-id)))))

(deftest ^:parallel ^:integration-fast test-basic-shell-command
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-cmd (kitchen-cmd "-p $PORT0")}
          {:keys [service-id] :as response} (make-request-with-debug-info headers #(make-shell-request waiter-url %))]
      (is (not (nil? service-id)))
      (assert-response-status response 200)

      (let [service-settings (service-settings waiter-url service-id)]
        (is (= (:x-waiter-cmd headers) (get-in service-settings [:service-description :cmd])))
        (is (nil? (get service-settings :effective-parameters))))

      (let [service-settings (service-settings waiter-url service-id
                                               :query-params {"effective-parameters" "true"})]
        (is (= (:x-waiter-cmd headers) (get-in service-settings [:service-description :cmd])))
        (is (not-empty (get service-settings :effective-parameters)))
        (is (= (:x-waiter-cmd headers) (get-in service-settings [:effective-parameters :cmd])))
        (is (= "other" (get-in service-settings [:effective-parameters :metric-group])) service-id))

      (testing "metric group should be other"
        (is (= "other" (service-id->metric-group waiter-url service-id))
            (str "Invalid metric group for " service-id)))

      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-basic-health-check-port-index
  (testing-using-waiter-url
    (when (using-shell? waiter-url)
      (let [headers {:x-waiter-cmd (str "while true; do echo -e 'HTTP/1.1 400 OK\\r\\n\\r\\nFrom PORT0: ${PORT0}' | nc -l $PORT0; done & "
                                        "while true; do echo -e 'HTTP/1.1 200 OK\\r\\n\\r\\nFrom PORT1: ${PORT1}' | nc -l $PORT1; done")
                     :x-waiter-name (rand-name)
                     :x-waiter-health-check-port-index 1
                     :x-waiter-ports 2}
            {:keys [body service-id] :as response} (make-request-with-debug-info headers #(make-shell-request waiter-url %))]
        (is (not (nil? service-id)))
        (assert-response-status response 400)
        (is (str/includes? (str body) "From PORT0"))

        (let [service-settings (service-settings waiter-url service-id)]
          (is (= (:x-waiter-cmd headers) (get-in service-settings [:service-description :cmd])))
          (is (= 1 (get-in service-settings [:service-description :health-check-port-index])))
          (is (every? #(= 1 (:health-check-port-index %)) (get-in service-settings [:instances :active-instance]))))

        (delete-service waiter-url service-id)))))

(deftest ^:parallel ^:integration-fast test-basic-unsupported-command-type
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-version "1"
                   :x-waiter-cmd "false"
                   :x-waiter-cmd-type "fakecommand"}
          {:keys [body status]} (make-light-request waiter-url headers)]
      (is (= 400 status))
      (is (str/includes? body "Command type fakecommand is not supported")))))

(deftest ^:parallel ^:integration-fast test-basic-parameters-violates-max-constraint
  (testing-using-waiter-url
    (let [constraints (setting waiter-url [:service-description-constraints])
          max-constraints (sd/extract-max-constraints constraints)]
      (is (seq max-constraints))
      (doseq [[parameter max-constraint] (rest max-constraints)]
        (let [headers {:x-waiter-cmd "false"
                       :x-waiter-cmd-type "shell"
                       :x-waiter-name (rand-name)
                       :x-waiter-version "1"
                       (keyword (str "x-waiter-" (name parameter))) (inc max-constraint)}
              {:keys [body status]} (make-light-request waiter-url headers)]
          (is (= 400 status))
          (is (not (str/includes? body "clojure")) body)
          (is (every? #(str/includes? body %)
                      ["The following fields exceed their allowed limits"
                       (str (name parameter) " is " (inc max-constraint) " but the max allowed is " max-constraint)])
              body))))))

(deftest ^:parallel ^:integration-fast test-header-metadata
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-metadata-foo "bar"
                   :x-waiter-metadata-baz "quux"
                   :x-waiter-metadata-beginDate "null"
                   :x-waiter-metadata-endDate "null"
                   :x-waiter-metadata-timestamp "20160713201333949"}
          {:keys [status service-id] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          value (:metadata (response->service-description waiter-url response))]
      (is (= 200 status))
      (is (= {:foo "bar", :baz "quux", :begindate "null", :enddate "null", :timestamp "20160713201333949"} value))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-header-environment
  (testing-using-waiter-url
    (testing "valid values"
      (let [headers {:x-waiter-name (rand-name)
                     :x-waiter-env-begin_date "foo"
                     :x-waiter-env-end_date "null"
                     :x-waiter-env-timestamp "20160713201333949"
                     :x-waiter-env-time2 "201607132013"}
            {:keys [body status service-id] :as response}
            (make-request-with-debug-info headers #(make-kitchen-request waiter-url % :path "/environment"))
            body-json (json/read-str (str body))]
        (is (= 200 status))
        (testing "waiter configured environment variables"
          (is (every? #(contains? body-json %)
                      ["HOME" "LOGNAME" "USER" "WAITER_CPUS" "WAITER_MEM_MB" "WAITER_SERVICE_ID" "WAITER_USERNAME"])
              (str body-json)))
        (testing "on-the-fly environment variables"
          (is (every? #(contains? body-json %) ["BEGIN_DATE" "END_DATE" "TIME2" "TIMESTAMP"])
              (str body-json)))
        (is (= {:BEGIN_DATE "foo" :END_DATE "null" :TIME2 "201607132013" :TIMESTAMP "20160713201333949"}
               (:env (response->service-description waiter-url response))))
        (delete-service waiter-url service-id)))

    (testing "invalid values"
      (let [headers {:accept "application/json"
                     :x-waiter-name (rand-name)
                     :x-waiter-env-begin-date "foo"
                     :x-waiter-env-1_invalid "20160713201333949"
                     :x-waiter-env-123456 "20160713201333949"
                     :x-waiter-env-end-date "null"
                     :x-waiter-env-foo "bar"
                     :x-waiter-env-fee_fie "fum"}
            {:keys [body status]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
            env-error-message (get-in (json/read-str body) ["waiter-error" "message"])]
        (is (= 400 status))
        (is (every? #(str/includes? env-error-message %)
                    ["The following environment variable keys are invalid:" "1_INVALID" "123456" "BEGIN-DATE" "END-DATE"]))
        (is (not-any? #(str/includes? (str/lower-case env-error-message) %) ["foo" "fee_fie"]))))))

(deftest ^:parallel ^:integration-fast test-last-request-time
  (testing-using-waiter-url
    (let [waiter-settings (waiter-settings waiter-url)
          metrics-sync-interval-ms (get-in waiter-settings [:metrics-config :metrics-sync-interval-ms])
          service-name (rand-name)
          headers {:x-kitchen-delay-ms (* 4 metrics-sync-interval-ms)
                   :x-waiter-name service-name
                   :x-waiter-cmd (kitchen-cmd "-p $PORT0")}
          {:keys [headers request-headers service-id] :as first-response}
          (make-request-with-debug-info headers #(make-kitchen-request waiter-url % :method :get))
          _ (assert-response-status first-response 200)
          canary-request-time-from-header (-> (get headers "x-waiter-request-date")
                                              (du/str-to-date du/formatter-rfc822))]
      (with-service-cleanup
        service-id
        (is (pos? metrics-sync-interval-ms))
        (let [service-last-request-time (service-id->last-request-time waiter-url service-id)]
          (is (pos? (.getMillis canary-request-time-from-header)))
          (is (pos? (.getMillis service-last-request-time)))
          (is (zero? (t/in-seconds (t/interval canary-request-time-from-header service-last-request-time)))))
        (make-kitchen-request waiter-url request-headers :method :get)
        (let [service-last-request-time (service-id->last-request-time waiter-url service-id)]
          (is (pos? (.getMillis service-last-request-time)))
          (is (t/before? canary-request-time-from-header service-last-request-time)))
        (let [service-settings (service-settings waiter-url service-id)
              service-last-request-time (-> service-settings :last-request-time du/str-to-date)]
          (is (pos? (.getMillis service-last-request-time)))
          (is (t/before? canary-request-time-from-header service-last-request-time)))))))

(deftest ^:parallel ^:integration-fast test-list-apps
  (let [current-user (retrieve-username)]
    (testing-using-waiter-url
      (let [service-id (:service-id (make-request-with-debug-info
                                      {:x-waiter-name (rand-name)}
                                      #(make-kitchen-request waiter-url %)))]
        (testing "without parameters"
          (let [service (service waiter-url service-id {})] ;; see my app as myself
            (is service)
            (is (contains? #{"Running" "Starting"} (get service "status")))
            (is (-> (get service "last-request-time") du/str-to-date .getMillis pos?))
            (is (pos? (get-in service ["service-description" "cpus"])) service)))

        (testing "with star run-as-user parameter"
          (let [run-as-user-param (->> current-user reverse (drop 2) (cons "*") reverse (str/join ""))
                service (service waiter-url service-id {"run-as-user" run-as-user-param})] ;; see my app as myself
            (is service)
            (is (contains? #{"Running" "Starting"} (get service "status")))
            (is (-> (get service "last-request-time") du/str-to-date .getMillis pos?))
            (is (pos? (get-in service ["service-description" "cpus"])) service)))

        (testing "waiter user disabled" ;; see my app as myself
          (let [service (service waiter-url service-id {"force" "false"})]
            (is service)
            (is (contains? #{"Running" "Starting"} (get service "status")))
            (is (-> (get service "last-request-time") du/str-to-date .getMillis pos?))
            (is (pos? (get-in service ["service-description" "cpus"])) service)))

        (testing "waiter user disabled and same user" ;; see my app as myself
          (let [service (service waiter-url service-id {"force" "false", "run-as-user" current-user})]
            (is service)
            (is (contains? #{"Running" "Starting"} (get service "status")))
            (is (-> (get service "last-request-time") du/str-to-date .getMillis pos?))
            (is (pos? (get-in service ["service-description" "cpus"])) service)))

        (testing "different run-as-user" ;; no such app
          (let [service (service waiter-url service-id {"run-as-user" "test-user"}
                                 :interval 2, :timeout 10)]
            (is (nil? service))))

        (testing "should not provide effective service description by default"
          (let [service (service waiter-url service-id {})]
            (is (nil? (get service "effective-parameters")))))

        (testing "should not provide effective service description when explicitly not requested"
          (let [service (service waiter-url service-id {"effective-parameters" "false"})]
            (is (nil? (get service "effective-parameters")))))

        (testing "should provide effective service description when requested"
          (let [service (service waiter-url service-id {"effective-parameters" "true"})]
            (is (= (disj sd/service-parameter-keys "scheduler" "image")
                   (set (keys (get service "effective-parameters")))))))

        (delete-service waiter-url service-id))

      (let [service-id (:service-id (make-request-with-debug-info
                                      {:x-waiter-name (rand-name)
                                       :x-waiter-run-as-user current-user}
                                      #(make-kitchen-request waiter-url %)))]
        (testing "list-apps-with-waiter-user-disabled-and-see-another-app" ;; can see another user's app
          (let [service (service waiter-url service-id {"force" "false", "run-as-user" current-user})]
            (is service)
            (is (contains? #{"Running" "Starting"} (get service "status")))
            (is (-> (get service "last-request-time") du/str-to-date .getMillis pos?))
            (is (pos? (get-in service ["service-description" "cpus"])) service)))
        (delete-service waiter-url service-id)))))

(deftest ^:parallel ^:integration-fast test-delete-service
  (testing-using-waiter-url
    "test-delete-service"
    (let [{:keys [service-id cookies]}
          (make-request-with-debug-info {:x-waiter-name (rand-name)} #(make-kitchen-request waiter-url %))
          router-id->router-url (routers waiter-url)]
      (with-service-cleanup
        service-id
        (testing "service-known-on-all-routers"
          (let [router-id->service-id
                (pc/map-from-keys
                  (fn [router-id]
                    (wait-for
                      (fn []
                        (let [router-url (router-id->router-url router-id)
                              {:keys [body]} (make-request router-url "/apps" :cookies cookies)]
                          (->> (json/read-str (str body))
                               (filter #(= service-id (get % "service-id")))
                               first
                               walk/keywordize-keys
                               :service-id)))
                      :interval 2 :timeout 30))
                  (keys router-id->router-url))]
            (is (every? #(= service-id (val %)) router-id->service-id)
                (str "Cannot find service: " service-id " in at least one router: " router-id->service-id))))

        (testing "delete service"
          (is (wait-for
                (fn []
                  (-> (make-request waiter-url (str "/apps/" service-id) :method :delete)
                      :status
                      (= 200)))
                :interval 5 :timeout 60)))

        (testing "service-deleted-from-all-routers"
          (let [router-id->service-id-deleted
                (pc/map-from-keys
                  (fn [router-id]
                    (wait-for
                      (fn []
                        (let [router-url (router-id->router-url router-id)
                              {:keys [body]} (make-request router-url "/apps" :cookies cookies)]
                          (->> (json/read-str (str body))
                               (filter #(= service-id (get % "service-id")))
                               seq
                               not)))
                      :interval 2 :timeout 30))
                  (keys router-id->router-url))]
            (is (every? #(true? (val %)) router-id->service-id-deleted)
                (str service-id " present in at least one router: " router-id->service-id-deleted))))))))

(deftest ^:parallel ^:integration-fast test-suspend-resume
  (testing-using-waiter-url
    (let [waiter-headers {:x-waiter-name (rand-name)}
          {:keys [service-id]} (make-request-with-debug-info waiter-headers #(make-kitchen-request waiter-url %))]
      (let [results (parallelize-requests 10 2
                                          #(let [response (make-kitchen-request waiter-url waiter-headers)]
                                             (= 200 (:status response)))
                                          :verbose true)]
        (is (every? true? results)))
      (log/info "Suspending service " service-id)
      (make-request waiter-url (str "/apps/" service-id "/suspend"))
      (let [results (parallelize-requests 10 2
                                          #(let [{:keys [body]} (make-kitchen-request waiter-url waiter-headers)]
                                             (str/includes? body "Service has been suspended"))
                                          :verbose true)]
        (is (every? true? results)))
      (log/info "Resuming service " service-id)
      (make-request waiter-url (str "/apps/" service-id "/resume"))
      (let [results (parallelize-requests 10 2
                                          #(let [_ (log/info "making kitchen request")
                                                 response (make-kitchen-request waiter-url waiter-headers)]
                                             (= 200 (:status response)))
                                          :verbose true)]
        (is (every? true? results)))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-override
  (testing-using-waiter-url
    (let [waiter-headers {:x-waiter-name (rand-name)}
          overrides {:max-instances 100
                     :min-instances 2
                     :scale-factor 0.3}
          {:keys [service-id]} (make-request-with-debug-info waiter-headers #(make-kitchen-request waiter-url %))
          override-endpoint (str "/apps/" service-id "/override")]
      (with-service-cleanup
        service-id
        (-> (make-request waiter-url override-endpoint :body (utils/clj->json overrides) :method :post)
            (assert-response-status 200))
        (let [{:keys [body] :as response}
              (make-request waiter-url override-endpoint :body (utils/clj->json overrides) :method :get)]
          (assert-response-status response 200)
          (let [response-data (-> body str json/read-str walk/keywordize-keys)]
            (is (= (retrieve-username) (:last-updated-by response-data)))
            (is (= overrides (:overrides response-data)))
            (is (= service-id (:service-id response-data)))
            (is (contains? response-data :time))))
        (let [service-settings (service-settings waiter-url service-id)
              service-description-overrides (-> service-settings :service-description-overrides :overrides)]
          (is (= overrides service-description-overrides)))
        (-> (make-request waiter-url override-endpoint :method :delete)
            (assert-response-status 200))
        (let [service-settings (service-settings waiter-url service-id)
              service-description-overrides (-> service-settings :service-description-overrides :overrides)]
          (is (not service-description-overrides)))))))

(deftest ^:parallel ^:integration-fast basic-waiter-auth-test
  (testing-using-waiter-url
    (log/info "Basic waiter-auth test")
    (let [{:keys [status body headers]} (make-request waiter-url "/waiter-auth")
          set-cookie (get headers "set-cookie")]
      (is (= 200 status))
      (is (str/includes? set-cookie "x-waiter-auth="))
      (is (str/includes? set-cookie "Max-Age="))
      (is (str/includes? set-cookie "Path=/"))
      (is (str/includes? set-cookie "HttpOnly=true"))
      (is (= (System/getProperty "user.name") (str body))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-killed-instances
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-max-instances 5
                   :x-waiter-scale-up-factor 0.99
                   :x-waiter-scale-down-factor 0.99
                   :x-kitchen-delay-ms 5000}
          _ (log/info "making canary request...")
          {:keys [service-id]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          request-fn (fn [] (->> #(make-kitchen-request waiter-url %)
                                 (make-request-with-debug-info headers)
                                 :instance-id))]
      (with-service-cleanup
        service-id
        (log/info "starting parallel requests")
        (let [instance-ids-atom (atom #{})
              instance-request-fn (fn []
                                    (let [instance-id (request-fn)]
                                      (swap! instance-ids-atom conj instance-id)))
              instance-ids (->> (parallelize-requests 4 25 instance-request-fn
                                                      :canceled? (fn [] (> (count @instance-ids-atom) 2))
                                                      :service-id service-id)
                                (reduce set/union))]
          (is (> (count instance-ids) 1) (str instance-ids)))

        (log/info "waiting for at least one instance to get killed")
        (is (wait-for #(->> (get-in (service-settings waiter-url service-id) [:instances :killed-instances])
                            (map :id)
                            set
                            seq)
                      :interval 2 :timeout 45)
            (str "No killed instances found for " service-id))))))

(deftest ^:parallel ^:integration-fast test-basic-priority-support
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-distribution-scheme "simple" ;; disallow work-stealing interference from balanced
                   :x-waiter-max-instances 1}
          {:keys [cookies service-id]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          router-url (some-router-url-with-assigned-slots waiter-url service-id)
          response-priorities-atom (atom [])
          num-threads 15
          request-priorities (vec (shuffle (range num-threads)))
          request-counter-atom (atom 0)
          make-prioritized-request (fn [priority delay-ms]
                                     (let [request-headers (assoc headers
                                                             :x-kitchen-delay-ms delay-ms
                                                             :x-waiter-priority priority)]
                                       (log/info "making kitchen request")
                                       (make-kitchen-request router-url request-headers :cookies cookies)))]
      (async/thread ; long request to make the following requests queue up
        (make-prioritized-request -1 5000))
      (Thread/sleep 500)
      (parallelize-requests num-threads 1
                            (fn []
                              (let [index (dec (swap! request-counter-atom inc))
                                    priority (nth request-priorities index)]
                                (make-prioritized-request priority 1000)
                                (swap! response-priorities-atom conj priority)))
                            :verbose true)
      ;; first item may be processed out of order as it can arrive before at the server
      (is (= (-> num-threads range reverse) @response-priorities-atom))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast ^:explicit test-multiple-ports
  (testing-using-waiter-url
    (let [num-ports 8
          waiter-headers {:x-waiter-name (rand-name)
                          :x-waiter-ports num-ports}
          {:keys [body service-id]} (make-request-with-debug-info
                                      waiter-headers #(make-kitchen-request waiter-url % :path "/environment"))
          body-json (json/read-str (str body))]
      (is (every? #(contains? body-json (str "PORT" %)) (range num-ports))
          (str body-json))
      (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
            _ (assert-service-on-all-routers waiter-url service-id cookies)
            {:keys [extra-ports port] :as active-instance} (first (active-instances waiter-url service-id))]
        (log/info service-id "active-instance:" active-instance)
        (is (seq active-instance) (str active-instance))
        (is (pos? port) (str active-instance))
        (is (= (get body-json "PORT0") (str port)) (str {:active-instance active-instance :body body-json}))
        (is (= (dec num-ports) (count extra-ports)) (str active-instance))
        (is (every? pos? extra-ports) (str active-instance))
        (is (->> (map #(= (get body-json (str "PORT" %1)) (str %2))
                      (range 1 (-> extra-ports count inc))
                      extra-ports)
                 (every? true?))
            (str {:active-instance active-instance :body body-json})))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-identical-version
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]
      (is (= 1 (->> (routers waiter-url)
                    vals
                    (map #(:git-version (waiter-settings % :cookies cookies)))
                    set
                    count))))))

(deftest ^:parallel ^:integration-fast test-cors-request-allowed
  (testing-using-waiter-url
    (let [{{:keys [exposed-headers kind]} :cors-config} (waiter-settings waiter-url)]
      (if (= kind "allow-all")
        (testing "cors allowed"
          ; Hit an endpoint that is guarded by CORS validation.
          ; There's nothing special about /state, any CORS validated endpoint will do.
          (let [{:keys [headers] :as response} (make-request waiter-url "/state"
                                                             :headers {"origin" "example.com"})]
            (assert-response-status response 200)
            (when (seq exposed-headers)
              (is (= (str/join ", " exposed-headers) (get headers "access-control-expose-headers"))
                  (str response)))))
        (testing "cors not allowed"
          (let [response (make-request waiter-url "/state" :headers {"origin" "badorigin.com"} :method :get)]
            (assert-response-status response 403))
          (let [response (make-request waiter-url "/state" :headers {"origin" "badorigin.com"} :method :post)]
            (assert-response-status response 403))
          (let [response (make-request waiter-url "/state" :headers {"origin" "badorigin.com"} :method :options)]
            (assert-response-status response 403)))))))

(deftest ^:parallel ^:integration-fast test-error-handling
  (testing-using-waiter-url
    (testing "text/plain default"
      (let [{:keys [body headers status]} (make-request waiter-url "/404")]
        (is (= 404 status))
        (is (= "text/plain" (get headers "content-type")))
        (is (str/includes? body "Waiter Error 404"))
        (is (str/includes? body "================"))))
    (testing "text/plain explicit"
      (let [{:keys [body headers status]} (make-request waiter-url "/404" :headers {"accept" "text/plain"})]
        (is (= 404 status))
        (is (= "text/plain" (get headers "content-type")))
        (is (str/includes? body "Waiter Error 404"))
        (is (str/includes? body "================"))))
    (testing "text/html"
      (let [{:keys [body headers status]} (make-request waiter-url "/404" :headers {"accept" "text/html"})]
        (is (= 404 status))
        (is (= "text/html" (get headers "content-type")))
        (is (str/includes? body "Waiter Error 404"))
        (is (str/includes? body "<html>"))))
    (testing "application/json explicit"
      (let [{:keys [body headers status]} (make-request waiter-url "/404" :headers {"accept" "application/json"})
            {:strs [waiter-error]} (try (json/read-str body)
                                        (catch Throwable _
                                          (is false (str "Could not parse body that is supposed to be JSON:\n" body))))]
        (is (= 404 status))
        (is (= "application/json" (get headers "content-type")))
        (is waiter-error (str "Could not find waiter-error element in body " body))
        (let [{:strs [status]} waiter-error]
          (is (= 404 status)))))
    (testing "application/json implied by content-type"
      (let [{:keys [body headers status]} (make-request waiter-url "/404" :headers {"content-type" "application/json"})
            {:strs [waiter-error]} (try (json/read-str body)
                                        (catch Throwable _
                                          (is false (str "Could not parse body that is supposed to be JSON:\n" body))))]
        (is (= 404 status))
        (is (= "application/json" (get headers "content-type")))
        (is waiter-error (str "Could not find waiter-error element in body " body))
        (let [{:strs [status]} waiter-error]
          (is (= 404 status)))))
    (testing "support information included"
      (let [{:keys [body headers status]} (make-request waiter-url "/404" :headers {"accept" "application/json"})
            {:keys [messages support-info]} (waiter-settings waiter-url)
            {:strs [waiter-error]} (try (json/read-str body)
                                        (catch Throwable _
                                          (is false (str "Could not parse body that is supposed to be JSON:\n" body))))]

        (is (= 404 status))
        (is (= "application/json" (get headers "content-type")))
        (is (= (:not-found messages) (get waiter-error "message")))
        (is waiter-error (str "Could not find waiter-error element in body " body))
        (let [{:strs [status]} waiter-error]
          (is (= 404 status))
          (is (= support-info (-> (get waiter-error "support-info")
                                  (walk/keywordize-keys)))))))))

(deftest ^:parallel ^:integration-fast test-welcome-page
  (testing-using-waiter-url
    (testing "default text/plain"
      (let [{:keys [body headers status]} (make-request waiter-url "/")]
        (is (= 200 status))
        (is (= "text/plain" (get headers "content-type")))
        (is (str/includes? body "Welcome to Waiter"))))
    (testing "accept text/plain"
      (let [{:keys [body headers status]} (make-request waiter-url "/" :headers {"accept" "text/plain"})]
        (is (= 200 status))
        (is (= "text/plain" (get headers "content-type")))
        (is (str/includes? body "Welcome to Waiter"))))
    (testing "accept text/html"
      (let [{:keys [body headers status]} (make-request waiter-url "/" :headers {"accept" "text/html"})]
        (is (= 200 status))
        (is (= "text/html" (get headers "content-type")))
        (is (str/includes? body "Welcome to Waiter"))))
    (testing "accept application/json"
      (let [{:keys [body headers status]} (make-request waiter-url "/" :headers {"accept" "application/json"})
            json-data (try (json/read-str body)
                           (catch Exception _
                             (is false ("Not json:\n" body))))]
        (is (= 200 status))
        (is (= "application/json" (get headers "content-type")))
        (is (= "Welcome to Waiter" (get json-data "message")))))
    (testing "only GET"
      (let [{:keys [body status]} (make-request waiter-url "/" :method :post)]
        (is (= 405 status))
        (is (str/includes? body "Only GET supported"))))))

(deftest ^:parallel ^:integration-fast test-interstitial-page
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]
      (doseq [[_ router-url] (routers waiter-url)]
        (wait-for #(-> (interstitial-state router-url :cookies cookies)
                       (get-in ["state" "interstitial" "initialized?"] false))))
      (let [interstitial-secs 5
            request-headers {"accept" "text/html"}
            token (str (rand-name) ".localtest.me")
            _ (post-token waiter-url (-> (kitchen-params)
                                         (assoc
                                           :concurrency-level 20
                                           :interstitial-secs interstitial-secs
                                           :metric-group "waiter_kitchen_test"
                                           :name token
                                           :permitted-user (retrieve-username)
                                           :run-as-user (retrieve-username)
                                           :token token)
                                         (update :cmd (fn [cmd] (str "sleep 10 && " cmd)))))
            service-id (retrieve-service-id waiter-url {"x-waiter-token" token})
            router-url (-> waiter-url routers vals first)]
        (with-service-cleanup
          service-id
          (try
            (->> [(async/thread ;; check interstitial rendering
                    (let [request-headers (assoc request-headers "host" token)
                          {:keys [body] :as response}
                          (make-request router-url "/waiter-interstitial/some-endpoint"
                                        :cookies cookies
                                        :headers request-headers
                                        :query-params {"a" "b"})]
                      (assert-response-status response 200)
                      (is (str/includes? body (str "<title>Waiter - Interstitial</title>")))
                      (is (str/includes? body (str "/some-endpoint?a=b&x-waiter-bypass-interstitial=")))))
                  (async/thread ;; GET request inside the interstitial period, using DNS token
                    (let [start-time (t/now)
                          endpoint "/hello"
                          request-headers (assoc request-headers "host" token)
                          {:keys [headers] :as response}
                          (make-request router-url endpoint
                                        :cookies cookies
                                        :headers request-headers
                                        :method :get)
                          end-time (t/now)]
                      (assert-response-status response 303)
                      (is (= (str "/waiter-interstitial" endpoint) (get headers "location")))
                      (is (= "true" (get headers "x-waiter-interstitial")))
                      (is (< (t/in-millis (t/interval start-time end-time))
                             (t/in-millis (t/seconds interstitial-secs))))))
                  (async/thread ;; POST request inside the interstitial period, using DNS token
                    (let [start-time (t/now)
                          endpoint "/hello"
                          request-headers (assoc request-headers "host" token)
                          {:keys [headers] :as response}
                          (make-request router-url endpoint
                                        :cookies cookies
                                        :headers request-headers
                                        :method :post)
                          end-time (t/now)]
                      (assert-response-status response 303)
                      (is (= (str "/waiter-interstitial" endpoint) (get headers "location")))
                      (is (= "true" (get headers "x-waiter-interstitial")))
                      (is (< (t/in-millis (t/interval start-time end-time))
                             (t/in-millis (t/seconds interstitial-secs))))))
                  (async/thread ;; request inside the interstitial period, but on-the-fly
                    (let [endpoint "/hello"
                          request-headers (assoc request-headers "x-waiter-token" token)
                          {:keys [body headers] :as response}
                          (make-request router-url endpoint
                                        :cookies cookies
                                        :headers request-headers)]
                      (assert-response-status response 200)
                      (is (str/includes? (str body) "Hello World"))
                      (is (not (contains? headers "x-waiter-interstitial")))))
                  (async/thread ;; request inside the interstitial period but with bypass query param
                    (let [endpoint "/hello"
                          request-headers (assoc request-headers "host" token)
                          {:keys [body headers] :as response}
                          (make-request router-url endpoint
                                        :cookies cookies
                                        :headers request-headers
                                        :query-params {"x-waiter-bypass-interstitial"
                                                       (interstitial/request-time->interstitial-param-value (t/now))})]
                      (assert-response-status response 200)
                      (is (str/includes? (str body) "Hello World"))
                      (is (not (contains? headers "x-waiter-interstitial")))))
                  (async/thread ;; request outside the interstitial period
                    (Thread/sleep (* 1000 (inc interstitial-secs)))
                    (let [endpoint "/hello"
                          request-headers (assoc request-headers "host" token)
                          {:keys [body headers] :as response}
                          (make-request router-url endpoint
                                        :cookies cookies
                                        :headers request-headers)]
                      (assert-response-status response 200)
                      (is (str/includes? (str body) "Hello World"))
                      (is (not (contains? headers "x-waiter-interstitial")))))]
                 (map async/<!!)
                 doall)
            (is (some (fn [[_ router-url]]
                        (some-> (interstitial-state router-url :cookies cookies)
                                (get-in ["state" "interstitial" "service-id->interstitial-promise"] {})
                                (get service-id)
                                #{"healthy-instance-found" "interstitial-timeout"}))
                      (routers waiter-url)))
            (finally
              (delete-token-and-assert waiter-url token))))))))

(deftest ^:parallel ^:integration-fast test-composite-scheduler-services
  (testing-using-waiter-url
    (let [{:keys [scheduler-config]} (waiter-settings waiter-url)
          {:keys [components factory-fn]} (get scheduler-config (-> scheduler-config :kind keyword))]
      (if (not= "waiter.scheduler.composite/create-composite-scheduler" factory-fn)
        (log/info "skipping as scheduler is not a composite scheduler")
        (let [service-ids
              (for [[component _] components]
                (let [scheduler-name (name component)
                      _ (log/info "testing" scheduler-name "scheduler inside composite scheduler")
                      {:keys [service-id] :as response} (make-request-with-debug-info
                                                          {:x-waiter-name (str (rand-name) "-" scheduler-name)
                                                           :x-waiter-scheduler scheduler-name}
                                                          #(make-kitchen-request waiter-url % :path "/hello"))
                      _ (assert-response-status response 200)
                      {:keys [name scheduler] :as service-description} (service-id->service-description waiter-url service-id)]
                  (is (= scheduler-name scheduler) (str service-description))
                  (is (str/ends-with? name scheduler-name) (str service-description))
                  service-id))]
          (is (seq service-ids) "No services were created using the composite scheduler")
          (doseq [service-id service-ids]
            (is (service waiter-url service-id {}) (str service-id "not found in /apps endpoint")))
          (doseq [service-id service-ids]
            (delete-service waiter-url service-id)))))))
