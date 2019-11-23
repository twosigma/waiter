(ns waiter.kubernetes-scheduler-integration-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all]))

(defn- get-watch-state [state-json]
  (or (get-in state-json ["state" "watch-state"])
      (get-in state-json ["state" "components" "kubernetes" "watch-state"])))

(deftest ^:parallel ^:integration-fast test-kubernetes-watch-state-update
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [cookies (all-cookies waiter-url)
            router-url (-> waiter-url routers first val)
            {:keys [body] :as response} (make-request router-url "/state/scheduler" :method :get :cookies cookies)
            _ (assert-response-status response 200)
            body-json (-> body str try-parse-json)
            watch-state-json (get-watch-state body-json)
            initial-pods-snapshot-version (get-in watch-state-json ["pods-metadata" "version" "snapshot"])
            initial-pods-watch-version (get-in watch-state-json ["pods-metadata" "version" "watch"])
            initial-rs-snapshot-version (get-in watch-state-json ["rs-metadata" "version" "snapshot"])
            initial-rs-watch-version (get-in watch-state-json ["rs-metadata" "version" "watch"])
            {:keys [service-id]} (make-request-with-debug-info
                                   {:x-waiter-name (rand-name)}
                                   #(make-kitchen-request waiter-url % :path "/hello"))]
        (with-service-cleanup
          service-id
          (let [{:keys [body] :as response} (make-request router-url "/state/scheduler" :method :get :cookies cookies)
                _ (assert-response-status response 200)
                body-json (-> body str try-parse-json)
                watch-state-json (get-watch-state body-json)
                pods-snapshot-version' (get-in watch-state-json ["pods-metadata" "version" "snapshot"])
                pods-watch-version' (get-in watch-state-json ["pods-metadata" "version" "watch"])
                rs-snapshot-version' (get-in watch-state-json ["rs-metadata" "version" "snapshot"])
                rs-watch-version' (get-in watch-state-json ["rs-metadata" "version" "watch"])]
            (is (or (nil? initial-pods-watch-version)
                    (< initial-pods-snapshot-version initial-pods-watch-version)))
            (is (<= initial-pods-snapshot-version pods-snapshot-version'))
            (is (< pods-snapshot-version' pods-watch-version'))
            (is (or (nil? initial-rs-watch-version)
                    (< initial-rs-snapshot-version initial-rs-watch-version)))
            (is (<= initial-rs-snapshot-version rs-snapshot-version'))
            (is (< rs-snapshot-version' rs-watch-version'))))))))

(defn- validate-kubernetes-custom-image
  [waiter-url custom-image]
  (let [{:keys [body service-id]} (make-request-with-debug-info
                                    {:x-waiter-name (rand-name)
                                     :x-waiter-image custom-image
                                     :x-waiter-cmd "echo -n $INTEGRATION_TEST_SENTINEL_VALUE > index.html && python3 -m http.server $PORT0"
                                     :x-waiter-health-check-url "/"}
                                    #(make-kitchen-request waiter-url % :method :get :path "/"))]
    (is (= "Integration Test Sentinel Value" body))
    (delete-service waiter-url service-id)))

; test that we can provide a custom docker image that contains /tmp/index.html with "Integration Test Image" in it
(deftest ^:parallel ^:integration-slow test-kubernetes-custom-image
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [custom-image (System/getenv "INTEGRATION_TEST_CUSTOM_IMAGE")
            _ (is (not (str/blank? custom-image)) "You must provide a custom image in the INTEGRATION_TEST_CUSTOM_IMAGE environment variable")]
        (validate-kubernetes-custom-image waiter-url custom-image)))))

(deftest ^:parallel ^:integration-slow test-kubernetes-image-alias
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [custom-image (System/getenv "INTEGRATION_TEST_CUSTOM_IMAGE_ALIAS")
            _ (is (not (str/blank? custom-image)) "You must provide a custom image in the INTEGRATION_TEST_CUSTOM_IMAGE_ALIAS environment variable")]
        (validate-kubernetes-custom-image waiter-url custom-image)))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-s3-logs
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (when-let [log-bucket-url (-> waiter-url get-kubernetes-scheduler-settings :log-bucket-url)]
        (let [router-url (-> waiter-url routers first val)
              headers {:x-waiter-concurrency-level 1
                       :x-waiter-distribution-scheme "simple"
                       :x-waiter-max-instances 2
                       :x-waiter-min-instances 1
                       :x-waiter-name (rand-name)
                       :x-waiter-scale-down-factor 0.99
                       :x-waiter-scale-up-factor 0.99}
              _ (log/info "making canary request...")
              {:keys [cookies service-id]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
              make-request-fn (fn [url] (make-request url "" :verbose true :cookies cookies))]

          (with-service-cleanup
            service-id
            (assert-service-on-all-routers waiter-url service-id cookies)

            (log/info "waiting for at least one active instance on target router")
            (is (wait-for #(seq (active-instances router-url service-id :cookies cookies))
                          :interval 2 :timeout 45)
                (str "no active instances found for " service-id))

            ;; Test that the active instances' logs are available.
            (let [active-instances (active-instances router-url service-id :cookies cookies)
                  log-url (:log-url (first active-instances))
                  {:keys [body] :as logs-response} (make-request-fn log-url)
                  _ (assert-response-status logs-response 200)
                  log-files-list (walk/keywordize-keys (json/read-str body))
                  stdout-file-link (:url (first (filter #(= (:name %) "stdout") log-files-list)))
                  stderr-file-link (:url (first (filter #(= (:name %) "stderr") log-files-list)))]
              (is (every? #(str/includes? body %) ["stderr" "stdout"])
                  (str "Live directory listing is missing entries: stderr and stdout, got response: " logs-response))
              (doseq [file-link [stderr-file-link stdout-file-link]]
                (if (str/starts-with? (str file-link) "http")
                  (assert-response-status (make-request-fn file-link) 200)
                  (log/warn "test-s3-logs did not verify file link:" file-link))))

            ;; Get a service with at least one killed instance.
            (log/info "starting parallel requests")
            (let [async-create-headers (merge headers {:x-kitchen-delay-ms 120000
                                                       :x-waiter-async-request-timeout 125000})
                  async-request-fn (fn [] (->> #(make-kitchen-request waiter-url % :method :get :path "/async/request")
                                               (make-request-with-debug-info async-create-headers)))
                  async-responses (->> async-request-fn (repeatedly 2) vec)
                  instance-ids (->> async-responses (map :instance-id) set)]
              (assert-response-status (first async-responses) 202)
              (assert-response-status (second async-responses) 202)
              (is (> (count instance-ids) 1) (str instance-ids))
              ;; Canceling both of the async requests should scale down to 1 by killing 1 instance.
              (doseq [async-response async-responses]
                (let [status-endpoint (response->location async-response)
                      cancel-response (make-kitchen-request waiter-url headers :method :delete :path status-endpoint)]
                  (assert-response-status cancel-response 204))))

            (log/info "waiting for at least one killed instance on target router")
            (is (wait-for #(seq (killed-instances router-url service-id :cookies cookies))
                          :interval 2 :timeout 45)
                (str "no killed instances found for " service-id))

            ;; Test that the killed instances' logs were persisted to S3.
            ;; This portion of the test logic was modified from the active-instances tests above.
            (let [killed-instances (killed-instances router-url service-id :cookies cookies)
                  log-url (:log-url (first killed-instances))
                  _ (do
                      (log/info "waiting s3 logs to appear")
                      (is (wait-for
                            #(let [{:keys [body]} (make-request-fn log-url)]
                               (str/includes? body log-bucket-url))
                            :interval 5 :timeout 300)
                          (str "Log URL never pointed to S3 bucket " log-bucket-url)))
                  {:keys [body] :as logs-response} (make-request-fn log-url)
                  _ (assert-response-status logs-response 200)
                  log-files-list (walk/keywordize-keys (json/read-str body))
                  stdout-file-link (:url (first (filter #(= (:name %) "stdout") log-files-list)))
                  stderr-file-link (:url (first (filter #(= (:name %) "stderr") log-files-list)))]
              (is (wait-for
                    #(every? (partial str/includes? body) ["stderr" "stdout"])
                    :interval 1 :timeout 30)
                  (str "Killed directory listing is missing entries: stderr and stdout, got response: " logs-response))
              (doseq [file-link [stderr-file-link stdout-file-link]]
                (if (str/starts-with? (str file-link) "http")
                  (assert-response-status (make-request file-link "" :verbose true) 200)
                  (log/warn "test-s3-logs did not verify file link:" file-link))))))))))

(defn- check-pod-namespace
  [waiter-url headers expected-namespace]
  (let [cookies (all-cookies waiter-url)
        router-url (-> waiter-url routers first val)
        testing-suffix (str (:x-waiter-run-as-user headers "nil") "-" (:x-waiter-namespace headers "nil"))
        {:keys [body error service-id status]}
        (make-request-with-debug-info
          (merge {:x-waiter-name (str (rand-name) "-" testing-suffix)} headers)
          #(make-kitchen-request waiter-url % :path "/hello"))]
    (when-not (= 200 status)
      (throw (ex-info "Failed to create service"
                      {:response-body body
                       :response-status status}
                      error)))
    (with-service-cleanup
      service-id
      (let [{:keys [body] :as response} (make-request router-url "/state/scheduler" :method :get :cookies cookies)
            _ (assert-response-status response 200)
            body-json (-> body str try-parse-json)
            watch-state-json (get-watch-state body-json)
            pod-spec (-> watch-state-json (get-in ["service-id->pod-id->pod" service-id]) first val)
            pod-namespace (get-in pod-spec ["metadata" "namespace"])]
        (is (some? pod-spec))
        (is (= expected-namespace pod-namespace))))))

(deftest ^:parallel ^:integration-slow test-pod-namespace
  "Expected behavior for services with namespaces:
   Run-As-User    Namespace   Validation
   Missing        Missing     OK
   Missing        *           OK
   Missing        foo         OK
   Missing        bar         FAIL
   foo            Missing     OK
   foo            *           OK
   foo            foo         OK
   foo            bar         FAIL
   *              Missing     OK
   *              *           OK
   *              foo         FAIL
   *              bar         FAIL"
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [current-user (retrieve-username)
            default-namespace (-> waiter-url get-kubernetes-scheduler-settings :replicaset-spec-builder :default-namespace)
            star-user-header {:x-waiter-run-as-user "*"}
            current-user-header {:x-waiter-run-as-user current-user}
            not-current-user "not-current-user"]
        (testing "namespaces for current user (implicit)"
          (check-pod-namespace waiter-url {} default-namespace)
          (check-pod-namespace waiter-url {:x-waiter-namespace "*"} current-user)
          (check-pod-namespace waiter-url {:x-waiter-namespace current-user} current-user)
          (is (thrown? Exception #"Service namespace must either be omitted or match the run-as-user"
                       (check-pod-namespace waiter-url {:x-waiter-namespace not-current-user} current-user))))
        (testing "namespaces for current user (explicit)"
          (check-pod-namespace waiter-url current-user-header default-namespace)
          (check-pod-namespace waiter-url (assoc current-user-header :x-waiter-namespace "*") current-user)
          (check-pod-namespace waiter-url (assoc current-user-header :x-waiter-namespace current-user) current-user)
          (is (thrown? Exception #"Service namespace must either be omitted or match the run-as-user"
                       (check-pod-namespace waiter-url (assoc current-user-header :x-waiter-namespace not-current-user) current-user))))
        (testing "namespaces for run-as-requester"
          (check-pod-namespace waiter-url star-user-header default-namespace)
          (check-pod-namespace waiter-url (assoc star-user-header :x-waiter-namespace "*") current-user)
          (is (thrown? Exception #"Cannot use run-as-requester with a specific namespace"
                       (check-pod-namespace waiter-url (assoc star-user-header :x-waiter-namespace current-user) current-user)))
          (is (thrown? Exception #"Cannot use run-as-requester with a specific namespace"
                       (check-pod-namespace waiter-url (assoc star-user-header :x-waiter-namespace not-current-user) current-user))))))))

(defn- get-pod-service-account
  [waiter-url namespace-arg]
  (let [{:keys [body error service-id status]}
        (make-request-with-debug-info
          (cond->
            {:x-waiter-name (rand-name)
             :x-waiter-cmd (str "env SERVICE_ACCOUNT=\"$(grep -hs . /var/run/secrets/kubernetes.io/serviceaccount/namespace)\" "
                                (kitchen-cmd "-p $PORT0"))}
            namespace-arg
            (assoc :x-waiter-namespace namespace-arg))
          #(make-kitchen-request waiter-url % :path "/environment"))]
    (when-not (= 200 status)
      (throw (ex-info "Failed to create service"
                      {:response-body body
                       :response-status status}
                      error)))
    (with-service-cleanup
      service-id
      (-> body str try-parse-json (get "SERVICE_ACCOUNT")))))

(deftest ^:parallel ^:integration-fast test-service-account-injection
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [current-user (retrieve-username)]
        (testing "No service account with default namespace"
          (let [service-account (get-pod-service-account waiter-url nil)]
            (is (str/blank? service-account))))
        (testing "Has service account with custom namespace"
          (let [service-account (get-pod-service-account waiter-url current-user)]
            (is (= current-user service-account))))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-kubernetes-pod-expiry-failing-instance
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [{:keys [request-headers service-id] :as response}
            (make-request-with-debug-info
              {:x-waiter-distribution-scheme "simple"
               :x-waiter-name (rand-name)}
              #(make-kitchen-request waiter-url % :method :get :path "/"))]
        (with-service-cleanup
          service-id
          (assert-response-status response 200)
          (dotimes [_ 5]
            (let [request-headers (assoc request-headers :x-kitchen-delay-ms 1000)
                  response (make-kitchen-request waiter-url request-headers :path "/die")]
              (assert-response-status response #{502 503})))
          ;; assert that more than one pod was created
          (is (wait-for
                (fn []
                  (let [{:keys [active-instances failed-instances]} (:instances (service-settings waiter-url service-id))
                        pod-ids (->> (concat active-instances failed-instances)
                                     (map :k8s/pod-name)
                                     (into #{}))]
                    (log/info pod-ids)
                    (< 1 (count pod-ids)))))))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-kubernetes-pod-expiry-grace-period
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (if-let [custom-image (System/getenv "INTEGRATION_TEST_BAD_IMAGE")]
        (let [{:keys [container-running-grace-secs]} (get-kubernetes-scheduler-settings waiter-url)
              waiter-headers (assoc (kitchen-request-headers)
                               :x-waiter-distribution-scheme "simple"
                               :x-waiter-image custom-image
                               :x-waiter-max-instances 1
                               :x-waiter-min-instances 1
                               :x-waiter-name (rand-name)
                               :x-waiter-timeout 30000)
              service-id (retrieve-service-id waiter-url waiter-headers)
              timeout-secs 150]
          (if (> container-running-grace-secs timeout-secs)
            (log/warn "skipping test as the configuration will cause the test to run for too long"
                      {:container-running-grace-secs container-running-grace-secs
                       :waiter-url waiter-url})
            (with-service-cleanup
              service-id
              ;; make request to launch service instance(s), we do not care about the response
              (make-request waiter-url "/status" :headers waiter-headers)
              ;; assert that more than one pod was created
              (is (wait-for
                    (fn []
                      (let [{:keys [instances]} (service-settings waiter-url service-id)
                            {:keys [active-instances failed-instances]} instances
                            pod-ids (->> (concat active-instances failed-instances)
                                         (map :k8s/pod-name)
                                         (into #{}))]
                        (log/info "active-instances" active-instances)
                        (log/info "failed-instances" failed-instances)
                        (< 1 (count pod-ids))))
                    :interval 15
                    :timeout timeout-secs)))))
        (log/warn "skipping test as INTEGRATION_TEST_BAD_IMAGE is not specified")))))
