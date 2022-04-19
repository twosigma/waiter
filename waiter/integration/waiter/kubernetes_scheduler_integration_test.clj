(ns waiter.kubernetes-scheduler-integration-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.utils :as utils]))

(deftest ^:parallel ^:integration-fast test-k8s-service-and-instance-fields
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [{:keys [cookies service-id] :as response}
            (make-request-with-debug-info
              {:x-waiter-name (rand-name)} #(make-kitchen-request waiter-url % :path "/hello"))]
        (with-service-cleanup
          service-id
          (assert-response-status response http-200-ok)
          (assert-service-on-all-routers waiter-url service-id cookies)
          (let [instances (active-instances waiter-url service-id :cookies cookies)]
            (testing "k8s scheduler service instance fields"
              (doseq [instance instances]
                (let [{:keys [k8s/app-name k8s/namespace k8s/node-name k8s/pod-name k8s/revision-timestamp k8s/user]} (walk/keywordize-keys instance)
                      assertion-message (str instance)]
                  (is app-name assertion-message)
                  (is namespace assertion-message)
                  (is node-name assertion-message)
                  (is pod-name assertion-message)
                  (is revision-timestamp assertion-message)
                  (is user assertion-message)))))

          (testing "k8s scheduler service fields"
            (doseq [router-url (-> waiter-url routers vals)]
              (let [watch-state-json (get-k8s-watch-state router-url cookies)
                    service (get-in watch-state-json ["service-id->service" service-id])]
                (if (map? service)
                  (let [{:keys [k8s/app-name k8s/container-resources k8s/containers k8s/namespace k8s/replicaset-annotations
                                k8s/replicaset-pod-annotations k8s/replicaset-uid]} (walk/keywordize-keys service)
                        k8s-containers (set containers)
                        assertion-message (str {:router-url router-url :service service})]
                    (is (= service-id (get service "id")) assertion-message)
                    (is app-name assertion-message)
                    (is (seq container-resources) assertion-message)
                    (is (seq k8s-containers) assertion-message)
                    (is (= (set k8s-containers) (set (map :name container-resources))) assertion-message)
                    (is (contains? k8s-containers "waiter-app") assertion-message)
                    (is namespace assertion-message)
                    (is replicaset-uid assertion-message)
                    (is (contains? replicaset-annotations :waiter/revision-timestamp) assertion-message)
                    (is (contains? replicaset-annotations :waiter/revision-version) assertion-message)
                    (is (contains? replicaset-pod-annotations :waiter/revision-timestamp) assertion-message)
                    (is (contains? replicaset-pod-annotations :waiter/revision-version) assertion-message))
                  (is false (str {:message "service unavailable in k8s watch state"
                                  :router-url router-url
                                  :service-id service-id
                                  :watch-state-json watch-state-json})))))))))))

(deftest ^:parallel ^:integration-fast test-kubernetes-watch-state-update
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [cookies (all-cookies waiter-url)
            router-url (-> waiter-url routers first val)
            watch-state-json (get-k8s-watch-state router-url cookies)
            initial-pods-snapshot-version (get-in watch-state-json ["pods-metadata" "version" "snapshot"])
            initial-pods-watch-version (get-in watch-state-json ["pods-metadata" "version" "watch"])
            initial-rs-snapshot-version (get-in watch-state-json ["rs-metadata" "version" "snapshot"])
            initial-rs-watch-version (get-in watch-state-json ["rs-metadata" "version" "watch"])
            {:keys [service-id]} (make-request-with-debug-info
                                   {:x-waiter-name (rand-name)}
                                   #(make-kitchen-request waiter-url % :path "/hello"))]
        (with-service-cleanup
          service-id
          (let [watch-state-json (get-k8s-watch-state router-url cookies)
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
  (let [{:keys [body service-id] :as response}
        (make-request-with-debug-info
          {:x-waiter-name (rand-name)
           :x-waiter-image custom-image
           :x-waiter-cmd "echo -n $INTEGRATION_TEST_SENTINEL_VALUE > index.html && python3 -m http.server $PORT0"
           :x-waiter-health-check-url "/"}
          #(make-kitchen-request waiter-url % :method :get :path "/"))]
    (assert-response-status response http-200-ok)
    (is (= "Integration Test Sentinel Value" body))
    (delete-service waiter-url service-id)))

; test that we can provide a custom docker image that contains /tmp/index.html with "Integration Test Image" in it
(deftest ^:parallel ^:integration-fast test-kubernetes-custom-image
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [custom-image (System/getenv "INTEGRATION_TEST_CUSTOM_IMAGE")
            _ (is (not (str/blank? custom-image)) "You must provide a custom image in the INTEGRATION_TEST_CUSTOM_IMAGE environment variable")]
        (validate-kubernetes-custom-image waiter-url custom-image)))))

(deftest ^:parallel ^:integration-fast test-kubernetes-image-alias
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
              headers {:x-waiter-max-instances 2
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
                  _ (assert-response-status logs-response http-200-ok)
                  log-files-list (walk/keywordize-keys (json/read-str body))
                  stdout-file-link (:url (first (filter #(= (:name %) "stdout") log-files-list)))
                  stderr-file-link (:url (first (filter #(= (:name %) "stderr") log-files-list)))]
              (is (every? #(str/includes? body %) ["stderr" "stdout"])
                  (str "Live directory listing is missing entries: stderr and stdout, got response: " logs-response))
              (doseq [file-link [stderr-file-link stdout-file-link]]
                (if (str/starts-with? (str file-link) "http")
                  (assert-response-status (make-request-fn file-link) http-200-ok)
                  (log/warn "test-s3-logs did not verify file link:" file-link))))

            ;; get a killed instance by scaling up to 2 and back down to 1
            (log/info "creating min-instances=2 override")
            (let [override-path (str "/apps/" service-id "/override")
                  post-override-response (make-request waiter-url override-path
                                                       :body (utils/clj->json {:min-instances 2})
                                                       :cookies cookies
                                                       :method :post
                                                       :verbose true)]
              (assert-response-status post-override-response http-200-ok)

              ;; wait for scale up
              (is (wait-for #(let [healthy-instance-count (->> (active-instances router-url service-id :cookies cookies)
                                                               (filter :healthy?)
                                                               (count))]
                               (>= healthy-instance-count 2))
                            :interval 2 :timeout 300)
                  (str service-id " never scaled to at least 2 healthy instances"))

              (log/info "deleting min-instances=2 override")
              (let [delete-override-response (make-request waiter-url override-path
                                                           :cookies cookies
                                                           :method :delete
                                                           :verbose true)]
                (assert-response-status delete-override-response http-200-ok))

              ;; wait for scale down
              (log/info "waiting for at least one killed instance on target router")
              (is (wait-for #(seq (killed-instances router-url service-id :cookies cookies))
                            :interval 2 :timeout 45)
                  (str "no killed instances found for " service-id)))

            ;; Test that the killed instance's logs were persisted to S3.
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
                  _ (assert-response-status logs-response http-200-ok)
                  log-files-list (walk/keywordize-keys (json/read-str body))
                  stdout-file-link (:url (first (filter #(= (:name %) "stdout") log-files-list)))
                  stderr-file-link (:url (first (filter #(= (:name %) "stderr") log-files-list)))]
              (is (wait-for
                    #(every? (partial str/includes? body) ["stderr" "stdout"])
                    :interval 1 :timeout 30)
                  (str "Killed directory listing is missing entries: stderr and stdout, got response: " logs-response))
              (doseq [file-link [stderr-file-link stdout-file-link]]
                (if (str/starts-with? (str file-link) "http")
                  (assert-response-status (make-request file-link "" :verbose true) http-200-ok)
                  (log/warn "test-s3-logs did not verify file link:" file-link))))))))))

(deftest ^:parallel ^:integration-fast test-s3-custom-bucket
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (when-let [log-bucket-url (-> waiter-url get-kubernetes-scheduler-settings :log-bucket-url)]
        (let [bucket-subpath "/my/custom/path"
              custom-bucket-url (str log-bucket-url bucket-subpath)
              service-headers {:x-waiter-name (rand-name)
                               :x-waiter-env-WAITER_CONFIG_LOG_BUCKET_URL custom-bucket-url}
              _ (log/info "making canary request...")
              {:keys [headers service-id]} (make-request-with-debug-info service-headers #(make-kitchen-request waiter-url %))
              {user "x-waiter-auth-user" instance-id "x-waiter-backend-id"} headers
              [_ pod-name run-number] (re-find #"^[^.]+\.(.*)-(\d+)$" instance-id)
              stderr-path (str/join "/" [bucket-subpath user service-id pod-name (str "r" run-number) "stderr"])]
          (with-service-cleanup service-id
            (comment "Kill the service"))
          (is (wait-for
                (fn look-for-s3-logs []
                  (let [stderr-response (make-request log-bucket-url stderr-path :method :get)]
                    (and (= (:status stderr-response) http-200-ok)
                         (str/includes? (:body stderr-response) service-id))))
                :interval 2 :timeout 45)))))))

(defn- check-pod-namespace
  [waiter-url headers expected-namespace]
  (let [cookies (all-cookies waiter-url)
        router-url (-> waiter-url routers first val)
        testing-suffix (str (:x-waiter-run-as-user headers "nil") "-" (:x-waiter-namespace headers "nil"))
        {:keys [body error service-id status]}
        (make-request-with-debug-info
          (merge {:x-waiter-name (str (rand-name) "-" testing-suffix)} headers)
          #(make-kitchen-request waiter-url % :path "/hello"))]
    (when-not (= http-200-ok status)
      (throw (ex-info "Failed to create service"
                      {:response-body body
                       :response-status status}
                      error)))
    (with-service-cleanup
      service-id
      (let [watch-state-json (get-k8s-watch-state router-url cookies)
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
            configured-namespace (-> waiter-url get-kubernetes-scheduler-settings :replicaset-spec-builder :default-namespace)
            default-namespace (if (= "*" configured-namespace) current-user configured-namespace)
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

(defn- get-pod-service-account-info
  [waiter-url namespace-arg]
  (let [{:keys [body cookies error service-id status] :as response}
        (make-request-with-debug-info
          (cond->
            {:x-waiter-name (rand-name)
             :x-waiter-cmd (str "env SERVICE_ACCOUNT=\"$(grep -hs . /var/run/secrets/kubernetes.io/serviceaccount/namespace)\" "
                                (kitchen-cmd "-p $PORT0"))}
            namespace-arg
            (assoc :x-waiter-namespace namespace-arg))
          #(make-kitchen-request waiter-url % :path "/environment"))]
    (with-service-cleanup
      service-id
      (assert-response-status response http-200-ok)
      (assert-service-on-all-routers waiter-url service-id cookies)
      (let [instance (first (active-instances waiter-url service-id :cookies cookies))
            instance-env (-> body str try-parse-json)]
        {:pod-namespace (:k8s/namespace instance)
         :service-account (get instance-env "SERVICE_ACCOUNT")}))))

(deftest ^:parallel ^:integration-fast test-service-account-injection
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [current-user (retrieve-username)]
        (testing "No service account for default namespace, or matches user"
          (let [{:keys [service-account pod-namespace]} (get-pod-service-account-info waiter-url nil)]
            ;; matches run-as-user when default-namespace resolves to run-as-user
            ;; blank when default-namespace resolves to some other user (don't leak credentials)
            (if (= current-user pod-namespace)
              (is (= current-user service-account))
              (is (str/blank? service-account)))))
        (testing "Has service account with custom namespace"
          (let [{:keys [service-account pod-namespace]} (get-pod-service-account-info waiter-url current-user)]
            (is (= current-user pod-namespace))
            (is (= current-user service-account))))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-kubernetes-pod-expiry-failing-instance
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [{:keys [cookies request-headers service-id] :as response}
            (make-request-with-debug-info
              {:x-waiter-distribution-scheme "simple"
               :x-waiter-name (rand-name)}
              #(make-kitchen-request waiter-url % :method :get :path "/"))]
        (with-service-cleanup
          service-id
          (assert-response-status response http-200-ok)
          (dotimes [_ 5]
            (let [request-headers (assoc request-headers :x-kitchen-delay-ms 1000)
                  response (make-kitchen-request waiter-url request-headers :path "/die")]
              (assert-response-status response #{http-502-bad-gateway http-503-service-unavailable})))
          ;; assert that more than one pod was created
          (is (wait-for
                (fn []
                  (let [{:keys [active-instances failed-instances]} (:instances (service-settings waiter-url service-id))
                        pod-ids (->> (concat active-instances failed-instances)
                                  (map :k8s/pod-name)
                                  (into #{}))]
                    (log/info pod-ids)
                    (< 1 (count pod-ids)))))))
        (assert-service-not-on-any-routers waiter-url service-id cookies)
        (let [{:keys [active-instances failed-instances]} (get (service-settings waiter-url service-id) :instances)]
          (is (empty? active-instances))
          (is (empty? failed-instances)))))))

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
                               :x-waiter-timeout 30000
                               :x-waiter-queue-timeout 30000)
              service-id (retrieve-service-id waiter-url waiter-headers)
              timeout-secs 150]
          (cond
            (zero? container-running-grace-secs)
            (log/info "skipping test as container-running-grace-secs is disabled"
                      {:container-running-grace-secs container-running-grace-secs
                       :waiter-url waiter-url})
            (> container-running-grace-secs timeout-secs)
            (log/warn "skipping test as the configuration will cause the test to run for too long"
                      {:container-running-grace-secs container-running-grace-secs
                       :waiter-url waiter-url})
            :else
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

(deftest ^:parallel ^:integration-fast test-kubernetes-raven-sidecar
  (testing-using-waiter-url
    (if-not (using-raven? waiter-url)
      (log/warn "skipping the integration test as :raven-sidecar is not configured")
      (let [x-waiter-name (rand-name)
            raven-sidecar-flag (get-raven-sidecar-flag waiter-url)
            request-headers {:x-waiter-name x-waiter-name
                             (keyword (str "x-waiter-env-" raven-sidecar-flag)) "true"}
            _ (log/info "making canary request")
            {:keys [cookies service-id] :as response} (make-request-with-debug-info
                                                        request-headers
                                                        #(make-kitchen-request waiter-url % :method :get :path "/status"))]
        (with-service-cleanup
          service-id
          (assert-service-on-all-routers waiter-url service-id cookies)
          (assert-response-status response http-200-ok)

          (let [response (make-kitchen-request waiter-url request-headers :method :get :path "/request-info")]
            (assert-response-status response http-200-ok)
            (testing "Expected Raven/Envoy specific headers are present in both request and response"
              (let [response-body (try-parse-json (:body response))
                    response-headers (:headers response)]
                ;; x-envoy-expected-rq-timeout-ms is absent when timeouts are disabled
                (is (some (get response-body "headers") ["x-envoy-external-address" "x-envoy-internal"]))
                (is (utils/raven-proxy-response? response)))))

          (let [response (make-request-with-debug-info
                           request-headers
                           #(make-kitchen-request waiter-url % :method :get :path "/environment"))]
            (assert-response-status response http-200-ok)
            (let [response-body (try-parse-json (:body response))
                  response-headers (:headers response)]
              (testing "Port value is correctly offset compared to instance value"
                (let [response-header-backend-port (get response-headers "x-waiter-backend-port")
                      env-response-port0 (get response-body "PORT0")]
                  (is (not= response-header-backend-port env-response-port0))))
              (testing "Reverse proxy flag environment variable is present"
                (is (contains? response-body raven-sidecar-flag))
                (is (= "true" (get response-body raven-sidecar-flag)))))))))))
