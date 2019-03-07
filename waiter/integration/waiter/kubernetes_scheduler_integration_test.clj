(ns waiter.kubernetes-scheduler-integration-test
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as string]
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
            {:keys [service-id request-headers]} (make-request-with-debug-info
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

; test that we can provide a custom docker image that contains /tmp/index.html with "Integration Test Image" in it
(deftest ^:parallel ^:integration-slow test-kubernetes-custom-image
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [custom-image (System/getenv "INTEGRATION_TEST_CUSTOM_IMAGE")
            _ (is (not (string/blank? custom-image)) "You must provide a custom image in the INTEGRATION_TEST_CUSTOM_IMAGE environment variable")
            {:keys [body]} (make-kitchen-request
                             waiter-url
                             {:x-waiter-name (rand-name)
                              :x-waiter-image custom-image
                              :x-waiter-cmd "echo -n $INTEGRATION_TEST_SENTINEL_VALUE > index.html && python3 -m http.server $PORT0"
                              :x-waiter-health-check-url "/"}
                             :method :get
                             :path "/")]
        (is (= "Integration Test Sentinel Value" body))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-s3-logs
  (testing-using-waiter-url
    (when (using-k8s? waiter-url)
      (let [headers {:x-waiter-name (rand-name)
                     :x-waiter-max-instances 2
                     :x-waiter-scale-up-factor 0.99
                     :x-waiter-scale-down-factor 0.99
                     :x-kitchen-delay-ms 500}
            _ (log/info "making canary request...")
            {:keys [cookies instance-id service-id]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
            request-fn (fn [] (->> #(make-kitchen-request waiter-url %)
                                   (make-request-with-debug-info headers)
                                   :instance-id))]
        (with-service-cleanup
          service-id
          (assert-service-on-all-routers waiter-url service-id cookies)

          ;; Test that the active instances' logs are available.
          ;; This portion of the test logic was copied from basic-test/test-basic-logs
          (let [active-instances (get-in (service-settings waiter-url service-id :cookies cookies)
                                         [:instances :active-instances])
                log-url (:log-url (first active-instances))
                _ (log/debug "Log Url Active:" log-url)
                make-request-fn (fn [url] (make-request url "" :verbose true))
                {:keys [body] :as logs-response} (make-request-fn log-url)
                _ (assert-response-status logs-response 200)
                _ (log/debug "Response body:" body)
                log-files-list (walk/keywordize-keys (json/read-str body))
                stdout-file-link (:url (first (filter #(= (:name %) "stdout") log-files-list)))
                stderr-file-link (:url (first (filter #(= (:name %) "stderr") log-files-list)))]
            (is (every? #(string/includes? body %) ["stderr" "stdout"])
                (str "Live directory listing is missing entries: stderr and stdout, got response: " logs-response))
            (doseq [file-link [stderr-file-link stdout-file-link]]
              (if (string/starts-with? (str file-link) "http")
                (assert-response-status (make-request-fn file-link) 200)
                (log/warn "test-basic-logs did not verify file link:" stdout-file-link))))

          ;; Get a service with at least one active and one killed instance.
          ;; This portion of the test logic was copied from basic-test/test-killed-instances
          (log/info "starting parallel requests")
          (let [instance-ids-atom (atom #{})
                instance-request-fn (fn []
                                      (let [instance-id (request-fn)]
                                        (swap! instance-ids-atom conj instance-id)))
                instance-ids (->> (parallelize-requests 4 16 instance-request-fn
                                                        :canceled? (fn [] (> (count @instance-ids-atom) 2))
                                                        :verbose true
                                                        :service-id service-id)
                                  (reduce set/union))]
            (is (> (count instance-ids) 1) (str instance-ids)))

          (log/info "waiting for at least one instance to get killed")
          (is (wait-for #(->> (get-in (service-settings waiter-url service-id) [:instances :killed-instances])
                              (map :id)
                              set
                              seq)
                        :interval 2 :timeout 45)
              (str "No killed instances found for " service-id))

          ;; Test that the killed instances' logs were persisted to S3.
          ;; This portion of the test logic was modified from the active-instances tests above.
          (let [log-bucket-url (k8s-log-bucket-url waiter-url)
                killed-instances (get-in (service-settings waiter-url service-id :cookies cookies)
                                         [:instances :killed-instances])
                log-url (:log-url (first killed-instances))
                make-request-fn (fn [url] (make-request url "" :verbose true))
                _ (do
                    (log/info "waiting s3 logs to appear")
                    (is (wait-for
                          #(let [{:keys [body] :as logs-response} (make-request-fn log-url)]
                             (string/includes? body log-bucket-url))
                          :interval 1 :timeout 60)
                        (str "Log URL never pointed to S3 bucket " log-bucket-url)))
                _ (log/debug "Log Url Killed:" log-url)
                {:keys [body] :as logs-response} (make-request-fn log-url)
                _ (assert-response-status logs-response 200)
                _ (log/debug "Response body:" body)
                log-files-list (walk/keywordize-keys (json/read-str body))
                stdout-file-link (:url (first (filter #(= (:name %) "stdout") log-files-list)))
                stderr-file-link (:url (first (filter #(= (:name %) "stderr") log-files-list)))]
            (is (wait-for
                  #(every? (partial string/includes? body) ["stderr" "stdout"])
                  :interval 1 :timeout 30)
                (str "Killed directory listing is missing entries: stderr and stdout, got response: " logs-response))
            (doseq [file-link [stderr-file-link stdout-file-link]]
              (if (string/starts-with? (str file-link) "http")
                (assert-response-status (make-request-fn file-link) 200)
                (log/warn "test-basic-logs did not verify file link:" stdout-file-link)))))))))
