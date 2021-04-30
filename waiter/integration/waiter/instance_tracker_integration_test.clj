(ns waiter.instance-tracker-integration-test
  (:require [clojure.set :as set]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [clj-time.core :as t]))

(defn- get-instance-tracker-state
  [waiter-url & {:keys [cookies keywordize-keys query-params]
                 :or {keywordize-keys true}}]
  (let [response (make-request waiter-url "/state/instance-tracker"
                               :cookies cookies
                               :query-params query-params
                               :method :get)]
    (assert-response-status response http-200-ok)
    (cond-> (some-> response :body try-parse-json)
            keywordize-keys walk/keywordize-keys)))

(deftest ^:parallel ^:integration-fast test-instance-tracker-daemon-state
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]

      (testing "no query parameters provides the default state fields"
        (let [{{:keys [supported-include-params] :as state} :state} (get-instance-tracker-state waiter-url :cookies cookies)
              default-state-fields #{:last-update-time :supported-include-params}]
          (is (= (set (keys state))
                 default-state-fields))
          (is (= (set supported-include-params)
                 #{"id->failed-instance" "instance-failure-handler"}))))

      (testing "InstanceEventHandler provides default state fields"
        (let [query-params "include=instance-failure-handler"
              body (get-instance-tracker-state waiter-url
                                               :cookies cookies
                                               :query-params query-params)
              default-inst-event-handler-state-fields #{:last-error-time :supported-include-params :type}]
          (is (set/superset? (set (keys (get-in body [:state :instance-failure-handler] #{"!@#!@#14132"})))
                             default-inst-event-handler-state-fields)))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-instance-tracker-failing-instance
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]

      ; TODO: When instance tracking becomes more reliable in multi router scenarios this test should not be
      ; on the same router
      (testing "new failing instances appear in instance-tracker state on same router"
        (let [start-time (t/now)
              router-url (rand-router-url waiter-url)
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-cmd "invalidcmd34sdfadsve"
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request router-url % :cookies cookies))]
          (with-service-cleanup
            service-id
            (assert-response-status response http-503-service-unavailable)
            (is (wait-for
                  (fn []
                    (let [{:keys [failed-instances]} (:instances (service-settings router-url service-id :cookies cookies))]
                      (pos? (count failed-instances))))))
            (let [{:keys [failed-instances]} (:instances (service-settings router-url service-id :cookies cookies))]
              (log/info "The failed instances should be tracked by the instance-tracker" {:failed-instances failed-instances})
              (is (pos? (count failed-instances)))
              (let [query-params "include=instance-failure-handler&include=recent-id->failed-instance-date&include=id->failed-instance"
                    {{:keys [id->failed-instance]
                      {:keys [last-error-time recent-id->failed-instance-date type]} :instance-failure-handler} :state}
                    (get-instance-tracker-state router-url
                                                :cookies cookies
                                                :query-params query-params)
                    daemon-failed-ids (set (keys id->failed-instance))
                    default-event-handler-ids (set (keys recent-id->failed-instance-date))]
                ; assert failed instances are tracked by daemon
                (doseq [{:keys [id]} failed-instances]
                  (is (contains? daemon-failed-ids
                                 (keyword id)))
                  ; assert failed instances are tracked by DefaultInstanceFailureHandler cache of new failed instances
                  (when (= type "DefaultInstanceFailureHandler")
                    (is (contains? default-event-handler-ids
                                   (keyword id)))))
                ; assert that the error time is recent
                (is (t/before? start-time (du/str-to-date last-error-time)))))))))))
