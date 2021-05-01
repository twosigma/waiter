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
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          routers (routers waiter-url)
          router-urls (vals routers)]
      (testing "new failing instances appear in instance-tracker state on all routers"
        (let [start-time (t/now)
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-kitchen-delay-ms 5000
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/die"))]
          (with-service-cleanup
            service-id
            (assert-response-status response #{http-502-bad-gateway http-503-service-unavailable})
            ; wait for all routers to have positive number of failed instances
            (is (wait-for
                  (fn every-router-has-failed-instances? []
                    (every? (fn has-failed-instances? [router-url]
                              (let [{:keys [failed-instances]} (:instances (service-settings router-url service-id :cookies cookies))]
                                (pos? (count failed-instances))))
                            router-urls))))
            (doseq [router-url router-urls]
              (let [{:keys [failed-instances]} (:instances (service-settings router-url service-id :cookies cookies))]
                (log/info "The failed instances should be tracked by the instance-tracker" {:failed-instances failed-instances})
                (is (pos? (count failed-instances)))
                (let [query-params "include=instance-failure-handler&include=id->failed-date"
                      {{:keys [last-update-time]
                        {:keys [last-error-time id->failed-date type]} :instance-failure-handler} :state}
                      (get-instance-tracker-state router-url
                                                  :cookies cookies
                                                  :query-params query-params)
                      default-event-handler-ids (set (keys id->failed-date))]
                  (is (t/before? start-time (du/str-to-date last-update-time)))
                  (when (= type "DefaultInstanceFailureHandler")
                    ; assert failed instances are tracked by DefaultInstanceFailureHandler cache of new failed instances
                    (doseq [{:keys [id]} failed-instances]
                      (is (contains? default-event-handler-ids
                                     (keyword id))))
                    ; assert that the error time is recent
                    (is (t/before? start-time (du/str-to-date last-error-time)))))))))))))
