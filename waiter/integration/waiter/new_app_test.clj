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
(ns waiter.new-app-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-new-app
  (testing-using-waiter-url
    (let [headers {:x-kitchen-delay-ms 10000 ;; allow Waiter router state to sync
                   :x-kitchen-echo "true"
                   :x-waiter-name (rand-name)}
          lorem-ipsum "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
          {:keys [body cookies router-id service-id] :as response}
          (make-request-with-debug-info headers #(make-kitchen-request waiter-url % :body lorem-ipsum))]
      (assert-response-status response 200)
      (is (= lorem-ipsum body))

      (let [router-endpoint (router-endpoint waiter-url router-id)]
        (testing "service state with valid service-id"
          (let [settings (service-state router-endpoint service-id :cookies cookies)]
            (is (= router-id (get settings :router-id)) (str settings))
            (is (get-in settings [:state :app-maintainer-state :maintainer-chan-available]) (str settings))
            (is (= 1 (get-in settings [:state :autoscaler-state :healthy-instances])) (str settings))))

        (testing "service state with invalid service-id"
          (let [settings (service-state router-endpoint (str "invalid-" service-id) :cookies cookies)]
            (is (= router-id (get settings :router-id)) (str settings))
            (is (not (get-in settings [:state :app-maintainer-state :maintainer-chan-available])) (str settings))
            (is (empty? (get-in settings [:state :autoscaler-state])) (str settings)))))

      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-slow test-new-app-gc
  (testing-using-waiter-url
    (let [idle-timeout-in-mins 1
          {:keys [service-id]} (make-request-with-debug-info
                                 {:x-waiter-name (rand-name)
                                  :x-waiter-idle-timeout-mins idle-timeout-in-mins}
                                 #(make-kitchen-request waiter-url %))]
      (log/debug "Waiting for" service-id "to show up...")
      (is (wait-for #(= 1 (num-instances waiter-url service-id)) :interval 1))
      (log/debug "Waiting for" service-id "to go away...")
      (is (wait-for #(= 0 (num-instances waiter-url service-id)) :interval 10))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-default-grace-period
  (testing-using-waiter-url
    (if (can-query-for-grace-period? waiter-url)
      (let [headers (-> (kitchen-request-headers)
                        (assoc :x-waiter-name (rand-name))
                        (dissoc :x-waiter-grace-period-secs))
            {:keys [service-id]} (make-request-with-debug-info headers #(make-request waiter-url "/endpoint" :headers %))
            settings-json (waiter-settings waiter-url)
            default-grace-period (get-in settings-json [:service-description-defaults :grace-period-secs])]
        (is (= default-grace-period (service-id->grace-period waiter-url service-id)))
        (delete-service waiter-url service-id))
      (log/warn "test-default-grace-period cannot run because the target Waiter is not using Marathon"))))

(deftest ^:parallel ^:integration-fast test-custom-grace-period
  (testing-using-waiter-url
    (if (can-query-for-grace-period? waiter-url)
      (let [custom-grace-period-secs 120
            headers (-> (kitchen-request-headers)
                        (assoc :x-waiter-name (rand-name)
                               :x-waiter-grace-period-secs custom-grace-period-secs))
            {:keys [service-id]} (make-request-with-debug-info headers #(make-request waiter-url "/endpoint" :headers %))]
        (is (= custom-grace-period-secs (service-id->grace-period waiter-url service-id)))
        (delete-service waiter-url service-id))
      (log/warn "test-custom-grace-period cannot run because the target Waiter is not using Marathon"))))
