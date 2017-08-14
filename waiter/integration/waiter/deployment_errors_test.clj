;;
;;       Copyright (c) Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.deployment-errors-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.client-tools :refer :all]
            [waiter.settings :as settings]))

(deftest ^:parallel ^:integration-fast test-health-check-requires-authentication
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name "test-health-check-requires-authentication")
                   :x-waiter-health-check-url "/status-401"}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/starts-with? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :health-check-requires-authentication))))
      (delete-service waiter-url service-id))))

; Marked explicit because PROD1 servers always allocate enough memory (this error is not reproducible on Jenkins)
(deftest ^:parallel ^:integration-fast ^:explicit test-not-enough-memory
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name "test-not-enough-memory")
                   :x-waiter-mem 1}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/starts-with? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :not-enough-memory))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-bad-startup-command
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name "test-bad-startup-command")
                   ; misspelled command (invalid prefix asdf)
                   :x-waiter-cmd (str "asdf" (kitchen-cmd "-p $PORT0"))}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/starts-with? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :bad-startup-command))))
      (delete-service waiter-url service-id))))

