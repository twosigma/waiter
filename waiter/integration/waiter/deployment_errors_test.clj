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

(deftest ^:parallel ^:integration-slow test-invalid-health-check-response
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; health check endpoint always returns status 402
                   :x-waiter-health-check-url "/bad-status?status=402"
                   :x-waiter-queue-timeout 600000}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :invalid-health-check-response))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-slow test-cannot-connect
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; nothing to connect to
                   :x-waiter-cmd "sleep 3600"
                   :x-waiter-queue-timeout 600000}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-shell-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :cannot-connect))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-slow test-health-check-timed-out
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; health check endpoint sleeps for 300000 ms (= 5 minutes)
                   :x-waiter-health-check-url "/sleep?sleep-ms=300000&status=400"
                   :x-waiter-queue-timeout 600000}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :health-check-timed-out))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-health-check-requires-authentication
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-health-check-url "/bad-status?status=401"}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :health-check-requires-authentication))))
      (delete-service waiter-url service-id))))

; Marked explicit because not all servers use cgroups to limit memory (this error is not reproducible on testing platforms)
(deftest ^:parallel ^:integration-fast ^:explicit test-not-enough-memory
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-mem 1}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :not-enough-memory))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-bad-startup-command
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; misspelled command (invalid prefix asdf)
                   :x-waiter-cmd (str "asdf" (kitchen-cmd "-p $PORT0"))}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (is (not (nil? service-id)))
      (assert-response-status response 503)
      (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url) :messages :bad-startup-command))))
      (delete-service waiter-url service-id))))

