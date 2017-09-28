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
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [waiter.client-tools :refer :all]
            [waiter.settings :as settings]))

(defn get-router->service-state
  [waiter-url service-id]
  (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]
    (->> (routers waiter-url)
         (map (fn [[_ router-url]]
                (service-state router-url service-id :cookies cookies)))
         (pc/map-from-keys :router-id))))

(deftest ^:parallel ^:integration-slow test-invalid-health-check-response
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; health check endpoint always returns status 402
                   :x-waiter-health-check-url "/bad-status?status=402"
                   :x-waiter-queue-timeout 600000}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (try
        (assert-response-status response 503)
        (is service-id)
        (is (get-router->service-state waiter-url service-id))
        (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url)
                                                              :messages
                                                              :invalid-health-check-response)))
            (str "router->service-state:\n" (with-out-str
                                              (pprint/pprint
                                                (get-router->service-state waiter-url service-id)))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-slow test-cannot-connect
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; nothing to connect to
                   :x-waiter-cmd "sleep 3600"
                   :x-waiter-queue-timeout 600000}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-shell-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (try
        (assert-response-status response 503)
        (is service-id)
        (is (get-router->service-state waiter-url service-id))
        (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url)
                                                              :messages
                                                              :cannot-connect)))
            (str "router->service-state:\n" (with-out-str
                                              (pprint/pprint
                                                (get-router->service-state waiter-url service-id)))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-slow test-health-check-timed-out
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; health check endpoint sleeps for 300000 ms (= 5 minutes)
                   :x-waiter-health-check-url "/sleep?sleep-ms=300000&status=400"
                   :x-waiter-queue-timeout 600000}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (try
        (assert-response-status response 503)
        (is service-id)
        (is (get-router->service-state waiter-url service-id))
        (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url)
                                                              :messages
                                                              :health-check-timed-out)))
            (str "router->service-state:\n" (with-out-str
                                              (pprint/pprint
                                                (get-router->service-state waiter-url service-id)))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-health-check-requires-authentication
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-health-check-url "/bad-status?status=401"}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (try
        (assert-response-status response 503)
        (is service-id)
        (is (get-router->service-state waiter-url service-id))
        (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url)
                                                         :messages
                                                         :health-check-requires-authentication)))
            (str "router->service-state:\n" (with-out-str
                                              (pprint/pprint
                                                (get-router->service-state waiter-url service-id)))))

        (finally
          (delete-service waiter-url service-id))))))

; Marked explicit because not all servers use cgroups to limit memory (this error is not reproducible on testing platforms)
(deftest ^:parallel ^:integration-fast ^:explicit test-not-enough-memory
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-mem 1}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (try
        (assert-response-status response 503)
        (is service-id)
        (is (get-router->service-state waiter-url service-id))
        (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url)
                                                              :messages
                                                              :not-enough-memory)))
            (str "router->service-state:\n" (with-out-str
                                              (pprint/pprint
                                                (get-router->service-state waiter-url service-id)))))
        (finally
          (delete-service waiter-url service-id))))))

(deftest ^:parallel ^:integration-fast test-bad-startup-command
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   ; misspelled command (invalid prefix asdf)
                   :x-waiter-cmd (str "asdf" (kitchen-cmd "-p $PORT0"))}
          {:keys [headers body] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          service-id (get headers "x-waiter-service-id")]
      (try
        (is service-id)
        (is (get-router->service-state waiter-url service-id))
        (assert-response-status response 503)
        (is (str/includes? body (str "Deployment error: " (-> (waiter-settings waiter-url)
                                                              :messages
                                                              :bad-startup-command)))
            (str "router->service-state:\n" (with-out-str
                                              (pprint/pprint
                                                (get-router->service-state waiter-url service-id)))))
        (finally
          (delete-service waiter-url service-id))))))

