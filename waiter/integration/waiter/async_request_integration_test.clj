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
(ns waiter.async-request-integration-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [waiter.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-202-response-without-location-header
  (testing-using-waiter-url
    (let [request-processing-time-ms 15000
          async-request-headers {:x-waiter-name (rand-name)
                                 :x-kitchen-delay-ms request-processing-time-ms
                                 :x-kitchen-exclude-headers "location"
                                 :x-kitchen-store-async-response-ms 40000}
          {:keys [headers service-id] :as response}
          (make-request-with-debug-info async-request-headers
                                        #(make-kitchen-request waiter-url % :http-method-fn http/get :path "/async/request"))
          status-location (get (pc/map-keys str/lower-case headers) "location")]
      (assert-response-status response 202)
      (is (str/blank? status-location))
      (testing "validate-in-flight-request-counters"
        (let [service-data (service-settings waiter-url service-id)
              {:keys [async outstanding successful total] :or {async 0} :as request-counts}
              (get-in service-data [:metrics :aggregate :counters :request-counts])]
          (is (= 1 successful) (str request-counts))
          (is (= 1 total) (str request-counts))
          (is (zero? async) (str request-counts))
          (is (zero? outstanding) (str request-counts))))
      (delete-service waiter-url service-id))))

(defn- make-async-request
  [waiter-url request-processing-time-ms]
  (let [headers {:x-waiter-name (rand-name)
                 :x-waiter-max-instances 1
                 :x-waiter-concurrency-level 100}
        {:keys [request-headers service-id cookies]}
        (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
        async-request-headers
        (assoc request-headers :x-kitchen-delay-ms request-processing-time-ms :x-kitchen-store-async-response-ms 40000)
        {:keys [headers] :as response}
        (make-kitchen-request waiter-url async-request-headers :http-method-fn http/get :body "" :path "/async/request")
        status-location (get (pc/map-keys str/lower-case headers) "location")]
    {:cookies cookies
     :request-headers request-headers
     :response response
     :service-id service-id
     :status-location status-location}))

(defn- async-status
  [[router-id endpoint-url] status-location cookies]
  (log/info "Retrieving async status from router" router-id "at" endpoint-url)
  (let [{:keys [headers] :as response} (make-request endpoint-url status-location :cookies cookies)
        result-location (get (pc/map-keys str/lower-case headers) "location")]
    {:response response
     :result-location result-location}))

(defn- cancel-async-request
  [waiter-url status-location allow-cancel cookies]
  (make-request waiter-url status-location
                :http-method-fn http/delete
                :headers {:x-kitchen-allow-async-cancel allow-cancel}
                :cookies cookies))

; Marked explicit due to:
; FAIL in (test-simple-async-request)
; test-simple-async-request validate-completed-request-counters
; {:async 1
;  :async-status 3
;  :successful 1
;  :async-monitor 5
;  :waiting-for-available-instance 0
;  :total 2
;  :async-result 1
;  :waiting-to-stream 0
;  :outstanding 1
;  :streaming 0}
; expected: (zero? (get request-counts :async))
;   actual: (not (zero? 1))
(deftest ^:parallel ^:integration-fast ^:explicit test-simple-async-request
  (testing-using-waiter-url
    (let [request-processing-time-ms 15000
          {:keys [cookies request-headers response service-id status-location]}
          (make-async-request waiter-url request-processing-time-ms)
          async-request-cid (get-in response [:headers "x-cid"] "")]
      (assert-response-status response 202)
      (is (not (str/blank? status-location)))
      (is (str/starts-with? (str status-location) "/waiter-async/status/") (str status-location))

      (testing "validate-in-flight-request-counters"
        (let [service-data (service-settings waiter-url service-id)
              request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
          (is (= 1 (get request-counts :async)) (str request-counts))
          (is (= 1 (get request-counts :outstanding)) (str request-counts))))

      (let [routers (routers waiter-url)]

        (testing "validating-status"
          (doseq [router (seq routers)]
            (let [{:keys [response]} (async-status router status-location cookies)]
              (assert-response-status response 200))))

        ;; wait for async request to complete processing
        (Thread/sleep (+ request-processing-time-ms 1000))

        (testing "validate-complete-status"
          (doseq [router (seq routers)]
            (let [{:keys [response result-location]} (async-status router status-location cookies)]
              (assert-response-status response 303)
              (is result-location))))

        (testing "validate-303-does-not-reset-in-flight-request-counters"
          (let [service-data (service-settings waiter-url service-id)
                request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
            (is (= 1 (get request-counts :async)) (str request-counts))
            (is (= 1 (get request-counts :outstanding)) (str request-counts))))

        (testing "validate-async-result"
          (let [[router-id endpoint-url] (first (seq routers))
                {:keys [result-location]} (async-status [router-id endpoint-url] status-location cookies)]
            (is (str/starts-with? (str result-location) "/waiter-async/result/") (str result-location))
            (is (str/includes? (str result-location) service-id) (str result-location))
            (log/info "validating async result from router" router-id "at" endpoint-url)
            (let [{:keys [body] :as response} (make-request endpoint-url result-location :headers request-headers :cookies cookies)]
              (assert-response-status response 200)
              ;; kitchen response includes the original CID
              (is (str/includes? (str body) async-request-cid) (str body))))))

      (testing "validate-completed-request-counters"
        (let [service-data (service-settings waiter-url service-id)
              request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
          (is (zero? (get request-counts :async)) (str request-counts))
          (is (zero? (get request-counts :outstanding)) (str request-counts))))

      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-cancel-supported-async-request
  (testing-using-waiter-url
    (let [request-processing-time-ms 15000
          {:keys [cookies response service-id status-location]}
          (make-async-request waiter-url request-processing-time-ms)]
      (assert-response-status response 202)
      (is (not (str/blank? status-location)))
      (is (str/starts-with? (str status-location) "/waiter-async/status/") (str status-location))
      (let [routers (routers waiter-url)]
        (let [response (cancel-async-request waiter-url status-location true cookies)]
          (assert-response-status response 204))
        (doseq [router (seq routers)]
          (let [{:keys [response result-location]} (async-status router status-location cookies)]
            (assert-response-status response 410)
            (is (nil? result-location)))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-cancel-unsupported-async-request
  (testing-using-waiter-url
    (let [request-processing-time-ms 15000
          {:keys [cookies response service-id status-location]}
          (make-async-request waiter-url request-processing-time-ms)]
      (assert-response-status response 202)
      (is (not (str/blank? status-location)))
      (let [routers (routers waiter-url)]
        (let [response (cancel-async-request waiter-url status-location false cookies)]
          (assert-response-status response 405))
        (doseq [router (seq routers)]
          (let [{:keys [response]} (async-status router status-location cookies)]
            (assert-response-status response 200))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-slow test-multiple-async-requests
  (testing-using-waiter-url
    (let [waiter-settings (waiter-settings waiter-url)
          metrics-sync-interval-ms (get-in waiter-settings [:metrics-config :metrics-sync-interval-ms])
          inter-router-metrics-interval-ms (max (int 2000) (int (* 3 metrics-sync-interval-ms)))
          headers {:x-waiter-name (rand-name)
                   :x-waiter-concurrency-level 5}
          {:keys [request-headers service-id cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          request-processing-time-ms 30000
          num-threads 20
          num-requests-to-delete 10
          async-responses (parallelize-requests
                            num-threads 1
                            (fn []
                              (log/info "making kitchen request")
                              (let [async-request-headers
                                    (assoc request-headers :x-kitchen-delay-ms (int (* (max 0.5 (double (rand))) request-processing-time-ms))
                                                           :x-kitchen-store-async-response-ms 600000)]
                                (make-kitchen-request waiter-url async-request-headers :http-method-fn http/get :body "" :path "/async/request")))
                            :verbose true)
          status-locations (for [{:keys [headers] :as response} async-responses]
                             (let [status-location (get (pc/map-keys str/lower-case headers) "location")]
                               (assert-response-status response 202)
                               (is (not (str/blank? status-location)))
                               status-location))]

      (testing "validate-pending-request-counters"
        (Thread/sleep inter-router-metrics-interval-ms) ;; allow routers to sync metrics
        (let [expected-total (inc num-threads)
              service-data (service-settings waiter-url service-id)
              {:keys [async outstanding successful total] :as request-counts}
              (get-in service-data [:metrics :aggregate :counters :request-counts])]
          (is (= expected-total total) (str request-counts))
          (is (= expected-total (+ async successful)) (str request-counts))
          (is (= expected-total (+ outstanding successful)) (str request-counts))))

      (testing "delete-arbitrary-requests"
        (let [kitchen-delete-headers {:x-kitchen-allow-async-cancel true, :x-kitchen-204-on-async-cancel false}]
          (doseq [status-location (take num-requests-to-delete status-locations)]
            (let [{:keys [body] :as response} (make-request waiter-url status-location :http-method-fn http/delete
                                                            :headers kitchen-delete-headers :cookies cookies)]
              (assert-response-status response 200)
              (is (str/includes? body "Deleted request-id")))))
        (Thread/sleep inter-router-metrics-interval-ms) ;; allow routers to sync metrics
        (let [service-data (service-settings waiter-url service-id)
              {:keys [async outstanding] :as request-counts}
              (get-in service-data [:metrics :aggregate :counters :request-counts])
              undeleted-requests (- num-threads num-requests-to-delete)]
          (is (<= 0 async undeleted-requests) (str request-counts))
          (is (<= 0 outstanding undeleted-requests) (str request-counts))))

      (wait-for
        (fn []
          (every? (fn [status-location]
                    (let [{:keys [status]} (make-request waiter-url status-location :cookies cookies)]
                      (not= 200 status)))
                  status-locations))
        :interval 10, :timeout 70)

      (testing "validate-async-results"
        (let [routers (routers waiter-url)]
          (doseq [status-location status-locations]
            (let [[router-id endpoint-url] (rand-nth (seq routers))
                  {:keys [result-location response]} (async-status [router-id endpoint-url] status-location cookies)]
              (when (not= 410 (:status response))
                (is (str/starts-with? (str result-location) "/waiter-async/result/") (str result-location))
                (is (str/includes? (str result-location) service-id) (str result-location))
                (log/info "validating async result via router" router-id "at" endpoint-url)
                (let [response (make-request endpoint-url result-location http/get {} {} "" :cookies cookies)]
                  (assert-response-status response 200))))))

        (testing "validate-completed-request-counters"
          (Thread/sleep inter-router-metrics-interval-ms) ;; allow routers to sync metrics
          (let [expected-total (inc num-threads)
                service-data (service-settings waiter-url service-id)
                {:keys [async outstanding successful total] :as request-counts}
                (get-in service-data [:metrics :aggregate :counters :request-counts])]
            (is (= expected-total total) (str request-counts))
            (is (= expected-total successful) (str request-counts))
            (is (zero? async) (str request-counts))
            (is (zero? outstanding) (str request-counts)))))

      (delete-service waiter-url service-id))))
