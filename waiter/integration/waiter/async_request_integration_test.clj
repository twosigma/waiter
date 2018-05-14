;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns waiter.async-request-integration-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-202-response-without-location-header
  (testing-using-waiter-url
    (let [processing-time-ms 15000
          async-request-headers {:x-waiter-name (rand-name)
                                 :x-kitchen-delay-ms processing-time-ms
                                 :x-kitchen-exclude-headers "location"
                                 :x-kitchen-store-async-response-ms 40000}
          {:keys [headers service-id] :as response}
          (make-request-with-debug-info async-request-headers
                                        #(make-kitchen-request waiter-url % :method :get :path "/async/request"))
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
  [waiter-url processing-time-ms]
  (let [headers {:x-waiter-name (rand-name)
                 :x-waiter-max-instances 1
                 :x-waiter-concurrency-level 100}
        {:keys [request-headers service-id cookies]}
        (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
        async-request-headers (-> request-headers
                                  (dissoc "x-cid")
                                  (assoc :x-kitchen-delay-ms processing-time-ms :x-kitchen-store-async-response-ms 40000))
        {:keys [headers] :as response}
        (make-kitchen-request waiter-url async-request-headers :body "" :method :get :path "/async/request")
        status-location (get (pc/map-keys str/lower-case headers) "location")]
    {:cookies cookies
     :request-headers request-headers
     :response response
     :service-id service-id
     :status-location status-location}))

(defn- async-status
  [[router-id router-url] status-location cookies]
  (log/info "Retrieving async status from router" router-id "at" router-url)
  (let [{:keys [headers] :as response} (make-request router-url status-location :cookies cookies)
        result-location (get (pc/map-keys str/lower-case headers) "location")]
    {:response response
     :result-location result-location}))

(defn- cancel-async-request
  [waiter-url status-location allow-cancel cookies]
  (make-request waiter-url status-location
                :cookies cookies
                :headers {:x-kitchen-allow-async-cancel allow-cancel}
                :method :delete))

(deftest ^:parallel ^:integration-fast ^:dev test-simple-async-request
  (testing-using-waiter-url
    (let [processing-time-secs 12
          _ (log/info "making initial request with processing time of" processing-time-secs "seconds")
          {:keys [cookies request-headers response service-id status-location]}
          (make-async-request waiter-url (* processing-time-secs 1000))
          async-request-cid (get-in response [:headers "x-cid"] "")]
      (assert-response-status response 202)
      (is (not (str/blank? status-location)))
      (is (str/starts-with? (str status-location) "/waiter-async/status/") (str status-location))
      (log/info async-request-cid "status-location:" status-location)

      (with-service-cleanup
        service-id
        (testing "validate-in-flight-request-counters"
          (log/info "validate in-flight request counters")
          (is (wait-for
                #(let [service-data (service-settings waiter-url service-id)
                       request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
                   (log/info "request counts" service-id request-counts)
                   (and (= 1 (get request-counts :async))
                        (= 1 (get request-counts :outstanding))))
                :interval 1 :timeout 5)))

        (let [routers (routers waiter-url)]

          (testing "validating-status"
            (doseq [router (seq routers)]
              (let [{:keys [response result-location]} (async-status router status-location cookies)]
                (assert-response-status response 200)
                (is (nil? result-location)))))

          ;; wait for async request to complete processing
          (wait-for
            #(let [router-entry (rand-nth (seq routers))
                   {:keys [response result-location]} (async-status router-entry status-location cookies)]
               (and result-location (= 303 (:status response))))
            :interval 1 :timeout (inc processing-time-secs))

          (testing "validate-complete-status"
            (doseq [router-entry (seq routers)]
              (let [{:keys [response result-location]} (async-status router-entry status-location cookies)]
                (assert-response-status response 303)
                (is result-location))))

          (testing "validate-303-does-not-reset-in-flight-request-counters"
            (log/info "validating 303 status does not reset in-flight request counters")
            (is (wait-for
                  #(let [service-data (service-settings waiter-url service-id)
                         request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
                     (log/info "request counts" service-id request-counts)
                     (and (= 1 (get request-counts :async))
                          (= 1 (get request-counts :outstanding))))
                  :interval 1 :timeout 5)))

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
          (log/info "validate completed request counters")
          (is (wait-for
                #(let [service-data (service-settings waiter-url service-id)
                       request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
                   (log/info "request counts" service-id request-counts)
                   (and (zero? (get request-counts :async))
                        (zero? (get request-counts :outstanding))))
                :interval 1 :timeout 5)))))))

(deftest ^:parallel ^:integration-fast test-cancel-supported-async-request
  (testing-using-waiter-url
    (let [processing-time-ms 15000
          {:keys [cookies response service-id status-location]}
          (make-async-request waiter-url processing-time-ms)]
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
    (let [processing-time-ms 15000
          {:keys [cookies response service-id status-location]}
          (make-async-request waiter-url processing-time-ms)]
      (assert-response-status response 202)
      (is (not (str/blank? status-location)))
      (let [routers (routers waiter-url)]
        (let [response (cancel-async-request waiter-url status-location false cookies)]
          (assert-response-status response 405))
        (doseq [router (seq routers)]
          (let [{:keys [response]} (async-status router status-location cookies)]
            (assert-response-status response 200))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-slow ^:dev test-multiple-async-requests
  (testing-using-waiter-url
    (let [metrics-sync-interval-ms (-> (waiter-settings waiter-url)
                                       (get-in [:metrics-config :metrics-sync-interval-ms]))
          inter-router-metrics-interval-ms (max (int 2000) (int (* 3 metrics-sync-interval-ms)))
          headers {:x-waiter-name (rand-name)
                   :x-waiter-concurrency-level 5}
          {:keys [request-headers service-id cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          processing-time-ms 30000
          num-threads 20
          num-requests-to-delete 10
          async-responses (parallelize-requests
                            num-threads 1
                            (fn []
                              (log/info "making kitchen request")
                              (let [async-request-headers
                                    (assoc request-headers
                                      :x-kitchen-delay-ms (int (* (max 0.5 (double (rand))) processing-time-ms))
                                      :x-kitchen-store-async-response-ms 600000)]
                                (make-kitchen-request waiter-url async-request-headers
                                                      :body ""
                                                      :method :get
                                                      :path "/async/request")))
                            :verbose true)
          status-locations (for [{:keys [headers] :as response} async-responses]
                             (let [status-location (get (pc/map-keys str/lower-case headers) "location")]
                               (assert-response-status response 202)
                               (is (not (str/blank? status-location)))
                               status-location))]

      (with-service-cleanup
        service-id
        (testing "validate-pending-request-counters"
          (Thread/sleep inter-router-metrics-interval-ms) ;; allow routers to sync metrics
          (let [expected-total (inc num-threads)
                {:keys [async outstanding successful total] :as request-counts}
                (-> (service-settings waiter-url service-id)
                    (get-in [:metrics :aggregate :counters :request-counts]))]
            (is (= expected-total total) (str request-counts))
            (is (= expected-total (+ async successful)) (str request-counts))
            (is (= expected-total (+ outstanding successful)) (str request-counts))))

        (testing "delete-arbitrary-requests"
          (let [kitchen-delete-headers {:x-kitchen-allow-async-cancel true, :x-kitchen-204-on-async-cancel false}]
            (doseq [status-location (take num-requests-to-delete status-locations)]
              (let [{:keys [body] :as response} (make-request waiter-url status-location
                                                              :cookies cookies
                                                              :headers kitchen-delete-headers
                                                              :method :delete)]
                (assert-response-status response 200)
                (is (str/includes? body "Deleted request-id")))))
          (Thread/sleep inter-router-metrics-interval-ms) ;; allow routers to sync metrics
          (let [{:keys [async outstanding] :as request-counts}
                (-> (service-settings waiter-url service-id)
                    (get-in [:metrics :aggregate :counters :request-counts]))
                num-requests-alive (- num-threads num-requests-to-delete)]
            (is (<= 0 async num-requests-alive) (str request-counts))
            (is (<= 0 outstanding num-requests-alive) (str request-counts))))

        (wait-for
          (fn []
            (every? (fn [status-location]
                      (let [{:keys [status]} (make-request waiter-url status-location :cookies cookies)]
                        (not= 200 status)))
                    status-locations))
          :interval 5, :timeout 70)

        (testing "validate-async-results"
          (let [routers (routers waiter-url)]
            (doseq [status-location status-locations]
              (let [[router-id endpoint-url] (rand-nth (seq routers))
                    {:keys [result-location response]} (async-status [router-id endpoint-url] status-location cookies)]
                (when (not= 410 (:status response))
                  (is (str/starts-with? (str result-location) "/waiter-async/result/") (str result-location))
                  (is (str/includes? (str result-location) service-id) (str result-location))
                  (log/info "validating async result via router" router-id "at" endpoint-url)
                  (let [response (make-request endpoint-url result-location :cookies cookies :method :get)]
                    (assert-response-status response 200))))))

          (testing "validate-completed-request-counters"
            (Thread/sleep inter-router-metrics-interval-ms) ;; allow routers to sync metrics
            (let [expected-total (inc num-threads)
                  {:keys [async outstanding successful total] :as request-counts}
                  (-> (service-settings waiter-url service-id)
                      (get-in [:metrics :aggregate :counters :request-counts]))]
              (is (= expected-total total) (str request-counts))
              (is (= expected-total successful) (str request-counts))
              (is (zero? async) (str request-counts))
              (is (zero? outstanding) (str request-counts)))))))))
