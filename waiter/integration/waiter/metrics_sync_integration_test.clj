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
(ns waiter.metrics-sync-integration-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]))

(defmacro assert-invalid-body
  "Asserts that sending the provided body results in the expected-msg being inside the response body. This is used
  to confirm waiter is validating the /metrics/external endpoint properly."
  [waiter-url req-body expected-msg]
  `(let [waiter-url# ~waiter-url
         req-body# ~req-body
         string-req-body# (utils/clj->json req-body#)
         expected-msg# ~expected-msg
         res# (make-request waiter-url# "/metrics/external" :method :post :body string-req-body#)
         res-body# (:body res#)]
     (assert-response-status res# http-400-bad-request)
     (is (.contains res-body# expected-msg#))))

(deftest ^:parallel ^:integration-fast test-external-metrics-validate
  (testing-using-waiter-url
    (let [cluster-name (retrieve-cluster-name waiter-url)
          metrics-payload
          {"cluster" cluster-name
           "service-metrics"
           {"s1" {"i1" {"updated-at" "2022-05-31T14:50:44.956Z"
                        "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                   "active-request-count" 0}}}}}]

      (testing "method must be POST"
        (let [{:keys [body status]} (make-request waiter-url "/metrics/external" :method :get)]
          (is (= status http-400-bad-request))
          (is (.contains body "Invalid request method. Only POST is supported.") body)))

      (testing "wrong cluster in payload causes a 400 response"
        (let [expected-msg "Metrics are for a different cluster."]
          (assert-invalid-body waiter-url (assoc-in metrics-payload ["cluster"] "different-cluster") expected-msg)))

      (testing "updated-at must be an ISO timestamp"
        (let [expected-msg "Invalid 's1.i1.updated-at' field. Must be ISO-8601 time."]
          (assert-invalid-body
            ; wrong time format
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "2022-05-31") expected-msg)
          (assert-invalid-body
            ; missing milliseconds
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "2022-05-31T02:50:44Z") expected-msg)
          (assert-invalid-body
            ; missing 'Z' character
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "2022-05-31T14:50:44.956") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "not-iso-string") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] 5) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "updated-at"] "") expected-msg)))

      (testing "last-request-time must be an ISO timestamp"
        (let [expected-msg "Invalid 's1.i1.metrics.last-request-time' field. Must be ISO-8601 time."]
          (assert-invalid-body
            ; wrong time format
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "2022-05-31") expected-msg)
          (assert-invalid-body
            ; missing milliseconds
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "2022-05-31T02:50:44Z") expected-msg)
          (assert-invalid-body
            ; missing 'Z' character
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "2022-05-31T14:50:44.956") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "not-iso-string") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] 5) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "last-request-time"] "") expected-msg)))

      (testing "active-request-count must be a non negative integer"
        (let [expected-msg "Invalid 's1.i1.metrics.active-request-count' field. Must be non-negative integer."]
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "active-request-count"] -1) expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "active-request-count"] "test") expected-msg)
          (assert-invalid-body
            waiter-url (assoc-in metrics-payload ["service-metrics" "s1" "i1" "metrics" "active-request-count"] "") expected-msg))))))

(defn get-last-update-time-from-metrics-response
  "Gets last-update-time for router from /state/router-metrics response"
  [router-id response]
  (-> response :body
    try-parse-json
    (get-in ["state" "last-update-times" router-id])
    du/str-to-date))

(defn send-metrics-and-assert-expected-metrics
  "Send metrics-payload to the waiter-url and assert that each router reports the expected-metrics as well as never
  reports any metrics for the expected-nil-keys-list."
  [routers cookies metrics-payload expected-metrics expected-nil-keys-list & {:keys [fail-eagerly-on-nil-keys]
                                                                              :or {fail-eagerly-on-nil-keys true}}]
  (let [[first-router-id first-router-url] (first routers)
        initial-metrics-response (make-request first-router-url "/state/router-metrics"
                                               :cookies cookies
                                               :headers {:content-type "application/json"})
        update-metrics-response (make-request first-router-url "/metrics/external"
                                              :method :post
                                              :cookies cookies
                                              :body (utils/clj->json metrics-payload)
                                              :headers {:content-type "application/json"})]
    (assert-response-status initial-metrics-response http-200-ok)
    (assert-response-status update-metrics-response http-200-ok)
    (is (= {"no-op" false}
           (-> update-metrics-response :body
             try-parse-json)))
    ; expect last-update-time to be changed to later time in the post update metrics response
    (is (wait-for
          (fn metrics-last-request-time-updated? []
            (let [metrics-response (make-request first-router-url "/state/router-metrics"
                                                 :cookies cookies
                                                 :headers {:content-type "application/json"})]
              (assert-response-status metrics-response http-200-ok)
              (t/before? (get-last-update-time-from-metrics-response first-router-id initial-metrics-response)
                         (get-last-update-time-from-metrics-response first-router-id metrics-response))))
          :interval 1 :timeout 5)
        "Router metrics state did not show there was any update to the last-update-time, even though our POST /metrics/external
        request was successful.")

    (log/info "expected metrics from routers:" {:expected-nil-keys-list expected-nil-keys-list
                                                :expected-metrics expected-metrics})
    (is (wait-for
          (fn []
            (every?
              (fn router-has-expected-metrics?-fn [[_ router-url]]
                (let [metrics-state-response (make-request router-url "/state/router-metrics"
                                                           :cookies cookies
                                                           :headers {:content-type "application/json"})
                      actual-metrics (-> metrics-state-response :body
                                       try-parse-json
                                       (get-in ["state" "external-metrics"]))]
                  (log/info "metrics for router:" {:cur-metrics actual-metrics
                                                   :router router-url})

                  ; We expect these requests to succeed ALL the time, and if failed, we consider the wait-for assertion
                  ; to fail.
                  (assert-response-status metrics-state-response http-200-ok)

                  (let [provided-key-values-are-nil? (every? nil? (map #(get-in actual-metrics %) expected-nil-keys-list))
                        ; When deep diffing the expected metrics and the actual metrics returned. We expect that the actual
                        ; metrics is a super map of the expected metrics. This is because the expected metrics keys->values
                        ; should all be in the actual metrics
                        [only-in-expected only-in-actual in-both] (data/diff expected-metrics actual-metrics)]
                    (log/info "actual metrics from routers:" {:expected-nil-keys-values (map #(get-in actual-metrics %) expected-nil-keys-list)
                                                              :in-both in-both
                                                              :only-in-expected only-in-expected
                                                              :only-in-actual only-in-actual
                                                              :router-url router-url})
                    (when fail-eagerly-on-nil-keys
                      (is provided-key-values-are-nil? "Expected metrics to not have metrics for provided keys."))
                    (and
                      provided-key-values-are-nil?
                      (nil? only-in-expected)
                      (= in-both expected-metrics)))))
              routers))
          :interval 1 :timeout 5)
        "All Waiter routers never reported the expected metrics.")))

(deftest ^:parallel ^:integration-slow test-external-metrics-updates-metrics-syncer
  (testing-using-waiter-url
    (let [cluster-name (retrieve-cluster-name waiter-url)
          routers (routers waiter-url)]
      (testing "Empty body results in no-op"
        (let [req-body {"cluster" cluster-name}
              {:keys [body] :as response} (make-request waiter-url "/metrics/external"
                                                        :method :post
                                                        :body (utils/clj->json req-body)
                                                        :headers {:content-type "application/json"})]
          (assert-response-status response http-200-ok)
          (is (= {"no-op" true} (try-parse-json (str body))))))

      (testing "Metrics payload with only irrelevant external metrics will result in a no-op"
        (let [; s1 and i1 are never going to be an actual service-id or instance-id on the waiter routers. These metrics
              ; are expected to be filtered out.
              req-body {"cluster" cluster-name
                        "service-metrics" {"s1" {"i1" {"updated-at" "2022-05-31T14:50:44.956Z"
                                                       "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                                                  "active-request-count" 0}}}}}
              {:keys [body] :as response} (make-request waiter-url "/metrics/external"
                                                        :method :post
                                                        :body (utils/clj->json req-body)
                                                        :headers {:content-type "application/json"})]
          (assert-response-status response http-200-ok)
          (is (= {"no-op" true} (try-parse-json (str body))))))

      (let [; make sure raven doesn't send external metrics for these services
            extra-headers {:content-type "application/json"
                           :x-waiter-env-raven_export_metrics "false"}
            extra-headers-1 (assoc extra-headers :x-waiter-name (rand-name))
            {:keys [cookies] :as canary-response-1} (make-request-with-debug-info extra-headers-1 #(make-kitchen-request waiter-url %))
            instance-id-1 (:instance-id canary-response-1)
            service-id-1 (:service-id canary-response-1)]
        (assert-response-status canary-response-1 http-200-ok)
        (with-service-cleanup
          service-id-1
          (let [extra-headers-2 (assoc extra-headers :x-waiter-name (rand-name))
                canary-response-2 (make-request-with-debug-info extra-headers-2 #(make-kitchen-request waiter-url %))
                instance-id-2 (:instance-id canary-response-2)
                service-id-2 (:service-id canary-response-2)]
            (assert-response-status canary-response-2 http-200-ok)
            (with-service-cleanup
              service-id-2
              (let [metrics-payload
                    {"cluster" cluster-name
                     "service-metrics"
                     {service-id-1 {instance-id-1 {"updated-at" "3000-05-31T14:50:44.956Z"
                                                   "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                                              "active-request-count" 1}}}
                      service-id-2 {instance-id-2 {"updated-at" "3000-05-31T14:50:44.956Z"
                                                   "metrics" {"last-request-time" "2022-05-31T14:00:50.103Z"
                                                              "active-request-count" 2
                                                              "extra-metadata-should-not-be-filtered" "any-value"}}}}}
                    expected-metrics (get metrics-payload "service-metrics")]

                (testing "Sending external metrics for multiple instances updates existing metrics. Extra metadata should not be filtered out."
                  (send-metrics-and-assert-expected-metrics routers cookies metrics-payload expected-metrics []))

                (testing "Strictly later updated-at timestamp for instance metrics are stored. Unknown instances are ignored."
                  (let [metrics-payload-instance-1-updated
                        {"cluster" cluster-name
                         "service-metrics"
                         {service-id-1 {"this-instance-is-gibberish" {"updated-at" "3001-05-31T14:50:44.956Z"
                                                                      "metrics" {"last-request-time" "2022-05-01T14:50:44.956Z"
                                                                                 "active-request-count" 0}}
                                        instance-id-1 {"updated-at" "3001-05-31T14:50:44.956Z"
                                                       "metrics" {"last-request-time" "2022-05-01T14:50:44.956Z"
                                                                  "active-request-count" 0}}}
                          service-id-2 {"another-fake-instance-id" {"updated-at" "3000-05-31T14:50:44.800Z"
                                                                    "metrics" {"last-request-time" "2022-06-01T14:00:50.103Z"
                                                                               "active-request-count" 0}}

                                        ; instance-id-2 is attempting to update with stale metrics
                                        instance-id-2 {"updated-at" "3000-05-31T14:50:44.800Z"
                                                       "metrics" {"last-request-time" "2022-06-01T14:00:50.103Z"
                                                                  "active-request-count" 0}}}}}

                        ; We only expect instance-id-1 to get updated because it has a later 'updated-at' timestamp
                        ; compared to previous stored while instance-id-2 has a stale timestamp and should be discarded
                        expected-metrics-instance-1-updated
                        (assoc-in expected-metrics [service-id-1 instance-id-1]
                                  (get-in metrics-payload-instance-1-updated ["service-metrics" service-id-1 instance-id-1]))

                        ; We expect these keys list have nil values on each router
                        expected-nil-keys-list [[service-id-1 "this-instance-is-gibberish"]
                                                [service-id-2 "another-fake-instance-id"]]]
                    (send-metrics-and-assert-expected-metrics
                      routers cookies metrics-payload-instance-1-updated expected-metrics-instance-1-updated expected-nil-keys-list)))

                (testing "After service is killed, external metrics for that service should be discarded when new metrics are sent"
                  (let [metrics-payload-instance-1-updated
                        {"cluster" cluster-name
                         "service-metrics"
                         {service-id-1 {instance-id-1 {"updated-at" "3002-05-31T14:50:44.956Z"
                                                       "metrics" {"last-request-time" "2022-05-01T14:50:44.956Z"
                                                                  "active-request-count" 0}}}}}
                        expected-metrics-instance-1-updated (get metrics-payload-instance-1-updated "service-metrics")

                        ; service-id-2 was killed, and should not be tracked in external metrics
                        expected-nil-keys-list [[service-id-2 instance-id-2]]]
                    (delete-service waiter-url service-id-2)

                    ; wait for service to no longer be tracked by routers
                    (async/<!! (async/timeout 3000))
                    (send-metrics-and-assert-expected-metrics
                      routers cookies metrics-payload-instance-1-updated expected-metrics-instance-1-updated
                      expected-nil-keys-list :fail-eagerly-on-nil-keys false)))))))))))

(deftest ^:parallel ^:integration-slow test-external-metrics-updates-trigger-new-service
  (testing-using-waiter-url
    (let [cluster-name (retrieve-cluster-name waiter-url)
          routers (routers waiter-url)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          token-name (create-token-name waiter-url ".")
          post-token-req-body (assoc (kitchen-params)
                                :token token-name
                                :metadata {"waiter-token-expiration-date"
                                           (du/date-to-str (t/from-now (t/days 2)) du/formatter-year-month-day)}
                                :name (rand-name)
                                ; we have to specify the run-as-user because the daemon does not support run-as-requester
                                :run-as-user (retrieve-username))
          post-token-res (post-token waiter-url post-token-req-body :cookies cookies)]
      (assert-response-status post-token-res 200)
      (testing "updating a token and sending new last-request-time for the token causes new service to start"
        (try
          (let [request-headers {"x-waiter-token" token-name}
                {:keys [instance-id service-id] :as ping-res}
                (make-request-with-debug-info request-headers #(make-request waiter-url "/waiter-ping" :headers %))]
            (assert-response-status ping-res http-200-ok)
            (with-service-cleanup
              service-id
              ; service-id and last-request-time should be tracked by the state maintainer
              (is (wait-for
                    (fn service-id-tracked-by-every-router?-fn
                      []
                      (every?
                        (fn [[_ router-url]]
                          (let [{:keys [body] :as res} (get-start-new-service-maintainer-state router-url cookies)]
                            (assert-response-status res http-200-ok)
                            (let [service-id-last-request-time
                                  (some-> body
                                          try-parse-json
                                          (get-in ["state" "service-id->last-request-time" service-id])
                                          du/str-to-date-safe)]
                              (log/info "router-url start-new-services-maintainer entry for service-id:"
                                        {:router-url router-url
                                         :service-id service-id
                                         :service-id-last-request-time service-id-last-request-time})
                              (some? service-id-last-request-time))))
                        routers))
                    :interval 1
                    :timeout 10)
                  (str "Service-id was never reported in service-id->last-request-time for start-new-services-maintainer: " service-id))

              ; make a dummy update to the token so that it is pointing to a new service-id
              (let [update-token-body (assoc-in post-token-req-body [:metadata "foo"] (rand-name))
                    update-token-res (post-token waiter-url update-token-body :cookies cookies)]
                (assert-response-status update-token-res http-200-ok))

              ; routers should never report token with multiple services because it should resolve to the same service-id.
              ; no new services for the token should be started. 10 seconds should be sufficient in waiting to see if a
              ; new service is started as the router maintainer runs on 5 second intervals
              (is (not
                    (wait-for
                      (fn service-id-tracked-by-every-router?-fn
                        []
                        (every?
                          (fn [[_ router-url]]
                            (let [service-ids (get-services-for-token-and-assert router-url token-name cookies)]
                              (log/info "router-url reported service-ids for token:"
                                        {:service-ids (str/join ", " service-ids)
                                         :router-url router-url})
                              (not= [service-id] service-ids)))
                          routers))
                      :interval 1
                      :timeout 10))
                  (str "Waiter routers reported different service-ids for token than the expected: " service-id))

              ; modify the last-request-time for the token externally, which should trigger starting of new service for
              ; the token as it should resolve to a new service-id
              (let [new-service-id (retrieve-service-id waiter-url {"x-waiter-token" token-name} :query-params {"latest" "true"})
                    ; last-request-time is always later than current last-request-time
                    last-request-time (du/date-to-str (t/from-now (t/days 2)))
                    metrics-payload {"cluster" cluster-name
                                     "service-metrics" {service-id {instance-id {"updated-at" last-request-time
                                                                                 "metrics" {"last-request-time" last-request-time
                                                                                            "active-request-count" 1}}}}}
                    expected-metrics (get metrics-payload "service-metrics")]
                (send-metrics-and-assert-expected-metrics routers cookies metrics-payload expected-metrics [])

                ; new service is started and recognized by every router
                (is (wait-for
                      (fn new-service-started?-fn
                        []
                        (every?
                          (fn [[_ router-url]]
                            (let [service-ids (get-services-for-token-and-assert router-url token-name cookies)]
                              (log/info "router-url reported service-ids for token while waiting for new service to start:"
                                        {:expected-service-ids (str/join ", " [service-id new-service-id])
                                         :service-ids (str/join ", " service-ids)
                                         :router-url router-url})
                              (= #{service-id new-service-id}
                                 (set service-ids))))
                          routers))
                      :interval 5
                      :timeout 30)
                    (str "new service never started for token: " token-name)))))
          (finally
            (delete-token-and-assert waiter-url token-name)))))))

(defn assert-num-queued-requests
  "Assert that all routers eventually report the correct number of queued requests for a 'service-id'"
  [routers cookies service-id expected-queued-requests]
  (is (wait-for
        (fn []
          (every?
            (fn router-has-expected-queued-requests [[router-id router-url]]
              (let [metrics-state-response (make-request router-url "/state/router-metrics"
                                                         :cookies cookies
                                                         :headers {:content-type "application/json"})
                    metrics (-> metrics-state-response :body
                                try-parse-json
                                (get-in ["state" "metrics" "routers"]))
                    router-id-queued-request-count-entries
                    (->> metrics seq (map (fn entry->queued-request-entry
                                            [[router-id metric]]
                                            [router-id (get-in metric [service-id "waiting-for-available-instance"])])))

                    ; we have to total the requests across routers before asserting
                    actual-queued-requests (->> router-id-queued-request-count-entries
                                                (map second)
                                                (remove nil?)
                                                (reduce +))]
                (log/info "queued requests reported by router" {:actual-queued-requests actual-queued-requests
                                                                :expected-queued-requests expected-queued-requests
                                                                :service-id service-id
                                                                :router-id router-id
                                                                :router-id->metric (into {} router-id-queued-request-count-entries)
                                                                :router-url router-url})
                (= expected-queued-requests actual-queued-requests)))
            routers))
        :interval 1 :timeout 5)))

(defn assert-num-outstanding-requests
  "Assert that all routers report the correct number of outstanding requests for a service-id"
  [routers cookies service-id expected-outstanding-requests]
  (is (every?
        (fn router-has-expected-outstanding-requests [[router-id router-url]]
          (let [service-settings (service-settings router-url service-id :cookies cookies)
                actual-outstanding-requests (get-in service-settings [:request-metrics :outstanding])]
            (log/info "outstanding requests reported by router" {:actual-outstanding-requests actual-outstanding-requests
                                                                 :expected-outstanding-requests expected-outstanding-requests
                                                                 :service-id service-id
                                                                 :router-id router-id
                                                                 :router-url router-url})
            (= expected-outstanding-requests actual-outstanding-requests)))
        routers)))

(deftest ^:parallel ^:integration-slow test-waiting-for-available-instance-metrics-syncer
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          routers (routers waiter-url)
          extra-headers {:content-type "application/json"
                         ; service should take at least 5 seconds to start so that we can guarantee requests
                         ; are in queue for at least 5 seconds (we will later assert metrics report queue count).
                         :x-waiter-cmd (str "sleep 5; " (kitchen-cmd "-p $PORT0"))
                         :x-waiter-name (rand-name)
                         :x-waiter-min-instances 1}
          make-service-request!
          (fn []
            (async/go
              (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %))))

          ; make initial request so that we know the calculated service-id for cleanup
          service-id (retrieve-kitchen-service-id waiter-url extra-headers)]
      (with-service-cleanup
        service-id
        (let [
              ; total number of queued requests should be 4
              num-expected-queued-requests 4
              result-ch
              (async/go
                ; start 4 requests concurrently, which should each take at least 5 seconds before exiting
                (let [request-chs (doall (repeatedly num-expected-queued-requests make-service-request!))]
                  (doseq [ping-response (map async/<!! request-chs)]
                    (is (= service-id (:service-id ping-response)))
                    (assert-response-status ping-response http-200-ok))))]
          (assert-num-queued-requests routers cookies service-id num-expected-queued-requests)
          ; let requests terminate before confirming that the queued requests became zero as there are no longer
          ; any requests hitting the service
          (async/<!! result-ch)
          (assert-num-queued-requests routers cookies service-id 0))))))

(defn assert-scale-to-instances
  "Assert number of instances for a service-id."
  [routers cookies service-id expected-num-instances]
  (is (wait-for
        (fn []
          (every?
            (fn router-has-expected-scale-to-instances [[router-id router-url]]
              (let [scale-to-instances (get-in (service-state router-url service-id :cookies cookies)
                                               [:state :autoscaler-state :scale-to-instances])]
                (log/info "outstanding requests reported by router" {:actual-scale-to-instances scale-to-instances
                                                                     :expected-scale-to-instances expected-num-instances
                                                                     :service-id service-id
                                                                     :router-id router-id
                                                                     :router-url router-url})
                (= expected-num-instances scale-to-instances)))
            routers))
        :interval 1 :timeout 5)))

;; Test is marked explicit because it is known to be flaky
(deftest ^:explicit ^:parallel ^:integration-slow ^:resource-heavy test-bypass-services-use-external-metrics-for-outstanding-requests
  (testing-using-waiter-url
    (let [cluster-name (retrieve-cluster-name waiter-url)
          routers (routers waiter-url)]

      (let [extra-headers {:content-type "application/json"
                           ; this must be simple distribution because we rely on x-kitchen-delay-ms to cause queue build up
                           :x-waiter-distribution-scheme "simple"
                           :x-waiter-concurrency-level 1
                           ; make sure raven doesn't send external metrics for these services
                           :x-waiter-env-raven_export_metrics "false"
                           ; force outstanding metrics to use external metrics for outstanding metrics calculation
                           :x-waiter-metadata-waiter-proxy-bypass-opt-in "true"
                           :x-waiter-min-instances 1
                           :x-waiter-name (rand-name)
                           ; force outstanding metrics to use external metrics for outstanding metrics calculation
                           :x-waiter-routing-mode "ingress-distributed"
                           ; used for assertions on scale-to-instances target
                           :x-waiter-scale-up-factor 0.99
                           :x-waiter-scale-down-factor 0.99}
            {:keys [cookies instance-id service-id] :as response} (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %))]
        (assert-response-status response http-200-ok)
        (with-service-cleanup
          service-id
          (let [make-delayed-service-request!
                (fn [& {:keys [delay-ms] :or {delay-ms 0}}]
                  (async/go
                    (make-request-with-debug-info
                      (assoc extra-headers :x-kitchen-delay-ms delay-ms)
                      #(make-kitchen-request waiter-url %))))
                expected-num-of-queued-requests 3
                expected-num-of-active-requests 4
                expected-num-of-outstanding-requests (+ expected-num-of-queued-requests expected-num-of-active-requests)
                result-ch
                (async/go
                  ; because the concurrency-level is 1, sending a long-running request will cause requests after it to be queued
                  (let [long-running-ch (make-delayed-service-request! :delay-ms 7000)
                        _ (async/<!! (async/timeout 1000))
                        request-chs (doall (repeatedly expected-num-of-queued-requests make-delayed-service-request!))]
                    (doseq [ping-response (map async/<!! (conj request-chs long-running-ch))]
                      (is (= service-id (:service-id ping-response)))
                      (assert-response-status ping-response http-200-ok))))
                metrics-payload
                {"cluster" cluster-name
                 "service-metrics"
                 {service-id {instance-id {"updated-at" "3000-05-31T14:50:44.956Z"
                                           "metrics" {"last-request-time" "2022-05-31T14:50:44.956Z"
                                                      "active-request-count" expected-num-of-active-requests}}}}}
                expected-metrics (get metrics-payload "service-metrics")]

            (testing "outstanding requests for a bypass service should be the same as active-request-count + queued-requests"
              (assert-num-queued-requests routers cookies service-id expected-num-of-queued-requests)
              (send-metrics-and-assert-expected-metrics routers cookies metrics-payload expected-metrics [])
              ; number of outstanding requests should be the number of queued requests and number of active-request-count
              (assert-num-outstanding-requests routers cookies service-id expected-num-of-outstanding-requests))

            (testing "scale-to-instances matches the number of outstanding requests for a bypass service"
              (assert-scale-to-instances routers cookies service-id expected-num-of-outstanding-requests))

            (testing "with no queued requests, the number of outstanding requests should be the same as the number of queued-requests"
              ; let requests terminate before confirming that the queued requests became zero as there are no longer
              ; any requests hitting the service
              (async/<!! result-ch)
              (assert-num-queued-requests routers cookies service-id 0)

              ; no more queued requests, so outstanding requests should be the same as the active requests
              (assert-num-outstanding-requests routers cookies service-id expected-num-of-active-requests))

            (testing "scale-to-instances matches the number outstanding requests when there are no queued requests"
              (assert-scale-to-instances routers cookies service-id expected-num-of-active-requests))))))))
