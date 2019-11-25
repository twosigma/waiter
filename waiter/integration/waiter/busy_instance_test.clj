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
(ns waiter.busy-instance-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all]
            [waiter.util.utils :as utils]))

;; the following assertion is flaky: (is (every? #(not %) results))
;; every now and then at least one of the values in that array will be true.
(deftest ^:explicit ^:parallel ^:integration-slow ^:resource-heavy test-busy-instance-not-reserved
  (testing-using-waiter-url
    (let [extra-headers {:x-waiter-name (rand-name)
                         :x-waiter-scale-up-factor 0.99}
          parallelism 8
          make-request-fn (fn [headers]
                            (make-request-with-debug-info (merge extra-headers headers) #(make-kitchen-request waiter-url %)))
          _ (log/info "making canary request")
          {:keys [service-id]} (make-request-fn {})]
      (with-service-cleanup
        service-id
        ;; Make requests to get instances started and avoid shuffling among routers later
        (let [canceled (promise)]
          (future (parallelize-requests parallelism 100 (partial make-request-fn {:x-kitchen-delay-ms 1000})
                                        :verbose true :canceled? (partial realized? canceled)))
          (wait-for #(<= parallelism (num-instances waiter-url service-id)) :timeout 60)
          (deliver canceled :cancel))

        ;; Make a request that returns a 503
        (let [start-millis (System/currentTimeMillis)
              {:keys [headers]} (make-request-fn {:x-kitchen-act-busy "true"})
              router-id (get headers "x-waiter-router-id")
              backend-id (get headers "x-waiter-backend-id")
              blacklist-time-millis (get-in (waiter-settings waiter-url) [:blacklist-config :blacklist-backoff-base-time-ms])]

          (is (integer? blacklist-time-millis))

          ;; We shouldn't see the same instance for blacklist-time-millis from the same router
          (let [canceled (promise)
                results (parallelize-requests
                          parallelism
                          100
                          #(let [{:keys [headers]} (make-request-fn {:x-kitchen-delay-ms 1000})]
                             (if (-> (System/currentTimeMillis) (- start-millis) (< (- blacklist-time-millis 1000)))
                               (and (= backend-id (get headers "x-waiter-backend-id"))
                                    (= router-id (get headers "x-waiter-router-id")))
                               (do (deliver canceled :canceled)
                                   false)))
                          :verbose true
                          :canceled? (partial realized? canceled))]
            (is (every? #(not %) results))))))))

(deftest ^:parallel ^:integration-fast test-max-queue-length
  (testing-using-waiter-url
    (let [max-queue-length 1
          stagger-ms 100
          headers {:x-waiter-name (rand-name)
                   :x-waiter-max-instances 1
                   :x-waiter-min-instances 1
                   :x-waiter-max-queue-length max-queue-length
                   ;; disallow work-stealing interference from balanced
                   :x-waiter-distribution-scheme "simple"
                   :x-kitchen-delay-ms (* 8 stagger-ms)}
          _ (log/debug "making canary request...")
          {:keys [cookies request-headers]} (make-kitchen-request waiter-url headers)
          service-id (retrieve-service-id waiter-url request-headers)]
      (with-service-cleanup
        service-id
        (let [responses (atom [])
              stagger-count (atom 0)
              router-url (some-router-url-with-assigned-slots waiter-url service-id)
              request-fn (fn []
                           (let [n (swap! stagger-count inc)]
                             (utils/sleep (* stagger-ms n))
                             (log/debug "making kitchen request" n))
                           (let [response (make-kitchen-request router-url headers :cookies cookies)]
                             (swap! responses conj response)
                             response))
              num-threads (* 4 max-queue-length)
              num-iters 1
              num-requests (* num-threads num-iters)]
          (parallelize-requests num-threads num-iters request-fn)
          (is (= num-requests (count @responses)))
          (log/info "response statuses:" (map :status @responses))
          (log/info "response bodies:" (map :body @responses))
          (let [responses-with-503 (filter #(= 503 (:status %)) @responses)]
            (is (not (empty? responses-with-503)))
            (is (< (count responses-with-503) num-requests))
            (is (every? (fn [{body :body}]
                          (every? #(str/includes? (str body) (str %))
                                  ["Max queue length exceeded" service-id]))
                        responses-with-503))))))))
