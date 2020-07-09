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
(ns waiter.autoscaling-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.utils :as utils]))

(defn- scaling-for-service-test
  [testing-str waiter-url target-instances concurrency-level request-fn]
  (testing testing-str
    (let [cancellation-token-atom (atom false)
          {:keys [cookies] :as first-request} (request-fn {})
          service-id (retrieve-service-id waiter-url (:request-headers first-request))
          count-instances (fn [] (num-instances waiter-url service-id :cookies cookies))]
      (with-service-cleanup
        service-id
        (is (wait-for #(= 1 (count-instances))) "First instance never started")
        (let [requests-per-thread 100
              num-threads (* target-instances concurrency-level)
              futures (parallelize-requests
                        num-threads
                        requests-per-thread
                        #(request-fn cookies)
                        :canceled? (fn [] @cancellation-token-atom)
                        :service-id service-id
                        :verbose true
                        :wait-for-tasks false)]
          (log/info "waiting for" service-id "to scale to" target-instances "instances")
          (is (wait-for
                (fn []
                  (log/info "scale-up:" service-id "retrieving instance count")
                  (let [current-instances (count-instances)]
                    (log/info "scale-up:" service-id "currently has" current-instances "instance(s)")
                    (= target-instances current-instances))))
              (str "Never scaled up to " target-instances " instances"))
          (log/info "cancelling parallel requests to" service-id)
          (reset! cancellation-token-atom true)
          (log/info "waiting for threads to complete making requests to" service-id)
          (await-futures futures)
          (log/info "completed making requests to" service-id))
        ; When scaling down in Marathon, we have to wait for forced kills,
        ; which by default occur after 60 seconds of failed kills.
        ; So give the scale down extra time
        (log/info "waiting for" service-id "to scale down to one instance")
        (is (wait-for
              (fn []
                (log/info "scale-down:" service-id "retrieving instance count")
                (let [current-instances (count-instances)]
                  (log/info "scale-down:" service-id "currently has" current-instances "instance(s)")
                  (= 1 current-instances)))
              :timeout 300)
            "Never scaled back down to 1 instance")))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-scaling-healthy-app
  (testing-using-waiter-url
    (let [concurrency-level 3
          custom-headers {:x-kitchen-delay-ms 5000
                          :x-waiter-concurrency-level concurrency-level
                          :x-waiter-min-instances 1
                          :x-waiter-scale-up-factor 0.9
                          :x-waiter-scale-down-factor 0.9
                          :x-waiter-name (rand-name)}]
      (scaling-for-service-test "Scaling healthy app" waiter-url 3 concurrency-level
                                #(make-kitchen-request waiter-url custom-headers :cookies %)))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-scaling-unhealthy-app
  (testing-using-waiter-url
    (let [concurrency-level 3
          custom-headers {:x-waiter-concurrency-level concurrency-level
                          :x-waiter-scale-up-factor 0.9
                          :x-waiter-scale-down-factor 0.9
                          :x-waiter-grace-period-secs 600
                          :x-waiter-min-instances 1
                          :x-waiter-name (rand-name)
                          :x-waiter-cmd "sleep 600"
                          :x-waiter-queue-timeout 5000}]
      (scaling-for-service-test "Scaling unhealthy app" waiter-url 3 concurrency-level
                                #(make-shell-request waiter-url custom-headers :cookies %)))))

(defn- run-scale-service-requests
  [waiter-url cookies request-fn requests-per-thread delay-secs service-id
   num-threads expected-instances]
  (let [router-id->router-url (routers waiter-url)
        router-url (-> router-id->router-url vals rand-nth)
        _ (log/info "router-url:" router-url)
        cancellation-token-atom (atom false)
        futures (parallelize-requests
                  num-threads
                  requests-per-thread
                  #(request-fn router-url cookies) ;; target same router to avoid metrics synchronization issues
                  :canceled? (fn [] @cancellation-token-atom)
                  :service-id service-id
                  :verbose true
                  :wait-for-tasks false)
        timeout-secs (* delay-secs requests-per-thread)]
    (log/debug "waiting up to" timeout-secs "seconds for scale up")
    (is (wait-for #(let [num-instances-running (num-instances waiter-url service-id)]
                     (log/info {:expected-instances expected-instances :num-instances num-instances-running})
                     (= expected-instances num-instances-running))
                  :interval 5 :timeout timeout-secs)
        (str "Expected: " expected-instances ", Actual: " (num-instances waiter-url service-id)))
    (reset! cancellation-token-atom true)
    (log/info "waiting for" num-threads "threads to complete")
    (await-futures futures)))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-scale-factor
  (testing-using-waiter-url
    (let [delay-secs 5
          num-threads 10
          requests-per-thread 50
          scale-factor 0.30
          expected-instances (int (* num-threads scale-factor)) ;; all requests to the same router
          extra-headers {:x-kitchen-delay-ms (-> delay-secs t/seconds t/in-millis)
                         :x-waiter-max-instances (inc expected-instances)
                         :x-waiter-min-instances 1
                         :x-waiter-name (rand-name)
                         :x-waiter-scale-down-factor 0.001
                         :x-waiter-scale-factor scale-factor
                         :x-waiter-scale-up-factor 0.99}
          request-fn (fn [target-url cookies]
                       (make-kitchen-request target-url extra-headers :cookies cookies))
          _ (log/info "Making canary request...")
          {:keys [cookies] :as first-request} (request-fn waiter-url {})
          service-id (retrieve-service-id waiter-url (:request-headers first-request))]
      (log/info "service-id:" service-id)
      (with-service-cleanup
        service-id
        (run-scale-service-requests
          waiter-url cookies request-fn requests-per-thread delay-secs service-id
          num-threads expected-instances)))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-concurrency-level
  (testing-using-waiter-url
    (let [delay-secs 5
          requests-per-thread 50
          concurrency-level 4
          extra-headers {:x-kitchen-delay-ms (-> delay-secs t/seconds t/in-millis)
                         :x-waiter-concurrency-level concurrency-level
                         :x-waiter-max-instances 5
                         :x-waiter-min-instances 1
                         :x-waiter-name (rand-name)
                         :x-waiter-scale-down-factor 0.99
                         :x-waiter-scale-up-factor 0.99}
          request-fn (fn [target-url cookies]
                       (make-kitchen-request target-url extra-headers :cookies cookies))
          _ (log/info "Making canary request...")
          {:keys [cookies] :as first-request} (request-fn waiter-url {})
          service-id (retrieve-service-id waiter-url (:request-headers first-request))]
      (log/info "service-id:" service-id)
      (with-service-cleanup
        service-id
        (let [num-threads 10
              expected-instances (int (Math/ceil (/ (* 1.0 num-threads) concurrency-level)))]
          (run-scale-service-requests
            waiter-url cookies request-fn requests-per-thread delay-secs service-id
            num-threads expected-instances))
        (let [num-threads 5
              expected-instances (int (Math/ceil (/ (* 1.0 num-threads) concurrency-level)))]
          (run-scale-service-requests
            waiter-url cookies request-fn requests-per-thread delay-secs service-id
            num-threads expected-instances))))))

;; (every? #(= 200 (:status %)) with at least one response having 502 Request to service backend failed
;; (> (count @response-instance-ids-atom) 1) fails with (not (> 1 1))
;; (contains? killed-instances instance-id) fails
(deftest ^:explicit ^:parallel ^:integration-slow test-expired-instance
  (testing-using-waiter-url
    (let [extra-headers {:x-waiter-instance-expiry-mins 1 ;; can't set it any lower :(
                         :x-waiter-name (rand-name)}
          {:keys [cookies instance-id service-id] :as response}
          (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %))
          extra-headers (assoc extra-headers :x-kitchen-delay-ms (-> 10 t/seconds t/in-millis))]
      (assert-response-status response http-200-ok)
      (log/info "service-id:" service-id)
      (with-service-cleanup
        service-id
        (let [response-instance-ids-atom (atom #{})
              cancellation-token-atom (atom false)
              responses (parallelize-requests
                          1
                          30
                          (fn []
                            (let [{:keys [instance-id] :as response}
                                  (make-request-with-debug-info
                                    extra-headers #(make-kitchen-request waiter-url % :cookies cookies))]
                              (swap! response-instance-ids-atom conj instance-id)
                              (when (and (not @cancellation-token-atom)
                                         (> (count @response-instance-ids-atom) 1))
                                (reset! cancellation-token-atom true))
                              response))
                          :canceled? (fn [] @cancellation-token-atom)
                          :service-id service-id
                          :verbose true)]
          (log/info "made" (count responses) "requests")
          (is (every? #(= http-200-ok (:status %)) responses))
          (is (contains? @response-instance-ids-atom instance-id)
              (str {:all-instances @response-instance-ids-atom :instance-id instance-id}))
          (is (> (count @response-instance-ids-atom) 1)
              (str @response-instance-ids-atom))
          (let [service-state (service-settings waiter-url service-id)
                killed-instances (->> (get-in service-state [:instances :killed-instances])
                                      (map :id)
                                      set)]
            (log/info "killed instances:" killed-instances)
            ;; expired instance gets killed by the autoscaler
            (is (contains? killed-instances instance-id)
                (str {:instance-id instance-id :killed-instances killed-instances}))))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-minmax-instances
  (testing-using-waiter-url
    (let [min-instances 2
          max-instances 5
          requests-per-thread 20
          request-delay-ms 2000
          custom-headers {:x-kitchen-delay-ms request-delay-ms
                          :x-waiter-max-instances 5
                          :x-waiter-min-instances 2
                          :x-waiter-name (rand-name)
                          :x-waiter-scale-up-factor 0.99}
          request-fn (fn [& {:keys [cookies] :or {cookies {}}}]
                       (log/info "making kitchen request")
                       (make-request-with-debug-info
                         custom-headers
                         #(make-kitchen-request waiter-url % :cookies cookies)))
          _ (log/info "making canary request...")
          {:keys [cookies service-id]} (request-fn)
          get-target-instances
          (fn []
            (let [instances
                  (loop [routers (routers waiter-url)
                         target-instances 0]
                    (if-let [[_ router-url] (first routers)]
                      (let [{:keys [state]} (service-state router-url service-id :cookies cookies)]
                        (recur (rest routers)
                               (max target-instances
                                    (int (get-in state [:autoscaler-state :scale-to-instances] 0)))))
                      target-instances))]
              (log/debug "target instances:" instances)
              instances))]
      (with-service-cleanup
        service-id
        (log/info "waiting up to 20 seconds for autoscaler to catch up for" service-id)
        (is (wait-for #(= min-instances (get-target-instances)) :interval 4 :timeout 20))
        (log/info "starting parallel requests")
        (let [cancellation-token-atom (atom false)
              futures (parallelize-requests (* 2 max-instances)
                                            requests-per-thread
                                            #(request-fn :cookies cookies)
                                            :canceled? (fn [] @cancellation-token-atom)
                                            :service-id service-id
                                            :verbose true
                                            :wait-for-tasks false)]
          (log/info "waiting for autoscaler to reach" max-instances)
          (is (wait-for #(= max-instances (get-target-instances)) :interval 1))
          (log/info "waiting to make sure autoscaler does not go above" max-instances)
          (utils/sleep (-> requests-per-thread (* request-delay-ms) (/ 4)))
          (is (= max-instances (get-target-instances)))
          (reset! cancellation-token-atom true)
          (await-futures futures))))))
