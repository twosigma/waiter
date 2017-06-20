;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.autoscaling-test
  (:require [clojure.set :as set]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.client-tools :refer :all]
            [waiter.utils :as utils]))

(defn- scaling-for-service-test [testing-str waiter-url threads requests-per-thread request-fn]
  (testing testing-str
    (let [expect-tasks-running (fn [service-id pred msg]
                                 (loop [i 1
                                        tasks-running 0]
                                   (if (< i (* 5 4 requests-per-thread))
                                     (let [tasks-running (num-tasks-running waiter-url service-id :prev-tasks-running tasks-running)]
                                       (when (not (pred tasks-running))
                                         (Thread/sleep 1000)
                                         (recur (inc i) (int tasks-running))))
                                     (is nil msg))))]
      (let [_ (log/info (str "Making canary request..."))
            first-request (request-fn)
            service-id (retrieve-service-id waiter-url (:request-headers first-request))]
        (expect-tasks-running service-id #(= % 1) "Never saw initial 1 task running")
        (future (dorun (pmap (fn [_] (dotimes [_ requests-per-thread] (request-fn))) (range threads))))
        (expect-tasks-running service-id #(> % (/ threads 2)) (str "Never scaled up to " (/ threads 2) " task(s)"))
        (expect-tasks-running service-id #(= % 1) "Never scaled down to 1 task")
        (delete-service waiter-url service-id)))))

; Marked explicit due to:
;   FAIL in (test-scaling-healthy-app)
;   test-scaling-healthy-app Scaling healthy app
;   Never scaled up to 5/2 task(s)
;   expected: nil
;     actual: nil
(deftest ^:parallel ^:integration-slow ^:explicit test-scaling-healthy-app
  (testing-using-waiter-url
    (log/info (str "Scaling healthy app test (should take " (colored-time "~2 minutes") ")"))
    (let [custom-headers {:x-kitchen-delay-ms 5000
                          :x-waiter-scale-up-factor 0.9
                          :x-waiter-scale-down-factor 0.9
                          :x-waiter-name (rand-name "testscaling1")}]
      (scaling-for-service-test "Scaling healthy app" waiter-url 5 10 #(make-kitchen-request waiter-url custom-headers)))))

; Marked explicit due to:
;   FAIL in (test-scaling-unhealthy-app)
;   Never scaled down to 1 task
;   expected: nil
;     actual: nil
(deftest ^:parallel ^:integration-slow ^:explicit test-scaling-unhealthy-app
  (testing-using-waiter-url
    (log/info (str "Scaling unhealthy app test (should take " (colored-time "~2 minutes") ")"))
    (let [custom-headers {:x-waiter-scale-up-factor 0.9
                          :x-waiter-scale-down-factor 0.9
                          :x-waiter-name (rand-name "testscaling2")
                          :x-waiter-cmd "sleep 600"
                          :x-waiter-queue-timeout 5000}]
      (scaling-for-service-test "Scaling unhealthy app" waiter-url 5 10 #(make-shell-request waiter-url custom-headers)))))

;; Marked explicit:
;; Expected: in range [2, 8], Actual: 1
(deftest ^:parallel ^:integration-slow ^:explicit test-scale-factor
  (testing-using-waiter-url
    (let [metrics-sync-interval-ms (get-in (waiter-settings waiter-url) [:metrics-config :metrics-sync-interval-ms])
          delay-ms (int (* 2.50 metrics-sync-interval-ms))
          requests-per-thread 60
          custom-headers {:x-kitchen-delay-ms delay-ms
                          :x-waiter-scale-factor 0.45
                          :x-waiter-scale-up-factor 0.99
                          :x-waiter-scale-down-factor 0.001
                          :x-waiter-name (rand-name "testscalefactor")}
          request-fn (fn [& {:keys [cookies] :or {cookies {}}}]
                       (make-kitchen-request waiter-url custom-headers :cookies cookies))
          _ (log/info (str "Making canary request..."))
          {:keys [cookies] :as first-request} (request-fn)
          service-id (retrieve-service-id waiter-url (:request-headers first-request))
          request-with-cookies-fn #(request-fn :cookies cookies)]
      (let [cancellation-token-atom (atom false)
            futures (parallelize-requests 10 requests-per-thread request-with-cookies-fn
                                          :canceled? (fn [] @cancellation-token-atom)
                                          :service-id service-id
                                          :verbose true
                                          :wait-for-tasks false)
            timeout-secs 15]
        (log/debug "waiting up to" timeout-secs "seconds for scale up")
        ;; Scale factor is 0.45 and we're making 10 concurrent requests,
        ;; so we should scale up, but not above 5 instances.
        ;; But because we run on multiple routers and are sampling metrics from routers at different times,
        ;; it's possible to observe that overall outstanding requests are higher than 10,
        ;; so we'll allow 6, eh 7 actually, no -- perhaps 8, instances.
        (is (wait-for #(<= 2 (num-instances waiter-url service-id) 8) :interval 1 :timeout timeout-secs)
            (str "Expected: in range [2, 8], Actual: " (num-instances waiter-url service-id)))
        (reset! cancellation-token-atom true)
        (await-futures futures))
      (delete-service waiter-url service-id))))

; Marked explicit due to:
; FAIL in (test-concurrency-level) (autoscaling_test.clj:134)
; Expected: in range [2, 3], Actual: 5
(deftest ^:parallel ^:integration-slow ^:explicit test-concurrency-level
  (testing-using-waiter-url
    (log/info (str "Concurrency-Level test (should take " (colored-time "~4 minutes") ")"))
    (let [requests-per-thread 20
          metrics-sync-interval-ms (get-in (waiter-settings waiter-url) [:metrics-config :metrics-sync-interval-ms])
          extra-headers {:x-kitchen-delay-ms (int (* 2.25 metrics-sync-interval-ms))
                         :x-waiter-name (rand-name "testconcurrencylevel")
                         :x-waiter-concurrency-level 4
                         :x-waiter-scale-up-factor 0.99
                         :x-waiter-scale-down-factor 0.75
                         :x-waiter-permitted-user "*"}
          request-fn (fn [& {:keys [cookies] :or {cookies {}}}]
                       (make-kitchen-request waiter-url extra-headers :cookies cookies))
          _ (log/info (str "Making canary request..."))
          {:keys [cookies] :as first-request} (request-fn)
          service-id (retrieve-service-id waiter-url (:request-headers first-request))
          assertion-delay-ms (+ requests-per-thread 30)
          request-with-cookies-fn #(request-fn :cookies cookies)]
      ; check that app gets scaled up
      (let [cancellation-token-atom (atom false)
            futures (parallelize-requests 14 requests-per-thread request-with-cookies-fn
                                          :canceled? (fn [] @cancellation-token-atom)
                                          :service-id service-id
                                          :verbose true
                                          :wait-for-tasks false)]
        (is (wait-for #(<= 3 (num-instances waiter-url service-id) 4)
                      :interval 1 :timeout assertion-delay-ms)
            (str "Expected: in range [3, 4], Actual: " (num-instances waiter-url service-id)))
        (reset! cancellation-token-atom true)
        (await-futures futures))
      ; check that app gets further scaled up
      (let [cancellation-token-atom (atom false)
            futures (parallelize-requests 22 requests-per-thread request-with-cookies-fn
                                          :canceled? (fn [] @cancellation-token-atom)
                                          :service-id service-id
                                          :verbose true
                                          :wait-for-tasks false)]
        (is (wait-for #(<= 5 (num-instances waiter-url service-id) 6)
                      :interval 1 :timeout assertion-delay-ms)
            (str "Expected: in range [5, 6], Actual: " (num-instances waiter-url service-id)))
        (reset! cancellation-token-atom true)
        (await-futures futures))
      ; check that app gets scaled down
      (let [cancellation-token-atom (atom false)
            futures (parallelize-requests 6 requests-per-thread request-with-cookies-fn
                                          :canceled? (fn [] @cancellation-token-atom)
                                          :service-id service-id
                                          :verbose true
                                          :wait-for-tasks false)]
        (is (wait-for #(<= 2 (num-instances waiter-url service-id) 3)
                      :interval 1 :timeout assertion-delay-ms)
            (str "Expected: in range [2, 3], Actual: " (num-instances waiter-url service-id)))
        (reset! cancellation-token-atom true)
        (await-futures futures))
      (delete-service waiter-url service-id)
      (log/info "Concurrency-Level test completed."))))

; Marked explicit due to:
;   FAIL in (test-expired-instance)
;   Did not scale up to two instances
(deftest ^:parallel ^:integration-slow ^:explicit test-expired-instance
  (testing-using-waiter-url
    (let [extra-headers {:x-waiter-cmd (kitchen-cmd "-p $PORT0 --start-up-sleep-ms 10000")
                         :x-waiter-idle-timeout-mins 60
                         :x-waiter-instance-expiry-mins 1
                         :x-waiter-name (rand-name "testexpiredinstance")}
          {:keys [service-id]} (make-request-with-debug-info extra-headers #(make-shell-request waiter-url %))
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          get-responder-states (fn []
                                 (loop [routers (routers waiter-url)
                                        state []]
                                   (if-let [[_ router-url] (first routers)]
                                     (let [router-state (router-service-state router-url service-id cookies)]
                                       (recur (rest routers) (conj state (get-in router-state ["state" "responder-state"]))))
                                     state)))
          initial-instance-state (->> (get-responder-states)
                                      (map #(get % "instance-id->state"))
                                      (remove nil?)
                                      (first))
          first-instance-id (first (keys initial-instance-state))
          restarted-expired-instance (fn [responder-states]
                                       (some (fn [router-state]
                                               (and (= 2 (count (get router-state "instance-id->state")))
                                                    (set/subset? #{"healthy" "expired"}
                                                                 (set (get-in router-state ["instance-id->state" first-instance-id "status-tags"])))
                                                    (let [instance-ids (keys (get router-state "instance-id->state"))
                                                          other-instance-id (first (remove #(= first-instance-id %) instance-ids))]
                                                      (= #{"unhealthy", "starting"} (set (get-in router-state ["instance-id->state" other-instance-id "status-tags"]))))))
                                             responder-states))]
      (is (= 1 (count initial-instance-state)))
      (is (wait-for #(restarted-expired-instance (get-responder-states)) :interval 1 :timeout 90) "Did not scale up to two instances")
      (is (wait-for #(every? (fn [router-state] (not (-> router-state
                                                         (get "id->instance")
                                                         (keys)
                                                         (set)
                                                         (contains? first-instance-id))))
                             (get-responder-states)) :interval 1 :timeout 90)
          "Failed to kill expired instance")
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-minmax-instances
  (testing-using-waiter-url
    (let [min-instances 2
          max-instances 5
          requests-per-thread 20
          request-delay-ms 2000
          custom-headers {:x-kitchen-delay-ms request-delay-ms
                          :x-waiter-min-instances 2
                          :x-waiter-max-instances 5
                          :x-waiter-name (rand-name "test-minmax-instances")
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
        (log/info "waiting to make sure autoscaler does not go above"  max-instances)
        (utils/sleep (-> requests-per-thread (* request-delay-ms) (/ 4)))
        (is (= max-instances (get-target-instances)))
        (reset! cancellation-token-atom true)
        (await-futures futures))
      (delete-service waiter-url service-id))))
