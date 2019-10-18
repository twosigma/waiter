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
(ns waiter.instance-reservation-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.state :as state]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du])
  (:import (java.util.concurrent CountDownLatch)))

(deftest ^:parallel ^:integration-slow test-instance-reservation
  (testing-using-waiter-url
    (log/info "Testing instance allocation for each request")
    (log/info "Making canary request...")
    (let [router-id->router-url (routers waiter-url)
          num-routers (count router-id->router-url)
          extra-headers {:x-waiter-concurrency-level num-routers
                         :x-waiter-name (rand-name)
                         :x-waiter-scale-up-factor 0.99}
          {:keys [cookies request-headers service-id]}
          (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %))]
      (with-service-cleanup
        service-id
        (log/info "service id is" service-id)
        (log/info "ensuring every router knows about an instance of the service")
        (is (wait-for (fn []
                        (every? (fn [[_ router-url]]
                                  (let [service-state (service-state router-url service-id :cookies cookies)]
                                    (->> (get-in service-state [:state :responder-state :instance-id->state])
                                         vals
                                         (filter (fn [instance-state]
                                                   (contains? (:status-tags instance-state) :healthy)))
                                         (every? #(pos? (:slots-assigned %))))))
                                router-id->router-url))
                      :interval 3 :timeout 30))
        (let [long-request-time-ms (t/in-millis (t/seconds 90))
              small-request-time-ms (t/in-millis (t/seconds 5))
              long-request-instance-ids (atom #{})
              small-request-instance-ids (atom #{})
              long-requests-started-latch (CountDownLatch. num-routers)
              long-requests-ended-latch (CountDownLatch. num-routers)
              small-batch-cancellation-token (atom false)
              request-fn (fn [target-url delay-ms]
                           (->> #(make-kitchen-request target-url % :cookies cookies)
                                (make-request-with-debug-info (assoc request-headers :x-kitchen-delay-ms delay-ms))
                                :instance-id))]
          (log/info "starting long requests...")
          (async/go ;; trigger cancellation if time is close to long request completing
            (-> long-request-time-ms (- 1000) async/timeout async/<!)
            (reset! small-batch-cancellation-token true))
          (doseq [[_ router-url] router-id->router-url]
            (launch-thread ;; make each router's slot busy to avoid work-stealing interference
              (.countDown long-requests-started-latch)
              (try
                (->> (request-fn router-url long-request-time-ms)
                     (swap! long-request-instance-ids conj))
                (finally
                  (.countDown long-requests-ended-latch)))))
          (.await long-requests-started-latch)
          (log/info "long requests have started.")

          ;; allow time for long requests to be processed
          (is (wait-for
                (fn []
                  (let [service-data (service-settings waiter-url service-id)
                        request-counts (get-in service-data [:metrics :aggregate :counters :request-counts])]
                    (= num-routers (get request-counts :outstanding))))
                :interval 200 :timeout 2000 :unit-multiplier 1))

          (log/info "starting small requests...")
          (parallelize-requests
            (* 2 num-routers) ;; num threads
            (/ long-request-time-ms small-request-time-ms) ;; num iterations
            (fn small-request-fn []
              (let [instance-id (request-fn waiter-url small-request-time-ms)]
                (when-not @small-batch-cancellation-token
                  (swap! small-request-instance-ids conj instance-id))))
            :canceled? (fn [] @small-batch-cancellation-token))
          (log/info "small requests have completed.")
          (when-not (seq @small-request-instance-ids)
            (log/warn "no small requests completed before long requests!"))
          (.await long-requests-ended-latch)
          (log/info "long requests have completed.")
          (is (= 1 (count @long-request-instance-ids)) (str @long-request-instance-ids))
          (is (empty? (set/intersection @small-request-instance-ids @long-request-instance-ids))
              (str {:long-request-instance-ids @long-request-instance-ids
                    :small-request-instance-ids @small-request-instance-ids})))))))

(deftest ^:parallel ^:integration-slow test-instance-reservation-for-concurrent-service
  (testing-using-waiter-url
    (log/info (str "Testing instance allocation for concurrent service for each request (will take "
                   (colored-time (str "~3 minutes"))
                   " to complete)."))
    (let [extra-headers {:x-waiter-concurrency-level 100
                         :x-waiter-min-instances 1
                         :x-waiter-name (rand-name)
                         :x-waiter-scale-up-factor 0.99}
          request-fn (fn [time & {:keys [cookies] :or {cookies {}}}]
                       (make-request-with-debug-info
                         (merge extra-headers {:x-kitchen-delay-ms time})
                         #(make-kitchen-request waiter-url % :cookies cookies)))
          _ (log/info (str "Making canary request..."))
          {:keys [cookies instance-id service-id] :as canary-response} (request-fn 100)]
      (assert-response-status canary-response 200)
      (log/info "Sending additional requests.")
      (time-it
        "additional-requests"
        (parallelize-requests
          8
          100
          #(let [response (request-fn 100 :cookies cookies)]
             ; all requests should be served by the same instance
             (is (= instance-id (:instance-id response))))))
      (let [service-state (service-state waiter-url service-id)
            responder-state (get-in service-state [:state :responder-state])]
        (is (= 1 (count (:id->instance responder-state))))
        (is (empty? (:instance-id->request-id->use-reason-map service-state)))
        (is (every? #(pos? (:slots-assigned %))
                    (filter (fn [instance-state] (contains? (:status-tags instance-state) :healthy))
                            (vals (:instance-id->state responder-state))))))
      (delete-service waiter-url service-id)
      (log/info (str "Instance reservation test for concurrent service completed.")))))

(defn run-load-balancing-test
  [waiter-url load-balancing num-threads num-iterations assertion-fn]
  (log/info "testing load balancing" load-balancing)
  (let [instance-count 2
        extra-headers {:x-waiter-concurrency-level 300
                       :x-waiter-load-balancing load-balancing
                       :x-waiter-max-instances instance-count
                       :x-waiter-min-instances instance-count
                       :x-waiter-name (rand-name)
                       :x-waiter-scale-down-factor 0.001
                       :x-waiter-scale-up-factor 0.99}
        request-fn (fn [router-url time & {:keys [cookies] :or {cookies {}}}]
                     (make-request-with-debug-info
                       (merge extra-headers {:x-kitchen-delay-ms time})
                       #(make-kitchen-request router-url % :cookies cookies)))
        _ (log/info "making canary request...")
        {:keys [cookies service-id] :as canary-response} (request-fn waiter-url 2)]
    (assert-response-status canary-response 200)
    (with-service-cleanup
      service-id
      (let [[_ router-url] (first (routers waiter-url))]
        (is (wait-for #(->> (active-instances router-url service-id :cookies cookies)
                         (filter :healthy?)
                         count
                         (= instance-count)))
            (str instance-count " healthy instances not found"))
        (log/info "sending additional requests.")
        (let [instance-id->request-count-atom (atom {})
              response-atom (atom [])]
          (parallelize-requests num-threads num-iterations
                                (fn []
                                  (let [{:keys [instance-id] :as response} (request-fn router-url 2 :cookies cookies)]
                                    (swap! response-atom conj response)
                                    (swap! instance-id->request-count-atom update instance-id (fnil inc 0)))))
          (doseq [response @response-atom]
            (assert-response-status response 200))
          (let [sorted-instance-ids (->> (active-instances router-url service-id :cookies cookies)
                                      (map (fn [instance] (update instance :started-at du/str-to-date)))
                                      (state/sort-instances-for-processing #{})
                                      (map :id))
                instance-id->request-count @instance-id->request-count-atom]
            (assertion-fn sorted-instance-ids instance-id->request-count)))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-load-balancing-random
  (testing-using-waiter-url
    (let [num-threads 4
          num-iterations 50
          total-requests (* num-threads num-iterations)]
      (run-load-balancing-test
        waiter-url "random" num-threads num-iterations
        (fn assert-load-balancing-random
          [_ instance-id->request-count]
          (let [assert-message (str instance-id->request-count)]
            (is (= (count instance-id->request-count) 2) assert-message)
            (is (every? #(>= % (* 0.2 total-requests)) (vals instance-id->request-count)) assert-message)))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-load-balancing-oldest
  (testing-using-waiter-url
    (let [num-threads 4
          num-iterations 25
          total-requests (* num-threads num-iterations)]
      (run-load-balancing-test
        waiter-url "oldest" num-threads num-iterations
        (fn assert-load-balancing-oldest
          [sorted-instance-ids instance-id->request-count]
          (let [first-instance-id (first sorted-instance-ids)
                assert-message (str instance-id->request-count)]
            (is (some-> instance-id->request-count (get first-instance-id) (= total-requests)) assert-message)))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-load-balancing-youngest
  (testing-using-waiter-url
    (let [num-threads 4
          num-iterations 25
          total-requests (* num-threads num-iterations)]
      (run-load-balancing-test
        waiter-url "youngest" num-threads num-iterations
        (fn assert-load-balancing-youngest
          [sorted-instance-ids instance-id->request-count]
          (let [last-instance-id (last sorted-instance-ids)
                assert-message (str instance-id->request-count)]
            (is (some-> instance-id->request-count (get last-instance-id) (= total-requests)) assert-message)))))))
