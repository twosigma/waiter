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
(ns waiter.instance-reservation-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all])
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
    (let [extra-headers {:x-waiter-name (rand-name)
                         :x-waiter-concurrency-level 100
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
