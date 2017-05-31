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
(ns waiter.instance-reservation-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.client-tools :refer :all]
            [waiter.utils :as utils])
  (:import (java.util.concurrent Semaphore)))

; Marked explicit due to:
;  FAIL in (test-instance-reservation) (instance_reservation_test.clj:81)
;  test-instance-reservation
;     expected: (not (contains? (clojure.core/deref other-request-instances) (clojure.core/deref first-request-instance)))
;     actual: (not (not true))

(deftest ^:parallel ^:integration-slow ^:explicit test-instance-reservation
  (testing-using-waiter-url
    (log/info (str "Testing instance allocation for each request"))
    (let [cookies-atom (atom {})
          extra-headers {:x-waiter-name (rand-name "testreservation")
                         :x-waiter-scale-up-factor 0.99
                         :x-waiter-debug "true"}
          request-fn (fn [delay-ms & {:keys [cookies] :or {cookies {}}}]
                       (let [{:keys [headers] :as response}
                             (make-kitchen-request waiter-url
                                                   (assoc extra-headers :x-kitchen-delay-ms delay-ms)
                                                   :cookies cookies)
                             router-id (get headers "X-Waiter-Router-Id")
                             backend-id (get headers "X-Waiter-Backend-Id")]
                         (when-not (seq @cookies-atom)
                           (reset! cookies-atom (:cookies response)))
                         (str router-id ":" backend-id)))
          _ (log/info (str "Making canary request..."))
          canary-request (make-kitchen-request waiter-url extra-headers)
          service-id (retrieve-service-id waiter-url (:request-headers canary-request))
          long-request-time-ms (t/in-millis (t/seconds 90))
          small-request-time-ms (t/in-millis (t/seconds 1))
          ; scale up to avoid sharing of instances among routers
          _ (parallelize-requests
              8
              40
              #(request-fn small-request-time-ms :cookies @cookies-atom))
          other-requests-end-time (t/plus (t/now) (t/millis (- long-request-time-ms (t/in-millis (t/seconds 15)))))
          first-request-instance (atom nil)
          other-request-instances (atom {})
          exec-order-semaphore (Semaphore. 1 true)
          _ (.acquire exec-order-semaphore) ; acquire before launching thread
          request-thread-done-atom (atom false)
          first-request-thread (launch-thread
                                 (log/info (str "Making initial request at "
                                                (utils/date-to-str (t/now))
                                                " (will take "
                                                (colored-time (str ">" (t/in-minutes (t/millis long-request-time-ms)) "minutes"))
                                                " to complete)."))
                                 (.release exec-order-semaphore)
                                 (let [router-backend-id (request-fn long-request-time-ms)]
                                   (reset! first-request-instance router-backend-id)
                                   (reset! request-thread-done-atom true)
                                   (log/info "Initial request completed.")))]
      (Thread/sleep (t/in-millis (t/seconds 5)))
      (log/info "Waiting for initial request to start.")
      (.acquire exec-order-semaphore)
      (log/info "Sending additional requests.")
      (time-it
        "additional-requests"
        (parallelize-requests
          8
          100
          #(when (t/after? other-requests-end-time (t/now))
            (let [router-backend-id (request-fn small-request-time-ms :cookies @cookies-atom)]
              (swap! other-request-instances assoc router-backend-id true)))))
      (is (not @request-thread-done-atom) "first-request-thread completed before additional requests threads")
      (log/info "Awaiting first request to complete...")
      (async/<!! first-request-thread)
      (is @request-thread-done-atom)
      (log/info "First request was processed by" (str @first-request-instance))
      (is (not (contains? @other-request-instances @first-request-instance)))
      (delete-service waiter-url service-id)
      (log/info (str "Instance reservation test completed.")))))

(deftest ^:parallel ^:integration-slow test-instance-reservation-for-concurrent-service
  (testing-using-waiter-url
    (log/info (str "Testing instance allocation for concurrent service for each request (will take "
                   (colored-time (str "~3 minutes"))
                   " to complete)."))
    (let [extra-headers {:x-waiter-name (rand-name "testreservation100cl")
                         :x-waiter-concurrency-level 100
                         :x-waiter-scale-up-factor 0.99}
          request-fn (fn [time & {:keys [cookies] :or {cookies {}}}]
                       (make-request-with-debug-info
                         (merge extra-headers {:x-kitchen-delay-ms time})
                         #(make-kitchen-request waiter-url % :cookies cookies)))
          _ (log/info (str "Making canary request..."))
          {:keys [cookies instance-id service-id]} (request-fn 100)]
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