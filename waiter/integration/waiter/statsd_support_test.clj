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
(ns waiter.statsd-support-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-header-metric-group
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)
                   :x-waiter-metric-group "foo"}
          {:keys [status service-id] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          value (:metric-group (response->service-description waiter-url response))]
      (is (= 200 status))
      (is (= "foo" value))
      (delete-service waiter-url service-id))))

(defn statsd-enabled?
  [waiter-url]
  (not= "disabled" (:statsd (waiter-settings waiter-url))))

(deftest ^:parallel ^:integration-fast test-statsd-disabled
  (testing-using-waiter-url
    (if (statsd-enabled? waiter-url)
      (log/info "Skipping statsd disabled assertion because statsd is turned on by Waiter")
      (let [headers {:x-waiter-name (rand-name)}
            cookies (all-cookies waiter-url)
            make-request (fn [url]
                           (make-request-with-debug-info headers #(make-kitchen-request url % :cookies cookies)))
            {:keys [status service-id router-id] :as response} (make-request waiter-url)]
        (assert-response-status response 200)
        (when (= 200 status)
          (let [router-url (router-endpoint waiter-url router-id)
                cancellation-token (atom false)
                background-requests (future
                                      (while (not @cancellation-token)
                                        (is (= 200 (:status (make-request router-url)))))
                                      (log/debug "Done sending background requests"))]
            (try
              (let [state (router-statsd-state waiter-url router-id)]
                (log/debug "State after request:" state)
                (is (= {} state)))
              (finally
                (reset! cancellation-token true)
                @background-requests))))
        (delete-service waiter-url service-id)))))