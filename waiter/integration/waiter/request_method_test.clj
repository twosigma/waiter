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
(ns waiter.request-method-test
  (:require [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-request-method
  (testing-using-waiter-url
    (let [service-name (rand-name)
          headers {:x-waiter-name service-name, :x-kitchen-echo "true"}
          canary-response (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          lorem-ipsum "Lorem ipsum dolor sit amet, consectetur adipiscing elit."]
      (is (= lorem-ipsum (:body (make-kitchen-request waiter-url headers :body lorem-ipsum :method :get))))
      (is (= lorem-ipsum (:body (make-kitchen-request waiter-url headers :body lorem-ipsum :method :post))))
      (is (= lorem-ipsum (:body (make-kitchen-request waiter-url headers :body lorem-ipsum :method :put))))
      (delete-service waiter-url (:service-id canary-response)))))
