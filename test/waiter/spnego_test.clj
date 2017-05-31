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
(ns waiter.spnego-test
  (:require [clojure.test :refer :all]
            [waiter.spnego :refer :all]))

(deftest test-url-decode
  (is (= "testtest" (url-decode "testtest")))
  (is (= "test test" (url-decode "test%20test")))
  (is (= nil (url-decode nil))))

(deftest test-get-auth-cookie-value
  (is (= "abc123" (get-auth-cookie-value "x-waiter-auth=abc123")))
  (is (= "abc123" (get-auth-cookie-value "blah=blah;x-waiter-auth=abc123"))))