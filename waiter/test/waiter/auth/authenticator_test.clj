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
(ns waiter.auth.kerberos-test
  (:require [clojure.test :refer :all]
            [waiter.auth.authentication :refer :all]))

(deftest test-single-user-authenticator
  (let [authenticator (single-user-authenticator {})]
    (is (= :anonymous (auth-type authenticator)))
    (let [request-handler (create-auth-handler authenticator identity)
          username (System/getProperty "user.name")
          request {}
          expected-request (assoc request :authorization/user username :authenticated-principal username)]
      (is (= expected-request
             (request-handler request)))
      (is (nil? (check-user authenticator "user" "service-id"))))))
