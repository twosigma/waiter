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
(ns waiter.auth.kerberos-test
  (:require [clojure.test :refer :all]
            [waiter.auth.authentication :refer :all]))

(deftest test-one-user-authenticator
  (let [authenticator (one-user-authenticator {})]
    (is (= :one-user (auth-type authenticator)))
    (let [request-handler (wrap-auth-handler authenticator identity)
          username (System/getProperty "user.name")
          request {}
          expected-request (assoc request :authorization/user username :authenticated-principal username)]
      (is (= expected-request
             (request-handler request)))
      (is (nil? (check-user authenticator "user" "service-id"))))))
