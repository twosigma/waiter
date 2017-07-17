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
(ns waiter.auth.authenticator-test
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.auth.authentication :refer :all]
            [waiter.cookie-support :as cs]))

(deftest test-one-user-authenticator
  (let [username (System/getProperty "user.name")
        authenticator (one-user-authenticator {:run-as-user username})]
    (is (= :one-user (auth-type authenticator)))
    (let [request-handler (wrap-auth-handler authenticator identity)
          request {}
          expected-request (assoc request
                             :authorization/user username
                             :authenticated-principal username)
          actual-result (request-handler request)]
      (is (= expected-request (dissoc actual-result :headers)))
      (is (str/includes? (get-in actual-result [:headers "set-cookie"]) "x-waiter-auth="))
      (is (nil? (check-user authenticator "user" "service-id"))))))

(deftest test-get-auth-cookie-value
  (is (= "abc123" (get-auth-cookie-value "x-waiter-auth=abc123")))
  (is (= "abc123" (get-auth-cookie-value "x-waiter-auth=\"abc123\"")))
  (is (= "abc123" (get-auth-cookie-value "blah=blah;x-waiter-auth=abc123"))))

(deftest test-decode-auth-cookie
  (let [password [:cached "test-password"]
        a-sequence-value ["cookie-value" 1234]
        an-int-value 123456]
    (is (= a-sequence-value (decode-auth-cookie (cs/encode-cookie a-sequence-value password) password)))
    (is (nil? (decode-auth-cookie (cs/encode-cookie an-int-value password) password)))
    (is (nil? (decode-auth-cookie a-sequence-value password)))
    (is (nil? (decode-auth-cookie an-int-value password)))))

(deftest test-decoded-auth-valid?
  (let [now-ms (System/currentTimeMillis)
        one-day-in-millis (-> 1 t/days t/in-millis)]
    (is (true? (decoded-auth-valid? ["test-principal" now-ms])))
    (is (true? (decoded-auth-valid? ["test-principal" (-> now-ms (- one-day-in-millis) (+ 1000))])))
    (is (false? (decoded-auth-valid? ["test-principal" (- now-ms one-day-in-millis 1000)])))
    (is (false? (decoded-auth-valid? ["test-principal" "invalid-string-time"])))
    (is (false? (decoded-auth-valid? [(rand-int 10000) "invalid-string-time"])))
    (is (false? (decoded-auth-valid? ["test-principal"])))
    (is (false? (decoded-auth-valid? [])))))
