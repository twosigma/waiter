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
(ns waiter.auth.authenticator-test
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.auth.authentication :refer :all]
            [waiter.cookie-support :as cs]
            [waiter.status-codes :refer :all])
  (:import (waiter.auth.authentication SingleUserAuthenticator)))

(deftest test-one-user-authenticator
  (let [username (System/getProperty "user.name")
        authenticator (one-user-authenticator {:password [:cached "some-password"]
                                               :run-as-user username})]
    (is (instance? SingleUserAuthenticator authenticator))
    (let [request-handler (wrap-auth-handler authenticator identity)
          request {}
          expected-request (assoc request
                             :authorization/method :single-user
                             :authorization/principal username
                             :authorization/user username)
          actual-result (request-handler request)]
      (is (= expected-request (dissoc actual-result :headers)))
      (is (str/includes? (get-in actual-result [:headers "set-cookie"]) "x-waiter-auth=")))))

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
        now-sec (long (/ now-ms 1000))
        expires-at (+ now-sec 4000)
        one-day-in-millis (-> 1 t/days t/in-millis)]
    (is (false? (decoded-auth-valid? ["test-principal" now-ms])))
    (is (false? (decoded-auth-valid? ["test-principal" (-> now-ms (- one-day-in-millis) (+ 1000))])))
    (is (false? (decoded-auth-valid? ["test-principal" now-ms {:jwt-access-token "a.b.c"}])))
    (is (false? (decoded-auth-valid? ["test-principal" (- now-ms one-day-in-millis 1000) {:expires-at (dec now-sec)}])))
    (is (false? (decoded-auth-valid? ["test-principal" "invalid-string-time" {:expires-at expires-at}])))
    (is (false? (decoded-auth-valid? [(rand-int 10000) "invalid-string-time" {:expires-at expires-at}])))
    (is (false? (decoded-auth-valid? [])))
    (is (false? (decoded-auth-valid? ["test-principal"])))
    (is (false? (decoded-auth-valid? ["test-principal" now-ms now-ms])))
    (is (false? (decoded-auth-valid? ["test-principal" now-ms {:expires-at expires-at :jwt-access-token "a.b.c"} now-ms])))
    ;; expires-at metadata must be present
    (is (true? (decoded-auth-valid? ["test-principal" now-ms {:expires-at expires-at}])))
    (is (true? (decoded-auth-valid? ["test-principal" (-> now-ms (- one-day-in-millis) (+ 1000)) {:expires-at expires-at}])))
    (is (true? (decoded-auth-valid? ["test-principal" now-ms {:expires-at expires-at :jwt-access-token "a.b.c"}])))))

(deftest test-auth-cookie-handler
  (let [request-handler (fn [{:keys [authorization/principal authorization/user]}]
                          {:body {:principal principal
                                  :user user}})
        password "test-password"
        auth-user "test-user"
        auth-principal (str auth-user "@test.com")
        now-ms (System/currentTimeMillis)
        now-sec (long (/ now-ms 1000))
        expires-at (+ now-sec 900000)]

    (testing "auth cookie and bearer token skips cookie auth"
      (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
        (is (= {:body {:principal nil
                       :user nil}}
               (auth-cookie-handler {:headers {"authorization" (str bearer-prefix "john.doe")
                                               "cookie" "x-waiter-auth=test-auth-cookie"}})))))

    (testing "valid auth cookie"
      (with-redefs [decode-auth-cookie (constantly [auth-principal (+ now-ms 60000) {:expires-at expires-at}])]
        (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
          (is (= {:authorization/metadata {:expires-at expires-at}
                  :authorization/method :cookie
                  :authorization/principal auth-principal
                  :authorization/user auth-user
                  :body {:principal auth-principal :user auth-user}}
                 (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=test-auth-cookie"}})))))

      (with-redefs [decode-auth-cookie (constantly [auth-principal (+ now-ms 60000)
                                                    {:expires-at expires-at
                                                     :jwt-access-token "test.access.token"}])]
        (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
          (is (= {:authorization/metadata {:expires-at expires-at
                                           :jwt-access-token "test.access.token"}
                  :authorization/method :cookie
                  :authorization/principal auth-principal
                  :authorization/user auth-user
                  :body {:principal auth-principal :user auth-user}}
                 (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=test-auth-cookie"}}))))))

    (testing "invalid auth cookie"
      (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
        (is (= {:body {:principal nil :user nil}} (auth-cookie-handler {:headers {}})))
        (is (= {:body {:principal nil :user nil}} (auth-cookie-handler {:headers {"cookie" "foo=bar"}})))
        (is (= {:body {:principal nil :user nil}} (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=foo"}}))))

      (with-redefs [decode-auth-cookie (constantly [auth-principal (+ now-ms 60000)])]
        (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
          (is (= {:body {:principal nil :user nil}}
                 (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=test-auth-cookie"}}))))))))
