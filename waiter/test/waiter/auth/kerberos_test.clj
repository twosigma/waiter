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
(ns waiter.auth.kerberos-test
  (:require [clojure.core.async :as async]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.auth.kerberos :refer :all]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (waiter.auth.kerberos KerberosAuthenticator)))

(deftest test-get-opt-in-accounts
  (testing "success"
    (with-redefs [shell/sh (constantly
                             {:exit 0
                              :out "test1@N.EXAMPLE.COM\ntest2@N.EXAMPLE.COM"})]
      (is (= #{"test1" "test2"} (get-opt-in-accounts "host")))))

  (testing "failure"
    (with-redefs [shell/sh (constantly {:exit 1 :err ""})]
      (is (nil? (get-opt-in-accounts "host"))))))

(deftest test-is-prestashed
  (let [cache (atom nil)]
    (testing "returns true for uninitialized cache"
      (is (is-prestashed? cache "fakeuser")))

    (testing "returns true for error loading cache"
      (with-redefs [get-opt-in-accounts (constantly nil)]
        (is (nil? (refresh-prestash-cache cache "host")))
        (is (is-prestashed? cache "fakeuser")))
      (reset! cache nil))

    (testing "checks populated cache"
      (with-redefs [get-opt-in-accounts (constantly #{"test1"})]
        (is #{"test1"} (refresh-prestash-cache cache "host"))
        (is (is-prestashed? cache "test1"))
        (is (not (is-prestashed? cache "fakeuser"))))
      (reset! cache nil))))

(deftest test-prestash-cache-maintainer
  (let [cache (atom nil)]
    (testing "exit chan"
      (with-redefs [refresh-prestash-cache (constantly #{"user"})]
        (let [{:keys [exit-chan query-chan]} (start-prestash-cache-maintainer cache 10 2 "foo.example.com" (async/chan 10))
              response-chan (async/promise-chan)]
          (async/>!! exit-chan :exit)
          (async/>!! query-chan {:response-chan response-chan})
          (let [timeout (async/timeout 1000)
                [_ chan] (async/alts!! [response-chan timeout])]
            ; Query should timeout since loop stopped
            (is (= timeout chan))))))

    (testing "query chan updates cache"
      (let [users #{"test1" "test2"}
            response-chan (async/promise-chan)]
        (with-redefs [get-opt-in-accounts (fn [_] users)]
          (let [{:keys [query-chan exit-chan]} (start-prestash-cache-maintainer cache 30000 1 "foo.example.com" (async/chan 10))]
            (Thread/sleep 5)
            (async/>!! query-chan {:response-chan response-chan})
            (let [response (async/<!! response-chan)]
              (is (= response users)))
            (async/>!! exit-chan :exit)))))))

(deftest test-check-has-prestashed-tickets
  (let [cache (atom nil)
        query-chan (async/chan 1)]
    (testing "returns error for user without tickets"
      (with-redefs [is-prestashed? (constantly false)]
        (async/go
          (let [{:keys [response-chan]} (async/<! query-chan)]
            (async/>! response-chan #{})))
        (try
          (utils/load-messages {:prestashed-tickets-not-available "Prestashed tickets"})
          (check-has-prestashed-tickets cache query-chan "kuser" "service-id")
          (is false "Expected exception to be thrown")
          (catch ExceptionInfo e
            (let [{:keys [status message]} (ex-data e)]
              (is (= 403 status))
              (is (str/includes? message "Prestashed tickets")))))))

    (testing "queries on cache miss"
      (with-redefs [is-prestashed? (constantly false)]
        (async/go
          (let [{:keys [response-chan]} (async/<! query-chan)]
            (async/>! response-chan #{"kuser"})))
        (is (nil? (check-has-prestashed-tickets cache query-chan "kuser" "service-id")))))

    (testing "returns nil on query timeout"
      (with-redefs [is-prestashed? (constantly false)]
        (is (nil? (check-has-prestashed-tickets cache (async/chan 1) "kuser" "service-id")))))

    (testing "returns nil for a user with tickets"
      (with-redefs [is-prestashed? (constantly true)]
        (is (nil? (check-has-prestashed-tickets cache query-chan nil "service-id")))))))

(deftest test-kerberos-authenticator
  (with-redefs [start-prestash-cache-maintainer (constantly nil)]
    (let [config {:password "test-password"
                  :prestash-cache-refresh-ms 100
                  :prestash-cache-min-refresh-ms 10
                  :prestash-query-host "example.com"}
          authenticator-fn (kerberos-authenticator config)]
      (is (instance? KerberosAuthenticator authenticator-fn)))))
