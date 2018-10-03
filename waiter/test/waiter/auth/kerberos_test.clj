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
            [waiter.auth.authentication :as auth]
            [waiter.auth.kerberos :refer :all]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-get-opt-in-accounts
  (testing "success"
    (with-redefs [shell/sh (constantly
                             {:exit 0
                              :out "test1@N.EXAMPLE.COM\ntest2@N.EXAMPLE.COM"})]
      (is (= #{"test1" "test2"} (get-opt-in-accounts "host")))))

  (testing "failure"
    (with-redefs [shell/sh (constantly {:exit 1 :err ""})]
      (is (nil? (get-opt-in-accounts "host"))))))

(defn- reset-prestash-cache
  "Resets the prestash cache to its original state"
  []
  (with-redefs [get-opt-in-accounts (constantly #{})]
    (refresh-prestash-cache "host")))

(deftest test-is-prestashed
  (testing "returns true for uninitialized cache"
    (is (empty? (reset-prestash-cache)))
    (is (is-prestashed? "fakeuser")))

  (testing "returns true for error loading cache"
    (with-redefs [get-opt-in-accounts (constantly nil)]
      (is (nil? (refresh-prestash-cache "host")))
      (is (is-prestashed? "fakeuser")))
    (reset-prestash-cache))

  (testing "checks populated cache"
    (with-redefs [get-opt-in-accounts (constantly #{"test1"})]
      (is #{"test1"} (refresh-prestash-cache "host"))
      (is (is-prestashed? "test1"))
      (is (not (is-prestashed? "fakeuser"))))
    (reset-prestash-cache)))

(deftest test-prestash-cache-maintainer
  (testing "exit chan"
    (with-redefs [refresh-prestash-cache (fn [_] #{"user"})]
      (let [{:keys [exit-chan query-chan]} (start-prestash-cache-maintainer 10 2 "foo.example.com" (async/chan 10))
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
        (let [{:keys [query-chan exit-chan]} (start-prestash-cache-maintainer 30000 1 "foo.example.com" (async/chan 10))]
          (Thread/sleep 5)
          (async/>!! query-chan {:response-chan response-chan})
          (let [response (async/<!! response-chan)]
            (is (= response users)))
          (async/>!! exit-chan :exit))))))

(deftest test-check-has-prestashed-tickets
  (let [query-chan (async/chan 1)]
    (testing "returns error for user without tickets"
      (with-redefs [is-prestashed? (fn [_] false)]
        (async/go
          (let [{:keys [response-chan]} (async/<! query-chan)]
            (async/>! response-chan #{})))
        (try
          (utils/load-messages {:prestashed-tickets-not-available "Prestashed tickets"})
          (check-has-prestashed-tickets query-chan "kuser" "service-id")
          (is false "Expected exception to be thrown")
          (catch ExceptionInfo e
            (let [{:keys [status message]} (ex-data e)]
              (is (= 403 status))
              (is (str/includes? message "Prestashed tickets")))))))

    (testing "queries on cache miss"
      (with-redefs [is-prestashed? (fn [_] false)]
        (async/go
          (let [{:keys [response-chan]} (async/<! query-chan)]
            (async/>! response-chan #{"kuser"})))
        (is (nil? (check-has-prestashed-tickets query-chan "kuser" "service-id")))))

    (testing "returns nil on query timeout"
      (with-redefs [is-prestashed? (fn [_] false)]
        (is (nil? (check-has-prestashed-tickets (async/chan 1) "kuser" "service-id")))))

    (testing "returns nil for a user with tickets"
      (with-redefs [is-prestashed? (fn [_] true)]
        (is (nil? (check-has-prestashed-tickets query-chan nil "service-id")))))))

(deftest test-kerberos-authenticator
  (with-redefs [start-prestash-cache-maintainer (constantly nil)]
    (let [config {:password "test-password"
                  :prestash-cache-refresh-ms 100
                  :prestash-cache-min-refresh-ms 10
                  :prestash-query-host "example.com"}
          authenticator-fn (kerberos-authenticator config)]
      (is (fn? authenticator-fn)))))
