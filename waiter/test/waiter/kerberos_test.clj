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
(ns waiter.kerberos-test
  (:require [clojure.core.async :as async]
            [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [waiter.kerberos :refer :all]))

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
