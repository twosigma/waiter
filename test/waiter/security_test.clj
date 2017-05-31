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
(ns waiter.security-test
  (:require [clojure.test :refer :all]
            [waiter.security :as sec]))

(defrecord TestEntitlementManager [entitlements]
  sec/EntitlementManager
  (can-run-as? [_ auth-user run-as-user]
    (entitlements [auth-user run-as-user])))

(deftest test-new-entitlement-manager
  (testing "Creating a new entitlement manager"

    (testing "should support custom implementations"
      (let [em (sec/new-entitlement-manager {:kind :custom
                                             :custom-impl (fn [_]
                                                            (TestEntitlementManager.
                                                              #{["foo@example.com" "waiteruser"]}))})]
        (is em)
        (is (sec/can-run-as? em "foo@example.com" "waiteruser"))
        (is (not (sec/can-run-as? em "foo@example.com" "waiteruser2")))
        (is (not (sec/can-run-as? em "randomguy@example.com" "waiteruser")))))

    (testing "should support :kind :simple"
      (let [em (sec/new-entitlement-manager {:kind :simple})]
        (is em)
        (is (sec/can-run-as? em "foo" "foo"))
        (is (not (sec/can-run-as? em "foo" "bar")))))))
