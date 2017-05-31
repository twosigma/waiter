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
            [waiter.security :as sec]
            [waiter.utils :as utils]))

(defrecord TestEntitlementManager [entitlements]
  sec/EntitlementManager
  (authorized? [_ subject action resource]
    (entitlements [subject action (:user resource)])))

(defn test-em
  "Creates a new TestEntitlementManager"
  [_]
  (TestEntitlementManager.
    #{["foo@example.com" :run-as "waiteruser"]}))

(deftest test-create-component
  (testing "Creating a new entitlement manager"

    (testing "should support custom implementations"
      (let [em (utils/create-component {:kind :test
                                        :test {:factory-fn 'waiter.security-test/test-em}})]
        (is em)
        (is (sec/authorized? em "foo@example.com" :run-as {:user "waiteruser"}))
        (is (not (sec/authorized? em "foo@example.com" :run-as {:user "waiteruser2"})))
        (is (not (sec/authorized? em "randomguy@example.com" :run-as {:user "waiteruser"})))))

    (testing "should support :kind :simple"
      (let [em (utils/create-component {:kind :simple
                                        :simple {:factory-fn 'waiter.security/->SimpleEntitlementManager}})]
        (is em)
        (is (sec/authorized? em "foo" :run-as {:user "foo"}))
        (is (not (sec/authorized? em "foo" :run-as {:user "bar"})))))))
