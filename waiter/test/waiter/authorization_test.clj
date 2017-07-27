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
(ns waiter.authorization-test
  (:require [clojure.test :refer :all]
            [waiter.authorization :as authz]
            [waiter.utils :as utils]))

(defrecord TestEntitlementManager [entitlements]
  authz/EntitlementManager
  (authorized? [_ subject action resource]
    (entitlements [subject action resource])))

(defn test-em
  "Creates a new TestEntitlementManager"
  [_]
  (TestEntitlementManager.
    #{["foo@example.com" :run-as {:user "waiteruser"}]}))

(deftest test-create-component
  (testing "Creating a new entitlement manager"

    (testing "should support custom implementations"
      (let [em (utils/create-component {:kind :test
                                        :test {:factory-fn 'waiter.authorization-test/test-em}})]
        (is em)
        (is (authz/authorized? em "foo@example.com" :run-as {:user "waiteruser"}))
        (is (not (authz/authorized? em "foo@example.com" :run-as {:user "waiteruser2"})))
        (is (not (authz/authorized? em "randomguy@example.com" :run-as {:user "waiteruser"})))))

    (testing "should support :kind :simple"
      (let [em (utils/create-component {:kind :simple
                                        :simple {:factory-fn 'waiter.authorization/->SimpleEntitlementManager}})]
        (is em)
        (is (authz/authorized? em "foo" :run-as {:user "foo"}))
        (is (not (authz/authorized? em "foo" :run-as {:user "bar"})))))))

(deftest test-manage-service?
  (let [test-user "test-user"
        test-service-id "service-id-1"
        test-service-description {"name" "test-desc", "run-as-user" "ru1"}
        assertion-fn (fn [[subject action resource]]
                       (and (= test-user subject)
                            (= :manage action)
                            (= {:resource-type :service
                                :user "ru1"
                                :service-id test-service-id}
                               resource)))
        entitlement-manager (TestEntitlementManager. assertion-fn)]
    (is (authz/manage-service? entitlement-manager test-user test-service-id test-service-description))))

(deftest test-manage-token?
  (let [test-user "test-user"
        test-token "token-id-1"
        test-token-metadata {"owner" "towner"}
        assertion-fn (fn [[subject action resource]]
                       (and (= test-user subject)
                            (= :manage action)
                            (= {:resource-type :token
                                :token test-token
                                :user "towner"}
                               resource)))
        entitlement-manager (TestEntitlementManager. assertion-fn)]
    (is (authz/manage-token? entitlement-manager test-user test-token test-token-metadata))))

(deftest test-run-as?
  (let [test-user-1 "test-user-1"
        test-user-2 "test-user-2"
        assertion-fn (fn [[subject action resource]]
                       (and (= test-user-1 subject)
                            (= :run-as action)
                            (= {:resource-type :credential
                                :user test-user-2}
                               resource)))
        entitlement-manager (TestEntitlementManager. assertion-fn)]
    (is (authz/run-as? entitlement-manager test-user-1 test-user-2))))
