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
(ns waiter.authorization-test
  (:require [clojure.test :refer :all]
            [waiter.authorization :as authz]
            [waiter.util.utils :as utils]))

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
                                        :simple {:factory-fn 'waiter.authorization/->SimpleEntitlementManager}})
            process-owner (System/getProperty "user.name")]
        (is em)
        (is (authz/authorized? em process-owner :admin {:user "test:foo"}))
        (is (not (authz/authorized? em process-owner :run-as {:user "test:foo"})))
        (is (authz/authorized? em "test:foo" :run-as {:user "test:foo"}))
        (is (not (authz/authorized? em "test:foo" :run-as {:user "test:bar"})))))))

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

(deftest test-member-of?
  (let [test-user "test-user-1"
        test-group "test-group-2"
        assertion-fn (fn [[subject action resource]]
                       (and (= test-user subject)
                            (= :member-of action)
                            (= {:resource-type :group
                                :user test-group}
                               resource)))
        entitlement-manager (TestEntitlementManager. assertion-fn)]
    (is (authz/member-of? entitlement-manager test-user test-group))))
