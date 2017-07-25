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
(ns waiter.authorization)

(defprotocol EntitlementManager
  "Security related methods"
  (authorized? [this subject action resource]
    "Determines if a given subject can perform action on a given resource."))

(defrecord SimpleEntitlementManager [_]
  EntitlementManager
  (authorized? [_ subject _ resource]
    (= subject (:user resource))))

(defn- make-service-resource
  "Creates a resource from a service description for use with an entitlement manager"
  [service-id {:strs [run-as-user]}]
  {:resource-type :service
   :user run-as-user
   :service-id service-id})

(defn manage-service?
  "Returns whether the auth-user is allowed to modify the specified service description."
  [entitlement-manager auth-user service-id service-description]
  (authorized? entitlement-manager auth-user :manage (make-service-resource service-id service-description)))

(defn- make-token-resource
  "Creates a resource from a token and token metadata for use with an entitlement manager"
  [token {:strs [owner]}]
  {:resource-type :token
   :token token
   :user owner})

(defn manage-token?
  "Returns whether the auth-user is allowed to modify the specified token."
  [entitlement-manager auth-user token token-metadata]
  (authorized? entitlement-manager auth-user :manage (make-token-resource token token-metadata)))

(defn administer-token?
  "Returns whether the auth-user is allowed to administer the specified token."
  [entitlement-manager auth-user token token-metadata]
    (authorized? entitlement-manager auth-user :admin (make-token-resource token token-metadata)))

(defn run-as?
  "Helper function that checks the whether the auth-user has privileges to run as the run-as-user."
  [entitlement-manager auth-user run-as-user]
  (and auth-user run-as-user
       (authorized? entitlement-manager auth-user :run-as {:resource-type :credential, :user run-as-user})))