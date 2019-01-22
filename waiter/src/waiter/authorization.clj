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
(ns waiter.authorization
  (require [clojure.string :as string]))

(defprotocol EntitlementManager
  "Security related methods"
  (authorized? [this subject action resource]
    "Determines if a given subject can perform action on a given resource."))

(defrecord SimpleEntitlementManager [_]
  EntitlementManager
  (authorized? [_ subject action resource]
    (or (= subject (:user resource))
        (and (= :admin action)
             (= subject (System/getProperty "user.name"))))))

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

(defprotocol Authorizer
  (check-user [this ^String user ^String service-id]
    "Checks if the user is set up correctly to successfully launch
     a service using the authentication scheme. Throws an exception if not.")
  (state [this]
    "Returns the state of the authorizer"))

;; Default Authorizer implementation that never throws an exception
;; (i.e., all checks are authorized).
(defrecord NoOpAuthorizer []
  Authorizer
  (check-user [_ _ _]
    (comment "do nothing"))
  (state [_]
    {:type :no-op}))

(defn noop-authorizer [context]
  "Factory function for the default (no-op) authorizer."
  (->NoOpAuthorizer))

;; Authorizer implementation that only checks for non-blank arguments.
;; We use this implementation for integration testing.
(defrecord SanityCheckAuthorizer []
  Authorizer
  (check-user [_ user service-id]
    (assert (not (string/blank? user)))
    (assert (not (string/blank? service-id)))
    (comment "ok"))
  (state [_]
    {:type :sanity-check}))

(defn sanity-check-authorizer [context]
  "Factory function for the sanity-check authorizer."
  (->SanityCheckAuthorizer))
