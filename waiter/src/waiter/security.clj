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
(ns waiter.security
  (:require [clj-time.core :as t]
            [clojure.core.cache :as cache]
            [waiter.utils :as utils]))

(defprotocol EntitlementManager 
  "Security related methods"
  (authorized? [this subject action resource]
               "Determines if a given subject can perform action on a given resource."))

(defrecord SimpleEntitlementManager [_]
  EntitlementManager
  (authorized? [_ subject _ resource]
    (= subject (:user resource))))

(defn make-service-resource
  "Creates a resource from a service description for use with an entitlement manager"
  [service-id {:strs [run-as-user]}]
  {:resource-type :service
   :user run-as-user
   :service-id service-id})
