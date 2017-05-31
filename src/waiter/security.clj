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
(ns waiter.security
  (:require [clj-time.core :as t]
            [clojure.core.cache :as cache]
            [waiter.utils :as utils]))

(defprotocol EntitlementManager 
  "Security related methods"
  (can-run-as? [this auth-user run-as-user]
               "Determines if a given auth-user can run as a given run-as-user."))

(defrecord CachingEntitlementManager [inner-em cache]
  EntitlementManager
  (can-run-as? [_ auth-user run-as-user]
    (utils/atom-cache-get-or-load cache [auth-user run-as-user] 
                                  (fn [] (can-run-as? inner-em auth-user run-as-user)))))

(defn new-cached-entitlement-manager [{:keys [threshold ttl]} inner-em]
  (CachingEntitlementManager. inner-em
                              (-> {}
                                  (cache/fifo-cache-factory :threshold threshold)
                                  (cache/ttl-cache-factory :ttl (-> ttl t/seconds t/in-millis))
                                  atom)))

(defrecord SimpleEntitlementManager []
  EntitlementManager
  (can-run-as? [_ auth-user run-as-user]
    (= auth-user run-as-user)))

(defn new-entitlement-manager [{:keys [kind cache] :as config}]
  (let [em (case kind 
             :custom (utils/evaluate-config-fn config)
             :simple (SimpleEntitlementManager.))]
    (if cache (new-cached-entitlement-manager cache em) em)))

