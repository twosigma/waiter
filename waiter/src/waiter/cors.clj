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
(ns waiter.cors
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [waiter.metrics :as metrics]
            [waiter.schema :as schema]
            [waiter.status-codes :refer :all]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (java.util.regex Pattern)))

(defprotocol CorsValidator
  "A simple protocol for validating CORS requests.
   It provides two functions: one for preflight requests, and one for regular requests"
  (preflight-check [this request]
    "Returns a map describing whether the preflight request is allowed.
     The map has three keys: :allowed?, :summary and, optionally, :allowed-methods.
     :allowed? contains a boolean value on whether preflight is allowed.
     :summary is a map that contains a summary of all the checks that were performed.
     If present, :allowed-methods is a list of the allowed HTTP request methods.")
  (request-check [this request]
    "Returns a map describing whether the CORS request is allowed.
     The map has two keys: :allowed? and :summary.
     :allowed? contains a boolean value on whether the request is allowed.
     :summary is a map that contains a summary of all the checks that were performed."))

(defn preflight-request?
  "Determines if a request is a CORS preflight request.
   A CORS preflight request is a CORS request that checks to see if the CORS protocol is understood.
   It is an OPTIONS request, using three HTTP request headers:
   Access-Control-Request-Method, Access-Control-Request-Headers, and the Origin header.
   Source: https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request"
  [{:keys [headers request-method] :as request}]
  (and (= :options request-method)
       (contains? headers "origin")
       (contains? headers "access-control-request-method")
       (contains? headers "access-control-request-headers")))

(defn wrap-cors-preflight
  "Preflight request handling middleware.
  This middleware needs to precede any authentication middleware since CORS preflight
  requests do not support authentication."
  [handler cors-validator max-age discover-service-parameters-fn]
  (fn wrap-cors-preflight-fn [{:keys [headers] :as request}]
    (if (preflight-request? request)
      (let [discovered-parameters (discover-service-parameters-fn headers)]
        (counters/inc! (metrics/waiter-counter "requests" "cors-preflight"))
        (let [{:keys [headers request-method]} request
              {:strs [origin]} headers]
          (when-not origin
            (throw (ex-info "No origin provided" {:status http-403-forbidden})))
          (let [{:keys [allowed? summary allowed-methods]}
                (preflight-check cors-validator (assoc request :waiter-discovery discovered-parameters))]
            (when-not allowed?
              (log/info "cors preflight request not allowed" summary)
              (throw (ex-info "Cross-origin request not allowed" {:cors-checks summary
                                                                  :origin origin
                                                                  :request-method request-method
                                                                  :status http-403-forbidden
                                                                  :log-level :warn})))
            (log/info "cors preflight request allowed" summary)
            (let [{:strs [access-control-request-headers]} headers]
              (utils/attach-waiter-source
                {:headers {"access-control-allow-origin" origin
                           "access-control-allow-headers" access-control-request-headers
                           "access-control-allow-methods" (str/join ", " (or allowed-methods schema/http-methods))
                           "access-control-allow-credentials" "true"
                           "access-control-max-age" (str max-age)}
                 :status http-200-ok})))))
      (handler request))))

(defn wrap-cors-request
  "Middleware that handles CORS request authorization.
   This middleware needs to come after any authentication middleware as the CORS
   validator may require the authenticated principal."
  [handler cors-validator waiter-request? exposed-headers]
  (let [exposed-headers-str (when (seq exposed-headers)
                              (str/join ", " exposed-headers))]
    (fn wrap-cors-fn [request]
      (let [{:keys [headers request-method]} request
            {:strs [origin]} headers
            {:keys [allowed? summary]} (if origin
                                         (request-check cors-validator request)
                                         {:allowed? false :summary {:wrap-cors-request [:origin-absent]}})
            bless (fn [response]
                    (cond-> (update-in response [:headers] assoc
                                       "access-control-allow-origin" origin
                                       "access-control-allow-credentials" "true")
                      (and exposed-headers-str ;; exposed headers are configured
                           (not (utils/same-origin request)) ;; CORS request
                           (waiter-request? request)) ;; request made to a waiter router
                      (update :headers assoc "access-control-expose-headers" exposed-headers-str)))]
        (if (not origin)
          (handler request)
          (do
            (when-not allowed?
              (log/info "cors request not allowed" summary)
              (throw (ex-info "Cross-origin request not allowed"
                              {:cors-checks summary
                               :origin origin
                               :request-method request-method
                               :status http-403-forbidden
                               :log-level :warn})))
            (log/info "cors request allowed" summary)
            (-> (handler request)
              (ru/update-response bless))))))))

(defrecord PatternBasedCorsValidator [pattern-check]
  CorsValidator
  (preflight-check [_ request] (pattern-check request))
  (request-check [_ request] (pattern-check request)))

(defn pattern-based-validator
  "Creates two validator functions, one for preflight requests, and one for regular requests.
  This validator uses the same function for both."
  [{:keys [allowed-origins]}]
  {:pre [(vector? allowed-origins)
         (every? #(instance? Pattern %) allowed-origins)]}
  (let [pattern-check
        (fn [{:keys [headers] :as request}]
          (let [{:strs [origin]} headers]
            (cond
              (not origin)
              {:allowed? false :summary {:pattern-based-validator [:origin-absent]}}
              (utils/same-origin request)
              {:allowed? true :summary {:pattern-based-validator [:origin-present :origin-same]}}
              (some #(re-matches % origin) allowed-origins)
              {:allowed? true :summary {:pattern-based-validator [:origin-present :origin-different :pattern-matched]}}
              :else
              {:allowed? false :summary {:pattern-based-validator [:origin-present :origin-different :pattern-not-matched]}})))]
    (->PatternBasedCorsValidator pattern-check)))

(defrecord AllowAllCorsValidator []
  CorsValidator
  (preflight-check [_ _] {:allowed? true :summary {:allow-all-validator [:always-allow]}})
  (request-check [_ _] {:allowed? true :summary {:allow-all-validator [:always-allow]}}))

(defn allow-all-validator
  "Creates a CORS validator that allows all cross-origin requests."
  [_]
  (->AllowAllCorsValidator))

(defrecord DenyAllCorsValidator []
  CorsValidator
  (preflight-check [_ _] {:allowed? false :summary {:deny-all-validator [:always-deny]}})
  (request-check [_ _] {:allowed? false :summary {:deny-all-validator [:always-deny]}}))

(defn deny-all-validator
  "Creates a CORS validator that denies all cross-origin requests."
  [_]
  (->DenyAllCorsValidator))

(defrecord TokenParametersCorsValidator [token-parameter-check]
  CorsValidator
  (preflight-check [_ request] (token-parameter-check request))
  (request-check [_ request] (token-parameter-check request)))

(defn- cors-rule-matches?
  "Checks if an allowed CORS rule matches"
  [origin path method [_ {:strs [origin-regex target-path-regex methods]}]]
  (and
    (or (nil? methods) (.contains methods (str/upper-case (name method))))
    (or (nil? target-path-regex) (re-matches (re-pattern target-path-regex) path))
    (re-matches (re-pattern origin-regex) origin)))

(defn- find-matching-cors-rule
  "Takes a cross origin request. Returns the token's matched allowed CORS rule if any."
  [request]
  (when-let [cors-rules (get-in request [:waiter-discovery :token-metadata "cors-rules"])]
    (let [{:keys [headers request-method uri]} request
          {:strs [origin]} headers]
      (first (filter #(cors-rule-matches? origin uri request-method %)
                     (map-indexed vector cors-rules))))))

(defn token-parameter-based-validator
  "Factory function for TokenParametersCorsValidator. Validates using token cors-rules parameter.
   Same logic for preflight and regular requests"
  [config]
  (let [token-parameter-check
        (fn token-parameter-check [{:keys [headers] :as request}]
          (let [{:strs [origin]} headers]
            (cond
              (not origin)
              {:allowed? false :summary {:token-parameter-based-validator [:origin-absent]}}
              (utils/same-origin request)
              {:allowed? true :summary {:token-parameter-based-validator [:origin-present :origin-same]}}
              :else
              (if-let [[matching-rule-index {:strs [methods]}] (find-matching-cors-rule request)]
                {:allowed-methods methods
                 :allowed? true
                 :summary {:token-parameter-based-validator [:origin-present :origin-different :rule-matched
                                                             (keyword (str "rule-" matching-rule-index "-matched"))]}}
                {:allowed? false :summary {:token-parameter-based-validator [:origin-present :origin-different :no-rule-matched]}}))))]
    (->TokenParametersCorsValidator token-parameter-check)))