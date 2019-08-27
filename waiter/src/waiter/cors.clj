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
            [metrics.counters :as counters]
            [waiter.metrics :as metrics]
            [waiter.service-description :as sd]
            [waiter.schema :as schema]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import java.util.regex.Pattern))

(defprotocol CorsValidator
  "A simple protocol for validating CORS requests.
   It provides two functions: one for preflight requests, and one for regular requests"
  (preflight-allowed? [this request]
    "Returns a map containing the result whether the preflight request is allowed.
     The map has two keys: :result and :summary.
     :result contains a boolean value on whether preflight is allowed.
     :summary is a map that contains a summary of all the checks that were performed.")
  (request-allowed? [this request]
    "Returns true if the CORS request is allowed.
     The map has two keys: :result and :summary.
     :result contains a boolean value on whether preflight is allowed.
     :summary is a map that contains a summary of all the checks that were performed."))

(defn preflight-request?
  "Determines if a request is a CORS preflight request."
  [request]
  (= :options (:request-method request)))

(defn wrap-cors-preflight
  "Preflight request handling middleware.
  This middleware needs to precede any authentication middleware since CORS preflight
  requests do not support authentication."
  [handler cors-validator max-age kv-store token-defaults waiter-hostnames]
  (fn wrap-cors-preflight-fn [{:keys [headers] :as request}]
    (if (preflight-request? request)
      (let [discovered-parameters (sd/discover-service-parameters kv-store token-defaults waiter-hostnames headers)]
        (counters/inc! (metrics/waiter-counter "requests" "cors-preflight"))
        (let [{:keys [headers request-method]} request
              {:strs [origin]} headers]
          (when-not origin
            (throw (ex-info "No origin provided" {:status 403})))
          (let [{:keys [result summary allowed-methods]} (preflight-allowed? cors-validator (assoc request :waiter-discovery discovered-parameters))]
            (when-not result
              (throw (ex-info "Cross-origin request not allowed" {:cors-checks summary
                                                                  :origin origin
                                                                  :request-method request-method
                                                                  :status 403
                                                                  :log-level :warn})))
            (let [{:strs [access-control-request-headers]} headers]
              {:status 200
               :headers {"access-control-allow-origin" origin
                         "access-control-allow-headers" access-control-request-headers
                         "access-control-allow-methods" (str/join ", " (or allowed-methods schema/http-methods))
                         "access-control-allow-credentials" "true"
                         "access-control-max-age" (str max-age)}}))))
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
            {:keys [result summary]} (if origin
                                       (request-allowed? cors-validator request)
                                       {:result false :summary [:origin-absent]})
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
            (when-not result
              (throw (ex-info "Cross-origin request not allowed"
                              {:cors-checks summary
                               :origin origin
                               :request-method request-method
                               :status 403
                               :log-level :warn})))
            (-> (handler request)
              (ru/update-response bless))))))))

(defrecord PatternBasedCorsValidator [pattern-matches?]
  CorsValidator
  (preflight-allowed? [_ request] (pattern-matches? request))
  (request-allowed? [_ request] (pattern-matches? request)))

(defn pattern-based-validator
  "Creates two validator functions, one for preflight requests, and one for regular requests.
  This validator uses the same function for both."
  [{:keys [allowed-origins]}]
  {:pre [(vector? allowed-origins)
         (every? #(instance? Pattern %) allowed-origins)]}
  (let [pattern-matches?
        (fn [{:keys [headers] :as request}]
          (let [{:strs [origin]} headers]
            (cond
              (not origin)
              {:result false :summary [:origin-absent]}
              (utils/same-origin request)
              {:result true :summary [:origin-present :origin-same]}
              (some #(re-matches % origin) allowed-origins)
              {:result true :summary [:origin-present :origin-different :pattern-matched]}
              :else
              {:result false :summary [:origin-present :origin-different :pattern-not-matched]})))]
    (->PatternBasedCorsValidator pattern-matches?)))

(defrecord AllowAllCorsValidator []
  CorsValidator
  (preflight-allowed? [_ _] {:result true :summary [:always-allow]})
  (request-allowed? [_ _] {:result true :summary [:always-allow]}))

(defn allow-all-validator
  "Creates a CORS validator that allows all cross-origin requests."
  [_]
  (->AllowAllCorsValidator))

(defrecord DenyAllCorsValidator []
  CorsValidator
  (preflight-allowed? [_ _] {:result false :summary [:always-deny]})
  (request-allowed? [_ _] {:result false :summary [:always-deny]}))

(defn deny-all-validator
  "Creates a CORS validator that denies all cross-origin requests."
  [_]
  (->DenyAllCorsValidator))

(defrecord TokenParametersCorsValidator [allow-cors?]
  CorsValidator
  (preflight-allowed? [_ request] (allow-cors? request))
  (request-allowed? [_ request] (allow-cors? request)))

(defn- cors-rule-matches?
  "Checks if an allowed CORS rule matches"
  [origin path method [_ {:strs [origin-regex target-path-regex methods]}]]
  (cond
    (and methods (not (.contains methods (str/upper-case (name method))))) false
    (and target-path-regex (not (re-matches (re-pattern target-path-regex) path))) false
    :else (re-matches (re-pattern origin-regex) origin)))

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
  (let [allow-cors?
        (fn allow-cors? [{:keys [headers] :as request}]
          (let [{:strs [origin]} headers]
            (cond
              (not origin)
              {:result false :summary [:origin-absent]}
              (utils/same-origin request)
              {:result true :summary [:origin-present :origin-same]}
              :else
              (if-let [[matching-rule-index {:strs [methods]}] (find-matching-cors-rule request)]
                {:allowed-methods methods
                 :result true
                 :summary [:origin-present :origin-different :rule-matched
                           (keyword (str "rule-" matching-rule-index "-matched"))]}
                {:result false :summary [:origin-present :origin-different :no-rule-matched]}))))]
    (->TokenParametersCorsValidator allow-cors?)))