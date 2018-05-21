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
  (:require [clojure.core.async :as async]
            [metrics.counters :as counters]
            [waiter.metrics :as metrics]
            [waiter.util.utils :as utils])
  (:import java.util.regex.Pattern))

(defprotocol CorsValidator
  "A simple protocol for validating CORS requests.
   It provides two functions: one for preflight requests, and one for regular requests"
  (preflight-allowed? [this request]
    "Returns true if the preflight request is allowed.")
  (request-allowed? [this request]
    "Returns true if the CORS request is allowed."))

(defn preflight-request?
  "Determines if a request is a CORS preflight request."
  [request]
  (= :options (:request-method request)))

(defn wrap-cors-preflight
  "Preflight request handling middleware.
  This middleware needs to precede any authentication middleware since CORS preflight
  requests do not support authentication."
  [handler cors-validator max-age]
  (fn wrap-cors-preflight-fn [request]
    (if (preflight-request? request)
      (do
        (counters/inc!  (metrics/waiter-counter "requests" "cors-preflight"))
        (let [{:keys [headers request-method]} request
              {:strs [origin]} headers]
          (when-not origin
            (throw (ex-info "No origin provided" {:status 403})))
          (when-not (preflight-allowed? cors-validator request)
            (throw (ex-info "Cross-origin request not allowed" {:origin origin
                                                                :request-method request-method
                                                                :status 403})))
          (let [{:strs [access-control-request-headers]} headers]
            {:status 200
             :headers {"Access-Control-Allow-Origin" origin
                       "Access-Control-Allow-Headers" access-control-request-headers
                       "Access-Control-Allow-Methods" "POST, GET, OPTIONS, DELETE"
                       "Access-Control-Allow-Credentials" "true"
                       "Access-Control-Max-Age" (str max-age)}})))
      (handler request))))

(defn wrap-cors-request
  "Middleware that handles CORS request authorization.
  This middleware needs to come after any authentication middleware as the CORS
  validator may require the authenticated principal."
  [handler cors-validator]
  (fn wrap-cors-fn [request]
    (let [{:keys [headers request-method]} request
          {:strs [origin]} headers
          bless #(if (and origin (request-allowed? cors-validator request))
                   (update-in % [:headers] assoc
                              "Access-Control-Allow-Origin" origin
                              "Access-Control-Allow-Credentials" "true")
                   %)]
      (-> request
          (#(if (or (not origin) (request-allowed? cors-validator %))
              (handler %)
              (throw (ex-info "Cross-origin request not allowed"
                              {:origin origin
                               :request-method request-method
                               :status 403}))))
          (#(if (map? %)
              (bless %)
              (async/go (bless (async/<! %)))))))))

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
            (and origin
                 (or (utils/same-origin request)
                     (some #(re-matches % origin) allowed-origins)))))]
    (->PatternBasedCorsValidator pattern-matches?)))

(defrecord AllowAllCorsValidator []
  CorsValidator
  (preflight-allowed? [_ _] true)
  (request-allowed? [_ _] true))

(defn allow-all-validator
  "Creates a CORS validator that allows all cross-origin requests."
  [_]
  (->AllowAllCorsValidator))

(defrecord DenyAllCorsValidator []
  CorsValidator
  (preflight-allowed? [_ _] false)
  (request-allowed? [_ _] false))

(defn deny-all-validator
  "Creates a CORS validator that denies all cross-origin requests."
  [_]
  (->DenyAllCorsValidator))
