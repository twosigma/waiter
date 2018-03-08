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
(ns waiter.cors
  (:require [clojure.core.async :as async]
            [waiter.utils :as utils])
  (:import java.util.regex.Pattern))

(defprotocol CorsValidator
  "A simple protocol for validating CORS requests.
   It provides two functions: one for preflight requests, and one for regular requests"
  (preflight-allowed? [this request]
    "Returns true if the preflight request is allowed.")
  (request-allowed? [this request]
    "Returns true if the CORS request is allowed."))

(defn preflight-request? [request]
  (= :options (:request-method request)))

(defn preflight-handler [cors-validator max-age request]
  (when-not (preflight-request? request)
    (throw (ex-info "Not a preflight request" {})))
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

(defn wrap-cors [request-handler cors-validator]
  (fn [req]
    (let [{:keys [headers request-method]} req
          {:strs [origin]} headers
          bless #(if (and origin (request-allowed? cors-validator req))
                   (update-in % [:headers] assoc
                              "Access-Control-Allow-Origin" origin
                              "Access-Control-Allow-Credentials" "true")
                   %)]
      (-> req
          (#(if (or (not origin) (request-allowed? cors-validator %))
              (request-handler %)
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
