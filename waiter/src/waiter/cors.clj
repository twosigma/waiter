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
  (let [{:keys [headers]} request
        {:strs [origin]} headers]
    (when-not origin
      (throw (ex-info "No origin provided" {:headers headers})))
    (when-not (preflight-allowed? cors-validator request)
      (throw (ex-info "Request not allowed" {:request request})))
    (let [{:strs [access-control-request-headers]} headers]
      {:status 200
       :headers {"Access-Control-Allow-Origin" origin
                 "Access-Control-Allow-Headers" access-control-request-headers
                 "Access-Control-Allow-Methods" "POST, GET, OPTIONS, DELETE"
                 "Access-Control-Allow-Credentials" "true"
                 "Access-Control-Max-Age" (str max-age)}})))

(defn handler [request-handler cors-validator]
  (fn [req]
    (let [{:keys [headers]} req
          {:strs [origin]} headers
          bless #(if (and origin (request-allowed? cors-validator req))
                   (update-in % [:headers] assoc
                              "Access-Control-Allow-Origin" origin
                              "Access-Control-Allow-Credentials" "true")
                   %)]
      (-> req
          (#(if (or (not origin) (request-allowed? cors-validator %))
              (request-handler %)
              {:status 403
               :body (str "Cross-origin request not allowed from " origin)}))
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
