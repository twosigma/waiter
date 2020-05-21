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
(ns jwks-server.handler
  (:require [buddy.core.keys :as buddy-keys]
            [buddy.sign.jwt :as jwt]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [jwks-server.config :as config]
            [ring.middleware.params :as ring-params]
            [ring.util.request :as ring-request])
  (:import (java.security MessageDigest)
           (java.util Base64 UUID)))

(defn prepare-response
  "Prepares and returns a standard response"
  ([{:keys [query-string request-method uri]} message status]
   (log/info message)
   (prepare-response (cond-> {"message" message
                              "request-method" request-method
                              "uri" uri}
                       (not (str/blank? query-string))
                       (assoc "query-string" query-string))
                     status))
  ([data-map status]
   {:body (json/write-str data-map)
    :headers {"content-type" "application/json"
              "server" "jwks-server"}
    :status status}))

(defn method-not-allowed-response
  "Prepares and returns a standard 405 method not allowed response"
  [request]
  (prepare-response request "method not allowed" 405))

(defn- request->query-params
  "Like Ring's params-request, but doesn't try to pull params from the body."
  [request]
  (->> (or (ring-request/character-encoding request) "UTF-8")
    (ring-params/assoc-query-params request)
    :query-params))

(defn- request->form-params
  "Like Ring's params-request, but doesn't try to pull params from the body."
  [request]
  (->> (or (ring-request/character-encoding request) "UTF-8")
    (ring-params/assoc-form-params request)
    :form-params))

(defn generate-jwt-access-token
  "Generates the JWT access token using the provided private key and algorithm."
  [jwk-entry algorithm payload header]
  (let [private-key (-> jwk-entry walk/keywordize-keys buddy-keys/jwk->private-key)
        options {:alg algorithm :header header}]
    (jwt/sign payload private-key options)))

(defn- generate-access-token
  [realm algorithm]
  (let [{:keys [keys]} (config/retrieve-jwks)
        available-keys (filter (fn [{:keys [alg crv]}]
                                 (cond
                                   (= :eddsa algorithm) (= "Ed25519" crv)
                                   (= :rs256 algorithm) (= "RS256" alg)))
                               keys)
        {:keys [kid] :as entry} (rand-nth available-keys)
        _ (log/info "selected kid:" kid)
        principal (or (System/getenv "JWT_SUBJECT")
                      (System/getProperty "user.name"))
        _ (log/info "principal:" principal)
        {:keys [issuer subject-key token-type]} (config/retrieve-settings)
        subject-key (keyword subject-key)
        expiry-time-secs (+ (long (/ (System/currentTimeMillis) 1000)) 600)
        payload (cond-> {:aud (str realm) :exp expiry-time-secs :iss issuer :sub principal}
                  (not= :sub subject-key) (assoc subject-key principal))
        header {:kid kid :typ token-type}
        access-token (generate-jwt-access-token entry algorithm payload header)]
    {:access-token access-token
     :issuer issuer
     :kid kid
     :principal principal
     :realm realm
     :subject-key subject-key
     :token-type token-type}))

(defn b64-encode-sha256
  "Returns the url encoding of the input string using SHA256."
  [clear-text]
  (let [clear-text-bytes (.getBytes clear-text "US-ASCII")
        message-digest (MessageDigest/getInstance "SHA-256")
        sha256-bytes (.digest message-digest clear-text-bytes)
        b64-encoder (.withoutPadding (Base64/getUrlEncoder))
        result-bytes (.encode b64-encoder sha256-bytes)]
    (String. result-bytes)))

(let [oidc-token-state (atom {})]
  (defn process-access-codes-request
    [_]
    (prepare-response
      {"access-code->metadata" @oidc-token-state}
      200))

  (defn process-authorize-request
    [request]
    (let [{:strs [client_id code_challenge code_challenge_method nonce redirect_uri
                  response_type scope state]} (request->query-params request)]
      (cond
        (not (<= 43 (count code_challenge) 44))
        (prepare-response request (str "Invalid code challenge: " code_challenge) 400)
        (not= "S256" code_challenge_method)
        (prepare-response request (str "Invalid code challenge method: " code_challenge_method) 400)
        (not= "code" response_type)
        (prepare-response request (str "Invalid response type: " response_type) 400)
        (not= "openid" scope)
        (prepare-response request (str "Invalid scope: " scope) 400)
        :else
        (let [access-code (str (UUID/randomUUID))]
          (swap! oidc-token-state
                 assoc access-code {:client-id client_id
                                    :code-challenge code_challenge
                                    :nonce nonce
                                    :redirect-uri redirect_uri
                                    :state state})
          (log/info "inserted access code entry for" access-code)
          {:headers {"location" (str redirect_uri "?code=" access-code "&state=" state)
                     "server" "jwks-server"}
           :status 301}))))

  (defn process-id-token-request
    [request]
    (let [{:strs [client_id code code_verifier grant_type redirect_uri]} (request->form-params request)
          access-code->metadata @oidc-token-state]
      (cond
        (not (contains? access-code->metadata code))
        (prepare-response
          {"error" "invalid_request"
           "error_description" (str "Invalid authorization code: " code)}
          400)
        (str/blank? client_id)
        (prepare-response
          {"error" "invalid_request"
           "error_description" "Client ID is required"}
          400)
        (str/blank? code_verifier)
        (prepare-response
          {"error" "invalid_request"
           "error_description" "Code verifier is required"}
          400)
        (not= "authorization_code" grant_type)
        (prepare-response
          {"error" "unsupported_grant_type"
           "error_description" (str "Invalid grant type: " grant_type)}
          400)
        (str/blank? redirect_uri)
        (prepare-response
          {"error" "invalid_request"
           "error_description" "Redirection URI is required"}
          400)
        :else
        (let [metadata (get access-code->metadata code)
              code-challenge (b64-encode-sha256 code_verifier)]
          (cond
            (not= client_id (:client-id metadata))
            (prepare-response
              {"error" "invalid_client"
               "error_description" (str "Invalid Client ID: " client_id)}
              400)
            (not= redirect_uri (:redirect-uri metadata))
            (prepare-response
              {"error" "invalid_grant"
               "error_description" (str "Invalid redirection uri: " redirect_uri)}
              400)
            (not= code-challenge (:code-challenge metadata))
            (prepare-response
              {"error" "invalid_grant"
               "error_description" (str "Invalid code challenge: " code-challenge)}
              400)
            :else
            (let [{:keys [access-token issuer kid principal realm subject-key]}
                  (generate-access-token client_id :rs256)]
              (swap! oidc-token-state dissoc code)
              (log/info "removed access code entry for" code)
              (prepare-response
                {"id_token" access-token
                 "kid" kid
                 "issuer" issuer
                 "issued_token_type" "urn:ietf:params:oauth:token-type:access_token"
                 "principal" principal
                 "realm" realm
                 "subject-key" subject-key
                 "token_type" "Bearer"}
                200))))))))

(defn process-get-token-request
  "Retrieves an JWT access token generated using a random EdDSA key."
  [{:keys [query-string] :as request}]
  (log/info "query string:" query-string)
  (let [{:strs [host]} (request->query-params request)
        _ (when (str/blank? host)
            (throw (ex-info "host query parameter not provided" {:status 400})))
        {:keys [access-token issuer kid principal realm subject-key token-type]}
        (generate-access-token host :eddsa)]
    (prepare-response
      {"access_token" access-token
       "kid" kid
       "issuer" issuer
       "principal" principal
       "realm" realm
       "subject-key" subject-key
       "token-type" token-type}
      200)))

(defn process-get-keys-request
  "Returns the JWKS managed bu this server."
  [_]
  (prepare-response
    (update (config/retrieve-jwks)
            :keys
            (fn [keys]
              (map (fn [entry] (dissoc entry :d)) keys)))
    200))

(defn request-handler
  "Factory for the ring request handler."
  [{:keys [headers request-method uri] :as request}]
  (log/info "received" request-method "request as path" uri)
  (log/info "request headers:" headers)
  (let [{:keys [headers status] :as response}
        (try
          (cond
            (= uri "/access-codes")
            (if (= request-method :get)
              (process-access-codes-request request)
              (method-not-allowed-response request))
            (= uri "/authorize")
            (if (= request-method :get)
              (process-authorize-request request)
              (method-not-allowed-response request))
            (= uri "/get-token")
            (if (= request-method :get)
              (process-get-token-request request)
              (method-not-allowed-response request))
            (= uri "/id-token")
            (if (= request-method :post)
              (process-id-token-request request)
              (method-not-allowed-response request))
            (= uri "/keys")
            (if (= request-method :get)
              (process-get-keys-request request)
              (method-not-allowed-response request))
            :else
            (prepare-response request "unsupported endpoint" 404))
          (catch Throwable th
            (log/error th "error in processing request")
            (prepare-response request (.getMessage th) (:status (ex-data th) 500))))]
    (log/info "response status:" status)
    (log/info "response headers:" headers)
    response))
