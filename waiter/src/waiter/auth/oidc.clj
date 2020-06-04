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
(ns waiter.auth.oidc
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.auth.authentication :as auth]
            [waiter.auth.jwt :as jwt]
            [waiter.cookie-support :as cookie-support]
            [waiter.status-codes :refer :all]
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (java.net URI)))

(def ^:const accept-redirect-header-name "accept-redirect")

(def ^:const accept-redirect-auth-header-name "accept-redirect-auth")

(def ^:const challenge-cookie-duration-secs 60)

(def ^:const code-verifier-length 128)

(def ^:const content-security-policy-value "default-src 'none'; frame-ancestors 'none'")

(def ^:const oidc-challenge-cookie "x-waiter-oidc-challenge")

(def ^:const oidc-callback-uri "/oidc/v1/callback")

(defn create-code-verifier
  "Creates a randomly generated string containing characters from A-Z of length code-verifier-length."
  []
  (apply str (take code-verifier-length (repeatedly #(char (+ (rand 26) 65))))))

(defn create-state-code
  "Creates an encoded string of the input state map."
  [password state-map]
  (utils/map->base-36-string state-map password))

(defn parse-state-code
  "Parses the encoded string into the state map."
  [password state-str]
  (utils/base-36-string->map state-str password))

(defn validate-oidc-callback-request
  [password {:keys [headers] :as request}]
  (let [{:strs [code state]} (-> request ru/query-params-request :query-params)
        challenge-cookie (some-> headers (get "cookie") str (cookie-support/cookie-value oidc-challenge-cookie))
        bad-request-map {:log-level :info
                         :status http-400-bad-request}]
    (when (str/blank? code)
      (throw (ex-info "Query parameter code is missing" bad-request-map)))
    (when (str/blank? state)
      (throw (ex-info "Query parameter state is missing" bad-request-map)))
    (when (str/blank? challenge-cookie)
      (throw (ex-info "No challenge cookie set" bad-request-map)))
    (let [state-map (try
                      (parse-state-code password state)
                      (catch Throwable throwable
                        (throw (ex-info (str "Unable to parse state: " state)
                                        bad-request-map throwable))))]
      (when-not (and (map? state-map) (string? (get state-map :redirect-uri)))
        (throw (ex-info "The state query parameter is invalid" bad-request-map)))
      (let [code-verifier (cookie-support/decode-cookie challenge-cookie password)]
        {:code code
         :code-verifier code-verifier
         :state-map state-map}))))

(defn oidc-callback-request-handler
  "Handler for the OIDC callback that will retrieve and validate the access token.
   Upon successful validation, the handler responds with a 302 redirect to the original url
   (preserved in the state) before the auth flow was triggered.
   Unsuccessful authentication returns either a 400 Bad request, the downstream auth server
   response, or a 401 unauthorized with appropriate details."
  [{:keys [jwt-auth-server jwt-validator password] :as oidc-authenticator} request]
  (if (nil? oidc-authenticator)
    (utils/clj->json-response {:message "OIDC authentication disabled"} :status http-501-not-implemented)
    (let [{:keys [code-verifier code state-map]} (validate-oidc-callback-request password request)]
      (async/go
        (let [access-token-ch (jwt/request-access-token jwt-auth-server request oidc-callback-uri code code-verifier)
              body-or-throwable (async/<! access-token-ch)]
          (try
            (if (instance? Throwable body-or-throwable)
              (throw body-or-throwable)
              (let [access-token body-or-throwable
                    _ (log/info "successfully retrieved access token" (utils/truncate access-token 30))
                    key-id->jwk (jwt/get-key-id->jwk jwt-auth-server)
                    result-map-or-throwable (jwt/extract-claims jwt-validator key-id->jwk request access-token)
                    _ (when (instance? Throwable result-map-or-throwable)
                        (throw result-map-or-throwable))
                    {:keys [expiry-time subject]} result-map-or-throwable
                    _ (log/info "authenticated subject is" subject)
                    auth-params-map (auth/build-auth-params-map :oidc subject {:jwt-access-token access-token})
                    auth-cookie-age-in-seconds (- expiry-time (jwt/current-time-secs))]
                (auth/handle-request-auth
                  (constantly
                    (let [{:keys [redirect-uri]} state-map]
                      (-> {:headers {"cache-control" "no-store"
                                     "content-security-policy" content-security-policy-value
                                     "location" redirect-uri}
                           :status http-302-moved-temporarily}
                        (cookie-support/add-encoded-cookie password oidc-challenge-cookie "" 0)
                        (utils/attach-waiter-source))))
                  request auth-params-map password auth-cookie-age-in-seconds)))
            (catch Throwable throwable
              (log/info throwable "error in retrieving access token")
              (utils/exception->response
                (ex-info (.getMessage throwable)
                         (-> (ex-data throwable)
                           (utils/assoc-if-absent :log-level :info)
                           (utils/assoc-if-absent :status http-401-unauthorized))
                         throwable)
                request))))))))

(defn trigger-authorize-redirect
  "Triggers a 302 temporary redirect response to the authorize endpoint."
  [jwt-auth-server password {:keys [query-string uri] :as request} response]
  (let [request-host (utils/request->host request)
        request-scheme (utils/request->scheme request)
        code-verifier (create-code-verifier)
        state-data {:redirect-uri (str (name request-scheme) "://" request-host uri
                                       (when query-string (str "?" query-string)))}
        state-code (create-state-code password state-data)
        authorize-uri (jwt/retrieve-authorize-url
                        jwt-auth-server request oidc-callback-uri code-verifier state-code)]
    (-> response
      (assoc :status http-302-moved-temporarily)
      (update :headers assoc
              "cache-control" "no-store"
              "content-security-policy" content-security-policy-value
              "location" authorize-uri)
      (cookie-support/add-encoded-cookie
        password oidc-challenge-cookie code-verifier challenge-cookie-duration-secs))))

(defn make-oidc-auth-response-updater
  "Returns a response updater that rewrites 401 waiter responses to 302 redirects."
  [jwt-auth-server password request]
  (fn update-oidc-auth-response [{:keys [status] :as response}]
    (if (and (= status http-401-unauthorized)
             (utils/waiter-generated-response? response))
      ;; issue 302 redirect
      (trigger-authorize-redirect jwt-auth-server password request response)
      ;; non-401 response, avoid authentication challenge
      response)))

(defn supports-redirect?
  "Returns true when:
   - either the request is deemed to have come from a browser
   - or the accept-redirect=yes request header is present in the request."
  [oidc-authority request]
  (or (hu/browser-request? request)
      ;; Accept-Redirect request header "yes" means the user-agent will follow redirects.
      ;; Accept-Redirect-Auth request header indicates which authorities the user-agent is willing to redirect to and authenticate at.
      ;; https://tools.ietf.org/id/draft-williams-http-accept-auth-and-redirect-02.html#rfc.section.2
      (and (= "yes" (get-in request [:headers accept-redirect-header-name]))
           (let [accept-redirect-auth (get-in request [:headers accept-redirect-auth-header-name])]
             (or (str/blank? accept-redirect-auth)
                 (= "." accept-redirect-auth)
                 (some #(= oidc-authority %) (str/split accept-redirect-auth #" ")))))))

(defn wrap-auth-handler
  "Wraps the request handler with a handler to trigger OIDC+PKCE authentication."
  [{:keys [allow-oidc-auth-api? allow-oidc-auth-services? jwt-auth-server oidc-authorize-uri password]} request-handler]
  (let [oidc-authority (-> oidc-authorize-uri (URI.) (.getAuthority))]
    (fn oidc-auth-handler [{:keys [waiter-api-call?] :as request}]
      (let [use-oidc-auth? (or
                             ;; service requests will enable OIDC auth based on env variable or when absent, allow-oidc-auth-services?
                             (and (not waiter-api-call?)
                                  (= "true" (get-in request [:waiter-discovery :service-description-template "env" "USE_OIDC_AUTH"]
                                                    (str allow-oidc-auth-services?))))
                             ;; waiter api requests will enable OIDC auth based on allow-oidc-auth-api?
                             (and waiter-api-call? allow-oidc-auth-api?))
            use-redirect? (supports-redirect? oidc-authority request)]
        (cond
          (or (not use-oidc-auth?)
              (not use-redirect?) ;; OIDC auth is no-op when request cannot be redirected
              (auth/request-authenticated? request))
          (request-handler request)

          :else
          (ru/update-response
            (request-handler request)
            (make-oidc-auth-response-updater jwt-auth-server password request)))))))

(defrecord OidcAuthenticator [allow-oidc-auth-api? allow-oidc-auth-services? oidc-authorize-uri
                              jwt-auth-server jwt-validator password])

(defn create-oidc-authenticator
  "Factory function for creating OIDC authenticator middleware"
  [jwt-auth-server jwt-validator
   {:keys [allow-oidc-auth-api? allow-oidc-auth-services? oidc-authorize-uri password]
    :or {allow-oidc-auth-api? false
         allow-oidc-auth-services? false}}]
  {:pre [(satisfies? jwt/AuthServer jwt-auth-server)
         (some? jwt-validator)
         (boolean? allow-oidc-auth-api?)
         (boolean? allow-oidc-auth-services?)
         (not (str/blank? oidc-authorize-uri))
         (not-empty password)]}
  (->OidcAuthenticator allow-oidc-auth-api? allow-oidc-auth-services? oidc-authorize-uri
                       jwt-auth-server jwt-validator password))
