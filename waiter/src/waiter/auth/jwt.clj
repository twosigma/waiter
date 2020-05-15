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
(ns waiter.auth.jwt
  (:require [buddy.core.keys :as buddy-keys]
            [buddy.sign.jwe :as jwe]
            [buddy.sign.jwt :as jwt]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metrics.counters :as counters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.auth.authentication :as auth]
            [waiter.metrics :as metrics]
            [waiter.status-codes :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.util.regex Pattern)))

(defn eddsa-key?
  "Returns true if the JWKS entry represents an EDSA key."
  [{:keys [crv kid kty use x]}]
  (and (= "Ed25519" crv)
       (= "OKP" kty)
       (= "sig" use)
       (not (str/blank? kid))
       (not (str/blank? x))))

(defn rs256-key?
  "Returns true if the JWKS entry represents an RSA256 key."
  [{:keys [e kid kty n use]}]
  (and (= "AQAB" e)
       (= "RSA" kty)
       (= "sig" use)
       (not (str/blank? kid))
       (not (str/blank? n))))

(defn supported-key?
  "Returns true if the JWKS entry represents a supported key."
  [supported-algorithms entry]
  (or (and (some #(= % :eddsa) supported-algorithms) (eddsa-key? entry))
      (and (some #(= % :rs256) supported-algorithms) (rs256-key? entry))))

(defn retrieve-public-key
  "Returns the EdDSAPublicKey public key from the provided string."
  [entry]
  (metrics/with-timer!
    (metrics/waiter-timer "core" "jwt" "key-creation")
    (constantly true)
    (buddy-keys/jwk->public-key entry)))

(defn attach-public-key
  "Attaches the EdDSA public key into the provided entries."
  [entry]
  (assoc entry ::public-key (retrieve-public-key entry)))

(defn retrieve-jwks-with-retries
  "Retrieves the JWKS using the specified url.
   JWKS retrieval tried retry-limit times at intervals on retry-interval-ms ms when there is an error."
  [http-client url
   {:keys [retry-interval-ms retry-limit]
    :or {retry-interval-ms 100
         retry-limit 2}
    :as options}]
  (let [with-retries (utils/retry-strategy
                       {:delay-multiplier 1.0, :initial-delay-ms retry-interval-ms, :max-retries retry-limit})
        http-options (dissoc options :retry-interval-ms :retry-limit)]
    (with-retries
      (fn retrieve-jwks-task []
        (if (str/starts-with? url "file://")
          (-> url slurp json/read-str walk/keywordize-keys)
          (let [correlation-id (utils/unique-identifier)
                http-options (update http-options :headers assoc "x-cid" correlation-id)]
            (log/info "updating jwks entries from server, cid is" correlation-id)
            (pc/mapply hu/http-request http-client url http-options)))))))

(defn refresh-keys-cache
  "Update the cache of users with prestashed JWK keys."
  [http-client http-options url supported-algorithms keys-cache]
  (metrics/with-timer!
    (metrics/waiter-timer "core" "jwt" "refresh-keys-cache")
    (fn [elapsed-nanos]
      (log/info "JWKS keys retrieval took" elapsed-nanos "ns"))
    (let [response (retrieve-jwks-with-retries http-client url http-options)]
      (when-not (map? response)
        (throw (ex-info "Invalid response from the JWKS endpoint"
                        {:response response :url url})))
      (let [all-keys (:keys response)
            keys (filter #(supported-key? supported-algorithms %) all-keys)]
        (when (empty? keys)
          (throw (ex-info "No supported keys found from the JWKS endpoint"
                          {:response response
                           :url url})))
        (log/info "retrieved entries from the JWKS endpoint" response)
        (reset! keys-cache {:key-id->jwk (->> keys
                                           (map attach-public-key)
                                           (pc/map-from-vals :kid))
                            :last-update-time (t/now)
                            :summary {:num-filtered-keys (count keys)
                                      :num-jwks-keys (count all-keys)}})))))

(defn start-jwt-cache-maintainer
  "Starts a timer task to maintain the keys-cache."
  [http-client http-options jwks-url update-interval-ms supported-algorithms keys-cache]
  {:cancel-fn (du/start-timer-task
                (t/millis update-interval-ms)
                (fn refresh-keys-cache-task []
                  (refresh-keys-cache http-client http-options jwks-url supported-algorithms keys-cache)))
   :query-state-fn (fn query-jwt-cache-state []
                     @keys-cache)})

(defprotocol AuthServer
  (get-key-id->jwk [this]
    "Returns a map for public key id to the JSON Web Key which contains the public key used to
     verify any JSON Web Token (JWT) issued by the authorization server.")
  (retrieve-server-state [this include-flags]
    "Returns the current state of the auth server."))

(defrecord JwtAuthServer [jwks-url keys-cache]
  AuthServer
  (get-key-id->jwk [_]
    (get @keys-cache :key-id->jwk))
  (retrieve-server-state [_ include-flags]
    (cond-> {:endpoints {:jwks jwks-url}}
      (contains? include-flags "jwks")
      (assoc :jwks {:cache-data (update @keys-cache :key-id->jwk
                                        (fn stringify-public-keys [key-id->jwk]
                                          (pc/map-vals #(update % ::public-key str) key-id->jwk)))}))))

(defn create-auth-server
  [{:keys [http-options jwks-url supported-algorithms update-interval-ms]}]
  {:pre [(map? http-options)
         (not (str/blank? jwks-url))
         supported-algorithms
         (set? supported-algorithms)
         (empty? (set/difference supported-algorithms #{:eddsa :rs256}))
         (and (integer? update-interval-ms)
              (not (neg? update-interval-ms)))]}
  (let [keys-cache (atom {})
        http-client (-> http-options
                      (utils/assoc-if-absent :client-name "waiter-jwt")
                      (utils/assoc-if-absent :user-agent "waiter-jwt")
                      hu/http-client-factory)]
    (start-jwt-cache-maintainer http-client http-options jwks-url update-interval-ms supported-algorithms keys-cache)
    (->JwtAuthServer jwks-url keys-cache)))

(def ^:const bearer-prefix "Bearer ")

(defn- regex-pattern?
  "Predicate to check if the input is a regex pattern."
  [candidate]
  (instance? Pattern candidate))

(defn- validate-issuer
  "Validates the issuers against the specific string or pattern constraint."
  [issuer-constraint issuer]
  (cond
    (string? issuer-constraint)
    (= issuer-constraint issuer)
    (regex-pattern? issuer-constraint)
    (re-find issuer-constraint issuer)))

(defn- validate-claims
  "Checks the issuer in the `:iss` claim against one of the allowed issuers in the passed `:iss`.
   Passed `:iss` must be a string.
   If no `:iss` is passed, this check is not performed.

   Checks the `:aud` claim against the single valid audience in the passed `:aud`.
   If no `:aud` is passed, this check is not performed.

   Checks the `:exp` claim is not less than now.
   If no `:exp` claim exists, this check is not performed.

   Checks the `:sub` and subject-key claims are present.

   A check that fails raises an exception."
  [{:keys [exp iss sub] :as claims}
   {:keys [aud issuer-constraints max-expiry-duration-ms subject-key subject-regex]}]
  ;; Check the `:iss` claim.
  (if (str/blank? iss)
    (throw (ex-info (str "Issuer not provided in claims")
                    {:log-level :info
                     :status http-401-unauthorized}))
    (when-not (some #(validate-issuer % iss) issuer-constraints)
      (throw (ex-info (str "Issuer does not match provided constraints")
                      {:issuer iss
                       :log-level :info
                       :status http-401-unauthorized}))))
  ;; Check the `:aud` claim.
  (when (and aud (not= aud (:aud claims)))
    (throw (ex-info (str "Audience does not match " aud)
                    {:log-level :info
                     :status http-401-unauthorized})))
  ;; Check the `:exp` claim.
  (when (nil? exp)
    (throw (ex-info "No expiry provided in the token payload"
                    {:log-level :info
                     :status http-401-unauthorized})))
  (let [now-epoch (tc/to-epoch (t/now))
        max-epoch (+ now-epoch max-expiry-duration-ms)
        claims-exp (:exp claims)]
    (when (and claims-exp (<= claims-exp now-epoch))
      (throw (ex-info (format "Token is expired (%s)" claims-exp)
                      {:log-level :info
                       :status http-401-unauthorized})))
    (when (and claims-exp (> claims-exp max-epoch))
      (throw (ex-info (format "Token expiry is too far into the future (%s)" claims-exp)
                      {:log-level :info
                       :status http-401-unauthorized}))))
  ;; Check the `:sub` claim.
  (when (str/blank? sub)
    (throw (ex-info "No subject provided in the token payload"
                    {:log-level :info
                     :status http-401-unauthorized})))
  ;; check the subject
  (when-not (= subject-key :sub)
    (when (str/blank? (subject-key claims))
      (throw (ex-info (str "No " (name subject-key) " provided in the token payload")
                      {:log-level :info
                       :status http-401-unauthorized}))))
  (let [subject (subject-key claims)]
    (when-not (re-find subject-regex subject)
      (throw (ex-info (str "Provided subject in the token payload does not satisfy the validation regex")
                      {:log-level :info
                       :status http-401-unauthorized
                       :subject subject
                       :subject-regex subject-regex}))))
  claims)

(defn validate-access-token
  "Validates the JWT access token using the provided keys, realm and issuer."
  [token-type issuer-constraints subject-key subject-regex supported-algorithms key-id->jwk
   max-expiry-duration-ms realm request-scheme access-token]
  (when (str/blank? realm)
    (throw (ex-info "JWT authentication can only be used with host header"
                    {:log-level :info
                     :message "Host header is missing"
                     :status http-403-forbidden})))
  (when (not= :https request-scheme)
    (throw (ex-info "JWT authentication can only be used with HTTPS connections"
                    {:log-level :info
                     :message "Must use HTTPS connection"
                     :status http-403-forbidden})))
  (when (str/blank? access-token)
    (throw (ex-info "Must provide Bearer token in Authorization header"
                    {:log-level :info
                     :message "Access token is empty"
                     :status http-401-unauthorized})))
  (let [[jwt-header jwt-payload jwt-signature] (str/split access-token #"\." 3)]
    (when (or (str/blank? jwt-header)
              (str/blank? jwt-payload)
              (str/blank? jwt-signature))
      (throw (ex-info "JWT access token is malformed"
                      {:log-level :info
                       :message "JWT access token is malformed"
                       :status http-401-unauthorized})))
    (let [{:keys [alg kid typ] :as decoded-header} (jwe/decode-header access-token)]
      (log/info "access token header:" decoded-header)
      (when (empty? decoded-header)
        (throw (ex-info "JWT authentication must include header part"
                        {:log-level :info
                         :message "JWT header is missing"
                         :status http-403-forbidden})))
      (when-not (contains? supported-algorithms alg)
        (throw (ex-info (str "Unsupported algorithm " alg " in token header, supported algorithms: " supported-algorithms)
                        {:log-level :info
                         :message "JWT header contains unsupported algorithm"
                         :status http-401-unauthorized})))
      (when (str/blank? kid)
        (throw (ex-info "JWT header is missing key ID"
                        {:log-level :info
                         :message "JWT header is missing key ID"
                         :status http-401-unauthorized})))
      (when (not= typ token-type)
        (throw (ex-info (str "Unsupported type " typ)
                        {:log-level :info
                         :message "JWT header contains unsupported type"
                         :status http-401-unauthorized})))
      (let [public-key (get-in key-id->jwk [kid ::public-key])]
        (when (nil? public-key)
          (throw (ex-info (str "No matching JWKS key found for key " kid)
                          {:key-id kid
                           :log-level :info
                           :message "No matching JWKS key found"
                           :status http-401-unauthorized})))
        (let [options {:alg alg
                       :skip-validation true}
              claims (try
                       (jwt/unsign access-token public-key options)
                       (catch ExceptionInfo ex
                         (let [data (assoc (ex-data ex)
                                      :log-level :info
                                      :status http-401-unauthorized)]
                           (throw (ex-info (.getMessage ex) data ex)))))
              validation-options (assoc options
                                   :aud realm
                                   :issuer-constraints issuer-constraints
                                   :max-expiry-duration-ms max-expiry-duration-ms
                                   :subject-key subject-key
                                   :subject-regex subject-regex)]
          (log/info "access token claims:" claims)
          (validate-claims claims validation-options))))))

(defn current-time-secs
  "Returns the current time in seconds."
  []
  (-> (t/now) tc/to-long (/ 1000) long))

(defn request->realm
  "Extracts the realm from the host header in the request."
  [request]
  (some-> request :headers (get "host") utils/authority->host))

(defn- access-token?
  "Predicate to determine if an authorization header represents an access token."
  [authorization]
  (let [authorization (str authorization)]
    (and (str/starts-with? authorization bearer-prefix)
         (= 3 (count (str/split authorization #"\."))))))

(defn extract-claims
  "Returns either claims in the access token provided in the request, or
   an exception that occurred while attempting to extract the claims."
  [token-type issuer-constraints subject-key subject-regex supported-algorithms key-id->jwk max-expiry-duration-ms
   request]
  (let [validation-timer (metrics/waiter-timer "core" "jwt" "validation")
        timer-context (timers/start validation-timer)]
    (try
      (let [realm (request->realm request)
            request-scheme (utils/request->scheme request)
            bearer-entry (auth/select-auth-header request access-token?)
            access-token (str/trim (subs bearer-entry (count bearer-prefix)))
            claims (validate-access-token token-type issuer-constraints subject-key subject-regex supported-algorithms
                                          key-id->jwk max-expiry-duration-ms realm request-scheme access-token)]
        (timers/stop timer-context)
        (counters/inc! (metrics/waiter-counter "core" "jwt" "validation" "success"))
        claims)
      (catch Throwable throwable
        (timers/stop timer-context)
        (counters/inc! (metrics/waiter-counter "core" "jwt" "validation" "failed"))
        (log/info throwable "error in access token validation")
        throwable))))

(defn make-401-response-updater
  "Returns a function that attaches the www-authenticate header to the response if it has status 401."
  [request]
  (fn update-401-response [{:keys [status] :as response}]
    (if (and (= status http-401-unauthorized) (utils/waiter-generated-response? response))
      ;; add to challenge initiated by Waiter
      (let [realm (request->realm request)
            www-auth-header (if (str/blank? realm)
                              (str/trim bearer-prefix)
                              (str bearer-prefix "realm=\"" realm "\""))]
        (log/debug "attaching www-authenticate header to response")
        (ru/attach-header response "www-authenticate" www-auth-header))
      ;; non-401 response, avoid authentication challenge
      response)))

(defn authenticate-request
  "Performs authentication and then
   - responds with an error response when authentication fails, or
   - invokes the downstream request handler using the authenticated credentials in the request."
  [request-handler token-type issuer-constraints subject-key subject-regex supported-algorithms key-id->jwk
   password max-expiry-duration-ms request]
  (let [claims-or-throwable (extract-claims token-type issuer-constraints subject-key subject-regex supported-algorithms
                                            key-id->jwk max-expiry-duration-ms request)]
    (if (instance? Throwable claims-or-throwable)
      (if (-> claims-or-throwable ex-data :status (= http-401-unauthorized))
        ;; allow downstream processing before deciding on authentication challenge in response
        (ru/update-response
          (request-handler request)
          (make-401-response-updater request))
        ;; non-401 response avoids further downstream handler processing
        (utils/exception->response claims-or-throwable request))
      (let [{:keys [exp] :as claims} claims-or-throwable
            subject (subject-key claims)
            auth-params-map (auth/auth-params-map :jwt subject)
            auth-cookie-age-in-seconds (- exp (current-time-secs))]
        (auth/handle-request-auth
          request-handler request subject auth-params-map password auth-cookie-age-in-seconds)))))

(defn wrap-auth-handler
  "Wraps the request handler with a handler to trigger JWT access token authentication."
  [{:keys [allow-bearer-auth-api? allow-bearer-auth-services? attach-www-authenticate-on-missing-bearer-token?
           auth-server issuer-constraints max-expiry-duration-ms password subject-key subject-regex
           supported-algorithms token-type]}
   request-handler]
  (fn jwt-auth-handler [{:keys [waiter-api-call?] :as request}]
    (let [use-jwt-auth? (or
                          ;; service requests will enable JWT auth based on env variable or when absent, the allow-bearer-auth-services?
                          (and (not waiter-api-call?)
                               (= "true" (get-in request [:waiter-discovery :service-description-template "env" "USE_BEARER_AUTH"]
                                                 (str allow-bearer-auth-services?))))
                          ;; waiter api requests will enable JWT auth based on allow-bearer-auth-api?
                          (and waiter-api-call? allow-bearer-auth-api?))]
      (cond
        (or (not use-jwt-auth?)
            (auth/request-authenticated? request))
        (request-handler request)

        (auth/select-auth-header request access-token?)
        (authenticate-request request-handler token-type issuer-constraints subject-key subject-regex supported-algorithms
                              (get-key-id->jwk auth-server) password max-expiry-duration-ms request)

        :else
        (cond-> (request-handler request)
          attach-www-authenticate-on-missing-bearer-token?
          (ru/update-response (make-401-response-updater request)))))))

(defrecord JwtAuthenticator [allow-bearer-auth-api? allow-bearer-auth-services?
                             attach-www-authenticate-on-missing-bearer-token?
                             auth-server issuer-constraints max-expiry-duration-ms password
                             subject-key subject-regex supported-algorithms token-type])

(def ^:const default-subject-regex #"([a-zA-Z0-9]+)@([a-zA-Z0-9]+(-[a-zA-Z0-9]+)*\.)+([a-zA-Z]{2,})$")

(defn- issuer->issuer-constraints
  "Converts the input issuer config to issuer-constraints vector."
  [issuer]
  (cond-> issuer
    (or (string? issuer) (regex-pattern? issuer)) vector))

(defn jwt-authenticator
  "Factory function for creating jwt authenticator middleware"
  [auth-server
   {:keys [allow-bearer-auth-api? allow-bearer-auth-services? attach-www-authenticate-on-missing-bearer-token?
           issuer max-expiry-duration-ms password subject-key subject-regex
           supported-algorithms token-type]
    :or {allow-bearer-auth-api? true
         allow-bearer-auth-services? false
         attach-www-authenticate-on-missing-bearer-token? true
         max-expiry-duration-ms (-> 24 t/hours t/in-millis)
         subject-regex default-subject-regex}}]
  {:pre [(satisfies? AuthServer auth-server)
         (boolean? allow-bearer-auth-api?)
         (boolean? allow-bearer-auth-services?)
         (boolean? attach-www-authenticate-on-missing-bearer-token?)
         (vector? (issuer->issuer-constraints issuer))
         (seq (issuer->issuer-constraints issuer))
         (every? #(or (and (string? %)
                           (not (str/blank? %)))
                      (and (regex-pattern? %)
                           (nil? (re-find % ""))
                           (nil? (re-find % " "))))
                 (issuer->issuer-constraints issuer))
         (pos? max-expiry-duration-ms)
         (some? password)
         (keyword? subject-key)
         supported-algorithms
         (set? supported-algorithms)
         (empty? (set/difference supported-algorithms #{:eddsa :rs256}))
         (not (str/blank? token-type))]}
  (let [issuer-constraints (issuer->issuer-constraints issuer)]
    (->JwtAuthenticator allow-bearer-auth-api? allow-bearer-auth-services?
                        attach-www-authenticate-on-missing-bearer-token?
                        auth-server issuer-constraints max-expiry-duration-ms password
                        subject-key subject-regex supported-algorithms token-type)))