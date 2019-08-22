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
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)))

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
          (pc/mapply hu/http-request http-client url http-options))))))

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

(def ^:const bearer-prefix "Bearer ")

;; access the private function for validating claims
(def validate-claims #'buddy.sign.jwt/validate-claims)

(defn validate-access-token
  "Validates the JWT access token using the provided keys, realm and issuer."
  [token-type issuer subject-key key-id->jwk realm request-scheme access-token]
  (when (str/blank? realm)
    (throw (ex-info "JWT authentication can only be used with host header"
                    {:log-level :info
                     :message "Host header is missing"
                     :status 403})))
  (when (not= :https request-scheme)
    (throw (ex-info "JWT authentication can only be used with HTTPS connections"
                    {:log-level :info
                     :message "Must use HTTPS connection"
                     :status 403})))
  (when (str/blank? access-token)
    (throw (ex-info "Must provide Bearer token in Authorization header"
                    {:headers {"www-authenticate" (str/trim bearer-prefix)}
                     :log-level :info
                     :message "Access token is empty"
                     :status 401})))
  (let [[jwt-header jwt-payload jwt-signature] (str/split access-token #"\." 3)
        www-auth-header (str bearer-prefix "realm=\"" realm "\"")]
    (when (or (str/blank? jwt-header)
              (str/blank? jwt-payload)
              (str/blank? jwt-signature))
      (throw (ex-info "JWT access token is malformed"
                      {:headers {"www-authenticate" www-auth-header}
                       :log-level :info
                       :message "JWT access token is malformed"
                       :status 401})))
    (let [{:keys [alg kid typ] :as decoded-header} (jwe/decode-header access-token)]
      (log/info "access token header:" decoded-header)
      (when (empty? decoded-header)
        (throw (ex-info "JWT authentication must include header part"
                        {:headers {"www-authenticate" www-auth-header}
                         :log-level :info
                         :message "JWT header is missing"
                         :status 403})))
      (when-not (contains? #{:eddsa :rs256} alg)
        (throw (ex-info (str "Only eddsa and rs256 algorithms supported, header specified algorithm as " alg)
                        {:headers {"www-authenticate" www-auth-header}
                         :log-level :info
                         :message "JWT header contains unsupported algorithm"
                         :status 401})))
      (when (str/blank? kid)
        (throw (ex-info "JWT header is missing key ID"
                        {:headers {"www-authenticate" www-auth-header}
                         :log-level :info
                         :message "JWT header is missing key ID"
                         :status 401})))
      (when (not= typ token-type)
        (throw (ex-info (str "Unsupported type " typ)
                        {:headers {"www-authenticate" www-auth-header}
                         :log-level :info
                         :message "JWT header contains unsupported type"
                         :status 401})))
      (let [public-key (get-in key-id->jwk [kid ::public-key])]
        (when (nil? public-key)
          (throw (ex-info (str "No matching JWKS key found for key " kid)
                          {:headers {"www-authenticate" www-auth-header}
                           :key-id kid
                           :log-level :info
                           :message "No matching JWKS key found"
                           :status 401})))
        (let [options {:alg alg
                       :aud realm
                       :iss issuer
                       :skip-validation true}
              {:keys [exp sub] :as claims} (try
                                             (jwt/unsign access-token public-key options)
                                             (catch ExceptionInfo ex
                                               (let [data (assoc (ex-data ex)
                                                            :headers {"www-authenticate" www-auth-header}
                                                            :log-level :info
                                                            :status 403)]
                                                 (throw (ex-info (.getMessage ex) data ex)))))]
          (log/info "access token claims:" claims)
          (try
            (validate-claims claims options)
            (catch ExceptionInfo ex
              (let [data (assoc (ex-data ex)
                           :headers {"www-authenticate" www-auth-header}
                           :log-level :info
                           :status 403)]
                (throw (ex-info (.getMessage ex) data ex)))))
          (when (str/blank? sub)
            (throw (ex-info (str "No subject provided in the token payload")
                            {:headers {"www-authenticate" www-auth-header}
                             :log-level :info
                             :message "No subject provided in the token payload"
                             :status 401})))
          (when (str/blank? (subject-key claims))
            (throw (ex-info (str "No " (name subject-key) " provided in the token payload")
                            {:headers {"www-authenticate" www-auth-header}
                             :log-level :info
                             :message (str "No " (name subject-key) " provided in the token payload")
                             :status 401})))
          (when (nil? exp)
            (throw (ex-info (str "No expiry provided in the token payload")
                            {:headers {"www-authenticate" www-auth-header}
                             :log-level :info
                             :message "No expiry provided in the token payload"
                             :status 401})))
          claims)))))

(defn current-time-secs
  "Returns the current time in seconds."
  []
  (-> (t/now) tc/to-long (/ 1000) long))

(defn- access-token?
  "Predicate to determine if an authorization header represents an access token."
  [authorization]
  (let [authorization (str authorization)]
    (and (str/starts-with? authorization bearer-prefix)
         (= 3 (count (str/split authorization #"\."))))))

(defn authenticate-request
  "Performs authentication and then
   - responds with an error response when authentication fails, or
   - invokes the downstream request handler using the authenticated credentials in the request."
  [request-handler token-type issuer subject-key key-id->jwk password {:keys [headers] :as request}]
  (let [validation-timer (metrics/waiter-timer "core" "jwt" "validation")
        timer-context (timers/start validation-timer)]
    (try
      (let [{:strs [host]} headers
            realm (utils/authority->host (str host))
            request-scheme (utils/request->scheme request)
            bearer-entry (auth/select-auth-header request #(str/starts-with? % bearer-prefix))
            access-token (str/trim (subs bearer-entry (count bearer-prefix)))
            {:keys [exp] :as claims} (validate-access-token token-type issuer subject-key key-id->jwk realm request-scheme access-token)
            _ (timers/stop timer-context)
            subject (subject-key claims)
            auth-params-map (auth/auth-params-map :jwt subject)
            auth-cookie-age-in-seconds (- exp (current-time-secs))]
        (counters/inc! (metrics/waiter-counter "core" "jwt" "validation" "success"))
        (auth/handle-request-auth
          request-handler request subject auth-params-map password auth-cookie-age-in-seconds))
      (catch Throwable th
        (timers/stop timer-context)
        (counters/inc! (metrics/waiter-counter "core" "jwt" "validation" "failed"))
        (utils/exception->response th request)))))

(defn wrap-auth-handler
  "Wraps the request handler with a handler to trigger JWT access token authentication."
  [{:keys [issuer keys-cache password subject-key token-type] :as jwt-authenticator} request-handler]
  (fn jwt-auth-handler [request]
    (if (and (not (auth/request-authenticated? request))
             (auth/select-auth-header request #(str/starts-with? % bearer-prefix)))
      (authenticate-request request-handler token-type issuer subject-key (:key-id->jwk @keys-cache) password request)
      (request-handler request))))

(defn retrieve-state
  "Returns the state of the JWT authenticator."
  [{:keys [keys-cache]}]
  (let [cache-data (update @keys-cache :key-id->jwk
                           (fn [key-id->jwk]
                             (pc/map-vals #(update % ::public-key str) key-id->jwk)))]
    {:cache-data cache-data}))

(defrecord JwtAuthenticator [issuer keys-cache password subject-key supported-algorithms token-type])

(defn jwt-authenticator
  "Factory function for creating jwt authenticator middleware"
  [{:keys [http-options issuer jwks-url password subject-key supported-algorithms token-type update-interval-ms]}]
  {:pre [(map? http-options)
         (not (str/blank? issuer))
         (some? password)
         (not (str/blank? jwks-url))
         (keyword? subject-key)
         supported-algorithms
         (set? supported-algorithms)
         (empty? (set/difference supported-algorithms #{:eddsa :rs256}))
         (not (str/blank? token-type))
         (and (integer? update-interval-ms)
              (not (neg? update-interval-ms)))]}
  (let [keys-cache (atom {})
        http-client (-> http-options
                      (utils/assoc-if-absent :client-name "waiter-jwt")
                      (utils/assoc-if-absent :user-agent "waiter-jwt")
                      hu/http-client-factory)]
    (start-jwt-cache-maintainer http-client http-options jwks-url update-interval-ms supported-algorithms keys-cache)
    (->JwtAuthenticator issuer keys-cache password subject-key supported-algorithms token-type)))
