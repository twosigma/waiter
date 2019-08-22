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
(ns waiter.auth.authentication
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.cookie-support :as cookie-support]
            [waiter.middleware :as middleware]))

(def ^:const AUTH-COOKIE-NAME "x-waiter-auth")

(defprotocol Authenticator
  (wrap-auth-handler [this request-handler]
    "Attaches middleware that enables the application to perform authentication.
     The middleware should
     - issue a 401 challenge, or redirect, to get the client to authenticate itself,
     - or upon successful authentication populate the request with :authorization/user and :authorization/principal"))

(defprotocol CompositeAuthenticator
  (get-authentication-providers [this]
    "Get a list of supported authentication provider names."))

(extend-protocol CompositeAuthenticator
  Object
  (get-authentication-providers [_] []))

(defprotocol CallbackAuthenticator
  (process-callback [this request]
    "Process any requests that might come in after initiating authentication. e.g. receive a request
     with an authentication assertion after redirecting a user to authenticate with an identity provider."))

(extend-protocol CallbackAuthenticator
  Object
  (process-callback [this _]
    (throw (ex-info (str this " does not support authentication callbacks.")
                    {:status 400}))))

(defn- add-cached-auth
  [response password principal age-in-seconds]
  (cookie-support/add-encoded-cookie response password AUTH-COOKIE-NAME [principal (tc/to-long (t/now))]
                                     (or age-in-seconds (-> 1 t/days t/in-seconds))))

(defn auth-params-map
  "Creates a map intended to be merged into requests/responses."
  ([method principal]
   (auth-params-map method principal (first (str/split principal #"@" 2))))
  ([method principal user]
   {:authorization/method method
    :authorization/principal principal
    :authorization/user user}))

(defn request-authenticated?
  "Returns true if the authorization info is already available in the input map."
  [{:keys [authorization/principal authorization/user]}]
  (and principal user))

(defn handle-request-auth
  "Invokes the given request-handler on the given request, adding the necessary
  auth headers on the way in, and the x-waiter-auth cookie on the way out."
  ([handler request method principal password]
   (handle-request-auth handler request principal (auth-params-map method principal) password nil))
  ([handler request principal auth-params-map password age-in-seconds]
   (let [handler' (middleware/wrap-merge handler auth-params-map)]
     (-> request
       handler'
       (add-cached-auth password principal age-in-seconds)))))

(defn decode-auth-cookie
  "Decodes the provided cookie using the provided password.
   Returns a sequence containing [auth-principal auth-time]."
  [waiter-cookie password]
  (try
    (log/debug "decoding cookie:" waiter-cookie)
    (when waiter-cookie
      (let [decoded-cookie (cookie-support/decode-cookie-cached waiter-cookie password)]
        (if (seq decoded-cookie)
          decoded-cookie
          (log/warn "invalid decoded cookie:" decoded-cookie))))
    (catch Exception e
      (log/warn e "failed to decode cookie:" waiter-cookie))))

(defn decoded-auth-valid?
  "Verifies whether the decoded authenticated cookie is valid as per the following rules:
   The decoded value must be a sequence in the format: [auth-principal auth-time].
   In addition, the auth-principal must be a string and the auth-time must be less than a day old."
  [[auth-principal auth-time :as decoded-auth-cookie]]
  (log/debug "well-formed?" decoded-auth-cookie (integer? auth-time) (string? auth-principal) (= 2 (count decoded-auth-cookie)))
  (let [well-formed? (and decoded-auth-cookie
                          (integer? auth-time)
                          (string? auth-principal)
                          (= 2 (count decoded-auth-cookie)))
        one-day-in-millis (-> 1 t/days t/in-millis)]
    (and well-formed? (> (+ auth-time one-day-in-millis) (System/currentTimeMillis)))))

(defn get-auth-cookie-value
  "Retrieves the auth cookie."
  [cookie-string]
  (cookie-support/cookie-value cookie-string AUTH-COOKIE-NAME))

(defn get-and-decode-auth-cookie-value
  "Retrieves the auth cookie and decodes it using the provided password."
  [headers password]
  (some-> (get headers "cookie")
    (get-auth-cookie-value)
    (decode-auth-cookie password)))

(defn remove-auth-cookie
  "Removes the auth cookie"
  [cookie-string]
  (cookie-support/remove-cookie cookie-string AUTH-COOKIE-NAME))

(defn select-auth-header
  "Filters and return the first authorization header that passes the predicate."
  [{:keys [headers]} predicate]
  (let [{:strs [authorization]} headers
        auth-headers (if (string? authorization)
                       (str/split (str authorization) #",")
                       authorization)]
    (some #(when (predicate %) %) auth-headers)))

;; An anonymous request does not contain any authentication information.
;; This is equivalent to granting everyone access to the resource.
;; The anonymous authenticator attaches the principal of run-as-user to the request.
;; In particular, this enables requests to launch processes as run-as-user.
;; Use of this authentication mechanism is strongly discouraged for production use.
;; Real middleware implementations should:
;;   - either issue a 401 challenge asking the client to authenticate itself,
;;   - or upon successful authentication populate the request with :authorization/user and :authorization/principal"
(defrecord SingleUserAuthenticator [run-as-user password]
  Authenticator
  (wrap-auth-handler [_ request-handler]
    (fn anonymous-handler [request]
      (let [auth-params-map (auth-params-map :single-user run-as-user run-as-user)]
        (handle-request-auth request-handler request run-as-user auth-params-map password nil)))))

(defn one-user-authenticator
  "Factory function for creating single-user authenticator"
  [{:keys [password run-as-user]}]
  (log/warn "use of single-user authenticator is strongly discouraged for production use:"
            "requests will use principal" run-as-user)
  (->SingleUserAuthenticator run-as-user password))

(defn wrap-auth-cookie-handler
  "Returns a handler that can authenticate a request that contains a valid x-waiter-auth cookie."
  [password handler]
  (fn auth-cookie-handler
    [{:keys [headers] :as request}]
    (let [[auth-principal _ :as decoded-auth-cookie] (get-and-decode-auth-cookie-value headers password)]
      (if (decoded-auth-valid? decoded-auth-cookie)
        (let [auth-params-map (auth-params-map :cookie auth-principal)
              handler' (middleware/wrap-merge handler auth-params-map)]
          (handler' request))
        (handler request)))))
