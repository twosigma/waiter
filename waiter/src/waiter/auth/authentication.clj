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
(ns waiter.auth.authentication
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.cookie-support :as cookie-support]
            [waiter.middleware :as middleware]))

(def ^:const AUTH-COOKIE-NAME "x-waiter-auth")

(defprotocol Authenticator
  (auth-type [this]
    "Returns a keyword identifying the type of authenticator.")

  (check-user [this user service-id]
    "Checks if the user is setup correctly to successfully launch a service using the authentication scheme.
     Throws an exception if not.")

  (wrap-auth-handler [this request-handler]
    "Attaches middleware that enables the application to perform authentication.
     The middleware should
     - either issue a 401 challenge asking the client to authenticate itself,
     - or upon successful authentication populate the request with :authorization/user and :authorization/principal"))

(defn- add-cached-auth
  [response password principal]
  (cookie-support/add-encoded-cookie response password AUTH-COOKIE-NAME [principal (System/currentTimeMillis)] 1))

(defn auth-params-map
  "Creates a map intended to be merged into requests/responses."
  ([principal]
   (auth-params-map principal (first (str/split principal #"@" 2))))
  ([principal user]
   {:authorization/principal principal
    :authorization/user user}))

(defn handle-request-auth
  "Invokes the given request-handler on the given request, adding the necessary
  auth headers on the way in, and the x-waiter-auth cookie on the way out."
  [handler request user principal password]
  (let [auth-params-map (auth-params-map principal user)
        handler' (middleware/wrap-merge handler auth-params-map)]
    (-> request
        handler'
        (add-cached-auth password principal))))

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

(defn remove-auth-cookie
  "Removes the auth cookie"
  [cookie-string]
  (cookie-support/remove-cookie cookie-string AUTH-COOKIE-NAME))

;; An anonymous request does not contain any authentication information.
;; This is equivalent to granting everyone access to the resource.
;; The anonymous authenticator attaches the principal of run-as-user to the request.
;; In particular, this enables requests to launch processes as run-as-user.
;; Use of this authentication mechanism is strongly discouraged for production use.
(defrecord SingleUserAuthenticator [run-as-user password]

  Authenticator

  (auth-type [_]
    :one-user)

  (check-user [_ _ _]
    (comment "do nothing"))

  (wrap-auth-handler [_ request-handler]
    (fn anonymous-handler [request]
      (handle-request-auth request-handler request run-as-user run-as-user password))))

(defn one-user-authenticator
  "Factory function for creating SingleUserAuthenticator"
  [{:keys [password run-as-user]}]
  (log/warn "use of SingleUserAuthenticator is strongly discouraged for production use:"
            "requests will use principal" run-as-user)
  (->SingleUserAuthenticator run-as-user password))
