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
(ns waiter.auth.authentication
  (:require [clojure.tools.logging :as log]
            [waiter.cookie-support :as cookie-support]))

(def ^:const AUTH-COOKIE-NAME "x-waiter-auth")

(defprotocol Authenticator
  (auth-type [this]
    "Returns a keyword identifying the type of authenticator.")

  (check-user [this user service-id]
    "Checks if the user is setup correctly to successfully launch a service using the authentication scheme.
     Throws an exception if not.")

  (create-auth-handler [this request-handler]
    "Attaches middleware that enables the application to perform authentication.
     The middleware should
     - either issue a 401 challenge asking the client to authenticate itself,
     - or upon successful authentication populate the request with :authorization/user and :authenticated-principal"))

(defn- add-cached-auth
  [response password principal]
  (cookie-support/add-encoded-cookie response password AUTH-COOKIE-NAME [principal (System/currentTimeMillis)] 1))

(defn handle-request-auth
  "Invokes the given request-handler on the given request, adding the necessary
  auth headers on the way in, and the x-waiter-auth cookie on the way out."
  [request-handler request user principal password]
  (-> request
      (assoc :authorization/user user
             :authenticated-principal principal)
      request-handler
      (add-cached-auth password principal)
      cookie-support/cookies-async-response))

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

  (create-auth-handler [_ request-handler]
    (fn anonymous-handler [request]
      (handle-request-auth request-handler request run-as-user run-as-user password))))

(defn one-user-authenticator
  "Factory function for creating SingleUserAuthenticator"
  [{:keys [password run-as-user]}]
  (log/warn "use of SingleUserAuthenticator is strongly discouraged for production use:"
            "requests will use principal" run-as-user)
  (->SingleUserAuthenticator run-as-user password))