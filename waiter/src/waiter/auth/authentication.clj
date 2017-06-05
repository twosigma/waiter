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
  (:require [clojure.tools.logging :as log]))

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

;; An anonymous request does not contain any authentication information.
;; This is equivalent to granting everyone access to the resource.
;; The anonymous authenticator attaches the principal of run-as-user to the request.
;; In particular, this enables requests to launch processes as run-as-user.
;; Use of this authentication mechanism is strongly discouraged for production use.
(defrecord SingleUserAuthenticator [run-as-user]

  Authenticator

  (auth-type [_]
    :one-user)

  (check-user [_ _ _]
    (comment "do nothing"))

  (create-auth-handler [_ request-handler]
    (fn anonymous-handler [request]
      (let [request' (assoc request
                       :authorization/user run-as-user
                       :authenticated-principal run-as-user)]
        (request-handler request')))))

(defn one-user-authenticator
  "Factory function for creating SingleUserAuthenticator"
  [{:keys [run-as-user]}]
  (log/warn "use of SingleUserAuthenticator is strongly discouraged for production use:"
            "requests will use principal" run-as-user)
  (->SingleUserAuthenticator run-as-user))