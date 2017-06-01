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
    "Checks if the user has valid tickets under the authentication scheme. Throws an exception if not.")
  (create-auth-handler [this request-handler]
    "Attaches middleware that enables the application to perform authentication.
     The middleware should
     - either issue a 401 challenge asking the client to authenticate itself,
     - or upon successful authentication populate the request with :authorization/user and :authenticated-principal"))

;; An anonymous request does not contain any authentication information.
;; This is equivalent to granting everyone access to the resource.
;; The anonymous authenticator attaches the prinicpal of the user running Waiter to the request.
;; In particular, this enables requests to launch processes as the user running Waiter.
;; Use of this authentication mechanism is strongly discouraged for production use.
(defrecord AnonymousAuthenticator []
  Authenticator
  (auth-type [_]
    :anonymous)
  (check-user [_ _ _]
    (comment "do nothing"))
  (create-auth-handler [_ request-handler]
    (let [process-username (System/getProperty "user.name")]
      (log/warn "use of AnonymousAuthenticator is strongly discouraged for production use:"
                "requests will use principal" process-username)
      (fn anonymous-handler [request]
        (let [request' (assoc request
                         :authorization/user process-username
                         :authenticated-principal process-username)]
          (request-handler request'))))))

(defn anonymous-authenticator
  "Factory function for creating AnonymousAuthenticator"
  [_]
  (->AnonymousAuthenticator))