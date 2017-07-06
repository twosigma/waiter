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
(ns waiter.password-store
  (:require [clojure.tools.logging :as log]
            [waiter.utils :as utils]))

(defprotocol PasswordProvider
  "A simple protocol for providing passwords."
  (retrieve-passwords [this]
    "Retrieve a list of passwords."))

(defrecord ConfiguredPasswordProvider [passwords]
  PasswordProvider
  (retrieve-passwords [_] passwords))

(defn configured-provider
  [{:keys [passwords]}]
  (->ConfiguredPasswordProvider passwords))

(defn check-empty-passwords
  "Checks whether the input passwords are empty, if so invokes the callback.
  If no callback is provided, it defaults to System.exit(1)"
  ([passwords] (check-empty-passwords passwords (fn [] (log/fatal "Got empty passwords!") (System/exit 1))))
  ([passwords callback]
   (when (or (empty? passwords) (some empty? passwords))
     (callback))))
