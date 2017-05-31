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
(ns waiter.kerberos
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.utils :as utils]))

(defn get-opt-in-accounts
  "Returns the list of users whose tickets are prestashed on host"
  [host]
  (try
    (let [{:keys [exit out err]} (shell/sh "krb5_prestash" "query" "host" host)]
      (if (zero? exit)
        (set (map #(first (str/split % #"@" 2)) (str/split-lines out)))
        (do
          (log/error "Failed to reload prestash cache: " err)
          nil)))
    (catch Exception e
      (log/error e "Failed to update prestash cache")
      nil)))

; use nil to initialize cache so that if it fails to populate, we can return true for all users
(let [prestash-cache (atom nil)]
  (defn refresh-prestash-cache
    "Update the cache of users with prestashed kerberos tickets"
    [host]
    (when-let [users (get-opt-in-accounts host)]
      (reset! prestash-cache users)
      (log/info "Refreshed the prestash cache with" (count users) "users")
      users))

  (defn start-prestash-cache-maintainer
    "Starts an async/go-loop to maintain the prestash-cache."
    [max-update-interval min-update-interval host query-chan]
    (let [exit-chan (async/chan 1)]
      (refresh-prestash-cache host)
      (async/go-loop [{:keys [timeout-chan continue-looping last-updated] :as current-state}
                      {:timeout-chan (async/timeout max-update-interval)
                       :last-updated (t/now)
                       :continue-looping true}]
        (let [[args chan] (async/alts! [exit-chan timeout-chan query-chan] :priority true)
              new-state
              (condp = chan
                exit-chan (assoc current-state :continue-looping false)
                timeout-chan
                (do
                  (refresh-prestash-cache host)
                  (assoc current-state :timeout-chan (async/timeout max-update-interval)
                                       :last-updated (t/now)))
                query-chan
                (let [{:keys [response-chan]} args]
                  (if (t/before? (t/now) (t/plus last-updated (t/millis min-update-interval)))
                    (do
                      (async/>! response-chan @prestash-cache)
                      current-state)
                    (let [users (refresh-prestash-cache host)]
                      (async/>! response-chan users)
                      (assoc current-state :timeout-chan (async/timeout max-update-interval)
                             :last-updated (t/now))))))]
          (when (:continue-looping new-state)
            (recur new-state))))
      {:exit-chan exit-chan
       :query-chan query-chan}))

  (defn is-prestashed?
    "Returns true if the user has prestashed
    tickets and false otherwise. If the cache has
    not been populated, returns true for all users."
    [user]
    (let [users @prestash-cache]
      (or (empty? users) (contains? users user)))))
