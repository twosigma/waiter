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
(ns waiter.auth.kerberos
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.auth.authentication :as auth]
            [waiter.authorization :as authz]
            [waiter.auth.spnego :as spnego]
            [waiter.metrics :as metrics]
            [waiter.util.utils :as utils])
  (:import (java.util.concurrent LinkedBlockingQueue ThreadPoolExecutor TimeUnit)))

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

(defn refresh-prestash-cache
  "Update the cache of users with prestashed kerberos tickets"
  [prestash-cache host]
  (when-let [users (get-opt-in-accounts host)]
    (reset! prestash-cache users)
    (log/debug "refreshed the prestash cache with" (count users) "users")
    users))

(defn start-prestash-cache-maintainer
  "Starts an async/go-loop to maintain the prestash-cache."
  [prestash-cache max-update-interval min-update-interval host query-chan]
  (let [exit-chan (async/chan 1)]
    (refresh-prestash-cache prestash-cache host)
    (async/go-loop [{:keys [timeout-chan last-updated] :as current-state}
                    {:timeout-chan (async/timeout max-update-interval)
                     :last-updated (t/now)
                     :continue-looping true}]
      (let [[args chan] (async/alts! [exit-chan timeout-chan query-chan] :priority true)
            new-state
            (condp = chan
              exit-chan (assoc current-state :continue-looping false)
              timeout-chan
              (do
                (refresh-prestash-cache prestash-cache host)
                (assoc current-state :timeout-chan (async/timeout max-update-interval)
                                     :last-updated (t/now)))
              query-chan
              (let [{:keys [response-chan]} args]
                (if (t/before? (t/now) (t/plus last-updated (t/millis min-update-interval)))
                  (do
                    (async/>! response-chan @prestash-cache)
                    current-state)
                  (let [users (refresh-prestash-cache prestash-cache host)]
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
  [prestash-cache user]
  (let [users @prestash-cache]
    (or (empty? users) (contains? users user))))

(defn check-has-prestashed-tickets
  "Checks if the run-as-user has prestashed tickets available. Throws an exception if not."
  [prestash-cache query-chan run-as-user service-id]
  (when-not (is-prestashed? prestash-cache run-as-user)
    (let [response-chan (async/promise-chan)
          _ (async/>!! query-chan {:response-chan response-chan})
          [users chan] (async/alts!! [response-chan (async/timeout 1000)] :priority true)]
      (when (and (= response-chan chan) (not (contains? users run-as-user)))
        (throw (ex-info "No prestashed tickets available"
                        {:message (utils/message :prestashed-tickets-not-available)
                         :service-id service-id
                         :status 403
                         :user run-as-user
                         :log-level :warn}))))))

(defrecord KerberosAuthenticator [^ThreadPoolExecutor executor max-queue-length password]
  auth/Authenticator
  (wrap-auth-handler [_ request-handler]
    (spnego/require-gss request-handler executor max-queue-length password)))

(defn kerberos-authenticator
  "Factory function for creating Kerberos authenticator middleware"
  [{:keys [concurrency-level keep-alive-mins max-queue-length password]}]
  {:pre [(not-empty password)
         (integer? concurrency-level)
         (pos? concurrency-level)
         (integer? keep-alive-mins)
         (pos? keep-alive-mins)
         (integer? max-queue-length)
         (pos? max-queue-length)]}
  (let [queue (LinkedBlockingQueue.)
        executor (ThreadPoolExecutor. concurrency-level concurrency-level keep-alive-mins TimeUnit/MINUTES queue)]
    (metrics/waiter-gauge #(.getActiveCount executor)
                          "core" "kerberos" "throttle" "active-thread-count")
    (metrics/waiter-gauge #(- concurrency-level (.getActiveCount executor))
                          "core" "kerberos" "throttle" "available-thread-count")
    (metrics/waiter-gauge #(.getMaximumPoolSize executor)
                          "core" "kerberos" "throttle" "max-thread-count")
    (metrics/waiter-gauge #(-> executor .getQueue .size)
                          "core" "kerberos" "throttle" "pending-task-count")
    (metrics/waiter-gauge #(.getTaskCount executor)
                          "core" "kerberos" "throttle" "scheduled-task-count")
    (->KerberosAuthenticator executor max-queue-length password)))

(defrecord KerberosAuthorizer
  [prestash-cache query-chan]
  authz/Authorizer
  (check-user [_ user service-id]
    (check-has-prestashed-tickets prestash-cache query-chan user service-id))
  (state [_]
    {:type :kerberos
     :users @prestash-cache}))

(defn kerberos-authorizer
  "Factory function for creating KerberosAuthorizer"
  [{:keys [prestash-cache-min-refresh-ms prestash-cache-refresh-ms prestash-query-host]}]
  {:pre [(pos-int? prestash-cache-min-refresh-ms)
         (pos-int? prestash-cache-refresh-ms)
         (not (str/blank? prestash-query-host))]}
  (let [; use nil to initialize cache so that if it fails to populate, we can return true for all users
        prestash-cache (atom nil)
        query-chan (async/chan 1024)]
    (start-prestash-cache-maintainer
      prestash-cache
      prestash-cache-refresh-ms
      prestash-cache-min-refresh-ms
      prestash-query-host
      query-chan)
    (->KerberosAuthorizer prestash-cache query-chan)))
