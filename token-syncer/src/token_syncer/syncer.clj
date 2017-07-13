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
(ns token-syncer.syncer
  (:require [plumbing.core :as pc]
            [token-syncer.correlation-id :as cid]
            [token-syncer.waiter :as waiter])
  (:import (org.eclipse.jetty.client HttpClient)))

(defn retrieve-token->router-url->token-data
  "Given a lists tokens and router urls, retrieve the token description on each router."
  [^HttpClient http-client router-urls all-tokens]
  (pc/map-from-keys
    (fn [token]
      (pc/map-from-keys
        (fn [router-url]
          (waiter/load-token-on-router http-client router-url token))
        router-urls))
    all-tokens))

(defn retrieve-token->latest-description
  "Given token->router-url->token-data, retrieves the latest token description for each token among all the routers."
  [token->router-url->token-data]
  (pc/map-from-keys
    (fn [token]
      (let [latest-data
            (reduce (fn [accum-data [router-url token-data]]
                      (cid/info router-url "token-data is" token-data ", accum-data" accum-data)
                      (let [accum-last-update-time (get-in accum-data [:description "last-update-time"] 0)
                            router-token-description (:description token-data)
                            router-last-update-time (get router-token-description "last-update-time" 0)]
                        (cid/info "accum-last-update-time" accum-last-update-time ", router-last-update-time" router-last-update-time)
                        (if (and (seq router-token-description) (< accum-last-update-time router-last-update-time))
                          {:description router-token-description, :router-url router-url}
                          accum-data)))
                    {}
                    (token->router-url->token-data token))]
        (cid/info "latest data for" token "is" latest-data)
        latest-data))
    (keys token->router-url->token-data)))

(defn hard-delete-token-on-all-routers
  "Hard-deletes a given token on all routers."
  [^HttpClient http-client router-urls token]
  (cid/info "hard-delete" token "on routers" router-urls)
  (loop [[router-url & remaining-router-urls] (vec router-urls)
         router-sync-result {}]
    (if router-url
      (let [router-result
            (try
              (let [response (waiter/hard-delete-token-on-router http-client router-url token)]
                {:message :successfully-hard-deleted-token-on-cluster
                 :response response})
              (catch Exception ex
                (cid/error ex "unable to delete" token "on" router-url)
                {:cause (.getMessage ex)
                 :message :error-in-delete}))]
        (recur remaining-router-urls
               (assoc router-sync-result router-url router-result)))
      router-sync-result)))

(defn sync-token-on-routers
  "Syncs a given token description on all routers.
   If the router-url->token-data says that a given token was not successfully loaded, it is skipped.
   Token sync-ing is also skipped if the owners of the tokens are different."
  [^HttpClient http-client router-urls token token-description router-url->token-data]
  (loop [[router-url & remaining-router-urls] (vec router-urls)
         router-sync-result {}]
    (if router-url
      (let [router-result
            (try
              (let [{:keys [description error status]} (get router-url->token-data router-url)
                    latest-owner (get token-description "owner")
                    router-owner (get description "owner")]
                (cond
                  error
                  {:cause (.getMessage error)
                   :message :unable-to-read-token-on-cluster}

                  (nil? status)
                  {:cause "status missing from response"
                   :message :unable-to-read-token-on-cluster}

                  (and router-owner latest-owner (not= latest-owner router-owner))
                  {:latest-token-description token-description
                   :message :token-owners-are-different
                   :router-data description}

                  (not= token-description (get-in router-url->token-data [router-url :description]))
                  (let [response (waiter/store-token-on-router http-client router-url token token-description)]
                    {:message (if (true? (get token-description "deleted"))
                                :soft-delete-token-on-cluster
                                :sync-token-on-cluster)
                     :response response})

                  :else
                  {:message :token-already-synced}))
              (catch Exception ex
                (cid/error ex "unable to sync token on" router-url)
                {:cause (.getMessage ex)
                 :message :error-in-token-sync}))]
        (cid/info router-url "sync result is" router-result)
        (recur remaining-router-urls
               (assoc router-sync-result router-url router-result)))
      router-sync-result)))

(defn sync-tokens
  "Syncs tokens across provided clusters based on router-urls."
  [^HttpClient http-client router-urls]
  (try
    (let [router-urls (set router-urls)
          router-url->tokens (pc/map-from-keys #(waiter/load-token-list http-client %) router-urls)
          all-tokens (set (mapcat identity (vals router-url->tokens)))
          token->router-url->token-data (retrieve-token->router-url->token-data http-client router-urls all-tokens)
          token->latest-description (retrieve-token->latest-description token->router-url->token-data)]
      (loop [[token & remaining-tokens] (vec all-tokens)
             token-sync-result {}]
        (if token
          (do
            (cid/info "syncing token:" token)
            (let [{:keys [router-url description]} (token->latest-description token)
                  remaining-router-urls (disj router-urls router-url)
                  all-tokens-match (every? (fn all-tokens-match-pred [router-url]
                                             (= description (get-in token->router-url->token-data [token router-url :description])))
                                           remaining-router-urls)
                  all-soft-deleted (every? (fn soft-delete-pred [[_ token-data]]
                                             (true? (get-in token-data [:description "deleted"])))
                                           (token->router-url->token-data token))]
              (cid/info "syncing" token "with token description from" router-url
                        {:all-soft-deleted all-soft-deleted, :all-tokens-match all-tokens-match})
              (let [sync-result (if (and all-tokens-match all-soft-deleted (seq description))
                                  (hard-delete-token-on-all-routers http-client router-urls token)
                                  (sync-token-on-routers http-client remaining-router-urls token description
                                                         (token->router-url->token-data token)))]
                (recur remaining-tokens
                       (assoc token-sync-result
                         token {:description (token->latest-description token)
                                :sync-result sync-result})))))
          (do
            (cid/info "token-sync-result:" token-sync-result)
            {:num-tokens-processed (count all-tokens)
             :result token-sync-result}))))
    (catch Throwable th
      (cid/error th "unable to sync tokens")
      (throw th))))
