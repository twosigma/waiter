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
  (:require [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [token-syncer.waiter :as waiter])
  (:import (org.eclipse.jetty.client HttpClient)))

(defn retrieve-token->cluster-url->token-data
  "Given a lists tokens and cluster urls, retrieve the token description on each cluster."
  [^HttpClient http-client cluster-urls all-tokens]
  (pc/map-from-keys
    (fn [token]
      (pc/map-from-keys
        (fn [cluster-url]
          (waiter/load-token-on-cluster http-client cluster-url token))
        cluster-urls))
    all-tokens))

(defn retrieve-token->latest-description
  "Given token->cluster-url->token-data, retrieves the latest token description for each token among all the clusters."
  [token->cluster-url->token-data]
  (pc/map-from-keys
    (fn [token]
      (let [latest-data
            (reduce (fn [accum-data [cluster-url token-data]]
                      (log/info cluster-url "token-data is" token-data ", accum-data" accum-data)
                      (let [accum-last-update-time (get-in accum-data [:description "last-update-time"] 0)
                            cluster-token-description (:description token-data)
                            cluster-last-update-time (get cluster-token-description "last-update-time" 0)]
                        (if (and (seq cluster-token-description) (< accum-last-update-time cluster-last-update-time))
                          {:cluster-url cluster-url, :description cluster-token-description}
                          accum-data)))
                    {}
                    (token->cluster-url->token-data token))]
        (log/info "Latest data for" token "is" latest-data)
        latest-data))
    (keys token->cluster-url->token-data)))

(defn hard-delete-token-on-all-clusters
  "Hard-deletes a given token on all clusters."
  [^HttpClient http-client cluster-urls token]
  (log/info "Hard-delete" token "on clusters" cluster-urls)
  (loop [[cluster-url & remaining-cluster-urls] (vec cluster-urls)
         cluster-sync-result {}]
    (if cluster-url
      (let [cluster-result
            (try
              (let [response (waiter/hard-delete-token-on-cluster http-client cluster-url token)]
                {:message :successfully-hard-deleted-token-on-cluster
                 :response response})
              (catch Exception ex
                (log/error ex "Unable to delete" token "on" cluster-url)
                {:cause (.getMessage ex)
                 :message :error-in-delete}))]
        (recur remaining-cluster-urls
               (assoc cluster-sync-result cluster-url cluster-result)))
      cluster-sync-result)))

(defn sync-token-on-clusters
  "Syncs a given token description on all clusters.
   If the cluster-url->token-data says that a given token was not successfully loaded, it is skipped.
   Token sync-ing is also skipped if the owners of the tokens are different."
  [^HttpClient http-client cluster-urls token token-description cluster-url->token-data]
  (loop [[cluster-url & remaining-cluster-urls] (vec cluster-urls)
         cluster-sync-result {}]
    (if cluster-url
      (let [cluster-result
            (try
              (let [{:keys [description error status]} (get cluster-url->token-data cluster-url)
                    latest-owner (get token-description "owner")
                    cluster-owner (get description "owner")]
                (cond
                  error
                  {:cause (.getMessage error)
                   :message :unable-to-read-token-on-cluster}

                  (nil? status)
                  {:cause "status missing from response"
                   :message :unable-to-read-token-on-cluster}

                  (and cluster-owner latest-owner (not= latest-owner cluster-owner))
                  {:cluster-data description
                   :latest-token-description token-description
                   :message :token-owners-are-different}

                  (not= token-description (get-in cluster-url->token-data [cluster-url :description]))
                  (let [response (waiter/store-token-on-cluster http-client cluster-url token token-description)]
                    {:message (if (true? (get token-description "deleted"))
                                :soft-delete-token-on-cluster
                                :sync-token-on-cluster)
                     :response response})

                  :else
                  {:message :token-already-synced}))
              (catch Exception ex
                (log/error ex "Unable to sync token on" cluster-url)
                {:cause (.getMessage ex)
                 :message :error-in-token-sync}))]
        (log/info cluster-url "sync result is" cluster-result)
        (recur remaining-cluster-urls
               (assoc cluster-sync-result cluster-url cluster-result)))
      cluster-sync-result)))

(defn sync-tokens
  "Syncs tokens across provided clusters based on cluster-urls."
  [http-client-wrapper cluster-urls]
  (try
    (log/info "Syncing tokens on clusters:" (set cluster-urls))
    (let [cluster-urls (set cluster-urls)
          cluster-url->tokens (pc/map-from-keys #(waiter/load-token-list http-client-wrapper %) cluster-urls)
          all-tokens (set (mapcat identity (vals cluster-url->tokens)))
          token->cluster-url->token-data (retrieve-token->cluster-url->token-data http-client-wrapper cluster-urls all-tokens)
          token->latest-description (retrieve-token->latest-description token->cluster-url->token-data)]
      (log/info "Found" (count all-tokens) "across the clusters")
      (loop [[token & remaining-tokens] (vec all-tokens)
             token-sync-result {}]
        (if token
          (do
            (log/info "Syncing token:" token)
            (let [{:keys [cluster-url description]} (token->latest-description token)
                  remaining-cluster-urls (disj cluster-urls cluster-url)
                  all-tokens-match (every? (fn all-tokens-match-pred [cluster-url]
                                             (= description (get-in token->cluster-url->token-data [token cluster-url :description])))
                                           remaining-cluster-urls)
                  all-soft-deleted (every? (fn soft-delete-pred [[_ token-data]]
                                             (true? (get-in token-data [:description "deleted"])))
                                           (token->cluster-url->token-data token))]
              (log/info "Syncing" token "with token description from" cluster-url
                        {:all-soft-deleted all-soft-deleted, :all-tokens-match all-tokens-match})
              (let [sync-result (if (and all-tokens-match all-soft-deleted (seq description))
                                  (hard-delete-token-on-all-clusters http-client-wrapper cluster-urls token)
                                  (sync-token-on-clusters http-client-wrapper remaining-cluster-urls token description
                                                          (token->cluster-url->token-data token)))]
                (recur remaining-tokens
                       (assoc token-sync-result
                         token {:description (token->latest-description token)
                                :sync-result sync-result})))))
          (do
            (log/info "Completed syncing tokens")
            {:num-tokens-processed (count all-tokens)
             :result token-sync-result}))))
    (catch Throwable th
      (log/error th "Unable to sync tokens")
      (throw th))))
