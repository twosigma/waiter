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
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [token-syncer.waiter :as waiter])
  (:import (org.eclipse.jetty.client HttpClient)))

(defn- successful?
  "Returns true if the response has a 2XX status code."
  [{:keys [status]}]
  (and (integer? status) (<= 200 status 299)))

(defn retrieve-token->url->token-data
  "Given a lists tokens and cluster urls, retrieve the token description on each cluster."
  [^HttpClient http-client cluster-urls all-tokens]
  (pc/map-from-keys
    (fn [token]
      (pc/map-from-keys
        (fn [cluster-url]
          (waiter/load-token http-client cluster-url token))
        cluster-urls))
    all-tokens))

(defn retrieve-token->latest-description
  "Given token->cluster-url->token-data, retrieves the latest token description for each token
   among all the clusters."
  [token->cluster-url->token-data]
  (->> token->cluster-url->token-data
       (pc/map-vals
         (fn [cluster-url->token-data]
           (when (seq cluster-url->token-data)
             (let [[cluster-url token-data]
                   (apply max-key #(-> % val (get-in [:description "last-update-time"] 0))
                          ;; sort the clusters to ensure consistent outputs
                          (-> cluster-url->token-data sort reverse))]
               {:cluster-url cluster-url
                :description (:description token-data)
                :token-etag (:token-etag token-data "0")}))))
       (pc/map-vals (fnil identity {}))))

(defn hard-delete-token-on-all-clusters
  "Hard-deletes a given token on all clusters."
  [^HttpClient http-client cluster-urls token token-etag]
  (log/info "hard-delete" token "on clusters" cluster-urls)
  (reduce
    (fn [cluster-sync-result cluster-url]
      (->> (try
             (let [{:keys [headers status] :as response}
                   (waiter/hard-delete-token http-client cluster-url token token-etag)]
               {:code (if (successful? response)
                        :success/hard-delete
                        :error/hard-delete)
                :details (cond-> {:status status}
                                 (successful? response) (assoc :etag (get headers "etag")))})
             (catch Exception ex
               (log/error ex "unable to delete" token "on" cluster-url)
               {:code :error/hard-delete
                :details {:message (.getMessage ex)}}))
           (assoc cluster-sync-result cluster-url)))
    {}
    cluster-urls))

(defn sync-token-on-clusters
  "Syncs a given token description on all clusters.
   If the cluster-url->token-data says that a given token was not successfully loaded, it is skipped.
   Token sync-ing is also skipped if the owners of the tokens are different."
  [^HttpClient http-client cluster-urls token token-description cluster-url->token-data]
  (pc/map-from-keys
    (fn [cluster-url]
      (let [cluster-result
            (try
              (let [{:keys [description error token-etag status]} (get cluster-url->token-data cluster-url)
                    latest-owner (get token-description "owner")
                    cluster-owner (get description "owner")]
                (cond
                  error
                  {:code :error/token-read
                   :details {:message (.getMessage error)}}

                  (nil? status)
                  {:code :error/token-read
                   :details {:message "status missing from response"}}

                  (not= latest-owner cluster-owner)
                  {:code :error/owner-different
                   :details {:cluster description
                             :latest token-description}}

                  (not= token-description (get-in cluster-url->token-data [cluster-url :description]))
                  (let [{:keys [headers status] :as response}
                        (waiter/store-token http-client cluster-url token token-etag token-description)]
                    {:code (if (get token-description "deleted")
                             (if (successful? response) :success/soft-delete :error/soft-delete)
                             (if (successful? response) :success/sync-update :error/sync-update))
                     :details (cond-> {:status status}
                                      (successful? response) (assoc :etag (get headers "etag")))})

                  :else
                  {:code :success/token-match}))
              (catch Exception ex
                (log/error ex "unable to sync token on" cluster-url)
                {:code :error/token-sync
                 :details {:message (.getMessage ex)}}))]
        (log/info cluster-url "sync result is" cluster-result)
        cluster-result))
    cluster-urls))

(defn- perform-token-syncs
  "Perform token syncs for all the specified tokens."
  [http-client cluster-urls all-tokens]
  (let [token->url->token-data (retrieve-token->url->token-data http-client cluster-urls all-tokens)
        token->latest-description (retrieve-token->latest-description token->url->token-data)]
    (pc/map-from-keys
      (fn [token]
        (log/info "syncing token:" token)
        (let [{:keys [cluster-url description]} (token->latest-description token)
              token-etag (get-in token->url->token-data [token cluster-url :token-etag])
              remaining-cluster-urls (disj cluster-urls cluster-url)
              all-tokens-match (every? (fn all-tokens-match-pred [cluster-url]
                                         (= description (get-in token->url->token-data
                                                                [token cluster-url :description])))
                                       remaining-cluster-urls)
              all-soft-deleted (every? (fn soft-delete-pred [[_ token-data]]
                                         (get-in token-data [:description "deleted"]))
                                       (token->url->token-data token))]
          (log/info "syncing" token "with token description from" cluster-url
                    {:all-soft-deleted all-soft-deleted, :all-tokens-match all-tokens-match})
          (let [sync-result (if (and all-tokens-match all-soft-deleted description)
                              (hard-delete-token-on-all-clusters http-client cluster-urls token token-etag)
                              (sync-token-on-clusters http-client remaining-cluster-urls token description
                                                      (token->url->token-data token)))]
            {:latest (token->latest-description token)
             :sync-result sync-result})))
      all-tokens)))

(defn summarize-sync-result
  "Summarizes the token sync result"
  [token-sync-result]
  (let [filter-tokens (fn [filter-fn]
                        (->> token-sync-result
                             keys
                             (filter
                               (fn [token]
                                 (every? filter-fn (-> token-sync-result (get token) :sync-result vals))))
                             set))
        unmodified-filter-fn (fn [result] (-> result :code (= :success/token-match)))
        unmodified-tokens (filter-tokens unmodified-filter-fn)
        updated-filter-fn (fn [result] (-> result :code namespace (= "success")))
        updated-tokens (-> (filter-tokens updated-filter-fn)
                           (set/difference unmodified-tokens))
        failed-tokens (-> token-sync-result keys set (set/difference unmodified-tokens updated-tokens))]
    {:sync {:failed failed-tokens
            :unmodified unmodified-tokens
            :updated updated-tokens}
     :tokens {:processed (count token-sync-result)}}))

(defn sync-tokens
  "Syncs tokens across provided clusters based on cluster-urls."
  [http-client cluster-urls]
  (try
    (log/info "syncing tokens on clusters:" (set cluster-urls))
    (let [cluster-urls (set cluster-urls)
          cluster-url->tokens (pc/map-from-keys #(waiter/load-token-list http-client %) cluster-urls)
          all-tokens (set (mapcat identity (vals cluster-url->tokens)))
          _ (log/info "found" (count all-tokens) "across the clusters")
          token-sync-result (perform-token-syncs http-client cluster-urls all-tokens)]
      (log/info "completed syncing tokens")
      {:details token-sync-result
       :summary (-> (summarize-sync-result token-sync-result)
                    (assoc-in [:tokens :total] (count all-tokens)))})
    (catch Throwable th
      (log/error th "unable to sync tokens")
      (throw th))))
