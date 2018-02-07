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
            [token-syncer.utils :as utils]))

;; The default etag for new tokens while running in admin mode
(def ^:const default-etag "E-0")

(defn retrieve-token->url->token-data
  "Given collections of cluster urls and tokens, retrieve the token description on each cluster.
   The resulting data structure is a map as follows: token->cluster->token-data where the
   token-data format is defined by the return value of load-token."
  [{:keys [load-token]} cluster-urls all-tokens]
  (pc/map-from-keys
    (fn [token]
      (pc/map-from-keys
        (fn [cluster-url]
          (load-token cluster-url token))
        cluster-urls))
    all-tokens))

(defn retrieve-token->latest-description
  "Given token->cluster-url->token-data, retrieves the latest token description (based on last-update-time)
   for each token among all the clusters. The cluster urls are sorted to ensure deterministic outputs."
  [token->cluster-url->token-data]
  (->> token->cluster-url->token-data
       (pc/map-vals
         (fn [cluster-url->token-data]
           (when (seq cluster-url->token-data)
             (let [[cluster-url token-data]
                   (apply max-key #(-> % val (get-in [:description "last-update-time"] 0))
                          (-> cluster-url->token-data sort reverse))]
               {:cluster-url cluster-url
                :description (:description token-data)
                :token-etag (:token-etag token-data default-etag)}))))
       (pc/map-vals (fnil identity {}))))

(defn hard-delete-token-on-all-clusters
  "Hard-deletes a given token on all clusters."
  [{:keys [hard-delete-token]} cluster-urls token token-etag]
  (log/info "hard-delete" token "on clusters" cluster-urls)
  (reduce
    (fn [cluster-sync-result cluster-url]
      (->> (try
             (let [{:keys [headers status] :as response} (hard-delete-token cluster-url token token-etag)]
               {:code (if (utils/successful? response)
                        :success/hard-delete
                        :error/hard-delete)
                :details (cond-> {:status status}
                                 (utils/successful? response) (assoc :etag (get headers "etag")))})
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
  [{:keys [store-token]} cluster-urls token token-description cluster-url->token-data]
  (pc/map-from-keys
    (fn [cluster-url]
      (let [cluster-result
            (try
              (let [{:keys [description error status] :as token-data} (get cluster-url->token-data cluster-url)
                    latest-root (get token-description "root")
                    cluster-root (get description "root")]
                (cond
                  error
                  {:code :error/token-read
                   :details {:message (.getMessage error)}}

                  (nil? status)
                  {:code :error/token-read
                   :details {:message "status missing from response"}}

                  (and (seq description) (not= latest-root cluster-root))
                  {:code :error/root-mismatch
                   :details {:cluster description
                             :latest token-description}}

                  (not= token-description (get-in cluster-url->token-data [cluster-url :description]))
                  (let [token-etag (:token-etag token-data default-etag)
                        {:keys [headers status] :as response} (store-token cluster-url token token-etag token-description)]
                    {:code (if (get token-description "deleted")
                             (if (utils/successful? response) :success/soft-delete :error/soft-delete)
                             (if (utils/successful? response) :success/sync-update :error/sync-update))
                     :details (cond-> {:status status}
                                      (utils/successful? response) (assoc :etag (get headers "etag")))})

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
  [waiter-functions cluster-urls all-tokens]
  (let [token->url->token-data (retrieve-token->url->token-data waiter-functions cluster-urls all-tokens)
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
                              (hard-delete-token-on-all-clusters waiter-functions cluster-urls token token-etag)
                              (sync-token-on-clusters waiter-functions remaining-cluster-urls token description
                                                      (token->url->token-data token)))]
            {:latest (token->latest-description token)
             :sync-result sync-result})))
      all-tokens)))

(defn load-and-classify-tokens
  "Retrieves a summary of all tokens available in the clusters as a map containing the following keys:
     :all-tokens, :pending-tokens and :synced-tokens.
   Synced tokens have the same non-nil etag value and deleted=false from load-token-list on each cluster.
   Pending tokens are un-synced tokens present in some cluster."
  [load-token-list cluster-urls-set]
  (let [cluster-url->index-entries (pc/map-from-keys #(load-token-list %) cluster-urls-set)
        cluster-url->token->index-entries (pc/map-vals
                                            (fn [index-entries]
                                              (pc/map-from-vals #(get % "token") index-entries))
                                            cluster-url->index-entries)
        all-tokens (->> cluster-url->index-entries
                        vals
                        (mapcat identity)
                        (map #(get % "token"))
                        (remove nil?)
                        set)
        synced-tokens (->> all-tokens
                           (filter
                             (fn [token]
                               (let [cluster-data (map
                                                     (fn [cluster-url]
                                                       (-> cluster-url->token->index-entries
                                                           (get cluster-url)
                                                           (get token)
                                                           (select-keys ["deleted" "etag"])))
                                                     cluster-urls-set)
                                     cluster-deleted (map #(get % "deleted") cluster-data)
                                     cluster-etags (map #(get % "etag") cluster-data)
                                     already-synced? (and (every? false? cluster-deleted)
                                                          (not-any? nil? cluster-etags)
                                                          (= 1 (-> cluster-etags set count)))]
                                 (when already-synced?
                                   (log/info token "has already been synced across clusters at time" (first cluster-etags)))
                                 already-synced?)))
                           set)]
    (log/info "found" (count all-tokens) "across the clusters," (count synced-tokens) "previously synced")
    {:all-tokens all-tokens
     :pending-tokens (set/difference all-tokens synced-tokens)
     :synced-tokens synced-tokens}))

(defn summarize-sync-result
  "Summarizes the token sync result.
   The summary includes the tokens that were unmodified, successfully synced, and failed to sync.
   It also includes counts of the total number of tokens that were processed."
  [token-sync-result synced-tokens]
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
            :previously-synced (set synced-tokens)
            :unmodified unmodified-tokens
            :updated updated-tokens}
     :tokens {:num-previously-synced (count synced-tokens)
              :num-processed (count token-sync-result)}}))

(defn sync-tokens
  "Syncs tokens across provided clusters based on cluster-urls and returns the result of token syncing.
   Throws an exception if there was an error during token syncing."
  [{:keys [load-token-list] :as waiter-functions} cluster-urls]
  (try
    (log/info "syncing tokens on clusters:" cluster-urls)
    (let [cluster-urls-set (set cluster-urls)
          {:keys [all-tokens pending-tokens synced-tokens]} (load-and-classify-tokens load-token-list cluster-urls-set)
          token-sync-result (perform-token-syncs waiter-functions cluster-urls-set pending-tokens)]
      (log/info "completed syncing tokens")
      {:details token-sync-result
       :summary (-> (summarize-sync-result token-sync-result synced-tokens)
                    (assoc-in [:tokens :total] (count all-tokens)))})
    (catch Throwable th
      (log/error th "unable to sync tokens")
      (throw th))))
