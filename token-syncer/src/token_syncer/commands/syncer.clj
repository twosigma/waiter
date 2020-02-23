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
(ns token-syncer.commands.syncer
  (:require [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [token-syncer.utils :as utils]))

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
                :token-etag (:token-etag token-data)}))))
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

(def admin-no-sync-user
  "Reserved value for last-update-user used to indicate a no-sync admin update.
   Value is nil if this feature is not enabled."
  (System/getenv "WAITER_TOKEN_ADMIN_NO_SYNC_USER"))

(defn sync-token-on-clusters
  "Syncs a given token description on all clusters.
   If the cluster-url->token-data says that a given token was not successfully loaded, it is skipped.
   Token sync-ing is also skipped if the tokens are active and the roots are different."
  [{:keys [store-token]} cluster-urls token latest-token-description cluster-url->token-data]
  (pc/map-from-keys
    (fn [cluster-url]
      (let [ignored-root-mismatch-equality-comparison-keys ["cluster" "last-update-time" "last-update-user" "previous" "root"]
            system-metadata-keys (conj ignored-root-mismatch-equality-comparison-keys "deleted")
            cluster-result
            (try
              (let [{:keys [description error status] :as token-data} (get cluster-url->token-data cluster-url)
                    {latest-root "root" latest-update-user "last-update-user"} latest-token-description
                    {cluster-root "root" cluster-update-user "last-update-user"} description]
                (cond
                  error
                  {:code :error/token-read
                   :details {:message (.getMessage error)}}

                  (nil? status)
                  {:code :error/token-read
                   :details {:message "status missing from response"}}

                  (nil? latest-root)
                  {:code :error/token-read
                   :details {:message "token root missing from latest token description"}}

                  (and latest-root
                       (= latest-token-description description))
                  {:code :success/token-match}

                  ;; deleted on both clusters, irrespective of remaining values
                  (and (get latest-token-description "deleted")
                       (get description "deleted"))
                  {:code :error/tokens-deleted
                   :details {:message "soft-deleted tokens should have already been hard-deleted"}}

                  ;; active token, content the same irrespective of system metadata keys but roots different
                  (and (seq description)
                       (not= latest-root cluster-root)
                       (not (get latest-token-description "deleted"))
                       (not (get description "deleted"))
                       (= (apply dissoc latest-token-description system-metadata-keys)
                          (apply dissoc description system-metadata-keys)))
                  {:code :skip/token-sync}

                  ;; token user-specified content, last update user, and root different
                  (and (seq description)
                       (not= latest-root cluster-root)
                       (not= latest-update-user cluster-update-user)
                       (not= (apply dissoc description ignored-root-mismatch-equality-comparison-keys)
                             (apply dissoc latest-token-description ignored-root-mismatch-equality-comparison-keys)))
                  {:code :error/root-mismatch
                   :details {:cluster description
                             :latest latest-token-description}}

                  ;; token explicitly marked for no-sync by admin-mode operation
                  (and admin-no-sync-user
                       (or (= admin-no-sync-user latest-update-user)
                           (= admin-no-sync-user cluster-update-user)))
                  {:code :skip/token-sync
                   :details "skipping token excluded from sync by admin"}

                  (not= latest-token-description description)
                  (let [token-etag (:token-etag token-data)
                        {:keys [headers status] :as response} (store-token cluster-url token token-etag latest-token-description)]
                    {:code (if (get latest-token-description "deleted")
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
  [waiter-api cluster-urls all-tokens]
  (let [token->url->token-data (retrieve-token->url->token-data waiter-api cluster-urls all-tokens)
        token->latest-description (retrieve-token->latest-description token->url->token-data)]
    (pc/map-from-keys
      (fn [token]
        (log/info "syncing token:" token)
        (let [{:keys [cluster-url description]} (token->latest-description token)
              token-etag (get-in token->url->token-data [token cluster-url :token-etag])
              remaining-cluster-urls (disj cluster-urls cluster-url)
              all-soft-deleted (every? (fn soft-delete-pred [[_ token-data]]
                                         (get-in token-data [:description "deleted"]))
                                       (token->url->token-data token))]
          (log/info "syncing" token "with token description from" cluster-url {:all-soft-deleted all-soft-deleted})
          (let [sync-result (if all-soft-deleted
                              (hard-delete-token-on-all-clusters waiter-api cluster-urls token token-etag)
                              (sync-token-on-clusters waiter-api remaining-cluster-urls token description
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
                                 (if already-synced?
                                   (log/info token "already synced across clusters with etag" (first cluster-etags))
                                   (log/info token "not synced across clusters" {:deleted cluster-deleted :etags cluster-etags}))
                                 already-synced?)))
                           set)]
    (log/info "found" (count all-tokens) "across the clusters," (count synced-tokens) "previously synced")
    {:all-tokens all-tokens
     :pending-tokens (set/difference all-tokens synced-tokens)
     :synced-tokens synced-tokens}))

(defn summarize-sync-result
  "Summarizes the token sync result.
   The summary includes the tokens that were unmodified, successfully synced, and failed to sync.
   Tokens that were already synced show up in previously synced entry of the summary.
   The summary also includes counts of the total number of tokens that were processed."
  [{:keys [all-tokens already-synced-tokens pending-tokens selected-tokens]} token-sync-result]
  (let [filter-tokens (fn [filter-fn]
                        (->> token-sync-result
                             keys
                             (filter
                               (fn [token]
                                 (every? filter-fn (-> token-sync-result (get token) :sync-result vals))))
                             (into (sorted-set))))
        unmodified-filter-fn (fn [result] (-> result :code (= :success/token-match)))
        unmodified-tokens (filter-tokens unmodified-filter-fn)
        updated-filter-fn (fn [result] (-> result :code namespace (= "success")))
        updated-tokens (into (sorted-set)
                             (-> (filter-tokens updated-filter-fn)
                                 (set/difference unmodified-tokens)))
        failed-tokens (into (sorted-set)
                            (-> token-sync-result
                                keys
                                set
                                (set/difference unmodified-tokens updated-tokens)))
        tokens->value-count (fn [tokens] {:count (count tokens)
                                          :value (->> tokens set (into (sorted-set)))})]
    {:sync {:failed failed-tokens
            :unmodified unmodified-tokens
            :updated updated-tokens}
     :tokens {:pending (tokens->value-count pending-tokens)
              :previously-synced (tokens->value-count already-synced-tokens)
              :processed (tokens->value-count (keys token-sync-result))
              :selected (tokens->value-count selected-tokens)
              :total (tokens->value-count all-tokens)}}))

(defn sync-tokens
  "Syncs tokens across provided clusters based on cluster-urls and returns the result of token syncing.
   Throws an exception if there was an error during token syncing."
  [{:keys [load-token-list] :as waiter-api} cluster-urls limit]
  (try
    (log/info "syncing tokens on clusters:" cluster-urls)
    (let [cluster-urls-set (set cluster-urls)
          {:keys [all-tokens pending-tokens synced-tokens]} (load-and-classify-tokens load-token-list cluster-urls-set)
          selected-tokens (->> (sort pending-tokens)
                               (take limit))
          token-sync-result (perform-token-syncs waiter-api cluster-urls-set selected-tokens)]
      (log/info "completed syncing tokens (limited to " (min limit (count pending-tokens)) "tokens)")
      {:details token-sync-result
       :summary (-> {:all-tokens all-tokens
                     :already-synced-tokens synced-tokens
                     :pending-tokens pending-tokens
                     :selected-tokens selected-tokens}
                    (summarize-sync-result token-sync-result))})
    (catch Throwable th
      (log/error th "unable to sync tokens")
      (throw th))))

(def sync-clusters-config
  {:execute-command (fn execute-sync-clusters-command
                      [{:keys [waiter-api]} {:keys [options]} arguments]
                      (let [{:keys [limit]} options
                            cluster-urls-set (set arguments)]
                        (cond
                          (<= (-> cluster-urls-set set count) 1)
                          {:exit-code 1
                           :message (str "at least two different cluster urls required, provided: " (vec arguments))}

                          :else
                          (let [sync-result (sync-tokens waiter-api cluster-urls-set limit)
                                exit-code (-> (get-in sync-result [:summary :sync :failed])
                                              empty?
                                              (if 0 1))]
                            (log/info (-> sync-result pp/pprint with-out-str str/trim))
                            {:exit-code exit-code
                             :message (str "exiting with code " exit-code)}))))
   :option-specs [["-l" "--limit LIMIT" "The maximum number of tokens to attempt to sync, must be between 1 and 10000"
                   :default 1000
                   :parse-fn #(Integer/parseInt %)
                   :validate [#(< 0 % 10001) "Must be between 1 and 10000"]]]
   :retrieve-documentation (fn retrieve-sync-clusters-documentation
                             [command-name _]
                             {:description (str "Syncs tokens across (at least two) Waiter clusters specified in the URL(s)")
                              :usage (str command-name " [OPTION]... URL URL...")})})
