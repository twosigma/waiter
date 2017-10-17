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
(ns token-syncer.syncer-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [token-syncer.syncer :refer :all]
            [token-syncer.waiter :as waiter]))

(deftest test-retrieve-token->url->token-data
  (let [http-client (Object.)
        test-cluster-urls ["www.cluster-1.com" "www.cluster-2.com" "www.cluster-3.com"]
        test-tokens ["token-1" "token-2" "token-3" "token-4" "token-5"]]
    (with-redefs [waiter/load-token (fn [_ cluster-url token] (str cluster-url ":" token))]
      (is (= (pc/map-from-keys
               (fn [token]
                 (pc/map-from-keys
                   (fn [cluster-url]
                     (str cluster-url ":" token))
                   test-cluster-urls))
               test-tokens)
             (retrieve-token->url->token-data http-client test-cluster-urls test-tokens))))))

(deftest test-retrieve-token->latest-description
  (testing "empty input"
    (is (= {} (retrieve-token->latest-description {}))))

  (testing "single empty entry"
    (let [token->cluster-url->token-data {"token-1" {}}]
      (is (= {"token-1" {}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "single entry"
    (let [token-data {:description {"last-update-time" 1234}
                      :token-etag "1234"}
          token->cluster-url->token-data {"token-1" {"cluster-1" token-data}}]
      (is (= {"token-1" {:cluster-url "cluster-1"
                         :description {"last-update-time" 1234}
                         :token-etag "1234"}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "single valid entry"
    (let [token-data {:description {"last-update-time" 1234}
                      :token-etag "1234"}
          token->cluster-url->token-data {"token-1" {"cluster-1" token-data
                                                     "cluster-2" {}}}]
      (is (= {"token-1" {:cluster-url "cluster-1"
                         :description {"last-update-time" 1234}
                         :token-etag "1234"}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "single valid entry"
    (let [token-data-1 {:description {"last-update-time" 1234}
                        :token-etag "1234"}
          token-data-2 {:description {"last-update-time" 4567}
                        :token-etag "4567"}
          token-data-3 {:description {"last-update-time" 1267}
                        :token-etag "1267"}
          token->cluster-url->token-data {"token-1" {"cluster-1" token-data-1
                                                     "cluster-2" token-data-2
                                                     "cluster-3" token-data-3}}]
      (is (= {"token-1" {:cluster-url "cluster-2"
                         :description {"last-update-time" 4567}
                         :token-etag "4567"}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "multiple-tokens:different times"
    (let [token-data-1 {:description {"last-update-time" 1234}
                        :token-etag "1234"}
          token-data-2 {:description {"last-update-time" 4567}
                        :token-etag "4567"}
          token-data-3 {:description {"last-update-time" 1267}
                        :token-etag "1267"}
          token->cluster-url->token-data {"token-A2" {"cluster-1" token-data-1
                                                      "cluster-2" token-data-2
                                                      "cluster-3" token-data-3}
                                          "token-B" {}
                                          "token-C1" {"cluster-1" token-data-2
                                                      "cluster-2" token-data-1
                                                      "cluster-3" token-data-3}
                                          "token-D3" {"cluster-1" token-data-3
                                                      "cluster-2" token-data-2
                                                      "cluster-3" token-data-1}}]
      (is (= {"token-A2" {:cluster-url "cluster-2"
                          :description {"last-update-time" 4567}
                          :token-etag "4567"}
              "token-B" {}
              "token-C1" {:cluster-url "cluster-1"
                          :description {"last-update-time" 4567}
                          :token-etag "4567"}
              "token-D3" {:cluster-url "cluster-2"
                          :description {"last-update-time" 4567}
                          :token-etag "4567"}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "multiple-tokens:same times"
    (let [token-data-1 {:description {"last-update-time" 1234}
                        :token-etag "1234"}
          token-data-2 {:description {"last-update-time" 1234}
                        :token-etag "1234"}
          token-data-3 {:description {"last-update-time" 1234}
                        :token-etag "1234"}
          token->cluster-url->token-data {"token-A2" {"cluster-1" token-data-1
                                                      "cluster-2" token-data-2
                                                      "cluster-3" token-data-3}
                                          "token-B" {}
                                          "token-C1" {"cluster-1" token-data-2
                                                      "cluster-2" token-data-1
                                                      "cluster-3" token-data-3}
                                          "token-D3" {"cluster-1" token-data-3
                                                      "cluster-2" token-data-2
                                                      "cluster-3" token-data-1}}]
      (is (= {"token-A2" {:cluster-url "cluster-1"
                          :description {"last-update-time" 1234}
                          :token-etag "1234"}
              "token-B" {}
              "token-C1" {:cluster-url "cluster-1"
                          :description {"last-update-time" 1234}
                          :token-etag "1234"}
              "token-D3" {:cluster-url "cluster-1"
                          :description {"last-update-time" 1234}
                          :token-etag "1234"}}
             (retrieve-token->latest-description token->cluster-url->token-data))))))

(deftest test-hard-delete-token-on-all-clusters
  (let [http-client (Object.)
        test-cluster-urls ["www.cluster-1.com" "www.cluster-2-error.com" "www.cluster-3.com"]
        test-token "token-1"
        test-token-etag (System/currentTimeMillis)]
    (with-redefs [waiter/hard-delete-token (fn [_ cluster-url token in-token-etag]
                                             (is (= test-token-etag in-token-etag))
                                             (if (str/includes? cluster-url "error")
                                               (throw (Exception. "Error in hard-delete thrown from test"))
                                               {:body (str cluster-url ":" token)
                                                :headers {"etag" in-token-etag}
                                                :status 200}))]
      (is (= {"www.cluster-1.com" {:code :success/hard-delete
                                   :details {:etag test-token-etag
                                             :status 200}}
              "www.cluster-2-error.com" {:code :error/hard-delete
                                         :details {:message "Error in hard-delete thrown from test"}}
              "www.cluster-3.com" {:code :success/hard-delete
                                   :details {:etag test-token-etag
                                             :status 200}}}
             (hard-delete-token-on-all-clusters http-client test-cluster-urls test-token test-token-etag))))))

(deftest test-sync-token-on-clusters
  (let [http-client (Object.)
        test-token-etag (System/currentTimeMillis)]
    (with-redefs [waiter/store-token (fn [_ cluster-url token token-etag token-description]
                                       (cond
                                         (str/includes? cluster-url "cluster-1")
                                         (is (= (str test-token-etag ".1") token-etag))
                                         (str/includes? cluster-url "cluster-2")
                                         (is (= (str test-token-etag ".2") token-etag)))

                                       (if (str/includes? cluster-url "error")
                                         (throw (Exception. "Error in storing token thrown from test"))
                                         {:body (-> token-description
                                                    walk/keywordize-keys
                                                    (assoc :cluster-url cluster-url :token token))
                                          :headers {"etag" (str token-etag ".new")}
                                          :status 200}))]

      (testing "sync cluster error while loading token"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                          "owner" "test-user-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2.com" {:error (Exception. "cluster-2 data cannot be loaded")}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2.com" {:code :error/token-read
                                       :details {:message "cluster-2 data cannot be loaded"}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster error while missing status"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                          "owner" "test-user-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2.com" {}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2.com" {:code :error/token-read
                                       :details {:message "status missing from response"}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster different owners"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                          "owner" "test-user-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2.com" {:description {"owner" "test-user-2"}
                                                            :token-etag (str test-token-etag ".2")
                                                            :status 200}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2.com" {:code :error/owner-different
                                       :details {:cluster {"owner" "test-user-2"}
                                                 :latest token-description}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster with missing owners"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2.com" {:description {"name" "test-name-2"}
                                                            :token-etag (str test-token-etag ".2")
                                                            :status 200}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2.com" {:code :success/sync-update
                                       :details {:etag (str test-token-etag ".2.new")
                                                 :status 200}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster with outdated missing owner"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name-1"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name-1"
                                                                          "owner" "test-user-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2.com" {:description {"name" "test-name-2"}
                                                            :token-etag (str test-token-etag ".2")
                                                            :status 200}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2.com" {:code :error/owner-different
                                       :details {:cluster {"name" "test-name-2"}
                                                 :latest {"name" "test-name-1"
                                                          "owner" "test-user-1"}}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster with latest missing owner"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2.com" {:description {"name" "test-name-2"
                                                                          "owner" "test-user-2"}
                                                            :token-etag (str test-token-etag ".2")
                                                            :status 200}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2.com" {:code :error/owner-different
                                       :details {:cluster {"name" "test-name-2"
                                                           "owner" "test-user-2"}
                                                 :latest {"name" "test-name-1"}}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster successfully"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                          "owner" "test-user-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2.com" {:description {"owner" "test-user-1"}
                                                            :token-etag (str test-token-etag ".2")
                                                            :status 200}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2.com" {:code :success/sync-update
                                       :details {:etag (str test-token-etag ".2.new")
                                                 :status 200}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster error"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2-error.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                          "owner" "test-user-1"}
                                                            :token-etag (str test-token-etag ".1")
                                                            :status 200}
                                       "www.cluster-2-error.com" {:description {"owner" "test-user-1"}
                                                                  :token-etag (str test-token-etag ".2")
                                                                  :status 200}}]
          (is (= {"www.cluster-1.com" {:code :success/token-match}
                  "www.cluster-2-error.com" {:code :error/token-sync
                                             :details {:message "Error in storing token thrown from test"}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data))))))))

(deftest test-summarize-sync-result
  (is (= {:sync {:error ["token-1B" "token-4B"]
                 :success ["token-1A" "token-2" "token-3" "token-4A"]}
          :tokens {:processed 6}}
         (summarize-sync-result
           {"token-1A" {:sync-result {"www.cluster-2.com" {:code :success/token-match}
                                      "www.cluster-3.com" {:code :success/token-match}}}
            "token-1B" {:sync-result {"www.cluster-2.com" {:code :success/token-match}
                                      "www.cluster-3.com" {:code :error/token-sync}}}
            "token-2" {:sync-result {"www.cluster-2.com" {:code :success/sync-update}
                                     "www.cluster-3.com" {:code :success/sync-update}}}
            "token-3" {:sync-result {"www.cluster-1.com" {:code :success/soft-delete}
                                     "www.cluster-3.com" {:code :success/soft-delete}}}
            "token-4A" {:sync-result {"www.cluster-1.com" {:code :success/hard-delete}
                                      "www.cluster-2.com" {:code :success/hard-delete}
                                      "www.cluster-3.com" {:code :success/hard-delete}}}
            "token-4B" {:sync-result {"www.cluster-1.com" {:code :success/hard-delete}
                                      "www.cluster-2.com" {:code :error/hard-delete}
                                      "www.cluster-3.com" {:code :success/hard-delete}}}}))))

(deftest test-sync-tokens
  (let [http-client (Object.)
        current-time-ms (System/currentTimeMillis)
        previous-time-ms (- current-time-ms 10101010)
        cluster-1 "www.cluster-1.com"
        cluster-2 "www.cluster-2.com"
        cluster-3 "www.cluster-3.com"
        cluster-urls [cluster-1 cluster-2 cluster-3]
        cluster-url->tokens {cluster-1 ["token-1" "token-2"]
                             cluster-2 ["token-1" "token-2" "token-3" "token-4"]
                             cluster-3 ["token-1" "token-3"]}
        token-desc-1 {"last-update-time" current-time-ms
                      "name" "t1-all-synced"}
        token-desc-2 {"last-update-time" current-time-ms
                      "name" "t2-needs-sync"}
        token-desc-3 {"deleted" true
                      "last-update-time" current-time-ms
                      "name" "t3-soft-delete"}
        token-desc-4 {"deleted" true
                      "last-update-time" current-time-ms
                      "name" "t4-hard-delete"}
        token->cluster-url->token-data {"token-1" {cluster-1 {:description token-desc-1
                                                              :token-etag current-time-ms}
                                                   cluster-2 {:description token-desc-1
                                                              :token-etag current-time-ms}
                                                   cluster-3 {:description token-desc-1
                                                              :token-etag current-time-ms}}
                                        "token-2" {cluster-1 {:description token-desc-2
                                                              :token-etag current-time-ms}
                                                   cluster-2 {:description (assoc token-desc-2 "cmd" "test")
                                                              :token-etag previous-time-ms}
                                                   cluster-3 {:description token-desc-2
                                                              :token-etag current-time-ms}}
                                        "token-3" {cluster-1 {:description (dissoc token-desc-3 "deleted")
                                                              :token-etag previous-time-ms}
                                                   cluster-2 {:description token-desc-3
                                                              :token-etag current-time-ms}
                                                   cluster-3 {:description token-desc-3
                                                              :token-etag current-time-ms}}
                                        "token-4" {cluster-1 {:description token-desc-4
                                                              :token-etag current-time-ms}
                                                   cluster-2 {:description token-desc-4
                                                              :token-etag current-time-ms}
                                                   cluster-3 {:description token-desc-4
                                                              :token-etag current-time-ms}}}
        token->latest-description {"token-1" {:cluster-url cluster-1
                                              :description token-desc-1
                                              :token-etag (str (get token-desc-1 "last-update-time"))}
                                   "token-2" {:cluster-url cluster-1
                                              :description token-desc-2
                                              :token-etag (str (get token-desc-2 "last-update-time"))}
                                   "token-3" {:cluster-url cluster-2
                                              :description token-desc-3
                                              :token-etag (str (get token-desc-3 "last-update-time"))}
                                   "token-4" {:cluster-url cluster-1
                                              :description token-desc-4
                                              :token-etag (str (get token-desc-4 "last-update-time"))}}
        compute-sync-result (fn [cluster-urls token code]
                              (pc/map-from-keys
                                (fn [cluster-url]
                                  {:code code
                                   :details {:etag (str token "." cluster-url "." (count cluster-urls))
                                             :status 200}})
                                cluster-urls))]
    (with-redefs [waiter/load-token-list (fn [_ cluster-url]
                                           (cluster-url->tokens cluster-url))
                  retrieve-token->url->token-data (fn [_ in-cluster-urls all-tokens]
                                                    (is (= (set cluster-urls) in-cluster-urls))
                                                    (is (= #{"token-1" "token-2" "token-3" "token-4"}
                                                           (set all-tokens)))
                                                    token->cluster-url->token-data)
                  retrieve-token->latest-description (fn [in-token->cluster-url->token-data]
                                                       (is (= token->cluster-url->token-data in-token->cluster-url->token-data))
                                                       token->latest-description)
                  hard-delete-token-on-all-clusters (fn [_ cluster-urls token _]
                                                      (compute-sync-result cluster-urls token :success/hard-delete))
                  sync-token-on-clusters (fn [_ cluster-urls token description _]
                                           (let [token-name (str (get description "name"))
                                                 code (cond
                                                        (str/includes? token-name "all-synced") :success/token-match
                                                        (str/includes? token-name "needs-sync") :success/sync-update
                                                        (str/includes? token-name "soft-delete") :success/soft-delete
                                                        :else :error/token-sync)]
                                             (compute-sync-result cluster-urls token code)))]
      (is (= {:details {"token-1" {:latest (token->latest-description "token-1")
                                   :sync-result (-> [cluster-2 cluster-3]
                                                    (compute-sync-result "token-1" :success/token-match))}
                        "token-2" {:latest (token->latest-description "token-2")
                                   :sync-result (-> [cluster-2 cluster-3]
                                                    (compute-sync-result "token-2" :success/sync-update))}
                        "token-3" {:latest (token->latest-description "token-3")
                                   :sync-result (-> [cluster-1 cluster-3]
                                                    (compute-sync-result "token-3" :success/soft-delete))}
                        "token-4" {:latest (token->latest-description "token-4")
                                   :sync-result (-> [cluster-1 cluster-2 cluster-3]
                                                    (compute-sync-result "token-4" :success/hard-delete))}}
              :summary {:sync {:error []
                               :success ["token-1" "token-2" "token-3" "token-4"]}
                        :tokens {:processed 4
                                 :total 4}}}
             (sync-tokens http-client cluster-urls))))))
