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
(ns token-syncer.commands.syncer-test
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.cli :as tools-cli]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [token-syncer.commands.syncer :refer :all]
            [token-syncer.cli :as cli]))

(deftest test-retrieve-token->url->token-data
  (let [test-cluster-urls ["www.cluster-1.com" "www.cluster-2.com" "www.cluster-3.com"]
        test-tokens ["token-1" "token-2" "token-3" "token-4" "token-5"]
        waiter-api {:load-token (fn [cluster-url token] (str cluster-url ":" token))}]
    (is (= (pc/map-from-keys
             (fn [token]
               (pc/map-from-keys
                 (fn [cluster-url]
                   (str cluster-url ":" token))
                 test-cluster-urls))
             test-tokens)
           (retrieve-token->url->token-data waiter-api test-cluster-urls test-tokens)))))

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
  (let [test-cluster-urls ["www.cluster-1.com" "www.cluster-2-error.com" "www.cluster-3.com"]
        test-token "token-1"
        test-token-etag (System/currentTimeMillis)
        waiter-api {:hard-delete-token (fn [cluster-url token in-token-etag]
                                         (is (= test-token-etag in-token-etag))
                                         (if (str/includes? cluster-url "error")
                                           (throw (Exception. "Error in hard-delete thrown from test"))
                                           {:body (str cluster-url ":" token)
                                            :headers {"etag" in-token-etag}
                                            :status 200}))}]
    (is (= {"www.cluster-1.com" {:code :success/hard-delete
                                 :details {:etag test-token-etag
                                           :status 200}}
            "www.cluster-2-error.com" {:code :error/hard-delete
                                       :details {:message "Error in hard-delete thrown from test"}}
            "www.cluster-3.com" {:code :success/hard-delete
                                 :details {:etag test-token-etag
                                           :status 200}}}
           (hard-delete-token-on-all-clusters waiter-api test-cluster-urls test-token test-token-etag)))))

(deftest test-sync-token-on-clusters
  (let [test-token-etag (System/currentTimeMillis)
        waiter-api {:store-token (fn [cluster-url token token-etag token-description]
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
                                      :status 200}))}]

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
        (is (= {"www.cluster-1.com" {:code :error/token-read
                                     :details {:message "token root missing from latest token description"}}
                "www.cluster-2.com" {:code :error/token-read
                                     :details {:message "cluster-2 data cannot be loaded"}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

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
        (is (= {"www.cluster-1.com" {:code :error/token-read
                                     :details {:message "token root missing from latest token description"}}
                "www.cluster-2.com" {:code :error/token-read
                                     :details {:message "status missing from response"}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster different owners but same root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"name" "test-name"
                               "owner" "test-user-1"
                               "root" "cluster-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description token-description
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description {"owner" "test-user-2"
                                                                        "root" "cluster-1"}
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :success/sync-update
                                     :details {:etag (str test-token-etag ".2.new")
                                               :status 200}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster different owners and different root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "name" "test-name"
                               "owner" "test-user-1"
                               "root" "cluster-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description token-description
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description {"owner" "test-user-2"
                                                                        "root" "cluster-2"}
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :error/root-mismatch
                                     :details {:cluster {"owner" "test-user-2", "root" "cluster-2"}
                                               :latest token-description}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster same parameters and owner but different root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"cpus" 1
                               "last-update-user" "john.doe"
                               "mem" 1024
                               "name" "test-name"
                               "owner" "test-user-1"}
            token-description-1 (assoc token-description "root" "cluster-1")
            token-description-2 (assoc token-description "root" "cluster-2")
            cluster-url->token-data {"www.cluster-1.com" {:description token-description-1
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description token-description-2
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :skip/token-sync}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description-1 cluster-url->token-data)))))

    (testing "skip token excluded from sync by admin"
      (let [skip-sync-user "admin-skip-sync"
            cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description-1 {"cpus" 1
                                 "last-update-user" "john.doe"
                                 "mem" 1024
                                 "name" "test-name"
                                 "owner" "test-user-1"
                                 "root" "cluster-1"}
            token-description-2 (assoc token-description-1
                                       "last-update-user" skip-sync-user
                                       "cpus" 10)
            cluster-url->token-data {"www.cluster-1.com" {:description token-description-1
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description token-description-2
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (with-redefs [admin-no-sync-user skip-sync-user]
          (is (= {"www.cluster-1.com" {:code :skip/token-sync
                                       :details "skipping token excluded from sync by admin"}
                  "www.cluster-2.com" {:code :success/token-match}}
                 (sync-token-on-clusters waiter-api cluster-urls test-token token-description-2 cluster-url->token-data))))))

    (testing "sync cluster same owner; soft-deleted on one; and different last-update-user and root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"cpus" 1
                               "last-update-user" "john.doe"
                               "mem" 1024
                               "name" "test-name"
                               "owner" "test-user-1"}
            token-description-1 (assoc token-description "deleted" true "root" "cluster-1")
            token-description-2 (assoc token-description "last-update-user" "jane.doe" "root" "cluster-2")
            cluster-url->token-data {"www.cluster-1.com" {:description token-description-1
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description token-description-2
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :error/root-mismatch
                                     :details {:cluster token-description-2
                                               :latest token-description-1}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description-1 cluster-url->token-data)))))

    (testing "sync cluster same last-update-user and owner; soft-deleted on one; and different root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"cpus" 1
                               "last-update-user" "john.doe"
                               "mem" 1024
                               "name" "test-name"
                               "owner" "test-user-1"}
            token-description-1 (assoc token-description "deleted" true "root" "cluster-1")
            token-description-2 (assoc token-description "root" "cluster-2")
            cluster-url->token-data {"www.cluster-1.com" {:description token-description-1
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description token-description-2
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :success/soft-delete
                                     :details {:etag (str test-token-etag ".2.new")
                                               :status 200}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description-1 cluster-url->token-data)))))

    (testing "sync cluster different root and parameters; deleted on both"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "mem" 1024
                               "name" "test-name"
                               "owner" "test-user-1"}
            token-description-1 (assoc token-description "cpus" 1 "deleted" true "root" "cluster-1")
            token-description-2 (assoc token-description "cpus" 2 "deleted" true "root" "cluster-2")
            cluster-url->token-data {"www.cluster-1.com" {:description token-description-1
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description token-description-2
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :error/tokens-deleted
                                     :details {:message "soft-deleted tokens should have already been hard-deleted"}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description-1 cluster-url->token-data)))))

    (testing "sync cluster same owner; different last-update-user, parameters and root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "mem" 1024
                               "name" "test-name"
                               "owner" "test-user-1"}
            token-description-1 (assoc token-description "cpus" 1 "root" "cluster-1")
            token-description-2 (assoc token-description "cpus" 2 "last-update-user" "jane.doe" "root" "cluster-2")
            cluster-url->token-data {"www.cluster-1.com" {:description token-description-1
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description token-description-2
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :error/root-mismatch
                                     :details {:cluster token-description-2
                                               :latest token-description-1}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description-1 cluster-url->token-data)))))

    (testing "sync cluster same last-update-user and owner; different parameters and root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "mem" 1024
                               "name" "test-name"
                               "owner" "test-user-1"}
            token-description-1 (assoc token-description "cpus" 1 "root" "cluster-1")
            token-description-2 (assoc token-description "cpus" 2 "root" "cluster-2")
            cluster-url->token-data {"www.cluster-1.com" {:description token-description-1
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description token-description-2
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :success/sync-update
                                     :details {:etag (str test-token-etag ".2.new")
                                               :status 200}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description-1 cluster-url->token-data)))))

    (testing "sync cluster with missing owners"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "name" "test-name-1"
                               "root" "test-cluster-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description {"last-update-user" "john.doe"
                                                                        "name" "test-name-1"
                                                                        "root" "test-cluster-1"}
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description {"last-update-user" "john.doe"
                                                                        "name" "test-name-2"
                                                                        "root" "test-cluster-1"}
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :success/sync-update
                                     :details {:etag (str test-token-etag ".2.new")
                                               :status 200}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster with outdated missing last-update-user and root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "name" "test-name-1"
                               "root" "test-user-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description {"last-update-user" "john.doe"
                                                                        "name" "test-name-1"
                                                                        "root" "test-user-1"}
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description {"name" "test-name-2"}
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :error/root-mismatch
                                     :details {:cluster {"name" "test-name-2"}
                                               :latest {"last-update-user" "john.doe"
                                                        "name" "test-name-1"
                                                        "root" "test-user-1"}}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster with outdated missing root; different last-update-user"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "name" "test-name-1"
                               "root" "test-user-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description {"last-update-user" "john.doe"
                                                                        "name" "test-name-1"
                                                                        "root" "test-user-1"}
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description {"last-update-user" "jane.doe"
                                                                        "name" "test-name-2"}
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :error/root-mismatch
                                     :details {:cluster {"last-update-user" "jane.doe"
                                                         "name" "test-name-2"}
                                               :latest {"last-update-user" "john.doe"
                                                        "name" "test-name-1"
                                                        "root" "test-user-1"}}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster with latest missing root"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"name" "test-name-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name-1"}
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description {"name" "test-name-2"
                                                                        "root" "test-user-2"}
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :error/token-read
                                     :details {:message "token root missing from latest token description"}}
                "www.cluster-2.com" {:code :error/token-read
                                     :details {:message "token root missing from latest token description"}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster successfully"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "name" "test-name"
                               "owner" "test-user-1"
                               "root" "test-cluster-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description {"last-update-user" "john.doe"
                                                                        "name" "test-name"
                                                                        "owner" "test-user-1"
                                                                        "root" "test-cluster-1"}
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2.com" {:description {"owner" "test-user-1"
                                                                        "root" "test-cluster-1"}
                                                          :token-etag (str test-token-etag ".2")
                                                          :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2.com" {:code :success/sync-update
                                     :details {:etag (str test-token-etag ".2.new")
                                               :status 200}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))

    (testing "sync cluster error"
      (let [cluster-urls ["www.cluster-1.com" "www.cluster-2-error.com"]
            test-token "test-token-1"
            token-description {"last-update-user" "john.doe"
                               "name" "test-name"
                               "owner" "test-user-1"
                               "root" "test-cluster-1"}
            cluster-url->token-data {"www.cluster-1.com" {:description {"last-update-user" "john.doe"
                                                                        "name" "test-name"
                                                                        "owner" "test-user-1"
                                                                        "root" "test-cluster-1"}
                                                          :token-etag (str test-token-etag ".1")
                                                          :status 200}
                                     "www.cluster-2-error.com" {:description {"owner" "test-user-1"
                                                                              "root" "test-cluster-1"}
                                                                :token-etag (str test-token-etag ".2")
                                                                :status 200}}]
        (is (= {"www.cluster-1.com" {:code :success/token-match}
                "www.cluster-2-error.com" {:code :error/token-sync
                                           :details {:message "Error in storing token thrown from test"}}}
               (sync-token-on-clusters waiter-api cluster-urls test-token token-description cluster-url->token-data)))))))

(deftest test-load-and-classify-tokens
  (let [current-time-ms (System/currentTimeMillis)
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
        load-token-list (fn [cluster-url]
                          (let [cluster-tokens (cluster-url->tokens cluster-url)]
                            (->> cluster-tokens
                                 (map (fn [token]
                                        (let [{:strs [last-update-time owner]}
                                              (get-in token->cluster-url->token-data [token cluster-url :description])]
                                          (cond-> {"token" token}
                                                  last-update-time (assoc "etag" (str "E" last-update-time))
                                                  owner (assoc "owner" owner)))))
                                 vec)))]
    (is (= {:all-tokens #{"token-1" "token-2" "token-3" "token-4"}
            :pending-tokens #{"token-2" "token-3" "token-4"},
            :synced-tokens #{"token-1"}})
        (load-and-classify-tokens load-token-list cluster-urls))))

(deftest test-summarize-sync-result
  (let [token-sync-result {"token-1A" {:sync-result {"www.cluster-2.com" {:code :success/token-match}
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
                                                     "www.cluster-3.com" {:code :success/hard-delete}}}}
        selected-tokens (-> token-sync-result keys set)
        unselected-tokens #{"token-6A" "token-6b"}
        pending-tokens (into unselected-tokens selected-tokens)
        already-synced-tokens #{"token-5"}
        all-tokens (set/union already-synced-tokens pending-tokens)]
    (is (= {:sync {:failed #{"token-1B" "token-4B"}
                   :unmodified #{"token-1A"}
                   :updated #{"token-2" "token-3" "token-4A"}}
            :tokens {:pending {:count (count pending-tokens)
                               :value pending-tokens}
                     :previously-synced {:count (count already-synced-tokens)
                                         :value already-synced-tokens}
                     :processed {:count (count selected-tokens)
                                 :value selected-tokens}
                     :selected {:count (count selected-tokens)
                                :value selected-tokens}
                     :total {:count (count all-tokens)
                             :value all-tokens}}}
           (summarize-sync-result
             {:all-tokens all-tokens
              :already-synced-tokens already-synced-tokens
              :pending-tokens pending-tokens
              :selected-tokens selected-tokens}
             token-sync-result)))))

(deftest test-sync-tokens
  (let [current-time-ms (System/currentTimeMillis)
        previous-time-ms (- current-time-ms 10101010)
        cluster-1 "www.cluster-1.com"
        cluster-2 "www.cluster-2.com"
        cluster-3 "www.cluster-3.com"
        cluster-urls [cluster-1 cluster-2 cluster-3]
        cluster-url->tokens {cluster-1 ["token-1" "token-2" "token-5" "token-6"]
                             cluster-2 ["token-1" "token-2" "token-3" "token-4" "token-5" "token-6"]
                             cluster-3 ["token-1" "token-3" "token-5" "token-6"]}
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
                                                              :token-etag current-time-ms}}
                                        "token-5" {cluster-1 {:description (assoc token-desc-4
                                                                             "cpus" 1
                                                                             "root" cluster-1)
                                                              :token-etag current-time-ms}
                                                   cluster-2 {:description (assoc token-desc-4
                                                                             "cpus" 2
                                                                             "last-update-time" previous-time-ms
                                                                             "root" cluster-2)
                                                              :token-etag current-time-ms}
                                                   cluster-3 {:description (assoc token-desc-4
                                                                             "cpus" 3
                                                                             "last-update-time" previous-time-ms
                                                                             "root" cluster-1)
                                                              :token-etag previous-time-ms}}
                                        "token-6" {cluster-1 {:description (assoc token-desc-1 "cpus" 6 "root" cluster-1)
                                                              :token-etag current-time-ms}
                                                   cluster-2 {:description (assoc token-desc-1 "cpus" 6 "root" cluster-2)
                                                              :token-etag current-time-ms}
                                                   cluster-3 {:description (assoc token-desc-1 "cpus" 6 "root" cluster-3)
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
                                              :token-etag (str (get token-desc-4 "last-update-time"))}
                                   "token-5" {:cluster-url cluster-1
                                              :description (assoc token-desc-4 "cpus" 1 "root" "c2")
                                              :token-etag (str (get token-desc-4 "last-update-time"))}
                                   "token-6" {:cluster-url cluster-1
                                              :description (assoc token-desc-1 "cpus" 6 "root" cluster-1)
                                              :token-etag (str (get token-desc-1 "last-update-time"))}}
        compute-sync-result (fn [cluster-urls token code]
                              (pc/map-from-keys
                                (fn [cluster-url]
                                  {:code code
                                   :details {:etag (str token "." cluster-url "." (count cluster-urls))
                                             :status 200}})
                                cluster-urls))
        waiter-api {:load-token-list
                    (fn [cluster-url]
                      (let [cluster-tokens (cluster-url->tokens cluster-url)]
                        (->> cluster-tokens
                             (map (fn [token]
                                    (let [{:strs [deleted last-update-time owner]}
                                          (get-in token->cluster-url->token-data [token cluster-url :description])]
                                      (cond-> {"deleted" (true? deleted)
                                               "token" token}
                                              last-update-time (assoc "etag" (str "E" last-update-time))
                                              owner (assoc "owner" owner)))))
                             vec)))}]
    (with-redefs [retrieve-token->url->token-data (fn [_ in-cluster-urls all-tokens]
                                                    (is (= (set cluster-urls) in-cluster-urls))
                                                    (is (= #{"token-2" "token-3" "token-4" "token-5"}
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
      (let [{:keys [details summary]} (sync-tokens waiter-api cluster-urls (inc (count token->latest-description)))]
        (is (= {"token-2" {:latest (token->latest-description "token-2")
                           :sync-result (-> [cluster-2 cluster-3]
                                            (compute-sync-result "token-2" :success/sync-update))}
                "token-3" {:latest (token->latest-description "token-3")
                           :sync-result (-> [cluster-1 cluster-3]
                                            (compute-sync-result "token-3" :success/soft-delete))}
                "token-4" {:latest (token->latest-description "token-4")
                           :sync-result (-> [cluster-1 cluster-2 cluster-3]
                                            (compute-sync-result "token-4" :success/hard-delete))}
                "token-5" {:latest (token->latest-description "token-5")
                           :sync-result (-> [cluster-1 cluster-2 cluster-3]
                                            (compute-sync-result "token-5" :success/hard-delete))}}
               details))
        (is (= {:sync {:failed #{}
                       :unmodified #{}
                       :updated #{"token-2" "token-3" "token-4" "token-5"}}
                :tokens {:pending {:count 4 :value #{"token-2" "token-3" "token-4" "token-5"}}
                         :previously-synced {:count 2 :value #{"token-1" "token-6"}}
                         :processed {:count 4 :value #{"token-2" "token-3" "token-4" "token-5"}}
                         :selected {:count 4 :value #{"token-2" "token-3" "token-4" "token-5"}}
                         :total {:count 6 :value #{"token-1" "token-2" "token-3" "token-4" "token-5" "token-6"}}}}
               summary))))))

(deftest test-parse-sync-clusters-cli-options
  (let [option-specs (:option-specs sync-clusters-config)
        parse-cli-options (fn [args] (tools-cli/parse-opts args option-specs))]
    (is (= ["Unknown option: \"-c\""]
           (:errors (parse-cli-options ["-c" "abcd"]))))
    (is (= ["Unknown option: \"-c\""]
           (:errors (parse-cli-options ["-c" "abcd,abcd"]))))
    (is (= ["Missing required argument for \"-l LIMIT\""]
           (:errors (parse-cli-options ["-l"]))))
    (is (= {:limit 200}
           (:options (parse-cli-options ["-l" "200"]))))
    (is (= {:limit 200}
           (:options (parse-cli-options ["--limit" "200"]))))))

(deftest test-sync-clusters-config
  (let [test-command-config (assoc sync-clusters-config :command-name "test-command")
        waiter-api (Object.)
        context {:waiter-api waiter-api}]
    (let [args []]
      (is (= {:exit-code 1
              :message "test-command: at least two different cluster urls required, provided: []"}
             (cli/process-command test-command-config context args))))
    (let [args ["-h"]]
      (with-out-str
        (is (= {:exit-code 0
                :message "test-command: displayed documentation"}
               (cli/process-command test-command-config context args)))))
    (let [args ["http://cluster-1.com"]]
      (is (= {:exit-code 1
              :message "test-command: at least two different cluster urls required, provided: [\"http://cluster-1.com\"]"}
             (cli/process-command test-command-config context args))))
    (let [args ["http://cluster-1.com" "http://cluster-1.com"]]
      (is (= {:exit-code 1
              :message "test-command: at least two different cluster urls required, provided: [\"http://cluster-1.com\" \"http://cluster-1.com\"]"}
             (cli/process-command test-command-config context args))))
    (let [args ["http://cluster-1.com" "http://cluster-2.com"]]
      (with-redefs [sync-tokens (fn [in-waiter-api cluster-urls-set limit]
                                  (is (= waiter-api in-waiter-api))
                                  (is (= #{"http://cluster-1.com" "http://cluster-2.com"} cluster-urls-set))
                                  (is (= 1000 limit)))]
        (is (= {:exit-code 0
                :message "test-command: exiting with code 0"}
               (cli/process-command test-command-config context args)))))
    (let [args ["-l" "20" "http://cluster-1.com" "http://cluster-2.com"]]
      (with-redefs [sync-tokens (fn [in-waiter-api cluster-urls-set limit]
                                  (is (= waiter-api in-waiter-api))
                                  (is (= #{"http://cluster-1.com" "http://cluster-2.com"} cluster-urls-set))
                                  (is (= 20 limit)))]
        (is (= {:exit-code 0
                :message "test-command: exiting with code 0"}
               (cli/process-command test-command-config context args)))))
    (let [args ["http://cluster-1.com" "http://cluster-2.com"]]
      (with-redefs [sync-tokens (fn [in-waiter-api cluster-urls-set limit]
                                  (is (= waiter-api in-waiter-api))
                                  (is (= #{"http://cluster-1.com" "http://cluster-2.com"} cluster-urls-set))
                                  (is (= 1000 limit))
                                  {:summary {:sync {:failed #{"foo"}}}})]
        (is (= {:exit-code 1
                :message "test-command: exiting with code 1"}
               (cli/process-command test-command-config context args)))))))
