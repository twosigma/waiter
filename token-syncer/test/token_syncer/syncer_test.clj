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
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [token-syncer.syncer :refer :all]
            [token-syncer.waiter :as waiter]))

(deftest test-retrieve-token->cluster-url->token-data
  (let [http-client (Object.)
        test-cluster-urls ["www.cluster-1.com" "www.cluster-2.com" "www.cluster-3.com"]
        test-tokens ["token-1" "token-2" "token-3" "token-4" "token-5"]]
    (with-redefs [waiter/load-token-on-cluster (fn [_ cluster-url token] (str cluster-url ":" token))]
      (is (= (pc/map-from-keys
               (fn [token]
                 (pc/map-from-keys
                   (fn [cluster-url]
                     (str cluster-url ":" token))
                   test-cluster-urls))
               test-tokens)
             (retrieve-token->cluster-url->token-data http-client test-cluster-urls test-tokens))))))

(deftest test-retrieve-token->latest-description
  (testing "empty input"
    (is (= {} (retrieve-token->latest-description {}))))

  (testing "single empty entry"
    (let [token->cluster-url->token-data {"token-1" {}}]
      (is (= {"token-1" {}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "single entry"
    (let [token-data {:description {"last-update-time" 1234}}
          token->cluster-url->token-data {"token-1" {"cluster-1" token-data}}]
      (is (= {"token-1" {:description {"last-update-time" 1234}, :cluster-url "cluster-1"}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "single valid entry"
    (let [token-data {:description {"last-update-time" 1234}}
          token->cluster-url->token-data {"token-1" {"cluster-1" token-data
                                                    "cluster-2" {}}}]
      (is (= {"token-1" {:description {"last-update-time" 1234}, :cluster-url "cluster-1"}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "single valid entry"
    (let [token-data-1 {:description {"last-update-time" 1234}}
          token-data-2 {:description {"last-update-time" 4567}}
          token-data-3 {:description {"last-update-time" 1267}}
          token->cluster-url->token-data {"token-1" {"cluster-1" token-data-1
                                                    "cluster-2" token-data-2
                                                    "cluster-3" token-data-3}}]
      (is (= {"token-1" {:description {"last-update-time" 4567}, :cluster-url "cluster-2"}}
             (retrieve-token->latest-description token->cluster-url->token-data)))))

  (testing "multiple-tokens"
    (let [token-data-1 {:description {"last-update-time" 1234}}
          token-data-2 {:description {"last-update-time" 4567}}
          token-data-3 {:description {"last-update-time" 1267}}
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
      (is (= {"token-A2" {:description {"last-update-time" 4567}, :cluster-url "cluster-2"}
              "token-B" {}
              "token-C1" {:description {"last-update-time" 4567}, :cluster-url "cluster-1"}
              "token-D3" {:description {"last-update-time" 4567}, :cluster-url "cluster-2"}}
             (retrieve-token->latest-description token->cluster-url->token-data))))))

(deftest test-hard-delete-token-on-all-clusters
  (let [http-client (Object.)
        test-cluster-urls ["www.cluster-1.com" "www.cluster-2-error.com" "www.cluster-3.com"]
        test-token "token-1"]
    (with-redefs [waiter/hard-delete-token-on-cluster
                  (fn [_ cluster-url token]
                    (if (str/includes? cluster-url "error")
                      (throw (Exception. "Error in hard-delete thrown from test"))
                      (str cluster-url ":" token)))]
      (is (= {"www.cluster-1.com" {:message :successfully-hard-deleted-token-on-cluster
                                  :response "www.cluster-1.com:token-1"}
              "www.cluster-2-error.com" {:cause "Error in hard-delete thrown from test"
                                        :message :error-in-delete}
              "www.cluster-3.com" {:message :successfully-hard-deleted-token-on-cluster
                                  :response "www.cluster-3.com:token-1"}}
             (hard-delete-token-on-all-clusters http-client test-cluster-urls test-token))))))

(deftest test-sync-token-on-clusters
  (let [http-client (Object.)]
    (with-redefs [waiter/store-token-on-cluster
                  (fn [_ cluster-url token token-description]
                    (if (str/includes? cluster-url "error")
                      (throw (Exception. "Error in storing token thrown from test"))
                      (assoc token-description :cluster-url cluster-url :token token)))]

      (testing "sync cluster error while loading token"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.cluster-2.com" {:error (Exception. "cluster-2 data cannot be loaded")}}]
          (is (= {"www.cluster-1.com" {:message :token-already-synced}
                  "www.cluster-2.com" {:cause "cluster-2 data cannot be loaded"
                                      :message :unable-to-read-token-on-cluster}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster error while missing status"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.cluster-2.com" {}}]
          (is (= {"www.cluster-1.com" {:message :token-already-synced}
                  "www.cluster-2.com" {:cause "status missing from response"
                                      :message :unable-to-read-token-on-cluster}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster different owners"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.cluster-2.com" {:description {"owner" "test-user-2"}
                                                          :status 200}}]
          (is (= {"www.cluster-1.com" {:message :token-already-synced}
                  "www.cluster-2.com" {:latest-token-description token-description
                                      :message :token-owners-are-different
                                      :cluster-data {"owner" "test-user-2"}}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster successfully"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.cluster-2.com" {:description {"owner" "test-user-1"}
                                                          :status 200}}]
          (is (= {"www.cluster-1.com" {:message :token-already-synced}
                  "www.cluster-2.com" {:message :sync-token-on-cluster
                                      :response (assoc token-description
                                                  :cluster-url "www.cluster-2.com"
                                                  :token test-token)}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data)))))

      (testing "sync cluster error"
        (let [cluster-urls ["www.cluster-1.com" "www.cluster-2-error.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              cluster-url->token-data {"www.cluster-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.cluster-2-error.com" {:description {"owner" "test-user-1"}
                                                                :status 200}}]
          (is (= {"www.cluster-1.com" {:message :token-already-synced}
                  "www.cluster-2-error.com" {:cause "Error in storing token thrown from test"
                                            :message :error-in-token-sync}}
                 (sync-token-on-clusters http-client cluster-urls test-token token-description cluster-url->token-data))))))))

(deftest test-sync-tokens
  (let [http-client (Object.)
        cluster-urls ["www.cluster-1.com" "www.cluster-2.com" "www.cluster-3.com"]
        cluster-url->tokens {"www.cluster-1.com" ["token-1" "token-2"]
                            "www.cluster-2.com" ["token-2" "token-3" "token-4"]
                            "www.cluster-3.com" ["token-1" "token-3"]}
        token-desc-1 {"name" "t1-all-synced"}
        token-desc-2 {"name" "t2-needs-sync"}
        token-desc-3 {"name" "t3-soft-delete"
                      "deleted" true}
        token-desc-4 {"name" "t4-hard-delete"
                      "deleted" true}
        token->cluster-url->token-data {"token-1" {"www.cluster-1.com" {:description token-desc-1}
                                                  "www.cluster-2.com" {:description (assoc token-desc-1 "cmd" "test")}
                                                  "www.cluster-3.com" {:description token-desc-1}}
                                       "token-2" {"www.cluster-1.com" {:description token-desc-2}
                                                  "www.cluster-2.com" {:description token-desc-2}
                                                  "www.cluster-3.com" {:description token-desc-2}}
                                       "token-3" {"www.cluster-1.com" {:description (dissoc token-desc-3 "deleted")}
                                                  "www.cluster-2.com" {:description token-desc-3}
                                                  "www.cluster-3.com" {:description token-desc-3}}
                                       "token-4" {"www.cluster-1.com" {:description token-desc-4}
                                                  "www.cluster-2.com" {:description token-desc-4}
                                                  "www.cluster-3.com" {:description token-desc-1}}}
        token->latest-description {"token-1" token-desc-1
                                   "token-2" token-desc-2
                                   "token-3" token-desc-3
                                   "token-4" token-desc-4}]
    (with-redefs [waiter/load-token-list (fn [_ cluster-url]
                                           (cluster-url->tokens cluster-url))
                  retrieve-token->cluster-url->token-data (fn [_ in-cluster-urls all-tokens]
                                                           (is (= (set cluster-urls) in-cluster-urls))
                                                           (is (= #{"token-1" "token-2" "token-3" "token-4"}
                                                                  (set all-tokens)))
                                                           token->cluster-url->token-data)
                  retrieve-token->latest-description (fn [in-token->cluster-url->token-data]
                                                       (is (= token->cluster-url->token-data in-token->cluster-url->token-data))
                                                       token->latest-description)
                  hard-delete-token-on-all-clusters (fn [_ cluster-urls token]
                                                     (keyword (str token "-" (count cluster-urls) "-hard-delete")))
                  sync-token-on-clusters (fn [_ cluster-urls token _ _]
                                          (keyword (str token "-" (count cluster-urls) "-sync-token")))]
      (is (= {:num-tokens-processed 4,
              :result
              {"token-4"
               {:description {"name" "t4-hard-delete", "deleted" true},
                :sync-result :token-4-3-sync-token},
               "token-3"
               {:description {"name" "t3-soft-delete", "deleted" true},
                :sync-result :token-3-3-sync-token},
               "token-2"
               {:description {"name" "t2-needs-sync"},
                :sync-result :token-2-3-sync-token},
               "token-1"
               {:description {"name" "t1-all-synced"},
                :sync-result :token-1-3-sync-token}}}
             (sync-tokens http-client cluster-urls))))))

;; TODO shams remove tests bdelow

(comment deftest test-retrieve-token
         (let [http-client (http/client {:connect-timeout 1000
                                         :idle-timeout 20000
                                         :follow-redirects? false})]
           (sync-tokens http-client ["http://localhost:9091" "http://localhost:9092"])))

(comment deftest test-register-tokens
         (let [http-client (http/client {:connect-timeout 1000
                                         :idle-timeout 20000
                                         :follow-redirects? false})
               waiter-port 9091
               waiter-url (str "http://localhost:" waiter-port)]
           (let [token-prefix "test-sync-token"
                 num-tokens-to-create 4
                 tokens-to-create (map #(str "token" %1 "." token-prefix "." waiter-port) (range num-tokens-to-create))]
             (cid/info "creating the tokens:" (str waiter-url "/token"))
             (doseq [token tokens-to-create]
               (let [{:keys [body error status]}
                     (async/<!!
                       (waiter/make-http-request http-client (str waiter-url "/token")
                                                 :body (json/write-str {:health-check-url "/custom-endpoint"
                                                                        :deleted true
                                                                        :token token
                                                                        :name token})
                                                 :method http/post))]
                 (when error
                   (.printStackTrace error))
                 (cid/info status (async/<!! body) error)
                 (cid/info (waiter/load-token-on-cluster http-client waiter-url token))
                 (is (= 200 status) (str "Error: " body)))))))