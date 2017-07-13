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

(deftest test-retrieve-token->router-url->token-data
  (let [http-client (Object.)
        test-router-urls ["www.router-1.com" "www.router-2.com" "www.router-3.com"]
        test-tokens ["token-1" "token-2" "token-3" "token-4" "token-5"]]
    (with-redefs [waiter/load-token-on-router (fn [_ router-url token] (str router-url ":" token))]
      (is (= (pc/map-from-keys
               (fn [token]
                 (pc/map-from-keys
                   (fn [router-url]
                     (str router-url ":" token))
                   test-router-urls))
               test-tokens)
             (retrieve-token->router-url->token-data http-client test-router-urls test-tokens))))))

(deftest test-retrieve-token->latest-description
  (testing "empty input"
    (is (= {} (retrieve-token->latest-description {}))))

  (testing "single empty entry"
    (let [token->router-url->token-data {"token-1" {}}]
      (is (= {"token-1" {}}
             (retrieve-token->latest-description token->router-url->token-data)))))

  (testing "single entry"
    (let [token-data {:description {"last-update-time" 1234}}
          token->router-url->token-data {"token-1" {"router-1" token-data}}]
      (is (= {"token-1" {:description {"last-update-time" 1234}, :router-url "router-1"}}
             (retrieve-token->latest-description token->router-url->token-data)))))

  (testing "single valid entry"
    (let [token-data {:description {"last-update-time" 1234}}
          token->router-url->token-data {"token-1" {"router-1" token-data
                                                    "router-2" {}}}]
      (is (= {"token-1" {:description {"last-update-time" 1234}, :router-url "router-1"}}
             (retrieve-token->latest-description token->router-url->token-data)))))

  (testing "single valid entry"
    (let [token-data-1 {:description {"last-update-time" 1234}}
          token-data-2 {:description {"last-update-time" 4567}}
          token-data-3 {:description {"last-update-time" 1267}}
          token->router-url->token-data {"token-1" {"router-1" token-data-1
                                                    "router-2" token-data-2
                                                    "router-3" token-data-3}}]
      (is (= {"token-1" {:description {"last-update-time" 4567}, :router-url "router-2"}}
             (retrieve-token->latest-description token->router-url->token-data)))))

  (testing "multiple-tokens"
    (let [token-data-1 {:description {"last-update-time" 1234}}
          token-data-2 {:description {"last-update-time" 4567}}
          token-data-3 {:description {"last-update-time" 1267}}
          token->router-url->token-data {"token-A2" {"router-1" token-data-1
                                                     "router-2" token-data-2
                                                     "router-3" token-data-3}
                                         "token-B" {}
                                         "token-C1" {"router-1" token-data-2
                                                     "router-2" token-data-1
                                                     "router-3" token-data-3}
                                         "token-D3" {"router-1" token-data-3
                                                     "router-2" token-data-2
                                                     "router-3" token-data-1}}]
      (is (= {"token-A2" {:description {"last-update-time" 4567}, :router-url "router-2"}
              "token-B" {}
              "token-C1" {:description {"last-update-time" 4567}, :router-url "router-1"}
              "token-D3" {:description {"last-update-time" 4567}, :router-url "router-2"}}
             (retrieve-token->latest-description token->router-url->token-data))))))

(deftest test-hard-delete-token-on-all-routers
  (let [http-client (Object.)
        test-router-urls ["www.router-1.com" "www.router-2-error.com" "www.router-3.com"]
        test-token "token-1"]
    (with-redefs [waiter/hard-delete-token-on-router
                  (fn [_ router-url token]
                    (if (str/includes? router-url "error")
                      (throw (Exception. "Error in hard-delete thrown from test"))
                      (str router-url ":" token)))]
      (is (= {"www.router-1.com" {:message :successfully-hard-deleted-token-on-cluster
                                  :response "www.router-1.com:token-1"}
              "www.router-2-error.com" {:cause "Error in hard-delete thrown from test"
                                        :message :error-in-delete}
              "www.router-3.com" {:message :successfully-hard-deleted-token-on-cluster
                                  :response "www.router-3.com:token-1"}}
             (hard-delete-token-on-all-routers http-client test-router-urls test-token))))))

(deftest test-sync-token-on-routers
  (let [http-client (Object.)]
    (with-redefs [waiter/store-token-on-router
                  (fn [_ router-url token token-description]
                    (if (str/includes? router-url "error")
                      (throw (Exception. "Error in storing token thrown from test"))
                      (assoc token-description :router-url router-url :token token)))]

      (testing "sync router error while loading token"
        (let [router-urls ["www.router-1.com" "www.router-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              router-url->token-data {"www.router-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.router-2.com" {:error (Exception. "router-2 data cannot be loaded")}}]
          (is (= {"www.router-1.com" {:message :token-already-synced}
                  "www.router-2.com" {:cause "router-2 data cannot be loaded"
                                      :message :unable-to-read-token-on-cluster}}
                 (sync-token-on-routers http-client router-urls test-token token-description router-url->token-data)))))

      (testing "sync router error while missing status"
        (let [router-urls ["www.router-1.com" "www.router-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              router-url->token-data {"www.router-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.router-2.com" {}}]
          (is (= {"www.router-1.com" {:message :token-already-synced}
                  "www.router-2.com" {:cause "status missing from response"
                                      :message :unable-to-read-token-on-cluster}}
                 (sync-token-on-routers http-client router-urls test-token token-description router-url->token-data)))))

      (testing "sync router different owners"
        (let [router-urls ["www.router-1.com" "www.router-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              router-url->token-data {"www.router-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.router-2.com" {:description {"owner" "test-user-2"}
                                                          :status 200}}]
          (is (= {"www.router-1.com" {:message :token-already-synced}
                  "www.router-2.com" {:latest-token-description token-description
                                      :message :token-owners-are-different
                                      :router-data {"owner" "test-user-2"}}}
                 (sync-token-on-routers http-client router-urls test-token token-description router-url->token-data)))))

      (testing "sync router successfully"
        (let [router-urls ["www.router-1.com" "www.router-2.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              router-url->token-data {"www.router-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.router-2.com" {:description {"owner" "test-user-1"}
                                                          :status 200}}]
          (is (= {"www.router-1.com" {:message :token-already-synced}
                  "www.router-2.com" {:message :sync-token-on-cluster
                                      :response (assoc token-description
                                                  :router-url "www.router-2.com"
                                                  :token test-token)}}
                 (sync-token-on-routers http-client router-urls test-token token-description router-url->token-data)))))

      (testing "sync router error"
        (let [router-urls ["www.router-1.com" "www.router-2-error.com"]
              test-token "test-token-1"
              token-description {"name" "test-name"
                                 "owner" "test-user-1"}
              router-url->token-data {"www.router-1.com" {:description {"name" "test-name"
                                                                        "owner" "test-user-1"}
                                                          :status 200}
                                      "www.router-2-error.com" {:description {"owner" "test-user-1"}
                                                                :status 200}}]
          (is (= {"www.router-1.com" {:message :token-already-synced}
                  "www.router-2-error.com" {:cause "Error in storing token thrown from test"
                                            :message :error-in-token-sync}}
                 (sync-token-on-routers http-client router-urls test-token token-description router-url->token-data))))))))

(deftest test-sync-tokens
  (let [http-client (Object.)
        router-urls ["www.router-1.com" "www.router-2.com" "www.router-3.com"]
        router-url->tokens {"www.router-1.com" ["token-1" "token-2"]
                            "www.router-2.com" ["token-2" "token-3" "token-4"]
                            "www.router-3.com" ["token-1" "token-3"]}
        token-desc-1 {"name" "t1-all-synced"}
        token-desc-2 {"name" "t2-needs-sync"}
        token-desc-3 {"name" "t3-soft-delete"
                      "deleted" true}
        token-desc-4 {"name" "t4-hard-delete"
                      "deleted" true}
        token->router-url->token-data {"token-1" {"www.router-1.com" {:description token-desc-1}
                                                  "www.router-2.com" {:description (assoc token-desc-1 "cmd" "test")}
                                                  "www.router-3.com" {:description token-desc-1}}
                                       "token-2" {"www.router-1.com" {:description token-desc-2}
                                                  "www.router-2.com" {:description token-desc-2}
                                                  "www.router-3.com" {:description token-desc-2}}
                                       "token-3" {"www.router-1.com" {:description (dissoc token-desc-3 "deleted")}
                                                  "www.router-2.com" {:description token-desc-3}
                                                  "www.router-3.com" {:description token-desc-3}}
                                       "token-4" {"www.router-1.com" {:description token-desc-4}
                                                  "www.router-2.com" {:description token-desc-4}
                                                  "www.router-3.com" {:description token-desc-1}}}
        token->latest-description {"token-1" token-desc-1
                                   "token-2" token-desc-2
                                   "token-3" token-desc-3
                                   "token-4" token-desc-4}]
    (with-redefs [waiter/load-token-list (fn [_ router-url]
                                           (router-url->tokens router-url))
                  retrieve-token->router-url->token-data (fn [_ in-router-urls all-tokens]
                                                           (is (= (set router-urls) in-router-urls))
                                                           (is (= #{"token-1" "token-2" "token-3" "token-4"}
                                                                  (set all-tokens)))
                                                           token->router-url->token-data)
                  retrieve-token->latest-description (fn [in-token->router-url->token-data]
                                                       (is (= token->router-url->token-data in-token->router-url->token-data))
                                                       token->latest-description)
                  hard-delete-token-on-all-routers (fn [_ router-urls token]
                                                     (keyword (str token "-" (count router-urls) "-hard-delete")))
                  sync-token-on-routers (fn [_ router-urls token _ _]
                                          (keyword (str token "-" (count router-urls) "-sync-token")))]
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
             (sync-tokens http-client router-urls))))))

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
                 (cid/info (waiter/load-token-on-router http-client waiter-url token))
                 (is (= 200 status) (str "Error: " body)))))))