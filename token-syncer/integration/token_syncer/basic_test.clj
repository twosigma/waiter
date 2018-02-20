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
(ns token-syncer.basic-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [token-syncer.main :as main]
            [token-syncer.syncer :as syncer])
  (:import (java.util UUID)))

(def basic-description {"cmd" "echo 'Hello World'", "cpus" 1, "mem" 2048, "metric-group" "syncer-test"})

(defn- waiter-urls []
  (let [waiter-uris (System/getenv "WAITER_URIS")]
    (is waiter-uris)
    (-> waiter-uris
        (str/split #",")
        sort)))

(defn- waiter-functions []
  (main/extract-waiter-functions {:connection-timeout-ms 5000, :idle-timeout-ms 5000}))

(deftest ^:integration test-environment
  (log/info "****** Running: test-environment")
  (testing "presence of environment variables"
    (log/info "env.WAITER_URIS" (System/getenv "WAITER_URIS"))
    (is (System/getenv "WAITER_URIS"))
    (is (> (count (waiter-urls)) 1))))

(defn- token->etag
  "Retrieves the etag for a token on a waiter router."
  [{:keys [load-token]} waiter-url token-name]
  (-> (load-token waiter-url token-name)
      (get :token-etag)
      (or 0) ;; Waiter defaults etag to 0
      str))

(defn- cleanup-token
  [{:keys [hard-delete-token] :as waiter-functions} waiter-urls token-name]
  (log/info "Cleaning up token:" token-name)
  (doseq [waiter-url waiter-urls]
    (try
      (let [token-etag (token->etag waiter-functions waiter-url token-name)]
        (hard-delete-token waiter-url token-name token-etag))
      (catch Exception _))))

(deftest ^:integration test-token-hard-delete
  (testing "token sync hard-delete"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-functions} (waiter-functions)
          token-name (str "test-token-hard-delete-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-hard-delete ARRANGE")
        (let [last-update-time-ms (- (System/currentTimeMillis) 10000)
              token-metadata {"deleted" true, "last-update-time" last-update-time-ms, "owner" "test-user", "root" "src1"}
              token-description (merge basic-description token-metadata)]

          (doseq [waiter-url waiter-urls]
            (let [token-etag (token->etag waiter-functions waiter-url token-name)]
              (store-token waiter-url token-name token-etag token-description)))

          (let [token-etag (token->etag waiter-functions (first waiter-urls) token-name)]

            (log/info "****** test-token-hard-delete ACT")
            (let [actual-result (syncer/sync-tokens waiter-functions waiter-urls)]

              (log/info "****** test-token-hard-delete ASSERT")
              (let [waiter-sync-result (constantly
                                         {:code :success/hard-delete
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description token-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result waiter-urls)}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:num-processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (let [response (load-token waiter-url token-name)]
                    (is (= 404 (:status response)) (str waiter-url " responded with " response))))))))
        (finally
          (cleanup-token waiter-functions waiter-urls token-name))))))

(deftest ^:integration test-token-soft-delete
  (testing "token sync soft-delete"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-functions} (waiter-functions)
          token-name (str "test-token-soft-delete-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-soft-delete ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata {"last-update-time" current-time-ms, "owner" "test-user", "root" "src1"}
              token-description (merge basic-description token-metadata)]

          (store-token (first waiter-urls) token-name "0" (assoc token-description "deleted" true))
          (doseq [waiter-url (rest waiter-urls)]
            (let [last-update-time-ms (- current-time-ms 10000)]
              (store-token waiter-url token-name "0"
                           (assoc token-description "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag waiter-functions (first waiter-urls) token-name)]

            (log/info "****** test-token-soft-delete ACT")
            (let [actual-result (syncer/sync-tokens waiter-functions waiter-urls)]

              (log/info "****** test-token-soft-delete ASSERT")
              (let [waiter-sync-result (constantly
                                         {:code :success/soft-delete
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description (assoc token-description "deleted" true)
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:num-processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description (assoc token-description "deleted" true)
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-functions waiter-urls token-name))))))

(deftest ^:integration test-token-token-on-single-cluster
  (testing "token exists on single cluster"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-functions} (waiter-functions)
          token-name (str "test-token-token-on-single-cluster-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-token-on-single-cluster ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata {"last-update-time" current-time-ms, "owner" "test-user", "root" "src1"}
              token-description (merge basic-description token-metadata)]

          (store-token (first waiter-urls) token-name "0" token-description)

          (let [token-etag (token->etag waiter-functions (first waiter-urls) token-name)]

            (log/info "****** test-token-token-on-single-cluster ACT")
            (let [actual-result (syncer/sync-tokens waiter-functions waiter-urls)]

              (log/info "****** test-token-token-on-single-cluster ASSERT")
              (let [waiter-sync-result (constantly
                                         {:code :success/sync-update
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description token-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:num-processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-functions waiter-urls token-name))))))

(deftest ^:integration test-token-already-synced
  (testing "token already synced"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-functions} (waiter-functions)
          token-name (str "test-token-already-synced-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-already-synced ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata {"last-update-time" current-time-ms, "owner" "test-user", "root" "src1"}
              token-description (merge basic-description token-metadata)]

          (doseq [waiter-url waiter-urls]
            (store-token waiter-url token-name "0" token-description))

          (let [token-etag (token->etag waiter-functions (first waiter-urls) token-name)]

            (log/info "****** test-token-already-synced ACT")
            (let [actual-result (syncer/sync-tokens waiter-functions waiter-urls)]

              (log/info "****** test-token-already-synced ASSERT")
              (let [waiter-sync-result (constantly {:code :success/token-match})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description token-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{token-name}
                                                      :updated #{}}
                                               :tokens {:num-processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-functions waiter-urls token-name))))))

(deftest ^:integration test-token-update
  (testing "token sync update"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-functions} (waiter-functions)
          token-name (str "test-token-update-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-update ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata {"last-update-time" current-time-ms, "owner" "test-user", "root" "src1"}
              token-description (merge basic-description token-metadata)]

          (let [last-update-time-ms (- current-time-ms 10000)]
            (store-token (first waiter-urls) token-name "0" token-description)
            (doseq [waiter-url (rest waiter-urls)]
              (store-token waiter-url token-name "0"
                           (assoc token-description "cpus" 2, "mem" 2048, "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag waiter-functions (first waiter-urls) token-name)]

            (log/info "****** test-token-update ACT")
            (let [actual-result (syncer/sync-tokens waiter-functions waiter-urls)]

              (log/info "****** test-token-update ASSERT")
              (let [waiter-sync-result (constantly
                                         {:code :success/sync-update
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description token-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:num-processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-functions waiter-urls token-name))))))

(deftest ^:integration test-token-different-owners-but-same-root
  (testing "token sync update with different owners but same root"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-functions} (waiter-functions)
          token-name (str "test-token-different-owners-but-same-root-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-different-owners-but-same-root ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name "0"
                             (assoc basic-description
                               "cpus" (inc index)
                               "last-update-time" (- last-update-time-ms index)
                               "owner" (str "test-user-" index)
                               "root" "common-root")))
              waiter-urls))

          (let [token-etag (token->etag waiter-functions (first waiter-urls) token-name)]

            (log/info "****** test-token-different-owners-but-same-root ACT")
            (let [actual-result (syncer/sync-tokens waiter-functions waiter-urls)]

              (log/info "****** test-token-different-owners-but-same-root ASSERT")
              (let [latest-description (assoc basic-description
                                         "cpus" 1
                                         "last-update-time" last-update-time-ms
                                         "owner" "test-user-0"
                                         "root" "common-root")
                    waiter-sync-result (constantly
                                         {:code :success/sync-update
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description latest-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:num-processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description latest-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-functions waiter-urls token-name))))))

(deftest ^:integration test-token-different-roots
  (testing "token sync update with different owners and different roots"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-functions} (waiter-functions)
          token-name (str "test-token-different-roots-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-different-roots ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name "0"
                             (assoc basic-description
                               "cpus" (inc index)
                               "last-update-time" (- last-update-time-ms index)
                               "owner" "test-user"
                               "root" waiter-url)))
              waiter-urls))

          (let [token-etag (token->etag waiter-functions (first waiter-urls) token-name)]

            (log/info "****** test-token-different-roots ACT")
            (let [actual-result (syncer/sync-tokens waiter-functions waiter-urls)]

              (log/info "****** test-token-different-roots ASSERT")
              (let [latest-description (assoc basic-description
                                         "cpus" 1
                                         "last-update-time" last-update-time-ms
                                         "owner" "test-user"
                                         "root" (first waiter-urls))
                    sync-result (->> (rest waiter-urls)
                                     (map-indexed
                                       (fn [index waiter-url]
                                         [waiter-url
                                          {:code :error/root-mismatch
                                           :details {:cluster (assoc basic-description
                                                                "cpus" (+ index 2)
                                                                "last-update-time" (- last-update-time-ms index 1)
                                                                "owner" "test-user"
                                                                "root" waiter-url)
                                                     :latest latest-description}}]))
                                     (into {}))
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description latest-description
                                                                    :token-etag token-etag}
                                                           :sync-result sync-result}}
                                     :summary {:sync {:failed #{token-name}
                                                      :unmodified #{}
                                                      :updated #{}}
                                               :tokens {:num-processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doall
                  (map-indexed
                    (fn [index waiter-url]
                      (let [token-last-modified-time (- last-update-time-ms index)]
                        (is (= {:description (assoc basic-description
                                               "cpus" (inc index)
                                               "last-update-time" token-last-modified-time
                                               "owner" "test-user"
                                               "root" waiter-url)
                                :headers {"content-type" "application/json"
                                          "etag" (str token-last-modified-time)}
                                :status 200
                                :token-etag (str token-last-modified-time)}
                               (load-token waiter-url token-name)))))
                    waiter-urls))))))
        (finally
          (cleanup-token waiter-functions waiter-urls token-name))))))
