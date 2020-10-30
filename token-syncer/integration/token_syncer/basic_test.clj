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
(ns token-syncer.basic-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [token-syncer.commands.backup :as backup]
            [token-syncer.commands.cleanup :as cleanup]
            [token-syncer.commands.ping :as ping]
            [token-syncer.commands.restore :as restore]
            [token-syncer.commands.syncer :as syncer]
            [token-syncer.file-utils :as file-utils]
            [token-syncer.main :as main])
  (:import (java.util UUID)))

(defn- kitchen-cmd []
  (str (System/getenv "WAITER_TEST_KITCHEN_CMD") " -p $PORT0"))

(def basic-description {"cmd" (kitchen-cmd)
                        "cpus" 1
                        "mem" 2048
                        "metric-group" "syncer-test"})

(defn- waiter-urls
  "Retrieves urls to the waiter clusters provided in the WAITER_URIS environment variable."
  []
  (let [waiter-uris (System/getenv "WAITER_URIS")]
    (is waiter-uris)
    (-> waiter-uris
        (str/split #",")
        sort)))

(defn- waiter-url->cluster
  "Retrieves the cluster name corresponding to the provided cluster,"
  [waiter-url]
  (str "cluster-" (hash waiter-url)))

(defn- waiter-api
  "Initializes and returns the Waiter API functions."
  []
  (main/init-waiter-api {:connection-timeout-ms 5000, :idle-timeout-ms 5000}))

(deftest ^:integration test-environment
  (log/info "Running: test-environment")
  (testing "verifies presence of environment variables for running integration tests"
    (testing "waiter cluster uris"
      (log/info "env.WAITER_URIS" (System/getenv "WAITER_URIS"))
      (is (System/getenv "WAITER_URIS"))
      (is (> (count (waiter-urls)) 1)))

    (testing "kitchen command"
      (log/info "env.WAITER_TEST_KITCHEN_CMD" (System/getenv "WAITER_TEST_KITCHEN_CMD"))
      (is (not (str/blank? (System/getenv "WAITER_TEST_KITCHEN_CMD")))))))

(defn- basic-token-metadata
  "Returns the common metadata used in the tests."
  [current-time-ms]
  {"cluster" "cl.1"
   "last-update-time" current-time-ms
   "last-update-user" "auth-user"
   "owner" "test-user"
   "previous" {"last-update-time" (- current-time-ms 30000)
               "last-update-user" "another-auth-user"}
   "root" "src1"})

(defn- token->etag
  "Retrieves the etag for a token on a waiter router."
  [{:keys [load-token]} waiter-url token-name]
  (-> (load-token waiter-url token-name)
      (get :token-etag)))

(defn- cleanup-token
  "'Hard' deletes the token on all provided clusters."
  [{:keys [hard-delete-token] :as waiter-api} waiter-urls token-name]
  (log/info "Cleaning up token:" token-name)
  (doseq [waiter-url waiter-urls]
    (try
      (let [token-etag (token->etag waiter-api waiter-url token-name)]
        (hard-delete-token waiter-url token-name token-etag))
      (catch Exception _))))

(deftest ^:integration test-token-hard-delete
  (testing "token sync hard-delete"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-hard-delete-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [last-update-time-ms (- (System/currentTimeMillis) 10000)
              token-metadata (-> (basic-token-metadata last-update-time-ms)
                                 (assoc "deleted" true))
              token-description (merge basic-description token-metadata)]

          (doseq [waiter-url waiter-urls]
            (let [token-etag (token->etag waiter-api waiter-url token-name)]
              (store-token waiter-url token-name token-etag token-description)))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [waiter-sync-result (constantly
                                         {:code :success/hard-delete
                                          :details {:etag (str token-etag)
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description token-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result waiter-urls)}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (let [response (load-token waiter-url token-name)]
                    (is (= 404 (:status response)) (str waiter-url " responded with " response))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-soft-delete
  (testing "token sync soft-delete"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-soft-delete-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata (basic-token-metadata current-time-ms)
              token-description (merge basic-description token-metadata)]

          (store-token (first waiter-urls) token-name nil (assoc token-description "deleted" true))
          (doseq [waiter-url (rest waiter-urls)]
            (let [last-update-time-ms (- current-time-ms 10000)]
              (store-token waiter-url token-name nil
                           (assoc token-description "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
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
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description (assoc token-description "deleted" true)
                          :headers {"content-type" "application/json"}
                          :status 200}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-hard-delete-previously-soft-deleted
  (testing "token sync hard-delete previously soft-deleted"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-update-previously-soft-deleted-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata (basic-token-metadata current-time-ms)
              token-description (merge basic-description token-metadata)]

          (store-token (first waiter-urls) token-name nil (assoc token-description "deleted" true))
          (doseq [waiter-url (rest waiter-urls)]
            (let [last-update-time-ms (- current-time-ms 10000)]
              (store-token waiter-url token-name nil
                           (assoc token-description
                             "deleted" true
                             "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [waiter-sync-result (constantly
                                         {:code :success/hard-delete
                                          :details {:etag ""
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description (assoc token-description "deleted" true)
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result waiter-urls)}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description {}
                          :headers {"content-type" "application/json"}
                          :status 404}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-token-on-single-cluster
  (testing "token exists on single cluster"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-token-on-single-cluster-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata (basic-token-metadata current-time-ms)
              token-description (merge basic-description token-metadata)]

          (store-token (first waiter-urls) token-name nil token-description)

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
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
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-already-synced
  (testing "token already synced"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-already-synced-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata (basic-token-metadata current-time-ms)
              token-description (merge basic-description token-metadata)]

          (doseq [waiter-url waiter-urls]
            (store-token waiter-url token-name nil token-description))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [expected-result {:details {}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{}}
                                               :tokens {:pending {:count 0 :value #{}}
                                                        :previously-synced {:count 1 :value #{token-name}}
                                                        :processed {:count 0 :value #{}}
                                                        :selected {:count 0 :value #{}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-update
  (testing "token sync update"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-update-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata (basic-token-metadata current-time-ms)
              token-description (merge basic-description token-metadata)]

          (let [last-update-time-ms (- current-time-ms 10000)]
            (store-token (first waiter-urls) token-name nil token-description)
            (doseq [waiter-url (rest waiter-urls)]
              (store-token waiter-url token-name nil
                           (assoc token-description
                             "cpus" 2
                             "mem" 2048
                             "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
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
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-update-maintenance-mode
  (testing "token sync token enabled maintenance mode"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-maintenance-mode-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              token-metadata (basic-token-metadata current-time-ms)
              token-description (merge basic-description token-metadata)
              maintenance-config {"message" "custom maintenance message"}]

          (store-token (first waiter-urls) token-name nil (assoc token-description "maintenance" maintenance-config))
          (doseq [waiter-url (rest waiter-urls)]
            (let [last-update-time-ms (- current-time-ms 10000)]
              (store-token waiter-url token-name nil
                           (assoc token-description "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [waiter-sync-result (constantly
                                         {:code :success/sync-update
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description (assoc
                                                                                   token-description
                                                                                   "maintenance" maintenance-config)
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description (assoc token-description "maintenance" maintenance-config)
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))

    (testing "token sync token disabled maintenance mode"
      (let [waiter-urls (waiter-urls)
            {:keys [load-token store-token] :as waiter-api} (waiter-api)
            limit 10
            token-name (str "test-token-maintenance-mode-" (UUID/randomUUID))]
        (try
          ;; ARRANGE
          (let [current-time-ms (System/currentTimeMillis)
                token-metadata (basic-token-metadata current-time-ms)
                token-description (merge basic-description token-metadata)
                maintenance-config {"message" "custom maintenance message"}]

            (store-token (first waiter-urls) token-name nil token-description)
            (doseq [waiter-url (rest waiter-urls)]
              (let [last-update-time-ms (- current-time-ms 10000)]
                (store-token waiter-url token-name nil
                             (assoc token-description
                               "last-update-time" last-update-time-ms
                               "maintenance" maintenance-config))))

            (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

              ;; ACT
              (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

                ;; ASSERT
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
                                                 :tokens {:pending {:count 1 :value #{token-name}}
                                                          :previously-synced {:count 0 :value #{}}
                                                          :processed {:count 1 :value #{token-name}}
                                                          :selected {:count 1 :value #{token-name}}
                                                          :total {:count 1 :value #{token-name}}}}}]
                  (is (= expected-result actual-result))
                  (doseq [waiter-url waiter-urls]
                    (is (= {:description token-description
                            :headers {"content-type" "application/json"
                                      "etag" token-etag}
                            :status 200
                            :token-etag token-etag}
                           (load-token waiter-url token-name))))))))
          (finally
            (cleanup-token waiter-api waiter-urls token-name)))))))

(deftest ^:integration test-token-different-owners-but-same-root
  (testing "token sync update with different owners but same root"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-different-owners-but-same-root-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil
                             (assoc basic-description
                               "cluster" (waiter-url->cluster waiter-url)
                               "cpus" (inc index)
                               "last-update-time" (- last-update-time-ms index)
                               "last-update-user" (str "auth-user-" index)
                               "owner" (str "test-user-" index)
                               "previous" {"last-update-time" (- current-time-ms 30000)
                                           "last-update-user" "foo-user"}
                               "root" "common-root")))
              waiter-urls))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [latest-description (assoc basic-description
                                         "cluster" (waiter-url->cluster (first waiter-urls))
                                         "cpus" 1
                                         "last-update-time" last-update-time-ms
                                         "last-update-user" "auth-user-0"
                                         "owner" "test-user-0"
                                         "previous" {"last-update-time" (- current-time-ms 30000)
                                                     "last-update-user" "foo-user"}
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
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description latest-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (load-token waiter-url token-name))))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-different-roots-and-last-update-user
  (testing "token sync update with different roots"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-diff-root-diff-user" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil
                             (assoc basic-description
                               "cluster" (waiter-url->cluster waiter-url)
                               "cpus" (inc index)
                               "last-update-time" (- last-update-time-ms index)
                               "last-update-user" (str "auth-user-" index)
                               "owner" "test-user"
                               "previous" {"last-update-time" (- current-time-ms 30000)
                                           "last-update-user" "foo-user"}
                               "root" waiter-url)))
              waiter-urls))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [latest-description (assoc basic-description
                                         "cluster" (waiter-url->cluster (first waiter-urls))
                                         "cpus" 1
                                         "last-update-time" last-update-time-ms
                                         "last-update-user" "auth-user-0"
                                         "owner" "test-user"
                                         "previous" {"last-update-time" (- current-time-ms 30000)
                                                     "last-update-user" "foo-user"}
                                         "root" (first waiter-urls))
                    sync-result (->> (rest waiter-urls)
                                     (map-indexed
                                       (fn [index waiter-url]
                                         [waiter-url
                                          {:code :error/root-mismatch
                                           :details {:cluster (assoc basic-description
                                                                "cluster" (waiter-url->cluster waiter-url)
                                                                "cpus" (+ index 2)
                                                                "last-update-time" (- last-update-time-ms index 1)
                                                                "last-update-user" (str "auth-user-" (inc index))
                                                                "owner" "test-user"
                                                                "previous" {"last-update-time" (- current-time-ms 30000)
                                                                            "last-update-user" "foo-user"}
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
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doall
                  (map-indexed
                    (fn [index waiter-url]
                      (let [token-last-modified-time (- last-update-time-ms index)
                            token-etag (token->etag waiter-api waiter-url token-name)]
                        (is (= {:description (assoc basic-description
                                               "cluster" (waiter-url->cluster waiter-url)
                                               "cpus" (inc index)
                                               "last-update-time" token-last-modified-time
                                               "last-update-user" (str "auth-user-" index)
                                               "owner" "test-user"
                                               "previous" {"last-update-time" (- current-time-ms 30000)
                                                           "last-update-user" "foo-user"}
                                               "root" waiter-url)
                                :headers {"content-type" "application/json"
                                          "etag" token-etag}
                                :status 200
                                :token-etag token-etag}
                               (load-token waiter-url token-name)))))
                    waiter-urls))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-different-roots-but-same-last-update-user
  (testing "token sync update with different roots"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "diff-root-same-user" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil
                             (assoc basic-description
                               "cluster" (waiter-url->cluster waiter-url)
                               "cpus" (inc index)
                               "last-update-time" (- last-update-time-ms index)
                               "last-update-user" "auth-user"
                               "owner" "test-user"
                               "previous" {"last-update-time" (- current-time-ms 30000)
                                           "last-update-user" "foo-user"}
                               "root" waiter-url)))
              waiter-urls))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [latest-description (assoc basic-description
                                         "cluster" (waiter-url->cluster (first waiter-urls))
                                         "cpus" 1
                                         "last-update-time" last-update-time-ms
                                         "last-update-user" "auth-user"
                                         "owner" "test-user"
                                         "previous" {"last-update-time" (- current-time-ms 30000)
                                                     "last-update-user" "foo-user"}
                                         "root" (first waiter-urls))
                    sync-result (->> (rest waiter-urls)
                                  (map
                                    (fn [waiter-url]
                                      [waiter-url
                                       {:code :success/sync-update
                                        :details {:etag token-etag
                                                  :status 200}}]))
                                  (into {}))
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description latest-description
                                                                    :token-etag token-etag}
                                                           :sync-result sync-result}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doall
                  (map
                    (fn [waiter-url]
                      (let [token-etag (token->etag waiter-api waiter-url token-name)]
                        (is (= {:description latest-description
                                :headers {"content-type" "application/json"
                                          "etag" token-etag}
                                :status 200
                                :token-etag token-etag}
                               (load-token waiter-url token-name)))))
                    waiter-urls))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-different-roots-and-deleted
  (testing "token sync hard-delete deleted tokens with different different roots"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-different-roots-and-deleted-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil
                             (assoc basic-description
                               "cluster" (waiter-url->cluster waiter-url)
                               "cpus" (inc index)
                               "deleted" true
                               "last-update-time" (- last-update-time-ms index)
                               "last-update-user" "auth-user"
                               "owner" "test-user"
                               "previous" {"last-update-time" (- current-time-ms 30000)
                                           "last-update-user" "foo-user"}
                               "root" waiter-url)))
              waiter-urls))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [latest-description (assoc basic-description
                                         "cluster" (waiter-url->cluster (first waiter-urls))
                                         "cpus" 1
                                         "deleted" true
                                         "last-update-time" last-update-time-ms
                                         "last-update-user" "auth-user"
                                         "owner" "test-user"
                                         "previous" {"last-update-time" (- current-time-ms 30000)
                                                     "last-update-user" "foo-user"}
                                         "root" (first waiter-urls))
                    sync-result (pc/map-from-keys
                                  (constantly
                                    {:code :success/hard-delete
                                     :details {:etag ""
                                               :status 200}})
                                  waiter-urls)
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description latest-description
                                                                    :token-etag token-etag}
                                                           :sync-result sync-result}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doall
                  (map
                    (fn [waiter-url]
                      (is (= {:description {}
                              :headers {"content-type" "application/json"}
                              :status 404}
                             (load-token waiter-url token-name))))
                    waiter-urls))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-same-user-params-but-different-root-and-last-update-user
  (testing "token sync update with different difference only in roots and last-update-user"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-same-user-params-but-different-root-and-last-update-user-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil
                             (assoc basic-description
                               "cluster" (waiter-url->cluster waiter-url)
                               "last-update-time" (- last-update-time-ms index)
                               "last-update-user" (str "auth-user-" index)
                               "owner" "test-user"
                               "previous" {"last-update-time" (- current-time-ms 30000)
                                           "last-update-user" "foo-user"}
                               "root" waiter-url)))
              waiter-urls))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [latest-description (assoc basic-description
                                         "cluster" (waiter-url->cluster (first waiter-urls))
                                         "last-update-time" last-update-time-ms
                                         "last-update-user" "auth-user-0"
                                         "owner" "test-user"
                                         "previous" {"last-update-time" (- current-time-ms 30000)
                                                     "last-update-user" "foo-user"}
                                         "root" (first waiter-urls))
                    sync-result (pc/map-from-keys
                                  (constantly {:code :error/token-sync
                                               :details {:message "token contents match, but were edited by different users"}})
                                  (rest waiter-urls))
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description latest-description
                                                                    :token-etag token-etag}
                                                           :sync-result sync-result}}
                                     :summary {:sync {:failed #{token-name}
                                                      :unmodified #{}
                                                      :updated #{}}
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doall
                  (map-indexed
                    (fn [index waiter-url]
                      (let [token-etag (token->etag waiter-api waiter-url token-name)]
                        (is (= {:description (assoc basic-description
                                               "cluster" (waiter-url->cluster waiter-url)
                                               "last-update-time" (- last-update-time-ms index)
                                               "last-update-user" (str "auth-user-" index)
                                               "owner" "test-user"
                                               "previous" {"last-update-time" (- current-time-ms 30000)
                                                           "last-update-user" "foo-user"}
                                               "root" waiter-url)
                                :headers {"content-type" "application/json"
                                          "etag" token-etag}
                                :status 200
                                :token-etag token-etag}
                               (load-token waiter-url token-name)))))
                    waiter-urls))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-token-same-user-params-and-last-update-user-but-different-root
  (testing "token sync update with different difference only in roots"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          limit 10
          token-name (str "test-token-same-user-params-and-last-update-user-but-different-root-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [current-time-ms (System/currentTimeMillis)
              last-update-time-ms (- current-time-ms 10000)]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil
                             (assoc basic-description
                               "cluster" (waiter-url->cluster waiter-url)
                               "last-update-time" (- last-update-time-ms index)
                               "last-update-user" "auth-user"
                               "owner" "test-user"
                               "previous" {"last-update-time" (- current-time-ms 30000)
                                           "last-update-user" "foo-user"}
                               "root" waiter-url)))
              waiter-urls))

          (let [token-etag (token->etag waiter-api (first waiter-urls) token-name)]

            ;; ACT
            (let [actual-result (syncer/sync-tokens waiter-api waiter-urls limit)]

              ;; ASSERT
              (let [latest-description (assoc basic-description
                                         "cluster" (waiter-url->cluster (first waiter-urls))
                                         "last-update-time" last-update-time-ms
                                         "last-update-user" "auth-user"
                                         "owner" "test-user"
                                         "previous" {"last-update-time" (- current-time-ms 30000)
                                                     "last-update-user" "foo-user"}
                                         "root" (first waiter-urls))
                    sync-result (pc/map-from-keys
                                  (constantly {:code :success/skip-token-sync})
                                  (rest waiter-urls))
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description latest-description
                                                                    :token-etag token-etag}
                                                           :sync-result sync-result}}
                                     :summary {:sync {:failed #{}
                                                      :unmodified #{}
                                                      :updated #{token-name}}
                                               :tokens {:pending {:count 1 :value #{token-name}}
                                                        :previously-synced {:count 0 :value #{}}
                                                        :processed {:count 1 :value #{token-name}}
                                                        :selected {:count 1 :value #{token-name}}
                                                        :total {:count 1 :value #{token-name}}}}}]
                (is (= expected-result actual-result))
                (doall
                  (map-indexed
                    (fn [index waiter-url]
                      (let [token-etag (token->etag waiter-api waiter-url token-name)]
                        (is (= {:description (assoc basic-description
                                               "cluster" (waiter-url->cluster waiter-url)
                                               "last-update-time" (- last-update-time-ms index)
                                               "last-update-user" "auth-user"
                                               "owner" "test-user"
                                               "previous" {"last-update-time" (- current-time-ms 30000)
                                                           "last-update-user" "foo-user"}
                                               "root" waiter-url)
                                :headers {"content-type" "application/json"
                                          "etag" token-etag}
                                :status 200
                                :token-etag token-etag}
                               (load-token waiter-url token-name)))))
                    waiter-urls))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-ping-tokens
  (testing "token ping on clusters"
    (let [waiter-urls (waiter-urls)
          queue-timeout-ms 120000
          {:keys [store-token] :as waiter-api} (waiter-api)]

      (testing "successful health check"
        (let [token-name (str "test-ping-tokens-" (UUID/randomUUID))]
          (try
            ;; ARRANGE
            (doall
              (map (fn [waiter-url]
                     (->> (assoc basic-description
                            "health-check-url" "/status"
                            "idle-timeout-mins" 2
                            "run-as-user" "*"
                            "version" "lorem-ipsum")
                       (store-token waiter-url token-name nil)))
                   waiter-urls))

            ;; ACT
            (let [actual-result (ping/ping-token waiter-api waiter-urls token-name queue-timeout-ms)]

              ;; ASSERT
              (let [expected-result {:details
                                     (pc/map-from-keys
                                       (fn [cluster-url]
                                         {:exit-code 0
                                          :message (str "successfully pinged token " token-name " on " cluster-url
                                                        ", reason: health check returned status code 200")})
                                       waiter-urls)
                                     :exit-code 0
                                     :message (str "pinging token " token-name " on "
                                                   (-> waiter-urls vec println with-out-str str/trim)
                                                   " was successful")
                                     :token token-name}]
                (is (= expected-result actual-result))))
            (finally
              (cleanup-token waiter-api waiter-urls token-name)))))

      (testing "unsuccessful health check"
        (let [token-name (str "test-ping-tokens-" (UUID/randomUUID))]
          (try
            ;; ARRANGE
            (doall
              (map (fn [waiter-url]
                     (->> (assoc basic-description
                            "health-check-url" "/bad-status"
                            "idle-timeout-mins" 2
                            "run-as-user" "*"
                            "version" "lorem-ipsum")
                       (store-token waiter-url token-name nil)))
                   waiter-urls))

            ;; ACT
            (let [actual-result (ping/ping-token waiter-api waiter-urls token-name queue-timeout-ms)]

              ;; ASSERT
              (let [expected-result {:details
                                     (pc/map-from-keys
                                       (fn [cluster-url]
                                         {:exit-code 1
                                          :message (str "unable to ping token " token-name " on " cluster-url
                                                        ", reason: health check returned status code 503")})
                                       waiter-urls)
                                     :exit-code (count waiter-urls)
                                     :message (str "pinging token " token-name " on "
                                                   (-> waiter-urls vec println with-out-str str/trim)
                                                   " failed")
                                     :token token-name}]
                (is (= expected-result actual-result))))
            (finally
              (cleanup-token waiter-api waiter-urls token-name))))))))

(deftest ^:integration test-cleanup-token
  (testing "token cleanup on cluster"
    (let [cluster-url (first (waiter-urls))
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          token-name (str "test-cleanup-token-" (UUID/randomUUID))
          current-time-ms (System/currentTimeMillis)
          last-update-time-ms (- current-time-ms 10000)]

      ;; ARRANGE
      (store-token cluster-url token-name nil
                   (assoc basic-description
                     "cluster" (waiter-url->cluster cluster-url)
                     "cpus" 1.0
                     "deleted" true
                     "last-update-time" last-update-time-ms
                     "last-update-user" "auth-user"
                     "owner" "test-user"
                     "root" cluster-url))

      (try
        ;; ACT
        (let [actual-result (cleanup/cleanup-tokens waiter-api cluster-url current-time-ms)]

          ;; ASSERT
          (is (= #{token-name} actual-result))
          (is (= {:description {}
                  :headers {"content-type" "application/json"}
                  :status 404}
                 (load-token cluster-url token-name))))

        (finally
          (cleanup-token waiter-api [cluster-url] token-name))))))

(deftest ^:integration test-restore-updated-token
  (testing "token restore with outdated token"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          token-name (str "test-restore-updated-token-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [last-update-time-ms (- (System/currentTimeMillis) 10000)
              retrieve-token-description (fn [index waiter-url]
                                           (assoc basic-description
                                             "cluster" (waiter-url->cluster waiter-url)
                                             "cpus" (inc index)
                                             "last-update-time" (cond-> last-update-time-ms
                                                                  (zero? index) (- 10000))
                                             "last-update-user" (str "auth-user-" index)
                                             "owner" "test-user"
                                             "root" waiter-url))]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil (retrieve-token-description index waiter-url)))
              waiter-urls))

          (let [target-cluster-url (first waiter-urls)
                token-description (retrieve-token-description 1 (second waiter-urls))]

            ;; ACT
            (let [restore-result (restore/restore-tokens-from-backup
                                  waiter-api target-cluster-url false {token-name token-description})]

              ;; ASSERT
              (let [{:keys [description]} (load-token target-cluster-url token-name)]
                (is (= {:error 0 :skip 0 :success 1} restore-result))
                (is (= (retrieve-token-description 1 (second waiter-urls)) (dissoc description "previous")))
                (is (= (retrieve-token-description 0 target-cluster-url) (get description "previous")))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-restore-skip-new-token
  (testing "token restore with outdated token"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          token-name (str "test-restore-updated-token-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [last-update-time-ms (- (System/currentTimeMillis) 10000)
              retrieve-token-description (fn [index waiter-url]
                                           (assoc basic-description
                                             "cluster" (waiter-url->cluster waiter-url)
                                             "cpus" (inc index)
                                             "last-update-time" (cond-> last-update-time-ms
                                                                  (zero? index) (+ 10000))
                                             "last-update-user" (str "auth-user-" index)
                                             "owner" "test-user"
                                             "root" waiter-url))]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (store-token waiter-url token-name nil (retrieve-token-description index waiter-url)))
              waiter-urls))

          (let [target-cluster-url (first waiter-urls)
                token-description (retrieve-token-description 1 (second waiter-urls))]

            ;; ACT
            (let [restore-result (restore/restore-tokens-from-backup
                                   waiter-api target-cluster-url false {token-name token-description})]

              ;; ASSERT
              (let [{:keys [description]} (load-token target-cluster-url token-name)]
                (is (= {:error 0 :skip 1 :success 0} restore-result))
                (is (= (retrieve-token-description 0 target-cluster-url) (dissoc description "previous")))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-restore-missing-token
  (testing "token restore missing token"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          token-name (str "test-restore-missing-token-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [last-update-time-ms (- (System/currentTimeMillis) 10000)
              retrieve-token-description (fn [index waiter-url]
                                           (assoc basic-description
                                             "cluster" (waiter-url->cluster waiter-url)
                                             "cpus" (inc index)
                                             "last-update-time" (cond-> last-update-time-ms
                                                                  (zero? index) (- 10000))
                                             "last-update-user" (str "auth-user-" index)
                                             "owner" "test-user"
                                             "root" waiter-url))]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (when (pos? index)
                  (store-token waiter-url token-name nil (retrieve-token-description index waiter-url))))
              waiter-urls))

          (let [target-cluster-url (first waiter-urls)
                token-description (retrieve-token-description 1 (second waiter-urls))]

            ;; ACT
            (let [restore-result (restore/restore-tokens-from-backup
                                   waiter-api target-cluster-url false {token-name token-description})]

              ;; ASSERT
              (let [{:keys [description]} (load-token target-cluster-url token-name)]
                (is (= {:error 0 :skip 0 :success 1} restore-result))
                (is (= (retrieve-token-description 1 (second waiter-urls)) (dissoc description "previous")))
                (is (empty? (get description "previous")))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))

(deftest ^:integration test-restore-from-backup
  (testing "token restore missing token"
    (let [waiter-urls (waiter-urls)
          {:keys [load-token store-token] :as waiter-api} (waiter-api)
          token-name (str "test-restore-missing-token-" (UUID/randomUUID))]
      (try
        ;; ARRANGE
        (let [last-update-time-ms (- (System/currentTimeMillis) 10000)
              retrieve-token-description (fn [index waiter-url]
                                           (assoc basic-description
                                             "cluster" (waiter-url->cluster waiter-url)
                                             "cpus" (inc index)
                                             "last-update-time" (cond-> last-update-time-ms
                                                                  (zero? index) (- 10000))
                                             "last-update-user" (str "auth-user-" index)
                                             "owner" "test-user"
                                             "root" waiter-url))]

          (doall
            (map-indexed
              (fn [index waiter-url]
                (when (pos? index)
                  (store-token waiter-url token-name nil (retrieve-token-description index waiter-url))))
              waiter-urls))

          (let [target-cluster-url (first waiter-urls)
                source-cluster-url (second waiter-urls)
                my-temp-file (file-utils/create-temp-file)
                my-temp-file-path (file-utils/file->path my-temp-file)
                file-operations-api (file-utils/init-file-operations-api false)]
            (backup/backup-tokens waiter-api file-operations-api source-cluster-url my-temp-file-path false)

            ;; ACT
            (let [restore-result (restore/restore-tokens
                                   waiter-api file-operations-api target-cluster-url my-temp-file-path false)]

              ;; ASSERT
              (let [{:keys [description]} (load-token target-cluster-url token-name)]
                (is (= {:error 0 :skip 0 :success 1} restore-result))
                (is (= (retrieve-token-description 1 (second waiter-urls)) (dissoc description "previous")))
                (is (empty? (get description "previous")))))))
        (finally
          (cleanup-token waiter-api waiter-urls token-name))))))
