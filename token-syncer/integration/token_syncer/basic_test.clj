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
            [qbits.jet.client.http :as http]
            [token-syncer.syncer :as syncer]
            [token-syncer.waiter :as waiter])
  (:import (java.util UUID)
           (org.eclipse.jetty.client HttpClient)))

(defn waiter-urls []
  (let [waiter-uris (System/getenv "WAITER_URIS")]
    (is waiter-uris)
    (-> waiter-uris
        (str/split #",")
        sort)))

(deftest ^:integration test-environment
  (log/info "****** Running: test-environment")
  (testing "presence of environment variables"
    (log/info "env.WAITER_URIS" (System/getenv "WAITER_URIS"))
    (is (System/getenv "WAITER_URIS"))
    (is (> (count (waiter-urls)) 1))))

(defn- ^HttpClient http-client-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms]}]
  (let [http-client (http/client {:connect-timeout connection-timeout-ms
                                  :idle-timeout idle-timeout-ms
                                  :follow-redirects? false})
        _ (.clear (.getContentDecoderFactories http-client))]
    http-client))

(defn- token->etag
  "Retrieves the etag for a token on a waiter router."
  [http-client waiter-url token-name]
  (-> (waiter/load-token http-client waiter-url token-name)
      (get :token-etag)
      (or 0) ;; Waiter defaults etag to 0
      str))

(defn- cleanup-token
  [http-client waiter-urls token-name]
  (log/info "Cleaning up token:" token-name)
  (with-out-str
    (doseq [waiter-url waiter-urls]
      (try
        (let [token-etag (token->etag http-client waiter-url token-name)]
          (waiter/hard-delete-token http-client waiter-url token-name token-etag))
        (catch Exception _)))))

(deftest ^:integration test-token-hard-delete
  (testing "token sync hard-delete"
    (let [waiter-urls (waiter-urls)
          http-client (http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})
          token-name (str "test-token-hard-delete-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-hard-delete ARRANGE")
        (let [last-update-time-ms (- (System/currentTimeMillis) 10000)
              token-description (merge {"cmd" "echo 'Hello World'", "cpus" 1, "mem" 2048, "name" token-name}
                                       {"deleted" true, "last-update-time" last-update-time-ms, "owner" "test-user"})]

          (doseq [waiter-url waiter-urls]
            (let [token-etag (token->etag http-client waiter-url token-name)]
              (waiter/store-token http-client waiter-url token-name token-etag token-description)))

          (let [token-etag (token->etag http-client (first waiter-urls) token-name)]

            (log/info "****** test-token-hard-delete ACT")
            (let [actual-result (syncer/sync-tokens http-client waiter-urls)]

              (log/info "****** test-token-hard-delete ASSERT")
              (let [waiter-sync-result (constantly
                                         {:code :success/hard-delete
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description token-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result waiter-urls)}}
                                     :summary {:sync {:error []
                                                      :success [token-name]}
                                               :tokens {:processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (let [response (waiter/load-token http-client waiter-url token-name)]
                    (is (= 404 (:status response)) (str waiter-url " responded with " response))))))))
        (finally
          (cleanup-token http-client waiter-urls token-name))))))

(deftest ^:integration test-token-soft-delete
  (testing "token sync soft-delete"
    (let [waiter-urls (waiter-urls)
          http-client (http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})
          token-name (str "test-token-soft-delete-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-soft-delete ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-description (merge {"cmd" "echo 'Hello World'", "cpus" 1, "mem" 2048, "name" token-name}
                                       {"last-update-time" current-time-ms, "owner" "test-user"})]

          (waiter/store-token http-client (first waiter-urls) token-name "0" (assoc token-description "deleted" true))
          (doseq [waiter-url (rest waiter-urls)]
            (let [last-update-time-ms (- current-time-ms 10000)]
              (waiter/store-token http-client waiter-url token-name "0"
                                  (assoc token-description "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag http-client (first waiter-urls) token-name)]

            (log/info "****** test-token-soft-delete ACT")
            (let [actual-result (syncer/sync-tokens http-client waiter-urls)]

              (log/info "****** test-token-soft-delete ASSERT")
              (let [waiter-sync-result (constantly
                                         {:code :success/soft-delete
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description (assoc token-description "deleted" true)
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:error []
                                                      :success [token-name]}
                                               :tokens {:processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description (assoc token-description "deleted" true)
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (waiter/load-token http-client waiter-url token-name))))))))
        (finally
          (cleanup-token http-client waiter-urls token-name))))))

(deftest ^:integration test-token-update
  (testing "token sync update"
    (let [waiter-urls (waiter-urls)
          http-client (http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})
          token-name (str "test-token-update-" (UUID/randomUUID))]
      (try
        (log/info "****** test-token-update ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-description (merge {"cmd" "echo 'Hello World'", "cpus" 1, "mem" 4096, "name" token-name}
                                       {"last-update-time" current-time-ms, "owner" "test-user"})]

          (let [last-update-time-ms (- current-time-ms 10000)]
            (waiter/store-token http-client (first waiter-urls) token-name "0" token-description)
            (doseq [waiter-url (rest waiter-urls)]
              (waiter/store-token http-client waiter-url token-name "0"
                                  (assoc token-description "cpus" 2, "mem" 2048, "last-update-time" last-update-time-ms))))

          (let [token-etag (token->etag http-client (first waiter-urls) token-name)]

            (log/info "****** test-token-update ACT")
            (let [actual-result (syncer/sync-tokens http-client waiter-urls)]

              (log/info "****** test-token-update ASSERT")
              (let [waiter-sync-result (constantly
                                         {:code :success/sync-update
                                          :details {:etag token-etag
                                                    :status 200}})
                    expected-result {:details {token-name {:latest {:cluster-url (first waiter-urls)
                                                                    :description token-description
                                                                    :token-etag token-etag}
                                                           :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}
                                     :summary {:sync {:error []
                                                      :success [token-name]}
                                               :tokens {:processed 1
                                                        :total 1}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" token-etag}
                          :status 200
                          :token-etag token-etag}
                         (waiter/load-token http-client waiter-url token-name))))))))
        (finally
          (cleanup-token http-client waiter-urls token-name))))))
