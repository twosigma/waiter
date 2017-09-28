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
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [clojure.string :as str]
            [qbits.jet.client.http :as http]
            [token-syncer.syncer :as syncer]
            [token-syncer.waiter :as waiter]
            [plumbing.core :as pc])
  (:import (org.eclipse.jetty.client HttpClient)))

(defn waiter-urls []
  (let [waiter-uris (System/getenv "WAITER_URIS")]
    (is waiter-uris)
    (-> waiter-uris
        (str/split #";")
        sort)))

(defn execute-command [& args]
  (let [shell-output (apply shell/sh args)]
    (when (not= 0 (:exit shell-output))
      (println (str "Error in running command: " (str/join " " args)))
      (throw (IllegalStateException. (str (:err shell-output)))))
    (str/trim (:out shell-output))))

(defn retrieve-username []
  (execute-command "id" "-un"))

(deftest ^:integration test-environment
  (println "****** Running: test-environment")
  (testing "presence of environment variables"
    (println "env.WAITER_URIS" (System/getenv "WAITER_URIS"))
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
      (get :last-modified-etag)))

(defn- cleanup-token
  [http-client waiter-urls token-name]
  (println "Cleaning up token:" token-name)
  (with-out-str
    (doseq [waiter-url waiter-urls]
      (try
        (let [last-modified-etag (token->etag http-client waiter-url token-name)]
          (waiter/hard-delete-token http-client waiter-url token-name last-modified-etag))
        (catch Exception _)))))

(deftest ^:integration test-token-hard-delete
  (testing "token sync hard-delete"
    (let [waiter-urls (waiter-urls)
          http-client (http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})
          token-name (str "test-token-hard-delete-" (rand-int 10000))]
      (try
        (println "****** test-token-hard-delete ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-description {"cpus" 1, "deleted" true, "mem" 2048, "name" token-name,
                                 "owner" (retrieve-username), "last-update-time" current-time-ms}]

          (doseq [waiter-url waiter-urls]
            (waiter/store-token http-client waiter-url token-name "0" token-description))

          (let [last-modified-etag (token->etag http-client (first waiter-urls) token-name)]

            (println "****** test-token-hard-delete ACT")
            (let [actual-result (syncer/sync-tokens http-client (vec waiter-urls))]

              (println "****** test-token-hard-delete ASSERT")
              (let [waiter-sync-result (constantly
                                         {:message :successfully-hard-deleted-token-on-cluster
                                          :response {:body {"delete" token-name, "hard-delete" true, "success" true}
                                                     :headers {"content-type" "application/json"}
                                                     :status 200}})
                    expected-result {:num-tokens-processed 1,
                                     :result {token-name {:description {:cluster-url (first waiter-urls)
                                                                        :description token-description
                                                                        :last-modified-etag last-modified-etag}
                                                          :sync-result (pc/map-from-keys waiter-sync-result waiter-urls)}}}]
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
          token-name (str "test-token-soft-delete-" (rand-int 10000))]
      (try
        (println "****** test-token-soft-delete ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-description {"cpus" 1, "mem" 2048, "name" token-name,
                                 "owner" (retrieve-username), "last-update-time" current-time-ms}]

          (waiter/store-token http-client (first waiter-urls) token-name "0" (assoc token-description "deleted" true))
          (doseq [waiter-url (rest waiter-urls)]
            (let [last-update-time-ms (- current-time-ms 10000)]
              (waiter/store-token http-client waiter-url token-name "0"
                                  (assoc token-description "last-update-time" last-update-time-ms))))

          (let [last-modified-etag (token->etag http-client (first waiter-urls) token-name)]

            (println "****** test-token-soft-delete ACT")
            (let [actual-result (syncer/sync-tokens http-client (vec waiter-urls))]

              (println "****** test-token-soft-delete ASSERT")
              (let [waiter-sync-result (constantly
                                         {:message :soft-delete-token-on-cluster
                                          :response {:body {"message" (str "Successfully created " token-name),
                                                            "service-description" (dissoc token-description "owner" "last-update-time")}
                                                     :headers {"content-type" "application/json"
                                                               "etag" last-modified-etag}
                                                     :status 200}})
                    expected-result {:num-tokens-processed 1,
                                     :result {token-name {:description {:cluster-url (first waiter-urls)
                                                                        :description (assoc token-description "deleted" true)
                                                                        :last-modified-etag last-modified-etag}
                                                          :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description (assoc token-description "deleted" true)
                          :headers {"content-type" "application/json"
                                    "etag" last-modified-etag}
                          :last-modified-etag last-modified-etag
                          :status 200}
                         (waiter/load-token http-client waiter-url token-name))))))))
        (finally
          (cleanup-token http-client waiter-urls token-name))))))

(deftest ^:integration test-token-update
  (testing "token sync update"
    (let [waiter-urls (waiter-urls)
          http-client (http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})
          token-name (str "test-token-update-" (rand-int 10000))]
      (try
        (println "****** test-token-update ARRANGE")
        (let [current-time-ms (System/currentTimeMillis)
              token-description {"cpus" 1, "mem" 4096, "name" token-name,
                                 "owner" (retrieve-username), "last-update-time" current-time-ms}]

          (let [last-update-time-ms (- current-time-ms 10000)]
            (waiter/store-token http-client (first waiter-urls) token-name "0" token-description)
            (doseq [waiter-url (rest waiter-urls)]
              (waiter/store-token http-client waiter-url token-name "0"
                                  (assoc token-description "cpus" 2, "mem" 2048, "last-update-time" last-update-time-ms))))

          (let [last-modified-etag (token->etag http-client (first waiter-urls) token-name)]

            (println "****** test-token-update ACT")
            (let [actual-result (syncer/sync-tokens http-client (vec waiter-urls))]

              (println "****** test-token-update ASSERT")
              (let [waiter-sync-result (constantly
                                         {:message :sync-token-on-cluster
                                          :response {:body {"message" (str "Successfully created " token-name),
                                                            "service-description" (dissoc token-description "owner" "last-update-time")}
                                                     :headers {"content-type" "application/json"
                                                               "etag" last-modified-etag}
                                                     :status 200}})
                    expected-result {:num-tokens-processed 1,
                                     :result {token-name {:description {:cluster-url (first waiter-urls)
                                                                        :description token-description
                                                                        :last-modified-etag last-modified-etag}
                                                          :sync-result (pc/map-from-keys waiter-sync-result (rest waiter-urls))}}}]
                (is (= expected-result actual-result))
                (doseq [waiter-url waiter-urls]
                  (is (= {:description token-description
                          :headers {"content-type" "application/json"
                                    "etag" last-modified-etag}
                          :last-modified-etag last-modified-etag
                          :status 200}
                         (waiter/load-token http-client waiter-url token-name))))))))
        (finally
          (cleanup-token http-client waiter-urls token-name))))))
