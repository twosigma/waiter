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
    (str/split waiter-uris #";")))

(defn execute-command [& args]
  (let [shell-output (apply shell/sh args)]
    (when (not= 0 (:exit shell-output))
      (println (str "Error in running command: " (str/join " " args)))
      (throw (IllegalStateException. (str (:err shell-output)))))
    (str/trim (:out shell-output))))

(defn retrieve-username []
  (execute-command "id" "-un"))

(deftest ^:integration test-environment
  (testing "presence of environment variables"
    (println "Running: test-environment")
    (println "env.WAITER_URIS" (System/getenv "WAITER_URIS"))
    (is (System/getenv "WAITER_URIS"))
    (is (> (count (waiter-urls)) 1))))

(defn- ^HttpClient http-client-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms]}]
  (let [client (http/client {:connect-timeout connection-timeout-ms
                             :idle-timeout idle-timeout-ms
                             :follow-redirects? false})
        _ (.clear (.getContentDecoderFactories client))]
    {:http-client client
     :use-spnego (Boolean/parseBoolean (System/getenv "USE_SPNEGO"))}))

(deftest ^:integration test-token-hard-delete
  (testing "token sync hard-delete"
    (let [waiter-urls (waiter-urls)
          http-client-wrapper (http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})]
      (println "Running: test-token-hard-delete")
      (let [token-name "test-token-hard-deletes-1"
            token-description {"cpus" 1, "deleted" true, "mem" 2048, "name" token-name,
                               "owner" (retrieve-username), "last-update-time" (System/currentTimeMillis)}]

        (doseq [waiter-url waiter-urls]
          (waiter/store-token-on-cluster http-client-wrapper waiter-url token-name token-description))

        (let [actual-result (syncer/sync-tokens http-client-wrapper (vec waiter-urls))
              waiter-sync-result (constantly
                                   {:message :successfully-hard-deleted-token-on-cluster
                                    :response {:body {"delete" token-name, "hard-delete" true, "success" true}
                                               :status 200}})
              expected-result {:num-tokens-processed 1,
                               :result {"test-token-hard-deletes-1"
                                        {:description {:cluster-url (first waiter-urls), :description token-description}
                                         :sync-result (pc/map-from-keys waiter-sync-result waiter-urls)}}}]
          (clojure.pprint/pprint actual-result)
          (is (= expected-result actual-result)))))))