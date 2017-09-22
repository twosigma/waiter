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
            [token-syncer.waiter :as waiter])
  (:import (java.net URI)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.client.util BasicAuthentication$BasicResult)))

(defn waiter-urls []
  (let [waiter-uris (System/getenv "WAITER_URIS")]
    (is waiter-uris)
    (->> (str/split waiter-uris #";")
         (map #(str "http://" %)))))

(defn execute-command [& args]
  (let [shell-output (apply shell/sh args)]
    (when (not= 0 (:exit shell-output))
      (println (str "Error in running command: " (str/join " " args)))
      (throw (IllegalStateException. (str (:err shell-output)))))
    (str/trim (:out shell-output))))

(defn retrieve-username []
  (execute-command "id" "-un"))

(defn make-syncer-request
  "Makes an asynchronous request to the request-url."
  [^HttpClient http-client request-url &
   {:keys [body headers method query-params]
    :or {body ""
         headers {}
         method http/get
         query-params {}}}]
  (method http-client request-url
          {:auth (BasicAuthentication$BasicResult. (URI. request-url)
                                                   (System/getenv "WAITER_USERNAME")
                                                   (System/getenv "WAITER_PASSWORD"))
           :body body
           :headers headers
           :fold-chunked-response? true
           :follow-redirects? false
           :query-string query-params}))

(deftest ^:integration test-environment
  (testing "presence of environment variables"
    (println "env.SYNCER_URI" (System/getenv "SYNCER_URI"))
    (is (System/getenv "SYNCER_URI"))

    (println "env.WAITER_PASSWORD" (System/getenv "WAITER_PASSWORD"))
    (is (System/getenv "WAITER_PASSWORD"))

    (println "env.WAITER_URIS" (System/getenv "WAITER_URIS"))
    (is (System/getenv "WAITER_URIS"))

    (println "env.WAITER_USERNAME" (System/getenv "WAITER_USERNAME"))
    (is (System/getenv "WAITER_USERNAME"))))

(defn- ^HttpClient http-client-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms]}]
  (let [client (http/client {:connect-timeout connection-timeout-ms
                             :idle-timeout idle-timeout-ms
                             :follow-redirects? false})
        _ (.clear (.getContentDecoderFactories client))]
    {:http-client client
     :use-spnego (Boolean/parseBoolean (System/getenv "USE_SPNEGO"))}))

(deftest ^:integration test-token-hard-deletes
  (testing "token sync hard-delete"
    (let [waiter-urls (waiter-urls)
          http-client-wrapper (http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})
          token-names ["test-token-hard-deletes-1" "test-token-hard-deletes-2" "test-token-hard-deletes-3"]
          token-description {"cpus" 1, "deleted" true, "mem" 2048, "owner" (retrieve-username)}]
      (doseq [waiter-url waiter-urls]
        (doseq [token-name token-names]
          (let [token-description (assoc token-description "name" token-name)]
            (waiter/store-token-on-cluster http-client-wrapper waiter-url token-name (dissoc token-description "deleted"))
            (waiter/store-token-on-cluster http-client-wrapper waiter-url token-name token-description))))

      (let [sync-response (syncer/sync-tokens http-client-wrapper (vec waiter-urls))]
        (println sync-response)) ;; TODO
      )))