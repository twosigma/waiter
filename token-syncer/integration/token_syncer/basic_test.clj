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
  (:require [clojure.core.async :as async]
            [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [qbits.jet.client.http :as http]
            [token-syncer.core :as core]
            [token-syncer.waiter :as waiter])
  (:import (java.net URI)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.client.util BasicAuthentication$BasicResult)))

(defn waiter-urls []
  (let [waiter-uris (System/getenv "WAITER_URIS")]
    (is waiter-uris)
    (->> (str/split waiter-uris #";")
         (map #(str "http://" %)))))

(defn syncer-url []
  (let [syncer-uri (System/getenv "SYNCER_URI")]
    (is syncer-uri)
    (str "http://" syncer-uri)))

(defn execute-command [& args]
  (let [shell-output (apply shell/sh args)]
    (when (not= 0 (:exit shell-output))
      (log/info (str "Error in running command: " (str/join " " args)))
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
    (log/info "env.SYNCER_URI" (System/getenv "SYNCER_URI"))
    (is (System/getenv "SYNCER_URI"))

    (log/info "env.WAITER_PASSWORD" (System/getenv "WAITER_PASSWORD"))
    (is (System/getenv "WAITER_PASSWORD"))

    (log/info "env.WAITER_URIS" (System/getenv "WAITER_URIS"))
    (is (System/getenv "WAITER_URIS"))

    (log/info "env.WAITER_USERNAME" (System/getenv "WAITER_USERNAME"))
    (is (System/getenv "WAITER_USERNAME"))))

(deftest ^:integration test-token-hard-deletes
  (testing "token sync hard-delete"
    (deliver waiter/use-spnego-promise (Boolean/parseBoolean (System/getenv "USE_SPNEGO")))
    (let [waiter-urls (waiter-urls)
          syncer-url (syncer-url)
          http-client (core/http-client-factory {:connection-timeout-ms 5000, :idle-timeout-ms 5000})
          token-names ["test-token-hard-deletes-1" "test-token-hard-deletes-2" "test-token-hard-deletes-3"]
          token-description {"cpus" 1, "deleted" true, "mem" 2048, "owner" (retrieve-username)}]
      (doseq [waiter-url waiter-urls]
        (doseq [token-name token-names]
          (let [token-description (assoc token-description "name" token-name)]
            (waiter/store-token-on-cluster http-client waiter-url token-name (dissoc token-description "deleted"))
            (waiter/store-token-on-cluster http-client waiter-url token-name token-description))))

      (let [sync-response-chan (make-syncer-request http-client (str syncer-url "/sync-tokens")
                                                    :query-params {"cluster-url" (vec waiter-urls)})
            sync-response (async/<!! sync-response-chan)
            sync-response-body (async/<!! (:body sync-response))]
        (println sync-response-body))

      )))