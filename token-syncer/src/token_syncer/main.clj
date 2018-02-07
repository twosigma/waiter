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
(ns token-syncer.main
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [qbits.jet.client.http :as http]
            [token-syncer.syncer :as syncer]
            [token-syncer.waiter :as waiter])
  (:import (org.eclipse.jetty.client HttpClient))
  (:gen-class))

(defn ^HttpClient http-client-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms]}]
  (http/client {:connect-timeout connection-timeout-ms
                :idle-timeout idle-timeout-ms
                :follow-redirects? false}))

(defn- setup-exception-handler
  "Sets up the UncaughtExceptionHandler."
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread throwable]
        (log/error throwable (str (.getName thread) " threw exception: " (.getMessage throwable)))))))

(defn parse-cli-options
  "Parses and returns the cli options passed to the token syncer."
  [args]
  (->> [["-d" "--dry-run"
         "Runs the syncer in dry run mode where it doesn't perform any token delete or update operations"]
        ["-h" "--help"]
        ["-i" "--idle-timeout-ms timeout" "The idle timeout in milliseconds"
         :default 30000
         :parse-fn #(Integer/parseInt %)
         :validate [#(< 0 % 300001) "Must be between 0 and 300000"]]
        ["-t" "--connection-timeout-ms timeout" "The connection timeout in milliseconds"
         :default 1000
         :parse-fn #(Integer/parseInt %)
         :validate [#(< 0 % 300001) "Must be between 0 and 300000"]]]
       (cli/parse-opts args)))

(defn exit
  "Helper function that prints the message and triggers a System exit."
  [status message]
  (if (zero? status)
    (log/info message)
    (log/error message))
  (System/exit status))

(defn extract-waiter-functions
  "Creates the map of methods used to interact with Waiter to load, store and delete tokens."
  [{:keys [dry-run] :as options}]
  (let [http-client (http-client-factory options)]
    {:hard-delete-token (if dry-run
                          (fn hard-delete-dry-run-version [cluster-url token token-etag]
                            (log/info "[dry-run] hard-delete" token "on" cluster-url "with etag" token-etag)
                            {:status "dry-run"})
                          (partial waiter/hard-delete-token http-client))
     :load-token (partial waiter/load-token http-client)
     :load-token-list (partial waiter/load-token-list http-client)
     :store-token (if dry-run
                    (fn store-token-dry-run-version [cluster-url token token-etag token-description]
                      (log/info "[dry-run] store-token" token "on" cluster-url "with etag" token-etag
                                "and description" token-description)
                      {:status "dry-run"})
                    (partial waiter/store-token http-client))}))

(defn -main
  "The main entry point."
  [& args]
  (setup-exception-handler)
  (log/info "command-line arguments:" (vec args))
  (let [{:keys [arguments errors options summary]} (parse-cli-options args)
        {:keys [help]} options
        cluster-urls arguments]
    (try
      (cond
        help
        (log/info summary)

        (seq errors)
        (do
          (doseq [error-message errors]
            (log/error error-message)
            (println System/err error-message))
          (exit 1 "error in parsing arguments"))

        (<= (-> cluster-urls set count) 1)
        (exit 1 (str "must provide at least two different cluster urls, provided:" cluster-urls))

        :else
        (do
          (when (:dry-run options)
            (log/info "executing token syncer in dry-run mode"))
          (let [waiter-functions (extract-waiter-functions options)
                cluster-urls-set (set cluster-urls)
                sync-result (syncer/sync-tokens waiter-functions cluster-urls-set)
                exit-code (-> (get-in sync-result [:summary :sync :error] 0)
                              zero?
                              (if 0 1))]
            (log/info (-> sync-result pp/pprint with-out-str str/trim))
            (exit exit-code (str "exiting with code " exit-code)))))
      (catch Exception e
        (log/error e "error in syncing tokens")
        (exit 1 (str "encountered error starting token-syncer: " (.getMessage e)))))))
