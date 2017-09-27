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
            [token-syncer.syncer :as syncer])
  (:import (org.eclipse.jetty.client HttpClient))
  (:gen-class))

(defn ^HttpClient http-client-wrapper-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms]}]
  (let [http-client (http/client {:connect-timeout connection-timeout-ms
                                  :idle-timeout idle-timeout-ms
                                  :follow-redirects? false})
        _ (.clear (.getContentDecoderFactories http-client))]
    http-client))

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
  (->> [["-c" "--cluster-urls urls" "The semi-colon separated cluster urls"
         :default []
         :parse-fn #(str/split % #";")
         :validate [#(> (count (set %)) 1) "Must provide at least two different semi-colon separated cluster urls"]]
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

(defn -main
  "The main entry point."
  [& args]
  (setup-exception-handler)
  (log/info "Command-line arguments:" (vec args))
  (let [{:keys [errors options summary]} (parse-cli-options args)
        {:keys [cluster-urls help]} options]
    (try
      (if help
        (log/info summary)
        (do
          (when (seq errors)
            (doseq [error-message errors]
              (log/error error-message))
            (exit 1 "Error in parsing arguments"))
          (let [http-client-wrapper (http-client-wrapper-factory options)
                sync-result (syncer/sync-tokens http-client-wrapper cluster-urls)]
            (log/info (-> sync-result pp/pprint with-out-str str/trim))
            (exit 0 "Exiting."))))
      (catch Exception e
        (log/error e "Error in syncing tokens")
        (exit 1 (str "Encountered error starting token-syncer: " (.getMessage e)))))))
