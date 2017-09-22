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
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [qbits.jet.client.http :as http]
            [token-syncer.syncer :as syncer]
            [token-syncer.waiter :as waiter])
  (:import (org.eclipse.jetty.client HttpClient))
  (:gen-class))

(defn ^HttpClient http-client-wrapper-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms use-spnego]}]
  (let [client (http/client {:connect-timeout connection-timeout-ms
                             :idle-timeout idle-timeout-ms
                             :follow-redirects? false})
        _ (.clear (.getContentDecoderFactories client))]
    {:http-client client
     :use-spnego use-spnego}))

(defn- setup-exception-handler
  "Sets up the UncaughtExceptionHandler."
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread throwable]
        (println (str (.getName thread) " threw exception: " (.getMessage throwable)))
        (.printStackTrace throwable)))))

(defn- configure-cli-options
  "Returns the cli options for the token syncer."
  []
  [["-c" "--cluster" "The comma-separated cluster urls"
    :default ""
    :parse-fn #(Integer/parseInt %)
    :validate [#(not (str/blank? %)) "Must be a non-empty string"]]
   ["-h" "--help"]
   ["-i" "--idle-timeout-ms timeout" "The idle timeout"
    :default 30000
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 300001) "Must be between 0 and 300000"]]
   ["-s" "--use-spnego true|false" "Whether or not to use spnego"
    :default false
    :parse-fn #(Boolean/parseBoolean %)]
   ["-t" "--connection-timeout-ms timeout" "The connection timeout"
    :default 1000
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 300001) "Must be between 0 and 300000"]]])

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main
  "The main entry point."
  [& args]
  (setup-exception-handler)
  (println "command-line arguments:" (vec args))
  (let [cli-options (configure-cli-options)
        {:keys [options summary]} (cli/parse-opts args cli-options)
        {:keys [cluster help use-spnego]} options
        cluster-urls (str/split cluster #",")]
    (try
      (if help
        (println summary)
        (do
          (when use-spnego
            (println "Using SPNEGO auth while communicating with Waiter clusters"))
          (when-not (seq cluster-urls) ;; TODO validate multiple urls
            (exit 1 "Missing cluster parameter!"))
          (let [http-client-wrapper (http-client-wrapper-factory options)]
            (syncer/sync-tokens http-client-wrapper cluster-urls))))
      (catch Exception e
        (.printStackTrace e)
        (exit 1 (str "Encountered error starting token-syncer: " (.getMessage e)))))))
