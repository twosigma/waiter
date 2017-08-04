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
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [qbits.jet.server :as server]
            [ring.middleware.params :as params]
            [token-syncer.core :as core]
            [token-syncer.correlation-id :as cid]
            [token-syncer.settings :as settings]
            [token-syncer.utils :as utils]
            [token-syncer.waiter :as waiter])
  (:gen-class))

(defn -main
  "The main entry point."
  [config & args]
  (log/info "command-line arguments:" (vec args))
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread throwable]
        (log/error throwable (str (.getName thread) " threw exception: " (.getMessage throwable))))))
  (let [cli-options [["-h" "--help"]
                     ["-p" "--port PORT" "Port number"
                      :default 9009
                      :parse-fn #(Integer/parseInt %)
                      :validate [#(< 0 % 0x10000) "Must be between 0 and 65536"]]]
        {:keys [options summary]} (cli/parse-opts args cli-options)
        {:keys [help port]} options
        git-version (utils/retrieve-git-version)
        settings (settings/load-settings config git-version)]
    (try
      (if help
        (println summary)
        (do
          (log/info "using config file:" config)
          (let [use-spnego (get-in settings [:http-client-properties :use-spnego])]
            (log/info (when-not use-spnego "NOT") "using SPNEGO auth while communicating with Waiter clusters")
            (deliver waiter/use-spnego-promise use-spnego))
          (let [username (System/getenv "WAITER_USERNAME")
                password (System/getenv "WAITER_PASSWORD")
                server-options {:port port
                                :request-header-size 32768
                                :ring-handler (->> (core/http-handler-factory settings)
                                                   params/wrap-params
                                                   (core/basic-auth-middleware username password)
                                                   cid/correlation-id-middleware)}]
            (server/run-jetty server-options))))
      (catch Exception e
        (log/fatal e "Encountered error starting token-syncer with" options)
        (System/exit 1)))))
