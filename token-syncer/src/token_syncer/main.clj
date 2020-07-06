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
(ns token-syncer.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [qbits.jet.client.http :as http]
            [token-syncer.cli :as cli]
            [token-syncer.commands.backup :as backup]
            [token-syncer.commands.cleanup :as cleanup]
            [token-syncer.commands.ping :as ping]
            [token-syncer.commands.restore :as restore]
            [token-syncer.commands.syncer :as syncer]
            [token-syncer.waiter :as waiter])
  (:import (org.eclipse.jetty.client HttpClient)))

(defn- setup-exception-handler
  "Sets up the UncaughtExceptionHandler."
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread throwable]
        (log/error throwable (str (.getName thread) " threw exception: " (.getMessage throwable)))))))

(defn exit
  "Helper function that prints the message and triggers a System exit."
  [status message]
  (if (zero? status)
    (log/info message)
    (log/error message))
  (System/exit status))

(defn ^HttpClient http-client-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms]}]
  (http/client {:connect-timeout connection-timeout-ms
                :idle-timeout idle-timeout-ms
                :follow-redirects? false}))

(defn init-waiter-api
  "Creates the map of methods used to interact with Waiter to load, store and delete tokens."
  [{:keys [dry-run] :as options}]
  (let [api-http-client (http-client-factory options)
        health-check-http-client (->> (assoc options :idle-timeout-ms 300000)
                                      (http-client-factory))]
    {:hard-delete-token (if dry-run
                          (fn hard-delete-dry-run-version [cluster-url token token-etag]
                            (log/info "[dry-run] hard-delete" token "on" cluster-url "with etag" token-etag)
                            {:status "dry-run"})
                          (partial waiter/hard-delete-token api-http-client))
     :health-check-token (partial waiter/health-check-token health-check-http-client)
     :load-token (partial waiter/load-token api-http-client)
     :load-token-list (partial waiter/load-token-list api-http-client)
     :store-token (if dry-run
                    (fn store-token-dry-run-version [cluster-url token token-etag token-description]
                      (log/info "[dry-run] store-token" token "on" cluster-url "with etag" token-etag
                                "and description" token-description)
                      {:status "dry-run"})
                    (partial waiter/store-token api-http-client))}))

(def base-command-config
  {:execute-command (fn execute-base-command
                      [{:keys [sub-command->config] :as context} {:keys [options]} arguments]
                      (if-not (seq arguments)
                        {:exit-code 1
                         :message "no sub-command specified"}
                        (let [sub-command (first arguments)]
                          (if-not (contains? sub-command->config sub-command)
                            {:exit-code 1
                             :message (str "unsupported sub-command: " sub-command)}
                            (do
                              (when (:dry-run options)
                                (log/info "executing token syncer in dry-run mode"))
                              (let [context' (assoc context
                                               :options options
                                               :waiter-api (init-waiter-api options))
                                    sub-command-config (-> (sub-command->config sub-command)
                                                           (assoc :command-name sub-command))]
                                (cli/process-command sub-command-config context' (rest arguments))))))))
   :option-specs [["-d" "--dry-run"
                   "Runs the syncer in dry run mode where it doesn't perform any write operations"]
                  ["-i" "--idle-timeout-ms TIMEOUT" "The idle timeout in milliseconds, must be between 1 and 300000"
                   :default 30000
                   :parse-fn #(Integer/parseInt %)
                   :validate [#(< 0 % 300001) "Must be between 0 and 300000"]]
                  ["-t" "--connection-timeout-ms TIMEOUT" "The connection timeout in milliseconds, must be between 1 and 300000"
                   :default 1000
                   :parse-fn #(Integer/parseInt %)
                   :validate [#(< 0 % 300001) "Must be between 1 and 300000"]]]
   :retrieve-documentation (fn retrieve-base-documentation
                             [command-name {:keys [sub-command->config]}]
                             {:description (str "delegates operations to the sub-commands (see Sub-commands section below)." \newline
                                                " Supported sub-commands: " (str/join ", " (-> sub-command->config keys sort)) "." \newline
                                                " Use '" command-name " SUB-COMMAND -h' to see documentation on the sub-command(s).")
                              :usage (str command-name " [OPTION]... SUB-COMMAND [OPTION]...")})})

(defn -main
  "The main entry point."
  [& args]
  (setup-exception-handler)
  (try
    (log/info "command-line arguments:" (vec args))
    (let [token-syncer-command-config (assoc base-command-config :command-name "token-syncer")
          context {:sub-command->config {"backup-tokens" backup/backup-tokens-config
                                         "cleanup-tokens" cleanup/cleanup-tokens-config
                                         "ping-token" ping/ping-token-config
                                         "restore-tokens" restore/restore-tokens-config
                                         "sync-clusters" syncer/sync-clusters-config}}
          {:keys [exit-code message]} (cli/process-command token-syncer-command-config context args)]
      (exit exit-code message))
    (catch Exception e
      (log/error e "error in syncing tokens")
      (exit 1 (str "encountered error running token-syncer: " (.getMessage e))))))
