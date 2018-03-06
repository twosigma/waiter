;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns token-syncer.cli
  (:require [clojure.tools.cli :as tools-cli]
            [clojure.tools.logging :as log]))

(defn- sanitize-result
  "Sanitizes the result map to include entries for exit-code and message."
  [result command-name]
  (-> result
      (update :exit-code (fn [exit-code]
                           (cond
                             (nil? exit-code)
                             (do
                               (log/info command-name "exit code is defaulting to 0")
                               0)
                             :else exit-code)))
      (update :message (fn [message]
                         (as-> message m
                               (or m "exiting")
                               (str command-name ": " m))))))

(defn process-command
  "Processes a command line option using the specified configuration in `config-map` and returns a map that is
   guaranteed to contain the following keys: exit-code and message.

   `config-map` is a map with the following keys:
     `build-context`: (fn [context options-map])
                        where `context` is the same as `parent-context` and
                              `options-map` is the result of the call to `clojure.tools.cli/parse-opts`.
     `command-name`: The name of the command that is being run.
     `execute-command`: (fn [context arguments] ...)
                          where `context` is the result of the call to `build-context` and
                                `arguments` is a list of unprocessed arguments (e.g. non-options from `args`).
     `option-specs`: Option specifications as defined in `clojure.tools.cli/parse-opts`.
                     The help option (-h and --help) are always added while executing any command, hence these
                     options are reserved for use in the option specs.
     `retrieve-documentation`: (fn [command-name parent-context summary] ...)
                                 where `command-name` is the name of the command,
                                       `parent-context` is a map that specifies the context, and
                                       `summary` is a string containing a minimal options summary.
   `parent-context` is a map that specifies the context in which the command is running.
   `args` is the arguments passed to the command (including any options)."
  [config-map parent-context args]
  (let [{:keys [build-context command-name execute-command option-specs retrieve-documentation]} config-map
        option-specs' (conj option-specs ["-h" "--help" "Displays this message"])
        options-map (tools-cli/parse-opts args option-specs' :in-order true)
        {:keys [arguments errors options summary]} options-map
        {:keys [help]} options]
    (cond
      help
      (do
        (.println System/out (retrieve-documentation command-name parent-context summary))
        {:exit-code 0
         :message (str command-name ": displayed documentation")})

      (seq errors)
      (do
        (log/error "error in parsing commands for" command-name errors)
        (.println System/err (str "error in parsing commands for " command-name ":"))
        (doseq [error-message errors]
          (.println System/err error-message))
        {:data errors
         :exit-code 1
         :message (str command-name ": error in parsing arguments")})

      :else
      (-> (build-context parent-context options-map)
          (execute-command arguments)
          (sanitize-result command-name)))))
