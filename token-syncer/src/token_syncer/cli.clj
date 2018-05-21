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
     `:command-name`: The name of the command that is being run.
     `:execute-command`: (fn [context options-map arguments] ...)
                           where `context` is the same as `parent-context`,
                                 `options-map` is the result of the call to `clojure.tools.cli/parse-opts`, and
                                 `arguments` is a list of unprocessed arguments (e.g. non-options from `args`).
     `:option-specs`: Option specifications as defined in `clojure.tools.cli/parse-opts`.
                      The help option (-h and --help) are always added while executing any command, hence these
                      options are reserved for use in the option specs.
     `:retrieve-documentation`: (fn [command-name parent-context summary] ...)
                                  where `command-name` is the name of the command,
                                        `parent-context` is a map that specifies the context, and
                                        `summary` is a string containing a minimal options summary.
                                  It should return a map containing the keys :description and :usage.
   `parent-context` is a map that specifies the context in which the command is running.
   `args` is the arguments passed to the command (including any options)."
  [config-map parent-context args]
  (let [{:keys [command-name execute-command option-specs retrieve-documentation]} config-map
        option-specs' (conj option-specs ["-h" "--help" "Displays this message"])
        options-map (tools-cli/parse-opts args option-specs' :in-order true)
        {:keys [arguments errors options summary]} options-map
        {:keys [help]} options]
    (cond
      help
      (let [{:keys [description usage]} (retrieve-documentation command-name parent-context)
            doc-string (str "Name: " command-name \newline
                            (when usage (str "Usage: " usage \newline))
                            (when description (str "Description: " description \newline))
                            "Options:" \newline summary)]
        (println doc-string)
        {:exit-code 0
         :message (str command-name ": displayed documentation")})

      (seq errors)
      (do
        (log/error "error in parsing commands for" command-name errors)
        (binding [*out* *err*]
          (println (str "error in parsing commands for " command-name ":"))
          (doseq [error-message errors]
            (println error-message)))
        {:data errors
         :exit-code 1
         :message (str command-name ": error in parsing arguments")})

      :else
      (-> (execute-command parent-context options-map arguments)
          (sanitize-result command-name)))))
