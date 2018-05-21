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
(ns token-syncer.commands.backup
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc])
  (:import (java.io File)))

(defn backup-tokens
  "Reads all the latest token descriptions from the specified cluster and writes it to the specified file.
   When accrete-mode is true, content is read from the specified file and merged before writing the new tokens."
  [{:keys [load-token load-token-list]} {:keys [read-from-file write-to-file]}
   cluster-url filename accrete-mode]
  (let [existing-token->token-description (read-from-file filename)
        index-entries (load-token-list cluster-url)
        token->etag (->> index-entries
                         (map (fn [{:strs [etag token]}] [token etag]))
                         (into (sorted-map)))
        _ (log/info "found" (count token->etag) "tokens on" cluster-url ":" (keys token->etag))
        current-token->token-description (pc/map-from-keys
                                           (fn [token]
                                             ;; use existing token information if etags match, else load from Waiter
                                             (let [existing-etag (get-in existing-token->token-description [token "token-etag"])]
                                               (if (and (not (str/blank? existing-etag))
                                                        (= existing-etag (token->etag token)))
                                                 (do
                                                   (log/info "using existing token description for" token "with etag" existing-etag)
                                                   (get existing-token->token-description token))
                                                 (-> (load-token cluster-url token)
                                                     (select-keys [:description :token-etag])
                                                     (walk/stringify-keys)))))
                                           (keys token->etag))]
    (->> (cond->> current-token->token-description
                  accrete-mode
                  (merge existing-token->token-description))
         (into (sorted-map))
         (write-to-file filename))))

(defn read-from-file
  "Reads the contents of the file as a json string."
  [filename]
  (if (.exists ^File (io/as-file filename))
    (do
      (log/info "reading contents from" filename)
      (let [file-content (slurp filename)
            result (if (str/blank? file-content) {} (json/read-str file-content))]
        (if (and (map? result)
                 (every? string? (keys result)))
          (do
            (log/info "found" (count result) "tokens in" filename ":" (keys result))
            result)
          (log/warn "contents of" filename "will be ignored as it is not a map with string keys:" result))))
    (log/warn "not reading contents from" filename "as it does not exist!")))

(defn write-to-file
  "Writes the content to the specified file as a json string."
  [filename content]
  (log/info "writing" (count content) "tokens to" filename ":" (keys content))
  (->> content
       json/write-str
       (spit filename)))

(defn init-file-operations-api
  "Creates the map of methods used to read/write tokens from/to a file."
  [dry-run]
  {:read-from-file read-from-file
   :write-to-file (if dry-run
                    (fn write-to-file-dry-run-version [filename token->token-description]
                      (log/info "[dry-run] writing to" filename "content:" token->token-description))
                    write-to-file)})

(def backup-tokens-config
  {:execute-command (fn execute-backup-tokens-command
                      [{:keys [waiter-api] :as parent-context} {:keys [options]} arguments]
                      (let [{:keys [accrete]} options]
                        (if-not (= (count arguments) 2)
                          {:exit-code 1
                           :message (str "expected 2 arguments FILE URL, provided " (count arguments) ": " (vec arguments))}
                          (let [dry-run (get-in parent-context [:options :dry-run])
                                file-operations-api (init-file-operations-api dry-run)
                                file (first arguments)
                                cluster-url (second arguments)]
                            (log/info "backing up tokens from" cluster-url "to" file
                                      (if accrete "in accretion mode" "in overwrite mode")
                                      (str (when dry-run "with dry-run enabled")))
                            (backup-tokens waiter-api file-operations-api cluster-url file accrete)
                            {:exit-code 0
                             :message "exiting with code 0"}))))
   :option-specs [["-a" "--accrete"
                   (str "Accrete new token descriptions into the backup file if it exists. "
                        "The default behavior (when this flag is not enabled) is overwriting the file with only the new token descriptions.")]]
   :retrieve-documentation (fn retrieve-sync-clusters-documentation
                             [command-name _]
                             {:description (str "Syncs tokens from a Waiter cluster specified in the URL to the FILE")
                              :usage (str command-name " [OPTION]... FILE URL")})})
