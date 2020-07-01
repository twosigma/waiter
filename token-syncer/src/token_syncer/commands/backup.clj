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
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [token-syncer.file-utils :as file-utils]))

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
        _ (log/info "found" (count token->etag) "token(s) on" cluster-url ":" (keys token->etag))
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
                                           (keys token->etag))
        backup-result (into (sorted-map)
                            (cond->> current-token->token-description
                              accrete-mode (merge existing-token->token-description)))]
    (log/info "backup has" (count backup-result) "token(s):" (keys backup-result))
    (write-to-file filename backup-result)))

(def backup-tokens-config
  {:execute-command (fn execute-backup-tokens-command
                      [{:keys [waiter-api] :as parent-context} {:keys [options]} arguments]
                      (let [{:keys [accrete]} options]
                        (if-not (= (count arguments) 2)
                          {:exit-code 1
                           :message (str "expected 2 arguments FILE URL, provided " (count arguments) ": " (vec arguments))}
                          (let [dry-run (get-in parent-context [:options :dry-run])
                                file-operations-api (file-utils/init-file-operations-api dry-run)
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
