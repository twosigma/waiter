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
(ns token-syncer.commands.restore
  (:require [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [token-syncer.file-utils :as file-utils]
            [token-syncer.utils :as utils]))

(defn restore-tokens-from-backup
  "Writes the provided tokens in backup-token->token-description to the specified cluster.
   When force-mode? is true, content from the specified file is force written to the cluster even if the token was updated.
   When force-mode? is false, this function effectively restores deleted tokens.
   Return the summary containing error, skip and success counts of the restore."
  [{:keys [load-token-list store-token]} cluster-url force-mode? backup-token->token-description]
  (let [token->current-metadata (pc/for-map
                                  [{:strs [etag last-update-time owner token]} (load-token-list cluster-url)]
                                  token
                                  {:etag etag
                                   :last-update-time (some-> last-update-time utils/iso8601->millis)
                                   :owner owner})
        error-counter (atom 0)
        skip-counter (atom 0)
        success-counter (atom 0)]
    (log/info "found" (count token->current-metadata) "token(s) on" cluster-url ":" (keys token->current-metadata))
    (doseq [[backup-token token-description] (seq backup-token->token-description)]
      (try
        (let [{:keys [etag last-update-time owner] :as current-metadata} (get token->current-metadata backup-token)
              backup-update-time (get token-description "last-update-time")
              update? (or (nil? last-update-time)
                          (neg? (compare last-update-time backup-update-time)))]
          (if (or update? force-mode?)
            (do
              (log/info (when-not update? "force") "restoring token" backup-token
                        {:current-metadata current-metadata
                         :new-token-description token-description})
              (store-token cluster-url backup-token etag token-description)
              (swap! success-counter inc))
            (do
              (log/info "skipping restore for token" backup-token
                        {:backup-update-time backup-update-time
                         :last-update-time last-update-time
                         :owner owner})
              (swap! skip-counter inc))))
        (catch Throwable th
          (swap! error-counter inc)
          (log/error th "error in restoring token" backup-token))))
    (let [summary-result {:error @error-counter :skip @skip-counter :success @success-counter}]
      (log/info "restore summary" summary-result)
      summary-result)))

(defn read-backup-file
  "Reads the token descriptions from the specified file and returns a token to token description map."
  [{:keys [read-from-file]} filename]
  (let [backup-token->info (read-from-file filename)]
    (pc/map-vals #(get % "description") backup-token->info)))

(defn restore-tokens
  "Reads the token descriptions from the specified file and writes it to the specified cluster.
   When force-mode? is true, content from the specified file is force written to the cluster even if the token was updated.
   When force-mode? is false, this function effectively restores deleted or missing tokens.
   Return the summary containing error, skip and success counts of the restore."
  [waiter-api file-operations-api cluster-url filename force-mode?]
  (let [backup-token->token-description (read-backup-file file-operations-api filename)]
    (restore-tokens-from-backup waiter-api cluster-url force-mode? backup-token->token-description)))

(def restore-tokens-config
  {:execute-command (fn execute-restore-tokens-command
                      [{:keys [waiter-api] :as parent-context} {:keys [options]} arguments]
                      (let [{:keys [force]} options]
                        (if-not (= (count arguments) 2)
                          {:exit-code 1
                           :message (str "expected 2 arguments FILE URL, provided " (count arguments) ": " (vec arguments))}
                          (let [dry-run (get-in parent-context [:options :dry-run])
                                file-operations-api (file-utils/init-file-operations-api dry-run)
                                file (first arguments)
                                cluster-url (second arguments)]
                            (log/info "restoring tokens to" cluster-url "from" file
                                      (if force "in overwrite mode" "in update mode")
                                      (str (when dry-run "with dry-run enabled")))
                            (let [{:keys [error]} (restore-tokens waiter-api file-operations-api cluster-url file force)]
                              {:exit-code error
                               :message (str "exiting with code " error)})))))
   :option-specs [["-f" "--force"
                   (str "Forces tokens to the version in the backup file. "
                        "The default behavior (when this flag is not enabled) is skipping tokens with updates since the backup.")]]
   :retrieve-documentation (fn retrieve-sync-clusters-documentation
                             [command-name _]
                             {:description (str "Restores tokens to a Waiter cluster specified in the URL from the FILE")
                              :usage (str command-name " [OPTION]... FILE URL")})})
