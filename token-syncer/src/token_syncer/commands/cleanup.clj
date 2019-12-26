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
(ns token-syncer.commands.cleanup
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [token-syncer.utils :as utils]))

(defn cleanup-tokens
  "Reads the latest tokens from the specified cluster and hard-deletes all the soft-deleted tokens last
   updated before the specified time."
  [{:keys [hard-delete-token load-token-list]} cluster-url before-epoch-time]
  (let [index-entries (load-token-list cluster-url)
        soft-deleted-index-entries (filter (fn [{:strs [deleted last-update-time]}]
                                             (and (true? deleted)
                                                  (< (utils/iso8601->millis last-update-time) before-epoch-time)))
                                           index-entries)]
    (log/info "found" (count soft-deleted-index-entries) "qualifying soft-deleted token(s) out of"
              (count index-entries) "total token(s) on" cluster-url)
    (doseq [{:strs [etag last-update-time token]} soft-deleted-index-entries]
      (log/info "hard-deleting token" token "with etag" etag "last updated on" last-update-time)
      (hard-delete-token cluster-url token etag))
    (log/info "hard-deleted" (count soft-deleted-index-entries) "token(s) on cluster" cluster-url)
    (->> soft-deleted-index-entries
      (map #(get % "token"))
      (into #{}))))

(def cleanup-tokens-config
  {:execute-command (fn execute-cleanup-tokens-command
                      [{:keys [waiter-api]} {:keys [options]} arguments]
                      (let [{:keys [before]} options]
                        (if-not (= (count arguments) 1)
                          {:exit-code 1
                           :message (str "expected one argument URL, provided " (count arguments) ": " (vec arguments))}
                          (let [cluster-url (first arguments)]
                            (log/info "hard-deleting tokens from" cluster-url "soft-deleted before"
                                      (utils/millis->iso8601 before))
                            (cleanup-tokens waiter-api cluster-url before)
                            {:exit-code 0
                             :message "exiting with code 0"}))))
   :option-specs [["-b" "--before BEFORE-TIME"
                   (str "Hard-delete tokens soft-deleted before the specified time. "
                        "Must be specified in ISO8601 time format. "
                        "The default value is 7 days before now.")
                   :default (-> (t/now) (t/minus (t/weeks 1)) tc/to-long)
                   :parse-fn #(try
                                (utils/iso8601->millis %)
                                (catch Throwable th
                                  (log/error th "Error in parsing input" %)
                                  (throw (IllegalArgumentException. (str "Unable to parse " % " as a ISO8601 date")))))]]
   :retrieve-documentation (fn retrieve-sync-clusters-documentation
                             [command-name _]
                             {:description (str "Hard-deletes soft-deleted tokens on the specified cluster")
                              :usage (str command-name " [OPTION]... URL")})})
