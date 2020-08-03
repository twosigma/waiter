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
(ns token-syncer.commands.sanitize
  (:require [clojure.tools.logging :as log]))

(defn sanitize-token
  "Sanitizes the specified token on the provided cluster.
   It posts the token description to the cluster to sanitize stored data on the cluster."
  [{:keys [load-token store-token]} cluster-url token]
  (let [{:keys [description token-etag]} (load-token cluster-url token)]
    (if (seq description)
      (do
        (log/info "sanitizing token" token "with etag" token-etag "and description" description)
        (let [store-response (store-token cluster-url token token-etag description)
              result-etag (get-in store-response [:headers "etag"])
              updated? (not= token-etag result-etag)]
          (if updated?
            (log/info "token" token "updated after sanitize operation to etag" result-etag)
            (log/info "token" token "did not update as result of the sanitize operation"))
          {:response store-response
           :updated? updated?}))
      (do
        (log/info "no token description found for" token)
        nil))))

(defn sanitize-tokens
  "Reads the latest tokens from the specified cluster and sanitizes up to limit tokens."
  [{:keys [load-token-list] :as waiter-api} cluster-url limit regex]
  (let [all-index-entries (load-token-list cluster-url)
        selected-index-entries (->> all-index-entries
                                 (filter #(re-matches regex (get % "token")))
                                 (take limit))
        processed-tokens-atom (atom #{})
        missing-tokens-atom (atom #{})
        updated-tokens-atom (atom #{})]
    (log/info "processing" (count selected-index-entries) "of" (count all-index-entries) "token(s) on" cluster-url)
    (doseq [{:strs [etag last-update-time token]} selected-index-entries]
      (log/info "processing token" token "with etag" etag "last updated on" last-update-time)
      (if-let [sanitize-response (sanitize-token waiter-api cluster-url token)]
        (when (:updated? sanitize-response)
          (swap! updated-tokens-atom conj token))
        (swap! missing-tokens-atom conj token))
      (swap! processed-tokens-atom conj token))
    (log/info "sanitized" (count @updated-tokens-atom) "token(s) on cluster" cluster-url)
    (let [result-summary {:missing @missing-tokens-atom
                          :processed @processed-tokens-atom
                          :updated @updated-tokens-atom}]
      (log/info "result summary" result-summary)
      result-summary)))

(def sanitize-tokens-config
  {:execute-command (fn execute-sanitize-tokens-command
                      [{:keys [waiter-api]} {:keys [options]} arguments]
                      (let [{:keys [limit regex]} options]
                        (if-not (= (count arguments) 1)
                          {:exit-code 1
                           :message (str "expected one argument URL, provided " (count arguments) ": " (vec arguments))}
                          (let [cluster-url (first arguments)]
                            (log/info "sanitizing up to" limit "token(s) from" cluster-url)
                            (sanitize-tokens waiter-api cluster-url limit regex)
                            {:exit-code 0
                             :message "exiting with code 0"}))))
   :option-specs [["-l" "--limit LIMIT" "The maximum number of tokens to attempt to sanitize, must be between 1 and 10000"
                   :default 1000
                   :parse-fn #(Integer/parseInt %)
                   :validate [#(< 0 % 10001) "Must be between 1 and 10000"]]
                  ["-r" "--regex REGEX" "The regex used to filter the tokens to sanitize"
                   :default (re-pattern ".*")
                   :parse-fn re-pattern]]
   :retrieve-documentation (fn retrieve-sync-clusters-documentation
                             [command-name _]
                             {:description (str "Sanitizes tokens on the specified cluster")
                              :usage (str command-name " [OPTION]... URL")})})
