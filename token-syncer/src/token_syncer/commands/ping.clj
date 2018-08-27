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
(ns token-syncer.commands.ping
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]))

(defn ping-token
  "Pings the specified tokens across provided clusters based on cluster-urls and returns the result.
   Throws an exception if there was an error while pinging the clusters with the provided token."
  [{:keys [health-check-token]} cluster-urls token queue-timeout-ms]
  (try
    (log/info "pinging token" token "on clusters:" cluster-urls)
    (let [cluster-url->result
          (pc/map-from-keys
            (fn [cluster-url]
              (try
                (let [{:keys [body headers status]} (health-check-token cluster-url token queue-timeout-ms)]
                  (log/info cluster-url "health check response:"
                            {:body (str/trim (str body))
                             :headers headers
                             :status status})
                  (if (and (integer? status) (<= 200 status 299))
                    {:exit-code 0
                     :message (str "successfully pinged token " token " on " cluster-url
                                   ", reason: health check returned status code " status)}
                    {:exit-code 1
                     :message (str "unable to ping token " token " on " cluster-url
                                   ", reason: health check returned status code " status)}))
                (catch Throwable th
                  (log/error th "unable to ping token" token "on" cluster-url)
                  {:exit-code 1
                   :message (str "unable to ping token " token " on " cluster-url
                                 ", reason: " (.getMessage th))})))
            (set cluster-urls))
          exit-code (reduce + 0 (->> cluster-url->result vals (map :exit-code)))]
      {:details cluster-url->result
       :exit-code exit-code
       :message (str "pinging token " token " on " (-> cluster-urls vec println with-out-str str/trim)
                     " " (if (zero? exit-code) "was successful" "failed"))
       :token token})
    (catch Throwable th
      (log/error th "unable to ping token")
      (throw th))))

(def ^:const valid-token-re #"[a-zA-Z]([a-zA-Z0-9\-_$\.])+")

(def ping-token-config
  {:execute-command (fn execute-ping-token-command
                      [{:keys [waiter-api]} {:keys [options]} arguments]
                      (let [{:keys [queue-timeout]} options
                            token (first arguments)
                            cluster-urls-set (-> arguments rest set)]
                        (cond
                          (empty? arguments)
                          {:exit-code 1
                           :message "no arguments provided, usage TOKEN URL..."}

                          (str/blank? token)
                          {:exit-code 1
                           :message (str "token is required, none provided: " (vec arguments))}

                          (not (re-matches valid-token-re token))
                          {:exit-code 1
                           :message (str "token is not valid: "
                                         (-> {:arguments (vec arguments)
                                              :pattern (str valid-token-re)
                                              :token token}
                                             println
                                             with-out-str
                                             str/trim))}

                          (-> cluster-urls-set set seq not)
                          {:exit-code 1
                           :message (str "at least one cluster url required, provided: " (vec cluster-urls-set))}

                          :else
                          (let [{:keys [exit-code] :as ping-result}
                                (ping-token waiter-api cluster-urls-set token queue-timeout)]
                            (log/info (-> ping-result pp/pprint with-out-str str/trim))
                            {:exit-code exit-code
                             :message (str "exiting with code " exit-code)}))))
   :option-specs [["-q" "--queue-timeout TIMEOUT" "The maximum time, in ms, allowed spent by a request waiting for an available service backend"
                   :default 12000
                   :parse-fn #(Integer/parseInt %)
                   :validate [#(< 0 % 300001) "Must be between 1 and 30000"]]]
   :retrieve-documentation (fn retrieve-ping-token-documentation
                             [command-name _]
                             {:description (str "Pings the specified TOKEN across Waiter cluster(s) specified in the URL(s)")
                              :usage (str command-name " TOKEN URL...")})})
