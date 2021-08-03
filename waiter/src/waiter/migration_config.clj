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
(ns waiter.migration-config
  (:require [clj-time.core :as t]
            [plumbing.core :as pc]
            [schema.core :as s]
            [waiter.settings :as settings]
            [waiter.util.date-utils :as du]
            [waiter.schema :as schema]))

(defn config-activated?
  "Returns whether the entry has been activated."
  [{:keys [start-time]} current-time]
  (or (nil? start-time)
      (not (t/after? start-time current-time))))

(defn filter-config
  "Drops exclude and include configurations that have not yet been activated."
  [config-entry current-time]
  (let [filter-entries (fn [entries] (filter #(config-activated? % current-time) entries))]
    (-> config-entry
      (update :excludes filter-entries)
      (update :includes filter-entries))))

(defn state
  "Retrieves the state of the migration config loader."
  [{:keys [config-atom]} include-flags]
  (let [{:keys [config time]} @config-atom]
    (cond-> {:supported-include-params ["activated-config" "config"]
             :name "MigrationConfigLoader"
             :time time}
      (contains? include-flags "activated-config")
      (assoc :activated-config (filter-config config (t/now)))
      (contains? include-flags "config")
      (assoc :config config))))

(defn load-config
  "Loads the edn config in the specified file into the config atom."
  [config-file config-atom]
  (when-let [config (settings/load-settings-file config-file)]
    ;; validate the schema
    (-> {s/Keyword {(s/optional-key :excludes) [{(s/required-key :entries) (s/pred vector? 'some-vector)
                                                 (s/optional-key :start-time) schema/non-empty-string}]
                    (s/required-key :includes) [{(s/required-key :entries) (s/pred vector? 'some-vector)
                                                 (s/optional-key :start-time) schema/non-empty-string}]}}
      (s/validate config))
    (let [update-date-in-entries (fn update-date-in-entries [entries] (map #(update % :start-time du/str-to-date-safe) entries))]
      (reset! config-atom {:config (pc/map-vals
                                     ;; update the time entries
                                     (fn [config-entry]
                                       (-> config-entry
                                         (update :excludes update-date-in-entries)
                                         (update :includes update-date-in-entries)))
                                     config)
                           :time (t/now)}))))

(defn retrieve-config
  "Loads the edn config in the specified file into the config atom."
  [{:keys [config-atom]} current-time key-seq]
  (let [{:keys [config]} @config-atom]
    (-> config
      (get-in key-seq)
      (filter-config current-time))))

(defn determine-selection
  "Determines whether the provided match-fn matches entries in the exclude list (higher preference) or include list.
   If no match is found, returns default-value."
  [{:keys [includes excludes]} match-fn default-value]
  (let [in-selection? (fn [selection-entries]
                        (some (fn [{:keys [entries]}]
                                (some match-fn entries))
                              selection-entries))]
    (cond
      (in-selection? excludes)
      :exclude
      (in-selection? includes)
      :include
      :else
      default-value)))

(defn initialize-config-loader
  [{:keys [config-file reload-interval-ms]}]
  (let [config-atom (atom {:config nil
                           :time (t/now)})
        cancel-fn (du/start-timer-task
                    (t/millis reload-interval-ms)
                    #(load-config config-file config-atom))]
    {:cancel-fn cancel-fn
     :config-atom config-atom}))
