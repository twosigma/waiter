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
(ns waiter.config
  (:require [clj-time.coerce :as tc]
            [waiter.util.date-utils :as du]))

(def ^:private config-promise (promise))

(defn initialize-config
  "Initializes the config that represents the settings used to launch the current Waiter router.
   The config is constant for the lifetime of the Waiter router."
  [config-value]
  (deliver config-promise config-value)
  (when-not (= @config-promise config-value)
    (throw (IllegalStateException. "The config has already been initialized to a different value"))))

(defn retrieve-cluster-name
  "Retrieves the configured cluster name."
  []
  {:pre [(realized? config-promise)]}
  (get-in @config-promise [:cluster-config :name]))

(defn retrieve-waiter-principal
  "Retrieves the configured waiter principal."
  []
  {:pre [(realized? config-promise)]}
  (get-in @config-promise [:waiter-principal]))

(defn retrieve-exclusive-promotion-start-epoch-time
  "Retrieves the configured (or system default) exclusive promotion start epoch time."
  []
  {:pre [(realized? config-promise)]}
  (let [default-exclusive-promotion-start-time "2022-02-01T00:00:00.000Z"
        exclusive-promotion-start-time (get-in @config-promise [:token-config :exclusive-promotion-start-time] default-exclusive-promotion-start-time)]
    (-> exclusive-promotion-start-time (du/str-to-date) (tc/to-long))))
