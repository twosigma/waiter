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
(ns waiter.state.router
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [digest]
            [metrics.core]
            [waiter.discovery :as discovery]
            [waiter.util.date-utils :as du]))

(defn retrieve-peer-routers
  "Query `discovery` for the list of currently running routers and populates it into `router-chan`."
  [discovery router-chan]
  (log/trace "querying discovery for routers")
  (async/>!! router-chan (discovery/router-id->endpoint-url discovery "http" "")))

(defn start-router-syncer
  "Starts loop to query discovery service for the list of currently running routers and
   sends the list to the router state maintainer."
  [discovery router-chan router-syncer-interval-ms router-syncer-delay-ms]
  (log/info "Starting router syncer")
  (du/start-timer-task
    (t/millis router-syncer-interval-ms)
    #(retrieve-peer-routers discovery router-chan)
    :delay-ms router-syncer-delay-ms))
