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
(ns waiter.mesos.mesos
  (:require [waiter.util.http-utils :as http-utils])
  (:import org.eclipse.jetty.client.HttpClient))

(defrecord MesosApi [^HttpClient http-client spnego-auth slave-port slave-directory])

(defn api-factory
  "Factory method for MesosApi."
  [http-client {:keys [spnego-auth]} slave-port slave-directory]
  (->MesosApi http-client spnego-auth slave-port slave-directory))

(defn build-sandbox-path
  "Builds the sandbox directory path of the instance on a Mesos agent."
  [{:keys [slave-directory]} slave-id framework-id instance-id]
  (when (and slave-directory slave-id framework-id instance-id)
    (str slave-directory "/" slave-id "/frameworks/" framework-id
         "/executors/" instance-id "/runs/latest")))

(defn list-directory-content
  "Lists files and directories contained in the path."
  [{:keys [http-client slave-port spnego-auth]} host directory]
  (http-utils/http-request http-client (str "http://" host ":" slave-port "/files/browse")
                            :query-string {"path" directory}
                            :request-method :get
                            :spnego-auth spnego-auth
                            :throw-exceptions false))

(defn build-directory-download-link
  "Generates a download link to the directory on the specified mesos agent."
  [{:keys [slave-port]} host directory]
  (when (and slave-port host directory)
    (str "http://" host ":" slave-port "/files/download?path=" directory)))

(defn get-agent-state
  "Returns information about the frameworks, executors and the agent’s master."
  [{:keys [http-client slave-port spnego-auth]} host]
  (when (and slave-port host)
    (http-utils/http-request http-client (str "http://" host ":" slave-port "/state.json")
                              :request-method :get
                              :spnego-auth spnego-auth
                              :throw-exceptions false)))
