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
  (:require [clojure.string :as str]
            [waiter.util.http-utils :as http-utils])
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

(defn retrieve-directory-content-from-host
  "Retrieve the content of the directory for the given instance on the specified agent."
  [mesos-api host directory]
  (let [response-parsed (list-directory-content mesos-api host directory)]
    (map (fn [entry]
           (merge {:name (subs (:path entry) (inc (count directory)))
                   :size (:size entry)}
                  (if (= (:nlink entry) 1)
                    {:type "file"
                     :url (build-directory-download-link mesos-api host (:path entry))}
                    {:type "directory"
                     :path (:path entry)})))
         response-parsed)))

(defn get-agent-state
  "Returns information about the frameworks, executors and the agent’s master."
  [{:keys [http-client slave-port spnego-auth]} host]
  (when (and slave-port host)
    (http-utils/http-request http-client (str "http://" host ":" slave-port "/state.json")
                             :request-method :get
                             :spnego-auth spnego-auth
                             :throw-exceptions false)))

(defn retrieve-log-url
  "Retrieve the directory path for the specified task running on the specified agent."
  [mesos-api task-id host framework-name]
  (let [response-parsed (get-agent-state mesos-api host)
        matching-frameworks (->> (concat (:completed_frameworks response-parsed) (:frameworks response-parsed))
                                 (filter #(or (str/includes? (str/lower-case (:role %)) framework-name)
                                              (str/includes? (str/lower-case (:name %)) framework-name))))
        executors (mapcat #(concat (:completed_executors %) (:executors %)) matching-frameworks)
        log-directory (str (:directory (first (filter #(= (:id %) task-id) executors))))]
    log-directory))
