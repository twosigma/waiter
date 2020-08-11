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
            [waiter.util.http-utils :as hu])
  (:import org.eclipse.jetty.client.HttpClient))

(defrecord MesosApi [^HttpClient http-client spnego-auth mesos-agent-port mesos-agent-directory])

(defn api-factory
  "Factory method for MesosApi."
  [http-client {:keys [spnego-auth]} mesos-agent-port mesos-agent-directory]
  (->MesosApi http-client spnego-auth mesos-agent-port mesos-agent-directory))

(defn build-sandbox-path
  "Builds the sandbox directory path of the instance on a Mesos agent."
  [{:keys [mesos-agent-directory]} agent-id framework-id instance-id]
  (when (and mesos-agent-directory agent-id framework-id instance-id)
    (str mesos-agent-directory "/" agent-id "/frameworks/" framework-id
         "/executors/" instance-id "/runs/latest")))

(defn list-directory-content
  "Lists files and directories contained in the path."
  [{:keys [http-client mesos-agent-port spnego-auth]} host directory]
  (hu/http-request http-client (str "http://" host ":" mesos-agent-port "/files/browse")
                   :query-string {"path" directory}
                   :request-method :get
                   :spnego-auth spnego-auth
                   :throw-exceptions false))

(defn build-directory-download-link
  "Generates a download link to the directory on the specified mesos agent."
  [{:keys [mesos-agent-port]} host directory]
  (when (and mesos-agent-port host directory)
    (str "http://" host ":" mesos-agent-port "/files/download?path=" directory)))

(defn- process-directory-entry
  "Converts an individual directory entry into a map representing the entry."
  [mesos-api host directory {:keys [nlink path size]}]
  (-> (if (= nlink 1)
        {:type "file"
         :url (build-directory-download-link mesos-api host path)}
        {:path path
         :type "directory"})
      (assoc :name (subs path (inc (count directory)))
             :size size)))

(defn retrieve-directory-content-from-host
  "Retrieve the content of the directory for the given instance on the specified agent."
  [mesos-api host directory]
  (->> (list-directory-content mesos-api host directory)
       (map #(process-directory-entry mesos-api host directory %))))

(defn get-agent-state
  "Returns information about the frameworks, executors and the agent’s master."
  [{:keys [http-client mesos-agent-port spnego-auth]} host]
  (when (and mesos-agent-port host)
    (hu/http-request http-client (str "http://" host ":" mesos-agent-port "/state.json")
                     :request-method :get
                     :spnego-auth spnego-auth
                     :throw-exceptions false)))

(defn retrieve-log-url
  "Retrieve the directory path for the specified task running on the specified agent."
  [mesos-api task-id host framework-name]
  (let [response-parsed (get-agent-state mesos-api host)
        matching-frameworks (->> (concat (:completed_frameworks response-parsed) (:frameworks response-parsed))
                                 (filter #(or (-> % :name str str/lower-case (str/includes? framework-name))
                                              (-> % :role str str/lower-case (str/includes? framework-name)))))
        executors (mapcat #(concat (:completed_executors %) (:executors %)) matching-frameworks)
        log-directory (str (:directory (first (filter #(= (:id %) task-id) executors))))]
    log-directory))
