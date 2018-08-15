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
(ns waiter.mesos.marathon
  (:require [waiter.util.http-utils :as http-utils]
            [waiter.util.utils :as utils])
  (:import org.eclipse.jetty.client.HttpClient))

(defrecord MarathonApi [^HttpClient http-client ^String marathon-url spnego-auth])

(defn api-factory
  "Factory method for MarathonApi."
  [http-client {:keys [spnego-auth]} url]
  (->MarathonApi http-client url spnego-auth))

(defn create-app
  "Create and start a new app specified by the descriptor."
  [{:keys [http-client marathon-url spnego-auth]} descriptor]
  (http-utils/http-request http-client (str marathon-url "/v2/apps")
                           :body (utils/clj->json descriptor)
                           :content-type "application/json"
                           :spnego-auth spnego-auth
                           :request-method :post))

(defn delete-app
  "Delete the app specified by the app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id]
  (http-utils/http-request http-client (str marathon-url "/v2/apps/" app-id)
                           :content-type "application/json"
                           :request-method :delete
                           :spnego-auth spnego-auth))

(defn delete-deployment
  "Cancel the deployment with deployment-id.
   No rollback deployment is created to revert the changes of deployment."
  [{:keys [http-client marathon-url spnego-auth]} deployment-id]
  (http-utils/http-request http-client (str marathon-url "/v2/deployments/" deployment-id)
                           :query-string {"force" true}
                           :request-method :delete
                           :spnego-auth spnego-auth))

(defn get-app
  "List the app specified by app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id]
  (http-utils/http-request http-client (str marathon-url "/v2/apps/" app-id)
                           :request-method :get
                           :spnego-auth spnego-auth))

(defn get-apps
  "List all running apps including running and failed tasks."
  [{:keys [http-client marathon-url spnego-auth]} query-params]
  (http-utils/http-request http-client (str marathon-url "/v2/apps")
                           :query-string query-params
                           :request-method :get
                           :spnego-auth spnego-auth))

(defn get-deployments
  "List all running deployments."
  [{:keys [http-client marathon-url spnego-auth]}]
  (http-utils/http-request http-client (str marathon-url "/v2/deployments")
                           :request-method :get
                           :spnego-auth spnego-auth))

(defn get-info
  "Get info about the Marathon instance."
  [{:keys [http-client marathon-url spnego-auth]}]
  (http-utils/http-request http-client (str marathon-url "/v2/info")
                           :request-method :get
                           :spnego-auth spnego-auth))

(defn kill-task
  "Kill the task task-id that belongs to the application app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id task-id scale force]
  (http-utils/http-request http-client (str marathon-url "/v2/apps/" app-id "/tasks/" task-id)
                           :query-string {"force" force, "scale" scale}
                           :request-method :delete
                           :spnego-auth spnego-auth))

(defn update-app
  "Update the descriptor of an existing app specified by the app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id descriptor]
  (http-utils/http-request http-client (str marathon-url "/v2/apps/" app-id)
                           :body (utils/clj->json descriptor)
                           :content-type "application/json"
                           :query-string {"force" true}
                           :request-method :put
                           :spnego-auth spnego-auth))
