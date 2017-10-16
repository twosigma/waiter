;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.mesos.marathon
  (:require [clojure.data.json :as json]
            [waiter.mesos.utils :as mutils])
  (:import org.eclipse.jetty.client.HttpClient))

(defrecord MarathonApi [^HttpClient http-client ^String marathon-url spnego-auth])

(defn api-factory
  "Factory method for MarathonApi."
  [http-client {:keys [spnego-auth]} url]
  (->MarathonApi http-client url spnego-auth))

(defn create-app
  "Create and start a new app specified by the descriptor."
  [{:keys [http-client marathon-url spnego-auth]} descriptor]
  (mutils/http-request http-client (str marathon-url "/v2/apps")
                       :body (json/write-str descriptor)
                       :content-type "application/json"
                       :spnego-auth spnego-auth
                       :request-method :post))

(defn delete-app
  "Delete the app specified by the app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id]
  (mutils/http-request http-client (str marathon-url "/v2/apps/" app-id)
                       :content-type "application/json"
                       :request-method :delete
                       :spnego-auth spnego-auth))

(defn get-app
  "List the app specified by app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id]
  (mutils/http-request http-client (str marathon-url "/v2/apps/" app-id)
                       :request-method :get
                       :spnego-auth spnego-auth))

(defn get-apps
  "List all running apps including running and failed tasks."
  [{:keys [http-client marathon-url spnego-auth]}]
  (mutils/http-request http-client (str marathon-url "/v2/apps")
                       :query-string {"embed" ["apps.lastTaskFailure" "apps.tasks"]}
                       :request-method :get
                       :spnego-auth spnego-auth))

(defn get-deployments
  "List all running deployments."
  [{:keys [http-client marathon-url spnego-auth]}]
  (mutils/http-request http-client (str marathon-url "/v2/deployments")
                       :request-method :get
                       :spnego-auth spnego-auth))

(defn get-info
  "Get info about the Marathon instance."
  [{:keys [http-client marathon-url spnego-auth]}]
  (mutils/http-request http-client (str marathon-url "/v2/info")
                       :request-method :get
                       :spnego-auth spnego-auth))

(defn kill-task
  "Kill the task task-id that belongs to the application app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id task-id scale force]
  (mutils/http-request http-client (str marathon-url "/v2/apps/" app-id "/tasks/" task-id)
                       :query-string {"force" force, "scale" scale}
                       :request-method :delete
                       :spnego-auth spnego-auth))

(defn update-app
  "Update the descriptor of an existing app specified by the app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id descriptor]
  (mutils/http-request http-client (str marathon-url "/v2/apps/" app-id)
                       :body (json/write-str descriptor)
                       :content-type "application/json"
                       :query-string {"force" true}
                       :request-method :put
                       :spnego-auth spnego-auth))
