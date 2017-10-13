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
(ns waiter.mesos
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [qbits.jet.client.http :as http]
            [slingshot.slingshot :as ss]
            [waiter.auth.spnego :as spnego])
  (:import java.net.URI
           org.eclipse.jetty.client.HttpClient))

(defn http-request
  "Wrapper over the qbits.jet.client.http/request function.
   It performs a blocking read on the response and the response body.
   The body is assumed to be json and is parsed into a clojure data structure.
   If the status of the response in not 2XX, the response is thrown as an exception."
  [http-client request-url & {:keys [accept body content-type headers query-string request-method
                                     spnego-auth throw-exceptions]
                              :or {spnego-auth false
                                   throw-exceptions true}}]
  (let [request-map (cond-> {:as :string
                             :method (or request-method :get)
                             :url request-url}
                            spnego-auth (assoc :auth (spnego/spnego-authentication (URI. request-url)))
                            accept (assoc :accept accept)
                            body (assoc :body body)
                            content-type (assoc :content-type content-type)
                            (seq headers) (assoc :headers headers)
                            query-string (assoc :query-string query-string))
        raw-response (http/request http-client request-map)
        {:keys [error status] :as response} (async/<!! raw-response)]
    (when error
      (throw error))
    (when (and throw-exceptions (not (<= 200 status 299)))
      (ss/throw+ response))
    (-> response
        :body
        async/<!!
        json/read-str
        walk/keywordize-keys)))

(defn http-client-factory
  "Creates a HttpClient."
  [{:keys [conn-timeout socket-timeout]}]
  (http/client {:connect-timeout conn-timeout, :idle-timeout socket-timeout}))


(defrecord MesosApi [^HttpClient http-client spnego-auth slave-port slave-directory])

(defn mesos-api-factory
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
  (http-request http-client (str "http://" host ":" slave-port "/files/browse")
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
    (http-request http-client (str "http://" host ":" slave-port "/state.json")
                  :request-method :get
                  :spnego-auth spnego-auth
                  :throw-exceptions false)))


(defrecord MarathonApi [^HttpClient http-client ^String marathon-url spnego-auth])

(defn marathon-rest-api-factory
  "Factory method for MarathonApi."
  [http-client {:keys [spnego-auth]} url]
  (->MarathonApi http-client url spnego-auth))

(defn create-app
  "Create and start a new app specified by the descriptor."
  [{:keys [http-client marathon-url spnego-auth]} descriptor]
  (http-request http-client (str marathon-url "/v2/apps")
                :body (json/write-str descriptor)
                :content-type "application/json"
                :spnego-auth spnego-auth
                :request-method :post))

(defn delete-app
  "Delete the app specified by the app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id]
  (http-request http-client (str marathon-url "/v2/apps/" app-id)
                :content-type "application/json"
                :request-method :delete
                :spnego-auth spnego-auth))

(defn get-app
  "List the app specified by app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id]
  (http-request http-client (str marathon-url "/v2/apps/" app-id)
                :request-method :get
                :spnego-auth spnego-auth))

(defn get-apps
  "List all running apps including running and failed tasks."
  [{:keys [http-client marathon-url spnego-auth]}]
  (http-request http-client (str marathon-url "/v2/apps")
                :query-string {"embed" ["apps.lastTaskFailure" "apps.tasks"]}
                :request-method :get
                :spnego-auth spnego-auth))

(defn get-deployments
  "List all running deployments."
  [{:keys [http-client marathon-url spnego-auth]}]
  (http-request http-client (str marathon-url "/v2/deployments")
                :request-method :get
                :spnego-auth spnego-auth))

(defn get-info
  "Get info about the Marathon instance."
  [{:keys [http-client marathon-url spnego-auth]}]
  (http-request http-client (str marathon-url "/v2/info")
                :request-method :get
                :spnego-auth spnego-auth))

(defn kill-task
  "Kill the task task-id that belongs to the application app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id task-id scale force]
  (http-request http-client (str marathon-url "/v2/apps/" app-id "/tasks/" task-id)
                :query-string {"force" force, "scale" scale}
                :request-method :delete
                :spnego-auth spnego-auth))

(defn update-app
  "Update the descriptor of an existing app specified by the app-id."
  [{:keys [http-client marathon-url spnego-auth]} app-id descriptor]
  (http-request http-client (str marathon-url "/v2/apps/" app-id)
                :body (json/write-str descriptor)
                :content-type "application/json"
                :query-string {"force" true}
                :request-method :put
                :spnego-auth spnego-auth))