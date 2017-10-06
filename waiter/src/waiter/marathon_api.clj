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
(ns waiter.marathon-api
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [qbits.jet.client.http :as http]
            [slingshot.slingshot :as ss]
            [waiter.auth.spnego :as spnego])
  (:import (java.net URI)
           (org.eclipse.jetty.client HttpClient)))

(defprotocol MarathonApi
  (agent-directory-content [_ host port directory]
    "Lists files and directories contained in the path.")
  (agent-state [this host port]
    "Returns information about the frameworks, executors and the agent’s master. ")
  (create-app [this descriptor]
    "Create and start a new app specified by the descriptor.")
  (delete-app [this app-id]
    "Delete the app specified by the app-id.")
  (get-app [this app-id]
    "List the app specified by app-id.")
  (get-apps [this]
    "List all running apps including running and failed tasks.")
  (get-deployments [this]
    "List all running deployments.")
  (get-info [this]
    "Get info about the Marathon instance.")
  (kill-task [this app-id task-id scale force]
    "Kill the task task-id that belongs to the application app-id.")
  (update-app [this app-id descriptor]
    "Update the descriptor of an existing app specified by the app-id."))

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
                            spnego-auth
                            (assoc :auth (spnego/spnego-authentication (URI. request-url)))
                            accept
                            (assoc :accept accept)
                            body
                            (assoc :body body)
                            content-type
                            (assoc :content-type content-type)
                            (seq headers)
                            (assoc :headers headers)
                            query-string
                            (assoc :query-string query-string))
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

(deftype MarathonApiImpl [^HttpClient http-client ^String marathon-url spnego-auth]
  MarathonApi

  (agent-directory-content [_ host port directory]
    (http-request http-client (str "http://" host ":" port "/files/browse")
                  :query-string {"path" directory}
                  :request-method :get
                  :spnego-auth spnego-auth
                  :throw-exceptions false))

  (agent-state [_ host port]
    (http-request http-client (str "http://" host ":" port "/state.json")
                  :request-method :get
                  :spnego-auth spnego-auth
                  :throw-exceptions false))

  (create-app [_ descriptor]
    (http-request http-client (str marathon-url "/v2/apps")
                  :body (json/write-str descriptor)
                  :content-type "application/json"
                  :spnego-auth spnego-auth
                  :request-method :post))

  (delete-app [_ app-id]
    (http-request http-client (str marathon-url "/v2/apps/" app-id)
                  :content-type "application/json"
                  :request-method :delete
                  :spnego-auth spnego-auth))

  (get-app [_ app-id]
    (http-request http-client (str marathon-url "/v2/apps/" app-id)
                  :request-method :get))

  (get-apps [_]
    (http-request http-client (str marathon-url "/v2/apps")
                  :query-string {"embed" ["apps.lastTaskFailure" "apps.tasks"]}
                  :request-method :get
                  :spnego-auth spnego-auth))

  (get-deployments [_]
    (http-request http-client (str marathon-url "/v2/deployments")
                  :request-method :get
                  :spnego-auth spnego-auth))

  (get-info [_]
    (http-request http-client (str marathon-url "/v2/info")
                  :request-method :get
                  :spnego-auth spnego-auth))

  (kill-task [_ app-id task-id scale force]
    (http-request http-client (str marathon-url "/v2/apps/" app-id "/tasks/" task-id)
                  :query-string {"force" force, "scale" scale}
                  :request-method :delete
                  :spnego-auth spnego-auth))

  (update-app [_ app-id descriptor]
    (http-request http-client (str marathon-url "/v2/apps/" app-id)
                  :body (json/write-str descriptor)
                  :content-type "application/json"
                  :query-string {"force" true}
                  :request-method :put
                  :spnego-auth spnego-auth)))

(defn marathon-rest-api-factory
  "Factory method for MarathonRestApi."
  [http-options url]
  (let [http-client (http/client {:connect-timeout (:conn-timeout http-options)
                                  :idle-timeout (:socket-timeout http-options)})]
    (->MarathonApiImpl http-client url (:spnego-auth http-options))))
