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
(ns waiter.mesos.mesos
  (:require [waiter.mesos.utils :as mesos-utils])
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
  (mesos-utils/http-request http-client (str "http://" host ":" slave-port "/files/browse")
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
    (mesos-utils/http-request http-client (str "http://" host ":" slave-port "/state.json")
                              :request-method :get
                              :spnego-auth spnego-auth
                              :throw-exceptions false)))
