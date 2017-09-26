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
(ns token-syncer.waiter
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [qbits.jet.client.http :as http]
            [token-syncer.spnego :as spnego])
  (:import (org.eclipse.jetty.client HttpClient)
           (java.net URI)))

(defn make-http-request
  "Makes an asynchronous request to the request-url."
  [{:keys [^HttpClient http-client use-spnego]} request-url &
   {:keys [body headers method query-params]
    :or {body ""
         headers {}
         method http/get
         query-params {}}}]
  (method http-client request-url
          (cond-> {:body body
                   :headers headers
                   :fold-chunked-response? true
                   :follow-redirects? false
                   :query-string query-params}

                  use-spnego
                  (assoc :auth (spnego/spnego-authentication (URI. request-url))))))

(defn- try-parse-json-data
  "Attempts to parse the data as json, else return the data unparsed."
  [data]
  (try
    (json/read-str (str data))
    (catch Exception _
      (log/error "Unable to parse as json:" data)
      data)))

(defn load-token-list
  "Loads the list of tokens on a specific cluster."
  [http-client-wrapper cluster-url]
  (let [token-list-url (str cluster-url "/tokens")
        {:keys [body error]} (async/<!! (make-http-request http-client-wrapper token-list-url :headers {"accept" "application/json"}))]
    (when error
      (log/error error "Error in retrieving tokens from" cluster-url)
      (throw error))
    (->> body
         (async/<!!)
         try-parse-json-data
         (map (fn entry->token [entry] (get entry "token")))
         set)))

(defn load-token-on-cluster
  "Loads the description of a token on a cluster."
  [http-client-wrapper cluster-url token]
  (try
    (let [token-get-url (str cluster-url "/token")
          {:keys [body error status]} (async/<!! (make-http-request http-client-wrapper token-get-url
                                                                    :headers {"accept" "application/json", "x-waiter-token" token}
                                                                    :query-params {"include-deleted" "true"}))]
      (when error
        (log/error error "Error in retrieving token" token "from" cluster-url)
        (throw error))
      {:description (->> body
                         (async/<!!)
                         try-parse-json-data)
       :status status})
    (catch Exception ex
      (log/error "Unable to retrieve token" token "from" cluster-url)
      {:error ex})))

(defn store-token-on-cluster
  "Stores the token description on a specific cluster."
  [http-client-wrapper cluster-url token token-description]
  (log/info "Storing token:" token-description ", soft-delete:" (true? (get token-description "deleted")) "on" cluster-url)
  (let [{:keys [body error status]}
        (async/<!!
          (make-http-request http-client-wrapper
                             (str cluster-url "/token")
                             :body (json/write-str (assoc token-description :token token))
                             :headers {"accept" "application/json"}
                             :method http/post
                             :query-params {"update-mode" "admin"}))
        body-data (when (not error) (async/<!! body))]
    (when error
      (throw error))
    (when (or (nil? status)
              (< status 200)
              (> status 299))
      (throw (ex-info "Token store failed"
                      {:body body-data, :status status, :token-data token-description})))
    {:body (try-parse-json-data body-data)
     :status status}))

(defn hard-delete-token-on-cluster
  "Hard-delete a token on a specific cluster."
  [http-client-wrapper cluster-url token]
  (log/info "Hard-delete" token "on" cluster-url)
  (let [{:keys [body error status]}
        (async/<!!
          (make-http-request http-client-wrapper
                             (str cluster-url "/token")
                             :headers {"accept" "application/json", "x-waiter-token" token}
                             :method http/delete
                             :query-params {"hard-delete" "true"}))
        body-data (when (not error) (async/<!! body))]
    (when error
      (throw error))
    (when (or (nil? status)
              (< status 200)
              (> status 299))
      (throw (ex-info "Token hard-delete failed"
                      {:body (try-parse-json-data body-data), :status status, :token token})))
    {:body (try-parse-json-data body-data)
     :status status}))
