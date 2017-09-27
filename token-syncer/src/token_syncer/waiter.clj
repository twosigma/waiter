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
  [^HttpClient http-client request-url &
   {:keys [body headers method query-params]
    :or {body ""
         headers {}
         method http/get
         query-params {}}}]
  (let [request-options {:body body
                         :headers headers
                         :fold-chunked-response? true
                         :follow-redirects? false
                         :query-string query-params}
        {:keys [headers status] :as response} (async/<!! (method http-client request-url request-options))]
    (if (and (= 401 status) (= "Negotiate" (get headers "www-authenticate")))
      (do
        (log/info "Using spnego auth to make request" request-url)
        (->> (assoc request-options :auth (spnego/spnego-authentication (URI. request-url)))
             (method http-client request-url)
             async/<!!))
      response)))

(defn- try-parse-json-data
  "Attempts to parse the data as json, else return the data unparsed."
  [data & {:keys [silent] :or {silent true}}]
  (try
    (json/read-str (str data))
    (catch Exception _
      (when (not silent)
        (log/error "Unable to parse as json:" data))
      data)))

(defn load-token-list
  "Loads the list of tokens on a specific cluster."
  [^HttpClient http-client cluster-url]
  (let [token-list-url (str cluster-url "/tokens")
        {:keys [body error status]} (make-http-request http-client token-list-url
                                                       :headers {"accept" "application/json"})]
    (when error
      (log/error error "Error in retrieving tokens from" cluster-url)
      (throw error))
    (if (and status (<= 200 status 299))
      (->> body
           async/<!!
           try-parse-json-data
           (map (fn entry->token [entry] (get entry "token")))
           set)
      (throw (ex-info (str "Unable to load tokens from " cluster-url)
                      {:body (->> body async/<!! try-parse-json-data)
                       :status status
                       :url token-list-url})))))

(defn load-token
  "Loads the description of a token on a cluster."
  [^HttpClient http-client cluster-url token]
  (try
    (let [token-get-url (str cluster-url "/token")
          {:keys [body error status]} (make-http-request http-client token-get-url
                                                         :headers {"accept" "application/json"
                                                                   "x-waiter-token" token}
                                                         :query-params {"include-deleted" "true"})]
      (when error
        (log/error error "Error in retrieving token" token "from" cluster-url)
        (throw error))
      {:description (->> body
                         async/<!!
                         try-parse-json-data)
       :status status})
    (catch Exception ex
      (log/error "Unable to retrieve token" token "from" cluster-url)
      {:error ex})))

(defn store-token
  "Stores the token description on a specific cluster."
  [^HttpClient http-client cluster-url token token-description]
  (log/info "Storing token:" token ", soft-delete:" (true? (get token-description "deleted")) "on" cluster-url)
  (let [{:keys [body error status]} (make-http-request http-client
                                                       (str cluster-url "/token")
                                                       :body (json/write-str (assoc token-description :token token))
                                                       :headers {"accept" "application/json"}
                                                       :method http/post
                                                       :query-params {"update-mode" "admin"})
        body-data (when (not error) (async/<!! body))]
    (when error
      (throw error))
    (when (or (nil? status)
              (< status 200)
              (> status 299))
      (throw (ex-info "Token store failed"
                      {:body body-data, :status status, :token-data token-description})))
    {:body (try-parse-json-data body-data :silent false)
     :status status}))

(defn hard-delete-token
  "Hard-delete a token on a specific cluster."
  [^HttpClient http-client cluster-url token]
  (log/info "Hard-delete" token "on" cluster-url)
  (let [{:keys [body error status]} (make-http-request http-client
                                                       (str cluster-url "/token")
                                                       :headers {"accept" "application/json", "x-waiter-token" token}
                                                       :method http/delete
                                                       :query-params {"hard-delete" "true"})
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
