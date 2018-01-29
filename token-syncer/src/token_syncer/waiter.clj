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
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [qbits.jet.client.http :as http]
            [token-syncer.spnego :as spnego])
  (:import (java.net URI)
           (java.util UUID)
           (org.eclipse.jetty.client HttpClient)))

(defn- method->http-fn
  "Converts the request method keyword to the equivalent qbits.jet.client.http function.
   Throws an IllegalArgumentException if the request method is not supported."
  [request-method]
  (case request-method
    :delete http/delete
    :get http/get
    :post http/post))

(defn- report-response-error
  "Throws an exception if an error is present in the response map, esle returns the input response map."
  [{:keys [error] :as response}]
  (when error
    (log/error error "error in making request")
    (throw error))
  response)

(defn make-http-request
  "Makes an asynchronous request to the request-url."
  [^HttpClient http-client request-url &
   {:keys [body headers method query-params] :or {method :get}}]
  (let [request-headers (-> {"x-cid" (str "token-syncer." (UUID/randomUUID))}
                            (merge headers)
                            walk/stringify-keys)
        request-options (cond-> {:headers request-headers
                                 :fold-chunked-response? true
                                 :follow-redirects? false}
                                body (assoc :body body)
                                query-params (assoc :query-string query-params))]
    (log/info (get request-headers "x-cid") "making" (-> method name str/upper-case) "request to" request-url)
    (-> (let [method-fn (method->http-fn method)
              {:keys [headers status] :as response} (-> (method-fn http-client request-url request-options)
                                                        async/<!!)]
          (if (and (= 401 status) (= "Negotiate" (get headers "www-authenticate")))
            (do
              (log/info "using spnego auth to make request" request-url)
              (->> (assoc request-options :auth (spnego/spnego-authentication (URI. request-url)))
                   (method-fn http-client request-url)
                   async/<!!))
            response))
        report-response-error)))

(defn- parse-json-data
  "Attempts to parse the data as json, logs and throws an error if the parsing fails."
  [data]
  (try
    (-> data str json/read-str)
    (catch Exception exception
      (log/error "unable to parse as json:" data)
      (throw exception))))

(defn- extract-relevant-headers
  "Extracts relevant headers from a Waiter response"
  [headers]
  (select-keys headers ["content-type" "etag"]))

(defn load-token-list
  "Loads the list of tokens on a specific cluster."
  [^HttpClient http-client cluster-url]
  (let [token-list-url (str cluster-url "/tokens")
        {:keys [body headers status]} (make-http-request http-client token-list-url
                                                         :headers {"accept" "application/json"})
        body-json (->> body async/<!! parse-json-data)]
    (if (and status (<= 200 status 299))
      (->> body-json
           (map (fn entry->token [entry] (get entry "token")))
           set)
      (throw (ex-info (str "Unable to load tokens from " cluster-url)
                      {:body body-json
                       :headers (extract-relevant-headers headers)
                       :status status
                       :url token-list-url})))))

(defn- iso8601->millis
  "Convert the ISO 8601 string to numeric milliseconds."
  [date-str]
  (-> (:date-time f/formatters)
      (f/with-zone (t/default-time-zone))
      (f/parse date-str)
      .getMillis))

(defn load-token
  "Loads the description of a token on a cluster."
  [^HttpClient http-client cluster-url token]
  (try
    (let [{:keys [body headers status]} (make-http-request http-client (str cluster-url "/token")
                                                           :headers {"accept" "application/json"
                                                                     "x-waiter-token" token}
                                                           :query-params {"include" ["deleted" "metadata"]})]
      (log/info "loading" token "on" cluster-url "responded with status" status
                (select-keys headers ["content-type" "etag" "x-cid"]))
      (let [token-etag (get headers "etag")
            token-description (->> body
                                   async/<!!
                                   parse-json-data)]
        {:description (cond-> token-description
                              (contains? token-description "last-update-time")
                              (update "last-update-time" iso8601->millis))
         :headers (extract-relevant-headers headers)
         :token-etag token-etag
         :status status}))
    (catch Exception ex
      (log/error ex "unable to retrieve token" token "from" cluster-url)
      {:error ex})))

(defn store-token
  "Stores the token description on a specific cluster."
  [^HttpClient http-client cluster-url token token-etag token-description]
  (log/info "storing token:" token ", soft-delete:" (true? (get token-description "deleted")) "on" cluster-url)
  (let [{:keys [body headers status]} (make-http-request http-client (str cluster-url "/token")
                                                         :body (json/write-str (assoc token-description :token token))
                                                         :headers {"accept" "application/json"
                                                                   "if-match" token-etag}
                                                         :method :post
                                                         :query-params {"update-mode" "admin"})
        body-data (async/<!! body)]
    (log/info "storing" token "on" cluster-url "responded with status" status
              (select-keys headers ["content-type" "etag" "x-cid"]))
    (when (or (nil? status)
              (< status 200)
              (> status 299))
      (log/error "token store failed" body-data)
      (throw (ex-info "Token store failed"
                      {:body body-data
                       :headers (extract-relevant-headers headers)
                       :status status
                       :token-data token-description})))
    {:body (parse-json-data body-data)
     :headers (extract-relevant-headers headers)
     :status status}))

(defn hard-delete-token
  "Hard-delete a token on a specific cluster."
  [^HttpClient http-client cluster-url token token-etag]
  (log/info "hard-delete" token "on" cluster-url)
  (let [{:keys [body headers status]} (make-http-request http-client (str cluster-url "/token")
                                                         :headers {"accept" "application/json"
                                                                   "if-match" token-etag
                                                                   "x-waiter-token" token}
                                                         :method :delete
                                                         :query-params {"hard-delete" "true"})
        body-data (async/<!! body)]
    (log/info "hard-deleting" token "on" cluster-url "responded with status" status
              (select-keys headers ["content-type" "etag" "x-cid"]))
    (when (or (nil? status)
              (not (<= 200 status 299)))
      (throw (ex-info "Token hard-delete failed"
                      {:body (parse-json-data body-data)
                       :headers (extract-relevant-headers headers)
                       :status status
                       :token token})))
    {:body (parse-json-data body-data)
     :headers (extract-relevant-headers headers)
     :status status}))
