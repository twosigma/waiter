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
(ns token-syncer.waiter
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [qbits.jet.client.http :as http]
            [token-syncer.spnego :as spnego]
            [token-syncer.utils :as utils])
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
                                 :fold-chunked-response? true}
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
        request-headers {"accept" "application/json"}
        request-params {"include" ["deleted" "metadata"]}
        {:keys [body headers status] :as response}
        (make-http-request http-client token-list-url :headers request-headers :query-params request-params)
        body-json (->> body async/<!! parse-json-data)]
    (if (utils/successful? response)
      body-json
      (throw (ex-info (str "Unable to load tokens from " cluster-url)
                      {:body body-json
                       :headers (extract-relevant-headers headers)
                       :status status
                       :url token-list-url})))))

(defn- convert-iso8601->millis
  [token-description]
  (loop [loop-token-description token-description
         nested-last-update-time-path ["last-update-time"]]
    (if (get-in loop-token-description nested-last-update-time-path)
      (recur (update-in loop-token-description nested-last-update-time-path utils/iso8601->millis)
             (concat ["previous"] nested-last-update-time-path))
      loop-token-description)))

(defn load-token
  "Loads the description of a token on a cluster."
  [^HttpClient http-client cluster-url token]
  (try
    (let [{:keys [body headers status] :as response}
          (make-http-request http-client (str cluster-url "/token")
                             :headers {"accept" "application/json"
                                       "x-waiter-token" token}
                             :query-params {"include" ["deleted" "metadata"]})]
      (log/info "loading" token "on" cluster-url "responded with status" status
                (select-keys headers ["content-type" "etag" "x-cid"]))
      (let [token-etag (get headers "etag")
            token-description (->> body
                                   async/<!!
                                   parse-json-data)]
        (cond-> {:description (if (utils/successful? response)
                                (convert-iso8601->millis token-description)
                                {})
                 :headers (extract-relevant-headers headers)
                 :status status}
                token-etag (assoc :token-etag token-etag))))
    (catch Exception ex
      (log/error ex "unable to retrieve token" token "from" cluster-url)
      {:error ex})))

(defn store-token
  "Stores the token description on a specific cluster."
  [^HttpClient http-client cluster-url token token-etag token-description]
  (log/info "storing token:" token ", soft-delete:" (true? (get token-description "deleted"))
            "on" cluster-url "with etag" token-etag)
  (let [{:keys [body headers status] :as response}
        (make-http-request http-client (str cluster-url "/token")
                           :body (-> token-description
                                     walk/stringify-keys
                                     (assoc "token" token)
                                     json/write-str)
                           :headers (cond-> {"accept" "application/json"}
                                            (not (str/blank? (str token-etag)))
                                            (assoc "if-match" token-etag))
                           :method :post
                           :query-params {"update-mode" "admin"})]
    (log/info "storing" token "on" cluster-url "responded with status" status
              (select-keys headers ["content-type" "etag" "x-cid"]))
    (let [body-data (async/<!! body)]
      (when-not (utils/successful? response)
        (log/error "token store failed" body-data)
        (throw (ex-info "Token store failed"
                        {:body body-data
                         :headers (extract-relevant-headers headers)
                         :status status
                         :token-data token-description})))
      {:body (parse-json-data body-data)
       :headers (extract-relevant-headers headers)
       :status status})))

(defn hard-delete-token
  "Hard-delete a token on a specific cluster."
  [^HttpClient http-client cluster-url token token-etag]
  (log/info "hard-delete" token "on" cluster-url)
  (let [{:keys [body headers status] :as response}
        (make-http-request http-client (str cluster-url "/token")
                           :headers {"accept" "application/json"
                                     "if-match" token-etag
                                     "x-waiter-token" token}
                           :method :delete
                           :query-params {"hard-delete" "true"})]
    (log/info "hard-deleting" token "on" cluster-url "responded with status" status
              (select-keys headers ["content-type" "etag" "x-cid"]))
    (let [body-data (async/<!! body)]
      (when-not (utils/successful? response)
        (throw (ex-info "Token hard-delete failed"
                        {:body (parse-json-data body-data)
                         :headers (extract-relevant-headers headers)
                         :status status
                         :token token})))
      {:body (parse-json-data body-data)
       :headers (extract-relevant-headers headers)
       :status status})))

(defn health-check-token
  "Performs health check on a token on a specific cluster."
  [^HttpClient http-client cluster-url token queue-timeout-ms]
  (log/info "health-check-token" token "on" cluster-url "with queue timeout of" queue-timeout-ms "ms")
  (let [{:keys [description]} (load-token http-client cluster-url token)
        {:strs [deleted health-check-url]} description]
    (if (and (not deleted) (not (str/blank? health-check-url)))
      (do
        (log/info "health-check-token" token "on" (str cluster-url health-check-url))
        (-> (make-http-request http-client (str cluster-url health-check-url)
                               :headers {"x-waiter-queue-timeout" queue-timeout-ms
                                         "x-waiter-token" token}
                               :method :get
                               :query-params {})
            (update :body async/<!!)))
      (log/info "health-check-token not performed"
                {:cluster-url cluster-url
                 :deleted deleted
                 :health-check-url health-check-url
                 :token token}))))
