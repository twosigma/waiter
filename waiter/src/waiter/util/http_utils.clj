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
(ns waiter.util.http-utils
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
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
                              :or {spnego-auth false throw-exceptions true}}]
  (let [request-map (cond-> {:as :string
                             :method (or request-method :get)
                             :url request-url}
                            spnego-auth (assoc :auth (spnego/spnego-authentication (URI. request-url)))
                            accept (assoc :accept accept)
                            body (assoc :body body)
                            (not (str/blank? content-type)) (assoc :content-type content-type)
                            (seq headers) (assoc :headers headers)
                            query-string (assoc :query-string query-string))
        raw-response (http/request http-client request-map)
        {:keys [error status] :as response} (async/<!! raw-response)]
    (when error
      (throw error))
    (let [response (update response :body async/<!!)]
      (when (and throw-exceptions (not (<= 200 status 299)))
        (ss/throw+ response))
      (let [{:keys [body]} response]
        (try
          (cond-> body
            (not-empty body)
            (-> json/read-str walk/keywordize-keys))
          (catch Exception _
            body))))))

(defn ^HttpClient http-client-factory
  "Creates a HttpClient."
  [{:keys [conn-timeout socket-timeout]}]
  (http/client {:connect-timeout conn-timeout, :idle-timeout socket-timeout}))
