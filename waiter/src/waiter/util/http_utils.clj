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
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [qbits.jet.client.http :as http]
            [slingshot.slingshot :as ss]
            [waiter.auth.spnego :as spnego])
  (:import (java.net URI)
           (java.util ArrayList)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.http HttpField HttpHeader HttpVersion)
           (org.eclipse.jetty.http2.client HTTP2Client)
           (org.eclipse.jetty.http2.client.http HttpClientTransportOverHTTP2)
           (org.eclipse.jetty.util HttpCookieStore$Empty)))

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
  [{:keys [clear-content-decoders conn-timeout follow-redirects? socket-timeout transport user-agent]
    :or {clear-content-decoders true}}]
  (let [^HttpClient client
        (http/client (cond-> {}
                       (some? conn-timeout) (assoc :connect-timeout conn-timeout)
                       (some? follow-redirects?) (assoc :follow-redirects? follow-redirects?)
                       (some? socket-timeout) (assoc :idle-timeout socket-timeout)
                       (some? transport) (assoc :transport transport)))]
    (when clear-content-decoders
      (.clear (.getContentDecoderFactories client)))
    (.setCookieStore client (HttpCookieStore$Empty.))
    (.setDefaultRequestContentType client nil)
    (when user-agent
      (let [new-user-agent-field (HttpField. HttpHeader/USER_AGENT (str user-agent))]
        (.setUserAgentField client new-user-agent-field)))
    client))

(defn- prepare-http2-transport
  "Returns the HTTP/2 client transport."
  [connection-timeout-ms]
  (let [http2-client (HTTP2Client.)
        http2-protocols (ArrayList. ["h2" "h2c"])]
    (.setConnectTimeout http2-client connection-timeout-ms)
    (.setProtocols http2-client http2-protocols)
    (HttpClientTransportOverHTTP2. http2-client)))

(defn prepare-http-clients
  "Prepares and returns a map of HTTP clients for http/1 and http/2 requests."
  [{:keys [conn-timeout user-agent] :as config}]
  (let [http2-transport (prepare-http2-transport conn-timeout)]
    {:http1-client (http-client-factory (cond-> config
                                          user-agent (update :user-agent str ".http1")))
     :http2-client (-> (cond-> config
                         user-agent (update :user-agent str ".http2"))
                       (assoc :transport http2-transport)
                       http-client-factory)}))

(defn retrieve-http-client
  "Returns the appropriate http client based on the backend protocol."
  [backend-proto {:keys [http1-client http2-client]}]
  (if (contains? #{"h2c" "h2"} backend-proto)
    http2-client
    http1-client))

(defn determine-backend-protocol-version
  "Determines the protocol version to use for the request to the backend.
   Returns HTTP/2.0 for http/2 backends.
   Returns HTTP/1.1, HTTP/1.0 or HTTP/0.9 for non-http/2 backends."
  [^String backend-proto ^String client-protocol]
  (cond
    ;; HTTP/2 backend
    (contains? #{"h2c" "h2"} backend-proto) "HTTP/2.0"
    ;; Not a HTTP/2 backend, default to HTTP/1.1 for HTTP/2 requests
    (= client-protocol "HTTP/2.0") "HTTP/1.1"
    ;; Use client version: HTTP/1.1, HTTP/1.0 or HTTP/0.9
    :else client-protocol))

(defn backend-proto->scheme
  "Determines the protocol scheme from the backend proto"
  [backend-proto]
  (case backend-proto
    "http" "http"
    "https" "https"
    "h2c" "http"
    "h2" "https"
    backend-proto))

(defn protocol->http-version
  "Determines the http protocol version to use for the request to the backend."
  [^String protocol]
  (try
    (when (and protocol (str/starts-with? protocol "HTTP"))
      (HttpVersion/fromString protocol))
    (catch Exception e
      (log/error e "unable to determine http version from" protocol))))
