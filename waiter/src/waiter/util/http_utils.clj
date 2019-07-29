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
            [slingshot.slingshot :as ss])
  (:import (java.net URI)
           (java.net URI)
           (java.util ArrayList)
           (org.apache.commons.codec.binary Base64)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.client.api Authentication$Result Request)
           (org.eclipse.jetty.http HttpField HttpHeader)
           (org.eclipse.jetty.http2.client HTTP2Client)
           (org.eclipse.jetty.http2.client.http HttpClientTransportOverHTTP2)
           (org.eclipse.jetty.util HttpCookieStore$Empty)
           (org.ietf.jgss GSSManager)
           (org.ietf.jgss GSSManager GSSContext GSSName Oid)))

(def ^Oid spnego-oid (Oid. "1.3.6.1.5.5.2"))

(def ^Base64 base64 (Base64.))

(defn spnego-authentication
  "Returns an Authentication$Result for endpoint which will use SPNEGO to generate an Authorization header"
  [^URI endpoint]
  (reify Authentication$Result
    (getURI [_] endpoint)

    (^void apply [_ ^Request request]
      (try
        (let [gss-manager (GSSManager/getInstance)
              server-principal (str "HTTP@" (.getHost endpoint))
              server-name (.createName gss-manager server-principal GSSName/NT_HOSTBASED_SERVICE spnego-oid)
              gss-context (.createContext gss-manager server-name spnego-oid nil GSSContext/DEFAULT_LIFETIME)
              _ (.requestMutualAuth gss-context true)
              token (.initSecContext gss-context (make-array Byte/TYPE 0) 0 0)
              header (str "Negotiate " (String. (.encode base64 token)))]
          (.header request HttpHeader/AUTHORIZATION header))
        (catch Exception e
          (log/warn e "failure during spnego authentication"))))))

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
                      spnego-auth (assoc :auth (spnego-authentication (URI. request-url)))
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
  [{:keys [clear-content-decoders conn-timeout socket-timeout user-agent]
    :or {clear-content-decoders true}
    :as config}]
  (let [^HttpClient client
        (http/client (cond-> (select-keys config [:client-name :follow-redirects? :transport])
                       (some? conn-timeout) (assoc :connect-timeout conn-timeout)
                       (some? socket-timeout) (assoc :idle-timeout socket-timeout)))]
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
  [connection-timeout-ms socket-timeout-ms]
  (let [http2-client (HTTP2Client.)
        http2-protocols (ArrayList. ["h2" "h2c"])]
    (when connection-timeout-ms
      (.setConnectTimeout http2-client connection-timeout-ms))
    (when socket-timeout-ms
      (.setIdleTimeout http2-client socket-timeout-ms))
    (.setProtocols http2-client http2-protocols)
    (HttpClientTransportOverHTTP2. http2-client)))

(defn prepare-http-clients
  "Prepares and returns a map of HTTP clients for http/1 and http/2 requests."
  [{:keys [client-name conn-timeout socket-timeout user-agent] :as config}]
  (let [http2-transport (prepare-http2-transport conn-timeout socket-timeout)]
    {:http1-client (http-client-factory (cond-> config
                                          client-name (update :client-name str "-http1")
                                          user-agent (update :user-agent str ".http1")))
     :http2-client (-> (cond-> config
                         client-name (update :client-name str "-http2")
                         user-agent (update :user-agent str ".http2"))
                       (assoc :transport http2-transport)
                       http-client-factory)}))

(defn select-http-client
  "Returns the appropriate http client based on the backend protocol."
  [backend-proto {:keys [http1-client http2-client]}]
  (cond
    (contains? #{"h2" "h2c"} backend-proto) http2-client
    (contains? #{"http" "https"} backend-proto) http1-client
    :else (throw (ex-info (str "Unsupported backend-proto: " backend-proto) {}))))

(defn backend-protocol->http-version
  "Determines the protocol version to use for the request to the backend.
   Returns HTTP/2.0 for http/2 backends.
   Returns HTTP/1.1, for non-http/2 backends."
  [^String backend-proto]
  (cond
    (contains? #{"h2" "h2c"} backend-proto) "HTTP/2.0"
    (contains? #{"http" "https"} backend-proto) "HTTP/1.1"
    :else (throw (ex-info (str "Unsupported backend-proto: " backend-proto) {}))))

(defn backend-proto->scheme
  "Determines the protocol scheme from the backend proto"
  [backend-proto]
  (case backend-proto
    "http" "http"
    "https" "https"
    "h2" "https"
    "h2c" "http"
    backend-proto))

(defn http2?
  "Returns true if the http versions represents a http2 request"
  [version]
  (= "HTTP/2.0" version))

(defn grpc?
  "Returns true if the request represents a grpc request"
  [{:strs [content-type]} proto-version]
  (and (= "HTTP/2.0" proto-version) (= content-type "application/grpc")))

(defn service-unavailable?
  "Returns true if the response represents the service is unavailable.
   This means either the response status is 503 or the grpc response status is UNAVAILABLE, i.e. 14."
  [request response]
  (or (= 503 (:status response))
      (and (grpc? (:headers request) (:client-protocol request))
           (= "14" (get-in response [:headers "grpc-status"])))))
