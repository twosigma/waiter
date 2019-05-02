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
(ns waiter.auth.spnego
  (:require [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [ring.middleware.cookies :as cookies]
            [ring.util.response :as rr]
            [waiter.auth.authentication :as auth]
            [waiter.correlation-id :as cid]
            [waiter.middleware :as middleware]
            [waiter.metrics :as metrics]
            [waiter.util.utils :as utils])
  (:import (org.apache.commons.codec.binary Base64)
           (org.eclipse.jetty.client.api Authentication$Result Request)
           (org.eclipse.jetty.http HttpHeader)
           (org.ietf.jgss GSSManager GSSCredential GSSContext GSSName Oid)
           (java.net URI)
           (java.util.concurrent ThreadPoolExecutor)))

(defn decode-input-token
  "Decode the input token from the negotiate line, expects the authorization token to exist"
  ^bytes
  [req]
  (let [enc_tok (get-in req [:headers "authorization"])
        token-fields (str/split enc_tok #" ")]
    (when (= "negotiate" (str/lower-case (first token-fields)))
      (b64/decode (.getBytes ^String (last token-fields))))))

(defn encode-output-token
  "Take a token from a gss accept context call and encode it for use in a -authenticate header"
  [token]
  (str "Negotiate " (String. ^bytes (b64/encode token))))

(defn do-gss-auth-check
  [^GSSContext gss_context req]
  (when-let [intok (decode-input-token req)]
    (when-let [ntok (.acceptSecContext gss_context intok 0 (alength intok))]
      (encode-output-token ntok))))

(defn response-401-negotiate
  "Tell the client you'd like them to use kerberos"
  []
  (log/info "triggering 401 negotiate for spnego authentication")
  (counters/inc! (metrics/waiter-counter "core" "response-status" "401"))
  (meters/mark! (metrics/waiter-meter "core" "response-status-rate" "401"))
  (-> (rr/response "Unauthorized")
      (rr/status 401)
      (rr/header "Content-Type" "text/plain")
      (rr/header "server" (utils/get-current-server-name))
      (rr/header "WWW-Authenticate" "Negotiate")
      (cookies/cookies-response)))

(defn response-503-temporarily-unavailable
  "Tell the client you're overloaded and would like them to try later"
  []
  (log/info "triggering 401 negotiate for spnego authentication")
  (counters/inc! (metrics/waiter-counter "core" "response-status" "503"))
  (meters/mark! (metrics/waiter-meter "core" "response-status-rate" "503"))
  (-> (rr/response "Too many Kerberos authentication requests")
      (rr/status 503)
      (rr/header "Content-Type" "text/plain")
      (rr/header "server" (utils/get-current-server-name))
      (cookies/cookies-response)))

(defn gss-context-init
  "Initialize a new gss context with name 'svc_name'"
  []
  (let [manager (GSSManager/getInstance)
        creds (.createCredential manager GSSCredential/ACCEPT_ONLY)
        gss (.createContext manager creds)]
    (counters/inc! (metrics/waiter-counter "core" "gss-context-count"))
    (meters/mark! (metrics/waiter-meter "core" "gss-context-creation"))
    gss))

(defn gss-get-principal
  [^GSSContext gss]
  (str (.getSrcName gss)))

(defn too-many-pending-auth-requests?
  "Returns true if there are too many pending Kerberos auth requests."
  [^ThreadPoolExecutor thread-pool-executor max-queue-length]
  (-> thread-pool-executor
      .getQueue
      .size
      (>= max-queue-length)))

(defn populate-gss-credentials
  "Perform Kerberos authentication on the provided thread pool and populate the result in the response channel."
  [^ThreadPoolExecutor thread-pool-executor request response-chan]
  (let [current-correlation-id (cid/get-correlation-id)
        timer-context (timers/start (metrics/waiter-timer "core" "kerberos" "throttle" "delay"))]
    (.execute
      thread-pool-executor
      (fn process-gss-task []
        (cid/with-correlation-id
          current-correlation-id
          (try
            (timers/stop timer-context)
            (let [^GSSContext gss-context (gss-context-init)
                  token (do-gss-auth-check gss-context request)
                  principal (when (.isEstablished gss-context)
                              (gss-get-principal gss-context))]
              (async/>!! response-chan {:principal principal
                                        :token token}))
            (catch Throwable th
              (log/error th "error while performing kerberos auth")
              (async/>!! response-chan {:error th}))
            (finally
              (async/close! response-chan))))))))

(defn require-gss
  "This middleware enables the application to require a SPNEGO
   authentication. If SPNEGO is successful then the handler `request-handler`
   will be run, otherwise the handler will not be run and 401
   returned instead.  This middleware doesn't handle cookies for
   authentication, but that should be stacked before this handler."
  [request-handler ^ThreadPoolExecutor thread-pool-executor max-queue-length password]
  (fn require-gss-handler [{:keys [headers] :as request}]
    (let [waiter-cookie (auth/get-auth-cookie-value (get headers "cookie"))
          [auth-principal _ :as decoded-auth-cookie] (auth/decode-auth-cookie waiter-cookie password)]
      (cond
        ;; Use the cookie, if not expired
        (auth/decoded-auth-valid? decoded-auth-cookie)
        (let [auth-params-map (auth/auth-params-map auth-principal)
              request-handler' (middleware/wrap-merge request-handler auth-params-map)]
          (request-handler' request))
        ;; Ensure we are not already queued with lots of Kerberos auth requests
        (too-many-pending-auth-requests? thread-pool-executor max-queue-length)
        (response-503-temporarily-unavailable)
        ;; Try and authenticate using kerberos and add cookie in response when valid
        (get-in request [:headers "authorization"])         ;TODO FIXME
        (let [current-correlation-id (cid/get-correlation-id)
              gss-response-chan (async/promise-chan)]
          ;; launch task that will populate the response in response-chan
          (populate-gss-credentials thread-pool-executor request gss-response-chan)
          (async/go
            (cid/with-correlation-id
              current-correlation-id
              (let [{:keys [error principal token]} (async/<! gss-response-chan)]
                (if-not error
                  (try
                    (if principal
                      (let [user (first (str/split principal #"@" 2))
                            response (auth/handle-request-auth request-handler request user principal password)]
                        (log/debug "added cookies to response")
                        (if token
                          (if (map? response)
                            (rr/header response "WWW-Authenticate" token)
                            (let [actual-response (async/<! response)]
                              (rr/header actual-response "WWW-Authenticate" token)))
                          response))
                      (response-401-negotiate))
                    (catch Throwable th
                      (log/error th "error while processing response")
                      th))
                  error)))))
        ;; Default to unauthorized
        :else
        (response-401-negotiate)))))

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
              server-princ (str "HTTP@" (.getHost endpoint))
              server-name (.createName gss-manager server-princ GSSName/NT_HOSTBASED_SERVICE spnego-oid)
              gss-context (.createContext gss-manager server-name spnego-oid nil GSSContext/DEFAULT_LIFETIME)
              _ (.requestMutualAuth gss-context true)
              token (.initSecContext gss-context (make-array Byte/TYPE 0) 0 0)
              header (str "Negotiate " (String. (.encode base64 token)))]
          (.header request HttpHeader/AUTHORIZATION header))
        (catch Exception e
          (log/warn e "failure during spnego authentication"))))))
