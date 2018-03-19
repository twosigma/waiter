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
(ns waiter.auth.spnego
  (:require [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [ring.middleware.cookies :as cookies]
            [ring.util.response :as rr]
            [waiter.auth.authentication :as auth]
            [waiter.middleware :as middleware]
            [waiter.metrics :as metrics])
  (:import (org.apache.commons.codec.binary Base64)
           (org.eclipse.jetty.client.api Authentication$Result Request)
           (org.eclipse.jetty.http HttpHeader)
           (org.ietf.jgss GSSManager GSSCredential GSSContext GSSName Oid)
           (java.net URI)))

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
      (rr/header "Content-Type" "text/html")
      (rr/header "WWW-Authenticate" "Negotiate")
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

(defn gss-get-princ
  [^GSSContext gss]
  (str (.getSrcName gss)))

(defn require-gss
  "This middleware enables the application to require a SPNEGO
   authentication. If SPNEGO is successful then the handler `request-handler`
   will be run, otherwise the handler will not be run and 401
   returned instead.  This middleware doesn't handle cookies for
   authentication, but that should be stacked before this handler."
  [request-handler password]
  (fn require-gss-handler [{:keys [headers] :as req}]
    (let [waiter-cookie (auth/get-auth-cookie-value (get headers "cookie"))
          [auth-principal _ :as decoded-auth-cookie] (auth/decode-auth-cookie waiter-cookie password)
          auth-params-map (auth/auth-params-map auth-principal)]
      (cond
        ;; Use the cookie, if not expired
        (auth/decoded-auth-valid? decoded-auth-cookie)
        (let [request-handler' (middleware/wrap-merge request-handler auth-params-map)]
          (request-handler' req))
        ;; Try and authenticate using kerberos and add cookie in response when valid
        (get-in req [:headers "authorization"])
        (let [^GSSContext gss_context (gss-context-init)
              token (do-gss-auth-check gss_context req)]
          (if (.isEstablished gss_context)
            (let [principal (gss-get-princ gss_context)
                  user (first (str/split principal #"@" 2))
                  resp (auth/handle-request-auth request-handler req user principal password)]
              (log/debug "added cookies to response")
              (if token
                (if (map? resp)
                  (rr/header resp "WWW-Authenticate" token)
                  (async/go
                    (rr/header (async/<! resp) "WWW-Authenticate" token)))
                resp))
            (response-401-negotiate)))
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
