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
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [ring.middleware.cookies :as cookies]
            [ring.util.response :as rr]
            [waiter.auth.authentication :as auth]
            [waiter.cookie-support :as cookie-support]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics])
  (:import [org.ietf.jgss GSSManager GSSCredential GSSContext]))

(defn decode-input-token
  "Decode the input token from the negotiate line, expects the authorization token to exist"
  ^bytes
  [req]
  (let [enc_tok (get-in req [:headers "authorization"])
        tfields (str/split enc_tok #" ")]
    (when (= "negotiate" (str/lower-case (first tfields)))
      (b64/decode (.getBytes ^String (last tfields))))))

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
      (rr/header cid/HEADER-CORRELATION-ID (cid/get-correlation-id))
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

(defn get-auth-cookie-value
  [cookie-string]
  (cookie-support/cookie-value cookie-string auth/AUTH-COOKIE-NAME))

(defn decode-auth-cookie
  "Decodes the provided cookie using the provided password.
   Returns a sequence containing [auth-principal auth-time]."
  [waiter-cookie password]
  (try
    (log/debug "decoding cookie:" waiter-cookie)
    (when waiter-cookie
      (let [decoded-cookie (cookie-support/decode-cookie-cached waiter-cookie password)]
        (if (seq decoded-cookie)
          decoded-cookie
          (log/warn "invalid decoded cookie:" decoded-cookie))))
    (catch Exception e
      (log/warn e "failed to decode cookie:" waiter-cookie))))

(defn decoded-auth-valid?
  "Verifies whether the decoded authenticated cookie is valid as per the following rules:
   The decoded value must be a sequence in the format: [auth-principal auth-time].
   In addition, the auth-principal must be a string and the auth-time must be less than a day old."
  [[auth-principal auth-time :as decoded-auth-cookie]]
  (log/debug "well-formed?" decoded-auth-cookie (integer? auth-time) (string? auth-principal) (= 2 (count decoded-auth-cookie)))
  (let [well-formed? (and decoded-auth-cookie (integer? auth-time) (string? auth-principal) (= 2 (count decoded-auth-cookie)))
        one-day-in-millis (-> 1 t/days t/in-millis)]
    (and well-formed? (> (+ auth-time one-day-in-millis) (System/currentTimeMillis)))))

(defn assoc-auth-in-request
  "Associate values for authenticated user in the request."
  [request auth-principal]
  (assoc request
    :authenticated-principal auth-principal
    :authorization/user (first (str/split auth-principal #"@" 2))))

(defn require-gss
  "This middleware enables the application to require a SPNEGO
   authentication. If SPNEGO is successful then the handler `request-handler`
   will be run, otherwise the handler will not be run and 401
   returned instead.  This middleware doesn't handle cookies for
   authentication, but that should be stacked before this handler."
  [request-handler password]
  (fn require-gss-handler [{:keys [headers] :as req}]
    (let [waiter-cookie (get-auth-cookie-value (get headers "cookie"))
          [auth-principal _ :as decoded-auth-cookie] (decode-auth-cookie waiter-cookie password)]
      (cond
        ;; Use the cookie, if not expired
        (decoded-auth-valid? decoded-auth-cookie)
        (-> (assoc-auth-in-request req auth-principal)
            (request-handler)
            (cookie-support/cookies-async-response))
        ;; Try and autheticate using kerberos and add cookie in response when valid
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
