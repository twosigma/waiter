;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
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

;; Decode the input token from the negotiate line
;; expects the authorization token to exist
;;
(defn decode-input-token ^bytes
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
  (log/info "Triggering 401 negotiate for spnego authentication")
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

(defn add-cached-auth
  [response password princ]
  (cookie-support/add-encoded-cookie response password auth/AUTH-COOKIE-NAME [princ (System/currentTimeMillis)] 1))

(defn get-auth-cookie-value
  [cookie-string]
  (cookie-support/cookie-value cookie-string auth/AUTH-COOKIE-NAME))

(defn require-gss
  "This middleware enables the application to require a SPNEGO
   authentication. If SPNEGO is successful then the handler `rh`
   will be run, otherwise the handler will not be run and 401
   returned instead.  This middleware doesn't handle cookies for
   authentication, but that should be stacked before this handler."
  [rh password]
  (fn require-gss-handler [{:keys [headers] :as req}]
    (let [waiter-cookie (get-auth-cookie-value (get headers "cookie"))
          [auth-princ auth-time :as decoded-auth-cookie]
          (try
            (log/debug "Decoding cookie:" waiter-cookie)
            (when waiter-cookie
              (let [decoded-cookie (cookie-support/decode-cookie-cached waiter-cookie password)]
                (if (seq decoded-cookie)
                  decoded-cookie
                  (do (log/warn "Invalid decoded cookie:" decoded-cookie)
                      nil))))
            (catch Exception e (do
                                 (log/warn e "Failed to decode cookie:" waiter-cookie)
                                 nil)))
          well-formed? (and decoded-auth-cookie (integer? auth-time) (string? auth-princ) (= 2 (count decoded-auth-cookie)))]
      (log/debug "Well-formed?" decoded-auth-cookie (integer? auth-time) (string? auth-princ) (= 2 (count decoded-auth-cookie)))
      (cond
        ;; Use the cookie, if not expired
        (and well-formed? (> (+ auth-time (-> 1 t/days t/in-millis)) (System/currentTimeMillis)))
        (do (log/debug "Using sane cookies")
            (-> req
                (assoc
                  :authenticated-principal auth-princ
                  :authorization/user (first (str/split auth-princ #"@" 2)))
                (rh)
                (cookie-support/cookies-async-response)))
        (get-in req [:headers "authorization"])
        (let [^GSSContext gss_context (gss-context-init)]
          (let [token (do-gss-auth-check gss_context req)]
            (if (.isEstablished gss_context)
              (let [princ (gss-get-princ gss_context)
                    resp (-> req
                             (assoc
                               :authenticated-principal princ
                               :authorization/user (first (str/split princ #"@" 2)))
                             (rh)
                             (add-cached-auth password princ)
                             (cookie-support/cookies-async-response))]
                (log/debug "Added cookies to response")
                (if token
                  (if (map? resp)
                    (rr/header resp "WWW-Authenticate" token)
                    (async/go
                      (rr/header (async/<! resp) "WWW-Authenticate" token)))
                  resp))
              (response-401-negotiate))))
        :else
        (response-401-negotiate)))))
