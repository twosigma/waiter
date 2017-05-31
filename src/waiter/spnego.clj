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
(ns waiter.spnego
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [ring.middleware.cookies :refer (cookies-response)]
            [ring.util.response :refer (header status response)]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.utils :as utils]
            [taoensso.nippy :as nippy])
  (:import [java.nio.charset StandardCharsets]
           [org.eclipse.jetty.util UrlEncoded]
           [org.ietf.jgss GSSManager GSSCredential GSSContext]))

(def ^:const WAITER-AUTH-COOKIE-NAME "x-waiter-auth")
(def ^:const WAITER-AUTH-COOKIE-RE #"(?i)x-waiter-auth=([^;]+)")

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
  (-> (response "Unauthorized")
      (status 401)
      (header "Content-Type" "text/html")
      (header "WWW-Authenticate" "Negotiate")
      (header cid/HEADER-CORRELATION-ID (cid/get-correlation-id))
      (cookies-response)))

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

(defn correct-service-cookies
  "Ring expects the Set-Cookie header to be a vector of cookies. This puts them in the 'right' format"
  [resp]
  (update-in resp [:headers "Set-Cookie"] #(if (string? %) [%] %)))

(defn cookies-async-resp
  [resp]
  (log/debug "making a response with cookies: " resp)
  (if (map? resp)
    (cookies-response resp)
    (async/go
      (-> (async/<! resp)
          correct-service-cookies
          cookies-response))))

(defn add-cached-auth
  [resp password princ]
  ;(nippy/thaw (b64/decode (.getBytes (String. (b64/encode (nippy/freeze "foo")) "utf-8"))))
  (letfn [(add-auth [resp]
            (assoc-in resp [:cookies WAITER-AUTH-COOKIE-NAME]
                      {:value (String. ^bytes (b64/encode (nippy/freeze [princ (System/currentTimeMillis)] {:password password :compressor nil})) "utf-8")
                       :max-age (-> 1 t/days t/in-seconds)}))]
    (if (map? resp)
      (add-auth resp)
      (async/go (add-auth (async/<! resp))))))

(defn url-decode
  "Decode a URL-encoded string.  java.util.URLDecoder is super slow.  Also Jetty 9.3 adds an overload
  to decodeString that takes just a string.  This implementation should use that once we upgrade."
  [^String str]
  (when str
    (UrlEncoded/decodeString str 0 (count str) StandardCharsets/UTF_8)))

(defn get-auth-cookie-value
  [cookie-string]
  (when cookie-string
    (if-let [^String cookie-value (-> (re-find WAITER-AUTH-COOKIE-RE cookie-string)
                                     (second))]
      (url-decode cookie-value))))

(def cookie-cache (-> {}
                      (cache/ttl-cache-factory :ttl (-> 300 t/seconds t/in-millis))
                      atom))

(defn decode-cookie
  "Decode the Waiter auth cookie"
  [^String waiter-cookie password]
  (utils/atom-cache-get-or-load
    cookie-cache waiter-cookie
    (fn [] (-> waiter-cookie
               (.getBytes)
               (b64/decode)
               (nippy/thaw {:password password :v1-compatibility? false :compressor nil})))))

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
              (let [decoded-cookie (decode-cookie waiter-cookie password)]
                (if (seq decoded-cookie)
                  decoded-cookie
                  (do (log/warn "Invalid decoded cookie:" decoded-cookie)
                      nil))))
            (catch Exception _ (do
                                 (log/info "Failed to decode cookie:" waiter-cookie)
                                 nil)))
          well-formed? (and decoded-auth-cookie (integer? auth-time) (string? auth-princ) (= 2 (count decoded-auth-cookie)))]
      (log/debug "Well-formed?" decoded-auth-cookie (integer? auth-time) (string? auth-princ) (= 2 (count decoded-auth-cookie)))
      (cond
        ;; Use the cookie, if not expired
        (and well-formed? (> (+ auth-time (-> 1 t/days t/in-millis)) (System/currentTimeMillis)))
        (do (log/debug "Using sane cookies")
            (-> req
                (assoc
                  :krb5-authenticated-princ auth-princ
                  :authorization/user (first (str/split auth-princ #"@" 2)))
                (rh)
                (cookies-async-resp)))
        (get-in req [:headers "authorization"])
        (let [^GSSContext gss_context (gss-context-init)]
          (let [token (do-gss-auth-check gss_context req)]
            (if (.isEstablished gss_context)
              (let [princ (gss-get-princ gss_context)
                    resp (-> req
                             (assoc
                               :krb5-authenticated-princ princ
                               :authorization/user (first (str/split princ #"@" 2)))
                             (rh)
                             (add-cached-auth password princ)
                             (cookies-async-resp))]
                (log/debug "Added cookies to response")
                (if token
                  (if (map? resp)
                    (header resp "WWW-Authenticate" token)
                    (async/go
                      (header (async/<! resp) "WWW-Authenticate" token)))
                  resp))
              (response-401-negotiate))))
        :else
        (response-401-negotiate)))))
