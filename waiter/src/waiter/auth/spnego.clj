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
            [waiter.metrics :as metrics]
            [waiter.util.utils :as utils])
  (:import (java.util.concurrent ThreadPoolExecutor)
           (org.ietf.jgss GSSManager GSSCredential GSSContext GSSException)))

(def ^:const negotiate-prefix "Negotiate ")

(defn- negotiate-token?
  "Predicate to determine if an authorization header represents a spnego negotiate token."
  [authorization]
  (str/starts-with? (str authorization) negotiate-prefix))

(defn decode-input-token
  "Decode the input token from the negotiate line, expects the authorization token to exist"
  ^bytes [request]
  (when-let [negotiate-token (auth/select-auth-header request negotiate-token?)]
    (some-> negotiate-token (str/split #" " 2) last str .getBytes b64/decode)))

(defn encode-output-token
  "Take a token from a gss accept context call and encode it for use in a -authenticate header"
  [token]
  (str negotiate-prefix (String. ^bytes (b64/encode token))))

(defn do-gss-auth-check
  [^GSSContext gss-context req]
  (when-let [intok (decode-input-token req)]
    (when-let [ntok (.acceptSecContext gss-context intok 0 (alength intok))]
      (encode-output-token ntok))))

(defn response-401-negotiate
  "Tell the client you'd like them to use kerberos"
  [request]
  (log/info "triggering 401 negotiate for spnego authentication")
  (counters/inc! (metrics/waiter-counter "core" "response-status" "401"))
  (meters/mark! (metrics/waiter-meter "core" "response-status-rate" "401"))
  (-> {:headers {"www-authenticate" (str/trim negotiate-prefix)}
       :message "Unauthorized"
       :status 401}
    (utils/data->error-response request)
    (cookies/cookies-response)))

(defn response-503-temporarily-unavailable
  "Tell the client you're overloaded and would like them to try later"
  [request]
  (log/info "triggering 401 negotiate for spnego authentication")
  (counters/inc! (metrics/waiter-counter "core" "response-status" "503"))
  (meters/mark! (metrics/waiter-meter "core" "response-status-rate" "503"))
  (-> {:message "Too many Kerberos authentication requests"
       :status 503}
    (utils/data->error-response request)
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
            (catch GSSException ex
              (log/error ex "gss exception during kerberos auth")
              (async/>!! response-chan
                         {:error (ex-info "Error during Kerberos authentication"
                                          {:details (.getMessage ex)
                                           :status 403}
                                          ex)}))
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
  (fn require-gss-handler [request]
    (cond
      ;; Ensure we are not already queued with lots of Kerberos auth requests
      (too-many-pending-auth-requests? thread-pool-executor max-queue-length)
      (response-503-temporarily-unavailable request)
      ;; Try and authenticate using kerberos and add cookie in response when valid
      (auth/select-auth-header request negotiate-token?)
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
                    (let [response (auth/handle-request-auth request-handler request :spnego principal password)]
                      (log/debug "added cookies to response")
                      (if token
                        (if (map? response)
                          (rr/header response "www-authenticate" token)
                          (let [actual-response (async/<! response)]
                            (rr/header actual-response "www-authenticate" token)))
                        response))
                    (response-401-negotiate request))
                  (catch Throwable th
                    (log/error th "error while processing response")
                    th))
                error)))))
      ;; Default to unauthorized
      :else
      (response-401-negotiate request))))
