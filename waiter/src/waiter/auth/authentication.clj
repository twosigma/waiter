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
(ns waiter.auth.authentication
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [waiter.cookie-support :as cookie-support]
            [waiter.headers :as headers]
            [waiter.middleware :as middleware]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils])
  (:import (java.net URI)))

(def ^:const AUTH-COOKIE-EXPIRES-AT "x-auth-expires-at")

(def ^:const AUTH-COOKIE-NAME "x-waiter-auth")

(def ^:const auth-expires-at-uri "/.well-known/auth/expires-at")

(def ^:const auth-keep-alive-uri "/.well-known/auth/keep-alive")

(defprotocol Authenticator
  (wrap-auth-handler [this request-handler]
    "Attaches middleware that enables the application to perform authentication.
     The middleware should
     - issue a 401 challenge, or redirect, to get the client to authenticate itself,
     - or upon successful authentication populate the request with :authorization/user and :authorization/principal"))

(defprotocol CompositeAuthenticator
  (get-authentication-providers [this]
    "Get a list of supported authentication provider names."))

(extend-protocol CompositeAuthenticator
  Object
  (get-authentication-providers [_] []))

(defprotocol CallbackAuthenticator
  (process-callback [this request]
    "Process any requests that might come in after initiating authentication. e.g. receive a request
     with an authentication assertion after redirecting a user to authenticate with an identity provider."))

(extend-protocol CallbackAuthenticator
  Object
  (process-callback [this _]
    (throw (ex-info (str this " does not support authentication callbacks.")
                    {:status http-400-bad-request}))))

(defn- add-cached-auth
  "Adds the Waiter auth related cookies into the response."
  [response password principal age-in-seconds auth-metadata]
  (let [creation-time (t/now)
        creation-time-millis (tc/to-long creation-time)
        creation-time-secs (tc/to-epoch creation-time)
        cookie-age-in-seconds (or age-in-seconds (-> 1 t/days t/in-seconds))
        expiry-time-secs (+ creation-time-secs cookie-age-in-seconds)
        cookie-metadata (assoc auth-metadata :expires-at expiry-time-secs)
        cookie-value [principal creation-time-millis cookie-metadata]]
    (-> response
      ;; x-auth-expires-at cookie allows javascript code to introspect when the auth cookie will expire and eagerly re-authenticate
      (cookie-support/add-cookie AUTH-COOKIE-EXPIRES-AT (str expiry-time-secs) cookie-age-in-seconds false)
      (cookie-support/add-encoded-cookie password AUTH-COOKIE-NAME cookie-value cookie-age-in-seconds))))

(defn select-auth-params
  "Returns a map that contains only the auth params from the input map"
  [m]
  (select-keys m [:authorization/method :authorization/principal :authorization/user]))

(defn build-auth-params-map
  "Creates a map intended to be merged into requests/responses."
  ([method principal]
   (build-auth-params-map method principal nil))
  ([method principal metadata]
   (let [user (utils/principal->username principal)]
     (cond-> {:authorization/method method
              :authorization/principal principal
              :authorization/user user}
       metadata (assoc :authorization/metadata metadata)))))

(defn request-authenticated?
  "Returns true if the authorization info is already available in the input map."
  [{:keys [authorization/principal authorization/user]}]
  (and principal user))

(defn handle-request-auth
  "Invokes the given request-handler on the given request, adding the necessary
  auth headers on the way in, and the x-waiter-auth cookie on the way out."
  ([handler request auth-params-map password]
   (handle-request-auth handler request auth-params-map password nil))
  ([handler request auth-params-map password age-in-seconds]
   (let [{:keys [authorization/metadata authorization/principal]} auth-params-map
         handler' (middleware/wrap-merge handler auth-params-map)]
     (-> request
       handler'
       (add-cached-auth password principal age-in-seconds metadata)))))

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
   In addition, the auth-principal must be a string and the expires at time must be greater than current time."
  [[auth-principal auth-time auth-metadata :as decoded-auth-cookie]]
  (if decoded-auth-cookie
    (let [expires-at (when (map? auth-metadata)
                       (get auth-metadata :expires-at))]
      (let [well-formed? (and decoded-auth-cookie
                              (<= 2 (count decoded-auth-cookie) 3)
                              (integer? auth-time)
                              (string? auth-principal)
                              (map? auth-metadata)
                              (integer? expires-at))
            result (and well-formed? (> (-> expires-at t/seconds t/in-millis) (System/currentTimeMillis)))]
        (when-not result
          (log/info "decoded auth cookie is not valid" auth-time auth-metadata))
        result))
    false))

(defn get-auth-cookie-value
  "Retrieves the auth cookie."
  [cookie-string]
  (cookie-support/cookie-value cookie-string AUTH-COOKIE-NAME))

(defn get-and-decode-auth-cookie-value
  "Retrieves the auth cookie and decodes it using the provided password."
  [headers password]
  (some-> (get headers "cookie")
    (get-auth-cookie-value)
    (decode-auth-cookie password)))

(defn remove-auth-cookie
  "Removes the auth cookies"
  [cookie-string]
  (-> cookie-string
    (cookie-support/remove-cookie AUTH-COOKIE-EXPIRES-AT)
    (cookie-support/remove-cookie AUTH-COOKIE-NAME)))

(defn select-auth-header
  "Filters and return the first authorization header that passes the predicate."
  [{:keys [headers]} predicate]
  (let [{:strs [authorization]} headers
        auth-headers (if (string? authorization)
                       (str/split (str authorization) #",")
                       authorization)]
    (some #(when (predicate %) %) auth-headers)))

;; An anonymous request does not contain any authentication information.
;; This is equivalent to granting everyone access to the resource.
;; The anonymous authenticator attaches the principal of run-as-user to the request.
;; In particular, this enables requests to launch processes as run-as-user.
;; Use of this authentication mechanism is strongly discouraged for production use.
;; Real middleware implementations should:
;;   - either issue a 401 challenge asking the client to authenticate itself,
;;   - or upon successful authentication populate the request with :authorization/user and :authorization/principal"
(defrecord SingleUserAuthenticator [run-as-user password]
  Authenticator
  (wrap-auth-handler [_ request-handler]
    ;; authentication behavior can be controlled by the following headers:
    ;; - Authorization: SingleUser <mode>, or
    ;; - x-waiter-single-user <mode>
    ;; The second header is provided to allow for scenarios wheere authorization header is not provided in the request.
    (let [single-user-prefix "SingleUser "]
      (fn anonymous-handler [{:keys [headers] :as request}]
        (let [auth-header (select-auth-header request #(str/starts-with? % single-user-prefix))
              {:strs [x-waiter-single-user]} headers
              auth-path (when auth-header
                          (str/trim (subs auth-header (count single-user-prefix))))]
          (cond
            (or (= "unauthorized" x-waiter-single-user) (= "unauthorized" auth-path))
            (utils/attach-waiter-source
              {:headers {"www-authenticate" "SingleUser"} :status http-401-unauthorized})
            (or (= "forbidden" x-waiter-single-user) (= "forbidden" auth-path))
            (utils/attach-waiter-source
              {:headers {} :status http-403-forbidden})
            (str/blank? auth-path)
            (let [auth-params-map (build-auth-params-map :single-user run-as-user)]
              (handle-request-auth request-handler request auth-params-map password))
            :else
            (utils/attach-waiter-source
              {:headers {"x-waiter-single-user" (str "unknown operation: " auth-path)} :status http-400-bad-request})))))))

(defn one-user-authenticator
  "Factory function for creating single-user authenticator"
  [{:keys [password run-as-user]}]
  {:pre [(some? password)
         (not (str/blank? run-as-user))]}
  (log/warn "use of single-user authenticator is strongly discouraged for production use:"
            "requests will use principal" run-as-user)
  (->SingleUserAuthenticator run-as-user password))

(defn wrap-auth-cookie-handler
  "Returns a handler that can authenticate a request that contains a valid x-waiter-auth cookie."
  [password handler]
  (fn auth-cookie-handler
    [{:keys [headers] :as request}]
    (let [decoded-auth-cookie (get-and-decode-auth-cookie-value headers password)
          [auth-principal _ auth-metadata] decoded-auth-cookie]
      (if (decoded-auth-valid? decoded-auth-cookie)
        (let [auth-params-map (build-auth-params-map :cookie auth-principal auth-metadata)
              handler' (middleware/wrap-merge handler auth-params-map)]
          (handler' request))
        (handler request)))))

(defn process-auth-expires-at-request
  "Handler to allow a client to update its knowledge of when a user's cookie-based credentials expire.
   Returns a json response containing the expires-at key containing the expiration time in UTC epoch seconds.
   Relies on the metadata in the x-waiter-auth cookie."
  [password {:keys [headers]}]
  (let [decoded-auth-cookie (get-and-decode-auth-cookie-value headers password)
        [auth-principal _ auth-metadata] decoded-auth-cookie
        {:keys [expires-at]} auth-metadata
        cookie-valid? (decoded-auth-valid? decoded-auth-cookie)
        sanitized-expires-at (or (when cookie-valid? expires-at) 0)]
    (log/info "waiter auth cookie parsed"
              (cond-> {:cookie-valid? cookie-valid? :expires-at expires-at}
                cookie-valid?
                (assoc :auth-principal auth-principal)))
    (utils/attach-waiter-source (utils/clj->json-response {:expires-at sanitized-expires-at
                                                           :principal auth-principal}))))

(defn attach-authorization-headers
  "Attaches authentication description headers into the response."
  [{:keys [authorization/method authorization/principal authorization/user] :as response}]
  (update response :headers
          (fn [headers]
            (cond-> headers
              method (assoc "x-waiter-auth-method" (name method))
              principal (assoc "x-waiter-auth-principal" (str principal))
              user (assoc "x-waiter-auth-user" (str user))))))

(let [auth-keep-alive-done-parameter "done=true"
      auth-keep-alive-done-parameter-replacement "waiter-redirect=true"]

  (defn remove-done-param-from-keep-alive-https-redirect
    "Removes the done=true query parameter from the response when it is an https redirect to the keep-alive endpoint.
     Keeping the done=true parameter would prevent authentication from being triggered for soon-to-expire cookies after
     the requests comes back with https.
     To simplify processing of the query string, we replace the done=true parameter with another parameter,
     waiter-redirect=true, reflecting that the request was redirected."
    [{:keys [headers status] :as response}]
    (let [{:strs [location]} headers
          location-uri (when-not (str/blank? location)
                         (URI. location))]
      (cond-> response
        (and location-uri
             (contains? #{http-302-moved-temporarily http-307-temporary-redirect} status)
             (= "https" (.getScheme location-uri))
             (= auth-keep-alive-uri (.getPath location-uri)))
        (assoc-in [:headers "location"]
                  (str/replace location auth-keep-alive-done-parameter auth-keep-alive-done-parameter-replacement)))))

  (defn process-auth-keep-alive-request
    "Handler to eagerly trigger authentication workflow even if cookie has not yet expired.
     This allows clients to pre-emptively refresh credentials before they expire.
     Presence of done parameter is used to avoid infinite auth redirect loops and will return 204 No Content.
     Invalid offset parameters result in a 400 error.
     Missing offset, soon to expire cookie (based on offset) or invalid auth cookie will trigger the authentication workflow.
     If cookie is expected to be live longer than offset value, return 204 No Content."
    [token->token-parameters waiter-hostnames password auth-handler {:keys [headers query-string] :as request}]
    (let [{:strs [done offset]} (-> request ru/query-params-request :query-params)
          offset-parsed (utils/parse-int offset)
          decoded-auth-cookie (get-and-decode-auth-cookie-value headers password)
          [auth-principal _ cookie-metadata] decoded-auth-cookie
          current-epoch-time (tc/to-epoch (t/now))
          {:keys [expires-at]} cookie-metadata
          ;; handle legacy cookies which will not have this value set
          expires-at (or expires-at current-epoch-time)
          {:keys [passthrough-headers waiter-headers]} (headers/split-headers headers)
          {:keys [token]} (sd/retrieve-token-from-service-description-or-hostname
                            waiter-headers passthrough-headers waiter-hostnames)
          waiter-token? (and token (not-empty (token->token-parameters token)))]
      (log/info auth-principal "cookie expires at" expires-at "offset is" offset-parsed)
      (cond
        ;; invalid token returns a 404
        (and token (not waiter-token?))
        (utils/clj->json-response {:message (str "Unknown token: " token)}
                                  :status http-404-not-found)

        ;; avoid infinite redirect loop
        done
        (cond-> (utils/attach-waiter-source {:status http-204-no-content})
          waiter-token?
          (assoc :waiter/token token))

        ;; offset parameter provided, but cannot be parsed
        (and offset (nil? offset-parsed))
        (cond-> (utils/clj->json-response {:message "Unable to parse offset parameter"
                                       :parameter {:offset offset}}
                                      :status http-400-bad-request)
          waiter-token?
          (assoc :waiter/token token))

        ;; offset parameter must be positive when provided
        (and offset-parsed (not (pos? offset-parsed)))
        (cond-> (utils/clj->json-response {:message "Invalid offset parameter"
                                           :parameter {:offset offset-parsed}}
                                          :status http-400-bad-request)
          waiter-token?
          (assoc :waiter/token token))

        ;; trigger auth workflow if
        ;; - offset query parameter is missing;
        ;; - cookie has already expired; or
        ;; - including offset time will cause the cookie to expire
        (or (nil? offset)
            (not (decoded-auth-valid? decoded-auth-cookie))
            (>= (+ current-epoch-time offset-parsed) expires-at))
        (do
          (log/info "initiating authentication flow for keep-alive")
          (-> request
            ;; ensure loop back to this endpoint terminates instead of continuously triggering re-authentication
            (assoc :query-string (str query-string (when query-string "&") auth-keep-alive-done-parameter))
            ;; remove existing auth cookies to force re-authentication even if current cookie is valid
            (update-in [:headers "cookie"] remove-auth-cookie)
            ;; trigger auth as if it is a proxy request for token-based requests,
            ;; e.g. OIDC and JWT auth behavior may be different for Proxy and Waiter api requests
            (assoc :waiter-api-call? (not waiter-token?))
            (auth-handler)
            (ru/update-response
              (fn [response]
                (cond-> (-> response
                          (remove-done-param-from-keep-alive-https-redirect))
                  waiter-token?
                  (assoc :waiter/token token)
                  (utils/request-flag headers "x-waiter-debug")
                  (attach-authorization-headers))))))

        ;; default response
        :else
        (cond-> (utils/attach-waiter-source {:status http-204-no-content})
          waiter-token?
          (assoc :waiter/token token))))))
