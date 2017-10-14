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
(ns waiter.auth.oauth
  (:require [bidi.bidi :as bidi]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [comb.template :as template]
            [digest]
            [full.async :as fa]
            [plumbing.core :as pc]
            [plumbing.graph :as graph]
            [qbits.jet.client.http :as http]
            [ring.middleware.params :as ring-params]
            [ring.util.codec :as ring-codec]
            [waiter.auth.authentication :as auth]
            [waiter.cookie-support :as cookie-support]
            [waiter.utils :as utils])
  (:import java.security.SecureRandom
           org.eclipse.jetty.util.HttpCookieStore$Empty
           org.eclipse.jetty.util.UrlEncoded))

(def secure-random (SecureRandom.))

(def OAUTH-COOKIE-NAME "x-waiter-oauth")

(defn make-http-client
  "Instantiates and returns a new HttpClient without a cookie store"
  []
  (let [client (http/client)]
    (.setCookieStore client (HttpCookieStore$Empty.))
    client))

(defn random-str
  "Generates a random string."
  []
  (let [bytes (byte-array 1024)
        _ (.nextBytes secure-random bytes)]
        (digest/sha1 bytes)))

(defn verify-oauth-cookie
  [{:keys [headers]} expected-cookie-value password]
  (let [cookie-value (cookie-support/cookie-value (get headers "cookie") OAUTH-COOKIE-NAME)
        decoded-cookie-value (cookie-support/decode-cookie cookie-value password)]
    (when-not (= expected-cookie-value decoded-cookie-value)
      (throw (ex-info "Invalid OAuth cookie" {:status 403})))
    (log/info "state cookie matched")))

(defprotocol OAuthProvider
  "An OAuth provider"
  (display-name [this]
    "Gets a display name for the provider.")
  (redirect [this request]
    "Handles a redirect to the OAuth provider.  Returns an async response.")
  (authenticate [this request]
    "Handles the response from the OAuth provider.  Returns an async response."))

(defn google-discovery-document
  "Gets Google's discovery document."
  [http-client]
  (fa/go-try
    (let [discovery-uri "https://accounts.google.com/.well-known/openid-configuration"
          {:keys [body]} (async/<! (http/get http-client discovery-uri {:headers {"accept" "application/json"}}))]
      (try
        (json/read-str (async/<! body))
        (catch Exception e
          (throw (ex-info "Couldn't retrieve Google discovery document" {:status 403})))))))

(defn make-uri
  [uri query-params]
  (str uri "?"
       (->> query-params
           (map (fn [[name value]]
                  (str name "=" (UrlEncoded/encodeString value))))
            (str/join "&"))))

; See https://developers.google.com/identity/protocols/OpenIDConnect#authenticatingtheuser
(defrecord GoogleOAuthProvider [authenticate-uri client-id client-secret http-client password]
  OAuthProvider
  (display-name [_] "Google")
  (redirect [_ {:keys [query-string] {:keys [path]} :route-params}]
    (fa/go-try
      (let [token (random-str)
            state (-> {:path (str \/ path)
                       :token token
                       :query-string query-string}
                      json/write-str)
            {:strs [authorization_endpoint]} (fa/<? (google-discovery-document http-client))
            _ (when-not authorization_endpoint
                (throw (ex-info "Couldn't get Google authorization endpoint" {:status 403})))
            location (make-uri authorization_endpoint
                               {"client_id" client-id
                                "nonce" (random-str)
                                "response_type" "code"
                                "redirect_uri" authenticate-uri
                                "scope" "openid email"
                                "state" state})]
        (-> {:status 307
             :headers {"location" location}}
            (cookie-support/add-encoded-cookie password OAUTH-COOKIE-NAME token (-> 15 t/minutes t/in-seconds))))))
  (authenticate [_ request]
    (fa/go-try
      (let [{:strs [code state]} (:params (ring-params/params-request request))
            {:strs [path query-string token]} (try (-> state
                                                       json/read-str)
                                                   (catch Exception e
                                                     (throw (ex-info "Couldn't parse state from Google" {:status 403}))))
            _ (verify-oauth-cookie request token password)
            {:strs [token_endpoint]} (fa/<? (google-discovery-document http-client))
            _ (when-not token_endpoint
                (throw (ex-info "Couldn't get Google token endpoint" {:status 403})))
            {:keys [body status]} (async/<! (http/post http-client token_endpoint
                                                       {:form-params {"client_id" client-id
                                                                      "client_secret" client-secret
                                                                      "code" code
                                                                      "grant_type" "authorization_code"
                                                                      "redirect_uri" authenticate-uri}}))
            _  (when-not (= status 200)
                 (throw (ex-info "Invalid token response from Google" {:status 403})))
            {:strs [id_token] :as response} (try
                                     (json/read-str (async/<! body))
                                     (catch Exception e
                                       (throw (ex-info "Couldn't parse JSON from Google" {:status 403}))))
            _ (log/info "google response" response)
            _  (when-not id_token
                 (throw (ex-info "Missing id token from Google" {:status 403})))
            {:strs [email email_verified]} (try
                                             (-> id_token
                                                 (str/split #"\.")
                                                 second
                                                 .getBytes
                                                 b64/decode
                                                 String.
                                                 json/read-str)
                                             (catch Exception e
                                               (throw (ex-info "Coudln't parse id token from Google"
                                                               {:status 403}))))
            _ (when-not (and (not (str/blank? email)) email_verified)
                (throw (ex-info "Can't get email from Google" {:status 403})))]
        (-> {:status 307
             :headers {"location" (cond-> path
                                    (not (str/blank? query-string)) (str \? query-string))}}
            (auth/add-cached-auth password email))))))

; See https://developer.github.com/apps/building-integrations/setting-up-and-registering-github-apps/identifying-users-for-github-apps/
(defrecord GitHubOAuthProvider [authenticate-uri client-id client-secret http-client password]
  OAuthProvider
  (display-name [_] "GitHub")
  (redirect [_ _]
    (fa/go-try
      (let [state (random-str)
            location (make-uri "https://github.com/login/oauth/authorize"
                               {"client_id" client-id
                                "scope" "user:email"
                                "redirect_uri" authenticate-uri
                                "state" state})]
        (-> {:status 307
             :headers {"location" location}}
            (cookie-support/add-encoded-cookie password OAUTH-COOKIE-NAME state (-> 15 t/minutes t/in-seconds))))))

  (authenticate [_ request]
    (fa/go-try
      (let [{:strs [code state]} (:params (ring-params/params-request request))
            _ (verify-oauth-cookie request state password)
            {:keys [body status]} (async/<! (http/post http-client "https://github.com/login/oauth/access_token"
                                                       {:form-params {"client_id" client-id
                                                                      "client_secret" client-secret
                                                                      "code" code
                                                                      "state" state}}))
            _  (when-not (= status 200)
                 (throw (ex-info "Invalid access token response from GitHub" {:status 403})))
            access-token-body (async/<! body)
            {:strs [access_token]} (-> access-token-body
                                       (ring-codec/form-decode))
            _  (when (str/blank? access_token)
                 (throw (ex-info "Invalid access token from GitHub" {:status 403})))
            {:keys [body status]} (async/<! (http/get http-client "https://api.github.com/user/emails"
                                                      {:headers {"authorization" (str "token " access_token)}}))
            _ (when-not (= status 200)
                (throw (ex-info "Invalid user API response from GitHub" {:status 403})))
            response-body (async/<! body)
            emails (try (json/read-str response-body)
                        (catch Exception e
                          (throw (ex-info "Invalid JSON from GitHub" {:status 403}))))
            email (->> emails
                       (keep (fn [{:strs [email primary verified]}]
                               (when (and primary verified)
                                 email)))
                       first)
            _ (when-not email
                (throw (ex-info "Can't get email from GitHub" {:status 403})))]
        (-> {:status 307
             :headers {"location" "finalredirect"}}
            (auth/add-cached-auth password email))))))

(defn provider-list-handler
  "Responds with a login page listing available OAuth providers."
  [providers {:keys [query-string request-method] {:keys [path]} :route-params}]
  (case request-method
    :get {:body (template/eval (slurp (io/resource "web/oauth.html"))
                               {:providers providers
                                :display-name (fn [provider] (display-name provider))
                                :path path
                                :query-string query-string})}
    (throw (ex-info "Invalid request method"
                    {:request-method request-method
                     :status 405}))))

(defn match-oauth-route
  "Match an OAuth route based upon a uri."
  [uri]
  (let [route-map ["/waiter-auth/oauth" {["/providers/" [#".*" :path]] :provider-list
                                         "/" {[:provider "/redirect/" [#".*" :path]] :redirect
                                              [:provider "/authenticate"] :authenticate} }]]
    (bidi/match-route route-map uri)))

(defn wrap-oauth
  "Middleware for handling OAuth."
  [handler-fn run-as-user password providers]
  (fn [{:keys [headers query-string uri] :as request}]
    (try
      (let [{:keys [handler route-params] :as match} (match-oauth-route uri)
            request' (assoc request :route-params route-params)
            provider-fn (fn [] (-> route-params :provider keyword providers))]
        (case handler
          :provider-list (provider-list-handler providers request')
          :redirect (redirect (provider-fn) request')
          :authenticate (authenticate (provider-fn) request')
          ; else
          (let [waiter-cookie (auth/get-auth-cookie-value (get headers "cookie"))
                [auth-principal _ :as decoded-auth-cookie] (auth/decode-auth-cookie waiter-cookie password)]
            (if (auth/decoded-auth-valid? decoded-auth-cookie)
              (-> request
                  (auth/assoc-auth-in-request auth-principal)
                  handler-fn)
              {:status 307
               :headers {"location" (cond-> (str "/waiter-auth/oauth/providers" uri)
                                      (not (str/blank? query-string)) (str \? query-string))}}))))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defrecord OAuthAuthenticator [password providers run-as-user]
  auth/Authenticator
  (auth-type [_]
    :oauth)
  (check-user [_ user _]
    user)
  (wrap-auth-handler [_ handler]
    (wrap-oauth handler run-as-user password providers)))

(defn oauth-authenticator
  "Factory function for creating OAuthAuthenticator"
  [{:keys [host password port providers run-as-user]}]
  (let [http-client (make-http-client) {:keys [kinds]} providers
        enabled-providers (pc/map-from-keys
                            (fn [kind]
                              (let [base-uri (str "http://" host ":" port "/waiter-auth/oauth/" (name kind))
                                    authenticate-uri (str base-uri "/authenticate")]
                                (utils/create-component (assoc providers :kind kind)
                                                        :context {:http-client http-client
                                                                  :authenticate-uri authenticate-uri})))
                            kinds)]
    (map->OAuthAuthenticator {:password password
                              :providers enabled-providers
                              :run-as-user run-as-user})))
