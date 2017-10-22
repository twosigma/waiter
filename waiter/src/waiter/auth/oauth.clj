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
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [comb.template :as template]
            [digest]
            [plumbing.core :as pc]
            [plumbing.graph :as graph]
            [qbits.jet.client.http :as http]
            [waiter.auth.authentication :as auth]
            [waiter.cookie-support :as cookie-support]
            [waiter.utils :as utils])
  (:import java.security.SecureRandom
           org.eclipse.jetty.util.HttpCookieStore$Empty
           org.eclipse.jetty.util.UrlEncoded))

(def secure-random (SecureRandom.))

(def OAUTH-COOKIE-NAME "x-waiter-oauth")
(def HASH-QUERY-PARAM-NAME "_waiter_hash")

(defn make-http-client
  "Instantiates and returns a new HttpClient without a cookie store"
  []
  (let [client (http/client {:connect-timeout 5000
                             :idle-timeout 5000})]
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
      (throw (ex-info "Invalid OAuth cookie" {:status 403})))))

(defprotocol OAuthProvider
  "An OAuth provider"
  (display-name [this]
    "Gets a display name for the provider.")
  (redirect [this request]
    "Handles a redirect to the OAuth provider.  Returns an async response.")
  (authenticate [this request]
    "Handles the response from the OAuth provider.  Returns an async response."))

(defn make-uri
  "Takes a URI and adds a formatted query-string from query parameters in a map."
  [uri query-params]
  (str uri "?"
       (->> query-params
            (map (fn [[name value]]
                   (str name "=" (UrlEncoded/encodeString value))))
            (str/join "&"))))

(defn make-location
  "Given a path and query-string, return the value for the Location header
  after successful authentication.
  Assumes the URL fragment (hash) is passed at the end of query-string under a special
  query parameter HASH-QUERY-PARAM-NAME."
  [{:keys [path query-string]}]
  (let [query-string' (when query-string
                        (str \? query-string))
        hash (when (and query-string' (str/includes? query-string' (str HASH-QUERY-PARAM-NAME \=)))
               (-> (subs query-string' (+ (str/index-of query-string' (str HASH-QUERY-PARAM-NAME \=))
                                          (count (str HASH-QUERY-PARAM-NAME \=))))
                   (UrlEncoded/decodeString)))
        query-string'' (if (and query-string' hash)
                         (subs query-string' 0 (dec (str/index-of query-string' (str HASH-QUERY-PARAM-NAME \=))))
                         query-string')]
    (str \/ path query-string'' hash)))

(defn provider-list-handler
  "Responds with a login page listing available OAuth providers."
  [providers {:keys [query-string request-method] {:keys [path]} :route-params}]
  (case request-method
    :get {:body (template/eval (slurp (io/resource "web/oauth.html"))
                               {:providers providers
                                :display-name (fn [provider] (display-name provider))
                                :hash-query-param-name HASH-QUERY-PARAM-NAME
                                :path path
                                :query-string query-string})
          :headers {"content-type" "text/html"}}
    (throw (ex-info "Invalid request method"
                    {:request-method request-method
                     :status 405}))))

(defn match-oauth-route
  "Match an OAuth route based upon a uri."
  [uri]
  (let [route-map ["/waiter-auth/oauth" {["/providers/" [#".*" :path]] :provider-list
                                         "/" {[:provider "/redirect/" [#".*" :path]] :redirect
                                              [:provider "/authenticate"] :authenticate}}]]
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
  [{:keys [password providers run-as-user]}]
  (let [http-client (make-http-client) {:keys [kinds]} providers
        enabled-providers (pc/map-from-keys
                            (fn [kind]
                              (let [authenticate-uri-fn (fn authenticate-uri-fn
                                                          [{{:strs [host]} :headers :as request}]
                                                          (str (utils/request->scheme request) "://"
                                                               host "/waiter-auth/oauth/" (name kind) "/authenticate"))]
                                (utils/create-component (assoc providers :kind kind)
                                                        :context {:http-client http-client
                                                                  :authenticate-uri-fn authenticate-uri-fn})))
                            kinds)]
    (map->OAuthAuthenticator {:password password
                              :providers enabled-providers
                              :run-as-user run-as-user})))
