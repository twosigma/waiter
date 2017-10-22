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
(ns waiter.auth.oauth-test
  (:require [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [full.async :as fa]
            [qbits.jet.client.http :as http]
            [waiter.auth.authentication :as auth]
            [waiter.auth.oauth :refer :all]
            [waiter.auth.oauth.github :refer :all]
            [waiter.auth.oauth.google :refer :all]
            [waiter.cookie-support :as cs]
            [waiter.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           org.eclipse.jetty.util.UrlEncoded))

(deftest test-random-str
  (is (random-str)))

(deftest test-provider-list-handler
  (testing "get"
    (let [{:keys [body headers]} (provider-list-handler
                                   {:github (map->GitHubOAuthProvider {})
                                    :google (map->GoogleOAuthProvider {})}
                                   {:request-method :get})]
      (is body)
      (is (str/includes? body "GitHub"))
      (is (str/includes? body "Google")))))

(deftest test-oauth-authenticator
  (let [config {:factory-fn waiter.auth.oauth/oauth-authenticator
                :providers {:kinds [:github :google]
                            :github {:factory-fn 'waiter.auth.oauth.github/map->GitHubOAuthProvider
                                     :client-id ""
                                     :client-secret ""}
                            :google {:factory-fn 'waiter.auth.oauth.google/map->GoogleOAuthProvider
                                     :client-id ""
                                     :client-secret ""}}
                :run-as-user "user"
                :host "host"
                :port 8080
                :password [:cached "password"]}
        authenticator (oauth-authenticator config)]
    (is authenticator)
    (is (= 2 (-> authenticator :providers count)))))

(deftest test-match-oauth-route
  (is (= {:handler :provider-list :route-params {:path ""}}
         (match-oauth-route "/waiter-auth/oauth/providers/")))
  (is (= {:handler :provider-list :route-params {:path "path"}}
         (match-oauth-route "/waiter-auth/oauth/providers/path")))
  (is (= {:handler :redirect :route-params {:provider "github" :path ""}}
         (match-oauth-route "/waiter-auth/oauth/github/redirect/")))
  (is (= {:handler :redirect :route-params {:provider "github" :path "path"}}
         (match-oauth-route "/waiter-auth/oauth/github/redirect/path")))
  (is (= {:handler :authenticate :route-params {:provider "google"}}
         (match-oauth-route "/waiter-auth/oauth/google/authenticate")))
  (is (= nil (match-oauth-route "/no-match"))))

(deftest test-wrap-oauth
  (let [providers {:dummy (reify OAuthProvider
                            (display-name [_] "Dummy")
                            (redirect [_ _]
                              {:headers {"location" "http://preauth"}
                               :status 307})
                            (authenticate [_ _]
                              {:headers {"location" "http://postauth"}
                               :status 307}))}
        handler (fn [_] {:status 200
                         :body "handler"})
        run-as-user "testuser"
        principal "testuser@example.com"
        now (System/currentTimeMillis)
        password [:cached "password"]
        wrapped-handler (wrap-oauth handler run-as-user password providers)]
    (testing "list providers"
      (let [{:keys [body]} (wrapped-handler {:request-method :get
                                             :uri "/waiter-auth/oauth/providers/"})]
        (is body)
        (is (str/includes? body "Dummy"))))
    (testing "redirect"
      (let [{{:strs [location]} :headers :keys [status]}
            (wrapped-handler {:uri "/waiter-auth/oauth/dummy/redirect/"})]
        (is (= "http://preauth" location))
        (is (= 307 status))))
    (testing "authenticate"
      (let [{{:strs [location]} :headers :keys [status]}
            (wrapped-handler {:uri "/waiter-auth/oauth/dummy/authenticate"})]
        (is (= "http://postauth" location))
        (is (= 307 status))))
    (testing "unauthenticated"
      (let [{{:strs [location]} :headers :keys [status]}
            (wrapped-handler {:uri "/requires-auth"})]
        (is (= "/waiter-auth/oauth/providers/requires-auth" location))
        (is (= 307 status))))
    (testing "already authenticated"
      (let [cookie (str auth/AUTH-COOKIE-NAME "=" (-> (cs/encode-cookie [principal now] password)
                                                      UrlEncoded/encodeString))
            {{:strs [location]} :headers :keys [body status]}
            (wrapped-handler {:uri "/requires-auth"
                              :headers {"cookie" cookie}})]
        (is (= 200 status))
        (is (= body "handler"))))))

(deftest test-github-provider
  (let [authenticate-uri-fn (constantly "http://authenticate")
        client-id "client-id"
        client-secret "client-secret"
        password [:cached "password"]
        provider (map->GitHubOAuthProvider {:authenticate-uri-fn authenticate-uri-fn
                                            :client-id client-id
                                            :client-secret client-secret
                                            :password password})
        token "abc123"
        state {:location "/path?a=1"
               :token token}
        cookie (str OAUTH-COOKIE-NAME "=" (-> (cs/encode-cookie token password)
                                              UrlEncoded/encodeString))]
    (testing "redirect"
      (let [{{:strs [set-cookie location]} :headers :keys [status]} (fa/<?? (redirect provider {}))]
        (is location)
        (is (= status 307))
        (is set-cookie)))
    (testing "authenticate"
      (with-redefs [random-str (fn [] token)
                    http/post (fn [_ _ _]
                                (async/go {:body (async/go "access_token=accesstoken&token_type=bearer")
                                           :status 200}))
                    http/get (fn [_ _ _]
                               (async/go
                                 {:body (async/go
                                          (json/write-str [{:email "user@example.com"
                                                            :primary true
                                                            :verified true}]))
                                  :status 200}))]
        (let [{{:strs [location]} :headers :keys [status] :as response}
              (fa/<?? (authenticate provider {:headers {"cookie" cookie}
                                              :query-string (str "code=code&state="
                                                                 (-> state
                                                                     json/write-str
                                                                     UrlEncoded/encodeString))}))]
          (is location)
          (is (= status 307)))))))

(deftest test-make-uri
  (is (= "http://test?a=1" (make-uri "http://test" {"a" "1"})))
  (is (= "http://test?a=1&b=2" (make-uri "http://test" {"a" "1"
                                                        "b" "2"})))
  (is (= "http://test?message=hello+world" (make-uri "http://test" {"message" "hello world"}))))

(deftest test-make-location
  (is (= "/path" (make-location {:path "path"})))
  (is (= "/path?a=1" (make-location {:path "path"
                                     :query-string "a=1"})))
  (is (= "/path?a=1&b=2" (make-location {:path "path"
                                         :query-string "a=1&b=2"})))
  (is (= "/path#b=2" (make-location {:path "path"
                                     :query-string "_waiter_hash=%23b%3D2"})))
  (is (= "/path?a=1#b=2" (make-location {:path "path"
                                         :query-string "a=1&_waiter_hash=%23b%3D2"}))))

(deftest test-google-provider
  (let [authenticate-uri-fn (constantly "http://authenticate")
        client-id "client-id"
        client-secret "client-secret"
        password [:cached "password"]
        provider (map->GoogleOAuthProvider {:authenticate-uri-fn authenticate-uri-fn
                                            :client-id client-id
                                            :client-secret client-secret
                                            :password password})
        token "abc123"
        state {:location "/path?a=1"
               :token token}
        cookie (str OAUTH-COOKIE-NAME "=" (-> (cs/encode-cookie token password)
                                              UrlEncoded/encodeString))]
    (testing "redirect"
      (with-redefs [google-discovery-document (fn [_] (async/go
                                                        {"authorization_endpoint" "http://endpoint"}))]
        (let [{{:strs [set-cookie location]} :headers :keys [status]} (fa/<?? (redirect provider {}))]
        (is location)
        (is (= status 307))
        (is set-cookie))))
    (testing "authenticate"
      (let [jwt (str "headernotused." (-> (json/write-str {:email "user@example.com"
                                                           :email_verified true})
                                          (.getBytes "utf8")
                                          b64/encode
                                          (String.)))]
        (with-redefs [random-str (fn [] "abc123")
                      google-discovery-document (fn [_] (async/go
                                                          {"token_endpoint" "http://endpoint"}))
                      http/post (fn [_ _ _]
                                  (async/go {:body (async/go
                                                     (json/write-str {:id_token jwt}))
                                             :status 200}))]
          (let [{{:strs [location]} :headers :keys [status] :as response}
                (fa/<?? (authenticate provider {:headers {"cookie" cookie}
                                                :query-string (str "code=code&state="
                                                                   (-> state
                                                                       json/write-str
                                                                       UrlEncoded/encodeString))}))]
            (is location)
            (is (= status 307))))))))
