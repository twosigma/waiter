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
(ns waiter.auth.oauth.github
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [full.async :as fa]
            [qbits.jet.client.http :as http]
            [ring.middleware.params :as ring-params]
            [ring.util.codec :as ring-codec]
            [waiter.auth.authentication :as auth]
            [waiter.auth.oauth :as oauth]
            [waiter.cookie-support :as cookie-support]))

; See https://developer.github.com/apps/building-integrations/setting-up-and-registering-github-apps/identifying-users-for-github-apps/
(defrecord GitHubOAuthProvider [authenticate-uri-fn client-id client-secret http-client password]
  oauth/OAuthProvider
  (display-name [_] "GitHub")
  (redirect [_ {:keys [query-string] {:keys [path]} :route-params :as request}]
    (fa/go-try
      (let [token (oauth/random-str)
            state (-> {:location (oauth/make-location {:path path
                                                       :query-string query-string})
                       :token token}
                      json/write-str)
            location (oauth/make-uri "https://github.com/login/oauth/authorize"
                                     {"client_id" client-id
                                      "scope" "user:email"
                                      "redirect_uri" (authenticate-uri-fn request)
                                      "state" state})]
        (-> {:status 307
             :headers {"location" location}}
            ;; We should store this cookie only long enough to provide a reasonable amount of time
            ;; for the client to authenticate.
            (cookie-support/add-encoded-cookie password oauth/OAUTH-COOKIE-NAME token (-> 15 t/minutes t/in-seconds))))))
  (authenticate [_ request]
    (fa/go-try
      (let [{:strs [code state]} (:params (ring-params/params-request request))
            _ (when-not (and code state)
                (throw (ex-info "Malformed request" {:status 400})))
            {:strs [location token]} (try (-> state
                                              json/read-str)
                                          (catch Exception e
                                            (throw (ex-info "Couldn't parse state from GitHub" {:status 403} e))))
            _ (oauth/verify-oauth-cookie request token password)
            {:keys [body error status]} (async/<! (http/post http-client "https://github.com/login/oauth/access_token"
                                                             {:form-params {"client_id" client-id
                                                                            "client_secret" client-secret
                                                                            "code" code
                                                                            "state" state}}))
            _ (when error
                (throw (ex-info "Error while communicating with GitHub" {:status 403} error)))
            _  (when-not (= status 200)
                 (throw (ex-info "Invalid access token response from GitHub" {:status 403})))
            access-token-body (async/<! body)
            {:strs [access_token]} (-> access-token-body
                                       (ring-codec/form-decode))
            _  (when (str/blank? access_token)
                 (throw (ex-info "Invalid access token from GitHub" {:status 403})))
            {:keys [body status error]} (async/<! (http/get http-client "https://api.github.com/user/emails"
                                                      {:headers {"authorization" (str "token " access_token)}}))
            _ (when error
                (throw (ex-info "Error while communicating with GitHub" {:status 403} error)))
            _ (when-not (= status 200)
                (throw (ex-info "Invalid user API response from GitHub" {:status 403})))
            response-body (async/<! body)
            emails (try (json/read-str response-body)
                        (catch Exception e
                          (throw (ex-info "Invalid JSON from GitHub" {:status 403} e))))
            email (->> emails
                       (keep (fn [{:strs [email primary verified]}]
                               (when (and primary verified)
                                 email)))
                       first)
            _ (when-not email
                (throw (ex-info "Can't get email from GitHub" {:status 403})))]
        (-> {:status 307
             :headers {"location" location}}
            (auth/add-cached-auth password email))))))
