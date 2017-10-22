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
(ns waiter.auth.oauth.google
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [full.async :as fa]
            [qbits.jet.client.http :as http]
            [ring.middleware.params :as ring-params]
            [waiter.auth.authentication :as auth]
            [waiter.auth.oauth :as oauth]
            [waiter.cookie-support :as cookie-support]))

(defn google-discovery-document
  "Gets Google's discovery document."
  [http-client]
  (fa/go-try
    (let [discovery-uri "https://accounts.google.com/.well-known/openid-configuration"
          {:keys [body error status]} (async/<! (http/get http-client discovery-uri {:headers {"accept" "application/json"}}))]
      (when error
        (throw (ex-info "Error while communicating with Google" {:status 403} error)))
      (when-not (= 200 status)
        (throw (ex-info "Invalid response from Google" {:status 403})))
      (try
        (json/read-str (async/<! body))
        (catch Exception e
          (throw (ex-info "Couldn't retrieve Google discovery document" {:status 403})))))))

; See https://developers.google.com/identity/protocols/OpenIDConnect#authenticatingtheuser
(defrecord GoogleOAuthProvider [authenticate-uri-fn client-id client-secret http-client password]
  oauth/OAuthProvider
  (display-name [_] "Google")
  (redirect [_ {:keys [query-string] {:keys [path]} :route-params :as request}]
    (fa/go-try
      (let [token (oauth/random-str)
            state (-> {:location (oauth/make-location {:path path
                                                       :query-string query-string})
                       :token token}
                      json/write-str)
            {:strs [authorization_endpoint]} (fa/<? (google-discovery-document http-client))
            _ (when-not authorization_endpoint
                (throw (ex-info "Couldn't get Google authorization endpoint" {:status 403})))
            location (oauth/make-uri authorization_endpoint
                                     {"client_id" client-id
                                      "nonce" (oauth/random-str)
                                      "response_type" "code"
                                      "redirect_uri" (authenticate-uri-fn request)
                                      "scope" "openid email"
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
                                            (throw (ex-info "Couldn't parse state from Google" {:status 403} e))))
            _ (oauth/verify-oauth-cookie request token password)
            {:strs [token_endpoint]} (fa/<? (google-discovery-document http-client))
            _ (when-not token_endpoint
                (throw (ex-info "Couldn't get Google token endpoint" {:status 403})))
            {:keys [body error status]} (async/<! (http/post http-client token_endpoint
                                                             {:form-params {"client_id" client-id
                                                                            "client_secret" client-secret
                                                                            "code" code
                                                                            "grant_type" "authorization_code"
                                                                            "redirect_uri" (authenticate-uri-fn request)}}))
            _ (when error
                (throw (ex-info "Error while communicating with Google" {:status 403} error)))
            _  (when-not (= status 200)
                 (throw (ex-info "Invalid token response from Google" {:status 403})))
            {:strs [id_token] :as response} (try
                                              (json/read-str (async/<! body))
                                              (catch Exception e
                                                (throw (ex-info "Couldn't parse JSON from Google" {:status 403} e))))
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
                                                               {:status 403} e))))
            _ (when-not (and (not (str/blank? email)) email_verified)
                (throw (ex-info "Can't get email from Google" {:status 403})))]
        (-> {:status 307
             :headers {"location" location}}
            (auth/add-cached-auth password email))))))
