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
(ns waiter.auth.oidc-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.auth.jwt :as jwt]
            [waiter.auth.oidc :refer :all]
            [waiter.cookie-support :as cookie-support]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (waiter.auth.oidc OidcAuthenticator)))

(deftest test-validate-oidc-callback-request
  (let [password [:cached "password"]
        identifier (utils/unique-identifier)
        state-map {:identifier identifier
                   :oidc-mode :strict
                   :redirect-uri "https://www.test.com/redirect-uri"}
        state-code (create-state-code state-map password)
        access-code (str "access-code-" (rand-int 1000))
        challenge-cookie (str "challenge-cookie-" (rand-int 1000))
        current-time-ms (System/currentTimeMillis)
        expired-time-ms (- current-time-ms 10000)
        not-expired-time-ms (+ current-time-ms 10000)]

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier (str "decoded:" cookie-value)
                                                  :expiry-time not-expired-time-ms})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (= {:code access-code
                :code-verifier (str "decoded:" challenge-cookie)
                :state-map state-map}
               (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 (str "decoded:" cookie-value))
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"Decoded challenge cookie is invalid"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier (str "decoded:" cookie-value)
                                                  :expiry-time (str not-expired-time-ms)})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"The challenge cookie has invalid format"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier (str "decoded:" cookie-value)
                                                  :expiry-time expired-time-ms})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"The challenge cookie has expired"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier nil
                                                  :expiry-time not-expired-time-ms})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"No challenge code available from cookie"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier " "
                                                  :expiry-time not-expired-time-ms})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"No challenge code available from cookie"
              (validate-oidc-callback-request password request)))))

    (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                   :query-string (str "state=" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"Query parameter code is missing"
            (validate-oidc-callback-request password request))))

    (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                   :query-string (str "code=" access-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"Query parameter state is missing"
            (validate-oidc-callback-request password request))))

    (let [request {:headers {"cookie" (str "foo" "=" challenge-cookie)}
                   :query-string (str "code=" access-code "&state=" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"No challenge cookie set"
            (validate-oidc-callback-request password request))))

    (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                   :query-string (str "code=" access-code "&state=invalid" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"Unable to parse state"
            (validate-oidc-callback-request password request))))

    (let [state-map {:callback-uri "https://www.test.com/redirect-uri"}
          state-code (create-state-code state-map password)
          request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                   :query-string (str "code=" access-code "&state=" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"The state query parameter is invalid"
            (validate-oidc-callback-request password request))))))

(deftest test-oidc-callback-request-handler
  (doseq [oidc-mode [:relaxed :strict]]
    (let [password [:cached "password"]
          identifier (utils/unique-identifier)
          state-map {:identifier identifier
                     :oidc-mode oidc-mode
                     :redirect-uri "https://www.test.com/redirect-uri"}
          state-code (create-state-code state-map password)
          access-code (str "access-code-" (rand-int 1000))
          challenge-cookie (str "challenge-cookie-" (rand-int 1000))
          access-token (str "access-token-" (rand-int 1000))
          subject-key :subject
          current-time-ms (System/currentTimeMillis)
          current-time-secs (/ current-time-ms 1000)]

      (with-redefs [cookie-support/add-encoded-cookie (fn [response in-password name value age-in-seconds]
                                                        (is (= password in-password))
                                                        (assoc-in response
                                                                  [:cookies name]
                                                                  {:age (int age-in-seconds)
                                                                   :value value}))
                    cookie-support/decode-cookie (fn [cookie-value in-password]
                                                   (is (= password in-password))
                                                   {:code-verifier (str "decoded:" cookie-value)
                                                    :expiry-time (+ current-time-ms 10000)})
                    jwt/current-time-secs (constantly current-time-secs)
                    jwt/get-key-id->jwk (constantly {})
                    jwt/request-access-token (fn [& _]
                                               (let [result-chan (async/promise-chan)]
                                                 (async/>!! result-chan access-token)
                                                 result-chan))
                    jwt/extract-claims (constantly {:expiry-time (+ current-time-secs 1000)
                                                    :subject "john.doe"})
                    t/now (constantly (tc/from-long current-time-ms))]
        (let [request {:headers {"cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                       :query-string (str "code=" access-code "&state=" state-code)
                       :scheme :https}
              oidc-authenticator {:password password
                                  :subject-key subject-key}
              response-chan (oidc-callback-request-handler oidc-authenticator request)
              response (async/<!! response-chan)]
          (is (= {:cookies {"x-waiter-auth" {:age (if (= :strict oidc-mode) 1000 (-> 1 t/days t/in-seconds))
                                             :value ["john.doe" current-time-ms {:jwt-access-token access-token}]}
                            (str oidc-challenge-cookie-prefix identifier) {:age 0
                                                                           :value ""}}
                  :headers {"cache-control" "no-store"
                            "content-security-policy" "default-src 'none'; frame-ancestors 'none'"
                            "location" "https://www.test.com/redirect-uri"}
                  :status http-302-moved-temporarily
                  :authorization/method :oidc
                  :authorization/principal "john.doe"
                  :authorization/user "john.doe"
                  :authorization/metadata {:jwt-access-token access-token}
                  :waiter/response-source :waiter
                  :waiter/token "www.test.com"}
                 response))))

      (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                   (is (= password in-password))
                                                   {:code-verifier (str "decoded:" cookie-value)
                                                    :expiry-time (+ current-time-ms 10000)})
                    jwt/get-key-id->jwk (constantly {})
                    jwt/request-access-token (fn [& _]
                                               (let [result-chan (async/promise-chan)]
                                                 (async/>!! result-chan access-token)
                                                 result-chan))
                    jwt/validate-access-token (fn [& _]
                                                (throw (ex-info "Created from validate-access-token" {})))]
        (let [request {:headers {"accept" "application/json"
                                 "cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                       :query-string (str "code=" access-code "&state=" state-code)
                       :scheme :https}
              oidc-authenticator {:password password
                                  :subject-key subject-key}
              response-chan (oidc-callback-request-handler oidc-authenticator request)
              response (async/<!! response-chan)]
          (is (= {:headers {"content-type" "application/json"}
                  :status http-401-unauthorized
                  :waiter/response-source :waiter}
                 (dissoc response :body)))
          (is (str/includes? (-> response :body str) "Error in retrieving access token"))))

      (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                   (is (= password in-password))
                                                   {:code-verifier (str "decoded:" cookie-value)
                                                    :expiry-time (+ current-time-ms 10000)})
                    jwt/get-key-id->jwk (constantly {})
                    jwt/request-access-token (fn [& _]
                                               (let [result-chan (async/promise-chan)]
                                                 (async/>!! result-chan (ex-info "Created from request-access-token" {}))
                                                 result-chan))]
        (let [request {:headers {"accept" "application/json"
                                 "cookie" (str oidc-challenge-cookie-prefix identifier "=" challenge-cookie)}
                       :query-string (str "code=" access-code "&state=" state-code)
                       :scheme :https}
              oidc-authenticator {:password password
                                  :subject-key subject-key}
              response-chan (oidc-callback-request-handler oidc-authenticator request)
              response (async/<!! response-chan)]
          (is (= {:headers {"content-type" "application/json"}
                  :status http-401-unauthorized
                  :waiter/response-source :waiter}
                 (dissoc response :body)))
          (is (str/includes? (-> response :body str) "Error in retrieving access token")))))))

(deftest test-update-oidc-auth-response
  (let [request-host "www.host.com:8080"
        request {:headers {"host" request-host}
                 :query-string "some=query-string"
                 :scheme :https
                 :uri "/test"}
        password [:cached "password"]
        code-verifier "code-verifier-1234"
        state-code "status-4567"
        oidc-authorize-uri "https://www.test.com:9090/authorize"
        oidc-auth-server (jwt/->JwtAuthServer nil "jwks-url" (atom nil) oidc-authorize-uri nil)
        current-time-ms (System/currentTimeMillis)
        identifier-prefix "code-identifier-"]
    (doseq [oidc-default-mode [:relaxed :strict]]
      (with-redefs [cookie-support/add-encoded-cookie (fn [response in-password name value age-in-seconds]
                                                        (is (= password in-password))
                                                        (is (= challenge-cookie-duration-secs age-in-seconds))
                                                        (assoc response :cookie {name value}))
                    utils/unique-identifier (constantly "123456")
                    t/now (constantly (tc/from-long current-time-ms))
                    create-code-identifier (fn [code-verifier] (str identifier-prefix code-verifier))
                    create-code-verifier (constantly code-verifier)
                    create-state-code (fn [state-data in-password]
                                        (is (= password in-password))
                                        (is (= {:identifier (str identifier-prefix code-verifier)
                                                :oidc-mode oidc-default-mode
                                                :redirect-uri (str "https://" request-host "/test?some=query-string")}
                                               state-data))
                                        state-code)]

        (testing "http request redirected to https"
          (doseq [request-method [:get :post]]
            (let [request (assoc request :request-method request-method :scheme :http)
                  update-response (make-oidc-auth-response-updater oidc-auth-server oidc-default-mode password request)]
              (let [response {:status http-401-unauthorized
                              :waiter/response-source :waiter}]
                (is (= (assoc response
                         :headers {"location" "https://www.host.com/test?some=query-string"}
                         :status (if (= request-method :get) http-302-moved-temporarily http-307-temporary-redirect))
                       (update-response response)))))))

        (let [request (assoc request :scheme :https)
              update-response (make-oidc-auth-response-updater oidc-auth-server oidc-default-mode password request)]

          (let [response {:body (utils/unique-identifier)
                          :status http-200-ok}]
            (is (= response (update-response response))))

          (let [response {:body (utils/unique-identifier)
                          :status http-401-unauthorized
                          :waiter/response-source :backend}]
            (is (= response (update-response response))))

          (let [response {:body (utils/unique-identifier)
                          :status http-401-unauthorized
                          :waiter/response-source :waiter}
                code-challenge (utils/b64-encode-sha256 code-verifier)
                redirect-uri-encoded (cookie-support/url-encode (str "https://" request-host oidc-callback-uri))
                authorize-uri (str oidc-authorize-uri "?"
                                   "client_id=www.host.com&"
                                   "code_challenge=" code-challenge "&"
                                   "code_challenge_method=S256&"
                                   "nonce=123456&"
                                   "redirect_uri=" redirect-uri-encoded "&"
                                   "response_type=code&"
                                   "scope=openid&"
                                   "state=" state-code)]
            (is (= (-> response
                     (assoc :cookie {(str oidc-challenge-cookie-prefix identifier-prefix code-verifier)
                                     {:code-verifier code-verifier
                                      :expiry-time (-> (t/now)
                                                     (t/plus (t/seconds challenge-cookie-duration-secs))
                                                     (tc/to-long))}}
                            :status http-302-moved-temporarily)
                     (update :headers assoc
                             "cache-control" "no-store"
                             "content-security-policy" "default-src 'none'; frame-ancestors 'none'"
                             "location" authorize-uri))
                   (update-response response)))))))))

(deftest test-wrap-auth-handler
  (with-redefs [trigger-authorize-redirect (fn [_ _ _ _ response]
                                             (assoc response :processed-by :oidc-updater))]
    (let [request-handler (fn [request] (assoc request
                                          :processed-by :request-handler
                                          :waiter/response-source :waiter))]

      (testing "allow oidc authentication combinations"
        (doseq [allow-oidc-auth-api? [false true]]
          (doseq [allow-oidc-auth-services? [false true]]
            (let [oidc-authenticator {:allow-oidc-auth-api? allow-oidc-auth-api?
                                      :allow-oidc-auth-services? allow-oidc-auth-services?
                                      :jwt-auth-server (Object.)
                                      :oidc-authorize-uri "http://www.test.com/authorize"
                                      :oidc-num-challenge-cookies-allowed-in-request 20}
                  oidc-auth-handler (wrap-auth-handler oidc-authenticator request-handler)]
              (doseq [status [http-200-ok http-301-moved-permanently http-400-bad-request http-401-unauthorized http-403-forbidden]]
                (doseq [waiter-api-call? [true false]]
                  (doseq [user-agent ["chrome" "mozilla" "jetty" "curl"]]
                    (let [request {:headers {"host" "www.test.com:1234"
                                             "user-agent" user-agent}
                                   :status status
                                   :waiter-api-call? waiter-api-call?}]
                      (is (= {:headers {"host" "www.test.com:1234"
                                        "user-agent" user-agent}
                              :processed-by (if (and (= status http-401-unauthorized)
                                                     (contains? #{"chrome" "mozilla"} user-agent)
                                                     (or (and allow-oidc-auth-api? waiter-api-call?)
                                                         (and allow-oidc-auth-services? (not waiter-api-call?))))
                                              :oidc-updater
                                              :request-handler)
                              :status status
                              :waiter-api-call? waiter-api-call?
                              :waiter/response-source :waiter}
                             (oidc-auth-handler request)))))))))))

      (testing "already authenticated"
        (let [oidc-authenticator {:allow-oidc-auth-api? false
                                  :allow-oidc-auth-services? false
                                  :jwt-auth-server (Object.)
                                  :oidc-authorize-uri "http://www.test.com/authorize"
                                  :oidc-num-challenge-cookies-allowed-in-request 20}
              oidc-auth-handler (wrap-auth-handler oidc-authenticator request-handler)]
          (doseq [status [http-200-ok http-301-moved-permanently http-400-bad-request http-401-unauthorized http-403-forbidden]]
            (doseq [waiter-api-call? [true false]]
              (doseq [user-agent ["chrome" "mozilla" "jetty" "curl"]]
                (is (= {:authorization/principal "auth-principal"
                        :authorization/user "auth-user"
                        :headers {"host" "www.test.com:1234"
                                  "user-agent" user-agent}
                        :processed-by :request-handler
                        :status status
                        :waiter-api-call? waiter-api-call?
                        :waiter/response-source :waiter}
                       (oidc-auth-handler {:authorization/principal "auth-principal"
                                           :authorization/user "auth-user"
                                           :headers {"host" "www.test.com:1234"
                                                     "user-agent" user-agent}
                                           :status status
                                           :waiter-api-call? waiter-api-call?}))))))))

      (testing "oidc-enabled and redirect supported"
        (with-redefs [supports-redirect? (constantly true)]
          (let [oidc-authenticator {:allow-oidc-auth-api? true
                                    :allow-oidc-auth-services? true
                                    :jwt-auth-server (Object.)
                                    :oidc-authorize-uri "http://www.test.com/authorize"
                                    :oidc-num-challenge-cookies-allowed-in-request 20}
                oidc-auth-handler (wrap-auth-handler oidc-authenticator request-handler)]
            (doseq [status [http-200-ok http-301-moved-permanently http-400-bad-request http-401-unauthorized http-403-forbidden]]
              (doseq [waiter-api-call? [true false]]
                (is (= {:headers {"host" "www.test.com:1234"}
                        :processed-by (if (= status http-401-unauthorized)
                                        :oidc-updater
                                        :request-handler)
                        :status status
                        :waiter-api-call? waiter-api-call?
                        :waiter/response-source :waiter}
                       (oidc-auth-handler {:headers {"host" "www.test.com:1234"}
                                           :status status
                                           :waiter-api-call? waiter-api-call?}))))))))

      (testing "oidc challenge cookies"
        (with-redefs [supports-redirect? (constantly true)]
          (let [num-challenge-cookies-allowed-in-request 20
                challenge-cookies (map #(str oidc-challenge-cookie-prefix % "=v" %) (range (inc num-challenge-cookies-allowed-in-request)))
                oidc-authenticator {:allow-oidc-auth-api? true
                                    :allow-oidc-auth-services? true
                                    :jwt-auth-server (Object.)
                                    :oidc-authorize-uri "http://www.test.com/authorize"
                                    :oidc-num-challenge-cookies-allowed-in-request num-challenge-cookies-allowed-in-request}
                oidc-auth-handler (wrap-auth-handler oidc-authenticator request-handler)]
            (let [cookie-header (str/join ";" (take num-challenge-cookies-allowed-in-request challenge-cookies))]
              (is (= {:headers {"cookie" cookie-header
                                "host" "www.test.com:1234"}
                      :processed-by :oidc-updater
                      :status http-401-unauthorized
                      :waiter-api-call? false
                      :waiter/response-source :waiter}
                     (oidc-auth-handler {:headers {"cookie" cookie-header
                                                   "host" "www.test.com:1234"}
                                         :status http-401-unauthorized
                                         :waiter-api-call? false}))))
            (let [cookie-header (str/join ";" (take (inc num-challenge-cookies-allowed-in-request) challenge-cookies))]
              (is (= {:headers {"cookie" cookie-header
                                "host" "www.test.com:1234"}
                      :processed-by :request-handler
                      :status http-401-unauthorized
                      :waiter-api-call? false
                      :waiter/response-source :waiter}
                     (oidc-auth-handler {:headers {"cookie" cookie-header
                                                   "host" "www.test.com:1234"}
                                         :status http-401-unauthorized
                                         :waiter-api-call? false}))))))))))

(deftest test-supports-redirect?
  (let [oidc-authority "www.auth.com:1234"
        supports-redirect-helper? (partial supports-redirect? oidc-authority)]
    (is (not (supports-redirect-helper? {:headers {"accept-redirect-auth" "www.test.com"}})))
    (is (not (supports-redirect-helper? {:headers {"accept-redirect" "no"
                                                   "accept-redirect-auth" oidc-authority}})))
    (is (not (supports-redirect-helper? {:headers {"accept-redirect" "yes"
                                                   "accept-redirect-auth" "www.auth.com"}})))
    (is (not (supports-redirect-helper? {:headers {"accept-redirect" "no"}})))
    (is (supports-redirect-helper? {:headers {"accept-redirect" "yes"}}))
    (is (supports-redirect-helper? {:headers {"accept-redirect" "yes"
                                              "accept-redirect-auth" oidc-authority}}))
    (is (supports-redirect-helper? {:headers {"accept-redirect" "yes"
                                              "accept-redirect-auth" (str "www.test.com " oidc-authority)}}))
    (is (supports-redirect-helper? {:headers {"accept-redirect" "yes"
                                              "accept-redirect-auth" (str "www.test.com " oidc-authority " www.foo.com")}}))

    (is (not (supports-redirect-helper? {:headers {"user-agent" "curl"}})))
    (is (not (supports-redirect-helper? {:headers {"user-agent" "jetty"}})))
    (is (not (supports-redirect-helper? {:headers {"user-agent" "python-requests"}})))
    (is (supports-redirect-helper? {:headers {"user-agent" "chrome"}}))
    (is (supports-redirect-helper? {:headers {"user-agent" "mozilla"}}))))

(deftest test-too-many-oidc-challenge-cookies?
  (let [challenge-cookies (map #(str oidc-challenge-cookie-prefix % "=v" %) (range 25))]
    (is (not (too-many-oidc-challenge-cookies? {:headers {}} 10)))
    (is (not (too-many-oidc-challenge-cookies? {:headers {"cookie" ""}} 10)))
    (is (not (too-many-oidc-challenge-cookies? {:headers {"cookie" (first challenge-cookies)}} 10)))
    (is (not (too-many-oidc-challenge-cookies? {:headers {"cookie" [(first challenge-cookies)]}} 10)))
    (is (not (too-many-oidc-challenge-cookies? {:headers {"cookie" (take 5 challenge-cookies)}} 10)))
    (is (not (too-many-oidc-challenge-cookies? {:headers {"cookie" (str/join ";" (take 5 challenge-cookies))}} 10)))
    (is (too-many-oidc-challenge-cookies? {:headers {"cookie" (take 15 challenge-cookies)}} 10))
    (is (too-many-oidc-challenge-cookies? {:headers {"cookie" (str/join ";" (take 15 challenge-cookies))}} 10))))

(deftest test-create-oidc-authenticator
  (let [jwt-auth-server (jwt/map->JwtAuthServer {})
        jwt-validator (jwt/map->JwtValidator {})
        make-jwt-authenticator (fn [config]
                                 (create-oidc-authenticator jwt-auth-server jwt-validator config))
        config {:oidc-authorize-uri "http://www.test.com/oidc/authorize"
                :password [:cached "test-password"]}]
    (testing "valid configuration"
      (is (instance? OidcAuthenticator (make-jwt-authenticator config)))
      (is (instance? OidcAuthenticator (make-jwt-authenticator (assoc config :allow-oidc-auth-api? true))))
      (is (instance? OidcAuthenticator (make-jwt-authenticator (assoc config :allow-oidc-auth-api? false))))
      (is (instance? OidcAuthenticator (make-jwt-authenticator (assoc config :allow-oidc-auth-services? true))))
      (is (instance? OidcAuthenticator (make-jwt-authenticator (assoc config :allow-oidc-auth-services? false))))
      (doseq [oidc-default-mode [:relaxed :strict]]
        (is (instance? OidcAuthenticator (make-jwt-authenticator (assoc config :oidc-default-mode oidc-default-mode))))))

    (testing "invalid configuration"
      (is (thrown? Throwable (make-jwt-authenticator (assoc config :allow-oidc-auth-api? "true"))))
      (is (thrown? Throwable (make-jwt-authenticator (assoc config :allow-oidc-auth-services? "true"))))
      (is (thrown? Throwable (make-jwt-authenticator (dissoc config :oidc-authorize-uri))))
      (is (thrown? Throwable (make-jwt-authenticator (assoc config :oidc-default-mode :disabled))))
      (is (thrown? Throwable (make-jwt-authenticator (assoc config :oidc-num-challenge-cookies-allowed-in-request "20"))))
      (is (thrown? Throwable (make-jwt-authenticator (dissoc config :password)))))))

(deftest test-oidc-enabled-on-request?
  (doseq [allow-oidc-auth-api? [true false]]
    (doseq [allow-oidc-auth-services? [true false]]
      (doseq [oidc-default-mode [:strict :relaxed]]
        (let [assertion-message {:allow-oidc-auth-api? allow-oidc-auth-api?
                                 :allow-oidc-auth-services? allow-oidc-auth-services?
                                 :oidc-default-mode oidc-default-mode}]
          (doseq [env-value [:disabled :relaxed :strict]]
            (let [assertion-message (str (assoc assertion-message :env-value env-value))
                  request {:waiter-api-call? false
                           :waiter-discovery {:service-description-template {"env" {"USE_OIDC_AUTH" (name env-value)}}}}]
              (is (= env-value (request->oidc-mode allow-oidc-auth-api? allow-oidc-auth-services? oidc-default-mode request))
                  assertion-message)))
          (doseq [env-value [true false]]
            (let [assertion-message (str (assoc assertion-message :env-value env-value))]
              (let [request {:waiter-api-call? false
                             :waiter-discovery {:service-description-template {"env" {"USE_OIDC_AUTH" (str env-value)}}}}]
                (is (= (if env-value :relaxed :disabled)
                       (request->oidc-mode allow-oidc-auth-api? allow-oidc-auth-services? oidc-default-mode request))
                    assertion-message))
              (let [request {:waiter-api-call? false
                             :waiter-discovery {:service-description-template {}}}]
                (is (= (if allow-oidc-auth-services? oidc-default-mode :disabled)
                       (request->oidc-mode allow-oidc-auth-api? allow-oidc-auth-services? oidc-default-mode request))
                    assertion-message))
              (let [request {:waiter-api-call? true
                             :waiter-discovery {:service-description-template {}}}]
                (is (= (if allow-oidc-auth-api? oidc-default-mode :disabled)
                       (request->oidc-mode allow-oidc-auth-api? allow-oidc-auth-services? oidc-default-mode request))
                    assertion-message)))))))))

(deftest test-oidc-enabled-request-handler
  (is (thrown-with-msg? ExceptionInfo #"OIDC authentication disabled"
                        (oidc-enabled-request-handler nil #{} {})))
  (doseq [expected-result [true false]]
    (is (= {:body (utils/clj->json {:client-id "www.test.com"
                                    :enabled expected-result
                                    :token? true})
            :headers {"content-type" "application/json"}
            :status (if expected-result http-200-ok http-404-not-found)
            :waiter/response-source :waiter}
           (let [oidc-authenticator {}
                 request {:headers {"host" "www.w8r.com:1234"}
                          :waiter-discovery {:service-description-template {"env" {"USE_OIDC_AUTH" (str expected-result)}}
                                             :token "www.test.com"}}]
             (oidc-enabled-request-handler oidc-authenticator #{} request))))
    (is (= {:body (utils/clj->json {:client-id "www.w8r.com"
                                    :enabled expected-result
                                    :token? false})
            :headers {"content-type" "application/json"}
            :status (if expected-result http-200-ok http-404-not-found)
            :waiter/response-source :waiter}
           (let [oidc-authenticator {:allow-oidc-auth-api? expected-result}
                 waiter-hostnames #{"www.w8r.com"}
                 request {:headers {"host" "www.w8r.com:1234"}}]
             (oidc-enabled-request-handler oidc-authenticator waiter-hostnames request))))))