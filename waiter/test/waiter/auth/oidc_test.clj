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
        state-map {:redirect-uri "https://www.test.com/redirect-uri"}
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
      (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (= {:code access-code
                :code-verifier (str "decoded:" challenge-cookie)
                :state-map state-map}
               (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 (str "decoded:" cookie-value))
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"Decoded challenge cookie is invalid"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier (str "decoded:" cookie-value)
                                                  :expiry-time (str not-expired-time-ms)})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"The challenge cookie has invalid format"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier (str "decoded:" cookie-value)
                                                  :expiry-time expired-time-ms})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"The challenge cookie has expired"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier nil
                                                  :expiry-time not-expired-time-ms})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"No challenge code available from cookie"
              (validate-oidc-callback-request password request)))))

    (with-redefs [cookie-support/decode-cookie (fn [cookie-value in-password]
                                                 (is (= password in-password))
                                                 {:code-verifier " "
                                                  :expiry-time not-expired-time-ms})
                  t/now (constantly (tc/from-long current-time-ms))]
      (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)}]
        (is (thrown-with-msg?
              ExceptionInfo #"No challenge code available from cookie"
              (validate-oidc-callback-request password request)))))

    (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                   :query-string (str "state=" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"Query parameter code is missing"
            (validate-oidc-callback-request password request))))

    (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                   :query-string (str "code=" access-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"Query parameter state is missing"
            (validate-oidc-callback-request password request))))

    (let [request {:headers {"cookie" (str "foo" "=" challenge-cookie)}
                   :query-string (str "code=" access-code "&state=" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"No challenge cookie set"
            (validate-oidc-callback-request password request))))

    (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                   :query-string (str "code=" access-code "&state=invalid" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"Unable to parse state"
            (validate-oidc-callback-request password request))))

    (let [state-map {:callback-uri "https://www.test.com/redirect-uri"}
          state-code (create-state-code state-map password)
          request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                   :query-string (str "code=" access-code "&state=" state-code)}]
      (is (thrown-with-msg?
            ExceptionInfo #"The state query parameter is invalid"
            (validate-oidc-callback-request password request))))))

(deftest test-oidc-callback-request-handler
  (let [password [:cached "password"]
        state-map {:redirect-uri "https://www.test.com/redirect-uri"}
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
      (let [request {:headers {"cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
                     :query-string (str "code=" access-code "&state=" state-code)
                     :scheme :https}
            oidc-authenticator {:password password
                                :subject-key subject-key}
            response-chan (oidc-callback-request-handler oidc-authenticator request)
            response (async/<!! response-chan)]
        (is (= {:cookies {"x-waiter-auth" {:age 1000
                                           :value ["john.doe" current-time-ms {:jwt-access-token access-token}]}
                          "x-waiter-oidc-challenge" {:age 0
                                                     :value ""}}
                :headers {"cache-control" "no-store"
                          "content-security-policy" "default-src 'none'; frame-ancestors 'none'"
                          "location" "https://www.test.com/redirect-uri"}
                :status http-302-moved-temporarily
                :authorization/method :oidc
                :authorization/principal "john.doe"
                :authorization/user "john.doe"
                :authorization/metadata {:jwt-access-token access-token}
                :waiter/response-source :waiter}
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
                               "cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
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
                               "cookie" (str oidc-challenge-cookie "=" challenge-cookie)}
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
        (is (str/includes? (-> response :body str) "Error in retrieving access token"))))))

(deftest test-update-oidc-auth-response
  (let [request-host "www.host.com:8080"
        request {:headers {"host" request-host}
                 :query-string "some-query-string"
                 :scheme "https"
                 :uri "/test"}
        password [:cached "password"]
        code-verifier "code-verifier-1234"
        state-code "status-4567"
        oidc-authorize-uri "https://www.test.com:9090/authorize"
        oidc-auth-server (jwt/->JwtAuthServer nil "jwks-url" (atom nil) oidc-authorize-uri nil)
        current-time-ms (System/currentTimeMillis)]
    (with-redefs [cookie-support/add-encoded-cookie (fn [response in-password name value age-in-seconds]
                                                      (is (= password in-password))
                                                      (is (= challenge-cookie-duration-secs age-in-seconds))
                                                      (assoc response :cookie {name value}))
                  utils/unique-identifier (constantly "123456")
                  t/now (constantly (tc/from-long current-time-ms))
                  create-code-verifier (constantly code-verifier)
                  create-state-code (fn [state-data in-password]
                                      (is (= password in-password))
                                      (is (= {:redirect-uri (str "https://" request-host "/test?some-query-string")}
                                             state-data))
                                      state-code)]
      (let [update-response (make-oidc-auth-response-updater oidc-auth-server password request)]

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
                   (assoc :cookie {oidc-challenge-cookie {:code-verifier code-verifier
                                                          :expiry-time (-> (t/now)
                                                                         (t/plus (t/seconds challenge-cookie-duration-secs))
                                                                         (tc/to-long))}}
                          :status http-302-moved-temporarily)
                   (update :headers assoc
                           "cache-control" "no-store"
                           "content-security-policy" "default-src 'none'; frame-ancestors 'none'"
                           "location" authorize-uri))
                 (update-response response))))))))

(deftest test-wrap-auth-handler
  (with-redefs [trigger-authorize-redirect (fn [_ _ _ response]
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
                                      :oidc-authorize-uri "http://www.test.com/authorize"}
                  oidc-auth-handler (wrap-auth-handler oidc-authenticator request-handler)]
              (doseq [status [200 301 400 401 403]]
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
                                  :oidc-authorize-uri "http://www.test.com/authorize"}
              oidc-auth-handler (wrap-auth-handler oidc-authenticator request-handler)]
          (doseq [status [200 301 400 401 403]]
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
                                    :oidc-authorize-uri "http://www.test.com/authorize"}
                oidc-auth-handler (wrap-auth-handler oidc-authenticator request-handler)]
            (doseq [status [200 301 400 401 403]]
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
                                           :waiter-api-call? waiter-api-call?})))))))))))

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
      (is (instance? OidcAuthenticator (make-jwt-authenticator (assoc config :allow-oidc-auth-services? false)))))

    (testing "invalid configuration"
      (is (thrown? Throwable (make-jwt-authenticator (assoc config :allow-oidc-auth-api? "true"))))
      (is (thrown? Throwable (make-jwt-authenticator (assoc config :allow-oidc-auth-services? "true"))))
      (is (thrown? Throwable (make-jwt-authenticator (dissoc config :oidc-authorize-uri))))
      (is (thrown? Throwable (make-jwt-authenticator (dissoc config :password)))))))