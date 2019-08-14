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
(ns waiter.auth.jwt-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [waiter.auth.authentication :as auth]
            [waiter.auth.jwt :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.util.client-tools :as ct]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (net.i2p.crypto.eddsa EdDSAPublicKey)
           (waiter.auth.jwt JwtAuthenticator)))

(deftest test-retrieve-edsa-public-key
  (let [k "3StIkhARy4UZMV0OkvB6ZrN3dAt9sh_X9ZI15n1yr-c"]
    (is (instance? EdDSAPublicKey (retrieve-edsa-public-key k)))))

(deftest test-refresh-keys-cache
  (let [http-client (Object.)
        keys-url "https://www.test.com/jwks/keys"
        http-options {:conn-timeout 10000
                      :socket-timeout 10000}]

    (testing "http-request throws exception"
      (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                      (is (= http-client in-http-client))
                                      (is (= keys-url in-url))
                                      (throw (IllegalStateException. "From test")))]
        (let [keys-cache (atom {})]
          (is (thrown-with-msg? IllegalStateException #"From test"
                                (refresh-keys-cache http-client http-options keys-url keys-cache)))
          (is (= {} @keys-cache)))))

    (testing "http-request returns string"
      (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                      (is (= http-client in-http-client))
                                      (is (= keys-url in-url))
                                      "From test")]
        (let [keys-cache (atom {})]
          (is (thrown-with-msg? ExceptionInfo #"Invalid response from the JWKS endpoint"
                                (refresh-keys-cache http-client http-options keys-url keys-cache)))
          (is (= {} @keys-cache)))))

    (testing "http-request returns no keys"
      (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                      (is (= http-client in-http-client))
                                      (is (= keys-url in-url))
                                      {"keys" []})]
        (let [keys-cache (atom {})]
          (is (thrown-with-msg? ExceptionInfo #"No Ed25519 keys found from the JWKS endpoint"
                                (refresh-keys-cache http-client http-options keys-url keys-cache)))
          (is (= {} @keys-cache)))))

    (testing "http-request returns some keys"
      (let [current-time (t/now)]
        (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                        (is (= http-client in-http-client))
                                        (is (= keys-url in-url))
                                        (walk/keywordize-keys
                                          {"keys" [{"crv" "P-256", "kty" "OKP", "name" "fee", "use" "sig"}
                                                   {"crv" "Ed25519", "kty" "RSA", "name" "fie", "use" "sig", "x" "x1"}
                                                   {"crv" "Ed25519", "kid" "k1", "kty" "OKP", "name" "foe", "use" "sig", "x" "x2"}
                                                   {"crv" "Ed25519", "kty" "OKP", "name" "fum", "use" "enc", "x" "x3"}
                                                   {"crv" "Ed25519", "kty" "OKP", "name" "fum", "use" "sig", "x" "x4"}]}))
                      t/now (constantly current-time)
                      retrieve-edsa-public-key (fn [k] (str "public-key-" k))]
          (let [keys-cache (atom {})]
            (refresh-keys-cache http-client http-options keys-url keys-cache)
            (is (= {:key-id->jwk {"k1" (-> {"crv" "Ed25519", "kid" "k1", "kty" "OKP", "name" "foe", "use" "sig", "x" "x2"}
                                         (walk/keywordize-keys)
                                         (assoc :waiter.auth.jwt/public-key "public-key-x2"))}
                    :last-update-time current-time
                    :summary {:num-filtered-keys 1 :num-jwks-keys 5}}
                   @keys-cache))))))))

(deftest test-jwt-cache-maintainer
  (let [jwks-data (-> "test-files/jwt/jwks.json" slurp json/read-str walk/keywordize-keys :keys)
        data-counter (atom 1)
        http-client (Object.)
        keys-url "https://www.test.com/jwks/keys"
        http-options {:conn-timeout 10000
                      :socket-timeout 10000}]
    (with-redefs [refresh-keys-cache (fn [in-http-client in-http-options in-url in-keys-cache]
                                       (is (= http-client in-http-client))
                                       (is (= http-options in-http-options))
                                       (is (= keys-url in-url))
                                       (reset! in-keys-cache {:keys (take @data-counter jwks-data)
                                                              :last-update-time (t/now)}))]
      (let [keys-cache (atom {})
            update-interval-ms 5
            {:keys [cancel-fn query-state-fn]}
            (start-jwt-cache-maintainer http-client http-options keys-url update-interval-ms keys-cache)]
        (try
          (loop [counter 2]
            (reset! data-counter counter)
            (is (wait-for
                  (fn [] (= (take counter jwks-data) (:keys (query-state-fn))))
                  :interval 5 :timeout 25 :unit-multiplier 1))
            (when (<= counter (count jwks-data))
              (recur (inc counter))))
          (finally
            (cancel-fn)))
        (is (= jwks-data (:keys @keys-cache)))))))

(deftest test-validate-access-token
  (let [all-keys (-> "test-files/jwt/jwks.json" slurp json/read-str walk/keywordize-keys :keys)
        ed25519-keys (->> (filter ed25519-key? all-keys)
                       (map attach-public-key)
                       (pc/map-from-vals :kid))
        issuer "test-issuer"
        subject-key :sub
        token-type "ty+pe"
        realm "www.test-realm.com"
        request-scheme :https
        access-token "access-token"]

    (is (thrown-with-msg? ExceptionInfo #"JWT authentication can only be used with host header"
                          (validate-access-token token-type issuer subject-key ed25519-keys nil request-scheme access-token)))

    (is (thrown-with-msg? ExceptionInfo #"JWT authentication can only be used with HTTPS connections"
                          (validate-access-token token-type issuer subject-key ed25519-keys realm :http access-token)))

    (is (thrown-with-msg? ExceptionInfo #"Must provide Bearer token in Authorization header"
                          (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme " ")))

    (is (thrown-with-msg? ExceptionInfo #"JWT access token is malformed"
                          (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme "abcd")))

    (let [{:keys [d]} (rand-nth (vals ed25519-keys))
          access-token (ct/generate-jwt-access-token d {} {})]
      (is (thrown-with-msg? ExceptionInfo #"JWT header is missing key ID"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d]} (rand-nth (vals ed25519-keys))
          access-token (ct/generate-jwt-access-token d {} {:kid "invalid-key" :typ (str token-type ".err")})]
      (is (thrown-with-msg? ExceptionInfo #"Unsupported type"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d]} (rand-nth (vals ed25519-keys))
          access-token (ct/generate-jwt-access-token d {} {:kid "invalid-key" :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"No matching JWKS key found for key invalid-key"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          access-token (ct/generate-jwt-access-token d {} {:kid kid :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"Issuer does not match test-issuer"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          access-token (ct/generate-jwt-access-token d {:iss issuer} {:kid kid :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"Audience does not match www.test-realm.com"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          access-token (ct/generate-jwt-access-token d {:aud realm :iss issuer} {:kid kid :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"No subject provided in the token payload"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          payload {:aud realm :iss issuer :sub "foo@bar.com"}
          access-token (ct/generate-jwt-access-token d payload {:kid kid :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"No expiry provided in the token payload"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          expiry-time (- (current-time-secs) 1000)
          payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
          access-token (ct/generate-jwt-access-token d payload {:kid kid :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"Token is expired"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          expiry-time (+ (current-time-secs) 10000)
          subject-key :custom-key
          payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
          access-token (ct/generate-jwt-access-token d payload {:kid kid :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"No custom-key provided in the token payload"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          expiry-time (+ (current-time-secs) 10000)
          payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
          access-token (ct/generate-jwt-access-token d payload {:kid kid :typ token-type})]
      (is (= payload (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          expiry-time (+ (current-time-secs) 10000)
          subject-key :custom-key
          payload {:aud realm :custom-key "foo@bar.baz" :exp expiry-time :iss issuer}
          access-token (ct/generate-jwt-access-token d payload {:kid kid :typ token-type})]
      (is (thrown-with-msg? ExceptionInfo #"No subject provided in the token payload"
                            (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))

    (let [{:keys [d kid]} (rand-nth (vals ed25519-keys))
          expiry-time (+ (current-time-secs) 10000)
          subject-key :custom-key
          payload {:aud realm :custom-key "foo@bar.baz" :exp expiry-time :iss issuer :sub "foo@bar.com"}
          access-token (ct/generate-jwt-access-token d payload {:kid kid :typ token-type})]
      (is (= payload (validate-access-token token-type issuer subject-key ed25519-keys realm request-scheme access-token))))))

(deftest test-authenticate-request
  (let [request-handler (fn [request] (assoc request :source ::request-handler))
        issuer "test-issuer"
        keys (Object.)
        subject-key :sub
        token-type "jwt+type"
        password "test-password"]

    (testing "error scenario"
      (let [ex (ex-info (str "Test Exception " (rand-int 10000)) {})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))
                      utils/exception->response (fn [in-exception in-request]
                                                  (is (= (.getMessage ex) (.getMessage in-exception)))
                                                  (is (= request in-request))
                                                  (assoc request :source ::exception-handler))]
          (is (= (assoc request :source ::exception-handler)
                 (authenticate-request request-handler issuer subject-key type keys password request))))))

    (testing "success scenario"
      (let [realm "www.test-realm.com"
            request {:headers {"authorization" "Bearer foo.bar.baz"
                               "host" realm}
                     :request-id (rand-int 10000)
                     :scheme :test-scheme}
            current-time (current-time-secs)
            expiry-interval-secs 10000
            expiry-time (+ current-time expiry-interval-secs)
            principal "foo@bar.com"
            payload {:aud realm :exp expiry-time :iss issuer :sub principal}]
        (with-redefs [validate-access-token (fn [in-type in-issuer in-sub-key in-keys in-realm in-request-scheme in-access-token]
                                              (is (= token-type in-type))
                                              (is (= issuer in-issuer))
                                              (is (= subject-key in-sub-key))
                                              (is (= keys in-keys))
                                              (is (= realm in-realm))
                                              (is (= :test-scheme in-request-scheme))
                                              (is (= "foo.bar.baz" in-access-token))
                                              (is (= keys in-keys))
                                              payload)
                      auth/handle-request-auth (fn [request-handler request principal auth-params-map password auth-cookie-age-in-seconds]
                                                 (-> request
                                                   (assoc :auth-cookie-age-in-seconds auth-cookie-age-in-seconds
                                                          :auth-params-map auth-params-map
                                                          :password password
                                                          :principal principal)
                                                   request-handler))
                      t/now (constantly (tc/from-long (* current-time 1000)))]
          (is (= (assoc request
                   :auth-cookie-age-in-seconds expiry-interval-secs
                   :auth-params-map (auth/auth-params-map :jwt principal)
                   :password password
                   :principal principal
                   :source ::request-handler)
                 (authenticate-request request-handler token-type issuer subject-key keys password request))))))))

(deftest test-jwt-authenticator
  (with-redefs [start-jwt-cache-maintainer (constantly nil)]
    (let [config {:http-options {:conn-timeout 10000
                                 :socket-timeout 10000}
                  :token-type "jwt+type"
                  :issuer "w8r"
                  :jwks-url "https://www.jwt-test.com/keys"
                  :password "test-password"
                  :subject-key :sub
                  :update-interval-ms 1000}]
      (testing "valid configuration"
        (is (instance? JwtAuthenticator (jwt-authenticator config))))

      (testing "invalid configuration"
        (is (thrown? Throwable (jwt-authenticator (dissoc config :http-options))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :token-type))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :issuer))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :jwks-url))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :password))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :subject-key))))
        (is (thrown? Throwable (jwt-authenticator (assoc config :subject-key "sub"))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :update-interval-ms))))))))

(deftest test-jwt-auth-handler
  (let [handler (fn [{:keys [source]}] {:body source})
        authenticator (->JwtAuthenticator "jwt+type" "issuer" (atom {:key-id->jwk ::jwt-keys}) "password" :sub)
        jwt-handler (auth/wrap-auth-handler authenticator handler)]
    (with-redefs [authenticate-request (fn [handler token-type issuer subject-key keys password request]
                                         (is (= "jwt+type" token-type))
                                         (is (= "issuer" issuer))
                                         (is (= :sub subject-key))
                                         (is (= ::jwt-keys keys))
                                         (is (= "password" password))
                                         (handler (assoc request :source ::jwt-auth)))]
      (is (= {:body ::standard-request}
             (jwt-handler {:headers {}
                           :source ::standard-request})))
      (is (= {:body ::standard-request}
             (jwt-handler {:headers {"authorization" "Negotiate abcd"}
                           :source ::standard-request})))
      (is (= {:body ::standard-request}
             (jwt-handler {:headers {"authorization" "Negotiate abcd,Negotiate wxyz"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Bearer abcd"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Negotiate abcd,Bearer abcd"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Bearer abcd,Negotiate wxyz"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Negotiate abcd,Bearer abcd,Negotiate wxyz"}
                           :source ::standard-request})))
      (is (= {:body ::standard-request}
             (jwt-handler {:authorization/principal "user@test.com"
                           :authorization/user "user"
                           :headers {"authorization" "Bearer abcd"}
                           :source ::standard-request}))))))
