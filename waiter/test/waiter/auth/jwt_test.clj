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
  (:require [buddy.core.keys :as buddy-keys]
            [buddy.sign.jwt :as jwt]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [waiter.auth.authentication :as auth]
            [waiter.auth.jwt :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.security.interfaces RSAPublicKey)
           (net.i2p.crypto.eddsa EdDSAPublicKey)
           (waiter.auth.jwt JwtAuthenticator)))

(deftest test-retrieve-public-key
  (let [eddsa-entry (pc/keywordize-map
                      {"crv" "Ed25519",
                       "d" "0uecAYPVTD8_-0Xx3rWSr1EQZx6mB4_lPvXHrKtNp1M",
                       "kid" "dc918afa-d8cc-41cf-b537-8a40859d3f46",
                       "kty" "OKP",
                       "use" "sig",
                       "x" "3StIkhARy4UZMV0OkvB6ZrN3dAt9sh_X9ZI15n1yr-c"})]
    (is (instance? EdDSAPublicKey (retrieve-public-key eddsa-entry))))
  (let [rs256-entry (pc/keywordize-map
                      {"alg" "RS256",
                       "d" "Eq5xpGnNCivDflJsRQBXHx1hdR1k6Ulwe2JZD50LpXyWPEAeP88vLNO97IjlA7_GQ5sLKMgvfTeXZx9SE-7YwVol2NXOoAJe46sui395IW_GO-pWJ1O0BkTGoVEn2bKVRUCgu-GjBVaYLU6f3l9kJfFNS3E0QbVdxzubSu3Mkqzjkn439X0M_V51gfpRLI9JYanrC4D4qAdGcopV_0ZHHzQlBjudU2QvXt4ehNYTCBr6XCLQUShb1juUO1ZdiYoFaFQT5Tw8bGUl_x_jTj3ccPDVZFD9pIuhLhBOneufuBiB4cS98l2SR_RQyGWSeWjnczT0QU91p1DhOVRuOopznQ",
                       "e" "AQAB",
                       "kty" "RSA",
                       "n" "ofgWCuLjybRlzo0tZWJjNiuSfb4p4fAkd_wWJcyQoTbji9k0l8W26mPddxHmfHQp-Vaw-4qPCJrcS2mJPMEzP1Pt0Bm4d4QlL-yRT-SFd2lZS-pCgNMsD1W_YpRPEwOWvG6b32690r2jZ47soMZo9wGzjb_7OMg0LOL-bSf63kpaSHSXndS5z5rexMdbBYUsLA9e-KXBdQOS-UTo7WTBEMa2R2CapHg665xsmtdVMTBQY4uDZlxvb3qCo5ZwKh9kG4LT6_I5IhlJH7aGhyxXFvUK-DWNmoudF8NAco9_h9iaGNj8q2ethFkMLs91kzk2PAcDTW9gb54h4FRWyuXpoQ",
                       "use" "sig"})]
    (is (instance? RSAPublicKey (retrieve-public-key rs256-entry)))))

(deftest test-refresh-keys-cache
  (let [http-client (Object.)
        keys-url "https://www.test.com/jwks/keys"
        retry-limit 3
        http-options {:conn-timeout 10000
                      :retry-interval-ms 10
                      :retry-limit retry-limit
                      :socket-timeout 10000}
        supported-algorithms #{:eddsa :rs256}
        jwks-keys (walk/keywordize-keys
                    {"keys" [{"crv" "P-256", "kty" "OKP", "name" "fee", "use" "sig"}
                             {"crv" "Ed25519", "kty" "RSA", "name" "fie", "use" "sig", "x" "x1"}
                             {"crv" "Ed25519", "kid" "k1", "kty" "OKP", "name" "foe", "use" "sig", "x" "x2"}
                             {"crv" "Ed25519", "kty" "OKP", "name" "fum", "use" "enc", "x" "x3"}
                             {"e" "AQAB", "kid" "k2", "kty" "RSA", "name" "fum", "use" "sig", "n" "x1"}
                             {"crv" "Ed25519", "kty" "OKP", "name" "fum", "use" "sig", "x" "x4"}]})
        valid-key-entry (pc/map-vals
                          (fn [{:strs [kid] :as entry}]
                            (-> entry
                              (pc/keywordize-map)
                              (assoc :waiter.auth.jwt/public-key (str "public-key-" kid))))
                          {"k1" {"crv" "Ed25519", "kid" "k1", "kty" "OKP", "name" "foe", "use" "sig", "x" "x2"}
                           "k2" {"e" "AQAB", "kid" "k2", "kty" "RSA", "name" "fum", "use" "sig", "n" "x1"}})]

    (testing "http-request throws exception always"
      (let [http-request-counter (atom 0)]
        (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                        (is (= http-client in-http-client))
                                        (is (= keys-url in-url))
                                        (swap! http-request-counter inc)
                                        (throw (IllegalStateException. "From test")))]
          (let [keys-cache (atom {})]
            (is (thrown-with-msg? IllegalStateException #"From test"
                                  (refresh-keys-cache http-client http-options keys-url supported-algorithms keys-cache)))
            (is (= {} @keys-cache))
            (is (= retry-limit @http-request-counter))))))

    (testing "http-request throws exception once"
      (let [current-time (t/now)
            http-request-counter (atom 0)]
        (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                        (is (= http-client in-http-client))
                                        (is (= keys-url in-url))
                                        (swap! http-request-counter inc)
                                        (if (= 1 @http-request-counter)
                                          (throw (IllegalStateException. "From test"))
                                          jwks-keys))
                      t/now (constantly current-time)
                      retrieve-public-key (fn [{:keys [kid]}] (str "public-key-" kid))]
          (let [keys-cache (atom {})]
            (refresh-keys-cache http-client http-options keys-url supported-algorithms keys-cache)
            (is (= {:key-id->jwk valid-key-entry
                    :last-update-time current-time
                    :summary {:num-filtered-keys 2 :num-jwks-keys 6}}
                   @keys-cache))
            (is (= 2 @http-request-counter))))))

    (testing "http-request returns string"
      (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                      (is (= http-client in-http-client))
                                      (is (= keys-url in-url))
                                      "From test")]
        (let [keys-cache (atom {})]
          (is (thrown-with-msg? ExceptionInfo #"Invalid response from the JWKS endpoint"
                                (refresh-keys-cache http-client http-options keys-url supported-algorithms keys-cache)))
          (is (= {} @keys-cache)))))

    (testing "http-request returns no keys"
      (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                      (is (= http-client in-http-client))
                                      (is (= keys-url in-url))
                                      {"keys" []})]
        (let [keys-cache (atom {})]
          (is (thrown-with-msg? ExceptionInfo #"No supported keys found from the JWKS endpoint"
                                (refresh-keys-cache http-client http-options keys-url supported-algorithms keys-cache)))
          (is (= {} @keys-cache)))))

    (testing "http-request returns some keys"
      (let [current-time (t/now)]
        (with-redefs [hu/http-request (fn [in-http-client in-url & _]
                                        (is (= http-client in-http-client))
                                        (is (= keys-url in-url))
                                        jwks-keys)
                      t/now (constantly current-time)
                      retrieve-public-key (fn [{:keys [kid]}] (str "public-key-" kid))]
          (let [keys-cache (atom {})]
            (refresh-keys-cache http-client http-options keys-url supported-algorithms keys-cache)
            (is (= {:key-id->jwk valid-key-entry
                    :last-update-time current-time
                    :summary {:num-filtered-keys 2 :num-jwks-keys 6}}
                   @keys-cache))))))))

(deftest test-jwt-cache-maintainer
  (let [jwks-data (-> "test-files/jwt/jwks.json" slurp json/read-str walk/keywordize-keys :keys)
        data-counter (atom 1)
        http-client (Object.)
        keys-url "https://www.test.com/jwks/keys"
        http-options {:conn-timeout 10000
                      :socket-timeout 10000}
        supported-algorithms #{:eddsa :rs256}]
    (with-redefs [refresh-keys-cache (fn [in-http-client in-http-options in-url in-algorithms in-keys-cache]
                                       (is (= http-client in-http-client))
                                       (is (= http-options in-http-options))
                                       (is (= keys-url in-url))
                                       (is (= supported-algorithms in-algorithms))
                                       (reset! in-keys-cache {:keys (take @data-counter jwks-data)
                                                              :last-update-time (t/now)}))]
      (let [keys-cache (atom {})
            update-interval-ms 5
            {:keys [cancel-fn query-state-fn]}
            (start-jwt-cache-maintainer
              http-client http-options keys-url update-interval-ms supported-algorithms keys-cache)]
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

(defn- generate-jwt-access-token
  "Generates the JWT access token using the provided private key."
  [alg jwk-entry payload header]
  (let [private-key (buddy-keys/jwk->private-key (pc/keywordize-map jwk-entry))
        options {:alg alg :header header}]
    (jwt/sign payload private-key options)))

(deftest test-validate-access-token
  (let [all-keys (-> "test-files/jwt/jwks.json" slurp json/read-str walk/keywordize-keys :keys)
        issuer "test-issuer"
        subject-key :sub
        supported-algorithms #{:eddsa :rs256}
        token-type "ty+pe"
        realm "www.test-realm.com"
        request-scheme :https
        access-token "access-token"]

    (doseq [{:keys [alg jwks]}
            [{:alg :eddsa
              :jwks (->> (filter eddsa-key? all-keys)
                      (map attach-public-key)
                      (pc/map-from-vals :kid))}
             {:alg :rs256
              :jwks (->> (filter rs256-key? all-keys)
                      (map attach-public-key)
                      (pc/map-from-vals :kid))}]]
      (testing (str "algorithm " (name alg))
        (is (thrown-with-msg? ExceptionInfo #"JWT authentication can only be used with host header"
                              (validate-access-token token-type issuer subject-key supported-algorithms jwks nil request-scheme access-token)))

        (is (thrown-with-msg? ExceptionInfo #"JWT authentication can only be used with HTTPS connections"
                              (validate-access-token token-type issuer subject-key supported-algorithms jwks realm :http access-token)))

        (is (thrown-with-msg? ExceptionInfo #"Must provide Bearer token in Authorization header"
                              (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme " ")))

        (is (thrown-with-msg? ExceptionInfo #"JWT access token is malformed"
                              (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme "abcd")))

        (let [jwk-entry (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {})]
          (is (thrown-with-msg? ExceptionInfo #"JWT header is missing key ID"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [jwk-entry (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {:kid "invalid-key" :typ (str token-type ".err")})]
          (is (thrown-with-msg? ExceptionInfo #"Unsupported type"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [jwk-entry (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {:kid "invalid-key" :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No matching JWKS key found for key invalid-key"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Issuer does not match test-issuer"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {:iss issuer} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Audience does not match www.test-realm.com"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              access-token (generate-jwt-access-token alg jwk-entry {:aud realm :exp expiry-time :iss issuer} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No subject provided in the token payload"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              payload {:aud realm :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No expiry provided in the token payload"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (- (current-time-secs) 1000)
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Token is expired"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No custom-key provided in the token payload"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (= payload (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              payload {:aud realm :custom-key "foo@bar.baz" :exp expiry-time :iss issuer}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No subject provided in the token payload"
                                (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              payload {:aud realm :custom-key "foo@bar.baz" :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (= payload (validate-access-token token-type issuer subject-key supported-algorithms jwks realm request-scheme access-token))))))))

(deftest test-authenticate-request
  (let [issuer "test-issuer"
        keys (Object.)
        subject-key :sub
        supported-algorithms #{:eddsa :rs256}
        token-type "jwt+type"
        password "test-password"]

    (testing "error scenario - non 401"
      (let [request-handler (fn [request] (assoc request :source ::request-handler))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))
                      utils/exception->response (fn [in-exception in-request]
                                                  (is (= (.getMessage ex) (.getMessage in-exception)))
                                                  (is (= request in-request))
                                                  (assoc request :source ::exception-handler))]
          (is (= (assoc request :source ::exception-handler)
                 (authenticate-request request-handler issuer subject-key supported-algorithms type keys password request))))))

    (testing "error scenario 401 - downstream 200 from backend"
      (let [request-handler (fn [request] (assoc request :source ::request-handler :status 200))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status 401})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (assoc request :source ::request-handler :status 200)
                 (authenticate-request request-handler issuer subject-key supported-algorithms type keys password request))))))

    (testing "error scenario 401 - downstream 200 from waiter"
      (let [request-handler (fn [request] (-> request (dissoc :headers) (assoc :source ::request-handler :status 200) utils/attach-waiter-source))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status 401})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (-> request
                   (dissoc :headers)
                   (assoc :source ::request-handler :status 200)
                   utils/attach-waiter-source)
                 (authenticate-request request-handler issuer subject-key supported-algorithms type keys password request))))))

    (testing "error scenario 401 - downstream 401 from backend"
      (let [request-handler (fn [request] (assoc request :source ::request-handler :status 401))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status 401})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (assoc request :source ::request-handler :status 401)
                 (authenticate-request request-handler issuer subject-key supported-algorithms type keys password request))))))

    (testing "error scenario 401 - downstream 401 from waiter"
      (let [request-handler (fn [request] (-> request (dissoc :headers) (assoc :source ::request-handler :status 401) utils/attach-waiter-source))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status 401})
            request {:headers {"authorization" "Bearer foo.bar.baz"
                               "host" "www.test.com"}
                     :request-id (rand-int 10000)}
            auth-header (str bearer-prefix "realm=\"www.test.com\"")]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (-> request
                   (assoc :headers {"www-authenticate" auth-header} :source ::request-handler :status 401)
                   utils/attach-waiter-source)
                 (authenticate-request request-handler issuer subject-key supported-algorithms type keys password request))))))

    (testing "success scenario - non 401"
      (let [request-handler (fn [request] (assoc request :source ::request-handler))
            realm "www.test-realm.com"
            request {:headers {"authorization" "Bearer foo.bar.baz"
                               "host" realm}
                     :request-id (rand-int 10000)
                     :scheme :test-scheme}
            current-time (current-time-secs)
            expiry-interval-secs 10000
            expiry-time (+ current-time expiry-interval-secs)
            principal "foo@bar.com"
            payload {:aud realm :exp expiry-time :iss issuer :sub principal}]
        (with-redefs [validate-access-token (fn [in-type in-issuer in-sub-key in-algorithms in-keys in-realm in-request-scheme in-access-token]
                                              (is (= token-type in-type))
                                              (is (= issuer in-issuer))
                                              (is (= subject-key in-sub-key))
                                              (is (= supported-algorithms in-algorithms))
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
                 (authenticate-request request-handler token-type issuer subject-key supported-algorithms keys password request))))))))

(deftest test-jwt-authenticator
  (with-redefs [start-jwt-cache-maintainer (constantly nil)]
    (let [config {:http-options {:conn-timeout 10000
                                 :socket-timeout 10000}
                  :token-type "jwt+type"
                  :issuer "w8r"
                  :jwks-url "https://www.jwt-test.com/keys"
                  :password "test-password"
                  :subject-key :sub
                  :supported-algorithms #{:eddsa}
                  :update-interval-ms 1000}]
      (testing "valid configuration"
        (is (instance? JwtAuthenticator (jwt-authenticator config)))
        (is (instance? JwtAuthenticator (jwt-authenticator (assoc config :supported-algorithms #{:eddsa :rs256})))))

      (testing "invalid configuration"
        (is (thrown? Throwable (jwt-authenticator (dissoc config :http-options))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :token-type))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :issuer))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :jwks-url))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :password))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :subject-key))))
        (is (thrown? Throwable (jwt-authenticator (assoc config :subject-key "sub"))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :supported-algorithms))))
        (is (thrown? Throwable (jwt-authenticator (assoc config :supported-algorithms [:eddsa :rs256]))))
        (is (thrown? Throwable (jwt-authenticator (assoc config :supported-algorithms #{:hs256}))))
        (is (thrown? Throwable (jwt-authenticator (dissoc config :update-interval-ms))))))))

(deftest test-jwt-auth-handler
  (let [handler (fn [{:keys [source]}] {:body source})
        supported-algorithms #{:eddsa}
        keys-cache (atom {:key-id->jwk ::jwt-keys})
        authenticator (->JwtAuthenticator "issuer" keys-cache "password" :sub supported-algorithms "jwt+type")
        jwt-handler (wrap-auth-handler authenticator handler)]
    (with-redefs [authenticate-request (fn [handler token-type issuer subject-key in-algorithms keys password request]
                                         (is (= "jwt+type" token-type))
                                         (is (= "issuer" issuer))
                                         (is (= :sub subject-key))
                                         (is (= supported-algorithms in-algorithms))
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
      (is (= {:body ::standard-request}
             (jwt-handler {:headers {"authorization" "Bearer abcdef"}
                           :source ::standard-request})))
      (is (= {:body ::standard-request}
             (jwt-handler {:headers {"authorization" "Bearer abcdef,Bearer wxyz"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Bearer ab.cd.ef"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Bearer wxyz,Bearer ab.cd.ef"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Bearer ab.cd.ef,Negotiate wxyz"}
                           :source ::standard-request})))
      (is (= {:body ::jwt-auth}
             (jwt-handler {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef,Negotiate wxyz"}
                           :source ::standard-request})))
      (is (= {:body ::standard-request}
             (jwt-handler {:authorization/principal "user@test.com"
                           :authorization/user "user"
                           :headers {"authorization" "Bearer abcd"}
                           :source ::standard-request}))))))
