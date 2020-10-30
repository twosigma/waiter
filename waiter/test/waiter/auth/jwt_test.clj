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
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [waiter.auth.authentication :as auth]
            [waiter.auth.jwt :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.security.interfaces RSAPublicKey)
           (net.i2p.crypto.eddsa EdDSAPublicKey)
           (waiter.auth.jwt JwtAuthenticator JwtAuthServer JwtValidator)))

(defn make-jwt-auth-server
  [& {:keys [http-client jwks-url keys-cache oidc-authorize-uri oidc-token-uri]
      :or {http-client (Object.)
           jwks-url nil
           keys-cache (atom {})
           oidc-authorize-uri nil
           oidc-token-uri nil}}]
  (->JwtAuthServer http-client jwks-url keys-cache oidc-authorize-uri oidc-token-uri))

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
        issuer-constraints [issuer]
        max-expiry-duration-ms (-> 1 t/hours t/in-millis)
        subject-key :sub
        subject-regex default-subject-regex
        supported-algorithms #{:eddsa :rs256}
        token-type "ty+pe"
        realm "www.test-realm.com"
        request-scheme :https
        accept-scope? false
        access-token "access-token"
        jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                      supported-algorithms token-type)]

    (testing "unsupported algorithm"
      (let [jwks (->> (filter (fn [{:keys [crv]}] (= crv "P-256")) all-keys)
                   (map attach-public-key)
                   (pc/map-from-vals :kid))
            alg :es256
            {:keys [kid] :as jwk-entry} (first (vals jwks))
            expiry-time (+ (current-time-secs) 10000)
            subject-key :sub
            jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                          supported-algorithms token-type)
            payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
            access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
        (is (thrown-with-msg? ExceptionInfo #"Unsupported algorithm :es256 in token header"
                              (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token)))))

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
                              (validate-access-token jwt-validator jwks nil request-scheme accept-scope? access-token)))

        (is (thrown-with-msg? ExceptionInfo #"JWT authentication can only be used with HTTPS connections"
                              (validate-access-token jwt-validator jwks realm :http accept-scope? access-token)))

        (is (thrown-with-msg? ExceptionInfo #"Must provide Bearer token in Authorization header"
                              (validate-access-token jwt-validator jwks realm request-scheme accept-scope? " ")))

        (is (thrown-with-msg? ExceptionInfo #"JWT access token is malformed"
                              (validate-access-token jwt-validator jwks realm request-scheme accept-scope? "abcd")))

        (let [jwk-entry (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {})]
          (is (thrown-with-msg? ExceptionInfo #"JWT header is missing key ID"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [jwk-entry (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {:kid "invalid-key" :typ (str token-type ".err")})]
          (is (thrown-with-msg? ExceptionInfo #"Unsupported type"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [jwk-entry (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {:kid "invalid-key" :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No matching JWKS key found for key invalid-key"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Issuer not provided in claims"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {:iss (str issuer "-john")} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Issuer does not match provided constraints"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              issuer-constraints [#"test-issuer-jane"]
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              issuer "test-issuer-john"
              access-token (generate-jwt-access-token alg jwk-entry {:iss issuer} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Issuer does not match provided constraints"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              issuer-constraints [#"https://jwt.com/jane"]
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              issuer "http://jwt.com/jane"
              access-token (generate-jwt-access-token alg jwk-entry {:iss issuer} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Issuer does not match provided constraints"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              access-token (generate-jwt-access-token alg jwk-entry {:iss issuer} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Audience does not match www.test-realm.com"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              access-token (generate-jwt-access-token alg jwk-entry {:aud realm :exp expiry-time :iss issuer} {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No subject provided in the token payload"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              payload {:aud realm :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No expiry provided in the token payload"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (- (current-time-secs) 1000)
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Token is expired"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) max-expiry-duration-ms 10000)
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Token expiry is too far into the future"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No custom-key provided in the token payload"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (= payload (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              payload {:aud realm :exp expiry-time :iss issuer :sub "foo@bar.com" :unknown-claim "john.doe"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (= payload (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              payload {:aud realm :custom-key "foo@bar.baz" :exp expiry-time :iss issuer}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"No subject provided in the token payload"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              payload {:aud realm :custom-key "foo@bar" :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Provided subject in the token payload does not satisfy the validation regex"
                                (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              payload {:aud realm :custom-key "foo@bar.baz" :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (= payload (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              payload {:aud realm :custom-key "foo@BAR.BAZ" :exp expiry-time :iss issuer :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (= payload (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token))))

        (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
              expiry-time (+ (current-time-secs) 10000)
              subject-key :custom-key
              jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                            supported-algorithms token-type)
              payload {:aud realm :custom-key "foo@BAR.BAZ" :exp expiry-time :iss issuer :scope "READ" :sub "foo@bar.com"}
              access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
          (is (thrown-with-msg? ExceptionInfo #"Token payload includes a scope"
                                (validate-access-token jwt-validator jwks realm request-scheme false access-token)))
          (is (= payload (validate-access-token jwt-validator jwks realm request-scheme true access-token))))

        (doseq [issuer ["custom-issuer" "https://www.jwt.com/ts" "ts-custom-issuer" "ts-issuer" "ts-jwt"]]
          (let [{:keys [kid] :as jwk-entry} (rand-nth (vals jwks))
                expiry-time (+ (current-time-secs) 10000)
                issuer-constraints ["ts-issuer" #"ts-.*" #"ts-.*-issuer" "custom-issuer" #"https://www.jwt.com/.*"]
                subject-key :custom-key
                jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                              supported-algorithms token-type)
                payload {:aud realm :custom-key "foo@BAR.BAZ" :exp expiry-time :iss issuer :sub "foo@bar.com"}
                access-token (generate-jwt-access-token alg jwk-entry payload {:kid kid :typ token-type})]
            (is (= payload (validate-access-token jwt-validator jwks realm request-scheme accept-scope? access-token)))))))))

(deftest test-authenticate-request
  (let [issuer-constraints "test-issuer"
        keys (Object.)
        subject-key :sub
        subject-regex default-subject-regex
        supported-algorithms #{:eddsa :rs256}
        token-type "jwt+type"
        password "test-password"
        max-expiry-duration-ms (-> 1 t/hours t/in-millis)
        jwt-validator (->JwtValidator issuer-constraints max-expiry-duration-ms subject-key subject-regex
                                      supported-algorithms token-type)]

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
                 (authenticate-request request-handler jwt-validator keys password request))))))

    (testing "error scenario 401 - downstream 200 from backend"
      (let [request-handler (fn [request] (assoc request :source ::request-handler :status http-200-ok))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status http-401-unauthorized})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (assoc request :source ::request-handler :status http-200-ok)
                 (authenticate-request request-handler jwt-validator keys password request))))))

    (testing "error scenario 401 - downstream 200 from waiter"
      (let [request-handler (fn [request] (-> request (dissoc :headers) (assoc :source ::request-handler :status http-200-ok) utils/attach-waiter-source))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status http-401-unauthorized})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (-> request
                   (dissoc :headers)
                   (assoc :source ::request-handler :status http-200-ok)
                   utils/attach-waiter-source)
                 (authenticate-request request-handler jwt-validator keys password request))))))

    (testing "error scenario 401 - downstream 401 from backend"
      (let [request-handler (fn [request] (assoc request :source ::request-handler :status http-401-unauthorized))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status http-401-unauthorized})
            request {:headers {"authorization" "Bearer foo.bar.baz"}
                     :request-id (rand-int 10000)}]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (assoc request :source ::request-handler :status http-401-unauthorized)
                 (authenticate-request request-handler jwt-validator keys password request))))))

    (testing "error scenario 401 - downstream 401 from waiter"
      (let [request-handler (fn [request] (-> request (dissoc :headers) (assoc :source ::request-handler :status http-401-unauthorized) utils/attach-waiter-source))
            ex (ex-info (str "Test Exception " (rand-int 10000)) {:status http-401-unauthorized})
            request {:headers {"authorization" "Bearer foo.bar.baz"
                               "host" "www.test.com"}
                     :request-id (rand-int 10000)}
            auth-header (str auth/bearer-prefix "realm=\"www.test.com\"")]
        (with-redefs [validate-access-token (fn [& _] (throw ex))]
          (is (= (-> request
                   (assoc :headers {"www-authenticate" auth-header} :source ::request-handler :status http-401-unauthorized)
                   utils/attach-waiter-source)
                 (authenticate-request request-handler jwt-validator keys password request))))))

    (testing "success scenario - non 401"
      (let [request-handler (fn [request] (assoc request :source ::request-handler))
            realm "www.test-realm.com"
            access-token "foo.bar.baz"
            current-time (current-time-secs)
            expiry-interval-secs 10000
            expiry-time (+ current-time expiry-interval-secs)
            principal "foo@bar.com"]
        (doseq [{:keys [access-token-scope payload]}
                [{:access-token-scope nil
                  :payload {:aud realm :exp expiry-time :iss issuer-constraints :sub principal}}
                 {:access-token-scope "false"
                  :payload {:aud realm :exp expiry-time :iss issuer-constraints :sub principal}}
                 {:access-token-scope "true"
                  :payload {:aud realm :exp expiry-time :iss issuer-constraints :scope "READ" :sub principal}}]]
          (let [request (cond-> {:headers {"authorization" (str "Bearer " access-token)
                                           "host" realm}
                                 :request-id (rand-int 10000)
                                 :scheme :test-scheme}
                          access-token-scope
                          (assoc-in [:waiter-discovery :service-description-template "metadata" "accept-scoped-token"] access-token-scope))]
            (with-redefs [validate-access-token (fn [jwt-validator in-keys in-realm in-request-scheme in-accept-scope? in-access-token]
                                                  (is (= token-type (:token-type jwt-validator)))
                                                  (is (= issuer-constraints (:issuer-constraints jwt-validator)))
                                                  (is (= subject-key (:subject-key jwt-validator)))
                                                  (is (= subject-regex (:subject-regex jwt-validator)))
                                                  (is (= supported-algorithms (:supported-algorithms jwt-validator)))
                                                  (is (= max-expiry-duration-ms (:max-expiry-duration-ms jwt-validator)))
                                                  (is (= keys in-keys))
                                                  (is (= realm in-realm))
                                                  (is (= :test-scheme in-request-scheme))
                                                  (is (= (= access-token-scope "true") in-accept-scope?))
                                                  (is (= "foo.bar.baz" in-access-token))
                                                  payload)
                          auth/handle-request-auth (fn [request-handler request auth-params-map password auth-cookie-age-in-seconds add-auth-cookie?]
                                                     (is (false? add-auth-cookie?))
                                                     (-> request
                                                       (assoc :auth-cookie-age-in-seconds auth-cookie-age-in-seconds
                                                              :auth-params-map auth-params-map
                                                              :password password
                                                              :principal (:authorization/principal auth-params-map))
                                                       request-handler))
                          t/now (constantly (tc/from-long (* current-time 1000)))]
              (is (= (assoc request
                       :auth-cookie-age-in-seconds expiry-interval-secs
                       :auth-params-map (auth/build-auth-params-map :jwt principal {:jwt-access-token access-token
                                                                                    :jwt-payload (json/write-str payload)})
                       :password password
                       :principal principal
                       :source ::request-handler)
                     (authenticate-request request-handler jwt-validator keys password request))))))))))

(deftest test-wrap-auth-handler
  (with-redefs [authenticate-request (fn [_ _ _ _ request] (assoc request :processed-by :authenticate-request))]
    (let [request-handler (fn [request] (assoc request :processed-by :request-handler))
          jwt-authenticator {:allow-bearer-auth-api? false
                             :allow-bearer-auth-services? false
                             :attach-www-authenticate-on-missing-bearer-token? false
                             :auth-server (make-jwt-auth-server :jwks-url "jwks-url")}]
      (testing "jwt-disabled"
        (let [jwt-auth-handler (wrap-auth-handler jwt-authenticator request-handler)]
          (doseq [status [http-200-ok
                          http-301-moved-permanently
                          http-400-bad-request
                          http-401-unauthorized
                          http-403-forbidden]]
            (doseq [waiter-api-call? [true false]]
              (is (= {:headers {"www-authenticate" "StandardAuth"}
                      :processed-by :request-handler
                      :status status
                      :waiter-api-call? waiter-api-call?}
                     (jwt-auth-handler {:headers {"www-authenticate" "StandardAuth"}
                                        :status status
                                        :waiter-api-call? waiter-api-call?})))))))

      (testing "already authenticated"
        (let [jwt-auth-handler (wrap-auth-handler jwt-authenticator request-handler)]
          (doseq [status [http-200-ok
                          http-301-moved-permanently
                          http-400-bad-request
                          http-401-unauthorized
                          http-403-forbidden]]
            (doseq [waiter-api-call? [true false]]
              (is (= {:authorization/principal "auth-principal"
                      :authorization/user "auth-user"
                      :headers {"www-authenticate" "StandardAuth"}
                      :processed-by :request-handler
                      :status status
                      :waiter-api-call? waiter-api-call?}
                     (jwt-auth-handler {:authorization/principal "auth-principal"
                                        :authorization/user "auth-user"
                                        :headers {"www-authenticate" "StandardAuth"}
                                        :status status
                                        :waiter-api-call? waiter-api-call?})))))))

      (testing "jwt-enabled and bearer token provided"
        (let [jwt-authenticator {:allow-bearer-auth-api? true
                                 :allow-bearer-auth-services? true
                                 :auth-server (make-jwt-auth-server :jwks-url "jwks-url")}
              jwt-auth-handler (wrap-auth-handler jwt-authenticator request-handler)]
          (doseq [status [http-200-ok
                          http-301-moved-permanently
                          http-400-bad-request
                          http-401-unauthorized
                          http-403-forbidden]]
            (doseq [waiter-api-call? [true false]]
              (is (= {:headers {"authorization" "Bearer 12.34.56"
                                "www-authenticate" "StandardAuth"}
                      :processed-by :authenticate-request
                      :status status
                      :waiter-api-call? waiter-api-call?}
                     (jwt-auth-handler {:headers {"authorization" "Bearer 12.34.56"
                                                  "www-authenticate" "StandardAuth"}
                                        :status status
                                        :waiter-api-call? waiter-api-call?})))))))

      (testing "jwt-enabled and bearer token NOT provided"
        (doseq [attach-www-authenticate-on-missing-bearer-token? [true false]]
          (let [jwt-authenticator {:allow-bearer-auth-api? true
                                   :allow-bearer-auth-services? true
                                   :attach-www-authenticate-on-missing-bearer-token? attach-www-authenticate-on-missing-bearer-token?
                                   :auth-server (make-jwt-auth-server :jwks-url "jwks-url")}
                jwt-auth-handler (wrap-auth-handler jwt-authenticator request-handler)]
            (doseq [status [http-200-ok
                            http-301-moved-permanently
                            http-400-bad-request
                            http-401-unauthorized
                            http-403-forbidden]]
              (doseq [waiter-api-call? [true false]]
                (doseq [response-source [:backend :waiter]]
                  (is (= {:headers {"authorization" "Basic 123456"
                                    "host" "www.test.com:1234"
                                    "www-authenticate" (if (and attach-www-authenticate-on-missing-bearer-token?
                                                                (= response-source :waiter)
                                                                (= status http-401-unauthorized))
                                                         ["StandardAuth" "Bearer realm=\"www.test.com\""]
                                                         "StandardAuth")}
                          :processed-by :request-handler
                          :status status
                          :waiter-api-call? waiter-api-call?
                          :waiter/response-source response-source}
                         (jwt-auth-handler {:headers {"authorization" "Basic 123456"
                                                      "host" "www.test.com:1234"
                                                      "www-authenticate" "StandardAuth"}
                                            :status status
                                            :waiter-api-call? waiter-api-call?
                                            :waiter/response-source response-source}))))))))))))

(deftest test-create-jwt-validator
  (with-redefs [start-jwt-cache-maintainer (constantly nil)]
    (let [config {:issuer "w8r"
                  :password "test-password"
                  :subject-key :sub
                  :supported-algorithms #{:eddsa}
                  :token-type "jwt+type"}]
      (testing "valid configuration"
        (is (instance? JwtValidator (create-jwt-validator config)))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :allow-bearer-auth-api? true))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :allow-bearer-auth-api? false))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :allow-bearer-auth-services? true))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :allow-bearer-auth-services? false))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :attach-www-authenticate-on-missing-bearer-token? true))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :attach-www-authenticate-on-missing-bearer-token? false))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :issuer "w8r.*"))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :issuer #"w8r.*"))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :issuer ["w8r" #"w8r.*"]))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :max-expiry-duration-ms 900000))))
        (is (instance? JwtValidator (create-jwt-validator (assoc config :supported-algorithms #{:eddsa :rs256})))))

      (testing "invalid configuration"
        (is (thrown? Throwable (create-jwt-validator (dissoc config :token-type))))
        (is (thrown? Throwable (create-jwt-validator (dissoc config :issuer))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :issuer []))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :issuer 123))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :issuer [123]))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :max-expiry-duration-ms 0))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :max-expiry-duration-ms -1000))))
        (is (thrown? Throwable (create-jwt-validator (dissoc config :subject-key))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :subject-key "sub"))))
        (is (thrown? Throwable (create-jwt-validator (dissoc config :supported-algorithms))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :supported-algorithms [:eddsa :rs256]))))
        (is (thrown? Throwable (create-jwt-validator (assoc config :supported-algorithms #{:hs256}))))))))

(deftest test-create-auth-server
  (with-redefs [start-jwt-cache-maintainer (constantly nil)]
    (let [create-jwt-auth-server (fn [config]
                                   (create-auth-server config))
          config {:http-options {:conn-timeout 10000
                                 :socket-timeout 10000}
                  :jwks-url "https://www.jwt-test.com/keys"
                  :supported-algorithms #{:eddsa}
                  :update-interval-ms 1000}]
      (testing "valid configuration"
        (is (instance? JwtAuthServer (create-jwt-auth-server config)))
        (is (instance? JwtAuthServer (create-jwt-auth-server (assoc config :supported-algorithms #{:eddsa :rs256})))))

      (testing "invalid configuration"
        (is (thrown? Throwable (create-jwt-auth-server (dissoc config :http-options))))
        (is (thrown? Throwable (create-jwt-auth-server (dissoc config :jwks-url))))
        (is (thrown? Throwable (create-jwt-auth-server (dissoc config :supported-algorithms))))
        (is (thrown? Throwable (create-jwt-auth-server (assoc config :supported-algorithms [:eddsa :rs256]))))
        (is (thrown? Throwable (create-jwt-auth-server (assoc config :supported-algorithms #{:hs256}))))
        (is (thrown? Throwable (create-jwt-auth-server (dissoc config :update-interval-ms))))))))

(deftest test-jwt-authenticator
  (with-redefs [start-jwt-cache-maintainer (constantly nil)]
    (let [auth-server (make-jwt-auth-server :jwks-url "jwks-url")
          jwt-validator (map->JwtValidator {})
          create-jwt-authenticator (fn [config]
                                     (jwt-authenticator auth-server jwt-validator config))
          config {:password "test-password"}]
      (testing "valid configuration"
        (is (instance? JwtAuthenticator (create-jwt-authenticator config)))
        (is (instance? JwtAuthenticator (create-jwt-authenticator (assoc config :allow-bearer-auth-api? true))))
        (is (instance? JwtAuthenticator (create-jwt-authenticator (assoc config :allow-bearer-auth-api? false))))
        (is (instance? JwtAuthenticator (create-jwt-authenticator (assoc config :allow-bearer-auth-services? true))))
        (is (instance? JwtAuthenticator (create-jwt-authenticator (assoc config :allow-bearer-auth-services? false))))
        (is (instance? JwtAuthenticator (create-jwt-authenticator (assoc config :attach-www-authenticate-on-missing-bearer-token? true))))
        (is (instance? JwtAuthenticator (create-jwt-authenticator (assoc config :attach-www-authenticate-on-missing-bearer-token? false)))))

      (testing "invalid configuration"
        (is (thrown? Throwable (create-jwt-authenticator (assoc config :allow-bearer-auth-api? "true"))))
        (is (thrown? Throwable (create-jwt-authenticator (assoc config :allow-bearer-auth-services? "true"))))
        (is (thrown? Throwable (create-jwt-authenticator (assoc config :attach-www-authenticate-on-missing-bearer-token? "true"))))
        (is (thrown? Throwable (create-jwt-authenticator (dissoc config :password))))))))

(deftest test-jwt-auth-handler
  (let [handler (fn [{:keys [source]}] {:body source})
        supported-algorithms #{:eddsa}
        keys-cache (atom {:key-id->jwk ::jwt-keys})
        max-expiry-duration-ms (-> 1 t/hours t/in-millis)
        subject-regex default-subject-regex
        issuer-constraints ["issuer"]
        attach-bearer-auth-env
        (fn attach-bearer-auth-env [request env-value]
          (assoc-in request [:waiter-discovery :service-description-template "env" "USE_BEARER_AUTH"]
                    (str env-value)))]
    (with-redefs [authenticate-request (fn [handler jwt-validator keys password request]
                                         (is (= "jwt+type" (:token-type jwt-validator)))
                                         (is (= issuer-constraints (:issuer-constraints jwt-validator)))
                                         (is (= :sub (:subject-key jwt-validator)))
                                         (is (= subject-regex (:subject-regex jwt-validator)))
                                         (is (= supported-algorithms (:supported-algorithms jwt-validator)))
                                         (is (= ::jwt-keys keys))
                                         (is (= "password" password))
                                         (is (= max-expiry-duration-ms (:max-expiry-duration-ms jwt-validator)))
                                         (handler (assoc request :source ::jwt-auth)))]
      (let [auth-server (make-jwt-auth-server :jwks-url "jwks-url" :keys-cache keys-cache)
            jwt-validator (->JwtValidator
                            issuer-constraints max-expiry-duration-ms :sub subject-regex supported-algorithms "jwt+type")
            authenticator (->JwtAuthenticator false false false auth-server jwt-validator "password")
            jwt-handler (wrap-auth-handler authenticator handler)]
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
        (is (= {:body ::standard-request}
               (jwt-handler {:headers {"authorization" "Bearer ab.cd.ef"}
                             :source ::standard-request})))
        (is (= {:body ::standard-request}
               (jwt-handler {:headers {"authorization" "Bearer wxyz,Bearer ab.cd.ef"}
                             :source ::standard-request})))
        (is (= {:body ::standard-request}
               (jwt-handler {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef"}
                             :source ::standard-request})))
        (is (= {:body ::standard-request}
               (jwt-handler {:headers {"authorization" "Bearer ab.cd.ef,Negotiate wxyz"}
                             :source ::standard-request})))
        (is (= {:body ::standard-request}
               (jwt-handler {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef,Negotiate wxyz"}
                             :source ::standard-request})))
        (is (= {:body ::jwt-auth}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Bearer ab.cd.ef"}
                    :source ::standard-request}
                   true))))
        (is (= {:body ::jwt-auth}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Bearer wxyz,Bearer ab.cd.ef"}
                    :source ::standard-request}
                   true))))
        (is (= {:body ::jwt-auth}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef"}
                    :source ::standard-request}
                   true))))
        (is (= {:body ::jwt-auth}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Bearer ab.cd.ef,Negotiate wxyz"}
                    :source ::standard-request}
                   true))))
        (is (= {:body ::jwt-auth}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef,Negotiate wxyz"}
                    :source ::standard-request}
                   true))))

        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Bearer wxyz,Bearer ab.cd.ef"}
                    :source ::standard-request}
                   false))))
        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef"}
                    :source ::standard-request}
                   false))))
        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Bearer ab.cd.ef,Negotiate wxyz"}
                    :source ::standard-request}
                   false))))
        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef,Negotiate wxyz"}
                    :source ::standard-request}
                   false))))

        (is (= {:body ::standard-request}
               (jwt-handler
                 {:authorization/principal "user@test.com"
                  :authorization/user "user"
                  :headers {"authorization" "Bearer abcd"}
                  :source ::standard-request})))

        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Bearer wxyz,Bearer ab.cd.ef"}
                    :source ::standard-request
                    :waiter-api-call? true}
                   true))))
        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef"}
                    :source ::standard-request
                    :waiter-api-call? true}
                   true))))
        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Bearer ab.cd.ef,Negotiate wxyz"}
                    :source ::standard-request
                    :waiter-api-call? true}
                   true))))
        (is (= {:body ::standard-request}
               (jwt-handler
                 (attach-bearer-auth-env
                   {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef,Negotiate wxyz"}
                    :source ::standard-request
                    :waiter-api-call? true}
                   true)))))

      (doseq [allow-bearer-auth-api? [true false]]
        (let [auth-server (make-jwt-auth-server :jwks-url "jwks-url" :keys-cache keys-cache)
              jwt-validator (->JwtValidator
                              issuer-constraints max-expiry-duration-ms :sub subject-regex supported-algorithms "jwt+type")
              authenticator (->JwtAuthenticator
                              allow-bearer-auth-api? true false auth-server jwt-validator "password")
              jwt-handler (wrap-auth-handler authenticator handler)]
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
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Bearer wxyz,Bearer ab.cd.ef"}
                      :source ::standard-request}
                     false))))
          (is (= {:body ::standard-request}
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef"}
                      :source ::standard-request}
                     false))))
          (is (= {:body ::standard-request}
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Bearer ab.cd.ef,Negotiate wxyz"}
                      :source ::standard-request}
                     false))))
          (is (= {:body ::standard-request}
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef,Negotiate wxyz"}
                      :source ::standard-request}
                     false))))))

      (doseq [allow-bearer-auth-services? [true false]]
        (let [auth-server (make-jwt-auth-server :jwks-url "jwks-url" :keys-cache keys-cache)
              jwt-validator (->JwtValidator
                              issuer-constraints max-expiry-duration-ms :sub subject-regex supported-algorithms "jwt+type")
              authenticator (->JwtAuthenticator
                              true allow-bearer-auth-services? false auth-server jwt-validator "password")
              jwt-handler (wrap-auth-handler authenticator handler)]
          (is (= {:body ::jwt-auth}
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Bearer wxyz,Bearer ab.cd.ef"}
                      :source ::standard-request
                      :waiter-api-call? true}
                     false))))
          (is (= {:body ::jwt-auth}
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef"}
                      :source ::standard-request
                      :waiter-api-call? true}
                     false))))
          (is (= {:body ::jwt-auth}
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Bearer ab.cd.ef,Negotiate wxyz"}
                      :source ::standard-request
                      :waiter-api-call? true}
                     false))))
          (is (= {:body ::jwt-auth}
                 (jwt-handler
                   (attach-bearer-auth-env
                     {:headers {"authorization" "Negotiate abcd,Bearer ab.cd.ef,Negotiate wxyz"}
                      :source ::standard-request
                      :waiter-api-call? true}
                     false)))))))))

(deftest test-request-access-token
  (let [access-code "access-code"
        challenge-cookie "challenge-cookie"
        oidc-callback "/oidc/callback"
        host-header "www.test.com:8080"
        request {:headers {"host" host-header}
                 :scheme "https"}]
    (is (thrown-with-msg?
          ExceptionInfo #"OIDC token endpoint not configured"
          (request-access-token (make-jwt-auth-server) request oidc-callback access-code challenge-cookie)))

    (let [{:keys [http-client oidc-token-uri] :as auth-server} (make-jwt-auth-server :oidc-token-uri "/oidc/token")
          access-token (str "access-token:" (rand-int 10000))
          make-http-request-async (fn [result]
                                    (fn [in-http-client in-oidc-token-uri & {:keys [form-params request-method]}]
                                      (is (= http-client in-http-client))
                                      (is (= oidc-token-uri in-oidc-token-uri))
                                      (is (= {"client_id" "www.test.com"
                                              "code" access-code
                                              "code_verifier" challenge-cookie
                                              "grant_type" "authorization_code"
                                              "redirect_uri" (str "https://" host-header oidc-callback)}
                                             form-params))
                                      (is (= :post request-method))
                                      (let [result-chan (async/promise-chan)]
                                        (async/>!! result-chan result)
                                        result-chan)))]

      (with-redefs [hu/http-request-async (make-http-request-async {:id_token access-token})]
        (let [result-chan (request-access-token auth-server request oidc-callback access-code challenge-cookie)
              result (async/<!! result-chan)]
          (is (= access-token result))))

      (with-redefs [hu/http-request-async (make-http-request-async {:access_token access-token})]
        (let [result-chan (request-access-token auth-server request oidc-callback access-code challenge-cookie)
              result (async/<!! result-chan)]
          (is (instance? ExceptionInfo result))
          (is (= "ID token missing in auth server response" (ex-message result)))))

      (with-redefs [hu/http-request-async (make-http-request-async (Exception. "Created from test"))]
        (let [result-chan (request-access-token auth-server request oidc-callback access-code challenge-cookie)
              result (async/<!! result-chan)]
          (is (instance? Exception result))
          (is (= "Created from test" (ex-message result)))))

      (with-redefs [hu/http-request-async (make-http-request-async (ex-info "Created from test"
                                                                            {:http-utils/response {:body {"error" "Bad request"}
                                                                                                   :headers {"content-type" "application/json"}
                                                                                                   :status 400}}))]
        (let [result-chan (request-access-token auth-server request oidc-callback access-code challenge-cookie)
              result (async/<!! result-chan)]
          (is (instance? Exception result))
          (is (= "Non-2XX response from auth server" (ex-message result)))
          (is (= {:body {"error" "Bad request"}
                  :headers {"content-type" "application/json"}
                  :source :auth-server
                  :status 400}
                 (ex-data result))))))))
