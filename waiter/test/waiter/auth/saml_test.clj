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
(ns waiter.auth.saml-test
  (:require [clj-time.core :as t]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [saml20-clj.shared :as saml-shared]
            [waiter.auth.authentication :as auth]
            [waiter.auth.saml :refer :all]
            [waiter.util.utils :as utils]))

; TODO check only GET method

(def valid-config
  {:idp-uri "https://idp-host/idp-endpoint"
   :idp-cert-uri "test-files/saml/idp.crt"
   :hostname "hostname"
   :password "password"})

(defn dummy-saml-authenticator
  ([config]
   (saml-authenticator config))
  ([]
   (dummy-saml-authenticator valid-config)))

(def dummy-request
  {:uri "/my-endpoint"
   :request-method :get
   :query-string "a=1&b=c"
   :headers {"cookie" (str auth/AUTH-COOKIE-NAME "=my-auth-cookie")}})

(def time-now
  (clj-time.format/parse "2019-05-03T16:43:39.151Z"))

(deftest test-make-saml-authenticator
  (testing "should throw on invalid configuration"
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :idp-uri nil))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :idp-cert-uri nil))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :hostname nil))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :password nil)))))
  (testing "should not throw on valid configuration"
    (dummy-saml-authenticator valid-config)))

(deftest test-wrap-handler
  (let [saml-authenticator (dummy-saml-authenticator)
        handler (fn [request] request)
        wrapped-handler (auth/wrap-auth-handler saml-authenticator handler)]
    (testing "has auth cookie"
      (with-redefs [auth/decode-auth-cookie (fn [waiter-cookie password] (if (= "my-auth-cookie" waiter-cookie) ["my-user@domain"] nil))
                    auth/decoded-auth-valid? (fn [decoded-auth-cookie] true)]
        (is (= (merge dummy-request {:authorization/principal "my-user@domain"
                                     :authorization/user "my-user"})
               (wrapped-handler dummy-request)))))
    (testing "does not have auth cookie"
      (with-redefs [clj-time.core/now (fn [] time-now)
                    utils/make-uuid (constantly "UUID")]
        (is (= {:body ""
                :headers {"Location" (slurp "test-files/saml/idp-redirect-location.txt")}
                :status 302}
               (wrapped-handler dummy-request)))))))

(defn- normalize-x509-certificate-string
  [cert-str]
  (-> cert-str
      (string/replace "Sat Dec 31 08:34:47 CST 2016" "")
      (string/replace "Thu Jun 25 09:34:47 CDT 2048" "")))

(deftest test-certificate-x509
  (is (= (normalize-x509-certificate-string (slurp "test-files/saml/x509-certificate-to-string.txt"))
         (normalize-x509-certificate-string (str (certificate-x509 (slurp (:idp-cert-uri valid-config))))))))

(defn- saml-response-from-xml
  [change-user?]
  (saml-shared/str->deflate->base64
    (string/replace (slurp "test-files/saml/saml-response.xml")
                    "user1@example.com"
                    (str (if change-user? "root" "user1") "@example.com"))))

(deftest test-saml-acs-handler
  (org.opensaml.DefaultBootstrap/bootstrap)
  (let [test-time (clj-time.format/parse "2019-05-14")
        processed-saml-response {:not-on-or-after (clj-time.format/parse "2019-05-15T21:52:46.000Z")
                                 :original-request {:headers {"accept" "*/*"
                                                              "host" "localhost:9091"
                                                              "user-agent" "curl/7.58.0"
                                                              "x-cid" "17ea73916fc6-46d0b85773af2452"
                                                              "x-waiter-token" "python-local"}
                                                    :query-string nil
                                                    :request-method :get
                                                    :uri "/"}
                                 :saml-principal "_c2c02940517f53c3ea1673f6406fb34fd39aa7bcf6"}]
    (with-redefs [t/now (fn [] test-time)
                  auth/handle-request-auth (fn [handler request user principal password]
                                             (merge (handler) {:user user :principal principal}))]
      (testing "has valid saml response"
        (let [request (merge {:form-params {"SAMLResponse" (slurp "test-files/saml/saml-response.txt") "RelayState" (slurp "test-files/saml/relay-state.txt")}} dummy-request)]
          (is (= processed-saml-response (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config)) :password "password"})))))
      (testing "has valid saml response (from xml)"
        (with-redefs [saml-shared/byte-deflate (fn [_] _)]
          (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml false) "RelayState" (slurp "test-files/saml/relay-state.txt")}} dummy-request)]
            (is (= processed-saml-response (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config)) :password "password"}))))))
      (testing "has invalid saml signature"
        (with-redefs [saml-shared/byte-deflate (fn [_] _)]
          (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml true) "RelayState" (slurp "test-files/saml/relay-state.txt")}} dummy-request)]
            (is (thrown-with-msg? Exception #"Could not authenticate user. Invalid SAML assertion signature."
                                  (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config)) :password "password"}))))))))
  (testing "has expired saml response"
    (let [request (merge {:form-params {"SAMLResponse" (slurp "test-files/saml/saml-response.txt") "RelayState" (slurp "test-files/saml/relay-state.txt")}} dummy-request)]
      (is (thrown-with-msg? Exception #"Could not authenticate user. Expired SAML assertion."
                            (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config)) :password "password"}))))))