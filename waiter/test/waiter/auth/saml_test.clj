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
  (:require [clojure.test :refer :all]
            [saml20-clj.shared :as saml-shared]
            [waiter.auth.authentication :as auth]
            [waiter.auth.saml :refer :all]))

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
      (with-redefs [clj-time.core/now (fn [] time-now)]
        (is (= {:body ""
                :headers {"Location" (slurp "test-files/saml/idp-redirect-location.txt")}
                :status 302}
               (wrapped-handler dummy-request)))))))

(deftest test-certificate-x509
  (is (= (slurp "test-files/saml/x509-certificate-to-string.txt") (str (certificate-x509 (slurp (:idp-cert-uri valid-config)))))))

(defn- saml-response-from-xml
  [change-user?]
  (saml-shared/str->deflate->base64
    (clojure.string/replace (slurp "test-files/saml/saml-response.xml")
                            "user1@example.com"
                            (str (if change-user? "root" "user1") "@example.com"))))

(deftest test-saml-acs-handler
  (testing "has valid saml response"
    (let [request (merge {:form-params {"SAMLResponse" (slurp "test-files/saml/saml-response.txt") "RelayState" "my-relay-state"}} dummy-request)]
      (is (= {:authorization/principal "user1@example.com"
              :authorization/user "user1"
              :body ""
              :headers {"Location" "my-relay-state"
                        "set-cookie" "x-waiter-auth=TlBZAHFpEXVzZXIxQGV4YW1wbGUuY29tKwAAAWp<variable>;Max-Age=86400;Path=/;HttpOnly=true"}
              :status 303}
             (update
               (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config))})
               :headers (fn [headers] (update headers "set-cookie" #(clojure.string/replace % #"%[^;]+" "<variable>"))))))))
  (testing "has valid saml response (from xml)"
    (with-redefs [saml-shared/byte-deflate (fn [_] _)]
      (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml false) "RelayState" "my-relay-state"}} dummy-request)]
        (is (= {:authorization/principal "user1@example.com"
                :authorization/user "user1"
                :body ""
                :headers {"Location" "my-relay-state"
                          "set-cookie" "x-waiter-auth=TlBZAHFpEXVzZXIxQGV4YW1wbGUuY29tKwAAAWp<variable>;Max-Age=86400;Path=/;HttpOnly=true"}
                :status 303}
               (update
                 (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config))})
                 :headers (fn [headers] (update headers "set-cookie" #(clojure.string/replace % #"%[^;]+" "<variable>")))))))))
  (testing "has invalid saml signature"
    (with-redefs [saml-shared/byte-deflate (fn [_] _)]
      (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml true) "RelayState" "my-relay-state"}} dummy-request)]
        (is (thrown-with-msg? Exception #"Could not authenticate user. Invalid SAML response signature."
                              (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config))})))))))