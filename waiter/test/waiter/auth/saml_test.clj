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
            [clojure.data.codec.base64 :as b64]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [saml20-clj.shared :as saml-shared]
            [taoensso.nippy :as nippy]
            [waiter.auth.authentication :as auth]
            [waiter.auth.saml :refer :all]
            [waiter.util.utils :as utils])
  (:import (java.io StringBufferInputStream)))

(def valid-config
  {:idp-uri "https://idp-host/idp-endpoint"
   :idp-cert-uri "test-files/saml/idp.crt"
   :hostname "hostname"
   :password [:salted "password"]})

(defn dummy-saml-authenticator
  ([config]
   (saml-authenticator config))
  ([]
   (dummy-saml-authenticator valid-config)))

(def dummy-request
  {:uri "/my-endpoint"
   :request-method :get
   :query-string "a=1&b=c"
   :headers {"cookie" (str auth/AUTH-COOKIE-NAME "=my-auth-cookie") "host" "my.app.domain"}
   :scheme :http})

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
        test-time (clj-time.format/parse "2019-05-14")
        wrapped-handler (auth/wrap-auth-handler saml-authenticator handler)]
    (testing "has auth cookie"
      (with-redefs [auth/decode-auth-cookie (fn [waiter-cookie password] (if (= "my-auth-cookie" waiter-cookie) ["my-user@domain"] nil))
                    auth/decoded-auth-valid? (fn [decoded-auth-cookie] true)]
        (is (= (merge dummy-request {:authorization/principal "my-user@domain"
                                     :authorization/user "my-user"})
               (wrapped-handler dummy-request)))))
    (testing "can't use POST"
      (is (thrown-with-msg? Exception #"Invalid request method for use with SAML authentication"
                            (wrapped-handler (assoc dummy-request :request-method :post)))))
    (testing "can't use POST - bad saml-auth-data"
      (with-redefs [clj-time.core/now (fn [] time-now)
                    utils/make-uuid (constantly "UUID")]
        (is (thrown-with-msg? Exception #"Could not parse saml-auth-data"
                              (wrapped-handler (-> (merge-with merge dummy-request {:headers {"content-type" "application/x-www-form-urlencoded"}})
                                                   (merge {:request-method :post
                                                           :body (StringBufferInputStream. "saml-auth-data=my-saml-auth-data")})))))))
    (testing "has saml-auth-data"
      (with-redefs [b64/decode identity
                    b64/encode identity
                    nippy/freeze (fn [data _] (.getBytes (str data)))
                    nippy/thaw (fn [data _]
                                 (if (= "my-saml-auth-data" (String. data))
                                              {:not-on-or-after (t/plus test-time (t/years 1)) :saml-principal "my-user@domain"}
                                              nil))
                    t/now (fn [] test-time)]
        (let [dummy-request' (-> (merge-with merge dummy-request {:headers {"content-type" "application/x-www-form-urlencoded"}})
                                 (merge {:request-method :post
                                         :body (StringBufferInputStream. "saml-auth-data=my-saml-auth-data")}))]
          (is (= (-> (merge-with merge dummy-request' {:headers {"set-cookie" "x-waiter-auth=%5B%22my%2Duser%40domain%22+1557792000000%5D;Max-Age=86400;Path=/;HttpOnly=true"}})
                     (update :headers dissoc "content-length" "content-type")
                     (merge {:authorization/principal "my-user@domain"
                             :authorization/user "my-user"
                             :body nil
                             :content-length nil
                             :content-type nil
                             :request-method :get}))
                 (wrapped-handler dummy-request'))))))
    (testing "has saml-auth-data short expiry"
      (with-redefs [b64/decode identity
                    b64/encode identity
                    nippy/freeze (fn [data _] (.getBytes (str data)))
                    nippy/thaw (fn [data _]
                                 (if (= "my-saml-auth-data" (String. data))
                                   {:not-on-or-after (t/plus test-time (t/hours 1)) :saml-principal "my-user@domain"}
                                   nil))
                    t/now (fn [] test-time)]
        (let [dummy-request' (-> (merge-with merge dummy-request {:headers {"content-type" "application/x-www-form-urlencoded"}})
                                 (merge {:request-method :post
                                         :body (StringBufferInputStream. "saml-auth-data=my-saml-auth-data")}))]
          (is (= (-> (merge-with merge dummy-request' {:headers {"set-cookie" "x-waiter-auth=%5B%22my%2Duser%40domain%22+1557792000000%5D;Max-Age=3600;Path=/;HttpOnly=true"}})
                     (update :headers dissoc "content-length" "content-type")
                     (merge {:authorization/principal "my-user@domain"
                             :authorization/user "my-user"
                             :body nil
                             :content-length nil
                             :content-type nil
                             :request-method :get}))
                 (wrapped-handler dummy-request'))))))
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
      (string/replace #"From: [^,]+" "")
      (string/replace #"To: [^\]]+" "")))

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
        processed-saml-response {:body "<!doctype html>
<html>
<head>
    <title>Working...</title>
</head>
<body>
<form action=\"auth-redirect-uri\" method=\"post\">
    <input type=\"hidden\" name=\"saml-auth-data\" value=\"ezpub3Qtb24tb3ItYWZ0ZXIgI2Nsai10aW1lL2RhdGUtdGltZSAiMjAxOS0wNS0xNVQyMTo1Mjo0Ni4wMDBaIiwgOnJlZGlyZWN0LXVybCAib3JpZ2luYWwtdXJsIiwgOnNhbWwtcHJpbmNpcGFsICJ1c2VyMUBleGFtcGxlLmNvbSJ9\">
    <noscript>
        <p>JavaScript is disabled. Click Continue to continue to your application.</p>
        <input type=\"submit\" value=\"Continue\">
    </noscript>
</form>
<script language=\"JavaScript\">window.setTimeout('document.forms[0].submit()', 0);</script>
</body>
</html>
"
                                 :status 200}]
    (with-redefs [nippy/freeze (fn [data _] (.getBytes (str data)))
                  t/now (fn [] test-time)
                  auth/handle-request-auth (fn [handler request user principal password]
                                             (merge (handler) {:user user :principal principal}))]
      (testing "has valid saml response"
        (let [request (merge {:form-params {"SAMLResponse" (slurp "test-files/saml/saml-response.txt") "RelayState" "original-url"}} dummy-request)]
          (is (= processed-saml-response (saml-acs-handler request {:auth-redirect-uri "auth-redirect-uri" :idp-cert (slurp (:idp-cert-uri valid-config)) :password [:salted "password"]})))))
      (testing "has valid saml response (from xml)"
        (with-redefs [saml-shared/byte-deflate (fn [_] _)]
          (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml false) "RelayState" "original-url"}} dummy-request)]
            (is (= processed-saml-response (saml-acs-handler request {:auth-redirect-uri "auth-redirect-uri" :idp-cert (slurp (:idp-cert-uri valid-config)) :password [:salted "password"]}))))))
      (testing "has invalid saml signature"
        (with-redefs [saml-shared/byte-deflate (fn [_] _)]
          (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml true) "RelayState" "original-url"}} dummy-request)]
            (is (thrown-with-msg? Exception #"Could not authenticate user. Invalid SAML assertion signature."
                                  (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config)) :password [:salted "password"]}))))))))
  (testing "has expired saml response"
    (let [request (merge {:form-params {"SAMLResponse" (slurp "test-files/saml/saml-response.txt") "RelayState" "original-url"}} dummy-request)]
      (is (thrown-with-msg? Exception #"Could not authenticate user. Expired SAML assertion."
                            (saml-acs-handler request {:idp-cert (slurp (:idp-cert-uri valid-config)) :password [:salted "password"]}))))))