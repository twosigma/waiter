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
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [taoensso.nippy :as nippy]
            [waiter.auth.authentication :as auth]
            [waiter.auth.saml :refer :all]
            [waiter.cookie-support :as cookie-support]
            [waiter.status-codes :refer :all]
            [waiter.util.utils :as utils])
  (:import (java.io ByteArrayInputStream)))

(defn string->stream
  [data-string]
  (ByteArrayInputStream. (.getBytes data-string)))

(def valid-config
  {:auth-redirect-endpoint "auth-redirect-endpoint"
   :hostname "hostname"
   :idp-cert-uri "test-files/saml/idp.crt"
   :idp-uri "https://idp-host/idp-endpoint"
   :password [:salted "password"]})

(defn dummy-saml-authenticator
  ([config]
   (saml-authenticator config))
  ([]
   (dummy-saml-authenticator valid-config)))

(def dummy-request
  {:headers {"cookie" (str auth/AUTH-COOKIE-NAME "=my-auth-cookie") "host" "my.app.domain"}
   :query-string "a=1&b=c"
   :request-method :get
   :scheme :http
   :uri "/my-endpoint"})

(def time-now
  (clj-time.format/parse "2019-05-03T16:43:39.151Z"))

(deftest test-make-saml-authenticator
  (testing "should throw on invalid configuration"
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :idp-uri nil))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :idp-cert-uri nil))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :hostname nil))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :password nil))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :idp-uri " "))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :idp-cert-uri " "))))
    (is (thrown? Throwable (dummy-saml-authenticator (assoc valid-config :hostname " ")))))
  (testing "should not throw on valid configuration"
    (dummy-saml-authenticator valid-config)))

(deftest test-wrap-handler
  (let [saml-authenticator (dummy-saml-authenticator)
        wrapped-handler (auth/wrap-auth-handler saml-authenticator identity)]
    (testing "can't use POST"
      (is (thrown-with-msg? Exception #"Invalid request method for use with SAML authentication"
                            (wrapped-handler (assoc dummy-request :request-method :post)))))
    (testing "does not have auth cookie"
      (let [get-idp-redirect-original get-idp-redirect]
        (with-redefs [nippy/freeze (fn [data _] (.getBytes (str data)))
                      t/now (fn [] time-now)
                      utils/unique-identifier (constantly "UUID")
                      encode-xml identity
                      render-saml-authentication-request-template identity
                      utils/map->base-64-string (fn [data _] data)
                      get-idp-redirect (fn [idp-url saml-request relay-state]
                                         (is (= {:time-issued "2019-05-03T16:43:39Z"
                                                 :saml-service-name "waiter"
                                                 :saml-id "WAITER-UUID"
                                                 :acs-url "https://hostname/waiter-auth/saml/acs"
                                                 :idp-uri "https://idp-host/idp-endpoint"}
                                                saml-request))
                                         (is (= {:host "my.app.domain"
                                                 :request-url "http://my.app.domain/my-endpoint?a=1&b=c"
                                                 :scheme "http"}
                                                relay-state))
                                         (get-idp-redirect-original idp-url "saml-request" "relay-state"))]
          (is (= {:body ""
                  :headers {"Location" "https://idp-host/idp-endpoint?SAMLRequest=saml-request&RelayState=relay-state"}
                  :status http-302-moved-temporarily}
                 (wrapped-handler dummy-request))))))))

(deftest test-auth-redirect-endpoint
  (let [saml-authenticator (dummy-saml-authenticator)
        test-time (clj-time.format/parse "2019-05-14")
        {:keys [password]} valid-config]
    (testing "bad saml-auth-data"
      (with-redefs [t/now (fn [] time-now)
                    utils/unique-identifier (constantly "UUID")]
        (is (thrown-with-msg? Exception #"Could not parse saml-auth-data"
                              (saml-auth-redirect-handler saml-authenticator
                                                          (-> (merge-with merge dummy-request {:headers {"content-type" "application/x-www-form-urlencoded"}})
                                                            (merge {:request-method :post
                                                                    :body (string->stream "saml-auth-data=my-saml-auth-data")})))))))
    (testing "has saml-auth-data"
      (with-redefs [cookie-support/add-encoded-cookie (fn [response in-password name value age-in-seconds]
                                                        (is (= password in-password))
                                                        (is (= (-> 1 t/days t/in-seconds) age-in-seconds))
                                                        (update response :headers
                                                                assoc "set-cookie" (str name "=" value)))
                    utils/base-64-string->map (fn [data in-password]
                                                (is (= password in-password))
                                                (when (= "my-saml-auth-data" data)
                                                  {:not-on-or-after (t/plus test-time (t/years 1)) :saml-principal "my-user@domain" :redirect-url "redirect-url"}))
                    t/now (fn [] test-time)]
        (let [dummy-request' (-> (merge-with merge dummy-request {:headers {"content-type" "application/x-www-form-urlencoded"}})
                               (merge {:request-method :post
                                       :body (string->stream "saml-auth-data=my-saml-auth-data")}))]
          (is (= {:authorization/method :saml
                  :authorization/principal "my-user@domain"
                  :authorization/user "my-user"
                  :body ""
                  :headers {"location" "redirect-url"
                            "set-cookie" (str "x-waiter-auth=" ["my-user@domain" (tc/to-long test-time)])}
                  :status http-303-see-other
                  :waiter/response-source :waiter}
                 (saml-auth-redirect-handler saml-authenticator dummy-request'))))))

    (testing "has saml-auth-data no expiry"
      (with-redefs [cookie-support/add-encoded-cookie (fn [response in-password name value age-in-seconds]
                                                        (is (= password in-password))
                                                        (is (= (-> 1 t/days t/in-seconds) age-in-seconds))
                                                        (update response :headers
                                                                assoc "set-cookie" (str name "=" value)))
                    utils/base-64-string->map (fn [data in-password]
                                                (is (= password in-password))
                                                (when (= "my-saml-auth-data" data)
                                                  {:saml-principal "my-user@domain" :redirect-url "redirect-url"}))
                    t/now (fn [] test-time)]
        (let [dummy-request' (-> (merge-with merge dummy-request {:headers {"content-type" "application/x-www-form-urlencoded"}})
                               (merge {:request-method :post
                                       :body (string->stream "saml-auth-data=my-saml-auth-data")}))]
          (is (= {:authorization/method :saml
                  :authorization/principal "my-user@domain"
                  :authorization/user "my-user"
                  :body ""
                  :headers {"location" "redirect-url"
                            "set-cookie" (str "x-waiter-auth=" ["my-user@domain" (tc/to-long test-time)])}
                  :status http-303-see-other
                  :waiter/response-source :waiter}
                 (saml-auth-redirect-handler saml-authenticator dummy-request'))))))

    (testing "has saml-auth-data short expiry"
      (with-redefs [cookie-support/add-encoded-cookie (fn [response in-password name value age-in-seconds]
                                                        (is (= password in-password))
                                                        (is (= (-> 1 t/hours t/in-seconds) age-in-seconds))
                                                        (update response :headers
                                                                assoc "set-cookie" (str name "=" value)))
                    utils/base-64-string->map (fn [data in-password]
                                                (is (= password in-password))
                                                (when (= "my-saml-auth-data" data)
                                                  {:min-session-not-on-or-after (t/plus test-time (t/hours 1)) :saml-principal "my-user@domain" :redirect-url "redirect-url"}))
                    t/now (fn [] test-time)]
        (let [dummy-request' (-> (merge-with merge dummy-request {:headers {"content-type" "application/x-www-form-urlencoded"}})
                               (merge {:request-method :post
                                       :body (string->stream "saml-auth-data=my-saml-auth-data")}))]
          (is (= {:authorization/method :saml
                  :authorization/principal "my-user@domain"
                  :authorization/user "my-user"
                  :body ""
                  :headers {"location" "redirect-url"
                            "set-cookie" (str "x-waiter-auth=" ["my-user@domain" (tc/to-long test-time)])}
                  :status http-303-see-other
                  :waiter/response-source :waiter}
                 (saml-auth-redirect-handler saml-authenticator dummy-request'))))))))

(defn- saml-response-from-xml
  [change-user?]
  (-> (slurp "test-files/saml/saml-response.xml")
    (str/replace "user1@example.com" (str (if change-user? "root" "user1") "@example.com"))
    .getBytes
    b64/encode
    String.))

(def relay-state-string
  (utils/map->base-64-string {:host "host" :request-url "request-url" :scheme "scheme"} [:salted "password"]))

(deftest test-saml-acs-handler
  (let [saml-authenticator (dummy-saml-authenticator)
        test-time (clj-time.format/parse "2019-05-14")
        expiry-time (clj-time.format/parse "2019-05-16T05:31:19.000Z")
        processed-saml-response {:body {:auth-redirect-uri "scheme://host/waiter-auth/saml/auth-redirect"
                                        :saml-auth-data {:min-session-not-on-or-after expiry-time
                                                         :redirect-url "request-url"
                                                         :saml-principal "user1@example.com"}}
                                 :status http-200-ok
                                 :waiter/response-source :waiter}]
    (with-redefs [nippy/freeze (fn [data _] (.getBytes (str data)))
                  t/now (fn [] test-time)
                  render-authenticated-redirect-template identity
                  utils/map->base-64-string (fn [data _] data)]
      (testing "has valid saml response"
        (let [request (merge {:form-params {"SAMLResponse" (slurp "test-files/saml/saml-response.txt") "RelayState" relay-state-string}} dummy-request)]
          (is (= processed-saml-response (saml-acs-handler saml-authenticator request)))))
      (testing "has valid saml response (from xml)"
        (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml false) "RelayState" relay-state-string}} dummy-request)]
          (is (= processed-saml-response (saml-acs-handler saml-authenticator request)))))
      (testing "has invalid saml signature"
        (let [request (merge {:form-params {"SAMLResponse" (saml-response-from-xml true) "RelayState" relay-state-string}} dummy-request)]
          (is (thrown-with-msg? Exception #"Could not authenticate user. Invalid SAML assertion signature."
                                (saml-acs-handler saml-authenticator request))))))
    (testing "has expired saml response"
      (let [request (merge {:form-params {"SAMLResponse" (slurp "test-files/saml/saml-response.txt") "RelayState" relay-state-string}} dummy-request)]
        (is (thrown-with-msg? Exception #"Could not authenticate user. Expired SAML assertion."
                              (saml-acs-handler saml-authenticator request)))))))