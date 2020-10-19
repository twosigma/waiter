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
(ns waiter.auth.composite-test
  (:require [clojure.test :refer :all]
            [waiter.auth.authentication :as auth]
            [waiter.auth.composite :refer :all]))


(defrecord TestAuthenticator [process-callback-response wrap-auth-handler-response]
  auth/Authenticator
  (wrap-auth-handler [_ _] (constantly wrap-auth-handler-response)))

(def one-user-process-callback-response "one-user-process-callback-response")
(def one-user-wrap-auth-handler-response "one-user-wrap-auth-handler-response")

(defn one-user-authenticator
  [_]
  (->TestAuthenticator one-user-process-callback-response one-user-wrap-auth-handler-response))

(def valid-config
  {:authentication-providers {"one-user" {:factory-fn 'waiter.auth.composite-test/one-user-authenticator
                                          :run-as-user "WAITER_AUTH_RUN_AS_USER"}}
   :default-authentication-provider "one-user"})

(defn dummy-composite-authenticator
  ([] (dummy-composite-authenticator valid-config))
  ([config] (composite-authenticator (assoc config :default-authentication "standard"))))


(def time-now
  (clj-time.format/parse "2019-05-03T16:43:39.151Z"))

(deftest test-make-composite-authenticator
  (testing "should throw on invalid configuration"
    (is (thrown? Throwable (dummy-composite-authenticator (assoc valid-config :authentication-providers nil))))
    (is (thrown? Throwable (dummy-composite-authenticator (assoc valid-config :authentication-providers {}))))
    (is (thrown? Throwable (dummy-composite-authenticator (assoc valid-config :default-authentication-provider nil))))
    (is (thrown? Throwable (dummy-composite-authenticator (assoc valid-config :default-authentication-provider " "))))
    (is (thrown? Throwable (dummy-composite-authenticator (assoc valid-config :default-authentication-provider "foo")))))
  (testing "should not throw on valid configuration"
    (dummy-composite-authenticator valid-config)))

(defn bad-one-user-authenticator [_])

(deftest auth-standard-auth-type
  (is (thrown-with-msg?
        Exception #"Authenticator factory did not create an instance of Authenticator"
        (dummy-composite-authenticator (assoc-in valid-config
                                                 [:authentication-schemes "one-user" :authenticator-factory-fn]
                                                 'waiter.auth.composite-test/bad-one-user-authenticator)))))

(defn- make-request
  [auth-type]
  {:waiter-discovery {:service-description-template {"authentication" auth-type}}})

(deftest auth-standard-auth-type
  (let [composite-authenticator (dummy-composite-authenticator)
        wrapped-handler (auth/wrap-auth-handler composite-authenticator identity)]
    (is (= one-user-wrap-auth-handler-response (wrapped-handler (make-request "standard"))))))

(deftest auth-invalid-auth-type
  (let [composite-authenticator (dummy-composite-authenticator)
        wrapped-handler (auth/wrap-auth-handler composite-authenticator identity)]
    (is (thrown-with-msg? Exception #"No authenticator found for invalid authentication."
                          (wrapped-handler (make-request "invalid"))))))
