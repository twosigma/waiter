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
(ns waiter.auth.spnego-test
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.auth.authentication :as auth]
            [waiter.auth.spnego :refer :all]
            [waiter.util.utils :as utils]))

(deftest test-decode-input-token
  (is (decode-input-token {:headers {"authorization" "Negotiate Kerberos-Auth"}}))
  (is (nil? (decode-input-token {:headers {"authorization" "Basic User-Pass"}}))))

(deftest test-encode-output-token
  (let [encoded-token (encode-output-token (.getBytes "Kerberos-Auth"))]
    (is (= "Kerberos-Auth"
           (String. (decode-input-token {:headers {"authorization" encoded-token}}))))))

(deftest test-require-gss
  (let [ideal-response {:body "OK" :status 200}
        request-handler (constantly ideal-response)
        thread-pool (Object.)
        max-queue-length 10
        password [:cached "test-password"]
        auth-principal "user@test.com"
        standard-request {}
        handler (require-gss request-handler thread-pool max-queue-length password)
        standard-401-response {:body "Unauthorized"
                               :headers {"content-type" "text/plain"
                                         "server" "waiter"
                                         "www-authenticate" "Negotiate"}
                               :status 401}]

    (testing "valid auth cookie"
      (with-redefs [auth/decode-auth-cookie (constantly [auth-principal nil])
                    auth/decoded-auth-valid? (constantly true)]
        (is (= (assoc ideal-response
                 :authorization/principal auth-principal
                 :authorization/user (first (str/split auth-principal #"@" 2)))
               (handler standard-request)))))

    (testing "too many pending kerberos requests"
      (with-redefs [auth/decode-auth-cookie (constantly [auth-principal nil])
                    auth/decoded-auth-valid? (constantly false)
                    too-many-pending-auth-requests? (constantly true)]
        (let [handler (require-gss request-handler thread-pool max-queue-length password)]
          (is (= {:body "Too many Kerberos authentication requests"
                  :headers {"content-type" "text/plain"
                            "server" "waiter"}
                  :status 503}
                 (handler standard-request))))))

    (testing "standard 401 response on missing authorization header"
      (with-redefs [auth/decode-auth-cookie (constantly [auth-principal nil])
                    auth/decoded-auth-valid? (constantly false)
                    too-many-pending-auth-requests? (constantly false)]
        (let [handler (require-gss request-handler thread-pool max-queue-length password)]
          (is (= standard-401-response (handler standard-request))))))

    (testing "kerberos authentication path"
      (with-redefs [auth/decode-auth-cookie (constantly [auth-principal nil])
                    auth/decoded-auth-valid? (constantly false)
                    too-many-pending-auth-requests? (constantly false)]
        (let [auth-request (update standard-request :headers assoc "authorization" "foo-bar")
              error-object (Object.)]

          (testing "401 response on failed authentication"
            (with-redefs [populate-gss-credentials (fn [_ _ response-chan]
                                                     (async/>!! response-chan {:foo :bar}))]
              (let [handler (require-gss request-handler thread-pool max-queue-length password)
                    response (handler auth-request)
                    response (if (map? response)
                               response
                               (async/<!! response))]
                (is (= standard-401-response response)))))

          (testing "error object on exception"
            (with-redefs [populate-gss-credentials (fn [_ _ response-chan]
                                                     (async/>!! response-chan {:error error-object}))]
              (let [handler (require-gss request-handler thread-pool max-queue-length password)
                    response (handler auth-request)
                    response (if (map? response)
                               response
                               (async/<!! response))]
                (is (= error-object response)))))

          (testing "successful authentication - principal and token"
            (with-redefs [populate-gss-credentials (fn [_ _ response-chan]
                                                     (async/>!! response-chan {:principal auth-principal
                                                                               :token "test-token"}))]
              (let [handler (require-gss request-handler thread-pool max-queue-length password)
                    response (handler auth-request)
                    response (if (map? response)
                               response
                               (async/<!! response))]
                (is (= (assoc ideal-response
                         :authorization/principal "user@test.com"
                         :authorization/user "user"
                         :headers {"www-authenticate" "test-token"})
                       (utils/dissoc-in response [:headers "set-cookie"]))))))

          (testing "successful authentication - principal only"
            (with-redefs [populate-gss-credentials (fn [_ _ response-chan]
                                                     (async/>!! response-chan {:principal auth-principal}))]
              (let [handler (require-gss request-handler thread-pool max-queue-length password)
                    response (handler auth-request)
                    response (if (map? response)
                               response
                               (async/<!! response))]
                (is (= (assoc ideal-response
                         :authorization/principal "user@test.com"
                         :authorization/user "user")
                       (utils/dissoc-in response [:headers "set-cookie"])))))))))))
