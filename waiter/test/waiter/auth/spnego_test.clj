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
            [clojure.test :refer :all]
            [waiter.auth.spnego :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.utils :as utils]))

(deftest test-decode-input-token
  (is (decode-input-token {:headers {"authorization" "Negotiate Kerberos-Auth"}}))
  (is (nil? (decode-input-token {:headers {"authorization" "Basic User-Pass"}}))))

(deftest test-encode-output-token
  (let [encoded-token (encode-output-token (.getBytes "Kerberos-Auth"))]
    (is (= "Kerberos-Auth"
           (String. (decode-input-token {:headers {"authorization" encoded-token}}))))))

(deftest test-require-gss
  (let [ideal-response {:body "OK" :status http-200-ok}
        request-handler (constantly ideal-response)
        thread-pool (Object.)
        max-queue-length 10
        password [:cached "test-password"]
        auth-principal "user@test.com"
        standard-request {}
        standard-401-response {:body "Unauthorized"
                               :error-class error-class-kerberos-negotiate
                               :headers {"content-type" "text/plain"
                                         "www-authenticate" "Negotiate"}
                               :status http-401-unauthorized
                               :waiter/response-source :waiter}]

    (with-redefs [utils/error-context->text-body (fn mocked-error-context->text-body [data-map _] (-> data-map :message str))]

      (testing "spnego authentication disabled"
        (with-redefs [too-many-pending-auth-requests? (constantly true)]
          (let [request (assoc-in standard-request
                          [:waiter-discovery :service-description-template "env" "USE_SPNEGO_AUTH"] "false")
                handler (require-gss request-handler thread-pool max-queue-length password)]
            (is (= {:body "Unauthorized"
                    :headers {"content-type" "text/plain"}
                    :status http-401-unauthorized
                    :waiter/response-source :waiter}
                   (handler request))))))

      (testing "too many pending kerberos requests"
        (with-redefs [too-many-pending-auth-requests? (constantly true)]
          (let [handler (require-gss request-handler thread-pool max-queue-length password)]
            (is (= {:body "Too many Kerberos authentication requests"
                    :error-class error-class-kerberos-queue-length
                    :headers {"content-type" "text/plain"}
                    :status http-503-service-unavailable
                    :waiter/response-source :waiter}
                   (handler standard-request))))))

      (testing "standard 401 response on missing authorization header"
        (with-redefs [too-many-pending-auth-requests? (constantly false)]
          (let [handler (require-gss request-handler thread-pool max-queue-length password)]
            (is (= standard-401-response (handler standard-request))))))

      (testing "kerberos authentication path"
        (with-redefs [too-many-pending-auth-requests? (constantly false)]
          (let [auth-request (update standard-request :headers assoc "authorization" "Negotiate foo-bar")
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

            (testing "401 response on missing authorization header"
              (with-redefs [populate-gss-credentials (fn [_ _ response-chan]
                                                       (async/>!! response-chan {:foo :bar}))]
                (let [handler (require-gss request-handler thread-pool max-queue-length password)
                      response (handler standard-request)
                      response (if (map? response)
                                 response
                                 (async/<!! response))]
                  (is (= standard-401-response response)))))

            (testing "401 response on invalid authorization header"
              (with-redefs [populate-gss-credentials (fn [_ _ response-chan]
                                                       (async/>!! response-chan {:foo :bar}))]
                (let [handler (require-gss request-handler thread-pool max-queue-length password)
                      auth-request (update standard-request :headers assoc "authorization" "Bearer foo-bar")
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
                           :authorization/method :spnego
                           :authorization/principal "user@test.com"
                           :authorization/user "user"
                           :headers {"www-authenticate" "test-token"})
                         (utils/dissoc-in response [:headers "set-cookie"]))))))

            (testing "successful authentication - principal and token - multiple authorization header"
              (with-redefs [populate-gss-credentials (fn [_ _ response-chan]
                                                       (async/>!! response-chan {:principal auth-principal
                                                                                 :token "test-token"}))]
                (let [handler (require-gss request-handler thread-pool max-queue-length password)
                      auth-request (update standard-request :headers
                                           assoc "authorization" "Bearer fee-fie,Negotiate foo-bar")
                      response (handler auth-request)
                      response (if (map? response)
                                 response
                                 (async/<!! response))]
                  (is (= (assoc ideal-response
                           :authorization/method :spnego
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
                           :authorization/method :spnego
                           :authorization/principal "user@test.com"
                           :authorization/user "user")
                         (utils/dissoc-in response [:headers "set-cookie"]))))))))))))
