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
(ns waiter.auth.authenticator-test
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.auth.authentication :refer :all]
            [waiter.cookie-support :as cs]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.util.utils :as utils])
  (:import (waiter.auth.authentication SingleUserAuthenticator)))

(deftest test-one-user-authenticator
  (let [username (System/getProperty "user.name")
        authenticator (one-user-authenticator {:password [:cached "some-password"]
                                               :run-as-user username})]
    (is (instance? SingleUserAuthenticator authenticator))
    (let [request-handler (wrap-auth-handler authenticator identity)
          request {}
          expected-request (assoc request
                             :authorization/method :single-user
                             :authorization/principal username
                             :authorization/user username)
          actual-result (request-handler request)]
      (is (= expected-request (dissoc actual-result :headers)))
      (is (str/includes? (get-in actual-result [:headers "set-cookie"]) "x-waiter-auth=")))))

(deftest test-get-auth-cookie-value
  (is (= "abc123" (get-auth-cookie-value "x-waiter-auth=abc123")))
  (is (= "abc123" (get-auth-cookie-value "x-waiter-auth=\"abc123\"")))
  (is (= "abc123" (get-auth-cookie-value "blah=blah;x-waiter-auth=abc123"))))

(deftest test-decode-auth-cookie
  (let [password [:cached "test-password"]
        a-sequence-value ["cookie-value" 1234]
        an-int-value 123456]
    (is (= a-sequence-value (decode-auth-cookie (cs/encode-cookie a-sequence-value password) password)))
    (is (nil? (decode-auth-cookie (cs/encode-cookie an-int-value password) password)))
    (is (nil? (decode-auth-cookie a-sequence-value password)))
    (is (nil? (decode-auth-cookie an-int-value password)))))

(deftest test-decoded-auth-valid?
  (let [now-ms (System/currentTimeMillis)
        now-sec (long (/ now-ms 1000))
        expires-at (+ now-sec 4000)
        one-day-in-millis (-> 1 t/days t/in-millis)]
    (is (false? (decoded-auth-valid? ["test-principal" now-ms])))
    (is (false? (decoded-auth-valid? ["test-principal" (-> now-ms (- one-day-in-millis) (+ 1000))])))
    (is (false? (decoded-auth-valid? ["test-principal" now-ms {:jwt-access-token "a.b.c"}])))
    (is (false? (decoded-auth-valid? ["test-principal" (- now-ms one-day-in-millis 1000) {:expires-at (dec now-sec)}])))
    (is (false? (decoded-auth-valid? ["test-principal" "invalid-string-time" {:expires-at expires-at}])))
    (is (false? (decoded-auth-valid? [(rand-int 10000) "invalid-string-time" {:expires-at expires-at}])))
    (is (false? (decoded-auth-valid? [])))
    (is (false? (decoded-auth-valid? ["test-principal"])))
    (is (false? (decoded-auth-valid? ["test-principal" now-ms now-ms])))
    (is (false? (decoded-auth-valid? ["test-principal" now-ms {:expires-at expires-at :jwt-access-token "a.b.c"} now-ms])))
    ;; expires-at metadata must be present
    (is (true? (decoded-auth-valid? ["test-principal" now-ms {:expires-at expires-at}])))
    (is (true? (decoded-auth-valid? ["test-principal" (-> now-ms (- one-day-in-millis) (+ 1000)) {:expires-at expires-at}])))
    (is (true? (decoded-auth-valid? ["test-principal" now-ms {:expires-at expires-at :jwt-access-token "a.b.c"}])))))

(deftest test-auth-cookie-handler
  (let [request-handler (fn [{:keys [authorization/principal authorization/user]}]
                          {:body {:principal principal
                                  :user user}})
        password "test-password"
        auth-user "test-user"
        auth-principal (str auth-user "@test.com")
        now-ms (System/currentTimeMillis)
        now-sec (long (/ now-ms 1000))
        expires-at (+ now-sec 900000)]

    (testing "auth cookie and bearer token skips cookie auth"
      (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
        (is (= {:body {:principal nil
                       :user nil}}
               (auth-cookie-handler {:headers {"authorization" (str bearer-prefix "john.doe")
                                               "cookie" "x-waiter-auth=test-auth-cookie"}})))))

    (testing "valid auth cookie"
      (with-redefs [decode-auth-cookie (constantly [auth-principal (+ now-ms 60000) {:expires-at expires-at}])]
        (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
          (is (= {:authorization/metadata {:expires-at expires-at}
                  :authorization/method :cookie
                  :authorization/principal auth-principal
                  :authorization/user auth-user
                  :body {:principal auth-principal :user auth-user}}
                 (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=test-auth-cookie"}})))))

      (with-redefs [decode-auth-cookie (constantly [auth-principal (+ now-ms 60000)
                                                    {:expires-at expires-at
                                                     :jwt-access-token "test.access.token"}])]
        (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
          (is (= {:authorization/metadata {:expires-at expires-at
                                           :jwt-access-token "test.access.token"}
                  :authorization/method :cookie
                  :authorization/principal auth-principal
                  :authorization/user auth-user
                  :body {:principal auth-principal :user auth-user}}
                 (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=test-auth-cookie"}}))))))

    (testing "invalid auth cookie"
      (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
        (is (= {:body {:principal nil :user nil}} (auth-cookie-handler {:headers {}})))
        (is (= {:body {:principal nil :user nil}} (auth-cookie-handler {:headers {"cookie" "foo=bar"}})))
        (is (= {:body {:principal nil :user nil}} (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=foo"}}))))

      (with-redefs [decode-auth-cookie (constantly [auth-principal (+ now-ms 60000)])]
        (let [auth-cookie-handler (wrap-auth-cookie-handler password request-handler)]
          (is (= {:body {:principal nil :user nil}}
                 (auth-cookie-handler {:headers {"cookie" "x-waiter-auth=test-auth-cookie"}}))))))))

(deftest test-wrap-auth-bypass
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-description-defaults {"concurrency-level" 100
                                      "health-check-url" "/status"}
        metric-group-mappings []
        profile->defaults {"webapp" {"concurrency-level" 120
                                     "fallback-period-secs" 100}
                           "service" {"authentication" "disabled"
                                      "concurrency-level" 30
                                      "fallback-period-secs" 90
                                      "permitted-user" "*"}}
        token-defaults {"fallback-period-secs" 300
                        "https-redirect" false}
        attach-service-defaults-fn #(sd/merge-defaults % service-description-defaults profile->defaults metric-group-mappings)
        attach-token-defaults-fn #(sd/attach-token-defaults % token-defaults profile->defaults)
        waiter-hostnames #{"www.waiter-router.com"}
        handler-response (Object.)
        execute-request (fn execute-request-fn [{:keys [headers] :as in-request}]
                          (let [test-request (->> (sd/discover-service-parameters
                                                    kv-store attach-service-defaults-fn attach-token-defaults-fn waiter-hostnames headers)
                                                  (assoc in-request :waiter-discovery))
                                request-handler-argument-atom (atom nil)
                                test-request-handler (fn request-handler-fn [request]
                                                       (reset! request-handler-argument-atom request)
                                                       handler-response)
                                test-response ((wrap-auth-bypass test-request-handler) test-request)]
                            {:handled-request @request-handler-argument-atom
                             :response test-response}))]

    (kv/store kv-store "www.token-1.com" {"cpus" 1
                                          "mem" 2048})
    (kv/store kv-store "www.token-1p.com" {"cpus" 1
                                           "mem" 2048
                                           "profile" "webapp"})
    (kv/store kv-store "www.token-1s.com" {"cpus" 1
                                           "mem" 2048
                                           "profile" "service"})
    (kv/store kv-store "www.token-2.com" {"authentication" "standard"
                                          "cpus" 1
                                          "mem" 2048})
    (kv/store kv-store "www.token-2s.com" {"authentication" "standard"
                                           "cpus" 1
                                           "mem" 2048
                                           "profile" "service"})
    (kv/store kv-store "www.token-3.com" {"authentication" "disabled"
                                          "cpus" 1
                                          "mem" 2048})
    (kv/store kv-store "a-named-token-A" {"cpus" 1
                                          "mem" 2048})
    (kv/store kv-store "a-named-token-B" {"authentication" "disabled"
                                          "cpus" 1
                                          "mem" 2048})
    (kv/store kv-store "a-named-token-C" {"authentication" "standard"
                                          "cpus" 1
                                          "mem" 2048})

    (testing "request-without-non-existing-hostname-token"
      (let [test-request {:headers {"host" "www.host.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.host.com"}
                                                      :service-description-template {}
                                                      :token "www.host.com"
                                                      :token-metadata token-defaults
                                                      :waiter-headers {}})
               handled-request))
        (is (= handler-response response))))

    (testing "request-without-non-existing-hostname-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.host.com" "x-waiter-run-as-user" "test-user"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.host.com"}
                                                      :service-description-template {}
                                                      :token "www.host.com"
                                                      :token-metadata token-defaults
                                                      :waiter-headers {"x-waiter-run-as-user" "test-user"}})
               handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-non-auth-hostname-token"
      (let [test-request {:headers {"host" "www.token-1.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.token-1.com"}
                                                      :service-description-template {"concurrency-level" 100
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"}
                                                      :token "www.token-1.com"
                                                      :token-metadata (assoc token-defaults "owner" nil "previous" {})
                                                      :waiter-headers {}})
               handled-request))
        (is (= handler-response response)))

      (let [test-request {:headers {"host" "www.token-1p.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.token-1p.com"}
                                                      :service-description-template {"concurrency-level" 120
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"
                                                                                     "profile" "webapp"}
                                                      :token "www.token-1p.com"
                                                      :token-metadata (merge {"owner" nil "previous" {}}
                                                                             token-defaults
                                                                             (select-keys
                                                                               (get profile->defaults "webapp")
                                                                               sd/user-metadata-keys))
                                                      :waiter-headers {}})
               handled-request))
        (is (= handler-response response)))

      (let [test-request {:headers {"host" "www.token-2s.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.token-2s.com"}
                                                      :service-description-template {"authentication" "standard"
                                                                                     "concurrency-level" 30
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"
                                                                                     "permitted-user" "*"
                                                                                     "profile" "service"}
                                                      :token "www.token-2s.com"
                                                      :token-metadata (merge {"owner" nil "previous" {}}
                                                                             token-defaults
                                                                             (select-keys
                                                                               (get profile->defaults "service")
                                                                               sd/user-metadata-keys))
                                                      :waiter-headers {}})
               handled-request))
        (is (= handler-response response)))

      (let [test-request {:headers {"host" "www.token-1.com"
                                    "x-waiter-profile" "service"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/clj->json-response {:error "An authentication disabled token may not be combined with on-the-fly headers"}
                                         :status http-400-bad-request)
               response)))

      (let [test-request {:headers {"host" "www.token-2s.com"
                                    "x-waiter-profile" "service"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.token-2s.com"}
                                                      :service-description-template {"authentication" "standard"
                                                                                     "concurrency-level" 30
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"
                                                                                     "permitted-user" "*"
                                                                                     "profile" "service"}
                                                      :token "www.token-2s.com"
                                                      :token-metadata (merge {"owner" nil "previous" {}}
                                                                             token-defaults
                                                                             (select-keys
                                                                               (get profile->defaults "service")
                                                                               sd/user-metadata-keys))
                                                      :waiter-headers {"x-waiter-profile" "service"}})
               handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-disabled-hostname-token"
      (let [test-request {:headers {"host" "www.token-3.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :skip-authentication true
                                   :waiter-discovery {:passthrough-headers {"host" "www.token-3.com"}
                                                      :service-description-template {"authentication" "disabled"
                                                                                     "concurrency-level" 100
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"}
                                                      :token "www.token-3.com"
                                                      :token-metadata (assoc token-defaults "owner" nil "previous" {})
                                                      :waiter-headers {}})
               handled-request))
        (is (= handler-response response)))

      (let [test-request {:headers {"host" "www.token-1s.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :skip-authentication true
                                   :waiter-discovery {:passthrough-headers {"host" "www.token-1s.com"}
                                                      :service-description-template {"authentication" "disabled"
                                                                                     "concurrency-level" 30
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"
                                                                                     "permitted-user" "*"
                                                                                     "profile" "service"}
                                                      :token "www.token-1s.com"
                                                      :token-metadata (merge {"owner" nil "previous" {}}
                                                                             token-defaults
                                                                             (select-keys
                                                                               (get profile->defaults "service")
                                                                               sd/user-metadata-keys))
                                                      :waiter-headers {}})
               handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-disabled-hostname-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.token-3.com"
                                    "x-waiter-run-as-user" "test-user"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/clj->json-response {:error "An authentication disabled token may not be combined with on-the-fly headers"}
                                         :status http-400-bad-request)
               response))))

    (testing "request-without-existing-non-auth-named-token"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-token" "a-named-token-A"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.service.com"}
                                                      :service-description-template {"concurrency-level" 100
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"}
                                                      :token "a-named-token-A"
                                                      :token-metadata (assoc token-defaults "owner" nil "previous" {})
                                                      :waiter-headers {"x-waiter-token" "a-named-token-A"}})
               handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-non-auth-named-token-with-authentication-header"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-token" "a-named-token-A"
                                    "x-waiter-authentication" "disabled"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/clj->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                         :status http-400-bad-request)
               response))))

    (testing "request-without-existing-auth-disabled-named-token"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-token" "a-named-token-B"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :skip-authentication true
                                   :waiter-discovery {:passthrough-headers {"host" "www.service.com"}
                                                      :service-description-template {"authentication" "disabled"
                                                                                     "concurrency-level" 100
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"}
                                                      :token "a-named-token-B"
                                                      :token-metadata (assoc token-defaults "owner" nil "previous" {})
                                                      :waiter-headers {"x-waiter-token" "a-named-token-B"}})
               handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-disabled-named-token-with-authentication-header"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-authentication" "disabled"
                                    "x-waiter-token" "a-named-token-B"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/clj->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                         :status http-400-bad-request)
               response))))

    (testing "request-without-existing-auth-disabled-named-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-run-as-user" "test-user"
                                    "x-waiter-token" "a-named-token-B"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/clj->json-response {:error "An authentication disabled token may not be combined with on-the-fly headers"}
                                         :status http-400-bad-request)
               response))))

    (testing "request-without-existing-auth-default-named-token"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-token" "a-named-token-C"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.service.com"}
                                                      :service-description-template {"authentication" "standard"
                                                                                     "concurrency-level" 100
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"}
                                                      :token "a-named-token-C"
                                                      :token-metadata (assoc token-defaults "owner" nil "previous" {})
                                                      :waiter-headers {"x-waiter-token" "a-named-token-C"}})
               handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-default-named-token-with-authentication-header"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-authentication" "disabled"
                                    "x-waiter-token" "a-named-token-C"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/clj->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                         :status http-400-bad-request)
               response))))
    (testing "request-without-existing-auth-default-named-token-with-authentication-header-2"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-authentication" "standard"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/clj->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                         :status http-400-bad-request)
               response))))

    (testing "request-without-existing-auth-default-named-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.service.com"
                                    "x-waiter-run-as-user" "test-user"
                                    "x-waiter-token" "a-named-token-C"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :waiter-discovery {:passthrough-headers {"host" "www.service.com"}
                                                      :service-description-template {"authentication" "standard"
                                                                                     "concurrency-level" 100
                                                                                     "cpus" 1
                                                                                     "health-check-url" "/status"
                                                                                     "mem" 2048
                                                                                     "metric-group" "other"}
                                                      :token "a-named-token-C"
                                                      :token-metadata (assoc token-defaults "owner" nil "previous" {})
                                                      :waiter-headers {"x-waiter-run-as-user" "test-user"
                                                                       "x-waiter-token" "a-named-token-C"}})
               handled-request))
        (is (= handler-response response))))))

(deftest test-wrap-auth-bypass-acceptor
  (testing "should 400 if authentication header is sent because it is not supported"
    (let [handler (wrap-auth-bypass-acceptor (fn [_] (is false "Not supposed to call this handler") true))
          upgrade-response (reified-upgrade-response)
          request {:upgrade-response upgrade-response
                   :waiter-discovery {:token-metadata {}
                                      :token "token"
                                      :waiter-headers {"x-waiter-authentication" "value"}}}
          response-status (handler request)]
      (is (= http-400-bad-request response-status))
      (is (= http-400-bad-request (.getStatusCode upgrade-response)))
      (is (str/includes? (.getStatusReason upgrade-response) "An authentication parameter is not supported for on-the-fly headers"))))

  (testing "should 400 if authentication is disabled and combined with on-the-fly headers"
    (let [handler (wrap-auth-bypass-acceptor (fn [_] (is false "Not supposed to call this handler") true))
          upgrade-response (reified-upgrade-response)
          request {:headers {"x-waiter-run-as-user" "test-user"}
                   :upgrade-response upgrade-response
                   :waiter-discovery {:service-description-template {"authentication" "disabled"}
                                      :token-metadata {}
                                      :token "token"
                                      :waiter-headers {"x-waiter-run-as-user" "test-user"}}}
          response-status (handler request)]
      (is (= http-400-bad-request response-status))
      (is (= http-400-bad-request (.getStatusCode upgrade-response)))
      (is (str/includes? (.getStatusReason upgrade-response) "An authentication disabled token may not be combined with on-the-fly headers")))))
