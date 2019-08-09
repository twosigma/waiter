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
(ns waiter.cors-test
  (:require [clojure.test :refer :all]
            [waiter.core :as core]
            [waiter.cors :refer :all])
  (:import waiter.cors.PatternBasedCorsValidator))

(deftest pattern-validator-test
  (let [validator (pattern-based-validator {:allowed-origins [#"^http://[^\.]+\.example\.org(:80)?$"
                                                              #"^https://anotherapp.example.org:12345$"]})
        create-request-with-origin (fn [origin] {:headers {"origin" origin}})]
    (is (= {:result true :summary [:origin-present :origin-different :pattern-matched]}
           (preflight-allowed? validator (create-request-with-origin "http://myapp.example.org"))))
    (is (= {:result true :summary [:origin-present :origin-different :pattern-matched]}
           (preflight-allowed? validator (create-request-with-origin "http://myapp.example.org:80"))))
    (is (= {:result true :summary [:origin-present :origin-different :pattern-matched]}
           (preflight-allowed? validator (create-request-with-origin "https://anotherapp.example.org:12345"))))
    (is (= {:result false :summary [:origin-present :origin-different :pattern-not-matched]}
           (preflight-allowed? validator (create-request-with-origin "http://anotherapp.example.org:12345"))))
    (is (= {:result false :summary [:origin-present :origin-different :pattern-not-matched]}
           (preflight-allowed? validator (create-request-with-origin "http://anotherapp.example.org:12346"))))
    (is (= {:result false :summary [:origin-present :origin-different :pattern-not-matched]}
           (preflight-allowed? validator (create-request-with-origin "http://myapp.baddomain.com"))))
    (is (= {:result false :summary [:origin-present :origin-different :pattern-not-matched]}
           (preflight-allowed? validator (create-request-with-origin "http://myapp.baddomain.com:8080"))))
    (is (= {:result true :summary [:origin-present :origin-same]}
           (request-allowed? validator {:headers {"origin" "http://example.com"
                                                  "host" "example.com"}
                                        :scheme :http})))
    (is (= {:result false :summary [:origin-present :origin-different :pattern-not-matched]}
           (request-allowed? validator {:headers {"origin" "http://bad.example.com"
                                                  "host" "bad.example.com"}
                                        :scheme :https})))
    (is (= {:result false :summary [:origin-present :origin-different :pattern-not-matched]}
           (request-allowed? validator {:headers {"origin" "http://bad.example.com"
                                                  "host" "good.example.com"}
                                        :scheme :http})))))

(deftest test-pattern-based-validator
  (is (thrown? Throwable (pattern-based-validator {})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins nil})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins ["foo"]})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins [#"foo" "bar"]})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins [#"foo" #"bar" "baz"]})))
  (is (instance? PatternBasedCorsValidator (pattern-based-validator {:allowed-origins [#"foo" #"bar" #"baz"]}))))

(deftest test-wrap-cors-request
  (let [waiter-request? (constantly false)
        exposed-headers []]
    (testing "cors request denied"
      (let [deny-all (deny-all-validator {})
            request {:headers {"origin" "doesnt.matter"}}
            handler (-> (fn [_] {:status 200})
                      (wrap-cors-request deny-all waiter-request? exposed-headers)
                      (core/wrap-error-handling))
            {:keys [status]} (handler request)]
        (is (= 403 status))))

    (testing "cors request allowed"
      (let [allow-all (allow-all-validator {})
            request {:headers {"origin" "doesnt.matter"}}
            handler (wrap-cors-request (fn [_] {:status 200}) allow-all waiter-request? exposed-headers)
            {:keys [headers status]} (handler request)]
        (is (= 200 status))
        (is (= "doesnt.matter" (get headers "access-control-allow-origin")))
        (is (= "true" (get headers "access-control-allow-credentials")))
        (is (nil? (get headers "access-control-expose-headers")))))

    (testing "cors request allowed to waiter api with exposed headers configured"
      (let [waiter-request? (constantly true)
            exposed-headers ["foo" "bar"]
            allow-all (allow-all-validator {})
            request {:headers {"origin" "doesnt.matter"}}
            handler (wrap-cors-request (fn [_] {:status 200}) allow-all waiter-request? exposed-headers)
            {:keys [headers status]} (handler request)]
        (is (= 200 status))
        (is (= "doesnt.matter" (get headers "access-control-allow-origin")))
        (is (= "true" (get headers "access-control-allow-credentials")))
        (is (= "foo, bar" (get headers "access-control-expose-headers")))))

    (testing "non-cors request allowed to waiter api with exposed headers configured"
      (let [waiter-request? (constantly true)
            exposed-headers ["foo" "bar"]
            allow-all (allow-all-validator {})
            request {:headers {"host" "does.matter"
                               "origin" "http://does.matter"}
                     :scheme "http"}
            handler (wrap-cors-request (fn [_] {:status 200}) allow-all waiter-request? exposed-headers)
            {:keys [headers status]} (handler request)]
        (is (= 200 status))
        (is (= "http://does.matter" (get headers "access-control-allow-origin")))
        (is (= "true" (get headers "access-control-allow-credentials")))
        (is (nil? (get headers "access-control-expose-headers")))))

    (testing "cors request allowed to waiter api without exposed headers configured"
      (let [waiter-request? (constantly true)
            exposed-headers []
            allow-all (allow-all-validator {})
            request {:headers {"origin" "doesnt.matter"}}
            handler (wrap-cors-request (fn [_] {:status 200}) allow-all waiter-request? exposed-headers)
            {:keys [headers status]} (handler request)]
        (is (= 200 status))
        (is (= "doesnt.matter" (get headers "access-control-allow-origin")))
        (is (= "true" (get headers "access-control-allow-credentials")))
        (is (nil? (get headers "access-control-expose-headers")))))))

(deftest test-wrap-cors-preflight
  (testing "cors preflight request denied"
    (let [deny-all (deny-all-validator {})
          max-age 100
          request {:request-method :options}
          handler (-> (fn [_] {:status 200})
                    (wrap-cors-preflight deny-all max-age)
                    (core/wrap-error-handling))
          {:keys [status]} (handler request)]
      (is (= 403 status))))
  (testing "cors preflight request allowed"
    (let [allow-all (allow-all-validator {})
          max-age 100
          request {:headers {"origin" "doesnt.matter"
                             "access-control-request-headers" "x-test-header"}
                   :request-method :options}
          handler (wrap-cors-preflight (fn [_] {:status 200}) allow-all max-age)
          {:keys [headers status]} (handler request)]
      (is (= 200 status))
      (is (= "doesnt.matter" (get headers "access-control-allow-origin")))
      (is (= "x-test-header" (get headers "access-control-allow-headers")))
      (is (= "POST, GET, OPTIONS, DELETE" (get headers "access-control-allow-methods")))
      (is (= (str max-age) (get headers "access-control-max-age")))
      (is (= "true" (get headers "access-control-allow-credentials"))))))
