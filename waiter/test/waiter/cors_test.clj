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
    (is (preflight-allowed? validator (create-request-with-origin "http://myapp.example.org")))
    (is (preflight-allowed? validator (create-request-with-origin "http://myapp.example.org:80")))
    (is (preflight-allowed? validator (create-request-with-origin "https://anotherapp.example.org:12345")))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://anotherapp.example.org:12345"))))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://anotherapp.example.org:12346"))))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://myapp.baddomain.com"))))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://myapp.baddomain.com:8080"))))
    (is (request-allowed? validator {:headers {"origin" "http://example.com"
                                               "host" "example.com"}
                                     :scheme :http}))
    (is (not (request-allowed? validator {:headers {"origin" "http://bad.example.com"
                                                    "host" "bad.example.com"}
                                          :scheme :https})))
    (is (not (request-allowed? validator {:headers {"origin" "http://bad.example.com"
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
  (testing "cors request denied"
    (let [deny-all (deny-all-validator {})
          request {:headers {"origin" "doesnt.matter"}}
          handler (-> (fn [request] {:status 200})
                      (wrap-cors-request deny-all)
                      (core/wrap-error-handling))
          {:keys [status] :as response} (handler request)]
      (is (= 403 status))))
  (testing "cors request allowed"
    (let [allow-all (allow-all-validator {})
          request {:headers {"origin" "doesnt.matter"}}
          handler (-> (fn [request] {:status 200})
                      (wrap-cors-request allow-all))
          {:keys [headers status] :as response} (handler request)]
      (is (= 200 status))
      (is (= "doesnt.matter" (get headers "Access-Control-Allow-Origin")))
      (is (= "true" (get headers "Access-Control-Allow-Credentials"))))))

(deftest test-wrap-cors-preflight
  (testing "cors preflight request denied"
    (let [deny-all (deny-all-validator {})
          max-age 100
          request {:request-method :options}
          handler (-> (fn [request] {:status 200})
                      (wrap-cors-preflight deny-all max-age)
                      (core/wrap-error-handling))
          {:keys [status] :as response} (handler request)]
      (is (= 403 status))))
  (testing "cors preflight request allowed"
    (let [allow-all (allow-all-validator {})
          max-age 100
          request {:headers {"origin" "doesnt.matter"
                             "access-control-request-headers" "x-test-header"}
                   :request-method :options}
          handler (-> (fn [request] {:status 200})
                      (wrap-cors-preflight allow-all max-age))
          {:keys [headers status] :as response} (handler request)]
      (is (= 200 status))
      (is (= "doesnt.matter" (get headers "Access-Control-Allow-Origin")))
      (is (= "x-test-header" (get headers "Access-Control-Allow-Headers")))
      (is (= "POST, GET, OPTIONS, DELETE" (get headers "Access-Control-Allow-Methods")))
      (is (= (str max-age) (get headers "Access-Control-Max-Age")))
      (is (= "true" (get headers "Access-Control-Allow-Credentials"))))))
