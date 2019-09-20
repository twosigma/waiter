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
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.core :as core]
            [waiter.cors :refer :all]
            [waiter.schema :as schema])
  (:import waiter.cors.PatternBasedCorsValidator))

(defn create-request-with-origin
  ([origin] (create-request-with-origin origin nil))
  ([origin access-control-request-method] (create-request-with-origin origin access-control-request-method nil))
  ([origin access-control-request-method access-control-request-headers]
   {:headers (cond-> {}
               origin (assoc "origin" origin)
               access-control-request-method (assoc "access-control-request-method" access-control-request-method)
               access-control-request-headers (assoc "access-control-request-headers" access-control-request-headers))}))

(deftest pattern-validator-test
  (let [validator (pattern-based-validator {:allowed-origins [#"^http://[^\.]+\.example\.org(:80)?$"
                                                              #"^https://anotherapp.example.org:12345$"]})]
    (is (= {:allowed? true :summary {:pattern-based-validator [:origin-present :origin-different :pattern-matched]}}
           (preflight-check validator (create-request-with-origin "http://myapp.example.org"))))
    (is (= {:allowed? true :summary {:pattern-based-validator [:origin-present :origin-different :pattern-matched]}}
           (preflight-check validator (create-request-with-origin "http://myapp.example.org:80"))))
    (is (= {:allowed? true :summary {:pattern-based-validator [:origin-present :origin-different :pattern-matched]}}
           (preflight-check validator (create-request-with-origin "https://anotherapp.example.org:12345"))))
    (is (= {:allowed? false :summary {:pattern-based-validator [:origin-present :origin-different :pattern-not-matched]}}
           (preflight-check validator (create-request-with-origin "http://anotherapp.example.org:12345"))))
    (is (= {:allowed? false :summary {:pattern-based-validator [:origin-present :origin-different :pattern-not-matched]}}
           (preflight-check validator (create-request-with-origin "http://anotherapp.example.org:12346"))))
    (is (= {:allowed? false :summary {:pattern-based-validator [:origin-present :origin-different :pattern-not-matched]}}
           (preflight-check validator (create-request-with-origin "http://myapp.baddomain.com"))))
    (is (= {:allowed? false :summary {:pattern-based-validator [:origin-present :origin-different :pattern-not-matched]}}
           (preflight-check validator (create-request-with-origin "http://myapp.baddomain.com:8080"))))
    (is (= {:allowed? true :summary {:pattern-based-validator [:origin-present :origin-same]}}
           (request-check validator {:headers {"origin" "http://example.com"
                                               "host" "example.com"}
                                     :scheme :http})))
    (is (= {:allowed? false :summary {:pattern-based-validator [:origin-present :origin-different :pattern-not-matched]}}
           (request-check validator {:headers {"origin" "http://bad.example.com"
                                               "host" "bad.example.com"}
                                     :scheme :https})))
    (is (= {:allowed? false :summary {:pattern-based-validator [:origin-present :origin-different :pattern-not-matched]}}
           (request-check validator {:headers {"origin" "http://bad.example.com"
                                               "host" "good.example.com"}
                                     :scheme :http})))))

(deftest test-pattern-based-validator
  (is (thrown? Throwable (pattern-based-validator {})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins nil})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins ["foo"]})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins [#"foo" "bar"]})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins [#"foo" #"bar" "baz"]})))
  (is (instance? PatternBasedCorsValidator (pattern-based-validator {:allowed-origins [#"foo" #"bar" #"baz"]}))))

(let [validator (token-parameter-based-validator {})
      create-request-with-origin (fn [origin path scheme method] {:headers {"origin" origin}
                                                                  :request-method method
                                                                  :scheme scheme
                                                                  :uri path
                                                                  :waiter-discovery
                                                                  {:token-metadata
                                                                   {"cors-rules"
                                                                    [; 0
                                                                     {"origin-regex" ".*origin-regex0\\.com"}
                                                                     ; 1
                                                                     {"origin-regex" "https://origin-regex1\\.com"}
                                                                     ; 2
                                                                     {"origin-regex" "https://origin-regex2\\.com"
                                                                      "target-path-regex" "/target/path"}
                                                                     ; 3
                                                                     {"origin-regex" "https://origin-regex3\\.com"
                                                                      "target-path-regex" "/target/path"}
                                                                     ; 4
                                                                     {"origin-regex" "https://origin-regex4\\.com"
                                                                      "target-path-regex" "/target/path"
                                                                      "methods" ["OPTIONS" "POST"]}]}}})]

  (deftest test-token-parameter-based-validator
    (is (= {:allowed? true :summary {:token-parameter-based-validator [:origin-present :origin-same]}}
           (preflight-check validator {:headers {"origin" "http://example.com" "host" "example.com"}
                                       :scheme :http})))
    (is (= {:allowed? true :summary {:token-parameter-based-validator [:origin-present :origin-same]}}
           (request-check validator {:headers {"origin" "http://example.com" "host" "example.com"}
                                     :scheme :http})))
    (is (= {:allowed? false :summary {:token-parameter-based-validator [:origin-present :origin-different :no-rule-matched]}}
           (preflight-check validator {:headers {"origin" "https://example.com" "host" "example.com"}
                                       :scheme :http})))
    (is (= {:allowed? false :summary {:token-parameter-based-validator [:origin-present :origin-different :no-rule-matched]}}
           (request-check validator {:headers {"origin" "https://example.com" "host" "example.com"}
                                     :scheme :http}))))

  (defn- check-cors-match
    ([origin matched-rule-index]
     (check-cors-match origin "" :http :get matched-rule-index nil))
    ([origin path matched-rule-index]
     (check-cors-match origin path :http :get matched-rule-index nil))
    ([origin path scheme matched-rule-index]
     (check-cors-match origin path scheme :get matched-rule-index nil))
    ([origin path scheme method matched-rule-index allowed-methods]
     (is (= {:allowed? true :summary {:token-parameter-based-validator [:origin-present :origin-different :rule-matched (keyword (str "rule-" matched-rule-index "-matched"))]} :allowed-methods allowed-methods}
            (preflight-check validator (create-request-with-origin origin path scheme method))))
     (is (= {:allowed? true :summary {:token-parameter-based-validator [:origin-present :origin-different :rule-matched (keyword (str "rule-" matched-rule-index "-matched"))]} :allowed-methods allowed-methods}
            (request-check validator (create-request-with-origin origin path scheme method))))))

  (deftest test-token-parameter-based-validator-match
    (check-cors-match "http://origin-regex0.com" 0)
    (check-cors-match "https://origin-regex0.com" 0)
    (check-cors-match "https://origin-regex1.com" 1)
    (check-cors-match "https://origin-regex2.com" "/target/path" 2)
    (check-cors-match "https://origin-regex3.com" "/target/path" :https 3)
    (check-cors-match "https://origin-regex4.com" "/target/path" :https :post 4 ["OPTIONS" "POST"]))

  (defn- check-cors-no-match
    ([origin]
     (check-cors-no-match origin "" :http :get))
    ([origin path]
     (check-cors-no-match origin path :http :get))
    ([origin path scheme]
     (check-cors-no-match origin path scheme :get))
    ([origin path scheme method]
     (is (= {:allowed? false :summary {:token-parameter-based-validator [:origin-present :origin-different :no-rule-matched]}}
            (preflight-check validator (create-request-with-origin origin path scheme method))))
     (is (= {:allowed? false :summary {:token-parameter-based-validator [:origin-present :origin-different :no-rule-matched]}}
            (request-check validator (create-request-with-origin origin path scheme method))))))

  (deftest test-token-parameter-based-validator-no-match
    (check-cors-no-match "http://origin-regexx.com")
    (check-cors-no-match "http://origin-regex1.com")
    (check-cors-no-match "https://origin-regex2.com")
    (check-cors-no-match "https://origin-regex4.com" "/target/path" :https))

  (deftest test-token-parameter-access-control-allow-methods
    (is (= nil
           (seq (:allowed-methods (preflight-check validator (create-request-with-origin "http://origin-regex0.com" "" :http :get))))))
    (is (= (seq ["OPTIONS" "POST"])
           (seq (:allowed-methods (preflight-check validator (create-request-with-origin "https://origin-regex4.com" "/target/path" :https :post))))))))

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

(deftest test-preflight-request?
  (is (preflight-request? {:headers {"access-control-request-headers" "x-test-header"
                                     "access-control-request-method" "DELETE"
                                     "origin" "doesnt.matter"}
                           :request-method :options}))
  (is (not (preflight-request? {:headers {"access-control-request-headers" "x-test-header"
                                          "access-control-request-method" "DELETE"
                                          "origin" "doesnt.matter"}
                                :request-method :get})))
  (is (not (preflight-request? {:headers {"access-control-request-method" "DELETE"
                                          "origin" "doesnt.matter"}
                                :request-method :options})))
  (is (not (preflight-request? {:headers {"access-control-request-headers" "x-test-header"
                                          "origin" "doesnt.matter"}
                                :request-method :options})))
  (is (not (preflight-request? {:headers {"access-control-request-headers" "x-test-header"
                                          "access-control-request-method" "DELETE"}
                                :request-method :options}))))

(deftest test-wrap-cors-preflight
  (testing "cors preflight request denied"
    (let [deny-all (deny-all-validator {})
          max-age 100
          request (assoc (create-request-with-origin "doesnt.matter" "DELETE" "x-test-header")
                    :request-method :options)
          handler (-> (fn [_] {:status 200})
                    (wrap-cors-preflight deny-all max-age nil nil nil)
                    (core/wrap-error-handling))
          {:keys [status]} (handler request)]
      (is (= 403 status))))

  (testing "cors preflight request allowed"
    (let [allow-all (allow-all-validator {})
          max-age 100
          request (assoc (create-request-with-origin "doesnt.matter" "DELETE" "x-test-header")
                    :request-method :options)
          handler (wrap-cors-preflight (fn [_] {:status 200}) allow-all max-age nil nil nil)
          {:keys [headers status]} (handler request)]
      (is (= 200 status))
      (is (= "doesnt.matter" (get headers "access-control-allow-origin")))
      (is (= "x-test-header" (get headers "access-control-allow-headers")))
      (is (= (str/join ", " schema/http-methods) (get headers "access-control-allow-methods")))
      (is (= (str max-age) (get headers "access-control-max-age")))
      (is (= "true" (get headers "access-control-allow-credentials"))))))
