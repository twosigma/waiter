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
(ns waiter.util.utils-test
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.periodic :as periodic]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [full.async :refer [<? <?? go-try]]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.util.cache-utils :refer :all]
            [waiter.util.date-utils :refer :all]
            [waiter.util.utils :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (java.net ServerSocket)
           (java.util UUID)
           (waiter.cors PatternBasedCorsValidator)
           (waiter.service_description DefaultServiceDescriptionBuilder)))

(deftest test-is-uuid?
  (testing "invalid value"
    (is (not (is-uuid? 1))))
  (testing "invalid string"
    (is (not (is-uuid? "abc"))))
  (testing "valid string uuid"
    (is (is-uuid? (str (UUID/randomUUID)))))
  (testing "valid uuid"
    (is (is-uuid? (UUID/randomUUID)))))

(deftest test-select-keys-pred
  (testing "identity pred"
    (let [m {:a 1 :b 2 :c 3}]
      (is (= m (select-keys-pred identity m)))))
  (testing "even pred"
    (let [m {1 :a 2 :b 3 :c 4 :d}]
      (is (= {2 :b 4 :d} (select-keys-pred even? m))))))

(deftest test-keys->nested-map
  (testing "no nesting"
    (let [m {"test" 1 "banana" 2}]
      (is (= m (keys->nested-map m #"-")))))
  (testing "simple nesting"
    (let [m {"this.is.an.example" 1 "this.is.an.example2" 2}
          nm {"this" {"is" {"an" {"example" 1 "example2" 2}}}}]
      (is (= nm (keys->nested-map m #"\."))))))

(deftest test-truncate
  (let [test-cases [{:name "truncate:nil-input"
                     :input-map {:str nil, :len 2}
                     :expected nil}
                    {:name "truncate:int-input"
                     :input-map {:str 1234, :len 2}
                     :expected 1234}
                    {:name "truncate:short-length"
                     :input-map {:str "abcd", :len 2}
                     :expected "abcd"}
                    {:name "truncate:short-input"
                     :input-map {:str "abcd", :len 6}
                     :expected "abcd"}
                    {:name "truncate:long-input"
                     :input-map {:str "abcdefgh", :len 6}
                     :expected "abc..."}]]
    (doseq [test-case test-cases]
      (testing (str "Test " (:name test-case))
        (let [{:keys [input-map expected]} test-case
              actual-map (truncate (:str input-map) (:len input-map))]
          (is (= expected actual-map)))))))

(deftest test-date-to-str
  (let [input (t/now)
        result (date-to-str input)
        formatter (f/with-zone (:date-time f/formatters) t/utc)]
    (is (= result (f/unparse formatter input)))
    (is (t/equal? input (str-to-date result formatter)))))

(deftest non-neg-test
  (is (non-neg? 0))
  (is (non-neg? 0.1))
  (is (non-neg? 1))
  (is (not (non-neg? -1)))
  (is (not (non-neg? -0.1))))

(deftest test-generate-secret-word
  (let [actual (generate-secret-word "src" "dest" [[:cached "pass"]])]
    (is (not= (generate-secret-word "src" "dest" ["pass"]) actual))
    (is (not= (generate-secret-word "dest" "src" [[:cached "pass"]]) actual))
    (is (not (str/includes? actual "src")))
    (is (not (str/includes? actual "dest")))
    (is (not (str/includes? actual "pass")))))

(deftest test-keyword->str
  (is (= "foo" (keyword->str :foo)))
  (is (= "foo-bar" (keyword->str :foo-bar)))
  (is (= "foo/bar" (keyword->str :foo/bar)))
  (is (= "foo.bar/fuu-baz" (keyword->str :foo.bar/fuu-baz))))

(deftest test-clj->json-response
  (testing "Conversion from map to JSON response"

    (testing "should convert empty map"
      (let [{:keys [body headers status]} (clj->json-response {})]
        (is (= http-200-ok status))
        (is (= expected-json-response-headers headers))
        (is (not (nil? body)))))

    (testing "should convert regex patterns to strings"
      (is (= (json/write-str {"bar" "foo"}) (:body (clj->json-response {:bar #"foo"}))))
      (is (= (json/write-str {"bar" ["foo" "baz"] "foo/bar" "baz"} :escape-slash false)
             (:body (clj->json-response {:bar [#"foo" #"baz"] :foo/bar :baz}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (clj->json-response {:bar ["foo" #"baz"]}))))
      (is (= (json/write-str {"bar" [["foo" "baz"]]}) (:body (clj->json-response {:bar [["foo" #"baz"]]})))))))

(deftest test-clj->streaming-json-response
  (testing "convert empty map"
    (let [{:keys [body headers status]} (clj->streaming-json-response {})]
      (is (= http-200-ok status))
      (is (= expected-json-response-headers headers))
      (is (= {} (json/read-str (json-response->str body))))))
  (testing "consumes status argument"
    (let [{:keys [status]} (clj->streaming-json-response {} :status http-404-not-found)]
      (is (= status http-404-not-found))))
  (testing "converts regex patters to strings"
    (is (= {"foo" ["bar"]}
           (-> {:foo [#"bar"]}
               clj->streaming-json-response
               :body
               json-response->str
               json/read-str))))
  (testing "converts namespaced keywords"
    (is (= {"foo/bar" "fuu/baz"}
           (-> {:foo/bar :fuu/baz}
               clj->streaming-json-response
               :body
               json-response->str
               json/read-str)))))

(defrecord TestResponse [status friendly-error-message])

(deftest test-exception->response
  (let [request {:request-method :get
                 :uri "/path"
                 :host "localhost"}]
    (testing "html response"
      (let [{:keys [body headers status]}
            (exception->response
              (ex-info "TestCase Exception" (map->TestResponse {:status http-400-bad-request}))
              (assoc-in request [:headers "accept"] "text/html"))]
        (is (= http-400-bad-request status))
        (is (= expected-html-response-headers headers))
        (is (str/includes? body "TestCase Exception"))))
    (testing "html response with links"
      (let [{:keys [body headers status]}
            (exception->response
              (ex-info "TestCase Exception" (map->TestResponse {:status http-400-bad-request
                                                                :friendly-error-message "See http://localhost/path"}))
              (assoc-in request [:headers "accept"] "text/html"))]
        (is (= http-400-bad-request status))
        (is (= expected-html-response-headers headers))
        (is (str/includes? body "See <a href=\"http://localhost/path\">http://localhost/path</a>"))))
    (testing "plaintext response"
      (let [{:keys [body headers status]}
            (exception->response
              (ex-info "TestCase Exception" (map->TestResponse {:status http-400-bad-request}))
              (assoc-in request [:headers "accept"] "text/plain"))]
        (is (= http-400-bad-request status))
        (is (= expected-text-response-headers headers))
        (is (str/includes? body "TestCase Exception"))))
    (testing "json response"
      (let [{:keys [body headers status]}
            (exception->response
              (ex-info "TestCase Exception" (map->TestResponse {:status http-500-internal-server-error}))
              (assoc-in request [:headers "accept"] "application/json"))]
        (is (= http-500-internal-server-error status))
        (is (= expected-json-response-headers headers))
        (is (str/includes? body "TestCase Exception"))))))

(deftest test-log-and-suppress-when-exception-thrown
  (let [counter-atom (atom 0)
        get-when-positive-fn (fn []
                               (let [cur @counter-atom]
                                 (swap! counter-atom inc)
                                 (if (pos? cur)
                                   cur
                                   (throw (ex-info "Non-positive value found!" {})))))]
    (is (nil? (log-and-suppress-when-exception-thrown "Error message" (get-when-positive-fn))))
    (is (= 1 (log-and-suppress-when-exception-thrown "Error message" (get-when-positive-fn))))
    (is (= 2 (log-and-suppress-when-exception-thrown "Error message" (get-when-positive-fn))))))

(deftest test-atom-cache-get-or-load
  (let [cache (cache-factory {:threshold 2})]
    (testing "first-new-key"
      (is (= 1 (cache-get-or-load cache "one" (constantly 1))))
      (is (cache-contains? cache "one"))
      (is (= 1 (cache-size cache))))

    (testing "cached-key"
      (is (= 1 (cache-get-or-load cache "one" (constantly 10))))
      (is (cache-contains? cache "one"))
      (is (= 1 (cache-size cache))))

    (testing "second-new-key"
      (is (= 2 (cache-get-or-load cache "two" (constantly 2))))
      (is (= 1 (cache-get-or-load cache "one" (constantly 10))))
      (is (cache-contains? cache "two"))
      (is (cache-contains? cache "one"))
      (is (= 2 (cache-size cache))))

    (testing "key-eviction"
      (is (= 3 (cache-get-or-load cache "three" (constantly 3))))
      (is (not (cache-contains? cache "two")))
      (is (cache-contains? cache "one"))
      (is (cache-contains? cache "three"))
      (is (= 2 (cache-size cache)))

      (is (= 20 (cache-get-or-load cache "two" (constantly 20))))
      (is (not (cache-contains? cache "one")))
      (is (cache-contains? cache "three"))
      (is (cache-contains? cache "two"))
      (is (= 2 (cache-size cache)))

      (is (= 10 (cache-get-or-load cache "one" (constantly 10))))
      (is (= 30 (cache-get-or-load cache "three" (constantly 30))))
      (is (not (cache-contains? cache "two")))
      (is (cache-contains? cache "one"))
      (is (cache-contains? cache "three"))
      (is (= 2 (cache-size cache)))))

  (testing "cache ttl eviction"
    (let [cache (cache-factory {:threshold 2 :ttl 100})]
      (is (= 10 (cache-get-or-load cache "one" (constantly 10))))
      (is (= 10 (cache-get-or-load cache "one" (constantly 11))))
      (is (= 1 (cache-size cache)))
      (Thread/sleep 110)
      (is (= 12 (cache-get-or-load cache "one" (constantly 12))))
      (is (= 1 (cache-size cache)))))

  (testing "get-fn-returns-nil"
    (let [cache (cache-factory {:threshold 2})]
      (is (nil? (cache-get-or-load cache "one" (constantly nil))))
      (is (nil? (cache-get-or-load cache "two" (constantly nil))))
      (is (cache-contains? cache "one"))
      (is (cache-contains? cache "two"))
      (is (= 2 (cache-size cache))))))

(deftest test-retry-strategy
  (let [make-call-atom-and-function (fn [num-failures return-value]
                                      (let [call-counter-atom (atom 0)
                                            function (fn []
                                                       (swap! call-counter-atom inc)
                                                       (when (<= @call-counter-atom num-failures)
                                                         (throw (IllegalStateException. "function throws error")))
                                                       return-value)]
                                        [call-counter-atom function]))
        return-value {:function-result true}]
    (testing "retry-strategy:no-retries"
      (let [[call-counter-atom function] (make-call-atom-and-function 0 return-value)
            retry-config {:delay-multiplier 1.0
                          :initial-delay-ms 1
                          :max-retries 0}
            actual-result ((retry-strategy retry-config) function)]
        (is (= return-value actual-result))
        (is (= 1 @call-counter-atom))))
    (testing "retry-strategy:multiple-retries-success"
      (let [[call-counter-atom function] (make-call-atom-and-function 4 return-value)
            retry-config {:delay-multiplier 1.0
                          :initial-delay-ms 1
                          :max-retries 10}
            actual-result ((retry-strategy retry-config) function)]
        (is (= return-value actual-result))
        (is (= 5 @call-counter-atom))))
    (testing "retry-strategy:multiple-retries-failure"
      (let [[call-counter-atom function] (make-call-atom-and-function 20 return-value)
            retry-config {:delay-multiplier 1.0
                          :initial-delay-ms 1
                          :max-retries 10}]
        (is (thrown-with-msg? IllegalStateException #"function throws error"
                              ((retry-strategy retry-config) function)))
        (is (= 10 @call-counter-atom))))
    (testing "retry-strategy:multiple-retries-failure-elapsed-time-constant"
      (let [actual-elapsed-time-atom (atom 0)]
        (with-redefs [sleep (fn [time] (swap! actual-elapsed-time-atom + time))]
          (let [[call-counter-atom function] (make-call-atom-and-function 20 return-value)
                retry-config {:delay-multiplier 1.0
                              :initial-delay-ms 10
                              :max-retries 5}]
            (is (thrown-with-msg? IllegalStateException #"function throws error"
                                  ((retry-strategy retry-config) function)))
            (is (= 5 @call-counter-atom))
            (let [actual-elapsed-time @actual-elapsed-time-atom
                  expected-elapsed-time (* (dec 5) 10)]
              (is (= expected-elapsed-time actual-elapsed-time)))))))
    (testing "retry-strategy:multiple-retries-failure-elapsed-time-exponential"
      (let [actual-elapsed-time-atom (atom 0)]
        (with-redefs [sleep (fn [time] (swap! actual-elapsed-time-atom + time))]
          (let [[call-counter-atom function] (make-call-atom-and-function 20 return-value)
                retry-config {:delay-multiplier 2
                              :initial-delay-ms 10
                              :max-retries 5}]
            (is (thrown-with-msg? IllegalStateException #"function throws error"
                                  ((retry-strategy retry-config) function)))
            (is (= 5 @call-counter-atom))
            (let [actual-elapsed-time @actual-elapsed-time-atom
                  expected-elapsed-time (* (reduce + [1 2 4 8]) 10)]
              (is (= expected-elapsed-time actual-elapsed-time)))))))
    (testing "retry-strategy:multiple-retries-success-elapsed-time-exponential-2"
      (let [actual-elapsed-time-atom (atom 0)]
        (with-redefs [sleep (fn [time] (swap! actual-elapsed-time-atom + time))]
          (let [[call-counter-atom function] (make-call-atom-and-function 8 return-value)
                retry-config {:delay-multiplier 2
                              :initial-delay-ms 10
                              :max-retries 10}
                actual-result ((retry-strategy retry-config) function)]
            (is (= return-value actual-result))
            (is (= 9 @call-counter-atom))
            (let [actual-elapsed-time @actual-elapsed-time-atom
                  expected-elapsed-time (* (reduce + [1 2 4 8 16 32 64 128]) 10)]
              (is (= expected-elapsed-time actual-elapsed-time)))))))
    (testing "retry-strategy:multiple-retries-success-elapsed-time-exponential-5"
      (let [actual-elapsed-time-atom (atom 0)]
        (with-redefs [sleep (fn [time] (swap! actual-elapsed-time-atom + time))]
          (let [[call-counter-atom function] (make-call-atom-and-function 4 return-value)
                retry-config {:delay-multiplier 5
                              :initial-delay-ms 10
                              :max-retries 10}
                actual-result ((retry-strategy retry-config) function)]
            (is (= return-value actual-result))
            (is (= 5 @call-counter-atom))
            (let [actual-elapsed-time @actual-elapsed-time-atom
                  expected-elapsed-time (* (reduce + [1 5 25 125]) 10)]
              (is (= expected-elapsed-time actual-elapsed-time)))))))
    (testing "retry-strategy:max-delay-ms"
      (let [actual-elapsed-time-atom (atom 0)]
        (with-redefs [sleep (fn [time] (swap! actual-elapsed-time-atom + time))]
          (let [[call-counter-atom function] (make-call-atom-and-function 5 return-value)
                retry-config {:delay-multiplier 5
                              :initial-delay-ms 10
                              :max-delay-ms 100
                              :max-retries 10}
                actual-result ((retry-strategy retry-config) function)]
            (is (= return-value actual-result))
            (is (= 6 @call-counter-atom))
            (let [actual-elapsed-time @actual-elapsed-time-atom
                  expected-elapsed-time (reduce + [10 50 100 100 100])]
              (is (= expected-elapsed-time actual-elapsed-time)))))))))

(deftest test-unique-identifier
  (testing "unique-identifier:test-uniqueness-in-100s-calls-in-parallel"
    (let [id-store-atom (atom #{})
          generate-id-fn #(swap! id-store-atom conj (unique-identifier))
          num-threads 30
          calls-per-thread 200
          threads (map
                    (fn [_]
                      (async/thread
                        (dotimes [_ calls-per-thread]
                          (generate-id-fn))))
                    (range num-threads))]
      (doseq [thread threads]
        (async/<!! thread))
      (is (= (* num-threads calls-per-thread) (count @id-store-atom))))))

(deftest test-older-than
  (let [now (t/now)]
    (testing "nil duration returns false"
      (is (not (older-than? now nil {:started-at now}))))
    (testing "nil started-at"
      (is (not (older-than? now (t/seconds 1) {}))))
    (is (older-than? now (t/minutes 5) {:started-at (t/minus now (t/minutes 6))}))
    (is (not (older-than? now (t/minutes 5) {:started-at (t/minus now (t/minutes 5))})))))

(deftest test-stringify-elements
  (testing "Converting all leaf elements in a collection to string"

    (testing "should work with and without nesting"
      (is (= '("foo") (stringify-elements :k [#"foo"])))
      (is (= '("foo" "bar") (stringify-elements :k [#"foo" #"bar"])))
      (is (= '("foo" "bar") (stringify-elements :k [#"foo" "bar"])))
      (is (= '(("foo" "bar")) (stringify-elements :k [[#"foo" "bar"]])))
      (is (= '(("foo" "bar") ("baz" "qux")) (stringify-elements :k [[#"foo" "bar"] [#"baz" "qux"]]))))

    (testing "should convert symbols to strings, inlcuding their namespace"
      (is (= "waiter.cors/pattern-based-validator" (stringify-elements :k 'waiter.cors/pattern-based-validator))))

    (testing "NaN and Infinity"
      (is (= "NaN" (stringify-elements :k Double/NaN)))
      (is (= "Infinity" (stringify-elements :k Double/POSITIVE_INFINITY)))
      (is (= "-Infinity" (stringify-elements :k Double/NEGATIVE_INFINITY)))
      (is (= "NaN" (stringify-elements :k Float/NaN)))
      (is (= "Infinity" (stringify-elements :k Float/POSITIVE_INFINITY)))
      (is (= "-Infinity" (stringify-elements :k Float/NEGATIVE_INFINITY))))))

(deftest test-deep-sort-map
  (let [deep-seq (fn [data] (walk/postwalk #(if (or (map? %) (seq? %)) (seq %) %) data))]
    (testing "deep-sort-map"
      (is (= {} (deep-sort-map nil)))
      (is (= {} (deep-sort-map {})))
      (is (= {:a 1} (deep-sort-map {:a 1})))
      (is (= {"a" 1} (deep-sort-map {"a" 1})))
      (is (= [["a" "b"] ["c" "d"] ["f" "g"]]
             (deep-seq (deep-sort-map {"a" "b", "f" "g", "c" "d"}))))
      (is (= [["a" "b"] ["f" [["e" "d"] ["g" "c"]]]]
             (deep-seq (deep-sort-map {"f" {"g" "c", "e" "d"}, "a" "b"}))))
      (is (= [["a" [["a" [["a" 1] ["b" "2"]]] ["f" [["e" "d"] ["g" "c"]]]]]
              ["f" [["e" "d"] ["g" "c"]]]]
             (deep-seq (deep-sort-map {"a" {"a" {"b" "2", "a" 1}, "f" {"g" "c" "e" "d"}}
                                       "f" {"g" "c", "e" "d"}})))))))

(deftest test-deep-merge-maps
  (let [merge-fn (fn [x y] (or x y))]
    (testing "deep-merge-map"
      (is (nil? (deep-merge-maps merge-fn nil nil)))
      (is (= {} (deep-merge-maps merge-fn {} {})))
      (is (= {:a 1} (deep-merge-maps merge-fn {:a 1} {:a 2})))
      (is (= {:a {:b {:c 1, :d 2}}, :e 1, :f 1}
             (deep-merge-maps merge-fn {:a {:b {:c 1, :d 2}}, :e 1} {:a {:b {:c 4}}, :f 1})))
      (is (= {:a {:b 1}} (deep-merge-maps merge-fn {:a {:b 1}} {:a 2}))))))

(deftest test-compression-decompression-clojure-data-maps
  (let [password [:cached "compression-key"]
        compress-and-decompress (fn [data-map] (-> data-map
                                                   (map->compressed-bytes password)
                                                   (compressed-bytes->map password)))]
    (testing "nil-map"
      (is (nil? (compress-and-decompress nil))))
    (testing "empty-map"
      (is (= {} (compress-and-decompress {}))))
    (testing "string-data-map"
      (is (= {"a" "b", "c" "d", "e" ["f" "g" "h"]}
             (compress-and-decompress {"a" "b", "c" "d", "e" ["f" "g" "h"]}))))
    (testing "string-and:keywords-data-map"
      (is (= {"a" :b, :c :d, :e [:f "g" "h"]}
             (compress-and-decompress {"a" :b, :c :d, :e [:f "g" "h"]}))))
    (testing "numbers-in-nested-data-map"
      (is (= {"a" {"b" 3, :d "e", "f" :g, :h {:i {:j "k", "l" 1.3}, :n [15 16 17]}}}
             (compress-and-decompress {"a" {"b" 3, :d "e", "f" :g, :h {:i {:j "k", "l" 1.3}, :n [15 16 17]}}})))))
  (let [encryption-password [:cached "encryption-password"]
        decryption-password [:cached "decryption-password"]
        compress-and-decompress (fn [data-map] (-> data-map
                                                   (map->compressed-bytes encryption-password)
                                                   (compressed-bytes->map decryption-password)))]
    (testing "incorrect-decryption-key"
      (is (thrown-with-msg? Exception #"Thaw failed"
                            (compress-and-decompress {"a" :b, :c :d, :e [:f "g" "h"]}))))))

(deftest test-request-flag
  (is (not (request-flag {} "foo")))
  (is (not (request-flag {"bar" 1} "foo")))
  (is (not (request-flag {"bar" 1} "bar")))
  (is (not (request-flag {"bar" "1"} "bar")))
  (is (not (request-flag {"bar" "false"} "bar")))
  (is (request-flag {"bar" "true"} "bar"))
  (is (request-flag {"bar" true} "bar")))

(deftest test-periodic-seq
  ; If this test fails after an upgrade to clj-time, we can switch back to using periodic-seq;
  ; in the meantime, this serves as a nice demonstration of the issue with periodic-seq and
  ; why we wrote waiter.util.date-utils/time-seq
  (testing "periodic-seq throws due to overflow after a large number of iterations"
    (let [every-ten-secs (periodic/periodic-seq (t/now) (t/millis 10000))]
      (is (thrown-with-msg? ArithmeticException #"Multiplication overflows an int" (nth every-ten-secs 1000000))))))

(deftest test-authority->host
  (is (nil? (authority->host nil)))
  (is (= "www.example.com" (authority->host "www.example.com")))
  (is (= "www.example.com" (authority->host "www.example.com:1234")))
  (is (= "www.example2.com" (authority->host "www.example2.com:80"))))

(deftest test-authority->port
  (is (= "" (authority->port nil)))
  (is (= "" (authority->port "www.example.com")))
  (is (= "8080" (authority->port "www.example.com" :default 8080)))
  (is (= "1234" (authority->port "www.example.com:1234")))
  (is (= "80" (authority->port "www.example2.com:80"))))

(deftest test-request->scheme
  (is (= :http (request->scheme {:headers {"x-forwarded-proto" "http"}})))
  (is (= :http (request->scheme {:headers {"x-forwarded-proto" "HTTP"}})))
  (is (= :http (request->scheme {:headers {"x-forwarded-proto" "http"} :scheme :http})))
  (is (= :http (request->scheme {:headers {"x-forwarded-proto" "http"} :scheme :https})))
  (is (= :http (request->scheme {:scheme :http})))

  (is (= :https (request->scheme {:headers {"x-forwarded-proto" "https"}})))
  (is (= :https (request->scheme {:headers {"x-forwarded-proto" "HTTPS"}})))
  (is (= :https (request->scheme {:headers {"x-forwarded-proto" "https"} :scheme :http})))
  (is (= :https (request->scheme {:headers {"x-forwarded-proto" "https"} :scheme :https})))
  (is (= :https (request->scheme {:scheme :https}))))

(deftest test-same-origin
  (is (not (same-origin nil)))
  (is (not (same-origin {})))
  (is (not (same-origin {:headers {}})))
  (is (not (same-origin {:headers {"host" "www.example.com", "origin" "http://www.example.com"}})))
  (is (not (same-origin {:headers {"origin" "http://www.example.com"}, :scheme :http})))
  (is (not (same-origin {:headers {"host" "www.example.com"}, :scheme :http})))
  (is (same-origin {:headers {"host" "www.example.com", "origin" "http://www.example.com"}, :scheme :http}))
  (is (not (same-origin {:headers {"host" "www.example.com", "origin" "http://www.example.com"}, :scheme :https})))
  (is (not (same-origin {:headers {"host" "www.example.com", "origin" "https://www.example.com"}, :scheme :http})))
  (is (same-origin {:headers {"host" "www.example.com", "origin" "http://www.example.com", "x-forwarded-proto" "http"}, :scheme :https}))
  (is (not (same-origin {:headers {"host" "www.example.com", "origin" "https://www.example.com", "x-forwarded-proto" "http"}, :scheme :https})))
  (is (not (same-origin {:headers {"host" "www.example.com", "origin" "http://www.example.com", "x-forwarded-proto" "https"}, :scheme :https}))))

(deftest test-create-component
  (testing "Creating a component"

    (testing "should support specifying a custom :kind"
      (is (= {:factory-fn 'identity, :bar 1, :baz 2}
             (create-component {:kind :foo, :foo {:factory-fn 'identity, :bar 1, :baz 2}})))
      (is (= {:factory-fn 'identity, :bar 1, :baz 2}
             (create-component {:kind :marathon, :marathon {:factory-fn 'identity, :bar 1, :baz 2}})))
      (is (instance? PatternBasedCorsValidator
                     (create-component {:kind :patterns
                                        :patterns {:factory-fn 'waiter.cors/pattern-based-validator
                                                   :allowed-origins []}})))
      (let [constraints {"cpus" {:max 100}
                         "mem" {:max (* 32 1024)}}
            builder (create-component {:kind :default
                                       :default {:factory-fn 'waiter.service-description/create-default-service-description-builder}}
                                      :context {:constraints constraints})]
        (is (instance? DefaultServiceDescriptionBuilder builder))
        (is (:max-constraints-schema builder))
        (waiter.service-description/validate builder {} {})))

    (testing "should throw when config sub-map is missing"
      (is (thrown-with-msg? ExceptionInfo
                            #"No :factory-fn specified"
                            (create-component {:kind :special}))))

    (testing "should throw when unable to resolve factory-fn"
      (is (thrown-with-msg? ExceptionInfo
                            #"Unable to resolve factory function"
                            (create-component {:kind :x
                                               :x {:factory-fn 'bar}}))))

    (let [test-component {:kind :x
                          :x {:factory-fn 'waiter.util.utils-test-ns/foo}}]
      (testing "should call use on namespace before attempting to resolve"
        (is (= :bar (create-component test-component))))

      (testing "create-component calls should be repeatable"
        (is (= :bar (create-component test-component)))))))

(deftest test-port-available?
  (let [port (first (filter port-available? (shuffle (range 10000 11000))))]
    (is (port-available? port))
    (let [ss (ServerSocket. port)]
      (.setReuseAddress ss true)
      (is (false? (port-available? port)))
      (.close ss))
    (is (port-available? port))))

(deftest test-escape-html
  (testing "nil"
    (is (nil? (escape-html nil))))
  (testing "script tag"
    (is (= "&lt;script&gt;&lt;/script&gt;"
           (escape-html "<script></script>"))))
  (testing "quotes"
    (is (= "&quot;&quot;&quot;hello world&quot;"
           (escape-html "\"\"\"hello world\""))))
  (testing "ampersand"
    (is (= "&amp;&amp;&amp;hello world"
           (escape-html "&&&hello world"))))
  (testing "combination of quotes, ampersands, script tags, and letters"
    (is (= "&amp;&amp;&gt;&lt;&lt;&amp;&gt;a&amp;&amp;&lt;b&quot;&lt;&lt;baa&amp;&lt;"
          (escape-html "&&><<&>a&&<b\"<<baa&<")))))

(deftest test-urls->html-links
  (testing "nil"
    (is (nil? (urls->html-links nil))))
  (testing "http"
    (is (= "<a href=\"http://localhost\">http://localhost</a>"
           (urls->html-links "http://localhost"))))
  (testing "with path"
    (is (= "<a href=\"http://localhost/path\">http://localhost/path</a>"
           (urls->html-links "http://localhost/path"))))
  (testing "https"
    (is (= "<a href=\"https://localhost/path\">https://localhost/path</a>"
           (urls->html-links "https://localhost/path"))))
  (testing "mixed content"
    (is (= "hello <a href=\"https://localhost/path\">https://localhost/path</a> world"
           (urls->html-links "hello https://localhost/path world")))))

(deftest test-request->content-type
  (testing "application/json if specified"
    (is (= "application/json" (request->content-type {:headers {"accept" "application/json"}}))))
  (testing "text/html if specified"
    (is (= "text/html" (request->content-type {:headers {"accept" "text/html"}}))))
  (testing "text/plain if specified"
    (is (= "text/plain" (request->content-type {:headers {"accept" "text/plain"}}))))
  (testing "application/json if content-type is application/json"
    (is (= "application/json" (request->content-type {:headers {"content-type" "application/json"}}))))
  (testing "else text/plain"
    (is (= "text/plain" (request->content-type {:headers {"accept" "*/*"}})))
    (is (= "text/plain" (request->content-type {:headers {"accept" ""}})))
    (is (= "text/plain" (request->content-type {})))))

(deftest test-merge-by
  (let [merge-fn (fn [k v1 v2]
                   (cond
                     (= :m k) (* v1 v2)
                     :else (+ v1 v2)))]
    (is (= {:a 4, :b 6, :c 5, :d 6}
           (merge-by merge-fn {:a 1, :b 2} {:a 3} {:b 4} {:c 5} {:d 6})))
    (is (= {:a 4, :b 6, :m 5}
           (merge-by merge-fn {:a 1, :b 2} {:a 3} {:b 4} {:m 5})))
    (is (= {:a 4, :b 2, :m 20}
           (merge-by merge-fn {:a 1, :b 2} {:a 3, :m 4} {:m 5})))))

(deftest test-update-exception
  (is (= {:a 1 :b 2} (ex-data (update-exception (ex-info "test" {:a 1}) #(assoc % :b 2)))))
  (is (= {:b 2} (ex-data (update-exception (RuntimeException. "test") #(assoc % :b 2))))))

(deftest test-add-grpc-headers-and-trailers
  (let [make-response (fn make-response
                        [status]
                        {:status status
                         :waiter/response-source :waiter})
        load-trailers (fn load-trailers [{:keys [trailers] :as response}]
                        (cond-> response
                          trailers (update :trailers async/<!!)))
        attach-grpc-errors (fn attach-grpc-errors [response grpc-status grpc-message]
                             (-> response
                               load-trailers
                               (update :headers assoc
                                       "content-type" "application/grpc"
                                       "grpc-message" grpc-message
                                       "grpc-status" (str grpc-status))
                               (update :trailers assoc
                                       "grpc-message" grpc-message
                                       "grpc-status" (str grpc-status))))]
    (let [response (make-response http-200-ok)]
      (is (= response (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-301-moved-permanently)]
      (is (= response (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-400-bad-request)]
      (is (= (attach-grpc-errors response grpc-3-invalid-argument "Bad Request")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-401-unauthorized)]
      (is (= (attach-grpc-errors response grpc-16-unauthenticated "Unauthorized")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-403-forbidden)]
      (is (= (attach-grpc-errors response grpc-7-permission-denied "Permission Denied")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-429-too-many-requests)]
      (is (= (attach-grpc-errors response grpc-14-unavailable "Too Many Requests")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-500-internal-server-error)]
      (is (= (attach-grpc-errors response grpc-13-internal "Internal Server Error")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-502-bad-gateway)]
      (is (= (attach-grpc-errors response grpc-14-unavailable "Bad Gateway")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-503-service-unavailable)]
      (is (= (attach-grpc-errors response grpc-14-unavailable "Service Unavailable")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))
    (let [response (make-response http-504-gateway-timeout)]
      (is (= (attach-grpc-errors response grpc-4-deadline-exceeded "Gateway Timeout")
             (-> response (add-grpc-headers-and-trailers {}) load-trailers))))))

(deftest test-attach-grpc-status
  (let [standard-response {:body "hello" :status http-200-ok}]
    (with-redefs [add-grpc-headers-and-trailers (fn [response _]
                                                  (assoc response :add-grpc-headers-and-trailers true))]
      (testing "non-grpc request"
        (is (= standard-response (attach-grpc-status standard-response {} {:headers {}})))
        (is (= standard-response (attach-grpc-status standard-response {} {:headers {"content-type" "application/xml"}})))
        (is (= standard-response (attach-grpc-status standard-response {} {:client-protocol "HTTP/1.1"
                                                                           :headers {"content-type" "application/grpc"}}))))

      (testing "grpc request"
        (is (= (assoc standard-response :add-grpc-headers-and-trailers true)
               (attach-grpc-status standard-response {} {:client-protocol "HTTP/2.0"
                                                         :headers {"content-type" "application/grpc"}})))))))

(deftest test-remove-keys
  (is (nil? (remove-keys nil nil)))
  (is (nil? (remove-keys nil [])))
  (is (nil? (remove-keys nil [:a])))
  (is (nil? (remove-keys nil [:a :b])))

  (is (= {} (remove-keys {} nil)))
  (is (= {} (remove-keys {} [])))
  (is (= {} (remove-keys {} [:a])))
  (is (= {} (remove-keys {} [:a :b])))

  (is (= {:a 1} (remove-keys {:a 1} nil)))
  (is (= {:a 1} (remove-keys {:a 1} [])))
  (is (= {} (remove-keys {:a 1} [:a])))
  (is (= {} (remove-keys {:a 1} [:a :b])))

  (is (= {:a 1 :b 2} (remove-keys {:a 1 :b 2} nil)))
  (is (= {:a 1 :b 2} (remove-keys {:a 1 :b 2} [])))
  (is (= {:b 2} (remove-keys {:a 1 :b 2} [:a])))
  (is (= {} (remove-keys {:a 1 :b 2} [:a :b])))
  (is (= {} (remove-keys {:a 1 :b 2} [:a :b :c])))
  (is (= {} (remove-keys {:a 1 :b 2} [:a :b :c :d])))

  (is (= {:b 2 :c 3} (remove-keys {:b 2 :c 3} nil)))
  (is (= {:b 2 :c 3} (remove-keys {:b 2 :c 3} [])))
  (is (= {:b 2 :c 3} (remove-keys {:b 2 :c 3} [:a])))
  (is (= {:b 2 :c 3} (remove-keys {:b 2 :c 3} [:a :d :e :f :g])))
  (is (= {:c 3} (remove-keys {:b 2 :c 3} [:a :b]))))
