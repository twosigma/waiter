;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.utils-test
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.periodic :as periodic]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [full.async :refer (<?? <? go-try)]
            [waiter.password-store]
            [waiter.test-helpers :refer :all]
            [waiter.utils :refer :all])
  (:import clojure.lang.ExceptionInfo
           java.net.ServerSocket
           java.util.UUID
           org.joda.time.DateTime
           waiter.cors.PatternBasedCorsValidator
           waiter.service_description.DefaultServiceDescriptionBuilder))

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

(deftest test-extract-expired-keys
  (let [current-time (t/now)
        time1 (t/minus current-time (t/millis 2000))
        time2 (t/minus current-time (t/millis 1000))
        time3 current-time
        time4 (t/plus current-time (t/millis 1000))
        time5 (t/plus current-time (t/millis 1400))
        time6 (t/plus current-time (t/millis 1500))
        time7 (t/plus current-time (t/millis 2000))
        test-cases [{:name "two-item-result"
                     :input-map {:a time1, :b time2, :c time3, :d time4, :e time5, :f time7}
                     :time-limit time6
                     :expected [:a :b :c :d :e]}
                    {:name "multiple-item-items"
                     :input-map {:a time1, :b time2, :c time3, :d time4, :e time5, :f time7}
                     :time-limit time3
                     :expected [:a :b :c]}
                    {:name "single-item-result"
                     :input-map {:a time1, :b time2, :c time3, :d time4, :f time7}
                     :time-limit time6
                     :expected [:a :b :c :d]}
                    {:name "empty-result"
                     :input-map {:a time1, :b time2, :c time3}
                     :time-limit time6
                     :expected [:a :b :c]}
                    {:name "nil-item-result"
                     :input-map {:a time1, :b nil, :c time3, :d time4, :f time7}
                     :time-limit time6
                     :expected [:a :c :d]}]]
    (doseq [test-case test-cases]
      (testing (str "Test " (:name test-case))
        (let [{:keys [input-map time-limit expected]} test-case
              actual (extract-expired-keys input-map time-limit)]
          (is (= expected actual)))))))

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
        formatter (f/with-zone (f/formatter "yyyy-MM-dd HH:mm:ss.SSS") (t/default-time-zone))]
    (is (= result (f/unparse formatter input)))))

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

(deftest test-map->json-response
  (testing "Conversion from map to JSON response"

    (testing "should convert empty map"
      (let [{:keys [body headers status]} (map->json-response {})]
        (is (= 200 status))
        (is (= {"content-type" "application/json"} headers))
        (is (not (nil? body)))))

    (testing "should convert regex patterns to strings"
      (is (= (json/write-str {"bar" "foo"}) (:body (map->json-response {:bar #"foo"}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (map->json-response {:bar [#"foo" #"baz"]}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (map->json-response {:bar ["foo" #"baz"]}))))
      (is (= (json/write-str {"bar" [["foo" "baz"]]}) (:body (map->json-response {:bar [["foo" #"baz"]]})))))))

(deftest test-map->streaming-json-response
  (testing "convert empty map"
    (let [{:keys [body headers status]} (map->streaming-json-response {})]
      (is (= 200 status))
      (is (= {"content-type" "application/json"} headers))
      (is (= {} (json/read-str (json-response->str body))))))
  (testing "consumes status argument"
    (let [{:keys [status]} (map->streaming-json-response {} :status 404)]
      (is (= status 404))))
  (testing "converts regex patters to strings"
    (is (= {"foo" ["bar"]} (-> {:foo [#"bar"]}
                               map->streaming-json-response
                               :body
                               json-response->str
                               json/read-str)))))

(deftest test-exception->response
  (let [request {:request-method :get
                 :uri "/path"
                 :host "localhost"}]
    (testing "html response"
      (let [{:keys [body headers status]} 
            (exception->response 
              (ex-info "TestCase Exception" {:status 400})
              (assoc-in request [:headers "accept"] "text/html"))]
        (is (= 400 status))
        (is (= {"content-type" "text/html"} headers))
        (is (str/includes? body "TestCase Exception"))))
    (testing "html response with links"
      (let [{:keys [body headers status]} 
            (exception->response 
              (ex-info "TestCase Exception" {:status 400
                                             :friendly-error-message "See http://localhost/path"})
              (assoc-in request [:headers "accept"] "text/html"))]
        (is (= 400 status))
        (is (= {"content-type" "text/html"} headers))
        (is (str/includes? body "See <a href=\"http://localhost/path\">http://localhost/path</a>"))))
    (testing "plaintext response"
      (let [{:keys [body headers status]}
            (exception->response
              (ex-info "TestCase Exception" {:status 400})
              (assoc-in request [:headers "accept"] "text/plain"))]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (str/includes? body "TestCase Exception"))))
    (testing "json response"
      (let [{:keys [body headers status]}
            (exception->response 
              (ex-info "TestCase Exception" {:status 500})
              (assoc-in request [:headers "accept"] "application/json"))]
        (is (= 500 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "TestCase Exception"))))))

(deftest test-log-and-suppress-when-exception-thrown
  (let [counter-atom (atom 0)
        get-when-positive-fn (fn []
                               (let [cur @counter-atom]
                                 (swap! counter-atom inc)
                                 (if (pos? cur)
                                   cur
                                   (throw (ex-info "Non-positive value found!" {})))))]
    (is (= nil (log-and-suppress-when-exception-thrown "Error message" (get-when-positive-fn))))
    (is (= 1 (log-and-suppress-when-exception-thrown "Error message" (get-when-positive-fn))))
    (is (= 2 (log-and-suppress-when-exception-thrown "Error message" (get-when-positive-fn))))))

(deftest test-atom-cache-get-or-load
  (let [cache (atom (cache/fifo-cache-factory {} :threshold 2))
        counter-atom (atom 0)
        get-fn #(even? (swap! counter-atom inc))]
    (testing "first-new-key"
      (is (false? (atom-cache-get-or-load cache "one" get-fn)))
      (is (= 1 @counter-atom)))
    (testing "cached-key"
      (is (false? (atom-cache-get-or-load cache "one" get-fn)))
      (is (= 1 @counter-atom)))
    (testing "second-new-key"
      (is (true? (atom-cache-get-or-load cache "two" get-fn)))
      (is (= 2 @counter-atom))
      (is (false? (atom-cache-get-or-load cache "one" get-fn)))
      (is (= 2 @counter-atom)))
    (testing "key-eviction"
      (is (false? (atom-cache-get-or-load cache "three" get-fn)))
      (is (= 3 @counter-atom))
      (is (true? (atom-cache-get-or-load cache "two" get-fn)))
      (is (= 3 @counter-atom))
      (is (true? (atom-cache-get-or-load cache "one" get-fn)))
      (is (= 4 @counter-atom))
      (is (false? (atom-cache-get-or-load cache "three" get-fn)))
      (is (= 4 @counter-atom))))
  (testing "get-fn-returns-nil"
    (let [cache (atom (cache/fifo-cache-factory {} :threshold 2))
          counter-atom (atom 0)
          get-fn #(do (swap! counter-atom inc) nil)]
      (is (nil? (atom-cache-get-or-load cache "one" get-fn)))
      (is (= 1 @counter-atom))
      (is (nil? (atom-cache-get-or-load cache "one" get-fn)))
      (is (= 1 @counter-atom))
      (is (nil? (atom-cache-get-or-load cache "two" get-fn)))
      (is (= 2 @counter-atom))
      (is (nil? (atom-cache-get-or-load cache "one" get-fn)))
      (is (= 2 @counter-atom)))))

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
                          :inital-delay-ms 1
                          :max-retries 0}
            actual-result ((retry-strategy retry-config) function)]
        (is (= return-value actual-result))
        (is (= 1 @call-counter-atom))))
    (testing "retry-strategy:multiple-retries-success"
      (let [[call-counter-atom function] (make-call-atom-and-function 4 return-value)
            retry-config {:delay-multiplier 1.0
                          :inital-delay-ms 1
                          :max-retries 10}
            actual-result ((retry-strategy retry-config) function)]
        (is (= return-value actual-result))
        (is (= 5 @call-counter-atom))))
    (testing "retry-strategy:multiple-retries-failure"
      (let [[call-counter-atom function] (make-call-atom-and-function 20 return-value)
            retry-config {:delay-multiplier 1.0
                          :inital-delay-ms 1
                          :max-retries 10}]
        (is (thrown-with-msg? IllegalStateException #"function throws error"
                              ((retry-strategy retry-config) function)))
        (is (= 10 @call-counter-atom))))
    (testing "retry-strategy:multiple-retries-failure-elapsed-time-constant"
      (let [actual-elapsed-time-atom (atom 0)]
        (with-redefs [sleep (fn [time] (swap! actual-elapsed-time-atom + time))]
          (let [[call-counter-atom function] (make-call-atom-and-function 20 return-value)
                retry-config {:delay-multiplier 1.0
                              :inital-delay-ms 10
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
                              :inital-delay-ms 10
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
                              :inital-delay-ms 10
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
                              :inital-delay-ms 10
                              :max-retries 10}
                actual-result ((retry-strategy retry-config) function)]
            (is (= return-value actual-result))
            (is (= 5 @call-counter-atom))
            (let [actual-elapsed-time @actual-elapsed-time-atom
                  expected-elapsed-time (* (reduce + [1 5 25 125]) 10)]
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
    (testing "nil/missing started-at"
      (is (not (older-than? now (t/seconds 1) {})))
      (is (not (older-than? now (t/seconds 1) {:started-at ""}))))
    (is (older-than? now (t/minutes 5) {:started-at (f/unparse (f/formatters :date-time) (t/minus now (t/minutes 6)))}))
    (is (not (older-than? now (t/minutes 5) {:started-at (f/unparse (f/formatters :date-time) (t/minus now (t/minutes 5)))})))))

(deftest test-stringify-elements
  (testing "Converting all leaf elements in a collection to string"

    (testing "should work with and without nesting"
      (is (= '("foo") (stringify-elements :k [#"foo"])))
      (is (= '("foo" "bar") (stringify-elements :k [#"foo" #"bar"])))
      (is (= '("foo" "bar") (stringify-elements :k [#"foo" "bar"])))
      (is (= '(("foo" "bar")) (stringify-elements :k [[#"foo" "bar"]])))
      (is (= '(("foo" "bar") ("baz" "qux")) (stringify-elements :k [[#"foo" "bar"] [#"baz" "qux"]]))))

    (testing "should convert symbols to strings, inlcuding their namespace"
      (is (= "waiter.cors/pattern-based-validator" (stringify-elements :k 'waiter.cors/pattern-based-validator))))))

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

(deftest test-compute-help-required
  (is (= -6 (compute-help-required {"outstanding" 6, "slots-available" 2, "slots-in-use" 10, "slots-offered" 0})))
  (is (= -10 (compute-help-required {"outstanding" 6, "slots-available" 2, "slots-in-use" 14, "slots-offered" 0})))
  (is (= 0 (compute-help-required {"outstanding" 10, "slots-available" 10, "slots-in-use" 0, "slots-offered" 0})))
  (is (= 0 (compute-help-required {"outstanding" 14, "slots-available" 10, "slots-in-use" 4, "slots-offered" 0})))

  (is (= 13 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 10, "slots-offered" 0})))
  (is (= 9 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 14, "slots-offered" 0})))
  (is (= 15 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 0, "slots-offered" 0})))
  (is (= 11 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 4, "slots-offered" 0})))

  (is (= 1 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 10, "slots-offered" 12})))
  (is (= -3 (compute-help-required {"outstanding" 25, "slots-available" 2, "slots-in-use" 14, "slots-offered" 12})))
  (is (= 3 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 0, "slots-offered" 12})))
  (is (= -1 (compute-help-required {"outstanding" 25, "slots-available" 10, "slots-in-use" 4, "slots-offered" 12}))))

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
  ; why we wrote waiter.utils/time-seq
  (testing "periodic-seq throws due to overflow after a large number of iterations"
    (let [every-ten-secs (periodic/periodic-seq (t/now) (t/millis 10000))]
      (is (thrown-with-msg? ArithmeticException #"Multiplication overflows an int" (nth every-ten-secs 1000000))))))

(deftest test-time-seq
  (testing "Generation of a sequence of times"

    (testing "should work for small numbers of iterations"
      (let [start (DateTime. 1000)
            every-milli (time-seq start (t/millis 1))
            every-ten-secs (time-seq start (t/seconds 10))]
        (is (= (DateTime. 1000) (first every-milli)))
        (is (= (DateTime. 1001) (second every-milli)))
        (is (= (DateTime. 1002) (nth every-milli 2)))
        (is (= (map #(DateTime. %) [1000 1001 1002 1003 1004 1005 1006 1007 1008 1009])
               (take 10 every-milli)))
        (is (= (map #(DateTime. %) [1000 11000 21000 31000 41000 51000 61000 71000 81000 91000])
               (take 10 every-ten-secs)))))

    (testing "should work for 52 weeks worth of ten-second intervals"
      (let [now (t/now)
            every-ten-secs (time-seq now (t/millis 10000))]
        (is (true? (t/equal? (t/plus now (t/weeks 52)) (nth every-ten-secs 3144960))))))))

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
      (is (instance? DefaultServiceDescriptionBuilder
                     (create-component {:kind :default
                                        :default {:factory-fn
                                                  'waiter.service-description/->DefaultServiceDescriptionBuilder}}))))

    (testing "should throw when config sub-map is missing"
      (is (thrown-with-msg? ExceptionInfo
                            #"No :factory-fn specified"
                            (create-component {:kind :special}))))

    (testing "should throw when unable to resolve factory-fn"
      (is (thrown-with-msg? ExceptionInfo
                            #"Unable to resolve factory function"
                            (create-component {:kind :x
                                               :x {:factory-fn 'bar}}))))

    (testing "should call use on namespace before attempting to resolve"
      (is (= :bar
             (create-component {:kind :x
                                :x {:factory-fn 'waiter.utils-test-ns/foo}}))))))

(deftest test-port-available?
  (let [port (first (filter port-available? (shuffle (range 10000 11000))))]
    (is (port-available? port))
    (let [ss (ServerSocket. port)]
      (.setReuseAddress ss true)
      (is (false? (port-available? port)))
      (.close ss))
    (is (port-available? port))))

(deftest test-urls->html-links
  (testing "nil"
    (is (= nil (urls->html-links nil))))
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
  (testing "else text/plain"
    (is (= "text/plain" (request->content-type {:headers {"accept" "*/*"}})))
    (is (= "text/plain" (request->content-type {:headers {"accept" ""}})))
    (is (= "text/plain" (request->content-type {})))))
