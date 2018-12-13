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
(ns waiter.correlation-id-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [full.async :refer (<?? <? go-try)]
            [waiter.correlation-id :refer :all])
  (:import java.util.UUID
           (org.apache.log4j Appender Category ConsoleAppender EnhancedPatternLayout Logger PatternLayout Priority SimpleLayout)
           org.apache.log4j.spi.LoggingEvent))

(deftest test-http-object->correlation-id
  (let [test-cases [{:name "http-object->correlation-id:nil-input",
                     :input nil,
                     :expected nil}
                    {:name "http-object->correlation-id:nil-headers",
                     :input {:headers nil},
                     :expected nil}
                    {:name "http-object->correlation-id:missing-cid-headers",
                     :input {:headers {:foo :bar}},
                     :expected nil}
                    {:name "http-object->correlation-id:invalid-case-cid-headers",
                     :input {:headers {:foo :bar, (str/upper-case HEADER-CORRELATION-ID) "baz"}},
                     :expected nil}
                    {:name "http-object->correlation-id:valid-cid-headers",
                     :input {:headers {:foo :bar, (str/lower-case HEADER-CORRELATION-ID) "baz"}},
                     :expected "baz"}]]
    (doseq [test-case test-cases]
      (let [{:keys [name input expected]} test-case]
        (testing (str "Test " name)
          (let [actual-value (http-object->correlation-id input)]
            (is (= expected actual-value))))))))

(deftest test-ensure-correlation-id
  (let [test-cases [{:name "ensure-correlation-id:nil-headers",
                     :input {:headers nil}}
                    {:name "ensure-correlation-id:missing-cid-headers",
                     :input {:headers {:foo :bar}}}
                    {:name "ensure-correlation-id:valid-cid-headers",
                     :input {:headers {:foo :bar, HEADER-CORRELATION-ID "baz"}}}]]
    (doseq [test-case test-cases]
      (let [{:keys [name input]} test-case]
        (testing (str "Test " name)
          (let [modified-request (ensure-correlation-id input #(str (UUID/randomUUID)))]
            (is (not-empty (http-object->correlation-id modified-request)))))))))

(deftest test-with-correlation-id
  (testing "Test with-correlation-id"
    (with-correlation-id
      "foo"
      (do
        (is (= "foo" dynamic-correlation-id))
        (is (= "foo" (get-correlation-id)))
        (with-correlation-id
          "bar"
          (do
            (is (= "bar" dynamic-correlation-id))
            (is (= "bar" (get-correlation-id)))))
        (is (= "foo" dynamic-correlation-id))
        (is (= "foo" (get-correlation-id)))))))

(deftest test-replace-pattern-layout-in-log4j-appenders
  []
  (let [format "Format String for [CID] testing"
        appender1 (ConsoleAppender. (PatternLayout. format))
        appender2 (ConsoleAppender. (SimpleLayout.))
        appender3 (ConsoleAppender. (EnhancedPatternLayout. format))
        root-logger (Logger/getRootLogger)
        log (fn [^Appender appender]
              (.format
                (.getLayout appender)
                (LoggingEvent. "category"
                               (Category/getInstance "category-name")
                               Priority/INFO
                               "message"
                               nil)))
        simple-layout-format (log appender2)]
    ; setup
    (.addAppender root-logger appender1)
    (.addAppender root-logger appender2)
    (.addAppender root-logger appender3)
    (with-out-str
      (is (= format (log appender1)))
      (is (not= format simple-layout-format))
      (is (= format (log appender3))))
    ; make call
    (replace-pattern-layout-in-log4j-appenders)
    ; assertions
    (with-correlation-id
      "test-cid"
      (let [expected-log (str/replace format "CID" "CID=test-cid")]
        (is (= expected-log (log appender1)))
        (is (= simple-layout-format (log appender2)))
        (is (= expected-log (log appender3)))))
    ; cleanup
    (.removeAppender root-logger appender1)
    (.removeAppender root-logger appender2)
    (.removeAppender root-logger appender3)))
