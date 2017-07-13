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
(ns token-syncer.utils-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [token-syncer.utils :as utils]))

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
              actual-map (utils/truncate (:str input-map) (:len input-map))]
          (is (= expected actual-map)))))))

(deftest exception->json-response-test
  (testing "Exception -> JSON response"
    (testing "should default to 500 status"
      (is (= 500 (:status (utils/exception->json-response (ex-info "foo" {}))))))))

(deftest test-map->json-response
  (testing "Conversion from map to JSON response"

    (testing "should convert empty map"
      (let [{:keys [body headers status]} (utils/map->json-response {})]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (not (nil? body)))))

    (testing "should convert regex patterns to strings"
      (is (= (json/write-str {"bar" "foo"}) (:body (utils/map->json-response {:bar "foo"}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (utils/map->json-response {:bar ["foo" "baz"]}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (utils/map->json-response {:bar ["foo" "baz"]}))))
      (is (= (json/write-str {"bar" [["foo" "baz"]]}) (:body (utils/map->json-response {:bar [["foo" "baz"]]})))))))
