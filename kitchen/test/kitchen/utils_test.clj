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
(ns kitchen.utils-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [kitchen.utils :as utils]))

(deftest exception->json-response-test
  (testing "Exception -> JSON response"
    (testing "should default to 500 status"
      (is (= 500 (:status (utils/exception->json-response (ex-info "foo" {}))))))))

(deftest parse-positive-int-test
  (testing "can parse int"
    (is (= 0 (utils/parse-positive-int "0" nil)))  
    (is (= 1 (utils/parse-positive-int "1" nil)))  
    (is (= 0 (utils/parse-positive-int "-1" 0)))
    (is (= 0 (utils/parse-positive-int "1.0" 0)))
    (is (= 0 (utils/parse-positive-int "test" 0)))))

(deftest test-map->json-response
  (testing "Conversion from map to JSON response"

    (testing "should convert empty map"
      (let [{:keys [body headers status]} (utils/map->json-response {})]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (not (nil? body)))))

    (testing "should convert regex patterns to strings"
      (is (= (json/write-str {"bar" "foo"}) (:body (utils/map->json-response {:bar #"foo"}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (utils/map->json-response {:bar [#"foo" #"baz"]}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (utils/map->json-response {:bar ["foo" #"baz"]}))))
      (is (= (json/write-str {"bar" [["foo" "baz"]]}) (:body (utils/map->json-response {:bar [["foo" #"baz"]]})))))))
