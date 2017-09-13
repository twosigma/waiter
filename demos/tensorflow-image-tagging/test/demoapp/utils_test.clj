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
(ns demoapp.utils-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [demoapp.utils :as utils]))

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
      (is (= (json/write-str {"bar" "foo"}) (:body (utils/map->json-response {:bar #"foo"}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (utils/map->json-response {:bar [#"foo" #"baz"]}))))
      (is (= (json/write-str {"bar" ["foo" "baz"]}) (:body (utils/map->json-response {:bar ["foo" #"baz"]}))))
      (is (= (json/write-str {"bar" [["foo" "baz"]]}) (:body (utils/map->json-response {:bar [["foo" #"baz"]]})))))))
