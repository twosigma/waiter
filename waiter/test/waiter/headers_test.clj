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
(ns waiter.headers-test
  (:require [clojure.test :refer :all]
            [waiter.headers :refer :all]
            [waiter.token :as token]
            [waiter.util.utils :as utils]))

(deftest test-parse-header-value

  (testing "parse-header-value:name"
    (is (= "my-name" (parse-header-value "x-waiter-name" "my-name"))))

  (testing "parse-header-value:cmd"
    (is (= "my-command" (parse-header-value "x-waiter-cmd" "my-command"))))

  (testing "parse-header-value:version"
    (is (= "my-version" (parse-header-value "x-waiter-version" "my-version"))))

  (testing "parse-header-value:endpoint-path"
    (is (= "/ep-path" (parse-header-value "x-waiter-endpoint-path" "/ep-path"))))

  (testing "parse-header-value:health-check-url"
    (is (= "/hc-path" (parse-header-value "x-waiter-health-check-url" "/hc-path"))))

  (testing "parse-header-value:any-permitted-user"
    (is (= token/ANY-USER (parse-header-value "x-waiter-permitted-user" token/ANY-USER))))

  (testing "parse-header-value:specific-permitted-user"
    (is (= "testuser" (parse-header-value "x-waiter-permitted-user" "testuser"))))

  (testing "parse-header-value:specific-run-as-user"
    (is (= "testuser" (parse-header-value "x-waiter-run-as-user" "testuser"))))

  (testing "parse-header-value:token"
    (is (= "JLgw1jg81Melpev3gXtL3COyATKrqZKj" (parse-header-value "x-waiter-token" "JLgw1jg81Melpev3gXtL3COyATKrqZKj"))))

  (testing "parse-header-value:json-boolean"
    (is (= true (parse-header-value "x-waiter-json-boolean" "true"))))

  (testing "parse-header-value:json-int"
    (is (= 123 (parse-header-value "x-waiter-json-int" "123"))))

  (testing "parse-header-value:random-string"
    (is (= "abcd" (parse-header-value "x-waiter-json-string" "abcd"))))

  (testing "parse-header-value:metadata-string"
    (is (= "bar" (parse-header-value "x-waiter-metadata-foo" "bar"))))

  (testing "parse-header-value:metadata-null"
    (is (= "null" (parse-header-value "x-waiter-metadata-foo" "null"))))

  (testing "parse-header-value:metadata-int"
    (is (= "1" (parse-header-value "x-waiter-metadata-foo" "1"))))

  (testing "parse-header-value:env-string"
    (is (= "bar" (parse-header-value "x-waiter-env-foo" "bar"))))

  (testing "parse-header-value:env-null"
    (is (= "null" (parse-header-value "x-waiter-env-foo-bar" "null"))))

  (testing "parse-header-value:env-int"
    (is (= "1" (parse-header-value "x-waiter-env-foo_bar" "1"))))

  (testing "parse-header-value:metric-group"
    (is (= "foo" (parse-header-value "x-waiter-metric-group" "foo"))))

  (testing "parse-header-value:metric-group-is-not-json"
    (is (= "true" (parse-header-value "x-waiter-metric-group" "true")))))

(deftest test-contains-waiter-header
  (let [test-cases (list
                     {:name "contains-waiter-header:search-header-exists"
                      :waiter-headers {(str waiter-header-prefix "name") "my-name"}
                      :search-keys ["name"]
                      :expected true
                      }
                     {:name "contains-waiter-header:search-header-exists-3"
                      :waiter-headers {(str waiter-header-prefix "version") "my-version"
                                       (str waiter-header-prefix "cpus") 1
                                       (str waiter-header-prefix "mem") 1024
                                       (str waiter-header-prefix "name") "my-name"}
                      :search-keys ["name"]
                      :expected true
                      }
                     {:name "contains-waiter-header:search-header-exists-2"
                      :waiter-headers {(str waiter-header-prefix "version") "my-version"
                                       (str waiter-header-prefix "cpus") 1
                                       (str waiter-header-prefix "mem") 1024
                                       (str waiter-header-prefix "name") "my-name"}
                      :search-keys ["cpus"]
                      :expected true
                      }
                     {:name "contains-waiter-header:search-header-missing"
                      :waiter-headers {"x-waiter-name" "my-name"}
                      :search-keys ["cmd"]
                      :expected nil
                      }
                     {:name "contains-waiter-header:search-header-missing-2"
                      :waiter-headers {(str waiter-header-prefix "version") "my-version"
                                       (str waiter-header-prefix "cpus") 1
                                       (str waiter-header-prefix "mem") 1024
                                       (str waiter-header-prefix "name") "my-name"}
                      :search-keys ["cmd"]
                      :expected nil
                      }
                     {:name "contains-waiter-header:waiter-headers-empty"
                      :waiter-headers {}
                      :search-keys ["cmd"]
                      :expected nil
                      }
                     )]
    (doseq [{:keys [name waiter-headers search-keys expected]} test-cases]
      (testing (str "Test " name)
        (is (= expected (contains-waiter-header waiter-headers search-keys)))))))

(deftest test-truncate-header-values
  (testing "Truncating header values"

    (testing "should truncate to 80 characters by default"
      (let [value (apply str (repeat 30 "foo"))]
        (is (= 90 (count value)))
        (is (= {"some-header" (utils/truncate value 80)} (truncate-header-values {"some-header" value})))))

    (testing "should not truncate x-waiter-token"
      (let [token (apply str (repeat 30 "foo"))]
        (is (= 90 (count token)))
        (is (= {"x-waiter-token" token} (truncate-header-values {"x-waiter-token" token})))))))
