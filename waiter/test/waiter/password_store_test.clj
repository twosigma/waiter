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
(ns waiter.password-store-test
  (:require [clojure.test :refer :all]
            [waiter.password-store :refer :all]))

(deftest test-configured-password-provider
  (let [configured-passwords ["2-password" "3-password" "1-password"]
        expected-result configured-passwords
        password-provider (->ConfiguredPasswordProvider configured-passwords)]
    (testing "Test test-configured-password-provider"
      (is (= expected-result
             (retrieve-passwords password-provider))))))

(deftest test-check-passwords
  (let [test-cases (list
                     {:name "test-check-passwords:empty-vec-of-passwords"
                      :input []
                      :expected true
                      },
                     {:name "test-check-passwords:one-empty-password"
                      :input [""]
                      :expected true
                      },
                     {:name "test-check-passwords:multiple-empty-passwords"
                      :input ["" "" ""]
                      :expected true
                      },
                     {:name "test-check-passwords:some-empty-password"
                      :input ["foo" "" "bar"]
                      :expected true
                      },
                     {:name "test-check-passwords:one-non-empty-password"
                      :input ["foo"]
                      :expected false
                      },
                     {:name "test-check-passwords:multiple-non-empty-passwords"
                      :input ["foo" "bar" "baz"]
                      :expected false
                      })]
    (doseq [{:keys [name input expected]} test-cases]
      (testing (str "Test " name)
        (let [flag (atom false)
              callback #(reset! flag true)]
          (check-empty-passwords input callback)
          (is (= expected @flag)))))))

(deftest test-configured-provider
  (let [provider (configured-provider {:passwords ["open-sesame"]})]
    (is (= ["open-sesame"]
           (retrieve-passwords provider)))))
