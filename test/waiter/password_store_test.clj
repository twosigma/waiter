;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
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