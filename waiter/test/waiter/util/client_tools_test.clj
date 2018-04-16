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
(ns waiter.util.client-tools-test
  (:require [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]))

(deftest test-parse-set-cookie-string
  (is (= (list {:name "name" :value "value"}) (parse-set-cookie-string "name=value")))
  (is (= (list {:name "name" :value "value"}) (parse-set-cookie-string "name=value;Path=/")))
  (is (= nil (parse-set-cookie-string nil))))

(deftest test-parse-cookies
  (is (= [{:name "name" :value "value"}] (parse-cookies "name=value")))
  (is (= [{:name "name" :value "value"} {:name "name2" :value "value2"}] (parse-cookies ["name=value" "name2=value2"]))))
