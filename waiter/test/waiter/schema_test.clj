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
(ns waiter.schema-test
  (:use [clojure.test])
  (:require [clojure.string :as string]
            [schema.core :as s]
            [waiter.schema :refer :all]))

(deftest positive-int-test
  (s/validate positive-int 5)
  (is (thrown? Exception (s/validate positive-int -5)))
  (is (thrown? Exception (s/validate positive-int 0))))

(deftest positive-fraction-less-than-1-test
  (s/validate positive-fraction-less-than-1 0.1)
  (is (thrown? Exception (s/validate positive-fraction-less-than-1 0)))
  (is (thrown? Exception (s/validate positive-fraction-less-than-1 1)))
  (is (thrown? Exception (s/validate positive-fraction-less-than-1 -1))))

(deftest non-empty-string-test
  (s/validate non-empty-string "test")
  (is (thrown? Exception (s/validate non-empty-string "")))
  (is (thrown? Exception (s/validate non-empty-string nil))))

(deftest valid-string-length-test
  (s/validate valid-string-length "abc")
  (is (thrown? Exception (s/validate valid-string-length 12)))
  (is (thrown? Exception (s/validate valid-string-length "")))
  (is (thrown? Exception (s/validate valid-string-length (string/join (take 2000 (repeat "A")))))))

(deftest positive-fraction-less-than-or-equal-to-1-test
  (s/validate positive-fraction-less-than-or-equal-to-1 0.1)
  (is (thrown? Exception (s/validate positive-fraction-less-than-or-equal-to-1 0)))
  (s/validate positive-fraction-less-than-or-equal-to-1 1) 
  (is (thrown? Exception (s/validate positive-fraction-less-than-or-equal-to-1 -1))))

(deftest greater-than-or-equal-to-0-less-than-1-test
  (s/validate greater-than-or-equal-to-0-less-than-1 0.1)
  (s/validate greater-than-or-equal-to-0-less-than-1 0) 
  (is (thrown? Exception (s/validate greater-than-or-equal-to-0-less-than-1 1))) 
  (is (thrown? Exception (s/validate greater-than-or-equal-to-0-less-than-1 -1))))

(deftest valid-metric-group-test
  (s/validate valid-metric-group "ab")
  (s/validate valid-metric-group "ab1")
  (s/validate valid-metric-group "ab1-foo")
  (s/validate valid-metric-group "bar-ab1-foo")
  (s/validate valid-metric-group "baz_qux")
  (s/validate valid-metric-group "abcdefghijklmnopqrstuvwxyz123456")
  (s/validate valid-metric-group "mixed-separators_are-allowed")
  (s/validate valid-metric-group "a_b_c_d_e_f_g_h_i_j_k_l_m_n_o_p")
  (s/validate valid-metric-group "other")
  (is (thrown? Exception (s/validate valid-metric-group "")))
  (is (thrown? Exception (s/validate valid-metric-group "a")))
  (is (thrown? Exception (s/validate valid-metric-group "1noleadingnumbers")))
  (is (thrown? Exception (s/validate valid-metric-group "no.dots")))
  (is (thrown? Exception (s/validate valid-metric-group "no__double__underscores")))
  (is (thrown? Exception (s/validate valid-metric-group "no--double--dashes")))
  (is (thrown? Exception (s/validate valid-metric-group "noCAPITALLETTERS")))
  (is (thrown? Exception (s/validate valid-metric-group "abcdefghijklmnopqrstuvwxyz1234567")))
  (is (thrown? Exception (s/validate valid-metric-group "_no_leading_underscores")))
  (is (thrown? Exception (s/validate valid-metric-group "-no-leading-dashes")))
  (is (thrown? Exception (s/validate valid-metric-group "no_trailing_underscores_")))
  (is (thrown? Exception (s/validate valid-metric-group "no-trailing-dashes-"))))

(deftest test-valid-metric-group-mappings
  (s/validate valid-metric-group-mappings [])
  (s/validate valid-metric-group-mappings [[#"foo" "bar"]])
  (s/validate valid-metric-group-mappings [[#"foo" "bar"] [#"baz" "qux"]])
  (is (thrown? Exception (s/validate valid-metric-group-mappings [[]])))
  (is (thrown? Exception (s/validate valid-metric-group-mappings [["foo" "bar"]])))
  (is (thrown? Exception (s/validate valid-metric-group-mappings [[#"foo" "bar" "baz"]]))))

(deftest test-valid-zookeeper-connect-config
  (s/validate valid-zookeeper-connect-config "a")
  (s/validate valid-zookeeper-connect-config "foo,bar,baz")
  (s/validate valid-zookeeper-connect-config :in-process)
  (is (thrown? Exception (s/validate valid-zookeeper-connect-config nil)))
  (is (thrown? Exception (s/validate valid-zookeeper-connect-config "")))
  (is (thrown? Exception (s/validate valid-zookeeper-connect-config :foo)))
  (is (thrown? Exception (s/validate valid-zookeeper-connect-config :in-processs))))
