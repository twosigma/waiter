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
;;;; This is a dummy test that exists only so that it can be run in the Jenkinsfile in order to force-load the
;;;; dependencies that parallel-test requires in order to run.

(ns waiter.dummy-test
  (:require [clojure.test :refer :all]))


(deftest test-addition
         (is (= 7 (+ 3 4))))
