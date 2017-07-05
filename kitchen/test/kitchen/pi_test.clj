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
(ns kitchen.pi-test
  (:require [clojure.test :refer :all]
            [kitchen.pi :as pi]))

(deftest test-estimate-pi
  (testing "estimate pi"
    (let [{:keys [iterations pi-estimate inside]} (pi/estimate-pi 100 2)] 
      (is (= 200 iterations))
      (is (< 100 inside))
      (is (< 2 pi-estimate))
      (is (> 4 pi-estimate)))))
