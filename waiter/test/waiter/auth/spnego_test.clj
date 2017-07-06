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
(ns waiter.auth.spnego-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [waiter.cookie-support :as cs]
            [waiter.auth.spnego :refer :all]))

(deftest test-decode-input-token
  (is (decode-input-token {:headers {"authorization" "Negotiate Kerberos-Auth"}}))
  (is (nil? (decode-input-token {:headers {"authorization" "Basic User-Pass"}}))))

(deftest test-encode-output-token
  (let [encoded-token (encode-output-token (.getBytes "Kerberos-Auth"))]
    (is (= "Kerberos-Auth"
           (String. (decode-input-token {:headers {"authorization" encoded-token}}))))))