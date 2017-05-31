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
(ns waiter.cookie-support-test
  (:require [clojure.test :refer :all]
            [waiter.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-cookie-support
  (testing-using-waiter-url
    (let [extra-headers {:x-waiter-name (rand-name "testcookies")
                         :x-kitchen-cookies "test=CrazyCase,test2=lol2"}
          response (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %))
          cookies (:cookies response)]
      (is (= "CrazyCase" (get-in cookies ["test" :value])))
      (is (= "lol2" (get-in cookies ["test2" :value])))
      (delete-service waiter-url (:service-id response)))))
