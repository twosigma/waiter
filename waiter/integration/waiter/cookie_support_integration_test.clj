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
(ns waiter.cookie-support-integration-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [waiter.client-tools :refer :all]))

(deftest ^:parallel ^:integration-fast test-cookie-support
  (let [headers {:x-waiter-name (rand-name)}]
    (testing-using-waiter-url
     (let [headers (assoc headers :x-kitchen-cookies "test=CrazyCase,test2=lol2,test3=\"lol3\"")
           {:keys [cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
       (is (= "CrazyCase" (get-in cookies ["test" :value])))
       (is (= "lol2" (get-in cookies ["test2" :value])))
       (is (= "%22lol3%22" (get-in cookies ["test3" :value])))
       (is (get-in cookies ["x-waiter-auth" :value]))))
    (testing-using-waiter-url
     (let [headers (assoc headers :x-kitchen-cookies "test=singlecookie")
           {:keys [cookies] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
       (is (= "singlecookie" (get-in cookies ["test" :value])))
       (is (get-in cookies ["x-waiter-auth" :value]))
       (delete-service waiter-url (:service-id response))))))

(deftest ^:parallel ^:integration-fast test-cookie-sent-to-backend
  (testing-using-waiter-url
   (let [extra-headers {:x-waiter-name (rand-name)}
         {:keys [service-id cookies]} (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %))
         {:keys [body]} (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url % :path "/request-info"
                                                                                           :cookies (assoc cookies "test" {:value "cookie"
                                                                                                                           :discard false
                                                                                                                           :path "/"})))
         body-json (json/read-str (str body))]
     (is (= "test=cookie" (get-in body-json ["headers" "cookie"])))
     (delete-service waiter-url service-id))))
