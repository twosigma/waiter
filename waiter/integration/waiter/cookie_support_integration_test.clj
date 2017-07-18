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
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)}
          cookie-fn (fn [cookies name] (some #(when (= name (:name %)) (:value %)) cookies))]
      (testing "multiple cookies sent from backend"
        (let [headers (assoc headers :x-kitchen-cookies "test=CrazyCase,test2=lol2,test3=\"lol3\"")
              {:keys [cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
          (is (= "CrazyCase" (cookie-fn cookies "test")))
          (is (= "lol2" (cookie-fn cookies "test2")))
          (is (= "%22lol3%22" (cookie-fn cookies "test3")))
          (is (cookie-fn cookies "x-waiter-auth"))))
      (testing "single cookie sent from backend"
        (let [headers (assoc headers :x-kitchen-cookies "test=singlecookie")
              {:keys [cookies] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
          (is (= "singlecookie" (cookie-fn cookies "test")))
          (is (cookie-fn cookies "x-waiter-auth"))
          (delete-service waiter-url (:service-id response)))))))

(deftest ^:parallel ^:integration-fast test-cookie-sent-to-backend
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name)}]
      (testing "single client cookie sent to backend (x-waiter-auth removed)"
        (let [{:keys [cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
              {:keys [body]} (make-request-with-debug-info headers #(make-kitchen-request
                                                                      waiter-url %
                                                                      :path "/request-info"
                                                                      :cookies (conj cookies {:name "test"
                                                                                              :value "cookie"
                                                                                              :discard false
                                                                                              :path "/"})))
              body-json (json/read-str (str body))]
          (is (= "test=cookie" (get-in body-json ["headers" "cookie"])))))
      (testing "no cookies sent to backend (x-waiter-auth removed)"
        (let [{:keys [service-id cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
              {:keys [body]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url % :path "/request-info"
                                                                                          :cookies cookies))
              {:strings [headers]} (json/read-str (str body))]
          (is (not (contains? (set (keys headers)) "cookie")))
          (delete-service waiter-url service-id))))))
