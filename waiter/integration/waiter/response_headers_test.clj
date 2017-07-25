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
(ns waiter.response-headers-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.client-tools :refer :all]))

(defn- make-debug-kitchen-request
  [waiter-url headers]
  (make-request-with-debug-info headers #(make-kitchen-request waiter-url %)))

(deftest ^:parallel ^:integration-fast test-response-headers
  (testing-using-waiter-url
    (let [extra-headers {:x-waiter-name (rand-name)}
          {:keys [request-headers]} (make-kitchen-request waiter-url extra-headers)
          service-id (retrieve-service-id waiter-url request-headers)]

      (testing "Basic response headers test using endpoint"
        (let [{:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers :debug false)]
          (assert-response-status response 200)
          (is (every? #(not (str/blank? (get headers %))) required-response-headers) (str "Response headers: " headers))
          (is (every? #(str/blank? (get headers %)) (retrieve-debug-response-headers waiter-url)) (str headers))))

      (testing "Router-Id in response headers test using endpoint"
        (let [{:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers :debug true)]
          (assert-response-status response 200)
          (is (every? #(not (str/blank? (get headers %)))
                      (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
              (str headers))))

      (testing "Basic response headers with CID included test using endpoint"
        (let [test-cid "1234567890"
              extra-headers (assoc extra-headers :x-cid test-cid)
              {:keys [headers] :as response} (make-kitchen-request waiter-url extra-headers :debug false)]
          (assert-response-status response 200)
          (is (= test-cid (get headers "x-cid")) (str headers))
          (is (every? #(not (str/blank? (get headers %))) required-response-headers) (str headers))
          (is (every? #(str/blank? (get headers %)) (retrieve-debug-response-headers waiter-url)) (str headers))))

      (delete-service waiter-url service-id))))
