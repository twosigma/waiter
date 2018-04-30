;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.middleware-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [waiter.middleware :refer :all]))

(deftest test-wrap-update
  (testing "sync"
    (let [handler (-> (fn [request]
                        (is (= :value (:key request)))
                        {:status 200})
                      (wrap-assoc :key :value))]
      (is (= :value (-> {} handler :key)))))
  (testing "async"
    (let [handler (-> (fn [request]
                        (is (= :value (:key request)))
                        (async/go {:status 200}))
                      (wrap-assoc :key :value))]
      (is (= :value (-> {} handler async/<!! :key)))))
  (testing "sync w/ exception"
    (let [handler (-> (fn [request]
                        (is (= :value (:key request)))
                        (throw (ex-data "test" {})))
                      (wrap-assoc :key :value))]
      (try
        (handler {})
        (catch Exception e
          (is (= :value (-> e ex-data :key)))))))
  (testing "async w/ exception"
    (let [handler (-> (fn [request]
                        (is (= :value (:key request)))
                        (async/go (ex-info "test" {})))
                      (wrap-assoc :key :value))]
      (is (= :value (-> {} handler async/<!! ex-data :key))))))
