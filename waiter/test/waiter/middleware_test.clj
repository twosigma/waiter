;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
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
