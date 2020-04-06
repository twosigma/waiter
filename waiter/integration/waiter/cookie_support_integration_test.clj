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
(ns waiter.cookie-support-integration-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]))

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
              {:keys [cookies service-id] :as response} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
          (with-service-cleanup
            service-id
            (is (= "singlecookie" (cookie-fn cookies "test")))
            (is (cookie-fn cookies "x-waiter-auth"))))))))

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
          (is (= ["test=cookie"] (get-in body-json ["headers" "cookie"])))))
      (testing "no cookies sent to backend (x-waiter-auth removed)"
        (let [{:keys [cookies service-id] :as response}
              (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
          (assert-response-status response 200)
          (with-service-cleanup
            service-id
            (let [{:keys [body] :as response} (make-kitchen-request waiter-url headers :cookies cookies :path "/request-info")
                  _ (assert-response-status response 200)
                  {:strs [headers]} (json/read-str (str body))]
              (is (empty? (get headers "cookie"))))))))))
