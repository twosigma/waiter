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
(ns waiter.token-history-integration-test
  (:require [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.utils :as utils]))

(deftest ^:parallel ^:integration-slow test-basic-token-history
  (testing-using-waiter-url
   (let [service-id-prefix (rand-name)
         token-suffix (create-token-name waiter-url ".")
         token (str "token1." token-suffix)
         token-param {"token" token}
         expiry-date (t/plus (t/now) (t/days 2)) ;; add metadata to make token expire in 2 days, in case not cleaned up
         yyyy (t/year expiry-date)
         mm (->> expiry-date t/month (format "%02d"))
         dd (->> expiry-date t/day (format "%02d"))
         token-desc {:name service-id-prefix
                     :token token
                     :cmd "do-not-start-this-service"
                     :version "image"
                     :cmd-type "shell"
                     :cpus 1
                     :mem 32
                     :metadata {"waiter-token-expiration-date" (str yyyy "-" mm "-" dd)}}]

     (testing "querying endpoint before token created"
       (let [{:keys [body] :as response} (make-request waiter-url "/token-history" :query-params token-param)]
         (assert-response-status response http-404-not-found)
         (is (str/includes? (str body) (str "Token not found: " token)))))


     (testing "creating token"
       (let [post-response (post-token waiter-url token-desc)
             {:keys [body] :as history-response} (make-request waiter-url "/token-history" :query-params token-param)
             token-history (utils/try-parse-json body keyword)]
         (assert-response-status post-response http-200-ok)
         (is (str/includes? (:body post-response) (str "Successfully created " token)))
         (assert-response-status history-response http-200-ok)
         (is (= 1 (count token-history)))
         (let [descriptor (first token-history)]
           (is (-> descriptor :service-description some?))
           (is (-> descriptor :source-component (= "token")))
           (is (-> descriptor :update-time some?))
           (is (-> descriptor :service-id some?)))))

     (testing "updating token"
       (let [post-response (post-token waiter-url (assoc token-desc :mem 64))
             {:keys [body] :as history-response} (make-request waiter-url "/token-history" :query-params token-param)
             token-history (utils/try-parse-json body keyword)]

         (assert-response-status post-response http-200-ok)
         (is (str/includes? (:body post-response) (str "Successfully updated " token)))
         (assert-response-status history-response http-200-ok)
         (is (= 2 (count token-history)))
         (let [[descriptor-2 descriptor-1] token-history
               update-time-1 (tc/from-string (:update-time descriptor-1))
               update-time-2 (tc/from-string (:update-time descriptor-2))]
           (is (t/after? update-time-2 update-time-1))
           (is (= "token" (:source-component descriptor-1)))
           (is (= "token" (:source-component descriptor-2)))
           (is (-> descriptor-1 :service-id some?))
           (is (-> descriptor-2 :service-id some?))
           (is (-> descriptor-1 :service-description some?))
           (is (-> descriptor-2 :service-description some?)))))

     (delete-token-and-assert waiter-url token)
     (testing "querying endpoint after deleting token"
       (let [{:keys [body] :as response} (make-request waiter-url "/token-history" :query-params token-param)]
         (assert-response-status response http-404-not-found)
         (is (str/includes? (str body) (str "Token not found: " token))))))))
