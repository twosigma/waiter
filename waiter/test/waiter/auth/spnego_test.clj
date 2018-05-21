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
