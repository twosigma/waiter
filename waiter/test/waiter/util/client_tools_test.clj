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
(ns waiter.util.client-tools-test
  (:require [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]))

(deftest test-parse-set-cookie-string
  (is (= (list {:http-only? false :max-age -1 :name "name" :path nil :secure? false :value "value"})
         (parse-set-cookie-string "name=value")))
  (is (= (list {:http-only? false :max-age -1 :name "name" :path "/" :secure? false :value "value"})
         (parse-set-cookie-string "name=value;Path=/")))
  (is (= (list {:http-only? false :max-age 12345 :name "name" :path "/" :secure? false :value "value"})
         (parse-set-cookie-string "name=value;Path=/;Max-Age=12345")))
  (is (nil? (parse-set-cookie-string nil))))

(deftest test-parse-cookies
  (is (= [{:http-only? false :max-age -1 :name "name" :path nil :secure? false :value "value"}]
         (parse-cookies "name=value")))
  (is (= [{:http-only? false :max-age -1 :name "name" :path nil :secure? false :value "value"}
          {:http-only? false :max-age -1 :name "name2" :path nil :secure? false :value "value2"}]
         (parse-cookies ["name=value" "name2=value2"]))))
