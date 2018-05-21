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
(ns kitchen.pi-test
  (:require [clojure.test :refer :all]
            [kitchen.pi :as pi]))

(deftest test-estimate-pi
  (testing "estimate pi"
    (let [{:keys [iterations pi-estimate inside]} (pi/estimate-pi 100 2)] 
      (is (= 200 iterations))
      (is (< 100 inside))
      (is (< 2 pi-estimate))
      (is (> 4 pi-estimate)))))
