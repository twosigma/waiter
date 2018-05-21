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
(ns waiter.simulator-test
  (:require [clojure.test :refer :all]
            [waiter.simulator :as simulator]))

(deftest simulator-test
  (doseq [num-clients (range 100)]
    (let [tickstate (simulator/simulate {"idle-ticks" 1
                                         "request-ticks" 100
                                         "startup-ticks" 1
                                         "scale-ticks" 1
                                         "total-ticks" 100
                                         "concurrency-level" 1
                                         "scale-up-factor" 0.9
                                         "scale-down-factor" 0.5
                                         "min-instances" 0
                                         "max-instances" 1000
                                         "expired-instance-restart-rate" 0.1}
                                        {}
                                        {25 num-clients 75 (- 0 num-clients)})]
      (is (= num-clients (:total-instances (get tickstate 50))))
      (is (= 0 (:total-instances (get tickstate 100))) (str "Didn't scale down to zero instances" "num-clients" num-clients "tickstate" (get tickstate 100))))))
