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
