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
(ns waiter.state.ejection-expiry-test
  (:require [clojure.test :refer :all]
            [waiter.state.ejection-expiry :refer :all]
            [plumbing.core :as pc]))

(deftest test-tracker-state
  (let [failure-threshold 10
        service-id->instance-ids-atom (atom {"S1" #{"S1.A"}
                                             "S2" #{"S2.A" "S2.B"}
                                             "S3" #{"S3.A" "S3.B" "S3.C"}})
        tracker (->EjectionExpiryTracker failure-threshold service-id->instance-ids-atom)]
    (is (= {:failure-threshold failure-threshold
            :supported-include-params ["service-details"]}
           (tracker-state tracker #{})))
    (is (= {:failure-threshold failure-threshold
            :service-id->instance-ids (pc/map-vals vec @service-id->instance-ids-atom)
            :supported-include-params ["service-details"]}
           (tracker-state tracker #{"service-details"})))))

(deftest test-instance-expired?
  (let [failure-threshold 10
        service-id->instance-ids-atom (atom {"S1" #{"S1.A"}
                                             "S2" #{"S2.A" "S2.B"}
                                             "S3" #{"S3.A" "S3.B" "S3.C"}})
        tracker (->EjectionExpiryTracker failure-threshold service-id->instance-ids-atom)]

    (is (not (instance-expired? tracker "S0" "S0.A")))

    (is (instance-expired? tracker "S1" "S1.A"))
    (is (not (instance-expired? tracker "S1" "S1.B")))

    (is (instance-expired? tracker "S2" "S2.A"))
    (is (instance-expired? tracker "S2" "S2.B"))
    (is (not (instance-expired? tracker "S2" "S2.C")))))

(deftest test-select-services!
  (let [failure-threshold 10
        service-id->instance-ids-atom (atom {"S0" #{}
                                             "S1" #{"S1.A"}
                                             "S2" #{"S2.A" "S2.B"}
                                             "S3" #{"S3.A" "S3.B" "S3.C"}})
        tracker (->EjectionExpiryTracker failure-threshold service-id->instance-ids-atom)]

    (select-services! tracker ["S1" "S3" "S5" "S7"])

    (is (= {"S1" #{"S1.A"}
            "S3" #{"S3.A" "S3.B" "S3.C"}}
           @service-id->instance-ids-atom))))

(deftest test-select-instances!
  (let [failure-threshold 10
        service-id->instance-ids-atom (atom {"S1" #{"S1.A"}
                                             "S2" #{"S2.A" "S2.B"}
                                             "S3" #{"S3.A" "S3.B" "S3.C"}})
        tracker (->EjectionExpiryTracker failure-threshold service-id->instance-ids-atom)]

    (select-instances! tracker "S1" [])
    (select-instances! tracker "S2" ["S2.B" "S2.D" "S2.F"])

    (is (= {"S2" #{"S2.B"}
            "S3" #{"S3.A" "S3.B" "S3.C"}}
           @service-id->instance-ids-atom))))

(deftest test-track-consecutive-failures!
  (let [failure-threshold 10
        service-id->instance-ids-atom (atom {"S1" #{"S1.A"}
                                             "S2" #{"S2.A" "S2.B"}
                                             "S3" #{"S3.A" "S3.B" "S3.C"}})
        tracker (->EjectionExpiryTracker failure-threshold service-id->instance-ids-atom)]

    (track-consecutive-failures! tracker "S0" "S0.A" 8)
    (is (= {"S1" #{"S1.A"}
            "S2" #{"S2.A" "S2.B"}
            "S3" #{"S3.A" "S3.B" "S3.C"}}
           @service-id->instance-ids-atom))

    (track-consecutive-failures! tracker "S0" "S0.A" 18)
    (is (= {"S0" #{"S0.A"}
            "S1" #{"S1.A"}
            "S2" #{"S2.A" "S2.B"}
            "S3" #{"S3.A" "S3.B" "S3.C"}}
           @service-id->instance-ids-atom))

    (track-consecutive-failures! tracker "S2" "S2.A" 8)
    (is (= {"S0" #{"S0.A"}
            "S1" #{"S1.A"}
            "S2" #{"S2.B"}
            "S3" #{"S3.A" "S3.B" "S3.C"}}
           @service-id->instance-ids-atom))

    (track-consecutive-failures! tracker "S2" "S2.A" 4)
    (is (= {"S0" #{"S0.A"}
            "S1" #{"S1.A"}
            "S2" #{"S2.B"}
            "S3" #{"S3.A" "S3.B" "S3.C"}}
           @service-id->instance-ids-atom))

    (track-consecutive-failures! tracker "S2" "S2.A" 18)
    (is (= {"S0" #{"S0.A"}
            "S1" #{"S1.A"}
            "S2" #{"S2.A" "S2.B"}
            "S3" #{"S3.A" "S3.B" "S3.C"}}
           @service-id->instance-ids-atom))

    (track-consecutive-failures! tracker "S2" "S2.B" 5)
    (is (= {"S0" #{"S0.A"}
            "S1" #{"S1.A"}
            "S2" #{"S2.A"}
            "S3" #{"S3.A" "S3.C" "S3.B"}}
           @service-id->instance-ids-atom))))

