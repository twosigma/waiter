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
(ns waiter.state-test
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [digest]
            [plumbing.core :as pc]
            [waiter.async-utils :as au]
            [waiter.discovery :as discovery]
            [waiter.state :refer :all]
            [waiter.utils :as utils]))

(deftest test-find-instance-to-offer-with-concurrency-level-1
  (let [instance-1 {:id "inst-1", :started-at "123456-1"}
        instance-2 {:id "inst-2", :started-at "123456-2"}
        instance-3 {:id "inst-3", :started-at "123456-3"}
        instance-4 {:id "inst-4", :started-at "123456-4"}
        instance-5 {:id "inst-5", :started-at "123456-5"}
        instance-6 {:id "inst-6", :started-at "123456-6"}
        instance-7 {:id "inst-7", :started-at "123456-7"}
        instance-8 {:id "inst-8", :started-at "123456-8"}
        healthy-instance-combo [instance-2 instance-3 instance-5 instance-6 instance-8]
        unhealthy-instance-combo [instance-1 instance-4 instance-7]
        all-instance-combo (concat healthy-instance-combo unhealthy-instance-combo)
        all-sorted-instance-ids (-> (map :id all-instance-combo) (sort))
        instance-id->state-fn #(merge
                                 (into {} (map (fn [instance-id] [instance-id {:slots-assigned 1, :slots-used 0, :status-tags #{:healthy}}]) %1))
                                 (into {} (map (fn [instance-id] [instance-id {:slots-assigned 0, :slots-used 0, :status-tags #{:unhealthy}}]) %2)))
        test-cases (list
                     {:name "find-instance-to-offer:serving-with-no-healthy-instances"
                      :expected nil
                      :reason :serve-request
                      :id->instance {}
                      :instance-id->state (instance-id->state-fn [] [])
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-instances"
                      :expected [instance-2]
                      :reason :serve-request
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo) [])
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:serving-healthy-unblacklisted-instance-with-no-unhealthy-instances"
                      :expected [instance-3]
                      :reason :serve-request
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-2) :status-tags] conj :blacklisted))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:serving-healthy-unblacklisted-instance-with-no-unhealthy-instances:limited-sorted-instance-ids"
                      :expected [instance-5]
                      :reason :serve-request
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-2) :status-tags] conj :blacklisted))
                      :sorted-instance-ids (drop 3 all-sorted-instance-ids)
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:serving-healthy-unblacklisted-instance-with-no-unhealthy-instances:limited-sorted-instance-ids-2"
                      :expected [instance-6]
                      :reason :serve-request
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-2) :status-tags] conj :blacklisted))
                      :sorted-instance-ids (drop 5 all-sorted-instance-ids)
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-instances:exclude-blacklisted-locked-and-killed"
                      :expected [instance-6]
                      :reason :serve-request
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-2) :status-tags] conj :blacklisted)
                                              (update-in [(:id instance-3) :status-tags] conj :killed)
                                              (update-in [(:id instance-5) :status-tags] conj :locked))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-instances-but-all-excluded"
                      :expected nil
                      :reason :serve-request
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo) [])
                      :exclude-ids-set (set (concat (map :id healthy-instance-combo) (map :id unhealthy-instance-combo)))
                      }
                     (let [exclude-ids-set #{(:id instance-1) (:id instance-2) (:id instance-7) (:id instance-8)}]
                       {:name "find-instance-to-offer:serving-healthy-instance-with-no-unhealthy-but-excluded-instances"
                        :expected [instance-3]
                        :reason :serve-request
                        :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                        :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo) [])
                        :exclude-ids-set exclude-ids-set
                        })
                     {:name "find-instance-to-offer:serving-healthy-instance-with-some-unhealthy-instances"
                      :expected [instance-2]
                      :reason :serve-request
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo)
                                                                 (map :id unhealthy-instance-combo))
                      :exclude-ids-set #{}
                      }
                     (let [exclude-ids-set #{(:id instance-1) (:id instance-2) (:id instance-3) (:id instance-7) (:id instance-8)}]
                       {:name "find-instance-to-offer:serving-healthy-instance-with-some-unhealthy-and-excluded-instances"
                        :expected [instance-5]
                        :reason :serve-request
                        :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                        :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo)
                                                                   (map :id unhealthy-instance-combo))
                        :exclude-ids-set exclude-ids-set
                        })
                     (let [exclude-ids-set (into #{} (map :id healthy-instance-combo))]
                       {:name "find-instance-to-offer:exclude-all-healthy-instances"
                        :expected [nil]
                        :reason :serve-request
                        :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                        :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo)
                                                                   (map :id unhealthy-instance-combo))
                        :exclude-ids-set exclude-ids-set
                        })
                     {:name "find-instance-to-offer:killing-with-no-instances"
                      :expected nil
                      :reason :kill-instance
                      :id->instance {}
                      :instance-id->state (instance-id->state-fn [] [])
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-healthy-instance-with-no-unhealthy-instances"
                      :expected [instance-8]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo) [])
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-healthy-instance-with-no-unhealthy-but-excluded-instances"
                      :expected [instance-6]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo) [])
                      :exclude-ids-set #{(:id instance-1) (:id instance-2) (:id instance-7) (:id instance-8)}
                      }
                     {:name "find-instance-to-offer:killing-healthy-instance-with-no-unhealthy-but-excluded-instances:exclude-busy"
                      :expected [instance-5]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-6)]
                                                         assoc :slots-assigned 2 :slots-used 1))
                      :exclude-ids-set #{(:id instance-1) (:id instance-2) (:id instance-7) (:id instance-8)}
                      }
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances"
                      :expected [instance-7]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo)
                                                                 (map :id unhealthy-instance-combo))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances:exclude-busy"
                      :expected [instance-4]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo)
                                                                     (map :id unhealthy-instance-combo))
                                              (update-in [(:id instance-7)]
                                                         assoc :slots-assigned 0 :slots-used 1))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances:exclude-killed"
                      :expected [instance-4]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo)
                                                                     (map :id unhealthy-instance-combo))
                                              (update-in [(:id instance-7) :status-tags] conj :killed))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-instances:exclude-killed-include-blacklisted"
                      :expected [instance-4]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo)
                                                                     (map :id unhealthy-instance-combo))
                                              (update-in [(:id instance-4) :status-tags] conj :blacklisted)
                                              (update-in [(:id instance-7) :status-tags] conj :killed))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-unhealthy-instance-with-some-unhealthy-and-excluded-instances"
                      :expected [instance-4]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (instance-id->state-fn (map :id healthy-instance-combo)
                                                                 (map :id unhealthy-instance-combo))
                      :exclude-ids-set #{(:id instance-1) (:id instance-2) (:id instance-7) (:id instance-8)}
                      }
                     {:name "find-instance-to-offer:killing-healthy-blacklisted-instance-with-no-unhealthy-instances"
                      :expected [instance-8]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-8) :status-tags] conj :blacklisted))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-healthy-instance-with-no-unhealthy-instances:exclude-locked-and-killed"
                      :expected [instance-2]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-2) :status-tags] conj :blacklisted)
                                              (update-in [(:id instance-3) :status-tags] conj :killed)
                                              (update-in [(:id instance-8) :status-tags] conj :locked))
                      :exclude-ids-set #{}
                      }
                     {:name "find-instance-to-offer:killing-healthy-blacklisted-instance-with-no-unhealthy-instances:exclude-locked-and-killed"
                      :expected [instance-6]
                      :reason :kill-instance
                      :id->instance (#(zipmap (map :id %) %) all-instance-combo)
                      :instance-id->state (-> (instance-id->state-fn (map :id healthy-instance-combo) [])
                                              (update-in [(:id instance-3) :status-tags] conj :killed)
                                              (update-in [(:id instance-8) :status-tags] conj :locked))
                      :exclude-ids-set #{}
                      }
                     )]
    (doseq [{:keys [name expected reason id->instance instance-id->state sorted-instance-ids exclude-ids-set]} test-cases]
      (testing (str "Test " name)
        (let [sorted-instance-ids (or sorted-instance-ids all-sorted-instance-ids)
              acceptable-instance-id? (fn [instance-id] (not (contains? exclude-ids-set instance-id)))
              actual (if (= :kill-instance reason)
                       (find-killable-instance id->instance instance-id->state acceptable-instance-id?)
                       (find-available-instance sorted-instance-ids id->instance instance-id->state acceptable-instance-id?))]
          (when (nil? expected)
            (is (nil? actual)))
          (when (not (nil? expected))
            (when (not (some #(= actual %) expected))
              (log/info name "Expected: " expected ", Actual: " actual))
            (is (not (contains? exclude-ids-set (:id actual))))
            (is (some #(= actual %) expected))))))))

(deftest test-md5-hash-function
  (let [test-cases (list
                     {:name "md5-hash-function:empty-inputs"
                      :router-id nil
                      :instance-id nil
                      :expected (digest/md5 "")
                      }
                     {:name "md5-digest/md5-function:empty-instance-id"
                      :router-id "AB"
                      :instance-id nil
                      :expected (digest/md5 "AB")
                      }
                     {:name "md5-hash-function:empty-router-id"
                      :router-id nil
                      :instance-id "AB"
                      :expected (digest/md5 "AB")
                      }
                     {:name "md5-hash-function:shorter-instance-id"
                      :router-id "AB"
                      :instance-id "C"
                      :expected (digest/md5 "ABC")
                      }
                     {:name "md5-hash-function:shorter-router-id"
                      :router-id "C"
                      :instance-id "AB"
                      :expected (digest/md5 "CAB")
                      }
                     {:name "md5-hash-function:equal-length-input"
                      :router-id "ABCDEF"
                      :instance-id "UVWXYZ"
                      :expected (digest/md5 "ABCDEFUVWXYZ")
                      })]
    (doseq [{:keys [name router-id instance-id expected]} test-cases]
      (testing (str "Test " name)
        (is (= expected (md5-hash-function router-id instance-id)))))))

(deftest test-build-instance-id->sorted-site-hash
  (let [all-instances (map (fn [id] {:id id}) ["i00" "i01" "i02" "i03" "i04" "i05" "i06" "i07" "i08" "i09"])
        all-routers ["p1" "p2" "p3" "p4" "p5" "p6"]
        hash-fn (fn [p i] (mod (* (mod (hash p) 100) (mod (hash i) 100)) 1000))]
    (doseq [num-routers (range (count all-routers))
            num-instances (range (count all-instances))]
      (let [name (str "build-instance-id->sorted-site-hash:" num-routers "p-" num-instances "i")
            routers (take num-routers all-routers)
            instances (take num-instances all-instances)]
        (testing (str "Test " name)
          (let [actual (build-instance-id->sorted-site-hash routers instances hash-fn)]
            (if (empty? instances)
              (is (empty? actual))
              (do
                (is (every? (fn [[instance-id _]] (some #(= instance-id (:id %)) instances)) actual))
                (is (every? true?
                            (for [[_ router-hash-list] actual]
                              (= (set routers) (set (map first router-hash-list))))))))))))))

;; These tests are enumerated to document the minimum number of cases considered
(deftest test-allocate-from-available-slots
  (let [allocate-from-available-slots-test-fn
        (fn [{:keys [name expected target-slots initial-instance-id->available-slots sorted-instance-ids]}]
          (testing name
            (let [actual (allocate-from-available-slots target-slots initial-instance-id->available-slots sorted-instance-ids)]
              (when (not= expected actual)
                (log/info name)
                (log/info "Expected:")
                (clojure.pprint/pprint expected)
                (log/info "Actual:")
                (clojure.pprint/pprint actual))
              (is (= expected actual)))))]
    (doseq [test-case (list
                        {:name "Documentation example"
                         :expected {"a" 2, "b" 3, "d" 3, "e" 2, "f" 2}
                         :target-slots 12
                         :initial-instance-id->available-slots {"a" 2, "b" 5, "c" 0, "d" 3, "e" 2, "f" 4, "g" 0}
                         :sorted-instance-ids ["a" "b" "c" "d" "e" "f" "g"]}
                        {:name "Nil inputs"
                         :expected {}
                         :target-slots nil
                         :initial-instance-id->available-slots nil
                         :sorted-instance-ids nil}
                        {:name "Nil inputs with non-zero target slots"
                         :expected {}
                         :target-slots 2
                         :initial-instance-id->available-slots nil
                         :sorted-instance-ids nil}
                        {:name "Empty instance inputs with non-zero target slots"
                         :expected {}
                         :target-slots 2
                         :initial-instance-id->available-slots {}
                         :sorted-instance-ids []}
                        {:name "Single target slot with single instance"
                         :expected {"a1" 1}
                         :target-slots 1
                         :initial-instance-id->available-slots {"a1" 1}
                         :sorted-instance-ids ["a1"]}
                        {:name "Multiple target slot with single instance and single slot"
                         :expected {"a1" 1}
                         :target-slots 3
                         :initial-instance-id->available-slots {"a1" 1}
                         :sorted-instance-ids ["a1"]}
                        {:name "Multiple target slot with single instance and multiple smaller slot"
                         :expected {"a1" 2}
                         :target-slots 3
                         :initial-instance-id->available-slots {"a1" 2}
                         :sorted-instance-ids ["a1"]}
                        {:name "Multiple target slot with single instance and multiple equal slot"
                         :expected {"a1" 3}
                         :target-slots 3
                         :initial-instance-id->available-slots {"a1" 3}
                         :sorted-instance-ids ["a1"]}
                        {:name "Multiple target slot with single instance and multiple larger slot"
                         :expected {"a1" 3}
                         :target-slots 3
                         :initial-instance-id->available-slots {"a1" 4}
                         :sorted-instance-ids ["a1"]}
                        {:name "Single target slot with prioritized instances - a1"
                         :expected {"a1" 1}
                         :target-slots 1
                         :initial-instance-id->available-slots {"a1" 1, "a2" 1}
                         :sorted-instance-ids ["a1" "a2"]}
                        {:name "Single target slot with prioritized instances - a2"
                         :expected {"a2" 1}
                         :target-slots 1
                         :initial-instance-id->available-slots {"a1" 1, "a2" 1}
                         :sorted-instance-ids ["a2" "a1"]}
                        {:name "Single target slot with prioritized instances - slots at low priority variant-1"
                         :expected {"a3" 1}
                         :target-slots 1
                         :initial-instance-id->available-slots {"a1" 0, "a2" 0, "a3" 1}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with prioritized instances - slots at low priority variant-2"
                         :expected {"a1" 1, "a3" 1}
                         :target-slots 2
                         :initial-instance-id->available-slots {"a1" 0, "a2" 0, "a3" 1}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with prioritized instances - slots at low priority variant-3"
                         :expected {"a2" 1, "a3" 1}
                         :target-slots 2
                         :initial-instance-id->available-slots {"a1" 0, "a2" 0, "a3" 1}
                         :sorted-instance-ids ["a2" "a1" "a3"]}
                        {:name "Target slots with slots from all instances variant-1"
                         :expected {"a1" 1, "a2" 1, "a3" 1}
                         :target-slots 3
                         :initial-instance-id->available-slots {"a1" 1, "a2" 1, "a3" 1}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-2"
                         :expected {"a1" 1, "a2" 1, "a3" 1}
                         :target-slots 3
                         :initial-instance-id->available-slots {"a1" 4, "a2" 4, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-3"
                         :expected {"a1" 2, "a2" 1, "a3" 1}
                         :target-slots 4
                         :initial-instance-id->available-slots {"a1" 4, "a2" 4, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-4"
                         :expected {"a1" 2, "a2" 2, "a3" 1}
                         :target-slots 5
                         :initial-instance-id->available-slots {"a1" 4, "a2" 4, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-5"
                         :expected {"a1" 2, "a2" 2, "a3" 2}
                         :target-slots 6
                         :initial-instance-id->available-slots {"a1" 4, "a2" 4, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-6"
                         :expected {"a1" 3, "a2" 2, "a3" 2}
                         :target-slots 7
                         :initial-instance-id->available-slots {"a1" 4, "a2" 4, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-7"
                         :expected {"a1" 3, "a2" 1, "a3" 3}
                         :target-slots 7
                         :initial-instance-id->available-slots {"a1" 4, "a2" 1, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-8"
                         :expected {"a1" 4, "a3" 3}
                         :target-slots 7
                         :initial-instance-id->available-slots {"a1" 4, "a2" 0, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]}
                        {:name "Target slots with slots from all instances variant-9"
                         :expected {"a1" 1, "a2" 1, "a3" 4}
                         :target-slots 7
                         :initial-instance-id->available-slots {"a1" 0, "a2" 1, "a3" 4}
                         :sorted-instance-ids ["a1" "a2" "a3"]})]
      (allocate-from-available-slots-test-fn test-case))))

;; These tests are enumerated to document the minimum number of cases considered
(deftest test-evenly-distribute-slots-across-routers
  (let [evenly-distribute-slots-test-fn
        (fn [{:keys [name expected instances router->ranked-instance-ids concurrency-level]}]
          (testing name
            (let [actual (evenly-distribute-slots-across-routers instances router->ranked-instance-ids concurrency-level)]
              (when (not= expected actual)
                (log/info name)
                (log/info "Expected:")
                (clojure.pprint/pprint expected)
                (log/info "Actual:")
                (clojure.pprint/pprint actual))
              (is (= expected actual)))))]
    (doseq [test-case (list
                        {:name "Nil inputs"
                         :expected {}
                         :instances nil
                         :router->ranked-instance-ids nil
                         :concurrency-level nil}
                        {:name "Nil inputs and integral concurrency level"
                         :expected {}
                         :instances nil
                         :router->ranked-instance-ids nil
                         :concurrency-level 2}
                        {:name "Empty inputs and integral concurrency level"
                         :expected {}
                         :instances []
                         :router->ranked-instance-ids {}
                         :concurrency-level 2}
                        {:name "Single instance, router and CL=1"
                         :expected {"a" {{:id "a1"} 1}}
                         :instances [{:id "a1"}]
                         :router->ranked-instance-ids {"a" [["a1"]]}
                         :concurrency-level 1}
                        {:name "Single instance, one router and CL=2"
                         :expected {"a" {{:id "a1"} 2}}
                         :instances [{:id "a1"}]
                         :router->ranked-instance-ids {"a" [["a1"]]}
                         :concurrency-level 2}
                        {:name "Single instance, two routers and CL=1"
                         :expected {"a" {{:id "a1"} 1}
                                    "b" {{:id "a1"} 1}}
                         :instances [{:id "a1"}]
                         :router->ranked-instance-ids {"a" [["a1"] []]
                                                       "b" [[] ["a1"]]}
                         :concurrency-level 1}
                        {:name "Single instance, three routers and CL=1"
                         :expected {"a" {{:id "a1"} 1}
                                    "b" {{:id "a1"} 1}
                                    "c" {{:id "a1"} 1}}
                         :instances [{:id "a1"}]
                         :router->ranked-instance-ids {"a" [["a1"] [] []]
                                                       "b" [[] ["a1"] []]
                                                       "c" [[] [] ["a1"]]}
                         :concurrency-level 1}
                        {:name "Single instance, three routers and CL=2"
                         :expected {"a" {{:id "a1"} 1}
                                    "b" {{:id "a1"} 1}
                                    "c" {{:id "a1"} 1}}
                         :instances [{:id "a1"}]
                         :router->ranked-instance-ids {"a" [["a1"] [] []]
                                                       "b" [[] ["a1"] []]
                                                       "c" [[] [] ["a1"]]}
                         :concurrency-level 2}
                        {:name "Two instances, one router and CL=1"
                         :expected {"a" {{:id "a1"} 1, {:id "a2"} 1}}
                         :instances [{:id "a1"} {:id "a2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2"]]}
                         :concurrency-level 1}
                        {:name "Two instances, one router and CL=2"
                         :expected {"a" {{:id "a1"} 2, {:id "a2"} 2}}
                         :instances [{:id "a1"} {:id "a2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2"]]}
                         :concurrency-level 2}
                        {:name "Two instances, two routers and CL=1"
                         :expected {"a" {{:id "a1"} 1}
                                    "b" {{:id "a2"} 1}}
                         :instances [{:id "a1"} {:id "a2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2"] []]
                                                       "b" [[] ["a1" "a2"]]}
                         :concurrency-level 1}
                        {:name "Two partitioned instances, two routers and CL=1"
                         :expected {"a" {{:id "a1"} 1}
                                    "b" {{:id "b1"} 1}}
                         :instances [{:id "a1"} {:id "b1"}]
                         :router->ranked-instance-ids {"a" [["a1"] ["b1"]]
                                                       "b" [["b1"] ["a1"]]}
                         :concurrency-level 1}
                        {:name "Two instances, two routers and CL=2"
                         :expected {"a" {{:id "a1"} 1, {:id "a2"} 1}
                                    "b" {{:id "a1"} 1, {:id "a2"} 1}}
                         :instances [{:id "a1"} {:id "a2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2"] []]
                                                       "b" [[] ["a1" "a2"]]}
                         :concurrency-level 2}
                        {:name "Two instances, three routers and CL=1"
                         :expected {"a" {{:id "a1"} 1}
                                    "b" {{:id "a2"} 1}
                                    "c" {{:id "a1"} 1}}
                         :instances [{:id "a1"} {:id "a2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2"] [] []]
                                                       "b" [[] ["a1" "a2"] []]
                                                       "c" [[] [] ["a1" "a2"]]}
                         :concurrency-level 1}
                        {:name "Two partitioned instances, three routers and CL=1"
                         :expected {"a" {{:id "a1"} 1}
                                    "b" {{:id "b1"} 1}
                                    "c" {{:id "b1"} 1}}
                         :instances [{:id "a1"} {:id "b1"}]
                         :router->ranked-instance-ids {"a" [["a1"] ["b1"] []]
                                                       "b" [["b1"] ["a1"] []]
                                                       "c" [[] ["b1"] ["a1"]]}
                         :concurrency-level 1}
                        {:name "Two instances, three routers and CL=2"
                         :expected {"a" {{:id "a1"} 1, {:id "a2"} 1}
                                    "b" {{:id "a2"} 1}
                                    "c" {{:id "a1"} 1}}
                         :instances [{:id "a1"} {:id "a2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2"] [] []]
                                                       "b" [[] ["a1" "a2"] []]
                                                       "c" [[] [] ["a1" "a2"]]}
                         :concurrency-level 2}
                        {:name "Two instances, three routers and CL=3"
                         :expected {"a" {{:id "a1"} 1, {:id "a2"} 1}
                                    "b" {{:id "a1"} 1, {:id "a2"} 1}
                                    "c" {{:id "a1"} 1, {:id "a2"} 1}}
                         :instances [{:id "a1"} {:id "a2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2"] [] []]
                                                       "b" [[] ["a1" "a2"] []]
                                                       "c" [[] [] ["a1" "a2"]]}
                         :concurrency-level 3}
                        {:name "Ten instances, three routers and CL=1"
                         :expected {"a" {{:id "a1"} 1, {:id "a2"} 1, {:id "a3"} 1, {:id "a4"} 1}
                                    "b" {{:id "b1"} 1, {:id "b2"} 1, {:id "b3"} 1}
                                    "c" {{:id "c1"} 1, {:id "c2"} 1, {:id "a5"} 1}}
                         :instances [{:id "a1"} {:id "a2"} {:id "a3"} {:id "a4"} {:id "a5"}
                                     {:id "b1"} {:id "b2"} {:id "b3"}
                                     {:id "c1"} {:id "c2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2" "a3" "a4" "a5"] ["b3" "c2"] ["b1" "b2" "c1"]]
                                                       "b" [["b1" "b2" "b3"] ["a1" "a3" "a4" "a5" "c1"] ["a2" "c2"]]
                                                       "c" [["c1" "c2"] ["a2" "b1" "b2"] ["a1" "a3" "a4" "a5" "b3"]]}
                         :concurrency-level 1}
                        {:name "Five instances, three routers and CL=2"
                         :expected {"a" {{:id "a1"} 1, {:id "a2"} 1, {:id "a3"} 1, {:id "b1"} 1},
                                    "b" {{:id "b1"} 1, {:id "a3"} 1, {:id "c1"} 1},
                                    "c" {{:id "c1"} 1, {:id "a1"} 1, {:id "a2"} 1}}
                         :instances [{:id "a1"} {:id "a2"} {:id "a3"} {:id "b1"} {:id "c1"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2" "a3"] ["b1"] ["c1"]]
                                                       "b" [["b1"] ["a1" "a3" "c1"] ["a2"]]
                                                       "c" [["c1"] ["a2"] ["a1" "a3" "b1"]]}
                         :concurrency-level 2}
                        {:name "Ten instances, three routers and CL=2"
                         :expected {"a" {{:id "a1"} 1, {:id "a2"} 1, {:id "a3"} 1, {:id "a4"} 1, {:id "a5"} 1, {:id "b3"} 1, {:id "c2"} 1}
                                    "b" {{:id "b1"} 1, {:id "b2"} 1, {:id "b3"} 1, {:id "a3"} 1, {:id "a4"} 1, {:id "a5"} 1, {:id "c1"} 1}
                                    "c" {{:id "c1"} 1, {:id "c2"} 1, {:id "a1"} 1, {:id "a2"} 1, {:id "b1"} 1, {:id "b2"} 1}}
                         :instances [{:id "a1"} {:id "a2"} {:id "a3"} {:id "a4"} {:id "a5"}
                                     {:id "b1"} {:id "b2"} {:id "b3"}
                                     {:id "c1"} {:id "c2"}]
                         :router->ranked-instance-ids {"a" [["a1" "a2" "a3" "a4" "a5"] ["b3" "c2"] ["b1" "b2" "c1"]]
                                                       "b" [["b1" "b2" "b3"] ["a1" "a3" "a4" "a5" "c1"] ["a2" "c2"]]
                                                       "c" [["c1" "c2"] ["a2" "b1" "b2"] ["a1" "a3" "a4" "a5" "b3"]]}
                         :concurrency-level 2}
                        )]
      (evenly-distribute-slots-test-fn test-case))))

(deftest test-distribute-slots-across-routers
  (let [distribute-slots-test-fn
        (fn [expected instances router->ranked-instance-ids concurrency-level]
          (let [actual (distribute-slots-across-routers instances router->ranked-instance-ids concurrency-level)]
            (when (not= expected actual)
              (log/info (first *testing-contexts*))
              (log/info "Expected:")
              (log/info (with-out-str (clojure.pprint/pprint expected)))
              (log/info "Actual:")
              (log/info (with-out-str (clojure.pprint/pprint actual))))
            actual))]
    (testing "Nil inputs"
      (let [expected {}
            instances nil
            router->ranked-instance-ids nil
            concurrency-level nil
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Nil inputs and integral concurrency level"
      (let [expected {}
            instances nil
            router->ranked-instance-ids nil
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Empty inputs and integral concurrency level"
      (let [expected {}
            instances []
            router->ranked-instance-ids {}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Single instance, router and CL=1"
      (let [expected {"a" {{:id "a1"} 1}}
            instances [{:id "a1"}]
            router->ranked-instance-ids {"a" [["a1"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Single instance, one router and CL=2"
      (let [expected {"a" {{:id "a1"} 2}}
            instances [{:id "a1"}]
            router->ranked-instance-ids {"a" [["a1"]]}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Single instance, two routers and CL=1"
      (let [expected {"a" {{:id "a1"} 1}, "b" {}}
            instances [{:id "a1"}]
            router->ranked-instance-ids {"a" [["a1"] []], "b" [[] ["a1"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Single instance, three routers and CL=1"
      (let [expected {"a" {{:id "a1"} 1}, "b" {}, "c" {}}
            instances [{:id "a1"}]
            router->ranked-instance-ids {"a" [["a1"] [] []], "b" [[] ["a1"] []], "c" [[] [] ["a1"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Single instance, three routers and CL=2"
      (let [expected {"a" {{:id "a1"} 2}, "b" {}, "c" {}}
            instances [{:id "a1"}]
            router->ranked-instance-ids {"a" [["a1"] [] []], "b" [[] ["a1"] []], "c" [[] [] ["a1"]]}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two instances, one router and CL=1"
      (let [expected {"a" {{:id "a1"} 1, {:id "a2"} 1}}
            instances [{:id "a1"} {:id "a2"}]
            router->ranked-instance-ids {"a" [["a1" "a2"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two instances, one router and CL=2"
      (let [expected {"a" {{:id "a1"} 2, {:id "a2"} 2}}
            instances [{:id "a1"} {:id "a2"}]
            router->ranked-instance-ids {"a" [["a1" "a2"]]}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two instances, two routers and CL=1"
      (let [expected {"a" {{:id "a1"} 1, {:id "a2"} 1}
                      "b" {}}
            instances [{:id "a1"} {:id "a2"}]
            router->ranked-instance-ids {"a" [["a1" "a2"] []]
                                         "b" [[] ["a1" "a2"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two partitioned instances, two routers and CL=1"
      (let [expected {"a" {{:id "a1"} 1}, "b" {{:id "b1"} 1}}
            instances [{:id "a1"} {:id "b1"}]
            router->ranked-instance-ids {"a" [["a1"] ["b1"]], "b" [["b1"] ["a1"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two instances, two routers and CL=2"
      (let [expected {"a" {{:id "a1"} 2, {:id "a2"} 2}, "b" {}}
            instances [{:id "a1"} {:id "a2"}]
            router->ranked-instance-ids {"a" [["a1" "a2"] []], "b" [[] ["a1" "a2"]]}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two instances, three routers and CL=1"
      (let [expected {"a" {{:id "a1"} 1, {:id "a2"} 1}, "b" {}, "c" {}}
            instances [{:id "a1"} {:id "a2"}]
            router->ranked-instance-ids {"a" [["a1" "a2"] [] []], "b" [[] ["a1" "a2"] []], "c" [[] [] ["a1" "a2"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two partitioned instances, three routers and CL=1"
      (let [expected {"a" {{:id "a1"} 1}, "b" {{:id "b1"} 1}, "c" {}}
            instances [{:id "a1"} {:id "b1"}]
            router->ranked-instance-ids {"a" [["a1"] ["b1"] []], "b" [["b1"] ["a1"] []], "c" [[] ["b1"] ["a1"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two instances, three routers and CL=2"
      (let [expected {"a" {{:id "a1"} 2, {:id "a2"} 2}, "b" {}, "c" {}}
            instances [{:id "a1"} {:id "a2"}]
            router->ranked-instance-ids {"a" [["a1" "a2"] [] []], "b" [[] ["a1" "a2"] []], "c" [[] [] ["a1" "a2"]]}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Two instances, three routers and CL=3"
      (let [expected {"a" {{:id "a1"} 3, {:id "a2"} 3}, "b" {}, "c" {}}
            instances [{:id "a1"} {:id "a2"}]
            router->ranked-instance-ids {"a" [["a1" "a2"] [] []], "b" [[] ["a1" "a2"] []], "c" [[] [] ["a1" "a2"]]}
            concurrency-level 3
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Ten instances, three routers and CL=1"
      (let [expected {"a" {{:id "a1"} 1, {:id "a2"} 1, {:id "a3"} 1, {:id "a4"} 1, {:id "a5"} 1}
                      "b" {{:id "b1"} 1, {:id "b2"} 1, {:id "b3"} 1}
                      "c" {{:id "c1"} 1, {:id "c2"} 1}}
            instances [{:id "a1"} {:id "a2"} {:id "a3"} {:id "a4"} {:id "a5"}
                       {:id "b1"} {:id "b2"} {:id "b3"}
                       {:id "c1"} {:id "c2"}]
            router->ranked-instance-ids {"a" [["a1" "a2" "a3" "a4" "a5"] ["b3" "c2"] ["b1" "b2" "c1"]]
                                         "b" [["b1" "b2" "b3"] ["a1" "a3" "a4" "a5" "c1"] ["a2" "c2"]]
                                         "c" [["c1" "c2"] ["a2" "b1" "b2"] ["a1" "a3" "a4" "a5" "b3"]]}
            concurrency-level 1
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Five instances, three routers and CL=2"
      (let [expected {"a" {{:id "a1"} 2, {:id "a2"} 2, {:id "a3"} 2},
                      "b" {{:id "b1"} 2},
                      "c" {{:id "c1"} 2}}
            instances [{:id "a1"} {:id "a2"} {:id "a3"} {:id "b1"} {:id "c1"}]
            router->ranked-instance-ids {"a" [["a1" "a2" "a3"] ["b1"] ["c1"]]
                                         "b" [["b1"] ["a1" "a3" "c1"] ["a2"]]
                                         "c" [["c1"] ["a2"] ["a1" "a3" "b1"]]}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))
    (testing "Ten instances, three routers and CL=2"
      (let [expected {"a" {{:id "a1"} 2, {:id "a2"} 2, {:id "a3"} 2, {:id "a4"} 2, {:id "a5"} 2}
                      "b" {{:id "b1"} 2, {:id "b2"} 2, {:id "b3"} 2}
                      "c" {{:id "c1"} 2, {:id "c2"} 2}}
            instances [{:id "a1"} {:id "a2"} {:id "a3"} {:id "a4"} {:id "a5"}
                       {:id "b1"} {:id "b2"} {:id "b3"}
                       {:id "c1"} {:id "c2"}]
            router->ranked-instance-ids {"a" [["a1" "a2" "a3" "a4" "a5"] ["b3" "c2"] ["b1" "b2" "c1"]]
                                         "b" [["b1" "b2" "b3"] ["a1" "a3" "a4" "a5" "c1"] ["a2" "c2"]]
                                         "c" [["c1" "c2"] ["a2" "b1" "b2"] ["a1" "a3" "a4" "a5" "b3"]]}
            concurrency-level 2
            actual (distribute-slots-test-fn expected instances router->ranked-instance-ids concurrency-level)]
        (is (= expected actual))))))

(deftest test-evenly-distribute-slots-using-consistent-hash-distribution
  (let [all-instances (map (fn [id] {:id id}) ["i00" "i01" "i02" "i03" "i04" "i05" "i06" "i07" "i08" "i09"
                                               "i10" "i11" "i12" "i13" "i14" "i15" "i16" "i17" "i18" "i19"])
        all-routers ["p1" "p2" "p3" "p4" "p5" "p6"]
        hash-fn (fn [p i] (mod (* (mod (hash p) 100) (mod (hash i) 100)) 1000))
        distribution-scheme "balanced"]
    (doseq [concurrency-level (range 1 6)]
      (doseq [num-routers (range (count all-routers))
              num-instances (range (count all-instances))]
        (let [name (str "evenly-distribute-slots-across-routers-using-consistent-hash-distribution:" num-routers "p-" num-instances "i-" concurrency-level "cl")
              routers (take num-routers all-routers)
              instances (take num-instances all-instances)]
          (testing (str "Test " name)
            (let [actual (distribute-slots-using-consistent-hash-distribution routers instances hash-fn concurrency-level distribution-scheme)]
              (is (= (set routers) (set (keys actual))))
              (when (> (count instances) 0)
                ; ensure every router got assigned at least one instance
                (is (every? #(pos? (count (second %))) actual))
                ; ensure a given instance is not shared more often than others
                (let [instance-distribution (map
                                              (fn [instance]
                                                (reduce + (vals (pc/map-vals (fn [instances->slots]
                                                                               (if (contains? instances->slots instance) 1 0))
                                                                             actual))))
                                              instances)
                      max-instance-frequency (apply max instance-distribution)
                      min-instance-frequency (apply min instance-distribution)]
                  (is (<= 0 (- max-instance-frequency min-instance-frequency) 1))
                  ; ensure every instance is assigned to at least one router
                  (when (not-empty routers)
                    (is (pos? min-instance-frequency)))))
              ; ensure even distribution across routers (to the best possible) has been done on slots for each instance
              (let [instance-slot-distribution (map
                                                 (fn [instance]
                                                   (reduce + (vals (pc/map-vals (fn [instances->slots]
                                                                                  (get instances->slots instance 0))
                                                                                actual))))
                                                 instances)
                    max-slots-allocated (reduce max (if (empty? instance-slot-distribution) 0 0) instance-slot-distribution)
                    min-slots-allocated (reduce min (if (empty? instance-slot-distribution) 0 1000) instance-slot-distribution)]
                (is (<= 0 (- max-slots-allocated min-slots-allocated) 1)))
              ; verify there was minimal change from previous configuration (i.e. benefited from our hash distribution)
              (let [previous (distribute-slots-using-consistent-hash-distribution routers (rest (reverse instances)) hash-fn concurrency-level distribution-scheme)
                    [left-diff & _] (data/diff previous actual)
                    instance-diff-set (let [extract-instance-id-fn (fn [diff-map]
                                                                     (reduce concat []
                                                                             (map (fn [router-id]
                                                                                    (map #(:id (key %)) (get diff-map router-id)))
                                                                                  (keys diff-map))))]
                                        (set (extract-instance-id-fn left-diff)))
                    total-diff-count (count instance-diff-set)]
                (if (<= num-instances num-routers)
                  (is (<= total-diff-count num-instances))
                  (is (<= total-diff-count (Math/ceil (/ num-instances 2)))))))))))))

(deftest test-distribute-slots-using-consistent-hash-distribution-partitioning
  (let [distribution-scheme-atom (atom nil)
        instances [{:id "a1"} {:id "a2"} {:id "a3"} {:id "a4"} {:id "a5"}]
        router-ids ["p1" "p2" "p3" "p4" "p5" "p6"]
        hash-fn (fn [p i] (mod (* (mod (hash p) 100) (mod (hash i) 100)) 1000))
        concurrency-level 1]
    (with-redefs [evenly-distribute-slots-across-routers (fn [_ _ _] (reset! distribution-scheme-atom "balanced"))
                  distribute-slots-across-routers (fn [_ _ _] (reset! distribution-scheme-atom "simple"))]
      (testing "balanced-distribution-scheme"
        (distribute-slots-using-consistent-hash-distribution router-ids instances hash-fn concurrency-level "balanced")
        (is (= "balanced" @distribution-scheme-atom)))

      (testing "simple-distribution-scheme"
        (distribute-slots-using-consistent-hash-distribution router-ids instances hash-fn concurrency-level "simple")
        (is (= "simple" @distribution-scheme-atom))))))

(deftest test-get-deployment-error
  (let [config {:grace-period-ms 10000
                :min-failed-instances 2
                :min-hosts 1
                :using-marathon true}
        alive-started-at (.toString (t/now))
        test-cases (list {:name "no-instances", :healthy-instances [], :unhealthy-instances [], :failed-instances [], :expected nil}
                         {:name "no-deployment-errors", :healthy-instances [:instance-one], :unhealthy-instances [], :failed-instances [], :expected nil}
                         {:name "healthy-and-unhealthy-instances", :healthy-instances [:instance-one],
                          :unhealthy-instances [{:health-check-status 400 :started-at alive-started-at}], :failed-instances [], :expected nil}
                         {:name "healthy-and-failed-instances", :healthy-instances [:instance-one],
                          :unhealthy-instances [], :failed-instances [{:message "Command exited with status" :exit-code 1}], :expected nil}
                         {:name "single-unhealthy-instance", :healthy-instances [],
                          :unhealthy-instances [{:health-check-status 400 :started-at alive-started-at}], :failed-instances [], :expected nil}
                         {:name "single-failed-instance", :healthy-instances [], :unhealthy-instances [],
                          :failed-instances [{:message "Command exited with status" :exit-code 1}], :expected nil}
                         {:name "multiple-different-unhealthy-instances", :healthy-instances [],
                          :unhealthy-instances [{:health-check-status 400 :started-at alive-started-at}
                                                {:health-check-status 401 :started-at alive-started-at}],
                          :failed-instances [], :expected nil}
                         {:name "multiple-different-failed-instances", :healthy-instances [], :unhealthy-instances [],
                          :failed-instances [{:message "Command exited with status" :exit-code 1}
                                             {:message "Memory limit exceeded:" :flags #{:has-connected :has-responded :memory-limit-exceeded}}],
                          :expected nil}
                         {:name "not-enough-memory", :healthy-instances [], :unhealthy-instances [],
                          :failed-instances [{:message "Memory limit exceeded:" :flags #{:memory-limit-exceeded}}
                                             {:message "Memory limit exceeded:" :flags #{:memory-limit-exceeded}}],
                          :expected :not-enough-memory}
                         {:name "invalid-health-check-response", :healthy-instances [], :unhealthy-instances [],
                          :failed-instances [{:message "Task was killed" :flags #{:has-connected :has-responded :never-passed-health-checks}}
                                             {:message nil :flags #{:has-connected :has-responded :never-passed-health-checks}}],
                          :expected :invalid-health-check-response}
                         {:name "health-check-timed-out", :healthy-instances [], :unhealthy-instances [],
                          :failed-instances [{:message "Task was killed" :flags #{:has-connected :never-passed-health-checks}}
                                             {:message nil :flags #{:has-connected :never-passed-health-checks}}],
                          :expected :health-check-timed-out}
                         {:name "cannot-connect", :healthy-instances [], :unhealthy-instances [],
                          :failed-instances [{:message "Task was killed" :flags #{:never-passed-health-checks}}
                                             {:message nil :flags #{:never-passed-health-checks}}],
                          :expected :cannot-connect}
                         {:name "bad-startup-command", :healthy-instances [], :unhealthy-instances [],
                          :failed-instances [{:message "Command exited with status" :exit-code 1}
                                             {:message "Command exited with status" :exit-code 1}],
                          :expected :bad-startup-command}
                         {:name "health-check-requires-authentication", :healthy-instances [],
                          :unhealthy-instances [{:health-check-status 401}], :failed-instances [],
                          :expected :health-check-requires-authentication}
                         {:name "unhealthy-and-failed-instances", :healthy-instances [],
                          :unhealthy-instances [{:health-check-status 401}],
                          :failed-instances [{:message "Command exited with status" :exit-code 1}
                                             {:message "Command exited with status" :exit-code 1}],
                          :expected :bad-startup-command})]
    (doseq [{:keys [name healthy-instances unhealthy-instances failed-instances expected]} test-cases]
      (testing (str "Test " name)
        (is (= expected (get-deployment-error healthy-instances unhealthy-instances failed-instances config)))))))

(deftest test-router-state-maintainer-scheduler-state
  (testing "router-state-maintainer-removes-expired-instances"
    (let [scheduler-state-chan (async/chan 1)
          router-chan (async/chan 1)
          router-id "router.0"
          router-state-push-chan (async/chan 1)
          exit-chan (async/chan 1)
          service-id->service-description-fn (constantly {"concurrency-level" 1
                                                          "grace-period-secs" 30
                                                          "instance-expiry-mins" 1})
          service-id "service-1"
          instance {:id (str service-id ".1")
                    :started-at (f/unparse (f/formatters :date-time)
                                           (t/minus (t/now) (t/minutes 2)))}
          deployment-error-config {:min-failed-instances 2
                                   :min-hosts 2}]
      (let [{:keys [router-state-push-mult]} (start-router-state-maintainer scheduler-state-chan router-chan router-id exit-chan service-id->service-description-fn deployment-error-config)]
        (async/tap router-state-push-mult router-state-push-chan))
      (async/>!! router-chan {router-id (str "http://www." router-id ".com")})
      (async/>!! scheduler-state-chan [[:update-available-apps {:available-apps [service-id]
                                                                :scheduler-sync-time (t/now)}]])
      (async/<!! router-state-push-chan)
      (async/>!! scheduler-state-chan [[:update-app-instances {:healthy-instances [instance]
                                                               :unhealthy-instances []
                                                               :sorted-instance-ids [(:id instance)]
                                                               :service-id service-id
                                                               :scheduler-sync-time (t/now)}]])
      (let [{:keys [service-id->healthy-instances service-id->expired-instances]} (async/<!! router-state-push-chan)]
        (is (= [instance] (get service-id->healthy-instances service-id)))
        (is (= [instance] (get service-id->expired-instances service-id))))
      (async/>!! scheduler-state-chan [[:update-app-instances {:healthy-instances []
                                                               :unhealthy-instances []
                                                               :sorted-instance-ids []
                                                               :service-id service-id
                                                               :scheduler-sync-time (t/now)}]])
      (let [{:keys [service-id->healthy-instances service-id->expired-instances]} (async/<!! router-state-push-chan)]
        (is (empty? (get service-id->healthy-instances service-id)))
        (is (empty? (get service-id->expired-instances service-id))))
      (async/>!! exit-chan :exit)))

  (testing "router-state-maintainer-scheduler-state-incremental"
    (let [scheduler-state-chan (async/chan 1)
          router-chan (async/chan 1)
          num-message-iterations 20
          router-state-push-chan (async/chan 1)
          exit-chan (async/chan 1)
          concurrency-level 1
          slot-partition-fn (fn [routers instances]
                              (pc/map-vals
                                (fn [my-instances] (into {} (map (fn [instance] {instance concurrency-level}) my-instances)))
                                (zipmap
                                  routers
                                  (let [instances-partition (partition (quot (inc (count instances)) (count routers)) instances)]
                                    (map #(sort (:id (remove nil? %))) instances-partition)))))
          router-id "router.0"
          routers {router-id (str "http://www." router-id ".com")
                   "router.1" (str "http://www.router.1.com")}
          services-fn #(vec (map (fn [i] (str "service-" i)) (range (inc (if (> % (/ num-message-iterations 2)) (- % 5) %)))))
          service-id->service-description-fn (fn [id] (let [service-num (Integer/parseInt (str/replace id "service-" ""))]
                                                        {"concurrency-level" concurrency-level
                                                         "instance-expiry-mins" service-num
                                                         "grace-period-secs" (* 60 service-num)}))
          deployment-error-config {:min-failed-instances 2
                                   :min-hosts 2}]
      (with-redefs [distribute-slots-using-consistent-hash-distribution (fn [routers instances _ _ _] (slot-partition-fn routers instances))]

        (let [{:keys [router-state-push-mult]} (start-router-state-maintainer scheduler-state-chan router-chan router-id exit-chan service-id->service-description-fn deployment-error-config)]
          (async/tap router-state-push-mult router-state-push-chan))


        (async/>!! router-chan routers)
        (is (= routers (:routers (async/<!! router-state-push-chan))))

        (let [start-time (t/now)
              healthy-instances-fn (fn [service-id index n]
                                     (vec (map #(assoc
                                                  {:started-at (f/unparse (f/formatters :date-time) start-time)}
                                                  :id (str service-id "." % "1"))
                                               (range (if (zero? (mod index 2)) 1 (max 1 n))))))
              unhealthy-instances-fn (fn [service-id index]
                                       (vec (map (fn [x] {:id (str service-id "." x "1")
                                                          :started-at (f/unparse (f/formatters :date-time) start-time)})
                                                 (range (if (zero? (mod index 2)) 1 0)))))
              failed-instances-fn (fn [service-id index]
                                    (vec (map (fn [x] {:id (str service-id "." x "1")
                                                       :started-at (f/unparse (f/formatters :date-time) start-time)})
                                              (range (if (zero? (mod index 2)) 1 0)))))]
          (dotimes [n num-message-iterations]
            (let [current-time (t/plus start-time (t/minutes n))]
              (let [services (services-fn n)]
                (loop [index 0
                       scheduler-messages [[:update-available-apps {:available-apps services
                                                                    :scheduler-sync-time current-time}]]]
                  (if (>= index (count services))
                    (async/>!! scheduler-state-chan scheduler-messages)
                    (let [service-id (str "service-" index)
                          failed-instances (failed-instances-fn service-id index)
                          healthy-instances (healthy-instances-fn service-id index n)
                          unhealthy-instances (unhealthy-instances-fn service-id index)
                          service-instances-message [:update-app-instances
                                                     (assoc {:healthy-instances healthy-instances
                                                             :unhealthy-instances unhealthy-instances}
                                                       :service-id service-id
                                                       :failed-instances failed-instances
                                                       :scheduler-sync-time current-time)]]
                      (recur (inc index) (conj scheduler-messages service-instances-message))))))
              (let [expected-services (services-fn n)
                    expected-state (let [index-fn #(Integer/parseInt (subs % (inc (.lastIndexOf ^String % "-"))))]
                                     {:service-id->healthy-instances
                                      (pc/map-from-keys #(healthy-instances-fn % (index-fn %) n) expected-services)
                                      :service-id->unhealthy-instances
                                      (pc/map-from-keys #(unhealthy-instances-fn % (index-fn %)) expected-services)
                                      :service-id->failed-instances
                                      (pc/map-from-keys #(failed-instances-fn % (index-fn %)) expected-services)
                                      :service-id->deployment-error {} ; should be no deployment errors
                                      :service-id->expired-instances
                                      (pc/map-from-keys
                                        (fn [service]
                                          (let [healthy-instances (healthy-instances-fn service (index-fn service) n)
                                                expiry-mins-int (Integer/parseInt (str/replace service "service-" ""))
                                                expiry-mins (t/minutes expiry-mins-int)]
                                            (filter #(and (pos? expiry-mins-int)
                                                          (utils/older-than? current-time expiry-mins %1))
                                                    healthy-instances)))
                                        expected-services)
                                      :service-id->starting-instances
                                      (pc/map-from-keys
                                        (fn [service]
                                          (let [unhealthy-instances (unhealthy-instances-fn service (index-fn service))
                                                grace-period-mins (t/minutes (Integer/parseInt (str/replace service "service-" "")))]
                                            (filter #(not (utils/older-than? current-time grace-period-mins %)) unhealthy-instances)))
                                        expected-services)
                                      :service-id->my-instance->slots
                                      (pc/map-from-keys
                                        (fn [service]
                                          (let [healthy-instances (healthy-instances-fn service (index-fn service) n)
                                                my-instances (second (first (slot-partition-fn routers healthy-instances)))]
                                            my-instances))
                                        expected-services)
                                      :routers routers
                                      :time current-time})
                    state (async/<!! router-state-push-chan)
                    actual-state (dissoc state :iteration)]
                (when (not= expected-state actual-state)
                  (clojure.pprint/pprint (clojure.data/diff expected-state actual-state)))
                (is (= expected-state actual-state) (str (clojure.data/diff expected-state actual-state))))))
          (async/>!! exit-chan :exit)))))

  (testing "router-state-maintainer-deployment-errors-updated"
    (let [scheduler-state-chan (async/chan 1)
          router-chan (async/chan 1)
          num-message-iterations 20
          router-state-push-chan (async/chan 1)
          exit-chan (async/chan 1)
          concurrency-level 1
          slot-partition-fn (fn [routers instances]
                              (pc/map-vals
                                (fn [my-instances] (into {} (map (fn [instance] {instance concurrency-level}) my-instances)))
                                (zipmap
                                  routers
                                  (let [instances-partition (partition (quot (inc (count instances)) (count routers)) instances)]
                                    (map #(sort (:id (remove nil? %))) instances-partition)))))
          router-id "router.0"
          routers {router-id (str "http://www." router-id ".com")
                   "router.1" (str "http://www.router.1.com")}
          services-fn #(vec (map (fn [i] (str "service-" i)) (range (inc (if (> % (/ num-message-iterations 2)) (- % 5) %)))))
          grace-period-secs 100
          service-id->service-description-fn (fn [id] (let [service-num (Integer/parseInt (str/replace id "service-" ""))]
                                                        {"concurrency-level" concurrency-level
                                                         "instance-expiry-mins" service-num
                                                         "grace-period-secs" grace-period-secs}))
          deployment-error-config {:grace-period-ms (* grace-period-secs 1000)
                                   :min-failed-instances 1
                                   :min-hosts 1}]
      (with-redefs [distribute-slots-using-consistent-hash-distribution (fn [routers instances _ _ _] (slot-partition-fn routers instances))]

        (let [{:keys [router-state-push-mult]} (start-router-state-maintainer scheduler-state-chan router-chan router-id exit-chan service-id->service-description-fn deployment-error-config)]
          (async/tap router-state-push-mult router-state-push-chan))

        (async/>!! router-chan routers)
        (is (= routers (:routers (async/<!! router-state-push-chan))))

        (let [start-time (t/now)
              unhealthy-health-check-statuses [400 401 402]
              unhealthy-instances-fn (fn [service-id index]
                                       (vec (map (fn [x] {:id (str service-id "." x "1")
                                                          :health-check-status (get unhealthy-health-check-statuses (mod index (count unhealthy-health-check-statuses)))
                                                          :started-at (f/unparse (f/formatters :date-time) start-time)})
                                                 (range (if (zero? (mod index 2)) 1 0)))))
              failed-messages [{:message nil} {:message nil} {:message nil} {:message "Memory limit exceeded:" :flags #{:memory-limit-exceeded}}
                               {:message nil :flags #{:never-passed-health-checks}} {:message "Command exited with status" :exit-code 1}
                               {:message nil :flags #{:connect-exception}} {:flags #{:timeout-exception :never-passed-health-checks}}]
              failed-instances-fn (fn [service-id index]
                                    (vec (map (fn [x] (merge (get failed-messages (mod index (count failed-messages)))
                                                        {:id (str service-id "." x "1")
                                                         :started-at (f/unparse (f/formatters :date-time) start-time)}))
                                              (range (if (zero? (mod index 2)) 1 0)))))
              deployment-error-fn (fn [service-id index]
                                    (get-deployment-error [] (unhealthy-instances-fn service-id index) (failed-instances-fn service-id index) deployment-error-config))]
          (dotimes [n num-message-iterations]
            (let [current-time (t/plus start-time (t/minutes n))]
              (let [services (services-fn n)]
                (loop [index 0
                       scheduler-messages [[:update-available-apps {:available-apps services
                                                                    :scheduler-sync-time current-time}]]]
                  (if (>= index (count services))
                    (async/>!! scheduler-state-chan scheduler-messages)
                    (let [service-id (str "service-" index)
                          failed-instances (failed-instances-fn service-id index)
                          unhealthy-instances (unhealthy-instances-fn service-id index)
                          service-instances-message [:update-app-instances
                                                     (assoc {:healthy-instances [] ; no healthy instances
                                                             :unhealthy-instances unhealthy-instances}
                                                       :service-id service-id
                                                       :failed-instances failed-instances
                                                       :scheduler-sync-time current-time)]]
                      (recur (inc index) (conj scheduler-messages service-instances-message))))))
              (let [expected-services (services-fn n)
                    expected-state (let [index-fn #(Integer/parseInt (subs % (inc (.lastIndexOf ^String % "-"))))]
                                     {:service-id->unhealthy-instances
                                               (zipmap expected-services
                                                       (map #(unhealthy-instances-fn % (index-fn %)) expected-services))
                                      :service-id->failed-instances
                                               (zipmap expected-services
                                                       (map #(failed-instances-fn % (index-fn %)) expected-services))
                                      :service-id->deployment-error
                                               (into {} (filter second (zipmap expected-services
                                                                               (map #(deployment-error-fn % (index-fn %)) expected-services))))})
                    state (async/<!! router-state-push-chan)
                    actual-state (dissoc state :iteration :service-id->healthy-instances :service-id->expired-instances :service-id->starting-instances
                                         :service-id->my-instance->slots :routers :time)]
                (when (not= expected-state actual-state)
                  (clojure.pprint/pprint (clojure.data/diff expected-state actual-state)))
                (is (= expected-state actual-state) (str (clojure.data/diff expected-state actual-state))))))
          (async/>!! exit-chan :exit))))))

(deftest test-retrieve-peer-routers
  (testing "successful-retrieval-from-discovery"
    (with-redefs [discovery/router-id->endpoint-url (constantly {"router-1" "url-1", "router-2" "url-2"})]
      (let [discovery (Object.)
            router-chan (async/chan 1)]
        (retrieve-peer-routers discovery router-chan)
        (is (= {"router-1" "url-1", "router-2" "url-2"} (async/<!! router-chan))))))
  (testing "exception-on-retrieval-from-discovery"
    (with-redefs [discovery/router-id->endpoint-url (fn [_ _ _]
                                                      (throw (RuntimeException. "Expected exception thrown from test")))]
      (let [discovery (Object.)
            router-chan (async/chan 1)]
        (is (thrown-with-msg? RuntimeException #"Expected exception thrown from test"
                              (retrieve-peer-routers discovery router-chan)))))))

(defmacro check-service-maintainer-state-fn
  [query-state-chan service-id expected-state]
  `(do
     (let [query-state-response-chan# (async/chan 1)]
       (async/>!! ~query-state-chan {:response-chan query-state-response-chan#, :service-id ~service-id})
       (let [actual-state# (async/<!! query-state-response-chan#)
             check-fn# (fn [item-key#]
                         (let [expected# (item-key# ~expected-state)
                               actual# (item-key# actual-state#)]
                           (when (not (nil? expected#))
                             (when (not= expected# actual#)
                               (let [sanitize-data# (fn [data#] (cond->> data# (map? data#) (into (sorted-map))))]
                                 (log/info (first *testing-vars*) ":" (name item-key#))
                                 (log/info "Expected: " (sanitize-data# expected#))
                                 (log/info "Actual:   " (sanitize-data# actual#))))
                             (is (= expected# actual#) (str "Checking: " (name item-key#))))))]
         (check-fn# :service-id->channel-map)
         (check-fn# :maintainer-chan-available)
         (check-fn# :last-state-update-time)
         actual-state#))))

(let [service-channel-map-atom (atom {})
      start-service (fn [service-id]
                      (swap! service-channel-map-atom assoc service-id {:update-state-chan (au/latest-chan)})
                      {:channel-map-for service-id})
      remove-service (fn [service-id {:keys [channel-map-for]}]
                       (is (= service-id channel-map-for))
                       (swap! service-channel-map-atom dissoc service-id))
      retrieve-channel (fn [{:keys [channel-map-for]} method]
                         (if (= :update-state method)
                           (get-in @service-channel-map-atom [channel-map-for :update-state-chan])
                           (str channel-map-for ":" method)))]

  (deftest test-start-service-chan-maintainer-initialization
    (testing "test-start-service-chan-maintainer-initialization"
      (let [state-source-chan (async/chan)
            request-chan (async/chan 1)
            query-chan (async/chan 1)
            {:keys [exit-chan]} (start-service-chan-maintainer {} request-chan state-source-chan query-chan
                                                               start-service remove-service retrieve-channel)]
        (check-service-maintainer-state-fn query-chan nil {:service-id->channel-map {}, :last-state-update-time nil})
        (async/>!! exit-chan :exit))))

  (deftest test-start-service-chan-maintainer-start-services
    (testing "test-start-service-chan-maintainer-start-services"
      (let [state-source-chan (async/chan)
            request-chan (async/chan 1)
            query-chan (async/chan 1)
            current-time (t/now)
            initial-state {}
            {:keys [exit-chan]} (start-service-chan-maintainer initial-state request-chan state-source-chan query-chan
                                                               start-service remove-service retrieve-channel)]

        (let [state-map {:service-id->my-instance->slots {"service-1" {"service-1.A" 1, "service-1.B" 2}
                                                          "service-2" {"service-2.A" 3}
                                                          "service-3" {"service-3.A" 6, "service-3.B" 8}}
                         :service-id->unhealthy-instances {}
                         :service-id->expired-instances {}
                         :service-id->starting-instances {}
                         :service-id->sorted-instance-ids {"service-1" ["service-1.A" "service-1.B"]
                                                           "service-2" ["service-2.A"]
                                                           "service-3" ["service-3.A" "service-3.B"]}
                         :time current-time}]
          (async/>!! state-source-chan state-map))

        (check-service-maintainer-state-fn
          query-chan nil
          {:service-id->channel-map {"service-1" {:channel-map-for "service-1"}
                                     "service-2" {:channel-map-for "service-2"}
                                     "service-3" {:channel-map-for "service-3"}}
           :last-state-update-time current-time})

        (async/>!! exit-chan :exit))))

  (deftest test-start-service-chan-maintainer-remove-services
    (testing "test-start-service-chan-maintainer-remove-services"
      (let [state-source-chan (async/chan)
            request-chan (async/chan 1)
            query-chan (async/chan 1)
            current-time (t/now)
            initial-state {:service-id->channel-map {"service-1" {:channel-map-for "service-1"}
                                                     "service-2" {:channel-map-for "service-2"}
                                                     "service-3" {:channel-map-for "service-3"}}
                           :last-state-update-time (t/minus current-time (t/seconds 10))}
            {:keys [exit-chan]} (start-service-chan-maintainer initial-state request-chan state-source-chan query-chan
                                                               start-service remove-service retrieve-channel)]
        (doseq [service-id ["service-1" "service-2" "service-3"]]
          (start-service service-id))

        (let [state-map {:service-id->my-instance->slots {"service-1" {"service-1.A" 1, "service-1.B" 2}
                                                          "service-3" {"service-3.A" 6, "service-3.B" 8}}
                         :service-id->unhealthy-instances {}
                         :service-id->expired-instances {}
                         :service-id->starting-instances {}
                         :service-id->sorted-instance-ids {"service-1" ["service-1.A" "service-1.B"]
                                                           "service-3" ["service-3.A" "service-3.B"]}
                         :time current-time}]
          (async/>!! state-source-chan state-map))

        (check-service-maintainer-state-fn
          query-chan nil
          {:service-id->channel-map {"service-1" {:channel-map-for "service-1"}
                                     "service-3" {:channel-map-for "service-3"}}
           :last-state-update-time current-time})

        (async/>!! exit-chan :exit))))

  (deftest test-start-service-chan-maintainer-start-and-remove-services
    (testing "test-start-service-chan-maintainer-start-and-remove-services"
      (let [state-source-chan (async/chan)
            request-chan (async/chan 1)
            query-chan (async/chan 1)
            current-time (t/now)
            initial-state {:service-id->channel-map {"service-1" {:channel-map-for "service-1"}
                                                     "service-2" {:channel-map-for "service-2"}
                                                     "service-3" {:channel-map-for "service-3"}}
                           :last-state-update-time (t/minus current-time (t/seconds 10))}
            {:keys [exit-chan]} (start-service-chan-maintainer initial-state request-chan state-source-chan query-chan
                                                               start-service remove-service retrieve-channel)]
        (doseq [service-id ["service-1" "service-2" "service-3"]]
          (start-service service-id))

        (let [state-map {:service-id->my-instance->slots {"service-1" {"service-1.A" 11, "service-1.B" 12}
                                                          "service-3" {"service-3.A" 6, "service-3.B" 8}
                                                          "service-4" {"service-4.B" 3}
                                                          "service-5" {"service-5.A" 5}}
                         :service-id->unhealthy-instances {}
                         :service-id->expired-instances {}
                         :service-id->starting-instances {}
                         :time current-time}]
          (async/>!! state-source-chan state-map))

        (check-service-maintainer-state-fn
          query-chan nil
          {:service-id->channel-map {"service-1" {:channel-map-for "service-1"}
                                     "service-3" {:channel-map-for "service-3"}
                                     "service-4" {:channel-map-for "service-4"}
                                     "service-5" {:channel-map-for "service-5"}}
           :last-state-update-time current-time})

        (is (= [{:healthy-instances ["service-1.A" "service-1.B"]
                 :expired-instances nil
                 :unhealthy-instances nil
                 :starting-instances nil
                 :my-instance->slots {"service-1.A" 11, "service-1.B" 12}
                 :deployment-error nil}
                current-time]
               (async/<!! (retrieve-channel {:channel-map-for "service-1"} :update-state))))
        (is (= [{:healthy-instances ["service-3.A" "service-3.B"]
                 :expired-instances nil
                 :unhealthy-instances nil
                 :starting-instances nil
                 :my-instance->slots {"service-3.A" 6, "service-3.B" 8}
                 :deployment-error nil}
                current-time]
               (async/<!! (retrieve-channel {:channel-map-for "service-3"} :update-state))))
        (is (= [{:healthy-instances ["service-4.B"]
                 :expired-instances nil
                 :unhealthy-instances nil
                 :starting-instances nil
                 :my-instance->slots {"service-4.B" 3}
                 :deployment-error nil}
                current-time]
               (async/<!! (retrieve-channel {:channel-map-for "service-4"} :update-state))))
        (is (= [{:healthy-instances ["service-5.A"]
                 :expired-instances nil
                 :unhealthy-instances nil
                 :starting-instances nil
                 :my-instance->slots {"service-5.A" 5}
                 :deployment-error nil}
                current-time]
               (async/<!! (retrieve-channel {:channel-map-for "service-5"} :update-state))))

        (async/>!! exit-chan :exit))))

  (deftest test-start-service-chan-maintainer-request-channel
    (testing "test-start-service-chan-maintainer-request-channel"
      (let [state-source-chan (async/chan)
            request-chan (async/chan 1)
            query-chan (async/chan 1)
            current-time (t/now)
            initial-state {:service-id->channel-map {"service-1" {:channel-map-for "service-1"}
                                                     "service-2" {:channel-map-for "service-2"}
                                                     "service-3" {:channel-map-for "service-3"}}
                           :last-state-update-time (t/minus current-time (t/seconds 10))}
            {:keys [exit-chan]} (start-service-chan-maintainer initial-state request-chan state-source-chan query-chan
                                                               start-service remove-service retrieve-channel)]
        (doseq [service-id ["service-1" "service-2" "service-3"]]
          (start-service service-id))

        (doseq [service-id ["service-2" "service-1" "service-3"]]
          (let [response-chan (async/promise-chan)]
            (->> {:cid "cid"
                  :method :method
                  :response-chan response-chan
                  :service-id service-id}
                 (async/>!! request-chan))
            (is (= (str service-id "::method") (async/<!! response-chan)))))

        (async/>!! exit-chan :exit)))))

(deftest test-trigger-unblacklist-process
  (let [correlation-id "test-correlation-id"
        test-instance-id "test-instance-id"
        unblacklist-instance-chan (async/chan 1)
        blacklist-period-ms 200
        current-time (t/now)]
    (with-redefs [t/now (fn [] current-time)]
      (trigger-unblacklist-process correlation-id test-instance-id blacklist-period-ms unblacklist-instance-chan))
    (let [{:keys [instance-id]} (async/<!! unblacklist-instance-chan)
          received-time (t/now)]
      (is (= test-instance-id instance-id))
      (is (not (t/before? received-time (t/plus current-time (t/millis blacklist-period-ms))))))))
