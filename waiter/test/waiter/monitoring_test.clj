;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.monitoring-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [waiter.monitoring :refer :all]
            [waiter.test-helpers :as test-helpers]))

(deftest test-any-thread-stack-stale
  (let [stale-threshold-ms 125
        current-time (t/now)
        clock (fn [] current-time)]
    (testing "nil-thread-id->data"
      (is (empty? (any-thread-stack-stale stale-threshold-ms clock nil))))
    (testing "empty-thread-id->data"
      (is (empty? (any-thread-stack-stale stale-threshold-ms clock {}))))
    (testing "no-stale-stack-in-unit-thread-id->data"
      (is (empty? (any-thread-stack-stale
                    stale-threshold-ms clock
                    {"thread-1" {:last-modified-time current-time, :stack-trace ["foo" "bar" "baz"]}}))))
    (testing "stale-stack-in-unit-thread-id->data"
      (is (= {"thread-1" {:last-modified-time (t/minus current-time (t/millis 150)), :stack-trace ["foo" "bar" "baz"]}}
             (any-thread-stack-stale
               stale-threshold-ms clock
               {"thread-1" {:last-modified-time (t/minus current-time (t/millis 150)), :stack-trace ["foo" "bar" "baz"]}}))))
    (testing "stale-stack-in-thread-id->data"
      (is (= {"thread-2" {:last-modified-time (t/minus current-time (t/millis 150)), :stack-trace ["fo2" "ba2" "ba2"]}
              "thread-5" {:last-modified-time (t/minus current-time (t/millis 200)), :stack-trace ["fo4", "ba4"]}}
             (any-thread-stack-stale
               stale-threshold-ms clock
               {"thread-1" {:last-modified-time (t/minus current-time (t/millis 100)), :stack-trace ["fo1" "ba1" "ba1"]}
                "thread-2" {:last-modified-time (t/minus current-time (t/millis 150)), :stack-trace ["fo2" "ba2" "ba2"]}
                "thread-3" {:last-modified-time current-time, :stack-trace ["fo3"]}
                "thread-4" {:last-modified-time (t/minus current-time (t/millis 50)), :stack-trace ["fo4", "ba4"]}
                "thread-5" {:last-modified-time (t/minus current-time (t/millis 200)), :stack-trace ["fo4", "ba4"]}}))))
    (testing "no-stale-stack-in-thread-id->data"
      (is (empty?
            (any-thread-stack-stale
              stale-threshold-ms clock
              {"thread-1" {:last-modified-time (t/minus current-time (t/millis 100)), :stack-trace ["fo1" "ba1" "ba1"]}
               "thread-2" {:last-modified-time (t/minus current-time (t/millis 110)), :stack-trace ["fo2" "ba2" "ba2"]}
               "thread-3" {:last-modified-time current-time, :stack-trace ["fo3"]}
               "thread-4" {:last-modified-time (t/minus current-time (t/millis 50)), :stack-trace ["fo4", "ba4"]}
               "thread-5" {:last-modified-time (t/minus current-time (t/millis 120)), :stack-trace ["fo4", "ba4"]}}))))))

(deftest test-filter-trace-map
  (let [excluded-methods ["java.lang.Object.wait"
                          "java.lang.Thread.dumpThreads"
                          "sun.misc.Unsafe.park"
                          "sun.nio.ch.EPollArrayWrapper.epollWait"
                          "sun.nio.ch.ServerSocketChannelImpl.accept"
                          "sun.nio.ch.ServerSocketChannelImpl.accept0"]
        filter-trace-map' #(into {} (filter-trace-map %1 %2))]
    (testing "nil-input"
      (is (= {} (filter-trace-map' excluded-methods nil))))
    (testing "empty-input"
      (is (= {} (filter-trace-map' excluded-methods {}))))
    (testing "nil-stack-trace-input"
      (is (= {} (filter-trace-map' excluded-methods {"t1" {}}))))
    (testing "empty-stack-trace-input"
      (is (= {} (filter-trace-map' excluded-methods {"t1" {:stack-trace {}}}))))
    (testing "allow-all-nil-excluded-methods"
      (is (= {"t1" {:stack-trace ["foo()" "bar()"]}
              "t2" {:stack-trace ["baz()"]}}
             (filter-trace-map' nil {"t1" {:stack-trace ["foo()" "bar()"]}
                                     "t2" {:stack-trace ["baz()"]}}))))
    (testing "allow-all-empty-excluded-methods"
      (is (= {"t1" {:stack-trace ["foo()" "bar()"]}
              "t2" {:stack-trace ["baz()"]}}
             (filter-trace-map' []
                                {"t1" {:stack-trace ["foo()" "bar()"]}
                                 "t2" {:stack-trace ["baz()"]}
                                 "t5" {:stack-trace []}}))))
    (testing "filter-none"
      (is (= {"t1" {:stack-trace ["foo()" "bar()"]}
              "t2" {:stack-trace ["baz()"]}}
             (filter-trace-map' excluded-methods
                                {"t1" {:stack-trace ["foo()" "bar()"]}
                                 "t2" {:stack-trace ["baz()"]}}))))
    (testing "filter-all"
      (is (= {}
             (filter-trace-map' excluded-methods
                                {"t1" {:stack-trace ["java.lang.Object.wait()" "foo()" "bar()"]}
                                 "t2" {:stack-trace ["sun.misc.Unsafe.park()" "baz()"]}}))))
    (testing "filter-some"
      (is (= {"t2" {:stack-trace ["foo()" "bar()"]}
              "t4" {:stack-trace ["java.lang.Thread.run()"]}}
             (filter-trace-map' excluded-methods
                                {"t1" {:stack-trace ["java.lang.Object.wait()" "foo()" "bar()"]}
                                 "t2" {:stack-trace ["foo()" "bar()"]}
                                 "t3" {:stack-trace ["java.lang.Thread.dumpThreads()"]}
                                 "t4" {:stack-trace ["java.lang.Thread.run()"]}
                                 "t5" {:stack-trace []}
                                 "t6" {:stack-trace ["sun.misc.Unsafe.park()" "baz()"]}
                                 "t7" {:stack-trace ["sun.nio.ch.EPollArrayWrapper.epollWait()"]}
                                 "t8" {:stack-trace ["sun.nio.ch.ServerSocketChannelImpl.accept()" "java.lang.Thread.run()"]}
                                 "t9" {:stack-trace ["sun.nio.ch.ServerSocketChannelImpl.accept0()"]}}))))))

(deftest test-thread-stack-tracker
  (let [thread-stack-state-refresh-interval-ms 10
        start-time (t/now)
        start-time-plus-10-secs (t/plus start-time (t/seconds 10))
        start-time-plus-20-secs (t/plus start-time (t/seconds 20))
        start-time-plus-30-secs (t/plus start-time (t/seconds 30))
        current-time-atom (atom start-time)
        clock (fn [] @current-time-atom)
        thread-id->stack-trace-atom (atom {})
        get-all-stack-traces (fn [] @thread-id->stack-trace-atom)
        state-store-atom (atom {})
        cancel-handle (thread-stack-tracker state-store-atom thread-stack-state-refresh-interval-ms get-all-stack-traces clock)]
    ; initially empty
    (is (test-helpers/wait-for
          #(= {} (:thread-id->stack-trace-state @state-store-atom))
          :interval 10, :timeout 100, :unit-multiplier 1)
        (str @state-store-atom))

    (reset! current-time-atom start-time-plus-10-secs)
    (reset! thread-id->stack-trace-atom {"t1" ["foo()" "bar()"], "t2" ["baz()"]})
    (is (test-helpers/wait-for
          #(= {"t1" {:last-modified-time start-time-plus-10-secs, :stack-trace ["foo()" "bar()"]}
               "t2" {:last-modified-time start-time-plus-10-secs, :stack-trace ["baz()"]}}
              (:thread-id->stack-trace-state @state-store-atom))
          :interval 10, :timeout 100, :unit-multiplier 1)
        (str @state-store-atom))

    (reset! current-time-atom start-time-plus-20-secs)
    (reset! thread-id->stack-trace-atom {"t1" ["fuu()" "foo()" "bar()"], "t2" ["baz()"], "t3" ["foo()" "baz()"]})
    (is (test-helpers/wait-for
          #(= {"t1" {:last-modified-time start-time-plus-20-secs, :stack-trace ["fuu()" "foo()" "bar()"]}
               "t2" {:last-modified-time start-time-plus-10-secs, :stack-trace ["baz()"]}
               "t3" {:last-modified-time start-time-plus-20-secs, :stack-trace ["foo()" "baz()"]}}
              (:thread-id->stack-trace-state @state-store-atom))
          :interval 10, :timeout 100, :unit-multiplier 1)
        (str @state-store-atom))

    (reset! current-time-atom start-time-plus-30-secs)
    (reset! thread-id->stack-trace-atom {"t1" ["fuu()" "foo()" "bar()"], "t2" ["baz()"], "t3" ["foo()" "baz()"]})
    (is (test-helpers/wait-for
          #(= {"t1" {:last-modified-time start-time-plus-20-secs, :stack-trace ["fuu()" "foo()" "bar()"]}
               "t2" {:last-modified-time start-time-plus-10-secs, :stack-trace ["baz()"]}
               "t3" {:last-modified-time start-time-plus-20-secs, :stack-trace ["foo()" "baz()"]}}
              (:thread-id->stack-trace-state @state-store-atom))
          :interval 10, :timeout 100, :unit-multiplier 1)
        (str @state-store-atom))

    (cancel-handle)))

(deftest test-retrieve-stale-thread-stack-trace-data
  (testing "retrieve-stale-thread-stack-trace-data"
    (let [stale-threshold-ms 15000
          start-time (t/now)
          start-time-plus-10-secs (t/plus start-time (t/seconds 10))
          start-time-plus-20-secs (t/plus start-time (t/seconds 20))
          start-time-plus-30-secs (t/plus start-time (t/seconds 30))
          clock (constantly start-time-plus-30-secs)
          state-store-atom (atom nil)]
      (reset! state-store-atom
              {:iteration 1
               :thread-id->stack-trace-state {"t1" {:last-modified-time start-time, :stack-trace ["fuu()" "foo()" "bar()"]}
                                              "t2" {:last-modified-time start-time-plus-10-secs, :stack-trace ["baz()"]}
                                              "t3" {:last-modified-time start-time-plus-20-secs, :stack-trace ["foo()" "baz()"]}}})
      (let [actual-result (retrieve-stale-thread-stack-trace-data state-store-atom clock [] stale-threshold-ms)]
        (is (= {"t1" {:last-modified-time start-time, :stack-trace ["fuu()" "foo()" "bar()"]}
                "t2" {:last-modified-time start-time-plus-10-secs, :stack-trace ["baz()"]}}
               actual-result))))))