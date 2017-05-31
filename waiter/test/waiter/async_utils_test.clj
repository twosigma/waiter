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
(ns waiter.async-utils-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [full.async :refer (<?? <? go-try)]
            [metrics.histograms :as histograms]
            [waiter.async-utils :refer :all]))

(deftest test-sliding-buffer-chan
  (let [buf-size 4
        test-chan (sliding-buffer-chan buf-size)
        limit 10]
    (dotimes [n limit]
      (async/>!! test-chan n))
    (dotimes [n buf-size]
      (is (= (+ n (- limit buf-size)) (async/<!! test-chan))))))

(deftest test-timing-out-pipeline
  (let [fail false
        in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn priority-fn [_] nil)
                                 (fn af [c {:keys [id resp-chan timeout-ms]
                                            :or {timeout-ms 100}}]
                                   (async/go
                                     (let [timeout (async/timeout timeout-ms)]
                                       (async/alt!
                                         c ([_] :do-nothing)
                                         timeout ([_] (async/>! c {:id id :resp-chan resp-chan}))))))
                                 (fn ex-handler [ex] (log/info ex)))]
    (async/<!!
      (async/go
        (let [req1 {:id 1 :value 1 :resp-chan (async/chan)}
              req2 {:id 2 :value 2 :resp-chan (async/chan) :timeout-ms 0}
              req3 {:id 3 :value 3 :resp-chan (async/chan)}
              req4 {:id 4 :value 4 :resp-chan (async/chan) :timeout-ms 0}
              req5 {:id 5 :value 5 :resp-chan (async/chan)}]
          (async/>! in req1)
          (async/>! in req2)
          (async/>! in req3)
          (async/>! in req4)
          (async/>! in req2) ; duplicate req2
          (async/<! (async/timeout 50))

          ; should receive a 1 from other end of pipeline (out)
          (async/alt!
            (async/timeout 200) ([_] (is fail "Should not have timed out"))
            out ([{:keys [value]}] (is (= 1 value))))

          (async/>! in req5)

          ; should receive a 3 from other end of pipeline (out)
          (async/alt!
            (async/timeout 200) ([_] (is fail "Should not have timed out"))
            out ([{:keys [value]}] (is (= 3 value))))

          ; should receive a 5 from other end of pipeline (out)
          (async/alt!
            (async/timeout 200) ([_] (is fail "Should not have timed out"))
            out ([{:keys [value]}] (is (= 5 value))))

          ; should not receive another value from pipeline (out)
          (async/alt!
            (async/timeout 200) ([_] :pass)
            out ([{:keys [value]}] (is fail "Should not have received a value")))

          ; req1 channel should be open
          (async/alt!
            (async/timeout 200) ([_] :open)
            (:resp-chan req1) ([_] (is fail "Channel is closed")))

          ; req2 channel should be closed
          (async/alt!
            (async/timeout 200) ([_] (is fail "Channel is open"))
            (:resp-chan req2) ([_] :closed))

          ; req3 channel should be open
          (async/alt!
            (async/timeout 200) ([_] :open)
            (:resp-chan req3) ([_] (is fail "Channel is closed")))

          ; req4 channel should be closed
          (async/alt!
            (async/timeout 200) ([_] (is fail "Channel is open"))
            (:resp-chan req4) ([_] :closed))

          ; req5 channel should be closed
          (async/alt!
            (async/timeout 200) ([_] :open)
            (:resp-chan req5) ([_] (is fail "Channel is closed"))))))))

(defmacro test-priority-data
  [out-chan expected-id]
  `(let [[data# channel#] (async/alts!! [(async/timeout 200) ~out-chan])]
     (is (= ~out-chan channel#) "Should not have timed out")
     (is (if (set? ~expected-id)
           (contains? ~expected-id (:id data#))
           (= ~expected-id (:id data#)))
         (str "Expected: " ~expected-id " Actual: " (:id data#)))
     data#))

(deftest test-timing-out-pipeline-prioritized-requests
  (let [in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn priority-fn [{:keys [priority]}]
                                   (when priority (unchecked-negate-int priority)))
                                 (constantly true)
                                 (fn ex-handler [ex] (log/info ex)))]
    (async/>!! in {:id 1 :priority 1}) ;; [] and [1]
    (test-priority-data out 1) ;; []
    (async/>!! in {:id 2 :priority 9}) ;; [] and [2]
    (async/>!! in {:id 3 :priority 8}) ;; [] and [3 2]
    (async/>!! in {:id 4 :priority 7}) ;; [] and [4 3 2]
    (test-priority-data out 4) ;; [3 2]
    (test-priority-data out 3) ;; [2]
    (async/>!! in {:id 5 :priority 3}) ;; [] and [5 2]
    (async/>!! in {:id 6 :priority 5}) ;; [] and [5 6 2]
    (async/>!! in {:id 7 :priority 7}) ;; [] and [5 6 7 2]
    (test-priority-data out 5) ;; [] and [6 7 2]
    (test-priority-data out 6) ;; [] and [7 2]
    (test-priority-data out 7) ;; [] and [2]
    (test-priority-data out 2) ;; [] and []
    (async/>!! in {:id 8 :priority 3}) ;; [] and [8]
    (async/>!! in {:id 9 :priority 1}) ;; [] and [9 8]
    (async/>!! in {:id 10 :priority 5}) ;; [] and [9 8 10]
    (test-priority-data out 9) ;; [8 10]
    (test-priority-data out 8) ;; [10]
    (test-priority-data out 10)))

(deftest test-timing-out-pipeline-regular-requests
  (let [in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn priority-fn [{:keys [priority]}]
                                   (when priority (unchecked-negate-int priority)))
                                 (constantly true)
                                 (fn ex-handler [ex] (log/info ex)))]
    (async/>!! in {:id 1}) ;; [1] and []
    (test-priority-data out 1) ;; [] and []
    (async/>!! in {:id 2}) ;; [2] and []
    (async/>!! in {:id 3}) ;; [2 3] and []
    (async/>!! in {:id 4}) ;; [2 3 4] and []
    (test-priority-data out 2) ;; [3 4] and []
    (test-priority-data out 3) ;; [4] and []
    (async/>!! in {:id 5}) ;; [4 5] and []
    (async/>!! in {:id 6}) ;; [4 5 6] and []
    (async/>!! in {:id 7}) ;; [4 5 6 7] and []
    (test-priority-data out 4) ;; [5 6 7] and []
    (test-priority-data out 5) ;; [6 7] and []
    (test-priority-data out 6) ;; [7] and []
    (test-priority-data out 7) ;; [] and []
    (async/>!! in {:id 8}) ;; [8] and []
    (async/>!! in {:id 9}) ;; [8 9] and []
    (async/>!! in {:id 10}) ;; [8 9 10] and []
    (test-priority-data out 8) ;; [9 10] and []
    (test-priority-data out 9) ;; [10] and []
    (test-priority-data out 10)))

(deftest test-timing-out-pipeline-regular-and-prioritized-requests
  (let [in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn priority-fn [{:keys [priority]}]
                                   (when priority (unchecked-negate-int priority)))
                                 (constantly true)
                                 (fn ex-handler [ex] (log/info ex)))]
    (async/>!! in {:id 1 :priority 1}) ;; [] and [1]
    (test-priority-data out 1) ;; []
    (async/>!! in {:id 2 :priority 9}) ;; [] and [2]
    (async/>!! in {:id 3}) ;; [3] and [2]
    (async/>!! in {:id 4 :priority 7}) ;; [3] and [4 2]
    (test-priority-data out 3) ;; [] and [4 2]
    (test-priority-data out 4) ;; [] and [2]
    (async/>!! in {:id 5 :priority 3}) ;; [] and [5 2]
    (async/>!! in {:id 6}) ;; [6] and [5 2]
    (async/>!! in {:id 7 :priority 7}) ;; [6] and [5 7 2]
    (async/>!! in {:id 8}) ;; [6 8] and [5 7 2]
    (test-priority-data out 6) ;; [8] and [5 7 2]
    (test-priority-data out 8) ;; [] and [5 7 2]
    (test-priority-data out 5) ;; [] and [7 2]
    (test-priority-data out 7) ;; [] and [2]
    (async/>!! in {:id 9 :priority 1}) ;; [] and [9 2]
    (async/>!! in {:id 10}) ;; [10] and [9 2]
    (test-priority-data out 10) ;; [] and [9 2]
    (test-priority-data out 9) ;; [] and [2]
    (test-priority-data out 2)))

(deftest test-timing-out-pipeline-regular-and-prioritized-request-timeouts
  (let [in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn priority-fn [{:keys [priority]}]
                                   (when priority (unchecked-negate-int priority)))
                                 (fn af [c {:keys [id resp-chan timeout-ms]
                                            :or {timeout-ms 1000}}]
                                   (async/go
                                     (let [timeout (async/timeout timeout-ms)]
                                       (async/alt!
                                         c ([_] :do-nothing)
                                         timeout ([_] (async/>! c {:id id :resp-chan resp-chan}))))))
                                 (fn ex-handler [ex] (log/info ex)))
        req6-chan (async/promise-chan)
        req7-chan (async/promise-chan)
        req9-chan (async/promise-chan)]
    (async/>!! in {:id 1 :priority 1}) ;; [] and [1]
    (test-priority-data out 1) ;; []
    (async/>!! in {:id 2 :priority 9}) ;; [] and [2]
    (async/>!! in {:id 3}) ;; [3] and [2]
    (async/>!! in {:id 4 :priority 7}) ;; [3] and [4 2]
    (test-priority-data out 3) ;; [] and [4 2]
    (test-priority-data out 4) ;; [] and [2]
    (async/>!! in {:id 5 :priority 3}) ;; [] and [5 2]
    (async/>!! in {:id 6 :resp-chan req6-chan :timeout-ms 1}) ;; [6] and [5 2]
    (async/>!! in {:id 7 :priority 7 :resp-chan req7-chan :timeout-ms 1}) ;; [6] and [5 7 2]
    (async/<!! (async/timeout 100)) ;; timeout requests, [] and [5 2]
    (is (nil? (async/<!! req6-chan)))
    (is (nil? (async/<!! req7-chan)))
    (async/>!! in {:id 8}) ;; [8] and [5 2]
    (test-priority-data out 8) ;; [8] and [5 2]
    (test-priority-data out 5) ;; [] and [2]
    (async/>!! in {:id 9 :priority 1 :resp-chan req9-chan :timeout-ms 1}) ;; [] and [9 2]
    (async/>!! in {:id 10}) ;; [10] and [9 2]
    (async/<!! (async/timeout 100)) ;; timeout requests, [10] and [2]
    (is (nil? (async/<!! req9-chan)))
    (test-priority-data out 10) ;; [] and [2]
    (test-priority-data out 2)))

(deftest test-timing-out-pipeline-equal-priorities
  (let [in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn priority-fn [{:keys [priority]}]
                                   (when priority (unchecked-negate-int priority)))
                                 (constantly true)
                                 (fn ex-handler [ex] (log/info ex)))
        out-ids-atom (atom #{})]
    (async/>!! in {:id 1 :priority 1}) ;; [1]
    (async/>!! in {:id 2 :priority 1}) ;; [1 2]
    (async/>!! in {:id 3 :priority 1}) ;; [1 2 3]
    (async/>!! in {:id 4 :priority 2}) ;; [1 2 3; 4]
    (async/>!! in {:id 5 :priority 2}) ;; [1 2 3; 4 5]
    (dotimes [_ 3]
      (let [{:keys [id]} (test-priority-data out #{1 2 3})]
        (swap! out-ids-atom conj id)))
    (is (= #{1 2 3} @out-ids-atom))
    (reset! out-ids-atom #{})
    (dotimes [_ 2]
      (let [{:keys [id]} (test-priority-data out #{4 5})]
        (swap! out-ids-atom conj id)))
    (is (= #{4 5} @out-ids-atom))))

(deftest test-timing-out-pipeline-equal-priorities-fifo-ordering
  (let [in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn priority-fn [{:keys [priority]}] priority)
                                 (constantly true)
                                 (fn ex-handler [ex] (log/info ex)))
        out-ids-atom (atom #{})]
    (async/>!! in {:id 1 :priority [5 -1]}) ;; [1]
    (async/>!! in {:id 2 :priority [4 -2]}) ;; [1; 2]
    (async/>!! in {:id 3 :priority [5 -3]}) ;; [1 3; 2]
    (async/>!! in {:id 4 :priority [4 -4]}) ;; [1 3; 2 4]
    (async/>!! in {:id 5 :priority [5 -5]}) ;; [1 3 5; 2 4]
    (test-priority-data out 1)
    (test-priority-data out 3)
    (test-priority-data out 5)
    (test-priority-data out 2)
    (test-priority-data out 4)))
