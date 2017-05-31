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
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [full.async :refer (<?? <? go-try)]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.timers :as timers]
            [waiter.async-utils :refer :all]))

(deftest test-sliding-buffer-chan
  (let [buf-size 4
        test-chan (sliding-buffer-chan buf-size)
        limit 10]
    (dotimes [n limit]
      (async/>!! test-chan n))
    (dotimes [n buf-size]
      (is (= (+ n (- limit buf-size)) (async/<!! test-chan))))))

(deftest timing-out-pipeline-test
  (let [fail false
        in (async/chan 1024)
        out (timing-out-pipeline "test"
                                 (histograms/histogram "test-size")
                                 in
                                 10
                                 :id
                                 (fn [c {:keys [id value resp-chan timeout-ms]
                                         :or   {timeout-ms 100}}]
                                   (async/go
                                     (let [timeout (async/timeout timeout-ms)]
                                       (async/alt!
                                         c ([_] :do-nothing)
                                         timeout ([_] (async/>! c {:id id :resp-chan resp-chan}))))))
                                 (fn [ex] (log/info ex)))]
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