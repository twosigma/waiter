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
(ns waiter.mocks
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [full.async :refer (<?? <? go-try)]))

(defn mock-reservation-system
  [instance-rpc-chan mock-fns]
  (async/thread
    (try
      (loop [mock (first mock-fns)
             remaining (rest mock-fns)]
        (let [{:keys [response-chan]} (async/<!! instance-rpc-chan)
                c (async/chan 1)]
          (async/>!! response-chan c)
          (mock (async/<!! c)))
        (recur (first remaining) (rest remaining)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)))))