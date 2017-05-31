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
(ns waiter.async-utils
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [metrics.core]
            [metrics.histograms :as histograms]
            [slingshot.slingshot :refer [throw+ try+]]))

(defn sliding-buffer-chan [n]
  (async/chan (async/sliding-buffer n)))

(defn latest-chan
  "Creates and returns a new sliding buffer channel of size 1"
  []
  (sliding-buffer-chan 1))

(defn on-chan-close
  "Repeatedly pulls off `c` until the channel closes.
   Once closed, calls `f`. If `ex-handler` is specified,
   and an exception occurs, ex-handler will be called with
   one parameter, the exception."
  [c f ex-handler]
  (async/go
    (try
      (loop []
        (if (nil? (async/<! c))
          (f)
          (recur)))
      (catch Exception e
        (ex-handler e)))))

(defn offer!
  "Tries to put `v` onto `chan`.
   If it can do so immediately it will, and return true
   otherwise it will return false. This function will always return immediately"
  [chan v]
  (async/alt!!
    [[chan v]] ([r] r) ; Will return false if chan is closed
    :default false
    :priority true))

(defmacro timed-offer!
 "Tries to put `v` onto `chan`.
  It will wait `millis` to put, if it can it will, and return true
  otherwise it will return false.
  This function will return after at most `millis` milliseconds"
  [chan v millis]
  `(let [timeout# (async/timeout ~millis)]
    (async/alt!
      [[~chan ~v]] ([r#] r#) ; Will return false if chan is closed
      timeout# false
      :priority true)))

(defn timed-offer!!
 "Tries to put `v` onto `chan`.
  It will wait `millis` to put, if it can it will, and return true
  otherwise it will return false.
  This function will return after at most `millis` milliseconds"
  [chan v millis]
  (let [timeout (async/timeout millis)]
    (async/alt!!
      [[chan v]] ([r] r) ; Will return false if chan is closed
      timeout false
      :priority true)))

(defn poll!
  "Tries to pull a value, `v`, from `chan`.
   If it can do so immediately it will, and return `v`
   otherwise it will return false. This function will always return immediately"
  ([chan]
   (poll! chan false))
  ([chan default]
   (async/alt!!
     chan ([v c] v) ; Will return nil if chan is closed
     :default default
     :priority true)))

(defn timing-out-pipeline
  "Pipeline that takes elements from `in` and returns a channel that elements can be pulled from.
   Will call `af` on each element that arrives, the first parameter will be a channel the second
   parameter the element. It is intended that `af` will return immediately and kick off an async process.
   `af` should put a map containing two keys, :id and :resp-chan.
   Putting the map on the channel signifies that any elements such that `(id-fn element)` = id
   should be removed. Note, this may not happen immediately and there is an inherent race condition
   in which the element is put on the out channel before it is removed.
   Therefore, if and only if the element is removed, the `resp-chan` will be closed.
   It is important to wait for the `resp-chan` to close before handling the removed element.
   The channel passed to `af` will be closed if the item has been passed along.
   This allows you to short circuit your async worker.

   `pipeline-id` is used in log messages.
   `buffer-size-histogram` will store metrics for the size of the buffer for processing messages.
   `buffer-diff-counter` will store difference in length of the size of the buffer for processing messages.
   `ex-handler` will be called if an exception occurs while calling `af`
   It will also be called if there is a fatal error in processing"
  [pipeline-id buffer-size-histogram in max-size id-fn af ex-handler]
  (let [time-out-chan (async/chan 5)
        out (async/chan)]
    (async/go
      (try
        (loop [buf []]
          (histograms/update! buffer-size-histogram (count buf))
          (let [chans [time-out-chan]
                chans (if (< (count buf) max-size)
                        (conj chans in)
                        chans)
                chans (if (seq buf)
                        (conj chans [out (:data (first buf))])
                        chans)
                [v ch] (async/alts! chans :priority true)
                buf'
                (condp = ch
                  in (when v
                       (let [c (async/chan)]
                         (async/pipe c time-out-chan false)
                         (if (try
                               (af c v)
                               true
                               (catch Exception e
                                 (log/warn e pipeline-id "af function threw error while processing" v)
                                 (ex-handler e)))
                           (do
                             (log/debug pipeline-id "Enqueuing message" v)
                             (conj buf {:data v :chan c :id (id-fn v)}))
                           buf)))

                  time-out-chan (let [{:keys [id resp-chan]} v]
                                  (if (some #(= id (:id %)) buf)
                                    (let [filtered-buf (filterv #(not= id (:id %)) buf)]
                                      (log/debug pipeline-id "removing buffer entries having id =" id)
                                      (async/close! resp-chan)
                                      filtered-buf)
                                    buf))

                  out (let [removed (first buf)]
                        (log/debug pipeline-id "Closing channel for" removed)
                        (async/close! (:chan removed))
                        (subvec buf 1)))]
            (if buf'
              (recur buf')
              (log/info pipeline-id "Terminating pipeline."))))
        (catch Exception e
          (ex-handler e)
          (log/error e "Fatal exception in timing-out-pipeline"))))
    out))
