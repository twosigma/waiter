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
(ns waiter.util.async-utils
  (:require [clojure.core.async :as async]
            [clojure.data.priority-map :as priority-map]
            [clojure.tools.logging :as log]
            [metrics.core]
            [metrics.histograms :as histograms]
            [waiter.correlation-id :as cid])
  (:import clojure.core.async.impl.channels.ManyToManyChannel
           java.util.concurrent.ExecutorService))

(defn sliding-buffer-chan [n]
  (async/chan (async/sliding-buffer n)))

(defn latest-chan
  "Creates and returns a new sliding buffer channel of size 1"
  []
  (sliding-buffer-chan 1))

(defn timer-chan
  "Returns a core.async channel that 'chimes' at the specified intervals after the specified delay (default of 0 ms).
   The go block that is triggering the chimes can be terminated by closing the returned channel."
  ([interval-ms]
   (timer-chan interval-ms 0))
  ([interval-ms delay-ms]
   (let [timer-ch (latest-chan)]
     (async/go
       (when (pos? delay-ms)
         (async/<! (async/timeout delay-ms)))
       (loop []
         (when (async/>! timer-ch ::trigger)
           (async/<! (async/timeout interval-ms))
           (recur))))
     timer-ch)))

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
  [pipeline-id buffer-size-histogram in max-size id-fn priority-fn af ex-handler]
  (let [time-out-chan (async/chan 5)
        out (async/chan)]
    (async/go
      (try
        ;; We maintain two data structures, a regular requests buffer for requests that did not specify priorities
        ;; and another prioritized-requests priority queue for requests that did specify priority.
        ;; This helps us improve performance for the common use-case (no priorities).
        ;; Effectively, this means requests that do not specify a priority are treated as highest priority and
        ;; does not allow requests (from newer clients) to jump the queue by using the priority feature.
        (loop [regular-requests []
               prioritized-requests (priority-map/priority-map-by #(compare %2 %1))]
          (let [num-pending-requests (+ (count regular-requests) (count prioritized-requests))
                _ (histograms/update! buffer-size-histogram num-pending-requests)
                chans [time-out-chan]
                chans (if (< num-pending-requests max-size)
                        (conj chans in)
                        chans)
                head-request (if (seq regular-requests)
                               (first regular-requests)
                               (when (seq prioritized-requests)
                                 (first (peek prioritized-requests))))
                chans (if head-request
                        (conj chans [out (:data head-request)])
                        chans)
                [v ch] (async/alts! chans :priority true)
                [regular-requests' prioritized-requests' :as request-queues']
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
                           (let [priority (priority-fn v)
                                 item {:data v :chan c :id (id-fn v)}]
                             (log/debug pipeline-id "Enqueuing message" v "with priority" priority)
                             (if priority
                               [regular-requests
                                (assoc prioritized-requests item priority)]
                               [(conj regular-requests item)
                                prioritized-requests]))
                           [regular-requests prioritized-requests])))

                  time-out-chan (let [{:keys [id resp-chan]} v]
                                  (if (some #(= id (:id %)) regular-requests)
                                    (let [filtered-requests (filterv #(not= id (:id %)) regular-requests)]
                                      (log/debug pipeline-id "removing regular request entries having id =" id)
                                      (async/close! resp-chan)
                                      [filtered-requests prioritized-requests])
                                    (if-let [filtered-items (->> prioritized-requests keys (filter #(= id (:id %))) seq)]
                                      (let [filtered-requests (apply dissoc prioritized-requests filtered-items)]
                                        (log/debug pipeline-id "removing prioritized request entries having id =" id)
                                        (async/close! resp-chan)
                                        [regular-requests filtered-requests])
                                      [regular-requests prioritized-requests])))

                  out (let [removed head-request]
                        (log/debug pipeline-id "Closing channel for" removed)
                        (async/close! (:chan removed))
                        (if (seq regular-requests)
                          [(subvec regular-requests 1) prioritized-requests]
                          [regular-requests (pop prioritized-requests)])))]
            (if request-queues'
              (recur regular-requests' prioritized-requests')
              (log/info pipeline-id "Terminating pipeline."))))
        (catch Exception e
          (ex-handler e)
          (log/error e "Fatal exception in timing-out-pipeline"))))
    out))

(defn chan?
  "Determines if v is a channel."
  [v]
  (instance? ManyToManyChannel v))

(defn execute
  "Helper function, like core.async/thread, which asynchronously executes task on a thread in the provided thread pool.
   Returns a channel which will receive the result of the task when completed, then close."
  [task ^ExecutorService task-thread-pool]
  (let [task-complete-chan (async/promise-chan)
        current-cid (cid/get-correlation-id)]
    (.submit task-thread-pool
             ^Runnable
             (fn execute-task []
               (cid/with-correlation-id
                 current-cid
                 (try
                   (async/>!! task-complete-chan {:result (task)})
                   (catch Throwable th
                     (log/error th "error while executing task")
                     (async/>!! task-complete-chan {:error th}))
                   (finally
                     (async/close! task-complete-chan))))))
    task-complete-chan))
