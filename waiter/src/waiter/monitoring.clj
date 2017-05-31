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
(ns waiter.monitoring
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.meters :as meters]
            [plumbing.core :as pc]
            [waiter.metrics :as metrics]
            [waiter.utils :as utils]))

(defn thread-stack-tracker
  "Launches a go-block that maintains the stacktraces for individual threads since it was last queried.
   Stack traces are obtained by invoking the `get-all-stack-traces` function .
   It relies on `filter-trace-map` to restrict the threads it tracks.

   Returns a map containing the query-chan and exit-chan:
   `query-chan` returns the currently tracked threads.
   `exit-chan` triggers the go-block to exit."
  [state-store-atom thread-stack-state-refresh-interval-ms get-all-stack-traces clock]
  (let [label "thread-stack-tracker-"]
    (utils/start-timer-task
      (t/millis thread-stack-state-refresh-interval-ms)
      (fn []
        (try
          (let [{:keys [iteration thread-id->stack-trace-state]} @state-store-atom]
            (log/debug label "state has been queried")
            (meters/mark! (metrics/waiter-meter "thread-stack-tracker" "query"))
            (let [current-time (clock)
                  thread-id->stack-trace-elements (->> (get-all-stack-traces)
                                                       (pc/map-vals #(map str %))
                                                       (pc/map-keys str))
                  thread-id->stack-trace-state'
                  (pc/map-from-keys
                    (fn [thread-id]
                      (let [previous-stack-trace-elements (get-in thread-id->stack-trace-state [thread-id :stack-trace])
                            current-stack-trace-elements (get thread-id->stack-trace-elements thread-id)
                            last-modified-time (if (= previous-stack-trace-elements current-stack-trace-elements)
                                                 (get-in thread-id->stack-trace-state [thread-id :last-modified-time] current-time)
                                                 current-time)]
                        {:last-modified-time last-modified-time
                         :stack-trace current-stack-trace-elements}))
                    (keys thread-id->stack-trace-elements))]
              (reset! state-store-atom {:iteration ((fnil inc 0) iteration)
                                        :thread-id->stack-trace-state thread-id->stack-trace-state'})))
          (log/debug label "exiting.")
          (catch Exception e
            (log/error e label "terminating go block.")))))))

(defn filter-trace-map
  "Filters stack trace entries based on two conditions:
   1. The stack trace must be non-empty (VMs are allowed to return empty stack traces for threads)
   2. The thread must have a method from the `excluded-methods` at the top of its stack."
  [excluded-methods thread-id->stack-trace-state]
  (remove (fn [[_ {:keys [stack-trace]}]]
            (or (empty? stack-trace)
                (let [stack-top (-> stack-trace (first) (str))]
                  (some #(str/includes? stack-top (str % "(")) excluded-methods))))
          thread-id->stack-trace-state))

(defn any-thread-stack-stale
  "Checks `thread-id->stack-trace-state` for any stale stack traces based on the `:last-modified-time`.
   It returns a map with entries whose last modified time is earlier than the current time
   minus the `stale-threshold-ms` ms.

   The current time is computed by using `clock`."
  [stale-threshold-ms clock thread-id->stack-trace-state]
  (let [current-time (clock)
        threshold-time (t/minus current-time (t/millis stale-threshold-ms))]
    (utils/filterm (fn [[_ {:keys [last-modified-time]}]]
                     (t/after? threshold-time last-modified-time))
                   thread-id->stack-trace-state)))

(defn retrieve-stale-thread-stack-trace-data
  "Retrieves the stale thread data based on retrieving `thread-id->stack-trace-state` from state-store-atom.
   The stale threshold is determined by `stale-threshold-ms`.

   The result is populated into `response-chan` in the format {:result <stale-thread-id->stack-trace-state>}"
  [state-store-atom clock excluded-methods stale-threshold-ms]
  (->> @state-store-atom
       (:thread-id->stack-trace-state)
       (filter-trace-map excluded-methods)
       (any-thread-stack-stale stale-threshold-ms clock)))