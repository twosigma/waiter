(ns waiter.token-watch
  (:require [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.token :as tkn]
            [waiter.util.async-utils :as au]))

(defn send-event-to-channels
  "Given a list of watch channels and the event to send to each channel, send the event in a non blocking fashion and
  close channels that error the async/put! operation. Returns a list of closed channels."
  [watch-chans event]
  (for [watch-chan watch-chans
        :when
        (try
          (not (async/put! watch-chan event))
          (catch AssertionError e
            (log/error e "closing watch-chan due to error")
            (async/close! watch-chan)
            true))]
    watch-chan))

(defn start-token-watch-maintainer
  "Starts daemon thread that maintains token watches and process/filters internal token events to be streamed to
  clients through the watch handlers. Returns map of various channels and state functions to control the daemon."
  [kv-store clock tokens-update-chan-buffer-size channels-update-chan-buffer-size watch-refresh-timer-chan]
  (cid/with-correlation-id
    "token-watch-maintainer"
    (let [exit-chan (async/promise-chan)
          tokens-update-chan-buffer (async/buffer tokens-update-chan-buffer-size)
          tokens-update-chan (async/chan tokens-update-chan-buffer)
          tokens-watch-channels-update-chan-buffer (async/buffer channels-update-chan-buffer-size)
          tokens-watch-channels-update-chan (async/chan tokens-watch-channels-update-chan-buffer)
          query-chan (async/chan)
          state-atom (atom {:last-update-time (clock)
                            :token-index-map (tkn/get-token-index-map kv-store :refresh true)
                            :watch-chans #{}})
          query-state-fn
          (fn tokens-watch-query-state-fn
            [include-flags]
            (let [{:keys [last-update-time token-index-map watch-chans]} @state-atom]
              (cond-> {:last-update-time last-update-time
                       :watch-count (count watch-chans)}
                      (contains? include-flags "token-index-map")
                      (assoc :token-index-map token-index-map)
                      (contains? include-flags "buffer-state")
                      (assoc :buffer-state {:update-chan-count (count tokens-update-chan-buffer)
                                            :watch-channels-update-chan-count
                                            (count tokens-watch-channels-update-chan-buffer)}))))
          go-chan
          (async/go
            (try
              (loop [{:keys [token-index-map watch-chans] :as current-state} @state-atom]
                (reset! state-atom current-state)
                (let [[msg current-chan]
                      (async/alts! [exit-chan tokens-update-chan tokens-watch-channels-update-chan
                                    watch-refresh-timer-chan query-chan]
                                   :priority true)
                      next-state
                      (condp = current-chan
                        exit-chan
                        (do
                          (log/warn "Stopping token-watch-maintainer")
                          (when (not= :exit msg)
                            (throw (ex-info "Stopping router-state maintainer" {:time (clock) :reason msg}))))

                        tokens-update-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "token-watch-maintainer" "process-tokens-update")
                          (let [{:keys [token owner]} msg
                                token-index-entry (tkn/get-token-index kv-store token owner :refresh true)
                                local-token-index-entry (get token-index-map token)]
                            (cond
                              ; There is no change detected, so no event to be reported
                              (= token-index-entry local-token-index-entry)
                              current-state
                              ; If index-entry retrieved from kv-store exists then treat as UPDATE (includes token creation)
                              (some? token-index-entry)
                              (let [closed-chans (send-event-to-channels watch-chans (tkn/make-index-event :UPDATE token-index-entry))]
                                (-> current-state
                                    (assoc-in [:token-index-map token] token-index-entry)
                                    (assoc :watch-chans (apply disj watch-chans closed-chans))))
                              :else
                              (let [event-obj {:owner owner :token token}
                                    closed-chans (send-event-to-channels watch-chans (tkn/make-index-event :DELETE event-obj))]
                                (-> current-state
                                    (assoc :token-index-map (dissoc token-index-map token))
                                    (assoc :watch-chans (apply disj watch-chans closed-chans)))))))

                        tokens-watch-channels-update-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "token-watch-maintainer" "process-tokens-channels-update")
                          (let [watch-chan msg]
                            (async/put! watch-chan (tkn/make-index-event :INITIAL (or (vals token-index-map) [])))
                            (assoc current-state :watch-chans (conj watch-chans watch-chan))))

                        watch-refresh-timer-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "token-watch-maintainer" "refresh")
                          (let [next-token-index-map (tkn/get-token-index-map kv-store :refresh true)
                                [only-old-indexes only-next-indexes _] (data/diff token-index-map next-token-index-map)
                                ; if token in old-indexes and not in only-next-indexes, then those token indexes were deleted
                                delete-events
                                (for [[token {:keys [owner]}] only-old-indexes
                                      :when (not (contains? only-next-indexes token))]
                                  (tkn/make-index-event :DELETE {:owner owner :token token}))
                                ; if token in only-next-indexes, it has been updated (soft-delete, and creation included)
                                update-events
                                (for [[token _] only-next-indexes]
                                  (tkn/make-index-event :UPDATE (get next-token-index-map token)))
                                events (concat delete-events update-events)
                                ; send events event if empty, which will serve as a heartbeat
                                closed-chans
                                (send-event-to-channels watch-chans (tkn/make-index-event :EVENTS events))]
                            (when (not-empty events)
                              (counters/inc! (metrics/waiter-counter "core" "token-watch-maintainer" "refresh-sync"))
                              (meters/mark! (metrics/waiter-meter "core" "token-watch-maintainer" "refresh-sync-rate"))
                              (meters/mark! (metrics/waiter-meter "core" "token-watch-maintainer" "refresh-sync-count")
                                            (count events))
                              (log/info "token-watch-maintainer found some differences in kv-store and current-state"
                                        {:only-in-current only-old-indexes
                                         :only-in-next only-next-indexes
                                         :token-count (count token-index-map)}))
                            (assoc current-state :token-index-map next-token-index-map
                                                 :watch-chans (apply disj watch-chans closed-chans))))

                        query-chan
                        (let [{:keys [response-chan include-flags]} msg]
                          (async/put! response-chan (query-state-fn include-flags))
                          current-state))]
                  (if next-state
                    (recur (assoc next-state :last-update-time (clock)))
                    (log/info "Stopping token-watch-maintainer as next loop values are nil"))))
              (catch Exception e
                (clojure.stacktrace/print-stack-trace e)
                (log/error e "Fatal error in token-watch-maintainer")
                (System/exit 1))))]
      (metrics/waiter-gauge #(count tokens-update-chan-buffer)
                            "core" "token-watch-maintainer" "update-chan-count")
      (metrics/waiter-gauge #(count tokens-watch-channels-update-chan-buffer)
                            "core" "token-watch-maintainer" "watch-channels-update-chan-count")
      {:exit-chan exit-chan
       :go-chan go-chan
       :query-chan query-chan
       :query-state-fn query-state-fn
       :tokens-update-chan tokens-update-chan
       :tokens-watch-channels-update-chan tokens-watch-channels-update-chan})))
