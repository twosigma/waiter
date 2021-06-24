(ns waiter.token-watch
  (:require [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.tools.logging :as log]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.token :as token]
            [waiter.util.utils :as utils]))

(defn make-index-event
  "Create an event for watch endpoints"
  [type object & {:keys [id]}]
  (cond->
    {:object object :type type}
    id (assoc :id id)))

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
                            :token->index (token/get-token->index kv-store :refresh true)
                            :watch-chans #{}})
          query-state-fn
          (fn tokens-watch-query-state-fn
            [include-flags]
            (let [{:keys [last-update-time token->index watch-chans]} @state-atom]
              (cond-> {:last-update-time last-update-time
                       :watch-count (count watch-chans)}
                      (contains? include-flags "token->index")
                      (assoc :token->index token->index)
                      (contains? include-flags "buffer-state")
                      (assoc :buffer-state {:update-chan-count (.count tokens-update-chan-buffer)
                                            :watch-channels-update-chan-count (.count tokens-watch-channels-update-chan-buffer)}))))
          go-chan
          (async/go
            (try
              (loop [{:keys [token->index watch-chans] :as current-state} @state-atom]
                (reset! state-atom current-state)
                (let [external-event-cid (utils/unique-identifier)
                      [msg current-chan]
                      (async/alts! [exit-chan tokens-update-chan tokens-watch-channels-update-chan
                                    watch-refresh-timer-chan query-chan]
                                   :priority true)
                      next-state
                      (condp = current-chan
                        exit-chan
                        (do
                          (log/warn "stopping token-watch-maintainer")
                          (when (not= :exit msg)
                            (throw (ex-info "Stopping router-state maintainer" {:time (clock) :reason msg}))))

                        tokens-update-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "token-watch-maintainer" "token-update")
                          (let [{:keys [token cid] :as internal-event} msg]
                            (cid/with-correlation-id
                              (str "token-watch-maintainer" "." cid "." external-event-cid)
                              (log/info "received an internal index event" internal-event)
                              (let [token-index-entry (token/get-token-index kv-store token :refresh true)
                                    local-token-index-entry (get token->index token)]
                                (if (= token-index-entry local-token-index-entry)
                                  ; There is no change detected, so no event to be reported
                                  current-state
                                  (let [[index-event next-state]
                                        (if (some? token-index-entry)
                                          ; If index-entry retrieved from kv-store exists then treat as UPDATE
                                          ; (includes token creation and soft deletion)
                                          [(make-index-event :UPDATE token-index-entry)
                                           (assoc-in current-state [:token->index token] token-index-entry)]
                                          ; index-entry doesn't exist then treat as DELETE
                                          [(make-index-event :DELETE {:token token})
                                           (assoc current-state :token->index (dissoc token->index token))])
                                        _ (log/info "sending a token event to watches" {:event index-event})
                                        open-chans (->> (make-index-event :EVENTS [index-event] :id external-event-cid)
                                                        (utils/send-event-to-channels! watch-chans))]
                                    (assoc next-state :watch-chans open-chans)))))))

                        tokens-watch-channels-update-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "token-watch-maintainer" "channel-update")
                          (cid/with-correlation-id
                            (str "token-watch-maintainer" "." external-event-cid)
                            (log/info "received watch-chan" msg)
                            (let [watch-chan msg
                                  initial-event
                                  (timers/start-stop-time!
                                    (metrics/waiter-timer "core" "token-watch-maintainer" "channel-update-build-event")
                                    (make-index-event :INITIAL (doall (or (vals token->index) [])) :id external-event-cid))]
                              (timers/start-stop-time!
                                (metrics/waiter-timer "core" "token-watch-maintainer" "channel-update-forward-event")
                                (async/put! watch-chan initial-event))
                              (log/info "finished sending initial event")
                              (assoc current-state :watch-chans (conj watch-chans watch-chan)))))

                        watch-refresh-timer-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "token-watch-maintainer" "refresh")
                          (cid/with-correlation-id
                            (str "token-watch-maintainer" "." external-event-cid)
                            (log/info "refresh starting...")
                            (let [next-token->index (token/get-token->index kv-store :refresh true)
                                  [only-old-indexes only-next-indexes _] (data/diff token->index next-token->index)
                                  ; if token in old-indexes and not in only-next-indexes, then those token indexes were deleted
                                  delete-events
                                  (for [[token {:keys [owner]}] only-old-indexes
                                        :when (not (contains? only-next-indexes token))]
                                    (make-index-event :DELETE {:owner owner :token token}))
                                  ; if token in only-next-indexes, it has been updated (soft-delete, and creation included)
                                  update-events
                                  (for [[token _] only-next-indexes]
                                    (make-index-event :UPDATE (get next-token->index token)))
                                  events (concat delete-events update-events)
                                  ; send events event if empty, which will serve as a heartbeat
                                  open-chans
                                  (utils/send-event-to-channels! watch-chans (make-index-event :EVENTS events :id external-event-cid))]
                              (when (not-empty events)
                                (counters/inc! (metrics/waiter-counter "core" "token-watch-maintainer" "refresh-sync"))
                                (meters/mark! (metrics/waiter-meter "core" "token-watch-maintainer" "refresh-sync-rate"))
                                (meters/mark! (metrics/waiter-meter "core" "token-watch-maintainer" "refresh-sync-count")
                                              (count events))
                                (log/info "found some differences in kv-store and current-state"
                                          {:only-in-current only-old-indexes
                                           :only-in-next only-next-indexes
                                           :token-count (count token->index)}))
                              (log/info "refresh ended.")
                              (assoc current-state :token->index next-token->index
                                                   :watch-chans open-chans))))

                        query-chan
                        (let [{:keys [response-chan include-flags]} msg]
                          (async/put! response-chan (query-state-fn include-flags))
                          current-state))]
                  (if next-state
                    (recur (assoc next-state :last-update-time (clock)))
                    (log/info "Stopping token-watch-maintainer as next loop values are nil"))))
              (catch Exception e
                (log/error e "Fatal error in token-watch-maintainer")
                (System/exit 1))))]
      (metrics/waiter-gauge #(:watch-count (query-state-fn #{}))
                            "core" "token-watch-maintainer" "connections")
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
