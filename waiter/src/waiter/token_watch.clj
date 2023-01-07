(ns waiter.token-watch
  (:require [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [waiter.correlation-id :as cid]
            [waiter.metrics :as metrics]
            [waiter.status-codes :refer :all]
            [waiter.token :as token]
            [waiter.util.async-utils :as au]
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils]))

(defn make-index-event
  "Create an event for watch endpoints"
  [type object & {:keys [id reference-type]}]
  (cond->
    {:object object :type type}
    id (assoc :id id)
    reference-type (assoc :reference-type reference-type)))

(defn- update-token-references
  "Updates the map that tracks the references used by the specified token using the provided update-fn."
  [reference-type->reference-name->tokens token reference-type->reference-name update-fn]
  (reduce (fn update-token-references-helper
            [reference-type->reference-name->tokens [reference-type reference-name]]
            (update-in reference-type->reference-name->tokens
              [reference-type reference-name] (fnil update-fn #{}) token))
          reference-type->reference-name->tokens
          (seq reference-type->reference-name)))

(defn assoc-token-references
  "Updates the map that tracks usage of references to include the specified token."
  [reference-type->reference-name->tokens token reference-type->reference-name]
  (update-token-references
    reference-type->reference-name->tokens token reference-type->reference-name conj))

(defn dissoc-token-references
  "Updates the map that tracks usage of references to exclude the specified token."
  [reference-type->reference-name->tokens token reference-type->reference-name]
  (update-token-references
    reference-type->reference-name->tokens token reference-type->reference-name disj))

(defn adjust-token-references
  "Updates the map that tracks usage of references to exclude the specified token."
  [reference-type->reference-name->tokens token old-reference-type->reference-name new-reference-type->reference-name]
  (let [reference-type->reference-name-changed? (not= old-reference-type->reference-name new-reference-type->reference-name)]
    (cond-> reference-type->reference-name->tokens
      reference-type->reference-name-changed? (dissoc-token-references token old-reference-type->reference-name)
      reference-type->reference-name-changed? (assoc-token-references token new-reference-type->reference-name))))

(defn extract-reference-update-tokens
  "Extracts tokens based on the reference that has been updated.
   The token->index map is consulted to confirm that entries in reference-type->reference-name->tokens are not stale and tokens indeed need to be updated."
  [{:keys [reference-type->reference-name->last-update-time reference-type->reference-name->tokens]}
   {:keys [reference-type reference-name update-time]}]
  (let [last-update-time (get-in reference-type->reference-name->last-update-time [reference-type reference-name] 0)]
    (when (> update-time last-update-time)
      (get-in reference-type->reference-name->tokens [reference-type reference-name]))))

(defn calculate-reference-type->reference-name->tokens
  "Recalculates the reference-type->reference-name->tokens map based on the token index."
  [token->index]
  (reduce (fn calculate-reference-type->reference-name->tokens-helper
            [reference-type->reference-name->tokens [token {:keys [reference-type->reference-name]}]]
            (assoc-token-references reference-type->reference-name->tokens token reference-type->reference-name))
          {}
          token->index))

(defn start-token-watch-maintainer
  "Starts daemon thread that maintains token watches and process/filters internal token events to be streamed to
  clients through the watch handlers. Returns map of various channels and state functions to control the daemon."
  [extract-reference-type->reference-name-fn kv-store clock tokens-update-chan-buffer-size channels-update-chan-buffer-size
   reference-update-chan watch-refresh-timer-chan]
  (cid/with-correlation-id
    "token-watch-maintainer"
    (let [exit-chan (async/promise-chan)
          tokens-update-chan-buffer (async/buffer tokens-update-chan-buffer-size)
          tokens-update-chan (async/chan tokens-update-chan-buffer)
          tokens-watch-channels-update-chan-buffer (async/buffer channels-update-chan-buffer-size)
          tokens-watch-channels-update-chan (async/chan tokens-watch-channels-update-chan-buffer)
          query-chan (async/chan)
          state-atom (atom (let [token->index (token/get-token->index kv-store :refresh true)
                                 reference-type->reference-name->tokens (calculate-reference-type->reference-name->tokens token->index)]
                             {:last-update-time (clock)
                              :reference-type->reference-name->last-update-time {}
                              :reference-type->reference-name->tokens reference-type->reference-name->tokens
                              :token->index token->index
                              :watch-chans #{}}))
          query-state-fn
          (fn tokens-watch-query-state-fn
            [include-flags]
            (let [{:keys [last-update-time reference-type->reference-name->last-update-time reference-type->reference-name->tokens token->index watch-chans]} @state-atom]
              (cond-> {:last-update-time last-update-time
                       :watch-count (count watch-chans)}
                (contains? include-flags "reference-type->reference-name->last-update-time")
                (assoc :reference-type->reference-name->last-update-time reference-type->reference-name->last-update-time)
                (contains? include-flags "reference-type->reference-name->tokens")
                (assoc :reference-type->reference-name->tokens reference-type->reference-name->tokens)
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
                      (async/alts! [exit-chan tokens-update-chan reference-update-chan tokens-watch-channels-update-chan
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
                          (let [{:keys [cid token] :as internal-event} msg]
                            (cid/with-correlation-id
                              (str "token-watch-maintainer" "." cid "." external-event-cid)
                              (log/info "received an internal index event" internal-event)
                              (let [token-index-entry (token/get-token-index extract-reference-type->reference-name-fn kv-store token :refresh true)
                                    local-token-index-entry (get token->index token)]
                                (if (= token-index-entry local-token-index-entry)
                                  ; There is no change detected, so no event to be reported
                                  current-state
                                  (let [{:keys [reference-type->reference-name]} token-index-entry
                                        local-reference-type->reference-name (get local-token-index-entry :reference-type->reference-name)
                                        [index-event next-state]
                                        (if (some? token-index-entry)
                                          ; If index-entry retrieved from kv-store exists then treat as UPDATE
                                          ; (includes token creation and soft deletion)
                                          [(make-index-event :UPDATE token-index-entry)
                                           (-> (assoc-in current-state [:token->index token] token-index-entry)
                                               (update :reference-type->reference-name->tokens
                                                       adjust-token-references token local-reference-type->reference-name reference-type->reference-name))]
                                          ; index-entry doesn't exist then treat as DELETE
                                          [(make-index-event :DELETE {:token token})
                                           (-> current-state
                                               (assoc :token->index (dissoc token->index token))
                                               (update :reference-type->reference-name->tokens dissoc-token-references token reference-type->reference-name))])
                                        _ (log/info "sending a token event to watches" {:event index-event})
                                        open-chans (->> (make-index-event :EVENTS [index-event] :id external-event-cid :reference-type :token)
                                                        (utils/send-event-to-channels! watch-chans))]
                                    (assoc next-state :watch-chans open-chans)))))))

                        reference-update-chan
                        (timers/start-stop-time!
                          (metrics/waiter-timer "core" "token-watch-maintainer" "reference-update")
                          (let [{:keys [cid] :as internal-event} msg]
                            (cid/with-correlation-id
                              (str "token-watch-maintainer" "." cid "." external-event-cid)
                              (log/info "received a reference update event" internal-event)
                              (let [{:keys [reference-name reference-type update-time]} internal-event
                                    current-update-time (get-in current-state [:reference-type->reference-name->last-update-time reference-type reference-name])]
                                (if (or (nil? current-update-time) (< current-update-time update-time))
                                  ;; calculate reference update events and emit them to the watch channels
                                  (let [candidate-tokens (extract-reference-update-tokens current-state internal-event)
                                        open-chans (if (seq candidate-tokens)
                                                     (let [index-events (map #(make-index-event :UPDATE (get token->index %) :reference-type reference-type) candidate-tokens)]
                                                       (log/info "sending token update events due to reference update" {:tokens candidate-tokens})
                                                       (->> (make-index-event :EVENTS index-events :id external-event-cid :reference-type reference-type)
                                                            (utils/send-event-to-channels! watch-chans)))
                                                     watch-chans)]
                                    ;; update the reference-type->reference-name->update-time entry
                                    (-> current-state
                                        (assoc-in [:reference-type->reference-name->last-update-time reference-type reference-name] update-time)
                                        (assoc :watch-chans open-chans)))
                                  (do
                                    (log/info "skipping stale reference update event" {:current-update-time current-update-time :event-update-time update-time})
                                    current-state))))))

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
                                    (make-index-event :INITIAL (doall (or (vals token->index) [])) :id external-event-cid :reference-type :token))]
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
                                  reference-type->reference-name->tokens (calculate-reference-type->reference-name->tokens next-token->index)
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
                                  (utils/send-event-to-channels! watch-chans (make-index-event :EVENTS events :id external-event-cid :reference-type :token))]
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
                              (assoc current-state :reference-type->reference-name->tokens reference-type->reference-name->tokens
                                                   :token->index next-token->index
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

(defn watch-for-reference-updates
  "Watches for reference update notifications and propagates it to peer routers."
  [make-inter-router-requests-async-fn reference-update-chan router-id]
  (async/go-loop []
    (let [{:keys [cid response-chan] :as update-event} (async/<! reference-update-chan)
          correlation-id (or cid (utils/unique-identifier))]
      (cond
        (nil? update-event)
        (log/info "exiting watch for reference updates go-loop")

        (not= router-id (get update-event :router-id))
        (do
          (log/info "not propagating remote update event to peer routers" update-event)
          (recur))

        :else
        (do
          (cid/with-correlation-id
            correlation-id
            (do
              (log/info "sending reference update to peer routers" update-event)
              (let [router-id->response-chan (make-inter-router-requests-async-fn
                                               "reference/notify"
                                               :body (-> update-event
                                                         (select-keys [:reference-name :reference-type :update-time])
                                                         (assoc :router-id router-id)
                                                         (utils/clj->json))
                                               :config {:headers {"content-type" "application/json"
                                                                  "x-cid" correlation-id}}
                                               :method :post)]
                (log/info "sent reference update to" (count router-id->response-chan) "peer routers")
                (async/go
                  (let [router-id->response-atom (atom {})]
                    (doseq [[router-id response-chan] (seq router-id->response-chan)]
                      (let [{:keys [body status] :as response} (async/<! response-chan)
                            successful? (hu/status-2XX? status)
                            response (cond-> response
                                       (not successful?)
                                       (assoc :body (if (au/chan? body) (async/<! body) body)))
                            response-keys (cond-> [:headers :status]
                                            (not successful?) (conj :body))]
                        (log/info "response from" router-id (select-keys response response-keys))
                        (swap! router-id->response-atom assoc router-id response)))
                    (when (au/chan? response-chan)
                      (async/>! response-chan @router-id->response-atom)))))))
          (recur))))))

(defn reference-notify-handler
  "Handler that receives reference update notifications and propagates it to the reference-listener-chan channel."
  [reference-listener-chan router-id {:keys [request-method] :as request}]
  (async/go
    (try
      (case request-method
        :post (let [correlation-id (cid/get-correlation-id)
                    {:keys [reference-name reference-type update-time] :as update-event} (-> request ru/json-request :body walk/keywordize-keys)]
                (log/info "received reference update" update-event)
                (when (or (str/blank? reference-name)
                          (str/blank? reference-type)
                          (not (pos-int? update-time)))
                  (throw (ex-info "reference-name, reference-type and integer update-time must be provided"
                                  {:log-level :info
                                   :request-method request-method
                                   :status http-400-bad-request
                                   :update-event update-event})))
                (let [reference-event (-> update-event
                                          (update :reference-type keyword)
                                          (assoc :cid correlation-id ))
                      success? (async/put! reference-listener-chan reference-event)]
                  (log/info (if success? "success" "failure") "in propagating reference update")
                  (-> update-event
                      (select-keys [:reference-name :reference-type :update-time])
                      (assoc :cid correlation-id :router-id router-id :success success?)
                      (utils/clj->json-response))))
        (utils/exception->response
          (ex-info "Only POST supported" {:log-level :info
                                          :request-method request-method
                                          :status http-405-method-not-allowed})
          request))
      (catch Exception ex
        (utils/exception->response ex request)))))

(defn start-reference-notify-forwarder
  "Intercepts reference update events from reference-listener-chan to invoke the notification function
   before propagating the event along the reference-notifier-chan."
  [reference-listener-chan reference-update-chan pre-notify-fn]
  (async/go
    (cid/with-correlation-id
      "reference-notify-forwarder"
      (log/info "starting reference notify forwarder")
      (loop []
        (let [{:keys [cid response-chan] :as reference-event} (async/<! reference-listener-chan)]
          (if (nil? reference-event)
            (log/info "exiting reference notify forwarder")
            (let [correlation-id (or cid (cid/get-correlation-id))]
              (log/info "processing reference event" reference-event)
              (async/thread
                (cid/with-correlation-id
                  correlation-id
                  (try
                    (let [success? (pre-notify-fn reference-event)]
                      (log/info (if success? "success" "failure") "in notifying reference update"))
                    (catch Throwable th
                      (log/error th "error in processing reference event" reference-event)))
                  (let [success? (async/put! reference-update-chan reference-event)]
                    (log/info (if success? "success" "failure") "in forwarding reference update")
                    (some-> response-chan (async/put! {:success success?})))))
              (recur))))))))
