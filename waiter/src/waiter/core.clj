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
(ns waiter.core
  (:require [bidi.bidi :as bidi]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [digest]
            [full.async :refer (<?? <? go-try)]
            [metrics.core]
            [metrics.counters :as counters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [ring.middleware.basic-authentication :as basic-authentication]
            [slingshot.slingshot :refer [try+]]
            [waiter.async-request :as async-req]
            [waiter.async-utils :as au]
            [waiter.auth.authentication :as auth]
            [waiter.authorization :as authz]
            [waiter.cookie-support :as cookie-support]
            [waiter.correlation-id :as cid]
            [waiter.cors :as cors]
            [waiter.curator :as curator]
            [waiter.discovery :as discovery]
            [waiter.handler :as handler]
            [waiter.headers :as headers]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.metrics-sync :as metrics-sync]
            [waiter.password-store :as password-store]
            [waiter.process-request :as pr]
            [waiter.scaling :as scaling]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.service-description :as sd]
            [waiter.settings :as settings]
            [waiter.simulator :as simulator]
            [waiter.state :as state]
            [waiter.statsd :as statsd]
            [waiter.token :as token]
            [waiter.utils :as utils]
            [waiter.websocket :as ws]
            [waiter.work-stealing :as work-stealing])
  (:import (java.net InetAddress URI)
           java.util.concurrent.Executors
           org.apache.curator.framework.CuratorFrameworkFactory
           org.apache.curator.framework.api.CuratorEventType
           org.apache.curator.framework.api.CuratorListener
           org.apache.curator.framework.recipes.leader.LeaderLatch
           org.apache.curator.retry.BoundedExponentialBackoffRetry
           org.eclipse.jetty.client.HttpClient
           org.eclipse.jetty.client.util.BasicAuthentication$BasicResult
           org.eclipse.jetty.util.HttpCookieStore$Empty
           org.eclipse.jetty.websocket.client.WebSocketClient))

(defn routes-mapper
  "Returns a map containing a keyword handler and the parsed route-params based on the request uri."
  ;; Please include/update a corresponding unit test anytime the routes data structure is modified
  [{:keys [uri]}]
  (let [routes ["/" {"" :welcome-handler-fn
                     "app-name" :app-name-handler-fn
                     "apps" {"" :service-list-handler-fn
                             ["/" :service-id] :service-handler-fn
                             ["/" :service-id "/logs"] :service-view-logs-handler-fn
                             ["/" :service-id "/override"] :service-override-handler-fn
                             ["/" :service-id "/refresh"] :service-refresh-handler-fn
                             ["/" :service-id "/resume"] :service-resume-handler-fn
                             ["/" :service-id "/suspend"] :service-suspend-handler-fn}
                     "blacklist" {"" :blacklist-instance-handler-fn
                                  ["/" :service-id] :blacklisted-instances-list-handler-fn}
                     "favicon.ico" :favicon-handler-fn
                     "metrics" :metrics-request-handler-fn
                     "secrun" :process-request-fn
                     "service-id" :service-id-handler-fn
                     "settings" :display-settings-handler-fn
                     "sim" :sim-request-handler
                     "state" {"" :state-all-handler-fn
                              "/kv-store" :state-kv-store-handler-fn
                              "/leader" :state-leader-handler-fn
                              "/maintainer" :state-maintainer-handler-fn
                              "/router-metrics" :state-router-metrics-handler-fn
                              "/scheduler" :state-scheduler-handler-fn
                              "/statsd" :state-statsd-handler-fn
                              ["/" :service-id] :state-service-handler-fn}
                     "status" :status-handler-fn
                     "token" :token-handler-fn
                     "tokens" { "" :token-list-handler-fn
                               "/owners" :token-owners-handler-fn
                               "/refresh":token-refresh-handler-fn
                               "/reindex" :token-reindex-handler-fn}
                     "waiter-async" {["/complete/" :request-id "/" :service-id] :async-complete-handler-fn
                                     ["/result/" :request-id "/" :router-id "/" :service-id "/" :host "/" :port "/" [#".+" :location]]
                                     :async-result-handler-fn
                                     ["/status/" :request-id "/" :router-id "/" :service-id "/" :host "/" :port "/" [#".+" :location]]
                                     :async-status-handler-fn}
                     "waiter-auth" :waiter-auth-handler-fn
                     "waiter-consent" {"" :waiter-acknowledge-consent-handler-fn
                                       ["/" [#".*" :path]] :waiter-request-consent-handler-fn}
                     "waiter-kill-instance" {["/" :service-id] :kill-instance-handler-fn}
                     "work-stealing" :work-stealing-handler-fn}]]
    (or (bidi/match-route routes uri)
        {:handler :not-found-handler-fn})))

(defn ring-handler-factory
  "Creates the handler for processing http requests."
  [waiter-request?-fn {:keys [cors-preflight-handler-fn process-request-fn] :as handlers}]
  (fn http-handler [{:keys [uri] :as request}]
    (cond
      (cors/preflight-request? request)
      (do
        (counters/inc! (metrics/waiter-counter "requests" "cors-preflight"))
        (cors-preflight-handler-fn request))

      (not (waiter-request?-fn request))
      (do
        (counters/inc! (metrics/waiter-counter "requests" "service-request"))
        (process-request-fn request))

      :else
      (let [{:keys [handler route-params]} (routes-mapper request)
            request (assoc request :route-params (or route-params {}))
            handler-fn (get handlers handler process-request-fn)]
        (when (and (not= handler :process-request-fn) (= handler-fn process-request-fn))
          (log/warn "using default handler as no mapping found for" handler "at uri" uri))
        (when handler
          (counters/inc! (metrics/waiter-counter "requests" (name handler))))
        (handler-fn request)))))

(defn websocket-handler-factory
  "Creates the handler for processing websocket requests.
   Websockets are currently used for inter-router metrics syncing."
  [{:keys [default-websocket-handler-fn router-metrics-handler-fn]}]
  (fn websocket-handler [{:keys [uri] :as request}]
    (case uri
      "/waiter-router-metrics" (router-metrics-handler-fn request)
      (default-websocket-handler-fn request))))

(defn correlation-id-middleware
  "Attaches an x-cid header to the request and response if one is not already provided."
  [handler]
  (fn correlation-id-middleware-fn [request]
    (let [request (cid/ensure-correlation-id request utils/unique-identifier)
          request-cid (cid/http-object->correlation-id request)]
      (cid/with-correlation-id
        request-cid
        (log/info "request received:"
                  (-> (dissoc request :body :ctrl :server-name :server-port :servlet-request :ssl-client-cert :support-info)
                      (update-in [:headers] headers/truncate-header-values)))
        (let [response (handler request)
              get-request-cid (fn get-request-cid [] request-cid)]
          (if (map? response)
            (cid/ensure-correlation-id response get-request-cid)
            (async/go
              (let [nested-response (async/<! response)]
                (if (map? nested-response) ;; websocket responses may be another channel
                  (cid/ensure-correlation-id nested-response get-request-cid)
                  nested-response)))))))))

(defn wrap-support-info
  "Attaches support-info to the request."
  [handler support-info]
  (fn wrap-support-info-fn [request]
    (-> request
        (assoc :support-info support-info)
        handler)))

(defn- make-blacklist-request
  [make-inter-router-requests-fn blacklist-period-ms dest-router-id dest-endpoint {:keys [id] :as instance} reason]
  (log/info "peer communication requesting" dest-router-id "to blacklist" id "via endpoint" dest-endpoint)
  (try
    (-> (make-inter-router-requests-fn
          dest-endpoint
          :acceptable-router? #(= dest-router-id %)
          :body (json/write-str {:instance instance
                                 :period-in-ms blacklist-period-ms
                                 :reason reason})
          :method :post)
        (get dest-router-id))
    (catch Exception e
      (log/error e "error in making blacklist request"))))

(defn peers-acknowledged-blacklist-requests?
  "Note that the ids used in here are internal ids generated by the curator api."
  [{:keys [id] :as instance} short-circuit? router-ids endpoint make-blacklist-request-fn reason]
  (if (seq router-ids)
    (loop [[dest-router-id & remaining-peer-ids] (seq router-ids)
           blacklist-successful? true]
      (log/info {:dest-id dest-router-id, :blacklist-successful? blacklist-successful?}, :reason reason)
      (let [response (make-blacklist-request-fn dest-router-id endpoint instance reason)
            response-successful? (= 200 (:status response))
            blacklist-successful? (and blacklist-successful? response-successful?)]
        (when (and short-circuit? (not response-successful?))
          (log/info "peer communication" dest-router-id "veto-ed killing of" id {:http-status (:status response)}))
        (when (and short-circuit? response-successful?)
          (log/info "peer communication" dest-router-id "approves killing of" id))
        (if (and remaining-peer-ids (or (not short-circuit?) response-successful?))
          (recur remaining-peer-ids blacklist-successful?)
          blacklist-successful?)))
    (do
      (log/warn "no peer routers found to acknowledge blacklist request!")
      true)))

(defn make-kill-instance-request
  "Makes a request to a peer router to kill an instance of a service."
  [make-inter-router-requests-fn service-id dest-router-id kill-instance-endpoint]
  (log/info "peer communication requesting" dest-router-id "to kill an instance of" service-id "via endpoint" kill-instance-endpoint)
  (try
    (-> (make-inter-router-requests-fn
          kill-instance-endpoint
          :acceptable-router? #(= dest-router-id %)
          :method :post)
        (get dest-router-id))
    (catch Exception e
      (log/error e "error in killing instance of" service-id))))

(defn delegate-instance-kill-request
  "Delegates requests to kill an instance of a service to peer routers."
  [service-id router-ids make-kill-instance-request-fn]
  (if (not-empty router-ids)
    (loop [[dest-router-id & remaining-router-ids] (seq router-ids)]
      (let [dest-endpoint (str "waiter-kill-instance/" service-id)
            {:keys [body status]} (make-kill-instance-request-fn dest-router-id dest-endpoint)
            kill-successful? (= 200 status)]
        (when kill-successful?
          (log/info "peer communication" dest-router-id "killed instance of" service-id body))
        (if (and remaining-router-ids (not kill-successful?))
          (recur remaining-router-ids)
          kill-successful?)))
    (do
      (log/warn "no peer routers found! Unable to delegate call to kill instance of" service-id)
      false)))

(defn service-gc-go-routine
  "Go-routine that performs GC of services.
   Only the leader gets to perform the GC operations.
   Other routers keep looping waiting for their turn to become a leader.

  Parameters:
  `read-state-fn`: (fn [name] ...) used to read the current state.
  `write-state-fn`: (fn [name state] ...) used to write the current state which potentially is used by the read.
  `leader?`: Returns true if the router is currently the leader, only the leader writes state into persistent store at
             `(str base-path '/' gc-relative-path '/' name`.
  `clock` (fn [] ...) returns the current time.
  `name`: Name of the go-routine.
  `service-data-chan`: A channel which produces service data.
  `timeout-interval-ms`: Timeout interval used as a refractory period while listening for data from `service-data-mult-chan`
                         to allow effects of any GC run to propagate through the system.
  `in-exit-chan`: The exit signal channel.
  `sanitize-state-fn`: (fn [prev-service->state cur-services] ...).
                       Sanitizes the previous state based on services available currently.
  `service->state-fn`: (fn [service cur-state data] ...).
                       Transforms `data` into state to be used by the gc-service? function.
  `gc-service?`: (fn [service {:keys [state last-modified-time]} cur-time] ...).
                 Predicate function that returns true for apps that need to be gc-ed.
  `perform-gc-fn`: (fn [service] ...). Function that performs GC of the service.
                   It must return a truth-y value when successful."
  [read-state-fn write-state-fn leader? clock name service-data-chan timeout-interval-ms sanitize-state-fn service->state-fn gc-service? perform-gc-fn]
  {:pre (pos? timeout-interval-ms)}
  (let [query-chan (async/chan 10)
        exit-chan (async/chan 1)]
    (async/go-loop [iter 0
                    timeout-chan (async/timeout timeout-interval-ms)]
      (let [[chan args] (async/alt!
                          exit-chan ([_] [:exit])
                          query-chan ([args] [:query args])
                          timeout-chan ([_] [:continue])
                          :priority true)]
        (case chan
          :exit (log/info "[service-gc-go-routine] exiting" name)
          :query (let [{:keys [service-id response-chan]} args
                       state (get (read-state-fn name) service-id)]
                   (async/>! response-chan (or state {}))
                   (recur (inc iter) timeout-chan))
          :continue
          (do
            (when (leader?)
              (let [service->raw-data (async/<! service-data-chan)]
                (cid/with-correlation-id
                  (str name "-" iter)
                  (timers/start-stop-time!
                    (metrics/waiter-timer "gc" name "iteration-duration")
                    (try
                      (let [service->state (or (read-state-fn name) {})
                            current-time (clock)
                            service->state' (apply merge
                                                   (sanitize-state-fn service->state (keys service->raw-data))
                                                   (map
                                                     (fn [[service raw-data]]
                                                       (let [cur-state (get service->state service)
                                                             new-state (service->state-fn service (:state cur-state) raw-data)]
                                                         (if (= (:state cur-state) new-state)
                                                           [service cur-state]
                                                           [service {:state new-state
                                                                     :last-modified-time current-time}])))
                                                     service->raw-data))
                            apps-to-gc (map first
                                            (filter (fn [[service state]]
                                                      (when-let [gc-service (gc-service? service state current-time)]
                                                        (log/info service "with state" (:state state) "and last modified time"
                                                                  (utils/date-to-str (:last-modified-time state)) "marked for deletion")
                                                        gc-service))
                                                    service->state'))
                            apps-successfully-gced (filter (fn [service]
                                                             (try
                                                               (when (leader?) ; check to ensure still the leader
                                                                 (perform-gc-fn service))
                                                               (catch Exception e
                                                                 (log/error e (str "error in deleting: " service)))))
                                                           apps-to-gc)
                            apps-failed-to-delete (set/difference (set apps-to-gc) (set apps-successfully-gced))
                            service->state'' (apply dissoc service->state' apps-successfully-gced)]
                        (when (or (not= (set (keys service->state'')) (set (keys service->raw-data)))
                                  (not= (set (keys service->state'')) (set (keys service->state))))
                          (log/info "state has" (count service->state'') "active services, received"
                                    (count service->raw-data) "services in latest update."))
                        (when (not-empty apps-failed-to-delete)
                          (log/warn "unable to delete services:" apps-failed-to-delete))
                        (write-state-fn name service->state''))
                      (catch Exception e
                        (log/error e "error in" name {:iteration iter})))))))
            (recur (inc iter) (async/timeout timeout-interval-ms))))))
    {:exit exit-chan
     :query query-chan}))

(defn make-inter-router-requests
  "Helper function to make inter-router requests with basic authentication.
   It assumes that the response from inter-router communication always supports json."
  [make-request-fn make-basic-auth-fn my-router-id discovery passwords endpoint &
   {:keys [acceptable-router? body config method]
    :or {acceptable-router? (constantly true)
         body ""
         config {}
         method :get}}]
  (let [router-id->endpoint-url (discovery/router-id->endpoint-url discovery "http" endpoint :exclude-set #{my-router-id})
        router-id->endpoint-url' (filter (fn [[router-id _]] (acceptable-router? router-id)) router-id->endpoint-url)
        config' (update config :headers assoc "accept" "application/json")]
    (when (and (empty? router-id->endpoint-url')
               (not-empty router-id->endpoint-url))
      (log/info "no acceptable routers found to make request!"))
    (loop [[[dest-router-id endpoint-url] & remaining-items] router-id->endpoint-url'
           router-id->response {}]
      (if dest-router-id
        (let [secret-word (utils/generate-secret-word my-router-id dest-router-id passwords)
              auth (make-basic-auth-fn endpoint-url my-router-id secret-word)
              response (make-request-fn method endpoint-url auth body config')]
          (recur remaining-items
                 (assoc router-id->response dest-router-id response)))
        router-id->response))))

(defn make-request-async
  "Makes an asynchronous request to the endpoint using the provided authentication scheme.
   Returns a core.async channel that will return the response map."
  [http-client idle-timeout method endpoint-url auth body config]
  (http/request http-client
                (merge
                  {:auth auth
                   :body body
                   :follow-redirects? false
                   :idle-timeout idle-timeout
                   :method method
                   :url endpoint-url}
                  config)))

(defn make-request-sync
  "Makes a synchronous request to the endpoint using the provided authentication scheme.
   Returns a response map with the body, status and headers populated."
  [http-client idle-timeout method endpoint-url auth body config]
  (let [resp-chan (make-request-async http-client idle-timeout method endpoint-url auth body config)
        {:keys [body error] :as response} (async/<!! resp-chan)]
    (if error
      (log/error error "Error in communicating at" endpoint-url)
      (let [body-response (async/<!! body)]
        ;; response has been read, close as we do not use chunked encoding in inter-router
        (async/close! body)
        (assoc response :body body-response)))))

(defn waiter-request?-factory
  "Creates a function that determines for a given request whether or not
  the request is intended for Waiter itself or a service of Waiter."
  [valid-waiter-hostnames]
  (let [valid-waiter-hostnames (set/union (set valid-waiter-hostnames) #{"localhost" "127.0.0.1"})]
    (fn waiter-request? [{:keys [uri headers]}]
      (let [{:strs [host]} headers]
        (or (#{"/app-name" "/service-id" "/token"} uri) ; special urls that are always for Waiter (FIXME)
            (some #(str/starts-with? (str uri) %)
                  ["/waiter-async/complete/" "/waiter-async/result/" "/waiter-async/status/" "/waiter-consent"])
            (and (or (str/blank? host)
                     (valid-waiter-hostnames (-> host
                                                 (str/split #":")
                                                 first)))
                 (not-any? #(str/starts-with? (key %) headers/waiter-header-prefix) headers)))))))

(defn leader-fn-factory
  "Creates the leader? function.
   Leadership is decided by the leader latch and presence of at least `min-cluster-routers` peers."
  [router-id has-leadership? discovery min-cluster-routers]
  #(and (has-leadership?)
        ; no one gets to be the leader if there aren't at least min-cluster-routers in the clique
        (let [num-routers (discovery/cluster-size discovery)]
          (when (< num-routers min-cluster-routers)
            (log/info router-id "relinquishing leadership as there are too few routers in cluster:" num-routers))
          (>= num-routers min-cluster-routers))))

(defn- ^HttpClient http-client-factory
  "Creates an instance of HttpClient with the specified timeout."
  [connection-timeout-ms]
  (let [client (http/client {:connect-timeout connection-timeout-ms
                             :follow-redirects? false})
        _ (.clear (.getContentDecoderFactories client))
        _ (.setCookieStore client (HttpCookieStore$Empty.))]
    client))

;; PRIVATE API
(def state
  {:async-request-store-atom (pc/fnk [] (atom {}))
   :authenticator (pc/fnk [[:settings authenticator-config]
                           passwords]
                    (utils/create-component authenticator-config :context {:password (first passwords)}))
   :clock (pc/fnk [] t/now)
   :cors-validator (pc/fnk [[:settings cors-config]]
                     (utils/create-component cors-config))
   :entitlement-manager (pc/fnk [[:settings entitlement-config]]
                          (utils/create-component entitlement-config))
   :http-client (pc/fnk [[:settings [:instance-request-properties connection-timeout-ms]]]
                  (http-client-factory connection-timeout-ms))
   :instance-rpc-chan (pc/fnk [] (async/chan 1024)) ; TODO move to service-chan-maintainer
   :passwords (pc/fnk [[:settings password-store-config]]
                (let [password-provider (utils/create-component password-store-config)
                      passwords (password-store/retrieve-passwords password-provider)
                      _ (password-store/check-empty-passwords passwords)
                      processed-passwords (mapv #(vector :cached %) passwords)]
                  processed-passwords))
   :query-app-maintainer-chan (pc/fnk [] (au/latest-chan)) ; TODO move to service-chan-maintainer
   :router-metrics-agent (pc/fnk [router-id] (metrics-sync/new-router-metrics-agent router-id {}))
   :router-id (pc/fnk [[:settings router-id-prefix]]
                (cond->> (utils/unique-identifier)
                         (not (str/blank? router-id-prefix))
                         (str (str/replace router-id-prefix #"[@.]" "-") "-")))
   :scheduler (pc/fnk [[:settings scheduler-config]
                       service-id-prefix]
                (let [is-waiter-app?-fn
                      (fn is-waiter-app? [^String service-id]
                        (str/starts-with? service-id service-id-prefix))]
                  (utils/create-component scheduler-config :context {:is-waiter-app?-fn is-waiter-app?-fn})))
   :scheduler-state-chan (pc/fnk [] (au/latest-chan))
   :service-description-builder (pc/fnk [[:settings service-description-builder-config]]
                                  (utils/create-component service-description-builder-config))
   :service-id-prefix (pc/fnk [[:settings [:cluster-config name]]]
                        (str name "-"))
   :start-app-cache-atom (pc/fnk []
                           (-> {}
                               (cache/fifo-cache-factory :threshold 100)
                               (cache/ttl-cache-factory :ttl (-> 1 t/minutes t/in-millis))
                               atom))
   :task-threadpool (pc/fnk [] (Executors/newFixedThreadPool 20))
   :thread-id->stack-state-atom (pc/fnk [] (atom {}))
   :waiter-hostnames (pc/fnk [[:settings hostname]]
                             (set (if (sequential? hostname)
                                    hostname
                                    [hostname])))
   :websocket-client (pc/fnk [[:settings [:websocket-config ws-max-binary-message-size ws-max-text-message-size]]
                              http-client]
                       (let [websocket-client (WebSocketClient. ^HttpClient http-client)]
                         (doto (.getPolicy websocket-client)
                           (.setMaxBinaryMessageSize ws-max-binary-message-size)
                           (.setMaxTextMessageSize ws-max-text-message-size))
                         websocket-client))})

(def curator
  {:curator (pc/fnk [[:settings [:zookeeper [:curator-retry-policy base-sleep-time-ms max-retries max-sleep-time-ms] connect-string]]]
              (let [retry-policy (BoundedExponentialBackoffRetry. base-sleep-time-ms max-sleep-time-ms max-retries)
                    zk-connection-string (if (= :in-process connect-string)
                                           (:zk-connection-string (curator/start-in-process-zookeeper))
                                           connect-string)
                    curator (CuratorFrameworkFactory/newClient zk-connection-string 5000 5000 retry-policy)]
                (.start curator)
                ; register listener that notifies of sync call completions
                (-> (.getCuratorListenable curator)
                    (.addListener (reify CuratorListener
                                    (eventReceived [_ _ event]
                                      (when (= CuratorEventType/SYNC (.getType event))
                                        (log/info "received SYNC event for" (.getPath event))
                                        (when-let [response-promise (.getContext event)]
                                          (log/info "releasing response promise provided for" (.getPath event))
                                          (deliver response-promise :release)))))))
                curator))
   :curator-base-init (pc/fnk [curator [:settings [:zookeeper base-path]]]
                        (curator/create-path curator base-path :create-parent-zknodes? true))
   :discovery (pc/fnk [[:settings [:cluster-config name] [:zookeeper base-path discovery-relative-path] host port]
                       [:state router-id]
                       curator]
                (discovery/register router-id curator name (str base-path "/" discovery-relative-path) {:host host :port port}))
   :gc-base-path (pc/fnk [[:settings [:zookeeper base-path gc-relative-path]]]
                   (str base-path "/" gc-relative-path))
   :gc-state-reader-fn (pc/fnk [curator gc-base-path]
                         (fn read-gc-state [name]
                           (:data (curator/read-path curator (str gc-base-path "/" name)
                                                     :nil-on-missing? true :serializer :nippy))))
   :gc-state-writer-fn (pc/fnk [curator gc-base-path]
                         (fn write-gc-state [name state]
                           (curator/write-path curator (str gc-base-path "/" name) state
                                               :serializer :nippy :create-parent-zknodes? true)))
   :kv-store (pc/fnk [[:settings [:zookeeper base-path] kv-config]
                      [:state passwords]
                      curator]
               (kv/new-kv-store kv-config curator base-path passwords))
   :leader?-fn (pc/fnk [[:settings [:cluster-config min-routers]]
                        [:state router-id]
                        discovery
                        leader-latch]
                 (let [has-leadership? #(.hasLeadership leader-latch)]
                   (leader-fn-factory router-id has-leadership? discovery min-routers)))
   :leader-id-fn (pc/fnk [leader-latch]
                   #(try
                      (-> leader-latch .getLeader .getId)
                      (catch Exception ex
                        (log/error ex "unable to retrieve leader id"))))
   :leader-latch (pc/fnk [[:settings [:zookeeper base-path leader-latch-relative-path]]
                          [:state router-id]
                          curator]
                   (let [leader-latch-path (str base-path "/" leader-latch-relative-path)
                         latch (LeaderLatch. curator leader-latch-path router-id)]
                     (.start latch)
                     latch))})

(def routines
  {:allowed-to-manage-service?-fn (pc/fnk [[:curator kv-store]
                                           [:state entitlement-manager]]
                                    (fn allowed-to-manage-service? [service-id auth-user]
                                      ; Returns whether the authenticated user is allowed to manage the service.
                                      ; Either she can run as the waiter user or the run-as-user of the service description."
                                      (sd/can-manage-service? kv-store entitlement-manager service-id auth-user)))
   :assoc-run-as-user-approved? (pc/fnk [[:settings consent-expiry-days]
                                         [:state clock passwords]
                                         token->token-description]
                                  (fn assoc-run-as-user-approved? [{:keys [headers]} service-id]
                                    (let [{:strs [cookie host]} headers
                                          token (when-not (headers/contains-waiter-header headers sd/on-the-fly-service-description-keys)
                                                  (utils/authority->host host))
                                          {:keys [token-metadata]} (when token (token->token-description token))
                                          service-consent-cookie (cookie-support/cookie-value cookie "x-waiter-consent")
                                          decoded-cookie (when service-consent-cookie
                                                           (some #(cookie-support/decode-cookie-cached service-consent-cookie %1)
                                                                 passwords))]
                                      (sd/assoc-run-as-user-approved? clock consent-expiry-days service-id token token-metadata decoded-cookie))))
   :async-request-terminate-fn (pc/fnk [[:state async-request-store-atom]]
                                 (fn async-request-terminate [request-id]
                                   (async-req/async-request-terminate async-request-store-atom request-id)))
   :async-trigger-terminate-fn (pc/fnk [[:state router-id]
                                        async-request-terminate-fn
                                        make-inter-router-requests-sync-fn]
                                 (fn async-trigger-terminate-fn [target-router-id service-id request-id]
                                   (async-req/async-trigger-terminate
                                     async-request-terminate-fn make-inter-router-requests-sync-fn router-id target-router-id service-id request-id)))
   :authentication-method-wrapper-fn (pc/fnk [[:state authenticator]]
                                       (fn authentication-method-wrapper [request-handler]
                                         (let [auth-handler (auth/wrap-auth-handler authenticator request-handler)]
                                           (fn authenticate-request [request]
                                             (if (:skip-authentication request)
                                               (do
                                                 (log/info "skipping authentication for request")
                                                 (request-handler request))
                                               (auth-handler request))))))
   :can-run-as?-fn (pc/fnk [[:state entitlement-manager]]
                     (fn can-run-as [auth-user run-as-user]
                       (authz/run-as? entitlement-manager auth-user run-as-user)))
   :crypt-helpers (pc/fnk [[:state passwords]]
                    (let [password (first passwords)]
                      {:bytes-decryptor (fn bytes-decryptor [data] (utils/compressed-bytes->map data password))
                       :bytes-encryptor (fn bytes-encryptor [data] (utils/map->compressed-bytes data password))}))
   :delegate-instance-kill-request-fn (pc/fnk [[:curator discovery]
                                               [:state router-id]
                                               make-inter-router-requests-sync-fn]
                                        (fn delegate-instance-kill-request-fn
                                          [service-id]
                                          (delegate-instance-kill-request
                                            service-id (discovery/router-ids discovery :exclude-set #{router-id})
                                            (partial make-kill-instance-request make-inter-router-requests-sync-fn service-id))))
   :determine-priority-fn (pc/fnk []
                            (let [position-generator-atom (atom 0)]
                              (fn determine-priority-fn [waiter-headers]
                                (pr/determine-priority position-generator-atom waiter-headers))))
   :list-tokens-fn (pc/fnk [[:curator curator]
                            [:settings [:zookeeper base-path] kv-config]]
                     (fn list-tokens-fn []
                       (let [{:keys [relative-path]} kv-config]
                         (->> (kv/zk-keys curator (str base-path "/" relative-path))
                              (filter (fn [k] (not (str/starts-with? k "^"))))))))
   :make-basic-auth-fn (pc/fnk []
                         (fn make-basic-auth-fn [endpoint username password]
                           (BasicAuthentication$BasicResult. (URI. endpoint) username password)))
   :make-http-request-fn (pc/fnk [[:settings instance-request-properties]
                                  [:state http-client]
                                  make-basic-auth-fn service-id->password-fn]
                           (handler/async-make-request-helper http-client instance-request-properties make-basic-auth-fn service-id->password-fn
                                                              pr/prepare-request-properties pr/make-request))
   :make-inter-router-requests-async-fn (pc/fnk [[:curator discovery]
                                                 [:settings [:instance-request-properties initial-socket-timeout-ms]]
                                                 [:state http-client passwords router-id]
                                                 make-basic-auth-fn]
                                          (letfn [(make-request-async-fn [method endpoint-url auth body config]
                                                    (make-request-async http-client initial-socket-timeout-ms method endpoint-url auth body config))]
                                            (fn make-inter-router-requests-async-fn [endpoint & args]
                                              (apply make-inter-router-requests make-request-async-fn make-basic-auth-fn router-id discovery passwords endpoint args))))
   :make-inter-router-requests-sync-fn (pc/fnk [[:curator discovery]
                                                [:settings [:instance-request-properties initial-socket-timeout-ms]]
                                                [:state http-client passwords router-id]
                                                make-basic-auth-fn]
                                         (letfn [(make-request-sync-fn [method endpoint-url auth body config]
                                                   (make-request-sync http-client initial-socket-timeout-ms method endpoint-url auth body config))]
                                           (fn make-inter-router-requests-sync-fn [endpoint & args]
                                             (apply make-inter-router-requests make-request-sync-fn make-basic-auth-fn router-id discovery passwords endpoint args))))
   :peers-acknowledged-blacklist-requests-fn (pc/fnk [[:curator discovery]
                                                      [:state router-id]
                                                      make-inter-router-requests-sync-fn]
                                               (fn peers-acknowledged-blacklist-requests
                                                 [instance short-circuit? blacklist-period-ms reason]
                                                 (let [router-ids (discovery/router-ids discovery :exclude-set #{router-id})]
                                                   (peers-acknowledged-blacklist-requests?
                                                     instance short-circuit? router-ids "blacklist"
                                                     (partial make-blacklist-request make-inter-router-requests-sync-fn blacklist-period-ms)
                                                     reason))))
   :post-process-async-request-response-fn (pc/fnk [[:state async-request-store-atom instance-rpc-chan router-id]
                                                    make-http-request-fn]
                                             (fn post-process-async-request-response-wrapper
                                               [service-id metric-group instance _ reason-map request-properties location response-headers]
                                               (async-req/post-process-async-request-response
                                                 router-id async-request-store-atom make-http-request-fn instance-rpc-chan service-id metric-group
                                                 instance reason-map request-properties location response-headers)))
   :prepend-waiter-url (pc/fnk [[:settings port hostname]]
                         (let [hostname (if (sequential? hostname) (first hostname) hostname)]
                           (fn [endpoint-url]
                             (if (str/blank? endpoint-url)
                               endpoint-url
                               (str "http://" hostname ":" port endpoint-url)))))
   :request->descriptor-fn (pc/fnk [[:curator kv-store]
                                    [:settings metric-group-mappings service-description-defaults]
                                    [:state service-description-builder service-id-prefix waiter-hostnames]
                                    assoc-run-as-user-approved?
                                    can-run-as?-fn]
                             (fn request->descriptor-fn [request]
                               (pr/request->descriptor service-description-defaults service-id-prefix kv-store waiter-hostnames can-run-as?-fn
                                                       metric-group-mappings service-description-builder assoc-run-as-user-approved? request)))
   :router-metrics-helpers (pc/fnk [[:state passwords router-metrics-agent]]
                             (let [password (first passwords)]
                               {:decryptor (fn router-metrics-decryptor [data] (utils/compressed-bytes->map data password))
                                :encryptor (fn router-metrics-encryptor [data] (utils/map->compressed-bytes data password))
                                :router-metrics-state-fn (fn router-metrics-state [] @router-metrics-agent)
                                :service-id->metrics-fn (fn service-id->metrics [] (metrics-sync/agent->service-id->metrics router-metrics-agent))
                                :service-id->router-id->metrics (fn service-id->router-id->metrics [service-id]
                                                                  (metrics-sync/agent->service-id->router-id->metrics router-metrics-agent service-id))}))
   :service-description->service-id (pc/fnk [[:state service-id-prefix]]
                                      (fn service-description->service-id [service-description]
                                        (sd/service-description->service-id service-id-prefix service-description)))
   :service-id->password-fn (pc/fnk [[:state passwords]]
                              (fn service-id->password [service-id]
                                (log/debug "generating password for" service-id)
                                (digest/md5 (str service-id (first passwords)))))
   :service-id->service-description-fn (pc/fnk [[:curator kv-store]
                                                [:settings service-description-defaults metric-group-mappings]]
                                         (fn service-id->service-description
                                           [service-id & {:keys [effective?] :or {effective? true}}]
                                           (sd/service-id->service-description
                                             kv-store service-id service-description-defaults
                                             metric-group-mappings :effective? effective?)))
   :start-new-service-fn (pc/fnk [[:state authenticator scheduler start-app-cache-atom task-threadpool]
                                  service-id->password-fn store-service-description-fn]
                           (fn start-new-service [{:keys [service-id core-service-description] :as descriptor}]
                             (let [run-as-user (get-in descriptor [:service-description "run-as-user"])]
                               (auth/check-user authenticator run-as-user service-id))
                             (service/start-new-service
                               scheduler service-id->password-fn descriptor start-app-cache-atom task-threadpool
                               :pre-start-fn #(store-service-description-fn service-id core-service-description))))
   :start-work-stealing-balancer-fn (pc/fnk [[:settings [:work-stealing offer-help-interval-ms reserve-timeout-ms]]
                                             [:state instance-rpc-chan router-id]
                                             make-inter-router-requests-async-fn router-metrics-helpers]
                                      (fn start-work-stealing-balancer [service-id]
                                        (let [{:keys [service-id->router-id->metrics]} router-metrics-helpers]
                                          (work-stealing/start-work-stealing-balancer
                                            instance-rpc-chan reserve-timeout-ms offer-help-interval-ms service-id->router-id->metrics
                                            make-inter-router-requests-async-fn router-id service-id))))
   :stop-work-stealing-balancer-fn (pc/fnk []
                                     (fn stop-work-stealing-balancer [service-id work-stealing-chan-map]
                                       (log/info "stopping work-stealing balancer for" service-id)
                                       (async/go
                                         (when-let [exit-chan (get work-stealing-chan-map [:exit-chan])]
                                           (async/>! exit-chan :exit)))))
   :store-service-description-fn (pc/fnk [[:curator kv-store]
                                          validate-service-description-fn]
                                   (fn store-service-description [service-id service-description]
                                     (sd/store-core kv-store service-id service-description validate-service-description-fn)))
   :synchronize-fn (pc/fnk [[:curator curator]
                            [:settings [:zookeeper base-path mutex-timeout-ms]]]
                     (fn synchronize-fn [path f]
                       (let [lock-path (str base-path "/" path)]
                         (curator/synchronize curator lock-path mutex-timeout-ms f))))
   :token->service-description-template (pc/fnk [[:curator kv-store]]
                                          (fn token->service-description-template [token]
                                            (sd/token->service-description-template kv-store token :error-on-missing false)))
   :token->token-description (pc/fnk [[:curator kv-store]]
                               (fn token->token-description [token]
                                 (sd/token->token-description kv-store token)))
   :validate-service-description-fn (pc/fnk [[:state service-description-builder]]
                                      (fn validate-service-description [service-description]
                                        (sd/validate service-description-builder service-description {})))
   :waiter-request?-fn (pc/fnk [[:state waiter-hostnames]]
                         (let [local-router (InetAddress/getLocalHost)
                               waiter-router-hostname (.getCanonicalHostName local-router)
                               waiter-router-ip (.getHostAddress local-router)
                               ;; use (set [...]) instead of #{...} below as there may be duplicate values
                               hostnames (set/union waiter-hostnames (set [waiter-router-hostname waiter-router-ip]))]
                           (waiter-request?-factory hostnames)))
   :websocket-request-auth-cookie-attacher (pc/fnk [[:state passwords router-id]]
                                             (fn websocket-request-auth-cookie-attacher [request]
                                               (ws/inter-router-request-middleware router-id (first passwords) request)))
   :websocket-request-authenticator (pc/fnk [[:state passwords]]
                                      (fn websocket-request-authenticator [request response]
                                        (ws/request-authenticator (first passwords) request response)))})

(def daemons
  {:autoscaler (pc/fnk [[:curator leader?-fn]
                        [:routines router-metrics-helpers service-id->service-description-fn]
                        [:settings [:scaling autoscaler-interval-ms]]
                        [:state scheduler]
                        autoscaling-multiplexer router-state-maintainer]
                 (let [service-id->metrics-fn (:service-id->metrics-fn router-metrics-helpers)
                       router-state-push-mult (get-in router-state-maintainer [:maintainer-chans :router-state-push-mult])
                       {:keys [executor-multiplexer-chan]} autoscaling-multiplexer]
                   (scaling/autoscaler-goroutine
                     {} leader?-fn service-id->metrics-fn executor-multiplexer-chan scheduler autoscaler-interval-ms
                     scaling/scale-app service-id->service-description-fn router-state-push-mult)))
   :autoscaling-multiplexer (pc/fnk [[:routines delegate-instance-kill-request-fn peers-acknowledged-blacklist-requests-fn]
                                     [:settings [:scaling inter-kill-request-wait-time-ms] blacklist-config]
                                     [:state instance-rpc-chan scheduler]]
                              (scaling/service-scaling-multiplexer
                                (fn scaling-executor-factory [service-id]
                                  (scaling/service-scaling-executor
                                    service-id scheduler instance-rpc-chan peers-acknowledged-blacklist-requests-fn
                                    delegate-instance-kill-request-fn inter-kill-request-wait-time-ms blacklist-config))
                                {}))
   :gc-for-transient-metrics (pc/fnk [[:routines router-metrics-helpers]
                                      [:settings metrics-config]
                                      [:state clock]
                                      scheduler-maintainer]
                               (let [state-store-atom (atom {})
                                     read-state-fn (fn read-state [_] @state-store-atom)
                                     write-state-fn (fn write-state [_ state] (reset! state-store-atom state))
                                     leader?-fn (constantly true)
                                     service-gc-go-routine (partial service-gc-go-routine read-state-fn write-state-fn leader?-fn clock)
                                     scheduler-state-chan (async/tap (:scheduler-state-mult-chan scheduler-maintainer) (au/latest-chan))
                                     {:keys [service-id->metrics-fn]} router-metrics-helpers
                                     {:keys [service-id->metrics-chan] :as metrics-gc-chans} (metrics/transient-metrics-gc scheduler-state-chan service-gc-go-routine metrics-config)]
                                 (metrics/transient-metrics-data-producer service-id->metrics-chan service-id->metrics-fn metrics-config)
                                 metrics-gc-chans))
   :messages (pc/fnk [[:settings {messages nil}]]
               (when messages
                 (utils/load-messages messages)))
   :router-list-maintainer (pc/fnk [[:curator discovery]
                                    [:settings router-syncer]]
                             (let [{:keys [delay-ms interval-ms]} router-syncer
                                   router-chan (au/latest-chan)
                                   router-mult-chan (async/mult router-chan)]
                               (state/start-router-syncer discovery router-chan interval-ms delay-ms)
                               {:router-mult-chan router-mult-chan}))
   :router-metrics-syncer (pc/fnk [[:routines crypt-helpers websocket-request-auth-cookie-attacher]
                                   [:settings [:metrics-config inter-router-metrics-idle-timeout-ms metrics-sync-interval-ms router-update-interval-ms]]
                                   [:state router-metrics-agent websocket-client]
                                   router-list-maintainer]
                            (let [{:keys [bytes-encryptor]} crypt-helpers
                                  router-chan (async/tap (:router-mult-chan router-list-maintainer) (au/latest-chan))]
                              {:metrics-syncer (metrics-sync/setup-metrics-syncer router-metrics-agent metrics-sync-interval-ms bytes-encryptor)
                               :router-syncer (metrics-sync/setup-router-syncer router-chan router-metrics-agent router-update-interval-ms
                                                                                inter-router-metrics-idle-timeout-ms metrics-sync-interval-ms
                                                                                websocket-client bytes-encryptor websocket-request-auth-cookie-attacher)}))
   :router-state-maintainer (pc/fnk [[:routines service-id->service-description-fn]
                                     [:settings deployment-error-config]
                                     [:state router-id]
                                     router-list-maintainer scheduler-maintainer]
                              (let [scheduler-state-chan (async/tap (:scheduler-state-mult-chan scheduler-maintainer) (au/latest-chan))
                                    exit-chan (async/chan)
                                    router-chan (async/tap (:router-mult-chan router-list-maintainer) (au/latest-chan))
                                    maintainer-chan (state/start-router-state-maintainer
                                                      scheduler-state-chan router-chan router-id exit-chan service-id->service-description-fn deployment-error-config)]
                                {:exit-chan exit-chan
                                 :maintainer-chans maintainer-chan}))
   :scheduler-broken-services-gc (pc/fnk [[:curator gc-state-reader-fn gc-state-writer-fn leader?-fn]
                                          [:settings scheduler-gc-config]
                                          [:state clock scheduler]
                                          scheduler-maintainer]
                                   (let [scheduler-state-chan (async/tap (:scheduler-state-mult-chan scheduler-maintainer) (au/latest-chan))
                                         service-gc-go-routine (partial service-gc-go-routine gc-state-reader-fn gc-state-writer-fn leader?-fn clock)]
                                     (scheduler/scheduler-broken-services-gc scheduler scheduler-state-chan scheduler-gc-config service-gc-go-routine)))
   :scheduler-maintainer (pc/fnk [[:routines service-id->service-description-fn]
                                  [:settings [:health-check-config health-check-timeout-ms failed-check-threshold] scheduler-syncer-interval-secs]
                                  [:state scheduler]]
                           (let [scheduler-state-chan (au/latest-chan)
                                 scheduler-state-mult-chan (async/mult scheduler-state-chan)
                                 http-client (http/client {:connect-timeout health-check-timeout-ms
                                                           :idle-timeout health-check-timeout-ms})]
                             (assoc (scheduler/start-scheduler-syncer
                                      scheduler scheduler-state-chan scheduler-syncer-interval-secs
                                      service-id->service-description-fn scheduler/available? http-client failed-check-threshold)
                               :scheduler-state-mult-chan scheduler-state-mult-chan)))
   :scheduler-services-gc (pc/fnk [[:curator gc-state-reader-fn gc-state-writer-fn leader?-fn]
                                   [:routines router-metrics-helpers service-id->service-description-fn]
                                   [:settings scheduler-gc-config]
                                   [:state clock scheduler]
                                   scheduler-maintainer]
                            (let [scheduler-state-chan (async/tap (:scheduler-state-mult-chan scheduler-maintainer) (au/latest-chan))
                                  {:keys [service-id->metrics-fn]} router-metrics-helpers
                                  service-gc-go-routine (partial service-gc-go-routine gc-state-reader-fn gc-state-writer-fn leader?-fn clock)]
                              (scheduler/scheduler-services-gc
                                scheduler scheduler-state-chan service-id->metrics-fn scheduler-gc-config service-gc-go-routine service-id->service-description-fn)))
   :service-chan-maintainer (pc/fnk [[:routines start-work-stealing-balancer-fn stop-work-stealing-balancer-fn]
                                     [:state instance-rpc-chan query-app-maintainer-chan]
                                     [:settings blacklist-config [:instance-request-properties queue-timeout-ms]]
                                     router-state-maintainer]
                              (let [start-service
                                    (fn start-service [service-id]
                                      (let [maintainer-chan-map (state/prepare-and-start-service-chan-responder service-id queue-timeout-ms blacklist-config)
                                            workstealing-chan-map (start-work-stealing-balancer-fn service-id)]
                                        {:maintainer-chan-map maintainer-chan-map
                                         :work-stealing-chan-map workstealing-chan-map}))
                                    remove-service
                                    (fn remove-service [service-id {:keys [maintainer-chan-map work-stealing-chan-map]}]
                                      (state/close-update-state-channel service-id maintainer-chan-map)
                                      (stop-work-stealing-balancer-fn service-id work-stealing-chan-map))
                                    retrieve-channel
                                    (fn retrieve-channel [channel-map method]
                                      (let [method-chan (case method
                                                          :blacklist [:maintainer-chan-map :blacklist-instance-chan]
                                                          :kill [:maintainer-chan-map :kill-instance-chan]
                                                          :offer [:maintainer-chan-map :work-stealing-chan]
                                                          :query-state [:maintainer-chan-map :query-state-chan]
                                                          :query-work-stealing [:work-stealing-chan-map :query-chan]
                                                          :release [:maintainer-chan-map :release-instance-chan]
                                                          :reserve [:maintainer-chan-map :reserve-instance-chan-in]
                                                          :update-state [:maintainer-chan-map :update-state-chan])]
                                        (get-in channel-map method-chan)))
                                    state-chan-mult (get-in router-state-maintainer [:maintainer-chans :router-state-push-mult])
                                    state-chan (au/latest-chan)]
                                (async/tap state-chan-mult state-chan)
                                (state/start-service-chan-maintainer
                                  {} instance-rpc-chan state-chan query-app-maintainer-chan start-service remove-service retrieve-channel)))
   :state-query-chans (pc/fnk [[:state query-app-maintainer-chan]
                               autoscaler autoscaling-multiplexer gc-for-transient-metrics scheduler-broken-services-gc scheduler-maintainer scheduler-services-gc]
                        {:app-maintainer-state query-app-maintainer-chan
                         :autoscaler-state (:query autoscaler)
                         :autoscaling-multiplexer-state (:query-chan autoscaling-multiplexer)
                         :scheduler-broken-services-gc-state (:query scheduler-broken-services-gc)
                         :scheduler-services-gc-state (:query scheduler-services-gc)
                         :scheduler-state (:query-chan scheduler-maintainer)
                         :transient-metrics-gc-state (:query gc-for-transient-metrics)})
   :statsd (pc/fnk [[:routines service-id->service-description-fn]
                    [:settings statsd]
                    scheduler-maintainer]
             (when (not= statsd :disabled)
               (statsd/setup statsd)
               (let [scheduler-state-chan (async/tap (:scheduler-state-mult-chan scheduler-maintainer) (au/latest-chan))
                     exit-chan (async/chan)]
                 (statsd/start-scheduler-metrics-publisher scheduler-state-chan exit-chan service-id->service-description-fn))))})

(def request-handlers
  {:app-name-handler-fn (pc/fnk [service-id-handler-fn]
                          service-id-handler-fn)
   :async-complete-handler-fn (pc/fnk [[:routines async-request-terminate-fn]
                                       handle-inter-router-request-fn]
                                (fn async-complete-handler-fn [request]
                                  (handle-inter-router-request-fn
                                    (fn inner-async-complete-handler-fn [src-router-id request]
                                      (handler/complete-async-handler async-request-terminate-fn src-router-id request))
                                    request)))
   :async-result-handler-fn (pc/fnk [[:routines async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn]
                                     handle-secure-request-fn]
                              (fn async-result-handler-fn [request]
                                (handle-secure-request-fn
                                  (fn inner-async-result-handler-fn [request]
                                    (handler/async-result-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))
                                  request)))
   :async-status-handler-fn (pc/fnk [[:routines async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn]
                                     handle-secure-request-fn]
                              (fn async-status-handler-fn [request]
                                (handle-secure-request-fn
                                  (fn inner-async-status-handler-fn [request]
                                    (handler/async-status-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))
                                  request)))
   :blacklist-instance-handler-fn (pc/fnk [[:state instance-rpc-chan]
                                           handle-inter-router-request-fn]
                                    (fn blacklist-instance-handler-fn [request]
                                      (handle-inter-router-request-fn
                                        (fn blacklist-instance-handler [_ request]
                                          (handler/blacklist-instance instance-rpc-chan request))
                                        request)))
   :blacklisted-instances-list-handler-fn (pc/fnk [[:state instance-rpc-chan]]
                                            (fn blacklisted-instances-list-handler-fn [{{:keys [service-id]} :route-params :as request}]
                                              (handler/get-blacklisted-instances instance-rpc-chan service-id request)))
   :cors-preflight-handler-fn (pc/fnk [[:settings cors-config]
                                       [:state cors-validator]]
                                (let [{max-age :max-age} cors-config]
                                  (fn cors-preflight-handler-fn [request]
                                    (try
                                      (cors/preflight-handler cors-validator max-age request)
                                      (catch Exception e
                                        (utils/exception->response (ex-info "Error in CORS pre-flight handler" {} e) request))))))
   :default-websocket-handler-fn (pc/fnk [[:routines determine-priority-fn prepend-waiter-url request->descriptor-fn service-id->password-fn start-new-service-fn]
                                          [:settings instance-request-properties]
                                          [:state instance-rpc-chan passwords router-id websocket-client]]
                                   (fn default-websocket-handler-fn [request]
                                     (let [password (first passwords)
                                           process-handlers []
                                           make-request-fn (fn make-ws-request
                                                             [instance request request-properties passthrough-headers end-route metric-group]
                                                             (ws/make-request websocket-client service-id->password-fn instance request request-properties
                                                                              passthrough-headers end-route metric-group))]
                                       (letfn [(process-request-fn [request]
                                                 (pr/process router-id make-request-fn instance-rpc-chan request->descriptor-fn start-new-service-fn
                                                             instance-request-properties process-handlers prepend-waiter-url
                                                             determine-priority-fn ws/process-response! ws/process-exception-in-request
                                                             ws/abort-request-callback-factory request))]
                                         (ws/request-handler password process-request-fn request)))))
   :display-settings-handler-fn (pc/fnk [handle-secure-request-fn settings]
                                  (fn display-settings-handler-fn [request]
                                    (handle-secure-request-fn
                                      (fn inner-display-settings-handler-fn [_]
                                        (settings/display-settings settings))
                                      request)))
   :favicon-handler-fn (pc/fnk []
                         (fn favicon-handler-fn [_]
                           {:body (io/input-stream (io/resource "web/favicon.ico"))
                            :content-type "image/png"}))
   :handle-authentication-wrapper-fn (pc/fnk [[:curator kv-store]
                                              [:state waiter-hostnames]]
                                       (fn handle-authentication-wrapper-fn [request-handler {:keys [headers] :as request}]
                                         (let [{:keys [passthrough-headers waiter-headers]} (headers/split-headers headers)
                                               {:keys [token]} (sd/retrieve-token-from-service-description-or-hostname waiter-headers passthrough-headers waiter-hostnames)
                                               {:strs [authentication] :as service-description} (and token (sd/token->service-description-template kv-store token :error-on-missing false))
                                               authentication-disabled? (= authentication "disabled")]
                                           (cond
                                             (contains? waiter-headers "x-waiter-authentication")
                                             (do
                                               (log/info "x-waiter-authentication is not supported as an on-the-fly header"
                                                         {:service-description service-description, :token token})
                                               (utils/map->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                                                         :status 400))

                                             ;; ensure service description formed comes entirely from the token by ensuring absence of on-the-fly headers
                                             (and authentication-disabled? (some sd/service-description-keys (-> waiter-headers headers/drop-waiter-header-prefix keys)))
                                             (do
                                               (log/info "request cannot proceed as it is mixing an authentication disabled token with on-the-fly headers"
                                                         {:service-description service-description, :token token})
                                               (utils/map->json-response {:error "An authentication disabled token may not be combined with on-the-fly headers"}
                                                                         :status 400))

                                             authentication-disabled?
                                             (do
                                               (log/info "request configured to skip authentication")
                                               (request-handler (assoc request :skip-authentication true)))

                                             :else
                                             (request-handler request)))))
   :handle-inter-router-request-fn (pc/fnk [[:state passwords router-id]]
                                     (fn handle-inter-router-request [on-succesful-auth-handler-fn request]
                                       (let [src-router-id (promise)
                                             router-comm-authenticated?
                                             (fn router-comm-authenticated? [my-router-id passwords source-id secret-word]
                                               (let [expected-word (utils/generate-secret-word source-id my-router-id passwords)
                                                     authenticated? (= expected-word secret-word)]
                                                 (log/info "Authenticating inter-router communication from" source-id)
                                                 (deliver src-router-id source-id)
                                                 (when-not authenticated?
                                                   (log/info "inter-router request authentication failed!"
                                                             {:actual secret-word, :expected expected-word}))
                                                 authenticated?))
                                             basic-auth-handler (basic-authentication/wrap-basic-authentication
                                                                  (fn [request]
                                                                    (deliver src-router-id "") ; ensure promise has been delivered
                                                                    (on-succesful-auth-handler-fn @src-router-id request))
                                                                  (partial router-comm-authenticated? router-id passwords))]
                                         (basic-auth-handler request))))
   :handle-secure-request-fn (pc/fnk [[:routines authentication-method-wrapper-fn]
                                      [:state cors-validator]]
                               (fn handle-secure-request-fn [request-handler {:keys [uri] :as request}]
                                 (log/debug "secure request received at" uri)
                                 (let [handler (-> request-handler
                                                   (cors/handler cors-validator)
                                                   authentication-method-wrapper-fn)]
                                   (handler request))))
   :kill-instance-handler-fn (pc/fnk [[:routines peers-acknowledged-blacklist-requests-fn]
                                      [:settings [:scaling inter-kill-request-wait-time-ms] blacklist-config]
                                      [:state instance-rpc-chan scheduler]
                                      handle-inter-router-request-fn]
                               (fn kill-instance-handler-fn [request]
                                 (handle-inter-router-request-fn
                                   (fn inner-kill-instance-handler-fn [src-router-id request]
                                     (scaling/kill-instance-handler scheduler instance-rpc-chan inter-kill-request-wait-time-ms blacklist-config
                                                                    peers-acknowledged-blacklist-requests-fn src-router-id request))
                                   request)))
   :metrics-request-handler-fn (pc/fnk []
                                 (fn metrics-request-handler-fn [request]
                                   (handler/metrics-request-handler request)))
   :not-found-handler-fn (pc/fnk [] handler/not-found-handler)
   :process-request-fn (pc/fnk [[:routines determine-priority-fn make-basic-auth-fn post-process-async-request-response-fn
                                 prepend-waiter-url request->descriptor-fn service-id->password-fn start-new-service-fn]
                                [:settings instance-request-properties]
                                [:state http-client instance-rpc-chan router-id]
                                handle-authentication-wrapper-fn handle-secure-request-fn]
                         (let [process-handlers [pr/handle-suspended-service pr/handle-too-many-requests]
                               make-request-fn (fn [instance request request-properties passthrough-headers end-route metric-group]
                                                 (pr/make-request http-client make-basic-auth-fn service-id->password-fn
                                                                  instance request request-properties passthrough-headers end-route metric-group))
                               process-response-fn (partial pr/process-http-response post-process-async-request-response-fn)]
                           (fn process-request [request]
                             (handle-authentication-wrapper-fn
                               (fn handle-auth-process-request [request]
                                 (handle-secure-request-fn
                                   (fn inner-process-request [request]
                                     (pr/process router-id make-request-fn instance-rpc-chan request->descriptor-fn start-new-service-fn
                                                 instance-request-properties process-handlers prepend-waiter-url
                                                 determine-priority-fn process-response-fn pr/process-exception-in-http-request
                                                 pr/abort-http-request-callback-factory request))
                                   request))
                               request))))
   :router-metrics-handler-fn (pc/fnk [[:routines crypt-helpers]
                                       [:settings [:metrics-config metrics-sync-interval-ms]]
                                       [:state router-metrics-agent]]
                                (let [{:keys [bytes-decryptor bytes-encryptor]} crypt-helpers]
                                  (fn router-metrics-handler-fn [request]
                                    (metrics-sync/incoming-router-metrics-handler
                                      router-metrics-agent metrics-sync-interval-ms bytes-encryptor bytes-decryptor request))))
   :service-handler-fn (pc/fnk [[:curator kv-store]
                                [:routines allowed-to-manage-service?-fn make-inter-router-requests-sync-fn prepend-waiter-url]
                                [:state router-id scheduler]
                                handle-secure-request-fn]
                         (fn service-handler-fn [request]
                           (handle-secure-request-fn
                             (fn inner-service-handler-fn [{:as request {:keys [service-id]} :route-params}]
                               (handler/service-handler router-id service-id scheduler kv-store allowed-to-manage-service?-fn
                                                        prepend-waiter-url make-inter-router-requests-sync-fn request))
                             request)))
   :service-id-handler-fn (pc/fnk [[:curator kv-store]
                                   [:routines request->descriptor-fn store-service-description-fn]
                                   handle-secure-request-fn]
                            (fn service-name-handler-fn [request]
                              (handle-secure-request-fn
                                (fn inner-service-name-handler-fn [request]
                                  (handler/service-name-handler request request->descriptor-fn kv-store store-service-description-fn))
                                request)))
   :service-list-handler-fn (pc/fnk [[:daemons router-state-maintainer]
                                     [:routines prepend-waiter-url service-id->service-description-fn]
                                     [:state entitlement-manager]
                                     handle-secure-request-fn]
                              (let [state-chan (get-in router-state-maintainer [:maintainer-chans :state-chan])]
                                (fn service-list-handler-fn [request]
                                  (handle-secure-request-fn
                                    (fn inner-service-list-handler-fn [request]
                                      (handler/list-services-handler entitlement-manager state-chan prepend-waiter-url
                                                                     service-id->service-description-fn request))
                                    request))))
   :service-override-handler-fn (pc/fnk [[:curator kv-store]
                                         [:routines allowed-to-manage-service?-fn make-inter-router-requests-sync-fn]
                                         handle-secure-request-fn]
                                  (fn service-override-handler-fn [request]
                                    (handle-secure-request-fn
                                      (fn inner-service-override-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                        (handler/override-service-handler kv-store allowed-to-manage-service?-fn
                                                                          make-inter-router-requests-sync-fn service-id request))
                                      request)))
   :service-refresh-handler-fn (pc/fnk [[:curator kv-store]
                                        handle-inter-router-request-fn]
                                 (fn service-refresh-handler-fn [request]
                                   (handle-inter-router-request-fn
                                     (fn innermost-service-refresh-handler [src-router-id {{:keys [service-id]} :route-params}]
                                       (log/info service-id "refresh triggered by router" src-router-id)
                                       (sd/fetch-core kv-store service-id :refresh true)
                                       (sd/service-id->suspended-state kv-store service-id :refresh true)
                                       (sd/service-id->overrides kv-store service-id :refresh true))
                                     request)))
   :service-resume-handler-fn (pc/fnk [[:curator kv-store]
                                       [:routines allowed-to-manage-service?-fn make-inter-router-requests-sync-fn]
                                       handle-secure-request-fn]
                                (fn service-resume-handler-fn [request]
                                  (handle-secure-request-fn
                                    (fn inner-service-resume-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                      (handler/suspend-or-resume-service-handler
                                        kv-store allowed-to-manage-service?-fn make-inter-router-requests-sync-fn service-id :resume request))
                                    request)))
   :service-suspend-handler-fn (pc/fnk [[:curator kv-store]
                                        [:routines allowed-to-manage-service?-fn make-inter-router-requests-sync-fn]
                                        handle-secure-request-fn]
                                 (fn service-suspend-handler-fn [request]
                                   (handle-secure-request-fn
                                     (fn inner-service-suspend-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                       (handler/suspend-or-resume-service-handler
                                         kv-store allowed-to-manage-service?-fn make-inter-router-requests-sync-fn service-id :suspend request))
                                     request)))
   :service-view-logs-handler-fn (pc/fnk [[:routines prepend-waiter-url]
                                          [:state scheduler]
                                          handle-secure-request-fn]
                                   (fn service-view-logs-handler-fn [request]
                                     (handle-secure-request-fn
                                       (fn inner-service-view-logs-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                         (handler/service-view-logs-handler scheduler service-id prepend-waiter-url request))
                                       request)))
   :sim-request-handler (pc/fnk [] simulator/handle-sim-request)
   :state-all-handler-fn (pc/fnk [[:curator leader?-fn kv-store]
                                  [:daemons router-state-maintainer scheduler-maintainer]
                                  [:routines router-metrics-helpers]
                                  handle-secure-request-fn]
                           (let [state-chan (get-in router-state-maintainer [:maintainer-chans :state-chan])
                                 scheduler-query-chan (:query-chan scheduler-maintainer)
                                 router-metrics-state-fn (:router-metrics-state-fn router-metrics-helpers)]
                             (fn state-all-handler-fn [request]
                               (handle-secure-request-fn
                                 (fn inner-state-all-handler-fn [request]
                                   (handler/get-router-state state-chan scheduler-query-chan router-metrics-state-fn kv-store leader?-fn request))
                                 request))))
   :state-kv-store-handler-fn (pc/fnk [[:curator kv-store]
                                       [:state router-id]
                                       handle-secure-request-fn]
                                (fn kv-store-state-handler-fn [request]
                                  (handle-secure-request-fn
                                    (fn inner-kv-store-state-handler-fn [request]
                                      (handler/get-kv-store-state router-id kv-store request))
                                    request)))
   :state-leader-handler-fn (pc/fnk [[:curator leader?-fn leader-id-fn]
                                     [:state router-id]
                                     handle-secure-request-fn]
                              (fn leader-state-handler-fn [request]
                                (handle-secure-request-fn
                                  (fn inner-leader-state-handler-fn [request]
                                    (handler/get-leader-state router-id leader?-fn leader-id-fn request))
                                  request)))
   :state-maintainer-handler-fn (pc/fnk [[:daemons router-state-maintainer]
                                         [:state router-id]
                                         handle-secure-request-fn]
                                  (let [state-chan (get-in router-state-maintainer [:maintainer-chans :state-chan])]
                                    (fn maintainer-state-handler-fn [request]
                                      (handle-secure-request-fn
                                        (fn inner-maintainer-state-handler-fn [request]
                                          (handler/get-maintainer-state router-id state-chan request))
                                        request))))
   :state-router-metrics-handler-fn (pc/fnk [[:routines router-metrics-helpers]
                                             [:state router-id]
                                             handle-secure-request-fn]
                                      (let [router-metrics-state-fn (:router-metrics-state-fn router-metrics-helpers)]
                                        (fn router-metrics-state-handler-fn [request]
                                          (handle-secure-request-fn
                                            (fn inner-router-metrics-state-handler-fn [request]
                                              (handler/get-router-metrics-state router-id router-metrics-state-fn request))
                                            request))))
   :state-scheduler-handler-fn (pc/fnk [[:daemons scheduler-maintainer]
                                        [:state router-id]
                                        handle-secure-request-fn]
                                 (let [scheduler-query-chan (:query-chan scheduler-maintainer)]
                                   (fn scheduler-state-handler-fn [request]
                                     (handle-secure-request-fn
                                       (fn inner-scheduler-state-handler-fn [request]
                                         (handler/get-scheduler-state router-id scheduler-query-chan request))
                                       request))))
   :state-service-handler-fn (pc/fnk [[:daemons state-query-chans]
                                      [:state instance-rpc-chan router-id]
                                      handle-secure-request-fn]
                               (fn service-state-handler-fn [request]
                                 (handle-secure-request-fn
                                   (fn inner-service-state-handler-fn [{{:keys [service-id]} :route-params}]
                                     (handler/get-service-state router-id instance-rpc-chan service-id state-query-chans request))
                                   request)))
   :state-statsd-handler-fn (pc/fnk [[:state router-id]
                                     handle-secure-request-fn]
                              (fn statsd-state-handler-fn [request]
                                (handle-secure-request-fn
                                  (fn inner-settings-handler-fn [request]
                                    (handler/get-statsd-state router-id request))
                                  request)))
   :status-handler-fn (pc/fnk []
                        (fn status-handler-fn [_] {:body "ok" :headers {} :status 200}))
   :token-handler-fn (pc/fnk [[:curator kv-store]
                              [:routines make-inter-router-requests-sync-fn synchronize-fn validate-service-description-fn]
                              [:state clock entitlement-manager waiter-hostnames]
                              handle-secure-request-fn]
                       (fn token-handler-fn [request]
                         (handle-secure-request-fn
                           (fn inner-token-handler-fn [request]
                             (token/handle-token-request
                               clock synchronize-fn kv-store waiter-hostnames entitlement-manager make-inter-router-requests-sync-fn
                               validate-service-description-fn request))
                           request)))
   :token-list-handler-fn (pc/fnk [[:curator kv-store]
                                   handle-secure-request-fn]
                            (fn token-list-handler-fn [request]
                              (handle-secure-request-fn
                                (fn inner-token-handler-fn [request]
                                  (token/handle-list-tokens-request kv-store request))
                                request)))
   :token-owners-handler-fn (pc/fnk [[:curator kv-store]
                                     handle-secure-request-fn]
                              (fn token-owners-handler-fn [request]
                                (handle-secure-request-fn
                                  (fn inner-token-owners-handler-fn [request]
                                    (token/handle-list-token-owners-request kv-store request))
                                  request)))
   :token-refresh-handler-fn (pc/fnk [[:curator kv-store]
                                      handle-inter-router-request-fn]
                               (fn token-refresh-handler-fn [request]
                                 (handle-inter-router-request-fn
                                   (fn service-refresh-handler [src-router-id request]
                                     (token/handle-refresh-token-request kv-store src-router-id request))
                                   request)))
   :token-reindex-handler-fn (pc/fnk [[:curator kv-store]
                                      [:routines list-tokens-fn make-inter-router-requests-sync-fn synchronize-fn]
                                      handle-secure-request-fn]
                               (fn token-reindex-handler-fn [request]
                                 (handle-secure-request-fn
                                   (fn inner-token-handler-fn [request]
                                     (token/handle-reindex-tokens-request synchronize-fn make-inter-router-requests-sync-fn
                                                                          kv-store list-tokens-fn request))
                                   request)))
   :waiter-auth-handler-fn (pc/fnk [handle-secure-request-fn]
                             (fn waiter-auth-handler-fn [request]
                               (handle-secure-request-fn
                                 (fn inner-waiter-auth-handler-fn [request]
                                   {:body (str (:authorization/user request)), :status 200})
                                 request)))
   :waiter-acknowledge-consent-handler-fn (pc/fnk [[:routines service-description->service-id token->token-description]
                                                   [:settings consent-expiry-days]
                                                   [:state clock passwords]
                                                   handle-secure-request-fn]
                                            (let [password (first passwords)]
                                              (letfn [(add-encoded-cookie [response cookie-name value expiry-days]
                                                        (cookie-support/add-encoded-cookie response password cookie-name value expiry-days))
                                                      (consent-cookie-value [mode service-id token token-metadata]
                                                        (sd/consent-cookie-value clock mode service-id token token-metadata))]
                                                (fn waiter-acknowledge-consent-handler-fn [request]
                                                  (handle-secure-request-fn
                                                    (fn inner-waiter-acknowledge-consent-handler-fn [request]
                                                      (handler/acknowledge-consent-handler
                                                        token->token-description service-description->service-id
                                                        consent-cookie-value add-encoded-cookie consent-expiry-days request))
                                                    request)))))
   :waiter-request-consent-handler-fn (pc/fnk [[:routines service-description->service-id token->service-description-template]
                                               [:settings consent-expiry-days]
                                               handle-secure-request-fn]
                                        (fn waiter-request-consent-handler-fn [request]
                                          (handle-secure-request-fn
                                            (fn inner-waiter-request-consent-handler-fn [request]
                                              (handler/request-consent-handler
                                                token->service-description-template service-description->service-id
                                                consent-expiry-days request))
                                            request)))
   :welcome-handler-fn (pc/fnk [handle-secure-request-fn settings]
                         (partial handler/welcome-handler settings))
   :work-stealing-handler-fn (pc/fnk [[:state instance-rpc-chan]
                                      handle-inter-router-request-fn]
                               (fn work-stealing-handler-fn [request]
                                 (handle-inter-router-request-fn
                                   (fn [_ request] (handler/work-stealing-handler instance-rpc-chan request))
                                   request)))})
