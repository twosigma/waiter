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
(ns waiter.core
  (:require [bidi.bidi :as bidi]
            [clj-time.core :as t]
            [clojure.core.async :as async]
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
            [ring.middleware.ssl :as ssl]
            [ring.util.response :as rr]
            [waiter.async-request :as async-req]
            [waiter.auth.authentication :as auth]
            [waiter.authorization :as authz]
            [waiter.cookie-support :as cookie-support]
            [waiter.correlation-id :as cid]
            [waiter.cors :as cors]
            [waiter.curator :as curator]
            [waiter.descriptor :as descriptor]
            [waiter.discovery :as discovery]
            [waiter.handler :as handler]
            [waiter.headers :as headers]
            [waiter.interstitial :as interstitial]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.metrics-sync :as metrics-sync]
            [waiter.password-store :as password-store]
            [waiter.process-request :as pr]
            [waiter.reporter :as reporter]
            [waiter.scaling :as scaling]
            [waiter.scheduler :as scheduler]
            [waiter.service :as service]
            [waiter.service-description :as sd]
            [waiter.settings :as settings]
            [waiter.simulator :as simulator]
            [waiter.state :as state]
            [waiter.statsd :as statsd]
            [waiter.token :as token]
            [waiter.util.async-utils :as au]
            [waiter.util.cache-utils :as cu]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils]
            [waiter.websocket :as ws]
            [waiter.work-stealing :as work-stealing])
  (:import (java.net InetAddress URI)
           java.util.concurrent.Executors
           (javax.servlet ServletRequest)
           org.apache.curator.framework.CuratorFrameworkFactory
           org.apache.curator.framework.api.CuratorEventType
           org.apache.curator.framework.api.CuratorListener
           org.apache.curator.framework.recipes.leader.LeaderLatch
           org.apache.curator.retry.BoundedExponentialBackoffRetry
           org.eclipse.jetty.client.HttpClient
           org.eclipse.jetty.client.util.BasicAuthentication$BasicResult
           org.eclipse.jetty.websocket.client.WebSocketClient
           (org.eclipse.jetty.websocket.servlet ServletUpgradeResponse ServletUpgradeRequest)))

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
                     "service-id" :service-id-handler-fn
                     "settings" :display-settings-handler-fn
                     "sim" :sim-request-handler
                     "state" [["" :state-all-handler-fn]
                              ["/autoscaler" :state-autoscaler-handler-fn]
                              ["/autoscaling-multiplexer" :state-autoscaling-multiplexer-handler-fn]
                              ["/codahale-reporters" :state-codahale-reporters-handler-fn]
                              ["/fallback" :state-fallback-handler-fn]
                              ["/gc-broken-services" :state-gc-for-broken-services]
                              ["/gc-services" :state-gc-for-services]
                              ["/gc-transient-metrics" :state-gc-for-transient-metrics]
                              ["/interstitial" :state-interstitial-handler-fn]
                              ["/launch-metrics" :state-launch-metrics-handler-fn]
                              ["/kv-store" :state-kv-store-handler-fn]
                              ["/leader" :state-leader-handler-fn]
                              ["/local-usage" :state-local-usage-handler-fn]
                              ["/maintainer" :state-maintainer-handler-fn]
                              ["/router-metrics" :state-router-metrics-handler-fn]
                              ["/scheduler" :state-scheduler-handler-fn]
                              ["/service-description-builder" :state-service-description-builder-handler-fn]
                              ["/statsd" :state-statsd-handler-fn]
                              [["/" :service-id] :state-service-handler-fn]]
                     "status" :status-handler-fn
                     "token" :token-handler-fn
                     "tokens" {"" :token-list-handler-fn
                               "/owners" :token-owners-handler-fn
                               "/refresh" :token-refresh-handler-fn
                               "/reindex" :token-reindex-handler-fn}
                     "waiter-async" {["/complete/" :request-id "/" :service-id] :async-complete-handler-fn
                                     ["/result/" :request-id "/" :router-id "/" :service-id "/" :host "/" :port "/" [#".+" :location]]
                                     :async-result-handler-fn
                                     ["/status/" :request-id "/" :router-id "/" :service-id "/" :host "/" :port "/" [#".+" :location]]
                                     :async-status-handler-fn}
                     "waiter-auth" {"" :waiter-auth-handler-fn
                                    ["/" :authentication-provider "/" :operation] :waiter-auth-callback-handler-fn}
                     "waiter-consent" {"" :waiter-acknowledge-consent-handler-fn
                                       ["/" [#".*" :path]] :waiter-request-consent-handler-fn}
                     "waiter-interstitial" {["/" [#".*" :path]] :waiter-request-interstitial-handler-fn}
                     "waiter-kill-instance" {["/" :service-id] :kill-instance-handler-fn}
                     "waiter-ping" :ping-service-handler
                     "work-stealing" :work-stealing-handler-fn}]]
    (or (bidi/match-route routes uri)
        {:handler :not-found-handler-fn})))

(defn ring-handler-factory
  "Creates the handler for processing http requests."
  [waiter-request?-fn {:keys [process-request-fn] :as handlers}]
  (fn http-handler [{:keys [uri] :as request}]
    (if-not (waiter-request?-fn request)
      (do
        (counters/inc! (metrics/waiter-counter "requests" "service-request"))
        (process-request-fn request))
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

(defn attach-server-header-middleware
  "Attaches server header to the response if it is generated by waiter."
  [handler server-name]
  (fn attach-server-header-middleware-fn [request]
    (let [response (handler request)
          add-server-header (fn add-server-header-fn [response]
                              (cond-> response
                                (utils/waiter-generated-response? response)
                                (rr/header "server" server-name)))]
      (ru/update-response response add-server-header))))

(defn correlation-id-middleware
  "Attaches an x-cid header to the request and response if one is not already provided."
  [handler]
  (fn correlation-id-middleware-fn [request]
    (let [request (cid/ensure-correlation-id request utils/unique-identifier)
          request-cid (cid/http-object->correlation-id request)]
      (cid/with-correlation-id
        request-cid
        (log/info "request received:"
                  (-> (dissoc request :body :ctrl :in :out :request-time :server-name :server-port :servlet-request
                              :ssl-client-cert :support-info :trailers-fn)
                      (update :headers headers/truncate-header-values)))
        (let [response (handler request)
              get-request-cid (fn get-request-cid [] request-cid)]
          (if (map? response)
            (cid/ensure-correlation-id response get-request-cid)
            (async/go
              (let [nested-response (async/<! response)]
                (if (map? nested-response) ;; websocket responses may be another channel
                  (cid/ensure-correlation-id nested-response get-request-cid)
                  nested-response)))))))))

(defn request->protocol
  "Determines the protocol and version used by the request.
   For HTTP requests, it returns values like HTTP/1.0, HTTP/1.1, HTTP/2.
   For WebSocket requests, it returns values like WS/8, WS/13."
  [{:keys [headers scheme ^ServletRequest servlet-request]}]
  (if servlet-request
    (or (some-> headers
                (get "x-forwarded-proto-version")
                str/upper-case)
        (.getProtocol servlet-request))
    (when scheme
      (str/upper-case
        ;; currently, only websockets need this branch to determine version
        (if-let [version (get headers "sec-websocket-version")]
          (str (name scheme) "/" version)
          (name scheme))))))

(defn wrap-request-info
  "Attaches request info to the request."
  [handler router-id support-info]
  (fn wrap-request-info-fn [{:keys [servlet-request] :as request}]
    (-> request
        (assoc :client-protocol (request->protocol request)
               :internal-protocol (some-> servlet-request .getProtocol)
               :request-id (str (utils/unique-identifier) "-" (-> request utils/request->scheme name))
               :request-time (t/now)
               :router-id router-id
               :support-info support-info)
        handler)))

(defn wrap-debug
  "Attaches debugging headers to requests when enabled.
   Logs any request trailers when they are provided."
  [handler generate-log-url-fn]
  (fn wrap-debug-fn
    [{:keys [client-protocol internal-protocol request-id request-time router-id trailers-fn] :as request}]
    (if (utils/request->debug-enabled? request)
      (let [request (cond-> request
                      trailers-fn
                      (assoc :trailers-fn (let [correlation-id (cid/get-correlation-id)]
                                            (fn retrieve-request-trailers []
                                              (when-let [trailers-data (trailers-fn)]
                                                (cid/cinfo correlation-id "request trailers:" trailers-data)
                                                trailers-data)))))
            response (handler request)
            add-headers (fn [{:keys [descriptor instance] :as response}]
                          (let [{:strs [backend-proto]} (:service-description descriptor)
                                backend-directory (:log-directory instance)
                                backend-log-url (when backend-directory
                                                  (generate-log-url-fn instance))
                                request-date (when request-time
                                               (du/date-to-str request-time du/formatter-rfc822))]
                            (update response :headers
                                    (fn [headers]
                                      (cond-> headers
                                        client-protocol (assoc "x-waiter-client-protocol" (name client-protocol))
                                        internal-protocol (assoc "x-waiter-internal-protocol" (name internal-protocol))
                                        request-time (assoc "x-waiter-request-date" request-date)
                                        request-id (assoc "x-waiter-request-id" request-id)
                                        router-id (assoc "x-waiter-router-id" router-id)
                                        descriptor (assoc "x-waiter-service-id" (:service-id descriptor))
                                        instance (assoc "x-waiter-backend-id" (:id instance)
                                                        "x-waiter-backend-host" (:host instance)
                                                        "x-waiter-backend-port" (str (:port instance))
                                                        "x-waiter-backend-proto" backend-proto)
                                        backend-directory (assoc "x-waiter-backend-directory" backend-directory
                                                                 "x-waiter-backend-log-url" backend-log-url))))))]
        (ru/update-response response add-headers))
      (handler request))))

(defn wrap-error-handling
  "Catches any uncaught exceptions and returns an error response."
  [handler]
  (fn wrap-error-handling-fn [request]
    (try
      (let [response (handler request)]
        (if (au/chan? response)
          (async/go
            (try
              (<? response)
              (catch Exception e
                (utils/exception->response e request))))
          response))
      (catch Exception e
        (utils/exception->response e request)))))



(defn- make-blacklist-request
  [make-inter-router-requests-fn blacklist-period-ms dest-router-id dest-endpoint {:keys [id] :as instance} reason]
  (log/info "peer communication requesting" dest-router-id "to blacklist" id "via endpoint" dest-endpoint)
  (try
    (-> (make-inter-router-requests-fn
          dest-endpoint
          :acceptable-router? #(= dest-router-id %)
          :body (utils/clj->json {:instance instance :period-in-ms blacklist-period-ms :reason reason})
          :method :post)
        (get dest-router-id))
    (catch Exception e
      (log/error e "error in making blacklist request"
                 {:instance instance :period-in-ms blacklist-period-ms :reason reason}))))

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
  `service->raw-data-source`: A channel or a no-args function which produces service data.
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
  [read-state-fn write-state-fn leader? clock name service->raw-data-source timeout-interval-ms sanitize-state-fn
   service->state-fn gc-service? perform-gc-fn]
  {:pre (pos? timeout-interval-ms)}
  (let [query-chan (async/chan 10)
        state-cache (cu/cache-factory {:ttl (/ timeout-interval-ms 2)})
        query-state-fn (fn query-state-fn []
                         (cu/cache-get-or-load state-cache name #(read-state-fn name)))
        query-service-state-fn (fn query-service-state-fn [{:keys [service-id]}]
                                 (-> (query-state-fn) (get service-id) (or {})))
        exit-chan (async/chan 1)]
    (cid/with-correlation-id
      name
      (async/go-loop [iter 0
                      timeout-chan (async/timeout timeout-interval-ms)]
        (let [[chan args] (async/alt!
                            exit-chan ([_] [:exit])
                            query-chan ([args] [:query args])
                            timeout-chan ([_] [:continue])
                            :priority true)]
          (case chan
            :exit (log/info "[service-gc-go-routine] exiting" name)
            :query (let [{:keys [response-chan]} args
                         state (query-service-state-fn args)]
                     (async/>! response-chan (or state {}))
                     (recur (inc iter) timeout-chan))
            :continue
            (do
              (when (leader?)
                (let [service->raw-data (if (fn? service->raw-data-source)
                                          (service->raw-data-source)
                                          (async/<! service->raw-data-source))]
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
                                                                  (du/date-to-str (:last-modified-time state)) "marked for deletion")
                                                        gc-service))
                                                    service->state'))
                            apps-successfully-gced (filter (fn [service]
                                                             (try
                                                               (when (leader?) ; check to ensure still the leader
                                                                 (perform-gc-fn service))
                                                               (catch Exception e
                                                                 (log/error e "error in deleting:" service))))
                                                           apps-to-gc)
                            apps-failed-to-delete (apply disj (set apps-to-gc) apps-successfully-gced)
                            service->state'' (apply dissoc service->state' apps-successfully-gced)]
                        (when (or (not= (set (keys service->state'')) (set (keys service->raw-data)))
                                  (not= (set (keys service->state'')) (set (keys service->state))))
                          (log/info "state has" (count service->state'') "active services, received"
                                    (count service->raw-data) "services in latest update."))
                        (when (not-empty apps-failed-to-delete)
                          (log/warn "unable to delete services:" apps-failed-to-delete))
                        (write-state-fn name service->state'')
                        (cu/cache-evict state-cache name))
                      (catch Exception e
                        (log/error e "error in" name {:iteration iter}))))))
              (recur (inc iter) (async/timeout timeout-interval-ms)))))))
    {:exit exit-chan
     :query query-chan
     :query-service-state-fn query-service-state-fn
     :query-state-fn query-state-fn}))

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
        request-config (update config :headers
                               (fn prepare-inter-router-requests-headers [headers]
                                 (-> headers
                                     (assoc "accept" "application/json")
                                     (update "x-cid"
                                             (fn attach-inter-router-cid [provided-cid]
                                               (or provided-cid
                                                   (let [current-cid (cid/get-correlation-id)
                                                         cid-prefix (if (or (nil? current-cid) (= cid/default-correlation-id current-cid))
                                                                      "waiter"
                                                                      current-cid)]
                                                     (str cid-prefix "." (utils/unique-identifier)))))))))]
    (when (and (empty? router-id->endpoint-url')
               (not-empty router-id->endpoint-url))
      (log/info "no acceptable routers found to make request!"))
    (loop [[[dest-router-id endpoint-url] & remaining-items] router-id->endpoint-url'
           router-id->response {}]
      (if dest-router-id
        (let [secret-word (utils/generate-secret-word my-router-id dest-router-id passwords)
              auth (make-basic-auth-fn endpoint-url my-router-id secret-word)
              response (make-request-fn method endpoint-url auth body request-config)]
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
  (let [valid-waiter-hostnames (set/union valid-waiter-hostnames #{"localhost" "127.0.0.1"})]
    (fn waiter-request? [{:keys [uri headers]}]
      (let [{:strs [host]} headers]
        ; special urls that are always for Waiter (FIXME)
        (or (#{"/app-name" "/service-id" "/token" "/waiter-ping"} uri)
            (some #(str/starts-with? (str uri) %)
                  ["/waiter-async/complete/" "/waiter-async/result/" "/waiter-async/status/" "/waiter-auth/"
                   "/waiter-consent" "/waiter-interstitial"])
            (and (or (str/blank? host)
                     (valid-waiter-hostnames (-> host
                                                 (str/split #":")
                                                 first)))
                 (not-any? #(str/starts-with? (key %) headers/waiter-header-prefix)
                           (remove #(= "x-waiter-debug" (key %)) headers))))))))

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

;; PRIVATE API
(def state
  {:async-request-store-atom (pc/fnk [] (atom {}))
   :authenticator (pc/fnk [[:settings authenticator-config hostname service-description-defaults]
                           passwords]
                    (let [hostname (if (sequential? hostname) (first hostname) hostname)]
                      (utils/create-component authenticator-config
                                              :context {:default-authentication (get service-description-defaults "authentication")
                                                        :hostname hostname
                                                        :password (first passwords)})))
   :clock (pc/fnk [] t/now)
   :cors-validator (pc/fnk [[:settings cors-config]]
                     (utils/create-component cors-config))
   :entitlement-manager (pc/fnk [[:settings entitlement-config]]
                          (utils/create-component entitlement-config))
   :fallback-state-atom (pc/fnk [] (atom {:available-service-ids #{}
                                          :healthy-service-ids #{}}))
   :http-clients (pc/fnk [[:settings [:instance-request-properties connection-timeout-ms]]]
                   (hu/prepare-http-clients
                     {:client-name "waiter-client"
                      :conn-timeout connection-timeout-ms
                      :follow-redirects? false}))
   :instance-rpc-chan (pc/fnk [] (async/chan 1024)) ; TODO move to service-chan-maintainer
   :interstitial-state-atom (pc/fnk [] (atom {:initialized? false
                                              :service-id->interstitial-promise {}}))
   :local-usage-agent (pc/fnk [] (agent {}))
   :passwords (pc/fnk [[:settings password-store-config]]
                (let [password-provider (utils/create-component password-store-config)
                      passwords (password-store/retrieve-passwords password-provider)
                      _ (password-store/check-empty-passwords passwords)
                      processed-passwords (mapv #(vector :cached %) passwords)]
                  processed-passwords))
   :query-service-maintainer-chan (pc/fnk [] (au/latest-chan)) ; TODO move to service-chan-maintainer
   :router-metrics-agent (pc/fnk [router-id] (metrics-sync/new-router-metrics-agent router-id {}))
   :router-id (pc/fnk [[:settings router-id-prefix]]
                (cond->> (utils/unique-identifier)
                  (not (str/blank? router-id-prefix))
                  (str (str/replace router-id-prefix #"[@.]" "-") "-")))
   :scaling-timeout-config (pc/fnk [[:settings
                                     [:blacklist-config blacklist-backoff-base-time-ms max-blacklist-time-ms]
                                     [:scaling inter-kill-request-wait-time-ms]]]
                             {:blacklist-backoff-base-time-ms blacklist-backoff-base-time-ms
                              :inter-kill-request-wait-time-ms inter-kill-request-wait-time-ms
                              :max-blacklist-time-ms max-blacklist-time-ms})
   :scheduler-interactions-thread-pool (pc/fnk [] (Executors/newFixedThreadPool 20))
   :scheduler-state-chan (pc/fnk [] (au/latest-chan))
   :server-name (pc/fnk [[:settings git-version]] (str "waiter/" (str/join (take 7 git-version))))
   :service-description-builder (pc/fnk [[:settings service-description-builder-config service-description-constraints]]
                                  (when-let [unknown-keys (-> service-description-constraints
                                                            keys
                                                            set
                                                            (set/difference sd/service-parameter-keys)
                                                            seq)]
                                    (throw (ex-info "Unsupported keys present in the service description constraints"
                                                    {:service-description-constraints service-description-constraints
                                                     :unsupported-keys (-> unknown-keys vec sort)})))
                                  (utils/create-component
                                    service-description-builder-config :context {:constraints service-description-constraints}))
   :service-id-prefix (pc/fnk [[:settings [:cluster-config service-prefix]]] service-prefix)
   :start-service-cache (pc/fnk []
                          (cu/cache-factory {:threshold 100
                                             :ttl (-> 1 t/minutes t/in-millis)}))
   :token-cluster-calculator (pc/fnk [[:settings [:cluster-config name] [:token-config cluster-calculator]]]
                               (utils/create-component
                                 cluster-calculator :context {:default-cluster name}))
   :token-root (pc/fnk [[:settings [:cluster-config name]]] name)
   :waiter-hostnames (pc/fnk [[:settings hostname]]
                       (set (if (sequential? hostname)
                              hostname
                              [hostname])))
   :websocket-client (pc/fnk [[:settings [:websocket-config ws-max-binary-message-size ws-max-text-message-size]]
                              http-clients]
                       (let [http-client (hu/select-http-client "http" http-clients)
                             websocket-client (WebSocketClient. ^HttpClient http-client)]
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
                (.addListener
                  (.getCuratorListenable curator)
                  (reify CuratorListener
                    (eventReceived [_ _ event]
                      (when (= CuratorEventType/SYNC (.getType event))
                        (log/info "received SYNC event for" (.getPath event))
                        (when-let [response-promise (.getContext event)]
                          (log/info "releasing response promise provided for" (.getPath event))
                          (deliver response-promise :release))))))
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

(def scheduler
  {:scheduler (pc/fnk [[:curator leader?-fn]
                       [:settings scheduler-config scheduler-syncer-interval-secs]
                       [:state scheduler-state-chan service-id-prefix]
                       service-id->password-fn*
                       service-id->service-description-fn*
                       start-scheduler-syncer-fn]
                (let [is-waiter-service?-fn (fn is-waiter-service? [^String service-id]
                                              (str/starts-with? service-id service-id-prefix))
                      scheduler-context {:is-waiter-service?-fn is-waiter-service?-fn
                                         :leader?-fn leader?-fn
                                         :scheduler-name (-> scheduler-config :kind utils/keyword->str)
                                         :scheduler-state-chan scheduler-state-chan
                                         ;; TODO scheduler-syncer-interval-secs should be inside the scheduler's config
                                         :scheduler-syncer-interval-secs scheduler-syncer-interval-secs
                                         :service-id->password-fn service-id->password-fn*
                                         :service-id->service-description-fn service-id->service-description-fn*
                                         :start-scheduler-syncer-fn start-scheduler-syncer-fn}]
                  (utils/create-component scheduler-config :context scheduler-context)))
   ; This function is only included here for initializing the scheduler above.
   ; Prefer accessing the non-starred version of this function through the routines map.
   :service-id->password-fn* (pc/fnk [[:state passwords]]
                               (fn service-id->password [service-id]
                                 (log/debug "generating password for" service-id)
                                 (digest/md5 (str service-id (first passwords)))))
   ; This function is only included here for initializing the scheduler above.
   ; Prefer accessing the non-starred version of this function through the routines map.
   :service-id->service-description-fn* (pc/fnk [[:curator kv-store]
                                                 [:settings service-description-defaults metric-group-mappings]]
                                          (fn service-id->service-description
                                            [service-id & {:keys [effective?] :or {effective? true}}]
                                            (sd/service-id->service-description
                                              kv-store service-id service-description-defaults
                                              metric-group-mappings :effective? effective?)))
   :start-scheduler-syncer-fn (pc/fnk [[:settings [:health-check-config health-check-timeout-ms failed-check-threshold] git-version]
                                       [:state clock]
                                       service-id->service-description-fn*]
                                (let [http-client (hu/http-client-factory
                                                    {:client-name (str "waiter-syncer-" (str/join (take 7 git-version)))
                                                     :conn-timeout health-check-timeout-ms
                                                     :socket-timeout health-check-timeout-ms
                                                     :user-agent (str "waiter-syncer/" (str/join (take 7 git-version)))})
                                      available? (fn scheduler-available?
                                                   [scheduler-name service-instance health-check-proto health-check-port-index health-check-path]
                                                   (scheduler/available? http-client scheduler-name service-instance health-check-proto
                                                                         health-check-port-index health-check-path))]
                                  (fn start-scheduler-syncer-fn
                                    [scheduler-name get-service->instances-fn scheduler-state-chan scheduler-syncer-interval-secs]
                                    (let [timer-ch (-> scheduler-syncer-interval-secs t/seconds t/in-millis au/timer-chan)]
                                      (scheduler/start-scheduler-syncer
                                        clock timer-ch service-id->service-description-fn* available?
                                        failed-check-threshold scheduler-name get-service->instances-fn scheduler-state-chan)))))})

(def routines
  {:allowed-to-manage-service?-fn (pc/fnk [[:curator kv-store]
                                           [:state entitlement-manager]]
                                    (fn allowed-to-manage-service? [service-id auth-user]
                                      ; Returns whether the authenticated user is allowed to manage the service.
                                      ; Either she can run as the waiter user or the run-as-user of the service description."
                                      (sd/can-manage-service? kv-store entitlement-manager service-id auth-user)))
   :assoc-run-as-user-approved? (pc/fnk [[:settings consent-expiry-days]
                                         [:state clock passwords]
                                         token->token-metadata]
                                  (fn assoc-run-as-user-approved? [{:keys [headers]} service-id]
                                    (let [{:strs [cookie host]} headers
                                          token (when-not (headers/contains-waiter-header headers sd/on-the-fly-service-description-keys)
                                                  (utils/authority->host host))
                                          token-metadata (when token (token->token-metadata token))
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
   :authentication-method-wrapper-fn (pc/fnk [[:state authenticator passwords]]
                                       (fn authentication-method-wrapper [request-handler]
                                         (let [auth-handler (auth/wrap-auth-handler authenticator request-handler)
                                               password (first passwords)]
                                           (auth/wrap-auth-cookie-handler
                                             password
                                             (fn authenticate-request [request]
                                               (cond
                                                 (:skip-authentication request) (do
                                                                                  (log/info "skipping authentication for request")
                                                                                  (request-handler request))
                                                 (auth/request-authenticated? request) (request-handler request)
                                                 :else (auth-handler request)))))))
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
   :generate-log-url-fn (pc/fnk [prepend-waiter-url]
                          (partial handler/generate-log-url prepend-waiter-url))
   :list-tokens-fn (pc/fnk [[:curator curator]
                            [:settings [:zookeeper base-path] kv-config]]
                     (fn list-tokens-fn []
                       (let [{:keys [relative-path]} kv-config]
                         (->> (kv/zk-keys curator (str base-path "/" relative-path))
                              (filter (fn [k] (not (str/starts-with? k "^"))))))))
   :make-basic-auth-fn (pc/fnk []
                         (fn make-basic-auth-fn [uri username password]
                           (BasicAuthentication$BasicResult. (URI. uri) username password)))
   :make-http-request-fn (pc/fnk [[:settings instance-request-properties]
                                  [:state http-clients]
                                  make-basic-auth-fn service-id->password-fn]
                           (let [make-request-fn pr/make-request]
                             (handler/async-make-request-helper
                               http-clients instance-request-properties make-basic-auth-fn service-id->password-fn
                               pr/prepare-request-properties make-request-fn)))
   :make-inter-router-requests-async-fn (pc/fnk [[:curator discovery]
                                                 [:settings [:instance-request-properties initial-socket-timeout-ms]]
                                                 [:state http-clients passwords router-id]
                                                 make-basic-auth-fn]
                                          (let [http-client (hu/select-http-client "http" http-clients)
                                                make-request-async-fn (fn make-request-async-fn [method endpoint-url auth body config]
                                                                        (make-request-async http-client initial-socket-timeout-ms method endpoint-url auth body config))]
                                            (fn make-inter-router-requests-async-fn [endpoint & args]
                                              (apply make-inter-router-requests make-request-async-fn make-basic-auth-fn router-id discovery passwords endpoint args))))
   :make-inter-router-requests-sync-fn (pc/fnk [[:curator discovery]
                                                [:settings [:instance-request-properties initial-socket-timeout-ms]]
                                                [:state http-clients passwords router-id]
                                                make-basic-auth-fn]
                                         (let [http-client (hu/select-http-client "http" http-clients)
                                               make-request-sync-fn (fn make-request-sync-fn [method endpoint-url auth body config]
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
                                               [response service-id metric-group backend-proto instance _
                                                reason-map request-properties location query-string]
                                               (async-req/post-process-async-request-response
                                                 router-id async-request-store-atom make-http-request-fn instance-rpc-chan response
                                                 service-id metric-group backend-proto instance reason-map request-properties
                                                 location query-string)))
   :prepend-waiter-url (pc/fnk [[:settings port hostname]]
                         (let [hostname (if (sequential? hostname) (first hostname) hostname)]
                           (fn [endpoint-url]
                             (if (str/blank? endpoint-url)
                               endpoint-url
                               (str "http://" hostname ":" port endpoint-url)))))
   :refresh-service-descriptions-fn (pc/fnk [[:curator kv-store]]
                                      (fn refresh-service-descriptions-fn [service-ids]
                                        (sd/refresh-service-descriptions kv-store service-ids)))
   :request->descriptor-fn (pc/fnk [[:curator kv-store]
                                    [:settings [:token-config history-length token-defaults] metric-group-mappings service-description-defaults]
                                    [:state fallback-state-atom service-description-builder service-id-prefix waiter-hostnames]
                                    assoc-run-as-user-approved? can-run-as?-fn store-source-tokens-fn]
                             (fn request->descriptor-fn [request]
                               (let [{:keys [latest-descriptor] :as result}
                                     (descriptor/request->descriptor
                                       assoc-run-as-user-approved? can-run-as?-fn fallback-state-atom kv-store metric-group-mappings
                                       history-length service-description-builder service-description-defaults service-id-prefix
                                       token-defaults waiter-hostnames request)]
                                 (when-let [source-tokens (-> latest-descriptor :source-tokens seq)]
                                   (store-source-tokens-fn (:service-id latest-descriptor) source-tokens))
                                 result)))
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
   :service-id->idle-timeout (pc/fnk [[:settings [:token-config token-defaults]]
                                      service-id->service-description-fn service-id->source-tokens-entries-fn
                                      token->token-hash token->token-metadata]
                               (fn service-id->idle-timeout [service-id]
                                 (sd/service-id->idle-timeout
                                   service-id->service-description-fn service-id->source-tokens-entries-fn
                                   token->token-hash token->token-metadata token-defaults service-id)))
   :service-id->password-fn (pc/fnk [[:scheduler service-id->password-fn*]]
                              service-id->password-fn*)
   :service-id->service-description-fn (pc/fnk [[:scheduler service-id->service-description-fn*]]
                                         service-id->service-description-fn*)
   :service-id->source-tokens-entries-fn (pc/fnk [[:curator kv-store]]
                                           (partial sd/service-id->source-tokens-entries kv-store))
   :start-new-service-fn (pc/fnk [[:scheduler scheduler]
                                  [:state start-service-cache scheduler-interactions-thread-pool]
                                  service-id->service-description-fn store-service-description-fn]
                           (fn start-new-service [{:keys [service-id] :as descriptor}]
                             (if-not (seq (service-id->service-description-fn service-id :effective? false))
                               (store-service-description-fn descriptor)
                               (log/info "not storing service description for" service-id "as it already exists"))
                             (scheduler/validate-service scheduler service-id)
                             (service/start-new-service
                               scheduler descriptor start-service-cache scheduler-interactions-thread-pool)))
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
                                   (fn store-service-description [{:keys [core-service-description service-id]}]
                                     (sd/store-core kv-store service-id core-service-description validate-service-description-fn)))
   :store-source-tokens-fn (pc/fnk [[:curator kv-store]
                                    synchronize-fn]
                             (fn store-source-tokens-fn [service-id source-tokens]
                               (sd/store-source-tokens! synchronize-fn kv-store service-id source-tokens)))
   :synchronize-fn (pc/fnk [[:curator curator]
                            [:settings [:zookeeper base-path mutex-timeout-ms]]]
                     (fn synchronize-fn [path f]
                       (let [lock-path (str base-path "/" path)]
                         (curator/synchronize curator lock-path mutex-timeout-ms f))))
   :token->service-description-template (pc/fnk [[:curator kv-store]]
                                          (fn token->service-description-template [token]
                                            (sd/token->service-description-template kv-store token :error-on-missing false)))
   :token->token-hash (pc/fnk [[:curator kv-store]]
                        (fn token->token-hash [token]
                          (sd/token->token-hash kv-store token)))
   :token->token-metadata (pc/fnk [[:curator kv-store]]
                            (fn token->token-metadata [token]
                              (sd/token->token-metadata kv-store token :error-on-missing false)))
   :validate-service-description-fn (pc/fnk [[:settings service-description-defaults]
                                             [:state authenticator service-description-builder]]
                                      (let [authentication-providers (into #{"disabled" "standard"} (auth/get-authentication-providers authenticator))
                                            default-authentication (get service-description-defaults "authentication")]
                                        (fn validate-service-description [service-description]
                                          (let [authentication (or (get service-description "authentication") default-authentication)]
                                            (when-not (contains? authentication-providers authentication)
                                              (throw (ex-info (str "authentication must be one of: '"
                                                                   (str/join "', '" (sort authentication-providers)) "'")
                                                              {:authentication authentication :status 400}))))
                                          (sd/validate service-description-builder service-description {}))))
   :waiter-request?-fn (pc/fnk [[:state waiter-hostnames]]
                         (let [local-router (InetAddress/getLocalHost)
                               waiter-router-hostname (.getCanonicalHostName local-router)
                               waiter-router-ip (.getHostAddress local-router)
                               hostnames (conj waiter-hostnames waiter-router-hostname waiter-router-ip)]
                           (waiter-request?-factory hostnames)))
   :websocket-request-auth-cookie-attacher (pc/fnk [[:state passwords router-id]]
                                             (fn websocket-request-auth-cookie-attacher [request]
                                               (ws/inter-router-request-middleware router-id (first passwords) request)))
   :websocket-request-acceptor (pc/fnk [[:state passwords]]
                                 (fn websocket-request-acceptor [^ServletUpgradeRequest request ^ServletUpgradeResponse response]
                                   (.setHeader response "x-cid" (cid/get-correlation-id))
                                   (if (ws/request-authenticator (first passwords) request response)
                                     (ws/request-subprotocol-acceptor request response)
                                     false)))})

(def daemons
  {:autoscaler (pc/fnk [[:curator leader?-fn]
                        [:routines router-metrics-helpers service-id->service-description-fn]
                        [:scheduler scheduler]
                        [:settings [:scaling autoscaler-interval-ms max-expired-unhealthy-instances-to-consider]]
                        [:state scheduler-interactions-thread-pool]
                        autoscaling-multiplexer router-state-maintainer]
                 (let [service-id->metrics-fn (:service-id->metrics-fn router-metrics-helpers)
                       {{:keys [router-state-push-mult]} :maintainer} router-state-maintainer
                       {:keys [executor-multiplexer-chan]} autoscaling-multiplexer]
                   (scaling/autoscaler-goroutine
                     {} leader?-fn service-id->metrics-fn executor-multiplexer-chan scheduler autoscaler-interval-ms
                     scaling/scale-service service-id->service-description-fn router-state-push-mult
                     scheduler-interactions-thread-pool max-expired-unhealthy-instances-to-consider)))
   :autoscaling-multiplexer (pc/fnk [[:routines delegate-instance-kill-request-fn peers-acknowledged-blacklist-requests-fn
                                      service-id->service-description-fn]
                                     [:scheduler scheduler]
                                     [:settings [:scaling quanta-constraints]]
                                     [:state instance-rpc-chan scheduler-interactions-thread-pool scaling-timeout-config]
                                     router-state-maintainer]
                              (let [{{:keys [notify-instance-killed-fn]} :maintainer} router-state-maintainer]
                                (scaling/service-scaling-multiplexer
                                  (fn scaling-executor-factory [service-id]
                                    (scaling/service-scaling-executor
                                      notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn
                                      delegate-instance-kill-request-fn service-id->service-description-fn
                                      scheduler instance-rpc-chan quanta-constraints scaling-timeout-config
                                      scheduler-interactions-thread-pool service-id))
                                  {})))
   :codahale-reporters (pc/fnk [[:settings [:metrics-config codahale-reporters]]]
                         (pc/map-vals
                           (fn make-codahale-reporter [{:keys [factory-fn] :as reporter-config}]
                             (let [resolved-factory-fn (utils/resolve-symbol! factory-fn)
                                   reporter-instance (resolved-factory-fn reporter-config)]
                               (when-not (satisfies? reporter/CodahaleReporter reporter-instance)
                                 (throw (ex-info "Reporter factory did not create an instance of CodahaleReporter"
                                                 {:reporter-config reporter-config
                                                  :reporter-instance reporter-instance
                                                  :resolved-factory-fn resolved-factory-fn})))
                               reporter-instance))
                           codahale-reporters))
   :fallback-maintainer (pc/fnk [[:state fallback-state-atom]
                                 router-state-maintainer]
                          (let [{{:keys [router-state-push-mult]} :maintainer} router-state-maintainer
                                router-state-chan (async/tap router-state-push-mult (au/latest-chan))]
                            (descriptor/fallback-maintainer router-state-chan fallback-state-atom)))
   :gc-for-transient-metrics (pc/fnk [[:routines router-metrics-helpers]
                                      [:settings metrics-config]
                                      [:state clock local-usage-agent]
                                      router-state-maintainer]
                               (let [state-store-atom (atom {})
                                     read-state-fn (fn read-state [_] @state-store-atom)
                                     write-state-fn (fn write-state [_ state] (reset! state-store-atom state))
                                     leader?-fn (constantly true)
                                     service-gc-go-routine (partial service-gc-go-routine read-state-fn write-state-fn leader?-fn clock)
                                     {{:keys [query-state-fn]} :maintainer} router-state-maintainer
                                     {:keys [service-id->metrics-fn]} router-metrics-helpers
                                     {:keys [service-id->metrics-chan] :as metrics-gc-chans}
                                     (metrics/transient-metrics-gc query-state-fn local-usage-agent service-gc-go-routine metrics-config)]
                                 (metrics/transient-metrics-data-producer service-id->metrics-chan service-id->metrics-fn metrics-config)
                                 metrics-gc-chans))
   :interstitial-maintainer (pc/fnk [[:routines service-id->service-description-fn]
                                     [:state interstitial-state-atom]
                                     router-state-maintainer]
                              (let [{{:keys [router-state-push-mult]} :maintainer} router-state-maintainer
                                    router-state-chan (async/tap router-state-push-mult (au/latest-chan))
                                    initial-state {}]
                                (interstitial/interstitial-maintainer
                                  service-id->service-description-fn router-state-chan interstitial-state-atom initial-state)))
   :launch-metrics-maintainer (pc/fnk [[:curator leader?-fn]
                                       [:routines service-id->service-description-fn]
                                       router-state-maintainer]
                                (let [{{:keys [router-state-push-mult]} :maintainer} router-state-maintainer]
                                  (scheduler/start-launch-metrics-maintainer
                                    (async/tap router-state-push-mult (au/latest-chan))
                                    leader?-fn
                                    service-id->service-description-fn)))
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
                                   [:state local-usage-agent router-metrics-agent websocket-client]
                                   router-list-maintainer]
                            (let [{:keys [bytes-encryptor]} crypt-helpers
                                  router-chan (async/tap (:router-mult-chan router-list-maintainer) (au/latest-chan))]
                              {:metrics-syncer (metrics-sync/setup-metrics-syncer
                                                 router-metrics-agent local-usage-agent metrics-sync-interval-ms bytes-encryptor)
                               :router-syncer (metrics-sync/setup-router-syncer router-chan router-metrics-agent router-update-interval-ms
                                                                                inter-router-metrics-idle-timeout-ms metrics-sync-interval-ms
                                                                                websocket-client bytes-encryptor websocket-request-auth-cookie-attacher)}))
   :router-state-maintainer (pc/fnk [[:routines refresh-service-descriptions-fn service-id->service-description-fn]
                                     [:scheduler scheduler]
                                     [:settings deployment-error-config]
                                     [:state router-id scheduler-state-chan]
                                     router-list-maintainer]
                              (let [exit-chan (async/chan)
                                    router-chan (async/tap (:router-mult-chan router-list-maintainer) (au/latest-chan))
                                    service-id->deployment-error-config-fn #(scheduler/deployment-error-config scheduler %)
                                    maintainer (state/start-router-state-maintainer
                                                 scheduler-state-chan router-chan router-id exit-chan service-id->service-description-fn
                                                 refresh-service-descriptions-fn service-id->deployment-error-config-fn
                                                 deployment-error-config)]
                                {:exit-chan exit-chan
                                 :maintainer maintainer}))
   :scheduler-broken-services-gc (pc/fnk [[:curator gc-state-reader-fn gc-state-writer-fn leader?-fn]
                                          [:scheduler scheduler]
                                          [:settings scheduler-gc-config]
                                          [:state clock]
                                          router-state-maintainer]
                                   (let [{{:keys [query-state-fn]} :maintainer} router-state-maintainer
                                         service-gc-go-routine (partial service-gc-go-routine gc-state-reader-fn gc-state-writer-fn leader?-fn clock)]
                                     (scheduler/scheduler-broken-services-gc service-gc-go-routine query-state-fn scheduler scheduler-gc-config)))
   :scheduler-services-gc (pc/fnk [[:curator gc-state-reader-fn gc-state-writer-fn leader?-fn]
                                   [:routines router-metrics-helpers service-id->idle-timeout]
                                   [:scheduler scheduler]
                                   [:settings scheduler-gc-config]
                                   [:state clock]
                                   router-state-maintainer]
                            (let [{{:keys [query-state-fn]} :maintainer} router-state-maintainer
                                  {:keys [service-id->metrics-fn]} router-metrics-helpers
                                  service-gc-go-routine (partial service-gc-go-routine gc-state-reader-fn gc-state-writer-fn leader?-fn clock)]
                              (scheduler/scheduler-services-gc
                                scheduler query-state-fn service-id->metrics-fn scheduler-gc-config service-gc-go-routine
                                service-id->idle-timeout)))
   :service-chan-maintainer (pc/fnk [[:routines start-work-stealing-balancer-fn stop-work-stealing-balancer-fn]
                                     [:settings blacklist-config instance-request-properties]
                                     [:state instance-rpc-chan query-service-maintainer-chan]
                                     router-state-maintainer]
                              (let [start-service
                                    (fn start-service [service-id]
                                      (let [maintainer-chan-map (state/prepare-and-start-service-chan-responder
                                                                  service-id instance-request-properties blacklist-config)
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
                                    {{:keys [router-state-push-mult]} :maintainer} router-state-maintainer
                                    state-chan (au/latest-chan)]
                                (async/tap router-state-push-mult state-chan)
                                (state/start-service-chan-maintainer
                                  {} instance-rpc-chan state-chan query-service-maintainer-chan start-service remove-service retrieve-channel)))
   :state-sources (pc/fnk [[:scheduler scheduler]
                           [:state query-service-maintainer-chan]
                           autoscaler autoscaling-multiplexer gc-for-transient-metrics interstitial-maintainer
                           scheduler-broken-services-gc scheduler-services-gc]
                    {:autoscaler-state (:query-service-state-fn autoscaler)
                     :autoscaling-multiplexer-state (:query-chan autoscaling-multiplexer)
                     :interstitial-maintainer-state (:query-chan interstitial-maintainer)
                     :scheduler-broken-services-gc-state (:query-service-state-fn scheduler-broken-services-gc)
                     :scheduler-services-gc-state (:query-service-state-fn scheduler-services-gc)
                     :scheduler-state (fn scheduler-state-fn [service-id]
                                        (scheduler/service-id->state scheduler service-id))
                     :service-maintainer-state query-service-maintainer-chan
                     :transient-metrics-gc-state (:query-service-state-fn gc-for-transient-metrics)})
   :statsd (pc/fnk [[:routines service-id->service-description-fn]
                    [:settings statsd]
                    router-state-maintainer]
             (when (not= statsd :disabled)
               (statsd/setup statsd)
               (let [{:keys [sync-instances-interval-ms]} statsd
                     {{:keys [query-state-fn]} :maintainer} router-state-maintainer]
                 (statsd/start-service-instance-metrics-publisher
                   service-id->service-description-fn query-state-fn sync-instances-interval-ms))))})

(def request-handlers
  {:app-name-handler-fn (pc/fnk [service-id-handler-fn]
                          service-id-handler-fn)
   :async-complete-handler-fn (pc/fnk [[:routines async-request-terminate-fn]
                                       wrap-router-auth-fn]
                                (wrap-router-auth-fn
                                  (fn async-complete-handler-fn [request]
                                    (handler/complete-async-handler async-request-terminate-fn request))))
   :async-result-handler-fn (pc/fnk [[:routines async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn]
                                     wrap-secure-request-fn]
                              (wrap-secure-request-fn
                                (fn async-result-handler-fn [request]
                                  (handler/async-result-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))))
   :async-status-handler-fn (pc/fnk [[:routines async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn]
                                     wrap-secure-request-fn]
                              (wrap-secure-request-fn
                                (fn async-status-handler-fn [request]
                                  (handler/async-status-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))))
   :blacklist-instance-handler-fn (pc/fnk [[:daemons router-state-maintainer]
                                           [:state instance-rpc-chan]
                                           wrap-router-auth-fn]
                                    (let [{{:keys [notify-instance-killed-fn]} :maintainer} router-state-maintainer]
                                      (wrap-router-auth-fn
                                        (fn blacklist-instance-handler-fn [request]
                                          (handler/blacklist-instance notify-instance-killed-fn instance-rpc-chan request)))))
   :blacklisted-instances-list-handler-fn (pc/fnk [[:state instance-rpc-chan]]
                                            (fn blacklisted-instances-list-handler-fn [{{:keys [service-id]} :route-params :as request}]
                                              (handler/get-blacklisted-instances instance-rpc-chan service-id request)))
   :default-websocket-handler-fn (pc/fnk [[:routines determine-priority-fn service-id->password-fn start-new-service-fn]
                                          [:settings instance-request-properties]
                                          [:state instance-rpc-chan local-usage-agent passwords websocket-client]
                                          wrap-descriptor-fn]
                                   (fn default-websocket-handler-fn [request]
                                     (let [password (first passwords)
                                           make-request-fn (fn make-ws-request
                                                             [instance request request-properties passthrough-headers end-route metric-group
                                                              backend-proto proto-version]
                                                             (ws/make-request websocket-client service-id->password-fn instance request request-properties
                                                                              passthrough-headers end-route metric-group backend-proto proto-version))
                                           process-request-fn (fn process-request-fn [request]
                                                                (pr/process make-request-fn instance-rpc-chan start-new-service-fn
                                                                            instance-request-properties determine-priority-fn ws/process-response!
                                                                            ws/abort-request-callback-factory local-usage-agent request))
                                           handler (-> process-request-fn
                                                       (ws/wrap-ws-close-on-error)
                                                       wrap-descriptor-fn)]
                                       (ws/request-handler password handler request))))
   :display-settings-handler-fn (pc/fnk [wrap-secure-request-fn settings]
                                  (wrap-secure-request-fn
                                    (fn display-settings-handler-fn [_]
                                      (settings/display-settings settings))))
   :favicon-handler-fn (pc/fnk []
                         (fn favicon-handler-fn [_]
                           {:body (io/input-stream (io/resource "web/favicon.ico"))
                            :content-type "image/png"}))
   :kill-instance-handler-fn (pc/fnk [[:daemons router-state-maintainer]
                                      [:routines peers-acknowledged-blacklist-requests-fn]
                                      [:scheduler scheduler]
                                      [:state instance-rpc-chan scaling-timeout-config scheduler-interactions-thread-pool]
                                      wrap-router-auth-fn]
                               (let [{{:keys [notify-instance-killed-fn]} :maintainer} router-state-maintainer]
                                 (wrap-router-auth-fn
                                   (fn kill-instance-handler-fn [request]
                                     (scaling/kill-instance-handler
                                       notify-instance-killed-fn peers-acknowledged-blacklist-requests-fn
                                       scheduler instance-rpc-chan scaling-timeout-config scheduler-interactions-thread-pool
                                       request)))))
   :metrics-request-handler-fn (pc/fnk []
                                 (fn metrics-request-handler-fn [request]
                                   (handler/metrics-request-handler request)))
   :not-found-handler-fn (pc/fnk [] handler/not-found-handler)
   :ping-service-handler (pc/fnk [[:daemons router-state-maintainer]
                                  [:state fallback-state-atom]
                                  process-request-handler-fn process-request-wrapper-fn wrap-secure-request-fn]
                           (let [{{:keys [query-state-fn]} :maintainer} router-state-maintainer
                                 handler (process-request-wrapper-fn
                                           (fn inner-ping-service-handler [request]
                                             (let [service-state-fn
                                                   (fn [service-id]
                                                     (let [fallback-state @fallback-state-atom
                                                           global-state (query-state-fn)]
                                                       {:exists? (descriptor/service-exists? fallback-state service-id)
                                                        :healthy? (descriptor/service-healthy? fallback-state service-id)
                                                        :service-id service-id
                                                        :status (service/retrieve-service-status-label service-id global-state)}))]
                                               (pr/ping-service process-request-handler-fn service-state-fn request))))]
                             (wrap-secure-request-fn
                               (fn ping-service-handler [request]
                                 (-> request
                                   (update :headers assoc "x-waiter-fallback-period-secs" "0")
                                   (handler))))))
   :process-request-fn (pc/fnk [process-request-handler-fn process-request-wrapper-fn]
                         (process-request-wrapper-fn process-request-handler-fn))
   :process-request-handler-fn (pc/fnk [[:routines determine-priority-fn make-basic-auth-fn post-process-async-request-response-fn
                                         service-id->password-fn start-new-service-fn]
                                        [:settings instance-request-properties]
                                        [:state http-clients instance-rpc-chan local-usage-agent]]
                                 (let [make-request-fn (fn [instance request request-properties passthrough-headers end-route metric-group
                                                            backend-proto proto-version]
                                                         (pr/make-request
                                                           http-clients make-basic-auth-fn service-id->password-fn
                                                           instance request request-properties passthrough-headers end-route metric-group
                                                           backend-proto proto-version))
                                       process-response-fn (partial pr/process-http-response post-process-async-request-response-fn)]
                                   (fn inner-process-request [request]
                                     (pr/process make-request-fn instance-rpc-chan start-new-service-fn
                                                 instance-request-properties determine-priority-fn process-response-fn
                                                 pr/abort-http-request-callback-factory local-usage-agent request))))
   :process-request-wrapper-fn (pc/fnk [[:state interstitial-state-atom]
                                        wrap-auth-bypass-fn wrap-descriptor-fn wrap-https-redirect-fn
                                        wrap-secure-request-fn wrap-service-discovery-fn]
                                 (fn process-handler-wrapper-fn [handler]
                                   (-> handler
                                     pr/wrap-too-many-requests
                                     pr/wrap-suspended-service
                                     pr/wrap-response-status-metrics
                                     (interstitial/wrap-interstitial interstitial-state-atom)
                                     wrap-descriptor-fn
                                     wrap-secure-request-fn
                                     wrap-auth-bypass-fn
                                     wrap-https-redirect-fn
                                     wrap-service-discovery-fn)))
   :router-metrics-handler-fn (pc/fnk [[:routines crypt-helpers]
                                       [:settings [:metrics-config metrics-sync-interval-ms]]
                                       [:state router-metrics-agent]]
                                (let [{:keys [bytes-decryptor bytes-encryptor]} crypt-helpers]
                                  (fn router-metrics-handler-fn [request]
                                    (metrics-sync/incoming-router-metrics-handler
                                      router-metrics-agent metrics-sync-interval-ms bytes-encryptor bytes-decryptor request))))
   :service-handler-fn (pc/fnk [[:curator kv-store]
                                [:daemons router-state-maintainer]
                                [:routines allowed-to-manage-service?-fn generate-log-url-fn make-inter-router-requests-sync-fn
                                 router-metrics-helpers service-id->service-description-fn service-id->source-tokens-entries-fn
                                 token->token-hash]
                                [:scheduler scheduler]
                                [:state router-id scheduler-interactions-thread-pool]
                                wrap-secure-request-fn]
                         (let [{{:keys [query-state-fn]} :maintainer} router-state-maintainer
                               {:keys [service-id->metrics-fn]} router-metrics-helpers]
                           (wrap-secure-request-fn
                             (fn service-handler-fn [{:as request {:keys [service-id]} :route-params}]
                               (handler/service-handler router-id service-id scheduler kv-store allowed-to-manage-service?-fn
                                                        generate-log-url-fn make-inter-router-requests-sync-fn
                                                        service-id->service-description-fn service-id->source-tokens-entries-fn
                                                        query-state-fn service-id->metrics-fn scheduler-interactions-thread-pool
                                                        token->token-hash request)))))
   :service-id-handler-fn (pc/fnk [[:curator kv-store]
                                   [:routines store-service-description-fn]
                                   wrap-descriptor-fn wrap-secure-request-fn]
                            (-> (fn service-id-handler-fn [request]
                                  (handler/service-id-handler request kv-store store-service-description-fn))
                                wrap-descriptor-fn
                                wrap-secure-request-fn))
   :service-list-handler-fn (pc/fnk [[:daemons router-state-maintainer]
                                     [:routines prepend-waiter-url router-metrics-helpers
                                      service-id->service-description-fn service-id->source-tokens-entries-fn]
                                     [:state entitlement-manager]
                                     wrap-secure-request-fn]
                              (let [{{:keys [query-state-fn]} :maintainer} router-state-maintainer
                                    {:keys [service-id->metrics-fn]} router-metrics-helpers]
                                (wrap-secure-request-fn
                                  (fn service-list-handler-fn [request]
                                    (handler/list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                                                   service-id->service-description-fn service-id->metrics-fn
                                                                   service-id->source-tokens-entries-fn request)))))
   :service-override-handler-fn (pc/fnk [[:curator kv-store]
                                         [:routines allowed-to-manage-service?-fn make-inter-router-requests-sync-fn]
                                         wrap-secure-request-fn]
                                  (wrap-secure-request-fn
                                    (fn service-override-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                      (handler/override-service-handler kv-store allowed-to-manage-service?-fn
                                                                        make-inter-router-requests-sync-fn service-id request))))
   :service-refresh-handler-fn (pc/fnk [[:curator kv-store]
                                        wrap-router-auth-fn]
                                 (wrap-router-auth-fn
                                   (fn service-refresh-handler [{{:keys [service-id]} :route-params
                                                                 {:keys [src-router-id]} :basic-authentication}]
                                     (log/info service-id "refresh triggered by router" src-router-id)
                                     (sd/fetch-core kv-store service-id :refresh true)
                                     (sd/service-id->suspended-state kv-store service-id :refresh true)
                                     (sd/service-id->overrides kv-store service-id :refresh true))))
   :service-resume-handler-fn (pc/fnk [[:curator kv-store]
                                       [:routines allowed-to-manage-service?-fn make-inter-router-requests-sync-fn]
                                       wrap-secure-request-fn]
                                (wrap-secure-request-fn
                                  (fn service-resume-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                    (handler/suspend-or-resume-service-handler
                                      kv-store allowed-to-manage-service?-fn make-inter-router-requests-sync-fn service-id :resume request))))
   :service-suspend-handler-fn (pc/fnk [[:curator kv-store]
                                        [:routines allowed-to-manage-service?-fn make-inter-router-requests-sync-fn]
                                        wrap-secure-request-fn]
                                 (wrap-secure-request-fn
                                   (fn service-suspend-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                     (handler/suspend-or-resume-service-handler
                                       kv-store allowed-to-manage-service?-fn make-inter-router-requests-sync-fn service-id :suspend request))))
   :service-view-logs-handler-fn (pc/fnk [[:routines generate-log-url-fn]
                                          [:scheduler scheduler]
                                          wrap-secure-request-fn]
                                   (wrap-secure-request-fn
                                     (fn service-view-logs-handler-fn [{:as request {:keys [service-id]} :route-params}]
                                       (handler/service-view-logs-handler scheduler service-id generate-log-url-fn request))))
   :sim-request-handler (pc/fnk [] simulator/handle-sim-request)
   :state-all-handler-fn (pc/fnk [[:daemons router-state-maintainer]
                                  [:state router-id]
                                  wrap-secure-request-fn]
                           (let [{{:keys [query-state-fn]} :maintainer} router-state-maintainer]
                             (wrap-secure-request-fn
                               (fn state-all-handler-fn [request]
                                 (handler/get-router-state router-id query-state-fn request)))))
   :state-autoscaler-handler-fn (pc/fnk [[:daemons autoscaler]
                                         [:state router-id]
                                         wrap-secure-request-fn]
                                  (let [{:keys [query-state-fn]} autoscaler]
                                    (wrap-secure-request-fn
                                      (fn state-autoscaler-handler-fn [request]
                                        (handler/get-query-fn-state router-id query-state-fn request)))))
   :state-autoscaling-multiplexer-handler-fn (pc/fnk [[:daemons autoscaling-multiplexer]
                                                      [:state router-id]
                                                      wrap-secure-request-fn]
                                               (let [{:keys [query-chan]} autoscaling-multiplexer]
                                                 (wrap-secure-request-fn
                                                   (fn state-autoscaling-multiplexer-handler-fn [request]
                                                     (handler/get-query-chan-state-handler router-id query-chan request)))))
   :state-codahale-reporters-handler-fn (pc/fnk [[:daemons codahale-reporters]
                                                 [:state router-id]]
                                          (fn codahale-reporter-state-handler-fn [request]
                                            (handler/get-query-fn-state
                                              router-id
                                              #(pc/map-vals reporter/state codahale-reporters)
                                              request)))
   :state-fallback-handler-fn (pc/fnk [[:daemons fallback-maintainer]
                                       [:state router-id]
                                       wrap-secure-request-fn]
                                (let [fallback-query-chan (:query-chan fallback-maintainer)]
                                  (wrap-secure-request-fn
                                    (fn state-fallback-handler-fn [request]
                                      (handler/get-query-chan-state-handler router-id fallback-query-chan request)))))
   :state-gc-for-broken-services (pc/fnk [[:daemons scheduler-broken-services-gc]
                                          [:state router-id]
                                          wrap-secure-request-fn]
                                   (let [{:keys [query-state-fn]} scheduler-broken-services-gc]
                                     (wrap-secure-request-fn
                                       (fn state-autoscaler-handler-fn [request]
                                         (handler/get-query-fn-state router-id query-state-fn request)))))
   :state-gc-for-services (pc/fnk [[:daemons scheduler-services-gc]
                                   [:state router-id]
                                   wrap-secure-request-fn]
                            (let [{:keys [query-state-fn]} scheduler-services-gc]
                              (wrap-secure-request-fn
                                (fn state-autoscaler-handler-fn [request]
                                  (handler/get-query-fn-state router-id query-state-fn request)))))
   :state-gc-for-transient-metrics (pc/fnk [[:daemons gc-for-transient-metrics]
                                            [:state router-id]
                                            wrap-secure-request-fn]
                                     (let [{:keys [query-state-fn]} gc-for-transient-metrics]
                                       (wrap-secure-request-fn
                                         (fn state-autoscaler-handler-fn [request]
                                           (handler/get-query-fn-state router-id query-state-fn request)))))
   :state-interstitial-handler-fn (pc/fnk [[:daemons interstitial-maintainer]
                                           [:state router-id]
                                           wrap-secure-request-fn]
                                    (let [interstitial-query-chan (:query-chan interstitial-maintainer)]
                                      (wrap-secure-request-fn
                                        (fn state-interstitial-handler-fn [request]
                                          (handler/get-query-chan-state-handler router-id interstitial-query-chan request)))))
   :state-launch-metrics-handler-fn (pc/fnk [[:daemons launch-metrics-maintainer]
                                             [:state router-id]
                                             wrap-secure-request-fn]
                                      (let [query-chan (:query-chan launch-metrics-maintainer)]
                                        (wrap-secure-request-fn
                                          (fn state-launch-metrics-handler-fn [request]
                                            (handler/get-query-chan-state-handler router-id query-chan request)))))
   :state-kv-store-handler-fn (pc/fnk [[:curator kv-store]
                                       [:state router-id]
                                       wrap-secure-request-fn]
                                (wrap-secure-request-fn
                                  (fn kv-store-state-handler-fn [request]
                                    (handler/get-kv-store-state router-id kv-store request))))
   :state-leader-handler-fn (pc/fnk [[:curator leader?-fn leader-id-fn]
                                     [:state router-id]
                                     wrap-secure-request-fn]
                              (wrap-secure-request-fn
                                (fn leader-state-handler-fn [request]
                                  (handler/get-leader-state router-id leader?-fn leader-id-fn request))))
   :state-local-usage-handler-fn (pc/fnk [[:state local-usage-agent router-id]
                                          wrap-secure-request-fn]
                                   (wrap-secure-request-fn
                                     (fn local-usage-state-handler-fn [request]
                                       (handler/get-local-usage-state router-id local-usage-agent request))))
   :state-maintainer-handler-fn (pc/fnk [[:daemons router-state-maintainer]
                                         [:state router-id]
                                         wrap-secure-request-fn]
                                  (let [{{:keys [query-state-fn]} :maintainer} router-state-maintainer]
                                    (wrap-secure-request-fn
                                      (fn maintainer-state-handler-fn [request]
                                        (handler/get-chan-latest-state-handler router-id query-state-fn request)))))
   :state-router-metrics-handler-fn (pc/fnk [[:routines router-metrics-helpers]
                                             [:state router-id]
                                             wrap-secure-request-fn]
                                      (let [router-metrics-state-fn (:router-metrics-state-fn router-metrics-helpers)]
                                        (wrap-secure-request-fn
                                          (fn r-router-metrics-state-handler-fn [request]
                                            (handler/get-router-metrics-state router-id router-metrics-state-fn request)))))
   :state-scheduler-handler-fn (pc/fnk [[:scheduler scheduler]
                                        [:state router-id]
                                        wrap-secure-request-fn]
                                 (wrap-secure-request-fn
                                   (fn scheduler-state-handler-fn [request]
                                     (handler/get-scheduler-state router-id scheduler request))))
   :state-service-description-builder-handler-fn (pc/fnk [[:state router-id service-description-builder]]
                                                   (fn service-description-builder-state-handler-fn [request]
                                                     (handler/get-query-fn-state
                                                       router-id
                                                       #(sd/state service-description-builder)
                                                       request)))
   :state-service-handler-fn (pc/fnk [[:daemons state-sources]
                                      [:state instance-rpc-chan local-usage-agent router-id]
                                      wrap-secure-request-fn]
                               (wrap-secure-request-fn
                                 (fn service-state-handler-fn [{{:keys [service-id]} :route-params :as request}]
                                   (handler/get-service-state router-id instance-rpc-chan local-usage-agent
                                                              service-id state-sources request))))
   :state-statsd-handler-fn (pc/fnk [[:state router-id]
                                     wrap-secure-request-fn]
                              (wrap-secure-request-fn
                                (fn state-statsd-handler-fn [request]
                                  (handler/get-statsd-state router-id request))))
   :status-handler-fn (pc/fnk [] handler/status-handler)
   :token-handler-fn (pc/fnk [[:curator kv-store]
                              [:routines make-inter-router-requests-sync-fn synchronize-fn validate-service-description-fn]
                              [:settings [:token-config history-length limit-per-owner]]
                              [:state clock entitlement-manager token-cluster-calculator token-root waiter-hostnames]
                              wrap-secure-request-fn]
                       (wrap-secure-request-fn
                         (fn token-handler-fn [request]
                           (token/handle-token-request
                             clock synchronize-fn kv-store token-cluster-calculator token-root history-length limit-per-owner
                             waiter-hostnames entitlement-manager make-inter-router-requests-sync-fn validate-service-description-fn
                             request))))
   :token-list-handler-fn (pc/fnk [[:curator kv-store]
                                   [:state entitlement-manager]
                                   wrap-secure-request-fn]
                            (wrap-secure-request-fn
                              (fn token-handler-fn [request]
                                (token/handle-list-tokens-request kv-store entitlement-manager request))))
   :token-owners-handler-fn (pc/fnk [[:curator kv-store]
                                     wrap-secure-request-fn]
                              (wrap-secure-request-fn
                                (fn token-owners-handler-fn [request]
                                  (token/handle-list-token-owners-request kv-store request))))
   :token-refresh-handler-fn (pc/fnk [[:curator kv-store]
                                      wrap-router-auth-fn]
                               (wrap-router-auth-fn
                                 (fn token-refresh-handler-fn [request]
                                   (token/handle-refresh-token-request kv-store request))))
   :token-reindex-handler-fn (pc/fnk [[:curator kv-store]
                                      [:routines list-tokens-fn make-inter-router-requests-sync-fn synchronize-fn]
                                      wrap-secure-request-fn]
                               (wrap-secure-request-fn
                                 (fn token-handler-fn [request]
                                   (token/handle-reindex-tokens-request synchronize-fn make-inter-router-requests-sync-fn
                                                                        kv-store list-tokens-fn request))))
   :waiter-acknowledge-consent-handler-fn (pc/fnk [[:routines service-description->service-id token->service-description-template
                                                    token->token-metadata]
                                                   [:settings consent-expiry-days]
                                                   [:state clock passwords]
                                                   wrap-secure-request-fn]
                                            (let [password (first passwords)]
                                              (letfn [(add-encoded-cookie [response cookie-name value expiry-days]
                                                        (cookie-support/add-encoded-cookie response password cookie-name value (-> expiry-days t/days t/in-seconds)))
                                                      (consent-cookie-value [mode service-id token token-metadata]
                                                        (sd/consent-cookie-value clock mode service-id token token-metadata))]
                                                (wrap-secure-request-fn
                                                  (fn inner-waiter-acknowledge-consent-handler-fn [request]
                                                    (handler/acknowledge-consent-handler
                                                      token->service-description-template token->token-metadata
                                                      service-description->service-id consent-cookie-value add-encoded-cookie
                                                      consent-expiry-days request))))))
   :waiter-auth-callback-handler-fn (pc/fnk [[:state authenticator]]
                                      (fn waiter-auth-callback-handler-fn [request]
                                        (auth/process-callback authenticator request)))
   :waiter-auth-handler-fn (pc/fnk [wrap-secure-request-fn]
                             (wrap-secure-request-fn
                               (fn waiter-auth-handler-fn
                                 [{:keys [authorization/method authorization/principal authorization/user]}]
                                 (utils/attach-waiter-source
                                   {:body (str user)
                                    :headers {"x-waiter-auth-method" (some-> method name)
                                              "x-waiter-auth-principal" (str principal)
                                              "x-waiter-auth-user" (str user)}
                                    :status 200}))))
   :waiter-request-consent-handler-fn (pc/fnk [[:routines service-description->service-id token->service-description-template]
                                               [:settings consent-expiry-days]
                                               wrap-secure-request-fn]
                                        (wrap-secure-request-fn
                                          (fn waiter-request-consent-handler-fn [request]
                                            (handler/request-consent-handler
                                              token->service-description-template service-description->service-id
                                              consent-expiry-days request))))
   :waiter-request-interstitial-handler-fn (pc/fnk [wrap-secure-request-fn]
                                             (wrap-secure-request-fn
                                               (fn waiter-request-interstitial-handler-fn [request]
                                                 (interstitial/display-interstitial-handler request))))
   :welcome-handler-fn (pc/fnk [settings]
                         (partial handler/welcome-handler settings))
   :work-stealing-handler-fn (pc/fnk [[:state instance-rpc-chan]
                                      wrap-router-auth-fn]
                               (wrap-router-auth-fn
                                 (fn [request]
                                   (handler/work-stealing-handler instance-rpc-chan request))))
   :wrap-auth-bypass-fn (pc/fnk []
                          (fn wrap-auth-bypass-fn
                            [handler]
                            (fn [{:keys [waiter-discovery] :as request}]
                              (let [{:keys [service-parameter-template token waiter-headers]} waiter-discovery
                                    {:strs [authentication] :as service-description} service-parameter-template
                                    authentication-disabled? (= authentication "disabled")]
                                (cond
                                  (contains? waiter-headers "x-waiter-authentication")
                                  (do
                                    (log/info "x-waiter-authentication is not supported as an on-the-fly header"
                                              {:service-description service-description, :token token})
                                    (utils/clj->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                                              :status 400))

                                  ;; ensure service description formed comes entirely from the token by ensuring absence of on-the-fly headers
                                  (and authentication-disabled? (some sd/service-parameter-keys (-> waiter-headers headers/drop-waiter-header-prefix keys)))
                                  (do
                                    (log/info "request cannot proceed as it is mixing an authentication disabled token with on-the-fly headers"
                                              {:service-description service-description, :token token})
                                    (utils/clj->json-response {:error "An authentication disabled token may not be combined with on-the-fly headers"}
                                                              :status 400))

                                  authentication-disabled?
                                  (do
                                    (log/info "request configured to skip authentication")
                                    (handler (assoc request :skip-authentication true)))

                                  :else
                                  (handler request))))))
   :wrap-descriptor-fn (pc/fnk [[:routines request->descriptor-fn start-new-service-fn]
                                [:state fallback-state-atom]]
                         (fn wrap-descriptor-fn [handler]
                           (descriptor/wrap-descriptor handler request->descriptor-fn start-new-service-fn fallback-state-atom)))
   :wrap-https-redirect-fn (pc/fnk []
                             (fn wrap-https-redirect-fn
                               [handler]
                               (fn [request]
                                 (cond
                                   (and (get-in request [:waiter-discovery :token-metadata "https-redirect"])
                                        ;; ignore websocket requests
                                        (= :http (utils/request->scheme request)))
                                   (do
                                     (log/info "triggering ssl redirect")
                                     (-> (ssl/ssl-redirect-response request {})
                                       (utils/attach-waiter-source)))

                                   :else
                                   (handler request)))))
   :wrap-router-auth-fn (pc/fnk [[:state passwords router-id]]
                          (fn wrap-router-auth-fn [handler]
                            (fn [request]
                              (let [router-comm-authenticated?
                                    (fn router-comm-authenticated? [source-id secret-word]
                                      (let [expected-word (utils/generate-secret-word source-id router-id passwords)
                                            authenticated? (= expected-word secret-word)]
                                        (log/info "Authenticating inter-router communication from" source-id)
                                        (if-not authenticated?
                                          (log/info "inter-router request authentication failed!"
                                                    {:actual secret-word, :expected expected-word})
                                          {:src-router-id source-id})))
                                    basic-auth-handler (basic-authentication/wrap-basic-authentication handler router-comm-authenticated?)]
                                (basic-auth-handler request)))))
   :wrap-secure-request-fn (pc/fnk [[:routines authentication-method-wrapper-fn waiter-request?-fn]
                                    [:settings cors-config]
                                    [:state cors-validator]]
                             (let [{:keys [exposed-headers]} cors-config]
                               (fn wrap-secure-request-fn
                                 [handler]
                                 (let [handler (-> handler
                                                 (cors/wrap-cors-request
                                                   cors-validator waiter-request?-fn exposed-headers)
                                                 authentication-method-wrapper-fn)]
                                   (fn inner-wrap-secure-request-fn [{:keys [uri] :as request}]
                                     (log/debug "secure request received at" uri)
                                     (handler request))))))
   :wrap-service-discovery-fn (pc/fnk [[:curator kv-store]
                                       [:settings [:token-config token-defaults]]
                                       [:state waiter-hostnames]]
                                (fn wrap-service-discovery-fn
                                  [handler]
                                  (fn [{:keys [headers] :as request}]
                                    ;; TODO optimization opportunity to avoid this re-computation later in the chain
                                    (let [discovered-parameters (sd/discover-service-parameters kv-store token-defaults waiter-hostnames headers)]
                                      (handler (assoc request :waiter-discovery discovered-parameters))))))})
