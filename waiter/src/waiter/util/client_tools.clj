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
(ns waiter.util.client-tools
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [waiter.auth.authentication :as authentication]
            [waiter.correlation-id :as cid]
            [waiter.mesos.marathon :as marathon]
            [waiter.statsd :as statsd]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu]
            [waiter.util.utils :as utils])
  (:import (java.net HttpCookie URI)
           (java.io ByteArrayInputStream)
           (java.nio ByteBuffer)
           (java.util.concurrent Callable Future Executors)
           (org.joda.time Period)
           (org.joda.time.format PeriodFormatterBuilder)))

(def ^:const WAITER-PORT 9091)
(def ^:const HTTP-SCHEME "http://")

(def use-spnego (-> (System/getenv "USE_SPNEGO") str Boolean/parseBoolean))

(def ^:const ANSI-YELLOW "\033[1m\033[33m")
(def ^:const ANSI-CYAN "\033[36m")
(def ^:const ANSI-RESET "\033[0m")

(defn yellow [message] (str ANSI-YELLOW message ANSI-RESET))
(defn cyan [message] (str ANSI-CYAN message ANSI-RESET))

(def ^:const INFO (yellow "INFO: "))
(def ^:const TIME (yellow "TIME: "))

(def ^:const required-response-headers ["x-cid"])

(defn colored-time [time-string] (yellow time-string))

(defn try-parse-json
  [s]
  (try
    (json/read-str s)
    (catch Exception e
      (throw (ex-info "Couldn't parse JSON" {:string s} e)))))

(defn execute-command [& args]
  (let [shell-output (apply shell/sh args)]
    (when (not= 0 (:exit shell-output))
      (log/info (str "Error in running command: " (str/join " " args)))
      (throw (IllegalStateException. (str (:err shell-output)))))
    (str/trim (:out shell-output))))

(defn retrieve-username []
  (or (System/getProperty "user.name")
      (execute-command "id" "-un")))

(defn retrieve-hostname []
  (if use-spnego
    (let [username (retrieve-username)
          machine-name (execute-command "hostname")]
      (str username "." machine-name))
    "127.0.0.1"))

(defn sanitize-waiter-url
  "Be kind to the tester, extracting the host:port of the URL if passed in as
  a URL instead of host:port."
  [waiter-uri]
  (if (and waiter-uri (str/starts-with? waiter-uri "http://"))
    (-> waiter-uri URI. .getAuthority)
    waiter-uri))

(defn retrieve-waiter-url []
  {:post [%]}
  (let [waiter-uri (-> "WAITER_URI" System/getenv sanitize-waiter-url)]
    (if waiter-uri
      (do
        (log/debug "using WAITER_URI from environment:" waiter-uri)
        waiter-uri)
      (let [port WAITER-PORT]
        (if (utils/port-available? port)
          (log/warn "port" port "is not in use, you may need to start Waiter")
          (log/info "port" port "is already in use, assuming Waiter is running"))
        (str (retrieve-hostname) ":" port)))))

(defn retrieve-waiter-port
  "Retrieves the Waiter port for receiving http requests."
  [waiter-url]
  (or (second (str/split waiter-url #":" 2))
      "80"))

(defn retrieve-h2c-port
  "Retrieves the Waiter port for receiving h2c requests."
  [waiter-url]
  (or (System/getenv "WAITER_H2C_PORT")
      (retrieve-waiter-port waiter-url)))

(defn retrieve-h2c-url
  "Retrieves the Waiter url for receiving h2c requests."
  [waiter-url]
  (str (first (str/split waiter-url #":" 2))
       ":"
       (retrieve-h2c-port waiter-url)))

(defn retrieve-ssl-port
  "Retrieves the Waiter port for receiving ssl requests."
  [ssl-port]
  (or (System/getenv "WAITER_SSL_PORT")
      ssl-port
      "443"))

(defn retrieve-ssl-url
  "Retrieves the Waiter url for receiving ssl requests."
  [waiter-url ssl-port]
  (str (first (str/split waiter-url #":" 2))
       ":"
       (retrieve-ssl-port ssl-port)))

(defn behind-proxy?
  "Returns true if Waiter if running behind a proxy."
  [waiter-url]
  (not= (retrieve-waiter-port waiter-url)
        (retrieve-h2c-port waiter-url)))

(defn interval-to-str [^Period interval]
  (let [builder (doto (PeriodFormatterBuilder.)
                  (.printZeroNever)
                  (.printZeroRarelyLast)
                  (.appendMinutes)
                  (.appendSuffix " min " " mins ")
                  (.appendSeconds)
                  (.appendSuffix " sec " " secs ")
                  (.appendMillis)
                  (.appendSuffix " ms "))
        formatter (.toFormatter builder)
        period (.normalizedStandard (.toPeriod interval))]
    (str/trim (.print formatter period))))

(def waiter-url "DUMMY-DEF-TO-KEEP-IDE-HAPPY-INSTEAD-WRAP-INSIDE-timing-using-waiter-url")

(def ^:dynamic http1-client nil)
(def ^:dynamic http2-client nil)

(defmacro using-waiter-url
  [& body]
  `(let [~'waiter-url (retrieve-waiter-url)]
     ~@body))

(defmacro elapsed-time-ms
  ([& body]
   `(let [start-time-ms# (System/currentTimeMillis)
          _# (do ~@body) end-time-ms# (System/currentTimeMillis)
          elapsed-time-ms# (- end-time-ms# start-time-ms#)]
      elapsed-time-ms#)))

(defmacro time-it
  ([name & body]
   `(do
      (let [date-str# (du/date-to-str (t/now))]
        (log/info (str INFO (cyan ~name) " started at " date-str#)))
      (let [start-time-ms# (System/currentTimeMillis)
            execution-result# (atom nil)
            execution-exception# (atom nil)
            elapsed-time-ms# (try
                               (elapsed-time-ms
                                 (reset! execution-result# (do ~@body)))
                               (catch Exception e#
                                 (reset! execution-exception# e#)
                                 (- (System/currentTimeMillis) start-time-ms#)))
            interval-str# (interval-to-str (t/millis elapsed-time-ms#))]
        (log/info (str TIME (cyan ~name) " took " (colored-time interval-str#) "."))
        (if @execution-exception#
          (throw @execution-exception#)
          @execution-result#)))))

(defmacro timing-using-waiter-url
  [name & body]
  `(using-waiter-url
     (time-it ~name ~@body)))

(defn- git-show->branch-name [text]
  (->
    text
    (str/trim)
    (str/split #", ")
    (last)
    (str/replace ")" "")
    (str/replace #"^.+/" "")))

(deftest test-git-show->branch-name
  (is (= "master" (git-show->branch-name "(HEAD, origin/master)")))
  (is (= "foo" (git-show->branch-name " (HEAD, origin/foo, foo)"))))

(defn- retrieve-git-branch []
  (->
    (shell/sh "git" "show" "-s" "--pretty=%d" "HEAD")
    (:out)
    (git-show->branch-name)))

(defn make-http-clients
  "Instantiates and returns http1 and http2 clients without a cookie store"
  []
  (hu/prepare-http-clients {:client-name (str "waiter-test-" (retrieve-git-branch))
                            :clear-content-decoders false
                            :conn-timeout 10000
                            :user-agent (str "waiter-test/" (retrieve-git-branch))}))

(defn current-test-name
  "Get the name of the currently-running test."
  []
  (str (-> *testing-vars* first meta :ns str)
       "/"
       (-> *testing-vars* first meta :name)))

(defmacro testing-using-waiter-url
  [& body]
  `(let [name# (current-test-name)]
     (testing name#
       (cid/with-correlation-id
         name#
         (timing-using-waiter-url
           name#
           (let [local-http-clients# (make-http-clients)
                 local-http1-client# (:http1-client local-http-clients#)
                 local-http2-client# (:http2-client local-http-clients#)]
             (binding [http1-client local-http1-client#
                       http2-client local-http2-client#]
               (try
                 ~@body
                 (finally
                   (http/stop-client! http1-client)
                   (http/stop-client! http2-client))))))))))

(defn instance-id->service-id [^String instance-id]
  (when (str/index-of instance-id ".") (subs instance-id 0 (str/index-of instance-id "."))))

(defn make-request-with-debug-info [request-headers request-fn]
  (let [response (request-fn (assoc request-headers :x-waiter-debug "true"))
        response-headers (or (:headers response) {})
        instance-id (str (get response-headers "x-waiter-backend-id"))
        service-id (or (get response-headers "x-waiter-service-id")
                       (instance-id->service-id instance-id))
        router-id (str (get-in response [:headers "x-waiter-router-id"]))]
    (assoc response
      :router-id router-id
      :instance-id instance-id
      :service-id service-id)))

(defn response->location
  "Get the Location header from an HTTP response.
   Handles both HTTP v1 and v2 styles of capitalization."
  [{:keys [headers]}]
  (or (get headers "Location") (get headers "location")))

(defn extract-acronym
  "Shortens the name taking only the leading characters across the delimiters.
   E.g. `(extract-acronym \"aaa.bbb-ccc/ddd#eee-fff.ggg\") -> \"abcdfg\"`"
  [name-with-dashes]
  (->> (str/split (str name-with-dashes) #"-|\.|/")
    (remove str/blank?)
    (map first)
    (str/join "")
    str/lower-case))

(defn ensure-cid-in-headers
  [request-headers & {:keys [verbose] :or {verbose false}}]
  (let [cid (or (get request-headers "x-cid")
                (get request-headers :x-cid))]
    (if cid
      request-headers
      (let [correlation-id (cid/get-correlation-id)
            new-cid (str "test-" (extract-acronym (current-test-name)) "-" (utils/unique-identifier))]
        (when verbose
          (log/info "Using cid" new-cid))
        (when (str/includes? correlation-id "UNKNOWN")
          (log/info "Correlation id context unspecified")
          (when verbose
            (log/warn (RuntimeException.))))
        (assoc request-headers "x-cid" new-cid)))))

(defn strip-trailing-slash
  "If s ends with /, returns s with the / stripped"
  [s]
  (cond-> s (str/ends-with? s "/") (subs 0 (dec (count s)))))

(defn parse-set-cookie-string
  "Parse the value of a single Set-Cookie header."
  [^String value]
  (when value
    (let [cookie-list (HttpCookie/parse value)]
      (map (fn [^HttpCookie cookie]
             {:max-age (.getMaxAge cookie)
              :name (.getName cookie)
              :value (.getValue cookie)}) cookie-list))))

(defn parse-cookies
  "Create a list of cookies from response headers."
  [set-cookie-header-value]
  (when set-cookie-header-value
    (vec (cond (coll? set-cookie-header-value) (mapcat parse-set-cookie-string set-cookie-header-value)
               (string? set-cookie-header-value) (parse-set-cookie-string set-cookie-header-value)))))

(defn make-request
  ([waiter-url path &
    {:keys [body client cookies content-type disable-auth form-params headers
            method multipart protocol query-params scheme trailers-fn verbose]
     :or {body nil
          cookies []
          disable-auth false
          headers {}
          method :get
          query-params {}
          verbose false}}]
   (let [scheme (or scheme
                    (some-> protocol hu/backend-proto->scheme)
                    "http")
         client (or client
                    (when protocol
                      (hu/select-http-client
                        protocol {:http1-client http1-client :http2-client http2-client}))
                    http1-client)
         request-url (str
                       (when-not (str/starts-with? waiter-url scheme)
                         (str scheme "://"))
                       (strip-trailing-slash waiter-url)
                       path)
         request-headers (walk/stringify-keys (ensure-cid-in-headers headers))]
     (try
       (when verbose
         (log/info "request url:" request-url)
         (log/info "request headers:" (into (sorted-map) request-headers)))
       (let [waiter-auth-cookie (some #(= authentication/AUTH-COOKIE-NAME (:name %)) cookies)
             add-spnego-auth (and (not disable-auth) use-spnego (not waiter-auth-cookie))
             {:keys [body error error-chan headers status trailers]}
             (async/<!! (http/request
                          client
                          (cond-> {:body body
                                   :follow-redirects? false
                                   :headers request-headers
                                   :method method
                                   :query-string query-params
                                   :url request-url}
                            multipart (assoc :multipart multipart)
                            add-spnego-auth (assoc :auth (hu/spnego-authentication (URI. request-url)))
                            form-params (assoc :form-params form-params)
                            (not (str/blank? content-type)) (assoc :content-type content-type)
                            cookies (assoc :cookies (map (fn [c] [(:name c) (:value c)]) cookies))
                            trailers-fn (assoc :trailers-fn trailers-fn))))
             response-body (when body (async/<!! body))
             error (or error
                       (when error-chan (async/<!! error-chan)))]
         (when verbose
           (log/info (get request-headers "x-cid") "response size:" (count (str response-body))))
         {:body response-body
          :cookies (parse-cookies (get headers "set-cookie"))
          :error error
          :headers headers
          :request-headers request-headers
          :status status
          :trailers (when trailers (async/<!! trailers))})
       (catch Exception e
         (when verbose
           (log/info (get request-headers "x-cid") "error in obtaining response" (.getMessage e)))
         (throw e))))))

(defmacro assert-response-status
  "Asserts the response status and includes CID in failed message log"
  [response expected-status]
  `(let [response-cid# (get-in ~response [:headers "x-cid"] "unknown")
         response-server# (get-in ~response [:headers "server"] "unknown")
         actual-status# (:status ~response)
         expected-status# ~expected-status
         expected-status-set# (if (set? expected-status#)
                                expected-status#
                                #{expected-status#})
         response-body# (:body ~response)
         response-error# (:error ~response)
         assertion-message# (str "[CID=" response-cid# ", server=" response-server# "] "
                                 "Expected status: " ~expected-status ", actual: " actual-status#
                                 \newline "Body:" response-body#
                                 (when response-error# (str \newline "Error: " response-error#)))]
     (when-not (contains? expected-status-set# actual-status#)
       (log/error assertion-message#))
     (is (contains? expected-status-set# actual-status#) assertion-message#)))

(defn kitchen-cmd
  ([] (kitchen-cmd ""))
  ([args]
   (let [cmd (System/getProperty "waiter.test.kitchen.cmd")]
     (if (str/blank? cmd)
       (throw (Exception. "Property waiter.test.kitchen.cmd is not set! (try `lein with-profile +test`)"))
       (str cmd (when-not (str/blank? args) " ") args)))))

(defn courier-server-command
  "Returns the command to launch the courier server."
  [args]
  (let [raw-command (System/getProperty "waiter.test.courier.cmd")]
    (if (str/blank? raw-command)
      (throw (Exception. "Property waiter.test.courier.cmd is not set! (try `lein with-profile +test`)"))
      (str raw-command (when-not (str/blank? args) " ") args))))

(defn nginx-server-command
  "Returns the command to launch the nginx server."
  [backend-proto]
  (let [raw-command (System/getProperty "waiter.test.nginx.cmd")]
    (if (str/blank? raw-command)
      (throw (Exception. "Property waiter.test.nginx.cmd is not set! (try `lein with-profile +test`)"))
      (str raw-command " " backend-proto))))

(defn sediment-server-command
  "Returns the command to launch the sediment server."
  [args]
  (let [raw-command (System/getProperty "waiter.test.sediment.cmd")]
    (if (str/blank? raw-command)
      (throw (Exception. "Property waiter.test.sediment.cmd is not set! (try `lein with-profile +test`)"))
      (str raw-command (when-not (str/blank? args) " ") args))))

(defn kitchen-params
  []
  {:cmd-type "shell"
   :cmd (kitchen-cmd "-p $PORT0")
   :cpus 0.1
   :grace-period-secs 120
   :health-check-url "/status"
   :idle-timeout-mins 10
   :mem 256
   :metric-group "waiter_test"
   :version "version-does-not-matter"})

(defn kitchen-request-headers
  [& {:keys [prefix] :or {prefix "x-waiter-"}}]
  (->> (kitchen-params)
       (pc/map-keys #(str prefix (name %)))
       (walk/keywordize-keys)))

(defn make-light-request
  [waiter-url custom-headers &
   {:keys [body cookies debug method path protocol query-params trailers-fn]
    :or {body nil cookies {} debug true method :post path "/endpoint" query-params {}}}]
  (let [headers (cond->
                  (-> {:x-waiter-cpus 0.1
                       :x-waiter-mem 256
                       :x-waiter-grace-period-secs 120
                       :x-waiter-health-check-url "/status"
                       :x-waiter-idle-timeout-mins 10}
                      (merge custom-headers)
                      (ensure-cid-in-headers)
                      (walk/stringify-keys))
                  debug (assoc :x-waiter-debug true))]
    (make-request waiter-url path
                  :body body
                  :cookies cookies
                  :headers headers
                  :method method
                  :protocol protocol
                  :query-params query-params
                  :trailers-fn trailers-fn)))

(defn make-shell-request
  [waiter-url custom-headers &
   {:keys [body cookies debug method path protocol query-params trailers-fn]
    :or {body nil cookies {} debug true method :post path "/endpoint" query-params {}}}]
  (make-light-request
    waiter-url
    (assoc
      custom-headers
      :x-waiter-cmd-type "shell"
      :x-waiter-version "version-does-not-matter")
    :body body
    :cookies cookies
    :debug debug
    :method method
    :path path
    :protocol protocol
    :query-params query-params
    :trailers-fn trailers-fn))

(defn make-kitchen-request
  "Makes an on-the-fly request to the Kitchen test service."
  [waiter-url custom-headers &
   {:keys [body cookies debug method protocol path query-params]
    :or {body nil cookies {} debug true method :post path "/endpoint" query-params {}}}]
  {:pre [(not (str/blank? waiter-url))]}
  (make-shell-request
    waiter-url
    (merge
      {:x-waiter-cmd (kitchen-cmd "-p $PORT0")
       :x-waiter-metric-group "waiter_test"}
      custom-headers)
    :body body
    :cookies cookies
    :debug debug
    :method method
    :path path
    :protocol protocol
    :query-params query-params))

(defn retrieve-service-id [waiter-url waiter-headers & {:keys [verbose] :or {verbose false}}]
  (let [service-id-result (make-request waiter-url "/service-id" :headers waiter-headers)
        service-id (str (:body service-id-result))]
    (when verbose
      (log/info "service id: " service-id))
    service-id))

(defn retrieve-kitchen-service-id [waiter-url waiter-headers & options]
  (pc/mapply retrieve-service-id waiter-url (merge (kitchen-request-headers) waiter-headers) options))

(defn waiter-settings [waiter-url & {:keys [cookies] :or {cookies []}}]
  (let [settings-result (make-request waiter-url "/settings" :verbose true :cookies cookies)
        settings-json (try-parse-json (:body settings-result))]
    (walk/keywordize-keys settings-json)))

(defn service-settings
  "Fetches and returns the service data at the /apps/<service-id> endpoint."
  [waiter-url service-id & {:keys [cookies keywordize-keys query-params]
                            :or {cookies [] keywordize-keys true query-params {}}}]
  (let [settings-path (str "/apps/" service-id)
        settings-result (make-request waiter-url settings-path :cookies cookies :query-params query-params)]
    (log/debug "service" service-id ":" settings-result)
    (cond-> (some-> settings-result :body try-parse-json)
      keywordize-keys walk/keywordize-keys)))

(defn service-state [waiter-url service-id & {:keys [cookies] :or {cookies {}}}]
  (let [state-result (make-request waiter-url (str "/state/" service-id) :cookies cookies)
        state-body (:body state-result)
        _ (log/debug "service" service-id "state:" state-body)
        state-json (try-parse-json state-body)]
    (walk/keywordize-keys state-json)))

(defn- retrieve-state-helper
  "Fetches and returns the state at the specified endpoint."
  [waiter-url endpoint & {:keys [cookies] :or {cookies {}}}]
  (let [state-body (:body (make-request waiter-url endpoint :verbose true :cookies cookies))]
    (log/debug endpoint "body:" state-body)
    (try-parse-json state-body)))

(defn fallback-state
  "Fetches and returns the fallback state."
  [waiter-url & {:keys [cookies] :or {cookies {}}}]
  (retrieve-state-helper waiter-url "/state/fallback" :cookies cookies))

(defn interstitial-state
  "Fetches and returns the interstitial state."
  [waiter-url & {:keys [cookies] :or {cookies {}}}]
  (retrieve-state-helper waiter-url "/state/interstitial" :cookies cookies))

(defn kv-store-state
  "Fetches and returns the kv-store state."
  [waiter-url & {:keys [cookies] :or {cookies {}}}]
  (retrieve-state-helper waiter-url "/state/kv-store" :cookies cookies))

(defn maintainer-state
  "Fetches and returns the maintainer state."
  [waiter-url & {:keys [cookies] :or {cookies {}}}]
  (retrieve-state-helper waiter-url "/state/maintainer" :cookies cookies))

(defn routers
  [waiter-url]
  (let [state-json (maintainer-state waiter-url)
        routers-raw (get-in state-json ["state" "routers"] {})]
    (log/debug "routers retrieved from /state:" routers-raw)
    (pc/map-vals (fn [router-url]
                   (cond-> router-url
                     (str/starts-with? router-url HTTP-SCHEME) (str/replace HTTP-SCHEME "")))
                 routers-raw)))

(defn router-endpoint
  [waiter-url router-id]
  (let [routers (routers waiter-url)]
    (when-not (contains? routers router-id)
      (log/warn "No router found for " router-id " routers were " routers))
    (get routers router-id)))

(defn setting
  "Returns the value of the Waiter setting at path ks"
  [waiter-url ks & {:keys [verbose] :or {verbose false}}]
  (let [settings-json (waiter-settings waiter-url)
        value (get-in settings-json ks)]
    (when verbose
      (log/info ks "=" value))
    value))

(defn retrieve-default-scheduler-name
  "Return the name of the default 'leaf-level' scheduler"
  [waiter-url & {:keys [settings] :or {settings nil}}]
  (let [settings (or settings (waiter-settings waiter-url))
        kind (get-in settings [:scheduler-config :kind])
        default-scheduler (get-in settings [:scheduler-config (keyword kind) :default-scheduler])]
    (or default-scheduler kind)))

(defn get-scheduler-settings
  "Return the specified scheduler's settings even if the scheduler is part of a composite scheduler."
  [waiter-url scheduler]
  (let [{:keys [kind] :as settings} (-> waiter-url waiter-settings :scheduler-config)]
    (or (get-in settings [(keyword kind) :components scheduler])
        (get-in settings [scheduler]))))

(defn get-marathon-scheduler-settings
  "Return the marathon scheduler's settings even if the scheduler is part of a composite scheduler."
  [waiter-url]
  (get-scheduler-settings waiter-url :marathon))

(defn get-kubernetes-scheduler-settings
  "Return the kubernetes scheduler's settings even if the scheduler is part of a composite scheduler."
  [waiter-url]
  (get-scheduler-settings waiter-url :kubernetes))

(defn marathon-url
  "Returns the Marathon URL setting"
  [waiter-url]
  (-> waiter-url get-marathon-scheduler-settings :url))

(defn num-marathon-tasks-running [waiter-url service-id & {:keys [prev-tasks-running] :or {prev-tasks-running -1}}]
  (let [http-options {:conn-timeout 10000, :socket-timeout 10000, :spnego-auth use-spnego}
        marathon-url (marathon-url waiter-url)
        marathon-api (marathon/api-factory http1-client http-options marathon-url)
        info-response (marathon/get-app marathon-api service-id)
        tasks-running' (get-in info-response [:app :tasksRunning])]
    (when (not= prev-tasks-running tasks-running')
      (log/debug service-id "has" tasks-running' "task(s) running."))
    (int tasks-running')))

(defn delete-service
  ([waiter-url service-id-or-waiter-headers]
   (let [service-id (if (string? service-id-or-waiter-headers)
                      service-id-or-waiter-headers
                      (retrieve-service-id waiter-url service-id-or-waiter-headers))]
     (delete-service waiter-url service-id 5)))
  ([waiter-url service-id limit]
   (when-not (str/blank? service-id)
     (try
       ((utils/retry-strategy {:delay-multiplier 1.2, :inital-delay-ms 250, :max-retries limit})
         (fn []
           (let [app-delete-path (str "/apps/" service-id "?force=true")
                 delete-response (make-request waiter-url app-delete-path :method :delete)
                 delete-json (try-parse-json (:body delete-response))
                 delete-success (true? (get delete-json "success"))
                 no-such-service (= "no-such-service-exists" (get delete-json "result"))]
             (log/debug "Delete response for" service-id ":" delete-json)
             (when (and (not delete-success) (not no-such-service))
               (log/warn "Unable to delete" service-id)
               (throw (Exception. (str "Unable to delete" service-id)))))))
       (catch Exception e
         (log/error "Error in deleting service" service-id ":" (.getMessage e)))))))

(defn await-futures
  [futures & {:keys [verbose] :or {verbose false}}]
  (when verbose
    (log/info "awaiting completion of" (count futures) "launched task(s)."))
  (doseq [future futures]
    (.get ^Future future)))

(defmacro retrieve-task
  [& body]
  `(let [current-testing-context# (first *testing-contexts*)
         current-report-counters# *report-counters*]
     (when (nil? current-testing-context#)
       (println "Nil current-testing-context#!"))
     (when (nil? current-report-counters#)
       (println "Nil current-report-counters#!"))
     (fn []
       (binding [*report-counters* current-report-counters#]
         (testing current-testing-context#
           (cid/with-correlation-id
             (str current-testing-context#)
             ~@body))))))

(defmacro launch-thread
  [& body]
  `(let [task# (retrieve-task ~@body)]
     (async/thread (task#))))

(defn parallelize-requests
  [nthreads niters f & {:keys [canceled? service-id verbose wait-for-tasks]
                        :or {canceled? (constantly false)
                             service-id nil
                             verbose false
                             wait-for-tasks true}}]
  (let [pool (Executors/newFixedThreadPool nthreads)
        start-counter (ref 0)
        finish-counter (ref 0)
        target-count (* nthreads niters)
        print-state-fn #(when verbose
                          (log/info (str (when service-id (str "requests to " service-id ":")))
                                    "started:" @start-counter
                                    ", completed:" @finish-counter
                                    ", target:" target-count))
        num-groups (cond
                     (> target-count 300) 6
                     (> target-count 200) 5
                     (> target-count 100) 4
                     (> target-count 60) 3
                     :else 2)
        checkpoints (set (map #(quot (* % target-count) num-groups) (range 1 num-groups)))
        http1-client-backup http1-client
        http2-client-backup http2-client
        tasks (map (fn [_]
                     (retrieve-task
                       (binding [http1-client http1-client-backup
                                 http2-client http2-client-backup]
                         (loop [iter-id 1
                                result []]
                           (dosync (alter start-counter inc))
                           (let [loop-result (f)
                                 result' (conj result loop-result)]
                             (dosync
                               (alter finish-counter inc)
                               (when (contains? checkpoints @finish-counter) (print-state-fn)))
                             (Thread/sleep 100)
                             (if (or (>= iter-id niters) (canceled?))
                               result'
                               (recur (inc iter-id) result')))))))
                   (range nthreads))
        futures (loop [futures []
                       [task & remaining-tasks] tasks]
                  (if task
                    (recur (conj futures (.submit pool ^Callable task))
                           remaining-tasks)
                    futures))]
    (when wait-for-tasks (await-futures futures))
    (.shutdown pool)
    (print-state-fn)
    (if wait-for-tasks
      (vec (reduce #(concat %1 (.get ^Future %2)) [] futures))
      futures)))

(defn rand-name
  ([]
   (let [service-name (current-test-name)]
     (rand-name service-name)))
  ([service-name]
   (let [test-prefix (System/getenv "WAITER_TEST_PREFIX")]
     (str/replace (str test-prefix (extract-acronym service-name) (System/nanoTime)) #"-" ""))))

(defn- token->etag
  "Returns the current etag of a token"
  [waiter-url token]
  (log/info "retrieving etag for token" token)
  (let [response (make-request waiter-url "/token"
                               :headers {"x-waiter-token" token}
                               :query-params {"include" "deleted"})]
    (get-in response [:headers "etag"])))

(defn- attach-token-etag
  "Attaches the if-match etag to the headers"
  [waiter-url token headers]
  (let [last-modified-etag (token->etag waiter-url token)]
    (assoc headers "if-match" last-modified-etag)))

(defn delete-token-and-assert
  "Deletes and token and asserts that the delete was successful."
  [waiter-url token & {:keys [hard-delete headers] :or {hard-delete true}}]
  (log/info "deleting token" token {:hard-delete hard-delete})
  (let [headers (cond->> headers
                  (and hard-delete (nil? headers)) (attach-token-etag waiter-url token))
        response (make-request waiter-url "/token"
                               :headers (assoc headers "host" token)
                               :method :delete
                               :query-params (if hard-delete {"hard-delete" true} {}))]
    (assert-response-status response 200)))

(defn wait-for
  "Invoke predicate every interval (default 10) seconds until it returns true,
   or timeout (default 150) seconds have elapsed. E.g.:
       (wait-for #(< (rand) 0.2) :interval 1 :timeout 10)
   Returns nil if the timeout elapses before the predicate becomes true, otherwise
   the value of the predicate on its last evaluation."
  [predicate & {:keys [interval timeout unit-multiplier]
                :or {interval 10
                     timeout 150
                     unit-multiplier 1000}}]
  (let [end-time (+ (System/currentTimeMillis) (* timeout unit-multiplier))]
    (loop []
      (if-let [result (predicate)]
        result
        (do
          (Thread/sleep (* interval unit-multiplier))
          (if (< (System/currentTimeMillis) end-time)
            (recur)))))))

(defn service-id->service-description
  [waiter-url service-id]
  (let [{:keys [service-description]} (service-settings waiter-url service-id)]
    (log/debug "service description for" service-id "is" service-description)
    service-description))

(defn response->service-description
  [waiter-url response]
  (service-id->service-description waiter-url (:service-id response)))

(defn- statsd-host []
  (System/getenv "WAITER_STATSD_HOST"))

(defn- statsd-port []
  (System/getenv "WAITER_STATSD_PORT"))

(defn- trim-port [url]
  (str/replace url #"\:[0-9]+$" ""))

(deftest test-trim-port
  (is (= "foo.bar.baz" (trim-port "foo.bar.baz:1234")))
  (is (= "foo.bar.baz" (trim-port "foo.bar.baz"))))

(let [git-branch (retrieve-git-branch)
      _ (log/info "Git branch =" git-branch)]
  (defn- statsd-metric-path [test-name waiter-url metric]
    (let [waiter-under-test (statsd/sanitize (trim-port waiter-url))]
      (str test-name "." git-branch "." waiter-under-test "." metric))))

(defn statsd-test-fixture
  "Calls the clj-statsd setup function prior to calling f. Use this when
  you want to send statsd metrics from tests in a namespace, e.g.:

      (use-fixtures :once statsd-test-fixture)

   Then, from individual tests in the same namespace, you can, for example:

      (statsd-timing test-name waiter-url metric 123)

   Which results in the metric being sent to statsd."
  [f]
  (let [host (statsd-host)
        port (statsd-port)]
    (if (and host port)
      (statsd/init-configuration host port :prefix "waiter_tests.")
      (log/info "Statsd is not initialized")))
  (f))

(deftest test-statsd-test-fixture
  (testing "Statsd test fixture invocation"
    (testing "should not initialize statsd if environment variables are not set"
      (with-redefs [statsd-host (constantly nil)
                    statsd-port (constantly nil)
                    statsd/init-configuration (fn [_ _ _ _] (throw (Exception.)))]
        (statsd-test-fixture #())))))

(defn statsd-timing
  "Make sure to use the statsd-test-fixture in namespaces where you wish to call this, e.g.:

      (use-fixtures :once statsd-test-fixture)

   Then, from individual tests in the same namespace, you can, for example:

      (statsd-timing test-name waiter-url metric 123)

   Which results in the metric being sent to statsd."
  [test-name waiter-url metric value]
  (let [metric-path #(statsd-metric-path test-name waiter-url metric)
        int-val (int value)]
    (log/debug "statsd timing:" (metric-path) int-val)
    (statsd/timing metric-path int-val)))

(defn all-cookies
  "Retrieves all cookies from the /waiter-auth endpoint"
  [waiter-url]
  (:cookies (make-request waiter-url "/waiter-auth")))

(defn auth-cookie
  "Retrieves and returns the value of the x-waiter-auth cookie"
  [waiter-url]
  (:value (first (filter #(= "x-waiter-auth" (:name %)) (all-cookies waiter-url)))))

(defn statsd-state
  "Fetches and returns the statsd state."
  [waiter-url & {:keys [cookies] :or {cookies {}}}]
  (retrieve-state-helper waiter-url "/state/statsd" :cookies cookies))

(defn router-statsd-state
  "Fetches and returns the statsd state for a specific router-id."
  [waiter-url router-id]
  (let [router-endpoint (router-endpoint waiter-url router-id)
        cookies (all-cookies waiter-url)
        router-state (statsd-state router-endpoint :cookies cookies)]
    (get router-state "state")))

(defn router-service-state
  "Fetches and returns the service state from a particular router url"
  [router-url service-id cookies]
  (let [state-json (:body (make-request router-url (str "/state/" service-id) :cookies cookies))]
    (log/debug "State received from" router-url ":" state-json)
    (try-parse-json state-json)))

(defn service
  "Retrieves the service (from /apps) corresponding to the provided service-id"
  [waiter-url service-id query-params & {:keys [interval timeout] :or {interval 2, timeout 30}}]
  ; allow time for router to receive updates from marathon
  (wait-for
    (fn []
      (let [{:keys [body]} (make-request waiter-url "/apps" :query-params query-params)
            _ (log/debug "Response body:" body)
            parsed-body (try-parse-json body)
            service (first (filter #(= service-id (get % "service-id")) parsed-body))]
        (when-not service
          (log/info "Service" service-id "is missing! Response:" body))
        service))
    :interval interval
    :timeout timeout))

(defn service-id->metric-group
  "Retrieves the metric-group corresponding to the provided service-id"
  [waiter-url service-id]
  (get-in (service waiter-url service-id {"effective-parameters" "true"})
          ["effective-parameters" "metric-group"]))

(defn retrieve-debug-response-headers
  [waiter-url]
  (let [settings (waiter-settings waiter-url)
        mesos-slave-port (get-in settings [:scheduler-config :mesos-slave-port])
        slave-directory (get-in settings [:scheduler-config :slave-directory])]
    (cond-> ["x-waiter-backend-id" "x-waiter-backend-host" "x-waiter-backend-port" "x-waiter-backend-proto"
             "x-waiter-backend-response-ns" "x-waiter-get-available-instance-ns" "x-waiter-router-id"]
      (and mesos-slave-port slave-directory)
      (concat ["x-waiter-backend-directory" "x-waiter-backend-log-url"]))))

(defn rand-router-url
  "Returns a random router url from the routers in the specified cluster"
  [waiter-url]
  (let [routers (routers waiter-url)
        router (-> routers (keys) (rand-nth))
        target-url (routers router)]
    target-url))

(defn some-router-id-with-assigned-slots
  "Returns the router-id of a router with slots assigned to the given service-id"
  [waiter-url service-id]
  {:pre [(not (str/blank? waiter-url))]
   :post [%]}
  (let [routers (routers waiter-url)
        settings (service-settings waiter-url service-id)
        assigned? (fn [router-id]
                    (let [slots (get-in
                                  settings
                                  [:metrics :routers (keyword router-id) :counters :instance-counts :slots-assigned])]
                      (when (and slots (pos? slots))
                        router-id)))
        router-id (->> routers keys (some assigned?))]
    (log/debug "router id with slots assigned:" router-id)
    router-id))

(defn some-router-url-with-assigned-slots
  "Returns the URL of a router with slots assigned to the given service-id"
  [waiter-url service-id]
  {:pre [(not (str/blank? waiter-url))]
   :post [%]}
  (let [router-id (some-router-id-with-assigned-slots waiter-url service-id)
        router-url (router-endpoint waiter-url router-id)]
    (log/debug "router url with slots assigned:" router-url)
    router-url))

(defn scheduler-kind
  "Returns the configured :scheduler-config :kind"
  [waiter-url & {:keys [verbose] :or {verbose false}}]
  (setting waiter-url [:scheduler-config :kind] :verbose verbose))

(defn service-id->grace-period
  "Fetches from Marathon and returns the grace period in seconds for the given service"
  [waiter-url service-id]
  (let [marathon-url (marathon-url waiter-url)
        app-info-path (str "/v2/apps/" service-id)
        app-info-response (make-request marathon-url app-info-path)
        app-info-map (walk/keywordize-keys (try-parse-json (:body app-info-response)))]
    (:gracePeriodSeconds (first (:healthChecks (:app app-info-map))))))

(defn using-cook?
  "Returns true if Waiter is configured to use Cook for scheduling"
  [waiter-url]
  (= "cook" (retrieve-default-scheduler-name waiter-url)))

(defn using-k8s?
  "Returns true if Waiter is configured to use Kubernetes for scheduling"
  [waiter-url]
  (= "kubernetes" (retrieve-default-scheduler-name waiter-url)))

(defn using-marathon?
  "Returns true if Waiter is configured to use Marathon for scheduling"
  [waiter-url]
  (= "marathon" (retrieve-default-scheduler-name waiter-url)))

(defn using-shell?
  "Returns true if Waiter is configured to use Shell Scheduler for scheduling"
  [waiter-url]
  (= "shell" (retrieve-default-scheduler-name waiter-url)))

(defn get-authenticator-kind
  "Get the authenticator that Waiter is configured to use"
  [waiter-url]
  (get-in (waiter-settings waiter-url) [:authenticator-config :kind]))

(defn using-composite-authenticator?
  "Returns true if Waiter is configured to use the composite authenticator"
  [waiter-url]
  (= "composite" (get-authenticator-kind waiter-url)))

(defn using-saml-authenticator?
  "Returns true if Waiter is configured to use the saml authenticator"
  [waiter-url]
  (= "saml" (get-authenticator-kind waiter-url)))

(defn supports-saml-authentication?
  "Returns true if Waiter is configured to support saml authentication"
  [waiter-url]
  (or (using-saml-authenticator? waiter-url)
      (and (using-composite-authenticator? waiter-url)
           (get-in (waiter-settings waiter-url) [:authenticator-config :composite :authentication-providers :saml]))))

;; TODO: make this work not just for marathon
(defn can-query-for-grace-period?
  "Returns true if Waiter supports querying for grace period"
  [waiter-url]
  (using-marathon? waiter-url))

(defn post-token
  "Sends a POST request with the given token definition"
  [waiter-url {:keys [token] :as token-map} &
   {:keys [headers query-params] :or {headers {}, query-params {}}}]
  (make-request waiter-url "/token"
                :body (utils/clj->json token-map)
                :headers (assoc headers "host" token)
                :method :post
                :query-params query-params))

(defn get-token
  "Gets the token with the given name"
  [waiter-url token & {:keys [cookies query-params request-headers] :or
                       {cookies {}, query-params {"include" "metadata"}}}]
  (let [request-headers (or request-headers {"host" token})
        token-response (make-request waiter-url "/token"
                                     :cookies cookies
                                     :headers request-headers
                                     :query-params query-params)]
    (log/debug "retrieved token" token ":" (:body token-response))
    token-response))

(defmacro with-service-cleanup
  "Ensures a service is cleaned up."
  [service-id & body]
  `(try
     ~@body
     (finally
       (delete-service ~'waiter-url ~service-id))))

(defn response->service-id
  "Gets the service-id from a response."
  [{:keys [headers]}]
  (get headers "x-waiter-service-id"))

(defn service-id->last-request-time
  [waiter-url service-id]
  (when-let [last-request-time-str (-> (service waiter-url service-id {})
                                       (get "last-request-time"))]
    (du/str-to-date last-request-time-str)))

(defn retrieve-services-on-router
  "Returns the service-ids of services known to be running at specified router."
  [router-url & {:keys [cookies] :or {cookies {}}}]
  (some->> (make-request router-url "/apps" :cookies cookies)
    :body
    json/read-str
    walk/keywordize-keys
    (map :service-id)
    set))

(defn active-instances
  "Returns the active instances for the given service-id"
  [waiter-url service-id & {:keys [cookies] :or {cookies {}}}]
  (get-in (service-settings waiter-url service-id :cookies cookies)
          [:instances :active-instances]))

(defn killed-instances
  "Returns the killed instances for the given service-id"
  [waiter-url service-id & {:keys [cookies] :or {cookies {}}}]
  (get-in (service-settings waiter-url service-id :cookies cookies)
          [:instances :killed-instances]))

(defn num-instances
  "Returns the number of active instances for the given service-id"
  [waiter-url service-id & {:keys [cookies] :or {cookies {}}}]
  (let [instances (count (active-instances waiter-url service-id :cookies cookies))]
    (log/debug service-id "has" instances "instances.")
    instances))

(defmacro assert-service-not-on-any-routers
  [waiter-url service-id cookies]
  `(let [service-id# ~service-id
         cookies# ~cookies]
     (doseq [[_# router-url#] (routers ~waiter-url)]
       (is (wait-for #(not (contains? (retrieve-services-on-router router-url# :cookies cookies#) service-id#)))))))

(defmacro assert-service-on-all-routers
  [waiter-url service-id cookies]
  `(let [service-id# ~service-id
         cookies# ~cookies]
     (doseq [[_# router-url#] (routers ~waiter-url)]
       (is (wait-for #(seq (active-instances router-url# service-id# :cookies cookies#)))))))

(defn make-chunked-body
  "Returns a channel that receives chunks from the input body string.
   Introducing the delay affects the jetty behavior of converting chunked encoding to non-chunked."
  [body-str chunk-size chunk-delay-ms]
  (let [body-ch (async/chan 1024)
        input-stream (ByteArrayInputStream. (.getBytes (str body-str)))]
    (async/go
      (loop []
        (let [buffer-bytes (byte-array chunk-size)
              bytes-read (.read input-stream buffer-bytes)]
          (when (pos? bytes-read)
            (async/put! body-ch (ByteBuffer/wrap buffer-bytes 0 bytes-read))
            (async/<! (async/timeout chunk-delay-ms))
            (recur))))
      (async/close! body-ch))
    body-ch))
