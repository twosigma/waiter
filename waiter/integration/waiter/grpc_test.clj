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
(ns waiter.grpc-test
  (:require [clojure.core.async :as async]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [waiter.correlation-id :as cid]
            [waiter.util.client-tools :refer :all])
  (:import (com.twosigma.waiter.courier
             CourierReply CourierSummary GrpcClient
             GrpcClient$CancellationPolicy GrpcClient$RpcResult StateReply)
           (io.grpc Status)
           (java.util.concurrent CountDownLatch)
           (java.util.function Function)))

(def cancel-policy-none GrpcClient$CancellationPolicy/NONE)

(def cancel-policy-context GrpcClient$CancellationPolicy/CONTEXT)

(def cancel-policy-exception GrpcClient$CancellationPolicy/EXCEPTION)

(def cancel-policy-observer GrpcClient$CancellationPolicy/OBSERVER)

(defn- initialize-grpc-client
  "Initializes grpc client logging to specific correlation id"
  [correlation-id host port]
  (let [log-function (reify Function
                       (apply [_ message]
                         (cid/with-correlation-id
                           correlation-id
                           (log/info message))))]
    (GrpcClient. host port log-function)))

(defn- basic-grpc-service-parameters
  [& {:keys [prefix] :or {prefix "x-waiter-"}}]
  (let [courier-command (courier-server-command "${PORT0} ${PORT1}")]
    (pc/map-keys
      #(str prefix %)
      {"backend-proto" "h2c"
       "cmd" courier-command
       "cmd-type" "shell"
       "concurrency-level" 32
       "cpus" 0.2
       "debug" true
       "grace-period-secs" 120
       "health-check-port-index" 1
       "health-check-proto" "http"
       "idle-timeout-mins" 10
       "max-instances" 1
       "min-instances" 1
       "mem" 512
       "name" (rand-name)
       "ports" 2
       "version" "version-does-not-matter"})))

(defn- rand-str
  "Generates a random string with the specified length."
  [length]
  (apply str (take length (repeatedly #(char (+ (rand 26) 65))))))

(defn ping-courier-service
  [waiter-url request-headers]
  (make-request waiter-url "/waiter-ping" :headers request-headers))

(defn start-courier-instance
  ([waiter-url]
   (let [request-headers (basic-grpc-service-parameters)]
     (start-courier-instance waiter-url request-headers)))
  ([waiter-url request-headers]
   (let [[host _] (str/split waiter-url #":")
         h2c-port (Integer/parseInt (retrieve-grpc-cleartext-port waiter-url))
         {:keys [cookies headers] :as response} (ping-courier-service waiter-url request-headers)
         cookie-header (str/join "; " (map #(str (:name %) "=" (:value %)) cookies))
         service-id (get headers "x-waiter-service-id")
         request-headers (assoc request-headers
                           "cookie" cookie-header
                           "x-waiter-timeout" "60000")]
     (assert-response-status response 200)
     (log/info "ping cid:" (get headers "x-cid"))
     (log/info "service-id:" service-id)
     (is service-id)
     (when-not service-id
       ;; abort test eagerly when service id is unavailable
       (throw (ex-info "missing service-id in response" {:response response})))
     (let [{:keys [ping-response service-state]} (some-> response :body try-parse-json walk/keywordize-keys)]
       (is (= "received-response" (:result ping-response)) (str ping-response))
       (is (= "OK" (some-> ping-response :body)) (str ping-response))
       (is (str/starts-with? (str (some-> ping-response :headers :server)) "courier-health-check") (str ping-response))
       (assert-response-status ping-response 200)
       (is (true? (:exists? service-state)) (str service-state))
       (is (= service-id (:service-id service-state)) (str service-state))
       (is (contains? #{"Running" "Starting"} (:status service-state)) (str service-state)))
     (assert-service-on-all-routers waiter-url service-id cookies)

     {:h2c-port h2c-port
      :host host
      :request-headers request-headers
      :service-id service-id})))

(defmacro assert-grpc-ok-status
  "Asserts that the status represents a grpc OK status."
  [status assertion-message]
  `(let [status# ~status
         assertion-message# ~assertion-message]
     (is status# assertion-message#)
     (when status#
       (is (= "OK" (-> status# .getCode str)) assertion-message#)
       (is (str/blank? (.getDescription status#)) assertion-message#))))

(defmacro assert-grpc-cancel-status
  "Asserts that the status represents a grpc OK status."
  [status message assertion-message]
  `(let [status# ~status
         message# ~message
         assertion-message# ~assertion-message]
     (is status# assertion-message#)
     (when status#
       (is (= "CANCELLED" (-> status# .getCode str)) assertion-message#)
       (is (= message# (.getDescription status#)) assertion-message#))))

(defmacro assert-grpc-deadline-exceeded-status
  "Asserts that the status represents a grpc OK status."
  [status assertion-message]
  `(let [status# ~status
         assertion-message# ~assertion-message]
     (is status# assertion-message#)
     (when status#
       (is (= "DEADLINE_EXCEEDED" (-> status# .getCode str)) assertion-message#)
       (is (str/includes? (.getDescription status#) "deadline exceeded after") assertion-message#))))

(defmacro assert-grpc-server-exit-status
  "Asserts that the status represents a grpc OK status."
  [status assertion-message]
  `(let [status# ~status
         assertion-message# ~assertion-message]
     (is status# assertion-message#)
     (when status#
       (is (contains? #{"CANCELLED" "INTERNAL" "UNAVAILABLE"} (-> status# .getCode str)) assertion-message#))))

(defmacro assert-grpc-unknown-status
  "Asserts that the status represents a grpc OK status."
  [status message assertion-message]
  `(let [status# ~status
         message# ~message
         assertion-message# ~assertion-message]
     (is status# assertion-message#)
     (when status#
       (is (= "UNKNOWN" (-> status# .getCode str)) assertion-message#)
       (when message#
         (is (= message# (.getDescription status#)) assertion-message#)))))

(defmacro assert-grpc-status
  "Asserts that the status represents a grpc status."
  [status code message-substring assertion-message]
  `(let [status# ~status
         code# ~code
         message-substring# ~message-substring
         assertion-message# ~assertion-message]
     (is status# assertion-message#)
     (when status#
       (is (= code# (-> status# .getCode str)) assertion-message#)
       (when message-substring#
         (is (str/includes? (.getDescription status#) message-substring#) assertion-message#)))))

(defn- count-items
  [xs x]
  (count (filter #(= x %) xs)))

(defn retrieve-request-state
  "Retrieves the request state for the specified correlation-id.
   When timeout is provided, retries for specified interval until state contains CLOSED."
  ([grpc-client request-headers query-correlation-id]
   (let [state-correlation-id (rand-name)
         state-request-headers (assoc request-headers "x-cid" state-correlation-id)]
     (.retrieveState grpc-client state-request-headers query-correlation-id)))
  ([grpc-client request-headers query-correlation-id timeout-secs]
   (or (wait-for
         (fn retrieve-closed-request-state []
           (when-let [^GrpcClient$RpcResult rpc-result
                      (retrieve-request-state grpc-client request-headers query-correlation-id)]
             (log/info "retrieve-request-state:" query-correlation-id
                       {:result (.result rpc-result) :status (.status rpc-result)})
             (let [^StateReply reply (.result rpc-result)
                   states (some-> reply .getStateList seq)]
               (log/info "retrieve-request-state:" query-correlation-id
                         {:cid (some-> reply .getCid) :state (some-> reply .getStateList)})
               (when (some #(= "CLOSE" %) states)
                 rpc-result))))
         :interval 1 :timeout timeout-secs)
       (retrieve-request-state grpc-client request-headers query-correlation-id))))

(defn assert-request-state
  "Asserts the states on the cid of a previous grpc call."
  [grpc-client request-headers service-id query-correlation-id mode]
  (Thread/sleep 1000) ;; sleep to allow completion of grpc server-side internal events
  (let [timeout-secs 10
        rpc-result (retrieve-request-state grpc-client request-headers query-correlation-id timeout-secs)
        ^StateReply reply (.result rpc-result)
        ^Status status (.status rpc-result)
        assertion-message (->> (cond-> {:correlation-id query-correlation-id
                                        :mode (name mode)
                                        :service-id service-id}
                                 reply (assoc :reply {:cid (.getCid reply)
                                                      :state (seq (.getStateList reply))})
                                 status (assoc :status {:code (-> status .getCode str)
                                                        :description (.getDescription status)}))
                            (into (sorted-map))
                            str)]
    (is status assertion-message)
    (assert-grpc-ok-status status assertion-message)
    (is reply assertion-message)
    (is (= query-correlation-id (.getCid reply)) assertion-message)
    (let [states (seq (.getStateList reply))]
      (cond
        (contains? #{::client-cancel ::deadline-exceeded} mode)
        (let [states-middle (set (drop-last 3 (drop 2 states)))]
          (is (= "INIT" (first states)) assertion-message)
          (is (<= (count-items (take 2 states) "READY") 1) assertion-message)
          (when (pos? (count-items states "SEND_MESSAGE"))
            (is (= (count-items states "SEND_HEADERS") 1) assertion-message))
          (is (every? #{"HALF_CLOSE" "RECEIVE_MESSAGE" "SEND_MESSAGE" "SEND_HEADERS"} states-middle) assertion-message)
          (is (<= (count-items states "HALF_CLOSE") 1) assertion-message)
          (is (= (count-items (take-last 3 states) "CANCEL") 1) assertion-message)
          (is (= (count-items (take-last 3 states) "CANCEL_HANDLER") 1) assertion-message)
          (is (= (count-items (take-last 3 states) "CLOSE") 1) assertion-message))
        (= ::server-cancel mode)
        (do
          (is (= ["INIT" "READY"] (take 2 states)) assertion-message)
          (is (pos? (count-items states "RECEIVE_MESSAGE")) assertion-message)
          (is (<= (count-items states "SEND_HEADERS") 1) assertion-message)
          (is (<= (count-items states "HALF_CLOSE") 1) assertion-message)
          (is (= (count-items states "CLOSE") 1) assertion-message)
          (is (<= (count-items states "COMPLETE") 1) assertion-message))
        (= ::success mode)
        (do
          (is (= ["INIT" "READY"] (take 2 states)) assertion-message)
          (is (pos? (count-items states "RECEIVE_MESSAGE")) assertion-message)
          (is (= (count-items states "SEND_HEADERS") 1) assertion-message)
          (is (pos? (count-items states "SEND_MESSAGE")) assertion-message)
          (is (= (count-items states "HALF_CLOSE") 1) assertion-message)
          (is (= ["CLOSE" "COMPLETE"] (take-last 2 states)) assertion-message))))))

(deftest ^:parallel ^:integration-slow test-grpc-unary-call
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
          id (rand-name "m")
          from (rand-name "f")
          content (rand-str 1000)
          grpc-result->assertion-message (fn [correlation-id rpc-result]
                                           (let [^CourierReply reply (.result rpc-result)
                                                 ^Status status (.status rpc-result)]
                                             (->> (cond-> {:correlation-id correlation-id
                                                           :service-id service-id}
                                                    reply (assoc :reply {:id (.getId reply)
                                                                         :response (.getResponse reply)})
                                                    status (assoc :status {:code (-> status .getCode str)
                                                                           :description (.getDescription status)}))
                                               (into (sorted-map))
                                               str)))]
      (with-service-cleanup
        service-id
        (testing "small request and reply"
          (log/info "starting small request and reply test")
          (let [correlation-id (rand-name)
                request-headers (assoc request-headers "x-cid" correlation-id)
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                sleep-ms 1000
                deadline-ms 10000
                rpc-result (.sendPackage grpc-client request-headers id from content sleep-ms deadline-ms)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (grpc-result->assertion-message correlation-id rpc-result)]
            (assert-grpc-ok-status status assertion-message)
            (is reply assertion-message)
            (when reply
              (is (= id (.getId reply)) assertion-message)
              (is (= content (.getMessage reply)) assertion-message)
              (is (= "received" (.getResponse reply)) assertion-message))
            (assert-request-state grpc-client request-headers service-id correlation-id ::success)))

        (testing "long request and reply"
          (log/info "starting long request and reply test")
          (let [correlation-id (rand-name)
                idle-timeout-config (setting waiter-url [:instance-request-properties :client-connection-idle-timeout-ms])
                idle-timeout-request (+ 10000 (* 2 idle-timeout-config))
                sleep-ms (+ 2000 idle-timeout-config)
                deadline-ms (+ 10000 sleep-ms)
                request-headers (assoc request-headers
                                  "x-cid" correlation-id
                                  "x-waiter-timeout" idle-timeout-request
                                  "x-waiter-streaming-timeout" idle-timeout-request)
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                rpc-result (.sendPackage grpc-client request-headers id from content sleep-ms deadline-ms)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (grpc-result->assertion-message correlation-id rpc-result)]
            (assert-grpc-ok-status status assertion-message)
            (is reply assertion-message)
            (when reply
              (is (= id (.getId reply)) assertion-message)
              (is (= content (.getMessage reply)) assertion-message)
              (is (= "received" (.getResponse reply)) assertion-message))
            (assert-request-state grpc-client request-headers service-id correlation-id ::success)))

        (testing "waiter error - invalid parameters"
          (log/info "starting waiter error test")
          (let [correlation-id (rand-name)
                request-headers (assoc request-headers
                                  "x-cid" correlation-id
                                  "x-waiter-concurrency-level" "invalid-value")
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                rpc-result (.sendPackage grpc-client request-headers id from content 1000 10000)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (grpc-result->assertion-message correlation-id rpc-result)]
            (assert-grpc-status status "INVALID_ARGUMENT" "concurrency-level must be an integer in the range [1, 10000]" assertion-message)
            (is (nil? reply) assertion-message)))

        (testing "waiter error - response timeout"
          (log/info "starting waiter error test")
          (let [correlation-id (rand-name)
                request-headers (assoc request-headers
                                  "x-cid" correlation-id
                                  "x-waiter-timeout" "1000")
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                rpc-result (.sendPackage grpc-client request-headers id from content 5000 10000)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (grpc-result->assertion-message correlation-id rpc-result)]
            (assert-grpc-status status "DEADLINE_EXCEEDED" "Request to service backend timed out" assertion-message)
            (is (nil? reply) assertion-message)))))))

(deftest ^:parallel ^:integration-fast test-grpc-unary-call-deadline-exceeded
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (testing "deadline exceeded"
          (let [id (rand-name "m")
                from (rand-name "f")
                content (rand-str 1000)
                correlation-id (rand-name)
                request-headers (assoc request-headers "x-cid" correlation-id)
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                sleep-duration-latch (CountDownLatch. 1)
                sleep-duration-ms 5000
                deadline-duration-ms (- sleep-duration-ms 1000)
                _ (async/go
                    (async/<! (async/timeout (+ sleep-duration-ms 1000)))
                    (.countDown sleep-duration-latch))
                rpc-result (.sendPackage grpc-client request-headers id from content sleep-duration-ms deadline-duration-ms)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (->> (cond-> {:correlation-id correlation-id
                                                :service-id service-id}
                                         reply (assoc :reply {:id (.getId reply)
                                                              :response (.getResponse reply)})
                                         status (assoc :status {:code (-> status .getCode str)
                                                                :description (.getDescription status)}))
                                    (into (sorted-map))
                                    str)]
            (assert-grpc-deadline-exceeded-status status assertion-message)
            (is (nil? reply) assertion-message)
            (.await sleep-duration-latch)
            (when (grpc-cancellations-supported? waiter-url)
              (assert-request-state grpc-client request-headers service-id correlation-id ::deadline-exceeded))))))))

(deftest ^:parallel ^:integration-fast test-grpc-unary-call-server-cancellation
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (testing "small request and reply"
          (log/info "starting small request and reply test")
          (let [id (str (rand-name "m") ".SEND_ERROR")
                from (rand-name "f")
                content (rand-str 1000)
                correlation-id (rand-name)
                _ (log/info "cid:" correlation-id)
                request-headers (assoc request-headers "x-cid" correlation-id)
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                rpc-result (.sendPackage grpc-client request-headers id from content 1000 10000)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (->> (cond-> {:correlation-id correlation-id
                                                :service-id service-id}
                                         reply (assoc :reply {:id (.getId reply)
                                                              :response (.getResponse reply)})
                                         status (assoc :status {:code (-> status .getCode str)
                                                                :description (.getDescription status)}))
                                    (into (sorted-map))
                                    str)]
            (is (nil? reply) assertion-message)
            (assert-grpc-cancel-status status "Cancelled by server" assertion-message)
            (assert-request-state grpc-client request-headers service-id correlation-id ::server-cancel)))))))

(deftest ^:parallel ^:integration-fast test-grpc-unary-call-server-exit
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (testing "small request and reply"
          (log/info "starting small request and reply test")
          (let [id (str (rand-name "m") ".EXIT_PRE_RESPONSE")
                from (rand-name "f")
                content (rand-str 1000)
                correlation-id (rand-name)
                _ (log/info "cid:" correlation-id)
                request-headers (assoc request-headers "x-cid" correlation-id)
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                rpc-result (.sendPackage grpc-client request-headers id from content 1000 10000)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (->> (cond-> {:correlation-id correlation-id
                                                :service-id service-id}
                                         reply (assoc :reply {:id (.getId reply)
                                                              :response (.getResponse reply)})
                                         status (assoc :status {:code (-> status .getCode str)
                                                                :description (.getDescription status)}))
                                    (into (sorted-map))
                                    str)]
            (assert-grpc-server-exit-status status assertion-message)
            (is (nil? reply) assertion-message)))))))

(deftest ^:parallel ^:integration-fast test-grpc-bidi-streaming-successful
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
          correlation-id-prefix (rand-name)]
      (with-service-cleanup
        service-id
        (doseq [max-message-length [1000 50000]]
          (let [num-messages 120
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

            (testing (str "independent mode " max-message-length " messages completion")
              (log/info "starting streaming to and from server - independent mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    correlation-id (str correlation-id-prefix "-in-" max-message-length)
                    request-headers (assoc request-headers "x-cid" correlation-id)
                    ids (map #(str "id-inde-" %) (range num-messages))
                    grpc-client (initialize-grpc-client correlation-id host h2c-port)
                    rpc-result (.collectPackages grpc-client request-headers ids from messages 10 false
                                                 cancel-threshold cancel-policy-none 60000)
                    summaries (.result rpc-result)
                    ^Status status (.status rpc-result)
                    assertion-message (->> (cond-> {:correlation-id correlation-id
                                                    :service-id service-id
                                                    :summaries (map (fn [^CourierSummary s]
                                                                      {:num-messages (.getNumMessages s)
                                                                       :total-length (.getTotalLength s)})
                                                                    summaries)}
                                             status (assoc :status {:code (-> status .getCode str)
                                                                    :description (.getDescription status)}))
                                        (into (sorted-map))
                                        str)]
                (log/info correlation-id "collecting independent packages...")
                (assert-grpc-ok-status status assertion-message)
                (is (= (count messages) (count summaries)) assertion-message)
                (when (seq summaries)
                  (is (= (range 1 (inc (count messages))) (map #(.getNumMessages ^CourierSummary %) summaries))
                      assertion-message)
                  (is (= (reductions + (map count messages)) (map #(.getTotalLength ^CourierSummary %) summaries))
                      assertion-message))
                (assert-request-state grpc-client request-headers service-id correlation-id ::success)))

            (testing (str "lock-step mode " max-message-length " messages completion")
              (log/info "starting streaming to and from server - lock-step mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    correlation-id (str correlation-id-prefix "-ls-" max-message-length)
                    request-headers (assoc request-headers "x-cid" correlation-id)
                    ids (map #(str "id-lock-" %) (range num-messages))
                    grpc-client (initialize-grpc-client correlation-id host h2c-port)
                    rpc-result (.collectPackages grpc-client request-headers ids from messages 1 true
                                                 cancel-threshold cancel-policy-none 60000)
                    summaries (.result rpc-result)
                    ^Status status (.status rpc-result)
                    assertion-message (->> (cond-> {:correlation-id correlation-id
                                                    :service-id service-id
                                                    :summaries (map (fn [^CourierSummary s]
                                                                      {:num-messages (.getNumMessages s)
                                                                       :total-length (.getTotalLength s)})
                                                                    summaries)}
                                             status (assoc :status {:code (-> status .getCode str)
                                                                    :description (.getDescription status)}))
                                        (into (sorted-map))
                                        str)]
                (log/info correlation-id "collecting lock-step packages...")
                (assert-grpc-ok-status status assertion-message)
                (is (= (count messages) (count summaries)) assertion-message)
                (when (seq summaries)
                  (is (= (range 1 (inc (count messages))) (map #(.getNumMessages ^CourierSummary %) summaries))
                      assertion-message)
                  (is (= (reductions + (map count messages)) (map #(.getTotalLength ^CourierSummary %) summaries))
                      assertion-message))
                (assert-request-state grpc-client request-headers service-id correlation-id ::success)))))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-grpc-bidi-streaming-multiple-services
  (testing-using-waiter-url
    (let [correlation-id-prefix (rand-name)
          current-user (retrieve-username)
          basic-service-parameters (-> (basic-grpc-service-parameters :prefix "")
                                     (dissoc "debug" "name")
                                     (assoc "permitted-user" current-user
                                            "run-as-user" current-user))
          num-tokens-to-create 5
          tokens-to-create (map #(str "token" %1 "." correlation-id-prefix) (range num-tokens-to-create))
          token->service-id-atom (atom {})
          host-atom (atom nil)
          port-atom (atom nil)]
      (try
        (doseq [token tokens-to-create]
          (let [token-parameters (assoc basic-service-parameters
                                   "name" token
                                   "token" token)
                _ (log/info "creating token:" token)
                post-response (post-token waiter-url (walk/keywordize-keys token-parameters))
                _ (assert-response-status post-response 200)
                request-headers {"host" token
                                 "x-waiter-debug" true}
                {:keys [h2c-port host service-id]} (start-courier-instance waiter-url request-headers)]
            (log/info "started service:" service-id)
            (is service-id)
            (when service-id (swap! token->service-id-atom assoc token service-id))
            (when host (reset! host-atom host))
            (when h2c-port (reset! port-atom h2c-port))))
        (is @host-atom)
        (is @port-atom)
        (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
              cookie-header (str/join "; " (map #(str (:name %) "=" (:value %)) cookies))
              max-message-length 50000
              task-index-atom (atom -1)
              host @host-atom
              h2c-port @port-atom]
          (parallelize-requests
            num-tokens-to-create 1
            (fn test-grpc-bidi-streaming-multiple-services-task []
              (testing (str "independent mode " max-message-length " messages completion")
                (let [task-index (swap! task-index-atom inc)
                      token (nth tokens-to-create task-index)
                      service-id (get @token->service-id-atom token)
                      _ (log/info "starting streaming to and from server" {:service-id service-id :token token})
                      num-messages (+ 50 (* task-index 20))
                      client-cancellation? (= 2 (mod task-index 2))
                      cancel-threshold (if client-cancellation? (/ num-messages 2) (inc num-messages))
                      from (rand-name "f")
                      correlation-id (str correlation-id-prefix "-in-" max-message-length)
                      messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))
                      request-headers {"cookie" cookie-header
                                       "x-cid" correlation-id
                                       "x-waiter-debug" true
                                       "x-waiter-timeout" "60000"
                                       "x-waiter-token" token}
                      ids (map #(str "id-" token "-" %) (range num-messages))
                      grpc-client (initialize-grpc-client correlation-id host h2c-port)
                      _ (log/info correlation-id "collecting independent packages...")
                      rpc-result (.collectPackages grpc-client request-headers ids from messages 10 false
                                                   cancel-threshold cancel-policy-context 60000)
                      summaries (.result rpc-result)
                      ^Status status (.status rpc-result)
                      assertion-message (->> (cond-> {:correlation-id correlation-id
                                                      :service-id service-id
                                                      :summaries (map (fn [^CourierSummary s]
                                                                        {:num-messages (.getNumMessages s)
                                                                         :total-length (.getTotalLength s)})
                                                                      summaries)}
                                               status (assoc :status {:code (-> status .getCode str)
                                                                      :description (.getDescription status)}))
                                          (into (sorted-map))
                                          str)]
                  (log/info correlation-id "asserting collected packages...")
                  (assert-grpc-ok-status status assertion-message)
                  (if client-cancellation?
                    (is (<= (count summaries) (inc cancel-threshold)) assertion-message)
                    (is (= (count messages) (count summaries)) assertion-message))
                  (when (seq summaries)
                    (is (= (range 1 (inc (count messages))) (map #(.getNumMessages ^CourierSummary %) summaries))
                        assertion-message)
                    (is (= (reductions + (map count messages)) (map #(.getTotalLength ^CourierSummary %) summaries))
                        assertion-message))
                  (if client-cancellation?
                    (assert-request-state grpc-client request-headers service-id correlation-id ::client-cancel)
                    (assert-request-state grpc-client request-headers service-id correlation-id ::success)))))))
        (finally
          (doseq [service-id (vals @token->service-id-atom)]
            (log/info "deleting service" service-id)
            (delete-service waiter-url service-id))
          (doseq [token tokens-to-create]
            (log/info "deleting token" token)
            (delete-token-and-assert waiter-url token)))))))

(deftest ^:parallel ^:integration-slow test-grpc-bidi-streaming-client-cancellation
  (testing-using-waiter-url
    (when (grpc-cancellations-supported? waiter-url)
      (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
            correlation-id-prefix (rand-name)]
        (with-service-cleanup
          service-id
          (doseq [cancel-policy [cancel-policy-context cancel-policy-exception cancel-policy-observer]]
            (doseq [max-message-length [1000 50000]]
              (let [num-messages 120
                    messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

                (testing (str "independent mode " max-message-length " messages completion " cancel-policy)
                  (log/info "starting streaming to and from server - independent mode test")
                  (let [cancel-threshold (/ num-messages 2)
                        from (rand-name "f")
                        correlation-id (str correlation-id-prefix "-in-" max-message-length "-" cancel-policy)
                        request-headers (assoc request-headers "x-cid" correlation-id)
                        ids (map #(str "id-inde-" %) (range num-messages))
                        grpc-client (initialize-grpc-client correlation-id host h2c-port)
                        rpc-result (.collectPackages grpc-client request-headers ids from messages 100 false cancel-threshold cancel-policy 60000)
                        summaries (.result rpc-result)
                        ^Status status (.status rpc-result)
                        assertion-message (->> (cond-> {:correlation-id correlation-id
                                                        :service-id service-id
                                                        :summaries (map (fn [^CourierSummary s]
                                                                          {:num-messages (.getNumMessages s)
                                                                           :total-length (.getTotalLength s)})
                                                                        summaries)}
                                                 status (assoc :status {:code (-> status .getCode str)
                                                                        :description (.getDescription status)}))
                                               (into (sorted-map))
                                               str)]
                    (log/info correlation-id "collecting independent packages...")
                    (cond
                      (= cancel-policy-context cancel-policy)
                      (assert-grpc-cancel-status status "Context cancelled" assertion-message)
                      (= cancel-policy-exception cancel-policy)
                      (assert-grpc-unknown-status status nil assertion-message)
                      (= cancel-policy-observer cancel-policy)
                      (assert-grpc-unknown-status status "call was cancelled" assertion-message))
                    ;; allow for cancellation to trigger before message has been received and processed on server-side
                    (is (<= (count summaries) (inc cancel-threshold)) assertion-message)
                    (when (seq summaries)
                      (is (= (range 1 (inc (count summaries)))
                             (map #(.getNumMessages ^CourierSummary %) summaries))
                          assertion-message)
                      (is (= (reductions + (map count (take (count summaries) messages)))
                             (map #(.getTotalLength ^CourierSummary %) summaries))
                          assertion-message))
                    (assert-request-state grpc-client request-headers service-id correlation-id ::client-cancel)))

                (testing (str "lock-step mode " max-message-length " messages completion")
                  (log/info "starting streaming to and from server - lock-step mode test")
                  (let [cancel-threshold (/ num-messages 2)
                        from (rand-name "f")
                        correlation-id (str correlation-id-prefix "-ls-" max-message-length "-" cancel-policy)
                        request-headers (assoc request-headers "x-cid" correlation-id)
                        ids (map #(str "id-lock-" %) (range num-messages))
                        grpc-client (initialize-grpc-client correlation-id host h2c-port)
                        rpc-result (.collectPackages grpc-client request-headers ids from messages 100 true cancel-threshold cancel-policy 60000)
                        summaries (.result rpc-result)
                        ^Status status (.status rpc-result)
                        assertion-message (->> (cond-> {:correlation-id correlation-id
                                                        :service-id service-id
                                                        :summaries (map (fn [^CourierSummary s]
                                                                          {:num-messages (.getNumMessages s)
                                                                           :total-length (.getTotalLength s)})
                                                                        summaries)}
                                                 status (assoc :status {:code (-> status .getCode str)
                                                                        :description (.getDescription status)}))
                                               (into (sorted-map))
                                               str)]
                    (log/info correlation-id "collecting lock-step packages...")
                    (cond
                      (= cancel-policy-context cancel-policy)
                      (assert-grpc-cancel-status status "Context cancelled" assertion-message)
                      (= cancel-policy-exception cancel-policy)
                      (assert-grpc-unknown-status status nil assertion-message)
                      (= cancel-policy-observer cancel-policy)
                      (assert-grpc-unknown-status status "call was cancelled" assertion-message))
                    (is (<= (count summaries) (inc cancel-threshold)) assertion-message)
                    (when (seq summaries)
                      (is (= (range 1 (inc (count summaries)))
                             (map #(.getNumMessages ^CourierSummary %) summaries))
                          assertion-message)
                      (is (= (reductions + (map count (take (count summaries) messages)))
                             (map #(.getTotalLength ^CourierSummary %) summaries))
                          assertion-message))
                    (Thread/sleep 1500) ;; sleep to allow cancellation propagation to backend
                    (assert-request-state grpc-client request-headers service-id correlation-id ::client-cancel)))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-bidi-streaming-server-exit
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3
          correlation-id-prefix (rand-name)]
      (doseq [max-message-length [1000 50000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (doseq [mode ["EXIT_PRE_RESPONSE" "EXIT_POST_RESPONSE"]]
              (testing (str "lock-step mode " max-message-length " messages " mode)
                (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                  (with-service-cleanup
                    service-id
                    (let [cancel-threshold (inc num-messages)
                          exit-index (* iteration (/ num-messages num-iterations))
                          correlation-id (str correlation-id-prefix "." mode "." exit-index "-" num-messages "." max-message-length)
                          _ (log/info "collect packages cid" correlation-id "for"
                                      {:iteration iteration :max-message-length max-message-length})
                          from (rand-name "f")
                          ids (map #(str "id-" (cond-> % (= % exit-index) (str "." mode))) (range num-messages))
                          request-headers (assoc request-headers "x-cid" correlation-id)
                          grpc-client (initialize-grpc-client correlation-id host h2c-port)
                          rpc-result (.collectPackages grpc-client request-headers ids from messages 1 true
                                                       cancel-threshold cancel-policy-none 60000)
                          message-summaries (.result rpc-result)
                          ^Status status (.status rpc-result)
                          assertion-message (->> (cond-> {:correlation-id correlation-id
                                                          :exit-index exit-index
                                                          :iteration iteration
                                                          :service-id service-id
                                                          :summaries (map (fn [^CourierSummary s]
                                                                            {:num-messages (.getNumMessages s)
                                                                             :total-length (.getTotalLength s)})
                                                                          message-summaries)}
                                                   status (assoc :status {:code (-> status .getCode str)
                                                                          :description (.getDescription status)}))
                                              (into (sorted-map))
                                              str)
                          expected-summary-count (cond-> exit-index
                                                   (= "EXIT_POST_RESPONSE" mode) inc)]
                      (log/info "result" assertion-message)
                      (assert-grpc-server-exit-status status assertion-message)
                      (is (= expected-summary-count (count message-summaries)) assertion-message)
                      (when (seq message-summaries)
                        (is (= (range 1 (inc expected-summary-count))
                               (map #(.getNumMessages %) message-summaries))
                            assertion-message)
                        (is (= (reductions + (map count (take expected-summary-count messages)))
                               (map #(.getTotalLength %) message-summaries))
                            assertion-message)))))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-bidi-streaming-server-cancellation
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3
          correlation-id-prefix (rand-name)]
      (doseq [max-message-length [1000 50000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (testing (str "lock-step mode " max-message-length " messages server error")
              (let [mode "SEND_ERROR"
                    {:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                (with-service-cleanup
                  service-id
                  (let [cancel-threshold (inc num-messages)
                        error-index (* iteration (/ num-messages num-iterations))
                        correlation-id (str correlation-id-prefix "." mode "." error-index "-" num-messages "." max-message-length)
                        from (rand-name "f")
                        ids (map #(str "id-" (cond-> % (= % error-index) (str "." mode))) (range num-messages))
                        request-headers (assoc request-headers "x-cid" correlation-id)
                        _ (log/info "collect packages cid" correlation-id "for"
                                    {:iteration iteration :max-message-length max-message-length})
                        grpc-client (initialize-grpc-client correlation-id host h2c-port)
                        rpc-result (.collectPackages grpc-client request-headers ids from messages 1 true
                                                     cancel-threshold cancel-policy-none 60000)
                        message-summaries (.result rpc-result)
                        ^Status status (.status rpc-result)
                        assertion-message (->> (cond-> {:correlation-id correlation-id
                                                        :error-index error-index
                                                        :iteration iteration
                                                        :service-id service-id
                                                        :summaries (map (fn [s]
                                                                          {:num-messages (.getNumMessages s)
                                                                           :total-length (.getTotalLength s)})
                                                                        message-summaries)}
                                                 status (assoc :status {:code (-> status .getCode str)
                                                                        :description (.getDescription status)}))
                                            (into (sorted-map))
                                            str)
                        expected-summary-count error-index]
                    (log/info "result" assertion-message)
                    (assert-grpc-cancel-status status "Cancelled by server" assertion-message)
                    (is (= expected-summary-count (count message-summaries)) assertion-message)
                    (when (seq message-summaries)
                      (is (= (range 1 (inc expected-summary-count))
                             (map #(.getNumMessages %) message-summaries))
                          assertion-message)
                      (is (= (reductions + (map count (take expected-summary-count messages)))
                             (map #(.getTotalLength %) message-summaries))
                          assertion-message))
                    (assert-request-state grpc-client request-headers service-id correlation-id ::server-cancel)))))))))))

(deftest ^:parallel ^:integration-fast test-grpc-client-streaming-successful
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
          correlation-id-prefix (rand-name)]
      (with-service-cleanup
        service-id
        (doseq [max-message-length [1000 50000]]
          (let [num-messages 120
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

            (testing (str max-message-length " messages completion")
              (log/info "starting streaming to and from server - independent mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    correlation-id (str correlation-id-prefix "-" max-message-length)
                    request-headers (assoc request-headers "x-cid" correlation-id)
                    ids (map #(str "id-" %) (range num-messages))
                    grpc-client (initialize-grpc-client correlation-id host h2c-port)
                    rpc-result (.aggregatePackages grpc-client request-headers ids from messages 10
                                                   cancel-threshold cancel-policy-none 60000)
                    ^CourierSummary summary (.result rpc-result)
                    ^Status status (.status rpc-result)
                    assertion-message (->> (cond-> {:correlation-id correlation-id
                                                    :service-id service-id}
                                             summary (assoc :summary {:num-messages (.getNumMessages summary)
                                                                      :total-length (.getTotalLength summary)})
                                             status (assoc :status {:code (-> status .getCode str)
                                                                    :description (.getDescription status)}))
                                        (into (sorted-map))
                                        str)]
                (log/info correlation-id "aggregated packages...")
                (assert-grpc-ok-status status assertion-message)
                (is summary assertion-message)
                (when summary
                  (is (= (count messages) (.getNumMessages summary)) assertion-message)
                  (is (= (reduce + (map count messages)) (.getTotalLength summary)) assertion-message))
                (assert-request-state grpc-client request-headers service-id correlation-id ::success)))))))))

(deftest ^:parallel ^:integration-fast test-grpc-client-streaming-client-cancellation
  (testing-using-waiter-url
    (when (grpc-cancellations-supported? waiter-url)
      (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
            correlation-id-prefix (rand-name)]
        (with-service-cleanup
          service-id
          (doseq [cancel-policy [cancel-policy-context cancel-policy-exception cancel-policy-observer]]
            (doseq [max-message-length [1000 50000]]
              (let [num-messages 120
                    messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

                (testing (str max-message-length " messages completion " cancel-policy)
                  (log/info "starting streaming to and from server - independent mode test")
                  (let [cancel-threshold (/ num-messages 2)
                        from (rand-name "f")
                        correlation-id (str correlation-id-prefix "-in-" max-message-length "-" cancel-policy)
                        request-headers (assoc request-headers "x-cid" correlation-id)
                        ids (map #(str "id-" %) (range num-messages))
                        grpc-client (initialize-grpc-client correlation-id host h2c-port)
                        rpc-result (.aggregatePackages grpc-client request-headers ids from messages 10 cancel-threshold cancel-policy 60000)
                        ^CourierSummary summary (.result rpc-result)
                        ^Status status (.status rpc-result)
                        assertion-message (->> (cond-> {:correlation-id correlation-id
                                                        :service-id service-id}
                                                 summary (assoc :summary {:num-messages (.getNumMessages summary)
                                                                          :total-length (.getTotalLength summary)})
                                                 status (assoc :status {:code (-> status .getCode str)
                                                                        :description (.getDescription status)}))
                                               (into (sorted-map))
                                               str)]
                    (log/info correlation-id "aggregated packages...")
                    (cond
                      (= cancel-policy-context cancel-policy)
                      (assert-grpc-cancel-status status "Context cancelled" assertion-message)
                      (= cancel-policy-exception cancel-policy)
                      (assert-grpc-unknown-status status nil assertion-message)
                      (= cancel-policy-observer cancel-policy)
                      (assert-grpc-unknown-status status "call was cancelled" assertion-message))
                    (is (nil? summary) assertion-message)
                    (assert-request-state grpc-client request-headers service-id correlation-id ::client-cancel)))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-bidi-streaming-client-exit
  (testing-using-waiter-url
    (when (grpc-cancellations-supported? waiter-url)
      (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
            {:strs [cookie]} request-headers]
        (with-service-cleanup
          service-id
          (let [correlation-id (rand-name)
                {:keys [err out]} (sh/sh "lein" "exec" "-p"
                                         "test-files/grpc/grpc_client_exit.clj"
                                         host (str h2c-port) service-id correlation-id cookie)]
            (log/info "exec stdout:" out)
            (log/info "exec stderr:" err)
            (Thread/sleep 1500) ;; sleep to allow cancellation propagation to backend
            (let [grpc-client (initialize-grpc-client correlation-id host h2c-port)]
              (assert-request-state grpc-client request-headers service-id correlation-id ::client-cancel))))))))

(deftest ^:parallel ^:integration-fast test-grpc-client-streaming-deadline-exceeded
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
          correlation-id-prefix (rand-name)]
      (with-service-cleanup
        service-id
        (doseq [max-message-length [1000 50000]]
          (let [num-messages 120
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

            (testing (str max-message-length " messages completion")
              (log/info "starting streaming to and from server - independent mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    correlation-id (str correlation-id-prefix "-" max-message-length)
                    request-headers (assoc request-headers "x-cid" correlation-id)
                    ids (map #(str "id-" %) (range num-messages))
                    grpc-client (initialize-grpc-client correlation-id host h2c-port)
                    sleep-duration-latch (CountDownLatch. 1)
                    sleep-duration-ms 5000
                    deadline-duration-ms (- sleep-duration-ms 1000)
                    _ (async/go
                        (async/<! (async/timeout (+ sleep-duration-ms 1000)))
                        (.countDown sleep-duration-latch))
                    rpc-result (.aggregatePackages grpc-client request-headers ids from messages 1000
                                                   cancel-threshold cancel-policy-none deadline-duration-ms)
                    ^CourierSummary summary (.result rpc-result)
                    ^Status status (.status rpc-result)
                    assertion-message (->> (cond-> {:correlation-id correlation-id
                                                    :service-id service-id}
                                             summary (assoc :summary {:num-messages (.getNumMessages summary)
                                                                      :total-length (.getTotalLength summary)})
                                             status (assoc :status {:code (-> status .getCode str)
                                                                    :description (.getDescription status)}))
                                        (into (sorted-map))
                                        str)]
                (log/info correlation-id "aggregated packages...")
                (cond
                  (and (grpc-cancellations-supported? waiter-url)
                       (= "UNAVAILABLE" (some-> status .getCode str)))
                  (assert-grpc-status status "UNAVAILABLE" "Received Rst Stream" assertion-message)
                  (= "INVALID_ARGUMENT" (some-> status .getCode str))
                  (assert-grpc-status status "INVALID_ARGUMENT" "Client action means stream is no longer needed"
                                      assertion-message)
                  :else
                  (assert-grpc-deadline-exceeded-status status assertion-message))
                (is (nil? summary) assertion-message)
                (.await sleep-duration-latch)
                (when (grpc-cancellations-supported? waiter-url)
                  (assert-request-state grpc-client request-headers service-id correlation-id ::deadline-exceeded))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-client-streaming-server-exit
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3
          correlation-id-prefix (rand-name)]
      (doseq [max-message-length [1000 50000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (doseq [mode ["EXIT_PRE_RESPONSE" "EXIT_POST_RESPONSE"]]
              (testing (str "lock-step mode " max-message-length " messages " mode)
                (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                  (with-service-cleanup
                    service-id
                    (let [cancel-threshold (inc num-messages)
                          exit-index (* iteration (/ num-messages num-iterations))
                          correlation-id (str correlation-id-prefix "." mode "." exit-index "-" num-messages "." max-message-length)
                          _ (log/info "aggregate packages cid" correlation-id "for"
                                      {:iteration iteration :max-message-length max-message-length})
                          from (rand-name "f")
                          ids (map #(str "id-" (cond-> % (= % exit-index) (str "." mode))) (range num-messages))
                          request-headers (assoc request-headers "x-cid" correlation-id)
                          grpc-client (initialize-grpc-client correlation-id host h2c-port)
                          rpc-result (.aggregatePackages grpc-client request-headers ids from messages 1
                                                         cancel-threshold cancel-policy-none 60000)
                          ^CourierSummary message-summary (.result rpc-result)
                          ^Status status (.status rpc-result)
                          assertion-message (->> (cond-> {:correlation-id correlation-id
                                                          :exit-index exit-index
                                                          :iteration iteration
                                                          :service-id service-id}
                                                   message-summary (assoc :summary {:num-messages (.getNumMessages message-summary)
                                                                                    :total-length (.getTotalLength message-summary)})
                                                   status (assoc :status {:code (-> status .getCode str)
                                                                          :description (.getDescription status)}))
                                              (into (sorted-map))
                                              str)]
                      (log/info "result" assertion-message)
                      (assert-grpc-server-exit-status status assertion-message)
                      (is (nil? message-summary) assertion-message))))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-client-streaming-server-cancellation
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3
          correlation-id-prefix (rand-name)]
      (doseq [max-message-length [1000 50000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (testing (str max-message-length " messages server error")
              (let [mode "SEND_ERROR"
                    {:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                (with-service-cleanup
                  service-id
                  (let [cancel-threshold (inc num-messages)
                        error-index (* iteration (/ num-messages num-iterations))
                        correlation-id (str correlation-id-prefix "." mode "." error-index "-" num-messages "." max-message-length)
                        from (rand-name "f")
                        ids (map #(str "id-" (cond-> % (= % error-index) (str "." mode))) (range num-messages))
                        request-headers (assoc request-headers "x-cid" correlation-id)
                        _ (log/info "aggregate packages cid" correlation-id "for"
                                    {:iteration iteration :max-message-length max-message-length})
                        grpc-client (initialize-grpc-client correlation-id host h2c-port)
                        rpc-result (.aggregatePackages grpc-client request-headers ids from messages 1
                                                       cancel-threshold cancel-policy-none 60000)
                        ^CourierSummary message-summary (.result rpc-result)
                        ^Status status (.status rpc-result)
                        assertion-message (->> (cond-> {:correlation-id correlation-id
                                                        :error-index error-index
                                                        :iteration iteration
                                                        :service-id service-id}
                                                 message-summary (assoc :summary {:num-messages (.getNumMessages message-summary)
                                                                                  :total-length (.getTotalLength message-summary)})
                                                 status (assoc :status {:code (-> status .getCode str)
                                                                        :description (.getDescription status)}))
                                            (into (sorted-map))
                                            str)]
                    (log/info "result" assertion-message)
                    (assert-grpc-cancel-status status "Cancelled by server" assertion-message)
                    (is (nil? message-summary) assertion-message)
                    (assert-request-state grpc-client request-headers service-id correlation-id ::server-cancel)))))))))))
