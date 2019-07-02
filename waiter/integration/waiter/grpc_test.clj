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
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [waiter.util.client-tools :refer :all])
  (:import (com.twosigma.waiter.courier GrpcClient)
           (java.util.function Function)))

;; initialize logging on the grpc client
(GrpcClient/setLogFunction (reify Function
                             (apply [_ message]
                               (log/info message))))

(defn- basic-grpc-service-parameters
  []
  (let [courier-command (courier-server-command "${PORT0} ${PORT1}")]
    (walk/stringify-keys
      {:x-waiter-backend-proto "h2c"
       :x-waiter-cmd courier-command
       :x-waiter-cmd-type "shell"
       :x-waiter-concurrency-level 32
       :x-waiter-cpus 0.2
       :x-waiter-debug true
       :x-waiter-grace-period-secs 120
       :x-waiter-health-check-port-index 1
       :x-waiter-health-check-proto "http"
       :x-waiter-idle-timeout-mins 10
       :x-waiter-max-instances 1
       :x-waiter-min-instances 1
       :x-waiter-mem 512
       :x-waiter-name (rand-name)
       :x-waiter-ports 2
       :x-waiter-version "version-does-not-matter"})))

(defn- rand-str
  "Generates a random string with the specified length."
  [length]
  (apply str (take length (repeatedly #(char (+ (rand 26) 65))))))

(defn ping-courier-service
  [waiter-url request-headers]
  (make-request waiter-url "/waiter-ping" :headers request-headers))

(defn start-courier-instance
  [waiter-url]
  (let [[host _] (str/split waiter-url #":")
        h2c-port (Integer/parseInt (retrieve-h2c-port waiter-url))
        request-headers (basic-grpc-service-parameters)
        {:keys [cookies headers] :as response} (ping-courier-service waiter-url request-headers)
        cookie-header (str/join "; " (map #(str (:name %) "=" (:value %)) cookies))
        service-id (get headers "x-waiter-service-id")
        request-headers (assoc request-headers
                          "cookie" cookie-header
                          "x-waiter-timeout" "60000")]
    (assert-response-status response 200)
    (is service-id)
    (log/info "ping cid:" (get headers "x-cid"))
    (log/info "service-id:" service-id)
    (let [{:keys [ping-response service-state]} (some-> response :body try-parse-json walk/keywordize-keys)]
      (is (= "received-response" (:result ping-response)) (str ping-response))
      (is (str/starts-with? (str (some-> ping-response :headers :server)) "courier-health-check"))
      (assert-response-status ping-response 200)
      (is (or (= {:exists? true :healthy? true :service-id service-id :status "Running"} service-state)
              (= {:exists? true :healthy? false :service-id service-id :status "Starting"} service-state))
          (str service-state)))
    (assert-service-on-all-routers waiter-url service-id cookies)

    {:cookies cookies
     :h2c-port h2c-port
     :host host
     :request-headers request-headers
     :service-id service-id}))

(defn- retrieve-states
  "Retrieves the states corresponding to a cid in the gRPC service defined by the request headers."
  [host h2c-port request-headers service-id correlation-id expect-cancel?]
  (let [retrieve-state-cid (rand-name)
        request-headers (assoc request-headers "x-cid" retrieve-state-cid)
        response (GrpcClient/retrieveState host h2c-port request-headers correlation-id)
        state-count (or (some-> response (.getStateCount)) 0)
        states (vec (doall (map #(.getState response %) (range state-count))))
        assertion-message (str {:retrieve-cid retrieve-state-cid
                                :result {:correlation-id correlation-id
                                         :states states}
                                :service-id service-id})]
    (log/info correlation-id "states:" states)
    (cond
      ;; request executed without sending any data to the backend should be in non-lock-step mode
      (zero? (count states))
      (is (str/includes? correlation-id "independent") assertion-message)

      ;; request executed without sending any data to the backend should be in non-lock-step mode
      (= 2 (count states))
      (do
        (is (str/includes? correlation-id "independent") assertion-message)
        (is (= ["INIT" "READY"] states) assertion-message))

      ;; request executed without sending any data to the backend should be in non-lock-step mode
      (= 3 (count states))
      (do
        (is (str/includes? correlation-id "independent") assertion-message)
        (is (= ["INIT" "CANCEL" "CLOSE"] states) assertion-message))

      ;; request executed without sending any data to the backend should be in non-lock-step mode
      (and (= 5 (count states))
           (not-any? #(= "RECEIVE_MESSAGE" %) states))
      (do
        (is (str/includes? correlation-id "independent") assertion-message)
        (is (= ["INIT" "READY" "HALF_CLOSE" "CLOSE" "COMPLETE"] states) assertion-message))

      ;; request connected to the backend and transmitted some data
      ;; we must see a cancellation state for such requests
      :else
      (do
        (when-not (= ["INIT" "READY"] (take 2 states))
          (println assertion-message)
          (log/info "FAIL:" assertion-message))
        (if (> (count states) 6)
          (is (= ["INIT" "READY" "RECEIVE_MESSAGE" "SEND_HEADERS"] (take 4 states)) assertion-message)
          (is (= ["INIT" "READY"] (take 2 states)) assertion-message))
        (when-not (every? #{"RECEIVE_MESSAGE" "SEND_MESSAGE"} (->> states (drop 4) (drop-last 4)))
          (println assertion-message)
          (log/info "FAIL:" assertion-message))
        (is (every? #{"RECEIVE_MESSAGE" "SEND_MESSAGE"} (->> states (drop 4) (drop-last 4))) assertion-message)
        (if expect-cancel?
          ;; abnormal / cancellation path
          (let [expected-last-two-state-members #{"CLOSE" "CANCEL" "COMPLETE"}
                actual-last-two-states (take-last 2 states)]
            (is (every? expected-last-two-state-members actual-last-two-states) assertion-message)
            (when-not (some #{"CANCEL"} actual-last-two-states)
              (println assertion-message)
              (log/info "FAIL:" assertion-message))
            (is (some #{"CANCEL"} actual-last-two-states) assertion-message)
            (is (= 2 (count (set actual-last-two-states))) assertion-message))
          ;; normal path
          (is (= ["SEND_MESSAGE" "HALF_CLOSE" "CLOSE" "COMPLETE"] (take-last 4 states)) assertion-message))))
    states))

(deftest ^:parallel ^:integration-fast test-grpc-unary-call
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (testing "small request and reply"
          (log/info "starting small request and reply test")
          (let [id (rand-name "m")
                from (rand-name "f")
                content (rand-str 1000)
                request-headers (assoc request-headers "x-cid" (rand-name))
                reply (GrpcClient/sendPackage host h2c-port request-headers id from content)]
            (is reply)
            (when reply
              (is (= id (.getId reply)))
              (is (= content (.getMessage reply)))
              (is (= "received" (.getResponse reply))))))))))

(deftest ^:parallel ^:integration-fast test-grpc-unary-call-server-error
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (testing "small request and reply"
          (log/info "starting small request and reply test")
          (let [id (str (rand-name "m") ".SEND_ERROR")
                from (rand-name "f")
                content (rand-str 1000)
                request-cid (rand-name)
                _ (println request-cid)
                request-headers (assoc request-headers "x-cid" request-cid)
                reply (GrpcClient/sendPackage host h2c-port request-headers id from content)]
            (is reply)
            (when reply
              (is (= "CANCELLED" (.getId reply)))
              (is (= "Cancelled by server" (.getMessage reply)))
              (is (= "error" (.getResponse reply))))))))))

(deftest ^:parallel ^:integration-fast test-grpc-streaming-successful
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (doseq [max-message-length [1000 10000 100000]]
          (let [num-messages 100
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

            (testing (str "independent mode " max-message-length " messages completion")
              (log/info "starting streaming to and from server - independent mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    collect-cid (str (rand-name) "-independent-complete")
                    request-headers (assoc request-headers "x-cid" collect-cid)
                    summaries (GrpcClient/collectPackages
                                host h2c-port request-headers "m-" from messages 10 false cancel-threshold)]
                (log/info collect-cid "collecting independent packages...")
                (is (= (count messages) (count summaries)))
                (when (seq summaries)
                  (is (= (range 1 (inc (count messages))) (map #(.getNumMessages %) summaries)))
                  (is (= (reductions + (map count messages)) (map #(.getTotalLength %) summaries))))
                (Thread/sleep 1000) ;; allow server-side error processing to complete
                (let [states (retrieve-states host h2c-port request-headers service-id collect-cid false)]
                  (is (= (+ 6 (* 2 num-messages)) (count states)) (str collect-cid " -> " states)))))

            (testing (str "lock-step mode " max-message-length " messages completion")
              (log/info "starting streaming to and from server - lock-step mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    collect-cid (str (rand-name) "-lock-step-complete")
                    request-headers (assoc request-headers "x-cid" collect-cid)
                    summaries (GrpcClient/collectPackages
                                host h2c-port request-headers "m-" from messages 1 true cancel-threshold)]
                (log/info collect-cid "collecting lock-step packages...")
                (is (= (count messages) (count summaries)))
                (when (seq summaries)
                  (is (= (range 1 (inc (count messages))) (map #(.getNumMessages %) summaries)))
                  (is (= (reductions + (map count messages)) (map #(.getTotalLength %) summaries))))
                (Thread/sleep 1000) ;; allow server-side error processing to complete
                (let [states (retrieve-states host h2c-port request-headers service-id collect-cid false)]
                  (is (= (+ 6 (* 2 num-messages)) (count states)) (str collect-cid " -> " states)))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-streaming-client-cancellation
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (doseq [max-message-length [1000 10000 100000]]
          (let [num-messages 100
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

            (dotimes [_ 20]
              (testing (str "independent mode " max-message-length " messages cancellations")
                (log/info "starting streaming to and from server - independent mode test")
                (let [cancel-threshold (+ 5 (rand-int (- num-messages 5)))
                      from (rand-name "f")
                      collect-cid (str (rand-name) "-independent-cancel." cancel-threshold "." max-message-length)
                      request-headers (assoc request-headers "x-cid" collect-cid)
                      summaries (GrpcClient/collectPackages
                                  host h2c-port request-headers "m-" from messages 1 false cancel-threshold)]
                  (log/info collect-cid "collecting independent packages...")
                  (is (nil? summaries))
                  (Thread/sleep 1000) ;; allow server-side error processing to complete
                  (let [states (retrieve-states host h2c-port request-headers service-id collect-cid true)]
                    (is (>= (+ 6 (* 2 cancel-threshold)) (count states)) (str collect-cid " -> " states)))))

              (testing (str "lock-step mode " max-message-length " messages cancellations")
                (log/info "starting streaming to and from server - lock-step mode test")
                (let [cancel-threshold (+ 5 (rand-int (- num-messages 5)))
                      from (rand-name "f")
                      collect-cid (str (rand-name) "-lock-step-cancel." cancel-threshold "." max-message-length)
                      request-headers (assoc request-headers "x-cid" collect-cid)
                      summaries (GrpcClient/collectPackages
                                  host h2c-port request-headers "m-" from messages 1 true cancel-threshold)]
                  (log/info collect-cid "collecting lock-step packages...")
                  (is (nil? summaries))
                  (Thread/sleep 1000) ;; allow server-side error processing to complete
                  (let [states (retrieve-states host h2c-port request-headers service-id collect-cid true)]
                    (is (>= (+ 6 (* 2 cancel-threshold)) (count states)) (str collect-cid " -> " states))))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-streaming-server-cancellation
  (testing-using-waiter-url
    (let [num-messages 100
          num-iterations 10]
      (dotimes [iteration 1] ;; TODO shams num-iterations
        (doseq [max-message-length [1000]] ;; TODO shams  10000 100000
          (doseq [mode ["EXIT_POST_RESPONSE"]] ;; TODO shams "EXIT_PRE_RESPONSE" "EXIT_POST_RESPONSE"
            (testing (str "lock-step mode " max-message-length " messages cancellations pre-response")
              (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)
                    ]
                (with-service-cleanup
                  service-id
                  (let [cancellation-index (* iteration (/ num-messages num-iterations))
                        collect-cid (str (rand-name) "." mode "." cancellation-index "-" num-messages "." max-message-length)
                        _ (println "collect packages cid" collect-cid "for"
                                    {:iteration iteration :max-message-length max-message-length})
                        from (rand-name "f")
                        ids (map #(str "id-" (cond-> % (= % cancellation-index) (str "." mode))) (range num-messages))
                        messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))
                        request-headers (assoc request-headers "x-cid" collect-cid)
                        summaries (GrpcClient/collectPackages
                                    host h2c-port request-headers ids from messages 1 true (inc num-messages))
                        assertion-message (str {:cancellation-index cancellation-index
                                                :collect-cid collect-cid
                                                :iteration iteration
                                                :service-id service-id
                                                :summaries (map (fn [s]
                                                                  {:num-messages (.getNumMessages s)
                                                                   :status-code (.getStatusCode s)
                                                                   :status-description (.getStatusDescription s)
                                                                   :total-length (.getTotalLength s)})
                                                                summaries)})
                        message-summaries (take (dec (count summaries)) summaries)
                        status-summary (last summaries)
                        expected-summary-count (cond-> cancellation-index
                                                 (= "EXIT_POST_RESPONSE" mode) inc)]
                    (log/info "result" assertion-message) ;; TODO shams convert to log
                    (is (= expected-summary-count (count message-summaries)) assertion-message)
                    (when (seq message-summaries)
                      (is (= (range 1 (inc expected-summary-count))
                             (map #(.getNumMessages %) message-summaries))
                          assertion-message)
                      (is (= (reductions + (map count (take expected-summary-count messages)))
                             (map #(.getTotalLength %) message-summaries))
                          assertion-message))
                    (is status-summary)
                    (when status-summary
                      (log/info "server cancellation summary" status-summary)
                      (is (= "UNAVAILABLE" (.getStatusCode status-summary))
                          assertion-message)
                      (is (zero? (.getNumMessages status-summary))
                          assertion-message))))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-streaming-server-error
  (testing-using-waiter-url
    (let [num-messages 100
          num-iterations 10]
      (dotimes [iteration num-iterations]
        (doseq [max-message-length [1000 10000 100000]]
          (testing (str "lock-step mode " max-message-length " messages server error")
            (let [mode "SEND_ERROR"
                  {:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
              (with-service-cleanup
                service-id
                (let [error-index (* iteration (/ num-messages num-iterations))
                      collect-cid (str (rand-name) "." mode "." error-index "-" num-messages "." max-message-length)
                      from (rand-name "f")
                      ids (map #(str "id-" (cond-> % (= % error-index) (str "." mode))) (range num-messages))
                      messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))
                      request-headers (assoc request-headers "x-cid" collect-cid)
                      _ (log/info "collect packages cid" collect-cid "for"
                                  {:iteration iteration :max-message-length max-message-length})
                      summaries (GrpcClient/collectPackages
                                  host h2c-port request-headers ids from messages 1 true (inc num-messages))
                      assertion-message (str {:collect-cid collect-cid
                                              :error-index error-index
                                              :iteration iteration
                                              :service-id service-id
                                              :summaries (map (fn [s]
                                                                {:num-messages (.getNumMessages s)
                                                                 :status-code (.getStatusCode s)
                                                                 :status-description (.getStatusDescription s)
                                                                 :total-length (.getTotalLength s)})
                                                              summaries)})
                      expected-summary-count error-index
                      message-summaries (take (dec (count summaries)) summaries)
                      status-summary (last summaries)]
                  (log/info "result" assertion-message)
                  (is (= expected-summary-count (count message-summaries)) assertion-message)
                  (when (seq message-summaries)
                    (is (= (range 1 (inc expected-summary-count))
                           (map #(.getNumMessages %) message-summaries))
                        assertion-message)
                    (is (= (reductions + (map count (take expected-summary-count messages)))
                           (map #(.getTotalLength %) message-summaries))
                        assertion-message))
                  (is status-summary)
                  (when status-summary
                    (log/info "server cancellation summary" status-summary)
                    (is (= "CANCELLED" (.getStatusCode status-summary))
                        assertion-message)
                    (is (= "Cancelled by server" (.getStatusDescription status-summary))
                        assertion-message)
                    (is (zero? (.getNumMessages status-summary))
                        assertion-message)))))))))))
