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
            [waiter.correlation-id :as cid]
            [waiter.util.client-tools :refer :all])
  (:import (com.twosigma.waiter.courier CourierReply CourierSummary GrpcClient)
           (io.grpc Status)
           (java.util.function Function)))

(defn- initialize-grpc-client
  "Initializes grpc client logging to specific correlation id"
  [correlation-id host port]
  (GrpcClient. host port (reify Function
                           (apply [_ message]
                             (cid/with-correlation-id
                               correlation-id
                               (log/info message))))))

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
     :service-id service-id}))

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
                correlation-id (rand-name)
                request-headers (assoc request-headers "x-cid" correlation-id)
                grpc-client (initialize-grpc-client correlation-id host h2c-port)
                rpc-result (.sendPackage grpc-client request-headers id from content)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (str (cond-> {:correlation-id correlation-id
                                                :service-id service-id}
                                         reply (assoc :reply {:id (.getId reply)
                                                              :response (.getResponse reply)})
                                         status (assoc :status {:code (-> status .getCode str)
                                                                :description (.getDescription status)})))]
            (is reply assertion-message)
            (when reply
              (is (= id (.getId reply)) assertion-message)
              (is (= content (.getMessage reply)) assertion-message)
              (is (= "received" (.getResponse reply)) assertion-message))
            (is status assertion-message)
            (when status
              (is (= "OK" (-> status .getCode str)) assertion-message)
              (is (str/blank? (.getDescription status)) assertion-message))))))))

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
                rpc-result (.sendPackage grpc-client request-headers id from content)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (str (cond-> {:correlation-id correlation-id
                                                :service-id service-id}
                                         reply (assoc :reply {:id (.getId reply)
                                                              :response (.getResponse reply)})
                                         status (assoc :status {:code (-> status .getCode str)
                                                                :description (.getDescription status)})))]
            (is (nil? reply) assertion-message)
            (is status assertion-message)
            (when status
              (is (= "CANCELLED" (-> status .getCode str)) assertion-message)
              (is (= "Cancelled by server" (.getDescription status)) assertion-message))))))))

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
                rpc-result (.sendPackage grpc-client request-headers id from content)
                ^CourierReply reply (.result rpc-result)
                ^Status status (.status rpc-result)
                assertion-message (str (cond-> {:correlation-id correlation-id
                                                :service-id service-id}
                                         reply (assoc :reply {:id (.getId reply)
                                                              :response (.getResponse reply)})
                                         status (assoc :status {:code (-> status .getCode str)
                                                                :description (.getDescription status)})))]
            (is (nil? reply) assertion-message)
            (is status assertion-message)
            (when status
              (is (contains? #{"UNAVAILABLE" "INTERNAL"} (-> status .getCode str)) assertion-message))))))))

(deftest ^:parallel ^:integration-fast test-grpc-bidi-streaming-successful
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (doseq [max-message-length [1000 100000]]
          (let [num-messages 120
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

            (testing (str "independent mode " max-message-length " messages completion")
              (log/info "starting streaming to and from server - independent mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    correlation-id (str (rand-name) "-independent-complete")
                    request-headers (assoc request-headers "x-cid" correlation-id)
                    ids (map #(str "id-inde-" %) (range num-messages))
                    grpc-client (initialize-grpc-client correlation-id host h2c-port)
                    rpc-result (.collectPackages grpc-client request-headers ids from messages 10 false cancel-threshold)
                    summaries (.result rpc-result)
                    ^Status status (.status rpc-result)
                    assertion-message (str (cond-> {:correlation-id correlation-id
                                                    :service-id service-id
                                                    :summaries (map (fn [^CourierSummary s]
                                                                      {:num-messages (.getNumMessages s)
                                                                       :total-length (.getTotalLength s)})
                                                                    summaries)}
                                             status (assoc :status {:code (-> status .getCode str)
                                                                    :description (.getDescription status)})))]
                (log/info correlation-id "collecting independent packages...")
                (is (= (count messages) (count summaries)) assertion-message)
                (when (seq summaries)
                  (is (= (range 1 (inc (count messages))) (map #(.getNumMessages ^CourierSummary %) summaries))
                      assertion-message)
                  (is (= (reductions + (map count messages)) (map #(.getTotalLength ^CourierSummary %) summaries))
                      assertion-message))
                (is status assertion-message)
                (when status
                  (is (= "OK" (-> status .getCode str)) assertion-message)
                  (is (str/blank? (.getDescription status)) assertion-message))))

            (testing (str "lock-step mode " max-message-length " messages completion")
              (log/info "starting streaming to and from server - lock-step mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    correlation-id (str (rand-name) "-lock-step-complete")
                    request-headers (assoc request-headers "x-cid" correlation-id)
                    ids (map #(str "id-lock-" %) (range num-messages))
                    grpc-client (initialize-grpc-client correlation-id host h2c-port)
                    rpc-result (.collectPackages grpc-client request-headers ids from messages 1 true cancel-threshold)
                    summaries (.result rpc-result)
                    ^Status status (.status rpc-result)
                    assertion-message (str (cond-> {:correlation-id correlation-id
                                                    :service-id service-id
                                                    :summaries (map (fn [^CourierSummary s]
                                                                      {:num-messages (.getNumMessages s)
                                                                       :total-length (.getTotalLength s)})
                                                                    summaries)}
                                             status (assoc :status {:code (-> status .getCode str)
                                                                    :description (.getDescription status)})))]
                (log/info correlation-id "collecting lock-step packages...")
                (is (= (count messages) (count summaries)) assertion-message)
                (when (seq summaries)
                  (is (= (range 1 (inc (count messages))) (map #(.getNumMessages ^CourierSummary %) summaries))
                      assertion-message)
                  (is (= (reductions + (map count messages)) (map #(.getTotalLength ^CourierSummary %) summaries))
                      assertion-message))
                (is status assertion-message)
                (when status
                  (is (= "OK" (-> status .getCode str)) assertion-message)
                  (is (str/blank? (.getDescription status)) assertion-message))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-bidi-streaming-server-exit
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3]
      (doseq [max-message-length [1000 100000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (doseq [mode ["EXIT_PRE_RESPONSE" "EXIT_POST_RESPONSE"]]
              (testing (str "lock-step mode " max-message-length " messages " mode)
                (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                  (with-service-cleanup
                    service-id
                    (let [exit-index (* iteration (/ num-messages num-iterations))
                          correlation-id (str (rand-name) "." mode "." exit-index "-" num-messages "." max-message-length)
                          _ (log/info "collect packages cid" correlation-id "for"
                                      {:iteration iteration :max-message-length max-message-length})
                          from (rand-name "f")
                          ids (map #(str "id-" (cond-> % (= % exit-index) (str "." mode))) (range num-messages))
                          request-headers (assoc request-headers "x-cid" correlation-id)
                          grpc-client (initialize-grpc-client correlation-id host h2c-port)
                          rpc-result (.collectPackages grpc-client request-headers ids from messages 1 true (inc num-messages))
                          message-summaries (.result rpc-result)
                          ^Status status (.status rpc-result)
                          assertion-message (str (cond-> {:correlation-id correlation-id
                                                          :exit-index exit-index
                                                          :iteration iteration
                                                          :service-id service-id
                                                          :summaries (map (fn [^CourierSummary s]
                                                                            {:num-messages (.getNumMessages s)
                                                                             :total-length (.getTotalLength s)})
                                                                          message-summaries)}
                                                   status (assoc :status {:code (-> status .getCode str)
                                                                          :description (.getDescription status)})))
                          expected-summary-count (cond-> exit-index
                                                   (= "EXIT_POST_RESPONSE" mode) inc)]
                      (log/info "result" assertion-message)
                      (is (= expected-summary-count (count message-summaries)) assertion-message)
                      (when (seq message-summaries)
                        (is (= (range 1 (inc expected-summary-count))
                               (map #(.getNumMessages %) message-summaries))
                            assertion-message)
                        (is (= (reductions + (map count (take expected-summary-count messages)))
                               (map #(.getTotalLength %) message-summaries))
                            assertion-message))
                      (is status assertion-message)
                      (when status
                        (is (contains? #{"UNAVAILABLE" "INTERNAL"} (-> status .getCode str)) assertion-message)))))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-bidi-streaming-server-cancellation
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3]
      (doseq [max-message-length [1000 100000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (testing (str "lock-step mode " max-message-length " messages server error")
              (let [mode "SEND_ERROR"
                    {:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                (with-service-cleanup
                  service-id
                  (let [error-index (* iteration (/ num-messages num-iterations))
                        correlation-id (str (rand-name) "." mode "." error-index "-" num-messages "." max-message-length)
                        from (rand-name "f")
                        ids (map #(str "id-" (cond-> % (= % error-index) (str "." mode))) (range num-messages))
                        request-headers (assoc request-headers "x-cid" correlation-id)
                        _ (log/info "collect packages cid" correlation-id "for"
                                    {:iteration iteration :max-message-length max-message-length})
                        grpc-client (initialize-grpc-client correlation-id host h2c-port)
                        rpc-result (.collectPackages grpc-client request-headers ids from messages 1 true (inc num-messages))
                        message-summaries (.result rpc-result)
                        ^Status status (.status rpc-result)
                        assertion-message (str (cond-> {:correlation-id correlation-id
                                                        :error-index error-index
                                                        :iteration iteration
                                                        :service-id service-id
                                                        :summaries (map (fn [s]
                                                                          {:num-messages (.getNumMessages s)
                                                                           :total-length (.getTotalLength s)})
                                                                        message-summaries)}
                                                 status (assoc :status {:code (-> status .getCode str)
                                                                        :description (.getDescription status)})))
                        expected-summary-count error-index]
                    (log/info "result" assertion-message)
                    (is (= expected-summary-count (count message-summaries)) assertion-message)
                    (when (seq message-summaries)
                      (is (= (range 1 (inc expected-summary-count))
                             (map #(.getNumMessages %) message-summaries))
                          assertion-message)
                      (is (= (reductions + (map count (take expected-summary-count messages)))
                             (map #(.getTotalLength %) message-summaries))
                          assertion-message))
                    (is status assertion-message)
                    (when status
                      (is (= "CANCELLED" (-> status .getCode str)) assertion-message)
                      (is (= "Cancelled by server" (.getDescription status)) assertion-message))))))))))))

(deftest ^:parallel ^:integration-fast test-grpc-client-streaming-successful
  (testing-using-waiter-url
    (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
      (with-service-cleanup
        service-id
        (doseq [max-message-length [1000 100000]]
          (let [num-messages 120
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]

            (testing (str max-message-length " messages completion")
              (log/info "starting streaming to and from server - independent mode test")
              (let [cancel-threshold (inc num-messages)
                    from (rand-name "f")
                    correlation-id (rand-name)
                    request-headers (assoc request-headers "x-cid" correlation-id)
                    ids (map #(str "id-" %) (range num-messages))
                    grpc-client (initialize-grpc-client correlation-id host h2c-port)
                    rpc-result (.aggregatePackages grpc-client request-headers ids from messages 10 cancel-threshold)
                    ^CourierSummary summary (.result rpc-result)
                    ^Status status (.status rpc-result)
                    assertion-message (str (cond-> {:correlation-id correlation-id
                                                    :service-id service-id}
                                             summary (assoc :summary {:num-messages (.getNumMessages summary)
                                                                      :total-length (.getTotalLength summary)})
                                             status (assoc :status {:code (-> status .getCode str)
                                                                    :description (.getDescription status)})))]
                (log/info correlation-id "aggregated packages...")
                (is summary)
                (when summary
                  (is (= (count messages) (.getNumMessages summary)) assertion-message)
                  (is (= (reduce + (map count messages)) (.getTotalLength summary)) assertion-message))
                (is status)
                (when status
                  (is (= "OK" (-> status .getCode str)) assertion-message)
                  (is (str/blank? (.getDescription status)) assertion-message))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-client-streaming-server-exit
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3]
      (doseq [max-message-length [1000 100000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (doseq [mode ["EXIT_PRE_RESPONSE" "EXIT_POST_RESPONSE"]]
              (testing (str "lock-step mode " max-message-length " messages " mode)
                (let [{:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                  (with-service-cleanup
                    service-id
                    (let [exit-index (* iteration (/ num-messages num-iterations))
                          correlation-id (str (rand-name) "." mode "." exit-index "-" num-messages "." max-message-length)
                          _ (log/info "aggregate packages cid" correlation-id "for"
                                      {:iteration iteration :max-message-length max-message-length})
                          from (rand-name "f")
                          ids (map #(str "id-" (cond-> % (= % exit-index) (str "." mode))) (range num-messages))
                          request-headers (assoc request-headers "x-cid" correlation-id)
                          grpc-client (initialize-grpc-client correlation-id host h2c-port)
                          rpc-result (.aggregatePackages grpc-client request-headers ids from messages 1 (inc num-messages))
                          ^CourierSummary message-summary (.result rpc-result)
                          ^Status status (.status rpc-result)
                          assertion-message (str (cond-> {:correlation-id correlation-id
                                                          :exit-index exit-index
                                                          :iteration iteration
                                                          :service-id service-id}
                                                   message-summary (assoc :summary {:num-messages (.getNumMessages message-summary)
                                                                                    :total-length (.getTotalLength message-summary)})
                                                   status (assoc :status {:code (-> status .getCode str)
                                                                          :description (.getDescription status)})))]
                      (log/info "result" assertion-message)
                      (is (nil? message-summary) assertion-message)
                      (is status assertion-message)
                      (when status
                        (is (contains? #{"UNAVAILABLE" "INTERNAL"} (-> status .getCode str)) assertion-message)))))))))))))

(deftest ^:parallel ^:integration-slow test-grpc-client-streaming-server-cancellation
  (testing-using-waiter-url
    (let [num-messages 120
          num-iterations 3]
      (doseq [max-message-length [1000 100000]]
        (let [messages (doall (repeatedly num-messages #(rand-str (inc (rand-int max-message-length)))))]
          (dotimes [iteration num-iterations]
            (testing (str max-message-length " messages server error")
              (let [mode "SEND_ERROR"
                    {:keys [h2c-port host request-headers service-id]} (start-courier-instance waiter-url)]
                (with-service-cleanup
                  service-id
                  (let [error-index (* iteration (/ num-messages num-iterations))
                        correlation-id (str (rand-name) "." mode "." error-index "-" num-messages "." max-message-length)
                        from (rand-name "f")
                        ids (map #(str "id-" (cond-> % (= % error-index) (str "." mode))) (range num-messages))
                        request-headers (assoc request-headers "x-cid" correlation-id)
                        _ (log/info "aggregate packages cid" correlation-id "for"
                                    {:iteration iteration :max-message-length max-message-length})
                        grpc-client (initialize-grpc-client correlation-id host h2c-port)
                        rpc-result (.aggregatePackages grpc-client request-headers ids from messages 1 (inc num-messages))
                        ^CourierSummary message-summary (.result rpc-result)
                        ^Status status (.status rpc-result)
                        assertion-message (str (cond-> {:correlation-id correlation-id
                                                        :error-index error-index
                                                        :iteration iteration
                                                        :service-id service-id}
                                                 message-summary (assoc :summary {:num-messages (.getNumMessages message-summary)
                                                                                  :total-length (.getTotalLength message-summary)})
                                                 status (assoc :status {:code (-> status .getCode str)
                                                                        :description (.getDescription status)})))]
                    (log/info "result" assertion-message)
                    (is (nil? message-summary) assertion-message)
                    (is status assertion-message)
                    (when status
                      (is (= "CANCELLED" (-> status .getCode str)) assertion-message)
                      (is (= "Cancelled by server" (.getDescription status)) assertion-message))))))))))))
