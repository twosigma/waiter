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

(defn- basic-grpc-service-parameters
  []
  (let [courier-command (courier-server-command "${PORT0} ${PORT1}")]
    (walk/stringify-keys
      {:x-waiter-backend-proto "h2c"
       :x-waiter-cmd courier-command
       :x-waiter-cmd-type "shell"
       :x-waiter-cpus 0.2
       :x-waiter-debug true
       :x-waiter-grace-period-secs 120
       :x-waiter-health-check-port-index 1
       :x-waiter-health-check-proto "http"
       :x-waiter-idle-timeout-mins 10
       :x-waiter-mem 512
       :x-waiter-name (rand-name)
       :x-waiter-ports 2
       :x-waiter-version "version-does-not-matter"})))

(defn- rand-str
  "Generates a random string with the specified length."
  [length]
  (apply str (take length (repeatedly #(char (+ (rand 26) 65))))))

(deftest ^:parallel ^:integration-fast test-basic-grpc-server
  (testing-using-waiter-url
    (GrpcClient/setLogFunction (reify Function
                                 (apply [_ message]
                                   (log/info message))))
    (let [[host _] (str/split waiter-url #":")
          h2c-port (Integer/parseInt (retrieve-h2c-port waiter-url))
          request-headers (basic-grpc-service-parameters)
          {:keys [cookies headers] :as response} (make-request waiter-url "/waiter-ping" :headers request-headers)
          cookie-header (str/join "; " (map #(str (:name %) "=" (:value %)) cookies))
          service-id (get headers "x-waiter-service-id")]

      (assert-response-status response 200)
      (is service-id)

      (with-service-cleanup
        service-id

        (let [{:keys [ping-response service-state]} (some-> response :body try-parse-json walk/keywordize-keys)]
          (is (= "received-response" (:result ping-response)) (str ping-response))
          (is (str/starts-with? (str (some-> ping-response :headers :server)) "courier-health-check"))
          (assert-response-status ping-response 200)
          (is (or (= {:exists? true :healthy? true :service-id service-id :status "Running"} service-state)
                  (= {:exists? true :healthy? false :service-id service-id :status "Starting"} service-state))
              (str service-state)))

        (testing "small request and reply"
          (log/info "starting small request and reply test")
          (let [id (rand-name "m")
                from (rand-name "f")
                content (rand-str 1000)]

            (testing "successful"
              (let [request-headers (assoc request-headers "cookie" cookie-header "x-cid" (rand-name))
                    reply (GrpcClient/sendPackage host h2c-port request-headers id from content 0 30000)]
                (is reply)
                (when reply
                  (is (= id (.getId reply)))
                  (is (= content (.getMessage reply)))
                  (is (= "received" (.getResponse reply))))))

            (testing "timeout"
              (let [request-headers (assoc request-headers "cookie" cookie-header "x-cid" (rand-name))
                    reply (GrpcClient/sendPackage host h2c-port request-headers id from content 10000 5000)]
                (is reply)
                (when reply
                  (is (= "java.util.concurrent.ExecutionException" (.getId reply)))
                  (is (str/includes? (.getMessage reply) "io.grpc.StatusRuntimeException"))
                  (is (str/includes? (.getMessage reply) "DEADLINE_EXCEEDED"))
                  (is (str/includes? (.getMessage reply) "deadline exceeded after"))
                  (is (= "ERROR" (.getResponse reply))))))

            (testing "instance not blacklisted"
              (let [request-headers (assoc request-headers "cookie" cookie-header "x-cid" (rand-name))
                    blacklist-time-ms (setting waiter-url [:blacklist-config :blacklist-backoff-base-time-ms])
                    deadline-ms (-> blacklist-time-ms (- 1000) (max 3000))
                    reply (GrpcClient/sendPackage host h2c-port request-headers id from content 0 deadline-ms)]
                (is reply)
                (when reply
                  (is (= id (.getId reply)))
                  (is (= content (.getMessage reply)))
                  (is (= "received" (.getResponse reply))))))))

        (comment testing "streaming to and from server"
          (doseq [max-message-length [100 1000 10000 100000]]
            (let [messages (doall (repeatedly 200 #(rand-str (inc (rand-int max-message-length)))))]

              (testing (str "independent mode " max-message-length " messages")
                (log/info "starting streaming to and from server - independent mode test")
                (let [from (rand-name "f")
                      request-headers (assoc request-headers "cookie" cookie-header "x-cid" (rand-name))
                      summaries (GrpcClient/collectPackages host h2c-port request-headers "m-" from messages 0 1 false)]
                  (is (= (count messages) (count summaries)))
                  (when (seq summaries)
                    (is (= (range 1 (inc (count messages))) (map #(.getNumMessages %) summaries)))
                    (is (= (reductions + (map count messages)) (map #(.getTotalLength %) summaries))))))

              (testing (str "lock-step mode " max-message-length " messages")
                (log/info "starting streaming to and from server - lock-step mode test")
                (let [from (rand-name "f")
                      request-headers (assoc request-headers "cookie" cookie-header "x-cid" (rand-name))
                      summaries (GrpcClient/collectPackages host h2c-port request-headers "m-" from messages 0 1 true)]
                  (is (= (count messages) (count summaries)))
                  (when (seq summaries)
                    (is (= (range 1 (inc (count messages))) (map #(.getNumMessages %) summaries)))
                    (is (= (reductions + (map count messages)) (map #(.getTotalLength %) summaries)))))))))))))
