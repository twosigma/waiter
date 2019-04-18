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
  (:import (com.twosigma.waiter.courier CourierReply CourierSummary GrpcClient)
           (java.util.function Function)
           (io.grpc Status$Code)))

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
          {:keys [cookies headers service-id] :as response}
          (make-request-with-debug-info request-headers #(make-request waiter-url "/bad-grpc-endpoint" :headers %))
          cookie-header (str/join "; " (map #(str (:name %) "=" (:value %)) cookies))]
      (with-service-cleanup
        service-id

        (is service-id)
        (assert-response-status response 415)
        (is (= "Content-Type is missing from the request" (get headers "grpc-message")))
        (is (= (str (.value Status$Code/INTERNAL)) (get headers "grpc-status")))

        (testing "small request and reply"
          (let [id (rand-name "m")
                from (rand-name "f")
                content (rand-str 1000)
                request-headers (assoc request-headers "cookie" cookie-header "x-cid" (rand-name))
                ^CourierReply reply (GrpcClient/sendPackage host h2c-port request-headers id from content)]
            (is reply)
            (when reply
              (is (= id (.getId reply)))
              (is (= content (.getMessage reply)))
              (is (= "received" (.getResponse reply))))))

        (testing "streaming to and from server"
          (let [from (rand-name "f")
                num-messages 200
                message-length 500000
                messages (doall (repeatedly num-messages #(rand-str (inc (rand-int message-length)))))
                request-headers (assoc request-headers "cookie" cookie-header "x-cid" (rand-name))
                ^CourierSummary summary (GrpcClient/collectPackages host h2c-port request-headers "m-" from messages 1)]
            (is summary)
            (when summary
              (is (= num-messages (.getNumMessages summary)))
              (is (= (reduce + (map count messages)) (.getTotalLength summary))))))))))
