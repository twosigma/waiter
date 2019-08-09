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
(ns grpc.grpc-client-exit
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (com.twosigma.waiter.courier GrpcClient GrpcClient$CancellationPolicy)
           (java.util.function Function)))

(defn- initialize-grpc-client
  "Initializes grpc client logging to specific correlation id"
  [correlation-id host port]
  (let [date-formatter (:date-hour-minute-second-fraction f/formatters)
        log-function (reify Function
                       (apply [_ message]
                         (println (du/date-to-str (t/now) date-formatter) correlation-id message)))]
    (GrpcClient. host port log-function)))

(try
  (println "num command line arguments:" (count *command-line-args*))
  (let [[_ host h2c-port service-id correlation-id cookie] *command-line-args*
        token (str "^SERVICE-ID#" service-id)
        port (Integer/parseInt h2c-port)
        grpc-client (initialize-grpc-client correlation-id host port)
        request-headers {"cookie" cookie
                         "x-cid" correlation-id
                         "x-waiter-token" token}
        num-messages 100
        ids (map #(str "id-" %) (range num-messages))
        from "from"
        messages (map #(str "message-" %) (range num-messages))
        inter-message-sleep-ms 100
        lock-step-mode true
        cancel-threshold (/ num-messages 2)
        cancellation-policy GrpcClient$CancellationPolicy/EXIT
        deadline-duration-ms 60000]
    (println "host:" host)
    (println "port:" port)
    (println "request-headers:" (update request-headers "cookie" utils/truncate 20))
    (println "num-messages:" num-messages)
    (println "cancel-threshold:" cancel-threshold)
    (println "invoking GrpcClient method...")
    (.collectPackages grpc-client request-headers ids from messages inter-message-sleep-ms lock-step-mode
                      cancel-threshold cancellation-policy deadline-duration-ms))
  (catch Throwable throwable
    (.printStackTrace throwable System/out))
  (finally
    (System/exit 0)))