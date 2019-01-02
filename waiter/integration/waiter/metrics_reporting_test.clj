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
(ns waiter.metrics-reporting-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [waiter.schema :as schema]
            [waiter.util.client-tools :refer :all])
  (:import [java.net ServerSocket]))

(defn- get-graphite-reporter-state
  []
  (let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)
        _ (assert-response-status response 200)
        {{:keys [graphite]} "state"} (-> body str try-parse-json)]
    graphite))

(defn- parse-date-time
  [value]
  (if value
    (f/parse (f/formatters :date-time) value)
    nil))

(deftest ^:parallel ^:integration-fast test-graphite-metrics-reporting
  (testing-using-waiter-url
    (let [{{{:keys [graphite]} :reporters} :metrics-config} (waiter-settings waiter-url)]
      (when graphite
        (let [{:keys [period-ms port]} graphite
              wait-for-delay (/ period-ms 2)
              graphite-reporter-active (wait-for #(let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)
                                                        _ (assert-response-status response 200)
                                                        {{{:strs [last-report-successful]} "graphite"} "state"} (-> body str try-parse-json)] (some? last-report-successful))
                                                 :interval wait-for-delay :timeout (* period-ms 2) :unit-multiplier 1)]
          (is (some? graphite-reporter-active))
          (let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)
                _ (assert-response-status response 200)
                {{{:strs [last-report-successful last-connect-failed-time]} "graphite"} "state"} (-> body str try-parse-json)
                _ (is (some? last-connect-failed-time))
                last-connect-failed-time-ms (.getMillis (parse-date-time last-connect-failed-time))
                next-last-connect-failed-time-ms (wait-for #(let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)
                                                                  _ (assert-response-status response 200)
                                                                  {{{:strs [last-connect-failed-time]} "graphite"} "state"} (-> body str try-parse-json)
                                                                  next-last-connect-failed-time-ms (.getMillis (parse-date-time last-connect-failed-time))]
                                                              (if (not= next-last-connect-failed-time-ms last-connect-failed-time-ms)
                                                                next-last-connect-failed-time-ms nil))
                                                           :interval wait-for-delay :timeout (* period-ms 2) :unit-multiplier 1)]

            (is (some? next-last-connect-failed-time-ms))
            (if (some? next-last-connect-failed-time-ms)
              (is (< (Math/abs (- next-last-connect-failed-time-ms last-connect-failed-time-ms period-ms)) 100)))


            (let [ctrl (async/chan)]
              (async/go (with-open [server-socket (ServerSocket. port)
                                    socket (.accept server-socket)]
                          (async/<! ctrl)
                          ))


              (wait-for #(let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)
                               _ (assert-response-status response 200)
                               {{{:strs [last-report-successful]} "graphite"} "state"} (-> body str try-parse-json)] last-report-successful)
                        :interval wait-for-delay :timeout (* period-ms 2) :unit-multiplier 1)

              (let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)
                    _ (assert-response-status response 200)
                    {{{:strs [last-report-successful last-reporting-time]} "graphite"} "state"} (-> body str try-parse-json)
                    _ (is last-report-successful)
                    last-reporting-time-ms (.getMillis (parse-date-time last-reporting-time))
                    next-last-reporting-time-ms (wait-for #(let [{:keys [body] :as response} (make-request waiter-url "/state/metrics-reporters" :method :get)
                                                                 _ (assert-response-status response 200)
                                                                 {{{:strs [last-reporting-time]} "graphite"} "state"} (-> body str try-parse-json)
                                                                 next-last-reporting-time-ms (.getMillis (parse-date-time last-reporting-time))]
                                                             (if (not= next-last-reporting-time-ms last-reporting-time-ms)
                                                               next-last-reporting-time-ms nil))
                                                          :interval wait-for-delay :timeout (* period-ms 2) :unit-multiplier 1)]


                (is (some? next-last-reporting-time-ms))
                (if (some? next-last-reporting-time-ms)
                  (is (< (Math/abs (- next-last-reporting-time-ms last-reporting-time-ms period-ms)) 100)))

                )
              (async/>!! ctrl true)
              )

            ;(with-open [ss (ServerSocket. port)
            ;            sock (.accept ss)
            ;            reader (io/reader sock)]
            ;
            ;  (println (.readLine reader))
            ;  ;(doseq [msg-in (#(line-seq (io/reader %)) sock)]
            ;  ;  (println msg-in)
            ;  ;  (.close sock))
            ;  )

            ;(let [ss (ServerSocket. port)
            ;      sock (.accept ss)
            ;      reader (io/reader sock)]
            ;
            ;  (println (.readLine reader))
            ;
            ;    (doseq [msg-in (#(line-seq (io/reader %)) sock)]
            ;      (println msg-in))
            ;  (.setKeepAlive sock false)
            ;  (.close reader)
            ;  (.close sock)
            ;  (.close ss)
            ;  )
            ;
            ;(with-open [sock (.accept (ServerSocket. port))
            ;            reader (io/reader sock)]
            ;
            ;  (println (.readLine reader))
            ;  ;(doseq [msg-in (#(line-seq (io/reader %)) sock)]
            ;  ;  (println msg-in)
            ;  ;  (.close sock))
            ;  (.setKeepAlive sock false)
            ;  (.close reader)
            ;  (.close sock)
            ;  )


            ;(println (get-graphite-reporter-state))
            ))))))