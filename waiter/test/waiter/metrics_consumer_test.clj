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
(ns waiter.metrics-consumer-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [waiter.kv :as kv]
            [waiter.metrics-consumer :refer :all]
            [waiter.token :as tk]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu])
  (:import (org.joda.time DateTime)))

(def ^:const history-length 5)
(def ^:const limit-per-owner 10)

(let [lock (Object.)]
  (defn- synchronize-fn
    [_ f]
    (locking lock
      (f))))

(let [current-time (t/now)]
  (defn- clock [] current-time)

  (defn- clock-millis [] (.getMillis ^DateTime (clock))))

(defn drain-channel!
  "Eagerly polls as many messages from a channel until unsuccessful"
  [chan]
  (loop [msg (async/poll! chan)
         result []]
    (if (nil? msg)
      result
      (recur (async/poll! chan)
             (conj result msg)))))

(defmacro assert-expected-blobs-from-event-chan-and-cleanup
  [body-chan-messages expected-blobs body-chan event-chan error-chan exit-chan go-chan expected-endpoint query-state-fn]
  `(let [body-chan-messages# ~body-chan-messages
         expected-blobs# ~expected-blobs
         body-chan# ~body-chan
         event-chan# ~event-chan
         error-chan# ~error-chan
         exit-chan# ~exit-chan
         go-chan# ~go-chan
         expected-endpoint# ~expected-endpoint
         query-state-fn# ~query-state-fn]
     ; push all the json string fragments to the body-chan
     (doseq [json-str# body-chan-messages#]
       (async/>!! body-chan# json-str#))

     ; expect each blob to be in event-chan and properly mapped
     (doseq [expected-blob# expected-blobs#]
       (let [blob# (async/<!! event-chan#)]
         (is (= expected-blob# blob#))))

     ; last-event-time and last-watch-time should be set to the clock due to new events
     (is (= {:endpoint expected-endpoint#
             :last-event-time (clock)
             :last-watch-time (clock)
             :supported-include-params []}
            (query-state-fn# #{})))

     (async/close! body-chan#)
     (async/close! error-chan#)
     (async/>!! exit-chan# :exit)
     (async/<!! go-chan#)
     (async/close! event-chan#)))

(deftest test-make-metrics-watch-request
  (let [http-client (hu/http-client-factory {})
        retry-delay-ms 100
        body-chan-messages ["{\"type\":\"initial\",\"object\":[{\"token\":\"t1\",\"lastRequestTime\":\"1\"}]}"
                            "{\"type\":\"unknown\",\"object\":[{\"token\":\"t6\",\"lastRequestTime\":\"1\"},{\"token\":\"t5\",\"lastRequestTime\":\"1\"}]}"
                            "{\"type\":\"update\",\"object\":[{\"token\":\"t2\",\"lastRequestTime\":\"1\"},{\"token\":\"t3\",\"lastRequestTime\":\"1\"}]}"]
        ; t5 and t6 are filtered out because the event type was "unknown"
        expected-blobs [{"token" "t1"
                         "lastRequestTime" "1"}
                        {"token" "t2"
                         "lastRequestTime" "1"}
                        {"token" "t3"
                         "lastRequestTime" "1"}]]

    (testing "json fragments are parsed and immediately pushed to the event-chan if valid"
      (let [body-chan (async/chan 100)
            event-chan (async/chan 100)
            error-chan (async/promise-chan)
            expected-metrics-service-url "http://metrics-service-url.com"
            expected-endpoint (str expected-metrics-service-url "/token-stats")
            http-streaming-request-async-fn (constantly (async/go {:body-chan body-chan :error-chan error-chan}))
            {:keys [exit-chan go-chan query-state-fn]}
            (make-metrics-watch-request
              http-client http-streaming-request-async-fn "router-id" "cur-cid" expected-metrics-service-url event-chan clock
              retry-delay-ms)]

        (assert-expected-blobs-from-event-chan-and-cleanup
          body-chan-messages expected-blobs body-chan event-chan error-chan exit-chan go-chan expected-endpoint query-state-fn)))

    (testing "watch requests are retried after immediate error when making the request"
      (let [http-client (hu/http-client-factory {})
            body-chan (async/chan 100)
            event-chan (async/chan 100)
            error-chan (async/promise-chan)
            expected-metrics-service-url "http://metrics-service-url.com"
            expected-endpoint (str expected-metrics-service-url "/token-stats")
            call-count (atom 0)
            http-streaming-request-async-fn
            (fn [_ _ & {}]
              (async/go
                (swap! call-count inc)
                (if (> @call-count 1)
                  {:body-chan body-chan :error-chan error-chan}
                  (ex-info "error immediately at first" {}))))

            {:keys [exit-chan go-chan query-state-fn]}
            (make-metrics-watch-request
              http-client http-streaming-request-async-fn "router-id" "cur-cid" expected-metrics-service-url event-chan clock
              retry-delay-ms)]

        (assert-expected-blobs-from-event-chan-and-cleanup
          body-chan-messages expected-blobs body-chan event-chan error-chan exit-chan go-chan expected-endpoint query-state-fn)))

    (testing "watch requests are retried after error while streaming"
      (let [http-client (hu/http-client-factory {})
            body-chan (async/chan 100)
            bad-body-chan (async/chan 100)
            event-chan (async/chan 100)
            error-chan (async/promise-chan)
            expected-metrics-service-url "http://metrics-service-url.com"
            expected-endpoint (str expected-metrics-service-url "/token-stats")
            call-count (atom 0)
            http-streaming-request-async-fn
            (fn [_ _ & {}]
              (async/go
                (swap! call-count inc)
                {:body-chan (if (> @call-count 1) body-chan bad-body-chan)
                 :error-chan error-chan}))

            {:keys [exit-chan go-chan query-state-fn]}
            (make-metrics-watch-request
              http-client http-streaming-request-async-fn "router-id" "cur-cid" expected-metrics-service-url event-chan clock
              retry-delay-ms)]

        ; push unfinished fragment to json string and close the body chan
        (async/>!! bad-body-chan "{\"type\":\"initial\",\"object\":")
        (async/close! bad-body-chan)

        (assert-expected-blobs-from-event-chan-and-cleanup
          body-chan-messages expected-blobs body-chan event-chan error-chan exit-chan go-chan expected-endpoint query-state-fn)))))

(deftest test-start-metrics-consumer-maintainer
  (let [default-cluster "cluster1"
        unknown-cluster "not-default-cluster"
        kv-store (kv/->LocalKeyValueStore (atom {}))
        token-metric-chan-buffer-size 1000
        connection-timeout-ms 1001
        idle-timeout-ms 1002
        http-client (hu/http-client-factory {:conn-timeout connection-timeout-ms :socket-timeout idle-timeout-ms})

        token-different-cluster "token-different-cluster"
        token-different-cluster-service-desc {"cpus" 1 "run-as-user" "user-1"}
        token-different-cluster-metadata {"cluster" unknown-cluster "last-update-time" (clock) "owner" "owner1"}
        _ (tk/store-service-description-for-token
            synchronize-fn kv-store history-length limit-per-owner token-different-cluster token-different-cluster-service-desc
            token-different-cluster-metadata)

        valid-bypass-token "valid-bypass-token"
        valid-bypass-token-service-desc {"cpus" 1 "run-as-user" "user-1"}
        valid-bypass-token-metadata {"cluster" default-cluster "last-update-time" (clock) "owner" "owner1"}
        _ (tk/store-service-description-for-token
            synchronize-fn kv-store history-length limit-per-owner valid-bypass-token valid-bypass-token-service-desc
            valid-bypass-token-metadata)

        token-run-as-requester "token-run-as-requester"
        token-run-as-requester-service-desc {"cpus" 1 "run-as-user" "*"}
        token-run-as-requester-metadata {"cluster" default-cluster "last-update-time" (clock) "owner" "owner1"}
        _ (tk/store-service-description-for-token
            synchronize-fn kv-store history-length limit-per-owner token-run-as-requester token-run-as-requester-service-desc
            token-run-as-requester-metadata)

        token-parameterized "token-run-as-requester"
        token-parameterized-service-desc {"cpus" 1 "run-as-user" "user-1" "allowed-params" ["test"]}
        token-parameterized-metadata {"cluster" default-cluster "last-update-time" (clock) "owner" "owner1"}
        _ (tk/store-service-description-for-token
            synchronize-fn kv-store history-length limit-per-owner token-parameterized token-parameterized-service-desc
            token-parameterized-metadata)

        token-not-stored "token-not-stored"

        token-cluster-calculator (tk/new-configured-cluster-calculator {:default-cluster default-cluster
                                                                        :host->cluster {}})
        retrieve-descriptor-fn (fn [_ _]
                                 ; latest descriptor not used, so no need to specify it
                                 {:descriptor {:service-id "service-id-1"}})
        make-service-id->metrics-fn (fn make-service-id->metrics-fn
                                      [local-agent]
                                      (fn []
                                        (await local-agent)
                                        @local-agent))
        router-id "router-id-1"
        metrics-service-urls ["https://metrics-service1.com" "https://metrics-service2.com"]
        retry-delay-ms 1003
        new-last-request-time (t/plus (clock) (t/seconds 1))
        make-metrics-watch-request-factory-fn
        (fn make-metrics-watch-request-factory-fn
          [raw-events]
          (let [trigger-ch (async/promise-chan)]
            {:make-metrics-watch-request-fn
             (fn make-metrics-watch-request-fn
               [http-client http-fn actual-router-id _ actual-metrics-service-url token-metric-chan _ actual-retry-delay-ms]
               (is (= router-id actual-router-id))
               (is (contains? (set metrics-service-urls) actual-metrics-service-url))
               (is (= hu/http-streaming-request-async http-fn))
               (is (= connection-timeout-ms (.getConnectTimeout http-client)))
               (is (= idle-timeout-ms (.getIdleTimeout http-client)))
               (is (= retry-delay-ms actual-retry-delay-ms))
               {:exit-chan (async/promise-chan)
                :go-chan (async/go
                           (async/<! trigger-ch)
                           (doseq [event raw-events]
                             (async/put! token-metric-chan event)))
                :query-state-fn (constantly {})})
             :trigger-ch trigger-ch}))]

    (testing "token-metric-chan supports multiple metrics services"
      (let [local-usage-agent (agent {"service-id-1" {"last-request-time" (clock)}})
            raw-events [{"token" token-different-cluster
                         "lastRequestTime" (du/date-to-str (clock))}
                        {"token" token-run-as-requester
                         "lastRequestTime" (du/date-to-str (clock))}
                        {"token" token-parameterized
                         "lastRequestTime" (du/date-to-str (clock))}
                        {"token" token-not-stored
                         "lastRequestTime" (du/date-to-str (clock))}
                        {"token" valid-bypass-token
                         "lastRequestTime" (du/date-to-str new-last-request-time)}]
            {:keys [make-metrics-watch-request-fn trigger-ch]} (make-metrics-watch-request-factory-fn raw-events)
            {:keys [exit-chan query-state-fn token-metric-chan-mult]}
            (start-metrics-consumer-maintainer
              http-client clock kv-store token-cluster-calculator retrieve-descriptor-fn (make-service-id->metrics-fn local-usage-agent)
              make-metrics-watch-request-fn local-usage-agent router-id metrics-service-urls token-metric-chan-buffer-size
              retry-delay-ms)
            listener-ch (async/chan 1)]
        (async/tap token-metric-chan-mult listener-ch)
        (async/>!! trigger-ch :start)
        (async/<!! (async/timeout 500))
        ; shows up once with multiple metrics service because "service-id->metrics-fn" got updated and remove duplicate event
        (is (= [{:token valid-bypass-token
                 :last-request-time new-last-request-time}]
               (drain-channel! listener-ch)))
        (is (= {:buffer-state {:token-metric-chan-count 0}
                :last-token-event-time (clock)
                :supported-include-params ["watches-state"]
                :watches-state (pc/map-from-keys (constantly {}) metrics-service-urls)}
               (query-state-fn #{"watches-state"})))
        (await local-usage-agent)
        (is (= new-last-request-time
               (get-in @local-usage-agent ["service-id-1" "last-request-time"])))
        (async/>!! exit-chan :exit)))

    (testing "token-metric-chan does not update last-request-time if not strictly after the previous last-request-time"
      (let [local-usage-agent (agent {"service-id-1" {"last-request-time" (clock)}})
            raw-events [{"token" valid-bypass-token
                         ; last-request-time is fixed to current time
                         "lastRequestTime" (du/date-to-str (clock))}
                        {"token" valid-bypass-token
                         ; last-request-time is fixed to previous time
                         "lastRequestTime" (du/date-to-str (t/minus (clock) (t/seconds 1)))}]
            {:keys [make-metrics-watch-request-fn trigger-ch]} (make-metrics-watch-request-factory-fn raw-events)
            {:keys [exit-chan query-state-fn token-metric-chan-mult]}
            (start-metrics-consumer-maintainer
              http-client clock kv-store token-cluster-calculator retrieve-descriptor-fn (make-service-id->metrics-fn local-usage-agent)
              make-metrics-watch-request-fn local-usage-agent router-id metrics-service-urls token-metric-chan-buffer-size
              retry-delay-ms)
            listener-ch (async/chan 1000)]
        (async/tap token-metric-chan-mult listener-ch)
        (async/>!! trigger-ch :start)
        (async/<!! (async/timeout 500))
        (is (= [] (drain-channel! listener-ch)))
        (is (= {:buffer-state {:token-metric-chan-count 0}
                :last-token-event-time nil
                :supported-include-params ["watches-state"]
                :watches-state (pc/map-from-keys (constantly {}) metrics-service-urls)}
               (query-state-fn #{"watches-state"})))
        (await local-usage-agent)
        (is (= (clock)
               (get-in @local-usage-agent ["service-id-1" "last-request-time"])))
        (async/>!! exit-chan :exit)))))
