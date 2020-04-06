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
(ns waiter.metrics-sync-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [qbits.jet.client.websocket :as ws]
            [waiter.metrics :as metrics]
            [waiter.metrics-sync :refer :all]
            [waiter.test-helpers :as test-helpers]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (org.eclipse.jetty.websocket.client WebSocketClient)
           (org.joda.time DateTime)
           (qbits.jet.websocket WebSocket)))

(defn- keyset
  "Returns the keys of the map as a set."
  [m]
  (-> m keys set))

(defn- retrieve-agent-state
  "Awaits all actions dispatched thus far to the agent to complete and then returns the state of the agent."
  [router-metrics-agent]
  (await router-metrics-agent)
  @router-metrics-agent)

(deftest test-deregister-router-ws
  (testing "deregister-router-ws:matching-request-id"
    (let [router-ws-key :ws-requests-key
          router-id "router-1"
          request-id "request-1"
          out-chan (async/chan 10)
          ws-request {:out out-chan, :request-id request-id}
          in-router-metrics-state {:ws-requests-key {router-id ws-request, :foo :bar}, :fee :fie}
          encrypt identity
          out-router-metrics-state (deregister-router-ws in-router-metrics-state router-ws-key router-id request-id encrypt)]
      (is (= {:ws-requests-key {:foo :bar}, :fee :fie} out-router-metrics-state))
      (is (= {:message "deregistering existing websocket request"} (async/<!! out-chan)))
      (is (nil? (async/<!! out-chan)) "Channel is closed.")))

  (testing "deregister-router-ws:different-request-id"
    (let [router-ws-key :ws-requests-key
          router-id "router-1"
          request-id "request-1"
          out-chan (async/chan 10)
          ws-request {:out out-chan, :request-id (str request-id "A")}
          in-router-metrics-state {:ws-requests-key {router-id ws-request, :foo :bar}, :fee :fie}
          encrypt identity
          out-router-metrics-state (deregister-router-ws in-router-metrics-state router-ws-key router-id request-id encrypt)]
      (is (= in-router-metrics-state out-router-metrics-state))
      (is (async/>!! out-chan {:message "successful-put-to-ensure-channel-is-open"}))
      (is (= {:message "successful-put-to-ensure-channel-is-open"} (async/<!! out-chan))))))

(deftest test-register-router-ws
  (testing "register-router-ws:matching-request-id"
    (let [router-ws-key :ws-requests-key
          router-id "router-1"
          ctrl-chan (async/chan)
          ws-request {:ctrl ctrl-chan, :out :baz, :request-id "request-1"}
          initial-state {:ws-requests-key {:foo :bar} :fee :fie}
          in-router-metrics-state initial-state
          router-metrics-agent (agent in-router-metrics-state)
          encrypt identity
          out-router-metrics-state (register-router-ws in-router-metrics-state router-ws-key router-id ws-request encrypt router-metrics-agent)]
      (is (= (assoc-in initial-state [:ws-requests-key router-id] ws-request) out-router-metrics-state))
      (async/close! ctrl-chan)))

  (testing "register-router-ws:missing-request-id"
    (let [router-ws-key :ws-requests-key
          router-id "router-1"
          ws-request {:out :baz}
          initial-state {:ws-requests-key {:foo :bar} :fee :fie}
          in-router-metrics-state initial-state
          router-metrics-agent (agent in-router-metrics-state)
          encrypt identity
          out-router-metrics-state (register-router-ws in-router-metrics-state router-ws-key router-id ws-request encrypt router-metrics-agent)]
      (is (= initial-state, out-router-metrics-state)))))

(deftest test-register-router-ws-with-ctrl-chan
  (testing "register-router-ws:deregistering-on-ctrl-chan"
    (let [router-ws-key :ws-requests-key
          router-id "router-1"
          ws-request {:ctrl (async/chan 1), :in (async/chan 1), :out (async/chan 1), :request-id "request-1"}
          in-router-metrics-state {:ws-requests-key {:foo :bar} :fee :fie}
          router-metrics-agent (agent in-router-metrics-state)

          encrypt identity]
      (send router-metrics-agent register-router-ws router-ws-key router-id ws-request encrypt router-metrics-agent)
      (let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)]
        (is (= {:ws-requests-key {:foo :bar, router-id ws-request}, :fee :fie} out-router-metrics-state)))
      (async/>!! (:ctrl ws-request) :close)
      (async/<!! (:out ws-request))
      (let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)]
        (is (= {:ws-requests-key {:foo :bar}, :fee :fie} out-router-metrics-state))))))

(deftest test-update-router-metrics
  (testing "update-router-metrics:new-router-metrics"
    (let [in-router-metrics-state {:metrics {:routers {"router-1" {"s1" {"c" 1}, "s2" {"c" 1}}
                                                       "router-2" {"s1" {"c" 2}, "s2" {"c" 2}}}}
                                   :last-update-times {"router-1" :router-1-time
                                                       "router-2" :router-2-time}
                                   :fee :fie}
          out-router-metrics-state (update-router-metrics in-router-metrics-state
                                                          {:router-metrics {"s1" {"c" 3}, "s2" {"c" 3}}
                                                           :source-router-id "router-3"
                                                           :time :router-3-time})]
      (is (= (-> in-router-metrics-state
                 (update-in [:metrics :routers] assoc "router-3" {"s1" {"c" 3}, "s2" {"c" 3}})
                 (update-in [:last-update-times] assoc "router-3" :router-3-time))
             out-router-metrics-state))))

  (testing "update-router-metrics:update-router-metrics"
    (let [in-router-metrics-state {:metrics {:routers {"router-1" {"s1" {"c" 1}, "s2" {"c" 1}}
                                                       "router-2" {"s1" {"c" 2}, "s2" {"c" 2}}
                                                       "router-3" {"s1" {"c" 1}
                                                                   "s2" {"c" {"d" 1, "e" 1, "f" {"g" 1, "h" 1}}}
                                                                   "s3" {"c" 1}}}}
                                   :last-update-times {"router-1" :router-1-time
                                                       "router-2" :router-2-time
                                                       "router-3" :router-3-time-old}
                                   :fee :fie}
          out-router-metrics-state (update-router-metrics in-router-metrics-state
                                                          {:router-metrics {"s1" {"c" 3}, "s2" {"c" {"e" 3, "f" {"g" 3}}}}
                                                           :source-router-id "router-3"
                                                           :time :router-3-time})]
      (is (= (-> in-router-metrics-state
                 (update-in [:metrics :routers] assoc "router-3" {"s1" {"c" 3}, "s2" {"c" {"d" 1, "e" 3, "f" {"g" 3, "h" 1}}}})
                 (update-in [:last-update-times] assoc "router-3" :router-3-time))
             out-router-metrics-state)))))

(deftest test-preserve-metrics-from-routers
  (testing "preserve-metrics-from-routers:preserve-metrics"
    (let [in-router-metrics-state {:metrics {:routers {"router-1" {"s1" {"c" 1}}
                                                       "router-2" {"s1" {"c" 2}}
                                                       "router-3" {"s1" {"c" 3}}
                                                       "router-4" {"s1" {"c" 4}}
                                                       "router-5" {"s1" {"c" 5}}}}
                                   :last-update-times {"router-1" :router-1-time
                                                       "router-2" :router-2-time
                                                       "router-3" :router-3-time
                                                       "router-4" :router-4-time
                                                       "router-5" :router-5-time}
                                   :fee :fie}
          out-router-metrics-state (preserve-metrics-from-routers in-router-metrics-state ["router-0" "router-2" "router-4" "router-6"])]
      (is (= (-> in-router-metrics-state
                 (update-in [:metrics :routers] select-keys ["router-2" "router-4"])
                 (update-in [:last-update-times] select-keys ["router-2" "router-4"]))
             out-router-metrics-state)))))

(deftest test-publish-router-metrics
  (testing "publish-router-metrics"
    (let [test-start-time (t/now)]
      (with-redefs [t/now (constantly test-start-time)]
        (let [encrypt (fn [data] {:data data})
              router-metrics {"s1" {"c" 1}, "s2" {"c" 1}, "s3" {"c" 1}}
              ws-request-2 {:out (async/chan 10), :request-id "request-id-2"}
              ws-request-3 {:out (async/chan 10), :request-id "request-id-3"}
              ws-request-4 {:out (async/chan 10), :request-id "request-id-4"}
              in-router-metrics-state {:metrics {:routers {"router-1" {"s1" {"c" 0}, "s2" {"c" 0}, "s3" {"c" 0}}
                                                           "router-2" {"s1" {"c" 2}, "s3" {"c" 2}}
                                                           "router-3" {"s1" {"c" 3}, "s2" {"c" 3}, "s3" {"c" 3}}
                                                           "router-4" {"s1" {"c" 4}, "s4" {"c" 4}}
                                                           "router-5" {"s1" {"c" 1}, "s4" {"c" 1}}}}
                                       :router-id "router-1"
                                       :router-id->outgoing-ws {"router-2" ws-request-2
                                                                "router-3" ws-request-3
                                                                "router-4" ws-request-4}
                                       :fee :fie}
              out-router-metrics-state (publish-router-metrics in-router-metrics-state encrypt router-metrics "core")]
          (let [output-data {:data {:router-metrics router-metrics, :source-router-id "router-1", :time (du/date-to-str test-start-time)}}]
            (is (= output-data (async/<!! (:out ws-request-2))))
            (is (= output-data (async/<!! (:out ws-request-3))))
            (is (= output-data (async/<!! (:out ws-request-4)))))
          (is (= (-> in-router-metrics-state
                     (assoc-in [:metrics :routers "router-1"] router-metrics)
                     (assoc-in [:last-update-times "router-1"] (du/date-to-str test-start-time)))
                 out-router-metrics-state)))))))

(deftest test-update-metrics-router-state-no-new-nor-missing-router-ids
  (with-redefs [ws/connect! (fn ws-connect! [_ _ _ _] (throw (Exception. "Unexpected call")))]
    (let [in-router-metrics-state {:router-id "router-0"
                                   :router-id->incoming-ws {"router-1" {:out :dummy-1}
                                                            "router-2" {:out :dummy-2}
                                                            "router-3" {:out :dummy-3}}
                                   :router-id->outgoing-ws {"router-1" {:out :dummy-4}
                                                            "router-2" {:out :dummy-5}
                                                            "router-3" {:out :dummy-6}}}
          websocket-client nil
          router-id->http-endpoint {"router-1" "http://www.router-1.com:1234/"
                                    "router-2" "http://www.router-2.com:1234/"
                                    "router-3" "http://www.router-3.com:1234/"}
          connect-options {:async-write-timeout 10000
                           :out async/chan}
          router-metrics-agent (agent in-router-metrics-state)
          encrypt identity
          out-router-metrics-state
          (update-metrics-router-state in-router-metrics-state websocket-client router-id->http-endpoint encrypt
                                       connect-options router-metrics-agent)]
      (is (= in-router-metrics-state out-router-metrics-state))
      (is (= in-router-metrics-state (retrieve-agent-state router-metrics-agent))))))

(deftest test-update-metrics-router-state-new-and-missing-router-ids-connect-failed
  (let [ctrl-chan (async/chan 1)]
    (with-redefs [ws/connect! (fn ws-connect! [_ ws-endpoint _ _]
                                (is (str/starts-with? ws-endpoint "ws://"))
                                (is (str/ends-with? ws-endpoint "/waiter-router-metrics"))
                                (let [in-chan (async/chan 10)
                                      out-chan (async/chan 10)
                                      web-socket (WebSocket. in-chan out-chan ctrl-chan nil nil nil)]
                                  {:socket web-socket}))]
      (let [my-router-id "router-0"
            in-router-metrics-state {:metrics {:routers {"router-0" {"s1" {"c" 0}, "s2" {"c" 0}, "s3" {"c" 0}}
                                                         "router-1" {"s1" {"c" 1}, "s2" {"c" 1}, "s3" {"c" 1}}
                                                         "router-2" {"s1" {"c" 2}, "s3" {"c" 2}}}}
                                     :router-id my-router-id
                                     :router-id->incoming-ws {"router-2" {:out (async/chan 10), :request-id :dummy-2}}
                                     :router-id->outgoing-ws {"router-2" {:out (async/chan 10), :request-id :dummy-5}}}
            router-metrics-agent (agent in-router-metrics-state)
            websocket-client nil
            router-id->http-endpoint {"router-0" "http://www.router-0.com:1234/"
                                      "router-1" "http://www.router-1.com:1234/"
                                      "router-2" "http://www.router-2.com:1234/"}
            encrypt identity
            connect-options {:async-write-timeout 10000
                             :out async/chan}]
        (send router-metrics-agent update-metrics-router-state websocket-client router-id->http-endpoint encrypt connect-options router-metrics-agent)
        (let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)
              new-router-ids (keyset router-id->http-endpoint)]
          (is (= (select-keys (get-in in-router-metrics-state [:metrics :routers]) (keys router-id->http-endpoint))
                 (get-in out-router-metrics-state [:metrics :routers])))
          (is (= new-router-ids (keyset (get-in out-router-metrics-state [:metrics :routers]))))
          (is (= (set/intersection (keyset (:router-id->incoming-ws out-router-metrics-state)) new-router-ids)
                 (keyset (get-in out-router-metrics-state [:router-id->incoming-ws]))))
          (is (= (disj new-router-ids my-router-id "router-1")
                 (keyset (get-in out-router-metrics-state [:router-id->outgoing-ws]))))
          (let [old-outgoing-router-ids (keyset (get-in in-router-metrics-state [:router-id->outgoing-ws]))
                new-outgoing-router-ids (set/difference new-router-ids old-outgoing-router-ids #{my-router-id})]
            (doseq [[router-id ws-request] (-> out-router-metrics-state :router-id->outgoing-ws seq)]
              (if (contains? new-outgoing-router-ids router-id)
                (do
                  (is (:request-id ws-request))
                  (is (:time ws-request)))
                (let [old-ws-request (get-in in-router-metrics-state [:router-id->outgoing-ws router-id])]
                  (is (and old-ws-request (= old-ws-request ws-request)))))))

          (async/>!! ctrl-chan [:error (ex-info "Thrown from test" {})])
          (is (test-helpers/wait-for
                #(let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)]
                   (= in-router-metrics-state out-router-metrics-state))
                :interval 200, :timeout 4000, :unit-multiplier 1)
              "Deregistering router-1 failed"))))))

(deftest test-update-metrics-router-state-new-and-missing-router-ids-connect-successful
  (with-redefs [ws/connect! (fn ws-connect! [_ ws-endpoint callback _]
                              (is (str/starts-with? ws-endpoint "ws://"))
                              (is (str/ends-with? ws-endpoint "/waiter-router-metrics"))
                              (let [ctrl-chan (async/chan 10)
                                    in-chan (async/chan 10)
                                    out-chan (async/chan 10)
                                    ws-request {:ctrl ctrl-chan, :in in-chan, :out out-chan, :tag ws-endpoint}
                                    web-socket (WebSocket. in-chan out-chan ctrl-chan nil nil nil)]
                                (async/>!! in-chan :success)
                                (callback ws-request)
                                (let [{:keys [dest-router-id]} (async/<!! out-chan)]
                                  (is (= (str "ws://www." dest-router-id ".com:1234/waiter-router-metrics") ws-endpoint)))
                                {:socket web-socket
                                 :tag ws-endpoint}))]
    (let [my-router-id "router-0"
          in-router-metrics-state {:metrics {:routers {"router-0" {"s1" {"c" 0}, "s2" {"c" 0}, "s3" {"c" 0}}
                                                       "router-1" {"s1" {"c" 1}, "s2" {"c" 1}, "s3" {"c" 1}}
                                                       "router-2" {"s1" {"c" 2}, "s3" {"c" 2}}
                                                       "router-3" {"s1" {"c" 3}, "s2" {"c" 3}, "s3" {"c" 3}}
                                                       "router-4" {"s1" {"c" 4}, "s4" {"c" 4}}}}
                                   :router-id my-router-id
                                   :router-id->incoming-ws {"router-2" {:out (async/chan 10), :request-id :dummy-2}
                                                            "router-3" {:out (async/chan 10), :request-id :dummy-3}
                                                            "router-4" {:out (async/chan 10), :request-id :dummy-1}}
                                   :router-id->outgoing-ws {"router-2" {:out (async/chan 10), :request-id :dummy-5}
                                                            "router-4" {:out (async/chan 10), :request-id :dummy-4}}}
          router-metrics-agent (agent in-router-metrics-state)
          websocket-client nil
          router-id->http-endpoint {"router-0" "http://www.router-0.com:1234/"
                                    "router-1" "http://www.router-1.com:1234/"
                                    "router-2" "http://www.router-2.com:1234/"
                                    "router-3" "http://www.router-3.com:1234/"}
          encrypt identity
          connect-options {:async-write-timeout 10000
                           :out async/chan}]
      (send router-metrics-agent update-metrics-router-state websocket-client router-id->http-endpoint encrypt connect-options router-metrics-agent)
      (await router-metrics-agent) ;; ensure update-metrics-router-state is processed and has triggered nested sends
      (let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)
            new-router-ids (keyset router-id->http-endpoint)]
        (is (= (select-keys (get-in in-router-metrics-state [:metrics :routers]) (keys router-id->http-endpoint))
               (get-in out-router-metrics-state [:metrics :routers])))
        (is (= new-router-ids (keyset (get-in out-router-metrics-state [:metrics :routers]))))
        (is (= (set/intersection (keyset (:router-id->incoming-ws out-router-metrics-state)) new-router-ids)
               (keyset (get-in out-router-metrics-state [:router-id->incoming-ws]))))
        (is (= (disj new-router-ids my-router-id)
               (keyset (get-in out-router-metrics-state [:router-id->outgoing-ws]))))
        (let [old-outgoing-router-ids (keyset (get-in in-router-metrics-state [:router-id->outgoing-ws]))
              new-outgoing-router-ids (set/difference new-router-ids old-outgoing-router-ids #{my-router-id})]
          (doseq [[router-id ws-request] (-> out-router-metrics-state :router-id->outgoing-ws seq)]
            (if (contains? new-outgoing-router-ids router-id)
              (do
                (is (:request-id ws-request))
                (is (:time ws-request)))
              (let [old-ws-request (get-in in-router-metrics-state [:router-id->outgoing-ws router-id])]
                (is (and old-ws-request (= old-ws-request ws-request)))))))))))

(deftest test-incoming-router-metrics-handler-missing-source-router-id
  (testing "incoming-router-metrics-handler:missing-source-router-id"
    (let [encrypt (fn [data] {:data data})
          decrypt (fn [data] (:data data))
          metrics-read-interval-ms 10
          in-router-metrics-state {}
          router-metrics-agent (agent in-router-metrics-state)
          ws-request {:in (async/chan 10), :out (async/chan 10)}]
      (async/>!! (:in ws-request) (encrypt {}))
      (incoming-router-metrics-handler router-metrics-agent metrics-read-interval-ms encrypt decrypt ws-request)
      (is (= (encrypt {:message "Missing source router!", :data {}}) (async/<!! (:out ws-request))))
      (is (nil? (async/<!! (:out ws-request)))))))

(deftest test-incoming-router-metrics-handler-valid-handshake
  (testing "incoming-router-metrics-handler:valid-handshake"
    (let [decrypt-call-counter (atom 0)
          encrypt (fn [data] {:data data})
          decrypt (fn [data]
                    (swap! decrypt-call-counter inc)
                    (let [{:keys [release-chan] :as decrypted-data} (:data data)]
                      (when release-chan
                        (async/>!! release-chan :release))
                      decrypted-data))
          router-metrics-agent (agent {})
          ws-request {:ctrl (async/chan 10), :in (async/chan 10), :out (async/chan 10)}
          source-router-id "router-1"
          iteration-limit 20]
      (async/>!! (:in ws-request) (encrypt {:source-router-id source-router-id}))
      (incoming-router-metrics-handler router-metrics-agent 10 encrypt decrypt ws-request)
      (let [release-chan (async/chan 1)]
        (async/go-loop [iteration 0]
          (log/debug "processing iteration" iteration)
          (let [raw-data (cond-> {:router-metrics {"s1" {:iteration iteration}, "s2" {:iteration iteration}},
                                  :source-router-id source-router-id,
                                  :time (str "time-" iteration)}
                           (= iteration iteration-limit) (assoc :release-chan release-chan))]
            (async/>! (:in ws-request) (encrypt raw-data)))
          (when (< iteration iteration-limit)
            (recur (inc iteration))))
        (async/<!! release-chan))
      (is (test-helpers/wait-for
            #(let [out-router-metrics-state @router-metrics-agent]
               (log/debug "router-metrics-state:" out-router-metrics-state)
               (= (str "time-" iteration-limit) (get-in out-router-metrics-state [:last-update-times source-router-id])))
            :interval 100 :timeout 2000 :unit-multiplier 1))
      (let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)]
        (is (< 1 @decrypt-call-counter iteration-limit)) ; expect throttling
        (is (= {"s1" {:iteration iteration-limit}, "s2" {:iteration iteration-limit}}
               (get-in out-router-metrics-state [:metrics :routers source-router-id]))
            (str "Call count: " @decrypt-call-counter ", out-router-metrics-state=" out-router-metrics-state))
        (is (= (str "time-" iteration-limit) (get-in out-router-metrics-state [:last-update-times source-router-id])))
        (let [actual-ws-request (get-in out-router-metrics-state [:router-id->incoming-ws source-router-id])]
          (is (:request-id actual-ws-request))
          (is (= ws-request (select-keys actual-ws-request (keys ws-request)))))))))

(deftest test-setup-router-syncer
  (with-redefs [update-metrics-router-state (fn update-metrics-router-state-fn [router-metrics-state _ {:keys [response-chan] :as router-state} _ _ _]
                                              (when response-chan
                                                (async/go (async/>! response-chan :response)))
                                              (assoc router-metrics-state
                                                :routers (dissoc router-state :response-chan)
                                                :source (if response-chan "update-2" "update-1")))]
    (testing "setup-router-syncer"
      (let [router-state-chan (async/chan 1)
            router-metrics-agent (agent {:router-id "router-0"})
            encrypt identity
            attach-auth-cookie! identity
            websocket-client (WebSocketClient.)
            {:keys [exit-chan query-chan]} (setup-router-syncer router-state-chan router-metrics-agent 10 10000 10000 websocket-client encrypt attach-auth-cookie!)
            query-go-block-state (fn query-go-block-state-fn []
                                   (let [response-chan (async/promise-chan)]
                                     (async/>!! query-chan {:response-chan response-chan})
                                     (async/<!! response-chan)))
            router-state {"router-1" "http://www.router-1.com/"
                          "router-2" "http://www.router-2.com/"
                          "router-3" "http://www.router-3.com/"}]
        (async/>!! router-state-chan router-state)
        (query-go-block-state) ; ensure router-state was read
        (let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)]
          (is (= {:router-id "router-0", :routers router-state, :source "update-1"} out-router-metrics-state)))
        (let [response-chan (async/promise-chan)]
          (async/>!! router-state-chan (assoc router-state :response-chan response-chan))
          (async/<!! response-chan))
        (is (pos? (:timeouts (query-go-block-state))))
        (let [out-router-metrics-state (retrieve-agent-state router-metrics-agent)]
          (is (= {:router-id "router-0", :routers router-state, :source "update-2"} out-router-metrics-state)))
        (async/>!! exit-chan :exit)))))

(deftest test-setup-metrics-syncer
  (let [counter (atom 0)
        response-chan-atom (atom nil)]
    (with-redefs [metrics/get-core-codahale-metrics (fn [] {"s1" {"slots-assigned" 1
                                                                  "outstanding" 1
                                                                  :response-chan @response-chan-atom
                                                                  :version @counter}})
                  publish-router-metrics (fn [agent-state _ router-metrics _]
                                           (let [response-chan (get-in router-metrics ["s1" :response-chan])]
                                             (when response-chan
                                               (async/>!! response-chan router-metrics)))
                                           (-> agent-state
                                               (assoc :metrics (utils/dissoc-in router-metrics ["s1" :response-chan]))
                                               (update-in [:version] inc)))]
      (testing "setup-metrics-syncer"
        (let [router-metrics-agent (agent {:router-id "router-0", :version 1})
              encrypt identity
              local-usage-agent (agent {"s1" {"last-request-time" (DateTime. 1000)}
                                        "s2" {"last-request-time" (DateTime. 2000)}})
              {:keys [exit-chan]} (setup-metrics-syncer router-metrics-agent local-usage-agent 10 encrypt)]
          (let [response-chan (async/promise-chan)]
            (reset! response-chan-atom response-chan)
            (swap! counter inc)
            (async/<!! response-chan)
            (is (= {:router-id "router-0", :metrics {"s1" {"last-request-time" (DateTime. 1000)
                                                           "outstanding" 1
                                                           "slots-assigned" 1
                                                           :version 1}}}
                   (dissoc (retrieve-agent-state router-metrics-agent) :version))))
          (let [response-chan (async/promise-chan)]
            (reset! response-chan-atom response-chan)
            (swap! counter inc)
            (async/<!! response-chan)
            (is (= {:router-id "router-0", :metrics {"s1" {"last-request-time" (DateTime. 1000)
                                                           "outstanding" 1
                                                           "slots-assigned" 1
                                                           :version 2}}}
                   (dissoc (retrieve-agent-state router-metrics-agent) :version))))
          (async/>!! exit-chan :exit))))))

(deftest test-new-router-metrics-agent
  (let [metrics-agent (new-router-metrics-agent "router-0" {:router-id "router-1", :metrics {:routers {"r1" {:a :b}}}})]
    (is (= {:last-update-times {}
            :metrics {:routers {"r1" {:a :b}}}
            :router-id "router-0"
            :router-id->incoming-ws {}
            :router-id->outgoing-ws {}}
           (retrieve-agent-state metrics-agent)))))

(deftest test-agent->service-id->metrics
  (testing "missing-router-data"
    (is (= {} (agent->service-id->metrics (agent {})))))

  (testing "aggregation-of-router-metrics"
    (let [time-1 (DateTime. 1000)
          time-2 (DateTime. 2000)
          time-3 (DateTime. 3000)
          time-4 (DateTime. 4000)
          time-5 (DateTime. 5000)
          in-router-metrics-state {:metrics
                                   {:routers
                                    {"router-0" {"s1" {"last-request-time" time-1, "outstanding" 0, "total" 2}
                                                 "s2" {"last-request-time" time-2, "outstanding" 0, "total" 1}
                                                 "s3" {"last-request-time" time-3, "outstanding" 0}}
                                     "router-1" {"s1" {"outstanding" 1}
                                                 "s2" {"last-request-time" time-1, "outstanding" 1, "total" 1}
                                                 "s3" {"last-request-time" time-4, "outstanding" 1, "total" 2}}
                                     "router-2" {"s1" {"outstanding" 2}
                                                 "s3" {"last-request-time" time-1, "outstanding" 2, "total" 5}
                                                 "s5" {"last-request-time" time-2, "outstanding" 1}}
                                     "router-3" {"s1" {"outstanding" 3}
                                                 "s2" {"last-request-time" time-3, "outstanding" 3}
                                                 "s3" {"last-request-time" time-2, "outstanding" 3, "total" 6}}
                                     "router-4" {"s1" {"outstanding" 4}
                                                 "s4" {"last-request-time" time-5, "outstanding" 4, "total" 4}
                                                 "s5" {"last-request-time" time-1, "outstanding" 2, "total" 3}}}}}
          router-metrics-agent (agent in-router-metrics-state)
          expected-output {"s1" {"last-request-time" time-1, "outstanding" 10, "total" 2}
                           "s2" {"last-request-time" time-3, "outstanding" 4, "total" 2}
                           "s3" {"last-request-time" time-4, "outstanding" 6, "total" 13}
                           "s4" {"last-request-time" time-5, "outstanding" 4, "total" 4}
                           "s5" {"last-request-time" time-2, "outstanding" 3, "total" 3}}]
      (is (= expected-output (agent->service-id->metrics router-metrics-agent)))))

  (testing "faulty-router-metrics"
    (let [in-router-metrics-state {:metrics {:routers {"router-0" {"s1" {"c" 0}, "s2" [], "s3" {"c" 0}}
                                                       "router-1" {"s1" {"c" 1}, "s2" {"c" {"d" 1}, "e" 1}, "s3" {"c" {"d" 2}}}
                                                       "router-2" {"s1" {"c" 2}, "s3" {"c" 2, "e" {"d" 0}}}}}}
          router-metrics-agent (agent in-router-metrics-state)
          expected-output {"s1" {"c" 3}}]
      (is (= expected-output (agent->service-id->metrics router-metrics-agent))))))

(deftest test-agent->service-id->router-id->metrics
  (let [call-counter-atom (atom 0)
        service-id "test-service-id"
        router-metrics-agent (agent {})
        update-agent-state (fn [agent-state]
                             (swap! call-counter-atom inc)
                             (assoc-in agent-state [:metrics :routers]
                                       {"router-1" {service-id {"count" @call-counter-atom, "service-id" service-id}}}))]
    (send router-metrics-agent update-agent-state)
    (await router-metrics-agent)
    (is (= {"router-1" {"count" 1, "service-id" service-id}} (agent->service-id->router-id->metrics router-metrics-agent service-id)))
    (send router-metrics-agent update-agent-state)
    (await router-metrics-agent)
    (is (= {"router-1" {"count" 2, "service-id" service-id}} (agent->service-id->router-id->metrics router-metrics-agent service-id)))
    (send router-metrics-agent update-agent-state)
    (await router-metrics-agent)
    (agent->service-id->router-id->metrics router-metrics-agent service-id)
    (is (= {"router-1" {"count" 3, "service-id" service-id}} (agent->service-id->router-id->metrics router-metrics-agent service-id)))))
