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
(ns waiter.token-watch-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.token :refer :all]
            [waiter.token-watch :refer :all]
            [waiter.correlation-id :as cid])
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

(defmacro assert-channels-no-new-message
  [chans timeout-ms]
  `(let [chans# ~chans
         timeout-ms# ~timeout-ms
         timeout-chan# (async/timeout timeout-ms#)
         [msg# res-chan#] (-> chans#
                            (conj timeout-chan#)
                            (async/alts!! :priority true))]
     (is (= res-chan# timeout-chan#)
         (str "Expected no message from channel instead got: " msg#))))

(defmacro assert-channels-next-message-with-fn
  [chans msg-fn]
  `(let [chans# ~chans
         msg-fn# ~msg-fn
         res# (for [chan# chans#] (async/<!! chan#))]
     (is (every? msg-fn# res#))))

(defmacro assert-channels-next-message
  "Assert that list of channels next message"
  [chans msg]
  `(let [chans# ~chans
         msg# ~msg]
     (assert-channels-next-message-with-fn chans# #(= % msg#))))

(defn- create-watch-chans
  [x]
  (for [_ (range x)]
    (async/chan (async/sliding-buffer 1024))))

(deftest test-send-event-to-channels
  (let [event {:event :DELETE :object {:owner "owner1" :token "token1"}}]

    (testing "sending basic event to channels"
      (let [chans (create-watch-chans 10)
            open-chans (send-event-to-channels! chans event)]
        (is (= (set chans) open-chans))
        (assert-channels-next-message chans event)))

    (testing "sending basic event to mix of open and closed channels"
      (let [closed-chan (async/chan 1)
            open-chans (create-watch-chans 10)
            chans (conj open-chans closed-chan)
            _ (async/close! closed-chan)
            result-open-chans (send-event-to-channels! chans event)]
        (is (= (set open-chans) result-open-chans))
        (assert-channels-next-message open-chans event)))

    (testing "sending event to a channel with maxed out put! buffer (1024 messages) will close the buffer"
      (let [filled-chan (async/chan)
            _ (dotimes [i 1024] (async/put! filled-chan i))
            open-chans (create-watch-chans 10)
            chans (conj open-chans filled-chan)
            result-open-chans (send-event-to-channels! chans event)]
        (is (= (set open-chans) result-open-chans))
        (assert-channels-next-message open-chans event)))))

(let [get-token-hash (fn [kv-store token] (sd/token-data->token-hash (kv/fetch kv-store token)))
      get-latest-state (fn [query-chan]
                         (let [temp-chan (async/promise-chan)]
                           (async/>!! query-chan {:include-flags #{"token->index"}
                                                  :response-chan temp-chan})
                           (async/<!! temp-chan)))
      add-watch-chans (fn [tokens-watch-channels-update-chan watch-chans]
                        (doseq [chan watch-chans]
                          (async/put! tokens-watch-channels-update-chan chan)))
      remove-watch-chans (fn [watch-chans]
                           (doseq [chan watch-chans]
                             (async/close! chan)))
      trigger-token-watch-refresh (fn [watch-refresh-timer-chan]
                                    (async/>!! watch-refresh-timer-chan {}))
      stop-token-watch-maintainer (fn [go-chan exit-chan]
                                    (async/>!! exit-chan :exit)
                                    (async/<!! go-chan))
      make-aggregate-index-events (fn [object & {:keys [id]}]
                                    (make-index-event :EVENTS [object] :id id))
      token-watch-test-cid "token-watch-test"
      token-watch-cid-factory-fn (constantly token-watch-test-cid)
      send-internal-index-event-fn (fn [tokens-update-chan token]
                                     (cid/with-correlation-id
                                       token-watch-test-cid
                                       (send-internal-index-event tokens-update-chan token)))
      auth-user "auth-user"
      token1-metadata {"cluster" "c1" "last-update-time" 1000 "owner" "owner1"}
      token1-service-desc {"cpus" 1}
      token1-index {:deleted false
                    :last-update-time (get token1-metadata "last-update-time")
                    :maintenance false
                    :owner (get token1-metadata "owner")
                    :token "token1"}]

  (deftest test-start-token-watch-maintainer-empty-starting-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-chans (create-watch-chans 10)
          {:keys [exit-chan go-chan query-chan tokens-watch-channels-update-chan]}
          (start-token-watch-maintainer kv-store clock 1 1 (async/chan) token-watch-cid-factory-fn)]
      (is (= {:last-update-time (clock)
              :token->index {}
              :watch-count 0}
             (get-latest-state query-chan)))

      (testing "watch-channels should receive empty list of tokens"
        (add-watch-chans tokens-watch-channels-update-chan watch-chans)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan)))
        (assert-channels-next-message watch-chans (make-index-event :INITIAL [] :id token-watch-test-cid)))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-non-empty-starting-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-chans (create-watch-chans 10)
          _ (store-service-description-for-token
              synchronize-fn kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
          {:keys [exit-chan go-chan query-chan tokens-watch-channels-update-chan]}
          (start-token-watch-maintainer kv-store clock 1 1 (async/chan) token-watch-cid-factory-fn)
          token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
          expected-token->index {"token1" token-cur-index}]
      (is (= {:last-update-time (clock)
              :token->index expected-token->index
              :watch-count 0}
             (get-latest-state query-chan)))

      (testing "watch-channels should receive starting list of tokens"
        (add-watch-chans tokens-watch-channels-update-chan watch-chans)
        (is (= {:last-update-time (clock) :token->index expected-token->index :watch-count 10}
               (get-latest-state query-chan)))
        (assert-channels-next-message-with-fn watch-chans
                                              #(and (= #{token-cur-index}
                                                       (set (get % :object)))
                                                    (= :INITIAL
                                                       (get % :type)))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-watch-channels-updates
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-chans (create-watch-chans 10)
          {:keys [exit-chan go-chan tokens-update-chan query-chan tokens-watch-channels-update-chan]}
          (start-token-watch-maintainer kv-store clock 1 1 (async/chan) token-watch-cid-factory-fn)]

      (testing "watch-channels get UPDATE event for added tokens"
        (add-watch-chans tokens-watch-channels-update-chan watch-chans)
        (is (= {:last-update-time (clock) :token->index {} :watch-count 10}
               (get-latest-state query-chan)))
        (store-service-description-for-token
          synchronize-fn kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (assert-channels-next-message watch-chans (make-index-event :INITIAL [] :id token-watch-test-cid))
          (send-internal-index-event-fn tokens-update-chan "token1")
          (assert-channels-next-message watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)
                                                                                 :id token-watch-test-cid))
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))))

      (testing "watch-channels get UPDATE event for modified tokens"
        (store-service-description-for-token
          synchronize-fn kv-store history-length limit-per-owner "token1" (assoc token1-service-desc "cpus" 2) token1-metadata)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (send-internal-index-event-fn tokens-update-chan "token1")
          (assert-channels-next-message watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)
                                                                                 :id token-watch-test-cid))
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))

          (testing "watch-channels doesn't send event if no changes in token-index-entry and current-state"
            (send-internal-index-event-fn tokens-update-chan "token1")
            (assert-channels-no-new-message watch-chans 1000)
            (is (= {:last-update-time (clock)
                    :token->index expected-token->index
                    :watch-count 10}
                   (get-latest-state query-chan))))))

      (testing "watch-channels get UPDATE event for soft deleted tokens"
        (delete-service-description-for-token clock synchronize-fn kv-store history-length "token1"
                                              (get token1-index :owner) auth-user)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1")
                                                  :last-update-time (clock-millis)
                                                  :deleted true)
              expected-token->index {"token1" token-cur-index}]
          (send-internal-index-event-fn tokens-update-chan "token1")
          (assert-channels-next-message watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)
                                                                                 :id token-watch-test-cid))
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))))

      (testing "watch-channels get DELETE event for hard deleted tokens"
        (delete-service-description-for-token clock synchronize-fn kv-store history-length "token1"
                                              (get token1-index :owner) auth-user :hard-delete true)
        (send-internal-index-event-fn tokens-update-chan "token1")
        (assert-channels-next-message watch-chans
                                      (make-aggregate-index-events
                                        (make-index-event :DELETE {:token "token1"})
                                        :id token-watch-test-cid))
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan))))

      (testing "watch-channels doesn't send event if no changes in token-index-entry and current-state"
        (send-internal-index-event-fn tokens-update-chan "token1")
        (assert-channels-no-new-message watch-chans 1000)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-watch-count
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-refresh-timer-chan (async/chan)
          watch-chans-1 (create-watch-chans 10)
          watch-chans-2 (create-watch-chans 10)
          {:keys [exit-chan go-chan tokens-watch-channels-update-chan query-chan]}
          (start-token-watch-maintainer kv-store clock 1 1 watch-refresh-timer-chan token-watch-cid-factory-fn)]
      (is (= {:last-update-time (clock)
              :token->index {}
              :watch-count 0}
             (get-latest-state query-chan)))

      (testing "watch-count should increment when channels are added"
        (add-watch-chans tokens-watch-channels-update-chan watch-chans-1)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan)))
        (assert-channels-next-message watch-chans-1 (make-index-event :INITIAL [] :id token-watch-test-cid))
        (add-watch-chans tokens-watch-channels-update-chan watch-chans-2)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 20}
               (get-latest-state query-chan)))
        (assert-channels-next-message watch-chans-2 (make-index-event :INITIAL [] :id token-watch-test-cid)))

      (testing "watch-count should decrement when daemon process is refreshed"
        (remove-watch-chans watch-chans-1)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (get-latest-state query-chan)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan)))
        (remove-watch-chans watch-chans-2)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 0}
               (get-latest-state query-chan))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-refresh-timeout
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-chans (create-watch-chans 10)
          watch-refresh-timer-chan (async/chan)
          {:keys [exit-chan go-chan tokens-watch-channels-update-chan query-chan]}
          (start-token-watch-maintainer kv-store clock 1 1 watch-refresh-timer-chan token-watch-cid-factory-fn)]
      (is (= {:last-update-time (clock)
              :token->index {}
              :watch-count 0}
             (get-latest-state query-chan)))

      (testing "refresh-timeout should not update if there are no changes in kv-store"
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 0}
               (get-latest-state query-chan))))

      (add-watch-chans tokens-watch-channels-update-chan watch-chans)
      (assert-channels-next-message watch-chans (make-index-event :INITIAL [] :id token-watch-test-cid))

      (testing "refresh-timeout should update current-state and watchers if token is added to kv-store"
        (store-service-description-for-token
          synchronize-fn kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))
          (assert-channels-next-message watch-chans (make-aggregate-index-events
                                                      (make-index-event :UPDATE token-cur-index)
                                                      :id token-watch-test-cid))))

      (testing "refresh-timeout should update current-state and watchers if token is out of date"
        (store-service-description-for-token
          synchronize-fn kv-store history-length limit-per-owner "token1" (assoc token1-service-desc "cpus" 2)
          token1-metadata)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))
          (assert-channels-next-message watch-chans (make-aggregate-index-events
                                                      (make-index-event :UPDATE token-cur-index)
                                                      :id token-watch-test-cid))))

      (testing "refresh-timeout should update current-state and watchers if token is soft deleted"
        (delete-service-description-for-token clock synchronize-fn kv-store history-length "token1"
                                              (get token1-index :owner) auth-user)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1")
                                                  :last-update-time (clock-millis)
                                                  :deleted true)
              expected-token->index {"token1" token-cur-index}]
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))
          (assert-channels-next-message watch-chans (make-aggregate-index-events
                                                      (make-index-event :UPDATE token-cur-index)
                                                      :id token-watch-test-cid))))

      (testing "refresh-timeout should update current-state and watchers if token is hard deleted"
        (delete-service-description-for-token clock synchronize-fn kv-store history-length "token1"
                                              (get token1-index :owner) auth-user :hard-delete true)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan)))
        (assert-channels-next-message watch-chans (make-aggregate-index-events
                                                    (make-index-event :DELETE
                                                                      {:owner (get token1-index :owner)
                                                                       :token "token1"})
                                                    :id token-watch-test-cid)))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-query-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          {:keys [exit-chan go-chan query-state-fn]}
          (start-token-watch-maintainer kv-store clock 1 1 (async/chan) token-watch-cid-factory-fn)]

      (testing "query-state-fn provides current state with default fields"
        (is (= {:last-update-time (clock)
                :watch-count 0}
               (query-state-fn #{}))))

      (testing "query-state-fn respects include-flags"
        (is (= {:buffer-state {:update-chan-count 0
                               :watch-channels-update-chan-count 0}
                :last-update-time (clock)
                :token->index {}
                :watch-count 0}
               (query-state-fn #{"token->index" "buffer-state"}))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-slow-channel
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          {:keys [exit-chan go-chan tokens-update-chan tokens-watch-channels-update-chan query-chan]}
          (start-token-watch-maintainer kv-store clock 1 1 (async/chan) token-watch-cid-factory-fn)]

      (testing "sending 5000 internal events does not halt daemon process with a slow watch-channel"
        (let [buffer-size 1
              slow-chan (async/chan buffer-size)
              msg-count 5000]
          (async/put! tokens-watch-channels-update-chan slow-chan)
          (is (= {:last-update-time (clock)
                  :token->index {}
                  :watch-count 1}
                 (get-latest-state query-chan)))
          (is (every?
                true?
                (for [i (range msg-count)]
                  (do
                    (store-service-description-for-token
                      synchronize-fn kv-store history-length limit-per-owner "token1" token1-service-desc
                      (assoc token1-metadata "last-update-time" i))
                    (send-internal-index-event-fn tokens-update-chan "token1")
                    (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1")
                                                              :last-update-time i)
                          expected-token->index {"token1" token-cur-index}]
                      (= {:last-update-time (clock)
                          :token->index expected-token->index}
                         (dissoc (get-latest-state query-chan)
                                 :watch-count)))))))

          (testing "channel is closed and no longer can have messages put! to it, but can still read"
            (is (not (async/put! slow-chan "temp")))
            (is (= { :id token-watch-test-cid :object [] :type :INITIAL}
                   (async/<!! slow-chan))))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-buffer-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          {:keys [exit-chan go-chan tokens-update-chan tokens-watch-channels-update-chan query-state-fn]}
          (start-token-watch-maintainer kv-store clock 1000 1000 (async/chan) token-watch-cid-factory-fn)
          expected-buffer-count 123]
      (stop-token-watch-maintainer go-chan exit-chan)

      (testing "state provides correct current buffer count for tokens-update-chan"
        (dotimes [_ expected-buffer-count]
          (async/put! tokens-update-chan (async/chan)))
        (is (= {:buffer-state {:update-chan-count expected-buffer-count
                               :watch-channels-update-chan-count 0}
                :last-update-time (clock)
                :watch-count 0}
               (query-state-fn #{"buffer-state"}))))

      (testing "state provides correct current buffer count for tokens-watch-channels-update-chan"
        (dotimes [_ expected-buffer-count]
          (async/put! tokens-watch-channels-update-chan (async/chan)))
        (is (= {:buffer-state {:update-chan-count expected-buffer-count
                               :watch-channels-update-chan-count expected-buffer-count}
                :last-update-time (clock)
                :watch-count 0}
               (query-state-fn #{"buffer-state"})))))))
