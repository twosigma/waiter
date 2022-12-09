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
            [clojure.walk :as walk]
            [waiter.correlation-id :as cid]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.token :refer :all]
            [waiter.token-watch :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.utils :as utils])
  (:import (org.joda.time DateTime)))

(def ^:const history-length 5)
(def ^:const limit-per-owner 10)

(let [lock (Object.)]
  (defn- synchronize-fn
    [_ f]
    (locking lock
      (f))))

(defn- extract-reference-type->reference-name-helper
  [_]
  {})

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

(defmacro assert-channels-next-event
  "Assert that list of channels next event"
  [chans msg]
  `(let [chans# ~chans
         msg# ~msg]
     (assert-channels-next-message-with-fn chans# #(and (:id %) (= (dissoc % :id) msg#)))))

(defn- create-watch-chans
  [x]
  (for [_ (range x)]
    (async/chan (async/sliding-buffer 1024))))

(deftest test-send-event-to-channels
  (let [event {:event :DELETE :object {:owner "owner1" :token "token1"}}]

    (testing "sending basic event to channels"
      (let [chans (create-watch-chans 10)
            open-chans (utils/send-event-to-channels! chans event)]
        (is (= (set chans) open-chans))
        (assert-channels-next-message chans event)))

    (testing "sending basic event to mix of open and closed channels"
      (let [closed-chan (async/chan 1)
            open-chans (create-watch-chans 10)
            chans (conj open-chans closed-chan)
            _ (async/close! closed-chan)
            result-open-chans (utils/send-event-to-channels! chans event)]
        (is (= (set open-chans) result-open-chans))
        (assert-channels-next-message open-chans event)))

    (testing "sending event to a channel with maxed out put! buffer (1024 messages) will close the buffer"
      (let [filled-chan (async/chan)
            _ (dotimes [i 1024] (async/put! filled-chan i))
            open-chans (create-watch-chans 10)
            chans (conj open-chans filled-chan)
            result-open-chans (utils/send-event-to-channels! chans event)]
        (is (= (set open-chans) result-open-chans))
        (assert-channels-next-message open-chans event)))))

(let [get-token-hash (fn [kv-store token] (sd/token-data->token-hash (kv/fetch kv-store token)))
      get-latest-state (fn [query-chan & {:keys [include-flags] :or {include-flags #{"token->index"}}}]
                         (let [temp-chan (async/promise-chan)]
                           (async/>!! query-chan {:include-flags include-flags
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
      make-aggregate-index-events (fn [object & {:keys [reference-type] :or {reference-type :token}}]
                                    (make-index-event :EVENTS [object] :reference-type reference-type))
      auth-user "auth-user"
      token1-metadata {"cluster" "c1" "last-update-time" 1000 "owner" "owner1"}
      token1-service-desc {"cpus" 1}
      token1-index {:deleted false
                    :last-update-time (get token1-metadata "last-update-time")
                    :maintenance false
                    :owner (get token1-metadata "owner")
                    :reference-type->reference-name {}
                    :token "token1"}]

  (deftest test-start-token-watch-maintainer-empty-starting-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-chans (create-watch-chans 10)
          reference-update-chan (au/latest-chan)
          watch-refresh-timer-chan (async/chan)
          {:keys [exit-chan go-chan query-chan tokens-watch-channels-update-chan]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)]
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
        (assert-channels-next-event watch-chans (make-index-event :INITIAL [] :reference-type :token)))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-non-empty-starting-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-chans (create-watch-chans 10)
          reference-update-chan (au/latest-chan)
          watch-refresh-timer-chan (async/chan)
          _ (store-service-description-for-token
              synchronize-fn extract-reference-type->reference-name-helper kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
          {:keys [exit-chan go-chan query-chan tokens-watch-channels-update-chan]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)
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
                                                       (set (:object %)))
                                                    (= :INITIAL
                                                       (:type %))
                                                    (:id %))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-watch-channels-updates
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          watch-chans (create-watch-chans 10)
          reference-update-chan (au/latest-chan)
          watch-refresh-timer-chan (async/chan)
          {:keys [exit-chan go-chan tokens-update-chan query-chan tokens-watch-channels-update-chan]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)]

      (testing "watch-channels get UPDATE event for added tokens"
        (add-watch-chans tokens-watch-channels-update-chan watch-chans)
        (is (= {:last-update-time (clock) :token->index {} :watch-count 10}
               (get-latest-state query-chan)))
        (store-service-description-for-token
          synchronize-fn extract-reference-type->reference-name-helper kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (assert-channels-next-event watch-chans (make-index-event :INITIAL [] :reference-type :token))
          (send-internal-index-event tokens-update-chan "token1")
          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)))
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))))

      (testing "watch-channels get UPDATE event for modified tokens"
        (store-service-description-for-token
          synchronize-fn extract-reference-type->reference-name-helper kv-store history-length limit-per-owner "token1" (assoc token1-service-desc "cpus" 2) token1-metadata)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (send-internal-index-event tokens-update-chan "token1")
          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)))
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))

          (testing "watch-channels doesn't send event if no changes in token-index-entry and current-state"
            (send-internal-index-event tokens-update-chan "token1")
            (assert-channels-no-new-message watch-chans 1000)
            (is (= {:last-update-time (clock)
                    :token->index expected-token->index
                    :watch-count 10}
                   (get-latest-state query-chan))))))

      (testing "watch-channels get UPDATE event for soft deleted tokens"
        (delete-service-description-for-token clock synchronize-fn extract-reference-type->reference-name-helper kv-store history-length "token1"
                                              (get token1-index :owner) auth-user)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1")
                                                  :last-update-time (clock-millis)
                                                  :deleted true)
              expected-token->index {"token1" token-cur-index}]
          (send-internal-index-event tokens-update-chan "token1")
          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)))
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))))

      (testing "watch-channels get DELETE event for hard deleted tokens"
        (delete-service-description-for-token clock synchronize-fn extract-reference-type->reference-name-helper kv-store history-length "token1"
                                              (get token1-index :owner) auth-user :hard-delete true)
        (send-internal-index-event tokens-update-chan "token1")
        (assert-channels-next-event watch-chans
                                    (make-aggregate-index-events
                                      (make-index-event :DELETE {:token "token1"})))
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan))))

      (testing "watch-channels doesn't send event if no changes in token-index-entry and current-state"
        (send-internal-index-event tokens-update-chan "token1")
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
          reference-update-chan (au/latest-chan)
          {:keys [exit-chan go-chan tokens-watch-channels-update-chan query-chan]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)]
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
        (assert-channels-next-event watch-chans-1 (make-index-event :INITIAL [] :reference-type :token))
        (add-watch-chans tokens-watch-channels-update-chan watch-chans-2)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 20}
               (get-latest-state query-chan)))
        (assert-channels-next-event watch-chans-2 (make-index-event :INITIAL [] :reference-type :token)))

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
          reference-update-chan (au/latest-chan)
          watch-refresh-timer-chan (async/chan)
          {:keys [exit-chan go-chan tokens-watch-channels-update-chan query-chan]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)]
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
      (assert-channels-next-event watch-chans (make-index-event :INITIAL [] :reference-type :token))

      (testing "refresh-timeout should update current-state and watchers if token is added to kv-store"
        (store-service-description-for-token
          synchronize-fn extract-reference-type->reference-name-helper kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))
          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)))))

      (testing "refresh-timeout should update current-state and watchers if token is out of date"
        (store-service-description-for-token
          synchronize-fn extract-reference-type->reference-name-helper kv-store history-length limit-per-owner "token1" (assoc token1-service-desc "cpus" 2)
          token1-metadata)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1"))
              expected-token->index {"token1" token-cur-index}]
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :watch-count 10}
                 (get-latest-state query-chan)))
          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)))))

      (testing "refresh-timeout should update current-state and watchers if token is soft deleted"
        (delete-service-description-for-token clock synchronize-fn extract-reference-type->reference-name-helper kv-store history-length "token1"
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
          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index)))))

      (testing "refresh-timeout should update current-state and watchers if token is hard deleted"
        (delete-service-description-for-token clock synchronize-fn extract-reference-type->reference-name-helper kv-store history-length "token1"
                                              (get token1-index :owner) auth-user :hard-delete true)
        (trigger-token-watch-refresh watch-refresh-timer-chan)
        (is (= {:last-update-time (clock)
                :token->index {}
                :watch-count 10}
               (get-latest-state query-chan)))
        (assert-channels-next-event watch-chans (make-aggregate-index-events
                                                  (make-index-event :DELETE
                                                                    {:owner (get token1-index :owner)
                                                                     :token "token1"}))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-query-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          reference-update-chan (au/latest-chan)
          watch-refresh-timer-chan (async/chan)
          {:keys [exit-chan go-chan query-state-fn]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)]

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
          reference-update-chan (au/latest-chan)
          watch-refresh-timer-chan (async/chan)
          {:keys [exit-chan go-chan tokens-update-chan tokens-watch-channels-update-chan query-chan]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)]

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
                      synchronize-fn extract-reference-type->reference-name-helper kv-store history-length limit-per-owner "token1" token1-service-desc
                      (assoc token1-metadata "last-update-time" i))
                    (send-internal-index-event tokens-update-chan "token1")
                    (let [token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1")
                                                              :last-update-time i)
                          expected-token->index {"token1" token-cur-index}]
                      (= {:last-update-time (clock)
                          :token->index expected-token->index}
                         (dissoc (get-latest-state query-chan)
                                 :watch-count)))))))

          (testing "channel is closed and no longer can have messages put! to it, but can still read"
            (is (not (async/put! slow-chan "temp")))
            (let [{:keys [id] :as event} (async/<!! slow-chan)]
              (is (not (nil? id)))
              (is (= {:object [] :type :INITIAL :reference-type :token}
                     (dissoc event :id)))))))

      (stop-token-watch-maintainer go-chan exit-chan)))

  (deftest test-start-token-watch-maintainer-buffer-state
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          reference-update-chan (au/latest-chan)
          watch-refresh-timer-chan (async/chan)
          {:keys [exit-chan go-chan tokens-update-chan tokens-watch-channels-update-chan query-state-fn]}
          (start-token-watch-maintainer extract-reference-type->reference-name-helper kv-store clock 1000 1000 reference-update-chan watch-refresh-timer-chan)
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
               (query-state-fn #{"buffer-state"}))))))

  (deftest test-start-token-watch-maintainer-token-reference-update
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          token1-service-desc {"cpus" 1 "version" "v1"}
          extract-reference-type->reference-name-fn (fn [{:strs [version]}] {:version version})
          watch-chans-count 5
          watch-chans (create-watch-chans watch-chans-count)
          reference-update-chan (async/chan 100)
          watch-refresh-timer-chan (async/chan)
          _ (store-service-description-for-token
              synchronize-fn extract-reference-type->reference-name-fn kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
          {:keys [exit-chan go-chan query-chan tokens-watch-channels-update-chan]}
          (start-token-watch-maintainer extract-reference-type->reference-name-fn kv-store clock 1 1 reference-update-chan watch-refresh-timer-chan)
          token-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1") :reference-type->reference-name {:version "v1"})
          expected-token->index {"token1" token-cur-index}]

      (is (= {:last-update-time (clock)
              :token->index expected-token->index
              :reference-type->reference-name->last-update-time {}
              :reference-type->reference-name->tokens {:version {"v1" #{"token1"}}}
              :watch-count 0}
             (get-latest-state query-chan :include-flags #{"reference-type->reference-name->last-update-time" "reference-type->reference-name->tokens" "token->index"})))

      (add-watch-chans tokens-watch-channels-update-chan watch-chans)
      (is (= {:last-update-time (clock)
              :token->index expected-token->index
              :reference-type->reference-name->last-update-time {}
              :reference-type->reference-name->tokens {:version {"v1" #{"token1"}}}
              :watch-count watch-chans-count}
             (get-latest-state query-chan :include-flags #{"reference-type->reference-name->last-update-time" "reference-type->reference-name->tokens" "token->index"})))

      (assert-channels-next-event watch-chans (make-index-event :INITIAL [token-cur-index] :reference-type :token))

      (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :version :reference-name "v2" :update-time 100000})

      (is (= {:last-update-time (clock)
              :token->index expected-token->index
              :reference-type->reference-name->last-update-time {:version {"v2" 100000}}
              :reference-type->reference-name->tokens {:version {"v1" #{"token1"}}}
              :watch-count watch-chans-count}
             (get-latest-state query-chan :include-flags #{"reference-type->reference-name->last-update-time" "reference-type->reference-name->tokens" "token->index"})))

      (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :version :reference-name "v1" :update-time 120000})
      (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :version :reference-name "v3" :update-time 140000})

      (is (= {:last-update-time (clock)
              :token->index expected-token->index
              :reference-type->reference-name->last-update-time {:version {"v1" 120000 "v2" 100000 "v3" 140000}}
              :reference-type->reference-name->tokens {:version {"v1" #{"token1"}}}
              :watch-count watch-chans-count}
             (get-latest-state query-chan :include-flags #{"reference-type->reference-name->last-update-time" "reference-type->reference-name->tokens" "token->index"})))

      (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token-cur-index :reference-type :version) :reference-type :version))

      (let [token1-service-desc {"cpus" 1 "mem" 1024 "version" "v3"}
            _ (store-service-description-for-token
                synchronize-fn extract-reference-type->reference-name-fn kv-store history-length limit-per-owner "token1" token1-service-desc token1-metadata)
            token1-cur-index (assoc token1-index :etag (get-token-hash kv-store "token1") :reference-type->reference-name {:version "v3"})
            expected-token->index {"token1" token1-cur-index}]

        (async/put! watch-refresh-timer-chan :trigger)
        (is (= {:last-update-time (clock)
                :token->index expected-token->index
                :reference-type->reference-name->last-update-time {:version {"v1" 120000 "v2" 100000 "v3" 140000}}
                :reference-type->reference-name->tokens {:version {"v3" #{"token1"}}}
                :watch-count watch-chans-count}
               (get-latest-state query-chan :include-flags #{"reference-type->reference-name->last-update-time" "reference-type->reference-name->tokens" "token->index"})))

        (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token1-cur-index)))

        (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :image :reference-name "i1" :update-time 110000})
        (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :version :reference-name "v1" :update-time 110000})
        (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :version :reference-name "v3" :update-time 120000})
        (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :version :reference-name "v3" :update-time 160000})
        (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token1-cur-index :reference-type :version) :reference-type :version))

        (is (= {:last-update-time (clock)
                :token->index expected-token->index
                :reference-type->reference-name->last-update-time {:image {"i1" 110000}
                                                                   :version {"v1" 120000 "v2" 100000 "v3" 160000}}
                :reference-type->reference-name->tokens {:version {"v3" #{"token1"}}}
                :watch-count watch-chans-count}
               (get-latest-state query-chan :include-flags #{"reference-type->reference-name->last-update-time" "reference-type->reference-name->tokens" "token->index"})))

        (let [token2-service-desc {"cpus" 1 "mem" 1024 "version" "v4"}
              token2-metadata {"cluster" "c1" "last-update-time" 2000 "owner" "owner2"}
              _ (store-service-description-for-token
                  synchronize-fn extract-reference-type->reference-name-fn kv-store history-length limit-per-owner "token2" token2-service-desc token2-metadata)
              token2-index {:deleted false
                            :etag (get-token-hash kv-store "token2")
                            :last-update-time (get token2-metadata "last-update-time")
                            :maintenance false
                            :owner (get token2-metadata "owner")
                            :reference-type->reference-name {:version "v4"}
                            :token "token2"}
              expected-token->index {"token1" token1-cur-index
                                     "token2" token2-index}]

          (async/put! watch-refresh-timer-chan :trigger)
          (is (= {:last-update-time (clock)
                  :token->index expected-token->index
                  :reference-type->reference-name->last-update-time {:image {"i1" 110000}
                                                                     :version {"v1" 120000 "v2" 100000 "v3" 160000}}
                  :reference-type->reference-name->tokens {:version {"v3" #{"token1"} "v4" #{"token2"}}}
                  :watch-count watch-chans-count}
                 (get-latest-state query-chan :include-flags #{"reference-type->reference-name->last-update-time" "reference-type->reference-name->tokens" "token->index"})))

          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token2-index)))

          (async/>!! reference-update-chan {:cid (str "r-" (System/currentTimeMillis)) :reference-type :version :reference-name "v4" :update-time 180000})
          (assert-channels-next-event watch-chans (make-aggregate-index-events (make-index-event :UPDATE token2-index :reference-type :version) :reference-type :version))))

      (stop-token-watch-maintainer go-chan exit-chan))))

(deftest test-assoc-token-references
  (let [reference-type->reference-name->tokens {:image {"image-1" #{"t1a" "t1b"}
                                                        "image-2" #{"t2a" "t2b"}
                                                        "image-3" #{"t3a"}}
                                                :version {"ver-1" #{"t1a"}
                                                          "ver-2" #{"t2a" "t2b"}
                                                          "ver-4" #{"t4a"}}}]
    (is (= reference-type->reference-name->tokens
           (assoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-1" :version "ver-1"})))
    (is (= (-> reference-type->reference-name->tokens
               (update-in [:image "image-2"] conj "t1a"))
           (assoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-2" :version "ver-1"})))
    (is (= (-> reference-type->reference-name->tokens
               (update-in [:version "ver-2"] conj "t1a"))
           (assoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-1" :version "ver-2"})))
    (is (= (-> reference-type->reference-name->tokens
               (update-in [:image "image-2"] conj "t1a")
               (update-in [:version "ver-2"] conj "t1a"))
           (assoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-2" :version "ver-2"})))
    (is (= (-> reference-type->reference-name->tokens
               (assoc-in [:image "image-5"] #{"t1a"})
               (assoc-in [:version "ver-5"] #{"t1a"}))
           (assoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-5" :version "ver-5"})))))

(deftest test-dissoc-token-references
  (let [reference-type->reference-name->tokens {:image {"image-1" #{"t1a" "t1b"}
                                                        "image-2" #{"t2a" "t2b"}
                                                        "image-3" #{"t3a"}}
                                                :version {"ver-1" #{"t1a"}
                                                          "ver-2" #{"t2a" "t2b"}
                                                          "ver-4" #{"t4a"}}}]
    (is (= (-> reference-type->reference-name->tokens
               (update-in [:image "image-1"] disj "t1a")
               (update-in [:version "ver-1"] disj "t1a"))
           (dissoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-1" :version "ver-1"})))
    (is (= (-> reference-type->reference-name->tokens
               (update-in [:version "ver-1"] disj "t1a"))
           (dissoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-2" :version "ver-1"})))
    (is (= (-> reference-type->reference-name->tokens
               (update-in [:image "image-1"] disj "t1a"))
           (dissoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-1" :version "ver-2"})))
    (is (= reference-type->reference-name->tokens
           (dissoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-2" :version "ver-2"})))
    (is (= (-> reference-type->reference-name->tokens
               (assoc-in [:image "image-5"] #{})
               (assoc-in [:version "ver-5"] #{}))
           (dissoc-token-references
             reference-type->reference-name->tokens "t1a" {:image "image-5" :version "ver-5"})))))

(deftest test-adjust-token-references
  (let [reference-type->reference-name->tokens {:image {"image-1" #{"t1a" "t1b"}
                                                        "image-2" #{"t2a" "t2b"}
                                                        "image-3" #{"t3a"}}
                                                :version {"ver-1" #{"t1a"}
                                                          "ver-2" #{"t2a" "t2b"}
                                                          "ver-4" #{"t4a"}}}]
    (is (= reference-type->reference-name->tokens
           (adjust-token-references reference-type->reference-name->tokens "t1a"
                                    {:image "image-1" :version "ver-1"} {:image "image-1" :version "ver-1"})))
    (is (= (-> reference-type->reference-name->tokens
               (assoc-in [:version "ver-1"] #{})
               (update-in [:version "ver-2"] conj "t1a"))
           (adjust-token-references reference-type->reference-name->tokens "t1a"
                                    {:image "image-1" :version "ver-1"} {:image "image-1" :version "ver-2"})))
    (is (= (-> reference-type->reference-name->tokens
               (update-in [:image "image-1"] disj "t1a")
               (update-in [:image "image-2"] conj "t1a")
               (assoc-in [:version "ver-1"] #{})
               (update-in [:version "ver-2"] conj "t1a"))
           (adjust-token-references reference-type->reference-name->tokens "t1a"
                                    {:image "image-1" :version "ver-1"} {:image "image-2" :version "ver-2"})))))

(deftest test-extract-reference-update-tokens
  (let [state {:reference-type->reference-name->last-update-time {:image {"image-1" 1500
                                                                          "image-2" 2500}
                                                                  :version {"ver-1" 1400
                                                                            "ver-2" 2600}}
               :reference-type->reference-name->tokens {:image {"image-1" #{"t1a" "t1b"}
                                                                "image-2" #{"t2a" "t2b"}}
                                                        :version {"ver-1" #{"t1a"}
                                                                  "ver-2" #{"t2a" "t2b"}}}}]
    (is (nil? (extract-reference-update-tokens state {:reference-type :image :reference-name "image-0" :update-time 1400})))
    (is (nil? (extract-reference-update-tokens state {:reference-type :image :reference-name "image-1" :update-time 1400})))
    (is (nil? (extract-reference-update-tokens state {:reference-type :image :reference-name "image-1" :update-time 1500})))
    (is (= #{"t1a" "t1b"} (extract-reference-update-tokens state {:reference-type :image :reference-name "image-1" :update-time 1600})))))

(deftest test-calculate-reference-type->reference-name->tokens
  (is (= {:image {"i2" #{"t2" "t4"}
                  "i5" #{"t5"}
                  "i6" #{"t6"}}
          :version {"v3" #{"t3" "t4"}
                    "v5" #{"t5"}}}
         (calculate-reference-type->reference-name->tokens {"t1" {:etag "e1"}
                                                            "t2" {:etag "e2" :reference-type->reference-name {:image "i2"}}
                                                            "t3" {:etag "e3" :reference-type->reference-name {:version "v3"}}
                                                            "t4" {:etag "e4" :reference-type->reference-name {:image "i2" :version "v3"}}
                                                            "t5" {:etag "e5" :reference-type->reference-name {:image "i5" :version "v5"}}
                                                            "t6" {:etag "e6" :reference-type->reference-name {:image "i6"}}}))))

(deftest test-watch-for-reference-updates
  (let [correlation-id (str "test-watch-for-reference-updates-" (System/currentTimeMillis))
        router-id "test-router-id"
        reference-update-chan (au/sliding-buffer-chan 1)
        make-inter-router-requests-async-fn (fn [endpoint & {:keys [body config method]}]
                                              (is (= "reference/notify" endpoint))
                                              (is (= "application/json" (get-in config [:headers "content-type"])))
                                              (is (= correlation-id (get-in config [:headers "x-cid"])))
                                              (is (= :post method))
                                              (let [response-chan (async/promise-chan)]
                                                (async/>!! response-chan {:body (-> body (utils/try-parse-json) (walk/keywordize-keys))})
                                                {"peer-router-id" response-chan}))
        response-chan (async/promise-chan)
        update-event {:cid correlation-id
                      :reference-name "test-reference-name"
                      :reference-type :test-reference-type
                      :response-chan response-chan
                      :router-id router-id
                      :update-time 12000}]
    (async/>!! reference-update-chan update-event)
    (watch-for-reference-updates make-inter-router-requests-async-fn reference-update-chan router-id)
    (is (= {"peer-router-id" {:body (-> update-event
                                        (select-keys [:reference-name :reference-type :update-time])
                                        (update :reference-type name)
                                        (assoc :router-id router-id))}}
           (async/<!! response-chan)))
    (async/close! reference-update-chan)))

(deftest test-reference-notify-handler
  (let [correlation-id (str "test-watch-for-reference-updates-" (System/currentTimeMillis))
        router-id "test-router-id"
        reference-name "test-reference-name"
        reference-type :test-reference-type
        update-time 12000]
    (cid/with-correlation-id
      correlation-id

      (is (= {:headers {"content-type" "text/plain"}
              :status http-405-method-not-allowed}
             (-> (reference-notify-handler nil router-id {:request-method :get})
                 (async/<!!)
                 (select-keys [:headers :status]))))

      (is (= {:headers {"content-type" "text/plain"}
              :status http-400-bad-request}
             (-> (reference-notify-handler nil router-id {:body (utils/clj->json-stream {})
                                                          :request-method :post})
                 (async/<!!)
                 (select-keys [:headers :status]))))

      (is (= {:headers {"content-type" "text/plain"}
              :status http-400-bad-request}
             (-> (reference-notify-handler nil router-id {:body (utils/clj->json-stream {:reference-name reference-name :reference-type reference-type})
                                                          :request-method :post})
                 (async/<!!)
                 (select-keys [:headers :status]))))

      (is (= {:headers {"content-type" "text/plain"}
              :status http-400-bad-request}
             (-> (reference-notify-handler nil router-id {:body (utils/clj->json-stream {:reference-name reference-name :update-time 12000})
                                                          :request-method :post})
                 (async/<!!)
                 (select-keys [:headers :status]))))

      (is (= {:headers {"content-type" "text/plain"}
              :status http-400-bad-request}
             (-> (reference-notify-handler nil router-id {:body (utils/clj->json-stream {:reference-type reference-type :update-time 12000})
                                                          :request-method :post})
                 (async/<!!)
                 (select-keys [:headers :status]))))

      (is (= {:headers {"content-type" "text/plain"}
              :status http-400-bad-request}
             (-> (reference-notify-handler nil router-id {:body (utils/clj->json-stream {:reference-name reference-name :reference-type reference-type :update-time "12000"})
                                                          :request-method :post})
                 (async/<!!)
                 (select-keys [:headers :status]))))

      (let [reference-update-chan (async/promise-chan)]
        (is (= {:body {:cid correlation-id
                       :reference-name reference-name
                       :reference-type (name reference-type)
                       :router-id router-id
                       :success true
                       :update-time update-time}
                :headers {"content-type" "application/json"}
                :status http-200-ok}
               (-> (reference-notify-handler reference-update-chan router-id {:body (utils/clj->json-stream {:reference-name reference-name :reference-type reference-type :update-time update-time})
                                                                              :request-method :post})
                   (async/<!!)
                   (select-keys [:body :headers :status])
                   (update :body #(-> % (utils/try-parse-json) (walk/keywordize-keys))))))
        (is (= {:cid correlation-id
                :reference-name reference-name
                :reference-type reference-type
                :update-time update-time}
               (async/<!! reference-update-chan)))))))

(deftest test-start-reference-notify-forwarder
  (let [num-events 10
        reference-listener-chan (async/chan num-events)
        reference-update-chan (async/chan num-events)
        track-event (fn [events-atom {:keys [event-id] :as reference-event}]
                      (->> (assoc reference-event :event-time (System/currentTimeMillis))
                           (swap! events-atom assoc event-id)))
        notified-events-atom (atom {})
        forwarded-events-atom (atom {})
        pre-notify-fn (fn [reference-event]
                        (track-event notified-events-atom reference-event))
        event-ids (map #(str "event-" % "-" (System/currentTimeMillis)) (range num-events))
        event-response-chans (map (fn [_] (async/promise-chan)) (range num-events))]
    (async/go-loop []
      (if-let [reference-event (async/<! reference-update-chan)]
        (do
          (track-event forwarded-events-atom reference-event)
          (recur))))

    ;; produce the events for the reference notify forwarder
    (doseq [index (range num-events)]
      (let [event-id (nth event-ids index)
            response-chan (nth event-response-chans index)]
        (async/>!! reference-listener-chan {:cid (str "cid-" event-id)
                                            :event-id event-id
                                            :response-chan response-chan})))
    (async/close! reference-listener-chan)

    ;; consume the events inside the reference notify forwarder
    (async/<!! (start-reference-notify-forwarder reference-listener-chan reference-update-chan pre-notify-fn))

    (doseq [index (range num-events)]
      (let [event-id (nth event-ids index)
            response-chan (nth event-response-chans index)
            ;; await processing of the events by the reference notify forwarder
            [value channel] (async/alts!! [response-chan (async/timeout 1000)])
            notified-event-time (get-in @notified-events-atom [event-id :event-time])
            forwarded-event-time (get-in @forwarded-events-atom [event-id :event-time])]
        (is (= {:success true} value))
        (is (= response-chan channel) (str value))
        (is (and forwarded-event-time notified-event-time (<= notified-event-time forwarded-event-time))
            (str {:event-id event-id :forwarded-event-time forwarded-event-time :notified-event-time notified-event-time}))))

    (async/close! reference-update-chan)))
