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
(ns waiter.scheduler-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as protocols]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [slingshot.slingshot :as ss]
            [waiter.config :as config]
            [waiter.core :as core]
            [waiter.correlation-id :as cid]
            [waiter.curator :as curator]
            [waiter.headers :as headers]
            [waiter.kv :as kv]
            [waiter.metrics :as metrics]
            [waiter.scheduler :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.token :as tk]
            [waiter.util.async-utils :as au]
            [waiter.util.client-tools :as ct]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (java.net ConnectException SocketTimeoutException)
           (java.util.concurrent TimeoutException)
           (org.eclipse.jetty.client HttpClient)
           (org.eclipse.jetty.http HttpField)
           (org.joda.time DateTime)))

(def ^:const history-length 5)
(def ^:const limit-per-owner 10)

(let [lock (Object.)]
  (defn- synchronize-fn
    [_ f]
    (locking lock
      (f))))

(deftest test-service-scale-down-throttle-cache
  (testing "can-bypass-service-scale-down? returns true initially for a service-id"
    (is (can-bypass-service-scale-down? "test-throttle-s1"))
    (is (can-bypass-service-scale-down? "test-throttle-s2"))
    (is (can-bypass-service-scale-down? "test-throttle-s3")))
  (testing "after the configured ttl is up can-bypass-service-scale-down? returns true"
    (let [now (t/now)]
      (with-redefs [t/now (constantly now)]
        (set-bypass-service-scale-down "test-throttle-s1")
        (is (not (can-bypass-service-scale-down? "test-throttle-s1"))))
      ;; service still can't scale down right before throttle-secs is reached
      (with-redefs [t/now (constantly (t/plus (t/now) (t/seconds (dec service-scale-down-throttle-secs))))]
        (is (not (can-bypass-service-scale-down? "test-throttle-s1"))))
      ;; t/now is now passed the throttle-secs; service should be able to scale down
      (with-redefs [t/now (constantly (t/plus (t/now) (t/seconds service-scale-down-throttle-secs)))]
        (is (can-bypass-service-scale-down? "test-throttle-s1")))
      ;; wait until cache is expired
      (utils/sleep (* service-scale-down-throttle-secs 1001))
      ;; service should be able to scale down
      (with-redefs [t/now now]
        (is (can-bypass-service-scale-down? "test-throttle-s1"))))))

(deftest test-record-Service
  (let [test-instance-1 (->Service "service1-id" 100 100 {:running 0, :healthy 0, :unhealthy 0, :staged 0})
        test-instance-2 (make-Service {:id "service2-id" :instances 200 :task-count 200})
        test-instance-3 (make-Service {:id "service1-id" :instances 100 :task-count 100})]
    (testing (str "Test record Service.1")
      (is (= "service1-id" (:id test-instance-1)))
      (is (= 100 (:instances test-instance-1)))
      (is (= 100 (:task-count test-instance-1))))
    (testing (str "Test record Service.2")
      (is (= "service2-id" (:id test-instance-2)))
      (is (= 200 (:instances test-instance-2)))
      (is (= 200 (:task-count test-instance-2))))
    (testing (str "Test record Service equality")
      (is (= test-instance-1 test-instance-3)))))

(deftest test-record-ServiceInstance
  (let [start-time (du/str-to-date "2014-09-13T00:24:46.959Z" du/formatter-iso8601)
        prepared-to-scale-down-at (du/str-to-date "2014-09-13T00:24:47.959Z" du/formatter-iso8601)
        test-instance (->ServiceInstance
                        "instance-id"
                        "service-id"
                        start-time
                        true
                        http-200-ok
                        #{}
                        nil
                        "www.scheduler-test.example.com"
                        1234
                        prepared-to-scale-down-at
                        []
                        "log-dir"
                        "instance-message"
                        nil)]
    (testing (str "Test record ServiceInstance")
      (is (= "instance-id" (:id test-instance)))
      (is (= "service-id" (:service-id test-instance)))
      (is (= start-time (:started-at test-instance)))
      (is (true? (:healthy? test-instance)))
      (is (= http-200-ok (:health-check-status test-instance)))
      (is (= "www.scheduler-test.example.com" (:host test-instance)))
      (is (= 1234 (:port test-instance)))
      (is (= prepared-to-scale-down-at (:prepared-to-scale-down-at test-instance)))
      (is (= "log-dir" (:log-directory test-instance)))
      (is (= "instance-message" (:message test-instance))))))

(deftest test-instance->service-id
  (let [test-cases [
                    {:name "nil-input", :input nil, :expected nil}
                    {:name "empty-input", :input {}, :expected nil}
                    {:name "string-input", :input "foo-bar-123", :expected nil}
                    {:name "valid-input", :input {:service-id "foo-bar-123"}, :expected "foo-bar-123"}
                    {:name "missing-id-input", :input {:name "foo-bar-123"}, :expected nil}
                    {:name "slash-id-input", :input {:service-id "/foo-bar-123"}, :expected "/foo-bar-123"}]]
    (doseq [{:keys [name input expected]} test-cases]
      (testing (str name)
        (is (= expected (instance->service-id input)))))))

(deftest test-sort-instances
  (let [i1 {:id "i11" :service-id "s1" :started-at "3456"}
        i2 {:id "i02" :service-id "s1" :started-at "1234"}
        i3 {:id "i13" :service-id "s1" :started-at "2345"}
        i4 {:id "i04" :service-id "s1" :started-at "3456"}
        i5 {:id "i15" :service-id "s2"}
        i6 {:id "i06" :service-id "s2"}]
    (testing (str "scheduler/sort-instances")
      (is (= [i2 i1] (sort-instances [i1 i2])))
      (is (= [i4 i1] (sort-instances [i1 i4])))
      (is (= [i1 i5] (sort-instances [i1 i5])))
      (is (= [i6 i5] (sort-instances [i5 i6])))
      (is (= [i2 i3] (sort-instances [i2 i3])))
      (is (= [i2 i3 i4 i1] (sort-instances [i1 i2 i3 i4])))
      (is (= [i2 i3 i4 i1 i6 i5] (sort-instances [i1 i2 i3 i4 i5 i6]))))))

(deftest test-scheduler-services-gc
  (let [curator (Object.)
        gc-base-path "/test-path/gc-base-path"
        leader? (constantly true)
        state-store (atom {})
        read-state-fn (fn [name] (:data (curator/read-path curator (str gc-base-path "/" name) :nil-on-missing? true :serializer :nippy)))
        write-state-fn (fn [name state] (curator/write-path curator (str gc-base-path "/" name) state :serializer :nippy :create-parent-zknodes? true))
        get-service-state-fn (fn [service-id] (-> (read-state-fn "scheduler-services-gc") (get service-id) (dissoc :last-modified-time)))]
    (with-redefs [curator/read-path (fn [_ path & _] {:data (get @state-store path)})
                  curator/write-path (fn [_ path data & _] (swap! state-store (fn [v] (assoc v path data))))]
      (let [available-services-atom (atom #{"service01" "service02" "service03" "service04stayalive" "service05"
                                            "service06faulty" "service07" "service08stayalive" "service09stayalive"
                                            "service10broken" "service11broken" "service12missingmetrics"
                                            "service13zerooutmetrics" "service14eternal" "service15eternal"})
            initial-global-state {"service01" {"last-request-time" (tc/from-long 10)
                                               "outstanding" 0}
                                  "service02" {"last-request-time" (tc/from-long 20)
                                               "outstanding" 5}
                                  "service03" {"outstanding" 0} ;; missing last-request-time
                                  "service04stayalive" {"outstanding" 1000} ;; missing last-request-time
                                  "service05" {"last-request-time" (tc/from-long 50)
                                               "outstanding" 10}
                                  "service06faulty" {"last-request-time" (tc/from-long 60)
                                                     "outstanding" 2000}
                                  "service07" {"last-request-time" (tc/from-long 70)
                                               "outstanding" 15}
                                  "service08stayalive" {"last-request-time" (tc/from-long 80)
                                                        "outstanding" 3000}
                                  "service09stayalive" {"last-request-time" (tc/from-long 80)
                                                        "outstanding" 70}
                                  "service10broken" {"last-request-time" (tc/from-long 80)
                                                     "outstanding" 70}
                                  "service11broken" {"last-request-time" (tc/from-long 80)
                                                     "outstanding" 95}
                                  "service12missingmetrics" {"last-request-time" (tc/from-long 30)
                                                             "outstanding" 24}
                                  "service13zerooutmetrics" {"last-request-time" (tc/from-long 40)
                                                             "outstanding" 5000}
                                  "service14eternal" {"last-request-time" (tc/from-long 30)
                                                      "outstanding" 20}
                                  "service15eternal" {"last-request-time" (tc/from-long 20)
                                                      "outstanding" 0}}
            deleted-services-atom (atom #{})
            scheduler (reify ServiceScheduler
                        (delete-service [_ service-id]
                          (swap! available-services-atom disj service-id)
                          (swap! deleted-services-atom conj service-id)))
            scheduler-state-chan (async/chan 1)
            metrics-chan (async/chan 1)
            service-id->metrics-fn (fn service-id->metrics-fn []
                                     (let [[value channel] (async/alts!! [metrics-chan (async/timeout 100)])]
                                       (when (= metrics-chan channel) value)))
            iteration-counter (atom 0)
            test-start-time (t/now)
            clock (fn [] (t/plus test-start-time (t/minutes @iteration-counter)))
            service-gc-go-routine (partial core/service-gc-go-routine read-state-fn write-state-fn leader? clock)]
        (testing "scheduler-services-gc"
          (let [timeout-interval-ms 1
                broken-service-timeout-mins 5
                broken-service-min-hosts 2
                service->gc-time-fn (fn [service-id last-modified-time]
                                      (when-not (str/includes? service-id "eternal")
                                        (t/plus last-modified-time (t/minutes 50))))
                query-state-fn (fn [] (async/<!! scheduler-state-chan))
                channel-map (scheduler-services-gc
                              scheduler query-state-fn service-id->metrics-fn
                              {:broken-service-min-hosts broken-service-min-hosts
                               :broken-service-timeout-mins broken-service-timeout-mins
                               :scheduler-gc-interval-ms timeout-interval-ms}
                              service-gc-go-routine service->gc-time-fn)
                service-gc-exit-chan (:exit channel-map)]
            (dotimes [n 100]
              (let [global-state (->> (map (fn [[service-id state]]
                                             [service-id
                                              (when (or (not (str/includes? service-id "missingmetrics"))
                                                        (< n 10))
                                                (cond-> (update state "outstanding" (fn [v] (max 0 (- v n))))
                                                  (str/includes? service-id "zerooutmetrics")
                                                  (assoc "outstanding" 0)
                                                  (-> n inc (mod 5) zero?)
                                                  (dissoc "last-request-time")
                                                  (-> n inc (mod 7) zero?)
                                                  (assoc "last-request-time" (tc/from-long 1))))])
                                           initial-global-state)
                                      (into {}))]
                (async/>!! scheduler-state-chan {:all-available-service-ids (set @available-services-atom)})
                (async/>!! metrics-chan global-state)
                (Thread/sleep 2)
                (swap! iteration-counter inc)))
            (async/>!! service-gc-exit-chan :exit)
            (is (= #{"service01" "service02" "service03" "service05" "service07" "service13zerooutmetrics"}
                   @deleted-services-atom))
            (is (= #{"service04stayalive" "service06faulty" "service08stayalive" "service09stayalive"
                     "service10broken" "service11broken" "service12missingmetrics" "service14eternal" "service15eternal"}
                   @available-services-atom))
            (is (= {:state {"last-request-time" (tc/from-long 30)
                            "outstanding" 15}}
                   (get-service-state-fn "service12missingmetrics")))
            (is (= {:state {"last-request-time" (tc/from-long 30)
                            "outstanding" 0}}
                   (get-service-state-fn "service14eternal")))
            (is (= {:state {"last-request-time" (tc/from-long 20)
                            "outstanding" 0}}
                   (get-service-state-fn "service15eternal")))
            ;; ensures last-request-time was not last when it was absent
            (is (= (pc/map-vals #(get % "last-request-time" (tc/from-long 1))
                                (select-keys initial-global-state @available-services-atom))
                   (pc/map-vals #(get-in % [:state "last-request-time"])
                                (read-state-fn "scheduler-services-gc"))))))))))

(deftest test-scheduler-broken-services-gc
  (let [leader? (constantly true)
        state-store (atom [])
        read-state-fn (fn [name] (-> @state-store (last) (get name)))
        max-iterations 20
        iteration-counter (atom 0)
        test-start-time (t/now)
        clock (fn [] (-> test-start-time
                         (t/plus (t/minutes @iteration-counter))
                         (t/plus (t/millis @iteration-counter))))
        write-state-fn (fn [name state]
                         (swap! iteration-counter #(min max-iterations (inc %)))
                         (swap! state-store conj {name state
                                                  :iteration @iteration-counter
                                                  :time (clock)}))
        service-id->service-description-fn (fn [service-id]
                                             (let [id (str/replace service-id #"[^0-9]" "")
                                                   id-int (Integer/parseInt id)]
                                               {"grace-period-secs" (* 60 id-int)}))]
    (let [available-services-atom (atom #{"service5broken" "service6faulty" "service7" "service8stayalive"
                                          "service10broken" "service11broken" "service14broken"})
          deleted-services-atom (atom #{})
          scheduler (reify ServiceScheduler
                      (delete-service [_ service-id]
                        (swap! available-services-atom disj service-id)
                        (swap! deleted-services-atom conj service-id)))
          scheduler-state-atom (atom nil)
          service-gc-go-routine (partial core/service-gc-go-routine read-state-fn write-state-fn leader? clock)]
      (testing "scheduler-broken-services-gc"
        (let [timeout-interval-ms 10
              broken-service-timeout-mins 5
              broken-service-min-hosts 3
              query-state-fn (fn [] @scheduler-state-atom)
              channel-map (scheduler-broken-services-gc service-gc-go-routine query-state-fn service-id->service-description-fn scheduler
                                                        {:broken-service-min-hosts broken-service-min-hosts
                                                         :broken-service-timeout-mins broken-service-timeout-mins
                                                         :scheduler-gc-broken-service-interval-ms timeout-interval-ms})
              service-gc-exit-chan (:exit channel-map)]
          (while (< @iteration-counter 20)
            (let [iteration (min (dec max-iterations) @iteration-counter)]
              (reset!
                scheduler-state-atom
                {:all-available-service-ids (set @available-services-atom)
                 :service-id->failed-instances
                 (pc/map-from-keys
                   (fn [service-id]
                     (cond
                       (str/includes? service-id "broken")
                       (map (fn [index] {:id (str service-id ".failed" index) :host (str "failed" index "-host.example.com")})
                            (range (inc (mod iteration 4))))
                       (str/includes? service-id "faulty")
                       (map (fn [index] {:id (str service-id ".faulty" index) :host "faulty-host.example.com"})
                            (range (mod iteration 4)))
                       :else []))
                   @available-services-atom)
                 :service-id->healthy-instances
                 (pc/map-from-keys
                   (fn [service-id]
                     (if (str/includes? service-id "broken") [] [{:id (str service-id ".unhealthy")}]))
                   @available-services-atom)})
              (utils/sleep timeout-interval-ms)))
          (async/>!! service-gc-exit-chan :exit)
          (let [assertion-msg (str @state-store)]
            (is (= #{"service5broken" "service10broken" "service11broken"} @deleted-services-atom) assertion-msg)
            (is (= #{"service6faulty" "service7" "service8stayalive" "service14broken"} @available-services-atom) assertion-msg)))))))

(deftest test-scheduler-syncer
  (let [clock t/now
        scheduler-state-chan (au/latest-chan)
        trigger-chan (async/chan 1)
        service-id->service-description-fn (fn [id] {"backend-proto" "http"
                                                     "health-check-authentication" "standard"
                                                     "health-check-proto" "https"
                                                     "health-check-port-index" 2
                                                     "health-check-url" (str "/" id)})
        started-at (t/minus (clock) (t/hours 1))
        instance1 (->ServiceInstance "s1.i1" "s1" started-at nil nil #{} nil "host" 123 nil [] "/log" "test" "Unhealthy")
        instance2 (->ServiceInstance "s1.i2" "s1" started-at true nil #{} nil "host" 123 nil [] "/log" "test" "Healthy")
        instance3 (->ServiceInstance "s1.i3" "s1" started-at nil nil #{} nil "host" 123 nil [] "/log" "test" "Unhealthy")
        instance4 (->ServiceInstance "s1.i4" "s1" started-at nil nil #{} nil "host" 123 nil [] "/log" "test" "Unhealthy")
        instance5 (->ServiceInstance "s1.i5" "s1" started-at nil nil #{} nil "host" 123 nil [] "/log" "test" "Unhealthy")
        instance6 (->ServiceInstance "s1.i6" "s1" started-at nil nil #{} nil "host" 123 nil [] "/log" "test" "Unhealthy")
        get-service->instances-invocation-count (atom 0)
        get-service->instances (fn []
                                 (swap! get-service->instances-invocation-count inc)
                                 {(->Service "s1" {} {} {}) {:active-instances (cond-> [instance1 instance2 instance3]
                                                                                 (= 1 @get-service->instances-invocation-count) (conj instance4 instance6))
                                                             :failed-instances [instance5]}
                                  (->Service "s2" {} {} {}) {:active-instances []
                                                             :failed-instances []}})
        available? (fn [_ {:keys [id]} {:strs [backend-proto health-check-authentication health-check-proto
                                               health-check-port-index health-check-url]}]
                     (async/go (cond
                                 (and (= "s1.i1" id)
                                      (= "standard" health-check-authentication)
                                      (= "https" (or health-check-proto backend-proto))
                                      (= 2 health-check-port-index)
                                      (= "/s1" health-check-url))
                                 {:healthy? true, :status http-200-ok}
                                 :else
                                 {:healthy? false, :status http-400-bad-request})))
        start-time-ms (.getMillis (clock))
        failed-check-threshold 1
        scheduler-name "test-scheduler"]
    (with-redefs [is-kill-candidate? (fn [instance-id] (= instance-id (get instance6 :id)))]
      (let [{:keys [exit-chan query-chan retrieve-syncer-state-fn]}
            (start-scheduler-syncer clock trigger-chan service-id->service-description-fn available?
                                    failed-check-threshold scheduler-name get-service->instances scheduler-state-chan)
            make-unhealthy-instance (fn [instance]
                                      (assoc instance
                                        :flags #{:has-connected :has-responded}
                                        :healthy? false
                                        :health-check-status http-400-bad-request))
            instance3-unhealthy (make-unhealthy-instance instance3)
            instance4-unhealthy (make-unhealthy-instance instance4)
            instance6-unhealthy (make-unhealthy-instance instance6)]
        (let [response-chan (async/promise-chan)]
          (async/>!! query-chan {:response-chan response-chan :service-id "s0"})
          (is (= {:last-update-time nil} (async/<!! response-chan)))
          (is (= {} (retrieve-syncer-state-fn))))
        (async/>!! trigger-chan :trigger)
        (let [[[update-apps-msg update-apps] [update-instances-msg update-instances]] (async/<!! scheduler-state-chan)]
          (is (= :update-available-services update-apps-msg))
          (is (= #{"s1" "s2"} (:available-service-ids update-apps)))
          (is (= #{"s1"} (:healthy-service-ids update-apps)))
          (is (= :update-service-instances update-instances-msg))
          (is (= [(assoc instance1 :healthy? true) instance2] (:healthy-instances update-instances)))
          (is (= [instance3-unhealthy instance4-unhealthy instance6-unhealthy] (:unhealthy-instances update-instances)))
          (is (= "s1" (:service-id update-instances))))
        ;; Retrieves scheduler state without service-id
        (let [response-chan (async/promise-chan)
              _ (async/>!! query-chan {:response-chan response-chan})
              response (async/alt!!
                         response-chan ([state] state)
                         (async/timeout 10000) ([_] {:message "Request timed out!"}))]
          (is (contains? response :service-id->health-check-context))
          (is (= {"s1" {:instance-id->unhealthy-instance {"s1.i3" instance3-unhealthy
                                                          "s1.i4" instance4-unhealthy
                                                          "s1.i6" instance6-unhealthy},
                        :instance-id->tracked-failed-instance {},
                        :instance-id->failed-health-check-count {"s1.i3" 1
                                                                 "s1.i4" 1
                                                                 "s1.i6" 1}}
                  "s2" {:instance-id->failed-health-check-count {}
                        :instance-id->tracked-failed-instance {}
                        :instance-id->unhealthy-instance {}}}
                 (:service-id->health-check-context response)))
          (is (= {"s1" {:instance-id->unhealthy-instance {"s1.i3" instance3-unhealthy
                                                          "s1.i4" instance4-unhealthy
                                                          "s1.i6" instance6-unhealthy},
                        :instance-id->tracked-failed-instance {},
                        :instance-id->failed-health-check-count {"s1.i3" 1
                                                                 "s1.i4" 1
                                                                 "s1.i6" 1}}
                  "s2" {:instance-id->failed-health-check-count {}
                        :instance-id->tracked-failed-instance {}
                        :instance-id->unhealthy-instance {}}}
                 (:service-id->health-check-context (retrieve-syncer-state-fn)))))
        ;; Retrieves scheduler state with service-id
        (let [response-chan (async/promise-chan)
              _ (async/>!! query-chan {:response-chan response-chan :service-id "s1"})
              response (async/alt!!
                         response-chan ([state] state)
                         (async/timeout 10000) ([_] {:message "Request timed out!"}))
              end-time-ms (.getMillis (clock))]
          (doseq [required-key [:instance-id->failed-health-check-count
                                :instance-id->tracked-failed-instance
                                :instance-id->unhealthy-instance
                                :last-update-time]]
            (is (contains? response required-key)))
          (is (nil? (:service-id->health-check-context response)))
          (is (<= start-time-ms (-> response :last-update-time .getMillis) end-time-ms)))
        ;; instance4 and instance6 go missing, instance4 will be tracked as failed, instance6 skipped as a kill candidate
        (async/>!! trigger-chan :trigger)
        (let [[[update-apps-msg update-apps] [update-instances-msg update-instances]] (async/<!! scheduler-state-chan)]
          (is (= :update-available-services update-apps-msg))
          (is (= #{"s1" "s2"} (:available-service-ids update-apps)))
          (is (= #{"s1"} (:healthy-service-ids update-apps)))
          (is (= :update-service-instances update-instances-msg))
          (is (= [(assoc instance1 :healthy? true) instance2] (:healthy-instances update-instances)))
          (is (= [instance3-unhealthy] (:unhealthy-instances update-instances)))
          (is (= "s1" (:service-id update-instances))))
        ;; Retrieves scheduler state without service-id
        (let [response-chan (async/promise-chan)
              _ (async/>!! query-chan {:response-chan response-chan})
              response (async/alt!!
                         response-chan ([state] state)
                         (async/timeout 10000) ([_] {:message "Request timed out!"}))
              instance4-unhealthy (update instance4-unhealthy :flags conj :never-passed-health-checks)]
          (is (contains? response :service-id->health-check-context))
          (is (= {"s1" {:instance-id->unhealthy-instance {"s1.i3" instance3-unhealthy},
                        :instance-id->tracked-failed-instance {"s1.i4" instance4-unhealthy},
                        :instance-id->failed-health-check-count {"s1.i3" 2}}
                  "s2" {:instance-id->failed-health-check-count {}
                        :instance-id->tracked-failed-instance {}
                        :instance-id->unhealthy-instance {}}}
                 (:service-id->health-check-context response)))
          (is (= {"s1" {:instance-id->unhealthy-instance {"s1.i3" instance3-unhealthy},
                        :instance-id->tracked-failed-instance {"s1.i4" instance4-unhealthy},
                        :instance-id->failed-health-check-count {"s1.i3" 2}}
                  "s2" {:instance-id->failed-health-check-count {}
                        :instance-id->tracked-failed-instance {}
                        :instance-id->unhealthy-instance {}}}
                 (:service-id->health-check-context (retrieve-syncer-state-fn)))))
        (async/>!! exit-chan :exit)))))

(deftest test-start-health-checks
  (let [available-instance "id1"
        service {:id "s1"}
        available? (fn [_ instance _]
                     (async/go
                       (let [healthy? (= (:id instance) available-instance)]
                         {:healthy? healthy?
                          :status (if healthy? http-200-ok http-400-bad-request)})))
        scheduler-name "test-scheduler"
        service->service-description-fn (constantly {:health-check-url "/health"})]
    (testing "Does not call available? for healthy apps"
      (let [service->service-instances {service {:active-instances [{:id "id1"
                                                                     :healthy? true}
                                                                    {:id "id2"
                                                                     :healthy? true}]}}
            service->service-instances' (start-health-checks scheduler-name
                                                             service->service-instances
                                                             (fn [_ _ _] (async/go false))
                                                             service->service-description-fn)
            active-instances (get-in service->service-instances' [service :active-instances])]
        (is (= 2 (count active-instances)))
        (is (and (map :healthy? active-instances)))))
    (testing "Calls available? for unhealthy apps"
      (let [service->service-instances {service {:active-instances [{:id "id1"}
                                                                    {:id "id2"
                                                                     :healthy? false}
                                                                    {:id "id3"
                                                                     :healthy? true}]}}
            service->service-instances' (start-health-checks scheduler-name
                                                             service->service-instances
                                                             available?
                                                             service->service-description-fn)
            active-instances (get-in service->service-instances' [service :active-instances])]
        (let [[instance1 instance2 instance3] (map async/<!! active-instances)]
          (is (:healthy? instance1))
          (is (not (:healthy? instance2)))
          (is (:healthy? instance3)))))))

(deftest test-do-health-checks
  (let [available-instance "id1"
        service {:id "s1"}
        available? (fn [_ {:keys [id]} _]
                     (async/go
                       (let [healthy? (= id available-instance)]
                         {:healthy? healthy?
                          :status (if healthy? http-200-ok http-400-bad-request)})))
        scheduler-name "test-scheduler"
        service->service-description-fn (constantly {:health-check-url "/healthy"})
        service->service-instances {service {:active-instances [{:id available-instance}
                                                                {:id "id2"
                                                                 :healthy? false}
                                                                {:id "id3"
                                                                 :healthy? true}]}}
        service->service-instances' (do-health-checks scheduler-name
                                                      service->service-instances
                                                      available?
                                                      service->service-description-fn)]
    (let [[instance1 instance2 instance3] (get-in service->service-instances' [service :active-instances])]
      (is (:healthy? instance1))
      (is (not (:healthy? instance2)))
      (is (:healthy? instance3)))))

(deftest test-retry-on-transient-server-exceptions
  (testing "successful-result-on-first-call"
    (let [call-counter (atom 0)
          call-result {:foo :bar}
          function (fn [] (swap! call-counter inc) call-result)]
      (is (= call-result (retry-on-transient-server-exceptions "test" (function))))
      (is (= 1 @call-counter))))

  (testing "successful-result-on-third-call"
    (let [call-counter (atom 0)
          call-result {:foo :bar}
          function (fn []
                     (swap! call-counter inc)
                     (when (< @call-counter 3)
                       (ss/throw+ {:status http-501-not-implemented}))
                     call-result)]
      (is (= call-result (retry-on-transient-server-exceptions "test" (function))))
      (is (= 3 @call-counter))))

  (testing "successful-result-on-connect-exceptions"
    (let [call-counter (atom 0)
          call-result {:foo :bar}
          function (fn []
                     (swap! call-counter inc)
                     (when (< @call-counter 3)
                       (throw (ConnectException. "test")))
                     call-result)]
      (is (= call-result (retry-on-transient-server-exceptions "test" (function))))
      (is (= 3 @call-counter))))

  (testing "successful-result-on-socket-timeout-exceptions"
    (let [call-counter (atom 0)
          call-result {:foo :bar}
          function (fn []
                     (swap! call-counter inc)
                     (when (< @call-counter 3)
                       (throw (SocketTimeoutException. "test")))
                     call-result)]
      (is (= call-result (retry-on-transient-server-exceptions "test" (function))))
      (is (= 3 @call-counter))))

  (testing "successful-result-on-timeout-exceptions"
    (let [call-counter (atom 0)
          call-result {:foo :bar}
          function (fn []
                     (swap! call-counter inc)
                     (when (< @call-counter 3)
                       (throw (TimeoutException. "test")))
                     call-result)]
      (is (= call-result (retry-on-transient-server-exceptions "test" (function))))
      (is (= 3 @call-counter))))

  (testing "failure-on-non-transient-exception-throw"
    (let [call-counter (atom 0)
          function (fn [] (swap! call-counter inc) (throw (Exception. "test")))]
      (is (thrown-with-msg?
            Exception #"test"
            (retry-on-transient-server-exceptions "test" (function))))
      (is (= 1 @call-counter))))

  (testing "failure-on-non-transient-connect-exception-throw"
    (let [call-counter (atom 0)
          function (fn [] (swap! call-counter inc) (throw (ConnectException. "test")))]
      (is (thrown-with-msg?
            ConnectException #"test"
            (retry-on-transient-server-exceptions "test" (function))))
      (is (= 5 @call-counter))))

  (testing "failure-on-non-transient-socket-timeout-exception-throw"
    (let [call-counter (atom 0)
          function (fn [] (swap! call-counter inc) (throw (SocketTimeoutException. "test")))]
      (is (thrown-with-msg?
            SocketTimeoutException #"test"
            (retry-on-transient-server-exceptions "test" (function))))
      (is (= 5 @call-counter))))

  (testing "failure-on-non-transient-timeout-exception-throw"
    (let [call-counter (atom 0)
          function (fn [] (swap! call-counter inc) (throw (TimeoutException. "test")))]
      (is (thrown-with-msg?
            TimeoutException #"test"
            (retry-on-transient-server-exceptions "test" (function))))
      (is (= 5 @call-counter))))

  (testing (str "failure-on-non-transient-exception-throw+")
    (doseq [status [http-300-multiple-choices http-302-moved-temporarily http-400-bad-request http-404-not-found]]
      (let [call-counter (atom 0)
            function (fn [] (swap! call-counter inc) (ss/throw+ {:status status}))]
        (is (thrown-with-msg?
              Exception #"test"
              (ss/try+
                (retry-on-transient-server-exceptions "test" (function))
                (catch [:status status] _
                  (throw (Exception. "test"))))))
        (is (= 1 @call-counter)))))

  (testing (str "failure-repeating-transient-exception")
    (doseq [status [http-500-internal-server-error http-501-not-implemented http-502-bad-gateway http-503-service-unavailable http-504-gateway-timeout]]
      (let [call-counter (atom 0)
            function (fn [] (swap! call-counter inc) (ss/throw+ {:status status}))]
        (is (thrown-with-msg?
              Exception #"test"
              (ss/try+
                (retry-on-transient-server-exceptions "test" (function))
                (catch [:status status] _
                  (throw (Exception. "test"))))))
        (is (= 5 @call-counter))))))

(defmacro assert-health-check-headers
  [health-check-authentication http-client service-password waiter-principal request-headers]
  `(let [health-check-authentication# ~health-check-authentication
         http-client# ~http-client
         service-password# ~service-password
         waiter-principal# ~waiter-principal
         request-headers# ~request-headers
         expected-headers# (cond-> {"host" "www.example.com"
                                    "user-agent" (some-> http-client# .getUserAgentField .getValue)
                                    "x-waiter-request-type" "health-check"}
                             (= "standard" health-check-authentication#)
                             (merge (headers/retrieve-basic-auth-headers "waiter" service-password# waiter-principal#)))]
     (is (contains? request-headers# "x-cid"))
     (is (= expected-headers# (dissoc request-headers# "x-cid")))))

(deftest test-available?
  (let [http-client (HttpClient.)
        service-password "test-password"
        service-id->password-fn (constantly service-password)
        make-scheduler (fn [service-id->service-description]
                         (reify ServiceScheduler
                           (request-protocol [_ _ i sd]
                             (retrieve-protocol i sd))
                           (use-authenticated-health-checks? [_ service-id]
                             (-> (service-id->service-description service-id)
                               (authenticated-health-check-configured?)))))
        scheduler-name "test-scheduler"
        waiter-principal "waiter@test.com"
        health-check-proto "http"
        available-fn? (fn available-fn? [service-instance service-description]
                        (let [scheduler (make-scheduler (constantly service-description))]
                          (available? service-id->password-fn http-client scheduler
                                      scheduler-name service-instance service-description)))
        service-instance {:extra-ports [81] :host "www.example.com" :port 80}
        service-description {"backend-proto" health-check-proto
                             "health-check-proto" health-check-proto
                             "health-check-port-index" 0
                             "health-check-url" "/health-check"}]

    (.setConnectTimeout http-client 200)
    (.setIdleTimeout http-client 200)
    (.setUserAgentField http-client (HttpField. "user-agent" "waiter-test"))

    (with-redefs [config/retrieve-request-log-request-headers (constantly  #{"content-type" "host"})
                  config/retrieve-request-log-response-headers (constantly #{"content-type" "server"})
                  config/retrieve-waiter-principal (constantly waiter-principal)]

      (testing "unknown host"
        (let [service-instance {:host "www.example.com"}
              resp (async/<!! (available-fn? service-instance service-description))]
          (is (= {:error :unknown-authority :healthy? false} resp)))
        (let [service-instance {:host "www.example.com" :port 0}
              resp (async/<!! (available-fn? service-instance service-description))]
          (is (= {:error :unknown-authority :healthy? false} resp)))
        (let [service-instance {:port 80}
              resp (async/<!! (available-fn? service-instance service-description))]
          (is (= {:error :unknown-authority :healthy? false} resp)))
        (let [service-instance {:host "0.0.0.0" :port 80}
              resp (async/<!! (available-fn? service-instance service-description))]
          (is (= {:error :unknown-authority :healthy? false} resp))))

      (doseq [health-check-authentication ["disabled" "standard"]]
        (let [service-description (assoc service-description "health-check-authentication" health-check-authentication)]

          (with-redefs [http/get (fn [in-http-client in-health-check-url {:keys [headers]}]
                                   (is (= http-client in-http-client))
                                   (is (= "http://www.example.com:80/health-check" in-health-check-url))
                                   (assert-health-check-headers
                                     health-check-authentication http-client service-password waiter-principal headers)
                                   (let [response-chan (async/promise-chan)]
                                     (async/>!! response-chan {:status http-200-ok})
                                     response-chan))]
            (let [resp (async/<!! (available-fn? service-instance service-description))]
              (is (= {:error nil, :healthy? true, :status http-200-ok} resp))))

          (with-redefs [http/get (fn [in-http-client in-health-check-url {:keys [headers]}]
                                   (is (= http-client in-http-client))
                                   (is (= "http://www.example.com:81/health-check" in-health-check-url))
                                   (assert-health-check-headers
                                     health-check-authentication http-client service-password waiter-principal headers)
                                   (throw (IllegalArgumentException. "Unable to make request")))]
            (let [service-description (assoc service-description "health-check-port-index" 1)
                  resp (async/<!! (available-fn? service-instance service-description))]
              (is (= {:healthy? false} resp))))

          (let [abort-chan-atom (atom nil)]
            (with-redefs [http/get (fn [in-http-client in-health-check-url {:keys [headers] :as in-request-config}]
                                     (reset! abort-chan-atom (:abort-ch in-request-config))
                                     (is (= http-client in-http-client))
                                     (is (= "http://www.example.com:80/health-check" in-health-check-url))
                                     (assert-health-check-headers
                                       health-check-authentication http-client service-password waiter-principal headers)
                                     (async/promise-chan))]
              (let [resp (async/<!! (available-fn? service-instance service-description))]
                (is (= {:error :operation-timeout, :healthy? false, :status nil} resp)))
              (let [abort-chan @abort-chan-atom]
                (is (au/chan? abort-chan))
                (when (au/chan? abort-chan)
                  (is (protocols/closed? abort-chan))
                  (let [[abort-ex abort-cb] (async/<!! abort-chan)]
                    (is (instance? TimeoutException abort-ex))
                    (is abort-cb)))))))))))

(defmacro assert-expected-events-new-services-daemon
  [token-metric-chan process-token-event-ch start-new-service-fn-calls token-metric-chan-events expected-result-events
   expected-start-new-service-calls]
  `(let [token-metric-chan# ~token-metric-chan
         process-token-event-ch# ~process-token-event-ch
         start-new-service-fn-calls# ~start-new-service-fn-calls
         token-metric-chan-events# ~token-metric-chan-events
         expected-result-events# ~expected-result-events
         expected-start-new-service-calls# ~expected-start-new-service-calls]
     ; push fake events in token-metric-chan-mult
     (async/onto-chan!! token-metric-chan# token-metric-chan-events# false)

     ; do assertions on process-token-event-ch
     (doseq [event# expected-result-events#]
       (is (= event# (async/<!! process-token-event-ch#))))

     ; no other messages should be in process-token-event-ch
     (let [timeout-ch# (async/timeout 500)
           [msg# res-ch#] (async/alts!! [process-token-event-ch# timeout-ch#])]
       (is (= timeout-ch# res-ch#) msg#))

     (is (= expected-start-new-service-calls#
            @start-new-service-fn-calls#))))

(defmacro check-trackers
  [all-trackers assertion-maps]
  `(let [assertion-maps# ~assertion-maps
         all-trackers# ~all-trackers]
     (is (= (count all-trackers#) (count assertion-maps#)))
     (doseq [tracker-entry# all-trackers#]
       (let [[service-id#
              {actual-known-instance-ids# :known-instance-ids
               actual-instance-scheduling-start-times# :instance-scheduling-start-times
               actual-starting-instance-id->start-timestamp# :starting-instance-id->start-timestamp}] tracker-entry#
             actual-starting-instance-ids# (keys actual-starting-instance-id->start-timestamp#)
             {expected-known-instance-ids# :known-instance-ids
              expected-scheduling-instance-count# :scheduling-instance-count
              expected-starting-instance-ids# :starting-instance-ids
              :or {expected-known-instance-ids# #{}
                   expected-scheduling-instance-count# 0
                   expected-starting-instance-ids# []} :as assertion-map#} (get assertion-maps# service-id#)]
         (is (= expected-known-instance-ids# actual-known-instance-ids#))
         (is (= expected-scheduling-instance-count# (count actual-instance-scheduling-start-times#)))
         (is (= (sort expected-starting-instance-ids#)
                (sort actual-starting-instance-ids#)))))))

(def base-start-time (t/minus (t/now) (t/minutes 1)))

(defn- make-service-instance
  [service-number instance-number]
  (let [offset-seconds (t/seconds (+ service-number instance-number))]
    {:id (str "inst-" service-number \. instance-number)
     :service-id (str "service-" service-number)
     :started-at (t/plus base-start-time offset-seconds)}))

(deftest test-update-launch-trackers
  (let [empty-trackers {}
        empty-new-service-ids #{}
        empty-removed-service-ids #{}
        empty-service-id->healthy-instances {}
        empty-service-id->unhealthy-instances {}
        empty-service-id->instance-counts {}
        empty-service-id->service-description {}
        leader? true
        req1 {:requested 1}
        req2 {:requested 2}
        req3 {:requested 3}
        req5 {:requested 5}
        waiter-timer (metrics/waiter-timer "launch-overhead" "schedule-time")

        empty-trackers' (update-launch-trackers
                          empty-trackers empty-new-service-ids empty-removed-service-ids
                          empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                          empty-service-id->instance-counts empty-service-id->service-description
                          leader? waiter-timer)
        empty-trackers'' (update-launch-trackers
                           empty-trackers empty-new-service-ids #{"service-foo"}
                           empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                           empty-service-id->instance-counts empty-service-id->service-description
                           leader? waiter-timer)
        _ (testing "update-launch-trackers: empty -> empty"
            (is (= empty-trackers empty-trackers'))
            (is (= empty-trackers empty-trackers'')))

        service-id->instance-counts-1 {"service-1" req1 "service-2" req1}
        trackers-1 (update-launch-trackers
                     empty-trackers #{"service-1" "service-2"} empty-removed-service-ids
                     empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                     service-id->instance-counts-1 empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: empty -> non-empty"
            (check-trackers trackers-1 {"service-1" {:scheduling-instance-count 1}
                                        "service-2" {:scheduling-instance-count 1}}))

        trackers-2 (update-launch-trackers
                     trackers-1 empty-new-service-ids #{"service-1" "service-2"}
                     empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                     empty-service-id->instance-counts empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: trivial non-empty -> empty"
            (is (= empty-trackers trackers-2)))

        service-id->instance-counts-3 {"service-1" req1 "service-3" req1}
        trackers-3 (update-launch-trackers
                     trackers-1 #{"service-3"} #{"service-2"}
                     empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                     service-id->instance-counts-3 empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: simultaneously add and remove services"
            (check-trackers trackers-3 {"service-1" {:scheduling-instance-count 1}
                                        "service-3" {:scheduling-instance-count 1}}))

        trackers-4 (update-launch-trackers
                     trackers-3 empty-new-service-ids empty-removed-service-ids
                     empty-service-id->healthy-instances {"service-1" [(make-service-instance 1 1)]}
                     service-id->instance-counts-3 empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: scheduled a service instance"
            (check-trackers trackers-4 {"service-1" {:known-instance-ids #{"inst-1.1"}
                                                     :starting-instance-ids ["inst-1.1"]}
                                        "service-3" {:scheduling-instance-count 1}}))

        service-id->instance-counts-5 {"service-1" req1 "service-3" req1 "service-4" req1}
        trackers-5 (update-launch-trackers
                     trackers-4 #{"service-4"} empty-removed-service-ids
                     {"service-1" [(make-service-instance 1 1)]
                      "service-3" [(make-service-instance 3 1)]}
                     empty-service-id->unhealthy-instances service-id->instance-counts-5
                     empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: service instances started, and a new service appears"
            (check-trackers trackers-5 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-3" {:known-instance-ids #{"inst-3.1"}}
                                        "service-4" {:scheduling-instance-count 1}}))

        service-id->instance-counts-6 {"service-1" req1 "service-4" req1 "service-5" req1}
        trackers-6 (update-launch-trackers
                     trackers-5 #{"service-5"} #{"service-3"}
                     {"service-1" [(make-service-instance 1 1)]
                      "service-4" [(make-service-instance 4 1)]}
                     {"service-5" [(make-service-instance 5 1)]}
                     service-id->instance-counts-6 empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: simultaneously add and remove services,
                    and a healthy instance appears"
            (check-trackers trackers-6 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-4" {:known-instance-ids #{"inst-4.1"}}
                                        "service-5" {:known-instance-ids #{"inst-5.1"}
                                                     :starting-instance-ids ["inst-5.1"]}}))

        service-id->instance-counts-7 {"service-1" req1 "service-4" req1 "service-5" req3}
        service-id->healthy-instances-7 {"service-1" [(make-service-instance 1 1)]
                                         "service-4" [(make-service-instance 4 1)]}
        service-id->unhealthy-instances-7 {"service-5" [(make-service-instance 5 1)]}
        trackers-7 (update-launch-trackers
                     trackers-6 empty-new-service-ids empty-removed-service-ids
                     service-id->healthy-instances-7 service-id->unhealthy-instances-7
                     service-id->instance-counts-7 empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: service 5 scales to 3 instances"
            (check-trackers trackers-7 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-4" {:known-instance-ids #{"inst-4.1"}}
                                        "service-5" {:known-instance-ids #{"inst-5.1"}
                                                     :scheduling-instance-count 2
                                                     :starting-instance-ids ["inst-5.1"]}}))

        service-id->instance-counts-7a {"service-1" req1 "service-4" req1 "service-5" req1}
        trackers-7a (update-launch-trackers
                      (assoc-in trackers-7 ["service-5" :instance-scheduling-start-times]
                                [:timestamp-older :timestamp-newer])
                      empty-new-service-ids empty-removed-service-ids
                      service-id->healthy-instances-7 empty-service-id->unhealthy-instances
                      service-id->instance-counts-7a empty-service-id->service-description
                      leader? waiter-timer)
        _ (testing "update-launch-trackers: service 5 scales down to 1 instance, killing a scheduled instance"
            (check-trackers trackers-7a {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                         "service-4" {:known-instance-ids #{"inst-4.1"}}
                                         "service-5" {:scheduling-instance-count 1}}))
        _ (testing "update-launch-trackers: newer start-times are dropped first"
            (is (= (get-in trackers-7a ["service-5" :instance-scheduling-start-times])
                   [:timestamp-older])))

        service-id->instance-counts-7b {"service-1" req1 "service-4" req1 "service-5" req1}
        trackers-7b (update-launch-trackers
                      trackers-7 empty-new-service-ids empty-removed-service-ids
                      service-id->healthy-instances-7 service-id->unhealthy-instances-7
                      service-id->instance-counts-7b empty-service-id->service-description
                      leader? waiter-timer)
        _ (testing "update-launch-trackers: service 5 scales down to 1 instance, with a known instance"
            (check-trackers trackers-7b {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                         "service-4" {:known-instance-ids #{"inst-4.1"}}
                                         "service-5" {:known-instance-ids #{"inst-5.1"}
                                                      :starting-instance-ids ["inst-5.1"]}}))

        service-id->instance-counts-7c {"service-1" req1 "service-4" req1 "service-5" req2}
        trackers-7c (update-launch-trackers
                      trackers-7 empty-new-service-ids empty-removed-service-ids
                      service-id->healthy-instances-7 service-id->unhealthy-instances-7
                      service-id->instance-counts-7c empty-service-id->service-description
                      leader? waiter-timer)
        _ (testing "update-launch-trackers: service 5 scales down to 2 instances, with a known instance"
            (check-trackers trackers-7c {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                         "service-4" {:known-instance-ids #{"inst-4.1"}}
                                         "service-5" {:known-instance-ids #{"inst-5.1"}
                                                      :scheduling-instance-count 1
                                                      :starting-instance-ids ["inst-5.1"]}}))

        service-id->instance-counts-7d {"service-1" req1 "service-4" req1 "service-5" req3}
        trackers-7d (update-launch-trackers
                      trackers-7 empty-new-service-ids empty-removed-service-ids
                      service-id->healthy-instances-7 empty-service-id->unhealthy-instances
                      service-id->instance-counts-7d empty-service-id->service-description
                      leader? waiter-timer)
        _ (testing "update-launch-trackers: service 5 stays at 3 instances, but kills a scheduled instance"
            (check-trackers trackers-7d {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                         "service-4" {:known-instance-ids #{"inst-4.1"}}
                                         "service-5" {:scheduling-instance-count 3}}))

        service-id->instance-counts-7e {"service-1" req1 "service-4" req1 "service-5" req5}
        trackers-7e (update-launch-trackers
                      trackers-7 empty-new-service-ids empty-removed-service-ids
                      service-id->healthy-instances-7 empty-service-id->unhealthy-instances
                      service-id->instance-counts-7e empty-service-id->service-description
                      leader? waiter-timer)
        _ (testing "update-launch-trackers: service 5 scales up to 5 instances, killing a scheduled instance"
            (check-trackers trackers-7e {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                         "service-4" {:known-instance-ids #{"inst-4.1"}}
                                         "service-5" {:scheduling-instance-count 5}}))

        service-id->instance-counts-7f {"service-1" req1 "service-4" req1 "service-5" req2}
        trackers-7f (update-launch-trackers
                      trackers-7 empty-new-service-ids empty-removed-service-ids
                      service-id->healthy-instances-7
                      {"service-5" [(make-service-instance 5 2)
                                    (make-service-instance 5 3)]}
                      service-id->instance-counts-7f empty-service-id->service-description
                      leader? waiter-timer)
        _ (testing "update-launch-trackers: service 5 scales down to 2 instances,
                    removing the current known instance, but getting two scheduled instances"
            (check-trackers trackers-7f {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                         "service-4" {:known-instance-ids #{"inst-4.1"}}
                                         "service-5" {:known-instance-ids #{"inst-5.2" "inst-5.3"}
                                                      :starting-instance-ids ["inst-5.2" "inst-5.3"]}}))

        service-id->instance-counts-8 {"service-1" req1 "service-4" req1 "service-5" req3}
        trackers-8 (update-launch-trackers
                     trackers-7 empty-new-service-ids empty-removed-service-ids
                     empty-service-id->healthy-instances
                     {"service-1" [(make-service-instance 1 1)]
                      "service-4" [(make-service-instance 4 1)]
                      "service-5" [(make-service-instance 5 1)
                                   (make-service-instance 5 2)
                                   (make-service-instance 5 3)]}
                     service-id->instance-counts-8 empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: all requested instances transition to unhealthy"
            (check-trackers trackers-8 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-4" {:known-instance-ids #{"inst-4.1"}}
                                        "service-5" {:known-instance-ids #{"inst-5.1" "inst-5.2" "inst-5.3"}
                                                     :starting-instance-ids ["inst-5.1" "inst-5.2" "inst-5.3"]}}))

        trackers-9 (update-launch-trackers
                     trackers-8 empty-new-service-ids empty-removed-service-ids
                     {"service-1" [(make-service-instance 1 1)]
                      "service-4" [(make-service-instance 4 1)]
                      "service-5" [(make-service-instance 5 1)
                                   (make-service-instance 5 2)
                                   (make-service-instance 5 3)]}
                     empty-service-id->unhealthy-instances
                     service-id->instance-counts-8 empty-service-id->service-description
                     leader? waiter-timer)
        _ (testing "update-launch-trackers: all instances transition to healthy"
            (check-trackers trackers-9 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-4" {:known-instance-ids #{"inst-4.1"}}
                                        "service-5" {:known-instance-ids #{"inst-5.1" "inst-5.2" "inst-5.3"}}}))

        trackers-10 (update-launch-trackers
                      trackers-9 empty-new-service-ids #{"service-1" "service-4" "service-5"}
                      empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                      empty-service-id->instance-counts empty-service-id->service-description
                      leader? waiter-timer)]
    (testing "update-launch-trackers: transition back to empty"
      (is (= empty-trackers trackers-10)))))

(deftest test-start-launch-metrics-maintainer
  (testing "start-launch-metrics-maintainer"
    (let [make-metric-maintainer
          (fn make-metric-maintainer
            [state-0]
            (let [state-updates-chan (async/chan 1)
                  maintainer (start-launch-metrics-maintainer
                               state-updates-chan
                               (fn leader?-fn [] true)
                               (fn service-id->service-description-fn [_]
                                 {"metric-group" "default"}))]
              (async/>!! state-updates-chan state-0)
              (assoc maintainer :update-chan state-updates-chan)))
          update-metric-maintainer-state
          (fn update-metric-maintainer-state
            [maintainer state']
            (async/>!! (:update-chan maintainer) state'))
          query-metric-maintainer-state
          (fn query-metric-maintainer-state
            [maintainer]
            (let [response-chan (async/chan 1)]
              (async/>!! (:query-chan maintainer)
                         {:cid (ct/current-test-name) :response-chan response-chan})
              (->> (async/<!! response-chan)
                   ;; replace unpredictable state timestamps in with `:time`
                   (walk/postwalk
                     (fn [x] (if (instance? DateTime x) :time x))))))]
      (let [empty-router-state {:iteration 0
                                :service-id->healthy-instances {}
                                :service-id->instance-counts {}
                                :service-id->unhealthy-instances {}}
            maintainer (make-metric-maintainer empty-router-state)
            actual-state (query-metric-maintainer-state maintainer)
            expected-state {:known-service-ids #{}
                            :previous-iteration 0
                            :service-id->launch-tracker {}}]
        (testing "empty initial router state"
          (is (= expected-state actual-state))))
      (let [service-id "service1"
            instance-counts {:healthy 1 :requested 3 :scheduled 2}
            initial-router-state {:iteration 7
                                  :service-id->healthy-instances {service-id [{:id "inst1"}]}
                                  :service-id->instance-counts {service-id instance-counts}
                                  :service-id->unhealthy-instances {service-id [{:id "inst2"}]}}
            maintainer (make-metric-maintainer initial-router-state)
            actual-state-1 (query-metric-maintainer-state maintainer)
            actual-state-1' (do (update-metric-maintainer-state maintainer {:iteration 3})
                                (query-metric-maintainer-state maintainer))
            expected-state-1 {:known-service-ids #{service-id}
                              :previous-iteration 7
                              :service-id->launch-tracker
                              {service-id {:instance-counts instance-counts
                                           :instance-scheduling-start-times []
                                           :known-instance-ids #{"inst1" "inst2"}
                                           :metric-group "default"
                                           :starting-instance-id->start-timestamp {}}}}

            instance-counts' {:healthy 2 :requested 6 :scheduled 3}
            updated-router-state {:iteration 9
                                  :service-id->healthy-instances {service-id [{:id "inst1"} {:id "inst2"}]}
                                  :service-id->instance-counts {service-id instance-counts'}
                                  :service-id->unhealthy-instances {service-id [{:id "inst3"}]}}
            actual-state-2 (do (update-metric-maintainer-state maintainer updated-router-state)
                               (query-metric-maintainer-state maintainer))
            expected-state-2 {:known-service-ids #{service-id}
                              :previous-iteration 9
                              :service-id->launch-tracker
                              {service-id {:instance-counts instance-counts'
                                           :instance-scheduling-start-times [:time :time]
                                           :known-instance-ids #{"inst1" "inst2" "inst3"}
                                           :metric-group "default"
                                           :starting-instance-id->start-timestamp {"inst3" :time}}}}]
        (testing "non-empty initial router state"
          (is (= expected-state-1 actual-state-1)))
        (testing "ignored router state update"
          (is (= expected-state-1 actual-state-1')))
        (testing "applied router state update"
          (is (= expected-state-2 actual-state-2)))))))

(deftest test-track-kill-candidate!
  (let [instance-id-1 (str "id-1-" (System/nanoTime))
        instance-id-2 (str "id-2-" (System/nanoTime))
        instance-id-3 (str "id-3-" (System/nanoTime))]
    (track-kill-candidate! instance-id-1 :prepare-to-kill 200)
    (track-kill-candidate! instance-id-2 :prepare-to-kill 200)
    (track-kill-candidate! instance-id-3 :prepare-to-kill 200)
    (track-kill-candidate! instance-id-1 :killed 1000)
    (track-kill-candidate! instance-id-2 :killed 1000)
    (Thread/sleep 100)
    (is (is-kill-candidate? instance-id-1))
    (is (is-kill-candidate? instance-id-2))
    (is (is-kill-candidate? instance-id-3))
    (track-kill-candidate! instance-id-2 :prepare-to-kill 200)
    (Thread/sleep 150)
    (is (not (is-kill-candidate? instance-id-1)))
    (is (is-kill-candidate? instance-id-2))
    (is (not (is-kill-candidate? instance-id-3)))
    (Thread/sleep 100)
    (is (not (is-kill-candidate? instance-id-1)))
    (is (not (is-kill-candidate? instance-id-2)))
    (is (not (is-kill-candidate? instance-id-3)))))

(deftest test-add-to-store-and-track-failed-instance!
  (let [max-instances-to-keep 2
        transient-store (atom {})
        service-id "s1"
        instance-1 {:id "i1" :service-id service-id :started-at "202204190010"}
        instance-2 {:id "i2" :service-id service-id :started-at "202204190020"}
        instance-3 {:id "i3" :service-id service-id :started-at "202204190030"}
        instance-4 {:id "i4" :service-id service-id :started-at "202204190040"}]
    (add-to-store-and-track-failed-instance! transient-store max-instances-to-keep service-id instance-1)
    (is (= #{instance-1} (set (get @transient-store service-id))))
    (add-to-store-and-track-failed-instance! transient-store max-instances-to-keep service-id instance-2)
    (is (= #{instance-1 instance-2} (set (get @transient-store service-id))))
    (add-to-store-and-track-failed-instance! transient-store max-instances-to-keep service-id instance-3)
    (is (= #{instance-2 instance-3} (set (get @transient-store service-id))))
    (add-to-store-and-track-failed-instance! transient-store max-instances-to-keep service-id instance-4)
    (is (= #{instance-3 instance-4} (set (get @transient-store service-id))))))

(defmacro assert-service->last-request-time-from-metrics
  [service-id->metrics expected-service-id->last-request-time]
  `(let [service-id->metrics# ~service-id->metrics
         expected-service-id->last-request-time# ~expected-service-id->last-request-time]
     (is (= expected-service-id->last-request-time#
            (create-service-id->last-request-time-from-metrics (constantly service-id->metrics#))))))

(deftest test-create-service-id->last-request-time-from-metrics
  (testing "creates map of service-id->last-request-time"
    (assert-service->last-request-time-from-metrics
      {"s1" {"boolean-too" true
             "foo" "bar"
             "last-request-time" "1"
             "temp" "temp"
             "this-is-a-number" 5}}
      {"s1" "1"}))
  (testing "filters out missing last-request-time"
    (assert-service->last-request-time-from-metrics
      {"s1" {"boolean-too" true
             "foo" "bar"
             "temp" "temp"
             "this-is-a-number" 5
             "nested" {"foo" "bar"}}
       "s2" {"boolean-too" true
             "foo" "bar"
             "last-request-time" 5
             "temp" "temp"
             "this-is-a-number" 5
             "nested" {"foo" "bar"}}}
      {"s2" 5})))

(deftest test-make-service-id-new-last-request-time?-fn
  (let [now (t/now)
        older-than-now (t/minus now (t/millis 1))
        newer-than-now (t/plus now (t/millis 1))
        service-id->last-request-time {"s1" now
                                       "s2" older-than-now}
        service-id-new-last-request-time?-fn (make-service-id-new-last-request-time?-fn service-id->last-request-time)]
    (testing "created function returns true if last-request-time is after previous last-request-time"
      (is (service-id-new-last-request-time?-fn ["s1" newer-than-now])))
    (testing "created function returns true if previous last-request-time is nil"
      (is (service-id-new-last-request-time?-fn ["s3" now])))
    (testing "created function returns false if last-request-time is before as the previous last-request-time"
      (is (not (service-id-new-last-request-time?-fn ["s1" older-than-now]))))
    (testing "created function returns false if last-request-time is the same as the previous last-request-time"
      (is (not (service-id-new-last-request-time?-fn ["s2" older-than-now]))))))

(defmacro assert-filter-factory-fn
  "Given a factory-fn and factor-args, create the filter function. Then test the filter function with input entry against
  expected result."
  [factory-fn factory-args entry expected]
  `(let [factory-fn# ~factory-fn
         factory-args# ~factory-args
         entry# ~entry
         expected# ~expected
         fn# (apply factory-fn# factory-args#)
         actual# (fn# entry#)]
     (is (= expected# actual#) (str "expected: " expected# " actual: " actual#))))

(deftest test-make-service-id-received-request-after-becoming-stale?-fn
  (let [now (t/now)
        older-than-now (t/minus now (t/millis 1))
        newer-than-now (t/plus now (t/millis 1))]
    (testing "created function that returns true 'update-time' is nil for service's stale info"
      (assert-filter-factory-fn make-service-id-received-request-after-becoming-stale?-fn [(constantly {:stale? true})]
                                ["s1" older-than-now] true))
    (testing "created function that returns true if 'last-request-time' is after the 'update-time' for service's stale info"
      (assert-filter-factory-fn make-service-id-received-request-after-becoming-stale?-fn [(constantly {:stale? true :update-time now})]
                                ["s1" newer-than-now] true))
    (testing "created function that returns true if 'last-request-time' is equal to the 'update-time' for service's stale info"
      (assert-filter-factory-fn make-service-id-received-request-after-becoming-stale?-fn [(constantly {:stale? true :update-time now})]
                                ["s1" now] true))
    (testing "created function that returns false when service is not stale"
      (assert-filter-factory-fn make-service-id-received-request-after-becoming-stale?-fn [(constantly {:stale? false :update-time now})]
                                ["s1" newer-than-now] false))
    (testing "created function that returns false when service 'update-time' is after the 'last-request-time'"
      (assert-filter-factory-fn make-service-id-received-request-after-becoming-stale?-fn [(constantly {:stale? false :update-time now})]
                                ["s1" older-than-now] false))))

(deftest test-make-one-source-token?-fn
  (let [correlation-id (cid/get-correlation-id)]
    (testing "created function that returns true if exactly 1 source token"
      (assert-filter-factory-fn make-one-source-token?-fn [correlation-id] {:service-id "s1" :source-tokens ["t1"]} true))
    (testing "created function that returns false more than 1 source token"
      (assert-filter-factory-fn make-one-source-token?-fn [correlation-id] {:service-id "s1" :source-tokens ["t1" "t2" "t3"]} false))
    (testing "created function that returns false with 0 source tokens"
      (assert-filter-factory-fn make-one-source-token?-fn [correlation-id] {:service-id "s1" :source-tokens []} false))))

(defn- store-service-desc-for-token-fn
  "Helper function for tests to store a service-description for token in provided 'kv-store'"
  [kv-store token service-desc token-metadata]
  (tk/store-service-description-for-token
    synchronize-fn kv-store history-length limit-per-owner token service-desc token-metadata))

(deftest test-make-token-is-not-run-as-requester-or-parameterized?-fn
  (let [correlation-id (cid/get-correlation-id)
        kv-store (kv/->LocalKeyValueStore (atom {}))]
    (store-service-desc-for-token-fn kv-store "t1" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
    (store-service-desc-for-token-fn kv-store "t2" {"cpus" 1 "run-as-user" "*"} {"owner" "u1"})
    (store-service-desc-for-token-fn kv-store "t3" {"cpus" 1} {"owner" "u1"})
    (store-service-desc-for-token-fn
      kv-store
      "t4"
      {"allowed-params" #{"PARAM_FOO"}
       "cpus" 1
       "run-as-user" "u1"}
      {"owner" "u1"})
    (testing "created a function that returns true if token is not run-as-requester or parameterized"
      (assert-filter-factory-fn make-token-is-not-run-as-requester-or-parameterized?-fn [correlation-id kv-store] "t1" true))
    (testing "created a function that returns false if token is run-as-requester"
      (assert-filter-factory-fn make-token-is-not-run-as-requester-or-parameterized?-fn [correlation-id kv-store] "t2" false))
    (testing "created a function that returns false if token is run-as-requester by default"
      (assert-filter-factory-fn make-token-is-not-run-as-requester-or-parameterized?-fn [correlation-id kv-store] "t3" false))
    (testing "created a function that returns false if token is parameterized"
      (assert-filter-factory-fn make-token-is-not-run-as-requester-or-parameterized?-fn [correlation-id kv-store] "t4" false))
    (testing "created a function that returns false if token does not have a service-description-template"
      (assert-filter-factory-fn make-token-is-not-run-as-requester-or-parameterized?-fn [correlation-id kv-store] "t5" false))))

(defmacro assert-start-latest-services-for-tokens
  [correlation-id kv-store fallback-state tokens expected-errors expected-descriptors-started]
  `(let [tokens# ~tokens
         expected-errors# ~expected-errors
         retrieve-descriptor-fn# #(let [run-as-user# %1 token# %2]
                                    (when (-> expected-errors# set (contains? token#))
                                      (throw (ex-info "Force error for token" {:token token#})))
                                    {:latest-descriptor {:run-as-user run-as-user#
                                                         :service-id (str "s." token#)
                                                         :token token#}})
         actual-descriptors-started-atom# (atom [])
         start-new-service-fn# #(swap! actual-descriptors-started-atom# conj (get % :token))
         actual-errors# (start-latest-services-for-tokens
                          ~correlation-id ~kv-store ~fallback-state start-new-service-fn# retrieve-descriptor-fn# tokens#)]
     (is (= expected-errors# (map :token actual-errors#)))
     (is (= ~expected-descriptors-started @actual-descriptors-started-atom#))))

(deftest test-start-latest-services-for-tokens
  (let [correlation-id (cid/get-correlation-id)
        kv-store (kv/->LocalKeyValueStore (atom {}))]
    (store-service-desc-for-token-fn kv-store "t1" {"run-as-user" "u1"} {"owner" "u1"})
    (store-service-desc-for-token-fn kv-store "t2" {"run-as-user" "u2"} {"owner" "u1"})
    (store-service-desc-for-token-fn kv-store "t3" {"run-as-user" "u3"} {"owner" "u1"})
    (testing "attempts to start services for tokens that do not already resolve to a service-id in the fallback-state"
      (let [fallback-state {:available-service-ids #{"s.t3"}}
            tokens ["t1" "t2" "t3"]
            expected-errors []
            expected-descriptors-started ["t1" "t2"]]
        (assert-start-latest-services-for-tokens
          correlation-id kv-store fallback-state tokens expected-errors expected-descriptors-started)))

    (testing "attempts to start services for tokens that do not already resolve to a service-id in the fallback-state"
      (let [fallback-state {:available-service-ids #{"s.t3"}}
            tokens ["t1" "t2" "t3"]
            expected-errors ["t2" "t3"]
            expected-descriptors-started ["t1"]]
        (assert-start-latest-services-for-tokens
          correlation-id kv-store fallback-state tokens expected-errors expected-descriptors-started)))))

(defn- start-new-services-maintainer-with-rounds
  "Starts a 'start-new-services-maintainer' using stubs provided at each round in the list 'rounds'. Calls to dependent
  function are stubbed to return what is provided in the 'rounds' fixture."
  [rounds]
  (let [round-count-atom (atom -1)
        get-round (fn get-round-fn []
                    (get rounds @round-count-atom))
        clock (fn clock-fn []
                (-> (get-round) :clock))
        trigger-ch (async/chan)
        service-id->source-tokens-fn
        (fn service-id->source-tokens-fn [service-id]
          (-> (get-round) (get-in [:service-id->source-tokens service-id])))
        service-id->metrics-fn (fn service-id->metrics-fn []
                                 (-> (get-round) :service-id->metrics))
        service-id->stale-info-fn (fn service-id->stale-info-fn [service-id]
                                    (-> (get-round)
                                      (get-in [:service-id->stale-info service-id])))
        retrieve-descriptor-fn (fn retrieve-descriptor-fn [run-as-user token]
                                 (-> (get-round)
                                   (get-in [:token-run-as-user->descriptor {:run-as-user run-as-user :token token}])))
        start-new-service-calls-atom (atom {})
        start-new-service-fn (fn start-new-service-fn [descriptor]
                               (swap! start-new-service-calls-atom update @round-count-atom conj descriptor))
        fallback-state-atom (atom {})
        kv-store (kv/->LocalKeyValueStore (atom {}))]
    (assoc
      (start-new-services-maintainer
        clock trigger-ch service-id->source-tokens-fn service-id->metrics-fn service-id->stale-info-fn start-new-service-fn
        retrieve-descriptor-fn fallback-state-atom kv-store)
      :fallback-state-atom fallback-state-atom
      :kv-store kv-store
      :round-count-atom round-count-atom
      :start-new-service-calls-atom start-new-service-calls-atom
      :trigger-maintainer-refresh!! (fn trigger-maintainer-refresh!! []
                                      (swap! round-count-atom inc)
                                      (async/>!! trigger-ch {})))))

(deftest test-start-new-services-maintainer
  (let [close-maintainer!!
        (fn close-maintainer!!
          [exit-chan go-chan]
          (async/>!! exit-chan :exit)
          (async/<!! go-chan))]
    (testing "when initialized, the maintainer queries for initial list of service metrics and tries to start services for
     their source token only if last-request-time is later than or equal to the update-time"
      (let [now (t/now)
            old-time (t/minus now (t/millis 2))
            prev-time (t/minus now (t/millis 1))
            rounds [{:clock now
                     :service-id->metrics {"s1" {"last-request-time" now}
                                           "s2" {"last-request-time" old-time}
                                           "s3" {"last-request-time" now}}
                     :service-id->source-tokens {"s1" [{"token" "t1"}]
                                                 "s2" [{"token" "t2"}]
                                                 "s3" [{"token" "t3"}]}
                     :service-id->stale-info {"s1" {:stale? true
                                                    :update-time prev-time}
                                              "s2" {:stale? true
                                                    :update-time prev-time}
                                              "s3" {:stale? true
                                                    :update-time now}}
                     :token-run-as-user->descriptor
                     {{:token "t1" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-1"}}
                      {:token "t2" :run-as-user "u2"} {:latest-descriptor {:service-id "s2-1"}}
                      {:token "t3" :run-as-user "u3"} {:latest-descriptor {:service-id "s3-1"}}}}]
            {:keys [exit-chan fallback-state-atom go-chan kv-store start-new-service-calls-atom trigger-maintainer-refresh!! query-chan]}
            (start-new-services-maintainer-with-rounds rounds)]
        ; store t1 and t2 in kv-store
        (store-service-desc-for-token-fn kv-store "t1" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t2" {"cpus" 1 "run-as-user" "u2"} {"owner" "u2"})
        (store-service-desc-for-token-fn kv-store "t3" {"cpus" 1 "run-as-user" "u3"} {"owner" "u3"})
        ; fallback-state-atom provides current available-service-ids (does not contain s1-1, s2-1, s3-1)
        (reset! fallback-state-atom {:available-service-ids #{"s1" "s2" "s3"}})
        (trigger-maintainer-refresh!!)

        ; assert that maintainer state reflects the new last-request-times
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= now last-update-time))
            (is (= {"s1" now
                    "s2" old-time
                    "s3" now}
                  service-id->last-request-time))))
        ; This confirms that services were attempted to start for both "s1-1" and "s2-1" after the first round.
        ; A service was started for "s2-1" even though the last-request-time was old for "s2". We prefer to eagerly start
        ; services for tokens with new last-request-times over potentially missing a new service-id update during router
        ; startup.
        (is (= {0 [{:service-id "s3-1"} {:service-id "s1-1"}]}
               @start-new-service-calls-atom))

        (close-maintainer!! exit-chan go-chan)))

    (testing "services with the same source token, will only result in one call for service to start"
      (let [now (t/now)
            rounds [{:clock now
                     :service-id->metrics {"s1" {"last-request-time" now}
                                           "s2" {"last-request-time" now}}
                     :service-id->source-tokens {"s1" [{"token" "t1"}]
                                                 "s2" [{"token" "t1"}]}
                     :service-id->stale-info {"s1" {:stale? true
                                                    :update-time now}
                                              "s2" {:stale? true
                                                    :update-time now}}
                     :token-run-as-user->descriptor
                     {{:token "t1" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-1"}}}}]
            {:keys [exit-chan fallback-state-atom go-chan kv-store start-new-service-calls-atom trigger-maintainer-refresh!! query-chan]}
            (start-new-services-maintainer-with-rounds rounds)]
        (store-service-desc-for-token-fn kv-store "t1" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (reset! fallback-state-atom {:available-service-ids #{"s1" "s2"}})
        (trigger-maintainer-refresh!!)
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= now last-update-time))
            (is (= {"s1" now
                    "s2" now}
                   service-id->last-request-time))))
        ; only 's1-1' is attempted to be started because both 's1' and 's2' resolve to token 't1'
        (is (= {0 [{:service-id "s1-1"}]}
               @start-new-service-calls-atom))

        (close-maintainer!! exit-chan go-chan)))

    (testing "run-as-requester tokens are ignored"
      (let [now (t/now)
            rounds [{:clock now
                     :service-id->metrics {"s1" {"last-request-time" now}}
                     :service-id->source-tokens {"s1" [{"token" "t1"}]}
                     :service-id->stale-info {"s1" {:stale? true :update-time now}}
                     :token-run-as-user->descriptor
                     {{:token "t1" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-1"}}}}
                    {:clock now
                     :service-id->metrics {"s1" {"last-request-time" (t/plus now (t/minutes 10))}}
                     :service-id->source-tokens {"s1" [{"token" "t1"}]}
                     :service-id->stale-info {"s1" {:stale? true :update-time now}}
                     :token-run-as-user->descriptor
                     {{:token "t1" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-1"}}}}]
            {:keys [exit-chan fallback-state-atom go-chan kv-store start-new-service-calls-atom trigger-maintainer-refresh!! query-chan]}
            (start-new-services-maintainer-with-rounds rounds)]
        ; not specifying the 'run-as-user' field defaults to run-as-requester with value '*'
        (store-service-desc-for-token-fn kv-store "t1" {"cpus" 1} {"owner" "u1"})
        (reset! fallback-state-atom {:available-service-ids #{"s1"}})
        (trigger-maintainer-refresh!!)

        ; round 0: no new service calls are made as it is run-as-requester
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= now last-update-time))
            (is (= {"s1" now}
                   service-id->last-request-time))))
        (is (= {} @start-new-service-calls-atom))

        ; explicitly specify 'run-as-user: *', token is still considered run-as-requester
        (store-service-desc-for-token-fn kv-store "t1" {"cpus" 1 "run-as-user" "*"} {"owner" "u1"})
        (trigger-maintainer-refresh!!)

        ; round 1: no new service calls are made as it is run-as-requester still
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= now last-update-time))
            (is (= {"s1" (t/plus now (t/minutes 10))}
                   service-id->last-request-time))))
        (is (= {} @start-new-service-calls-atom))

        (close-maintainer!! exit-chan go-chan)))

    (testing "skip starting services for service-ids that are already active"
      (let [now (t/now)
            rounds [{:clock now
                     :service-id->metrics {"s1" {"last-request-time" now}
                                           "s1-1" {"last-request-time" now}}
                     :service-id->source-tokens {"s1" [{"token" "t1"}]
                                                 "s1-1" [{"token" "t2"}]}
                     :service-id->stale-info {"s1" {:stale? true :update-time now}
                                              "s1-1" {:stale? false}}
                     :token-run-as-user->descriptor
                     {{:token "t1" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-1"}}
                      {:token "t2" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-1"}}}}]
            {:keys [exit-chan fallback-state-atom go-chan kv-store start-new-service-calls-atom trigger-maintainer-refresh!! query-chan]}
            (start-new-services-maintainer-with-rounds rounds)]
        (store-service-desc-for-token-fn kv-store "t1" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (reset! fallback-state-atom {:available-service-ids #{"s1" "s1-1"}})
        (trigger-maintainer-refresh!!)

        ; no new service calls are made as s1-1 already exists
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= now last-update-time))
            (is (= {"s1" now
                    "s1-1" now}
                   service-id->last-request-time))))
        (is (= {} @start-new-service-calls-atom))

        (close-maintainer!! exit-chan go-chan)))

    (testing "combinations of several new service last-request-times after 1 round"
      (let [now (t/now)
            later-time (t/plus now (t/seconds 1))
            service-id->source-tokens {"s1-new-lrt" [{"token" "t1-new-lrt" "version" "1"}
                                                     {"token" "t1-new-lrt" "version" "2"}
                                                     {"token" "t1-new-lrt" "version" "3"}]
                                       "s2-no-change" [{"token" "t2-no-change"}]
                                       "s3-shared-token" [{"token" "t6-1"}]
                                       "s4-run-as-req" [{"token" "t4-run-as-req"}]
                                       "s5-token-update" [{"token" "t5-token-update"}]
                                       "s6-many-source-tokens" [{"token" "t6-1"} {"token" "t6-2"} {"token" "t6-3" "version" "1"}
                                                                {"token" "t6-3" "version" "2"}]
                                       "s7-killed" [{"token" "t7-killed"}]
                                       "s8-no-tokens" []}
            rounds [{:clock now
                     :service-id->metrics {"s1-new-lrt" {"last-request-time" now}
                                           "s2-no-change" {"last-request-time" (t/minus now (t/days 100))}
                                           "s3-shared-token" {"last-request-time" now}
                                           "s4-run-as-req" {"last-request-time" now}
                                           "s5-token-update" {"last-request-time" now}
                                           "s6-many-source-tokens" {"last-request-time" now}
                                           "s7-killed" {"last-request-time" now}
                                           "s8-no-tokens" {"last-request-time" now}}
                     :service-id->source-tokens service-id->source-tokens
                     :service-id->stale-info {"s1-new-lrt" {:stale? true :update-time now}
                                              "s2-no-change" {:stale? true :update-time now}
                                              "s3-shared-token" {:stale? true :update-time now}
                                              "s4-run-as-req" {:stale? true :update-time now}
                                              "s5-token-update" {:stale? true :update-time now}
                                              "s6-many-source-tokens" {:stale? true :update-time now}
                                              "s7-killed" {:stale? true :update-time now}
                                              "s8-no-tokens" {:stale? true :update-time now}}
                     :token-run-as-user->descriptor
                     {{:token "t1-new-lrt" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-new-lrt"}}
                      {:token "t2-no-change" :run-as-user "u1"} {:latest-descriptor {:service-id "s2-no-change"}}
                      {:token "t4-run-as-req" :run-as-user "u1"} {:latest-descriptor {:service-id "s4-run-as-req"}}
                      {:token "t5-token-update" :run-as-user "u1"} {:latest-descriptor {:service-id "s5-token-update"}}
                      {:token "t6-1" :run-as-user "u1"} {:latest-descriptor {:service-id "s6-many-source-tokens"}}
                      {:token "t6-2" :run-as-user "u1"} {:latest-descriptor {:service-id "s6-many-source-tokens"}}
                      {:token "t6-3" :run-as-user "u1"} {:latest-descriptor {:service-id "s6-many-source-tokens"}}
                      {:token "t7-killed" :run-as-user "u1"} {:latest-descriptor {:service-id "s7-killed"}}}}
                    {:clock later-time
                     :service-id->metrics {"s1-new-lrt" {"last-request-time" later-time}
                                           "s2-no-change" {"last-request-time" (t/minus now (t/days 100))}
                                           "s3-shared-token" {"last-request-time" later-time}
                                           "s4-run-as-req" {"last-request-time" later-time}
                                           "s5-token-update" {"last-request-time" later-time}
                                           "s6-many-source-tokens" {"last-request-time" later-time}
                                           "s8-no-tokens" {"last-request-time" later-time}}
                     :service-id->source-tokens service-id->source-tokens
                     :service-id->stale-info {"s1-new-lrt" {:stale? true :update-time now}
                                              "s2-no-change" {:stale? true :update-time now}
                                              "s3-shared-token" {:stale? true :update-time now}
                                              "s4-run-as-req" {:stale? true :update-time now}
                                              "s5-token-update" {:stale? true :update-time now}
                                              "s6-many-source-tokens" {:stale? true :update-time now}
                                              "s7-killed" {:stale? true :update-time now}
                                              "s8-no-tokens" {:stale? true :update-time now}}
                     :token-run-as-user->descriptor
                     {{:token "t1-new-lrt" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-new"}}
                      {:token "t2-no-change" :run-as-user "u1"} {:latest-descriptor {:service-id "s2-new"}}
                      {:token "t4-run-as-req" :run-as-user "u1"} {:latest-descriptor {:service-id "s4-new"}}
                      {:token "t5-token-update" :run-as-user "u1"} {:latest-descriptor {:service-id "s5-new"}}
                      {:token "t6-1" :run-as-user "u1"} {:latest-descriptor {:service-id "s6-1-new"}}
                      {:token "t6-2" :run-as-user "u1"} {:latest-descriptor {:service-id "s6-2-new"}}
                      {:token "t6-3" :run-as-user "u1"} {:latest-descriptor {:service-id "s6-3-new"}}
                      {:token "t7-killed" :run-as-user "u1"} {:latest-descriptor {:service-id "s7-killed"}}}}]
            {:keys [exit-chan fallback-state-atom go-chan kv-store start-new-service-calls-atom trigger-maintainer-refresh!! query-chan]}
            (start-new-services-maintainer-with-rounds rounds)]
        (store-service-desc-for-token-fn kv-store "t1-new-lrt" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t2-no-change" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t4-run-as-req" {"cpus" 1 "run-as-user" "*"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t5-token-update" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t6-1" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t6-2" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t6-3" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (store-service-desc-for-token-fn kv-store "t7-killed" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (reset! fallback-state-atom {:available-service-ids #{"s1-new-lrt" "s2-no-change" "s4-run-as-req" "s5-token-update"
                                                              "s6-many-source-tokens" "s7-killed" "s8-no-tokens"}})

        ; expect that no new services to be started as they are all in fallback-state-atom
        (trigger-maintainer-refresh!!)
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= now last-update-time))
            (is (= {"s1-new-lrt" now
                    "s2-no-change" (t/minus now (t/days 100))
                    "s3-shared-token" now
                    "s4-run-as-req" now
                    "s5-token-update" now
                    "s6-many-source-tokens" now
                    "s7-killed" now
                    "s8-no-tokens" now}
                   service-id->last-request-time))))
        (is (= {} @start-new-service-calls-atom))

        ; remove s7-killed service
        (swap! fallback-state-atom update :available-service-ids disj "s7-killed")
        ; update t5 to be run-as-requester
        (store-service-desc-for-token-fn kv-store "t5-token-update" {"cpus" 1 "run-as-user" "*"} {"owner" "u1"})

        ; expect only certain services to be triggered to start: s1-new, s6-3-new
        ; s6-2-new and s6-3-new are not started because s6 has many source tokens. s6-1-new is started because s3 mapped
        ; to the single t3 source token.
        (trigger-maintainer-refresh!!)
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= later-time last-update-time))
            (is (= {"s1-new-lrt" later-time
                    "s2-no-change" (t/minus now (t/days 100))
                    "s3-shared-token" later-time
                    "s4-run-as-req" later-time
                    "s5-token-update" later-time
                    "s6-many-source-tokens" later-time
                    "s8-no-tokens" later-time}
                   service-id->last-request-time))))
        (is (= #{{:service-id "s1-new"} {:service-id "s6-1-new"}}
               (-> @start-new-service-calls-atom (get 1) set))
            (str "services created does not match expected: " @start-new-service-calls-atom))

        (close-maintainer!! exit-chan go-chan)))

    (testing "many rounds with no last-request-time changes, the maintainer will not attempt to start new service for tokens"
      (let [now (t/now)
            rounds (-> (repeat 10 {:clock now
                                   :service-id->metrics {"s1" {"last-request-time" now}}
                                   :service-id->source-tokens {"s1" [{"token" "t1"}]}
                                   :service-id->stale-info {"s1" {:stale? true :update-time now}}
                                   :token-run-as-user->descriptor
                                   {{:token "t1" :run-as-user "u1"} {:latest-descriptor {:service-id "s1-1"}}}})
                     vec)
            {:keys [exit-chan fallback-state-atom go-chan kv-store start-new-service-calls-atom trigger-maintainer-refresh!! query-chan]}
            (start-new-services-maintainer-with-rounds rounds)]
        (store-service-desc-for-token-fn kv-store "t1" {"cpus" 1 "run-as-user" "u1"} {"owner" "u1"})
        (reset! fallback-state-atom {:available-service-ids #{"s1"}})
        (trigger-maintainer-refresh!!)

        ; assert that the maintainer attempts to start the service on the initial round
        (let [res-ch (async/promise-chan)]
          (async/>!! query-chan {:response-chan res-ch :include-flags []})
          (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
            (is (= now last-update-time))
            (is (= {"s1" now}
                   service-id->last-request-time))))
        (is (= {0 [{:service-id "s1-1"}]}
               @start-new-service-calls-atom))

        ; there should be no new attempts to start a new service as there are no new last-request-times
        (dotimes [count 9]
          (let [round (inc count)]
            (trigger-maintainer-refresh!!)
            (let [res-ch (async/promise-chan)]
              (async/>!! query-chan {:response-chan res-ch :include-flags []})
              (let [{:keys [last-update-time service-id->last-request-time]} (async/<!! res-ch)]
                (is (= now last-update-time))
                (is (= {"s1" now}
                       service-id->last-request-time))))
            (is (nil? (get @start-new-service-calls-atom round))
                (str "service calls expected to be empty: " @start-new-service-calls-atom))))

        (close-maintainer!! exit-chan go-chan)))))
