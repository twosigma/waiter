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
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [slingshot.slingshot :as ss]
            [waiter.core :as core]
            [waiter.curator :as curator]
            [waiter.metrics :as metrics]
            [waiter.scheduler :refer :all]
            [waiter.util.client-tools :as ct]
            [waiter.util.date-utils :as du])
  (:import (java.net ConnectException SocketTimeoutException)
           (java.util.concurrent TimeoutException)
           (org.joda.time DateTime)))

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
        test-instance (->ServiceInstance
                        "instance-id"
                        "service-id"
                        start-time
                        true
                        200
                        #{}
                        nil
                        "www.scheduler-test.example.com"
                        1234
                        []
                        "proto"
                        "log-dir"
                        "instance-message")]
    (testing (str "Test record ServiceInstance")
      (is (= "instance-id" (:id test-instance)))
      (is (= "service-id" (:service-id test-instance)))
      (is (= start-time (:started-at test-instance)))
      (is (= true (:healthy? test-instance)))
      (is (= 200 (:health-check-status test-instance)))
      (is (= "www.scheduler-test.example.com" (:host test-instance)))
      (is (= 1234 (:port test-instance)))
      (is (= "proto" (:protocol test-instance)))
      (is (= "log-dir" (:log-directory test-instance)))
      (is (= "instance-message" (:message test-instance)))
      (is (= "proto://www.scheduler-test.example.com:1234" (base-url test-instance)))
      (is (= "proto://www.scheduler-test.example.com:1234/test-end-point" (end-point-url test-instance "test-end-point"))))))

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
        write-state-fn (fn [name state] (curator/write-path curator (str gc-base-path "/" name) state :serializer :nippy :create-parent-zknodes? true))]
    (with-redefs [curator/read-path (fn [_ path & _] {:data (get @state-store path)})
                  curator/write-path (fn [_ path data & _] (swap! state-store (fn [v] (assoc v path data))))]
      (let [available-services-atom (atom #{"service1" "service2" "service3" "service4stayalive" "service5"
                                            "service6faulty" "service7" "service8stayalive" "service9stayalive" "service10broken"
                                            "service11broken"})
            initial-global-state {"service1" {"outstanding" 0, "total" 10}
                                  "service2" {"outstanding" 5, "total" 20}
                                  "service3" {"outstanding" 0, "total" 30}
                                  "service4stayalive" {"outstanding" 1000, "total" 40}
                                  "service5" {"outstanding" 10, "total" 50}
                                  "service6faulty" {"outstanding" 2000, "total" 60}
                                  "service7" {"outstanding" 15, "total" 70}
                                  "service8stayalive" {"outstanding" 3000, "total" 80}
                                  "service9stayalive" {"outstanding" 70, "total" 80}
                                  "service10broken" {"outstanding" 70, "total" 80}
                                  "service11broken" {"outstanding" 95, "total" 80}}
            deleted-services-atom (atom #{})
            scheduler (reify ServiceScheduler
                        (delete-app [_ service-id]
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
                service-id->idle-timeout (constantly 50)
                channel-map (scheduler-services-gc
                              scheduler scheduler-state-chan service-id->metrics-fn
                              {:broken-service-min-hosts broken-service-min-hosts
                               :broken-service-timeout-mins broken-service-timeout-mins
                               :scheduler-gc-interval-ms timeout-interval-ms}
                              service-gc-go-routine service-id->idle-timeout)
                service-gc-exit-chan (:exit channel-map)]
            (dotimes [n 100]
              (let [global-state (pc/map-vals #(update-in % ["outstanding"] (fn [v] (max 0 (- v n))))
                                              initial-global-state)]
                (async/>!! scheduler-state-chan (concat
                                                  [[:update-available-services {:available-service-ids (set @available-services-atom)}]]
                                                  (vec
                                                    (map (fn [service-id]
                                                           [:update-service-instances
                                                            {:service-id service-id
                                                             :failed-instances (cond
                                                                                 (str/includes? service-id "broken") [{:id (str service-id ".failed1"), :host "failed1.example.com"},
                                                                                                                      {:id (str service-id ".failed2"), :host "failed2.example.com"}]
                                                                                 (str/includes? service-id "faulty") [{:id (str service-id ".failed4a"), :host "failed4.example.com"},
                                                                                                                      {:id (str service-id ".failed4b"), :host "failed4.example.com"},
                                                                                                                      {:id (str service-id ".failed4c"), :host "failed4.example.com"},
                                                                                                                      {:id (str service-id ".failed4d"), :host "failed4.example.com"}]
                                                                                 :else [])
                                                             :healthy-instances (if (str/includes? service-id "broken") [] [{:id (str service-id ".unhealthy")}])}])
                                                         @available-services-atom))))
                (async/>!! metrics-chan global-state)
                (Thread/sleep 2)
                (swap! iteration-counter inc)))
            (async/>!! service-gc-exit-chan :exit)
            (is (= #{"service3" "service2" "service1" "service5" "service7"} @deleted-services-atom))
            (is (= #{"service4stayalive" "service6faulty" "service8stayalive", "service9stayalive", "service10broken", "service11broken"} @available-services-atom))))))))

(deftest test-scheduler-broken-services-gc
  (let [leader? (constantly true)
        state-store (atom {})
        read-state-fn (fn [name] (get @state-store name))
        write-iteration-counter (atom 0)
        write-state-fn (fn [name state] (swap! state-store assoc name state) (swap! write-iteration-counter inc))]
    (let [available-services-atom (atom #{"service6faulty" "service7" "service8stayalive" "service9stayalive" "service10broken" "service11broken"})
          deleted-services-atom (atom #{})
          scheduler (reify ServiceScheduler
                      (delete-app [_ service-id]
                        (swap! available-services-atom disj service-id)
                        (swap! deleted-services-atom conj service-id)))
          scheduler-state-chan (async/chan 1)
          iteration-counter (atom 0)
          test-start-time (t/now)
          clock (fn [] (t/plus test-start-time (t/minutes @iteration-counter)))
          service-gc-go-routine (partial core/service-gc-go-routine read-state-fn write-state-fn leader? clock)]
      (testing "scheduler-broken-services-gc"
        (let [timeout-interval-ms 10
              broken-service-timeout-mins 5
              broken-service-min-hosts 3
              channel-map (scheduler-broken-services-gc scheduler scheduler-state-chan
                                                        {:broken-service-min-hosts broken-service-min-hosts
                                                         :broken-service-timeout-mins broken-service-timeout-mins
                                                         :scheduler-gc-broken-service-interval-ms timeout-interval-ms}
                                                        service-gc-go-routine)
              service-gc-exit-chan (:exit channel-map)]
          (dotimes [iteration 20]
            (async/>!! scheduler-state-chan
                       (concat
                         [[:update-available-services {:available-service-ids (set @available-services-atom)}]]
                         (vec
                           (map (fn [service-id]
                                  [:update-service-instances
                                   {:service-id service-id
                                    :failed-instances
                                    (cond
                                      (str/includes? service-id "broken")
                                      (map (fn [index] {:id (str service-id ".failed" index), :host (str "failed" index "-host.example.com")})
                                           (range (inc (mod iteration 4))))
                                      (str/includes? service-id "faulty")
                                      (map (fn [index] {:id (str service-id ".faulty" index), :host "faulty-host.example.com"})
                                           (range (mod iteration 4)))
                                      :else [])
                                    :healthy-instances (if (str/includes? service-id "broken") [] [{:id (str service-id ".unhealthy")}])}])
                                @available-services-atom))))
            (swap! iteration-counter inc)
            (while (> @iteration-counter @write-iteration-counter) nil))
          (async/>!! service-gc-exit-chan :exit)
          (is (= #{"service10broken", "service11broken"} @deleted-services-atom))
          (is (= #{"service6faulty", "service7", "service8stayalive", "service9stayalive"} @available-services-atom)))))))

(deftest test-scheduler-syncer
  (let [clock t/now
        scheduler-state-chan (async/chan 1)
        timeout-chan (async/chan 1)
        service-id->service-description-fn (fn [id] {"health-check-url" (str "/" id)})
        started-at (t/minus (clock) (t/hours 1))
        instance1 (->ServiceInstance "s1.i1" "s1" started-at nil nil #{} nil "host" 123 [] "proto" "/log" "test")
        instance2 (->ServiceInstance "s1.i2" "s1" started-at true nil #{} nil "host" 123 [] "proto" "/log" "test")
        instance3 (->ServiceInstance "s1.i3" "s1" started-at nil nil #{} nil "host" 123 [] "proto" "/log" "test")
        scheduler (reify ServiceScheduler
                    (get-apps->instances [_]
                      {(->Service "s1" {} {} {}) {:active-instances [instance1 instance2 instance3]
                                                  :failed-instances []}
                       (->Service "s2" {} {} {}) {:active-instances []
                                                  :failed-instances []}})
                    (service-id->state [_ _]
                      {:service-specific-state []})
                    (state [_]
                      {:state []}))
        available? (fn [{:keys [id]} url _]
                     (async/go (cond
                                 (and (= "s1.i1" id) (= "/s1" url)) {:healthy? true
                                                                     :status 200}
                                 :else {:healthy? false
                                        :status 400})))
        start-time-ms (-> (clock) .getMillis)
        {:keys [exit-chan query-chan]}
        (start-scheduler-syncer clock scheduler scheduler-state-chan timeout-chan service-id->service-description-fn available? {} 5)
        instance3-unhealthy (assoc instance3
                              :flags #{:has-connected :has-responded}
                              :healthy? false
                              :health-check-status 400)]
    (async/>!! timeout-chan :timeout)
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
                     (async/timeout 10000) ([_] {:message "Request timed out!"}))]
      (doseq [required-key [:service-id->health-check-context
                            :state]]
        (is (contains? response required-key)))
      (is (= {"s1" {:instance-id->unhealthy-instance {"s1.i3" instance3-unhealthy},
                    :instance-id->tracked-failed-instance {},
                    :instance-id->failed-health-check-count {"s1.i3" 1}}
              "s2" {:instance-id->failed-health-check-count {}
                    :instance-id->tracked-failed-instance {}
                    :instance-id->unhealthy-instance {}}}
             (:service-id->health-check-context response))))
    ;; Retrieves scheduler state with service-id
    (let [response-chan (async/promise-chan)
          _ (async/>!! query-chan {:response-chan response-chan :service-id "s1"})
          response (async/alt!!
                     response-chan ([state] state)
                     (async/timeout 10000) ([_] {:message "Request timed out!"}))
          end-time-ms (-> (clock) .getMillis)]
      (doseq [required-key [:instance-id->failed-health-check-count
                            :instance-id->tracked-failed-instance
                            :instance-id->unhealthy-instance
                            :last-update-time
                            :service-specific-state]]
        (is (contains? response required-key)))
      (is (nil? (:service-id->health-check-context response)))
      (is (<= start-time-ms (-> response :last-update-time .getMillis) end-time-ms)))
    (async/>!! exit-chan :exit)))

(deftest test-start-health-checks
  (let [available-instance "id1"
        service {:id "s1"}
        available? (fn [instance _]
                     (async/go
                       (let [healthy? (= (:id instance) available-instance)]
                         {:healthy? healthy?
                          :status (if healthy? 200 400)})))
        service->service-description-fn (constantly {:health-check-url "/health"})]
    (testing "Does not call available? for healthy apps"
      (let [service->service-instances {service {:active-instances [{:id "id1"
                                                                     :healthy? true}
                                                                    {:id "id2"
                                                                     :healthy? true}]}}
            service->service-instances' (start-health-checks service->service-instances
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
            service->service-instances' (start-health-checks service->service-instances
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
        available? (fn [{:keys [id]} _]
                     (async/go
                       (let [healthy? (= id available-instance)]
                         {:healthy? healthy?
                          :status (if healthy? 200 400)})))
        service->service-description-fn (constantly {:health-check-url "/healthy"})
        service->service-instances {service {:active-instances [{:id available-instance}
                                                                {:id "id2"
                                                                 :healthy? false}
                                                                {:id "id3"
                                                                 :healthy? true}]}}
        service->service-instances' (do-health-checks service->service-instances
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
                       (ss/throw+ {:status 501}))
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
    (doseq [status [300 302 400 404]]
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
    (doseq [status [500 501 502 503 504]]
      (let [call-counter (atom 0)
            function (fn [] (swap! call-counter inc) (ss/throw+ {:status status}))]
        (is (thrown-with-msg?
              Exception #"test"
              (ss/try+
                (retry-on-transient-server-exceptions "test" (function))
                (catch [:status status] _
                  (throw (Exception. "test"))))))
        (is (= 5 @call-counter))))))

(deftest test-killed-instances-transient-store
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time)
        make-instance (fn [service-id instance-id]
                        {:id instance-id
                         :service-id service-id})]
    (with-redefs [t/now (fn [] current-time)]
      (testing "tracking-instance-killed"

        (preserve-only-killed-instances-for-services! [])

        (process-instance-killed! (make-instance "service-1" "service-1.A"))
        (process-instance-killed! (make-instance "service-2" "service-2.A"))
        (process-instance-killed! (make-instance "service-1" "service-1.C"))
        (process-instance-killed! (make-instance "service-1" "service-1.B"))

        (is (= [{:id "service-1.A", :service-id "service-1", :killed-at current-time-str}
                {:id "service-1.B", :service-id "service-1", :killed-at current-time-str}
                {:id "service-1.C", :service-id "service-1", :killed-at current-time-str}]
               (service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (service-id->killed-instances "service-2")))
        (is (= [] (service-id->killed-instances "service-3")))

        (remove-killed-instances-for-service! "service-1")
        (is (= [] (service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (service-id->killed-instances "service-2")))
        (is (= [] (service-id->killed-instances "service-3")))

        (process-instance-killed! (make-instance "service-3" "service-3.A"))
        (process-instance-killed! (make-instance "service-3" "service-3.B"))
        (is (= [] (service-id->killed-instances "service-1")))
        (is (= [{:id "service-2.A" :service-id "service-2", :killed-at current-time-str}]
               (service-id->killed-instances "service-2")))
        (is (= [{:id "service-3.A", :service-id "service-3", :killed-at current-time-str}
                {:id "service-3.B", :service-id "service-3", :killed-at current-time-str}]
               (service-id->killed-instances "service-3")))

        (remove-killed-instances-for-service! "service-2")
        (is (= [] (service-id->killed-instances "service-1")))
        (is (= [] (service-id->killed-instances "service-2")))
        (is (= [{:id "service-3.A", :service-id "service-3", :killed-at current-time-str}
                {:id "service-3.B", :service-id "service-3", :killed-at current-time-str}]
               (service-id->killed-instances "service-3")))

        (preserve-only-killed-instances-for-services! [])
        (is (= [] (service-id->killed-instances "service-1")))
        (is (= [] (service-id->killed-instances "service-2")))
        (is (= [] (service-id->killed-instances "service-3")))))))

(deftest test-max-killed-instances-cache
  (let [current-time (t/now)
        current-time-str (du/date-to-str current-time)
        make-instance (fn [service-id instance-id]
                        {:id instance-id, :service-id service-id, :killed-at current-time-str})]
    (with-redefs [t/now (fn [] current-time)]
      (testing "test-max-killed-instances-cache"
        (preserve-only-killed-instances-for-services! [])
        (doseq [n (range 10 50)]
          (process-instance-killed! (make-instance "service-1" (str "service-1." n))))
        (let [killed-instances (map (fn [n] {:id (str "service-1." n), :service-id "service-1", :killed-at current-time-str}) (range 40 50))]
          (is (= killed-instances
                 (service-id->killed-instances "service-1"))))))))


(deftest test-available?
  (with-redefs [http/get (fn [_ _] (throw (IllegalArgumentException. "Unable to make request")))]
    (let [resp (async/<!! (available? {:port 80 :protocol "http" :host "www.example.com"}
                                      "/health-check"
                                      (Object.)))]
      (is (= {:healthy? false} resp)))))

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
        req1 {:requested 1}
        req3 {:requested 3}
        waiter-timer (metrics/waiter-timer "launch-overhead" "schedule-time")

        empty-trackers' (update-launch-trackers
                          empty-trackers empty-new-service-ids empty-removed-service-ids
                          empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                          empty-service-id->instance-counts waiter-timer)
        empty-trackers'' (update-launch-trackers
                           empty-trackers empty-new-service-ids #{"service-foo"}
                           empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                           empty-service-id->instance-counts waiter-timer)
        _ (testing "update-launch-trackers: empty -> empty"
            (is (= empty-trackers empty-trackers'))
            (is (= empty-trackers empty-trackers'')))

        service-id->instance-counts-1 {"service-1" req1 "service-2" req1}
        trackers-1 (update-launch-trackers
                     empty-trackers #{"service-1" "service-2"} empty-removed-service-ids
                     empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                     service-id->instance-counts-1 waiter-timer)
        _ (testing "update-launch-trackers: empty -> non-empty"
            (check-trackers trackers-1 {"service-1" {:scheduling-instance-count 1}
                                        "service-2" {:scheduling-instance-count 1}}))

        trackers-2 (update-launch-trackers
                     trackers-1 empty-new-service-ids #{"service-1" "service-2"}
                     empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                     empty-service-id->instance-counts waiter-timer)
        _ (testing "update-launch-trackers: trivial non-empty -> empty"
            (is (= empty-trackers trackers-2)))

        service-id->instance-counts-3 {"service-1" req1 "service-3" req1}
        trackers-3 (update-launch-trackers
                     trackers-1 #{"service-3"} #{"service-2"}
                     empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                     service-id->instance-counts-3 waiter-timer)
        _ (testing "update-launch-trackers: simultaneously add and remove services"
            (check-trackers trackers-3 {"service-1" {:scheduling-instance-count 1}
                                        "service-3" {:scheduling-instance-count 1}}))

        trackers-4 (update-launch-trackers
                     trackers-3 empty-new-service-ids empty-removed-service-ids
                     empty-service-id->healthy-instances {"service-1" [(make-service-instance 1 1)]}
                     service-id->instance-counts-3 waiter-timer)
        _ (testing "update-launch-trackers: scheduled a service instance"
            (check-trackers trackers-4 {"service-1" {:known-instance-ids #{"inst-1.1"}
                                                     :starting-instance-ids ["inst-1.1"]}
                                        "service-3" {:scheduling-instance-count 1}}))

        service-id->instance-counts-5 {"service-1" req1 "service-3" req1 "service-4" req1}
        trackers-5 (update-launch-trackers
                     trackers-4 #{"service-4"} empty-removed-service-ids
                     {"service-1" [(make-service-instance 1 1)]
                      "service-3" [(make-service-instance 3 1)]}
                     empty-service-id->unhealthy-instances service-id->instance-counts-5 waiter-timer)
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
                     service-id->instance-counts-6 waiter-timer)
        _ (testing "update-launch-trackers: simultaneously add and remove services,
                    and a healthy instance appears"
            (check-trackers trackers-6 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-4" {:known-instance-ids #{"inst-4.1"}}
                                        "service-5" {:known-instance-ids #{"inst-5.1"}
                                                     :starting-instance-ids ["inst-5.1"]}}))

        service-id->instance-counts-7 {"service-1" req1 "service-4" req1 "service-5" req3}
        trackers-7 (update-launch-trackers
                     trackers-6 empty-new-service-ids empty-removed-service-ids
                     {"service-1" [(make-service-instance 1 1)]
                      "service-4" [(make-service-instance 4 1)]}
                     {"service-5" [(make-service-instance 5 1)]}
                     service-id->instance-counts-7 waiter-timer)
        _ (testing "update-launch-trackers: service 5 scales to 3 instances"
            (check-trackers trackers-7 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-4" {:known-instance-ids #{"inst-4.1"}}
                                        "service-5" {:known-instance-ids #{"inst-5.1"}
                                                     :scheduling-instance-count 2
                                                     :starting-instance-ids ["inst-5.1"]}}))

        service-id->instance-counts-8 {"service-1" req1 "service-4" req1 "service-5" req3}
        trackers-8 (update-launch-trackers
                     trackers-7 empty-new-service-ids empty-removed-service-ids
                     empty-service-id->healthy-instances
                     {"service-1" [(make-service-instance 1 1)]
                      "service-4" [(make-service-instance 4 1)]
                      "service-5" [(make-service-instance 5 1)
                                   (make-service-instance 5 2)
                                   (make-service-instance 5 3)]}
                     service-id->instance-counts-8 waiter-timer)
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
                     service-id->instance-counts-8 waiter-timer)
        _ (testing "update-launch-trackers: all instances transition to healthy"
            (check-trackers trackers-9 {"service-1" {:known-instance-ids #{"inst-1.1"}}
                                        "service-4" {:known-instance-ids #{"inst-4.1"}}
                                        "service-5" {:known-instance-ids #{"inst-5.1" "inst-5.2" "inst-5.3"}}}))

        trackers-10 (update-launch-trackers
                      trackers-9 empty-new-service-ids #{"service-1" "service-4" "service-5"}
                      empty-service-id->healthy-instances empty-service-id->unhealthy-instances
                      empty-service-id->instance-counts waiter-timer)]
    (testing "update-launch-trackers: transition back to empty"
      (is (= empty-trackers trackers-10)))))

(deftest test-start-launch-metrics-maintainer
  (testing "start-launch-metrics-maintainer"
    (let [make-metric-maintainer
          (fn make-metric-maintainer
            [state-0]
            (let [state-updates-chan (async/chan 1)
                  maintainer (start-launch-metrics-maintainer state-updates-chan)]
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
            instance-counts {:requested 3 :scheduled 2}
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
                                           :starting-instance-id->start-timestamp {}}}}

            instance-counts' {:requested 6 :scheduled 3}
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
                                           :starting-instance-id->start-timestamp {"inst3" :time}}}}]
        (testing "non-empty initial router state"
          (is (= expected-state-1 actual-state-1)))
        (testing "ignored router state update"
          (is (= expected-state-1 actual-state-1')))
        (testing "applied router state update"
          (is (= expected-state-2 actual-state-2)))))))
