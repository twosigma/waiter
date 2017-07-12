;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.scheduler.scheduler-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [slingshot.slingshot :as ss]
            [waiter.core :as core]
            [waiter.curator :as curator]
            [waiter.scheduler.scheduler :refer :all]
            [waiter.utils :as utils]))

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
  (let [test-instance (->ServiceInstance
                        "instance-id"
                        "service-id"
                        "2014-09-13T00:24:46.959Z"
                        true
                        "www.scheduler-test.example.com"
                        1234
                        []
                        "proto"
                        "log-dir"
                        "instance-message")]
    (testing (str "Test record ServiceInstance")
      (is (= "instance-id" (:id test-instance)))
      (is (= "service-id" (:service-id test-instance)))
      (is (= "2014-09-13T00:24:46.959Z" (:started-at test-instance)))
      (is (= true (:healthy? test-instance)))
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
                channel-map (scheduler-services-gc
                              scheduler scheduler-state-chan service-id->metrics-fn
                              {:broken-service-min-hosts broken-service-min-hosts
                               :broken-service-timeout-mins broken-service-timeout-mins
                               :scheduler-gc-interval-ms timeout-interval-ms}
                              service-gc-go-routine (constantly {"idle-timeout-mins" 50}))
                service-gc-exit-chan (:exit channel-map)]
            (dotimes [n 100]
              (let [global-state (pc/map-vals #(update-in % ["outstanding"] (fn [v] (max 0 (- v n))))
                                              initial-global-state)]
                (async/>!! scheduler-state-chan (concat
                                                  [[:update-available-apps {:available-apps (vec @available-services-atom)}]]
                                                  (vec
                                                    (map (fn [service-id]
                                                           [:update-app-instances
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
                         [[:update-available-apps {:available-apps (vec @available-services-atom)}]]
                         (vec
                           (map (fn [service-id]
                                  [:update-app-instances
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
  (let [scheduler-state-chan (async/chan 1)
        scheduler-syncer-interval-secs 1
        service-id->service-description-fn (fn [id] {"health-check-url" (str "/" id)})
        started-at "2014-09-14T002446.965Z"
        instance1 (->ServiceInstance "1.1" "1" started-at nil "host" 123 [] "proto" "/log" "test")
        instance2 (->ServiceInstance "1.2" "1" started-at true "host" 123 [] "proto" "/log" "test")
        instance3 (->ServiceInstance "1.3" "1" started-at nil "host" 123 [] "proto" "/log" "test")
        scheduler (reify ServiceScheduler
                    (get-apps->instances [_]
                      {(->Service "1" {} {} {}) {:active-instances [instance1 instance2 instance3]
                                                 :failed-instances []}}))
        available? (fn [{:keys [id]} url _]
                     (async/go (cond
                                 (and (= "1.1" id) (= "/1" url)) true
                                 :else false)))
        syncer-cancel (start-scheduler-syncer scheduler scheduler-state-chan
                                              scheduler-syncer-interval-secs service-id->service-description-fn available?
                                              {})]
    (Thread/sleep (* 1000 scheduler-syncer-interval-secs))
    (let [[[update-apps-msg update-apps] [update-instances-msg update-instances]] (async/<!! scheduler-state-chan)]
      (is (= :update-available-apps update-apps-msg))
      (is (= (list "1") (:available-apps update-apps)))
      (is (= :update-app-instances update-instances-msg))
      (is (= [(assoc instance1 :healthy? true) instance2] (:healthy-instances update-instances)))
      (is (= [(assoc instance3 :healthy? false)] (:unhealthy-instances update-instances)))
      (is (= "1" (:service-id update-instances))))
    (syncer-cancel)))

(deftest test-start-health-checks
  (let [available-instance "id1"
        service {:id "s1"}
        available? (fn [instance _]
                     (async/go
                       (= (:id instance) available-instance)))
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
                       (= id available-instance)))
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

  (testing "failure-on-non-transient-exception-throw"
    (let [call-counter (atom 0)
          function (fn [] (swap! call-counter inc) (throw (Exception. "test")))]
      (is (thrown-with-msg?
            Exception #"test"
            (retry-on-transient-server-exceptions "test" (function))))
      (is (= 1 @call-counter))))

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
        current-time-str (utils/date-to-str current-time)
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
        current-time-str (utils/date-to-str current-time)
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