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
(ns waiter.service-test
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [full.async :as fa]
            [waiter.mocks :refer :all]
            [waiter.service :refer :all])
  (:import (java.util.concurrent Future Executors)))

(deftest test-extract-health-check-url
  (let [default-url "/defaultStatus"
        custom-url  "/customStatus"]
    (let [test-cases (list
                       {:name     "extract-health-check-url-nil-data"
                        :app-info nil
                        :expected default-url
                        }
                       {:name     "extract-health-check-url-empty-data"
                        :app-info {}
                        :expected default-url
                        }
                       {:name     "extract-health-check-url-empty-app"
                        :app-info {:app {}}
                        :expected default-url
                        }
                       {:name     "extract-health-check-url-empty-health-check"
                        :app-info {:app {:healthChecks []}}
                        :expected default-url
                        }
                       {:name     "extract-health-check-url-missing-health-check-url"
                        :app-info {:app {:healthChecks [{:foo 1 :bar 2}]}}
                        :expected default-url
                        }
                       {:name     "extract-health-check-url-present-health-check-url"
                        :app-info {:app {:healthChecks [{:foo 1 :bar 2 :path custom-url}]}}
                        :expected custom-url
                        }
                       {:name     "extract-health-check-url-multiple-health-check-urls"
                        :app-info {:app {:healthChecks [{:foo 1 :path custom-url} {:bar 2 :path "/fooBarBaz"}]}}
                        :expected custom-url
                        })]
      (doseq [test-case test-cases]
        (testing (str "Test " (:name test-case))
          (is (= (:expected test-case)
                (extract-health-check-url (:app-info test-case) default-url))))))))


(deftest test-annotate-tasks-with-health-url
  (let [custom-url  "/customStatus"
        app-info->task (fn [x] {:tasks (get-in x [:app :tasks])})]
    (let [test-cases (list
                       {:name     "annotate-tasks-with-health-url:nil-data"
                        :app-info nil
                        :expected {:tasks ()}
                        }
                       {:name     "annotate-tasks-with-health-url:empty-data"
                        :app-info {}
                        :expected {:tasks ()}
                        }
                       {:name     "annotate-tasks-with-health-url:valid-data"
                        :app-info {:app {:healthChecks [{:path custom-url}] :tasks [{:foo 1 :bar 2}]}}
                        :expected {:tasks [{:foo 1 :bar 2 :health-check-url custom-url}]}
                        }
                       {:name     "annotate-tasks-with-health-url:valid-data2"
                        :app-info {:app {:healthChecks [{:path custom-url}]
                                         :tasks [{:foo 1 :bar 2}, {:fie 3 :foe 4}]}}
                        :expected {:tasks [{:foo 1 :bar 2 :health-check-url custom-url},
                                           {:fie 3 :foe 4 :health-check-url custom-url}]}
                        }
                       )]
      (doseq [test-case test-cases]
        (testing (str "Test " (:name test-case))
          (is (= (:expected test-case)
                 (-> (:app-info test-case) annotate-tasks-with-health-url app-info->task))))))))

(defn- mock-blacklisting-instance
  [instance-rpc-chan service-id callback-fn]
  (async/thread
    (try
      (let [[operation in-service-id _ chan-resp-chan] (async/<!! instance-rpc-chan)
            blacklist-chan (async/chan 1)]
        (is (= :blacklist operation))
        (is (= service-id in-service-id))
        (async/>!! chan-resp-chan blacklist-chan)
        (callback-fn (async/<!! blacklist-chan)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)))))

(deftest test-blacklist-instance
  (testing "basic blacklist"
    (let [instance-rpc-chan (async/chan 1)
          service-id "test-id"
          instance-id "test-id.12345"
          instance {:service-id service-id :id instance-id}
          custom-blacklist-period-ms 5000
          response-chan (async/promise-chan)]
      (mock-blacklisting-instance
        instance-rpc-chan
        service-id
        (fn [[{:keys [cid blacklist-period-ms instance-id]} _]]
          (is (= (:id instance) instance-id))
          (is (not (nil? cid)))
          (is (= custom-blacklist-period-ms blacklist-period-ms))))
      (blacklist-instance-go instance-rpc-chan service-id instance-id custom-blacklist-period-ms response-chan)
      (async/<!! (async/timeout 10)))))

(defn- mock-offering-instance
  [instance-rpc-chan service-id callback-fn]
  (async/thread
    (try
      (let [[operation in-service-id _ chan-resp-chan] (async/<!! instance-rpc-chan)
            offer-chan (async/chan 1)]
        (is (= :offer operation))
        (is (= service-id in-service-id))
        (async/>!! chan-resp-chan offer-chan)
        (callback-fn (async/<!! offer-chan)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)
        (is false "Unexpected exception")))))

(deftest test-offer-instance
  (testing "basic offer"
    (let [instance-rpc-chan (async/chan 1)
          router-id "router-id"
          service-id "test-id"
          instance-id "test-id.12345"
          instance {:service-id service-id, :id instance-id}
          response-chan (async/promise-chan)]
      (mock-offering-instance
        instance-rpc-chan
        service-id
        (fn [offer-params]
          (is (= instance (:instance offer-params)))
          (is (= response-chan (:response-chan offer-params)))
          (is (= router-id (:router-id offer-params)))))
      (offer-instance-go instance-rpc-chan service-id {:instance instance
                                                       :response-chan response-chan
                                                       :router-id router-id})
      (async/<!! (async/timeout 10)))))

(deftest test-query-maintainer-channel-map-with-timeout!
  (testing "basic query-state success"
    (let [instance-rpc-chan (async/chan 1)
          service-id "test-id"
          result-query-chan (async/chan 1)]
      (async/thread
        (let [[operation in-service-id _ chan-resp-chan] (async/<!! instance-rpc-chan)]
          (is (= :query-key operation))
          (is (= service-id in-service-id))
          (is (not (nil? chan-resp-chan)))
          (async/>!! chan-resp-chan result-query-chan)))
      (async/<!! ;; wait for the go-block to complete execution
        (async/go
          (let [result-chan (query-maintainer-channel-map-with-timeout! instance-rpc-chan service-id 1000 :query-key)]
            (is (= result-query-chan result-chan)))))))
  (testing "basic query-state timeout"
    (let [instance-rpc-chan (async/chan 1)
          service-id "test-id"]
      (async/thread
        (let [[operation in-service-id _ chan-resp-chan] (async/<!! instance-rpc-chan)]
          (is (= :query-key operation))
          (is (= service-id in-service-id))
          (is (not (nil? chan-resp-chan)))))
      (async/<!! ;; wait for the go-block to complete execution
        (async/go
          (let [result-chan (query-maintainer-channel-map-with-timeout! instance-rpc-chan service-id 100 :query-key)]
            (is (= {:message "Request timed-out!"} result-chan))))))))

(defn- mock-query-state-instance
  [instance-rpc-chan service-id callback-fn]
  (async/thread
    (try
      (let [[operation in-service-id _ chan-resp-chan] (async/<!! instance-rpc-chan)
            query-state-chan (async/chan 1)]
        (is (= :query-state operation))
        (is (= service-id in-service-id))
        (async/>!! chan-resp-chan query-state-chan)
        (callback-fn (async/<!! query-state-chan)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)))))

(deftest test-query-state-instance
  (testing "basic query-state"
    (let [instance-rpc-chan (async/chan 1)
          in-service-id "test-id"
          response-chan (async/promise-chan)]
      (mock-query-state-instance
        instance-rpc-chan
        in-service-id
        (fn [{:keys [cid response-chan service-id]}]
          (is (= in-service-id service-id))
          (is (not (nil? cid)))
          (is (not (nil? response-chan)))))
      (query-instance-go instance-rpc-chan in-service-id response-chan)
      (async/<!! (async/timeout 10)))))

(deftest test-release-instance
  (testing "basic release"
    (let [instance-rpc-chan (async/chan 1)
          instance {:service-id "test-id" :id "test-id.12345"}
          reservation-result {:status :success, :cid "CID"}]
      (mock-reservation-system instance-rpc-chan
                               [(fn [[inst reservation-result]]
                                  (is (= instance inst))
                                  (is (= :success (:status reservation-result))))])
      (release-instance-go instance-rpc-chan instance reservation-result)
      ; Let mock propogate
      (async/<!! (async/timeout 10)))))

(deftest test-get-rand-inst
  (testing "basic get"
    (let [instance-rpc-chan (async/chan 1)
          instance {:service-id "test-id" :id "test-id.12345"}
          service-id "test-id"]
      (mock-reservation-system instance-rpc-chan [(fn [[_ resp-chan]] (async/>!! resp-chan instance))])
      (is (= instance
             (fa/<?? (async/go (get-rand-inst instance-rpc-chan service-id {:reason :serve-request} nil 100))))))))

(deftest test-start-new-service
  (let [make-cache-fn (fn [threshold ttl]
                        (-> {}
                            (cache/fifo-cache-factory :threshold threshold)
                            (cache/ttl-cache-factory :ttl ttl)
                            atom))
        start-app-threadpool (Executors/newFixedThreadPool 20)
        scheduler (Object.)
        descriptor {:id "test-service-id"}
        service-id->password-fn #(str % "-password")]
    (testing "start-new-service"
      (let [cache-atom (make-cache-fn 100 20)
            start-called-atom (atom false)
            start-fn (fn [] (reset! start-called-atom (not @start-called-atom)))
            start-app-result (start-new-service scheduler service-id->password-fn descriptor cache-atom start-app-threadpool :start-fn start-fn)]
        (is (not (nil? start-app-result)))
        (.get ^Future start-app-result)
        (is @start-called-atom)))
    (testing "app-already-starting"
      (let [cache-atom (make-cache-fn 100 20)
            start-called-atom (atom false)
            start-fn (fn [] (reset! start-called-atom (not @start-called-atom)))]
        (let [start-app-result-1 (start-new-service scheduler service-id->password-fn descriptor cache-atom start-app-threadpool :start-fn start-fn)]
          (is (not (nil? start-app-result-1)))
          (.get ^Future start-app-result-1)
          (is @start-called-atom))
        (let [start-app-result-2 (start-new-service scheduler service-id->password-fn descriptor cache-atom start-app-threadpool :start-fn start-fn)]
          (is (nil? start-app-result-2)))))
    (testing "app-starting-after-cache-eviction"
      (let [cache-atom (make-cache-fn 100 20)
            start-called-atom (atom 0)]
        (let [start-fn (fn [] (reset! start-called-atom 1))
              start-app-result-1 (start-new-service scheduler service-id->password-fn descriptor cache-atom start-app-threadpool :start-fn start-fn)]
          (is (not (nil? start-app-result-1)))
          (.get ^Future start-app-result-1)
          (is (= 1 @start-called-atom)))
        (Thread/sleep 30)
        (let [start-fn (fn [] (reset! start-called-atom 2))
              start-app-result-2 (start-new-service scheduler service-id->password-fn descriptor cache-atom start-app-threadpool :start-fn start-fn)]
          (is (not (nil? start-app-result-2)))
          (.get ^Future start-app-result-2))
        (is (= 2 @start-called-atom))))
    (testing "tens-of-apps-starting-simultaneously"
      (let [cache-atom (make-cache-fn 100 15000)
            start-called-atom (atom {})
            individual-call-result-atom (atom {})]
        (doall
          (pmap
            (fn [n]
              (let [app-num (rand-int 50)
                    service-id (str "test-service-id-" app-num)
                    descriptor {:id service-id}
                    start-fn (fn [] (swap! start-called-atom assoc service-id (not (get @start-called-atom service-id))))
                    start-app-result (start-new-service scheduler service-id->password-fn descriptor cache-atom start-app-threadpool :start-fn start-fn)]
                (if-not (nil? start-app-result)
                  (do
                    (.get ^Future start-app-result)
                    (swap! individual-call-result-atom assoc n (get @start-called-atom service-id)))
                  (swap! individual-call-result-atom assoc n true))))
            (range 1 1000)))
        (is (and (pos? (count @start-called-atom))
                 (every? (fn [[_ value]] (true? value)) @start-called-atom)))
        (is (and (pos? (count @individual-call-result-atom))
                 (every? (fn [[_ value]] (true? value)) @individual-call-result-atom)))))))
