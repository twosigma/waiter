;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.fallback-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [waiter.fallback :refer :all]
            [waiter.util.async-utils :as au])
  (:import [org.joda.time DateTime]))

(deftest test-fallback-maintainer
  (let [current-healthy-service-ids #{"service-1" "service-3"}
        current-available-service-ids (set/union current-healthy-service-ids #{"service-5" "service-7"})
        fallback-state-atom (-> {:available-service-ids current-available-service-ids
                                 :healthy-service-ids current-healthy-service-ids}
                                atom)
        new-healthy-service-ids (set/union current-available-service-ids #{"service-2" "service-4"})
        new-available-service-ids (set/union new-healthy-service-ids #{"service-6" "service-8"})
        scheduler-messages [[:update-available-services {:available-service-ids new-available-service-ids
                                                         :healthy-service-ids new-healthy-service-ids}]]
        scheduler-state-chan (au/latest-chan)
        {:keys [exit-chan query-chan]} (fallback-maintainer scheduler-state-chan fallback-state-atom)]

    (async/>!! scheduler-state-chan scheduler-messages)
    (let [response-chan (async/promise-chan)
          _ (async/>!! query-chan {:response-chan response-chan})
          state (async/<!! response-chan)]
      (is (= {:state {:available-service-ids new-available-service-ids
                      :healthy-service-ids new-healthy-service-ids}}
             state))
      (is (= {:available-service-ids new-available-service-ids
              :healthy-service-ids new-healthy-service-ids}
             (deref fallback-state-atom))))
    (async/>!! exit-chan :exit)))

(deftest test-service-lookups
  (let [healthy-service-ids #{"service-1" "service-3"}
        available-service-ids (set/union healthy-service-ids #{"service-5" "service-7"})
        fallback-state {:available-service-ids available-service-ids :healthy-service-ids healthy-service-ids}
        additional-service-ids #{"service-2" "service-4" "service-6"}]
    (doseq [service-id available-service-ids]
      (is (service-exists? fallback-state service-id)))
    (doseq [service-id additional-service-ids]
      (is (not (service-exists? fallback-state service-id))))
    (doseq [service-id healthy-service-ids]
      (is (service-healthy? fallback-state service-id)))
    (doseq [service-id (set/union additional-service-ids (set/difference available-service-ids healthy-service-ids))]
      (is (not (service-healthy? fallback-state service-id))))))

(deftest test-retrieve-fallback-descriptor
  (let [current-time (t/now)
        current-time-millis (.getMillis ^DateTime current-time)
        descriptor->previous-descriptor (fn [descriptor] (:previous descriptor))
        service-fallback-period-secs 120
        default-service-fallback-period-secs (* 4 service-fallback-period-secs)
        search-history-length 5
        time-1 (- current-time-millis (t/in-millis (t/seconds 30)))
        descriptor-1 {:service-id "service-1"
                      :sources {:service-fallback-period-secs service-fallback-period-secs
                                :token->token-data {"test-token" {"last-update-time" time-1}}
                                :token-sequence ["test-token"]}}
        time-2 (- current-time-millis (t/in-millis (t/seconds 20)))
        descriptor-2 {:previous descriptor-1
                      :service-id "service-2"
                      :sources {:service-fallback-period-secs service-fallback-period-secs
                                :token->token-data {"test-token" {"last-update-time" time-2}}
                                :token-sequence ["test-token"]}}
        request-time current-time]

    (testing "no fallback service for on-the-fly"
      (let [descriptor-4 {:previous descriptor-2
                          :service-id "service-4"
                          :sources {:token->token-data {} :token-sequence []}}
            fallback-state {:available-service-ids #{"service-1" "service-2"}
                            :healthy-service-ids #{"service-1" "service-2"}}
            result-descriptor (retrieve-fallback-descriptor
                                descriptor->previous-descriptor default-service-fallback-period-secs search-history-length
                                fallback-state request-time descriptor-4)]
        (is (nil? result-descriptor))))

    (let [time-3 (- current-time-millis (t/in-millis (t/seconds 10)))
          descriptor-3 {:previous descriptor-2
                        :service-id "service-3"
                        :sources {:service-fallback-period-secs service-fallback-period-secs
                                  :token->token-data {"test-token" {"last-update-time" time-3}}
                                  :token-sequence ["test-token"]}}]

      (testing "fallback to previous healthy instance inside fallback period"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1" "service-2"}}
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor default-service-fallback-period-secs search-history-length
                                  fallback-state request-time descriptor-3)]
          (is (= descriptor-2 result-descriptor))))

      (testing "no healthy fallback service"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{}}
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor default-service-fallback-period-secs search-history-length
                                  fallback-state request-time descriptor-3)]
          (is (nil? result-descriptor))))

      (testing "no fallback service outside period"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1" "service-2"}}
              request-time (t/plus current-time (t/seconds (* 2 service-fallback-period-secs)))
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor default-service-fallback-period-secs search-history-length
                                  fallback-state request-time descriptor-3)]
          (is (nil? result-descriptor))))

      (testing "fallback to 2-level previous healthy instance inside fallback period"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1"}}
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor default-service-fallback-period-secs search-history-length
                                  fallback-state request-time descriptor-3)]
          (is (= descriptor-1 result-descriptor))))

      (testing "no fallback for limited history"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1"}}
              search-history-length 1
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor default-service-fallback-period-secs search-history-length
                                  fallback-state request-time descriptor-3)]
          (is (nil? result-descriptor)))))))

(deftest test-wrap-fallback
  (let [handler (fn [request] (assoc request :status 201))
        service-id (str "test-service-id-" (rand-int 100000))
        prev-service-id (str service-id ".prev")
        descriptor-1 {:service-id prev-service-id}
        descriptor-2 {:service-id service-id}
        default-service-fallback-period-secs 60
        search-history-length 5
        descriptor->previous-descriptor-fn nil
        start-new-service-fn nil
        assoc-run-as-user-approved? nil
        request-time (t/now)]

    (testing "healthy service"
      (let [retrieve-healthy-fallback-promise (promise)]
        (with-redefs [retrieve-fallback-descriptor
                      (fn [_ in-service-fallback-period-secs in-history-length in-fallback-state in-request-time in-descriptor]
                        (deliver retrieve-healthy-fallback-promise :called)
                        (is (= default-service-fallback-period-secs in-service-fallback-period-secs))
                        (is (= search-history-length in-history-length))
                        (is in-fallback-state)
                        (is (= request-time in-request-time))
                        (is (= descriptor-2 in-descriptor))
                        (throw (IllegalStateException. "Unexpected call to retrieve-fallback-descriptor")))]
          (let [fallback-state-atom (atom {:available-service-ids #{service-id}
                                           :healthy-service-ids #{service-id}})
                fallback-handler (wrap-fallback handler descriptor->previous-descriptor-fn start-new-service-fn assoc-run-as-user-approved?
                                                default-service-fallback-period-secs search-history-length fallback-state-atom)
                request {:descriptor descriptor-2 :request-time request-time}
                response (fallback-handler request)]
            (is (= :not-called (deref retrieve-healthy-fallback-promise 0 :not-called)))
            (is (= (assoc request :status 201) response))))))

    (testing "unhealthy service with healthy fallback"
      (let [retrieve-healthy-fallback-promise (promise)]
        (with-redefs [retrieve-fallback-descriptor
                      (fn [_ in-service-fallback-period-secs in-history-length in-fallback-state in-request-time in-descriptor]
                        (deliver retrieve-healthy-fallback-promise :called)
                        (is (= default-service-fallback-period-secs in-service-fallback-period-secs))
                        (is (= search-history-length in-history-length))
                        (is in-fallback-state)
                        (is (= request-time in-request-time))
                        (is (= descriptor-2 in-descriptor))
                        descriptor-1)]
          (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id service-id}
                                           :healthy-service-ids #{prev-service-id}})
                fallback-handler (wrap-fallback handler descriptor->previous-descriptor-fn start-new-service-fn assoc-run-as-user-approved?
                                                default-service-fallback-period-secs search-history-length fallback-state-atom)
                request {:descriptor descriptor-2 :request-time request-time}
                response (fallback-handler request)]
            (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))
            (is (= (assoc request :descriptor descriptor-1 :fallback-source-id service-id :status 201) response))))))

    (testing "unhealthy service with no fallback"
      (let [retrieve-healthy-fallback-promise (promise)]
        (with-redefs [retrieve-fallback-descriptor
                      (fn [_ in-service-fallback-period-secs in-history-length in-fallback-state in-request-time in-descriptor]
                        (deliver retrieve-healthy-fallback-promise :called)
                        (is (= default-service-fallback-period-secs in-service-fallback-period-secs))
                        (is (= search-history-length in-history-length))
                        (is in-fallback-state)
                        (is (= request-time in-request-time))
                        (is (= descriptor-2 in-descriptor))
                        nil)]
          (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id service-id}
                                           :healthy-service-ids #{}})
                fallback-handler (wrap-fallback handler descriptor->previous-descriptor-fn start-new-service-fn assoc-run-as-user-approved?
                                                default-service-fallback-period-secs search-history-length fallback-state-atom)
                request {:descriptor descriptor-2 :request-time request-time}
                response (fallback-handler request)]
            (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))
            (is (= (assoc request :descriptor descriptor-2 :status 201) response))))))))
