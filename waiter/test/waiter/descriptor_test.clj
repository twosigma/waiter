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
(ns waiter.descriptor-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [waiter.descriptor :refer :all]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.token :as token]
            [waiter.util.async-utils :as au])
  (:import (clojure.lang ExceptionInfo)
           (org.joda.time DateTime)))

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
                                descriptor->previous-descriptor search-history-length fallback-state request-time descriptor-4)]
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
                                  descriptor->previous-descriptor search-history-length fallback-state request-time descriptor-3)]
          (is (= descriptor-2 result-descriptor))))

      (testing "no healthy fallback service"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{}}
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor search-history-length fallback-state request-time descriptor-3)]
          (is (nil? result-descriptor))))

      (testing "no fallback service outside period"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1" "service-2"}}
              request-time (t/plus current-time (t/seconds (* 2 service-fallback-period-secs)))
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor search-history-length fallback-state request-time descriptor-3)]
          (is (nil? result-descriptor))))

      (testing "fallback to 2-level previous healthy instance inside fallback period"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1"}}
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor search-history-length fallback-state request-time descriptor-3)]
          (is (= descriptor-1 result-descriptor))))

      (testing "no fallback for limited history"
        (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1"}}
              search-history-length 1
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor search-history-length fallback-state request-time descriptor-3)]
          (is (nil? result-descriptor)))))))

(deftest test-request-authorized?
  (let [test-cases
        (list
          {:name "request-authorized?:missing-permission-and-user"
           :input-data {:user nil, :waiter-headers {"foo" "bar"}}
           :expected false
           }
          {:name "request-authorized?:missing-permitted-but-valid-user"
           :input-data {:user "test-user", :waiter-headers {"foo" "bar"}}
           :expected false
           }
          {:name "request-authorized?:unauthorized-user"
           :input-data {:user "test-user", :waiter-headers {"permitted-user" "another-user"}}
           :expected false
           }
          {:name "request-authorized?:authorized-user-match"
           :input-data {:user "test-user", :waiter-headers {"permitted-user" "test-user"}}
           :expected true
           }
          {:name "request-authorized?:authorized-user-any"
           :input-data {:user "test-user", :waiter-headers {"permitted-user" token/ANY-USER}}
           :expected true
           }
          {:name "request-authorized?:authorized-user-any-with-missing-user"
           :input-data {:user nil, :waiter-headers {"permitted-user" token/ANY-USER}}
           :expected true
           })]
    (doseq [test-case test-cases]
      (testing (str "Test " (:name test-case))
        (is (= (:expected test-case)
               (request-authorized?
                 (get-in test-case [:input-data :user])
                 (get-in test-case [:input-data :waiter-headers "permitted-user"]))))))))

(deftest test-request->descriptor
  (let [default-search-history-length 5
        run-request->descriptor
        (fn run-request->descriptor
          [request &
           {:keys [assoc-run-as-user-approved? can-run-as? fallback-state-atom kv-store metric-group-mappings search-history-length
                   service-description-builder service-description-defaults service-id-prefix start-new-service-fn token-defaults
                   waiter-hostnames]
            :or {assoc-run-as-user-approved? (fn [_ _] false)
                 can-run-as? #(= %1 %2)
                 fallback-state-atom (atom {})
                 kv-store (kv/->LocalKeyValueStore (atom {}))
                 metric-group-mappings []
                 search-history-length default-search-history-length
                 service-description-builder (sd/create-default-service-description-builder {})
                 service-description-defaults {}
                 service-id-prefix "service-prefix-"
                 start-new-service-fn (constantly nil)
                 token-defaults {}
                 waiter-hostnames ["waiter-hostname.app.example.com"]}}]
          (request->descriptor
            assoc-run-as-user-approved? can-run-as? start-new-service-fn fallback-state-atom kv-store metric-group-mappings
            search-history-length service-description-builder service-description-defaults service-id-prefix token-defaults
            waiter-hostnames request))]

    (testing "missing user in request"
      (let [request {}
            descriptor {:service-description {}
                        :service-preauthorized false}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service")
               is))))

    (testing "not preauthorized service and different user"
      (let [request {:authorization/user "ru"}
            descriptor {:service-description {"run-as-user" "su"}
                        :service-preauthorized false}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service")
               is))))

    (testing "not permitted to run service"
      (let [request {:authorization/user "su"}
            descriptor {:service-description {"run-as-user" "su", "permitted-user" "puser"}
                        :service-preauthorized false}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"This user isn't allowed to invoke this service")
               is))))

    (testing "preauthorized service, not permitted to run service"
      (let [request {:authorization/user "ru"}
            descriptor {:service-description {"run-as-user" "su", "permitted-user" "puser"}
                        :service-preauthorized true}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"This user isn't allowed to invoke this service")
               is))))

    (testing "preauthorized service, permitted to run service-specific-user"
      (let [request {:authorization/user "ru"}
            service-id "test-service-id"
            descriptor {:service-description {"run-as-user" "su", "permitted-user" "ru"}
                        :service-id "test-service-id"
                        :service-preauthorized true}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (= {:descriptor descriptor :latest-service-id service-id})
               is))))

    (testing "authentication-disabled service, allow anonymous"
      (let [request {}
            service-id "test-service-id"
            descriptor {:service-authentication-disabled true
                        :service-id "test-service-id"
                        :service-description {"run-as-user" "su", "permitted-user" "*"}}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (= {:descriptor descriptor :latest-service-id service-id})
               is))))

    (testing "not authentication-disabled service, no anonymous access"
      (let [request {}
            descriptor {:service-authentication-disabled false
                        :service-description {"run-as-user" "su", "permitted-user" "*"}
                        :service-preauthorized false}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service")
               is))))

    (testing "not pre-authorized service, permitted to run service"
      (let [request {:authorization/user "ru"}
            service-id "test-service-id"
            descriptor {:service-description {"run-as-user" "ru", "permitted-user" "ru"}
                        :service-id "test-service-id"
                        :service-preauthorized false}]
        (with-redefs [sd/request->descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (= {:descriptor descriptor :latest-service-id service-id})
               is))))

    (let [curr-service-id (str "test-service-id-" (rand-int 100000))
          prev-service-id (str curr-service-id ".prev")
          descriptor-1a {:service-description {"permitted-user" "*"} :service-id prev-service-id :service-preauthorized true}
          descriptor-1b {:service-description {"permitted-user" "*"} :service-id prev-service-id :service-preauthorized false}
          descriptor-1c {:service-description {"permitted-user" "pu"} :service-id prev-service-id :service-preauthorized true}
          descriptor-2 {:service-description {"permitted-user" "*"} :service-id curr-service-id :service-preauthorized true}
          request-time (t/now)]

      (testing "healthy service"
        (let [retrieve-healthy-fallback-promise (promise)]
          (with-redefs [retrieve-fallback-descriptor
                        (fn [& _]
                          (throw (IllegalStateException. "Unexpected call to retrieve-fallback-descriptor")))
                        sd/request->descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{curr-service-id}
                                             :healthy-service-ids #{curr-service-id}})
                  request {:request-time request-time}
                  result (run-request->descriptor
                           request
                           :fallback-state-atom fallback-state-atom)]
              (is (= :not-called (deref retrieve-healthy-fallback-promise 0 :not-called)))
              (is (= {:descriptor descriptor-2 :latest-service-id curr-service-id} result))))))

      (testing "unhealthy service with healthy fallback"
        (let [retrieve-healthy-fallback-promise (promise)]
          (with-redefs [retrieve-fallback-descriptor
                        (fn [_ in-history-length in-fallback-state in-request-time in-descriptor]
                          (deliver retrieve-healthy-fallback-promise :called)
                          (is (= default-search-history-length in-history-length))
                          (is in-fallback-state)
                          (is (= request-time in-request-time))
                          (is (= descriptor-2 in-descriptor))
                          descriptor-1a)
                        sd/request->descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id curr-service-id}
                                             :healthy-service-ids #{prev-service-id}})
                  request {:request-time request-time}
                  result (run-request->descriptor
                           request
                           :fallback-state-atom fallback-state-atom)]
              (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))
              (is (= {:descriptor descriptor-1a :latest-service-id curr-service-id} result))))))

      (testing "unhealthy service with healthy fallback - unauthorized to run"
        (let [retrieve-healthy-fallback-promise (promise)]
          (with-redefs [retrieve-fallback-descriptor
                        (fn [_ in-history-length in-fallback-state in-request-time in-descriptor]
                          (deliver retrieve-healthy-fallback-promise :called)
                          (is (= default-search-history-length in-history-length))
                          (is in-fallback-state)
                          (is (= request-time in-request-time))
                          (is (= descriptor-2 in-descriptor))
                          descriptor-1b)
                        sd/request->descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id curr-service-id}
                                             :healthy-service-ids #{prev-service-id}})
                  request {:authorization/user "ru" :request-time request-time}]
              (->> (run-request->descriptor request :fallback-state-atom fallback-state-atom)
                   (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service")
                   is)
              (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))))))

      (testing "unhealthy service with healthy fallback - not permitted"
        (let [retrieve-healthy-fallback-promise (promise)]
          (with-redefs [retrieve-fallback-descriptor
                        (fn [_ in-history-length in-fallback-state in-request-time in-descriptor]
                          (deliver retrieve-healthy-fallback-promise :called)
                          (is (= default-search-history-length in-history-length))
                          (is in-fallback-state)
                          (is (= request-time in-request-time))
                          (is (= descriptor-2 in-descriptor))
                          descriptor-1c)
                        sd/request->descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id curr-service-id}
                                             :healthy-service-ids #{prev-service-id}})
                  request {:authorization/user "ru" :request-time request-time}]
              (->> (run-request->descriptor request :fallback-state-atom fallback-state-atom)
                   (thrown-with-msg? ExceptionInfo #"This user isn't allowed to invoke this service")
                   is)
              (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))))))

      (testing "unhealthy service with no fallback"
        (let [retrieve-healthy-fallback-promise (promise)]
          (with-redefs [retrieve-fallback-descriptor
                        (fn [_ in-history-length in-fallback-state in-request-time in-descriptor]
                          (deliver retrieve-healthy-fallback-promise :called)
                          (is (= default-search-history-length in-history-length))
                          (is in-fallback-state)
                          (is (= request-time in-request-time))
                          (is (= descriptor-2 in-descriptor))
                          nil)
                        sd/request->descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id curr-service-id}
                                             :healthy-service-ids #{}})
                  request {:request-time request-time}
                  result (run-request->descriptor
                           request
                           :fallback-state-atom fallback-state-atom)]
              (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))
              (is (= {:descriptor descriptor-2 :latest-service-id curr-service-id} result)))))))))

(deftest test-missing-run-as-user?
  (let [exception (ex-info "Test exception" {})]
    (is (not (missing-run-as-user? exception))))
  (let [exception (ex-info "Test exception" {:issue {"run-as-user" "missing-required-key"}
                                             :x-waiter-headers {}})]
    (is (not (missing-run-as-user? exception))))
  (let [exception (ex-info "Test exception" {:type :service-description-error
                                             :issue {"cmd" "missing-required-key"
                                                     "run-as-user" "missing-required-key"}
                                             :x-waiter-headers {}})]
    (is (not (missing-run-as-user? exception))))
  (let [exception (ex-info "Test exception" {:type :service-description-error
                                             :issue {"run-as-user" "invalid-length"}
                                             :x-waiter-headers {}})]
    (is (not (missing-run-as-user? exception))))
  (let [exception (ex-info "Test exception" {:type :service-description-error
                                             :issue {"run-as-user" "missing-required-key"}
                                             :x-waiter-headers {"token" "www.example.com"}})]
    (is (not (missing-run-as-user? exception))))
  (let [exception (ex-info "Test exception" {:type :service-description-error
                                             :issue {"run-as-user" "missing-required-key"}
                                             :x-waiter-headers {}})]
    (is (missing-run-as-user? exception)))
  (let [exception (ex-info "Test exception" {:type :service-description-error
                                             :issue {"run-as-user" "missing-required-key"}
                                             :x-waiter-headers {"queue-length" 100}})]
    (is (missing-run-as-user? exception))))

