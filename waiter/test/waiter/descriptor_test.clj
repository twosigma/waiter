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

(deftest test-wrap-descriptor
  (let [latest-service-id "latest-service-id"
        fallback-service-id "fallback-service-id"
        default-handler (fn [_] {:status 200})
        latest-descriptor {:service-id latest-service-id}]
    (testing "latest-service-request"
      (let [request {:request-id (str "req-" (rand-int 1000))}
            descriptor {:service-id latest-service-id}
            request->descriptor-fn (fn [in-request]
                                     (is (= request in-request))
                                     {:descriptor descriptor :latest-descriptor latest-descriptor})
            started-service-id-promise (promise)
            start-new-service-fn (fn [in-descriptor]
                                   (is (= latest-descriptor in-descriptor))
                                   (deliver started-service-id-promise (:service-id in-descriptor))
                                   (throw (UnsupportedOperationException. "Not expecting call in test")))
            fallback-state-atom (atom {:available-service-ids #{} :healthy-service-ids #{}})
            handler (wrap-descriptor default-handler request->descriptor-fn start-new-service-fn fallback-state-atom)
            response (handler request)]
        (is (= {:descriptor descriptor :latest-service-id latest-service-id :status 200} response))
        (is (= :no-service (deref started-service-id-promise 0 :no-service)))))

    (testing "fallback-with-latest-service-exists"
      (let [request {:request-id (str "req-" (rand-int 1000))}
            descriptor {:service-id fallback-service-id}
            request->descriptor-fn (fn [in-request]
                                     (is (= request in-request))
                                     {:descriptor descriptor :latest-descriptor latest-descriptor})
            started-service-id-promise (promise)
            start-new-service-fn (fn [in-descriptor]
                                   (is (= latest-descriptor in-descriptor))
                                   (deliver started-service-id-promise (:service-id in-descriptor))
                                   (throw (UnsupportedOperationException. "Not expecting call in test")))
            fallback-state-atom (atom {:available-service-ids #{latest-service-id} :healthy-service-ids #{}})
            handler (wrap-descriptor default-handler request->descriptor-fn start-new-service-fn fallback-state-atom)
            response (handler request)]
        (is (= {:descriptor descriptor :latest-service-id latest-service-id :status 200} response))
        (is (= :no-service (deref started-service-id-promise 0 :no-service)))))

    (testing "fallback-with-latest-service-does-not-exist"
      (let [request {:request-id (str "req-" (rand-int 1000))}
            descriptor {:service-id fallback-service-id}
            request->descriptor-fn (fn [in-request]
                                     (is (= request in-request))
                                     {:descriptor descriptor :latest-descriptor latest-descriptor})
            started-service-id-promise (promise)
            start-new-service-fn (fn [in-descriptor]
                                   (is (= latest-descriptor in-descriptor))
                                   (deliver started-service-id-promise (:service-id in-descriptor)))
            fallback-state-atom (atom {:available-service-ids #{} :healthy-service-ids #{}})
            handler (wrap-descriptor default-handler request->descriptor-fn start-new-service-fn fallback-state-atom)
            response (handler request)]
        (is (= {:descriptor descriptor :latest-service-id latest-service-id :status 200} response))
        (is (= latest-service-id (deref started-service-id-promise 0 :no-service)))))))

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
        fallback-period-secs 120
        search-history-length 5
        time-1 (- current-time-millis (t/in-millis (t/seconds 30)))
        descriptor-1 {:service-id "service-1"
                      :sources {:fallback-period-secs fallback-period-secs
                                :token->token-data {"test-token" {"last-update-time" time-1}}
                                :token-sequence ["test-token"]}}
        time-2 (- current-time-millis (t/in-millis (t/seconds 20)))
        descriptor-2 {:previous descriptor-1
                      :service-id "service-2"
                      :sources {:fallback-period-secs fallback-period-secs
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
                        :sources {:fallback-period-secs fallback-period-secs
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
              request-time (t/plus current-time (t/seconds (* 2 fallback-period-secs)))
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
          {:expected false
           :input-data {:user nil
                        :waiter-headers {"foo" "bar"}}
           :name "request-authorized?:missing-permission-and-user"}
          {:expected false
           :input-data {:user "test-user"
                        :waiter-headers {"foo" "bar"}}
           :name "request-authorized?:missing-permitted-but-valid-user"}
          {:expected false
           :input-data {:user "test-user"
                        :waiter-headers {"permitted-user" "another-user"}}
           :name "request-authorized?:unauthorized-user"}
          {:expected true
           :input-data {:member-of? (fn [auth-user permitted-group]
                                      (and (= auth-user "test-user") (= permitted-group "another-user")))
                        :user "test-user"
                        :waiter-headers {"permitted-user" "another-user"}}
           :name "request-authorized?:can-run-as-another-user"}
          {:expected true
           :input-data {:user "test-user"
                        :waiter-headers {"permitted-user" "test-user"}}
           :name "request-authorized?:authorized-user-match"}
          {:expected true
           :input-data {:user "test-user"
                        :waiter-headers {"permitted-user" token/ANY-USER}}
           :name "request-authorized?:authorized-user-any"}
          {:expected true
           :input-data {:user nil
                        :waiter-headers {"permitted-user" token/ANY-USER}}
           :name "request-authorized?:authorized-user-any-with-missing-user"})]
    (doseq [{:keys [expected input-data name]} test-cases]
      (testing (str "Test " name)
        (let [{:keys [user waiter-headers]} input-data
              member-of? (or (:member-of? input-data) =)]
          (is (= expected (request-authorized? member-of? user (get waiter-headers "permitted-user")))))))))

(deftest test-request->descriptor
  (let [default-search-history-length 5
        run-request->descriptor
        (fn run-request->descriptor
          [request &
           {:keys [assoc-run-as-user-approved? can-run-as? member-of? fallback-state-atom kv-store metric-group-mappings
                   search-history-length service-description-builder service-description-defaults service-id-prefix
                   token-defaults waiter-hostnames]
            :or {assoc-run-as-user-approved? (fn [_ _] false)
                 can-run-as? #(= %1 %2)
                 fallback-state-atom (atom {})
                 kv-store (kv/->LocalKeyValueStore (atom {}))
                 member-of? #(= %1 %2)
                 metric-group-mappings []
                 search-history-length default-search-history-length
                 service-description-builder (sd/create-default-service-description-builder {})
                 service-description-defaults {}
                 service-id-prefix "service-prefix-"
                 token-defaults {}
                 waiter-hostnames ["waiter-hostname.app.example.com"]}}]
          (request->descriptor
            assoc-run-as-user-approved? can-run-as? member-of? fallback-state-atom kv-store metric-group-mappings
            search-history-length service-description-builder service-description-defaults service-id-prefix token-defaults
            waiter-hostnames request))]

    (testing "missing user in request"
      (let [request {}
            descriptor {:service-description {}
                        :service-preauthorized false}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service")
               is))))

    (testing "not preauthorized service and different user"
      (let [request {:authorization/user "ru"}
            descriptor {:service-description {"run-as-user" "su"}
                        :service-preauthorized false}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service")
               is))))

    (testing "not permitted to run service"
      (let [request {:authorization/user "su"}
            descriptor {:service-description {"run-as-user" "su", "permitted-user" "puser"}
                        :service-preauthorized false}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"This user isn't allowed to invoke this service")
               is))))

    (testing "preauthorized service, not permitted to run service"
      (let [request {:authorization/user "ru"}
            descriptor {:service-description {"run-as-user" "su", "permitted-user" "puser"}
                        :service-preauthorized true}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"This user isn't allowed to invoke this service")
               is))))

    (testing "preauthorized service, permitted to run service-specific-user"
      (let [request {:authorization/user "ru"}
            descriptor {:service-description {"run-as-user" "su", "permitted-user" "ru"}
                        :service-id "test-service-id"
                        :service-preauthorized true}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (= {:descriptor descriptor :latest-descriptor descriptor})
               is))))

    (testing "authentication-disabled service, allow anonymous"
      (let [request {}
            descriptor {:service-authentication-disabled true
                        :service-id "test-service-id"
                        :service-description {"run-as-user" "su", "permitted-user" "*"}}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (= {:descriptor descriptor :latest-descriptor descriptor})
               is))))

    (testing "not authentication-disabled service, no anonymous access"
      (let [request {}
            descriptor {:service-authentication-disabled false
                        :service-description {"run-as-user" "su", "permitted-user" "*"}
                        :service-preauthorized false}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (thrown-with-msg? ExceptionInfo #"Authenticated user cannot run service")
               is))))

    (testing "not pre-authorized service, permitted to run service"
      (let [request {:authorization/user "ru"}
            descriptor {:service-description {"run-as-user" "ru", "permitted-user" "ru"}
                        :service-id "test-service-id"
                        :service-preauthorized false}]
        (with-redefs [compute-descriptor (constantly descriptor)]
          (->> (run-request->descriptor request)
               (= {:descriptor descriptor :latest-descriptor descriptor})
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
                        compute-descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{curr-service-id}
                                             :healthy-service-ids #{curr-service-id}})
                  request {:request-time request-time}
                  result (run-request->descriptor
                           request
                           :fallback-state-atom fallback-state-atom)]
              (is (= :not-called (deref retrieve-healthy-fallback-promise 0 :not-called)))
              (is (= {:descriptor descriptor-2 :latest-descriptor descriptor-2} result))))))

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
                        compute-descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id curr-service-id}
                                             :healthy-service-ids #{prev-service-id}})
                  request {:request-time request-time}
                  result (run-request->descriptor
                           request
                           :fallback-state-atom fallback-state-atom)]
              (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))
              (is (= {:descriptor descriptor-1a :latest-descriptor descriptor-2} result))))))

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
                        compute-descriptor (constantly descriptor-2)]
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
                        compute-descriptor (constantly descriptor-2)]
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
                        compute-descriptor (constantly descriptor-2)]
            (let [fallback-state-atom (atom {:available-service-ids #{prev-service-id curr-service-id}
                                             :healthy-service-ids #{}})
                  request {:request-time request-time}
                  result (run-request->descriptor
                           request
                           :fallback-state-atom fallback-state-atom)]
              (is (= :called (deref retrieve-healthy-fallback-promise 0 :not-called)))
              (is (= {:descriptor descriptor-2 :latest-descriptor descriptor-2} result)))))))))

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

(deftest test-descriptor->previous-descriptor
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id-prefix "service-prefix-"
        token-defaults {"fallback-period-secs" 300}
        username "test-user"
        metric-group-mappings []
        constraints {"cpus" {:max 100} "mem" {:max 1024}}
        builder (sd/create-default-service-description-builder {:constraints constraints})
        assoc-run-as-user-approved? (constantly false)]

    (testing "no token"
      (let [sources {:defaults {"permitted-user" "*"}
                     :headers {}
                     :service-description-template {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}
                     :token->token-data {}
                     :token-authentication-disabled false
                     :token-preauthorized false
                     :token-sequence []}
            passthrough-headers {}
            waiter-headers {}]
        (is (nil? (descriptor->previous-descriptor
                    kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                    {:passthrough-headers passthrough-headers
                     :sources sources
                     :waiter-headers waiter-headers})))))

    (testing "single token without previous"
      (let [service-description-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}
            sources {:defaults {"permitted-user" "*"}
                     :headers {}
                     :service-description-template service-description-1
                     :token->token-data {"token-1" {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}}
                     :token-authentication-disabled false
                     :token-preauthorized false
                     :token-sequence ["token-1"]}
            passthrough-headers {}
            waiter-headers {}]
        (is (nil? (descriptor->previous-descriptor
                    kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                    {:passthrough-headers passthrough-headers
                     :sources sources
                     :waiter-headers waiter-headers})))))

    (testing "single token with previous"
      (let [test-token "test-token"
            token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo1"}
            service-description-1 (assoc token-data-1 "source-tokens" [(sd/source-tokens-entry test-token token-data-1)])
            token-data-2 {"cmd" "ls" "cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" "ru" "version" "foo2"}
            service-description-2 (assoc token-data-2 "source-tokens" [(sd/source-tokens-entry test-token token-data-2)])
            sources {:defaults {"metric-group" "other" "permitted-user" "*"}
                     :headers {}
                     :service-description-template service-description-2
                     :token->token-data {test-token token-data-2}
                     :token-authentication-disabled false
                     :token-preauthorized false
                     :token-sequence [test-token]}
            passthrough-headers {}
            waiter-headers {}
            previous-descriptor (descriptor->previous-descriptor
                                  kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                                  {:passthrough-headers passthrough-headers
                                   :sources sources
                                   :waiter-headers waiter-headers})]
        (is (= {:core-service-description service-description-1
                :on-the-fly? nil
                :passthrough-headers passthrough-headers
                :service-authentication-disabled false
                :service-description (merge (:defaults sources) service-description-1)
                :service-id (sd/service-description->service-id service-id-prefix service-description-1)
                :service-preauthorized false
                :sources (assoc sources
                           :fallback-period-secs 300
                           :service-description-template service-description-1
                           :token->token-data {test-token token-data-1})
                :waiter-headers waiter-headers}
               previous-descriptor))
        (is (nil? (descriptor->previous-descriptor
                    kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                    previous-descriptor)))))

    (testing "single on-the-fly+token with previous"
      (let [test-token "test-token"
            token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru1" "version" "foo1"}
            service-description-1 (assoc token-data-1 "source-tokens" [(sd/source-tokens-entry test-token token-data-1)])
            token-data-2 {"cmd" "ls" "cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" "ru2" "version" "foo2"}
            service-description-2 (assoc token-data-1 "source-tokens" [(sd/source-tokens-entry test-token token-data-2)])
            sources {:defaults {"metric-group" "other" "permitted-user" "*"}
                     :headers {"cpus" 20}
                     :on-the-fly? nil ;; invalid value to check if it is ignored and generated in the fallback
                     :service-description-template service-description-2
                     :token->token-data {test-token token-data-2}
                     :token-authentication-disabled false
                     :token-preauthorized false
                     :token-sequence [test-token]}
            passthrough-headers {}
            waiter-headers {"x-waiter-cpus" 20}
            previous-descriptor (descriptor->previous-descriptor
                                  kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                                  {:passthrough-headers passthrough-headers
                                   :sources sources
                                   :waiter-headers waiter-headers})]
        (let [expected-core-service-description (assoc service-description-1 "cpus" 20 "permitted-user" username "run-as-user" username)]
          (is (= {:core-service-description expected-core-service-description
                  :on-the-fly? true
                  :passthrough-headers passthrough-headers
                  :service-authentication-disabled false
                  :service-description (merge (:defaults sources) expected-core-service-description)
                  :service-id (sd/service-description->service-id service-id-prefix expected-core-service-description)
                  :service-preauthorized false
                  :sources (assoc sources
                             :fallback-period-secs 300
                             :service-description-template service-description-1
                             :token->token-data {test-token token-data-1})
                  :waiter-headers waiter-headers}
                 previous-descriptor)))))

    (testing "multiple tokens without previous"
      (let [test-token "test-token"
            service-description-1 {"cmd" "ls" "cpus" 1 "mem" 32}
            service-description-2 {"run-as-user" "ru" "version" "foo"}
            sources {:defaults {"permitted-user" "*"}
                     :headers {}
                     :service-description-template (merge service-description-1 service-description-2)
                     :token->token-data {test-token service-description-1
                                         "token-2" service-description-2}
                     :token-authentication-disabled false
                     :token-preauthorized false
                     :token-sequence [test-token "token-2"]}
            passthrough-headers {}
            waiter-headers {}]
        (is (nil? (descriptor->previous-descriptor
                    kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                    {:passthrough-headers passthrough-headers
                     :sources sources
                     :waiter-headers waiter-headers})))))

    (testing "multiple tokens with previous"
      (let [test-token-1 "test-token-1"
            token-data-1p {"cmd" "lsp" "cpus" 1 "last-update-time" 1000 "mem" 32}
            service-description-1p (assoc token-data-1p "source-tokens" [(sd/source-tokens-entry test-token-1 token-data-1p)])
            token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "previous" token-data-1p}
            service-description-1 (assoc token-data-1 "source-tokens" (sd/source-tokens-entry test-token-1 token-data-1))
            test-token-2 "test-token-2"
            token-data-2p {"last-update-time" 2000 "run-as-user" "rup" "version" "foo"}
            service-description-2p (assoc token-data-2p "source-tokens" (sd/source-tokens-entry test-token-2 token-data-2p))
            token-data-2 {"previous" token-data-2p "run-as-user" "ru" "version" "foo"}
            sources {:defaults {"metric-group" "other" "permitted-user" "*"}
                     :headers {}
                     :service-description-template (-> (merge service-description-1 service-description-2p)
                                                       (assoc "source-tokens"
                                                              [(sd/source-tokens-entry test-token-1 token-data-1)
                                                               (sd/source-tokens-entry test-token-2 token-data-2)]))
                     :token->token-data {test-token-1 token-data-1
                                         test-token-2 token-data-2}
                     :token-authentication-disabled false
                     :token-preauthorized false
                     :token-sequence [test-token-1 test-token-2]}
            passthrough-headers {}
            waiter-headers {}
            previous-descriptor (descriptor->previous-descriptor
                                  kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                                  {:passthrough-headers passthrough-headers
                                   :sources sources
                                   :waiter-headers waiter-headers})]
        (let [expected-core-service-description (-> (merge service-description-1 service-description-2p)
                                                    (select-keys sd/service-parameter-keys)
                                                    (assoc "source-tokens"
                                                           [(sd/source-tokens-entry test-token-1 token-data-1)
                                                            (sd/source-tokens-entry test-token-2 token-data-2p)]))]
          (is (= {:core-service-description expected-core-service-description
                  :on-the-fly? nil
                  :passthrough-headers passthrough-headers
                  :service-authentication-disabled false
                  :service-description (merge (:defaults sources) expected-core-service-description)
                  :service-id (sd/service-description->service-id service-id-prefix expected-core-service-description)
                  :service-preauthorized false
                  :sources (-> sources
                               (assoc :fallback-period-secs 300
                                      :service-description-template expected-core-service-description
                                      :token->token-data {test-token-1 token-data-1
                                                          test-token-2 token-data-2p}))
                  :waiter-headers waiter-headers}
                 previous-descriptor)))
        (let [prev-descriptor-2 (descriptor->previous-descriptor
                                  kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                                  previous-descriptor)]
          (let [expected-core-service-description (-> (merge service-description-1p service-description-2p)
                                                      (select-keys sd/service-parameter-keys)
                                                      (assoc "source-tokens"
                                                             [(sd/source-tokens-entry test-token-1 token-data-1p)
                                                              (sd/source-tokens-entry test-token-2 token-data-2p)]))]
            (is (= {:core-service-description expected-core-service-description
                    :on-the-fly? nil
                    :passthrough-headers passthrough-headers
                    :service-authentication-disabled false
                    :service-description (merge (:defaults sources) expected-core-service-description)
                    :service-id (sd/service-description->service-id service-id-prefix expected-core-service-description)
                    :service-preauthorized false
                    :sources (assoc sources
                               :fallback-period-secs 300
                               :service-description-template expected-core-service-description
                               :token->token-data {test-token-1 token-data-1p
                                                   test-token-2 token-data-2p})
                    :waiter-headers waiter-headers}
                   (dissoc prev-descriptor-2 :retrieve-fallback-service-description))))
          (is (nil? (descriptor->previous-descriptor
                      kv-store service-id-prefix token-defaults metric-group-mappings builder assoc-run-as-user-approved? username
                      prev-descriptor-2))))))))
