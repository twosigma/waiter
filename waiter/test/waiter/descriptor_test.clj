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
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [full.async :refer [<??]]
            [plumbing.core :as pc]
            [waiter.descriptor :refer :all]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.token :as token]
            [waiter.util.async-utils :as au])
  (:import (clojure.lang ExceptionInfo)
           (org.joda.time DateTime)))

(deftest test-wrap-descriptor
  (let [latest-service-id "latest-service-id"
        fallback-service-id "fallback-service-id"
        default-handler (fn [_] {:status http-200-ok})
        auth-user "test-auth-user"
        latest-descriptor {:service-id latest-service-id}
        make-service-invocation-authorized? (fn [auth-result]
                                              (fn [in-auth-user in-descriptor]
                                                (is (= auth-user in-auth-user))
                                                (is (= latest-descriptor in-descriptor))
                                                auth-result))]
    (testing "latest-service-request"
      (let [request {:authorization/user auth-user
                     :request-id (str "req-" (rand-int 1000))}
            descriptor {:service-id latest-service-id}
            request->descriptor-fn (fn [in-request]
                                     (is (= request in-request))
                                     {:descriptor descriptor :latest-descriptor latest-descriptor})
            started-service-id-promise (promise)
            start-new-service-fn (fn [in-descriptor]
                                   (is (= latest-descriptor in-descriptor))
                                   (deliver started-service-id-promise (:service-id in-descriptor))
                                   (throw (UnsupportedOperationException. "Not expecting call in test")))
            service-invocation-authorized? (make-service-invocation-authorized? true)
            fallback-state-atom (atom {:available-service-ids #{} :healthy-service-ids #{}})
            handler (wrap-descriptor default-handler request->descriptor-fn service-invocation-authorized? start-new-service-fn fallback-state-atom)
            response (handler request)]
        (is (= {:descriptor descriptor :latest-service-id latest-service-id :status http-200-ok} response))
        (is (= :no-service (deref started-service-id-promise 0 :no-service)))))

    (testing "fallback-with-latest-service-exists"
      (let [request {:authorization/user auth-user
                     :request-id (str "req-" (rand-int 1000))}
            descriptor {:service-id fallback-service-id}
            request->descriptor-fn (fn [in-request]
                                     (is (= request in-request))
                                     {:descriptor descriptor :latest-descriptor latest-descriptor})
            started-service-id-promise (promise)
            start-new-service-fn (fn [in-descriptor]
                                   (is (= latest-descriptor in-descriptor))
                                   (deliver started-service-id-promise (:service-id in-descriptor))
                                   (throw (UnsupportedOperationException. "Not expecting call in test")))
            service-invocation-authorized? (make-service-invocation-authorized? true)
            fallback-state-atom (atom {:available-service-ids #{latest-service-id} :healthy-service-ids #{}})
            handler (wrap-descriptor default-handler request->descriptor-fn service-invocation-authorized? start-new-service-fn fallback-state-atom)
            response (handler request)]
        (is (= {:descriptor descriptor :latest-service-id latest-service-id :status http-200-ok} response))
        (is (= :no-service (deref started-service-id-promise 0 :no-service)))))

    (testing "fallback-with-latest-service-does-not-exist"
      (let [request {:authorization/user auth-user
                     :request-id (str "req-" (rand-int 1000))}
            descriptor {:service-id fallback-service-id}
            request->descriptor-fn (fn [in-request]
                                     (is (= request in-request))
                                     {:descriptor descriptor :latest-descriptor latest-descriptor})]

        (testing "and request authorized"
          (let [started-service-id-promise (promise)
                start-new-service-fn (fn [in-descriptor]
                                       (is (= latest-descriptor in-descriptor))
                                       (deliver started-service-id-promise (:service-id in-descriptor)))
                service-invocation-authorized? (make-service-invocation-authorized? true)
                fallback-state-atom (atom {:available-service-ids #{} :healthy-service-ids #{}})
                handler (wrap-descriptor default-handler request->descriptor-fn service-invocation-authorized? start-new-service-fn fallback-state-atom)
                response (handler request)]
            (is (= {:descriptor descriptor :latest-service-id latest-service-id :status http-200-ok} response))
            (is (= latest-service-id (deref started-service-id-promise 0 :no-service)))))

        (testing "and request not authorized"
          (let [started-service-id-promise (promise)
                start-new-service-fn (fn [in-descriptor]
                                       (is (= latest-descriptor in-descriptor))
                                       (deliver started-service-id-promise (:service-id in-descriptor)))
                service-invocation-authorized? (make-service-invocation-authorized? false)
                fallback-state-atom (atom {:available-service-ids #{} :healthy-service-ids #{}})
                handler (wrap-descriptor default-handler request->descriptor-fn service-invocation-authorized? start-new-service-fn fallback-state-atom)
                response (handler request)]
            (is (= {:descriptor descriptor :latest-service-id latest-service-id :status http-200-ok} response))
            (is (= :no-service (deref started-service-id-promise 0 :no-service)))))))))

(deftest test-fallback-maintainer
  (let [current-healthy-service-ids #{"service-1" "service-3"}
        current-available-service-ids (set/union current-healthy-service-ids #{"service-5" "service-7"})
        fallback-state-atom (-> {:available-service-ids current-available-service-ids
                                 :healthy-service-ids current-healthy-service-ids}
                                atom)
        new-healthy-service-ids (set/union current-available-service-ids #{"service-2" "service-4"})
        new-available-service-ids (set/union new-healthy-service-ids #{"service-6" "service-8"})
        scheduler-messages {:all-available-service-ids new-available-service-ids
                            :service-id->healthy-instances (-> (pc/map-from-keys
                                                                 (fn [service-id]
                                                                   [{:id (str service-id ".instance-1")
                                                                     :service-id service-id}])
                                                                 new-healthy-service-ids)
                                                               (assoc "service-9" []
                                                                      "service-10" nil))}
        router-state-chan (au/latest-chan)
        {:keys [exit-chan query-chan]} (fallback-maintainer router-state-chan fallback-state-atom)]

    (async/>!! router-state-chan scheduler-messages)
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
        descriptor->previous-descriptor-fn (fn [descriptor] (descriptor->previous-descriptor nil nil descriptor))
        fallback-period-secs 120
        search-history-length 5
        time-1 (- current-time-millis (t/in-millis (t/seconds 30)))
        descriptor-1 {:component->previous-descriptor-fns {:token {:retrieve-last-update-time (constantly time-1)
                                                                   :retrieve-previous-descriptor (constantly nil)}}
                      :service-id "service-1"
                      :sources {:fallback-period-secs fallback-period-secs}}
        time-2 (- current-time-millis (t/in-millis (t/seconds 20)))
        descriptor-2 {:component->previous-descriptor-fns {:token {:retrieve-last-update-time (constantly time-2)
                                                                   :retrieve-previous-descriptor (constantly descriptor-1)}}
                      :previous descriptor-1
                      :service-id "service-2"
                      :sources {:fallback-period-secs fallback-period-secs}}
        request-time current-time]

    (with-redefs [sd/validate-service-description (constantly nil)
                  t/now (constantly current-time)]

      (testing "no fallback service for on-the-fly"
        (let [descriptor-4 {:component->previous-descriptor-fns {}
                            :previous descriptor-2
                            :service-id "service-4"
                            :sources {:token->token-data {} :token-sequence []}}
              fallback-state {:available-service-ids #{"service-1" "service-2"}
                              :healthy-service-ids #{"service-1" "service-2"}}
              result-descriptor (retrieve-fallback-descriptor
                                  descriptor->previous-descriptor-fn search-history-length fallback-state request-time descriptor-4)]
          (is (nil? result-descriptor))))

      (let [time-3 (- current-time-millis (t/in-millis (t/seconds 10)))
            descriptor-3 {:component->previous-descriptor-fns {:token {:retrieve-last-update-time (constantly time-3)
                                                                       :retrieve-previous-descriptor (constantly descriptor-2)}}
                          :previous descriptor-2
                          :service-id "service-3"
                          :sources {:fallback-period-secs fallback-period-secs
                                    :token->token-data {"test-token" {"last-update-time" time-3}}
                                    :token-sequence ["test-token"]}}]

        (testing "fallback to previous healthy instance inside fallback period"
          (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                                :healthy-service-ids #{"service-1" "service-2"}}
                result-descriptor (retrieve-fallback-descriptor
                                    descriptor->previous-descriptor-fn search-history-length fallback-state request-time descriptor-3)]
            (is (= descriptor-2 result-descriptor))))

        (testing "no healthy fallback service"
          (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                                :healthy-service-ids #{}}
                result-descriptor (retrieve-fallback-descriptor
                                    descriptor->previous-descriptor-fn search-history-length fallback-state request-time descriptor-3)]
            (is (nil? result-descriptor))))

        (testing "no fallback service outside period"
          (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                                :healthy-service-ids #{"service-1" "service-2"}}
                request-time (t/plus current-time (t/seconds (* 2 fallback-period-secs)))
                result-descriptor (retrieve-fallback-descriptor
                                    descriptor->previous-descriptor-fn search-history-length fallback-state request-time descriptor-3)]
            (is (nil? result-descriptor))))

        (testing "fallback to 2-level previous healthy instance inside fallback period"
          (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                                :healthy-service-ids #{"service-1"}}
                result-descriptor (retrieve-fallback-descriptor
                                    descriptor->previous-descriptor-fn search-history-length fallback-state request-time descriptor-3)]
            (is (= descriptor-1 result-descriptor))))

        (testing "no fallback for limited history"
          (let [fallback-state {:available-service-ids #{"service-1" "service-2"}
                                :healthy-service-ids #{"service-1"}}
                search-history-length 1
                result-descriptor (retrieve-fallback-descriptor
                                    descriptor->previous-descriptor-fn search-history-length fallback-state request-time descriptor-3)]
            (is (nil? result-descriptor))))))))

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
           {:keys [assoc-run-as-user-approved? can-run-as? fallback-state-atom kv-store metric-group-mappings profile->defaults
                   search-history-length service-description-builder service-description-defaults service-id-prefix token-defaults
                   waiter-hostnames]
            :or {assoc-run-as-user-approved? (fn [_ _] false)
                 can-run-as? #(= %1 %2)
                 fallback-state-atom (atom {})
                 kv-store (kv/->LocalKeyValueStore (atom {}))
                 metric-group-mappings []
                 profile->defaults {"webapp" {"concurrency-level" 120
                                              "fallback-period-secs" 100}
                                    "service" {"concurrency-level" 30
                                               "fallback-period-secs" 90
                                               "permitted-user" "*"}}
                 search-history-length default-search-history-length
                 service-description-defaults {}
                 service-id-prefix "service-prefix-"
                 token-defaults {}
                 waiter-hostnames ["waiter-hostname.app.example.com"]}}]
          (let [service-description-builder (or service-description-builder
                                                (sd/create-default-service-description-builder
                                                  {:metric-group-mappings metric-group-mappings
                                                   :profile->defaults profile->defaults
                                                   :service-description-defaults service-description-defaults}))
                attach-service-defaults-fn #(sd/merge-defaults % service-description-defaults profile->defaults metric-group-mappings)
                attach-token-defaults-fn #(sd/attach-token-defaults % token-defaults profile->defaults)]
            (request->descriptor
              assoc-run-as-user-approved? can-run-as? attach-service-defaults-fn attach-token-defaults-fn fallback-state-atom
              kv-store search-history-length service-description-builder service-id-prefix waiter-hostnames request)))]

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

(let [kv-store (kv/->LocalKeyValueStore (atom {}))
      service-id-prefix "service-prefix-"
      profile->defaults {"webapp" {"concurrency-level" 120
                                   "fallback-period-secs" 100}
                         "service" {"concurrency-level" 30
                                    "fallback-period-secs" 90
                                    "permitted-user" "*"}}
      service-description-defaults {}
      token-defaults {"fallback-period-secs" 300}
      metric-group-mappings []
      profile->defaults {"webapp" {"concurrency-level" 120
                                   "fallback-period-secs" 100}}
      attach-service-defaults-fn #(sd/merge-defaults % service-description-defaults profile->defaults metric-group-mappings)
      attach-token-defaults-fn #(sd/attach-token-defaults % token-defaults profile->defaults)
      username "test-user"
      metric-group-mappings []
      service-description-defaults {"metric-group" "other" "permitted-user" "*"}
      constraints {"cpus" {:max 100} "mem" {:max 1024}}
      builder (sd/create-default-service-description-builder {:constraints constraints
                                                              :metric-group-mappings metric-group-mappings
                                                              :profile->defaults profile->defaults
                                                              :service-description-defaults service-description-defaults})
      assoc-run-as-user-approved? (constantly false)
      build-service-description-and-id-helper (sd/make-build-service-description-and-id-helper
                                                kv-store service-id-prefix username builder assoc-run-as-user-approved?)]

  (deftest test-descriptor->previous-descriptor-no-token
    (let [sources {:headers {}
                   :service-description-template {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}
                   :token->token-data {}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence []}
          passthrough-headers {}
          waiter-headers {}]
      (is (nil? (descriptor->previous-descriptor
                  kv-store builder
                  {:passthrough-headers passthrough-headers
                   :sources sources
                   :waiter-headers waiter-headers})))))

  (deftest test-descriptor->previous-descriptor-single-token-without-previous
    (let [service-description-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}
          sources {:headers {}
                   :service-description-template service-description-1
                   :token->token-data {"token-1" {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence ["token-1"]}
          passthrough-headers {}
          waiter-headers {}]
      (is (nil? (descriptor->previous-descriptor
                  kv-store builder
                  (-> {:passthrough-headers passthrough-headers
                       :sources sources
                       :waiter-headers waiter-headers}
                    (attach-token-fallback-source
                      attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper)))))))

  (deftest test-descriptor->previous-descriptor-multiple-sources
    (let [service-description-1 {"cmd" "ls1" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}
          create-retrieve-last-update-time (fn [component]
                                             (fn [descriptor]
                                               (get-in descriptor [:sources component "last-update-time"] 0)))
          create-retrieve-previous-descriptor (fn [component]
                                                (fn [descriptor]
                                                  (when-let [template (get-in descriptor [:sources component])]
                                                    (let [template-basic (dissoc template "last-update-time" "previous")]
                                                      (-> descriptor
                                                        (update :core-service-description merge template-basic)
                                                        (update :reference-type->entry
                                                                (fn [reference]
                                                                  (cond-> (dissoc reference component)
                                                                    (get-in template ["previous" "last-update-time"])
                                                                    (assoc component {:version (str "v" (get-in template ["previous" "last-update-time"]))}))))
                                                        (update :service-description merge template-basic)
                                                        (assoc-in [:sources component] (get template "previous")))))))
          sources {:cmd-source {"cmd" "ls2"
                                "last-update-time" 8
                                "previous" {"cmd" "ls3"
                                            "last-update-time" 6}}
                   :cpu-source {"cpus" 2
                                "last-update-time" 10
                                "previous" {"cpus" 3
                                            "last-update-time" 4}}}
          passthrough-headers {}
          waiter-headers {}
          curr-descriptor {:component->previous-descriptor-fns (pc/map-from-keys
                                                                 (fn [component]
                                                                   {:retrieve-last-update-time (create-retrieve-last-update-time component)
                                                                    :retrieve-previous-descriptor (create-retrieve-previous-descriptor component)})
                                                                 [:cmd-source :cpu-source])
                           :core-service-description service-description-1
                           :passthrough-headers passthrough-headers
                           :reference-type->entry {:cmd-source {:version "v8"} :cpu-source {:version "v10"}}
                           :service-description service-description-1
                           :sources sources
                           :waiter-headers waiter-headers}
          compute-most-recently-updated-component-fn
          (fn compute-most-recently-updated-component-fn [descriptor]
            (let [[component {:keys [retrieve-last-update-time]}] (retrieve-most-recently-updated-component-entry descriptor)]
              {:component component
               :last-update-time (retrieve-last-update-time descriptor)}))]
      (let [prev-descriptor-1 (descriptor->previous-descriptor kv-store builder curr-descriptor)
            template-1 (get-in curr-descriptor [:sources :cpu-source])
            template-basic-1 (dissoc template-1 "last-update-time" "previous")]
        (is (= {:component :cpu-source, :last-update-time 10}
               (compute-most-recently-updated-component-fn curr-descriptor)))
        (is (= (-> curr-descriptor
                 (update :core-service-description merge template-basic-1)
                 (assoc :reference-type->entry {:cmd-source {:version "v8"} :cpu-source {:version "v4"}})
                 (update :service-description merge template-basic-1)
                 (assoc-in [:sources :cpu-source] (get template-1 "previous")))
               prev-descriptor-1))

        (let [prev-descriptor-2 (descriptor->previous-descriptor kv-store builder prev-descriptor-1)
              template-2 (get-in curr-descriptor [:sources :cmd-source])
              template-basic-2 (dissoc template-2 "last-update-time" "previous")]
          (is (= {:component :cmd-source, :last-update-time 8}
                 (compute-most-recently-updated-component-fn prev-descriptor-1)))
          (is (= (-> prev-descriptor-1
                   (update :core-service-description merge template-basic-2)
                   (assoc :reference-type->entry {:cmd-source {:version "v6"} :cpu-source {:version "v4"}})
                   (update :service-description merge template-basic-2)
                   (assoc-in [:sources :cmd-source] (get template-2 "previous")))
                 prev-descriptor-2))

          (let [prev-descriptor-3 (descriptor->previous-descriptor kv-store builder prev-descriptor-2)
                template-3 (get-in curr-descriptor [:sources :cmd-source "previous"])
                template-basic-3 (dissoc template-3 "last-update-time" "previous")]
            (is (= {:component :cmd-source, :last-update-time 6}
                   (compute-most-recently-updated-component-fn prev-descriptor-2)))
            (is (= (-> prev-descriptor-3
                     (update :core-service-description merge template-basic-3)
                     (assoc-in [:reference-type->entry :cpu-source :version] "v4")
                     (update :service-description merge template-basic-3)
                     (assoc-in [:sources :cmd-source] (get template-3 "previous")))
                   prev-descriptor-3))

            (let [prev-descriptor-4 (descriptor->previous-descriptor kv-store builder prev-descriptor-3)
                  template-4 (get-in curr-descriptor [:sources :cpu-source "previous"])
                  template-basic-4 (dissoc template-4 "last-update-time" "previous")]
              (is (= {:component :cpu-source, :last-update-time 4}
                     (compute-most-recently-updated-component-fn prev-descriptor-3)))
              (is (= (-> prev-descriptor-3
                       (update :core-service-description merge template-basic-4)
                       (assoc :reference-type->entry {})
                       (update :service-description merge template-basic-4)
                       (assoc-in [:sources :cpu-source] (get template-4 "previous")))
                     prev-descriptor-4))

              (is (nil? (descriptor->previous-descriptor kv-store builder prev-descriptor-4)))))))))

  (deftest test-descriptor->previous-descriptor-multiple-sources-with-invalid-descriptions
    (let [service-description-1 {"cmd" "ls1" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo"}
          create-retrieve-last-update-time (fn [component]
                                             (fn [descriptor]
                                               (get-in descriptor [:sources component "last-update-time"] 0)))
          create-retrieve-previous-descriptor (fn [component]
                                                (fn [descriptor]
                                                  (when-let [template (get-in descriptor [:sources component])]
                                                    (let [template-basic (dissoc template "last-update-time" "previous")]
                                                      (-> descriptor
                                                        (update :core-service-description merge template-basic)
                                                        (update :service-description merge template-basic)
                                                        (assoc-in [:sources component] (get template "previous")))))))
          sources {:cmd-source {"cmd" :invalid
                                "last-update-time" 8
                                "previous" {"cmd" :invalid
                                            "last-update-time" 6}}
                   :cpu-source {"cpus" :invalid
                                "last-update-time" 10
                                "previous" {"cmd" "ls3"
                                            "cpus" 3
                                            "last-update-time" 4}}}
          passthrough-headers {}
          waiter-headers {}
          curr-descriptor {:component->previous-descriptor-fns (pc/map-from-keys
                                                                 (fn [component]
                                                                   {:retrieve-last-update-time (create-retrieve-last-update-time component)
                                                                    :retrieve-previous-descriptor (create-retrieve-previous-descriptor component)})
                                                                 [:cmd-source :cpu-source])
                           :core-service-description service-description-1
                           :passthrough-headers passthrough-headers
                           :service-description service-description-1
                           :sources sources
                           :waiter-headers waiter-headers}]
      (let [prev-descriptor (descriptor->previous-descriptor kv-store builder curr-descriptor)
            template-1 {"cmd" "ls3"
                        "cpus" 3}]
        (is (= (-> curr-descriptor
                 (update :core-service-description merge template-1)
                 (update :service-description merge template-1)
                 (assoc-in [:sources :cmd-source] nil)
                 (assoc-in [:sources :cpu-source] nil))
               prev-descriptor))
        (is (nil? (descriptor->previous-descriptor kv-store builder prev-descriptor))))))

  (defn reference-tokens-entry
    "Creates an entry for the source-tokens field"
    [token token-data]
    (walk/keywordize-keys
      (sd/source-tokens-entry token token-data)))

  (deftest test-descriptor->previous-descriptor-single-token-with-previous
    (let [test-token "test-token"
          token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo1"}
          service-description-1 token-data-1
          token-data-2 {"cmd" "ls" "cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" "ru" "version" "foo2"}
          service-description-2 token-data-2
          sources {:headers {}
                   :service-description-defaults {"metric-group" "other" "permitted-user" "*"}
                   :service-description-template service-description-2
                   :token->token-data {test-token token-data-2}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token]}
          passthrough-headers {}
          waiter-headers {}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-2)]}}
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))
          previous-descriptor (descriptor->previous-descriptor kv-store builder current-descriptor)]
      (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
              :core-service-description service-description-1
              :on-the-fly? nil
              :passthrough-headers passthrough-headers
              :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-1)]}}
              :service-authentication-disabled false
              :service-description (merge service-description-defaults service-description-1)
              :service-id (sd/service-description->service-id service-id-prefix service-description-1)
              :service-preauthorized false
              :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
              :sources (assoc sources
                         :fallback-period-secs 300
                         :service-description-template service-description-1
                         :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
                         :token->token-data {test-token token-data-1})
              :waiter-headers waiter-headers}
             previous-descriptor))
      (is (nil? (descriptor->previous-descriptor kv-store builder previous-descriptor)))))

  (deftest test-descriptor->previous-descriptor-single-token-with-profile-in-previous
    (let [test-token "test-token"
          token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "profile" "webapp" "run-as-user" "ru" "version" "foo1"}
          service-description-1 token-data-1
          token-data-2 {"cmd" "ls" "cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" "ru" "version" "foo2"}
          service-description-2 token-data-2
          sources {:headers {}
                   :service-description-template service-description-2
                   :token->token-data {test-token token-data-2}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token]}
          passthrough-headers {}
          waiter-headers {}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-2)]}}
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))
          previous-descriptor (descriptor->previous-descriptor kv-store builder current-descriptor)]
      (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
              :core-service-description service-description-1
              :on-the-fly? nil
              :passthrough-headers passthrough-headers
              :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-1)]}}
              :service-authentication-disabled false
              :service-description (merge (sd/compute-service-defaults
                                            service-description-defaults profile->defaults "webapp")
                                          service-description-1)
              :service-id (sd/service-description->service-id service-id-prefix service-description-1)
              :service-preauthorized false
              :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
              :sources (assoc sources
                         :fallback-period-secs 100
                         :service-description-template service-description-1
                         :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
                         :token->token-data {test-token token-data-1})
              :waiter-headers waiter-headers}
             previous-descriptor))
      (is (nil? (descriptor->previous-descriptor kv-store builder previous-descriptor)))))

  (deftest test-descriptor->previous-descriptor-token-with-invalid-intervening-previous
    (let [test-token "test-token"
          token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo1"}
          service-description-1 token-data-1
          token-data-2 {"cmd" 1234 "cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" 9876 "version" "foo2"}
          token-data-3 {"cmd" "ls" "cpus" 2 "mem" 64 "previous" token-data-2 "run-as-user" "ru" "version" "foo2"}
          service-description-3 token-data-3
          sources {:headers {}
                   :service-description-template service-description-3
                   :token->token-data {test-token token-data-3}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token]}
          passthrough-headers {}
          waiter-headers {}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-3)]}}
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))
          previous-descriptor (descriptor->previous-descriptor kv-store builder current-descriptor)]
      (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
              :core-service-description service-description-1
              :on-the-fly? nil
              :passthrough-headers passthrough-headers
              :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-1)]}}
              :service-authentication-disabled false
              :service-description (merge service-description-defaults service-description-1)
              :service-id (sd/service-description->service-id service-id-prefix service-description-1)
              :service-preauthorized false
              :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
              :sources (assoc sources
                         :fallback-period-secs 300
                         :service-description-template service-description-1
                         :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
                         :token->token-data {test-token token-data-1})
              :waiter-headers waiter-headers}
             previous-descriptor))
      (is (nil? (descriptor->previous-descriptor kv-store builder previous-descriptor)))))

  (deftest test-descriptor->previous-descriptor-token-with-exception-on-intervening-previous
    (let [test-token "test-token"
          token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo1"}
          service-description-1 token-data-1
          token-data-2 {"cmd" 1234 "cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" 9876 "version" "foo2"}
          token-data-3 {"cmd" "ls" "cpus" 2 "mem" 64 "previous" token-data-2 "run-as-user" "ru" "version" "foo2"}
          service-description-3 token-data-3
          sources {:headers {}
                   :service-description-template service-description-3
                   :token->token-data {test-token token-data-3}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token]}
          passthrough-headers {}
          waiter-headers {}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-3)]}}
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))
          current-service-description->service-id sd/service-description->service-id
          exception-thrown-promise (promise)
          previous-descriptor (with-redefs [sd/service-description->service-id
                                            (fn [service-prefix {:strs [cmd] :as service-description}]
                                              (if (integer? cmd)
                                                (do
                                                  (deliver exception-thrown-promise true)
                                                  (throw (ex-info "Invalid command" service-description)))
                                                (current-service-description->service-id service-prefix service-description)))]
                                (descriptor->previous-descriptor kv-store builder current-descriptor))]
      (is (realized? exception-thrown-promise))
      (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
              :core-service-description service-description-1
              :on-the-fly? nil
              :passthrough-headers passthrough-headers
              :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-1)]}}
              :service-authentication-disabled false
              :service-description (merge service-description-defaults service-description-1)
              :service-id (sd/service-description->service-id service-id-prefix service-description-1)
              :service-preauthorized false
              :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
              :sources (assoc sources
                         :fallback-period-secs 300
                         :service-description-template service-description-1
                         :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
                         :token->token-data {test-token token-data-1})
              :waiter-headers waiter-headers}
             previous-descriptor))
      (is (nil? (descriptor->previous-descriptor kv-store builder previous-descriptor)))))

  (deftest test-descriptor->previous-descriptor-single-token-with-invalid-intermediate
    (let [test-token "test-token"
          token-data-1 {"cmd" "ls-1" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "foo1"}
          service-description-1 token-data-1
          token-data-2 {"cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" "ru" "version" "foo2"}
          token-data-3 {"cmd" "ls-3" "cpus" 3 "mem" 128 "previous" token-data-2 "run-as-user" "ru" "version" "foo3"}
          service-description-3 token-data-2
          sources {:headers {}
                   :service-description-template service-description-3
                   :token->token-data {test-token token-data-3}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token]}
          passthrough-headers {}
          waiter-headers {}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-3)]}}
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))
          previous-descriptor (descriptor->previous-descriptor kv-store builder current-descriptor)]
      (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
              :core-service-description service-description-1
              :on-the-fly? nil
              :passthrough-headers passthrough-headers
              :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-1)]}}
              :service-authentication-disabled false
              :service-description (merge service-description-defaults service-description-1)
              :service-id (sd/service-description->service-id service-id-prefix service-description-1)
              :service-preauthorized false
              :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
              :sources (assoc sources
                         :fallback-period-secs 300
                         :service-description-template service-description-1
                         :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
                         :token->token-data {test-token token-data-1})
              :waiter-headers waiter-headers}
             previous-descriptor))
      (is (nil? (descriptor->previous-descriptor kv-store builder previous-descriptor)))))

  (deftest test-descriptor->previous-descriptor-single-on-the-fly+token-with-previous
    (let [test-token "test-token"
          token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru1" "version" "foo1"}
          service-description-1 token-data-1
          token-data-2 {"cmd" "ls" "cpus" 2 "mem" 64 "previous" token-data-1 "run-as-user" "ru2" "version" "foo2"}
          service-description-2 token-data-2
          sources {:headers {"cpus" 20}
                   :on-the-fly? nil
                   :service-description-template service-description-2
                   :token->token-data {test-token token-data-2}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token]}
          passthrough-headers {}
          waiter-headers {"x-waiter-cpus" 20}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-2)]}}
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))
          previous-descriptor (descriptor->previous-descriptor kv-store builder current-descriptor)]
      (let [expected-core-service-description (assoc service-description-1 "cpus" 20 "permitted-user" username "run-as-user" username)]
        (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
                :core-service-description expected-core-service-description
                :on-the-fly? true
                :passthrough-headers passthrough-headers
                :reference-type->entry {:token {:sources [(reference-tokens-entry test-token token-data-1)]}}
                :service-authentication-disabled false
                :service-description (merge service-description-defaults expected-core-service-description)
                :service-id (sd/service-description->service-id service-id-prefix expected-core-service-description)
                :service-preauthorized false
                :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
                :sources (assoc sources
                           :fallback-period-secs 300
                           :service-description-template service-description-1
                           :source-tokens [(sd/source-tokens-entry test-token token-data-1)]
                           :token->token-data {test-token token-data-1})
                :waiter-headers waiter-headers}
               previous-descriptor)))))

  (deftest test-descriptor->previous-descriptor-multiple-tokens-without-previous
    (let [test-token "test-token"
          service-description-1 {"cmd" "ls" "cpus" 1 "mem" 32}
          service-description-2 {"run-as-user" "ru" "version" "foo"}
          sources {:headers {}
                   :service-description-template (merge service-description-1 service-description-2)
                   :token->token-data {test-token service-description-1
                                       "token-2" service-description-2}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token "token-2"]}
          passthrough-headers {}
          waiter-headers {}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))]
      (is (nil? (descriptor->previous-descriptor kv-store builder current-descriptor)))))

  (deftest test-descriptor->previous-descriptor-multiple-tokens-with-previous
    (let [test-token-1 "test-token-1"
          token-data-1p {"cmd" "lsp" "cpus" 1 "last-update-time" 1000 "mem" 32}
          service-description-1p token-data-1p
          token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32 "previous" token-data-1p}
          service-description-1 token-data-1
          test-token-2 "test-token-2"
          token-data-2p {"last-update-time" 2000 "run-as-user" "rup" "version" "foo"}
          service-description-2p token-data-2p
          token-data-2 {"previous" token-data-2p "run-as-user" "ru" "version" "foo"}
          sources {:headers {}
                   :service-description-template (merge service-description-1 service-description-2p)
                   :source-tokens [(sd/source-tokens-entry test-token-1 token-data-1)
                                   (sd/source-tokens-entry test-token-2 token-data-2)]
                   :token->token-data {test-token-1 token-data-1
                                       test-token-2 token-data-2}
                   :token-authentication-disabled false
                   :token-preauthorized false
                   :token-sequence [test-token-1 test-token-2]}
          passthrough-headers {}
          waiter-headers {}
          current-descriptor (-> {:passthrough-headers passthrough-headers
                                  :reference-type->entry {:token {:sources (:source-tokens sources)}}
                                  :sources sources
                                  :waiter-headers waiter-headers}
                               (attach-token-fallback-source
                                 attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper))
          previous-descriptor (descriptor->previous-descriptor kv-store builder current-descriptor)]
      (let [expected-core-service-description (-> (merge service-description-1 service-description-2p)
                                                (select-keys sd/service-parameter-keys))]
        (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
                :core-service-description expected-core-service-description
                :on-the-fly? nil
                :passthrough-headers passthrough-headers
                :reference-type->entry {:token {:sources [(reference-tokens-entry test-token-1 token-data-1)
                                                          (reference-tokens-entry test-token-2 token-data-2p)]}}
                :service-authentication-disabled false
                :service-description (merge service-description-defaults expected-core-service-description)
                :service-id (sd/service-description->service-id service-id-prefix expected-core-service-description)
                :service-preauthorized false
                :source-tokens [(sd/source-tokens-entry test-token-1 token-data-1)
                                (sd/source-tokens-entry test-token-2 token-data-2p)]
                :sources (-> sources
                           (assoc :fallback-period-secs 300
                                  :service-description-template expected-core-service-description
                                  :source-tokens [(sd/source-tokens-entry test-token-1 token-data-1)
                                                  (sd/source-tokens-entry test-token-2 token-data-2p)]
                                  :token->token-data {test-token-1 token-data-1
                                                      test-token-2 token-data-2p}))
                :waiter-headers waiter-headers}
               previous-descriptor)))
      (let [prev-descriptor-2 (descriptor->previous-descriptor
                                kv-store builder
                                previous-descriptor)]
        (let [expected-core-service-description (-> (merge service-description-1p service-description-2p)
                                                  (select-keys sd/service-parameter-keys))]
          (is (= {:component->previous-descriptor-fns (:component->previous-descriptor-fns current-descriptor)
                  :core-service-description expected-core-service-description
                  :on-the-fly? nil
                  :passthrough-headers passthrough-headers
                  :reference-type->entry {:token {:sources [(reference-tokens-entry test-token-1 token-data-1p)
                                                            (reference-tokens-entry test-token-2 token-data-2p)]}}
                  :service-authentication-disabled false
                  :service-description (merge service-description-defaults expected-core-service-description)
                  :service-id (sd/service-description->service-id service-id-prefix expected-core-service-description)
                  :service-preauthorized false
                  :source-tokens [(sd/source-tokens-entry test-token-1 token-data-1p)
                                  (sd/source-tokens-entry test-token-2 token-data-2p)]
                  :sources (assoc sources
                             :fallback-period-secs 300
                             :service-description-template expected-core-service-description
                             :source-tokens [(sd/source-tokens-entry test-token-1 token-data-1p)
                                             (sd/source-tokens-entry test-token-2 token-data-2p)]
                             :token->token-data {test-token-1 token-data-1p
                                                 test-token-2 token-data-2p})
                  :waiter-headers waiter-headers}
                 (dissoc prev-descriptor-2 :retrieve-fallback-service-description))))
        (is (nil? (descriptor->previous-descriptor
                    kv-store builder
                    prev-descriptor-2)))))))

(deftest test-service-invocation-authorized?
  (let [can-run-as? #(= %1 %2)
        auth-user "test-auth-user"
        other-user "test-other-user"]

    (let [descriptor {:service-authentication-disabled true}]
      (is (false? (service-invocation-authorized? can-run-as? auth-user descriptor))))

    (let [descriptor {:service-authentication-disabled false
                      :service-preauthorized true}]
      (is (false? (service-invocation-authorized? can-run-as? auth-user descriptor))))

    (let [descriptor {:service-authentication-disabled false
                      :service-preauthorized false
                      :service-description {"run-as-user" other-user}}]
      (is (false? (service-invocation-authorized? can-run-as? auth-user descriptor))))

    (let [descriptor {:service-authentication-disabled false
                      :service-preauthorized false
                      :service-description {"permitted-user" other-user
                                            "run-as-user" auth-user}}]
      (is (false? (service-invocation-authorized? can-run-as? auth-user descriptor))))

    (let [descriptor {:service-authentication-disabled false
                      :service-description {"permitted-user" auth-user
                                            "run-as-user" auth-user}
                      :service-preauthorized false}]
      (is (true? (service-invocation-authorized? can-run-as? auth-user descriptor))))

    (let [descriptor {:service-authentication-disabled true
                      :service-description {"permitted-user" auth-user}}]
      (is (true? (service-invocation-authorized? can-run-as? auth-user descriptor))))

    (let [descriptor {:service-authentication-disabled false
                      :service-description {"permitted-user" auth-user}
                      :service-preauthorized true}]
      (is (true? (service-invocation-authorized? can-run-as? auth-user descriptor))))

    (let [descriptor {:service-authentication-disabled false
                      :service-description {"permitted-user" "*"
                                            "run-as-user" auth-user}
                      :service-preauthorized false}]
      (is (true? (service-invocation-authorized? can-run-as? auth-user descriptor))))))

(deftest test-extract-service-state
  (let [fn-name "test-extract-service-state"
        router-id "r1"
        service-id "s1"
        retrieve-service-status-label-fn (constantly nil)]

    (testing (str fn-name ":ping-result-received-response")
      (let [ping-result :received-response
            goal-state "healthy"
            make-inter-router-requests-async-fn (fn [endpoint & _]
                                                  (is (= endpoint (str "apps/" service-id "/await/" goal-state)))
                                                  {"r1" (async/go {:body (async/go (json/write-str {"success?" true}))})
                                                   "r2" (async/go {:body (async/go (json/write-str {"success?" true}))})})
            fallback-state-atom (atom {:available-service-ids #{"s1"}
                                       :healthy-service-ids #{"s1"}})
            service-state (<?? (extract-service-state router-id retrieve-service-status-label-fn fallback-state-atom make-inter-router-requests-async-fn service-id ping-result))]
        (is (= (:service-id service-state) service-id))
        (is (true? (:exists? service-state)))
        (is (true? (:healthy? service-state)))))

    (testing (str fn-name ":ping-result-timed-out")
      (let [ping-result :timed-out
            make-inter-router-requests-async-fn (fn [& _] (is false))
            fallback-state-atom (atom {:available-service-ids #{"s1"}
                                       :healthy-service-ids #{"s1"}})
            service-state (<?? (extract-service-state router-id retrieve-service-status-label-fn fallback-state-atom make-inter-router-requests-async-fn service-id ping-result))]
        (is (= (:service-id service-state) service-id))
        (is (true? (:exists? service-state)))
        (is (true? (:healthy? service-state)))))))
