(ns waiter.instance-tracker-integration-test
  (:require [cheshire.core :as cheshire]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (java.io SequenceInputStream InputStreamReader ByteArrayInputStream)
           (java.util Collections)))

(defn- get-instance-tracker-state
  [waiter-url & {:keys [cookies keywordize-keys query-params]
                 :or {keywordize-keys true}}]
  (let [response (make-request waiter-url "/state/instance-tracker"
                               :cookies cookies
                               :query-params query-params
                               :method :get)]
    (assert-response-status response http-200-ok)
    (cond-> (some-> response :body try-parse-json)
            keywordize-keys walk/keywordize-keys)))

(deftest ^:parallel ^:integration-fast test-instance-tracker-daemon-state
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]

      (testing "no query parameters provides the default state fields"
        (let [{{:keys [supported-include-params] :as state} :state} (get-instance-tracker-state waiter-url :cookies cookies)
              default-state-fields #{:last-update-time :supported-include-params :watch-count}]
          (is (set/superset? (set (keys state)) default-state-fields) (str state))
          (is (set/superset? (set supported-include-params) #{"id->failed-instance" "id->healthy-instance" "instance-failure-handler"})
              (str state))))

      (testing "InstanceEventHandler provides default state fields"
        (let [query-params "include=instance-failure-handler"
              body (get-instance-tracker-state waiter-url
                                               :cookies cookies
                                               :query-params query-params)
              default-inst-event-handler-state-fields #{:last-error-time :supported-include-params :type}]
          (is (set/superset? (set (keys (get-in body [:state :instance-failure-handler]))) default-inst-event-handler-state-fields)
              (str body)))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-instance-tracker-failing-instance
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          routers (routers waiter-url)
          router-urls (vals routers)]
      (testing "new failing instances appear in instance-tracker state on all routers"
        (let [start-time (t/now)
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-kitchen-delay-ms 5000
                 :x-kitchen-die-after-ms 6000
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id
            (assert-response-status response http-200-ok)
            (assert-backend-response response)
            ; wait for all routers to have positive number of failed instances
            (is (wait-for
                  (fn every-router-has-failed-instances? []
                    (every? (fn has-failed-instances? [router-url]
                              (let [{:keys [failed-instances]} (:instances (service-settings router-url service-id :cookies cookies))]
                                (pos? (count failed-instances))))
                            router-urls))))
            (doseq [router-url router-urls]
              (let [{:keys [failed-instances]} (:instances (service-settings router-url service-id :cookies cookies))]
                (log/info "The failed instances should be tracked by the instance-tracker" {:failed-instances failed-instances})
                (is (pos? (count failed-instances)))
                (let [query-params "include=instance-failure-handler&include=id->failed-date"
                      {{:keys [last-update-time]
                        {:keys [last-error-time id->failed-date type]} :instance-failure-handler} :state}
                      (get-instance-tracker-state router-url
                                                  :cookies cookies
                                                  :query-params query-params)
                      default-event-handler-ids (set (keys id->failed-date))]
                  (is (t/before? start-time (du/str-to-date last-update-time)))
                  (when (= type "DefaultInstanceFailureHandler")
                    ; assert failed instances are tracked by DefaultInstanceFailureHandler cache of new failed instances
                    (doseq [{:keys [id]} failed-instances]
                      (is (contains? default-event-handler-ids
                                     (keyword id))))
                    ; assert that the error time is recent
                    (is (t/before? start-time (du/str-to-date last-error-time)))))))))))))

(defn- start-watch
  [router-url cookies & {:keys [query-params] :or {query-params {"streaming-timeout" "120000"
                                                                 "watch" "true"}}}]
  (let [{:keys [body error headers] :as response}
        (make-request router-url "/apps/instances" :async? true :cookies cookies :query-params query-params)
        _ (assert-response-status response 200)
        json-objects (->> body
                          utils/chan-to-seq!!
                          (map (fn [chunk] (-> chunk .getBytes ByteArrayInputStream.)))
                          Collections/enumeration
                          SequenceInputStream.
                          InputStreamReader.
                          cheshire/parsed-seq)
        id->healthy-instance-atom (atom {})
        query-state-fn (fn [] @id->healthy-instance-atom)
        exit-fn (fn []
                  (async/close! body))
        go-chan
        (async/go
          (try
            (doseq [msg json-objects
                    :when (some? msg)]
              (reset!
                id->healthy-instance-atom
                (let [{:strs [object type]} msg
                      id->healthy-instance @id->healthy-instance-atom]
                  (if (contains? #{"initial" "events"} type)
                    (let [updated-healthy-instances (get-in object ["healthy-instances" "updated"])
                          removed-healthy-instances (get-in object ["healthy-instances" "removed"])
                          add-healthy-instances-fn (fn add-healthy-instances
                                                     [id->inst]
                                                     (reduce
                                                       (fn [updated-id->inst inst]
                                                         (assoc updated-id->inst (get inst "id") inst))
                                                       id->inst
                                                       updated-healthy-instances))
                          remove-healthy-instances-fn (fn remove-healthy-instances
                                                        [id->inst]
                                                        (reduce
                                                          (fn [removed-id->inst {:strs [id] :as inst}]
                                                            (if (get removed-id->inst id)
                                                              (dissoc removed-id->inst (get inst "id"))
                                                              (throw (ex-info "No instance to remove from client listener"
                                                                              {:instance-id id
                                                                               :event msg}))))
                                                          id->inst
                                                          removed-healthy-instances))]
                      (cond-> id->healthy-instance
                              (some? updated-healthy-instances)
                              add-healthy-instances-fn
                              (some? removed-healthy-instances)
                              remove-healthy-instances-fn))
                    (throw (ex-info "Unknown event type received from watch" {:event msg}))))))
            (catch Exception e
              (exit-fn)
              (log/error e "Error in test watch-chan" {:router-url router-url}))))]
    {:exit-fn exit-fn
     :error-chan error
     :go-chan go-chan
     :headers headers
     :query-state-fn query-state-fn
     :router-url router-url}))

(defn- start-watches
  [router-urls cookies]
  (mapv
    #(start-watch % cookies)
    router-urls))

(defn- stop-watch
  [{:keys [exit-fn go-chan]}]
  (exit-fn)
  (async/<!! go-chan))

(defn- stop-watches
  [watches]
  (doseq [watch watches]
    (stop-watch watch)))

(defmacro assert-watch-instance-id-entry-with-fn
  [watch instance-id validator-fn]
  `(let [watch# ~watch
         instance-id# ~instance-id
         router-url# (:router-url watch#)
         query-state-fn# (:query-state-fn watch#)
         get-current-instance-entry-fn# #(get (query-state-fn#) instance-id#)
         validator-fn# ~validator-fn]
     (is (wait-for
           #(validator-fn# (get-current-instance-entry-fn#))
           ; this timeout is so high due to scheduler syncer
           :interval 1 :timeout 10)
         (str "watch for " router-url# " id->instance entry for instance-id '" instance-id# "' was " (get-current-instance-entry-fn#)))))

(defmacro assert-watches-some-instance-id-entry
  [watches instance-id]
  `(let [watches# ~watches
         instance-id# ~instance-id]
     (doseq [watch# watches#]
       (assert-watch-instance-id-entry-with-fn watch# instance-id# some?))))

(defmacro assert-watches-nil-instance-id-entry
  [watches instance-id]
  `(let [watches# ~watches
         instance-id# ~instance-id]
     (doseq [watch# watches#]
       (assert-watch-instance-id-entry-with-fn watch# instance-id# nil?))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-instance-watch
  (testing-using-waiter-url
    (let [routers (routers waiter-url)
          router-urls (vals routers)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          every-router-has-healthy-instances?-fn
          (fn every-router-has-healthy-instances? [service-id]
            (every? (fn has-healthy-instances? [router-url]
                      (let [{:keys [active-instances]} (:instances (service-settings router-url service-id :cookies cookies))
                            healthy-instances (filter :healthy? active-instances)]
                        (pos? (count healthy-instances))))
                    router-urls))
          every-router-has-failed-instances?-fn
          (fn every-router-has-failed-instances? [service-id]
            (every? (fn has-failed-instances? [router-url]
                      (let [{:keys [failed-instances]} (:instances (service-settings router-url service-id :cookies cookies))]
                        (pos? (count failed-instances))))
                    router-urls))]

      (testing "watch stream gets initial list of healthy instances that were created before watch started"
        (let [{:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id
            (assert-response-status response http-200-ok)
            (assert-backend-response response)
            ; wait for all routers to have positive number of healthy instances
            (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
            (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                  healthy-instances (filter :healthy? active-instances)
                  watches (start-watches router-urls cookies)]
              (is (pos? (count healthy-instances)))
              (doseq [{:keys [id]} healthy-instances]
                (assert-watches-some-instance-id-entry watches id))
              (stop-watches watches)))))

      (testing "stream receives events when instances become healthy"
        (let [watches (start-watches router-urls cookies)
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id
            (assert-response-status response http-200-ok)
            (assert-backend-response response)
            ; wait for all routers to have positive number of healthy instances
            (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
            (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                  healthy-instances (filter :healthy? active-instances)]
              (is (pos? (count healthy-instances)))
              (doseq [{:keys [id]} healthy-instances]
                (assert-watches-some-instance-id-entry watches id))))
          (stop-watches watches)))

      (testing "stream receives events when instances are no longer healthy"
        (let [watches (start-watches router-urls cookies)
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-kitchen-die-after-ms 10000
                 :x-waiter-max-instances 1
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id
            (assert-response-status response http-200-ok)
            (assert-backend-response response)
            ; wait for all routers to have positive number of healthy instances
            (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
            (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                  healthy-instances (filter :healthy? active-instances)]
              (is (pos? (count healthy-instances)))
              ; wait for all routers to report failed instances
              (is (wait-for #(every-router-has-failed-instances?-fn service-id)))
              (doseq [{:keys [id]} healthy-instances]
                (assert-watches-nil-instance-id-entry watches id))))
          (stop-watches watches)))

      (testing "stream receives events when instances are no longer healthy due to service getting killed"
        (let [watches (start-watches router-urls cookies)
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id
            (assert-response-status response http-200-ok)
            (assert-backend-response response)
            ; wait for all routers to have positive number of healthy instances
            (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
            (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                  healthy-instances (filter :healthy? active-instances)]
              (is (pos? (count healthy-instances)))
              ; kill service
              (delete-service waiter-url service-id)
              (doseq [{:keys [id]} healthy-instances]
                (assert-watches-nil-instance-id-entry watches id))))
          (stop-watches watches)))

      (testing "service-id filter provides initial healthy-instances only for a service"
        (let [{:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))
              {service-id-filtered :service-id :as response-filtered}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-metadata-foo "baz"
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id-filtered
            (with-service-cleanup
              service-id
              (is (not= service-id service-id-filtered))
              (assert-response-status response http-200-ok)
              (assert-backend-response response)
              (assert-response-status response-filtered http-200-ok)
              (assert-backend-response response-filtered)
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id-filtered)))
              (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                    {active-instances-filtered :active-instances}
                    (:instances (service-settings waiter-url service-id-filtered :cookies cookies))
                    healthy-instances (filter :healthy? active-instances)
                    watch (start-watch waiter-url cookies
                                       :query-params {"service-id" service-id
                                                      "watch" "true"})]
                (is (pos? (count healthy-instances)))
                (doseq [{:keys [id]} healthy-instances]
                  (assert-watches-some-instance-id-entry [watch] id))
                (doseq [{:keys [id]} (filter :healthy? active-instances-filtered)]
                  (assert-watches-nil-instance-id-entry [watch] id))
                (stop-watch watch))))))

      (testing "service-id filter provides [:healthy-instances :update] events only for a service"
        (let [{:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))
              {service-id-filtered :service-id :as response-filtered}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-metadata-foo "baz"
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id-filtered
            (with-service-cleanup
              service-id
              (is (not= service-id service-id-filtered))
              (assert-response-status response http-200-ok)
              (assert-backend-response response)
              (assert-response-status response-filtered http-200-ok)
              (assert-backend-response response-filtered)
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id-filtered)))
              (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                    {active-instances-filtered :active-instances}
                    (:instances (service-settings waiter-url service-id-filtered :cookies cookies))
                    healthy-instances (filter :healthy? active-instances)
                    watch (start-watch waiter-url cookies
                                       :query-params {"service-id" service-id
                                                      "watch" "true"})]
                (is (pos? (count healthy-instances)))
                (doseq [{:keys [id]} healthy-instances]
                  (assert-watches-some-instance-id-entry [watch] id))
                (doseq [{:keys [id]} (filter :healthy? active-instances-filtered)]
                  (assert-watches-nil-instance-id-entry [watch] id))
                (stop-watch watch))))))

      (testing "streams initial instances for service-ids that match service description filter"
        (let [{:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-metadata-foo "testingfoo1013"
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))
              {service-id-filtered :service-id :as response-filtered}
              (make-request-with-debug-info
                {:x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id-filtered
            (with-service-cleanup
              service-id
              (is (not= service-id service-id-filtered))
              (assert-response-status response http-200-ok)
              (assert-backend-response response)
              (assert-response-status response-filtered http-200-ok)
              (assert-backend-response response-filtered)
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id-filtered)))
              (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                    {active-instances-filtered :active-instances}
                    (:instances (service-settings waiter-url service-id-filtered :cookies cookies))
                    healthy-instances (filter :healthy? active-instances)
                    healthy-instances-filtered (filter :healthy? active-instances-filtered)
                    watch (start-watch waiter-url cookies
                                       :query-params {"metadata.foo" "testingfoo1013"
                                                      "watch" "true"})]
                (is (pos? (count healthy-instances)))
                (is (pos? (count healthy-instances-filtered)))
                (doseq [{:keys [id]} healthy-instances]
                  (assert-watches-some-instance-id-entry [watch] id))
                (doseq [{:keys [id]} healthy-instances-filtered]
                  (assert-watches-nil-instance-id-entry [watch] id))
                (stop-watch watch))))))

      (testing "streams updated healthy instances for service-ids that match service description filter"
        (let [watch (start-watch waiter-url cookies
                                 :query-params {"metadata.foo" "random-value-required"
                                                "watch" "true"})
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-max-instances 1
                 :x-waiter-metadata-foo "random-value-required"
                 :x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))
              {service-id-filtered :service-id :as response-filtered}
              (make-request-with-debug-info
                {:x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id-filtered
            (with-service-cleanup
              service-id
              (is (not= service-id service-id-filtered))
              (assert-response-status response http-200-ok)
              (assert-backend-response response)
              (assert-response-status response-filtered http-200-ok)
              (assert-backend-response response-filtered)
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
              (is (wait-for #(every-router-has-healthy-instances?-fn service-id-filtered)))
              (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                    {active-instances-filtered :active-instances}
                    (:instances (service-settings waiter-url service-id-filtered :cookies cookies))
                    healthy-instances (filter :healthy? active-instances)]
                (is (pos? (count healthy-instances)))
                (doseq [{:keys [id]} healthy-instances]
                  (assert-watches-some-instance-id-entry [watch] id))
                (doseq [{:keys [id]} (filter :healthy? active-instances-filtered)]
                  (assert-watches-nil-instance-id-entry [watch] id))
                (stop-watch watch)))))))))

(deftest ^:parallel ^:integration-fast test-instance-watch-streaming-timeout
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          streaming-timeout-ms 5000
          start-time-epoch-ms (System/currentTimeMillis)
          {:keys [exit-fn go-chan headers query-state-fn]}
          (start-watch waiter-url cookies :query-params {"service-id" "this-is-not-a-valid-service-id"
                                                         "streaming-timeout" (str streaming-timeout-ms)
                                                         "watch" "true"})
          _ (async/alts!! [go-chan (async/timeout (* 2 streaming-timeout-ms))] :priority true)
          end-time-epoch-ms (System/currentTimeMillis)
          elapsed-time-ms (- end-time-epoch-ms start-time-epoch-ms)
          _ (exit-fn)
          assertion-message (str {:elapsed-time-ms elapsed-time-ms
                                  :headers headers})]
      (is (empty? (query-state-fn)) assertion-message)
      (is (<= streaming-timeout-ms elapsed-time-ms (+ streaming-timeout-ms 1000)) assertion-message))))
