(ns waiter.instance-tracker-integration-test
  (:require [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [clojure.core.async :as async]
            [waiter.util.utils :as utils]
            [cheshire.core :as cheshire])
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
  [router-url cookies]
  (let [{:keys [body error headers] :as response}
        (make-request router-url "/instances" :async? true :cookies cookies :query-params {"watch" "true"})
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
                  (println "received msg")
                  (println msg)
                  (case type
                    "initial"
                    (reduce
                      (fn [new-id->healthy-instance inst]
                        (assoc new-id->healthy-instance (get inst "id") inst))
                      {}
                      (get object "healthy-instances"))

                    "events"
                    (let [new-healthy-instances (get-in object ["healthy-instances" "new"])
                          removed-healthy-instances (get-in object ["healthy-instances" "removed"])
                          add-healthy-instances-fn (fn add-healthy-instances
                                                     [id->inst]
                                                     (reduce
                                                       (fn [new-id->inst inst]
                                                         (assoc new-id->inst (get inst "id") inst))
                                                       id->inst
                                                       new-healthy-instances))
                          remove-healthy-instances-fn (fn remove-healthy-instances
                                                        [id->inst]
                                                        (reduce
                                                          (fn [new-id->inst inst]
                                                            (dissoc new-id->inst (get inst "id")))
                                                          id->inst
                                                          removed-healthy-instances))
                          id->healthy-instance' (cond-> id->healthy-instance
                                                        (some? new-healthy-instances)
                                                        add-healthy-instances-fn
                                                        (some? removed-healthy-instances)
                                                        remove-healthy-instances-fn)]
                      (println "new-healthy-instances" new-healthy-instances)
                      (println "removed-healthy-instances" removed-healthy-instances)
                      (println "id->healthy-instance'" id->healthy-instance')
                      id->healthy-instance')
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

(defmacro assert-watch-instance-id-entry
  [watch instance-id entry]
  `(let [watch# ~watch
         instance-id# ~instance-id
         entry# ~entry
         router-url# (:router-url watch#)
         query-state-fn# (:query-state-fn watch#)
         get-current-instance-entry-fn# #(get (query-state-fn#) instance-id#)]
     (is (wait-for
           #(= (walk/keywordize-keys (get-current-instance-entry-fn#))
               (dissoc entry# :log-url))
           ; this timeout is so high due to scheduler syncer
           :interval 1 :timeout 10)
         (str "watch for " router-url# " id->instance entry for instance-id '" instance-id# "' was '" (get-current-instance-entry-fn#)
              "' instead of '" entry# "'"))))

(defmacro assert-watches-instance-id-entry
  [watches instance-id entry]
  `(let [watches# ~watches
         instance-id# ~instance-id
         entry# ~entry]
     (doseq [watch# watches#]
       (assert-watch-instance-id-entry watch# instance-id# entry#))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-instance-watch
  (testing-using-waiter-url
    (let [routers (routers waiter-url)
          router-urls (vals routers)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          every-router-has-healthy-instances?-fn
          (fn every-router-has-healthy-instances? [service-id]
            (every? (fn has-failed-instances? [router-url]
                      (let [{:keys [active-instances]} (:instances (service-settings router-url service-id :cookies cookies))
                            healthy-instances (filter :healthy? active-instances)]
                        (pos? (count healthy-instances))))
                    router-urls))]

      (testing "watch stream gets initial list of healthy instances that were created before watch started"
        (let [{:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-name (rand-name)}
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
              (doseq [{:keys [id] :as inst} healthy-instances]
                (assert-watches-instance-id-entry watches id inst))))))

      (testing "stream receives events when instances become healthy"
        (let [watches (start-watches router-urls cookies)
              {:keys [service-id] :as response}
              (make-request-with-debug-info
                {:x-waiter-name (rand-name)}
                #(make-kitchen-request waiter-url % :cookies cookies :path "/status"))]
          (with-service-cleanup
            service-id
            (assert-response-status response http-200-ok)
            (assert-backend-response response)
            ; wait for all routers to have positive number of healthy instances
            (is (wait-for #(every-router-has-healthy-instances?-fn service-id)))
            (let [{:keys [active-instances]} (:instances (service-settings waiter-url service-id :cookies cookies))
                  healthy-instances (filter :healthy? active-instances)]
              (println "looking for healthy-instances" healthy-instances)
              (is (pos? (count healthy-instances)))
              (doseq [{:keys [id] :as inst} healthy-instances]
                (assert-watches-instance-id-entry watches id inst))))))

      (testing "stream receives events when instances are no longer healthy")

      (testing "stream receives events when instances are no longer healthy due to service getting killed")


      )))
