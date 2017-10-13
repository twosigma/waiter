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
(ns waiter.core-test
  (:require [clj-http.client]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [qbits.jet.client.http :as http]
            [waiter.auth.authentication :as auth]
            [waiter.authorization :as authz]
            [waiter.core :refer :all]
            [waiter.cors :as cors]
            [waiter.curator :as curator]
            [waiter.discovery :as discovery]
            [waiter.handler :as handler]
            [waiter.kv :as kv]
            [waiter.marathon :as marathon]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.test-helpers :refer :all]
            [waiter.utils :as utils])
  (:import java.io.StringBufferInputStream))

(defn request
  [resource request-method & params]
  {:request-method request-method :uri resource :params (first params)})

(deftest test-peers-acknowledged-blacklist-requests?
  (let [service-id "service-id"
        instance-id "service-id.instance-id"
        endpoint "router/endpoint"
        test-cases (list
                     {:name "no-routers-in-input"
                      :short-circuit true
                      :router-ids []
                      :expected-result true
                      :expected-routers-connected []}
                     {:name "all-routers-approve-kill"
                      :short-circuit true
                      :router-ids ["abcd-success" "bcde-success" "defg-success"]
                      :expected-result true
                      :expected-routers-connected ["abcd-success" "bcde-success" "defg-success"]}
                     {:name "second-router-vetoes-kill-with-short-circuit"
                      :short-circuit true
                      :router-ids ["abcd-success" "bcde-fail" "defg-success"]
                      :expected-result false
                      :expected-routers-connected ["abcd-success" "bcde-fail"]}
                     {:name "second-router-vetoes-kill-without-short-circuit"
                      :short-circuit false
                      :router-ids ["abcd-success" "bcde-fail" "defg-success"]
                      :expected-result false
                      :expected-routers-connected ["abcd-success" "bcde-fail" "defg-success"]})]
    (doseq [{:keys [name short-circuit router-ids expected-result expected-routers-connected]} test-cases]
      (testing (str "Test " name)
        (let [invoked-routers-atom (atom [])
              make-blacklist-request-fn (fn [in-dest-router-id in-dest-endpoint instance reason]
                                          (swap! invoked-routers-atom conj in-dest-router-id)
                                          (is (= service-id (:service-id instance)))
                                          (is (= instance-id (:id instance)))
                                          (is (= "blacklist-reason" reason))
                                          (is (= endpoint in-dest-endpoint))
                                          (if (str/includes? in-dest-router-id "fail") {:status 400} {:status 200}))
              actual-result (peers-acknowledged-blacklist-requests?
                              {:id instance-id, :service-id service-id} short-circuit router-ids endpoint
                              make-blacklist-request-fn "blacklist-reason")]
          (is (= expected-routers-connected @invoked-routers-atom))
          (is (= expected-result actual-result)))))))

(defn- mock-service-data-chan [service-data-chan exit-chan timeout-interval-ms service-data-atom num-times]
  (async/go-loop [n num-times]
    (when (pos? n)
      (async/>! service-data-chan @service-data-atom)
      (when-let [_ (async/alt!
                     exit-chan ([_] nil)
                     (async/timeout timeout-interval-ms) ([_] true)
                     :priority true)]
        (let [new-service-data (into {} (map (fn [[service data]]
                                               [service (if (:delete data)
                                                          (assoc data :counter (dec (:counter data)))
                                                          data)])
                                             @service-data-atom))]
          (reset! service-data-atom new-service-data))
        (recur (dec n))))))

(deftest test-service-gc-go-routine
  (let [curator (Object.)
        gc-base-path "/test-path/gc-base-path"
        leader? (constantly true)
        state-store (atom {})]
    (with-redefs [curator/read-path (fn [_ path & _] {:data (get @state-store path)})
                  curator/write-path (fn [_ path data & _] (swap! state-store (fn [v] (assoc v path data))))]
      (let [service-gc-block (fn [service-data-atom num-times]
                               (let [service-data-exit-chan (async/chan 1)
                                     service-data-chan (async/chan (async/sliding-buffer num-times))
                                     timeout-interval-ms 1
                                     read-state-fn (fn [name] (:data (curator/read-path curator (str gc-base-path "/" name) :nil-on-missing? true :serializer :nippy)))
                                     write-state-fn (fn [name state] (curator/write-path curator (str gc-base-path "/" name) state :serializer :nippy :create-parent-zknodes? true))]
                                 (mock-service-data-chan
                                   service-data-chan
                                   service-data-exit-chan
                                   (* 2 timeout-interval-ms)
                                   service-data-atom
                                   num-times)
                                 (let [{:keys [exit query]} (service-gc-go-routine
                                                              read-state-fn
                                                              write-state-fn
                                                              leader?
                                                              t/now
                                                              "test-routine"
                                                              service-data-chan
                                                              timeout-interval-ms
                                                              (fn [prev-service->state _] prev-service->state)
                                                              (fn [_ _ data] data)
                                                              (fn [_ {:keys [state _]} _]
                                                                (and (:delete state) (neg? (:counter state))))
                                                              (fn [service]
                                                                (is (neg? (get-in @service-data-atom [service :counter] -1)))
                                                                (when (= :service-3 service)
                                                                  (throw (ex-info "Will not delete service-3 in test!" {})))
                                                                (swap! service-data-atom dissoc service)))
                                       query-response-chan (async/promise-chan)]
                                   (async/<!! (async/timeout 500))
                                   (let [service-data @service-data-atom]
                                     (async/>!! query {:service-id (first (keys service-data)) :response-chan query-response-chan})
                                     ; Update service data chan one final time to read query chan
                                     (async/>!! service-data-chan service-data)
                                     (is (= (first (vals service-data))
                                            (:state (async/<!! query-response-chan)))))
                                   (async/>!! exit :exit)
                                   (async/>!! service-data-exit-chan :exit)
                                   (async/<!! (async/timeout 50)))))]
        (testing "service-gc-go-routine"
          (let [service-data-atom (atom {:service-1 {:delete false :counter 1}
                                         :service-2 {:delete false :counter 10}
                                         :service-3 {:delete true :counter 1}
                                         :service-4 {:delete false :counter 1}
                                         :service-5 {:delete true :counter 3}
                                         :service-6 {:delete true :counter 20}
                                         :service-7 {:delete true, :counter 5}
                                         :service-8 {:delete true, :counter 0}
                                         :service-9 {:delete true, :counter 2}})
                num-times 5
                expected-service-data {:service-1 {:delete false, :counter 1}
                                       :service-2 {:delete false, :counter 10}
                                       :service-3 {:delete true, :counter -1}
                                       :service-4 {:delete false, :counter 1}
                                       :service-6 {:delete true, :counter 16}
                                       :service-7 {:delete true, :counter 0}}]
            (service-gc-block service-data-atom num-times)
            (is (= (set (keys expected-service-data)) (set (keys @service-data-atom))))))))))

(deftest test-suspend-or-resume-service-handler
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-description-defaults {"cmd" "tc", "cpus" 1, "mem" 200, "version" "a1b2c3", "run-as-user" "tu1", "permitted-user" "tu2"}
        waiter-request?-fn (fn [_] true)
        entitlement-manager (reify authz/EntitlementManager
                              (authorized? [_ subject _ {:keys [user]}] (= subject user)))
        allowed-to-manage-service? (fn [service-id auth-user]
                                     (sd/can-manage-service? kv-store entitlement-manager service-id auth-user))
        make-inter-router-requests-sync-fn (fn [path _ _] (is (str/includes? path "service-id-")))
        configuration {:curator {:kv-store kv-store}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:allowed-to-manage-service?-fn allowed-to-manage-service?
                                  :make-inter-router-requests-sync-fn make-inter-router-requests-sync-fn
                                  :service-description-defaults service-description-defaults}}
        handlers {:service-resume-handler-fn ((:service-resume-handler-fn request-handlers) configuration)
                  :service-suspend-handler-fn ((:service-suspend-handler-fn request-handlers) configuration)}
        test-service-id "service-id-1"]
    (sd/store-core kv-store test-service-id service-description-defaults (constantly true))
    (testing "suspend-or-resume-service-handler"
      (let [user "tu1"
            request {:uri (str "/apps/" test-service-id "/suspend")
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (every? #(str/includes? (str body) %) ["true" test-service-id "suspend"])))
      (let [user "tu1"
            request {:uri (str "/apps/" test-service-id "/resume")
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (every? #(str/includes? (str body) %) ["true" test-service-id "resume"])))
      (let [user "tu2"
            request {:uri (str "/apps/" test-service-id "/suspend")
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 403 status))
        (is (every? #(str/includes? (str body) %) ["not allowed" test-service-id])))
      (let [user "tu2"
            request {:uri (str "/apps/" test-service-id "/resume")
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 403 status))
        (is (every? #(str/includes? (str body) %) ["not allowed" test-service-id]))))))

(deftest test-override-service-handler
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-description-defaults {"cmd" "tc", "cpus" 1, "mem" 200, "version" "a1b2c3", "run-as-user" "tu1", "permitted-user" "tu2"}
        waiter-request?-fn (fn [_] true)
        entitlement-manager (reify authz/EntitlementManager
                              (authorized? [_ subject _ {:keys [user]}] (= subject user)))
        allowed-to-manage-service? (fn [service-id auth-user]
                                     (sd/can-manage-service? kv-store entitlement-manager service-id auth-user))
        make-inter-router-requests-sync-fn (fn [path _ _] (is (str/includes? path "service-id-")))
        configuration {:curator {:kv-store kv-store}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:allowed-to-manage-service?-fn allowed-to-manage-service?
                                  :make-inter-router-requests-sync-fn make-inter-router-requests-sync-fn
                                  :service-description-defaults service-description-defaults}}
        handlers {:service-override-handler-fn ((:service-override-handler-fn request-handlers) configuration)}
        test-service-id "service-id-1"]
    (sd/store-core kv-store test-service-id service-description-defaults (constantly nil))
    (testing "override-service-handler"
      (let [user "tu1"
            request {:uri (str "/apps/" test-service-id "/override")
                     :request-method :post
                     :body (StringBufferInputStream. (json/write-str {"scale-factor" 0.3, "cmd" "overridden-cmd"}))
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (every? #(str/includes? (str body) %) ["true" test-service-id "success"]))
        (is (= {"scale-factor" 0.3} (:overrides (sd/service-id->overrides kv-store test-service-id)))))
      (let [user "tu1"
            request {:uri (str "/apps/" test-service-id "/override")
                     :request-method :delete
                     :body (StringBufferInputStream. (json/write-str {"scale-factor" 0.3, "cmd" "overridden-cmd"}))
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (every? #(str/includes? (str body) %) ["true" test-service-id "success"]))
        (is (= {} (:overrides (sd/service-id->overrides kv-store test-service-id)))))
      (let [user "tu2"
            request {:uri (str "/apps/" test-service-id "/override")
                     :request-method :post
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 403 status))
        (is (every? #(str/includes? (str body) %) ["not allowed" test-service-id])))
      (let [user "tu2"
            request {:uri (str "/apps/" test-service-id "/override")
                     :request-method :delete
                     :authorization/user user}
            {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 403 status))
        (is (every? #(str/includes? (str body) %) ["not allowed" test-service-id]))))))

(deftest test-service-view-logs-handler
  (let [scheduler (marathon/->MarathonScheduler {} 5051 (fn [] nil) "/slave/directory" "/home/path/"
                                                (atom {}) (atom {}) 0 (constantly true))
        configuration {:handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:prepend-waiter-url identity}
                       :state {:scheduler scheduler}}
        handlers {:service-view-logs-handler-fn ((:service-view-logs-handler-fn request-handlers) configuration)}
        waiter-request?-fn (fn [_] true)
        test-service-id "test-service-id"
        user "test-user"]
    (with-redefs [clj-http.client/get (fn [_ _] {:body "{}"})]
      (testing "Missing instance id"
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :query-string ""
                       :request-method :get
                       :uri (str "/apps/" test-service-id "/logs")}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)
              json-body (json/read-str body)]
          (is (= status 400))
          (is (= "Missing instance-id parameter" (get-in json-body ["waiter-error" "message"])))))
      (testing "Missing host"
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :query-string "instance-id=instance-id-1"
                       :request-method :get
                       :uri (str "/apps/" test-service-id "/logs")}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)
              json-body (json/read-str body)]
          (is (= status 400))
          (is (= "Missing host parameter" (get-in json-body ["waiter-error" "message"]))))))
    (with-redefs [clj-http.client/get (fn [url _]
                                        (is (every? #(str/includes? url %) ["test.host.com" "5051"]))
                                        (let [state-json-response-body "
                                   {
                                    \"frameworks\": [{
                                                   \"role\": \"marathon\",
                                                   \"completed_executors\": [{
                                                                            \"id\": \"service-id-1.instance-id-1\",
                                                                            \"directory\": \"/path/to/instance1/directory\"
                                                                            }],
                                                   \"executors\": [{
                                                                  \"id\": \"service-id-1.instance-id-2\",
                                                                  \"directory\": \"/path/to/instance2/directory\"
                                                                  }]
                                                   }]
                                    }"
                                              file-browse-response-body "
                                   [{\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil1\", \"size\": 1000},
                                    {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir2\", \"size\": 2000},
                                    {\"nlink\": 1, \"path\": \"/path/to/instance2/directory/fil3\", \"size\": 3000},
                                    {\"nlink\": 2, \"path\": \"/path/to/instance2/directory/dir4\", \"size\": 4000}]"]
                                          (if (str/includes? url "state.json")
                                            {:body state-json-response-body}
                                            {:body file-browse-response-body})))]
      (testing "Missing directory"
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :query-string "instance-id=service-id-1.instance-id-1&host=test.host.com"
                       :request-method :get
                       :uri (str "/apps/" test-service-id "/logs")}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)
              json-body (json/read-str body)]
          (is (= status 200))
          (is (= [{"name" "fil1"
                   "size" 1000
                   "type" "file"
                   "url" "http://test.host.com:5051/files/download?path=/path/to/instance2/directory/fil1"}
                  {"name" "dir2"
                   "size" 2000
                   "type" "directory"
                   "url" "/apps/test-service-id/logs?instance-id=service-id-1.instance-id-1&host=test.host.com&directory=/path/to/instance2/directory/dir2"}
                  {"name" "fil3"
                   "size" 3000
                   "type" "file"
                   "url" "http://test.host.com:5051/files/download?path=/path/to/instance2/directory/fil3"}
                  {"name" "dir4"
                   "size" 4000
                   "type" "directory"
                   "url" "/apps/test-service-id/logs?instance-id=service-id-1.instance-id-1&host=test.host.com&directory=/path/to/instance2/directory/dir4"}]
                 json-body))))
      (testing "Valid response"
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :query-string "instance-id=service-id-1.instance-id-2&host=test.host.com&directory=/path/to/instance2/directory/"
                       :request-method :get
                       :uri (str "/apps/" test-service-id "/logs")}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 200 status) body)
          (is (every? #(str/includes? body %) ["test.host.com" "5051" "file" "directory" "name" "url" "download"])))))))

(deftest test-apps-handler-delete
  (let [user "test-user"
        service-id "test-service-1"
        kv-store (kv/->LocalKeyValueStore (atom {}))
        waiter-request?-fn (fn [_] true)
        entitlement-manager (reify authz/EntitlementManager
                              (authorized? [_ subject _ {:keys [user]}] (= subject user)))
        allowed-to-manage-service? (fn [service-id auth-user]
                                     (sd/can-manage-service? kv-store entitlement-manager service-id auth-user))
        configuration {:curator {:kv-store nil}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:allowed-to-manage-service?-fn allowed-to-manage-service?
                                  :make-inter-router-requests-sync-fn nil
                                  :prepend-waiter-url nil}
                       :state {:router-id "router-id"
                               :scheduler (Object.)}}
        handlers {:service-handler-fn ((:service-handler-fn request-handlers) configuration)}]
    (testing "service-handler:delete-successful"
      (with-redefs [scheduler/delete-app (fn [_ service-id] {:result :deleted, :service-id service-id})
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 200 status))
          (is (= {"content-type" "application/json"} headers))
          (is (= {"success" true, "service-id" service-id, "result" "deleted"} (json/read-str body))))))
    (testing "service-handler:delete-nil-response"
      (with-redefs [scheduler/delete-app (fn [_ _] nil)
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 400 status))
          (is (= {"content-type" "application/json"} headers))
          (is (= {"success" false, "service-id" service-id} (json/read-str body))))))
    (testing "service-handler:delete-unauthorized-user"
      (with-redefs [scheduler/delete-app (fn [_ _] (throw (IllegalStateException. "Unexpected call!")))
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" (str "another-" user), "name" (str service-id "-name")})]
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :request-method :delete
                       :uri (str "/apps/" service-id)}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 403 status))
          (is (= {"content-type" "application/json"} headers))
          (is (str/includes? body "User not allowed to delete service")))))
    (testing "service-handler:delete-404-response"
      (with-redefs [scheduler/delete-app (fn [_ _] {:result :no-such-service-exists})
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 404 status))
          (is (= {"content-type" "application/json"} headers))
          (is (= {"result" "no-such-service-exists", "service-id" service-id, "success" false} (json/read-str body))))))
    (testing "service-handler:delete-non-existent-service"
      (with-redefs [scheduler/delete-app (fn [_ _] (throw (IllegalStateException. "Unexpected call!")))
                    sd/fetch-core (fn [_ _ & _] {})]
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :request-method :delete
                       :uri (str "/apps/" service-id)}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)
              {{message "message"
                {:strs [service-id]} "details"} "waiter-error"} (json/read-str body)]
          (is (= 404 status))
          (is (= {"content-type" "application/json"} headers))
          (is (= "Service not found" message))
          (is (= "test-service-1" service-id)))))
    (testing "service-handler:delete-throws-exception"
      (with-redefs [scheduler/delete-app (fn [_ _] (throw (RuntimeException. "Error in deleting service")))
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :request-method :delete
                       :uri (str "/apps/" service-id)}
              {:keys [headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 500 status))
          (is (= {"content-type" "application/json"} headers)))))))

(deftest test-apps-handler-get
  (let [user "waiter-user"
        service-id "test-service-1"
        waiter-request?-fn (fn [_] true)
        configuration {:curator {:kv-store nil}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:allowed-to-manage-service?-fn (constantly true)
                                  :make-inter-router-requests-sync-fn nil
                                  :prepend-waiter-url #(str "http://www.example.com" %)}
                       :state {:router-id "router-id"
                               :scheduler (Object.)}}
        handlers {:service-handler-fn ((:service-handler-fn request-handlers) configuration)}
        ring-handler (wrap-handler-json-response (ring-handler-factory waiter-request?-fn handlers))]
    (testing "service-handler:get-missing-service-description"
      (with-redefs [sd/fetch-core (constantly nil)]
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :request-method :get
                       :uri (str "/apps/" service-id)}
              {:keys [body headers status]} (ring-handler request)]
          (is (= 404 status))
          (is (= {"content-type" "application/json"} headers))
          (let [{{message "message"
                  {:strs [service-id]} "details"} "waiter-error"} (json/read-str (str body))]
            (is (= "Service not found" message))
            (is (= "test-service-1" service-id))))))
    (testing "service-handler:valid-response-missing-killed-and-failed"
      (with-redefs [sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})
                    scheduler/get-instances (fn [_ service-id]
                                              {:active-instances [{:id (str service-id ".A")
                                                                   :service-id service-id
                                                                   :healthy? true,
                                                                   :host "10.141.141.11"
                                                                   :port 31045,
                                                                   :started-at "2014-09-13T002446.959Z"}]
                                               :failed-instances []})]
        (let [request {:authorization/user user
                       :headers {"accept" "application/json"}
                       :request-method :get
                       :uri (str "/apps/" service-id)}
              {:keys [body headers status]} (ring-handler request)]
          (is (= 200 status))
          (is (= {"content-type" "application/json"} headers))
          (let [body-json (json/read-str (str body))]
            (is (= (get body-json "instances")
                   {"active-instances" [{"id" (str service-id ".A")
                                         "service-id" service-id
                                         "healthy?" true,
                                         "host" "10.141.141.11"
                                         "log-url" "http://www.example.com/apps/test-service-1/logs?instance-id=test-service-1.A&host=10.141.141.11"
                                         "port" 31045,
                                         "started-at" "2014-09-13T002446.959Z"}]}))
            (is (= (get body-json "metrics")
                   {"aggregate" {"routers-sent-requests-to" 0}}))
            (is (= (get body-json "num-active-instances") 1))
            (is (= (get body-json "num-routers") 0))
            (is (= (get body-json "service-description")
                   {"name" "test-service-1-name", "run-as-user" "waiter-user"}))))))
    (testing "service-handler:valid-response-including-active-killed-and-failed"
      (with-redefs [sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})
                    scheduler/get-instances (fn [_ service-id]
                                              {:active-instances [{:id (str service-id ".A"), :service-id service-id}]
                                               :failed-instances [{:id (str service-id ".F"), :service-id service-id}]
                                               :killed-instances [{:id (str service-id ".K"), :service-id service-id}]})]
        (let [request {:request-method :get, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} (ring-handler request)]
          (is (= 200 status))
          (is (= {"content-type" "application/json"} headers))
          (let [body-json (json/read-str (str body))]
            (is (= (get body-json "instances")
                   {"active-instances" [{"id" (str service-id ".A")
                                         "service-id" service-id
                                         "log-url" "http://www.example.com/apps/test-service-1/logs?instance-id=test-service-1.A&host="}]
                    "killed-instances" [{"id" (str service-id ".K")
                                         "service-id" service-id
                                         "log-url" "http://www.example.com/apps/test-service-1/logs?instance-id=test-service-1.K&host="}]
                    "failed-instances" [{"id" (str service-id ".F")
                                         "service-id" service-id
                                         "log-url" "http://www.example.com/apps/test-service-1/logs?instance-id=test-service-1.F&host="}]}))
            (is (= (get body-json "metrics")
                   {"aggregate" {"routers-sent-requests-to" 0}}))
            (is (= (get body-json "num-active-instances") 1))
            (is (= (get body-json "num-routers") 0))
            (is (= (get body-json "service-description")
                   {"name" "test-service-1-name", "run-as-user" "waiter-user"}))))))))

(deftest test-make-inter-router-requests
  (let [auth-object (Object.)
        my-router-id "router-0"
        discovery "dicovery-object!"
        passwords ["password-1" "password-2"]
        make-response #(let [response-chan (async/promise-chan)
                             body-chan (async/promise-chan)]
                         (async/>!! body-chan "dummy response body")
                         (async/>!! response-chan {:body body-chan})
                         response-chan)
        make-basic-auth-fn (fn make-basic-auth-fn [_ _ _] auth-object)
        make-request-fn-factory (fn [urls-invoked-atom]
                                  (fn [method endpoint-url auth body config]
                                    (is (str/blank? body))
                                    (is (= {:headers {"accept" "application/json"}} config))
                                    (is (= :get method))
                                    (is (= auth-object auth))
                                    (swap! urls-invoked-atom conj endpoint-url)
                                    (make-response)))]
    (with-redefs [discovery/router-id->endpoint-url
                  (fn [_ protocol endpoint & {:keys [exclude-set] :or {exclude-set #{}}}]
                    (pc/map-from-keys (fn [router-id]
                                        (str protocol "://" router-id "/" endpoint))
                                      (remove #(contains? exclude-set %)
                                              ["router-0", "router-1", "router-2", "router-3", "router-4"])))]
      (testing "make-call-to-all-other-routers"
        (let [urls-invoked-atom (atom [])
              make-request-fn (make-request-fn-factory urls-invoked-atom)]
          (make-inter-router-requests make-request-fn make-basic-auth-fn my-router-id discovery passwords "test/endpoint1")
          (is (= 4 (count @urls-invoked-atom)))
          (is (= 1 (count (filter #(= "http://router-1/test/endpoint1" %) @urls-invoked-atom))))
          (is (= 1 (count (filter #(= "http://router-2/test/endpoint1" %) @urls-invoked-atom))))
          (is (= 1 (count (filter #(= "http://router-3/test/endpoint1" %) @urls-invoked-atom))))
          (is (= 1 (count (filter #(= "http://router-4/test/endpoint1" %) @urls-invoked-atom))))))
      (testing "filter-some-routers"
        (let [urls-invoked-atom (atom [])
              make-request-fn (make-request-fn-factory urls-invoked-atom)]
          (make-inter-router-requests make-request-fn make-basic-auth-fn my-router-id discovery passwords "test/endpoint2"
                                      :acceptable-router? (fn [router-id] (some #(str/includes? router-id %) ["0" "1" "2" "4"])))
          (is (= 3 (count @urls-invoked-atom)))
          (is (= 1 (count (filter #(= "http://router-1/test/endpoint2" %) @urls-invoked-atom))))
          (is (= 1 (count (filter #(= "http://router-2/test/endpoint2" %) @urls-invoked-atom))))
          (is (= 1 (count (filter #(= "http://router-4/test/endpoint2" %) @urls-invoked-atom))))))
      (testing "filter-all-routers"
        (let [urls-invoked-atom (atom [])
              make-request-fn (make-request-fn-factory urls-invoked-atom)]
          (make-inter-router-requests make-request-fn make-basic-auth-fn my-router-id discovery passwords "test/endpoint3"
                                      :acceptable-router? (fn [router-id] (some #(str/includes? router-id %) ["A" "B" "C"])))
          (is (= 0 (count @urls-invoked-atom))))))))

(deftest test-make-request-async
  (let [http-client (Object.)
        idle-timeout 1234
        method :test
        endpoint-url "endpoint/url"
        auth (Object.)
        body "body-str"
        config {:foo :bar, :test :map}
        expected-response (async/chan)]
    (with-redefs [http/request (fn [in-http-client config-map]
                                 (is (= http-client in-http-client))
                                 (is (= (merge {:auth auth
                                                :body body
                                                :follow-redirects? false
                                                :idle-timeout idle-timeout
                                                :method method
                                                :url endpoint-url}
                                               config)
                                        config-map))
                                 expected-response)]
      (is (= expected-response
             (make-request-async http-client idle-timeout method endpoint-url auth body config))))))

(deftest test-make-request-sync
  (let [http-client (Object.)
        idle-timeout 1234
        method :test
        endpoint-url "endpoint/url"
        auth (Object.)
        config {:foo :bar, :test :map}]
    (with-redefs [make-request-async (fn [in-http-client in-idle-timeout in-method in-endpoint-url in-auth body in-config]
                                       (is (= http-client in-http-client))
                                       (is (= idle-timeout in-idle-timeout))
                                       (is (= method in-method))
                                       (is (= endpoint-url in-endpoint-url))
                                       (is (= auth in-auth))
                                       (is (= config in-config))
                                       (let [response-chan (async/promise-chan)
                                             body-chan (async/promise-chan)]
                                         (if (str/includes? body "error")
                                           (throw (ex-info (str body) {}))
                                           (async/>!! response-chan {:body body-chan, :status 200}))
                                         (async/>!! body-chan body)
                                         response-chan))]
      (testing "error-in-response"
        (is (thrown-with-msg? Exception #"error-in-request"
                              (make-request-sync http-client idle-timeout method endpoint-url auth "error-in-request" config))))

      (testing "successful-response"
        (let [body-string "successful-response"]
          (is (= {:body body-string, :status 200}
                 (make-request-sync http-client idle-timeout method endpoint-url auth body-string config))))))))

(deftest test-waiter-request?-factory
  (testing "waiter-request?"
    (let [waiter-request? (waiter-request?-factory
                            #{"waiter-cluster.example.com" "waiter-router.example.com" "0.0.0.0"})]
      (is (waiter-request? {:headers {"host" "waiter-cluster.example.com"}}))
      (is (waiter-request? {:headers {"host" "waiter-cluster.example.com:80"}}))
      (is (waiter-request? {:headers {"host" "waiter-router.example.com"}}))
      (is (waiter-request? {:headers {"host" "waiter-router.example.com:80"}}))
      (is (waiter-request? {:headers {"host" "0.0.0.0"}}))
      (is (waiter-request? {:headers {"host" "0.0.0.0:80"}}))
      (is (waiter-request? {:headers {"host" "localhost"}}))
      (is (waiter-request? {:headers {"host" "localhost:80"}}))
      (is (waiter-request? {:headers {"host" "127.0.0.1"}}))
      (is (waiter-request? {:headers {"host" "127.0.0.1:80"}}))
      (is (waiter-request? {}))
      (is (waiter-request? {:uri "/app-name"}))
      (is (waiter-request? {:uri "/service-id"}))
      (is (waiter-request? {:uri "/token"}))
      (is (waiter-request? {:uri "/waiter-async/complete/remaining-parts"}))
      (is (waiter-request? {:uri "/waiter-async/result/remaining-parts"}))
      (is (waiter-request? {:uri "/waiter-async/status/remaining-parts"}))
      (is (not (waiter-request? {:headers {"host" "service.example.com"}})))
      (is (not (waiter-request? {:headers {"host" "service.example.com:80"}})))
      (is (not (waiter-request? {:headers {"host" "waiter-cluster.example.com"
                                           "x-waiter-token" "token"}})))
      (is (not (waiter-request? {:headers {"host" "waiter-cluster.example.com:80"
                                           "x-waiter-token" "token"}})))
      (is (not (waiter-request? {:headers {"host" "waiter-cluster.example.com"
                                           "x-waiter-cmd" "my command"}})))
      (is (not (waiter-request? {:headers {"host" "waiter-cluster.example.com:80"
                                           "x-waiter-cmd" "my command"}}))))))

(deftest test-health-check-handler-handler
  (testing "health-check-handler:status-ok"
    (let [request {:request-method :get, :uri "/status"}
          waiter-request?-fn (fn [_] true)
          handlers {:status-handler-fn ((:status-handler-fn request-handlers) {})
                    :waiter-request?-fn (constantly true)}
          {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
      (is (= 200 status))
      (is (= {} headers))
      (is (= "ok" (str body))))))

(deftest test-leader-fn-factory
  (with-redefs [discovery/cluster-size (fn [discovery] (int discovery))]
    (testing "leader-as-single-instance"
      (let [discovery 1
            has-leadership? (constantly true)
            leader? (leader-fn-factory "router-1" has-leadership? discovery 1)]
        (is (leader?) "leader in unit size cluster")))

    (testing "not-leader-as-single-instance"
      (let [discovery 1
            has-leadership? (constantly false)
            leader? (leader-fn-factory "router-1" has-leadership? discovery 1)]
        (is (not (leader?)) "not leader in unit size cluster")))

    (testing "leader-among-two-instances"
      (let [discovery 2
            has-leadership? (constantly true)
            leader? (leader-fn-factory "router-1" has-leadership? discovery 2)]
        (is (leader?) "leader? falsy among 2")))

    (testing "leader-among-three-instances-2"
      (let [discovery 3
            has-leadership? (constantly true)
            leader? (leader-fn-factory "router-1" has-leadership? discovery 2)]
        (is (leader?) "leader? falsy among 3 (with min = 2)")))

    (testing "leader-among-three-instances-3"
      (let [discovery 3
            has-leadership? (constantly true)
            leader? (leader-fn-factory "router-1" has-leadership? discovery 3)]
        (is (leader?) "leader? falsy among 3 (with min = 3)")))

    (testing "not-leader-due-to-too-few-peers-2"
      (let [discovery 1
            has-leadership? (constantly true)
            leader? (leader-fn-factory "router-1" has-leadership? discovery 2)]
        (is (not (leader?)) "not leader when too few peers")))

    (testing "not-leader-due-to-too-few-peers-3"
      (let [discovery 2
            has-leadership? (constantly true)
            leader? (leader-fn-factory "router-1" has-leadership? discovery 3)]
        (is (not (leader?)) "not leader when too few peers")))))

(deftest test-metrics-request-handler
  (let [handlers {:metrics-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
        waiter-request?-fn (fn [_] true)
        ring-handler (wrap-handler-json-response (ring-handler-factory waiter-request?-fn handlers))]
    (testing "metrics-request-handler:all-metrics"
      (with-redefs [metrics/get-metrics (fn get-metrics [] {:data "metrics-from-get-metrics"})]
        (let [request {:headers {"accept" "application/json"}
                       :request-method :get
                       :uri "/metrics"}
              {:keys [body headers status]} (ring-handler request)]
          (is (= 200 status))
          (is (= {"content-type" "application/json"} headers))
          (is (str/includes? (str body) "metrics-from-get-metrics")))))

    (testing "metrics-request-handler:all-metrics:error"
      (with-redefs [metrics/get-metrics (fn get-metrics [] (throw (Exception. "get-metrics")))]
        (let [request {:headers {"accept" "application/json"}
                       :request-method :get
                       :uri "/metrics"}
              {:keys [headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 500 status))
          (is (= {"content-type" "application/json"} headers)))))

    (testing "metrics-request-handler:waiter-metrics"
      (with-redefs [metrics/get-waiter-metrics (fn get-waiter-metrics-fn [] {:data (str "metrics-for-waiter")})]
        (let [request {:request-method :get, :uri "/metrics", :query-string "exclude-services=true"}
              {:keys [body headers status]} (ring-handler request)]
          (is (= 200 status))
          (is (= {"content-type" "application/json"} headers))
          (is (str/includes? (str body) "metrics-for-waiter")))))

    (testing "metrics-request-handler:service-metrics"
      (with-redefs [metrics/get-service-metrics (fn get-service-metrics [service-id] {:data (str "metrics-for-" service-id)})]
        (let [request {:request-method :get, :uri "/metrics", :query-string "service-id=abcd"}
              {:keys [body headers status]} (ring-handler request)]
          (is (= 200 status))
          (is (= {"content-type" "application/json"} headers))
          (is (str/includes? (str body) "metrics-for-abcd")))))

    (testing "metrics-request-handler:service-metrics:error"
      (with-redefs [metrics/get-service-metrics (fn get-service-metrics [_] (throw (Exception. "get-service-metrics")))]
        (let [request {:headers {"accept" "application/json"}
                       :request-method :get
                       :query-string "service-id=abcd"
                       :uri "/metrics"}
              {:keys [headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 500 status))
          (is (= {"content-type" "application/json"} headers)))))))

(deftest test-async-result-handler-call
  (testing "test-async-result-handler-call"
    (with-redefs [cors/handler (fn [handler _] handler)]
      (let [request {:authorization/user "test-user"
                     :request-method :get
                     :uri "/waiter-async/result/test-request-id/test-router-id/test-service-id/test-host/test-port/some/test/location"}
            response-map {:source :async-result-handler-fn}
            waiter-request?-fn (fn [_] true)
            handlers {:async-result-handler-fn
                      (fn [in-request]
                        (is (= request (select-keys in-request (keys request))))
                        response-map)
                      :handle-secure-request-fn ((:handle-secure-request-fn request-handlers)
                                                  {:routines {:authentication-method-wrapper-fn identity}
                                                   :state {:authenticator (auth/one-user-authenticator {})
                                                           :cors-validator []
                                                           :passwords []}})}]
        (is (= response-map ((ring-handler-factory waiter-request?-fn handlers) request)))))))

(deftest test-async-status-handler-call
  (testing "test-async-status-handler-call"
    (with-redefs [cors/handler (fn [handler _] handler)]
      (let [request {:authorization/user "test-user"
                     :request-method :get
                     :uri "/waiter-async/status/test-request-id/test-router-id/test-service-id/test-host/test-port/some/test/location"}
            response-map {:source :async-status-handler-fn}
            waiter-request?-fn (fn [_] true)
            handlers {:async-status-handler-fn
                      (fn [in-request]
                        (is (= request (select-keys in-request (keys request))))
                        response-map)
                      :handle-secure-request-fn ((:handle-secure-request-fn request-handlers)
                                                  {:routines {:authentication-method-wrapper-fn identity}
                                                   :state {:authenticator (auth/one-user-authenticator {})
                                                           :cors-validator []
                                                           :passwords []}})}]
        (is (= response-map ((ring-handler-factory waiter-request?-fn handlers) request)))))))

(deftest test-async-complete-handler-call
  (testing "test-async-complete-handler-call"
    (let [request {:authorization/user "test-user"
                   :request-method :get
                   :uri "/waiter-async/complete/request-id/test-service-id"}
          response-map {:source :async-complete-handler-fn}
          async-request-terminate-fn (Object.)]
      (with-redefs [handler/complete-async-handler
                    (fn [in-async-request-terminate-fn src-router-id in-request]
                      (is (= async-request-terminate-fn in-async-request-terminate-fn))
                      (is (= "router-id" src-router-id))
                      (is (= request (select-keys in-request (keys request))))
                      response-map)]
        (let [waiter-request?-fn (fn [_] true)
              configuration {:routines {:async-request-terminate-fn async-request-terminate-fn}
                             :handle-inter-router-request-fn (fn [handler request] (handler "router-id" request))}
              async-complete-handler-fn ((:async-complete-handler-fn request-handlers) configuration)
              handlers {:async-complete-handler-fn async-complete-handler-fn}]
          (is (= response-map ((ring-handler-factory waiter-request?-fn handlers) request))))))))

(deftest test-routes-mapper
  (let [exec-routes-mapper (fn [uri] (routes-mapper {:uri uri}))]
    (is (= {:handler :welcome-handler-fn}
           (exec-routes-mapper "/")))
    (is (= {:handler :app-name-handler-fn}
           (exec-routes-mapper "/app-name")))
    (is (= {:handler :service-list-handler-fn}
           (exec-routes-mapper "/apps")))
    (is (= {:handler :service-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/apps/test-service")))
    (is (= {:handler :service-view-logs-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/apps/test-service/logs")))
    (is (= {:handler :service-override-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/apps/test-service/override")))
    (is (= {:handler :service-refresh-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/apps/test-service/refresh")))
    (is (= {:handler :service-resume-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/apps/test-service/resume")))
    (is (= {:handler :service-suspend-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/apps/test-service/suspend")))
    (is (= {:handler :blacklist-instance-handler-fn}
           (exec-routes-mapper "/blacklist")))
    (is (= {:handler :blacklisted-instances-list-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/blacklist/test-service")))
    (is (= {:handler :favicon-handler-fn}
           (exec-routes-mapper "/favicon.ico")))
    (is (= {:handler :metrics-request-handler-fn}
           (exec-routes-mapper "/metrics")))
    (is (= {:handler :not-found-handler-fn}
           (exec-routes-mapper "/not-found"))) ; any path that isn't mapped
    (is (= {:handler :service-id-handler-fn}
           (exec-routes-mapper "/service-id")))
    (is (= {:handler :display-settings-handler-fn}
           (exec-routes-mapper "/settings")))
    (is (= {:handler :sim-request-handler}
           (exec-routes-mapper "/sim")))
    (is (= {:handler :state-all-handler-fn}
           (exec-routes-mapper "/state")))
    (is (= {:handler :state-kv-store-handler-fn}
           (exec-routes-mapper "/state/kv-store")))
    (is (= {:handler :state-leader-handler-fn}
           (exec-routes-mapper "/state/leader")))
    (is (= {:handler :state-maintainer-handler-fn}
           (exec-routes-mapper "/state/maintainer")))
    (is (= {:handler :state-router-metrics-handler-fn}
           (exec-routes-mapper "/state/router-metrics")))
    (is (= {:handler :state-scheduler-handler-fn}
           (exec-routes-mapper "/state/scheduler")))
    (is (= {:handler :state-statsd-handler-fn}
           (exec-routes-mapper "/state/statsd")))
    (is (= {:handler :state-service-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/state/test-service")))
    (is (= {:handler :status-handler-fn}
           (exec-routes-mapper "/status")))
    (is (= {:handler :token-handler-fn}
           (exec-routes-mapper "/token")))
    (is (= {:handler :token-list-handler-fn}
           (exec-routes-mapper "/tokens")))
    (is (= {:handler :token-owners-handler-fn}
           (exec-routes-mapper "/tokens/owners")))
    (is (= {:handler :token-refresh-handler-fn}
           (exec-routes-mapper "/tokens/refresh")))
    (is (= {:handler :token-reindex-handler-fn}
           (exec-routes-mapper "/tokens/reindex")))
    (is (= {:handler :async-complete-handler-fn, :route-params {:request-id "test-request-id", :service-id "test-service-id"}}
           (exec-routes-mapper "/waiter-async/complete/test-request-id/test-service-id")))
    (is (= {:handler :async-result-handler-fn
            :route-params {:host "test-host"
                           :location "some/test/location"
                           :port "test-port"
                           :request-id "test-request-id"
                           :router-id "test-router-id"
                           :service-id "test-service-id"}}
           (exec-routes-mapper "/waiter-async/result/test-request-id/test-router-id/test-service-id/test-host/test-port/some/test/location?a=b")))
    (is (= {:handler :async-status-handler-fn
            :route-params {:host "test-host"
                           :location "some/test/location"
                           :port "test-port"
                           :request-id "test-request-id"
                           :router-id "test-router-id"
                           :service-id "test-service-id"}}
           (exec-routes-mapper "/waiter-async/status/test-request-id/test-router-id/test-service-id/test-host/test-port/some/test/location?a=b")))
    (is (= {:handler :waiter-auth-handler-fn}
           (exec-routes-mapper "/waiter-auth")))
    (is (= {:handler :waiter-acknowledge-consent-handler-fn}
           (exec-routes-mapper "/waiter-consent")))
    (is (= {:handler :waiter-request-consent-handler-fn
            :route-params {:path ""}}
           (exec-routes-mapper "/waiter-consent/")))
    (is (= {:handler :waiter-request-consent-handler-fn
            :route-params {:path "simple-path"}}
           (exec-routes-mapper "/waiter-consent/simple-path")))
    (is (= {:handler :waiter-request-consent-handler-fn
            :route-params {:path "simple-path"}}
           (exec-routes-mapper "/waiter-consent/simple-path?with=params")))
    (is (= {:handler :waiter-request-consent-handler-fn
            :route-params {:path "nested/path/example"}}
           (exec-routes-mapper "/waiter-consent/nested/path/example")))
    (is (= {:handler :waiter-request-consent-handler-fn
            :route-params {:path "nested/path/example"}}
           (exec-routes-mapper "/waiter-consent/nested/path/example?with=params")))
    (is (= {:handler :kill-instance-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/waiter-kill-instance/test-service")))
    (is (= {:handler :work-stealing-handler-fn}
           (exec-routes-mapper "/work-stealing")))))

(deftest test-delegate-instance-kill-request
  (let [service-id "service-id"]

    (testing "no-peers-available"
      (let [router-id->endpoint {}
            make-kill-instance-request-fn (fn [_ _] (is false "Unexpected call to make-kill-instance-request-fn") {})]
        (is (not (delegate-instance-kill-request service-id router-id->endpoint make-kill-instance-request-fn)))))

    (testing "one-peer-available-unable-to-kill"
      (let [router-ids #{"peer-1"}
            make-kill-instance-request-fn (fn [dest-router-id dest-endpoint]
                                            (is (= (str "waiter-kill-instance/" service-id) dest-endpoint))
                                            (is (= dest-router-id "peer-1"))
                                            {})]
        (is (not (delegate-instance-kill-request service-id router-ids make-kill-instance-request-fn)))))

    (testing "three-peers-available-none-able-to-kill"
      (let [router-ids #{"peer-1" "peer-2" "peer-3"}
            requested-router-ids-atom (atom #{})
            make-kill-instance-request-fn (fn [dest-router-id dest-endpoint]
                                            (swap! requested-router-ids-atom (fn [s] (conj s dest-router-id)))
                                            (is (= (str "waiter-kill-instance/" service-id) dest-endpoint))
                                            {})]
        (is (not (delegate-instance-kill-request service-id router-ids make-kill-instance-request-fn)))
        (is (= router-ids @requested-router-ids-atom))))

    (testing "three-peers-available-first-able-to-kill"
      (let [router-ids #{"peer-1" "peer-2" "peer-3"}
            make-kill-instance-request-count-atom (atom 0)
            make-kill-instance-request-fn (fn [_ dest-endpoint]
                                            (swap! make-kill-instance-request-count-atom inc)
                                            (is (= (str "waiter-kill-instance/" service-id) dest-endpoint))
                                            (when (= 1 @make-kill-instance-request-count-atom)
                                              {:status 200}))]
        (is (delegate-instance-kill-request service-id router-ids make-kill-instance-request-fn))
        (is (= 1 @make-kill-instance-request-count-atom))))

    (testing "three-peers-available-last-able-to-kill"
      (let [router-ids #{"peer-1" "peer-2" "peer-3"}
            make-kill-instance-request-count-atom (atom 0)
            make-kill-instance-request-fn (fn [_ dest-endpoint]
                                            (swap! make-kill-instance-request-count-atom inc)
                                            (is (= (str "waiter-kill-instance/" service-id) dest-endpoint))
                                            (when (= (count router-ids) @make-kill-instance-request-count-atom)
                                              {:status 200}))]
        (is (delegate-instance-kill-request service-id router-ids make-kill-instance-request-fn))
        (is (= (count router-ids) @make-kill-instance-request-count-atom))))))

(deftest test-delegate-instance-kill-request-routine
  (let [my-router-id "my-router-id"
        service-id "service-id"
        discovery (Object.)
        make-inter-router-requests-sync-fn (Object.)
        configuration {:curator {:discovery discovery}
                       :make-inter-router-requests-sync-fn make-inter-router-requests-sync-fn
                       :state {:router-id my-router-id}}
        delegate-instance-kill-request-fn ((:delegate-instance-kill-request-fn routines) configuration)
        router-ids #{"peer-1" "peer-2" "peer-3"}
        make-kill-instance-peer-ids-atom (atom #{})]
    (with-redefs [discovery/router-ids (fn [in-discovery exclude-set-key exclude-set-value]
                                         (is (= discovery in-discovery))
                                         (is (= :exclude-set exclude-set-key))
                                         (is (= #{my-router-id} exclude-set-value))
                                         router-ids)
                  make-kill-instance-request (fn [in-make-inter-router-requests-fn in-service-id dest-router-id kill-instance-endpoint]
                                               (swap! make-kill-instance-peer-ids-atom (fn [s] (conj s dest-router-id)))
                                               (is (= make-inter-router-requests-sync-fn in-make-inter-router-requests-fn))
                                               (is (= service-id in-service-id))
                                               (is (= (str "waiter-kill-instance/" service-id) kill-instance-endpoint))
                                               false)]
      (delegate-instance-kill-request-fn service-id)
      (is (= router-ids @make-kill-instance-peer-ids-atom)))))

(deftest test-handle-authentication-wrapper-fn
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        configuration {:curator {:kv-store kv-store}
                       :state {:waiter-hostnames #{"www.waiter-router.com"}}}
        handle-authentication-wrapper-fn ((:handle-authentication-wrapper-fn request-handlers) configuration)
        handler-response (Object.)
        execute-request (fn execute-request-fn [test-request]
                          (let [request-handler-argument-atom (atom nil)
                                test-request-handler (fn request-handler-fn [request]
                                                       (reset! request-handler-argument-atom request)
                                                       handler-response)
                                test-response (handle-authentication-wrapper-fn test-request-handler test-request)]
                            {:handled-request @request-handler-argument-atom
                             :response test-response}))]

    (kv/store kv-store "www.token-1.com" {"cpu" 1, "mem" 2048})
    (kv/store kv-store "www.token-2.com" {"authentication" "standard", "cpu" 1, "mem" 2048})
    (kv/store kv-store "www.token-3.com" {"authentication" "disabled", "cpu" 1, "mem" 2048})
    (kv/store kv-store "a-named-token-A" {"cpu" 1, "mem" 2048})
    (kv/store kv-store "a-named-token-B" {"authentication" "disabled", "cpu" 1, "mem" 2048})
    (kv/store kv-store "a-named-token-C" {"authentication" "standard", "cpu" 1, "mem" 2048})

    (testing "request-without-non-existing-hostname-token"
      (let [test-request {:headers {"host" "www.host.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= test-request handled-request))
        (is (= handler-response response))))

    (testing "request-without-non-existing-hostname-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.host.com", "x-waiter-run-as-user" "test-user"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= test-request handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-non-auth-hostname-token"
      (let [test-request {:headers {"host" "www.token-1.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= test-request handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-disabled-hostname-token"
      (let [test-request {:headers {"host" "www.token-3.com"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :skip-authentication true) handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-disabled-hostname-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.token-3.com", "x-waiter-run-as-user" "test-user"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/map->json-response {:error "An authentication disabled token may not be combined with on-the-fly headers"}
                                         :status 400)
               response))))

    (testing "request-without-existing-non-auth-named-token"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-token" "a-named-token-A"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= test-request handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-non-auth-named-token-with-authentication-header"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-token" "a-named-token-A", "x-waiter-authentication" "disabled"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/map->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                         :status 400)
               response))))

    (testing "request-without-existing-auth-disabled-named-token"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-token" "a-named-token-B"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= (assoc test-request :skip-authentication true) handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-disabled-named-token-with-authentication-header"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-authentication" "disabled", "x-waiter-token" "a-named-token-B"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/map->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                         :status 400)
               response))))

    (testing "request-without-existing-auth-disabled-named-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-run-as-user" "test-user", "x-waiter-token" "a-named-token-B"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/map->json-response {:error "An authentication disabled token may not be combined with on-the-fly headers"}
                                         :status 400)
               response))))

    (testing "request-without-existing-auth-default-named-token"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-token" "a-named-token-C"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= test-request handled-request))
        (is (= handler-response response))))

    (testing "request-without-existing-auth-default-named-token-with-authentication-header"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-authentication" "disabled", "x-waiter-token" "a-named-token-C"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (nil? handled-request))
        (is (= (utils/map->json-response {:error "An authentication parameter is not supported for on-the-fly headers"}
                                         :status 400)
               response))))

    (testing "request-without-existing-auth-default-named-token-with-on-the-fly-headers"
      (let [test-request {:headers {"host" "www.service.com", "x-waiter-run-as-user" "test-user", "x-waiter-token" "a-named-token-C"}}
            {:keys [handled-request response]} (execute-request test-request)]
        (is (= test-request handled-request))
        (is (= handler-response response))))))

(deftest test-authentication-method-wrapper-fn
  (let [standard-handler (fn [_] {:source :standard-handler})]
    (let [authenticator (reify auth/Authenticator
                          (wrap-auth-handler [_ request-handler]
                            (is (= standard-handler request-handler))
                            (fn [_]
                              {:source :spnego-handler})))
          authenticate-request-handler ((:authentication-method-wrapper-fn routines) {:state {:authenticator authenticator}})
          request-handler (authenticate-request-handler standard-handler)]

      (testing "skip-authentication"
        (is (= {:source :standard-handler} (request-handler {:skip-authentication true, :headers {}}))))

      (testing "require-authentication"
        (is (= {:source :spnego-handler} (request-handler {:headers {}})))))))

(deftest test-waiter-request?-fn
  (testing "string hostname config"
    (let [config {:state {:waiter-hostnames #{"waiter-host"}}}
          waiter-request?-fn ((:waiter-request?-fn routines) config)]
      (is (waiter-request?-fn {:headers {"host" "waiter-host"}}))
      (is (not (waiter-request?-fn {:headers {"host" "waiter-host-1"}}))))))
