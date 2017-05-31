;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
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
            [waiter.core :refer :all]
            [waiter.cors :as cors]
            [waiter.curator :as curator]
            [waiter.discovery :as discovery]
            [waiter.handler :as handler]
            [waiter.kerberos :as kerberos]
            [waiter.kv :as kv]
            [waiter.marathon :as marathon]
            [waiter.metrics :as metrics]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.spnego :as spnego]
            [waiter.utils :as utils])
  (:import clojure.lang.ExceptionInfo
           java.io.StringBufferInputStream))

(defn request
  [resource request-method & params]
  {:request-method request-method :uri resource :params (first params)})

(deftest test-peers-acknowledged-blacklist-requests?
  (let [my-router-id "my-router-id"
        service-id "service-id"
        instance-id "service-id.instance-id"
        passwords [[:cached "cached-password"]]
        test-cases (list
                     {:name "no-routers-in-input"
                      :short-circuit true
                      :router-id->endpoint {}
                      :expected-result true
                      :expected-routers-connected []}
                     {:name "all-routers-approve-kill"
                      :short-circuit true
                      :router-id->endpoint {"abcd" "http://success", "bcde" "http://success", "defg" "http://success"}
                      :expected-result true
                      :expected-routers-connected ["abcd" "bcde" "defg"]}
                     {:name "second-router-vetoes-kill-with-short-circuit"
                      :short-circuit true
                      :router-id->endpoint {"abcd" "http://success", "bcde" "http://fail", "defg" "http://success"}
                      :expected-result false
                      :expected-routers-connected ["abcd" "bcde"]}
                     {:name "second-router-vetoes-kill-without-short-circuit"
                      :short-circuit false
                      :router-id->endpoint {"abcd" "http://success", "bcde" "http://fail", "defg" "http://success"}
                      :expected-result false
                      :expected-routers-connected ["abcd" "bcde" "defg"]})]
    (doseq [{:keys [name short-circuit router-id->endpoint expected-result expected-routers-connected]} test-cases]
      (testing (str "Test " name)
        (let [invoked-routers-atom (atom [])
              make-blacklist-request-fn (fn [in-dest-router-id in-dest-endpoint in-my-router-id in-secret-word instance reason]
                                          (swap! invoked-routers-atom conj in-dest-router-id)
                                          (is (= (utils/generate-secret-word in-my-router-id in-dest-router-id passwords) in-secret-word))
                                          (is (= service-id (:service-id instance)))
                                          (is (= instance-id (:id instance)))
                                          (is (= "blacklist-reason" reason))
                                          (if (str/includes? in-dest-endpoint "fail") {:status 400} {:status 200}))
              actual-result (peers-acknowledged-blacklist-requests?
                              my-router-id {:id instance-id, :service-id service-id} passwords short-circuit router-id->endpoint
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
        can-run-as? #(= %1 %2)
        waiter-request?-fn (fn [_] true)
        allowed-to-manage-service? (fn [service-id auth-user]
                                     (sd/can-manage-service? kv-store service-id can-run-as? auth-user))
        make-inter-router-requests-fn (fn [path _ _] (is (str/includes? path "service-id-")))
        configuration {:curator {:kv-store kv-store}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:allowed-to-manage-service?-fn allowed-to-manage-service?
                                  :can-run-as? can-run-as?
                                  :make-inter-router-requests-fn make-inter-router-requests-fn
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
        can-run-as? #(= %1 %2)
        waiter-request?-fn (fn [_] true)
        allowed-to-manage-service? (fn [service-id auth-user]
                                     (sd/can-manage-service? kv-store service-id can-run-as? auth-user))
        make-inter-router-requests-fn (fn [path _ _] (is (str/includes? path "service-id-")))
        configuration {:curator {:kv-store kv-store}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:allowed-to-manage-service?-fn allowed-to-manage-service?
                                  :can-run-as? can-run-as?
                                  :make-inter-router-requests-fn make-inter-router-requests-fn
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

(deftest test-check-has-prestashed-tickets
  (let [query-chan (async/chan 1)]
    (testing "returns error for user without tickets"
      (with-redefs [kerberos/is-prestashed? (fn [_] false)]
        (async/go
          (let [{:keys [response-chan]} (async/<! query-chan)]
            (async/>! response-chan #{})))
        (try
          (utils/load-messages {:prestashed-tickets-not-available "Prestashed tickets"})
          (check-has-prestashed-tickets query-chan {:service-description {"run-as-user" "kuser"}})
          (is false "Expected exception to be thrown")
          (catch ExceptionInfo e
            (let [{:keys [status message supress-logging]} (ex-data e)]
              (is (= 403 status))
              (is supress-logging "Exception should be thrown with supress-logging")
              (is (str/includes? message "Prestashed tickets"))
              (is (str/includes? message "kuser")))))))
    (testing "queries on cache miss"
      (with-redefs [kerberos/is-prestashed? (fn [_] false)]
        (async/go
          (let [{:keys [response-chan]} (async/<! query-chan)]
            (async/>! response-chan #{"kuser"})))
        (is (nil? (check-has-prestashed-tickets query-chan {:service-description {"run-as-user" "kuser"}})))))
    (testing "returns nil on query timeout"
      (with-redefs [kerberos/is-prestashed? (fn [_] false)]
        (is (nil? (check-has-prestashed-tickets (async/chan 1) {:service-description {"run-as-user" "kuser"}})))))
    (testing "returns nil for a user with tickets"
      (with-redefs [kerberos/is-prestashed? (fn [_] true)]
        (is (nil? (check-has-prestashed-tickets query-chan {})))))))

(deftest test-service-view-logs-handler
  (let [scheduler (marathon/->MarathonScheduler nil 5051 (fn [] nil) "/slave/directory" "/home/path/" (atom {}) (atom {}) 0)
        configuration {:handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:prepend-waiter-url identity}
                       :state {:scheduler scheduler}}
        handlers {:service-view-logs-handler-fn ((:service-view-logs-handler-fn request-handlers) configuration)}
        waiter-request?-fn (fn [_] true)
        test-service-id "test-service-id"
        user "test-user"]
    (with-redefs [clj-http.client/get (fn [_ _] {:body "{}"})]
      (testing "Missing instance id"
        (let [request {:uri (str "/apps/" test-service-id "/logs")
                       :request-method :get
                       :query-string ""
                       :authorization/user user}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= status 500))
          (is (str/includes? body "Instance id is missing!"))))
      (testing "Missing host"
        (let [request {:uri (str "/apps/" test-service-id "/logs")
                       :request-method :get
                       :query-string "instance-id=instance-id-1"
                       :authorization/user user}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= status 500))
          (is (str/includes? body "Host is missing!"))))
      (testing "Missing directory"
        (let [request {:uri (str "/apps/" test-service-id "/logs")
                       :request-method :get
                       :query-string "instance-id=instance-id-1&host=test.host.com"
                       :authorization/user user}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= status 500))
          (is (str/includes? body "No directory found for instance!")))))
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
        (let [request {:uri (str "/apps/" test-service-id "/logs")
                       :request-method :get
                       :query-string "instance-id=instance-id-1&host=test.host.com"
                       :authorization/user user}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= status 500))
          (is (str/includes? body "No directory found for instance!"))))
      (testing "Valid response"
        (let [request {:uri (str "/apps/" test-service-id "/logs")
                       :request-method :get
                       :query-string "instance-id=service-id-1.instance-id-2&host=test.host.com"
                       :authorization/user user}
              {:keys [status body]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= status 200))
          (is (every? #(str/includes? body %) ["test.host.com" "5051" "file" "directory" "name" "url" "download"])))))))

(deftest test-apps-handler-delete
  (let [user "test-user"
        service-id "test-service-1"
        can-run-as? =
        waiter-request?-fn (fn [_] true)
        configuration {:curator {:kv-store nil}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:can-run-as?-fn can-run-as?
                                  :make-inter-router-requests-fn nil
                                  :prepend-waiter-url nil}
                       :state {:router-id "router-id"
                               :scheduler (Object.)}}
        handlers {:service-handler-fn ((:service-handler-fn request-handlers) configuration)}]
    (testing "service-handler:delete-successful"
      (with-redefs [scheduler/delete-app (fn [_ service-id] {:deploymentId "12389132987", :service-id service-id})
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 200 status))
          (is (= {"Content-Type" "application/json"} headers))
          (is (= {"success" true, "service-id" service-id, "deploymentId" "12389132987"} (json/read-str body))))))
    (testing "service-handler:delete-nil-response"
      (with-redefs [scheduler/delete-app (fn [_ _] nil)
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 400 status))
          (is (= {"Content-Type" "application/json"} headers))
          (is (= {"success" false, "service-id" service-id} (json/read-str body))))))
    (testing "service-handler:delete-unauthorized-user"
      (with-redefs [scheduler/delete-app (fn [_ _] (throw (IllegalStateException. "Unexpected call!")))
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" (str "another-" user), "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 403 status))
          (is (= {"Content-Type" "application/json"} headers))
          (is (str/includes? body "User not allowed to delete service")))))
    (testing "service-handler:delete-404-response"
      (with-redefs [scheduler/delete-app (fn [_ _] {:result :no-such-service-exists})
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 404 status))
          (is (= {"Content-Type" "application/json"} headers))
          (is (= {"result" "no-such-service-exists", "service-id" service-id, "success" false} (json/read-str body))))))
    (testing "service-handler:delete-non-existent-service"
      (with-redefs [scheduler/delete-app (fn [_ _] (throw (IllegalStateException. "Unexpected call!")))
                    sd/fetch-core (fn [_ _ & _] {})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 404 status))
          (is (= {"Content-Type" "application/json"} headers))
          (is (= {"message" "No service description found: test-service-1"} (json/read-str body))))))
    (testing "service-handler:delete-throws-exception"
      (with-redefs [scheduler/delete-app (fn [_ _] (throw (RuntimeException. "Error in deleting service")))
                    sd/fetch-core (fn [_ service-id & _] {"run-as-user" user, "name" (str service-id "-name")})]
        (let [request {:request-method :delete, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 400 status))
          (is (= {"Content-Type" "application/json"} headers))
          (is (str/includes? body "Error in deleting service")))))))

(deftest test-apps-handler-get
  (let [user "waiter-user"
        service-id "test-service-1"
        can-run-as? =
        waiter-request?-fn (fn [_] true)
        configuration {:curator {:kv-store nil}
                       :handle-secure-request-fn (fn [handler request] (handler request))
                       :routines {:can-run-as?-fn can-run-as?
                                  :make-inter-router-requests-fn nil
                                  :prepend-waiter-url #(str "http://www.example.com" %)}
                       :state {:router-id "router-id"
                               :scheduler (Object.)}}
        handlers {:service-handler-fn ((:service-handler-fn request-handlers) configuration)}]
    (testing "service-handler:get-missing-service-description"
      (with-redefs [sd/fetch-core (constantly nil)]
        (let [request {:request-method :get, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 404 status))
          (is (= {"Content-Type" "application/json"} headers))
          (let [body-json (json/read-str (str body))]
            (is (= body-json {"message" "No service description found: test-service-1"}))))))
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
        (let [request {:request-method :get, :uri (str "/apps/" service-id), :authorization/user user}
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 200 status))
          (is (= {"Content-Type" "application/json"} headers))
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
              {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
          (is (= 200 status))
          (is (= {"Content-Type" "application/json"} headers))
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
  (let [my-router-id "router-0"
        discovery "dicovery-object!"
        instance-request-properties {:connection-timeout-ms 1, :initial-socket-timeout-ms 1}
        passwords ["password-1" "password-2"]]
    (with-redefs [discovery/router-id->endpoint-url
                  (fn [_ protocol endpoint & {:keys [exclude-set] :or {exclude-set #{}}}]
                    (pc/map-from-keys (fn [router-id]
                                        (str protocol "://" router-id "/" endpoint))
                                      (remove #(contains? exclude-set %)
                                              ["router-0", "router-1", "router-2", "router-3", "router-4"])))]
      (testing "make-call-to-all-other-routers"
        (let [urls-invoked-atom (atom [])]
          (with-redefs [clj-http.client/request (fn [config]
                                                  (is (contains? config :basic-auth))
                                                  (swap! urls-invoked-atom conj (:url config)))]
            (make-inter-router-requests my-router-id discovery instance-request-properties passwords
                                        "test/endpoint1")
            (is (= 4 (count @urls-invoked-atom)))
            (is (= 1 (count (filter #(= "http://router-1/test/endpoint1" %) @urls-invoked-atom))))
            (is (= 1 (count (filter #(= "http://router-2/test/endpoint1" %) @urls-invoked-atom))))
            (is (= 1 (count (filter #(= "http://router-3/test/endpoint1" %) @urls-invoked-atom))))
            (is (= 1 (count (filter #(= "http://router-4/test/endpoint1" %) @urls-invoked-atom)))))))
      (testing "filter-some-routers"
        (let [urls-invoked-atom (atom [])]
          (with-redefs [clj-http.client/request (fn [{:keys [url]}] (swap! urls-invoked-atom conj url))]
            (make-inter-router-requests my-router-id discovery instance-request-properties passwords
                                        "test/endpoint2"
                                        :acceptable-router? (fn [router-id] (some #(str/includes? router-id %) ["0" "1" "2" "4"])))
            (is (= 3 (count @urls-invoked-atom)))
            (is (= 1 (count (filter #(= "http://router-1/test/endpoint2" %) @urls-invoked-atom))))
            (is (= 1 (count (filter #(= "http://router-2/test/endpoint2" %) @urls-invoked-atom))))
            (is (= 1 (count (filter #(= "http://router-4/test/endpoint2" %) @urls-invoked-atom)))))))
      (testing "filter-all-routers"
        (let [urls-invoked-atom (atom [])]
          (with-redefs [clj-http.client/request (fn [{:keys [url]}] (swap! urls-invoked-atom conj url))]
            (make-inter-router-requests my-router-id discovery instance-request-properties passwords
                                        "test/endpoint3"
                                        :acceptable-router? (fn [router-id] (some #(str/includes? router-id %) ["A" "B" "C"])))
            (is (= 0 (count @urls-invoked-atom)))))))))

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

(deftest test-thread-dump-handler
  (testing "thread-dump-handler:success-no-params"
    (let [request {:request-method :get, :uri "/waiter-thread-dump"}
          retrieve-stale-thread-stack-trace-data (fn [excluded-methods stale-threshold-ms]
                                                   (is (= [] excluded-methods))
                                                   (is (zero? stale-threshold-ms))
                                                   {"thread-1-key" ["thread-1-content"]})
          waiter-request?-fn (fn [_] true)
          configuration {:routines {:retrieve-stale-thread-stack-trace-data retrieve-stale-thread-stack-trace-data}}
          handlers {:thread-dump-handler-fn ((:thread-dump-handler-fn request-handlers) configuration)}
          {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
      (is (= 200 status))
      (is (= {"Content-Type" "application/json"} headers))
      (is (= {"thread-1-key" ["thread-1-content"]} (json/read-str (str body))))))
  (testing "thread-dump-handler:success-excluded-params"
    (let [request {:request-method :get, :uri "/waiter-thread-dump", :query-string "excluded-methods=foo,bar,baz"}
          retrieve-stale-thread-stack-trace-data (fn [excluded-methods stale-threshold-ms]
                                                   (is (= ["foo" "bar" "baz"] excluded-methods))
                                                   (is (zero? stale-threshold-ms))
                                                   {"thread-2-key" ["thread-2-content"]})
          waiter-request?-fn (fn [_] true)
          configuration {:routines {:retrieve-stale-thread-stack-trace-data retrieve-stale-thread-stack-trace-data}}
          handlers {:thread-dump-handler-fn ((:thread-dump-handler-fn request-handlers) configuration)}
          {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
      (is (= 200 status))
      (is (= {"Content-Type" "application/json"} headers))
      (is (= {"thread-2-key" ["thread-2-content"]} (json/read-str (str body))))))
  (testing "thread-dump-handler:success-threshold-params"
    (let [request {:request-method :get, :uri "/waiter-thread-dump", :query-string "stale-threshold-ms=10"}
          retrieve-stale-thread-stack-trace-data (fn [excluded-methods stale-threshold-ms]
                                                   (is (= [] excluded-methods))
                                                   (is (= 10 stale-threshold-ms))
                                                   {"thread-3-key" ["thread-3-content"]})
          waiter-request?-fn (fn [_] true)
          configuration {:routines {:retrieve-stale-thread-stack-trace-data retrieve-stale-thread-stack-trace-data}}
          handlers {:thread-dump-handler-fn ((:thread-dump-handler-fn request-handlers) configuration)}
          {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
      (is (= 200 status))
      (is (= {"Content-Type" "application/json"} headers))
      (is (= {"thread-3-key" ["thread-3-content"]} (json/read-str (str body))))))
  (testing "thread-dump-handler:failure-threshold-params"
    (let [request {:request-method :get, :uri "/waiter-thread-dump", :query-string "stale-threshold-ms=foo"}
          retrieve-stale-thread-stack-trace-data (fn [_ _]
                                                   (is false "Unexpected call!"))
          waiter-request?-fn (fn [_] true)
          configuration {:routines {:retrieve-stale-thread-stack-trace-data retrieve-stale-thread-stack-trace-data}}
          handlers {:thread-dump-handler-fn ((:thread-dump-handler-fn request-handlers) configuration)}
          {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
      (is (= 500 status))
      (is (= {"Content-Type" "application/json"} headers))
      (is (str/includes? (str body) "java.lang.NumberFormatException"))))
  (testing "thread-dump-handler:success-both-params"
    (let [request {:request-method :get, :uri "/waiter-thread-dump", :query-string "excluded-methods=foo,bar,baz&stale-threshold-ms=10"}
          retrieve-stale-thread-stack-trace-data (fn [excluded-methods stale-threshold-ms]
                                                   (is (= ["foo" "bar" "baz"] excluded-methods))
                                                   (is (= 10 stale-threshold-ms))
                                                   {"thread-4-key" ["thread-4-content"]})
          waiter-request?-fn (fn [_] true)
          configuration {:routines {:retrieve-stale-thread-stack-trace-data retrieve-stale-thread-stack-trace-data}}
          handlers {:thread-dump-handler-fn ((:thread-dump-handler-fn request-handlers) configuration)}
          {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
      (is (= 200 status))
      (is (= {"Content-Type" "application/json"} headers))
      (is (= {"thread-4-key" ["thread-4-content"]} (json/read-str (str body)))))))

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
  (testing "metrics-request-handler:all-metrics"
    (with-redefs [metrics/get-metrics (fn get-metrics [] {:data "metrics-from-get-metrics"})]
      (let [request {:request-method :get, :uri "/metrics"}
            waiter-request?-fn (fn [_] true)
            handlers {:metrics-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "metrics-from-get-metrics")))))

  (testing "metrics-request-handler:all-metrics:error"
    (with-redefs [metrics/get-metrics (fn get-metrics [] (throw (Exception. "get-metrics")))]
      (let [request {:request-method :get, :uri "/metrics"}
            waiter-request?-fn (fn [_] true)
            handlers {:metrics-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 500 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "get-metrics")))))

  (testing "metrics-request-handler:waiter-metrics"
    (with-redefs [metrics/get-waiter-metrics (fn get-waiter-metrics-fn [] {:data (str "metrics-for-waiter")})]
      (let [request {:request-method :get, :uri "/metrics", :query-string "exclude-services=true"}
            waiter-request?-fn (fn [_] true)
            handlers {:metrics-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "metrics-for-waiter")))))

  (testing "metrics-request-handler:service-metrics"
    (with-redefs [metrics/get-service-metrics (fn get-service-metrics [service-id] {:data (str "metrics-for-" service-id)})]
      (let [request {:request-method :get, :uri "/metrics", :query-string "service-id=abcd"}
            waiter-request?-fn (fn [_] true)
            handlers {:metrics-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "metrics-for-abcd")))))

  (testing "metrics-request-handler:service-metrics:error"
    (with-redefs [metrics/get-service-metrics (fn get-service-metrics [_] (throw (Exception. "get-service-metrics")))]
      (let [request {:request-method :get, :uri "/metrics", :query-string "service-id=abcd"}
            waiter-request?-fn (fn [_] true)
            handlers {:metrics-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 500 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "get-service-metrics"))))))

(deftest test-stats-request-handler
  (testing "stats-request-handler:resolution"
    (let [metrics-request-handler-fn (Object.)]
      (is (= metrics-request-handler-fn
             ((:stats-request-handler-fn request-handlers)
               {:metrics-request-handler-fn metrics-request-handler-fn})))))

  (testing "stats-request-handler:all-metrics"
    (with-redefs [metrics/get-metrics (fn get-metrics [] {:data "metrics-from-get-metrics"})]
      (let [request {:request-method :get, :uri "/stats"}
            waiter-request?-fn (fn [_] true)
            handlers {:stats-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "metrics-from-get-metrics")))))

  (testing "stats-request-handler:all-metrics:error"
    (with-redefs [metrics/get-metrics (fn get-metrics [] (throw (Exception. "get-metrics")))]
      (let [request {:request-method :get, :uri "/stats"}
            waiter-request?-fn (fn [_] true)
            handlers {:stats-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 500 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "get-metrics")))))

  (testing "stats-request-handler:waiter-metrics"
    (with-redefs [metrics/get-waiter-metrics (fn get-waiter-metrics-fn [] {:data (str "metrics-for-waiter")})]
      (let [request {:request-method :get, :uri "/stats", :query-string "exclude-services=true"}
            waiter-request?-fn (fn [_] true)
            handlers {:stats-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "metrics-for-waiter")))))

  (testing "stats-request-handler:service-metrics"
    (with-redefs [metrics/get-service-metrics (fn get-service-metrics [service-id] {:data (str "metrics-for-" service-id)})]
      (let [request {:request-method :get, :uri "/stats", :query-string "service-id=abcd"}
            waiter-request?-fn (fn [_] true)
            handlers {:stats-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 200 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "metrics-for-abcd")))))

  (testing "stats-request-handler:service-metrics:error"
    (with-redefs [metrics/get-service-metrics (fn get-service-metrics [_] (throw (Exception. "get-service-metrics")))]
      (let [request {:request-method :get, :uri "/stats", :query-string "service-id=abcd"}
            waiter-request?-fn (fn [_] true)
            handlers {:stats-request-handler-fn ((:metrics-request-handler-fn request-handlers) {})}
            {:keys [body headers status]} ((ring-handler-factory waiter-request?-fn handlers) request)]
        (is (= 500 status))
        (is (= {"Content-Type" "application/json"} headers))
        (is (str/includes? (str body) "get-service-metrics"))))))

(deftest test-async-result-handler-call
  (testing "test-async-result-handler-call"
    (with-redefs [cors/handler (fn [handler _] handler)
                  spnego/require-gss (fn [handler _] handler)]
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
                                                  {:state {:cors-validator [], :passwords []}})}]
        (is (= response-map ((ring-handler-factory waiter-request?-fn handlers) request)))))))

(deftest test-async-status-handler-call
  (testing "test-async-status-handler-call"
    (with-redefs [cors/handler (fn [handler _] handler)
                  spnego/require-gss (fn [handler _] handler)]
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
                                                  {:state {:cors-validator [], :passwords []}})}]
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
    (is (= {:handler :process-request-fn} ;; outdated mapping
           (exec-routes-mapper "/kill-instance")))
    (is (= {:handler :metrics-request-handler-fn}
           (exec-routes-mapper "/metrics")))
    (is (= {:handler :process-request-fn}
           (exec-routes-mapper "/secrun")))
    (is (= {:handler :service-id-handler-fn}
           (exec-routes-mapper "/service-id")))
    (is (= {:handler :display-settings-handler-fn}
           (exec-routes-mapper "/settings")))
    (is (= {:handler :sim-request-handler}
           (exec-routes-mapper "/sim")))
    (is (= {:handler :display-state-handler-fn}
           (exec-routes-mapper "/state")))
    (is (= {:handler :service-state-handler-fn, :route-params {:service-id "test-service"}}
           (exec-routes-mapper "/state/test-service")))
    (is (= {:handler :stats-request-handler-fn}
           (exec-routes-mapper "/stats")))
    (is (= {:handler :status-handler-fn}
           (exec-routes-mapper "/status")))
    (is (= {:handler :token-handler-fn}
           (exec-routes-mapper "/token")))
    (is (= {:handler :token-refresh-handler-fn, :route-params {:token "test-token"}}
           (exec-routes-mapper "/token/test-token/refresh")))
    (is (= {:handler :process-request-fn}
           (exec-routes-mapper "/through-to-backend")))
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
    (is (= {:handler :thread-dump-handler-fn}
           (exec-routes-mapper "/waiter-thread-dump")))
    (is (= {:handler :work-stealing-handler-fn}
           (exec-routes-mapper "/work-stealing")))))
