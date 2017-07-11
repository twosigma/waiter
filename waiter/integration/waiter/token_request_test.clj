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
(ns waiter.token-request-test
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [waiter.client-tools :refer :all])
  (:import java.net.URL))

(defn- waiter-url->token-url
  [waiter-url]
  (str HTTP-SCHEME waiter-url "/token"))

(deftest ^:parallel ^:integration-fast test-update-token-cache-consistency
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name "testhostname")
          token (rand-name)
          update-token-fn (fn [version]
                            (let [{:keys [status]} (http/post (waiter-url->token-url waiter-url)
                                                              {:headers {}
                                                               :throw-exceptions false
                                                               :spnego-auth true
                                                               :body (json/write-str
                                                                       {:name service-id-prefix
                                                                        :cpus 1
                                                                        :mem 1250
                                                                        :version version
                                                                        :cmd "test command"
                                                                        :health-check-url "/ping"
                                                                        :permitted-user "*"
                                                                        :run-as-user (retrieve-username)
                                                                        :token token})})]
                              (is (= 200 status))))
          validate-token-fn (fn [version num-threads num-iter]
                              (parallelize-requests
                                num-threads
                                num-iter
                                (fn []
                                  (let [request-headers (clojure.walk/stringify-keys {:x-waiter-token token})
                                        request-url (waiter-url->token-url waiter-url)
                                        {:keys [body status]} (http/get request-url
                                                                        {:headers request-headers
                                                                         :spnego-auth true
                                                                         :throw-exceptions false})]
                                    (is (= 200 status))
                                    (is (every? #(str/includes? (str body) (str %)) [service-id-prefix "1250" version "test command"]))
                                    (is (not-any? #(str/includes? (str body) (str %)) ["invalid"]))))))
          update-and-validate-token-fn (fn [version]
                                         (log/info (str "Creating configuration using token " token " with version " version))
                                         (update-token-fn version)
                                         (log/info (str "Asserting configuration for token " token " from routers (best-effort)"))
                                         (validate-token-fn version 10 10))
          version1 "123987132937213712"
          version2 "656760465406467480"
          version3 "678219671796032121"]
      (try
        (update-and-validate-token-fn version1)
        (update-and-validate-token-fn version2)
        (update-and-validate-token-fn version3)
        (finally
          (delete-token-and-assert waiter-url token))))))

(defn- name-from-service-description [waiter-url service-id]
  (get-in (service-settings waiter-url service-id) [:service-description :name]))

(defn- create-token-name
  [waiter-url service-id-prefix]
  (str service-id-prefix "." (subs waiter-url 0 (str/index-of waiter-url ":"))))

(defn- post-token
  [waiter-url post-body]
  (make-request waiter-url "/token" :http-method-fn http/post :headers {"host" (:token post-body)} :body (json/write-str post-body)))

(defn- get-token
  [waiter-url token & {:keys [cookies] :or {cookies {}}}]
  (let [request-headers (clojure.walk/stringify-keys {:host token})
        token-response (make-request waiter-url "/token" :headers request-headers :cookies cookies)]
    (log/debug "Retrieved token" token ":" (:body token-response))
    token-response))

(defn- list-tokens
  [waiter-url owner & {:keys [cookies] :or {cookies {}}}]
  (let [tokens-response (make-request waiter-url "/tokens" :cookies cookies)]
    (log/debug "Retrieved tokens for owner " owner ":" (:body tokens-response))
    tokens-response))

(deftest ^:parallel ^:integration-fast test-token-create-delete
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token-prefix (create-token-name waiter-url service-id-prefix)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          num-tokens-to-create 10
          tokens-to-create (map #(str "token" %1 "." token-prefix) (range num-tokens-to-create))
          current-user (System/getProperty "user.name")]
      (log/info "creating the tokens")
      (doseq [token tokens-to-create]
        (let [{:keys [body status]} (post-token waiter-url {:health-check-url "/custom-endpoint"
                                                            :token token
                                                            :name service-id-prefix})]
          (is (= 200 status) (str "Error: " body))))
      (log/info "ensuring tokens can be retrieved and listed on each router")
      (doseq [token tokens-to-create]
        (doseq [[_ router-url] (routers waiter-url)]
          (let [{:keys [body] :as token-response} (get-token router-url token :cookies cookies)
                token-description (try (json/read-str (str body))
                                       (catch Exception _
                                         (is false (str "Failed to parse token " body))))]
            (assert-response-status token-response 200)
            (is (= "/custom-endpoint" (token-description "health-check-url")))
            (is (= service-id-prefix (token-description "name")))
            (is current-user (token-description "owner"))
            (is (not-any? #(contains? token-description %) ["cmd" "cpus" "mem" "run-as-user" "permitted-user"])
                (str "Unexpected content in token response: " body)))
          (let [{:keys [body] :as tokens-response} (list-tokens router-url current-user :cookies cookies)
                tokens (json/read-str body)]
            (assert-response-status tokens-response 200)
            (is (some (fn [token-entry] (= token (get token-entry "token"))) tokens)))))
      (log/info "deleting the tokens")
      (doseq [token tokens-to-create]
        (delete-token-and-assert waiter-url token))
      (log/info "ensuring tokens can no longer be retrieved on each router")
      (doseq [token tokens-to-create]
        (doseq [[router-id router-url] (routers waiter-url)]
          (let [router-state (router-state router-url :cookies cookies)
                cache-data (get-in router-state [:kv-store :cache :data])
                token-cache-data (get cache-data (keyword token))]
            (is (nil? token-cache-data)
                (str token " data not nil (" token-cache-data ") on " router-id ", cache data =" cache-data)))
          (let [{:keys [body] :as response} (get-token router-url token :cookies cookies)]
            (assert-response-status response 404)
            (is (str/includes? (str body) "couldn't find token") (str body)))
          (let [{:keys [body] :as tokens-response} (list-tokens router-url current-user :cookies cookies)
                tokens (json/read-str body)]
            (assert-response-status tokens-response 200)
            (is (not-any? (fn [token-entry] (= token (get token-entry "token"))) tokens))))))))

(deftest ^:parallel ^:integration-fast test-hostname-token
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token (create-token-name waiter-url service-id-prefix)]
      (testing "hostname-token-test"
        (try
          (log/info (str "Basic hostname as token test"))
          (let [current-user (retrieve-username)
                service-description (assoc (kitchen-request-headers) :x-waiter-permitted-user "*"
                                                                     :x-waiter-run-as-user current-user)]
            (testing "hostname-token-creation"
              (log/info (str "Creating configuration using token " token))
              (let [{:keys [body status]} (post-token waiter-url {:health-check-url "/custom-endpoint"
                                                                  :token token
                                                                  :name service-id-prefix})]
                (when (not= 200 status)
                  (log/info (str "Error: " body))
                  (is (not body))))
              (log/info (str "Created configuration using token " token))
              (let [token-response (get-token waiter-url token)
                    response-body (json/read-str (:body token-response))]
                (is (= {"health-check-url" "/custom-endpoint", "name" service-id-prefix,
                        "token-metadata" {"owner" (retrieve-username)}}
                       response-body)))
              (log/info (str "Asserted retrieval of configuration for token " token)))

            (testing "support-for-token-with-x-waiter-headers"
              (log/info "Request with hostname token" token "along with x-waiter headers")
              (let [request-headers (merge service-description {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix)))

              (log/info "Request with hostname token" token "along with x-waiter headers except permitted-user")
              (let [request-headers (merge (dissoc service-description :x-waiter-permitted-user) {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
                ;; the above request hashes to a different service-id than the rest of the test, so we need to cleanup
                (delete-service waiter-url service-id))

              (log/info "Request with hostname token" token "along with x-waiter headers except run-as-user")
              (let [request-headers (merge (dissoc service-description :x-waiter-run-as-user) {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix))

                (testing "backend request headers"
                  (let [{:keys [body] :as response} (make-request waiter-url "/request-info" :headers request-headers)
                        {:strs [headers]} (json/read-str (str body))]
                    (assert-response-status response 200)
                    (is (contains? headers "x-waiter-auth-principal"))
                    (is (contains? headers "x-waiter-authenticated-principal"))))

                ;; the above request hashes to a different service-id only when running as someone other than current-user
                ;; when the service-id is different, we need to cleanup
                (when (not= (System/getProperty "user.name") current-user)
                  (delete-service waiter-url service-id)))

              (log/info "Request with hostname token" token "along with missing x-waiter headers except cmd")
              (let [request-headers (merge (dissoc service-description :x-waiter-cmd) {"host" token})
                    path "/foo"
                    {:keys [body] :as response} (make-request waiter-url path :headers request-headers)]
                (is (or (every? #(str/includes? body %)
                                ["Service description using waiter headers/token improperly configured",
                                 "issue: {\"cmd\" missing-required-key}"])
                        (str/includes? body "Invalid command or version"))
                    (str "response body was: " response))
                (assert-response-status response 400))

              (log/info "Request with hostname token and x-waiter-debug token" token " along with x-waiter headers")
              (let [request-headers (merge service-description {:x-waiter-debug "true", "host" token})
                    path "/foo"
                    {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url request-headers)]
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
                (is (every? #(not (str/blank? (get headers %)))
                            (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
                    (str headers))
                (delete-service waiter-url service-id))))
          (finally
            (delete-token-and-assert waiter-url token)))))))

(deftest ^:parallel ^:integration-fast test-named-token
  (testing-using-waiter-url
    (log/info (str "Basic named token test"))
    (let [service-id-prefix (rand-name)
          token (create-token-name waiter-url service-id-prefix)]
      (try
        (log/info (str "Creating configuration using token " token))
        (let [token-definition (assoc
                                 (kitchen-request-headers :prefix "")
                                 :name service-id-prefix
                                 :token token)
              {:keys [body status]} (post-token waiter-url token-definition)]
          (when (not= 200 status)
            (log/info (str "Error: " body))
            (is (not body))))
        (log/info (str "Created configuration using token " token))
        (log/info (str "Retrieving configuration for token " token))
        (let [token-response (get-token waiter-url token)
              response-body (str (:body token-response))]
          (when (not (str/includes? response-body service-id-prefix))
            (log/info response-body))
          (assert-response-status token-response 200)
          (is (str/includes? response-body service-id-prefix)))
        (log/info (str "Asserted retrieval of configuration for token " token))

        (log/info (str "Making Waiter request with token " token " in header"))
        (let [request-headers {:x-waiter-token token}
              path "/foo"
              response (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url (:request-headers response))]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix)))

        (log/info (str "Making Waiter request with token and x-waiter-debug token" token " in header"))
        (let [request-headers {:x-waiter-debug "true", :x-waiter-token token}
              path "/foo"
              {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url request-headers)]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (every? #(not (str/blank? (get headers %)))
                      (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
              (str headers))
          (delete-service waiter-url service-id))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-star-run-as-user-token
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token (create-token-name waiter-url service-id-prefix)]
      (try
        (log/info (str "Creating configuration using token " token))
        (let [token-definition (assoc
                                 (kitchen-request-headers :prefix "")
                                 :name service-id-prefix
                                 :run-as-user "*"
                                 :token token)
              {:keys [body status]} (post-token waiter-url token-definition)]
          (when (not= 200 status)
            (log/info (str "Error: " body))
            (is (not body))))
        (log/info (str "Created configuration using token " token))
        (log/info (str "Retrieving configuration for token " token))
        (let [token-response (get-token waiter-url token)
              response-body (str (:body token-response))]
          (when (not (str/includes? response-body service-id-prefix))
            (log/info response-body))
          (assert-response-status token-response 200)
          (is (str/includes? response-body service-id-prefix)))
        (log/info (str "Asserted retrieval of configuration for token " token))

        (let [{:keys [body] :as token-response} (get-token waiter-url token)
              token-description (try (json/read-str (str body))
                                     (catch Exception _
                                       (is false (str "Failed to parse token " body))))]
          (assert-response-status token-response 200)
          (is (= "/status" (token-description "health-check-url")))
          (is (= service-id-prefix (token-description "name")))
          (is (= "*" (token-description "run-as-user")))
          (is (System/getProperty "user.name") (token-description "owner")))

        (log/info (str "Making Waiter request with token " token " in header"))
        (let [request-headers {:x-waiter-token token}
              path "/foo"
              response (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url (:request-headers response))]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (= (retrieve-username) (:run-as-user (service-id->service-description waiter-url service-id)))))

        (log/info (str "Making Waiter request with token and x-waiter-debug token" token " in header"))
        (let [request-headers {:x-waiter-debug "true", :x-waiter-token token}
              path "/foo"
              {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url request-headers)]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (every? #(not (str/blank? (get headers %)))
                      (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
              (str headers))
          (delete-service waiter-url service-id))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-on-the-fly-to-token
  (testing-using-waiter-url
    (let [name-string (rand-name)
          canary-response (make-kitchen-request waiter-url {:x-waiter-name name-string})
          service-id (retrieve-service-id waiter-url (:request-headers canary-response))]
      (is (str/includes? service-id name-string) (str "ERROR: App-name is missing " name-string))
      (is (= 200 (:status (http/post (str "http://" waiter-url) {:headers {:x-waiter-token (str "^SERVICE-ID#" service-id)}
                                                                 :throw-exceptions false
                                                                 :spnego-auth true}))))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-bad-token
  (testing-using-waiter-url

    (testing "can't use bad token"
      (let [response (http/get (str "http://" waiter-url "/pathabc") {:spnego-auth true
                                                                      :headers {"X-Waiter-Token" "bad#token"}
                                                                      :throw-exceptions false})]
        (is (str/includes? (:body response) "No service description template available for token bad#token"))
        (assert-response-status response 400)))

    (testing "can't create bad token"
      (let [service-desc {:name (rand-name "notused")
                          :cpus 1
                          :debug true
                          :mem 1024
                          :version "universe b10452d0b0380ce61764543847085631ee3d7af9"
                          :token "bad#token"
                          :cmd "not-used"
                          :permitted-user "*"
                          :run-as-user (retrieve-username)
                          :health-check-url "/not-used"}
            response (http/post (str "http://" waiter-url "/token") {:spnego-auth true
                                                                     :throw-exceptions false
                                                                     :body (json/write-str (clojure.walk/stringify-keys service-desc))})]
        (is (str/includes? (:body response) "Token must match pattern"))
        (assert-response-status response 400)))))

(deftest ^:parallel ^:integration-fast test-token-metadata
  (testing-using-waiter-url
    (let [token (rand-name)
          service-desc {"name" token
                        "cpus" 1
                        "mem" 100
                        "version" "1"
                        "cmd-type" "shell"
                        "token" token
                        "cmd" "exit 0"
                        "run-as-user" (retrieve-username)
                        "health-check-url" "/not-used"
                        "metadata" {"a" "b", "c" "d"}}
          register-response (http/post (str "http://" waiter-url "/token") {:spnego-auth true
                                                                            :throw-exceptions false
                                                                            :body (json/write-str service-desc)})
          {:keys [body]} (http/get (str "http://" waiter-url "/token") {:spnego-auth true
                                                                        :throw-exceptions false
                                                                        :headers {"X-Waiter-Token" (get service-desc "token")}})]
      (assert-response-status register-response 200)
      (is (= (get service-desc "metadata") (get (json/read-str body) "metadata")))
      (delete-token-and-assert waiter-url token)
      (delete-service waiter-url (:name service-desc)))))

(deftest ^:parallel ^:integration-fast test-token-bad-metadata
  (testing-using-waiter-url
    (let [service-desc {"name" "token-bad-metadata"
                        "cpus" 1
                        "mem" 100
                        "version" "1"
                        "cmd-type" "shell"
                        "token" (rand-name)
                        "cmd" "exit 0"
                        "run-as-user" (retrieve-username)
                        "health-check-url" "/not-used"
                        "metadata" {"a" "b", "c" {"d" "e"}}}
          register-response (http/post (str "http://" waiter-url "/token") {:spnego-auth true
                                                                            :throw-exceptions false
                                                                            :body (json/write-str service-desc)})]
      (is (= 400 (:status register-response))))))

(deftest ^:parallel ^:integration-fast test-token-environment-variables
  (testing-using-waiter-url
    (let [token (rand-name)
          binary (kitchen-cmd)
          token-response (http/post (waiter-url->token-url waiter-url)
                                    {:body (json/write-str
                                             (clojure.walk/stringify-keys
                                               {:cmd "$BINARY -p $PORT0"
                                                :cmd-type "shell"
                                                :version "does-not-matter"
                                                :name token
                                                :env {"BINARY" binary}
                                                :token token}))
                                     :spnego-auth true
                                     :throw-exceptions false})
          {:keys [service-id status] :as response} (make-request-with-debug-info {:x-waiter-token token}
                                                                                 #(make-light-request waiter-url %))
          {:keys [env] :as service-description} (response->service-description waiter-url response)]
      (is (= 200 (:status token-response)) (:body token-response))
      (is (= 200 status))
      (is (= {:BINARY binary} env) (str service-description))
      (delete-token-and-assert waiter-url token)
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-token-invalid-environment-variables
  (testing-using-waiter-url
    (let [{:keys [body status]} (http/post (waiter-url->token-url waiter-url)
                                           {:body (json/write-str (clojure.walk/stringify-keys
                                                                    {:env {"HOME" "/my/home"}
                                                                     :token (rand-name)}))
                                            :spnego-auth true
                                            :throw-exceptions false})]
      (is (= 400 status))
      (is (not (str/includes? body "clojure")) body)
      (is (str/includes? body "The following environment variable keys are reserved: HOME.") body))))

(deftest ^:parallel ^:integration-fast test-auto-run-as-requester-support
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url service-name)
          service-description (-> (kitchen-request-headers :prefix "")
                                  (assoc :name service-name :permitted-user "*")
                                  (dissoc :run-as-user))
          waiter-port (.getPort (URL. (str "http://" waiter-url)))
          waiter-port (if (neg? waiter-port) 80 waiter-port)
          host-header (str token ":" waiter-port)]
      (try
        (testing "token creation"
          (let [token-description (assoc service-description :token token)
                response (post-token waiter-url token-description)]
            (assert-response-status response 200)))

        (testing "token retrieval"
          (let [token-response (get-token waiter-url token)
                response-body (-> token-response (:body) (json/read-str) (pc/keywordize-map))]
            (is (nil? (get response-body :run-as-user)))
            (is (= (assoc service-description :token-metadata {:owner (retrieve-username)})
                   response-body))))

        (testing "expecting redirect"
          (let [{:keys [body headers] :as response} (make-request waiter-url "/hello-world" :headers {"host" host-header})]
            (is (str/includes? body "service-description-error"))
            (is (= (str "/waiter-consent/hello-world")
                   (get headers "Location")))
            (assert-response-status response 303)))

        (testing "waiter-consent"
          (let [consent-path (str "/waiter-consent/hello-world")
                {:keys [body] :as response} (make-request waiter-url consent-path :headers {"host" host-header})
                service-id (when-let [service-id-index (str/index-of body "name=\"service-id\"")]
                             (when-let [value-index (str/index-of body "value=\"" service-id-index)]
                               (when-let [end-index (str/index-of body "\"" (+ value-index 7))]
                                 (subs body (+ value-index 7) end-index))))]
            (is service-id)
            (assert-response-status response 200)

            (let [cookies-atom (atom nil)]
              (testing "approval of specific service"
                (let [{:keys [body cookies] :as response}
                      (make-request waiter-url "/waiter-consent"
                                    :headers {"host" host-header
                                              "referer" (str "http://" host-header)
                                              "origin" (str "http://" host-header)
                                              "x-requested-with" "XMLHttpRequest"}
                                    :http-method-fn http/post
                                    :multipart [{:name "mode" :content "service"}
                                                {:name "service-id" :content service-id}])]
                  (reset! cookies-atom cookies)
                  (is (= "Added cookie x-waiter-consent" body))
                  (is (every? #(contains? cookies %1) ["x-waiter-auth" "x-waiter-consent"]))
                  (assert-response-status response 200)))

              (testing "auto run-as-user population on expected service-id"
                (let [service-id-atom (atom nil)]
                  (try
                    (let [expected-service-id service-id
                          {:keys [body cookies service-id] :as response}
                          (make-request-with-debug-info {"host" host-header}
                                                        #(make-request waiter-url "/hello-world" :cookies @cookies-atom :headers %1))
                          {:keys [service-description]} (service-settings waiter-url service-id)
                          {:keys [run-as-user permitted-user]} service-description]
                      (reset! service-id-atom service-id)
                      (is (= "Hello World" body))
                      (is (every? #(contains? cookies %1) ["x-waiter-auth" "x-waiter-consent"]))
                      (is (= expected-service-id service-id))
                      (is (not (str/blank? permitted-user)))
                      (is (= run-as-user permitted-user))
                      (assert-response-status response 200))
                    (finally
                      (when @service-id-atom
                        (delete-service waiter-url @service-id-atom))))))

              (testing "token update"
                (let [updated-service-name (rand-name)
                      token-description (assoc service-description :name updated-service-name :token token)
                      response (post-token waiter-url token-description)]
                  (assert-response-status response 200)))

              (testing "expecting redirect after token update"
                (let [{:keys [body headers] :as response} (make-request waiter-url "/hello-world" :cookies @cookies-atom :headers {"host" host-header})]
                  (is (str/includes? body "service-description-error"))
                  (is (= (str "/waiter-consent/hello-world")
                         (get headers "Location")))
                  (assert-response-status response 303)))

              (testing "approval of token"
                (let [{:keys [body cookies] :as response}
                      (make-request waiter-url "/waiter-consent"
                                    :cookies @cookies-atom
                                    :headers {"host" host-header
                                              "referer" (str "http://" host-header)
                                              "origin" (str "http://" host-header)
                                              "x-requested-with" "XMLHttpRequest"}
                                    :http-method-fn http/post
                                    :multipart [{:name "mode" :content "token"}])]
                  (is (not= @cookies-atom cookies))
                  (reset! cookies-atom cookies)
                  (is (= "Added cookie x-waiter-consent" body))
                  (is (every? #(contains? cookies %1) ["x-waiter-auth" "x-waiter-consent"]))
                  (assert-response-status response 200)))

              (testing "auto run-as-user population on approved token"
                (let [service-id-atom (atom nil)]
                  (try
                    (let [previous-service-id service-id
                          {:keys [body cookies service-id] :as response}
                          (make-request-with-debug-info {"host" host-header}
                                                        #(make-request waiter-url "/hello-world" :cookies @cookies-atom :headers %1))]
                      (reset! service-id-atom service-id)
                      (is (= "Hello World" body))
                      (is (every? #(contains? cookies %1) ["x-waiter-auth" "x-waiter-consent"]))
                      (is (not= previous-service-id service-id))
                      (assert-response-status response 200))
                    (finally
                      (when @service-id-atom
                        (delete-service waiter-url @service-id-atom)))))))))

        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-authentication-disabled-support
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url service-name)
          current-user (retrieve-username)
          service-description (-> (kitchen-request-headers :prefix "")
                                  (assoc :authentication "disabled" :name service-name :permitted-user "*" :run-as-user current-user))
          request-headers {:x-waiter-token token}]
      (try
        (testing "token creation"
          (let [token-description (assoc service-description :token token)
                response (post-token waiter-url token-description)]
            (assert-response-status response 200)))

        (testing "token retrieval"
          (let [token-response (get-token waiter-url token)
                response-body (-> token-response (:body) (json/read-str) (pc/keywordize-map))]
            (is (= (assoc service-description :authentication "disabled" :token-metadata {:owner current-user})
                   response-body))))

        (testing "successful request"
          (let [{:keys [body] :as response} (make-request waiter-url "/hello-world" :headers request-headers :spnego-auth false)]
            (assert-response-status response 200)
            (is (= "Hello World" body))))

        (testing "backend request headers"
          (let [{:keys [body] :as response} (make-request waiter-url "/request-info" :headers request-headers :spnego-auth false)
                {:strs [headers]} (json/read-str (str body))]
            (assert-response-status response 200)
            (is (not (contains? headers "x-waiter-auth-principal")))
            (is (not (contains? headers "x-waiter-authenticated-principal")))
            (is (contains? headers "x-cid"))))

        (finally
          (delete-token-and-assert waiter-url token))))))
