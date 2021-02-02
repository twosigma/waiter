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
(ns waiter.token-request-test
  (:require [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [waiter.headers :as headers]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.token :as token]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (java.net URL)
           (org.joda.time DateTime)))

(defn- waiter-url->token-url
  [waiter-url]
  (str HTTP-SCHEME waiter-url "/token"))

(defn- retrieve-token-root
  [waiter-url]
  (-> (waiter-settings waiter-url)
      (get-in [:cluster-config :name])))

(defn- retrieve-token-cluster
  [waiter-url]
  (let [settings-json (waiter-settings waiter-url)
        default-cluster (get-in settings-json [:cluster-config :name])
        cluster-calculator-config (-> settings-json
                                      (get-in [:token-config :cluster-calculator])
                                      (update :kind keyword)
                                      (as-> $ (update-in $ [(:kind $) :factory-fn] symbol)))
        cluster-calculator (utils/create-component cluster-calculator-config
                                                   :context {:default-cluster default-cluster})]
    (token/calculate-cluster cluster-calculator {:headers {"host" waiter-url}})))

(deftest ^:parallel ^:integration-fast test-update-token-cache-consistency
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token (rand-name)
          update-token-fn (fn [cmd]
                            (let [response (post-token waiter-url {:cmd cmd
                                                                   :cpus 1
                                                                   :health-check-url "/ping"
                                                                   :mem 1250
                                                                   :name service-id-prefix
                                                                   :permitted-user "*"
                                                                   :run-as-user (retrieve-username)
                                                                   :token token})]
                              (assert-response-status response http-200-ok)))
          validate-token-fn (fn [cmd num-threads num-iter]
                              (parallelize-requests
                                num-threads
                                num-iter
                                (fn []
                                  (let [{:keys [body] :as response} (get-token waiter-url token)]
                                    (assert-response-status response http-200-ok)
                                    (is (every? #(str/includes? (str body) (str %)) [service-id-prefix "1250" cmd "/ping"]))
                                    (is (not-any? #(str/includes? (str body) (str %)) ["invalid"]))))))
          update-and-validate-token-fn (fn [cmd]
                                         (log/info "creating configuration using token" token "with cmd" cmd)
                                         (update-token-fn cmd)
                                         (log/info "asserting configuration for token" token "from routers (best-effort)")
                                         (validate-token-fn cmd 10 10))
          cmd1 "123987132937213712"
          cmd2 "656760465406467480"
          cmd3 "678219671796032121"]
      (try
        (update-and-validate-token-fn cmd1)
        (update-and-validate-token-fn cmd2)
        (update-and-validate-token-fn cmd3)
        (finally
          (delete-token-and-assert waiter-url token))))))

(defn- name-from-service-description [waiter-url service-id]
  (get-in (service-settings waiter-url service-id) [:service-description :name]))

(defn- service-id->source-tokens-entries [waiter-url service-id]
  (some-> (service-settings waiter-url service-id)
          :source-tokens
          walk/stringify-keys
          set))

(defn- make-source-tokens-entries [waiter-url & tokens]
  (mapv (fn [token] {"token" token "version" (token->etag waiter-url token)}) tokens))

(defn- list-tokens
  [waiter-url owner cookies query-params]
  (let [tokens-response (make-request waiter-url "/tokens" :cookies cookies :query-params query-params)]
    (log/debug "retrieved tokens for owner " owner ":" (:body tokens-response))
    tokens-response))

(defn- parse-token-description
  "Parses a response as json and keywordizes the map."
  [response-body]
  (try
    (-> response-body str json/read-str pc/keywordize-map)
    (catch Exception _
      (is false (str "Failed to parse token " response-body)))))

(defmacro assert-token-response
  "Asserts the token data in the response"
  ([response token-root token-cluster service-id-prefix deleted]
   `(assert-token-response ~response ~token-root ~token-cluster ~service-id-prefix ~deleted true))
  ([response token-root token-cluster service-id-prefix deleted include-metadata]
   `(let [body# (:body ~response)
          token-description# (parse-token-description body#)]
      (assert-response-status ~response http-200-ok)
      (when ~include-metadata
        (is (contains? token-description# :cluster))
        (is (contains? token-description# :last-update-time))
        (is (contains? token-description# :last-update-user))
        (is (contains? token-description# :owner))
        (is (contains? token-description# :root)))
      (is (= (cond-> {:health-check-url "/probe"
                      :name ~service-id-prefix
                      :owner (retrieve-username)}
               ~include-metadata (assoc :cluster ~token-cluster
                                        :last-update-user (retrieve-username)
                                        :root ~token-root)
               (and ~deleted ~include-metadata) (assoc :deleted ~deleted))
             (dissoc token-description# :last-update-time :previous))))))

(deftest ^:parallel ^:integration-fast test-token-create-delete
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token-prefix (create-token-name waiter-url ".")
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          num-tokens-to-create 10
          tokens-to-create (map #(str "token" %1 "." token-prefix) (range num-tokens-to-create))
          current-user (System/getProperty "user.name")
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)]

      (log/info "creating token without parameters should fail")
      (let [token (str service-id-prefix ".empty")
            {:keys [body] :as response} (post-token waiter-url {:token token})]
        (assert-response-status response http-400-bad-request)
        (is (str/includes? (str body) (str "No parameters provided for " token)) (str body)))

      (testing "creating and deleting token with only metadata should succeed"
        (let [token (str service-id-prefix ".fallback-period-secs")
              {:keys [body] :as response} (post-token waiter-url {:fallback-period-secs 60 :token token})]
          (assert-response-status response http-200-ok)
          (is (str/includes? (str body) (str "Successfully created " token)) (str body))
          (let [{:keys [body] :as response}  (get-token waiter-url token :cookies cookies :query-params {"token" token})]
            (assert-response-status response http-200-ok)
            (is (= {"fallback-period-secs" 60 "owner" (retrieve-username)} (json/read-str body)) (str body)))
          (delete-token-and-assert waiter-url token))

        (let [token (str service-id-prefix ".https-redirect")
              {:keys [body] :as response} (post-token waiter-url {:https-redirect true :token token})]
          (assert-response-status response http-200-ok)
          (is (str/includes? (str body) (str "Successfully created " token)) (str body))
          (let [{:keys [body] :as response}  (get-token waiter-url token :cookies cookies :query-params {"token" token})]
            (assert-response-status response http-200-ok)
            (is (= {"https-redirect" true "owner" (retrieve-username)} (json/read-str body)) (str body)))
          (delete-token-and-assert waiter-url token)))

      (log/info "creating the tokens")
      (doseq [token tokens-to-create]
        (let [response (post-token waiter-url {:health-check-url "/check"
                                               :name service-id-prefix
                                               :token token})]
          (assert-response-status response http-200-ok)
          (is (str/includes? (:body response) (str "Successfully created " token)))))

      (testing "update without etags"
        (doseq [token tokens-to-create]
          (let [response (post-token waiter-url {:health-check-url "/health"
                                                 :name service-id-prefix
                                                 :token token})]
            (assert-response-status response http-200-ok)
            (is (str/includes? (:body response) (str "Successfully updated " token))))))

      (testing "update with etags"
        (doseq [token tokens-to-create]
          (let [{:keys [body headers]} (get-token waiter-url token)
                token-description (-> body
                                      json/read-str
                                      walk/keywordize-keys
                                      (select-keys [:name])
                                      (assoc :health-check-url "/probe"
                                             :token token))
                response (post-token waiter-url token-description :headers {"if-match" (get headers "etag")})]
            (assert-response-status response http-200-ok)
            (is (str/includes? (:body response) (str "Successfully updated " token))))))

      (testing "update without changes"
        (doseq [token tokens-to-create]
          (let [{:keys [body headers]} (get-token waiter-url token)
                token-description (-> body
                                      json/read-str
                                      (select-keys sd/token-user-editable-keys)
                                      walk/keywordize-keys
                                      (assoc :token token))
                current-etag (get headers "etag")
                response (post-token waiter-url token-description :headers {"if-match" current-etag})]
            (assert-response-status response http-200-ok)
            (is (str/includes? (:body response) (str "No changes detected for " token)))
            (is (= current-etag (get-in response [:headers "etag"]))))))

      (testing "token retrieval - presence of etag header"
        (doseq [token tokens-to-create]
          (testing "via query parameters"
            (let [{:keys [headers] :as response} (get-token waiter-url token
                                                            :cookies cookies
                                                            :query-params {"include" "metadata"
                                                                           "token" token}
                                                            :request-headers {})
                  actual-etag (get headers "etag")]
              (assert-response-status response http-200-ok)
              (let [token-description (-> response :body json/read-str)]
                (is (= {"health-check-url" "/check", "name" service-id-prefix}
                       (-> token-description (get-in (repeat 2 "previous")) (select-keys sd/service-parameter-keys))))
                (is (= {"health-check-url" "/health", "name" service-id-prefix}
                       (-> token-description (get-in (repeat 1 "previous")) (select-keys sd/service-parameter-keys)))))
              (is actual-etag)))
          (testing "via x-waiter-token header"
            (let [{:keys [body headers] :as response} (get-token waiter-url token :cookies cookies)
                  actual-etag (get headers "etag")]
              (assert-response-status response http-200-ok)
              (is actual-etag)
              (when actual-etag
                (let [convert-last-update-time (fn [{:strs [last-update-time] :as service-description}]
                                                 (cond-> service-description
                                                   last-update-time
                                                   (assoc "last-update-time"
                                                          (-> last-update-time du/str-to-date .getMillis))))
                      expected-etag (str "E-"
                                         (-> body
                                             json/read-str
                                             convert-last-update-time
                                             (dissoc "previous")
                                             sd/parameters->id))]
                  (is (= expected-etag actual-etag))))))))

      (log/info "ensuring tokens can be retrieved and listed on each router")
      (doseq [token tokens-to-create]
        (doseq [[_ router-url] (routers waiter-url)]
          (-> (get-token router-url token :cookies cookies :query-params {"include" "none"})
              (assert-token-response token-root token-cluster service-id-prefix false false))
          (-> (get-token router-url token :cookies cookies :query-params {"include" "metadata"})
              (assert-token-response token-root token-cluster service-id-prefix false true))
          (let [{:keys [body] :as tokens-response}
                (list-tokens router-url current-user cookies {"include" ["deleted" "metadata"]})
                tokens (json/read-str body)]
            (assert-response-status tokens-response http-200-ok)
            (is (every? (fn [token-entry] (contains? token-entry "deleted")) tokens))
            (is (every? (fn [token-entry] (contains? token-entry "etag")) tokens))
            (is (some (fn [token-entry] (= token (get token-entry "token"))) tokens)))))

      (log/info "soft-deleting the tokens")
      (doseq [token tokens-to-create]
        (delete-token-and-assert waiter-url token :hard-delete false))

      (log/info "ensuring tokens can no longer be retrieved on each router without include=deleted parameter")
      (doseq [token tokens-to-create]
        (doseq [[router-id router-url] (routers waiter-url)]
          (let [router-state (kv-store-state router-url :cookies cookies)
                cache-data (get-in router-state ["state" "cache" "data"])
                token-cache-data (get cache-data (keyword token))]
            (is (nil? token-cache-data)
                (str token " data not nil (" token-cache-data ") on " router-id ", cache data =" cache-data)))
          (let [{:keys [body] :as response} (get-token router-url token :cookies cookies)]
            (assert-response-status response http-404-not-found)
            (is (str/includes? (str body) "Couldn't find token") (str body)))
          (-> (get-token router-url token
                         :cookies cookies
                         :query-params {"include" ["deleted" "metadata"]})
              (assert-token-response token-root token-cluster service-id-prefix true))
          (let [{:keys [body] :as tokens-response}
                (list-tokens router-url current-user cookies {"include" ["metadata"]})
                tokens (json/read-str body)]
            (assert-response-status tokens-response http-200-ok)
            (is (every? (fn [token-entry] (contains? token-entry "deleted")) tokens))
            (is (every? (fn [token-entry] (contains? token-entry "etag")) tokens))
            (is (not-any? (fn [token-entry] (= token (get token-entry "token"))) tokens)))
          (let [{:keys [body] :as tokens-response}
                (list-tokens router-url current-user cookies {"include" ["deleted" "metadata"]})
                tokens (json/read-str body)]
            (assert-response-status tokens-response http-200-ok)
            (is (every? (fn [token-entry] (contains? token-entry "deleted")) tokens))
            (is (every? (fn [token-entry] (contains? token-entry "etag")) tokens))
            (is (some (fn [token-entry] (= token (get token-entry "token"))) tokens)))))

      (log/info "hard-deleting the tokens")
      (doseq [token tokens-to-create]
        (delete-token-and-assert waiter-url token)
        (doseq [[_ router-url] (routers waiter-url)]
          (let [{:keys [body] :as response} (get-token router-url token :cookies cookies)]
            (assert-response-status response http-404-not-found)
            (is (str/includes? (str body) "Couldn't find token") (str {:body body :token token})))
          (let [{:keys [body] :as tokens-response}
                (list-tokens router-url current-user cookies {"include" ["deleted" "metadata"]})
                token-entries (json/read-str body)]
            (assert-response-status tokens-response http-200-ok)
            (is (every? (fn [token-entry] (contains? token-entry "deleted")) token-entries))
            (is (every? (fn [token-entry] (contains? token-entry "etag")) token-entries))))
        ;; the token must be removed from at least one router (others may have temporarily cached values)
        (is (some
              (fn [router-url]
                (let [{:keys [body]} (list-tokens router-url current-user cookies {"include" ["deleted" "metadata"]})
                      token-entries (json/read-str body)]
                  (not-any? (fn [token-entry] (= token (get token-entry "token"))) token-entries)))
              (vals (routers waiter-url)))
            (str token " entry found in list of deleted tokens on all routers")))

      (log/info "ensuring tokens can no longer be retrieved on each router with include=deleted parameter after hard-delete")
      (doseq [token tokens-to-create]
        (doseq [[_ router-url] (routers waiter-url)]
          (let [{:keys [body] :as response} (get-token router-url token
                                                       :cookies cookies
                                                       :query-params {"include" "deleted"})]
            (assert-response-status response http-404-not-found)
            (is (str/includes? (str body) "Couldn't find token") (str body))))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-service-list-filtering
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token-1 (create-token-name waiter-url "." (str service-name ".t1"))
          token-2 (create-token-name waiter-url "." (str service-name ".t2"))
          service-ids-atom (atom #{})
          token->version->etag-atom (atom {})
          all-tokens [token-1 token-2]
          all-version-suffixes ["v1" "v2" "v3"]
          router-id->router-url (routers waiter-url)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")]

      (try
        (doseq [version-suffix all-version-suffixes]
          (doseq [token all-tokens]
            (let [token-description (merge
                                      (kitchen-request-headers :prefix "")
                                      {:fallback-period-secs 0
                                       :name (str service-name "." (hash token))
                                       :token token
                                       :version (str service-name "." version-suffix)})
                  {:keys [headers] :as response} (post-token waiter-url token-description)]
              (assert-response-status response http-200-ok)
              (let [token-etag (get headers "etag")]
                (log/info token "->" token-etag)
                (is (-> token-etag str/blank? not))
                (let [{:keys [service-id] :as response} (make-request-with-debug-info
                                                          {:x-waiter-token token}
                                                          #(make-request waiter-url "/environment" :headers %))]
                  (assert-response-status response http-200-ok)
                  (is (-> service-id str/blank? not))
                  (swap! service-ids-atom conj service-id)
                  ;; ensure all routers know about the service
                  (doseq [[_ router-url] router-id->router-url]
                    (let [response (make-request-with-debug-info
                                     {:x-waiter-token token}
                                     #(make-request router-url "/environment" :headers % :cookies cookies))]
                      (assert-response-status response http-200-ok)
                      (is (= service-id (:service-id response))))))
                (swap! token->version->etag-atom assoc-in [token version-suffix] token-etag)))))
        (is (= (* (count all-tokens) (count all-version-suffixes)) (count @service-ids-atom))
            (str {:service-ids @service-ids-atom}))

        (doseq [service-id @service-ids-atom]
          (assert-service-on-all-routers waiter-url service-id cookies))

        (testing "star in token filter"
          (doseq [[_ router-url] (routers waiter-url)]
            (let [query-params {"token" (str service-name ".t*")}
                  _ (log/info query-params)
                  {:keys [body] :as response} (make-request router-url "/apps" :cookies cookies :query-params query-params)
                  services (json/read-str body)
                  service-tokens (mapcat (fn [entry]
                                           (some->> entry
                                             walk/keywordize-keys
                                             :source-tokens
                                             flatten
                                             (map :token)))
                                         services)]
              (assert-response-status response http-200-ok)
              (is (= (count @service-ids-atom) (count service-tokens))
                  (str {:query-params query-params
                        :router-url router-url
                        :service-count (count services)
                        :service-tokens service-tokens})))))

        (doseq [loop-token all-tokens]
          (doseq [[_ router-url] (routers waiter-url)]
            (let [query-params {"token" loop-token}
                  _ (log/info query-params)
                  {:keys [body] :as response} (make-request router-url "/apps" :cookies cookies :query-params query-params)
                  services (json/read-str body)
                  service-tokens (mapcat (fn [entry]
                                           (some->> entry
                                             walk/keywordize-keys
                                             :source-tokens
                                             flatten
                                             (map :token)))
                                         services)]
              (assert-response-status response http-200-ok)
              (is (= 3 (count service-tokens))
                  (str {:query-params query-params
                        :router-url router-url
                        :service-count (count services)
                        :service-tokens service-tokens}))))

          (doseq [version-suffix all-version-suffixes]
            (doseq [[_ router-url] (routers waiter-url)]
              (let [loop-etag (get-in @token->version->etag-atom [loop-token version-suffix])
                    query-params {"token-version" loop-etag}
                    _ (log/info query-params)
                    {:keys [body] :as response} (make-request router-url "/apps" :cookies cookies :query-params query-params)
                    services (json/read-str body)
                    service-token-versions (mapcat (fn [entry]
                                                     (some->> entry
                                                       walk/keywordize-keys
                                                       :source-tokens
                                                       flatten
                                                       (map :version)))
                                                   services)]
                (assert-response-status response http-200-ok)
                (is (= 1 (count service-token-versions))
                    (str {:query-params query-params
                          :router-url router-url
                          :service-count (count services)
                          :service-token-versions service-token-versions}))))))

        (finally
          (doseq [token all-tokens]
            (delete-token-and-assert waiter-url token))
          (doseq [service-id @service-ids-atom]
            (delete-service waiter-url service-id)))))))

(defn- service-id->metadata-id [waiter-url service-id]
  (get-in (service-settings waiter-url service-id) [:service-description :metadata :id]))

(deftest ^:parallel ^:integration-fast test-hostname-token
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token-prefix (str "token-" (rand-int 3000000))
          token (create-token-name waiter-url ".")
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)]
      (testing "hostname-token-test"
        (try
          (log/info "basic hostname as token test")
          (let [current-user (retrieve-username)
                service-description (assoc (kitchen-request-headers)
                                      :x-waiter-metric-group "waiter_test"
                                      :x-waiter-name token-prefix
                                      :x-waiter-permitted-user "*"
                                      :x-waiter-run-as-user current-user)]
            (testing "hostname-token-creation"
              (log/info "creating configuration using token" token)
              (let [{:keys [body status]}
                    (post-token waiter-url {:health-check-url "/probe"
                                            :metric-group "waiter_test"
                                            :metadata {"id" token-prefix}
                                            :name service-id-prefix
                                            :token token})]
                (when (not= http-200-ok status)
                  (log/info "error:" body)
                  (is (not body))))
              (log/info "created configuration using token" token)
              (let [token-response (get-token waiter-url token)
                    response-body (json/read-str (:body token-response))]
                (is (contains? response-body "last-update-time"))
                (is (= {"cluster" token-cluster
                        "health-check-url" "/probe"
                        "last-update-user" (retrieve-username)
                        "metadata" {"id" token-prefix}
                        "metric-group" "waiter_test"
                        "name" service-id-prefix
                        "owner" (retrieve-username)
                        "previous" {}
                        "root" token-root}
                       (dissoc response-body "last-update-time"))))
              (log/info "asserted retrieval of configuration for token" token))

            (testing "support-for-token-with-x-waiter-headers"
              (log/info "request with hostname token" token "along with x-waiter headers")
              (let [request-headers (merge service-description {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response http-200-ok)
                (is (= token-prefix (name-from-service-description waiter-url service-id)))
                (is (= #{(make-source-tokens-entries waiter-url token)}
                       (service-id->source-tokens-entries waiter-url service-id)))
                (is (= "waiter_test" (service-id->metric-group waiter-url service-id))
                    (str {:service-id service-id :token token}))
                (is (= token-prefix (service-id->metadata-id waiter-url service-id))))

              (log/info "request with hostname token" token "along with x-waiter headers except permitted-user")
              (let [request-headers (merge (dissoc service-description :x-waiter-permitted-user) {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response http-200-ok)
                (is (= token-prefix (name-from-service-description waiter-url service-id)))
                (is (= #{(make-source-tokens-entries waiter-url token)}
                       (service-id->source-tokens-entries waiter-url service-id)))
                (is (= "waiter_test" (service-id->metric-group waiter-url service-id))
                    (str {:service-id service-id :token token}))
                (is (= token-prefix (service-id->metadata-id waiter-url service-id)))
                ;; the above request hashes to a different service-id than the rest of the test, so we need to cleanup
                (delete-service waiter-url service-id))

              (log/info "request with hostname token" token "along with x-waiter headers except run-as-user")
              (let [request-headers (merge (dissoc service-description :x-waiter-run-as-user) {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response http-200-ok)
                (is (= token-prefix (name-from-service-description waiter-url service-id)))
                (is (= #{(make-source-tokens-entries waiter-url token)}
                       (service-id->source-tokens-entries waiter-url service-id)))
                (is (= "waiter_test" (service-id->metric-group waiter-url service-id))
                    (str {:service-id service-id :token token}))
                (is (= token-prefix (service-id->metadata-id waiter-url service-id)))

                (testing "backend request headers"
                  (let [{:keys [body] :as response} (make-request waiter-url "/request-info" :headers request-headers)
                        {:strs [headers]} (json/read-str (str body))]
                    (assert-response-status response http-200-ok)
                    (is (contains? headers "x-waiter-auth-principal"))
                    (is (contains? headers "x-waiter-authenticated-principal"))))

                ;; the above request hashes to a different service-id only when running as someone other than current-user
                ;; when the service-id is different, we need to cleanup
                (when (not= (System/getProperty "user.name") current-user)
                  (delete-service waiter-url service-id)))

              (log/info "request with hostname token" token "along with missing x-waiter headers except cmd")
              (let [request-headers (merge (dissoc service-description :x-waiter-cmd) {"host" token})
                    path "/foo"
                    {:keys [body] :as response} (make-request waiter-url path :headers request-headers)]
                (is (some #(str/includes? body %) ["cmd must be a non-empty string" "Invalid command or version"])
                    (str "response body was: " response))
                (assert-response-status response http-400-bad-request))

              (log/info "request with hostname token and x-waiter-debug token" token "along with x-waiter headers")
              (let [request-headers (merge service-description {:x-waiter-debug "true", "host" token})
                    path "/foo"
                    {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url request-headers)]
                (assert-response-status response http-200-ok)
                (with-service-cleanup
                  service-id
                  (is (= token-prefix (name-from-service-description waiter-url service-id)))
                  (is (= #{(make-source-tokens-entries waiter-url token)}
                         (service-id->source-tokens-entries waiter-url service-id)))
                  (is (every? #(not (str/blank? (get headers %)))
                              (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
                      (str headers))
                  (is (= "waiter_test" (service-id->metric-group waiter-url service-id))
                      (str {:service-id service-id :token token}))))))
          (finally
            (delete-token-and-assert waiter-url token)))))))

(deftest ^:parallel ^:integration-fast test-token-administer-unaffected-by-run-as-user-permissions
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")]
      (testing "token-administering"
        (testing "active-token"
          (let [last-update-time (System/currentTimeMillis)
                token (create-token-name waiter-url ".")]

            (let [token-description {:health-check-url "/probe"
                                     :name service-id-prefix
                                     :owner (retrieve-username)
                                     :run-as-user (retrieve-username)
                                     :token token}
                  response (post-token waiter-url token-description)]
              (assert-response-status response http-200-ok))

            (testing "if-match-required during update"
              (let [token-description {:health-check-url "/probe-1"
                                       :last-update-time last-update-time
                                       :name service-id-prefix
                                       :owner (retrieve-username)
                                       :token token}
                    {:keys [body] :as response} (post-token waiter-url token-description
                                                            :headers {}
                                                            :query-params {"update-mode" "admin"})]
                (assert-response-status response http-400-bad-request)
                (is (str/includes? body "Must specify if-match header for admin mode token updates"))))

            (try
              (testing "successful admin mode update"
                (log/info "creating configuration using token" token)
                (let [{:keys [body headers]} (get-token waiter-url token)
                      token-description (-> body
                                            json/read-str
                                            walk/keywordize-keys
                                            (assoc :health-check-url "/probe-2"
                                                   :last-update-time last-update-time
                                                   :owner (retrieve-username)
                                                   :run-as-user "i-do-not-exist-but-will-not-be-checked"
                                                   :token token))
                      response (post-token waiter-url token-description
                                           :headers {"if-match" (get headers "etag")}
                                           :query-params {"update-mode" "admin"})]
                  (assert-response-status response http-200-ok)
                  (log/info "created configuration using token" token))

                (let [token-response (get-token waiter-url token)
                      response-body (json/read-str (:body token-response))]
                  (is (= {"cluster" token-cluster
                          "health-check-url" "/probe-2"
                          "last-update-time" (-> last-update-time DateTime. du/date-to-str)
                          "last-update-user" (retrieve-username)
                          "name" service-id-prefix,
                          "owner" (retrieve-username)
                          "root" token-root
                          "run-as-user" "i-do-not-exist-but-will-not-be-checked"}
                         (dissoc response-body "previous")))
                  (log/info "asserted retrieval of configuration for token" token)))

              (testing "update with invalid etag"
                (let [token-description {:health-check-url "/probe-3"
                                         :last-update-time last-update-time
                                         :name service-id-prefix
                                         :owner (retrieve-username)
                                         :run-as-user "foo-bar"
                                         :token token}
                      {:keys [body] :as response} (post-token waiter-url token-description
                                                              :headers {"host" token
                                                                        "if-match" "1010"}
                                                              :query-params {"update-mode" "admin"})]
                  (assert-response-status response 412)
                  (is (str/includes? (str body) "Cannot modify stale token"))))

              (testing "hard-delete without etag"
                (let [{:keys [body] :as response} (make-request waiter-url "/token"
                                                                :headers {"host" token}
                                                                :method :delete
                                                                :query-params {"hard-delete" true})]
                  (assert-response-status response http-400-bad-request)
                  (is (str/includes? (str body) "Must specify if-match header for token hard deletes"))))

              (testing "hard-delete with invalid etag"
                (let [{:keys [body] :as response} (make-request waiter-url "/token"
                                                                :headers {"host" token
                                                                          "if-match" "1010"}
                                                                :method :delete
                                                                :query-params {"hard-delete" true})]
                  (assert-response-status response 412)
                  (is (str/includes? (str body) "Cannot modify stale token"))))

              (finally
                (delete-token-and-assert waiter-url token)))))

        (testing "deleted-token"
          (let [last-update-time (System/currentTimeMillis)
                token (create-token-name waiter-url ".")]
            (try
              (log/info "creating configuration using token" token)
              (let [token-description {:health-check-url "/probe-3"
                                       :name service-id-prefix
                                       :owner (retrieve-username)
                                       :token token}
                    response (post-token waiter-url token-description :headers {"host" token})]
                (assert-response-status response http-200-ok))

              (let [{:keys [body headers]} (get-token waiter-url token)
                    token-description (-> body
                                          json/read-str
                                          walk/keywordize-keys
                                          (assoc :deleted true
                                                 :health-check-url "/probe"
                                                 :last-update-time last-update-time
                                                 :run-as-user "foo-bar"
                                                 :token token))
                    response (post-token waiter-url token-description
                                         :headers {"if-match" (get headers "etag")}
                                         :query-params {"update-mode" "admin"})]
                (assert-response-status response http-200-ok))
              (log/info "created configuration using token" token)
              (let [{:keys [body] :as response} (get-token waiter-url token)]
                (assert-response-status response http-404-not-found)
                (is (str/includes? (str body) "Couldn't find token") (str body)))
              (let [token-response (get-token waiter-url token :query-params {"include" ["deleted" "metadata"]})
                    response-body (json/read-str (:body token-response))]
                (is (= {"cluster" token-cluster
                        "deleted" true
                        "health-check-url" "/probe"
                        "last-update-time" (-> last-update-time DateTime. du/date-to-str)
                        "last-update-user" (retrieve-username)
                        "name" service-id-prefix
                        "owner" (retrieve-username)
                        "root" token-root
                        "run-as-user" "foo-bar"}
                       (dissoc response-body "previous"))))
              (log/info "asserted retrieval of configuration for token" token)
              (finally
                (delete-token-and-assert waiter-url token)))))))))

(deftest ^:parallel ^:integration-fast test-named-token
  (testing-using-waiter-url
    (log/info "basic named token test")
    (let [service-id-prefix (rand-name)
          token (create-token-name waiter-url ".")]
      (try
        (log/info "creating configuration using token" token)
        (let [token-definition (assoc
                                 (kitchen-request-headers :prefix "")
                                 :name service-id-prefix
                                 :token token)
              {:keys [body status]} (post-token waiter-url token-definition)]
          (when (not= http-200-ok status)
            (log/info "error:" body)
            (is (not body))))
        (log/info "created configuration using token" token)
        (log/info "retrieving configuration for token" token)
        (let [token-response (get-token waiter-url token)
              response-body (str (:body token-response))]
          (when (not (str/includes? response-body service-id-prefix))
            (log/info response-body))
          (assert-response-status token-response http-200-ok)
          (is (str/includes? response-body service-id-prefix)))
        (log/info "asserted retrieval of configuration for token" token)

        (log/info "making Waiter request with token" token "in header")
        (let [request-headers {:x-waiter-token token}
              path "/foo"
              response (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url (:request-headers response))]
          (assert-response-status response http-200-ok)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (= #{(make-source-tokens-entries waiter-url token)}
                 (service-id->source-tokens-entries waiter-url service-id))))

        (log/info "making Waiter request with token and x-waiter-debug token" token "in header")
        (let [request-headers {:x-waiter-debug "true", :x-waiter-token token}
              path "/foo"
              {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url request-headers)]
          (assert-response-status response http-200-ok)
          (with-service-cleanup
            service-id
            (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
            (is (= #{(make-source-tokens-entries waiter-url token)}
                   (service-id->source-tokens-entries waiter-url service-id)))
            (is (every? #(not (str/blank? (get headers %)))
                        (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
                (str headers))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-star-run-as-user-token
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token (create-token-name waiter-url ".")]
      (try
        (log/info "creating configuration using token" token)
        (let [token-definition (assoc
                                 (kitchen-request-headers :prefix "")
                                 :name service-id-prefix
                                 :run-as-user "*"
                                 :token token)
              {:keys [body status]} (post-token waiter-url token-definition)]
          (when (not= http-200-ok status)
            (log/info "error:" body)
            (is (not body))))
        (log/info "created configuration using token" token)
        (log/info "retrieving configuration for token" token)
        (let [token-response (get-token waiter-url token)
              response-body (str (:body token-response))]
          (when (not (str/includes? response-body service-id-prefix))
            (log/info response-body))
          (assert-response-status token-response http-200-ok)
          (is (str/includes? response-body service-id-prefix)))
        (log/info "asserted retrieval of configuration for token" token)

        (let [{:keys [body] :as token-response} (get-token waiter-url token)
              token-description (try (json/read-str (str body))
                                     (catch Exception _
                                       (is false (str "Failed to parse token" body))))]
          (assert-response-status token-response http-200-ok)
          (is (= "/status" (token-description "health-check-url")))
          (is (= service-id-prefix (token-description "name")))
          (is (= "*" (token-description "run-as-user")))
          (is (System/getProperty "user.name") (token-description "owner")))

        (log/info "making Waiter request with token" token "in header")
        (let [request-headers {:x-waiter-token token}
              path "/foo"
              response (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url (:request-headers response))]
          (assert-response-status response http-200-ok)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (= #{(make-source-tokens-entries waiter-url token)}
                 (service-id->source-tokens-entries waiter-url service-id)))
          (is (= (retrieve-username) (:run-as-user (service-id->service-description waiter-url service-id)))))

        (log/info "making Waiter request with token and x-waiter-debug token" token "in header")
        (let [request-headers {:x-waiter-debug "true", :x-waiter-token token}
              path "/foo"
              {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url request-headers)]
          (assert-response-status response http-200-ok)
          (with-service-cleanup
            service-id
            (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
            (is (= #{(make-source-tokens-entries waiter-url token)}
                   (service-id->source-tokens-entries waiter-url service-id)))
            (is (every? #(not (str/blank? (get headers %)))
                        (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
                (str headers))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast ^:explicit test-on-the-fly-to-token
  (testing-using-waiter-url
    (let [name-string (rand-name)
          {:keys [cookies] :as canary-response}
          (make-request-with-debug-info {:x-waiter-name name-string} #(make-kitchen-request waiter-url %))
          service-id-1 (:service-id canary-response)]
      (with-service-cleanup
        service-id-1
        (is (str/includes? service-id-1 name-string) (str "ERROR: App-name is missing " name-string))
        (assert-service-on-all-routers waiter-url service-id-1 cookies)
        (is (nil? (service-id->source-tokens-entries waiter-url service-id-1)))

        (let [service-settings (service-settings waiter-url service-id-1 :query-params {"include" "references"})]
          (is (= [{}] (get service-settings :references)) (str service-settings)))

        (let [token (str "^SERVICE-ID#" service-id-1)
              response (make-request-with-debug-info {:x-waiter-token token} #(make-request waiter-url "" :headers %))
              service-id-2 (:service-id response)]
          (assert-response-status response http-200-ok)
          (with-service-cleanup
            service-id-2
            (is (= service-id-1 service-id-2) "The on-the-fly and token-based service ids do not match")
            (assert-service-on-all-routers waiter-url service-id-1 cookies)
            (is (= #{(make-source-tokens-entries waiter-url token)}
                   (service-id->source-tokens-entries waiter-url service-id-2)))
            (let [service-settings (service-settings waiter-url service-id-2 :query-params {"include" "references"})
                  references (set (get service-settings :references))]
              (is (contains? references {}) (str service-settings))
              (is (contains? references {:token {:sources [{:token token :version (token->etag waiter-url token)}]}})
                  (str service-settings)))))))))

(deftest ^:parallel ^:integration-fast test-namespace-token
  (testing-using-waiter-url
    (log/info "basic token with namespace field test")
    (let [service-id-prefix (rand-name)
          target-user (retrieve-username)
          token (create-token-name waiter-url ".")]
      (for [token-user [target-user "*"]]
        (try
          (log/info "creating configuration using token" token)
          (let [token-definition (assoc
                                   (kitchen-request-headers :prefix "")
                                   :name service-id-prefix
                                   :namespace token-user
                                   :run-as-user token-user
                                   :token token)
                {:keys [body status]} (post-token waiter-url token-definition)]
            (when (not= http-200-ok status)
              (log/info "error:" body)
              (is (not body))))
          (log/info "created configuration using token" token)
          (log/info "retrieving configuration for token" token)
          (let [token-response (get-token waiter-url token)
                response-body (str (:body token-response))]
            (when (not (str/includes? response-body service-id-prefix))
              (log/info response-body))
            (assert-response-status token-response http-200-ok)
            (is (str/includes? response-body service-id-prefix))
            (let [{:strs [namespace run-as-user]} (try-parse-json response-body)]
              (is (= target-user run-as-user))
              (is (= target-user namespace))))
          (log/info "asserted retrieval of configuration for token" token)
          (finally
            (delete-token-and-assert waiter-url token)))))

    (testing "can't create token with bad namespace"
      (let [service-desc {:name (rand-name "notused")
                          :cpus 1
                          :debug true
                          :mem 1024
                          :version "universe b10452d0b0380ce61764543847085631ee3d7af9"
                          :token "token-with-bad-namespace"
                          :cmd "not-used"
                          :permitted-user "*"
                          :namespace "not-run-as-user"
                          :run-as-user (retrieve-username)
                          :health-check-url "/not-used"}
            response (post-token waiter-url service-desc)]
        (is (str/includes? (:body response) "Service namespace must either be omitted or match the run-as-user"))
        (assert-response-status response http-400-bad-request)))))


(deftest ^:parallel ^:integration-fast test-bad-token
  (testing-using-waiter-url

    (let [common-headers {"x-waiter-cmd" "foo-bar"
                          "x-waiter-cmd-type" "shell"}]
      (testing "ignore missing token when have service description"
        (let [response (make-request waiter-url "/pathabc" :headers (assoc common-headers "host" "missing_token"))]
          (is (str/includes? (:body response) "Service description using waiter headers/token improperly configured"))
          (assert-response-status response http-400-bad-request)))

      (testing "ignore invalid token when have service description"
        (let [response (make-request waiter-url "/pathabc" :headers (assoc common-headers "host" "bad/token"))]
          (is (str/includes? (:body response) "Service description using waiter headers/token improperly configured"))
          (assert-response-status response http-400-bad-request)))

      (testing "ignore invalid token when have invalid service description"
        (let [response (make-request waiter-url "/pathabc" :headers (assoc common-headers "host" "bad/token"))]
          (is (str/includes? (:body response) "Service description using waiter headers/token improperly configured"))
          (assert-response-status response http-400-bad-request))))

    (testing "can't use missing token server"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "missing_token"})]
        (is (str/includes? (:body response) "Unable to identify service using waiter headers/token"))
        (assert-response-status response http-400-bad-request)))

    (testing "can't use invalid token server"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "bad/token"})]
        (is (str/includes? (:body response) "Unable to identify service using waiter headers/token"))
        (assert-response-status response http-400-bad-request)))

    (testing "can't use invalid token that's valid for zookeeper"
      (let [response (make-request waiter-url "/pathabc" :headers {"x-waiter-token" "bad#token"})]
        (is (str/includes? (:body response) "Token not found: bad#token"))
        (assert-response-status response http-400-bad-request)))

    (testing "can't use invalid token"
      (let [response (make-request waiter-url "/pathabc" :headers {"x-waiter-token" "bad/token"})]
        (is (str/includes? (:body response) "Token not found: bad/token"))
        (assert-response-status response http-400-bad-request)))

    (testing "can't use invalid token with host set"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "missing_token" "x-waiter-token" "bad/token"})]
        (is (str/includes? (:body response) "Token not found: bad/token"))
        (assert-response-status response http-400-bad-request)))

    (testing "can't use missing token with host set"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "missing_token" "x-waiter-token" "missing_token"})]
        (is (str/includes? (:body response) "Token not found: missing_token"))
        (assert-response-status response http-400-bad-request)))

    (testing "can't use missing token"
      (let [response (make-request waiter-url "/pathabc" :headers {"x-waiter-token" "missing_token"})]
        (is (str/includes? (:body response) "Token not found: missing_token"))
        (assert-response-status response http-400-bad-request)))


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
            response (post-token waiter-url service-desc)]
        (is (str/includes? (:body response) "Token must match pattern"))
        (assert-response-status response http-400-bad-request)))))

(deftest ^:parallel ^:integration-fast test-token-metadata
  (testing-using-waiter-url
    (let [token (rand-name)
          service-desc {:name token
                        :cpus 1
                        :mem 100
                        :version "1"
                        :cmd-type "shell"
                        :token token
                        :cmd "exit 0"
                        :run-as-user (retrieve-username)
                        :health-check-url "/not-used"
                        :metadata {"a" "b", "c" "d"}}
          register-response (post-token waiter-url service-desc)
          {:keys [body]} (get-token waiter-url token)]
      (assert-response-status register-response http-200-ok)
      (is (= (:metadata service-desc) (get (json/read-str body) "metadata")))
      (delete-token-and-assert waiter-url token))))

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
          register-response (post-token waiter-url service-desc)]
      (assert-response-status register-response http-400-bad-request))))

(deftest ^:parallel ^:integration-fast test-token-bad-payload
  (testing-using-waiter-url
    (let [response (make-request waiter-url "/token"
                                 :body "i'm bad at json"
                                 :headers {"host" "test-token"}
                                 :method :post)]
      (assert-response-status response http-400-bad-request))))

(deftest ^:parallel ^:integration-fast test-token-environment-variables
  (testing-using-waiter-url
    (let [token (rand-name)
          binary (kitchen-cmd)
          token-response (post-token waiter-url {:cmd "$BINARY -p $PORT0"
                                                 :cmd-type "shell"
                                                 :version "does-not-matter"
                                                 :name token
                                                 :env {"BINARY" binary}
                                                 :token token})
          {:keys [service-id] :as response} (make-request-with-debug-info {:x-waiter-token token}
                                                                          #(make-light-request waiter-url %))]
      (assert-response-status token-response http-200-ok)
      (assert-response-status response http-200-ok)
      (with-service-cleanup
        service-id
        (let [{:keys [env] :as service-description} (response->service-description waiter-url response)]
          (is (= {:BINARY binary} env) (str service-description))
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-token-https-redirects
  (testing-using-waiter-url
    (let [token (rand-name)
          token-response (post-token waiter-url {:cmd (str (kitchen-cmd) " -p $PORT0")
                                                 :cmd-type "shell"
                                                 :https-redirect true
                                                 :name token
                                                 :version "does-not-matter"
                                                 :token token})]
      (assert-response-status token-response http-200-ok)

      (testing "https redirects"
        (let [request-headers {:x-waiter-token token}
              url (URL. (str "http://" waiter-url))
              endpoint "/request-info"]

          (testing "get request"
            (let [{:keys [headers] :as response}
                  (make-kitchen-request waiter-url request-headers :method :get :path endpoint)]
              (assert-response-status response http-301-moved-permanently)
              (is (= (str "https://" (.getHost url) endpoint) (get headers "location")))
              (is (str/starts-with? (str (get headers "server")) "waiter") (str "headers:" headers))))

          (testing "post request"
            (let [{:keys [headers] :as response}
                  (make-kitchen-request waiter-url request-headers :method :post :path endpoint)]
              (assert-response-status response http-307-temporary-redirect)
              (is (= (str "https://" (.getHost url) endpoint) (get headers "location")))
              (is (str/starts-with? (str (get headers "server")) "waiter") (str "headers:" headers))))))

      (delete-token-and-assert waiter-url token))))

(defmacro run-token-param-support
  [waiter-url request-fn request-headers expected-env]
  `(let [response# (make-request-with-debug-info ~request-headers ~request-fn)
         service-description# (response->service-description ~waiter-url response#)
         service-id# (:service-id response#)]
     (assert-response-status response# http-200-ok)
     (is (= ~expected-env (:env service-description#)) (str service-description#))
     (delete-service ~waiter-url service-id#)
     service-id#))

(deftest ^:parallel ^:integration-fast test-token-param-support
  (testing-using-waiter-url
    (let [token (rand-name)
          binary (kitchen-cmd)
          service-description {:allowed-params ["FAA" "FEE" "FII" "FOO" "FUU"]
                               :cmd "$BINARY -p $PORT0"
                               :cmd-type "shell"
                               :env {"BINARY" binary
                                     "FEE" "FIE"
                                     "FOO" "BAR"}
                               :name (rand-name)
                               :version "does-not-matter"}
          kitchen-request #(make-kitchen-request waiter-url % :path "/environment")]
      (try
        (let [token-description (assoc service-description :token token)
              token-response (post-token waiter-url token-description :query-params {"update-mode" "admin"})]
          (assert-response-status token-response http-200-ok)
          (let [request-headers {:x-waiter-param-my_variable "value-1"
                                 :x-waiter-token token}
                {:keys [body] :as response} (make-request-with-debug-info request-headers kitchen-request)]
            (assert-response-status response http-400-bad-request)
            (is (str/includes? body "Some params cannot be configured")))
          (let [request-headers {:x-waiter-allowed-params ""
                                 :x-waiter-param-my_variable "value-1"
                                 :x-waiter-token token}
                {:keys [body] :as response} (make-request-with-debug-info request-headers kitchen-request)]
            (assert-response-status response http-400-bad-request)
            (is (str/includes? body "Some params cannot be configured"))))
        (let [token-description (-> service-description (dissoc :allowed-params) (assoc :token (str token ".1")))
              token-response (post-token waiter-url token-description :query-params {"update-mode" "admin"})]
          (assert-response-status token-response http-200-ok)
          (let [request-headers {:x-waiter-param-my_variable "value-1"
                                 :x-waiter-token token}
                {:keys [body] :as response} (make-request-with-debug-info request-headers kitchen-request)]
            (assert-response-status response http-400-bad-request)
            (is (str/includes? body "Some params cannot be configured"))))
        (let [service-ids (set [(run-token-param-support
                                  waiter-url kitchen-request
                                  {:x-waiter-token token}
                                  {:BINARY binary :FEE "FIE" :FOO "BAR"})
                                (run-token-param-support
                                  waiter-url kitchen-request
                                  {:x-waiter-param-fee "value-1p" :x-waiter-token token}
                                  {:BINARY binary :FEE "value-1p" :FOO "BAR"})
                                (run-token-param-support
                                  waiter-url kitchen-request
                                  {:x-waiter-allowed-params ["FEE" "FII" "FOO"] :x-waiter-param-fee "value-1p" :x-waiter-token token}
                                  {:BINARY binary :FEE "value-1p" :FOO "BAR"})
                                (run-token-param-support
                                  waiter-url kitchen-request
                                  {:x-waiter-allowed-params "FEE,FOO" :x-waiter-param-fee "value-1p" :x-waiter-token token}
                                  {:BINARY binary :FEE "value-1p" :FOO "BAR"})
                                (run-token-param-support
                                  waiter-url kitchen-request
                                  {:x-waiter-param-fee "value-1p" :x-waiter-param-foo "value-2p" :x-waiter-token token}
                                  {:BINARY binary :FEE "value-1p" :FOO "value-2p"})])]
          (is (= 5 (count service-ids)) "Unique service-ids were not created!"))
        (finally
          (delete-token-and-assert waiter-url token)
          (delete-token-and-assert waiter-url (str token ".1")))))))

(deftest ^:parallel ^:integration-fast test-token-invalid-environment-variables
  (testing-using-waiter-url
    (let [{:keys [body] :as response} (post-token waiter-url {:env {"HOME" "/my/home"} :token (rand-name)})]
      (assert-response-status response http-400-bad-request)
      (is (not (str/includes? body "clojure")) body)
      (is (str/includes? body "The following environment variable keys are reserved: HOME.") body))))

(deftest ^:parallel ^:integration-fast test-token-parameters-exceed-limits
  (testing-using-waiter-url
    (let [constraints (setting waiter-url [:service-description-constraints])
          max-constraints (sd/extract-max-constraints constraints)]
      (is (seq max-constraints))
      (doseq [[parameter max-constraint] max-constraints]
        (let [string-param? (contains? headers/params-with-str-value (name parameter))
              param-value (if string-param?
                            (apply str parameter " " (repeat max-constraint "x"))
                            (inc max-constraint))
              {:keys [body] :as response} (post-token waiter-url {parameter param-value :token (rand-name)})]
          (assert-response-status response http-400-bad-request)
          (is (not (str/includes? body "clojure")) body)
          (is (every? #(str/includes? body %)
                      ["The following fields exceed their allowed limits"
                       (if string-param?
                         (str (name parameter) " must be at most " max-constraint " characters")
                         (str (name parameter) " is " param-value " but the max allowed is " max-constraint))])
              body))))))

(deftest ^:parallel ^:integration-fast test-auto-run-as-requester-support
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url ".")
          service-description (-> (kitchen-request-headers :prefix "")
                                  (assoc :name service-name :permitted-user "*")
                                  (dissoc :run-as-user))
          waiter-port (.getPort (URL. (str "http://" waiter-url)))
          waiter-port (if (neg? waiter-port) 80 waiter-port)
          host-header (str token ":" waiter-port)
          has-x-waiter-consent? (partial some #(= (:name %) "x-waiter-consent"))
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)]
      (try
        (testing "token creation"
          (let [token-description (assoc service-description :token token)
                response (post-token waiter-url token-description)]
            (assert-response-status response http-200-ok)))

        (testing "token retrieval"
          (let [token-response (get-token waiter-url token)
                response-body (-> token-response (:body) (json/read-str) (pc/keywordize-map))]
            (is (nil? (get response-body :run-as-user)))
            (is (contains? response-body :last-update-time))
            (is (= (assoc service-description
                     :cluster token-cluster
                     :last-update-user (retrieve-username)
                     :owner (retrieve-username)
                     :previous {}
                     :root token-root)
                   (dissoc response-body :last-update-time)))))

        (testing "expecting redirect"
          (let [{:keys [headers] :as response} (make-request waiter-url "/hello-world" :headers {"host" host-header})]
            (is (= (str "/waiter-consent/hello-world")
                   (get headers "location")))
            (assert-response-status response http-303-see-other)))

        (testing "waiter-consent"
          (let [consent-path (str "/waiter-consent/hello-world")
                {:keys [body] :as response} (make-request waiter-url consent-path :headers {"host" host-header})
                service-id (when-let [service-id-index (str/index-of body "name=\"service-id\"")]
                             (when-let [value-index (str/index-of body "value=\"" service-id-index)]
                               (when-let [end-index (str/index-of body "\"" (+ value-index 7))]
                                 (subs body (+ value-index 7) end-index))))]
            (is service-id)
            (assert-response-status response http-200-ok)

            (let [cookies-atom (atom nil)]
              (testing "approval of specific service"
                (let [{:keys [body cookies] :as response}
                      (make-request waiter-url "/waiter-consent"
                                    :headers {"host" host-header
                                              "referer" (str "http://" host-header)
                                              "origin" (str "http://" host-header)
                                              "x-requested-with" "XMLHttpRequest"}
                                    :method :post
                                    :multipart {"mode" "service"
                                                "service-id" service-id})]
                  (reset! cookies-atom cookies)
                  (is (= "Added cookie x-waiter-consent" body))
                  ; x-waiter-consent should be emitted Waiter
                  (is (has-x-waiter-consent? cookies))
                  (assert-response-status response http-200-ok)))

              (testing "auto run-as-user population on expected service-id"
                (let [service-id-atom (atom nil)]
                  (try
                    (let [expected-service-id service-id
                          {:keys [body cookies service-id] :as response}
                          (make-request-with-debug-info
                            {"host" host-header}
                            #(make-request waiter-url "/hello-world" :cookies @cookies-atom :headers %1))
                          {:keys [service-description]} (service-settings waiter-url service-id)
                          {:keys [run-as-user permitted-user]} service-description]
                      (reset! service-id-atom service-id)
                      (is (= "Hello World" body))
                      ; x-waiter-consent should not be re-emitted Waiter
                      (is (not (has-x-waiter-consent? cookies)))
                      (is (= expected-service-id service-id))
                      (is (not (str/blank? permitted-user)))
                      (is (= run-as-user permitted-user))
                      (assert-response-status response http-200-ok))
                    (finally
                      (when @service-id-atom
                        (delete-service waiter-url @service-id-atom))))))

              (testing "token update"
                (let [updated-service-name (rand-name)
                      token-description (assoc service-description :name updated-service-name :token token)
                      response (post-token waiter-url token-description)]
                  (assert-response-status response http-200-ok)))

              (testing "expecting redirect after token update"
                (let [{:keys [headers] :as response}
                      (make-request waiter-url "/hello-world" :cookies @cookies-atom :headers {"host" host-header})]
                  (is (= (str "/waiter-consent/hello-world")
                         (get headers "location")))
                  (assert-response-status response http-303-see-other)))

              (testing "approval of token"
                (let [{:keys [body cookies] :as response}
                      (make-request waiter-url "/waiter-consent"
                                    :cookies @cookies-atom
                                    :headers {"host" host-header
                                              "referer" (str "http://" host-header)
                                              "origin" (str "http://" host-header)
                                              "x-requested-with" "XMLHttpRequest"}
                                    :method :post
                                    :multipart {"mode" "token"})]
                  (reset! cookies-atom cookies)
                  (is (= "Added cookie x-waiter-consent" body))
                  ; x-waiter-consent should be emitted Waiter
                  (is (has-x-waiter-consent? cookies))
                  (assert-response-status response http-200-ok)))

              (testing "auto run-as-user population on approved token"
                (let [service-id-atom (atom nil)]
                  (try
                    (let [previous-service-id service-id
                          {:keys [body cookies service-id] :as response}
                          (make-request-with-debug-info
                            {"host" host-header "x-waiter-fallback-period-secs" 0}
                            #(make-request waiter-url "/hello-world" :cookies @cookies-atom :headers %1))]
                      (reset! service-id-atom service-id)
                      (is (= "Hello World" body))
                      ; x-waiter-consent should not be re-emitted Waiter
                      (is (not (has-x-waiter-consent? cookies)))
                      (is (not= previous-service-id service-id))
                      (assert-response-status response http-200-ok))
                    (finally
                      (when @service-id-atom
                        (delete-service waiter-url @service-id-atom)))))))))

        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-authentication-disabled-support
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url ".")
          current-user (retrieve-username)
          service-description (-> (kitchen-request-headers :prefix "")
                                  (assoc :authentication "disabled"
                                         :name service-name
                                         :permitted-user "*"
                                         :run-as-user current-user))
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)
          request-headers {:x-waiter-token token}]
      (try
        (testing "token creation"
          (let [token-description (assoc service-description :token token)
                response (post-token waiter-url token-description)]
            (assert-response-status response http-200-ok)))

        (testing "token retrieval"
          (let [token-response (get-token waiter-url token)
                response-body (-> token-response :body json/read-str pc/keywordize-map)]
            (is (contains? response-body :last-update-time))
            (is (= (assoc service-description
                     :authentication "disabled"
                     :cluster token-cluster
                     :last-update-user current-user
                     :owner current-user
                     :previous {}
                     :root token-root)
                   (dissoc response-body :last-update-time)))))

        (testing "successful request"
          (let [{:keys [body] :as response}
                (make-request waiter-url "/hello-world" :headers request-headers :disable-auth true)]
            (assert-response-status response http-200-ok)
            (is (= "Hello World" body))))

        (testing "backend request headers"
          (let [{:keys [body] :as response}
                (make-request waiter-url "/request-info" :headers request-headers :disable-auth true)
                {:strs [headers]} (json/read-str (str body))
                service-id (retrieve-service-id waiter-url (:request-headers response))]
            (assert-response-status response http-200-ok)
            (is (not (contains? headers "x-waiter-auth-principal")))
            (is (not (contains? headers "x-waiter-authenticated-principal")))
            (is (contains? headers "x-cid"))
            (delete-service waiter-url service-id)))

        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-maintenance-mode
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url ".")
          current-user (retrieve-username)
          service-description (-> (kitchen-request-headers :prefix "")
                                  (assoc :https-redirect true
                                         :interstitial-secs 60
                                         :name service-name
                                         :permitted-user "*"
                                         :run-as-user "*"))
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          custom-maintenance-message "&&><<&>a&&<b\"<<baa&<"
          token-maintenance {:message custom-maintenance-message}
          request-headers {:x-waiter-token token}]
      (try
        (testing "token created in maintenance mode"
          (let [token-description (assoc service-description
                                    :maintenance token-maintenance
                                    :token token)
                response (post-token waiter-url token-description)]
            (assert-response-status response http-200-ok)))

        (testing "token retrieval"
          (let [token-response (get-token waiter-url token)
                response-body (-> token-response :body try-parse-json walk/keywordize-keys)]
            (assert-response-status token-response http-200-ok)
            (is (contains? response-body :last-update-time))
            (is (= (assoc service-description
                     :cluster token-cluster
                     :last-update-user current-user
                     :maintenance token-maintenance
                     :owner current-user
                     :previous {}
                     :root token-root)
                   (dissoc response-body :last-update-time)))))

        (testing "index should be updated with maintenance enabled"
          (let [response (make-request waiter-url "/tokens")
                body (-> response :body try-parse-json walk/keywordize-keys)
                index (first (filter #(= (:token %) token) body))]
            (is (true? (:maintenance index)))))

        (testing "request to service should error with custom maintenance message"
          (let [{:keys [body] :as response}
                (make-request waiter-url "/hello-world" :headers request-headers)]
            (assert-response-status response http-503-service-unavailable)
            (assert-waiter-response response)
            (is (str/includes? body custom-maintenance-message))))

        (testing "request (text/html) to service should error with maintenance message where the special characters are html encoded"
          (let [{:keys [body] :as response}
                (make-request waiter-url "/hello-world" :headers (assoc request-headers :Accept "text/html"))]
            (assert-response-status response http-503-service-unavailable)
            (assert-waiter-response response)
            (is (str/includes? body "&amp;&amp;&gt;&lt;&lt;&amp;&gt;a&amp;&amp;&lt;b&quot;&lt;&lt;baa&amp;&lt;"))))

        (testing "token update maintenance field not defined"
          (let [token-description (-> service-description
                                      (dissoc :https-redirect :interstitial-secs)
                                      (assoc :token token :run-as-user current-user))
                response (post-token waiter-url token-description)]
            (assert-response-status response http-200-ok)))

        (testing "index should be updated with maintenance disabled"
          (let [response (make-request waiter-url "/tokens")
                body (-> response :body try-parse-json walk/keywordize-keys)
                index (first (filter #(= (:token %) token) body))]
            (is (false? (:maintenance index)))))

        (testing "service should handle request if maintenance mode is not enabled"
          (let [{:keys [body] :as response}
                (make-request waiter-url "/hello-world" :headers request-headers)]
            (assert-response-status response http-200-ok)
            (assert-backend-response response)
            (is (= body "Hello World"))))

        (testing "soft-deleted token in maintenance mode provides correct index"
          (let [token-description (assoc service-description
                                    :maintenance token-maintenance
                                    :token token)
                response (post-token waiter-url token-description)
                {:keys [body] :as del-response} (make-request waiter-url "/token"
                                                          :headers {"host" token}
                                                          :method :delete
                                                          :query-params {})
                {index-body :body :as index-response}
                (list-tokens waiter-url (retrieve-username) cookies {"include" ["deleted" "metadata"]})
                token-index (->> index-body
                                 try-parse-json
                                 (filter (fn [cur-index] (= token (get cur-index "token"))))
                                 first)]
            (assert-response-status response 200)
            (assert-response-status del-response 200)
            (is (= {"delete" token "hard-delete" false "success" true}
                   (try-parse-json body)))
            (assert-response-status index-response 200)
            (is (= {"deleted" true
                    "etag" nil
                    "maintenance" true
                    "owner" (retrieve-username)
                    "token" token}
                   (dissoc token-index "last-update-time")))))

        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-current-for-tokens
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url ".")
          service-description (assoc (kitchen-request-headers :prefix "")
                                :name service-name)]
      (try
        (testing "creating initial token"
          (let [response (post-token waiter-url (assoc service-description :token token))]
            (assert-response-status response http-200-ok)))

        (let [initial-service-id (retrieve-service-id waiter-url {:x-waiter-token token})]
          (testing "current service reports as current for token"
            (let [initial-service-details (service-settings waiter-url initial-service-id)]
              (is (= [token] (:current-for-tokens initial-service-details)))))

          (testing "updating token"
            (let [new-service-description (assoc service-description :metadata {"foo" "bar"})
                  response (post-token waiter-url (assoc new-service-description :token token))]
              (assert-response-status response http-200-ok)))

          (testing "old service is no longer current for token"
            (let [initial-service-details' (service-settings waiter-url initial-service-id)
                  new-service-id (retrieve-service-id waiter-url {:x-waiter-token token})
                  new-service-details (service-settings waiter-url new-service-id)]
              (is (nil? (:current-for-tokens initial-service-details')))
              (is (= [token] (:current-for-tokens new-service-details))))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(defn- response->etag
  "Retrieves the etag header from a response."
  [response]
  (-> response
    :headers
    (get "etag")))

(defn update-token
  "Updates the token and returns the etag."
  [waiter-url token-name service-description]
  (let [response (post-token waiter-url (assoc service-description :token token-name))
        etag (response->etag response)]
    (assert-response-status response http-200-ok)
    (is (not (str/blank? etag)))
    etag))

(deftest ^:parallel ^:integration-fast test-current-for-tokens-multiple-source-tokens
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token-name-a (create-token-name waiter-url "." (str service-name "-A"))
          token-name-b (create-token-name waiter-url "." (str service-name "-B"))
          base-service-description (assoc (kitchen-request-headers :prefix "") :name service-name)
          service-description-a1 (dissoc base-service-description :cpus)
          service-description-b1 (dissoc base-service-description :mem)
          build-reference (fn [etag-a etag-b]
                            {:token {:sources [{:token token-name-a :version etag-a}
                                               {:token token-name-b :version etag-b}]}})
          combined-token-header (str token-name-a "," token-name-b)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")]
      (try
        (let [etag-a1 (update-token waiter-url token-name-a service-description-a1)
              _ (assert-token-on-all-routers waiter-url token-name-a etag-a1 cookies)
              etag-b1 (update-token waiter-url token-name-b service-description-b1)
              _ (assert-token-on-all-routers waiter-url token-name-b etag-b1 cookies)
              service-id-a (retrieve-service-id waiter-url {:x-waiter-token combined-token-header})]

          (let [service-settings (service-settings waiter-url service-id-a :query-params {"include" "references"})
                references-a (get service-settings :references)]
            (is (= references-a [(build-reference etag-a1 etag-b1)])))

          (let [service-description-a2 (update service-description-a1 :mem #(+ % 8))
                etag-a2 (update-token waiter-url token-name-a service-description-a2)
                _ (assert-token-on-all-routers waiter-url token-name-a etag-a2 cookies)
                _ (is (not= etag-a1 etag-a2))
                service-id-b (retrieve-service-id waiter-url {:x-waiter-token combined-token-header})]

            (is (not= service-id-a service-id-b))
            (let [service-settings (service-settings waiter-url service-id-b :query-params {"include" "references"})
                  references-b (get service-settings :references)]
              (is (= references-b [(build-reference etag-a2 etag-b1)])))

            (let [service-description-b2 (update service-description-b1 :cpus #(+ % 0.1))
                  etag-b2 (update-token waiter-url token-name-b service-description-b2)
                  _ (assert-token-on-all-routers waiter-url token-name-b etag-b2 cookies)
                  _ (is (not= etag-b1 etag-b2))
                  service-id-c (retrieve-service-id waiter-url {:x-waiter-token combined-token-header})]

              (is (not= service-id-a service-id-c))
              (is (not= service-id-b service-id-c))
              (let [service-settings (service-settings waiter-url service-id-c :query-params {"include" "references"})
                    references-c (get service-settings :references)]
                (is (= references-c [(build-reference etag-a2 etag-b2)])))

              (let [service-a-details (service-settings waiter-url service-id-a :query-params {"include" "references"})
                    references-a (get service-a-details :references)
                    service-b-details (service-settings waiter-url service-id-b :query-params {"include" "references"})
                    references-b (get service-b-details :references)
                    service-c-details (service-settings waiter-url service-id-c :query-params {"include" "references"})
                    references-c (get service-c-details :references)]
                (is (nil? (:current-for-tokens service-a-details)))
                (is (nil? (:current-for-tokens service-b-details)))
                (is (= [token-name-a token-name-b] (:current-for-tokens service-c-details)))
                (is (= 1 (count references-a)))
                (is (= 1 (count references-b)))
                (is (= 1 (count references-c)))
                (is (not= references-a references-b))
                (is (not= references-a references-c))
                (is (not= references-b references-c))))))
        (finally
          (delete-token-and-assert waiter-url token-name-a)
          (delete-token-and-assert waiter-url token-name-b))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-service-fallback-support
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url ".")
          scheduler-syncer-interval-secs (setting waiter-url [:scheduler-syncer-interval-secs])
          fallback-period-secs 30
          service-description-1 (-> (kitchen-request-headers :prefix "")
                                    (assoc :fallback-period-secs fallback-period-secs
                                           :name (str service-name "-v1")
                                           :permitted-user "*"
                                           :run-as-user (retrieve-username)
                                           :version "version-1"))
          request-headers {:x-waiter-token token}
          service-id-headers (assoc request-headers :x-waiter-fallback-period-secs 0)
          kitchen-env-service-id (fn []
                                   (let [kitchen-response (make-request-with-debug-info
                                                            request-headers
                                                            #(make-request waiter-url "/environment" :headers %))
                                         kitchen-service-id (-> kitchen-response
                                                                :body
                                                                (json/read-str)
                                                                (get "WAITER_SERVICE_ID"))]
                                     (is (= kitchen-service-id (:service-id kitchen-response)))
                                     kitchen-service-id))
          waiter-routers (routers waiter-url)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          await-healthy-service-on-routers (fn await-healthy-service-on-routers [service-id]
                                             (wait-for (fn []
                                                         (every? (fn [[_ router-url]]
                                                                   (-> (fallback-state router-url :cookies cookies)
                                                                       :state
                                                                       :healthy-service-ids
                                                                       set
                                                                       (contains? service-id)))
                                                                 waiter-routers))
                                                       :interval 1
                                                       :timeout (inc scheduler-syncer-interval-secs)))
          await-service-deletion-on-routers (fn await-service-deletion-on-routers [service-id]
                                              (wait-for (fn []
                                                          (every? (fn [[_ router-url]]
                                                                    (-> (service-state router-url service-id :cookies cookies)
                                                                        :state
                                                                        :service-maintainer-state
                                                                        :maintainer-chan-available
                                                                        false?))
                                                                  waiter-routers))
                                                        :interval 1
                                                        :timeout (inc scheduler-syncer-interval-secs)))
          thread-sleep (fn [time-in-secs] (-> time-in-secs inc t/seconds t/in-millis Thread/sleep))]
      (try
        (let [token-description-1 (assoc service-description-1 :token token)
              post-response (post-token waiter-url token-description-1)
              _ (assert-response-status post-response http-200-ok)
              service-id-1 (retrieve-service-id waiter-url service-id-headers)]

          (testing "token response contains fallback and owner"
            (let [{:keys [body] :as response} (get-token waiter-url token :query-params {})]
              (assert-response-status response http-200-ok)
              (is (= (assoc service-description-1 :owner (retrieve-username))
                     (-> body json/read-str walk/keywordize-keys)))))

          (with-service-cleanup
            service-id-1

            (is (= service-id-1 (kitchen-env-service-id)))
            ;; allow syncer state to get updated
            (await-healthy-service-on-routers service-id-1)

            (let [service-description-2 (assoc service-description-1 :name (str service-name "-v2") :version "version-2")
                  token-description-2 (assoc service-description-2 :token token)
                  post-response (post-token waiter-url token-description-2)
                  _ (assert-response-status post-response http-200-ok)
                  service-id-2 (retrieve-service-id waiter-url service-id-headers)]
              (with-service-cleanup
                service-id-2

                (is (not= service-id-1 service-id-2))
                ;; fallback to service-id-1
                (is (= service-id-1 (retrieve-service-id waiter-url request-headers)))
                (is (= service-id-1 (kitchen-env-service-id)))
                (thread-sleep fallback-period-secs)
                ;; outside fallback duration
                (is (= service-id-2 (kitchen-env-service-id)))
                ;; allow syncer state to get updated
                (await-healthy-service-on-routers service-id-2)

                (let [sleep-duration (* 2 scheduler-syncer-interval-secs)
                      service-description-3 (-> service-description-1
                                                (assoc :name (str service-name "-v3") :version "version-3")
                                                (update "cmd" (fn [c] (str "sleep " sleep-duration " && " c))))
                      token-description-3 (assoc service-description-3 :token token)
                      post-response (post-token waiter-url token-description-3)
                      _ (assert-response-status post-response http-200-ok)
                      service-id-3 (retrieve-service-id waiter-url service-id-headers)]
                  (with-service-cleanup
                    service-id-3

                    (is (not= service-id-1 service-id-3))
                    (is (not= service-id-2 service-id-3))
                    ;; fallback to service-id-2
                    (is (= service-id-2 (retrieve-service-id waiter-url request-headers)))
                    (is (= service-id-2 (kitchen-env-service-id)))
                    (thread-sleep fallback-period-secs)
                    ;; outside fallback duration
                    (is (= service-id-3 (kitchen-env-service-id)))
                    ;; delete service-id-3 to trigger fallback logic on next request
                    (delete-service waiter-url service-id-3)
                    ;; allow syncer state to get updated
                    (await-service-deletion-on-routers service-id-3)
                    ;; outside fallback duration
                    (is (= service-id-3 (kitchen-env-service-id)))))))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-valid-fallback-service-resolution
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url ".")
          request-headers {:x-waiter-token token}
          fallback-period-secs 300
          token-description-1 (-> (kitchen-request-headers :prefix "")
                                  (assoc :fallback-period-secs fallback-period-secs
                                         :idle-timeout-mins 1
                                         :name (str service-name "-v1")
                                         :permitted-user "*"
                                         :run-as-user (retrieve-username)
                                         :run-as-user (retrieve-username)
                                         :token token
                                         :version "version-1"))
          token-description-2 (-> token-description-1
                                  (assoc :name (str service-name "-v2")
                                         :version "version-2")
                                  (dissoc :cpus :mem :version))
          token-description-3 (-> token-description-1
                                  (assoc :cmd (kitchen-cmd (str "-p $PORT0 --start-up-sleep-ms 240000"))
                                         :grace-period-secs 300
                                         :name (str service-name "-v3")
                                         :version "version-3"))]
      (try
        (assert-response-status (post-token waiter-url token-description-1) http-200-ok)
        (let [service-id-1 (retrieve-service-id waiter-url request-headers)]
          (with-service-cleanup
            service-id-1
            (let [response-1 (make-request-with-debug-info request-headers #(make-request waiter-url "/hello" :headers %))]
              (assert-response-status response-1 http-200-ok)
              (is (= service-id-1 (:service-id response-1))))
            ;; allow every router to learn about the new instance
            (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")]
              (doseq [[_ router-url] (routers waiter-url)]
                (is (wait-for
                      (fn []
                        (let [active-instances (-> (service-settings router-url service-id-1 :cookies cookies)
                                                 (get-in [:instances :active-instances]))]
                          (and (seq active-instances)
                               (some :healthy? active-instances))))))))
            (assert-response-status (post-token waiter-url token-description-2) http-200-ok)
            (assert-response-status (make-request waiter-url "/hello" :headers request-headers) http-400-bad-request)
            (assert-response-status (post-token waiter-url token-description-3) http-200-ok)
            (let [service-id-2 (retrieve-service-id waiter-url (assoc request-headers :x-waiter-fallback-period-secs 0))]
              (is (not= service-id-1 service-id-2))
              (with-service-cleanup
                service-id-2
                (let [response-2 (make-request-with-debug-info request-headers #(make-request waiter-url "/hello" :headers %))]
                  (assert-response-status response-2 http-200-ok)
                  (is (= service-id-1 (:service-id response-2))))))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-profile-inside-token
  (testing-using-waiter-url
    (let [{:keys [profile-config]} (waiter-settings waiter-url)
          base-description {:cmd "will-not-work"
                            :cmd-type "shell"
                            :cpus 1
                            :health-check-url "/ping"
                            :mem 100
                            :metric-group "waiter_test"
                            :name "test-profile-inside-token"
                            :permitted-user "*"
                            :run-as-user (retrieve-username)
                            :version "1"}]
      (doseq [[profile {:keys [defaults]}] (seq profile-config)]
        (let [token (rand-name)
              token-description (-> (utils/remove-keys base-description (keys defaults))
                                  (assoc :profile (name profile) :token token))]
          (try
            (testing (str "token creation with profile " profile)
              (let [register-response (post-token waiter-url token-description)]
                (assert-response-status register-response http-200-ok)
                (let [{:keys [body]} (get-token waiter-url token)]
                  (is (= (:profile token-description) (-> body str json/read-str (get "profile"))))
                  (let [service-id (retrieve-service-id waiter-url {"x-waiter-token" token})
                        _ (is service-id "No service created by using token!")
                        service-description (service-id->service-description waiter-url service-id)]
                    (is (= service-description (dissoc token-description :token)))))))

            (testing (str "token creation with interstitial and profile " profile)
              (let [token-description (assoc token-description :interstitial-secs 10)
                    register-response (post-token waiter-url token-description)]
                (assert-response-status register-response http-200-ok)
                (let [{:keys [body]} (get-token waiter-url token)]
                  (is (= (:profile token-description) (-> body str json/read-str (get "profile"))))
                  (let [service-id (retrieve-service-id waiter-url {"x-waiter-token" token})
                        _ (is service-id "No service created by using token!")
                        service-description (service-id->service-description waiter-url service-id)]
                    (is (= service-description (dissoc token-description :token)))))))

            (finally
              (delete-token-and-assert waiter-url token))))))))

(deftest ^:parallel ^:integration-fast test-min-instances-and-namespace-combo-in-tokens
  (testing-using-waiter-url
    (let [token (rand-name)
          current-user (retrieve-username)
          base-description {:cmd "will-not-work"
                            :cmd-type "shell"
                            :cpus 1
                            :health-check-url "/ping"
                            :mem 100
                            :metric-group "waiter_test"
                            :name "test-profile-inside-token"
                            :permitted-user "*"
                            :run-as-user current-user
                            :token token
                            :version "1"}]
      (try
        (testing (str "token creation with increased min-instances")
          (testing "with default namespace"
            (let [token-description (assoc base-description :min-instances 50)
                  {:keys [body] :as register-response} (post-token waiter-url token-description)]
              (assert-response-status register-response http-400-bad-request)
              (is (str/includes? (str body) "min-instances (50) in the default namespace must be less than or equal to 4"))))

          (testing "with provided namespace"
            (let [token-description (assoc base-description :min-instances 50 :namespace current-user)
                  register-response (post-token waiter-url token-description)]
              (assert-response-status register-response http-200-ok)
              (let [{:keys [body]} (get-token waiter-url token :query-params {})
                    parsed-description (some-> body str json/read-str walk/keywordize-keys)]
                (is (= (-> token-description (dissoc :token) (assoc :owner current-user)) parsed-description))))))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast test-tokens-watch-maintainer
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          {:keys [body] :as response} (get-tokens-watch-maintainer-state waiter-url)
          {:strs [router-id state]} (try-parse-json body)
          router-url (router-endpoint waiter-url router-id)]
      (testing "no query parameters provide entire state"
        (assert-response-status response 200)
        (is (= (set (keys state))
               #{"token->token-index" "watches-count"})))
      (testing "include only watches-count"
        (let [{:keys [body] :as response}
              (get-tokens-watch-maintainer-state router-url :query-params "include=watches-count" :cookies cookies)
              {:strs [state]} (try-parse-json body)]
          (assert-response-status response 200)
          (is (= (set (keys state))
                 #{"watches-count"}))))
      (testing "include multiple fields"
        (let [{:keys [body] :as response}
              (get-tokens-watch-maintainer-state router-url
                                                 :query-params "include=watches-count&include=token->token-index"
                                                 :cookies cookies)
              {:strs [state]} (try-parse-json body)]
          (assert-response-status response 200)
          (is (= (set (keys state))
                 #{"token->token-index" "watches-count"})))))))
