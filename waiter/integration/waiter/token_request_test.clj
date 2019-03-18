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
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [plumbing.core :as pc]
            [waiter.headers :as headers]
            [waiter.service-description :as sd]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]
            [waiter.token :as token])
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
    (let [service-id-prefix (rand-name "testhostname")
          token (rand-name)
          update-token-fn (fn [version]
                            (let [{:keys [status]} (post-token waiter-url {:name service-id-prefix
                                                                           :cpus 1
                                                                           :mem 1250
                                                                           :version version
                                                                           :cmd "test command"
                                                                           :health-check-url "/ping"
                                                                           :permitted-user "*"
                                                                           :run-as-user (retrieve-username)
                                                                           :token token})]
                              (is (= 200 status))))
          validate-token-fn (fn [version num-threads num-iter]
                              (parallelize-requests
                                num-threads
                                num-iter
                                (fn []
                                  (let [{:keys [body status]} (get-token waiter-url token)]
                                    (is (= 200 status))
                                    (is (every? #(str/includes? (str body) (str %)) [service-id-prefix "1250" version "test command"]))
                                    (is (not-any? #(str/includes? (str body) (str %)) ["invalid"]))))))
          update-and-validate-token-fn (fn [version]
                                         (log/info "creating configuration using token" token "with version" version)
                                         (update-token-fn version)
                                         (log/info "asserting configuration for token" token "from routers (best-effort)")
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

(defn- service-id->source-tokens-entries [waiter-url service-id]
  (some-> (service-settings waiter-url service-id)
          :source-tokens
          walk/stringify-keys
          set))

(defn- token->etag [waiter-url token]
  (-> (get-token waiter-url token :query-params {"token" token})
      :headers
      (get "etag")))

(defn- make-source-tokens-entries [waiter-url & tokens]
  (mapv (fn [token] {"token" token "version" (token->etag waiter-url token)}) tokens))

(defn- create-token-name
  [waiter-url service-id-prefix]
  (str service-id-prefix "." (subs waiter-url 0 (str/index-of waiter-url ":"))))

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
      (assert-response-status ~response 200)
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
          token-prefix (create-token-name waiter-url service-id-prefix)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")
          num-tokens-to-create 10
          tokens-to-create (map #(str "token" %1 "." token-prefix) (range num-tokens-to-create))
          current-user (System/getProperty "user.name")
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)]

      (log/info "creating token without parameters should fail")
      (let [token (str service-id-prefix ".empty")
            {:keys [body] :as response} (post-token waiter-url {:token token})]
        (assert-response-status response 400)
        (is (str/includes? (str body) (str "No parameters provided for " token)) (str body)))

      (log/info "creating the tokens")
      (doseq [token tokens-to-create]
        (let [response (post-token waiter-url {:health-check-url "/check"
                                               :name service-id-prefix
                                               :token token})]
          (assert-response-status response 200)
          (is (str/includes? (:body response) (str "Successfully created " token)))))

      (testing "update without etags"
        (doseq [token tokens-to-create]
          (let [response (post-token waiter-url {:health-check-url "/health"
                                                 :name service-id-prefix
                                                 :token token})]
            (assert-response-status response 200)
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
            (assert-response-status response 200)
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
            (assert-response-status response 200)
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
              (assert-response-status response 200)
              (let [token-description (-> response :body json/read-str)]
                (is (= {"health-check-url" "/check", "name" service-id-prefix}
                       (-> token-description (get-in (repeat 2 "previous")) (select-keys sd/service-parameter-keys))))
                (is (= {"health-check-url" "/health", "name" service-id-prefix}
                       (-> token-description (get-in (repeat 1 "previous")) (select-keys sd/service-parameter-keys)))))
              (is actual-etag)))
          (testing "via x-waiter-token header"
            (let [{:keys [body headers] :as response} (get-token waiter-url token :cookies cookies)
                  actual-etag (get headers "etag")]
              (assert-response-status response 200)
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
            (assert-response-status tokens-response 200)
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
            (assert-response-status response 404)
            (is (str/includes? (str body) "Couldn't find token") (str body)))
          (-> (get-token router-url token
                         :cookies cookies
                         :query-params {"include" ["deleted" "metadata"]})
              (assert-token-response token-root token-cluster service-id-prefix true))
          (let [{:keys [body] :as tokens-response}
                (list-tokens router-url current-user cookies {"include" ["metadata"]})
                tokens (json/read-str body)]
            (assert-response-status tokens-response 200)
            (is (every? (fn [token-entry] (contains? token-entry "deleted")) tokens))
            (is (every? (fn [token-entry] (contains? token-entry "etag")) tokens))
            (is (not-any? (fn [token-entry] (= token (get token-entry "token"))) tokens)))
          (let [{:keys [body] :as tokens-response}
                (list-tokens router-url current-user cookies {"include" ["deleted" "metadata"]})
                tokens (json/read-str body)]
            (assert-response-status tokens-response 200)
            (is (every? (fn [token-entry] (contains? token-entry "deleted")) tokens))
            (is (every? (fn [token-entry] (contains? token-entry "etag")) tokens))
            (is (some (fn [token-entry] (= token (get token-entry "token"))) tokens)))))

      (log/info "hard-deleting the tokens")
      (doseq [token tokens-to-create]
        (delete-token-and-assert waiter-url token)
        (doseq [[_ router-url] (routers waiter-url)]
          (let [{:keys [body] :as response} (get-token router-url token :cookies cookies)]
            (assert-response-status response 404)
            (is (str/includes? (str body) "Couldn't find token") (str {:body body :token token})))
          (let [{:keys [body] :as tokens-response}
                (list-tokens router-url current-user cookies {"include" ["deleted" "metadata"]})
                token-entries (json/read-str body)]
            (assert-response-status tokens-response 200)
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
            (assert-response-status response 404)
            (is (str/includes? (str body) "Couldn't find token") (str body))))))))

(deftest ^:parallel ^:integration-fast ^:resource-heavy test-service-list-filtering
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token-1 (create-token-name waiter-url (str "www." service-name ".t1"))
          token-2 (create-token-name waiter-url (str "www." service-name ".t2"))
          service-ids-atom (atom #{})
          token->version->etag-atom (atom {})
          all-tokens [token-1 token-2]
          all-version-suffixes ["v1" "v2" "v3"]
          router-id->router-url (routers waiter-url)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")]

      (doseq [version-suffix all-version-suffixes]
        (doseq [token all-tokens]
          (let [token-description (merge
                                    (kitchen-request-headers :prefix "")
                                    {:fallback-period-secs 0
                                     :name (str service-name "." (hash token))
                                     :token token
                                     :version (str service-name "." version-suffix)})
                {:keys [headers] :as response} (post-token waiter-url token-description)]
            (assert-response-status response 200)
            (let [token-etag (get headers "etag")]
              (log/info token "->" token-etag)
              (is (-> token-etag str/blank? not))
              (let [{:keys [service-id] :as response} (make-request-with-debug-info
                                                        {:x-waiter-token token}
                                                        #(make-request waiter-url "/environment" :headers %))]
                (assert-response-status response 200)
                (is (-> service-id str/blank? not))
                (swap! service-ids-atom conj service-id)
                ;; ensure all routers know about the service
                (doseq [[_ router-url] router-id->router-url]
                  (let [response (make-request-with-debug-info
                                   {:x-waiter-token token}
                                   #(make-request router-url "/environment" :headers % :cookies cookies))]
                    (assert-response-status response 200)
                    (is (= service-id (:service-id response))))))
              (swap! token->version->etag-atom assoc-in [token version-suffix] token-etag)))))
      (is (= (* (count all-tokens) (count all-version-suffixes)) (count @service-ids-atom))
          (str {:service-ids @service-ids-atom}))

      (testing "star in token filter"
        (doseq [[_ router-url] (routers waiter-url)]
          (let [query-params {"token" (str "www." service-name ".t*")}
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
            (assert-response-status response 200)
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
            (assert-response-status response 200)
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
              (assert-response-status response 200)
              (is (= 1 (count service-token-versions))
                  (str {:query-params query-params
                        :router-url router-url
                        :service-count (count services)
                        :service-token-versions service-token-versions}))))))

      (doseq [service-id @service-ids-atom]
        (delete-service waiter-url service-id)))))

(deftest ^:parallel ^:integration-fast test-hostname-token
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token-prefix (str "token-" (rand-int 3000000))
          token (create-token-name waiter-url token-prefix)
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)]
      (testing "hostname-token-test"
        (try
          (log/info "basic hostname as token test")
          (let [current-user (retrieve-username)
                service-description (assoc (kitchen-request-headers)
                                           :x-waiter-metric-group token-prefix
                                           :x-waiter-permitted-user "*"
                                           :x-waiter-run-as-user current-user)]
            (testing "hostname-token-creation"
              (log/info "creating configuration using token" token)
              (let [{:keys [body status]}
                    (post-token waiter-url {:health-check-url "/probe"
                                            :metric-group token-prefix
                                            :name service-id-prefix
                                            :token token})]
                (when (not= 200 status)
                  (log/info "error:" body)
                  (is (not body))))
              (log/info "created configuration using token" token)
              (let [token-response (get-token waiter-url token)
                    response-body (json/read-str (:body token-response))]
                (is (contains? response-body "last-update-time"))
                (is (= {"cluster" token-cluster
                        "health-check-url" "/probe"
                        "last-update-user" (retrieve-username)
                        "metric-group" token-prefix
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
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
                (is (= #{(make-source-tokens-entries waiter-url token)}
                       (service-id->source-tokens-entries waiter-url service-id)))
                (is (= token-prefix (service-id->metric-group waiter-url service-id))
                    (str {:service-id service-id :token token})))

              (log/info "request with hostname token" token "along with x-waiter headers except permitted-user")
              (let [request-headers (merge (dissoc service-description :x-waiter-permitted-user) {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
                (is (= #{(make-source-tokens-entries waiter-url token)}
                       (service-id->source-tokens-entries waiter-url service-id)))
                (is (= token-prefix (service-id->metric-group waiter-url service-id))
                    (str {:service-id service-id :token token}))
                ;; the above request hashes to a different service-id than the rest of the test, so we need to cleanup
                (delete-service waiter-url service-id))

              (log/info "request with hostname token" token "along with x-waiter headers except run-as-user")
              (let [request-headers (merge (dissoc service-description :x-waiter-run-as-user) {"host" token})
                    path "/foo"
                    response (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url (:request-headers response))]
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
                (is (= #{(make-source-tokens-entries waiter-url token)}
                       (service-id->source-tokens-entries waiter-url service-id)))
                (is (= token-prefix (service-id->metric-group waiter-url service-id))
                    (str {:service-id service-id :token token}))

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

              (log/info "request with hostname token" token "along with missing x-waiter headers except cmd")
              (let [request-headers (merge (dissoc service-description :x-waiter-cmd) {"host" token})
                    path "/foo"
                    {:keys [body] :as response} (make-request waiter-url path :headers request-headers)]
                (is (some #(str/includes? body %) ["cmd must be a non-empty string" "Invalid command or version"])
                    (str "response body was: " response))
                (assert-response-status response 400))

              (log/info "request with hostname token and x-waiter-debug token" token "along with x-waiter headers")
              (let [request-headers (merge service-description {:x-waiter-debug "true", "host" token})
                    path "/foo"
                    {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
                    service-id (retrieve-service-id waiter-url request-headers)]
                (assert-response-status response 200)
                (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
                (is (= #{(make-source-tokens-entries waiter-url token)}
                       (service-id->source-tokens-entries waiter-url service-id)))
                (is (every? #(not (str/blank? (get headers %)))
                            (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
                    (str headers))
                (is (= token-prefix (service-id->metric-group waiter-url service-id))
                    (str {:service-id service-id :token token}))
                (delete-service waiter-url service-id))))
          (finally
            (delete-token-and-assert waiter-url token)))))))

(deftest ^:parallel ^:integration-fast test-token-administer-unaffected-by-run-as-user-permissions
  (testing-using-waiter-url
    (let [service-id-prefix (rand-name)
          token-root (retrieve-token-root waiter-url)
          token-cluster (retrieve-token-cluster waiter-url)]
      (testing "token-administering"
        (testing "active-token"
          (let [last-update-time (System/currentTimeMillis)
                token (create-token-name waiter-url service-id-prefix)]

            (let [token-description {:health-check-url "/probe"
                                     :name service-id-prefix
                                     :owner (retrieve-username)
                                     :run-as-user (retrieve-username)
                                     :token token}
                  response (post-token waiter-url token-description)]
              (assert-response-status response 200))

            (testing "if-match-required during update"
              (let [token-description {:health-check-url "/probe-1"
                                       :last-update-time last-update-time
                                       :name service-id-prefix
                                       :owner (retrieve-username)
                                       :token token}
                    {:keys [body] :as response} (post-token waiter-url token-description
                                                            :headers {}
                                                            :query-params {"update-mode" "admin"})]
                (assert-response-status response 400)
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
                  (assert-response-status response 200)
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
                  (assert-response-status response 400)
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
                token (create-token-name waiter-url service-id-prefix)]
            (try
              (log/info "creating configuration using token" token)
              (let [token-description {:health-check-url "/probe-3"
                                       :name service-id-prefix
                                       :owner (retrieve-username)
                                       :token token}
                    response (post-token waiter-url token-description :headers {"host" token})]
                (assert-response-status response 200))

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
                (assert-response-status response 200))
              (log/info "created configuration using token" token)
              (let [{:keys [body] :as response} (get-token waiter-url token)]
                (assert-response-status response 404)
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
          token (create-token-name waiter-url service-id-prefix)]
      (try
        (log/info "creating configuration using token" token)
        (let [token-definition (assoc
                                 (kitchen-request-headers :prefix "")
                                 :name service-id-prefix
                                 :token token)
              {:keys [body status]} (post-token waiter-url token-definition)]
          (when (not= 200 status)
            (log/info "error:" body)
            (is (not body))))
        (log/info "created configuration using token" token)
        (log/info "retrieving configuration for token" token)
        (let [token-response (get-token waiter-url token)
              response-body (str (:body token-response))]
          (when (not (str/includes? response-body service-id-prefix))
            (log/info response-body))
          (assert-response-status token-response 200)
          (is (str/includes? response-body service-id-prefix)))
        (log/info "asserted retrieval of configuration for token" token)

        (log/info "making Waiter request with token" token "in header")
        (let [request-headers {:x-waiter-token token}
              path "/foo"
              response (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url (:request-headers response))]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (= #{(make-source-tokens-entries waiter-url token)}
                 (service-id->source-tokens-entries waiter-url service-id))))

        (log/info "making Waiter request with token and x-waiter-debug token" token "in header")
        (let [request-headers {:x-waiter-debug "true", :x-waiter-token token}
              path "/foo"
              {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url request-headers)]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (= #{(make-source-tokens-entries waiter-url token)}
                 (service-id->source-tokens-entries waiter-url service-id)))
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
        (log/info "creating configuration using token" token)
        (let [token-definition (assoc
                                 (kitchen-request-headers :prefix "")
                                 :name service-id-prefix
                                 :run-as-user "*"
                                 :token token)
              {:keys [body status]} (post-token waiter-url token-definition)]
          (when (not= 200 status)
            (log/info "error:" body)
            (is (not body))))
        (log/info "created configuration using token" token)
        (log/info "retrieving configuration for token" token)
        (let [token-response (get-token waiter-url token)
              response-body (str (:body token-response))]
          (when (not (str/includes? response-body service-id-prefix))
            (log/info response-body))
          (assert-response-status token-response 200)
          (is (str/includes? response-body service-id-prefix)))
        (log/info "asserted retrieval of configuration for token" token)

        (let [{:keys [body] :as token-response} (get-token waiter-url token)
              token-description (try (json/read-str (str body))
                                     (catch Exception _
                                       (is false (str "Failed to parse token" body))))]
          (assert-response-status token-response 200)
          (is (= "/status" (token-description "health-check-url")))
          (is (= service-id-prefix (token-description "name")))
          (is (= "*" (token-description "run-as-user")))
          (is (System/getProperty "user.name") (token-description "owner")))

        (log/info "making Waiter request with token" token "in header")
        (let [request-headers {:x-waiter-token token}
              path "/foo"
              response (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url (:request-headers response))]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (= #{(make-source-tokens-entries waiter-url token)}
                 (service-id->source-tokens-entries waiter-url service-id)))
          (is (= (retrieve-username) (:run-as-user (service-id->service-description waiter-url service-id)))))

        (log/info "making Waiter request with token and x-waiter-debug token" token "in header")
        (let [request-headers {:x-waiter-debug "true", :x-waiter-token token}
              path "/foo"
              {:keys [headers request-headers] :as response} (make-request waiter-url path :headers request-headers)
              service-id (retrieve-service-id waiter-url request-headers)]
          (assert-response-status response 200)
          (is (= (name-from-service-description waiter-url service-id) service-id-prefix))
          (is (= #{(make-source-tokens-entries waiter-url token)}
                 (service-id->source-tokens-entries waiter-url service-id)))
          (is (every? #(not (str/blank? (get headers %)))
                      (concat required-response-headers (retrieve-debug-response-headers waiter-url)))
              (str headers))
          (delete-service waiter-url service-id))
        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-fast ^:explicit test-on-the-fly-to-token
  (testing-using-waiter-url
    (let [name-string (rand-name)
          {:keys[cookies] :as canary-response}
          (make-request-with-debug-info {:x-waiter-name name-string} #(make-kitchen-request waiter-url %))
          service-id-1 (:service-id canary-response)]
      (with-service-cleanup
        service-id-1
        (is (str/includes? service-id-1 name-string) (str "ERROR: App-name is missing " name-string))
        (assert-service-on-all-routers waiter-url service-id-1 cookies)
        (is (nil? (service-id->source-tokens-entries waiter-url service-id-1)))
        (let [token (str "^SERVICE-ID#" service-id-1)
              response (make-request-with-debug-info {:x-waiter-token token} #(make-request waiter-url "" :headers %))
              service-id-2 (:service-id response)]
          (with-service-cleanup
            service-id-2
            (assert-response-status response 200)
            (is service-id-2)
            (is (= service-id-1 service-id-2) "The on-the-fly and token-based service ids do not match")
            (assert-service-on-all-routers waiter-url service-id-1 cookies)
            (is (= #{(make-source-tokens-entries waiter-url token)}
                   (service-id->source-tokens-entries waiter-url service-id-2)))))))))

(deftest ^:parallel ^:integration-fast test-bad-token
  (testing-using-waiter-url

    (let [common-headers {"x-waiter-cmd" "foo-bar"
                          "x-waiter-cmd-type" "shell"}]
      (testing "ignore missing token when have service description"
        (let [response (make-request waiter-url "/pathabc" :headers (assoc common-headers "host" "missing_token"))]
          (is (str/includes? (:body response) "Service description using waiter headers/token improperly configured"))
          (assert-response-status response 400)))

      (testing "ignore invalid token when have service description"
        (let [response (make-request waiter-url "/pathabc" :headers (assoc common-headers "host" "bad/token"))]
          (is (str/includes? (:body response) "Service description using waiter headers/token improperly configured"))
          (assert-response-status response 400)))

      (testing "ignore invalid token when have invalid service description"
        (let [response (make-request waiter-url "/pathabc" :headers (assoc common-headers "host" "bad/token"))]
          (is (str/includes? (:body response) "Service description using waiter headers/token improperly configured"))
          (assert-response-status response 400))))

    (testing "can't use missing token server"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "missing_token"})]
        (is (str/includes? (:body response) "Unable to identify service using waiter headers/token"))
        (assert-response-status response 400)))

    (testing "can't use invalid token server"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "bad/token"})]
        (is (str/includes? (:body response) "Unable to identify service using waiter headers/token"))
        (assert-response-status response 400)))

    (testing "can't use invalid token that's valid for zookeeper"
      (let [response (make-request waiter-url "/pathabc" :headers {"X-Waiter-Token" "bad#token"})]
        (is (str/includes? (:body response) "Token not found: bad#token"))
        (assert-response-status response 400)))

    (testing "can't use invalid token"
      (let [response (make-request waiter-url "/pathabc" :headers {"X-Waiter-Token" "bad/token"})]
        (is (str/includes? (:body response) "Token must match pattern"))
        (assert-response-status response 400)))

    (testing "can't use invalid token with host set"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "missing_token" "X-Waiter-Token" "bad/token"})]
        (is (str/includes? (:body response) "Token must match pattern"))
        (assert-response-status response 400)))

    (testing "can't use missing token with host set"
      (let [response (make-request waiter-url "/pathabc" :headers {"host" "missing_token" "X-Waiter-Token" "missing_token"})]
        (is (str/includes? (:body response) "Token not found: missing_token"))
        (assert-response-status response 400)))

    (testing "can't use missing token"
      (let [response (make-request waiter-url "/pathabc" :headers {"X-Waiter-Token" "missing_token"})]
        (is (str/includes? (:body response) "Token not found: missing_token"))
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
            response (post-token waiter-url service-desc)]
        (is (str/includes? (:body response) "Token must match pattern"))
        (assert-response-status response 400)))))

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
      (assert-response-status register-response 200)
      (is (= (:metadata service-desc) (get (json/read-str body) "metadata")))
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
          register-response (post-token waiter-url service-desc)]
      (is (= 400 (:status register-response))))))

(deftest ^:parallel ^:integration-fast test-token-bad-payload
  (testing-using-waiter-url
    (let [{:keys [status]} (make-request waiter-url "/token"
                                         :body "i'm bad at json"
                                         :headers {"host" "test-token"}
                                         :method :post)]
      (is (= 400 status)))))

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
          {:keys [service-id status] :as response} (make-request-with-debug-info {:x-waiter-token token}
                                                                                 #(make-light-request waiter-url %))
          {:keys [env] :as service-description} (response->service-description waiter-url response)]
      (is (= 200 (:status token-response)) (:body token-response))
      (is (= 200 status))
      (is (= {:BINARY binary} env) (str service-description))
      (delete-token-and-assert waiter-url token)
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-token-https-redirects
  (testing-using-waiter-url
    (let [token (rand-name)
          token-response (post-token waiter-url {:cmd (str (kitchen-cmd) " -p $PORT0")
                                                 :cmd-type "shell"
                                                 :https-redirect true
                                                 :name token
                                                 :version "does-not-matter"
                                                 :token token})]
      (assert-response-status token-response 200)

      (testing "https redirects"
        (let [request-headers {:x-waiter-token token}
              url (URL. (str "http://" waiter-url))
              endpoint "/request-info"]

          (testing "get request"
            (let [{:keys [headers] :as response}
                  (make-kitchen-request waiter-url request-headers :method :get :path endpoint)]
              (assert-response-status response 301)
              (is (= (str "https://" (.getHost url) endpoint) (get headers "location")))
              (is (str/starts-with? (str (get headers "server")) "waiter") (str "headers:" headers))))

          (testing "post request"
            (let [{:keys [headers] :as response}
                  (make-kitchen-request waiter-url request-headers :method :post :path endpoint)]
              (assert-response-status response 307)
              (is (= (str "https://" (.getHost url) endpoint) (get headers "location")))
              (is (str/starts-with? (str (get headers "server")) "waiter") (str "headers:" headers))))))

      (delete-token-and-assert waiter-url token))))

(defmacro run-token-param-support
  [waiter-url request-fn request-headers expected-env]
  `(let [response# (make-request-with-debug-info ~request-headers ~request-fn)
         service-description# (response->service-description ~waiter-url response#)
         service-id# (:service-id response#)]
     (is (= 200 (:status response#)))
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
                               :name "test-token-param-support"
                               :version "does-not-matter"}
          kitchen-request #(make-kitchen-request waiter-url % :path "/environment")]
      (try
        (let [token-description (assoc service-description :token token)
              token-response (post-token waiter-url token-description :query-params {"update-mode" "admin"})]
          (assert-response-status token-response 200)
          (let [request-headers {:x-waiter-param-my_variable "value-1"
                                 :x-waiter-token token}
                {:keys [body status]} (make-request-with-debug-info request-headers kitchen-request)]
            (is (= 400 status))
            (is (str/includes? body "Some params cannot be configured")))
          (let [request-headers {:x-waiter-allowed-params ""
                                 :x-waiter-param-my_variable "value-1"
                                 :x-waiter-token token}
                {:keys [body status]} (make-request-with-debug-info request-headers kitchen-request)]
            (is (= 400 status))
            (is (str/includes? body "Some params cannot be configured"))))
        (let [token-description (-> service-description (dissoc :allowed-params) (assoc :token (str token ".1")))
              token-response (post-token waiter-url token-description :query-params {"update-mode" "admin"})]
          (assert-response-status token-response 200)
          (let [request-headers {:x-waiter-param-my_variable "value-1"
                                 :x-waiter-token token}
                {:keys [body status]} (make-request-with-debug-info request-headers kitchen-request)]
            (is (= 400 status))
            (is (str/includes? body "Some params cannot be configured"))))
        (let [service-ids (->> [(async/thread
                                  (run-token-param-support
                                    waiter-url kitchen-request
                                    {:x-waiter-token token}
                                    {:BINARY binary :FEE "FIE" :FOO "BAR"}))
                                (async/thread
                                  (run-token-param-support
                                    waiter-url kitchen-request
                                    {:x-waiter-param-fee "value-1p" :x-waiter-token token}
                                    {:BINARY binary :FEE "value-1p" :FOO "BAR"}))
                                (async/thread
                                  (run-token-param-support
                                    waiter-url kitchen-request
                                    {:x-waiter-allowed-params ["FEE" "FII" "FOO"] :x-waiter-param-fee "value-1p" :x-waiter-token token}
                                    {:BINARY binary :FEE "value-1p" :FOO "BAR"}))
                                (async/thread
                                  (run-token-param-support
                                    waiter-url kitchen-request
                                    {:x-waiter-allowed-params "FEE,FOO" :x-waiter-param-fee "value-1p" :x-waiter-token token}
                                    {:BINARY binary :FEE "value-1p" :FOO "BAR"}))
                                (async/thread
                                  (run-token-param-support
                                    waiter-url kitchen-request
                                    {:x-waiter-param-fee "value-1p" :x-waiter-param-foo "value-2p" :x-waiter-token token}
                                    {:BINARY binary :FEE "value-1p" :FOO "value-2p"}))]
                               (map async/<!!)
                               doall
                               (into #{}))]
          (is (= 5 (count service-ids)) "Unique service-ids were not created!"))
        (finally
          (delete-token-and-assert waiter-url token)
          (delete-token-and-assert waiter-url (str token ".1")))))))

(deftest ^:parallel ^:integration-fast test-token-invalid-environment-variables
  (testing-using-waiter-url
    (let [{:keys [body status]} (post-token waiter-url {:env {"HOME" "/my/home"}
                                                        :token (rand-name)})]
      (is (= 400 status))
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
              {:keys [body status]} (post-token waiter-url {parameter param-value :token (rand-name)})]
          (is (= 400 status))
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
          token (create-token-name waiter-url service-name)
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
            (assert-response-status response 200)))

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
                                    :method :post
                                    :multipart {"mode" "service"
                                                "service-id" service-id})]
                  (reset! cookies-atom cookies)
                  (is (= "Added cookie x-waiter-consent" body))
                  ; x-waiter-consent should be emitted Waiter
                  (is (has-x-waiter-consent? cookies))
                  (assert-response-status response 200)))

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
                (let [{:keys [headers] :as response}
                      (make-request waiter-url "/hello-world" :cookies @cookies-atom :headers {"host" host-header})]
                  (is (= (str "/waiter-consent/hello-world")
                         (get headers "location")))
                  (assert-response-status response 303)))

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
                  (assert-response-status response 200)))

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
            (assert-response-status response 200)))

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
            (assert-response-status response 200)
            (is (= "Hello World" body))))

        (testing "backend request headers"
          (let [{:keys [body] :as response}
                (make-request waiter-url "/request-info" :headers request-headers :disable-auth true)
                {:strs [headers]} (json/read-str (str body))
                service-id (retrieve-service-id waiter-url (:request-headers response))]
            (assert-response-status response 200)
            (is (not (contains? headers "x-waiter-auth-principal")))
            (is (not (contains? headers "x-waiter-authenticated-principal")))
            (is (contains? headers "x-cid"))
            (delete-service waiter-url service-id)))

        (finally
          (delete-token-and-assert waiter-url token))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-service-fallback-support
  (testing-using-waiter-url
    (let [service-name (rand-name)
          token (create-token-name waiter-url service-name)
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
              _ (assert-response-status post-response 200)
              service-id-1 (retrieve-service-id waiter-url service-id-headers)]

          (testing "token response contains fallback and owner"
            (let [{:keys [body] :as response} (get-token waiter-url token :query-params {})]
              (assert-response-status response 200)
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
                  _ (assert-response-status post-response 200)
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
                      _ (assert-response-status post-response 200)
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
          token (create-token-name waiter-url service-name)
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
      (assert-response-status (post-token waiter-url token-description-1) 200)
      (let [service-id-1 (retrieve-service-id waiter-url request-headers)]
        (with-service-cleanup
          service-id-1
          (let [response-1 (make-request-with-debug-info request-headers #(make-request waiter-url "/hello" :headers %))]
            (assert-response-status response-1 200)
            (is (= service-id-1 (:service-id response-1))))
          (assert-response-status (post-token waiter-url token-description-2) 200)
          (assert-response-status (make-request waiter-url "/hello" :headers request-headers) 400)
          (assert-response-status (post-token waiter-url token-description-3) 200)
          (let [service-id-2 (retrieve-service-id waiter-url (assoc request-headers :x-waiter-fallback-period-secs 0))]
            (is (not= service-id-1 service-id-2))
            (with-service-cleanup
              service-id-2
              (let [response-2 (make-request-with-debug-info request-headers #(make-request waiter-url "/hello" :headers %))]
                (assert-response-status response-2 200)
                (is (= service-id-1 (:service-id response-2)))))))))))
