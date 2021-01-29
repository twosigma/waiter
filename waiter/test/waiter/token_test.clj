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
(ns waiter.token-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [schema.core :as s]
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.token :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.io StringBufferInputStream)
           (org.joda.time DateTime)))

(def ^:const history-length 5)
(def ^:const limit-per-owner 10)

(let [current-time (t/now)]
  (defn- clock [] current-time)

  (defn- clock-millis [] (.getMillis ^DateTime (clock))))

(let [lock (Object.)]
  (defn- synchronize-fn
    [_ f]
    (locking lock
      (f))))

(defn- public-entitlement-manager
  []
  (reify authz/EntitlementManager
    (authorized? [_ _ _ _]
      true)))

(deftest test-constant-cluster-calculator
  (let [default-cluster "test-cluster"
        cluster-calculator-1 (new-configured-cluster-calculator {:default-cluster default-cluster
                                                                 :host->cluster {}})
        cluster-calculator-2 (new-configured-cluster-calculator {:default-cluster default-cluster
                                                                 :host->cluster {"waiter.localtest.me" "bar"
                                                                                 "waiter-foo.localtest.me" "foo"}})]
    (is (= default-cluster (get-default-cluster cluster-calculator-1)))
    (is (= default-cluster (get-default-cluster cluster-calculator-2)))
    (is (= default-cluster (calculate-cluster cluster-calculator-1 {})))
    (is (= default-cluster (calculate-cluster cluster-calculator-2 {})))
    (is (= default-cluster (calculate-cluster cluster-calculator-1 {:headers {"host" "test.com"}})))
    (is (= default-cluster (calculate-cluster cluster-calculator-2 {:headers {"host" "test.com"}})))
    (is (= default-cluster (calculate-cluster cluster-calculator-1 {:headers {"host" "waiter.localtest.me"}})))
    (is (= "bar" (calculate-cluster cluster-calculator-2 {:headers {"host" "waiter.localtest.me"}})))
    (is (= default-cluster (calculate-cluster cluster-calculator-1 {:headers {"host" "waiter-foo.localtest.me"}})))
    (is (= "foo" (calculate-cluster cluster-calculator-2 {:headers {"host" "waiter-foo.localtest.me"}})))
    (is (= default-cluster (calculate-cluster cluster-calculator-1 {:headers {"host" "waiter-foo-bar.localtest.me:1234"}})))
    (is (= default-cluster (calculate-cluster cluster-calculator-2 {:headers {"host" "waiter-foo-bar.localtest.me:1234"}})))
    (is (= default-cluster (calculate-cluster cluster-calculator-1 {:headers {"host" "waiter-foo.bar.localtest.me"}})))
    (is (= default-cluster (calculate-cluster cluster-calculator-2 {:headers {"host" "waiter-foo.bar.localtest.me"}})))))

(defn- run-handle-token-request
  [kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
   attach-service-defaults-fn request]
  (let [cluster-calculator (new-configured-cluster-calculator {:default-cluster (str token-root "-cluster")
                                                               :host->cluster {}})]
    (handle-token-request clock synchronize-fn kv-store cluster-calculator token-root history-length limit-per-owner
                          waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                          attach-service-defaults-fn request)))

(def optional-metadata-keys (disj sd/user-metadata-keys "owner"))

(def required-metadata-keys (conj sd/system-metadata-keys "owner"))

(deftest test-handle-token-request
  (with-redefs [sd/service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd sd/service-parameter-keys))))]
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          service-id-prefix "test#"
          entitlement-manager (authz/->SimpleEntitlementManager nil)
          make-peer-requests-fn (fn [endpoint & _] (and (str/starts-with? endpoint "token/") (str/ends-with? endpoint "/refresh")) {})
          token "test-token"
          service-description-1 (walk/stringify-keys
                                  {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :permitted-user "tu2" :token token
                                   :metadata {"a" "b" "c" "d"} :env {"MY_VAR" "a" "MY_VAR_2" "b"}})
          service-id-1 (sd/service-description->service-id service-id-prefix service-description-1)
          service-description-2 (walk/stringify-keys
                                  {:cmd "tc2" :cpus 2 :mem 400 :version "d1e2f3" :run-as-user "tu1" :permitted-user "tu3" :token token})
          attach-service-defaults-fn identity
          service-id-2 (sd/service-description->service-id service-id-prefix service-description-2)
          token-root "test-token-root"
          auth-user "tu1"
          waiter-hostname "waiter-hostname.app.example.com"
          waiter-hostnames [waiter-hostname]
          handle-list-tokens-request (wrap-handler-json-response handle-list-tokens-request)]

      (testing "put:unsupported-request-method"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {}
                 :request-method :put})]
          (is (= http-405-method-not-allowed status))
          (is (str/includes? body "Invalid request method"))))

      (testing "delete:no-token-in-request"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {}
                 :request-method :delete})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Couldn't find token in request"))))

      (testing "delete:token-does-not-exist"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" (str "invalid-" token)}
                 :request-method :delete})]
          (is (= http-404-not-found status))
          (is (str/includes? body (str "Token invalid-" token " does not exist")))))

      (testing "delete:token-does-exist-unauthorized"
        (try
          (kv/store kv-store token (assoc service-description-1 "owner" "tu2"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:authorization/user auth-user
                   :headers {"accept" "application/json"
                             "x-waiter-token" token}
                   :request-method :delete})
                {{message "message"
                  {:strs [owner user]} "details"} "waiter-error"} (json/read-str body)]
            (is (= http-403-forbidden status))
            (is (= "User not allowed to delete token" message))
            (is (= "tu2" owner) body)
            (is (= "tu1" user))
            (is (not (nil? (kv/fetch kv-store token)))))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:already-deleted"
        (let [token-last-update-time (- (clock-millis) 10000)
              token-description (assoc service-description-1
                                  "owner" "tu1"
                                  "deleted" true
                                  "last-update-time" token-last-update-time)]
          (try
            (kv/store kv-store token token-description)
            (is (not (nil? (kv/fetch kv-store token))))
            (let [{:keys [body status]}
                  (run-handle-token-request
                    kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                    {:authorization/user auth-user
                     :headers {"x-waiter-token" token}
                     :request-method :delete})]
              (is (= http-404-not-found status))
              (is (str/includes? body (str "Token " token " does not exist")))
              (is (= token-description (kv/fetch kv-store token)) "Entry deleted from kv-store!"))
            (finally
              (kv/delete kv-store token)))))

      (testing "delete:token-does-exist-authorized:hard-delete-missing"
        (try
          (kv/store kv-store token (assoc service-description-1 "owner" "tu1"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:authorization/user auth-user
                   :headers {"x-waiter-token" token}
                   :request-method :delete})]
            (is (= http-200-ok status))
            (is (every? #(str/includes? body (str %)) [(str "\"delete\":\"" token "\""), "\"success\":true"]))
            (is (= (assoc service-description-1
                     "deleted" true
                     "last-update-time" (clock-millis)
                     "last-update-user" auth-user
                     "owner" "tu1"
                     "previous" (assoc service-description-1 "owner" "tu1"))
                   (kv/fetch kv-store token))
                "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:hard-delete-false"
        (try
          (kv/store kv-store token (assoc service-description-1 "owner" "tu1"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:authorization/user auth-user
                   :headers {"x-waiter-token" token}
                   :query-params {"hard-delete" "false"}
                   :request-method :delete})]
            (is (= http-200-ok status))
            (is (every? #(str/includes? body (str %)) [(str "\"delete\":\"" token "\""), "\"success\":true"]))
            (is (= (assoc service-description-1
                     "deleted" true
                     "last-update-time" (clock-millis)
                     "last-update-user" "tu1"
                     "owner" "tu1"
                     "previous" (assoc service-description-1 "owner" "tu1"))
                   (kv/fetch kv-store token))
                "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:hard-delete-true:without-if-match-header"
        (try
          (->> (assoc service-description-1 "owner" "tu1" "last-update-time" (- (clock-millis) 1000))
               (kv/store kv-store token))
          (is (kv/fetch kv-store token))
          (let [{:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:authorization/user auth-user
                   :headers {"x-waiter-token" token}
                   :query-params {"hard-delete" "true"}
                   :request-method :delete})]
            (is (= http-400-bad-request status))
            (is (str/includes? body "Must specify if-match header for token hard deletes"))
            (is (kv/fetch kv-store token)))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:hard-delete-true:with-stale-if-match-header"
        (try
          (->> (assoc service-description-1 "owner" "tu1" "last-update-time" (- (clock-millis) 1000))
               (kv/store kv-store token))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:authorization/user auth-user
                   :headers {"if-match" (str (- (clock-millis) 5000)) "x-waiter-token" token}
                   :query-params {"hard-delete" "true"}
                   :request-method :delete})]
            (is (= 412 status))
            (is (str/includes? body "Cannot modify stale token"))
            (is (kv/fetch kv-store token) "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:hard-delete-true:with-valid-if-match-header"
        (let [service-description-1' (assoc service-description-1 "owner" "tu1" "last-update-time" (- (clock-millis) 1000))]
          (try
            (kv/store kv-store token service-description-1')
            (is (not (nil? (kv/fetch kv-store token))))
            (let [token-hash (sd/token-data->token-hash service-description-1')
                  {:keys [body headers status]}
                  (run-handle-token-request
                    kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                    {:authorization/user auth-user
                     :headers {"if-match" token-hash
                               "x-waiter-token" token}
                     :query-params {"hard-delete" "true"}
                     :request-method :delete})]
              (is (= http-200-ok status))
              (is (str/includes? body (str "\"delete\":\"" token "\"")))
              (is (str/includes? body "\"hard-delete\":true"))
              (is (str/includes? body "\"success\":true"))
              (is (get headers "etag"))
              (is (nil? (kv/fetch kv-store token)) "Entry not deleted from kv-store!"))
            (finally
              (kv/delete kv-store token)))))

      (testing "delete:token-does-exist-authorized:hard-delete-true:disallowed"
        (try
          (->> (assoc service-description-1 "owner" "tu2" "last-update-time" (- (clock-millis) 1000))
               (kv/store kv-store token))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [entitlement-manager (reify authz/EntitlementManager
                                      (authorized? [_ subject verb {:keys [user]}]
                                        (is (and (= subject "tu1") (= :admin verb) (= "tu2" user)))
                                        false))
                {:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:authorization/user auth-user
                   :headers {"accept" "application/json"
                             "if-match" (str (clock-millis))
                             "x-waiter-token" token}
                   :query-params {"hard-delete" "true"}
                   :request-method :delete})
                {{message "message"
                  {{:strs [owner]} "metadata"} "details"} "waiter-error"} (json/read-str body)]
            (is (= http-403-forbidden status))
            (is (= "Cannot hard-delete token" message))
            (is (= "tu2" owner) body)
            (is (not-empty (kv/fetch kv-store token)) "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "get:empty-service-description"
        (let [{:keys [status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" token}
                 :request-method :get})]
          (is (= http-404-not-found status))))

      (testing "get:token-with-only-metadata"
        (try
          (->> {"fallback-period-secs" 100
                "https-redirect" true
                "owner" "tu1"
                "stale-timeout-mins" 15}
            (kv/store kv-store token))
          (let [{:keys [body headers status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:headers {"x-waiter-token" token}
                   :request-method :get})]
            (is (= http-200-ok status))
            (is (= (get headers "etag") (sd/token-data->token-hash (kv/fetch kv-store token))))
            (is (= {"fallback-period-secs" 100
                    "https-redirect" true
                    "owner" "tu1"
                    "stale-timeout-mins" 15}
                   (json/read-str body))))
          (finally
            (kv/delete kv-store token))))

      (testing "post:new-service-description"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description-1))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (= (get headers "etag") (sd/token-data->token-hash (kv/fetch kv-store token))))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-1 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-1 "token") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu1"
                    "previous" {}
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))))

      (testing "test:list-tokens"
        (let [{:keys [body status]}
              (handle-list-tokens-request
                kv-store
                entitlement-manager
                {:authorization/user auth-user
                 :query-string "include=metadata"
                 :request-method :get})
              {:strs [last-update-time] :as token-data} (kv/fetch kv-store token)]
          (is (= http-200-ok status))
          (is (= [{"deleted" false
                   "etag" (sd/token-data->token-hash token-data)
                   "last-update-time" (-> last-update-time tc/from-long du/date-to-str)
                   "maintenance" false
                   "owner" "tu1"
                   "token" token}]
                 (json/read-str body)))))

      (testing "post:new-service-description-different-owner"
        (let [token (str token "-tu")
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (-> service-description-1 (assoc "owner" "tu2" "token" token) utils/clj->json StringBufferInputStream.)
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-1 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-1 "token") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu2"
                    "previous" {}
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))))

      (testing "get:new-service-description:x-waiter-token header"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" token}
                 :request-method :get})
              json-keys ["metadata" "env"]]
          (is (= http-200-ok status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (doseq [key (keys (utils/remove-keys (select-keys service-description-1 sd/service-parameter-keys) json-keys))]
            (is (str/includes? body (str (get service-description-1 key)))))
          (doseq [key json-keys]
            (is (str/includes? body (utils/clj->json (get service-description-1 key)))))))

      (testing "get:new-service-description:token query parameter"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {}
                 :query-params {"token" token}
                 :request-method :get})
              json-keys ["metadata" "env"]]
          (is (= http-200-ok status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (doseq [key (keys (utils/remove-keys (select-keys service-description-1 sd/service-parameter-keys) json-keys))]
            (is (str/includes? body (str (get service-description-1 key)))))
          (doseq [key json-keys]
            (is (str/includes? body (utils/clj->json (get service-description-1 key)))))))

      (testing "post:update-service-description:with-changes"
        (let [existing-service-description (kv/fetch kv-store token :refresh true)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description-2))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully updated " token)))
          (is (= (select-keys service-description-2 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-2 "token") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu1"
                    "previous" existing-service-description
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))
          (is (empty? (sd/fetch-core kv-store service-id-2)))))

      (testing "post:update-service-description:no-changes"
        (let [_ (kv/store kv-store token (-> service-description-1
                                             (dissoc "token")
                                             (assoc "owner" auth-user)))
              existing-service-parameter-template (kv/fetch kv-store token :refresh true)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json (assoc existing-service-parameter-template "token" token)))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "No changes detected for " token)))
          (is (= (-> existing-service-parameter-template
                     (dissoc "owner")
                     (select-keys sd/token-data-keys))
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc existing-service-parameter-template "owner") service-parameter-template))
            (is (= {"owner" auth-user
                    "previous" {}}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))
          (is (empty? (sd/fetch-core kv-store service-id-2)))))

      (testing "post:update-service-description:no-changes-except-owner"
        (let [_ (kv/store kv-store token (-> service-description-1
                                             (assoc "owner" (str auth-user "-a"))
                                             (dissoc "token")))
              existing-service-parameter-template (kv/fetch kv-store token :refresh true)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json (assoc existing-service-parameter-template "owner" auth-user "token" token)))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully updated " token)))
          (is (= (-> (dissoc existing-service-parameter-template "owner")
                     (select-keys sd/token-data-keys))
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc existing-service-parameter-template "owner") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" auth-user
                    "owner" auth-user
                    "previous" existing-service-parameter-template
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))
          (is (empty? (sd/fetch-core kv-store service-id-2)))))

      (testing "post:update-service-description-change-owner"
        (let [existing-service-description (kv/fetch kv-store token :refresh true)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json (assoc service-description-2 "owner" "tu2")))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully updated " token)))
          (is (= (select-keys service-description-2 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-2 "token") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu2"
                    "previous" existing-service-description
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))
          (is (empty? (sd/fetch-core kv-store service-id-2)))))

      (testing "post:update-service-description-do-not-change-owner"
        (let [existing-service-description (kv/fetch kv-store token :refresh true)
              {:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description-2))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (= "application/json" (get headers "content-type")))
          (is (str/includes? body (str "No changes detected for " token)))
          (is (= (select-keys service-description-2 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-2 "token") service-parameter-template))
            (is (= (select-keys existing-service-description sd/token-metadata-keys) token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))
          (is (empty? (sd/fetch-core kv-store service-id-2)))))

      (testing "get:updated-service-description:include-metadata"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" token}
                 :query-params {"include" "metadata"}
                 :request-method :get})]
          (is (= http-200-ok status))
          (is (= "application/json" (get headers "content-type")))
          (is (-> body json/read-str (get "last-update-time") du/str-to-date))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-parameter-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key (disj required-metadata-keys "deleted")]
              (is (contains? body-map key) (str "Missing entry for " key)))
            (doseq [key (conj optional-metadata-keys "deleted")]
              (is (not (contains? body-map key)) (str "Existing entry for " key)))
            (is (not (contains? body-map "deleted"))))))

      (testing "get:updated-service-description:include-foo"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" token}
                 :query-params {"include" "foo"}
                 :request-method :get})]
          (is (= http-200-ok status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-parameter-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key sd/system-metadata-keys]
              (is (not (contains? body-map key)) (str key))))))

      (testing "get:updated-service-description:include-metadata-and-foo"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" token}
                 :query-params {"include" ["foo" "metadata"]}
                 :request-method :get})]
          (is (= http-200-ok status))
          (is (= "application/json" (get headers "content-type")))
          (is (-> body json/read-str (get "last-update-time") du/str-to-date))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-parameter-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key (disj required-metadata-keys "deleted")]
              (is (contains? body-map key) (str "Missing entry for " key)))
            (doseq [key (conj optional-metadata-keys "deleted")]
              (is (not (contains? body-map key)) (str "Existing entry for " key)))
            (is (not (contains? body-map "deleted"))))))

      (testing "get:updated-service-description:exclude-metadata"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" token}
                 :request-method :get})]
          (is (= http-200-ok status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-parameter-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key sd/system-metadata-keys]
              (is (not (contains? body-map key)) (str key))))))

      (testing "get:invalid-token"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:headers {"x-waiter-token" "###"}
                 :request-method :get})]
          (is (= http-404-not-found status))
          (is (str/includes? body "Couldn't find token ###"))))

      (testing "post:new-service-description:missing-permitted-user"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post:new-service-description:star-run-as-user"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post:new-service-description:allowed-params-vector"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:allowed-params ["VAR_1" "VAR_2" "VAR_3"]
                                     :cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description (dissoc "token") sd/transform-allowed-params-token-entry)
                 (-> body json/read-str (get "service-description") sd/transform-allowed-params-token-entry)))
          (is (= (-> service-description
                     sd/transform-allowed-params-token-entry
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post::new-user-metadata:maintenance-mode"
        (let [token (str token "-maintenance-mode")
              {:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (-> service-description-1
                           (assoc "maintenance" {"message" "custom maintenance message"}
                                  "token" token)
                           utils/clj->json StringBufferInputStream.)
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (= (get headers "etag") (sd/token-data->token-hash (kv/fetch kv-store token))))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-1 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-1 "token") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "maintenance" {"message" "custom maintenance message"}
                    "owner" "tu1"
                    "previous" {}
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))))

      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            token (str token (rand-int 100000))
            service-description (walk/stringify-keys
                                  {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*"
                                   :fallback-period-secs 120
                                   :token token})]
        (testing "post:new-user-metadata:fallback-period-secs"
          (let [{:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                  {:authorization/user auth-user
                   :body (StringBufferInputStream. (utils/clj->json service-description))
                   :headers {}
                   :request-method :post})]
            (is (= http-200-ok status))
            (is (str/includes? body (str "Successfully created " token)))
            (is (= (-> service-description (select-keys sd/service-parameter-keys) sd/transform-allowed-params-token-entry)
                   (-> body json/read-str (get "service-description") sd/transform-allowed-params-token-entry)))
            (is (= (-> service-description
                       sd/transform-allowed-params-token-entry
                       (dissoc "token")
                       (assoc "cluster" (str token-root "-cluster")
                              "last-update-time" (clock-millis)
                              "last-update-user" auth-user
                              "owner" "tu1"
                              "root" token-root))
                   (kv/fetch kv-store token)))))

        (testing "get:updated-service-description:fallback-period-secs"
          (let [service-description (assoc service-description "owner" "tu1")
                {:keys [body headers status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                  {:headers {"x-waiter-token" token}
                   :query-params {"include" "foo"}
                   :request-method :get})]
            (is (= http-200-ok status))
            (is (= "application/json" (get headers "content-type")))
            (is (not (str/includes? body "last-update-time")))
            (let [body-map (-> body str json/read-str)]
              (doseq [key sd/service-parameter-keys]
                (is (= (get service-description key) (get body-map key))))
              (doseq [key (disj sd/user-metadata-keys "stale-timeout-mins")]
                (is (= (get service-description key) (get body-map key))))
              (doseq [key sd/system-metadata-keys]
                (is (not (contains? body-map key)) (str key)))))))

      (testing "post:edit-user-metadata:fallback-period-secs"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description-1 (walk/stringify-keys
                                      {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*"
                                       :fallback-period-secs 100
                                       :last-update-time (- (clock-millis) 1000) :owner auth-user
                                       :token token})
              _ (kv/store kv-store token service-description-1)
              service-description-2 (-> service-description-1
                                        (assoc "fallback-period-secs" 120)
                                        (dissoc "last-update-time" "owner"))
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description-2))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully updated " token)))
          (is (= (-> service-description-2 (select-keys sd/service-parameter-keys) sd/transform-allowed-params-token-entry)
                 (-> body json/read-str (get "service-description") sd/transform-allowed-params-token-entry)))
          (is (= (-> service-description-2
                     sd/transform-allowed-params-token-entry
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "previous" service-description-1
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post:edit-user-metadata:fallback-period-secs-no-changes"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description-1 (walk/stringify-keys
                                      {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*"
                                       :fallback-period-secs 120
                                       :last-update-time (- (clock-millis) 1000) :owner auth-user
                                       :token token})
              _ (kv/store kv-store token service-description-1)
              service-description-2 (dissoc service-description-1 "last-update-time" "owner")
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description-2))
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "No changes detected for " token)))
          (is (= (-> service-description-2 (select-keys sd/service-parameter-keys) sd/transform-allowed-params-token-entry)
                 (-> body json/read-str (get "service-description") sd/transform-allowed-params-token-entry)))
          (is (= service-description-1 (kv/fetch kv-store token)))))

      (testing "post:update-service-description:edit-star-run-as-user"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*"
                                     :permitted-user "tu2" :token token})
              existing-service-description (assoc service-description "cpus" 2 "owner" "tu1" "run-as-user" "tu0")
              _ (kv/store kv-store token existing-service-description)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body "Successfully updated test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "previous" existing-service-description
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post:update-service-description:preserve-root"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*"
                                     :permitted-user "tu2" :token token})
              existing-service-description (assoc service-description "cpus" 100 "owner" "tu1" "root" "foo")
              _ (kv/store kv-store token existing-service-description)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body "Successfully updated test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "previous" existing-service-description
                            "root" "foo"))
                 (kv/fetch kv-store token)))))

      (testing "post:new-service-description:token-sync:allowed"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject test-user) (= :admin verb) (= "user2" user))))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "user1" :run-as-user "user1" :version "a1b2c3"
                                     :owner "user2" :root "foo-bar" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user test-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" test-user
                            "owner" "user2"
                            "root" "foo-bar"))
                 (kv/fetch kv-store token)))))

      (testing "post:new-service-description:token-sync:allowed:missing-if-match"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject test-user) (= :admin verb) (= "user2" user))))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "user1" :run-as-user "user1" :version "a1b2c3"
                                     :owner "user2" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user test-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" test-user
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post:service-description:history"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-history"
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :mem 200 :permitted-user test-user :run-as-user test-user :version "v1"
                                     :owner test-user :token token})
              _ (kv/store kv-store token service-description)]
          (dotimes [n 10]
            (let [iteration (inc n)
                  existing-service-description (kv/fetch kv-store token)
                  new-service-description (assoc service-description "cpus" iteration)
                  {:keys [body status]}
                  (run-handle-token-request
                    kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                    {:authorization/user test-user
                     :body (StringBufferInputStream. (utils/clj->json new-service-description))
                     :headers {"x-waiter-token" token}
                     :request-method :post})]
              (is (= http-200-ok status))
              (is (str/includes? body "Successfully updated test-token"))
              (is (= (-> service-description
                         (dissoc "token")
                         (assoc "cluster" (str token-root "-cluster")
                                "cpus" iteration
                                "last-update-time" (clock-millis)
                                "last-update-user" test-user
                                "previous" existing-service-description
                                "root" token-root)
                         (utils/dissoc-in (repeat history-length "previous")))
                     (kv/fetch kv-store token)))))))

      (testing "post:new-service-description:token-limit-not-validated-for-admin-mode"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              _ (dotimes [n limit-per-owner]
                  (let [test-token (str token "-" n)]
                    (is (nil? (kv/fetch kv-store test-token)))
                    (store-service-description-for-token
                      synchronize-fn kv-store history-length limit-per-owner test-token
                      {"cmd" "tc1" "cpus" 1 "mem" 200} {"owner" test-user})
                    (is (kv/fetch kv-store test-token))))
              token "test-token-sync-limit"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject test-user) (= :admin verb) (= test-user user))))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "user1" :run-as-user test-user
                                     :owner test-user :root "foo-bar" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user test-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "cluster" (str token-root "-cluster")
                            "last-update-time" (clock-millis)
                            "last-update-user" test-user
                            "owner" test-user
                            "root" "foo-bar"))
                 (kv/fetch kv-store token)))))

      (testing "post:last-update-time"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user (str "test-user-" (utils/unique-identifier))]
          (testing "not-in-admin-mode"
            (let [entitlement-manager (reify authz/EntitlementManager
                                        (authorized? [_ subject _ {:keys [user]}]
                                          (and (= subject test-user) (= test-user user))))
                  test-token (str "token-" (utils/unique-identifier))
                  service-description (walk/stringify-keys
                                        {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "*" :run-as-user test-user
                                         :last-update-time 123456 :owner test-user :root "foo-bar" :token test-token})
                  {:keys [body status]}
                  (run-handle-token-request
                    kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                    {:authorization/user test-user
                     :body (StringBufferInputStream. (str (utils/clj->json service-description)))
                     :headers {"x-waiter-token" token}
                     :query-params {}
                     :request-method :post})]
              (is (= http-400-bad-request status))
              (is (str/includes? body "Cannot modify last-update-time token metadata"))
              (is (nil? (kv/fetch kv-store test-token)))))

          (testing "admin-mode"
            (let [entitlement-manager (reify authz/EntitlementManager
                                        (authorized? [_ subject verb {:keys [user]}]
                                          (and (= subject test-user) (= :admin verb) (= test-user user))))
                  base-service-description (walk/stringify-keys
                                             {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "*" :run-as-user test-user
                                              :owner test-user :root "foo-bar"})]
              (testing "long-value"
                (let [test-token (str "token-" (utils/unique-identifier))
                      service-description (assoc base-service-description
                                            "last-update-time" 123456 "token" test-token)
                      {:keys [body status]}
                      (run-handle-token-request
                        kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                        {:authorization/user test-user
                         :body (StringBufferInputStream. (str (utils/clj->json service-description)))
                         :headers {"x-waiter-token" token}
                         :query-params {"update-mode" "admin"}
                         :request-method :post})]
                  (is (= http-200-ok status))
                  (is (str/includes? body "Successfully created token"))
                  (is (= (-> service-description
                           (dissoc "token")
                           (assoc "cluster" (str token-root "-cluster")
                                  "last-update-time" 123456
                                  "last-update-user" test-user
                                  "owner" test-user
                                  "root" "foo-bar"))
                         (kv/fetch kv-store test-token)))))

              (testing "valid-string-value"
                (let [test-token (str "token-" (utils/unique-identifier))
                      current-time (t/now)
                      service-description (assoc base-service-description
                                            "last-update-time" (du/date-to-str current-time) "token" test-token)
                      {:keys [body status]}
                      (run-handle-token-request
                        kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                        {:authorization/user test-user
                         :body (StringBufferInputStream. (str (utils/clj->json service-description)))
                         :headers {"x-waiter-token" token}
                         :query-params {"update-mode" "admin"}
                         :request-method :post})]
                  (is (= http-200-ok status))
                  (is (str/includes? body "Successfully created token"))
                  (is (= (-> service-description
                           (dissoc "token")
                           (assoc "cluster" (str token-root "-cluster")
                                  "last-update-time" (tc/to-long current-time)
                                  "last-update-user" test-user
                                  "owner" test-user
                                  "root" "foo-bar"))
                         (kv/fetch kv-store test-token)))))

              (testing "invalid-string-value"
                (let [test-token (str "token-" (utils/unique-identifier))
                      service-description (assoc base-service-description
                                            "last-update-time" "foo-bar" "token" test-token)
                      {:keys [body status]}
                      (run-handle-token-request
                        kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                        {:authorization/user test-user
                         :body (StringBufferInputStream. (str (utils/clj->json service-description)))
                         :headers {"x-waiter-token" token}
                         :query-params {"update-mode" "admin"}
                         :request-method :post})]
                  (is (= http-400-bad-request status))
                  (is (str/includes? body "Invalid date format for last-update-time string"))
                  (is (nil? (kv/fetch kv-store test-token)))))))))

      (testing "post:new-service-description:token-query-param"
        (let [test-token (str "token-" (rand-int 10000))
              {:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (-> service-description-1 (dissoc "token") utils/clj->json StringBufferInputStream.)
                 :headers {}
                 :query-params {"token" test-token}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (= (get headers "etag") (sd/token-data->token-hash (kv/fetch kv-store test-token))))
          (is (str/includes? body (str "Successfully created " test-token)))
          (is (= (select-keys service-description-1 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store test-token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store test-token)]
            (is (= (dissoc service-description-1 "token") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu1"
                    "previous" {}
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))))

      (testing "post:new-service-description-cors-rules"
        (let [token (str token "-cors-rules")
              cors-rules [{"origin-regex" "test\\.com"
                           "methods" ["GET" "POST"]}]
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (-> service-description-1 (assoc "cors-rules" cors-rules "token" token) utils/clj->json StringBufferInputStream.)
                 :headers {}
                 :request-method :post})]
          (is (= http-200-ok status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-1 sd/token-data-keys)
                 (sd/token->service-parameter-template kv-store token)))
          (let [{:keys [service-parameter-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-1 "token") service-parameter-template))
            (is (= {"cluster" (str token-root "-cluster")
                    "last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "cors-rules" cors-rules
                    "owner" "tu1"
                    "previous" {}
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1))))))))

(defmacro assert-bad-user-metadata-response
  "Asserts the response is due to invalid user-metadata for a specific key path"
  [response invalid-keys]
  `(let [response# ~response
         invalid-keys# ~invalid-keys
         status# (:status response#)
         body# (:body response#)
         parsed-body# (json/read-str body#)
         details# (get-in parsed-body# ["waiter-error" "details"])
         message# (get-in parsed-body# ["waiter-error" "message"])]
     (is (= http-400-bad-request status#))
     (is (not (str/includes? body# "clojure")))
     (is (every? (partial str/includes? details#) invalid-keys#))
     (is (str/includes? message# "Validation failed for user metadata on token") body#)))

(deftest test-post-failure-in-handle-token-request
  (with-redefs [sd/service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd sd/service-parameter-keys))))]
    (let [entitlement-manager (authz/->SimpleEntitlementManager nil)
          make-peer-requests-fn (fn [endpoint & _]
                                  (and (str/starts-with? endpoint "token/")
                                       (str/ends-with? endpoint "/refresh")) {})
          profile->defaults {"test-profile" {"cpus" 1 "mem" 1024}}
          validate-service-description-fn (fn validate-service-description-fn [service-description]
                                            (sd/validate-schema service-description {s/Str s/Any} profile->defaults nil))
          attach-service-defaults-fn (fn [{:strs [profile] :as template}]
                                       (sd/compute-service-defaults template profile->defaults profile))
          token "test-token"
          token-root "test-token-root"
          waiter-hostname "waiter-hostname.app.example.com"
          waiter-hostnames [waiter-hostname]
          auth-user "tu1"]
      (testing "post:new-service-description:missing-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {}
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Must provide the token"))))

      (testing "post:new-service-description:reserved-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token waiter-hostname})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {}
                 :request-method :post})]
          (is (= http-403-forbidden status))
          (is (str/includes? body "Token name is reserved"))))

      (testing "post:new-service-description:no-parameters"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys {:token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body (str "No parameters provided for abcdefgh")))))

      (testing "post:new-service-description:schema-fail"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus "not-an-int" :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "issue"))))

      (testing "post:new-service-description:edit-unauthorized-run-as-user"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu0"
                                     :permitted-user "tu2" :token token})
              _ (kv/store kv-store token (assoc service-description "run-as-user" "tu0" "owner" "tu1" "cpus" 2))
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= http-403-forbidden status))
          (is (str/includes? body "Cannot run as user"))))

      (testing "post:new-service-description:edit-unauthorized-owner"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token token})
              _ (kv/store kv-store token (assoc service-description "run-as-user" "tu0" "owner" "tu0" "cpus" 2))
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= http-403-forbidden status))
          (is (str/includes? body "Cannot change owner of token"))))

      (testing "post:new-service-description:create-unauthorized-owner"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token token :owner "tu0"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= http-403-forbidden status))
          (is (str/includes? body "Cannot create token as user"))))

      (testing "post:new-service-description:token-sync:invalid-admin-mode"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject user) (= :admin verb))))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "user1" :run-as-user "user1" :version "a1b2c3"
                                     :owner "user2" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:request-method :post :authorization/user test-user :headers {"x-waiter-token" token}
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :query-params {"update-mode" "foobar"}})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Invalid update-mode"))
          (is (nil? (kv/fetch kv-store token)))))

      (testing "post:edit-service-description:token-sync:not-allowed:missing-if-match"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject user) (= :admin verb))))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "user1" :run-as-user "user1" :version "a1b2c3"
                                     :last-update-time (- (clock-millis) 10000) :owner "user2" :token token})
              service-description' (assoc service-description "cpus" 4 "mem" 1024)
              _ (kv/store kv-store token (dissoc service-description :token))
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user test-user
                 :body (StringBufferInputStream. (utils/clj->json service-description'))
                 :headers {"x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Must specify if-match header for admin mode token updates"))
          (is (= service-description (kv/fetch kv-store token)))))

      (testing "post:new-service-description:token-sync:not-allowed"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject user) (= :admin verb))))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "user1" :run-as-user "user1" :version "a1b2c3"
                                     :owner "user2" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user test-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"if-match" (str (clock-millis))
                           "x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= http-403-forbidden status))
          (is (str/includes? body "Cannot administer token"))
          (is (nil? (kv/fetch kv-store token)))))

      (testing "post:new-service-description:token-sync:invalid-previous"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ _ _ _] true))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :permitted-user "user1" :run-as-user "user1" :version "a1b2c3"
                                     :owner "user2" :previous "previous" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user test-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"if-match" (str (clock-millis))
                           "x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Token previous must be a map"))
          (is (nil? (kv/fetch kv-store token)))))

      (testing "post:new-service-description:invalid-instance-counts"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token token
                                     :min-instances 2 :max-instances 1})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "min-instances (2) must be less than or equal to max-instances (1)"))))

      (testing "post:new-service-description:invalid-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "###"
                                     :min-instances 2 :max-instances 10})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Token must match pattern"))))

      (testing "post:new-service-description:invalid-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "abcdefgh"
                                     :min-instances 2 :max-instances 10 :invalid-key "invalid-value"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Unsupported key(s) in token"))))

      (testing "post:new-service-description:cannot-modify-last-update-time"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "abcdefgh"
                                     :min-instances 2 :max-instances 10 :last-update-time (clock-millis)})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Cannot modify last-update-time token metadata"))))

      (testing "post:new-service-description:cannot-modify-root"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "abcdefgh"
                                     :min-instances 2 :max-instances 10 :root "foo-bar"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :request-method :post})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Cannot modify root token metadata"))))

      (testing "post:new-service-description:cannot-modify-previous"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "abcdefgh"
                                     :min-instances 2 :max-instances 10
                                     :previous {"cmd" "tc0"}})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:request-method :post :authorization/user "tu1"
                 :body (StringBufferInputStream. (utils/clj->json service-description))})]
          (is (= http-400-bad-request status))
          (is (str/includes? body "Cannot modify previous token metadata"))))

      (testing "post:new-service-description:bad-token-command"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? message "cmd must be a non-empty string") body)))

      (testing "post:new-service-description:bad-token-metadata"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :token "abcdefgh" :metadata {"a" 12}})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? message "did not have string values: a") body)
          (is (str/includes? message "Metadata values must be strings"))))

      (testing "post:new-service-description:bad-token-environment-vars"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :token "abcdefgh" :env {"HOME" "12"}})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? message "environment variable keys are reserved: HOME") body)))

      (testing "post:new-service-description:invalid-authentication"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication ""
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (str/includes? message "authentication must be 'disabled', 'standard', or the specific authentication scheme if supported by the configured authenticator") body)))

      (testing "post:new-service-description:valid-authentication"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication "unsupported"
                                     :token "abcdefgh"})
              {:keys [status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})]
          (is (= http-200-ok status))))

      (testing "post:new-service-description:valid-authentication-2"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication "jwt"
                                     :token "abcdefgh"})
              {:keys [status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})]
          (is (= http-200-ok status))))

      (testing "post:new-service-description:missing-permitted-user-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication "disabled"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (str/includes? message "Tokens with authentication disabled must specify permitted-user as *, instead provided") body)))

      (testing "post:new-service-description:non-star-permitted-user-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication "disabled" :permitted-user "pu1"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (str/includes? message "Tokens with authentication disabled must specify permitted-user as *, instead provided") body)))

      (testing "post:new-service-description:partial-description-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :authentication "disabled" :permitted-user "*"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (str/includes? message "Tokens with authentication disabled must specify all required parameters") body)))

      (testing "post:new-service-description:partial-description-with-authentication-disabled-with-profile"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:authentication "disabled" :cmd "tc1" :permitted-user "*" :profile "test-profile"
                                     :run-as-user "tu1" :version "version"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})]
          (is (= http-200-ok status))))

      (testing "post:new-service-description:partial-description-with-interstitial"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :interstitial-secs 10 :permitted-user "*"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (str/includes? message "Tokens with missing required parameters cannot use interstitial support") body)
          (is (= #{"run-as-user"} (set (get details "missing-parameters"))) body)))

      (testing "post:new-service-description:partial-description-with-interstitial-with-profile"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:interstitial-secs 10 :permitted-user "*" :profile "test-profile"
                                     :run-as-user "tu1" :version "version"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (str/includes? message "Tokens with missing required parameters cannot use interstitial support") body)
          (is (= #{"cmd"} (set (get details "missing-parameters"))) body))

        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :interstitial-secs 10 :permitted-user "*" :profile "test-profile"
                                     :run-as-user "tu1" :version "version"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})]
          (is (= http-200-ok status) (str body))))

      (let [run-allowed-params-check
            (fn [allowed-params-value error-messages]
              (let [kv-store (kv/->LocalKeyValueStore (atom {}))
                    service-description (walk/stringify-keys
                                          {:allowed-params allowed-params-value
                                           :cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :permitted-user "*" :token "abcdefgh"})
                    {:keys [body status]}
                    (run-handle-token-request
                      kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                      {:authorization/user auth-user
                       :body (StringBufferInputStream. (utils/clj->json service-description))
                       :headers {"accept" "application/json"}
                       :request-method :post})
                    {{:strs [message]} "waiter-error"} (json/read-str body)]
                (is (= http-400-bad-request status))
                (doseq [error-message error-messages]
                  (is (str/includes? (str message) error-message) body))))]

        (testing "post:new-service-description:invalid-allowed-params-int"
          (run-allowed-params-check 1 ["Provided allowed-params is not a vector"]))

        (testing "post:new-service-description:empty-allowed-params-string"
          (run-allowed-params-check
            [""]
            ["Individual params may not be empty."]))

        (testing "post:new-service-description:invalid-first-underscore"
          (run-allowed-params-check
            ["_HOME"]
            ["Individual params must be made up of letters, numbers, and underscores and must start with a letter."]))

        (testing "post:new-service-description:invalid-first-digit"
          (run-allowed-params-check
            ["1HOME"]
            ["Individual params must be made up of letters, numbers, and underscores and must start with a letter."]))

        (testing "post:new-service-description:invalid-use-of-hyphen"
          (run-allowed-params-check
            ["MY-ENV"]
            ["Individual params must be made up of letters, numbers, and underscores and must start with a letter."]))

        (testing "post:new-service-description:reserved-MESOS"
          (run-allowed-params-check
            ["MESOS_CPU"]
            ["Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."]))

        (testing "post:new-service-description:reserved-MARATHON"
          (run-allowed-params-check
            ["MARATHON_CPU"]
            ["Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."]))

        (testing "post:new-service-description:reserved-PORT"
          (run-allowed-params-check
            ["PORT1"]
            ["Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."]))

        (testing "post:new-service-description:reserved-WAITER"
          (run-allowed-params-check
            ["WAITER_CPU"]
            ["Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."]))

        (testing "post:new-service-description:reserved-HOME"
          (run-allowed-params-check
            ["HOME"]
            ["Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."]))

        (testing "post:new-service-description:reserved-LOGNAME"
          (run-allowed-params-check
            ["LOGNAME"]
            ["Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."]))

        (testing "post:new-service-description:reserved-USER"
          (run-allowed-params-check
            ["USER"]
            ["Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."]))

        (testing "post:new-service-description:all-errors"
          (run-allowed-params-check
            ["" "HOME" "VAR.1"]
            ["Individual params may not be empty."
             "Individual params must be made up of letters, numbers, and underscores and must start with a letter."
             "Individual params cannot start with MESOS_, MARATHON_, PORT, or WAITER_ and cannot be HOME, USER, LOGNAME."])))

      (testing "post:new-user-metadata:bad-fallback-period-secs"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:fallback-period-secs "bad" :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? (str details) "fallback-period-secs") body)
          (is (str/includes? message "Validation failed for user metadata on token") body)))

      (testing "post:new-user-metadata:fallback-period-secs-limit-exceeded"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:fallback-period-secs (-> 1 t/days t/in-seconds inc)
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? (str details) "fallback-period-secs") body)
          (is (str/includes? message "Validation failed for user metadata on token") body)))

      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            service-description (walk/stringify-keys
                                  {:maintenance {:message "custom maintenance message"}
                                   :token "abcdefgh"})
            make-token-request-with-invalid-user-metadata
            (fn [service-description]
              (let [response
                    (run-handle-token-request
                      kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                      {:authorization/user auth-user
                       :body (StringBufferInputStream. (utils/clj->json service-description))
                       :headers {"accept" "application/json"}
                       :request-method :post})]
                response))]

        (testing "post:new-user-metadata:maintenance-message-max-length-exceeded"
          (let [invalid-keys ["maintenance" "message"]
                service-description (assoc-in service-description invalid-keys (apply str (repeat 513 "a")))
                response (make-token-request-with-invalid-user-metadata service-description) ]
            (assert-bad-user-metadata-response response invalid-keys)))

        (testing "post:new-user-metadata:maintenance-message-is-empty-string"
          (let [invalid-keys ["maintenance" "message"]
                service-description (assoc-in service-description invalid-keys "")
                response (make-token-request-with-invalid-user-metadata service-description)]
            (assert-bad-user-metadata-response response invalid-keys)))

        (testing "post:new-user-metadata:maintenance-message-is-not-a-string"
          (let [invalid-keys ["maintenance" "message"]
                service-description (assoc-in service-description invalid-keys 100)
                response (make-token-request-with-invalid-user-metadata service-description)]
            (assert-bad-user-metadata-response response invalid-keys)))

        (testing "post:new-user-metadata:maintenance-message-is-not-defined"
          (let [invalid-keys ["maintenance" "message"]
                service-description (update service-description "maintenance" dissoc "message")
                response (make-token-request-with-invalid-user-metadata service-description)]
            (assert-bad-user-metadata-response response invalid-keys)))

        (testing "post:new-user-metadata:maintenance-is-not-a-map"
          (let [service-description (assoc service-description "maintenance" "not a map")
                response (make-token-request-with-invalid-user-metadata service-description)]
            (assert-bad-user-metadata-response response ["maintenance"])))

        (testing "post:new-user-metadata:maintenance-is-nil"
          (let [service-description (assoc service-description "maintenance" nil)
                response (make-token-request-with-invalid-user-metadata service-description)]
            (assert-bad-user-metadata-response response ["maintenance"]))))

      (testing "post:new-service-description:token-limit-reached"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-limits"
              service-description-2 {"cpus" 4 "mem" 1024 "token" token}
              _ (dotimes [n limit-per-owner]
                  (store-service-description-for-token
                    synchronize-fn kv-store history-length limit-per-owner (str token "-" n)
                    {"cmd" "tc1" "cpus" 1 "mem" 200} {"owner" test-user}))
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true) attach-service-defaults-fn
                {:authorization/user test-user
                 :body (StringBufferInputStream. (utils/clj->json service-description-2))
                 :request-method :post})]
          (is (= http-403-forbidden status))
          (is (str/includes? body "You have reached the limit of number of tokens allowed"))
          (is (nil? (kv/fetch kv-store token)))))

      (testing "post:same-token-in-query-and-payload"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys {:cpus 1 :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :query-params {"token" "abcdefgh"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? (str details) "json-payload") body)
          (is (str/includes? (str details) "query-parameter") body)
          (is (str/includes? message "The token should be provided only as a query parameter or in the json payload") body)))

      (testing "post:different-token-in-query-and-payload"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys {:cpus 1 :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :query-params {"token" "stuvwxyz"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? (str details) "json-payload") body)
          (is (str/includes? (str details) "query-parameter") body)
          (is (str/includes? message "The token should be provided only as a query parameter or in the json payload") body)))

      (testing "post:new-service-description-cors-rules"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              cors-rules [{"origin-regex" "test\\.co(m"
                           "methods" ["a"]
                           "target-schemes" ["https"]}]
              service-description (walk/stringify-keys
                                    {:cors-rules cors-rules
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? (str details) "cors-rules") body)
          (is (str/includes? (str details) "origin-regex\\\" (throws? (is-a-valid-regular-expression?") body)
          (is (str/includes? (str details) "methods\\\" [(not (is-an-http-method?") body)
          (is (str/includes? (str details) "target-schemes\\\" disallowed-key") body)
          (is (str/includes? message "Validation failed for user metadata on token") body)))

      (testing "post:new-service-description-cors-rules-2"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              cors-rules [{"origin-regex" "test\\.co(m"
                           "methods" []
                           "target-schemes" []}]
              service-description (walk/stringify-keys
                                    {:cors-rules cors-rules
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? (str details) "cors-rules") body)
          (is (str/includes? (str details) "origin-regex\\\" (throws? (is-a-valid-regular-expression?") body)
          (is (str/includes? (str details) "methods\\\" (not (not-empty []") body)
          (is (str/includes? (str details) "target-schemes\\\" disallowed-key") body)
          (is (str/includes? message "Validation failed for user metadata on token") body)))

      (testing "post:new-service-description-cors-rules-2"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              cors-rules {"origin-regex" "test\\.com"
                          "methods" ["GET" "POST"]}
              service-description (walk/stringify-keys
                                    {:cors-rules cors-rules
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn attach-service-defaults-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (utils/clj->json service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= http-400-bad-request status))
          (is (str/includes? (str details) "cors-rules\\\" (not (sequential?") body)
          (is (str/includes? message "Validation failed for user metadata on token") body))))))

(deftest test-store-service-description
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        token "test-token"
        service-description-1 (walk/stringify-keys
                                {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                 :permitted-user "tu2" :name token :min-instances 2 :max-instances 10})
        service-description-2 (assoc service-description-1 "cpus" 200 "version" "foo-bar")
        service-description-3 (assoc service-description-1 "cpus" 100 "version" "fee-fie")
        current-time (clock-millis)
        token-metadata-1 {"last-update-time" current-time
                          "owner" "test-user"}]

    (testing "basic creation"
      (store-service-description-for-token synchronize-fn kv-store history-length limit-per-owner token
                                           service-description-1 token-metadata-1)
      (let [token-description (kv/fetch kv-store token)]
        (is (= service-description-1 (select-keys token-description sd/service-parameter-keys)))
        (is (= token-metadata-1 (select-keys token-description sd/token-metadata-keys)))
        (is (= (merge service-description-1 token-metadata-1) token-description))))

    (let [token-metadata-2 {"last-update-time" (+ current-time 1250)
                            "owner" "test-user"
                            "previous" (kv/fetch kv-store token :refresh true)}]
      (testing "basic update with valid etag"
        (let [token-data (kv/fetch kv-store token)
              token-hash (sd/token-data->token-hash token-data)]
          (store-service-description-for-token synchronize-fn kv-store history-length limit-per-owner token
                                               service-description-2 token-metadata-2
                                               :version-hash token-hash)
          (let [token-description (kv/fetch kv-store token)]
            (is (= service-description-2 (select-keys token-description sd/service-parameter-keys)))
            (is (= token-metadata-2 (select-keys token-description sd/token-metadata-keys)))
            (is (= (merge service-description-2 token-metadata-2) token-description)))))

      (testing "failing update with outdated etag"
        (let [{:strs [last-update-time]} (kv/fetch kv-store token)]
          (is (thrown-with-msg?
                ExceptionInfo #"Cannot modify stale token"
                (store-service-description-for-token synchronize-fn kv-store history-length limit-per-owner token
                                                     service-description-3 token-metadata-1
                                                     :version-hash (- last-update-time 1000))))
          (let [token-description (kv/fetch kv-store token)]
            (is (= service-description-2 (select-keys token-description sd/service-parameter-keys)))
            (is (= token-metadata-2 (select-keys token-description sd/token-metadata-keys)))
            (is (= (merge service-description-2 token-metadata-2) token-description))))))))

(deftest test-store-service-description-limits-per-owner
  (let [token-prefix "test-token"
        service-description (walk/stringify-keys
                              {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                               :permitted-user "tu2" :name token-prefix :min-instances 2 :max-instances 10})
        current-time (clock-millis)]
    (testing "failing due to limit per owner reached on creation"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            test-user "test-user-1234"
            token-metadata {"last-update-time" current-time
                            "owner" test-user}]
        (is (> limit-per-owner 5))
        (dotimes [n limit-per-owner]
          (let [token (str token-prefix "-" n)]
            (store-service-description-for-token
              synchronize-fn kv-store history-length limit-per-owner token
              service-description token-metadata)
            (is (kv/fetch kv-store token))))
        (let [token (str token-prefix "-" limit-per-owner)]
          (is (thrown?
                ExceptionInfo #"You have reached the limit of number of tokens allowed"
                (store-service-description-for-token
                  synchronize-fn kv-store history-length limit-per-owner token service-description token-metadata)))
          (is (nil? (kv/fetch kv-store token))))))

    (testing "token limits ignored when nil passed"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            test-user "test-user-1234"
            token-metadata {"last-update-time" current-time
                            "owner" test-user}]
        (is (> limit-per-owner 5))
        (dotimes [n (* 2 limit-per-owner)]
          (let [token (str token-prefix "-" n)]
            (store-service-description-for-token
              synchronize-fn kv-store history-length nil token service-description token-metadata)
            (is (kv/fetch kv-store token))))))

    (testing "limit per owner not reached on deleted tokens"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            test-user "test-user-1234"
            token-metadata {"last-update-time" current-time
                            "owner" test-user}]
        (is (> limit-per-owner 5))
        (dotimes [n limit-per-owner]
          (let [token (str token-prefix "-" n)]
            (store-service-description-for-token
              synchronize-fn kv-store history-length limit-per-owner token service-description token-metadata)
            (is (kv/fetch kv-store token))))
        (dotimes [n (min 2 limit-per-owner)]
          (let [token (str token-prefix "-" n)]
            (delete-service-description-for-token
              clock synchronize-fn kv-store history-length token test-user test-user)
            (is (get (kv/fetch kv-store token) "deleted"))))
        (let [token (str token-prefix "-" limit-per-owner)]
          (store-service-description-for-token
            synchronize-fn kv-store history-length limit-per-owner token service-description token-metadata)
          (is (kv/fetch kv-store token)))))))

(deftest test-delete-service-description-for-token
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        token "test-token"
        owner "test-user"
        auth-user "auth-user"
        service-description {"cpus" 200 "version" "foo-bar"}
        current-time (clock-millis)
        last-update-time (- current-time 1000)
        token-metadata {"owner" owner "last-update-time" last-update-time}
        token-description (merge service-description token-metadata)]

    (testing "valid soft delete"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user)
      (is (= (assoc token-description
               "deleted" true
               "last-update-time" current-time
               "last-update-user" auth-user
               "previous" token-description)
             (kv/fetch kv-store token)))
      (is (= {:deleted true
              :last-update-time current-time
              :maintenance false
              :etag nil}
             (-> (list-index-entries-for-owner kv-store owner)
                 (get token)))))

    (testing "valid soft delete with up-to-date etag"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (let [token-hash (sd/token-data->token-hash token-description)]
        (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user
                                              :version-hash token-hash))
      (is (= (assoc token-description
               "deleted" true
               "last-update-time" current-time
               "last-update-user" auth-user
               "previous" token-description)
             (kv/fetch kv-store token)))
      (is (= {:deleted true
              :last-update-time current-time
              :maintenance false
              :etag nil}
             (-> (list-index-entries-for-owner kv-store owner)
                 (get token)))))

    (testing "invalid soft delete"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (is (thrown-with-msg?
            ExceptionInfo #"Cannot modify stale token"
            (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user
                                                  :version-hash (- current-time 5000))))
      (is (= token-description (kv/fetch kv-store token))))

    (testing "valid hard delete"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user
                                            :hard-delete true)
      (is (nil? (kv/fetch kv-store token)))
      (is (nil? (-> (list-index-entries-for-owner kv-store owner)
                    (get token)))))

    (testing "valid hard delete with up-to-date etag"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (let [token-hash (sd/token-data->token-hash token-description)]
        (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user
                                              :hard-delete true :version-hash token-hash))
      (is (nil? (kv/fetch kv-store token)))
      (is (nil? (-> (list-index-entries-for-owner kv-store owner)
                    (get token)))))

    (testing "invalid hard delete"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (is (thrown-with-msg?
            ExceptionInfo #"Cannot modify stale token"
            (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user
                                                  :hard-delete true :version-hash (- current-time 5000))))
      (is (= token-description (kv/fetch kv-store token))))))

(deftest test-deleted-token-with-new-owner-index
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        token "test-token"
        service-parameter-template {"cpus" 1}
        owner-1 "foo"
        token-metadata {"owner" owner-1}
        token-data (merge service-parameter-template token-metadata)]
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner token service-parameter-template token-metadata)

    (is (= token-data (kv/fetch kv-store token)))
    (is (= {token {:deleted false :etag (sd/token-data->token-hash token-data) :last-update-time nil :maintenance false}}
           (list-index-entries-for-owner kv-store owner-1)))

    (delete-service-description-for-token
      clock synchronize-fn kv-store history-length token owner-1 owner-1)

    (let [deleted-token-data (assoc token-data
                               "deleted" true
                               "last-update-time" (clock-millis)
                               "last-update-user" owner-1
                               "previous" token-data)]
      (is (= deleted-token-data (kv/fetch kv-store token)))
      (is (= {token {:deleted true :etag (sd/token-data->token-hash deleted-token-data) :last-update-time nil :maintenance false}}
             (list-index-entries-for-owner kv-store owner-1))))

    (let [service-parameter-template {"cpus" 2}
          owner-2 "bar"
          token-metadata {"owner" owner-2}
          token-data (merge service-parameter-template token-metadata)]
      (store-service-description-for-token
        synchronize-fn kv-store history-length limit-per-owner token service-parameter-template token-metadata)

      (is (= token-data (kv/fetch kv-store token)))
      (is (= {token {:deleted false :etag (sd/token-data->token-hash token-data) :last-update-time nil :maintenance false}}
             (list-index-entries-for-owner kv-store owner-2)))
      (is (empty? (list-index-entries-for-owner kv-store owner-1))))))

(deftest test-soft-delete-loses-history
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        token "test-token"
        service-description-1 {"cpus" 200 "version" "foo-bar-1"}
        token-metadata-1 {"last-update-time" (clock-millis) "owner" "test-user-1"}
        token-data-1 (merge service-description-1 token-metadata-1)]

    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner token service-description-1 token-metadata-1)
    (is (= token-data-1
           (kv/fetch kv-store token)))

    (let [service-description-2 {"mem" 300 "version" "foo-bar-2"}
          token-metadata-2 {"last-update-time" (clock-millis) "owner" "test-user-2"}
          token-data-2 (merge service-description-2 token-metadata-2)]
      (store-service-description-for-token
        synchronize-fn kv-store history-length limit-per-owner token service-description-2 token-metadata-2)
      (is (= (assoc token-data-2 "previous" token-data-1)
             (kv/fetch kv-store token)))

      (let [service-description-3 {"cmd" "cmd-400" "version" "foo-bar-3"}
            token-metadata-3 {"last-update-time" (clock-millis) "owner" "test-user-3"}
            token-data-3 (merge service-description-3 token-metadata-3)]
        (store-service-description-for-token
          synchronize-fn kv-store history-length limit-per-owner token service-description-3 token-metadata-3)
        (is (= (->> token-data-1
                    (assoc token-data-2 "previous")
                    (assoc token-data-3 "previous"))
               (kv/fetch kv-store token)))

        (delete-service-description-for-token
          clock synchronize-fn kv-store history-length token "test-user-3" "test-auth-3")

        (is (= (assoc token-data-3
                 "deleted" true
                 "last-update-user" "test-auth-3"
                 "previous" (->> token-data-1
                                 (assoc token-data-2 "previous")
                                 (assoc token-data-3 "previous")))
               (kv/fetch kv-store token)))

        (let [service-description-4 {"cmd" "cmd-400" "version" "foo-bar-4"}
              token-metadata-4 {"last-update-time" (clock-millis) "owner" "test-user-4"}
              token-data-4 (merge service-description-4 token-metadata-4)]
          (store-service-description-for-token
            synchronize-fn kv-store history-length limit-per-owner token service-description-4 token-metadata-4)
          (is (= token-data-4
                 (kv/fetch kv-store token))))))))

(deftest test-token-index
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        tokens {"token1" {"last-update-time" 1000 "owner" "owner1"}
                "token2" {"owner" "owner1"}
                "token3" {"last-update-time" 3000 "owner" "owner2" "maintenance" {"message" "custom maintenance message"}}}
        kv-store (kv/->LocalKeyValueStore (atom {}))
        token-owners-key "^TOKEN_OWNERS"
        token1-etag (sd/token-data->token-hash (get tokens "token1"))
        token1-index-entry (make-index-entry token1-etag false 1000 nil)
        token2-etag (sd/token-data->token-hash (get tokens "token2"))
        token2-index-entry (make-index-entry token2-etag false nil nil)
        token3-etag (sd/token-data->token-hash (get tokens "token3"))
        token3-index-entry (make-index-entry token3-etag false 3000 true)
        bad-token-entry (make-index-entry "E-123456" true 2000 true)]
    (doseq [[token token-data] tokens]
      (kv/store kv-store token token-data))
    (reindex-tokens synchronize-fn kv-store (keys tokens))
    (is (= #{"owner1" "owner2"}
           (-> (kv/fetch kv-store token-owners-key) keys set)))
    (is (= {"token1" token1-index-entry
            "token2" token2-index-entry}
           (list-index-entries-for-owner kv-store "owner1")))
    (is (= {"token3" token3-index-entry}
           (list-index-entries-for-owner kv-store "owner2")))
    ;; corrupt the index
    (let [owner->owner-key (kv/fetch kv-store token-owners-key)
          owner-key (get owner->owner-key "owner2")
          token->index-entry (kv/fetch kv-store owner-key)
          token->index-entry' (assoc token->index-entry "token-bad" bad-token-entry)]
      (kv/store kv-store owner-key token->index-entry'))
    (is (= {"token1" token1-index-entry
            "token2" token2-index-entry}
           (list-index-entries-for-owner kv-store "owner1")))
    (is (= {"token3" token3-index-entry
            "token-bad" bad-token-entry}
           (list-index-entries-for-owner kv-store "owner2")))
    ;; validate re-index cleans the index
    (reindex-tokens synchronize-fn kv-store (keys tokens))
    (is (= #{"owner1" "owner2"}
           (-> (kv/fetch kv-store token-owners-key) keys set)))
    (is (= {"token1" token1-index-entry
            "token2" token2-index-entry}
           (list-index-entries-for-owner kv-store "owner1")))
    (is (= {"token3" token3-index-entry}
           (list-index-entries-for-owner kv-store "owner2")))
    ;; validate re-index drops user
    (reindex-tokens synchronize-fn kv-store (keys (dissoc tokens "token3")))
    (is (= #{"owner1"}
           (-> (kv/fetch kv-store token-owners-key) keys set)))
    (is (= {"token1" token1-index-entry
            "token2" token2-index-entry}
           (list-index-entries-for-owner kv-store "owner1")))
    (is (nil? (list-index-entries-for-owner kv-store "owner2")))))

(deftest test-handle-reindex-tokens-request
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        kv-store (kv/->LocalKeyValueStore (atom {}))
        list-tokens-fn (fn [] ["token1" "token2" "token3"])]
    (let [inter-router-request-fn-called (atom nil)
          inter-router-request-fn (fn [path & {:keys [body]}]
                                    (let [{:strs [index]} (json/read-str body)]
                                      (is (= "tokens/refresh" path))
                                      (is index)
                                      (reset! inter-router-request-fn-called true)))
          {:keys [body status]} (handle-reindex-tokens-request
                                  synchronize-fn inter-router-request-fn kv-store list-tokens-fn
                                  {:request-method :post})
          json-response (json/read-str body)]
      (is (= http-200-ok status))
      (is (= {:message "Successfully re-indexed" :tokens 3} (walk/keywordize-keys json-response)))
      (is @inter-router-request-fn-called))

    (let [inter-router-request-fn-called (atom nil)
          inter-router-request-fn (fn [] (reset! inter-router-request-fn-called true))
          {:keys [body status]} (handle-reindex-tokens-request
                                  synchronize-fn inter-router-request-fn kv-store list-tokens-fn
                                  {:headers {"accept" "application/json"} :request-method :get})
          json-response (json/read-str body)]
      (is (= http-405-method-not-allowed status))
      (is json-response)
      (is (not @inter-router-request-fn-called)))))

(deftest test-handle-list-tokens-request
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        kv-store (kv/->LocalKeyValueStore (atom {}))
        entitlement-manager (reify authz/EntitlementManager
                              (authorized? [_ _ _ _] (throw (UnsupportedOperationException. "unexpected call"))))
        handle-list-tokens-request (wrap-handler-json-response handle-list-tokens-request)
        last-update-time-seed (clock-millis)
        token->token-hash (fn [token] (sd/token-data->token-hash (kv/fetch kv-store token)))]
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token1"
      {"cpus" 1 "idle-timeout-mins" 0 "mem" 1024}
      {"cluster" "c1" "last-update-time" (- last-update-time-seed 1000) "owner" "owner1"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token2"
      {"cpus" 2 "idle-timeout-mins" 9 "image" "test-image" "mem" 2048}
      {"cluster" "c1" "last-update-time" (- last-update-time-seed 2000) "owner" "owner1"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token3"
      {"cpus" 3 "idle-timeout-mins" 5 "mem" 2048}
      {"cluster" "c2" "last-update-time" (- last-update-time-seed 3000) "owner" "owner2"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token4"
      {"cpus" 4 "idle-timeout-mins" 0 "mem" 2048}
      {"cluster" "c2" "deleted" true "last-update-time" (- last-update-time-seed 3000) "owner" "owner2"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token5"
      {"cpus" 4 "mem" 2048 "run-as-user" "*"}
      {"cluster" "c1" "last-update-time" (- last-update-time-seed 3000) "owner" "owner3"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token6"
      {"cpus" 4 "idle-timeout-mins" 0 "mem" 2048 "run-as-user" "*"}
      {"cluster" "c1" "last-update-time" (- last-update-time-seed 3000) "owner" "owner3"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token7"
      {"allowed-params" #{"P1" "P2"} "cpus" 4 "mem" 1024}
      {"cluster" "c1" "last-update-time" (- last-update-time-seed 3000) "owner" "owner3"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token8"
      {"allowed-params" #{"P1" "P2"} "env" {"E1" "v0" "P1" "v1"} "cpus" 4 "mem" 1024}
      {"cluster" "c1" "last-update-time" (- last-update-time-seed 3000) "owner" "owner3"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length limit-per-owner "token9"
      {"allowed-params" #{"P1" "P2"} "env" {"E1" "v0" "P1" "v1" "P2" "v2"} "cpus" 4 "mem" 1024}
      {"cluster" "c1" "last-update-time" (- last-update-time-seed 3000) "maintenance" {"message" "msg1"} "owner" "owner3"})
    (let [request {:query-string "include=metadata" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"deleted" false
                "etag" (token->token-hash "token1")
                "last-update-time" (-> (- last-update-time-seed 1000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner1"
                "token" "token1"}
               {"deleted" false
                "etag" (token->token-hash "token2")
                "last-update-time" (-> (- last-update-time-seed 2000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner1"
                "token" "token2"}
               {"deleted" false
                "etag" (token->token-hash "token3")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner2"
                "token" "token3"}
               {"deleted" false
                "etag" (token->token-hash "token5")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token5"}
               {"deleted" false
                "etag" (token->token-hash "token6")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token6"}
               {"deleted" false
                "etag" (token->token-hash "token7")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token7"}
               {"deleted" false
                "etag" (token->token-hash "token8")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token8"}
               {"deleted" false
                "etag" (token->token-hash "token9")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" true
                "owner" "owner3"
                "token" "token9"}}
             (set (json/read-str body)))))
    (let [request {:query-string "include=metadata&include=deleted" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"deleted" false
                "etag" (token->token-hash "token1")
                "last-update-time" (-> (- last-update-time-seed 1000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner1"
                "token" "token1"}
               {"deleted" false
                "etag" (token->token-hash "token2")
                "last-update-time" (-> (- last-update-time-seed 2000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner1"
                "token" "token2"}
               {"deleted" false
                "etag" (token->token-hash "token3")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner2"
                "token" "token3"}
               {"deleted" true
                "etag" (token->token-hash "token4")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner2"
                "token" "token4"}
               {"deleted" false
                "etag" (token->token-hash "token5")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token5"}
               {"deleted" false
                "etag" (token->token-hash "token6")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token6"}
               {"deleted" false
                "etag" (token->token-hash "token7")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token7"}
               {"deleted" false
                "etag" (token->token-hash "token8")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner3"
                "token" "token8"}
               {"deleted" false
                "etag" (token->token-hash "token9")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" true
                "owner" "owner3"
                "token" "token9"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner1" "token" "token2"}
               {"maintenance" false "owner" "owner2" "token" "token3"}
               {"maintenance" false "owner" "owner3" "token" "token5"}
               {"maintenance" false "owner" "owner3" "token" "token6"}
               {"maintenance" false "owner" "owner3" "token" "token7"}
               {"maintenance" false "owner" "owner3" "token" "token8"}
               {"maintenance" true "owner" "owner3" "token" "token9"}}
             (set (json/read-str body)))))
    (let [entitlement-manager (reify authz/EntitlementManager
                                (authorized? [_ subject action resource]
                                  (is (= :manage action))
                                  (str/starts-with? (:user resource) subject)))]
      (let [request {:query-string "can-manage-as-user=owner" :request-method :get}
            {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
        (is (= http-200-ok status))
        (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
                 {"maintenance" false "owner" "owner1" "token" "token2"}
                 {"maintenance" false "owner" "owner2" "token" "token3"}
                 {"maintenance" false "owner" "owner3" "token" "token5"}
                 {"maintenance" false "owner" "owner3" "token" "token6"}
                 {"maintenance" false "owner" "owner3" "token" "token7"}
                 {"maintenance" false "owner" "owner3" "token" "token8"}
                 {"maintenance" true "owner" "owner3" "token" "token9"}}
               (set (json/read-str body)))))
      (let [request {:query-string "can-manage-as-user=owner1" :request-method :get}
            {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
        (is (= http-200-ok status))
        (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
                 {"maintenance" false "owner" "owner1" "token" "token2"}}
               (set (json/read-str body)))))
      (let [request {:query-string "can-manage-as-user=owner2" :request-method :get}
            {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
        (is (= http-200-ok status))
        (is (= #{{"maintenance" false "owner" "owner2" "token" "token3"}}
               (set (json/read-str body)))))
      (let [request {:query-string "can-manage-as-user=test" :request-method :get}
            {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
        (is (= http-200-ok status))
        (is (= #{} (set (json/read-str body))))))
    (let [request {:query-string "owner=owner1" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner1" "token" "token2"}}
             (set (json/read-str body)))))
    (let [request {:query-string "owner=owner1&include=metadata" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"deleted" false
                "etag" (token->token-hash "token1")
                "last-update-time" (-> (- last-update-time-seed 1000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner1"
                "token" "token1"}
               {"deleted" false
                "etag" (token->token-hash "token2")
                "last-update-time" (-> (- last-update-time-seed 2000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner1"
                "token" "token2"}}
             (set (json/read-str body)))))
    (let [request {:query-string "owner=does-not-exist" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= [] (json/read-str body))))
    (let [request {:headers {"accept" "application/json"}
                   :request-method :post}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)
          json-response (try (json/read-str body)
                             (catch Exception _
                               (is (str "Failed to parse body as JSON:\n" body))))]
      (is (= http-405-method-not-allowed status))
      (is json-response))
    (let [request {:request-method :get :query-string "owner=owner2&include=metadata"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"deleted" false
                "etag" (token->token-hash "token3")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner2"
                "token" "token3"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "owner=owner2&include=metadata&include=deleted"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"deleted" false
                "etag" (token->token-hash "token3")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner2"
                "token" "token3"}
               {"deleted" true
                "etag" (token->token-hash "token4")
                "last-update-time" (-> (- last-update-time-seed 3000) tc/from-long du/date-to-str)
                "maintenance" false
                "owner" "owner2"
                "token" "token4"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "cpus=1"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "mem=2048"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token2"}
               {"maintenance" false "owner" "owner2" "token" "token3"}
               {"maintenance" false "owner" "owner3" "token" "token5"}
               {"maintenance" false "owner" "owner3" "token" "token6"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "cluster=c1&mem=2048"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token2"}
               {"maintenance" false "owner" "owner3" "token" "token5"}
               {"maintenance" false "owner" "owner3" "token" "token6"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "cluster=c2&mem=2048"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner2" "token" "token3"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "include=deleted&mem=2048"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token2"}
               {"maintenance" false "owner" "owner2" "token" "token3"}
               {"maintenance" false "owner" "owner2" "token" "token4"}
               {"maintenance" false "owner" "owner3" "token" "token5"}
               {"maintenance" false "owner" "owner3" "token" "token6"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "idle-timeout-mins=0"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner3" "token" "token6"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "idle-timeout-mins=0&include=deleted"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner2" "token" "token4"}
               {"maintenance" false "owner" "owner3" "token" "token6"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "cluster=c1&idle-timeout-mins=0"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner3" "token" "token6"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "run-as-requester=true"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner3" "token" "token5"}
               {"maintenance" false "owner" "owner3" "token" "token6"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "run-as-requester=false"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner1" "token" "token2"}
               {"maintenance" false "owner" "owner2" "token" "token3"}
               {"maintenance" false "owner" "owner3" "token" "token7"}
               {"maintenance" false "owner" "owner3" "token" "token8"}
               {"maintenance" true "owner" "owner3" "token" "token9"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "requires-parameters=true"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner3" "token" "token7"}
               {"maintenance" false "owner" "owner3" "token" "token8"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "requires-parameters=false"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner1" "token" "token2"}
               {"maintenance" false "owner" "owner2" "token" "token3"}
               {"maintenance" false "owner" "owner3" "token" "token5"}
               {"maintenance" false "owner" "owner3" "token" "token6"}
               {"maintenance" true "owner" "owner3" "token" "token9"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "cluster=c2&idle-timeout-mins=0"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (empty? (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "idle-timeout-mins=5"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner2" "token" "token3"}}
             (set (json/read-str body)))))
    (let [request {:headers {"accept" "application/json"}
                   :request-method :get}
          {:keys [body]} (handle-list-token-owners-request kv-store request)
          owner-map-keys (keys (json/read-str body))]
      (is (some #(= "owner1" %) owner-map-keys) "Should have had a key 'owner1'")
      (is (some #(= "owner2" %) owner-map-keys) "Should have had a key 'owner2'"))
    (let [request {:request-method :get :query-string "maintenance=true"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" true "owner" "owner3" "token" "token9"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "maintenance=false"}
          {:keys [body status]} (handle-list-tokens-request kv-store entitlement-manager request)]
      (is (= http-200-ok status))
      (is (= #{{"maintenance" false "owner" "owner1" "token" "token1"}
               {"maintenance" false "owner" "owner1" "token" "token2"}
               {"maintenance" false "owner" "owner2" "token" "token3"}
               {"maintenance" false "owner" "owner3" "token" "token5"}
               {"maintenance" false "owner" "owner3" "token" "token6"}
               {"maintenance" false "owner" "owner3" "token" "token7"}
               {"maintenance" false "owner" "owner3" "token" "token8"}}
             (set (json/read-str body)))))))
