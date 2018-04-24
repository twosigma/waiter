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
(ns waiter.token-test
  (:require [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [schema.core :as s]
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.test-helpers :refer :all]
            [waiter.token :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils])
  (:import (clojure.lang ExceptionInfo)
           (java.io StringBufferInputStream)
           (org.joda.time DateTime)))

(def ^:const history-length 5)

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

(defn- run-handle-token-request
  [kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn request]
  (handle-token-request clock synchronize-fn kv-store token-root history-length waiter-hostnames entitlement-manager
                        make-peer-requests-fn validate-service-description-fn request))

(deftest test-handle-token-request
  (with-redefs [sd/service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd sd/service-description-keys))))]
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
          service-id-2 (sd/service-description->service-id service-id-prefix service-description-2)
          token-root "test-token-root"
          auth-user "tu1"
          waiter-hostname "waiter-hostname.app.example.com"
          waiter-hostnames [waiter-hostname]
          handle-list-tokens-request (wrap-handler-json-response handle-list-tokens-request)]

      (testing "put:unsupported-request-method"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {}
                 :request-method :put})]
          (is (= 405 status))
          (is (str/includes? body "Invalid request method"))))

      (testing "delete:no-token-in-request"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {}
                 :request-method :delete})]
          (is (= 400 status))
          (is (str/includes? body "Couldn't find token in request"))))

      (testing "delete:token-does-not-exist"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" (str "invalid-" token)}
                 :request-method :delete})]
          (is (= 404 status))
          (is (str/includes? body (str "Token invalid-" token " does not exist")))))

      (testing "delete:token-does-exist-unauthorized"
        (try
          (kv/store kv-store token (assoc service-description-1 "owner" "tu2"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [body status]}
                (run-handle-token-request
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                  {:authorization/user auth-user
                   :headers {"accept" "application/json"
                             "x-waiter-token" token}
                   :request-method :delete})
                {{message "message"
                  {:strs [owner user]} "details"} "waiter-error"} (json/read-str body)]
            (is (= 403 status))
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
                    kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                    {:authorization/user auth-user
                     :headers {"x-waiter-token" token}
                     :request-method :delete})]
              (is (= 404 status))
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
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                  {:authorization/user auth-user
                   :headers {"x-waiter-token" token}
                   :request-method :delete})]
            (is (= 200 status))
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
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                  {:authorization/user auth-user
                   :headers {"x-waiter-token" token}
                   :query-params {"hard-delete" "false"}
                   :request-method :delete})]
            (is (= 200 status))
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
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                  {:authorization/user auth-user
                   :headers {"x-waiter-token" token}
                   :query-params {"hard-delete" "true"}
                   :request-method :delete})]
            (is (= 400 status))
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
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
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
                    kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                    {:authorization/user auth-user
                     :headers {"if-match" token-hash
                               "x-waiter-token" token}
                     :query-params {"hard-delete" "true"}
                     :request-method :delete})]
              (is (= 200 status))
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
                  kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                  {:authorization/user auth-user
                   :headers {"accept" "application/json"
                             "if-match" (str (clock-millis))
                             "x-waiter-token" token}
                   :query-params {"hard-delete" "true"}
                   :request-method :delete})
                {{message "message"
                  {{:strs [owner]} "metadata"} "details"} "waiter-error"} (json/read-str body)]
            (is (= 403 status))
            (is (= "Cannot hard-delete token" message))
            (is (= "tu2" owner) body)
            (is (not-empty (kv/fetch kv-store token)) "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "get:empty-service-description"
        (let [{:keys [status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" token}
                 :request-method :get})]
          (is (= 404 status))))

      (testing "post:new-service-description"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description-1))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (= (get headers "etag") (sd/token-data->token-hash (kv/fetch kv-store token))))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-1 sd/token-data-keys)
                 (sd/token->service-description-template kv-store token)))
          (is (nil? (-> body json/read-str (get "request-parameters"))))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-1 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis)
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
                {:authorization/user auth-user
                 :query-string "include=metadata"
                 :request-method :get})]
          (is (= 200 status))
          (is (= [{"deleted" false
                   "etag" (sd/token-data->token-hash (kv/fetch kv-store token))
                   "owner" "tu1"
                   "token" token}]
                 (json/read-str body)))))

      (testing "post:new-service-description-different-owner"
        (let [token (str token "-tu")
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (-> service-description-1 (assoc "owner" "tu2" "token" token) json/write-str StringBufferInputStream.)
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-1 sd/token-data-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-1 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu2"
                    "previous" {}
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))))

      (testing "get:new-service-description:x-waiter-token header"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" token}
                 :request-method :get})
              json-keys ["metadata" "env"]]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (doseq [key (keys (apply dissoc (select-keys service-description-1 sd/service-description-keys) json-keys))]
            (is (str/includes? body (str (get service-description-1 key)))))
          (doseq [key json-keys]
            (is (str/includes? body (json/write-str (get service-description-1 key)))))))

      (testing "get:new-service-description:token query parameter"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {}
                 :query-params {"token" token}
                 :request-method :get})
              json-keys ["metadata" "env"]]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (is (nil? (-> body json/read-str (get "request-parameters"))))
          (doseq [key (keys (apply dissoc (select-keys service-description-1 sd/service-description-keys) json-keys))]
            (is (str/includes? body (str (get service-description-1 key)))))
          (doseq [key json-keys]
            (is (str/includes? body (json/write-str (get service-description-1 key)))))))

      (testing "post:update-service-description"
        (let [existing-service-description (kv/fetch kv-store token :refresh true)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description-2))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-2 sd/token-data-keys)
                 (sd/token->service-description-template kv-store token)))
          (is (nil? (-> body json/read-str (get "request-parameters"))))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-2 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu1"
                    "previous" existing-service-description
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))
          (is (empty? (sd/fetch-core kv-store service-id-2)))))

      (testing "post:update-service-description-change-owner"
        (let [existing-service-description (kv/fetch kv-store token :refresh true)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str (assoc service-description-2 "owner" "tu2")))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-2 sd/token-data-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-2 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis)
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
                kv-store token-root waiter-hostnames (public-entitlement-manager) make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description-2))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description-2 sd/token-data-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description-2 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis)
                    "last-update-user" "tu1"
                    "owner" "tu2"
                    "previous" existing-service-description
                    "root" token-root}
                   token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id-1)))
          (is (empty? (sd/fetch-core kv-store service-id-2)))))

      (testing "get:updated-service-description:include-metadata"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" token}
                 :query-params {"include" "metadata"}
                 :request-method :get})]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (-> body json/read-str (get "last-update-time") du/str-to-date))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-description-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key (disj sd/token-metadata-keys "deleted")]
              (is (contains? body-map key) (str "Missing entry for " key)))
            (is (not (contains? body-map "deleted"))))))

      (testing "get:updated-service-description:include-foo"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" token}
                 :query-params {"include" "foo"}
                 :request-method :get})]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-description-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key sd/token-metadata-keys]
              (is (not (contains? body-map key)))))))

      (testing "get:updated-service-description:include-metadata-and-foo"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" token}
                 :query-params {"include" ["foo" "metadata"]}
                 :request-method :get})]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (-> body json/read-str (get "last-update-time") du/str-to-date))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-description-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key (disj sd/token-metadata-keys "deleted")]
              (is (contains? body-map key) (str "Missing entry for " key)))
            (is (not (contains? body-map "deleted"))))))

      (testing "get:updated-service-description:exclude-metadata"
        (let [{:keys [body headers status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" token}
                 :request-method :get})]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (not (str/includes? body "last-update-time")))
          (let [body-map (-> body str json/read-str)]
            (doseq [key sd/service-description-keys]
              (is (= (get service-description-2 key) (get body-map key))))
            (doseq [key sd/token-metadata-keys]
              (is (not (contains? body-map key)))))))

      (testing "get:invalid-token"
        (let [{:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:headers {"x-waiter-token" "###"}
                 :request-method :get})]
          (is (= 404 status))
          (is (str/includes? body "Couldn't find token ###"))))

      (testing "post:new-service-description:missing-permitted-user"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "root" token-root))
                 (kv/fetch kv-store token)))
          (is (nil? (-> body json/read-str (get "request-parameters"))))))

      (testing "post:new-service-description:star-run-as-user"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "root" token-root))
                 (kv/fetch kv-store token)))
          (is (nil? (-> body json/read-str (get "request-parameters"))))))

      (testing "post:new-service-description:allowed-params-vector"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:allowed-params ["VAR_1" "VAR_2" "VAR_3"]
                                     :cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description (dissoc "token") sd/transform-allowed-params-token-entry)
                 (-> body json/read-str (get "service-description") sd/transform-allowed-params-token-entry)))
          (is (nil? (-> body json/read-str (get "request-parameters"))))
          (is (= (-> service-description
                     sd/transform-allowed-params-token-entry
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post:new-request-parameter:service-fallback-period-secs"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:service-fallback-period-secs 120
                                     :cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description (select-keys sd/service-description-keys) sd/transform-allowed-params-token-entry)
                 (-> body json/read-str (get "service-description") sd/transform-allowed-params-token-entry)))
          (is (= (-> service-description (select-keys sd/request-parameter-keys))
                 (-> body json/read-str (get "request-parameters"))))
          (is (= (-> service-description
                     sd/transform-allowed-params-token-entry
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
                            "last-update-user" auth-user
                            "owner" "tu1"
                            "root" token-root))
                 (kv/fetch kv-store token)))))

      (testing "post:update-service-description:edit-star-run-as-user"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "*"
                                     :permitted-user "tu2" :token token})
              existing-service-description (assoc service-description "cpus" 2 "owner" "tu1" "run-as-user" "tu0")
              _ (kv/store kv-store token existing-service-description)
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user test-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user test-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= 200 status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis)
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
                    kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                    {:authorization/user test-user
                     :body (StringBufferInputStream. (json/write-str new-service-description))
                     :headers {"x-waiter-token" token}
                     :request-method :post})]
              (is (= 200 status))
              (is (str/includes? body "Successfully created test-token"))
              (is (= (-> service-description
                         (dissoc "token")
                         (assoc "cpus" iteration
                                "last-update-time" (clock-millis)
                                "last-update-user" test-user
                                "previous" existing-service-description
                                "root" token-root)
                         (utils/dissoc-in (repeat history-length "previous")))
                     (kv/fetch kv-store token))))))))))

(deftest test-post-failure-in-handle-token-request
  (with-redefs [sd/service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd sd/service-description-keys))))]
    (let [entitlement-manager (authz/->SimpleEntitlementManager nil)
          make-peer-requests-fn (fn [endpoint & _]
                                  (and (str/starts-with? endpoint "token/")
                                       (str/ends-with? endpoint "/refresh")) {})
          validate-service-description-fn (fn validate-service-description-fn [service-description]
                                            (sd/validate-schema service-description {s/Str s/Any} nil))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {}
                 :request-method :post})]
          (is (= 400 status))
          (is (str/includes? body "Must provide the token"))))

      (testing "post:new-service-description:reserved-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token waiter-hostname})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {}
                 :request-method :post})]
          (is (= 403 status))
          (is (str/includes? body "Token name is reserved"))))

      (testing "post:new-service-description:schema-fail"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus "not-an-int" :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token token})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= 400 status))
          (is (str/includes? body "issue"))))

      (testing "post:new-service-description:edit-unauthorized-run-as-user"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu0"
                                     :permitted-user "tu2" :token token})
              _ (kv/store kv-store token (assoc service-description "run-as-user" "tu0" "owner" "tu1" "cpus" 2))
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= 403 status))
          (is (str/includes? body "Cannot run as user"))))

      (testing "post:new-service-description:edit-unauthorized-owner"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token token})
              _ (kv/store kv-store token (assoc service-description "run-as-user" "tu0" "owner" "tu0" "cpus" 2))
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= 403 status))
          (is (str/includes? body "Cannot change owner of token"))))

      (testing "post:new-service-description:create-unauthorized-owner"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token token :owner "tu0"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= 403 status))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post :authorization/user test-user :headers {"x-waiter-token" token}
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :query-params {"update-mode" "foobar"}})]
          (is (= 400 status))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user test-user
                 :body (StringBufferInputStream. (json/write-str service-description'))
                 :headers {"x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= 400 status))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user test-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"if-match" (str (clock-millis))
                           "x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= 403 status))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user test-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"if-match" (str (clock-millis))
                           "x-waiter-token" token}
                 :query-params {"update-mode" "admin"}
                 :request-method :post})]
          (is (= 400 status))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"x-waiter-token" token}
                 :request-method :post})]
          (is (= 400 status))
          (is (str/includes? body "Minimum instances (2) must be <= Maximum instances (1)"))))

      (testing "post:new-service-description:invalid-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "###"
                                     :min-instances 2 :max-instances 10})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn nil
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :request-method :post})]
          (is (= 400 status))
          (is (str/includes? body "Token must match pattern"))))

      (testing "post:new-service-description:invalid-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "abcdefgh"
                                     :min-instances 2 :max-instances 10 :invalid-key "invalid-value"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :request-method :post})]
          (is (= 400 status))
          (is (str/includes? body "Unsupported key(s) in token"))))

      (testing "post:new-service-description:cannot-modify-last-update-time"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "abcdefgh"
                                     :min-instances 2 :max-instances 10 :last-update-time (clock-millis)})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :request-method :post})]
          (is (= 400 status))
          (is (str/includes? body "Cannot modify last-update-time token metadata"))))

      (testing "post:new-service-description:cannot-modify-root"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :permitted-user "tu2" :token "abcdefgh"
                                     :min-instances 2 :max-instances 10 :root "foo-bar"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :request-method :post})]
          (is (= 400 status))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post :authorization/user "tu1"
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 400 status))
          (is (str/includes? body "Cannot modify previous token metadata"))))

      (testing "post:new-service-description:bad-token-metadata"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1"
                                     :token "abcdefgh" :metadata {"a" 12}})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
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
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? message "environment variable keys are reserved: HOME") body)))

      (testing "post:new-service-description:invalid-authentication"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication "unsupported"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "invalid-authentication") body)))

      (testing "post:new-service-description:missing-permitted-user-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication "disabled"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "Tokens with authentication disabled must specify permitted-user as *, instead provided") body)))

      (testing "post:new-service-description:non-star-permitted-user-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :run-as-user "tu1" :authentication "disabled" :permitted-user "pu1"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "Tokens with authentication disabled must specify permitted-user as *, instead provided") body)))

      (testing "post:new-service-description:partial-description-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :authentication "disabled" :permitted-user "*"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "Tokens with authentication disabled must specify all required parameters") body)))

      (testing "post:new-service-description:partial-description-with-interstitial"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :interstitial-secs 10 :permitted-user "*"
                                     :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "Tokens with missing required parameters cannot use interstitial support") body)))

      (let [run-allowed-params-check
            (fn [allowed-params-value error-messages]
              (let [kv-store (kv/->LocalKeyValueStore (atom {}))
                    service-description (walk/stringify-keys
                                          {:allowed-params allowed-params-value
                                           :cmd "tc1" :cpus 1 :mem 200 :version "a1b2c3" :permitted-user "*" :token "abcdefgh"})
                    {:keys [body status]}
                    (run-handle-token-request
                      kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                      {:authorization/user auth-user
                       :body (StringBufferInputStream. (json/write-str service-description))
                       :headers {"accept" "application/json"}
                       :request-method :post})
                    {{:strs [message]} "waiter-error"} (json/read-str body)]
                (is (= 400 status))
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

      (testing "post:new-request-parameter:bad-service-fallback-period-secs"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (walk/stringify-keys
                                    {:service-fallback-period-secs "bad" :token "abcdefgh"})
              {:keys [body status]}
              (run-handle-token-request
                kv-store token-root waiter-hostnames entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:authorization/user auth-user
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :headers {"accept" "application/json"}
                 :request-method :post})
              {{:strs [details message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? (str details) "service-fallback-period-secs") body)
          (is (str/includes? message "Request parameter validation failed") body))))))

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
      (store-service-description-for-token synchronize-fn kv-store history-length token {} service-description-1 token-metadata-1)
      (let [token-description (kv/fetch kv-store token)]
        (is (= service-description-1 (select-keys token-description sd/service-description-keys)))
        (is (= token-metadata-1 (select-keys token-description sd/token-metadata-keys)))
        (is (= (merge service-description-1 token-metadata-1) token-description))))

    (let [token-metadata-2 {"last-update-time" (+ current-time 1250)
                            "owner" "test-user"
                            "previous" (kv/fetch kv-store token :refresh true)}]
      (testing "basic update with valid etag"
        (let [token-data (kv/fetch kv-store token)
              token-hash (sd/token-data->token-hash token-data)]
          (store-service-description-for-token synchronize-fn kv-store history-length token {} service-description-2 token-metadata-2
                                               :version-hash token-hash)
          (let [token-description (kv/fetch kv-store token)]
            (is (= service-description-2 (select-keys token-description sd/service-description-keys)))
            (is (= token-metadata-2 (select-keys token-description sd/token-metadata-keys)))
            (is (= (merge service-description-2 token-metadata-2) token-description)))))

      (testing "failing update with outdated etag"
        (let [{:strs [last-update-time]} (kv/fetch kv-store token)]
          (is (thrown-with-msg?
                ExceptionInfo #"Cannot modify stale token"
                (store-service-description-for-token synchronize-fn kv-store history-length token {} service-description-3 token-metadata-1
                                                     :version-hash (- last-update-time 1000))))
          (let [token-description (kv/fetch kv-store token)]
            (is (= service-description-2 (select-keys token-description sd/service-description-keys)))
            (is (= token-metadata-2 (select-keys token-description sd/token-metadata-keys)))
            (is (= (merge service-description-2 token-metadata-2) token-description))))))))

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
             (kv/fetch kv-store token))))

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
             (kv/fetch kv-store token))))

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
      (is (nil? (kv/fetch kv-store token))))

    (testing "valid hard delete with up-to-date etag"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (let [token-hash (sd/token-data->token-hash token-description)]
        (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user
                                              :hard-delete true :version-hash token-hash))
      (is (nil? (kv/fetch kv-store token))))

    (testing "invalid hard delete"
      (kv/store kv-store token token-description)
      (is (= token-description (kv/fetch kv-store token)))
      (is (thrown-with-msg?
            ExceptionInfo #"Cannot modify stale token"
            (delete-service-description-for-token clock synchronize-fn kv-store history-length token owner auth-user
                                                  :hard-delete true :version-hash (- current-time 5000))))
      (is (= token-description (kv/fetch kv-store token))))))

(deftest test-soft-delete-loses-history
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        token "test-token"
        service-description-1 {"cpus" 200 "version" "foo-bar-1"}
        token-metadata-1 {"last-update-time" (clock-millis) "owner" "test-user-1"}
        token-data-1 (merge service-description-1 token-metadata-1)]

    (store-service-description-for-token
      synchronize-fn kv-store history-length token {} service-description-1 token-metadata-1)
    (is (= token-data-1
           (kv/fetch kv-store token)))

    (let [service-description-2 {"mem" 300 "version" "foo-bar-2"}
          token-metadata-2 {"last-update-time" (clock-millis) "owner" "test-user-2"}
          token-data-2 (merge service-description-2 token-metadata-2)]
      (store-service-description-for-token
        synchronize-fn kv-store history-length token {} service-description-2 token-metadata-2)
      (is (= (assoc token-data-2 "previous" token-data-1)
             (kv/fetch kv-store token)))

      (let [service-description-3 {"cmd" "cmd-400" "version" "foo-bar-3"}
            token-metadata-3 {"last-update-time" (clock-millis) "owner" "test-user-3"}
            token-data-3 (merge service-description-3 token-metadata-3)]
        (store-service-description-for-token
          synchronize-fn kv-store history-length token {} service-description-3 token-metadata-3)
        (is (= (->> token-data-1
                    (assoc token-data-2 "previous")
                    (assoc token-data-3 "previous"))
               (kv/fetch kv-store token)))

        (delete-service-description-for-token
          clock synchronize-fn kv-store history-length token "test-user-3" "test-auth-3")

        (is (= (-> token-data-3
                   (assoc "deleted" true
                          "last-update-user" "test-auth-3"
                          "previous" (->> token-data-1
                                          (assoc token-data-2 "previous")
                                          (assoc token-data-3 "previous"))))
               (kv/fetch kv-store token)))

        (let [service-description-4 {"cmd" "cmd-400" "version" "foo-bar-4"}
              token-metadata-4 {"last-update-time" (clock-millis) "owner" "test-user-4"}
              token-data-4 (merge service-description-4 token-metadata-4)]
          (store-service-description-for-token
            synchronize-fn kv-store history-length token {} service-description-4 token-metadata-4)
          (is (= token-data-4
                 (kv/fetch kv-store token))))))))

(deftest test-token-index
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        tokens {"token1" {"last-update-time" 1000 "owner" "owner1"}
                "token2" {"owner" "owner1"}
                "token3" {"last-update-time" 3000 "owner" "owner2"}}
        kv-store (kv/->LocalKeyValueStore (atom {}))]
    (doseq [[token token-data] tokens]
      (kv/store kv-store token token-data))
    (reindex-tokens synchronize-fn kv-store (keys tokens))
    (is (= {"token1" {:deleted false :etag (-> (get tokens "token1") sd/token-data->token-hash)}
            "token2" {:deleted false :etag (-> (get tokens "token2") sd/token-data->token-hash)}}
           (list-index-entries-for-owner kv-store "owner1")))
    (is (= {"token3" {:deleted false :etag (-> (get tokens "token3") sd/token-data->token-hash)}}
           (list-index-entries-for-owner kv-store "owner2")))))

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
      (is (= 200 status))
      (is (= {:message "Successfully re-indexed" :tokens 3} (-> json-response walk/keywordize-keys)))
      (is @inter-router-request-fn-called))

    (let [inter-router-request-fn-called (atom nil)
          inter-router-request-fn (fn [] (reset! inter-router-request-fn-called true))
          {:keys [body status]} (handle-reindex-tokens-request
                                  synchronize-fn inter-router-request-fn kv-store list-tokens-fn
                                  {:headers {"accept" "application/json"} :request-method :get})
          json-response (json/read-str body)]
      (is (= 405 status))
      (is json-response)
      (is (not @inter-router-request-fn-called)))))

(deftest test-handle-list-tokens-request
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        kv-store (kv/->LocalKeyValueStore (atom {}))
        handle-list-tokens-request (wrap-handler-json-response handle-list-tokens-request)
        last-update-time-seed (clock-millis)
        token->token-hash (fn [token] (-> (kv/fetch kv-store token) sd/token-data->token-hash))]
    (store-service-description-for-token
      synchronize-fn kv-store history-length "token1"
      {} {"cpus" 1} {"last-update-time" (- last-update-time-seed 1000) "owner" "owner1"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length "token2"
      {} {"cpus" 2} {"last-update-time" (- last-update-time-seed 2000) "owner" "owner1"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length "token3"
      {} {"cpus" 3} {"last-update-time" (- last-update-time-seed 3000) "owner" "owner2"})
    (store-service-description-for-token
      synchronize-fn kv-store history-length "token4"
      {} {"cpus" 4} {"deleted" true "last-update-time" (- last-update-time-seed 3000) "owner" "owner2"})
    (let [request {:query-string "include=metadata" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store request)]
      (is (= 200 status))
      (is (= #{{"deleted" false "etag" (token->token-hash "token1") "owner" "owner1" "token" "token1"}
               {"deleted" false "etag" (token->token-hash "token2") "owner" "owner1" "token" "token2"}
               {"deleted" false "etag" (token->token-hash "token3") "owner" "owner2" "token" "token3"}}
             (set (json/read-str body)))))
    (let [request {:query-string "include=metadata&include=deleted" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store request)]
      (is (= 200 status))
      (is (= #{{"deleted" false "etag" (token->token-hash "token1") "owner" "owner1" "token" "token1"}
               {"deleted" false "etag" (token->token-hash "token2") "owner" "owner1" "token" "token2"}
               {"deleted" false "etag" (token->token-hash "token3") "owner" "owner2" "token" "token3"}
               {"deleted" true "etag" (token->token-hash "token4") "owner" "owner2" "token" "token4"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store request)]
      (is (= 200 status))
      (is (= #{{"owner" "owner1" "token" "token1"}
               {"owner" "owner1" "token" "token2"}
               {"owner" "owner2" "token" "token3"}}
             (set (json/read-str body)))))
    (let [request {:query-string "owner=owner1" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store request)]
      (is (= 200 status))
      (is (= #{{"owner" "owner1" "token" "token1"}
               {"owner" "owner1" "token" "token2"}}
             (set (json/read-str body)))))
    (let [request {:query-string "owner=owner1&include=metadata" :request-method :get}
          {:keys [body status]} (handle-list-tokens-request kv-store request)]
      (is (= 200 status))
      (is (= #{{"deleted" false "etag" (token->token-hash "token1") "owner" "owner1" "token" "token1"}
               {"deleted" false "etag" (token->token-hash "token2") "owner" "owner1" "token" "token2"}}
             (set (json/read-str body)))))
    (let [{:keys [body status]} (handle-list-tokens-request kv-store {:headers {"accept" "application/json"}
                                                                      :request-method :post})
          json-response (try (json/read-str body)
                             (catch Exception _
                               (is (str "Failed to parse body as JSON:\n" body))))]
      (is (= 405 status))
      (is json-response))
    (let [request {:request-method :get :query-string "owner=owner2&include=metadata"}
          {:keys [body status]} (handle-list-tokens-request kv-store request)]
      (is (= 200 status))
      (is (= #{{"deleted" false "etag" (token->token-hash "token3") "owner" "owner2" "token" "token3"}}
             (set (json/read-str body)))))
    (let [request {:request-method :get :query-string "owner=owner2&include=metadata&include=deleted"}
          {:keys [body status]} (handle-list-tokens-request kv-store request)]
      (is (= 200 status))
      (is (= #{{"deleted" false "etag" (token->token-hash "token3") "owner" "owner2" "token" "token3"}
               {"deleted" true "etag" (token->token-hash "token4") "owner" "owner2" "token" "token4"}}
             (set (json/read-str body)))))
    (let [{:keys [body]} (handle-list-token-owners-request kv-store {:headers {"accept" "application/json"}
                                                                     :request-method :get})
          owner-map-keys (keys (json/read-str body))]
      (is (some #(= "owner1" %) owner-map-keys) "Should have had a key 'owner1'")
      (is (some #(= "owner2" %) owner-map-keys) "Should have had a key 'owner2'"))))
