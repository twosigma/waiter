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
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.test-helpers :refer :all]
            [waiter.token :refer :all])
  (:import (java.io StringBufferInputStream)
           (org.joda.time DateTime)))

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
  [kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn request]
  (handle-token-request clock synchronize-fn kv-store waiter-hostname entitlement-manager make-peer-requests-fn
                        validate-service-description-fn request))

(deftest test-handle-token-request
  (with-redefs [sd/service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd sd/service-description-keys))))]
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          service-id-prefix "test#"
          entitlement-manager (authz/->SimpleEntitlementManager nil)
          make-peer-requests-fn (fn [endpoint & _] (and (str/starts-with? endpoint "token/") (str/ends-with? endpoint "/refresh")) {})
          token "test-token"
          service-description1 (clojure.walk/stringify-keys
                                 {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1", :permitted-user "tu2", :token token,
                                  :metadata {"a" "b", "c" "d"}, :env {"MY_VAR" "a", "MY_VAR_2" "b"}})
          service-id1 (sd/service-description->service-id service-id-prefix service-description1)
          service-description2 (clojure.walk/stringify-keys
                                 {:cmd "tc2", :cpus 2, :mem 400, :version "d1e2f3", :run-as-user "tu1", :permitted-user "tu3", :token token})
          service-id2 (sd/service-description->service-id service-id-prefix service-description2)
          waiter-hostname "waiter-hostname.app.example.com"
          handle-list-tokens-request (wrap-handler-json-response handle-list-tokens-request)]

      (testing "delete:no-token-in-request"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :delete, :headers {}})]
          (is (= 400 status))
          (is (str/includes? body "Couldn't find token in request"))))

      (testing "delete:token-does-not-exist"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :delete, :headers {"x-waiter-token" (str "invalid-" token)}})]
          (is (= 404 status))
          (is (str/includes? body (str "Token invalid-" token " does not exist")))))

      (testing "delete:token-does-exist-unauthorized"
        (try
          (kv/store kv-store token (assoc service-description1 "owner" "tu2"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [status body]}
                (run-handle-token-request
                  kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                  {:headers {"accept" "application/json"
                             "x-waiter-token" token} 
                   :request-method :delete, :authorization/user "tu1"})
                {{message "message"
                  {:strs [owner user]} "details"} "waiter-error"} (json/read-str body)]
            (is (= 403 status))
            (is (= "User not allowed to delete token." message))
            (is (= "tu2" owner) body)
            (is (= "tu1" user))
            (is (not (nil? (kv/fetch kv-store token)))))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:already-deleted"
        (let [token-last-update-time (- (clock-millis) 10000)
              token-description (assoc service-description1
                      "owner" "tu1"
                      "deleted" true
                      "last-update-time" token-last-update-time)]
          (try
            (kv/store kv-store token token-description)
            (is (not (nil? (kv/fetch kv-store token))))
            (let [{:keys [status body]}
                  (run-handle-token-request
                    kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                    {:request-method :delete, :authorization/user "tu1", :headers {"x-waiter-token" token}})]
              (is (= 404 status))
              (is (str/includes? body (str "Token " token " does not exist")))
              (is (= token-description (kv/fetch kv-store token)) "Entry deleted from kv-store!"))
            (finally
              (kv/delete kv-store token)))))

      (testing "delete:token-does-exist-authorized:hard-delete-missing"
        (try
          (kv/store kv-store token (assoc service-description1 "owner" "tu1"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [status body]}
                (run-handle-token-request
                  kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                  {:request-method :delete, :authorization/user "tu1", :headers {"x-waiter-token" token}})]
            (is (= 200 status))
            (is (every? #(str/includes? body (str %)) [(str "\"delete\":\"" token "\""), "\"success\":true"]))
            (is (= (assoc service-description1 "deleted" true, "last-update-time" (clock-millis), "owner" "tu1")
                   (kv/fetch kv-store token))
                "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:hard-delete-false"
        (try
          (kv/store kv-store token (assoc service-description1 "owner" "tu1"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [status body]}
                (run-handle-token-request
                  kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                  {:authorization/user "tu1", :headers {"x-waiter-token" token}, :query-params {"hard-delete" "false"}, :request-method :delete})]
            (is (= 200 status))
            (is (every? #(str/includes? body (str %)) [(str "\"delete\":\"" token "\""), "\"success\":true"]))
            (is (= (assoc service-description1 "deleted" true, "last-update-time" (clock-millis), "owner" "tu1")
                   (kv/fetch kv-store token))
                "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:hard-delete-true"
        (try
          (kv/store kv-store token (assoc service-description1 "owner" "tu1"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [{:keys [status body]}
                (run-handle-token-request
                  kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                  {:authorization/user "tu1", :headers {"x-waiter-token" token}, :query-params {"hard-delete" "true"}, :request-method :delete})]
            (is (= 200 status))
            (is (str/includes? body (str "\"delete\":\"" token "\"")))
            (is (str/includes? body "\"hard-delete\":true"))
            (is (str/includes? body "\"success\":true"))
            (is (nil? (kv/fetch kv-store token)) "Entry not deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "delete:token-does-exist-authorized:hard-delete-true:disallowed"
        (try
          (kv/store kv-store token (assoc service-description1 "owner" "tu2"))
          (is (not (nil? (kv/fetch kv-store token))))
          (let [entitlement-manager (reify authz/EntitlementManager
                                      (authorized? [_ subject verb {:keys [user]}]
                                        (is (and (= subject "tu1") (= :admin verb) (= "tu2" user)))
                                        false))
                {:keys [status body]}
                (run-handle-token-request
                  kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                  {:authorization/user "tu1"
                   :headers {"accept" "application/json", "x-waiter-token" token}
                   :query-params {"hard-delete" "true"}
                   :request-method :delete})
                {{message "message" 
                  {{:strs [owner]} "metadata"} "details"} "waiter-error"} (json/read-str body)]
            (is (= 403 status))
            (is (= "Cannot hard-delete token." message))
            (is (= "tu2" owner) body)
            (is (not-empty (kv/fetch kv-store token)) "Entry deleted from kv-store!"))
          (finally
            (kv/delete kv-store token))))

      (testing "test:get-empty-service-description"
        (let [{:keys [status]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :get, :headers {"x-waiter-token" token}})]
          (is (= 404 status))))

      (testing "test:post-new-service-description"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str service-description1))})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description1 sd/token-description-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description1 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis), "owner" "tu1"} token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id1)))))

      (testing "test:list-tokens"
        (let [{:keys [status body]}
              (handle-list-tokens-request
                kv-store
                {:request-method :get, :authorization/user "tu1"})]
          (is (= 200 status))
          (is (= [{"token" token "owner" "tu1"}] (json/read-str body)))))

      (testing "test:post-new-service-description-different-owner"
        (let [token (str token "-tu")
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname (public-entitlement-manager) make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str (assoc service-description1 "owner" "tu2"
                                                                                             "token" token)))})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description1 sd/token-description-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description1 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis), "owner" "tu2"} token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id1)))))

      (testing "test:get-new-service-description"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :get, :headers {"x-waiter-token" token}})
              json-keys ["metadata" "env"]]
          (is (= 200 status))
          (doseq [key (keys (apply dissoc (select-keys service-description1 sd/service-description-keys) json-keys))]
            (is (str/includes? body (str (get service-description1 key)))))
          (doseq [key json-keys]
            (is (str/includes? body (json/write-str (get service-description1 key)))))))

      (testing "test:post-update-service-description"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str service-description2))})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description2 sd/token-description-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description2 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis), "owner" "tu1"} token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id1)))
          (is (empty? (sd/fetch-core kv-store service-id2)))))

      (testing "test:post-update-service-description-change-owner"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname (public-entitlement-manager) make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str (assoc service-description2 "owner" "tu2")))})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description2 sd/token-description-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description2 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis), "owner" "tu2"} token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id1)))
          (is (empty? (sd/fetch-core kv-store service-id2)))))

      (testing "test:post-update-service-description-do-not-change-owner"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname (public-entitlement-manager) make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str service-description2))})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (select-keys service-description2 sd/token-description-keys)
                 (sd/token->service-description-template kv-store token)))
          (let [{:keys [service-description-template token-metadata]} (sd/token->token-description kv-store token)]
            (is (= (dissoc service-description2 "token") service-description-template))
            (is (= {"last-update-time" (clock-millis), "owner" "tu2"} token-metadata)))
          (is (empty? (sd/fetch-core kv-store service-id1)))
          (is (empty? (sd/fetch-core kv-store service-id2)))))

      (testing "test:get-updated-service-description"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :get, :headers {"x-waiter-token" token}})]
          (is (= 200 status))
          (doseq [key (keys (select-keys service-description2 sd/service-description-keys))]
            (is (str/includes? body (str (get service-description2 key)))))))

      (testing "test:get-invalid-token"
        (let [{:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :get, :headers {"x-waiter-token" "###"}})]
          (is (= 404 status))
          (is (str/includes? body "Couldn't find token ###"))))

      (testing "test:post-new-service-description:missing-permitted-user"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1", :token token})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis), "owner" "tu1"))
                 (kv/fetch kv-store token)))))

      (testing "test:post-new-service-description:star-run-as-user"
        (let [token (str token (rand-int 100000))
              kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "*", :token token})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 200 status))
          (is (str/includes? body (str "Successfully created " token)))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis), "owner" "tu1"))
                 (kv/fetch kv-store token)))))

      (testing "test:post-new-service-description:edit-star-run-as-user"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "*",
                                     :permitted-user "tu2", :token token})
              _ (kv/store kv-store token (assoc service-description "run-as-user" "tu0" "owner" "tu1" "cpus" 2))
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 200 status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis), "owner" "tu1"))
                 (kv/fetch kv-store token)))))

      (testing "test:post-new-service-description:token-sync:allowed"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject test-user) (= :admin verb) (= "user2" user))))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :permitted-user "user1", :run-as-user "user1", :version "a1b2c3",
                                     :owner "user2", :token token})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user test-user, :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :query-params {"update-mode" "admin"}})]
          (is (= 200 status))
          (is (str/includes? body "Successfully created test-token"))
          (is (= (-> service-description
                     (dissoc "token")
                     (assoc "last-update-time" (clock-millis), "owner" "user2"))
                 (kv/fetch kv-store token))))))))

(deftest test-post-failure-in-handle-token-request
  (with-redefs [sd/service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd sd/service-description-keys))))]
    (let [entitlement-manager (authz/->SimpleEntitlementManager nil)
          make-peer-requests-fn (fn [endpoint _ _] (and (str/starts-with? endpoint "token/") (str/ends-with? endpoint "/refresh")) {})
          validate-service-description-fn (fn validate-service-description-fn [service-description] (sd/validate-schema service-description nil))
          token "test-token"
          waiter-hostname "waiter-hostname.app.example.com"]
      (testing "test:post-new-service-description:missing-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2"})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 400 status))
          (is (str/includes? body "Must provide the token"))))

      (testing "test:post-new-service-description:reserved-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token waiter-hostname})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :post, :authorization/user "tu1", :headers {},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 403 status))
          (is (str/includes? body "Token name is reserved"))))

      (testing "test:post-new-service-description:schema-fail"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus "not-an-int", :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token token})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:request-method :post, :authorization/user "tu1", :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 400 status))
          (is (str/includes? body "issue"))))

      (testing "test:post-new-service-description:edit-unauthorized-run-as-user"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu0",
                                     :permitted-user "tu2", :token token})
              _ (kv/store kv-store token (assoc service-description "run-as-user" "tu0" "owner" "tu1" "cpus" 2))
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 403 status))
          (is (str/includes? body "Cannot run as user"))))

      (testing "test:post-new-service-description:edit-unauthorized-owner"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token token})
              _ (kv/store kv-store token (assoc service-description "run-as-user" "tu0" "owner" "tu0" "cpus" 2))
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 403 status))
          (is (str/includes? body "Cannot change owner of token"))))

      (testing "test:post-new-service-description:create-unauthorized-owner"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token token, :owner "tu0"})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1", :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 403 status))
          (is (str/includes? body "Cannot create token as user"))))

      (testing "test:post-new-service-description:token-sync:invalid-admin-mode"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject user) (= :admin verb))))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :permitted-user "user1", :run-as-user "user1", :version "a1b2c3",
                                     :owner "user2", :token token})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user test-user, :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :query-params {"update-mode" "foobar"}})]
          (is (= 400 status))
          (is (str/includes? body "Invalid update-mode"))
          (is (nil? (kv/fetch kv-store token)))))

      (testing "test:post-new-service-description:token-sync:not-allowed"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              test-user "test-user"
              token "test-token-sync"
              entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ subject verb {:keys [user]}]
                                      (and (= subject user) (= :admin verb))))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :permitted-user "user1", :run-as-user "user1", :version "a1b2c3",
                                     :owner "user2", :token token})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user test-user, :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))
                 :query-params {"update-mode" "admin"}})]
          (is (= 403 status))
          (is (str/includes? body "Cannot administer token"))
          (is (nil? (kv/fetch kv-store token)))))

      (testing "test:post-new-service-description:invalid-instance-counts"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token token,
                                     :min-instances 2, :max-instances 1})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:request-method :post, :authorization/user "tu1", :headers {"x-waiter-token" token},
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 400 status))
          (is (str/includes? body "Minimum instances (2) must be <= Maximum instances (1)"))))

      (testing "test:post-new-service-description:invalid-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token "###",
                                     :min-instances 2, :max-instances 10})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn nil
                {:request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 400 status))
          (is (str/includes? body "Token must match pattern"))))

      (testing "test:post-new-service-description:invalid-token"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token "abcdefgh",
                                     :min-instances 2, :max-instances 10, :invalid-key "invalid-value"})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 400 status))
          (is (str/includes? body "Unsupported key(s) in token"))))

      (testing "test:post-new-service-description:cannot-modify-last-update-time"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :permitted-user "tu2", :token "abcdefgh",
                                     :min-instances 2, :max-instances 10, :last-update-time (System/currentTimeMillis)})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn (constantly true)
                {:request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})]
          (is (= 400 status))
          (is (str/includes? body "Cannot modify last-update-time token metadata"))))

      (testing "test:post-new-service-description:bad-token-metadata"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :token "abcdefgh", :metadata {"a" 12}})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:headers {"accept" "application/json"}
                 :request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})
              {{:strs [message]} "waiter-error"} (json/read-str body)] 
          (is (= 400 status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? message "did not have string values: a") body)
          (is (str/includes? message "Metadata values must be strings"))))

      (testing "test:post-new-service-description:bad-token-environment-vars"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                                     :token "abcdefgh", :env {"HOME" "12"}})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:headers {"accept" "application/json"}
                 :request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (not (str/includes? body "clojure")))
          (is (str/includes? message "environment variable keys are reserved: HOME") body)))

      (testing "test:post-new-service-description:invalid-authentication"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1", :authentication "unsupported"
                                     :token "abcdefgh"})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:headers {"accept" "application/json"}
                 :request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "invalid-authentication") body)))

      (testing "test:post-new-service-description:missing-permitted-user-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1", :authentication "disabled"
                                     :token "abcdefgh"})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:headers {"accept" "application/json"}
                 :request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "Tokens with authentication disabled must specify permitted-user as *, instead provided") body)))

      (testing "test:post-new-service-description:non-star-permitted-user-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1", :authentication "disabled", :permitted-user "pu1"
                                     :token "abcdefgh"})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:headers {"accept" "application/json"}
                 :request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "Tokens with authentication disabled must specify permitted-user as *, instead provided") body)))

      (testing "test:post-new-service-description:partial-description-with-authentication-disabled"
        (let [kv-store (kv/->LocalKeyValueStore (atom {}))
              service-description (clojure.walk/stringify-keys
                                    {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :authentication "disabled", :permitted-user "*"
                                     :token "abcdefgh"})
              {:keys [status body]}
              (run-handle-token-request
                kv-store waiter-hostname entitlement-manager make-peer-requests-fn validate-service-description-fn
                {:headers {"accept" "application/json"}
                 :request-method :post, :authorization/user "tu1",
                 :body (StringBufferInputStream. (json/write-str service-description))})
              {{:strs [message]} "waiter-error"} (json/read-str body)]
          (is (= 400 status))
          (is (str/includes? message "Tokens with authentication disabled must specify all required parameters") body))))))

(deftest test-store-service-description
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        kv-store (kv/->LocalKeyValueStore (atom {}))
        token "test-token"
        service-description (clojure.walk/stringify-keys
                              {:cmd "tc1", :cpus 1, :mem 200, :version "a1b2c3", :run-as-user "tu1",
                               :permitted-user "tu2", :name token, :min-instances 2, :max-instances 10})
        token-metadata {"owner" "test-user"}]
    (store-service-description-for-token synchronize-fn kv-store token service-description token-metadata)
    (let [token-description (kv/fetch kv-store token)]
      (is (= service-description (select-keys token-description sd/service-description-keys)))
      (is (= token-metadata (select-keys token-description sd/token-metadata-keys)))
      (is (= (merge service-description token-metadata) token-description)))))

(deftest test-token-index
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        tokens {"token1" {"owner" "owner1"}
                "token2" {"owner" "owner1"}
                "token3" {"owner" "owner2"}}
        kv-store (kv/->LocalKeyValueStore (atom {}))]
    (doseq [[token token-data] tokens]
      (kv/store kv-store token token-data))
    (reindex-tokens synchronize-fn kv-store (keys tokens))
    (is (= #{"token1" "token2"} (list-tokens-for-owner kv-store "owner1")))
    (is (= #{"token3"} (list-tokens-for-owner kv-store "owner2")))))

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
          {:keys [status body]} (handle-reindex-tokens-request
                                  synchronize-fn inter-router-request-fn kv-store list-tokens-fn
                                  {:request-method :post})
          json-response (json/read-str body)]
      (is (= 200 status))
      (is (= {:message "Successfully re-indexed." :tokens 3} (-> json-response clojure.walk/keywordize-keys)))
      (is @inter-router-request-fn-called))

    (let [inter-router-request-fn-called (atom nil)
          inter-router-request-fn (fn [] (reset! inter-router-request-fn-called true))
          {:keys [status body]} (handle-reindex-tokens-request 
                                  synchronize-fn inter-router-request-fn kv-store list-tokens-fn
                                  {:headers {"accept" "application/json"}, :request-method :get})
          json-response (json/read-str body)]
      (is (= 405 status))
      (is (not @inter-router-request-fn-called)))))

(deftest test-handle-list-tokens-request
  (let [lock (Object.)
        synchronize-fn (fn [_ f]
                         (locking lock
                           (f)))
        kv-store (kv/->LocalKeyValueStore (atom {}))
        handle-list-tokens-request (wrap-handler-json-response handle-list-tokens-request)]
    (store-service-description-for-token synchronize-fn kv-store "token1" {} {"owner" "owner1"})
    (store-service-description-for-token synchronize-fn kv-store "token2" {} {"owner" "owner1"})
    (store-service-description-for-token synchronize-fn kv-store "token3" {} {"owner" "owner2"})
    (let [{:keys [body status]} (handle-list-tokens-request kv-store {:request-method :get})]
      (is (= 200 status))
      (is (= #{{"owner" "owner1", "token" "token1"}
               {"owner" "owner1", "token" "token2"}
               {"owner" "owner2", "token" "token3"}} (set (json/read-str body)))))
    (let [{:keys [body status]} (handle-list-tokens-request kv-store {:request-method :get :query-string "owner=owner1"})]
      (is (= 200 status))
      (is (= #{{"owner" "owner1", "token" "token1"}
               {"owner" "owner1", "token" "token2"}} (set (json/read-str body)))))
    (let [{:keys [status body]} (handle-list-tokens-request kv-store {:headers {"accept" "application/json"}
                                                                      :request-method :post})
          json-response (try (json/read-str body)
                             (catch Exception _
                               (is (str "Failed to parse body as JSON:\n" body))))]
      (is (= 405 status)))
    (let [{:keys [body status]} (handle-list-tokens-request kv-store {:request-method :get :query-string "owner=owner2"})]
      (is (= 200 status))
      (is (= #{{"owner" "owner2", "token" "token3"}} (set (json/read-str body)))))
    (let [{:keys [body]} (handle-list-token-owners-request kv-store {:headers {"accept" "application/json"}
                                                                     :request-method :get})
          owner-map-keys (keys (json/read-str body))]
      (is (some #(= "owner1" %) owner-map-keys) "Should have had a key 'owner1'")
      (is (some #(= "owner2" %) owner-map-keys) "Should have had a key 'owner2'"))))
