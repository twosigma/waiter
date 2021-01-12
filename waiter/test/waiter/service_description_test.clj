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
(ns waiter.service-description-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.service-description :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.cache-utils :as cu]
            [waiter.util.date-utils :as du])
  (:import (clojure.lang ExceptionInfo)
           (org.joda.time DateTime)))

(deftest test-validate-service-description-schema
  (let [basic-description {"cpus" 1
                           "mem" 1
                           "cmd" "test command"
                           "version" "v123"
                           "run-as-user" "test-user"}]
    (is (nil? (s/check service-description-schema basic-description)))
    (is (nil? (s/check service-description-schema (assoc basic-description "cpus" 1.5 "mem" 1.5))))

    (is (nil? (s/check service-description-schema (assoc basic-description "backend-proto" "http"))))
    (is (nil? (s/check service-description-schema (assoc basic-description "backend-proto" "https"))))
    (is (nil? (s/check service-description-schema (assoc basic-description "backend-proto" "h2c"))))
    (is (nil? (s/check service-description-schema (assoc basic-description "backend-proto" "h2"))))
    (is (s/check service-description-schema (assoc basic-description "backend-proto" ["http"])))
    (is (s/check service-description-schema (assoc basic-description "backend-proto" "foo")))

    (is (s/check service-description-schema (dissoc basic-description "cmd")))
    (is (s/check service-description-schema (assoc basic-description "cmd" "")))

    (is (nil? (s/check service-description-schema (assoc basic-description "cmd-type" "shell"))))
    (is (s/check service-description-schema (assoc basic-description "cmd-type" "")))

    (is (nil? (s/check service-description-schema (assoc basic-description "concurrency-level" 5))))
    (is (s/check service-description-schema (assoc basic-description "concurrency-level" -1)))
    (is (s/check service-description-schema (assoc basic-description "concurrency-level" 20000000)))

    (is (s/check service-description-schema (dissoc basic-description "cpus")))
    (is (s/check service-description-schema (assoc basic-description "cpus" 0)))
    (is (s/check service-description-schema (assoc basic-description "cpus" -1)))
    (is (s/check service-description-schema (assoc basic-description "cpus" "1")))

    (is (nil? (s/check service-description-schema (assoc basic-description "env" {}))))
    (is (nil? (s/check service-description-schema (assoc basic-description "env" {"MY_VAR" "1", "MY_VAR_2" "2"}))))
    (is (nil? (s/check service-description-schema (assoc basic-description "env" {"MY_VAR" "1", "MY_other_VAR" "2"}))))
    (is (s/check service-description-schema (assoc basic-description "env" {"2MY_VAR" "1", "MY_OTHER_VAR" "2"})))
    (is (s/check service-description-schema (assoc basic-description "env" {(str/join (take 513 (repeat "A"))) "A"
                                                                            "MY_other_VAR" "2"})))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "env" {"MY_VAR" (str/join (take 513 (repeat "A")))
                                                                                       "MY_other_VAR" "2"})))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "env" (zipmap (take 150 (iterate #(str % "A") "a"))
                                                                                              (take 150 (iterate #(str % "A") "a"))))))))

    (is (nil? (s/check service-description-schema (assoc basic-description "disk" 1))))

    (is (nil? (s/check service-description-schema (assoc basic-description "grace-period-secs" 0))))
    (is (nil? (s/check service-description-schema (assoc basic-description "grace-period-secs" 5))))
    (is (nil? (s/check service-description-schema (assoc basic-description "grace-period-secs" 11))))
    (is (s/check service-description-schema (assoc basic-description "grace-period-secs" -1)))
    (is (s/check service-description-schema (assoc basic-description "grace-period-secs" (t/in-seconds (t/minutes 75)))))

    (is (nil? (s/check service-description-schema (assoc basic-description "health-check-authentication" "disabled"))))
    (is (nil? (s/check service-description-schema (assoc basic-description "health-check-authentication" "standard"))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "health-check-authentication" "jwt")))))

    (is (nil? (s/check service-description-schema (assoc basic-description "health-check-port-index" 0))))
    (is (nil? (s/check service-description-schema (assoc basic-description "health-check-port-index" 1))))
    (is (nil? (s/check service-description-schema (assoc basic-description "health-check-port-index" 9))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "health-check-port-index" -1)))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "health-check-port-index" 10)))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "health-check-port-index" "0")))))

    (is (nil? (s/check service-description-schema (assoc basic-description "health-check-url" "http://www.example.com/test/status"))))

    (is (s/check service-description-schema (dissoc basic-description "mem")))
    (is (s/check service-description-schema (assoc basic-description "mem" 0)))
    (is (s/check service-description-schema (assoc basic-description "mem" -1)))
    (is (s/check service-description-schema (assoc basic-description "mem" "1")))

    (is (nil? (s/check service-description-schema (assoc basic-description "metadata" {}))))
    (is (nil? (s/check service-description-schema (assoc basic-description "metadata" {"a" "b", "c-e" "d"}))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "metadata" {"a" "b", "1c" "e"})))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "metadata" {"a" "b", "c" 1})))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "metadata" {"a" "b", "c" {"d" "e"}})))))
    (is (not (nil? (s/check service-description-schema (assoc basic-description "metadata" (zipmap (take 400 (iterate #(str % "a") "a"))
                                                                                                   (take 400 (iterate #(str % "a") "a"))))))))
    (is (nil? (s/check service-description-schema (assoc basic-description "name" "testname123"))))
    (is (nil? (s/check service-description-schema (assoc basic-description "name" "testName123"))))
    (is (nil? (s/check service-description-schema (assoc basic-description "name" "test.name"))))
    (is (nil? (s/check service-description-schema (assoc basic-description "name" "test.n&me"))))
    (is (s/check service-description-schema (assoc basic-description "name" "")))

    (is (nil? (s/check service-description-schema (assoc basic-description "permitted-user" "testuser2"))))

    (is (nil? (s/check service-description-schema (assoc basic-description "ports" 1))))
    (is (nil? (s/check service-description-schema (assoc basic-description "ports" 5))))
    (is (s/check service-description-schema (assoc basic-description "ports" 11)))

    (is (s/check service-description-schema (dissoc basic-description "run-as-user")))
    (is (s/check service-description-schema (assoc basic-description "run-as-user" "")))

    (is (nil? (s/check service-description-schema (assoc basic-description "termination-grace-period-secs" 0))))
    (is (nil? (s/check service-description-schema (assoc basic-description "termination-grace-period-secs" 5))))
    (is (nil? (s/check service-description-schema (assoc basic-description "termination-grace-period-secs" 11))))
    (is (s/check service-description-schema (assoc basic-description "termination-grace-period-secs" -1)))
    (is (s/check service-description-schema (assoc basic-description "termination-grace-period-secs" (t/in-seconds (t/minutes 6)))))))

(deftest test-retrieve-token-from-service-description-or-hostname
  (let [test-cases (list
                     {:name "retrieve-token-from-service-description-or-hostname:missing-token"
                      :service-desc {"foo" "bar"}
                      :request-headers {}
                      :waiter-hostname "waiter-hostname.app.example.com"
                      :expected nil
                      }
                     {:name "retrieve-token-from-service-description-or-hostname:token-from-service-desc"
                      :service-desc {"x-waiter-token" "token-from-desc"}
                      :request-headers {}
                      :waiter-hostname "waiter-hostname.app.example.com"
                      :expected "token-from-desc"
                      }
                     {:name "retrieve-token-from-service-description-or-hostname:token-from-service-desc-in-presence -of-host"
                      :service-desc {"x-waiter-token" "token-from-desc"}
                      :request-headers {"host" "waiter1.test.example.com:1234"}
                      :waiter-hostname "waiter-hostname.app.example.com"
                      :expected "token-from-desc"
                      }
                     {:name "retrieve-token-from-service-description-or-hostname:token-from-hostname-with-port"
                      :service-desc {"foo" "bar"}
                      :request-headers {"host" "waiter1.test.example.com:1234"}
                      :waiter-hostname "waiter-hostname.app.example.com"
                      :expected "waiter1.test.example.com"
                      }
                     {:name "retrieve-token-from-service-description-or-hostname:token-from-hostname-without-port"
                      :service-desc {"foo" "bar"}
                      :request-headers {"host" "waiter2.test.example.com"}
                      :waiter-hostname "waiter-hostname.app.example.com"
                      :expected "waiter2.test.example.com"
                      }
                     {:name "retrieve-token-from-service-description-or-hostname:host-matching-waiter-hostname"
                      :service-desc {"foo" "bar"}
                      :request-headers {"host" "waiter-hostname.app.example.com"}
                      :waiter-hostname "waiter-hostname.app.example.com"
                      :expected nil
                      })]
    (doseq [{:keys [name service-desc request-headers waiter-hostname expected]} test-cases]
      (testing (str "Test " name)
        (let [{:keys [source token]}
              (retrieve-token-from-service-description-or-hostname service-desc request-headers #{waiter-hostname})]
          (when-not (= expected token)
            (log/info name ": expected=" expected ", actual=" token "with source:" source))
          (is (= expected token)))))))

(deftest test-service-description->service-id
  (testing "Service description to service id conversion"
    (testing "should produce ids with correct hashes"
      (let [service-id-prefix "test-waiter-prefix-"
            test-cases (list
                         {:name "service-description->service-id:no-description"
                          :input-data {}
                          :expected (str service-id-prefix "d41d8cd98f00b204e9800998ecf8427e")}
                         {:name "service-description->service-id:no-name-and-unused-keys"
                          :input-data {"foo" "bar", "baz" "fie"}
                          :expected (str service-id-prefix "d41d8cd98f00b204e9800998ecf8427e")}
                         {:name "service-description->service-id:name-present"
                          :input-data {"foo" "bar", "baz" "fie", "name" "fum"}
                          :expected (str service-id-prefix "fum-a33d34e194a19776939a94ae6dc1defc")}
                         {:name "service-description->service-id:cpus-mem-keys"
                          :input-data {"cpus" "bar", "mem" "fie"}
                          :expected (str service-id-prefix "c1bcd765471020b358c6e6853498abef")}
                         {:name "service-description->service-id:cpus-mem-keys-name-present"
                          :input-data {"cpus" "bar", "mem" "fie", "name" "fum"}
                          :expected (str service-id-prefix "fum-6332d43d6497743a0e2aef5420a5be2e")}
                         {:name "service-description->service-id:name-and-token-present"
                          :input-data {"foo" "bar", "baz" "fie", "name" "fum", "token" "my-cool-token"}
                          :expected (str service-id-prefix "fum-a33d34e194a19776939a94ae6dc1defc")}
                         {:name "service-description->service-id:cmd-name-and-token-present"
                          :input-data {"foo" "bar", "cmd" "fie", "name" "fum", "token" "my-cool-token"}
                          :expected (str service-id-prefix "fum-381b865ec36e621aff837ac20df731d9")}
                         {:name "service-description->service-id:name-and-permitted-user-present"
                          :input-data {"foo" "bar", "baz" "fie", "name" "fum", "permitted-user" "waiter-user"}
                          :expected (str service-id-prefix "fum-b28e57847e692f041edfcbb1a97d7e03")}
                         {:name "service-description->service-id:no-name-but-token-and-permitted-user-present"
                          :input-data {"foo" "bar", "baz" "fie", "token" "my-cool-token", "permitted-user" "waiter-user"}
                          :expected (str service-id-prefix "4a5e087ef4187dc8f100700dfa73f546")}
                         {:name "service-description->service-id:only-name-present"
                          :input-data {"name" "fum"}
                          :expected (str service-id-prefix "fum-a33d34e194a19776939a94ae6dc1defc")}
                         {:name "service-description->service-id:only-uppercase-letter-in-name-present"
                          :input-data {"name" "FU.M"}
                          :expected (str service-id-prefix "fum-1bb7478aabf479502e49fc26ae0f9a04")}
                         {:name "service-description->service-id:extra-chars-present-in--name"
                          :input-data {"name" "fum-123.4$A"}
                          :expected (str service-id-prefix "fum1234a-a1030ca63357baad681c25935eb4e494")}
                         {:name "service-description->service-id:invalid-chars-present-in--name"
                          :input-data {"name" "fum-!@#$%.,:()"}
                          :expected (str service-id-prefix "fum-df72716b57632adfc64b74165eb7d7f2")})]
        (doseq [{:keys [name input-data expected]} test-cases]
          (testing (str "Test " name)
            (is (= expected (service-description->service-id service-id-prefix input-data)))))))

    (testing "should take metric-group into account"
      (let [service-description {"cmd" "foo", "version" "bar", "run-as-user" "baz", "mem" 128, "cpus" 0.1}
            service-id #(service-description->service-id "prefix"
                                                         (s/validate service-description-schema
                                                                     (assoc service-description "metric-group" %)))]
        (is (not= (service-id "abc") (service-id "def")))))))

(deftest test-prepare-service-description-sources
  (let [test-user "test-header-user"
        token-user "token-user"
        kv-store (Object.)
        waiter-hostname "waiter-hostname.app.example.com"
        waiter-hostnames #{waiter-hostname}
        create-token-data (fn [token]
                            (if (and token (not (str/includes? token "no-token")))
                              (cond-> {"name" token
                                       "cmd" token-user
                                       "owner" "token-owner"
                                       "previous" {}
                                       "version" "token"}
                                (str/includes? token "allowed") (assoc "allowed-params" #{"BAR" "FOO"})
                                (str/includes? token "cpus") (assoc "cpus" "1")
                                (str/includes? token "fall") (assoc "fallback-period-secs" 600)
                                (str/includes? token "mem") (assoc "mem" "2")
                                (str/includes? token "per") (assoc "permitted-user" "puser")
                                (str/includes? token "proser") (assoc "profile" "service")
                                (str/includes? token "proweb") (assoc "profile" "webapp")
                                (str/includes? token "run") (assoc "run-as-user" "ruser"))
                              {}))
        build-source-tokens (fn [& tokens]
                              (mapv (fn [token] (source-tokens-entry token (create-token-data token))) tokens))]
    (with-redefs [kv/fetch (fn [in-kv-store token]
                             (is (= kv-store in-kv-store))
                             (create-token-data token))]
      (let [profile->defaults {"webapp" {"concurrency-level" 120
                                         "fallback-period-secs" 100}
                               "service" {"concurrency-level" 30
                                          "fallback-period-secs" 90
                                          "permitted-user" "*"}}
            service-description-defaults {}
            token-defaults {"fallback-period-secs" 300}
            metric-group-mappings []
            attach-service-defaults-fn #(merge-defaults % service-description-defaults profile->defaults metric-group-mappings)
            attach-token-defaults-fn #(attach-token-defaults % token-defaults profile->defaults)
            test-cases (list
                         {:name "prepare-service-description-sources:WITH Service Desc specific Waiter Headers except run-as-user"
                          :waiter-headers {"x-waiter-cmd" "test-cmd"
                                           "x-waiter-cpus" 1
                                           "x-waiter-foo" "bar"
                                           "x-waiter-mem" 1024
                                           "x-waiter-run-as-user" test-user
                                           "x-waiter-source" "serv-desc"
                                           "x-waiter-version" "test-version"}
                          :passthrough-headers {"host" "test-host"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" 1
                                               "mem" 1024
                                               "cmd" "test-cmd"
                                               "version" "test-version"
                                               "run-as-user" test-user}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-host"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host")
                                     :token->token-data {"test-host" (create-token-data "test-host")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-host"]}}
                         {:name "prepare-service-description-sources:WITH Waiter Hostname"
                          :waiter-headers {"x-waiter-cmd" "test-cmd"
                                           "x-waiter-cpus" 1
                                           "x-waiter-foo" "bar"
                                           "x-waiter-mem" 1024
                                           "x-waiter-run-as-user" test-user
                                           "x-waiter-source" "serv-desc"
                                           "x-waiter-version" "test-version"}
                          :passthrough-headers {"host" waiter-hostname
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" 1
                                               "mem" 1024
                                               "cmd" "test-cmd"
                                               "version" "test-version"
                                               "run-as-user" test-user}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:WITH Service Desc specific Waiter Headers"
                          :waiter-headers {"x-waiter-cmd" "test-cmd"
                                           "x-waiter-cpus" 1
                                           "x-waiter-foo" "bar"
                                           "x-waiter-mem" 1024
                                           "x-waiter-run-as-user" test-user
                                           "x-waiter-source" "serv-desc"
                                           "x-waiter-version" "test-version"}
                          :passthrough-headers {"host" "test-host"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" 1
                                               "mem" 1024
                                               "cmd" "test-cmd"
                                               "version" "test-version"
                                               "run-as-user" test-user}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-host"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host")
                                     :token->token-data {"test-host" (create-token-data "test-host")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-host"]}}
                         {:name "prepare-service-description-sources:WITH Service Desc specific Waiter Headers"
                          :waiter-headers {"x-waiter-cmd" "test-cmd"
                                           "x-waiter-cpus" 1
                                           "x-waiter-foo" "bar"
                                           "x-waiter-mem" 1024
                                           "x-waiter-run-as-user" test-user
                                           "x-waiter-source" "serv-desc"
                                           "x-waiter-version" "test-version"}
                          :passthrough-headers {"host" "test-host-no-token" "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" 1
                                               "mem" 1024
                                               "cmd" "test-cmd"
                                               "version" "test-version"
                                               "run-as-user" test-user}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:WITHOUT Service Desc specific Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar"
                                           "x-waiter-source" "serv-desc"}
                          :passthrough-headers {"host" "test-host"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-host"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host")
                                     :token->token-data {"test-host" (create-token-data "test-host")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-host"]}}
                         {:name "prepare-service-description-sources:Token in Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar"
                                           "x-waiter-source" "serv-desc"
                                           "x-waiter-token" "test-token"}
                          :passthrough-headers {"host" "test-host" "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-token")
                                     :token->token-data {"test-token" (create-token-data "test-token")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-token"]}}
                         {:name "prepare-service-description-sources:Two tokens in Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar"
                                           "x-waiter-source" "serv-desc"
                                           "x-waiter-token" "test-token,test-token2"}
                          :passthrough-headers {"host" "test-host"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token2"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-token" "test-token2")
                                     :token->token-data {"test-token" (create-token-data "test-token")
                                                         "test-token2" (create-token-data "test-token2")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-token" "test-token2"]}}
                         {:name "prepare-service-description-sources:Multiple tokens in Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar"
                                           "x-waiter-source" "serv-desc"
                                           "x-waiter-token" "test-token,test-token2,test-cpus-token,test-mem-token"}
                          :passthrough-headers {"host" "test-host" "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "cpus" "1"
                                                                    "mem" "2"
                                                                    "name" "test-mem-token"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-token" "test-token2" "test-cpus-token" "test-mem-token")
                                     :token->token-data {"test-cpus-token" (create-token-data "test-cpus-token")
                                                         "test-mem-token" (create-token-data "test-mem-token")
                                                         "test-token" (create-token-data "test-token")
                                                         "test-token2" (create-token-data "test-token2")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-token" "test-token2" "test-cpus-token" "test-mem-token"]}}
                         {:name "prepare-service-description-sources:Using Host with missing values"
                          :waiter-headers {}
                          :passthrough-headers {"host" "test-host"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-host"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host")
                                     :token->token-data {"test-host" (create-token-data "test-host")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-host"]}}
                         {:name "prepare-service-description-sources:Using Host without port with missing values"
                          :waiter-headers {}
                          :passthrough-headers {"host" "test-host:1234"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-host"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host")
                                     :token->token-data {"test-host" (create-token-data "test-host")}
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence ["test-host"]}}
                         {:name "prepare-service-description-sources:Using Token with run-as-user"
                          :waiter-headers {"x-waiter-token" "test-token-run"}
                          :passthrough-headers {"host" "test-host:1234"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token-run"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-token-run")
                                     :token->token-data {"test-token-run" (create-token-data "test-token-run")}
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence ["test-token-run"]}}
                         {:name "prepare-service-description-sources:Using Token with permitted-user"
                          :waiter-headers {"x-waiter-token" "test-token-per-fall"}
                          :passthrough-headers {"host" "test-host:1234"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 600
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token-per-fall"
                                                                    "version" "token" "permitted-user" "puser"}
                                     :source-tokens (build-source-tokens "test-token-per-fall")
                                     :token->token-data {"test-token-per-fall" (create-token-data "test-token-per-fall")}
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence ["test-token-per-fall"]}}
                         {:name "prepare-service-description-sources:Using Token with run-as-user and permitted-user and another token"
                          :waiter-headers {"x-waiter-token" "test-token-per-run"}
                          :passthrough-headers {"host" "test-host:1234"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token-per-run"
                                                                    "permitted-user" "puser"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-token-per-run")
                                     :token->token-data {"test-token-per-run" (create-token-data "test-token-per-run")}
                                     :token-authentication-disabled false
                                     :token-preauthorized true
                                     :token-sequence ["test-token-per-run"]}}
                         {:name "prepare-service-description-sources:Using Token with run-as-user and permitted-user"
                          :waiter-headers {"x-waiter-token" "test-token-per-run,test-cpus-token"}
                          :passthrough-headers {"host" "test-host:1234"
                                                "fee" "foe"}
                          :expected {:fallback-period-secs 300
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "cpus" "1"
                                                                    "name" "test-cpus-token"
                                                                    "permitted-user" "puser"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-token-per-run" "test-cpus-token")
                                     :token->token-data {"test-cpus-token" (create-token-data "test-cpus-token")
                                                         "test-token-per-run" (create-token-data "test-token-per-run")}
                                     :token-authentication-disabled false
                                     :token-preauthorized false
                                     :token-sequence ["test-token-per-run" "test-cpus-token"]}}
                         {:name "prepare-service-description-sources:Parse metadata headers"
                          :waiter-headers {"x-waiter-cpus" "1"
                                           "x-waiter-metadata-baz" "quux"
                                           "x-waiter-metadata-foo" "bar"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 300
                                     :headers {"metadata" {"foo" "bar"
                                                           "baz" "quux"}
                                               "cpus" "1"}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:Parse environment headers:valid keys"
                          :waiter-headers {"x-waiter-cpus" "1"
                                           "x-waiter-env-baz" "quux"
                                           "x-waiter-env-foo_bar" "bar"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 300
                                     :headers {"env" {"BAZ" "quux"
                                                      "FOO_BAR" "bar"}
                                               "cpus" "1"}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:Parse param headers:valid keys:host-token"
                          :waiter-headers {"x-waiter-param-bar" "bar-value"
                                           "x-waiter-param-foo" "foo-value"}
                          :passthrough-headers {"host" "test-host-allowed-cpus-mem-per-run:1234"}
                          :expected {:fallback-period-secs 300
                                     :headers {"param" {"BAR" "bar-value"
                                                        "FOO" "foo-value"}}
                                     :service-description-template {"allowed-params" #{"BAR" "FOO"}
                                                                    "cmd" "token-user"
                                                                    "cpus" "1"
                                                                    "mem" "2"
                                                                    "name" "test-host-allowed-cpus-mem-per-run"
                                                                    "permitted-user" "puser"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host-allowed-cpus-mem-per-run")
                                     :token->token-data {"test-host-allowed-cpus-mem-per-run" (create-token-data "test-host-allowed-cpus-mem-per-run")}
                                     :token-authentication-disabled false
                                     :token-preauthorized true
                                     :token-sequence ["test-host-allowed-cpus-mem-per-run"]}}
                         {:name "prepare-service-description-sources:Parse param headers:valid keys:host-token:on-the-fly"
                          :waiter-headers {"x-waiter-cpus" "20"
                                           "x-waiter-param-bar" "bar-value"
                                           "x-waiter-param-foo" "foo-value"}
                          :passthrough-headers {"host" "test-host-allowed-cpus-mem-per-run:1234"}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" "20"
                                               "param" {"BAR" "bar-value"
                                                        "FOO" "foo-value"}}
                                     :service-description-template {"allowed-params" #{"BAR" "FOO"}
                                                                    "cmd" "token-user"
                                                                    "cpus" "1"
                                                                    "mem" "2"
                                                                    "name" "test-host-allowed-cpus-mem-per-run"
                                                                    "permitted-user" "puser"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host-allowed-cpus-mem-per-run")
                                     :token->token-data {"test-host-allowed-cpus-mem-per-run" (create-token-data "test-host-allowed-cpus-mem-per-run")}
                                     :token-authentication-disabled false
                                     :token-preauthorized true
                                     :token-sequence ["test-host-allowed-cpus-mem-per-run"]}}
                         {:name "prepare-service-description-sources:Parse param headers:valid keys:token-header"
                          :waiter-headers {"x-waiter-param-bar" "bar-value"
                                           "x-waiter-param-foo" "foo-value"
                                           "x-waiter-token" "test-host-allowed-cpus-mem-per-run"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 300
                                     :headers {"param" {"BAR" "bar-value"
                                                        "FOO" "foo-value"}}
                                     :service-description-template {"allowed-params" #{"BAR" "FOO"}
                                                                    "cmd" "token-user"
                                                                    "cpus" "1"
                                                                    "mem" "2"
                                                                    "name" "test-host-allowed-cpus-mem-per-run"
                                                                    "permitted-user" "puser"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"}
                                     :source-tokens (build-source-tokens "test-host-allowed-cpus-mem-per-run")
                                     :token->token-data {"test-host-allowed-cpus-mem-per-run" (create-token-data "test-host-allowed-cpus-mem-per-run")}
                                     :token-authentication-disabled false
                                     :token-preauthorized true
                                     :token-sequence ["test-host-allowed-cpus-mem-per-run"]}}
                         {:name "prepare-service-description-sources:Parse param headers:valid keys"
                          :waiter-headers {"x-waiter-cpus" "1"
                                           "x-waiter-param-baz" "quux"
                                           "x-waiter-param-foo_bar" "bar"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" "1"
                                               "param" {"BAZ" "quux"
                                                        "FOO_BAR" "bar"}}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:Parse distinct e and param headers:valid keys"
                          :waiter-headers {"x-waiter-cpus" "1"
                                           "x-waiter-env-baz" "quux"
                                           "x-waiter-env-foo_bar" "bar"
                                           "x-waiter-param-baz" "quux"
                                           "x-waiter-param-foo_bar" "bar"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" "1"
                                               "env" {"BAZ" "quux"
                                                      "FOO_BAR" "bar"}
                                               "param" {"BAZ" "quux"
                                                        "FOO_BAR" "bar"}}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:Parse overlap env and param headers:valid keys"
                          :waiter-headers {"x-waiter-cpus" "1"
                                           "x-waiter-env-baz" "quux"
                                           "x-waiter-env-foo_bar" "bar1"
                                           "x-waiter-param-baz" "quux"
                                           "x-waiter-param-foo_bar" "bar2"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" "1"
                                               "env" {"BAZ" "quux"
                                                      "FOO_BAR" "bar1"}
                                               "param" {"BAZ" "quux"
                                                        "FOO_BAR" "bar2"}}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:Parse environment headers:invalid keys"
                          :waiter-headers {"x-waiter-cpus" "1"
                                           "x-waiter-env-1" "quux"
                                           "x-waiter-env-foo-bar" "bar"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 300
                                     :headers {"cpus" "1"
                                               "env" {"1" "quux"
                                                      "FOO-BAR" "bar"}}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         {:name "prepare-service-description-sources:profile:not-preauthorized-missing-permitted-user"
                          :waiter-headers {"x-waiter-token" "test-token-run-proweb"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 100
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token-run-proweb"
                                                                    "profile" "webapp"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"},
                                     :source-tokens (build-source-tokens "test-token-run-proweb")
                                     :token->token-data {"test-token-run-proweb" (create-token-data "test-token-run-proweb")}
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence ["test-token-run-proweb"]}}
                         {:name "prepare-service-description-sources:profile:not-preauthorized-missing-run-as-user"
                          :waiter-headers {"x-waiter-token" "test-token-proser"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 90
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token-proser"
                                                                    "profile" "service"
                                                                    "version" "token"},
                                     :source-tokens (build-source-tokens "test-token-proser")
                                     :token->token-data {"test-token-proser" (create-token-data "test-token-proser")}
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence ["test-token-proser"]}}
                         {:name "prepare-service-description-sources:profile:preauthorized-missing-run-as-user"
                          :waiter-headers {"x-waiter-token" "test-token-run-proser"}
                          :passthrough-headers {}
                          :expected {:fallback-period-secs 90
                                     :headers {}
                                     :service-description-template {"cmd" "token-user"
                                                                    "name" "test-token-run-proser"
                                                                    "profile" "service"
                                                                    "run-as-user" "ruser"
                                                                    "version" "token"},
                                     :source-tokens (build-source-tokens "test-token-run-proser")
                                     :token->token-data {"test-token-run-proser" (create-token-data "test-token-run-proser")}
                                     :token-authentication-disabled false,
                                     :token-preauthorized true,
                                     :token-sequence ["test-token-run-proser"]}}
                         )]
        (doseq [{:keys [expected name passthrough-headers waiter-headers]} test-cases]
          (testing (str "Test " name)
            (let [actual (prepare-service-description-sources
                           {:passthrough-headers passthrough-headers
                            :waiter-headers waiter-headers}
                           kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn)]
              (when (not= expected actual)
                (println name)
                (println "Expected: " (into (sorted-map) expected))
                (println "Actual:   " (into (sorted-map) actual)))
              (is (= expected actual)))))))))

(deftest test-prepare-service-description-sources-with-authentication-disabled
  (let [kv-store (Object.)
        waiter-hostnames ["waiter-hostname.app.example.com"]
        test-token "test-token-name"
        service-description-defaults {}
        profile->defaults {}
        metric-group-mappings []
        attach-service-defaults-fn #(merge-defaults % service-description-defaults profile->defaults metric-group-mappings)
        token-defaults {"fallback-period-secs" 300}
        attach-token-defaults-fn #(merge token-defaults %)]
    (testing "authentication-disabled token"
      (let [token-data {"authentication" "disabled"
                        "cmd" "a-command"
                        "cpus" "1"
                        "mem" "2"
                        "name" test-token
                        "owner" "token-owner"
                        "permitted-user" "*"
                        "previous" {}
                        "run-as-user" "ruser"
                        "version" "token"}]
        (with-redefs [kv/fetch (fn [in-kv-store token]
                                 (is (= kv-store in-kv-store))
                                 (is (= test-token token))
                                 token-data)]
          (let [waiter-headers {"x-waiter-token" test-token}
                passthrough-headers {"host" "test-host:1234", "fee" "foe"}
                actual (prepare-service-description-sources
                         {:waiter-headers waiter-headers
                          :passthrough-headers passthrough-headers}
                         kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn)
                expected {:fallback-period-secs 300
                          :headers {}
                          :service-description-template (select-keys token-data service-parameter-keys)
                          :source-tokens [(source-tokens-entry test-token token-data)]
                          :token->token-data {test-token token-data}
                          :token-authentication-disabled true
                          :token-preauthorized true
                          :token-sequence [test-token]}]
            (is (= expected actual))))))

    (testing "limited-access token"
      (let [token-data {"authentication" "standard"
                        "cmd" "a-command"
                        "cpus" "1"
                        "mem" "2"
                        "name" test-token
                        "owner" "token-owner"
                        "permitted-user" "*"
                        "previous" {}
                        "run-as-user" "ruser"
                        "version" "token"}]
        (with-redefs [kv/fetch (fn [in-kv-store token]
                                 (is (= kv-store in-kv-store))
                                 (is (= test-token token))
                                 token-data)]
          (let [waiter-headers {"x-waiter-token" test-token}
                passthrough-headers {"host" "test-host:1234", "fee" "foe"}
                actual (prepare-service-description-sources
                         {:waiter-headers waiter-headers
                          :passthrough-headers passthrough-headers}
                         kv-store waiter-hostnames attach-service-defaults-fn attach-token-defaults-fn)
                expected {:fallback-period-secs 300
                          :headers {}
                          :service-description-template (select-keys token-data service-parameter-keys)
                          :source-tokens [(source-tokens-entry test-token token-data)]
                          :token->token-data {test-token token-data}
                          :token-authentication-disabled false
                          :token-preauthorized true
                          :token-sequence [test-token]}]
            (is (= expected actual))))))))

(defn- compute-service-description-helper
  ([sources &
    {:keys [assoc-run-as-user-approved? component->previous-descriptor-fns kv-store service-id-prefix
            metric-group-mappings profile->defaults service-description-defaults  waiter-headers]
     :or {metric-group-mappings []
          profile->defaults {}
          service-description-defaults {}
          service-id-prefix "test-service-"}}]
   (with-redefs [metric-group-filter (fn [sd _] sd)
                 service-description-schema {s/Str s/Any}]
     (let [assoc-run-as-user-approved? (or assoc-run-as-user-approved? (constantly false))
           component->previous-descriptor-fns (or component->previous-descriptor-fns {})
           kv-store (or kv-store (kv/->LocalKeyValueStore (atom {})))
           waiter-headers (or waiter-headers {})
           passthrough-headers {}
           metric-group-mappings []
           current-user "current-request-user"
           builder-context {:kv-store kv-store
                            :metric-group-mappings metric-group-mappings
                            :profile->defaults profile->defaults
                            :service-description-defaults service-description-defaults}
           service-description-builder (create-default-service-description-builder builder-context)]
       (compute-service-description
         sources waiter-headers passthrough-headers component->previous-descriptor-fns service-id-prefix
         current-user assoc-run-as-user-approved? service-description-builder)))))

(defn- service-description
  ([sources &
    {:keys [assoc-run-as-user-approved? kv-store profile->defaults service-description-defaults waiter-headers]}]
   (let [{:keys [service-description]}
         (compute-service-description-helper
           sources
           :assoc-run-as-user-approved? assoc-run-as-user-approved?
           :kv-store kv-store
           :profile->defaults profile->defaults
           :service-description-defaults service-description-defaults
           :waiter-headers waiter-headers)]
     service-description)))

(deftest test-compute-service-description-on-the-fly?
  (let [defaults {"health-check-url" "/ping", "permitted-user" "bob"}
        sources {:profile->defaults {:default defaults}
                 :service-description-template {"cmd" "token-cmd"}}
        compute-on-the-fly (fn compute-on-the-fly [waiter-headers]
                             (-> (compute-service-description-helper sources :waiter-headers waiter-headers)
                               :on-the-fly?))]
    (is (nil? (compute-on-the-fly {})))
    (is (nil? (compute-on-the-fly {"x-waiter-dummy" "value-does-not-matter"})))
    (is (nil? (compute-on-the-fly {"cmd" "on-the-fly-cmd", "run-as-user" "on-the-fly-ru"})))
    (is (compute-on-the-fly {"x-waiter-cmd" "on-the-fly-cmd", "x-waiter-run-as-user" "on-the-fly-ru"}))
    (is (compute-on-the-fly {"x-waiter-token" "value-does-not-matter"}))))

(deftest test-compute-service-description-source-tokens
  (let [defaults {"health-check-url" "/ping", "permitted-user" "bob"}
        source-tokens [:foo-bar]
        sources {:profile->defaults {:default defaults}
                 :service-description-template {"cmd" "token-cmd"}
                 :source-tokens source-tokens}
        compute-source-tokens (fn compute-source-tokens [waiter-headers]
                                (-> (compute-service-description-helper sources :waiter-headers waiter-headers)
                                  :source-tokens))]
    (is (= source-tokens (compute-source-tokens {})))))

(deftest test-compute-service-description
  (testing "Service description computation"

    (testing "only token from host with permitted-user in defaults"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"}
             (service-description {:service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "only token from host without permitted-user in defaults"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}))))

    (testing "only token from header without permitted-user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "only token from header with permitted-user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-user"
              "run-as-user" "token-user"}
             (service-description {:service-description-template {"cmd" "token-cmd"
                                                                  "permitted-user" "token-user"
                                                                  "run-as-user" "token-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "token and run-as-user from header with permitted-user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:headers {"run-as-user" "on-the-fly-ru"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "permitted-user" "token-user"
                                                                  "run-as-user" "token-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"
                                                   "x-waiter-run-as-user" "on-the-fly-ru"}))))

    (testing "only token from host with defaults missing permitted user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}))))

    (testing "only token from header with defaults missing permitted user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "only token from host with dummy header"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}
                                  :waiter-headers {"x-waiter-dummy" "value-does-not-matter"}))))

    (testing "only on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token host with non-intersecting values"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:headers {"version" "on-the-fly-version"}
                                   :service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token header with non-intersecting values"
      (is (= {"cmd" "token-cmd"
              "concurrency-level" 5
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:headers {"version" "on-the-fly-version"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "concurrency-level" 5}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token + param header - on-the-fly"
      (is (= {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
              "cmd" "token-cmd"
              "concurrency-level" 5
              "env" {"VAR_1" "VALUE-1"
                     "VAR_2" "VALUE-2"}
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:headers {"param" {"VAR_1" "VALUE-1"
                                                      "VAR_2" "VALUE-2"}
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "run-as-user" "test-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token + distinct param header - on-the-fly"
      (is (= {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
              "cmd" "token-cmd"
              "concurrency-level" 5
              "env" {"VAR_1" "VALUE-1e"
                     "VAR_2" "VALUE-2e"
                     "VAR_3" "VALUE-3p"
                     "VAR_4" "VALUE-4p"}
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:headers {"param" {"VAR_3" "VALUE-3p"
                                                      "VAR_4" "VALUE-4p"}
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token + overlap param header - on-the-fly"
      (is (= {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
              "cmd" "token-cmd"
              "concurrency-level" 5
              "env" {"VAR_1" "VALUE-1e"
                     "VAR_2" "VALUE-2p"
                     "VAR_3" "VALUE-3p"}
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:headers {"param" {"VAR_2" "VALUE-2p"
                                                      "VAR_3" "VALUE-3p"}
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token + param header - token due to param"
      (is (= {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
              "cmd" "token-cmd"
              "concurrency-level" 5
              "env" {"VAR_1" "VALUE-1"
                     "VAR_2" "VALUE-2"}
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "test-user"}
             (service-description {:headers {"param" {"VAR_1" "VALUE-1"
                                                      "VAR_2" "VALUE-2"}}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "run-as-user" "test-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token + distinct param header - token due to param"
      (is (= {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
              "cmd" "token-cmd"
              "concurrency-level" 5
              "env" {"VAR_1" "VALUE-1e"
                     "VAR_2" "VALUE-2e"
                     "VAR_3" "VALUE-3p"
                     "VAR_4" "VALUE-4p"}
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "test-user"}
             (service-description {:headers {"param" {"VAR_3" "VALUE-3p"
                                                      "VAR_4" "VALUE-4p"}}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token + overlap param header - token due to param"
      (is (= {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
              "cmd" "token-cmd"
              "concurrency-level" 5
              "env" {"VAR_1" "VALUE-1e"
                     "VAR_2" "VALUE-2p"
                     "VAR_3" "VALUE-3p"}
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "test-user"}
             (service-description {:headers {"param" {"VAR_2" "VALUE-2p"
                                                      "VAR_3" "VALUE-3p"}}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token host with intersecting values"
      (is (= {"cmd" "on-the-fly-cmd"
              "concurrency-level" 6
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "concurrency-level" 6}
                                   :service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "token header with intersecting values"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "intersecting values with additional fields"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "name" "token-name"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "name" "token-name"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "permitted user from token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"permitted-user" "token-pu"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}))))

    (testing "permitted user from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "on-the-fly-pu"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "permitted-user" "on-the-fly-pu"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}))))

    (testing "permitted user intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "on-the-fly-pu"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "permitted-user" "on-the-fly-pu"}
                                   :service-description-template {"permitted-user" "token-pu"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}))))

    (testing "run as user and permitted user only in token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"run-as-user" "token-ru"
                                                                  "permitted-user" "token-pu"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}))))

    (testing "run as user and permitted user from token and no on-the-fly headers"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "token-ru"}
             (service-description {:service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "token-ru"
                                                                  "permitted-user" "token-pu"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}))))

    (testing "missing permitted user in token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "token-ru"}
             (service-description {:service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "token-ru"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "run as user from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "run-as-user" "on-the-fly-ru"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-run-as-user" "on-the-fly-ru"}))))

    (testing "run as user intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "run-as-user" "on-the-fly-ru"}
                                   :service-description-template {"run-as-user" "token-ru"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-run-as-user" "on-the-fly-ru"}))))

    (testing "run as user provided from on-the-fly header with hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "chris"}
             (service-description {:headers {"run-as-user" "chris"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "alice"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-run-as-user" "chris"}))))

    (testing "run as user star from on-the-fly header with hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"run-as-user" "*"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "alice"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-run-as-user" "*"}))))

    (testing "run as user star from hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"}
             (service-description {:headers {}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "*"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {}))))

    (testing "run as user star from on-the-fly token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:headers {}
                                   :service-description-template {"cmd" "token-cmd", "run-as-user" "*"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-token" "on-the-fly-token"}))))

    (testing "run as user star from on-the-fly headers without permitted-user"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "run-as-user" "*"}
                                   :service-description-template {"run-as-user" "token-ru"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-run-as-user" "*"}))))

    (testing "run as user star from on-the-fly headers with permitted-user"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "alice"
              "run-as-user" "current-request-user"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "permitted-user" "alice"
                                             "run-as-user" "*"}
                                   :service-description-template {"run-as-user" "token-ru"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-permitted-user" "alice"
                                                   "x-waiter-run-as-user" "*"}))))

    (testing "run as user in headers with permitted-user * in tokens"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "header-user"}
             (service-description {:headers {"run-as-user" "header-user"}
                                   :service-description-template {"run-as-user" "*"
                                                                  "permitted-user" "*"
                                                                  "cmd" "token-cmd"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}
                                  :waiter-headers {"x-waiter-run-as-user" "header-user"}))))

    (testing "overrides"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            basic-service-description {"cmd" "on-the-fly-cmd"
                                       "run-as-user" "on-the-fly-ru"}
            basic-service-id (service-description->service-id "test-service-" basic-service-description)]

        (testing "active"
          (store-service-description-overrides
            kv-store
            basic-service-id
            "current-request-user"
            {"scale-factor" 0.3})
          (is (= (assoc basic-service-description
                   "health-check-url" "/ping"
                   "permitted-user" "bob"
                   "scale-factor" 0.3)
                 (service-description {:headers {"cmd" "on-the-fly-cmd"
                                                 "run-as-user" "on-the-fly-ru"}}
                                      :profile->defaults {"webapp" {"concurrency-level" 120}}
                                      :service-description-defaults {"health-check-url" "/ping"
                                                                     "permitted-user" "bob"
                                                                     "scale-factor" 1}
                                      :kv-store kv-store))))

        (testing "inactive"
          (clear-service-description-overrides
            kv-store
            basic-service-id
            "current-request-user")
          (is (= (assoc basic-service-description
                   "health-check-url" "/ping"
                   "permitted-user" "bob")
                 (service-description {:headers {"cmd" "on-the-fly-cmd"
                                                 "run-as-user" "on-the-fly-ru"}}
                                      :profile->defaults {"webapp" {"concurrency-level" 120}}
                                      :service-description-defaults {"health-check-url" "/ping"
                                                                     "permitted-user" "bob"}
                                      :kv-store kv-store))))))

    (testing "override token metadata from headers"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "metadata" {"e" "f"}}
             (service-description {:headers {"metadata" {"e" "f"}}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "metadata" {"a" "b", "c" "d"}}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"
                                                                 "permitted-user" "bob"}))))

    (testing "sanitize metadata"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"
              "metadata" {"abc" "DEF"}}
             (service-description {:service-description-template {"cmd" "token-cmd"
                                                                  "metadata" {"Abc" "DEF"}}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/ping"}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "metric group from token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "token-mg"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"metric-group" "token-mg"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/health"}))))

    (testing "metric group from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "on-the-fly-mg"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "metric-group" "on-the-fly-mg"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/health"}))))

    (testing "metric group intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "on-the-fly-mg"}
             (service-description {:headers {"cmd" "on-the-fly-cmd"
                                             "metric-group" "on-the-fly-mg"}
                                   :service-description-template {"metric-group" "token-mg"}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/health"}))))

    (testing "auto-populate run-as-user"
      (is (= {"cmd" "some-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "permitted-user" "current-request-user"
              "metric-group" "token-mg"}
             (service-description {:service-description-template {"cmd" "some-cmd"
                                                                  "metric-group" "token-mg"}}
                                  :assoc-run-as-user-approved? (constantly true)
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/health"}))))

    (testing "disable instance-expiry"
      (is (= {"cmd" "some-cmd"
              "health-check-url" "/health"
              "instance-expiry-mins" 0}
             (service-description {:service-description-template {"cmd" "some-cmd"
                                                                  "instance-expiry-mins" 0}}
                                  :profile->defaults {"webapp" {"concurrency-level" 120}}
                                  :service-description-defaults {"health-check-url" "/health"}))))

    (testing "profile parameter"
      (let [service-description-defaults {"cmd" "default-cmd"
                                          "health-check-url" "/ping"}
            profile->defaults {"webapp" {"cmd" "web-cmd"}}]
        (testing "from token"
          (is (= {"cmd" "web-cmd"
                  "health-check-url" "/ping"
                  "profile" "webapp"}
                 (service-description {:headers {}
                                       :service-description-template {"profile" "webapp"}}
                                      :profile->defaults profile->defaults
                                      :service-description-defaults service-description-defaults)))
          (is (= {"cmd" "token-cmd"
                  "health-check-url" "/ping"
                  "profile" "webapp"}
                 (service-description {:headers {}
                                       :service-description-template {"cmd" "token-cmd"
                                                                      "profile" "webapp"}}
                                      :profile->defaults profile->defaults
                                      :service-description-defaults service-description-defaults)))
          (is (= {"cmd" "on-the-fly-cmd"
                  "health-check-url" "/ping"
                  "profile" "webapp"
                  "run-as-user" "current-request-user"}
                 (service-description {:headers {"cmd" "on-the-fly-cmd"}
                                       :service-description-template {"cmd" "token-cmd"
                                                                      "profile" "webapp"}}
                                      :profile->defaults profile->defaults
                                      :service-description-defaults service-description-defaults))))

        (testing "from on-the-fly"
          (is (= {"cmd" "web-cmd"
                  "health-check-url" "/ping"
                  "profile" "webapp"
                  "run-as-user" "current-request-user"}
                 (service-description {:headers {"profile" "webapp"}}
                                      :profile->defaults profile->defaults
                                      :service-description-defaults service-description-defaults)))
          (is (= {"cmd" "on-the-fly-cmd"
                  "health-check-url" "/ping"
                  "profile" "webapp"
                  "run-as-user" "current-request-user"}
                 (service-description {:headers {"cmd" "on-the-fly-cmd"
                                                 "profile" "webapp"}}
                                      :profile->defaults profile->defaults
                                      :service-description-defaults service-description-defaults))))))))

(deftest test-compute-service-description-error-scenarios
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id-prefix "test-service-"
        test-user "test-header-user"
        waiter-headers {}
        passthrough-headers {}
        component->previous-descriptor-fns {}
        assoc-run-as-user-approved? (constantly false)
        run-compute-service-description
        (fn run-compute-service-description [{:keys [profile->defaults service-description-defaults] :as sources}]
          (let [builder-context {:kv-store kv-store
                                 :profile->defaults profile->defaults
                                 :service-description-defaults service-description-defaults}
                service-description-builder (create-default-service-description-builder builder-context)
                result-descriptor
                (compute-service-description
                  sources waiter-headers passthrough-headers component->previous-descriptor-fns service-id-prefix
                  test-user assoc-run-as-user-approved? service-description-builder)
                result-errors (validate-service-description kv-store service-description-builder result-descriptor)]
            (when result-errors
              (throw result-errors))
            result-descriptor))]
    (is (thrown? Exception
                 (run-compute-service-description {:headers {}
                                                   :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                   :service-description-defaults {"health-check-url" "/ping"}
                                                   :service-description-template {"cmd" "test command"
                                                                                  "cpus" "one"
                                                                                  "mem" 200
                                                                                  "version" "a1b2c3"
                                                                                  "run-as-user" test-user}})))
    (is (thrown? Exception
                 (run-compute-service-description {:headers {}
                                                   :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                   :service-description-defaults {"health-check-url" 1}
                                                   :service-description-template {"cmd" "test command"
                                                                                  "cpus" 1
                                                                                  "mem" 200
                                                                                  "version" "a1b2c3"
                                                                                  "run-as-user" test-user}})))
    (is (thrown? Exception
                 (run-compute-service-description {:headers {}
                                                   :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                   :service-description-defaults {"health-check-url" 1}
                                                   :service-description-template {}})))
    (is (thrown? Exception
                 (run-compute-service-description {:profile->defaults {"webapp" {"concurrency-level" 120}}
                                                   :service-description-defaults {"health-check-url" "/health"}
                                                   :service-description-template {"cmd" "cmd for missing run-as-user"
                                                                                  "cpus" 1
                                                                                  "mem" 200
                                                                                  "version" "a1b2c3"}})))

    (testing "invalid allowed params - reserved"
      (is (thrown? Exception
                   (run-compute-service-description {:headers {}
                                                     :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                     :service-description-defaults {"health-check-url" "/ping"
                                                                                    "permitted-user" "bob"}
                                                     :service-description-template {"allowed-params" #{"HOME" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                    "cmd" "token-cmd"
                                                                                    "cpus" 1
                                                                                    "mem" 200
                                                                                    "run-as-user" "test-user"
                                                                                    "version" "a1b2c3"}}))))

    (testing "invalid allowed params - bad naming"
      (is (thrown? Exception
                   (run-compute-service-description {:headers {}
                                                     :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                     :service-description-defaults {"health-check-url" "/ping"
                                                                                    "permitted-user" "bob"}
                                                     :service-description-template {"allowed-params" #{"VAR.1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                    "cmd" "token-cmd"
                                                                                    "cpus" 1
                                                                                    "mem" 200
                                                                                    "run-as-user" "test-user"
                                                                                    "version" "a1b2c3"}}))))

    (testing "invalid allowed params - reserved and bad naming"
      (is (thrown? Exception
                   (run-compute-service-description {:headers {}
                                                     :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                     :service-description-defaults {"health-check-url" "/ping"
                                                                                    "permitted-user" "bob"}
                                                     :service-description-template {"allowed-params" #{"USER" "VAR.1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                    "cmd" "token-cmd"
                                                                                    "cpus" 1
                                                                                    "mem" 200
                                                                                    "run-as-user" "test-user"
                                                                                    "version" "a1b2c3"}}))))

    (testing "token + disallowed param header - on-the-fly"
      (is (thrown? Exception
                   (run-compute-service-description {:headers {"param" {"VAR_1" "VALUE-1"
                                                                        "ANOTHER_VAR_2" "VALUE-2"}
                                                               "version" "on-the-fly-version"}
                                                     :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                     :service-description-defaults {"health-check-url" "/ping"
                                                                                    "permitted-user" "bob"}
                                                     :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                    "cmd" "token-cmd"
                                                                                    "concurrency-level" 5
                                                                                    "run-as-user" "test-user"}}))))

    (testing "token + no allowed params - on-the-fly"
      (is (thrown? Exception
                   (run-compute-service-description {:headers {"param" {"VAR_1" "VALUE-1"
                                                                        "VAR_2" "VALUE-2"}
                                                               "version" "on-the-fly-version"}
                                                     :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                     :service-description-defaults {"health-check-url" "/ping"
                                                                                    "permitted-user" "bob"}
                                                     :service-description-template {"allowed-params" #{}
                                                                                    "cmd" "token-cmd"
                                                                                    "concurrency-level" 5
                                                                                    "run-as-user" "test-user"}}))))

    (testing "instance-expiry-mins"
      (let [core-service-description {"cmd" "cmd for missing run-as-user"
                                      "cpus" 1
                                      "mem" 200
                                      "run-as-user" test-user
                                      "version" "a1b2c3"}
            run-compute-service-description-helper (fn [service-description]
                                                     (run-compute-service-description {:profile->defaults {"webapp" {"concurrency-level" 120}}
                                                                                       :service-description-defaults {"health-check-url" "/healthy"}
                                                                                       :service-description-template service-description}))]
        (is (thrown? Exception (run-compute-service-description-helper (assoc core-service-description "instance-expiry-mins" -1))))
        (is (run-compute-service-description-helper (assoc core-service-description "instance-expiry-mins" 0)))
        (is (run-compute-service-description-helper (assoc core-service-description "instance-expiry-mins" 1)))))))

(deftest test-compute-service-description-service-preauthorized-and-authentication-disabled
  (letfn [(execute-test [service-description-template header-parameters]
            (let [{:keys [service-authentication-disabled service-preauthorized]}
                  (compute-service-description-helper {:headers header-parameters
                                                       :profile->defaults {"webapp" {"concurrency-level" 120}}
                                                       :service-description-defaults {}
                                                       :service-description-template service-description-template
                                                       :token-authentication-disabled (token-authentication-disabled? service-description-template)
                                                       :token-preauthorized (token-preauthorized? service-description-template)})]
              {:service-authentication-disabled service-authentication-disabled :service-preauthorized service-preauthorized}))]

    (testing "not-preauthorized-service-1"
      (is (= {:service-authentication-disabled false :service-preauthorized false}
             (execute-test {"cmd" "tc" "cpus" 1 "mem" 200 "permitted-user" "tu2" "run-as-user" "*" "version" "a1b2c3"}
                           {}))))

    (testing "not-preauthorized-service-2"
      (is (= {:service-authentication-disabled false :service-preauthorized false}
             (execute-test {"authentication" "disabled" "cmd" "tc" "cpus" 1 "mem" 200 "permitted-user" "tu2" "run-as-user" "*" "version" "a1b2c3"}
                           {}))))

    (testing "preauthorized-service"
      (is (= {:service-authentication-disabled false :service-preauthorized true}
             (execute-test {"cmd" "tc" "cpus" 1 "mem" 200 "permitted-user" "tu2" "run-as-user" "tu1" "version" "a1b2c3"}
                           {}))))

    (testing "not-preauthorized-service-due-to-headers"
      (is (= {:service-authentication-disabled false :service-preauthorized false}
             (execute-test {"cmd" "tc" "cpus" 1 "mem" 200 "permitted-user" "tu2" "run-as-user" "tu1" "version" "a1b2c3"}
                           {"cpus" 10}))))

    (testing "partial-preauthorized-service"
      (is (= {:service-authentication-disabled false :service-preauthorized true}
             (execute-test {"authentication" "disabled" "cmd" "tc" "cpus" 1 "permitted-user" "*" "run-as-user" "tu1" "version" "a1b2c3"}
                           {}))))

    (testing "authentication-disabled-service"
      (is (= {:service-authentication-disabled true :service-preauthorized true}
             (execute-test {"authentication" "disabled" "cmd" "tc" "cpus" 1 "mem" 200 "permitted-user" "*" "run-as-user" "tu1" "version" "a1b2c3"}
                           {}))))

    (testing "not-authentication-disabled-service-due-to-headers"
      (is (= {:service-authentication-disabled false :service-preauthorized false}
             (execute-test {"authentication" "disabled" "cmd" "tc" "cpus" 1 "mem" 200 "permitted-user" "*" "run-as-user" "tu1" "version" "a1b2c3"}
                           {"cmd" "tc2"}))))

    (testing "preauthorized-service:param-headers+token-preauth"
      (is (= {:service-authentication-disabled false :service-preauthorized true}
             (execute-test {"allowed-params" #{"BAR" "FOO"} "permitted-user" "puser" "run-as-user" "ruser" "version" "token"}
                           {"param" {"BAR" "bar-value" "FOO" "foo-value"}}))))

    (testing "not-preauthorized-service:param-headers+on-the-fly-cpus"
      (is (= {:service-authentication-disabled false :service-preauthorized false}
             (execute-test {"allowed-params" #{"BAR" "FOO"} "permitted-user" "puser" "run-as-user" "ruser" "version" "token"}
                           {"cpus" "20" "param" {"BAR" "bar-value" "FOO" "foo-value"}}))))

    (testing "not-preauthorized-service:param-headers+token-not-preauth"
      (is (= {:service-authentication-disabled false :service-preauthorized false}
             (execute-test {"allowed-params" #{"BAR" "FOO"} "cmd" "token-user" "version" "token"}
                           {"param" {"BAR" "bar-value" "FOO" "foo-value"}}))))))

(deftest test-service-id-and-token-storing
  (with-redefs [service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd service-description-keys))))]
    (let [kv-store (kv/->LocalKeyValueStore (atom {}))
          service-id-prefix "test#"
          token "test-token"
          in-service-description {"cmd" "tc", "cpus" 1, "mem" 200, "version" "a1b2c3", "token" token,
                                  "run-as-user" "tu1", "permitted-user" "tu2", "owner" "tu3"}
          service-id (service-description->service-id service-id-prefix in-service-description)]
      ; prepare
      (kv/store kv-store token in-service-description)

      ; test
      (testing "retrieve-invalid-token"
        (is (= {} (token->service-parameter-template kv-store "invalid-token" :error-on-missing false)))
        (is (thrown? ExceptionInfo (token->service-parameter-template kv-store "invalid-token")))
        (is (nil? (kv/fetch kv-store service-id))))

      (testing "error-on-token-invalid-format"
        (with-redefs [kv/fetch (fn [in-kv-store in-token]
                                 (is (= kv-store in-kv-store))
                                 (is (= "invalid-format/token" in-token))
                                 (kv/validate-zk-key in-token))]
          (is (empty? (token->service-parameter-template kv-store "invalid-format/token" :error-on-missing false)))
          (is (thrown-with-msg? ExceptionInfo #"Token not found: invalid-format/token" (token->service-parameter-template kv-store "invalid-format/token")))))

      (testing "test:token->service-description-2"
        (let [{:keys [service-parameter-template token-metadata]} (token->token-description kv-store token)
              service-description-template-2 (token->service-parameter-template kv-store token)]
          (is (= service-parameter-template service-description-template-2))
          (is (= (select-keys in-service-description service-description-keys) service-parameter-template))
          (is (= (-> in-service-description
                   (assoc "previous" {})
                   (select-keys token-metadata-keys))
                 token-metadata))))

      (testing "test:deleted:token->service-description-2"
        (kv/store kv-store token (assoc in-service-description "deleted" true))
        (let [{:keys [service-parameter-template token-metadata]} (token->token-description kv-store token)
              service-description-template-2 (token->service-parameter-template kv-store token)]
          (is (empty? service-description-template-2))
          (is (empty? service-parameter-template))
          (is (empty? token-metadata)))
        (let [{:keys [service-parameter-template token-metadata]} (token->token-description kv-store token :include-deleted true)
              service-description-template-2 (token->service-parameter-template kv-store token)]
          (is (empty? service-description-template-2))
          (is (= (select-keys in-service-description service-description-keys) service-parameter-template))
          (is (= {"deleted" true, "owner" "tu3", "previous" {}} token-metadata)))))))

(deftest test-fetch-core
  (let [service-id "test-service-1"
        service-key (str "^SERVICE-ID#" service-id)]

    (testing "no data available"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))]
        (is (nil? (kv/fetch kv-store service-key)))
        (is (nil? (fetch-core kv-store service-id :refresh false)))
        (is (nil? (kv/fetch kv-store service-key)))))

    (testing "data without refresh"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            service-description {"cmd" "tc" "cpus" 1 "mem" 200 "version" "a1b2c3"}]
        (is (nil? (kv/fetch kv-store service-key)))
        (kv/store kv-store service-key service-description)
        (is (= service-description (fetch-core kv-store service-id :refresh false)))
        (is (= service-description (kv/fetch kv-store service-key)))))

    (testing "cached empty data"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            cache (cu/cache-factory {:threshold 10})
            cache-kv-store (kv/->CachedKeyValueStore kv-store cache)
            service-id "test-service-1"
            service-description {"cmd" "tc" "cpus" 1 "mem" 200 "version" "a1b2c3"}]
        (is (nil? (fetch-core kv-store service-id :refresh false)))
        (is (nil? (fetch-core cache-kv-store service-id :refresh false)))
        (kv/store kv-store service-key service-description)
        (is (nil? (kv/fetch cache-kv-store (str "^SERVICE-ID#" service-id))))
        (is (nil? (fetch-core cache-kv-store service-id :refresh false)))
        (is (nil? (kv/fetch cache-kv-store (str "^SERVICE-ID#" service-id))))
        (is (= service-description (fetch-core cache-kv-store service-id :refresh true)))
        (is (= service-description (kv/fetch cache-kv-store (str "^SERVICE-ID#" service-id))))))))

(deftest test-refresh-service-descriptions
  (let [raw-kv-store (kv/->LocalKeyValueStore (atom {}))
        cache (cu/cache-factory {:threshold 10})
        cache-kv-store (kv/->CachedKeyValueStore raw-kv-store cache)
        service-id->key (fn [service-id] (str "^SERVICE-ID#" service-id))
        service-id->service-description (fn [service-id] {"cmd" "tc" "cpus" 1 "mem" 200 "version" service-id})
        service-id-1 "service-id-1"
        service-id-2 "service-id-2"
        service-id-3 "service-id-3"
        service-id-4 "service-id-4"]
    (is (nil? (kv/fetch cache-kv-store (service-id->key service-id-1))))
    (is (nil? (kv/fetch cache-kv-store (service-id->key service-id-3))))

    (kv/store raw-kv-store (service-id->key service-id-1) (service-id->service-description service-id-1))
    (kv/store raw-kv-store (service-id->key service-id-2) (service-id->service-description service-id-2))
    (kv/store raw-kv-store (service-id->key service-id-3) (service-id->service-description service-id-3))
    (kv/store raw-kv-store (service-id->key service-id-4) (service-id->service-description service-id-4))

    (is (nil? (kv/fetch cache-kv-store (service-id->key service-id-1))))
    (is (= (service-id->service-description service-id-2) (kv/fetch cache-kv-store (service-id->key service-id-2))))
    (is (nil? (kv/fetch cache-kv-store (service-id->key service-id-3))))
    (is (= (service-id->service-description service-id-4) (kv/fetch cache-kv-store (service-id->key service-id-4))))

    (let [service-ids #{service-id-1 service-id-2 service-id-3 service-id-4}
          service-ids-in (conj service-ids "service-id-unknown1" "service-id-unknown2")
          service-ids-out (refresh-service-descriptions cache-kv-store service-ids-in)]
      (is (= service-ids service-ids-out)))

    (is (= (service-id->service-description service-id-1) (kv/fetch cache-kv-store (service-id->key service-id-1))))
    (is (= (service-id->service-description service-id-2) (kv/fetch cache-kv-store (service-id->key service-id-2))))
    (is (= (service-id->service-description service-id-3) (kv/fetch cache-kv-store (service-id->key service-id-3))))
    (is (= (service-id->service-description service-id-4) (kv/fetch cache-kv-store (service-id->key service-id-4))))))

(deftest test-service-id->service-description
  (let [service-id "test-service-1"
        service-key (str "^SERVICE-ID#" service-id)
        fetch-service-description (fn [kv-store & {:keys [effective? profile->defaults service-description-defaults]
                                                   :or {effective? false
                                                        profile->defaults {"webapp" {"concurrency-level" 120}}
                                                        service-description-defaults {}}}]
                                    (let [context {:kv-store kv-store
                                                   :profile->defaults profile->defaults
                                                   :service-description-defaults service-description-defaults}
                                          service-description-builder (create-default-service-description-builder context)]
                                      (service-id->service-description
                                        service-description-builder kv-store service-id :effective? effective?)))]

    (testing "no data available"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))]
        (is (nil? (kv/fetch kv-store service-key)))
        (is (nil? (fetch-service-description kv-store)))
        (is (nil? (kv/fetch kv-store service-key)))))

    (testing "data without refresh"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            service-description {"cmd" "tc" "cpus" 1 "mem" 200 "version" "a1b2c3"}]
        (is (nil? (kv/fetch kv-store service-key)))
        (kv/store kv-store service-key service-description)
        (is (= service-description (fetch-service-description kv-store)))
        (is (= service-description (kv/fetch kv-store service-key)))))

    (testing "data with profile"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            service-description {"cmd" "tc" "cpus" 1 "mem" 200 "profile" "webapp" "version" "a1b2c3"}
            profile->defaults {"webapp" {"concurrency-level" 120}}
            service-description-defaults {"metric-group" "webapp"}]
        (is (nil? (kv/fetch kv-store service-key)))
        (kv/store kv-store service-key service-description)
        (is (= service-description
               (fetch-service-description
                 kv-store
                 :profile->defaults profile->defaults
                 :service-description-defaults service-description-defaults)))
        (is (= (merge service-description
                      service-description-defaults
                      (get profile->defaults "webapp"))
               (fetch-service-description
                 kv-store
                 :effective? true
                 :profile->defaults profile->defaults
                 :service-description-defaults service-description-defaults)))
        (is (= service-description (kv/fetch kv-store service-key)))))

    (testing "cached empty data"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            cache (cu/cache-factory {:threshold 10})
            cache-kv-store (kv/->CachedKeyValueStore kv-store cache)
            service-id "test-service-1"
            service-description {"cmd" "tc" "cpus" 1 "mem" 200 "version" "a1b2c3"}]
        (is (nil? (fetch-service-description cache-kv-store)))
        (kv/store kv-store service-key service-description)
        (is (nil? (kv/fetch cache-kv-store (str "^SERVICE-ID#" service-id))))
        (is (nil? (fetch-service-description cache-kv-store)))
        (is (nil? (kv/fetch cache-kv-store (str "^SERVICE-ID#" service-id))))
        (is (= service-description (kv/fetch cache-kv-store (str "^SERVICE-ID#" service-id) :refresh true)))
        (is (= service-description (fetch-service-description cache-kv-store)))
        (is (= service-description (kv/fetch cache-kv-store (str "^SERVICE-ID#" service-id))))))))

(deftest test-service-suspend-resume
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id-1 "test-service-1"
        service-id-2 "test-service-2"
        username "test-user"
        service-description {"cmd" "tc", "cpus" 1, "mem" 200, "permitted-user" "tu2", "run-as-user" "tu1", "version" "a1b2c3"}
        service-description-1 (assoc service-description "run-as-user" username)
        service-description-2 (assoc service-description "run-as-user" (str username "2"))
        entitlement-manager (reify authz/EntitlementManager
                              (authorized? [_ subject _ {:keys [user]}]
                                (= subject user)))
        validate-description (constantly true)]
    (testing "test-service-suspend-resume"
      (store-core kv-store service-id-1 service-description-1 validate-description)
      (store-core kv-store service-id-2 service-description-2 validate-description)
      (is (can-manage-service? kv-store entitlement-manager service-id-1 username))
      (is (not (can-manage-service? kv-store entitlement-manager service-id-2 username)))
      (is (nil? (service-id->suspended-state kv-store service-id-1)))
      (is (nil? (service-id->suspended-state kv-store service-id-2)))
      (suspend-service kv-store service-id-1 username)
      (is (= {:suspended true, :last-updated-by username} (dissoc (service-id->suspended-state kv-store service-id-1) :time)))
      (is (nil? (service-id->suspended-state kv-store service-id-2)))
      (resume-service kv-store service-id-1 username)
      (is (= {:suspended false, :last-updated-by username} (dissoc (service-id->suspended-state kv-store service-id-1) :time)))
      (is (nil? (service-id->suspended-state kv-store service-id-2))))))

(deftest test-can-manage-service?
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id-1 "test-service-1"
        service-id-2 "test-service-2"
        service-id-3 "test-service-3"
        username-1 "tu1"
        username-2 "tu2"
        admin-username "admin"
        service-description-1 {"cmd" "tc", "cpus" 1, "mem" 200, "permitted-user" "tu2", "run-as-user" "tu1a", "version" "a1b2c3"}
        service-description-2 (assoc service-description-1 "run-as-user" username-1)
        service-description-3 (assoc service-description-1 "run-as-user" "tu2")
        entitlement-manager (reify authz/EntitlementManager
                              (authorized? [_ subject verb {:keys [user]}]
                                (and (= verb :manage) (or (str/includes? user subject) (= admin-username subject)))))
        validate-description (constantly true)]
    (testing "test-service-suspend-resume"
      (store-core kv-store service-id-1 service-description-1 validate-description)
      (store-core kv-store service-id-2 service-description-2 validate-description)
      (store-core kv-store service-id-3 service-description-3 validate-description)
      (is (can-manage-service? kv-store entitlement-manager service-id-1 username-1))
      (is (can-manage-service? kv-store entitlement-manager service-id-2 username-1))
      (is (not (can-manage-service? kv-store entitlement-manager service-id-3 username-1)))
      (is (not (can-manage-service? kv-store entitlement-manager service-id-1 username-2)))
      (is (not (can-manage-service? kv-store entitlement-manager service-id-2 username-2)))
      (is (can-manage-service? kv-store entitlement-manager service-id-3 username-2))
      (is (can-manage-service? kv-store entitlement-manager service-id-1 admin-username))
      (is (can-manage-service? kv-store entitlement-manager service-id-2 admin-username))
      (is (can-manage-service? kv-store entitlement-manager service-id-3 admin-username)))))

(deftest test-metadata-error-message
  (let [service-description {"cpus" 1, "mem" 1, "cmd" "exit 0", "version" "1", "run-as-user" "someone"}]
    (testing "metadata schema error"
      (try
        (validate-schema (assoc service-description "metadata" {"a" "b" "c" 1}) {s/Str s/Any} {} nil)
        (is false "Exception should have been thrown for invalid service description.")
        (catch ExceptionInfo ex
          (let [friendly-message (-> ex ex-data :friendly-error-message)]
            (is (str/includes? friendly-message "Metadata values must be strings.") friendly-message)
            (is (str/includes? friendly-message "did not have string values: c: 1.") friendly-message)))))
    (testing "too many metadata keys"
      (let [error-msg (generate-friendly-metadata-error-message
                        (s/check service-description-schema
                                 (assoc service-description "metadata"
                                                            (zipmap (take 200 (iterate #(str % "a") "a"))
                                                                    (take 200 (iterate #(str % "a") "a"))))))]
        (is (str/includes? error-msg "200") error-msg)))
    (testing "not a map"
      (let [error-msg (generate-friendly-metadata-error-message
                        (s/check service-description-schema
                                 (assoc service-description "metadata" 12)))]
        (is (str/includes? error-msg "Metadata must be a map") error-msg)))
    (testing "invalid keys"
      (let [error-msg (generate-friendly-metadata-error-message
                        (s/check service-description-schema
                                 (assoc service-description "metadata" {1 "a" 2 "b" "C" "c"})))]
        (is (str/includes? error-msg "Keys must be made up of lower-case letters, numbers, and hyphens") error-msg)
        (is (str/includes? error-msg "The following metadata keys are invalid: 1, 2, C") error-msg)
        (is (not (str/includes? error-msg "Metadata values must be strings.")) error-msg)))
    (testing "invalid keys and values"
      (let [error-msg (generate-friendly-metadata-error-message
                        (s/check service-description-schema
                                 (assoc service-description "metadata" {1 "a" "b" 2})))]
        (is (str/includes? error-msg "The following metadata keys are invalid: 1") error-msg)
        (is (str/includes? error-msg "did not have string values: b: 2.") error-msg)))))

(deftest test-environment-variable-schema
  (let [service-description {"cpus" 1, "mem" 1, "cmd" "exit 0", "version" "1", "run-as-user" "someone"}]
    (testing "environment variable schema error"
      (try
        (validate-schema (assoc service-description "env" {"abc" "def", "ABC" 1}) {s/Str s/Any} {} nil)
        (is false "Exception should have been thrown for invalid service description")
        (catch ExceptionInfo ex
          (let [friendly-message (-> ex ex-data :friendly-error-message)]
            (is (str/includes? friendly-message "values must be strings") friendly-message)
            (is (str/includes? friendly-message "did not have string values: ABC: 1.") friendly-message)))))

    (testing "too many environment variables"
      (let [error-msg (generate-friendly-environment-variable-error-message
                        (s/check service-description-schema
                                 (assoc service-description "env"
                                                            (zipmap (take 200 (iterate #(str % "A") "A"))
                                                                    (take 200 (iterate #(str % "a") "a"))))))]
        (is (str/includes? error-msg "200") error-msg)))

    (testing "not a map"
      (let [error-msg (generate-friendly-environment-variable-error-message
                        (s/check service-description-schema
                                 (assoc service-description "env" 12)))]
        (is (str/includes? error-msg "Environment variables must be a map") error-msg)))

    (testing "invalid keys"
      (let [error-msg (generate-friendly-environment-variable-error-message
                        (s/check service-description-schema
                                 (assoc service-description "env" {1 "a", 2 "b"})))]
        (is (str/includes? error-msg "The following environment variable keys are invalid: 1, 2") error-msg)
        (is (not (str/includes? error-msg "Environment variable values must be strings.")) error-msg)
        (is (not (str/includes? error-msg "cannot be assigned")) error-msg)))

    (testing "invalid keys and values"
      (let [error-msg (generate-friendly-environment-variable-error-message
                        (s/check service-description-schema
                                 (assoc service-description "env" {1 "a" "B" 2})))]
        (is (str/includes? error-msg "The following environment variable keys are invalid: 1") error-msg)
        (is (str/includes? error-msg "did not have string values: B: 2.") error-msg)))
    (testing "using reserved variables"
      (let [error-msg (generate-friendly-environment-variable-error-message
                        (s/check service-description-schema
                                 (assoc service-description "env" {"WAITER_USERNAME" "badwaiter"
                                                                   "MARATHON_HOST" "foo"
                                                                   "MESOS_TASK_ID" "bar"
                                                                   "PORT_USED" "123"
                                                                   "PORT0" "123"})))]
        (is (str/includes? error-msg "WAITER_USERNAME") error-msg)
        (is (str/includes? error-msg "MARATHON_HOST") error-msg)
        (is (str/includes? error-msg "MESOS_TASK_ID") error-msg)
        (is (not (str/includes? error-msg "PORT_USED")) error-msg)
        (is (str/includes? error-msg "PORT0") error-msg)
        (is (str/includes? error-msg "reserved") error-msg)
        (is (not (str/includes? error-msg "upper case")) error-msg)))))

(defmacro run-validate-schema-test
  [valid-description constraints-schema profile->defaults config error-message]
  `(let [error-message# ~error-message]
     (try
       (validate-schema ~valid-description ~constraints-schema ~profile->defaults ~config)
       (is false "Fail as exception was not thrown!")
       (catch ExceptionInfo ex#
         (let [exception-data# (ex-data ex#)
               actual-error-message# (:friendly-error-message exception-data#)]
           (is (str/includes? (str actual-error-message#) (str error-message#))))))))

(deftest test-validate-schema
  (let [valid-description {"cpus" 1
                           "mem" 1
                           "cmd" "default-cmd"
                           "version" "default-version"
                           "run-as-user" "default-run-as-user"}
        constraints-schema {(s/optional-key "cmd") (s/pred #(<= (count %) 100) (symbol "limit-100"))
                            s/Str s/Any}
        profile->defaults {"webapp" {"name" "webapp-name"}}
        config {:allow-missing-required-fields? false}]
    (is (nil? (validate-schema valid-description constraints-schema profile->defaults config)))

    (testing "testing empty cmd"
      (run-validate-schema-test
        (assoc valid-description "cmd" "")
        constraints-schema profile->defaults config "cmd must be a non-empty string"))

    (testing "testing long cmd"
      (run-validate-schema-test
        (assoc valid-description "cmd" (str/join "" (repeat 150 "c")))
        constraints-schema profile->defaults config "cmd must be at most 100 characters"))

    (testing "testing grace-period-secs"
      (doseq [grace-period-secs [9000 "5" -1]]
        (run-validate-schema-test
          (assoc valid-description "grace-period-secs" grace-period-secs)
          constraints-schema profile->defaults config "grace-period-secs must be an integer in the range [0, 3600].")))

    (testing "idle-timeout-mins"
      (is (nil? (validate-schema (assoc valid-description "idle-timeout-mins" 0)
                                 constraints-schema profile->defaults config)))
      (is (nil? (validate-schema (assoc valid-description "idle-timeout-mins" 100)
                                 constraints-schema profile->defaults config)))
      (run-validate-schema-test
        (assoc valid-description "idle-timeout-mins" 50000)
        constraints-schema profile->defaults config "idle-timeout-mins must be an integer in the range [0, 43200].")
      (run-validate-schema-test
        (assoc valid-description "idle-timeout-mins" -1)
        constraints-schema profile->defaults config "idle-timeout-mins must be an integer in the range [0, 43200]."))

    (testing "testing instance counts"
      (run-validate-schema-test
        (assoc valid-description "max-instances" 0)
        constraints-schema profile->defaults config "max-instances must be between 1 and 1000")
      (run-validate-schema-test
        (assoc valid-description "max-instances" 1001)
        constraints-schema profile->defaults config "max-instances must be between 1 and 1000")
      (run-validate-schema-test
        (assoc valid-description "min-instances" 0)
        constraints-schema profile->defaults config "min-instances must be between 1 and 1000")
      (run-validate-schema-test
        (assoc valid-description "min-instances" 5)
        constraints-schema profile->defaults config
        "min-instances (5) in the default namespace must be less than or equal to 4")
      (run-validate-schema-test
        (assoc valid-description "min-instances" 1001 "namespace" "test-user")
        constraints-schema profile->defaults config "min-instances must be between 1 and 1000")
      (run-validate-schema-test
        (assoc valid-description "max-instances" 2 "min-instances" 3)
        constraints-schema profile->defaults config
        "min-instances (3) must be less than or equal to max-instances (2)"))

    (testing "testing invalid health check port index"
      (run-validate-schema-test
        (assoc valid-description "health-check-port-index" 1 "ports" 1)
        constraints-schema profile->defaults config
        "The health check port index (1) must be smaller than ports (1)")
      (run-validate-schema-test
        (assoc valid-description "health-check-port-index" 5 "ports" 3)
        constraints-schema profile->defaults config
        "The health check port index (5) must be smaller than ports (3)"))

    (testing "testing invalid metric-group"
      (run-validate-schema-test
        (assoc valid-description "metric-group" (str/join "" (repeat 100 "m")))
        constraints-schema profile->defaults config "The metric-group must be be between 2 and 32 characters"))

    (testing "testing invalid profile"
      (run-validate-schema-test
        (assoc valid-description "profile" 1234)
        constraints-schema profile->defaults config
        "profile must be a non-empty string, supported profile(s) are webapp")
      (run-validate-schema-test
        (assoc valid-description "profile" "web-service")
        constraints-schema profile->defaults config
        "Unsupported profile: web-service, supported profile(s) are webapp")

      (let [config {:allow-missing-required-fields? false}
            profile->defaults (dissoc profile->defaults "webapp")]
        (run-validate-schema-test
          (assoc valid-description "profile" 1234)
          constraints-schema profile->defaults config
          "profile must be a non-empty string, there are no supported profiles")
        (run-validate-schema-test
          (assoc valid-description "profile" "web-service")
          constraints-schema profile->defaults config
          "Unsupported profile: web-service, there are no supported profiles"))

      (let [profile->defaults {"webapp" {"cpus" 1 "mem" 2048 "name" "webapp-name"}}
            service-description (dissoc valid-description "cpus" "mem")
            profile-description (assoc service-description "profile" "webapp")]

        (run-validate-schema-test
          service-description constraints-schema profile->defaults
          {:allow-missing-required-fields? false}
          "cpus must be a positive number")
        (run-validate-schema-test
          service-description constraints-schema profile->defaults
          {:allow-missing-required-fields? false}
          "mem must be a positive number")

        (is (nil? (validate-schema profile-description constraints-schema profile->defaults
                                   {:allow-missing-required-fields? true})))
        (is (nil? (validate-schema profile-description constraints-schema profile->defaults
                                   {:allow-missing-required-fields? false})))))

    (testing "testing termination-grace-period-secs"
      (doseq [termination-grace-period-secs [900 "5" -1]]
        (run-validate-schema-test
          (assoc valid-description "termination-grace-period-secs" termination-grace-period-secs)
          constraints-schema profile->defaults config "termination-grace-period-secs must be an integer in the range [0, 300].")))))

(deftest test-service-description-schema
  (testing "Service description schema"
    (testing "should validate user-provided metric groups"
      (let [validate #(s/validate service-description-schema {"cmd" "foo"
                                                              "version" "bar"
                                                              "run-as-user" "baz"
                                                              "mem" 128
                                                              "cpus" 0.1
                                                              "metric-group" %})]
        (validate "ab")
        (validate "ab1")
        (is (thrown? Exception (validate "")))
        (is (thrown? Exception (validate "a")))))))

(deftest test-metric-group-filter
  (testing "Metric group filtering"
    (let [mg-filter #(get (metric-group-filter % [[#".*" "mapped"]]) "metric-group")]

      (testing "should use provided metric group when specified"
        (is (= "provided" (mg-filter {"metric-group" "provided"}))))

      (testing "should use mapping when metric group not specified"
        (is (= "mapped" (mg-filter {"name" "foo"}))))

      (testing "should use 'other' when metric group not specified and name not mapped - 1"
        (is (= "other" (mg-filter {"cpus" 1 "mem" 1024}))))

      (testing "should use 'other' when metric group not specified and name not mapped - 2"
        (is (= "other" (mg-filter {})))))))

(deftest test-name->metric-group
  (testing "Conversion from service name to metric group"

    (testing "should pick first matching metric group"
      (is (= "bar" (name->metric-group [[#"foo" "bar"] [#"f.*" "baz"]] "foo")))
      (is (= "baz" (name->metric-group [[#"foo." "bar"] [#"f.*" "baz"]] "foo"))))

    (testing "should return nil if no matches"
      (is (nil? (name->metric-group [] "foo")))
      (is (nil? (name->metric-group [[#"bar" "baz"]] "foo"))))))

(deftest test-merge-defaults-into-service-description
  (testing "Merging defaults into service description"
    (let [profile->defaults {"webapp" {"concurrency-level" 120
                                       "fallback-period-secs" 100}}
          metric-group-mappings [[#"f.." "bar"]]]
      (testing "should incorporate profile"
        (is (= {"concurrency-level" 120
                "metric-group" "other"
                "name" "lorem"
                "profile" "webapp"}
               (merge-defaults {"name" "lorem", "profile" "webapp"} {}
                               profile->defaults metric-group-mappings))))
      (testing "should incorporate metric group mappings"
        (is (= {"metric-group" "bar"
                "name" "foo"}
               (merge-defaults {"name" "foo"} {}
                               profile->defaults metric-group-mappings))))
      (testing "should incorporate defaults, profile, and metric-group"
        (is (= {"concurrency-level" 120
                "metric-group" "bar"
                "name" "foo"
                "ports" 2
                "profile" "webapp"}
               (merge-defaults {"name" "foo", "profile" "webapp"} {"concurrency-level" 30, "ports" 2}
                               profile->defaults metric-group-mappings))))
      (testing "min-instances default missing but only max-instances provided"
        (is (= {"max-instances" 2
                "metric-group" "other"}
               (merge-defaults {"max-instances" 2} {}
                               profile->defaults metric-group-mappings))))
      (testing "min-instances should be updated when not provided"
        (is (= {"max-instances" 2
                "metric-group" "other"
                "min-instances" 2}
               (merge-defaults {"max-instances" 2} {"min-instances" 3}
                               profile->defaults metric-group-mappings))))
      (testing "min-instances should be adjusted when only max-instances provided in profile"
        (is (= {"max-instances" 2
                "metric-group" "other"
                "min-instances" 2
                "profile" "test-profile"}
               (let [profile->defaults (assoc profile->defaults
                                         "test-profile" {"max-instances" 2})]
                 (merge-defaults {"profile" "test-profile"} {"min-instances" 3}
                                 profile->defaults metric-group-mappings))))
        (is (= {"max-instances" 2
                "metric-group" "other"
                "min-instances" 1
                "profile" "test-profile"}
               (let [profile->defaults (assoc profile->defaults
                                         "test-profile" {"max-instances" 2})]
                 (merge-defaults {"profile" "test-profile"} {"min-instances" 1}
                                 profile->defaults metric-group-mappings)))))
      (testing "min-instances should not be updated when provided without max-instances"
        (is (= {"metric-group" "other"
                "min-instances" 4}
               (merge-defaults {"min-instances" 4} {"min-instances" 3}
                               profile->defaults metric-group-mappings))))
      (testing "min-instances should not be updated when provided with max-instances"
        (is (= {"max-instances" 2
                "metric-group" "other"
                "min-instances" 4}
               (merge-defaults {"max-instances" 2 "min-instances" 4} {"min-instances" 3}
                               profile->defaults metric-group-mappings)))))))

(deftest test-validate-cmd-type
  (testing "DefaultServiceDescriptionBuilder validation"
    (testing "should accept no cmd-type or shell cmd-type"
      (validate (create-default-service-description-builder {}) {} {})
      (validate (create-default-service-description-builder {}) {"cmd-type" "shell"} {})
      (is (thrown? Exception (validate (create-default-service-description-builder {}) {"cmd-type" ""} {})))
      (is (thrown-with-msg? Exception #"Command type invalid is not supported"
                            (validate (create-default-service-description-builder {}) {"cmd-type" "invalid"} {}))))))

(deftest test-consent-cookie-value
  (let [current-time (t/now)
        current-time-ms (.getMillis ^DateTime current-time)
        clock (constantly current-time)]
    (is (nil? (consent-cookie-value clock nil nil nil nil)))
    (is (= ["unsupported" current-time-ms] (consent-cookie-value clock "unsupported" nil nil nil)))
    (is (= ["service" current-time-ms] (consent-cookie-value clock "service" nil nil nil)))
    (is (= ["service" current-time-ms "service-id"] (consent-cookie-value clock "service" "service-id" nil nil)))
    (is (= ["token" current-time-ms] (consent-cookie-value clock "token" nil nil nil)))
    (is (= ["token" current-time-ms] (consent-cookie-value clock "token" nil nil {"owner" "user"})))
    (is (= ["token" current-time-ms] (consent-cookie-value clock "token" nil "token-id" {})))
    (is (= ["token" current-time-ms "token-id" "user"] (consent-cookie-value clock "token" nil "token-id" {"owner" "user"})))))

(deftest test-assoc-run-as-user-approved?
  (let [current-time (t/now)
        current-time-ms (.getMillis ^DateTime current-time)
        clock (constantly current-time)
        consent-expiry-days 10
        valid-timestamp-ms (->> (dec consent-expiry-days) (t/days) (t/in-millis) (- current-time-ms))
        invalid-timestamp-ms (->> (inc consent-expiry-days) (t/days) (t/in-millis) (- current-time-ms))
        test-fn (fn [service-id token service-description decoded-consent-cookie]
                  (assoc-run-as-user-approved? clock consent-expiry-days service-id token service-description decoded-consent-cookie))
        service-description {"owner" "user"}]
    (is (not (test-fn "service-id" {} nil ["service" invalid-timestamp-ms "service-id"])))
    (is (test-fn "service-id" {} nil ["service" valid-timestamp-ms "service-id"]))
    (is (test-fn "service-id" nil service-description ["service" valid-timestamp-ms "service-id"]))
    (is (not (test-fn "service-id" nil service-description ["service" valid-timestamp-ms])))
    (is (not (test-fn "service-id-2" nil service-description ["service" valid-timestamp-ms "service-id"])))
    (is (not (test-fn "service-id" nil {} ["token" invalid-timestamp-ms "token-id" "user"])))
    (is (not (test-fn "service-id" "token-id" {} ["token" invalid-timestamp-ms "token-id" "user"])))
    (is (test-fn "service-id" "token-id" service-description ["token" valid-timestamp-ms "token-id" "user"]))
    (is (not (test-fn "service-id" "token-id" service-description ["token" valid-timestamp-ms "token-id"])))
    (is (not (test-fn "service-id" "token-id" service-description ["token" valid-timestamp-ms])))
    (is (not (test-fn "service-id" "token-id-2" service-description ["token" valid-timestamp-ms "token-id" "user"])))))

(deftest test-required-keys-present?
  (is (not (required-keys-present? {})))
  (is (not (required-keys-present? {"mem" 1, "cmd" "default-cmd", "version" "default-version", "run-as-user" "default-run-as-user"})))
  (is (not (required-keys-present? {"cpus" 1, "cmd" "default-cmd", "version" "default-version", "run-as-user" "default-run-as-user"})))
  (is (not (required-keys-present? {"cpus" 1, "mem" 1, "version" "default-version", "run-as-user" "default-run-as-user"})))
  (is (not (required-keys-present? {"cpus" 1, "mem" 1, "cmd" "default-cmd", "run-as-user" "default-run-as-user"})))
  (is (not (required-keys-present? {"cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version"})))
  (is (required-keys-present? {"cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "run-as-user" "default-run-as-user"})))

(deftest test-token-preauthorized?
  (is (not (token-preauthorized? {})))
  (is (not (token-preauthorized? {"permitted-user" "*", "run-as-user" "*"})))
  (is (token-preauthorized? {"permitted-user" "*", "run-as-user" "ru"}))
  (is (token-preauthorized? {"cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "ru"}))
  (is (token-preauthorized? {"authentication" "standard", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "ru"}))
  (is (token-preauthorized? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "ru"}))
  (is (not (token-preauthorized? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "*"})))
  (is (not (token-preauthorized? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "*", "run-as-user" "*"})))
  (is (token-preauthorized? {"authentication" "disabled", "cpus" 1, "mem" 1, "version" "default-version", "permitted-user" "*", "run-as-user" "ru"}))
  (is (token-preauthorized? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "*", "run-as-user" "ru"})))

(deftest test-token-authentication-disabled?
  (is (not (token-authentication-disabled? {})))
  (is (not (token-authentication-disabled? {"permitted-user" "*", "run-as-user" "*"})))
  (is (not (token-authentication-disabled? {"permitted-user" "*", "run-as-user" "ru"})))
  (is (not (token-authentication-disabled? {"cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "ru"})))
  (is (not (token-authentication-disabled? {"authentication" "standard", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "ru"})))
  (is (not (token-authentication-disabled? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "ru"})))
  (is (not (token-authentication-disabled? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "pu", "run-as-user" "*"})))
  (is (not (token-authentication-disabled? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "*", "run-as-user" "*"})))
  (is (not (token-authentication-disabled? {"authentication" "disabled", "cpus" 1, "mem" 1, "version" "default-version", "permitted-user" "*", "run-as-user" "ru"})))
  (is (token-authentication-disabled? {"authentication" "disabled", "cpus" 1, "mem" 1, "cmd" "default-cmd", "version" "default-version", "permitted-user" "*", "run-as-user" "ru"})))

(deftest test-no-intersection-in-token-request-scope-and-service-description-and-metadata
  (is (empty? (set/intersection service-override-keys service-non-override-keys))
      "We found common elements in service-override-keys and service-non-override-keys!")
  (is (empty? (set/intersection service-parameter-keys service-metadata-keys))
      "We found common elements in service-description-without-metadata-keys and service-metadata-keys!")
  (is (empty? (set/intersection system-metadata-keys user-metadata-keys))
      "We found common elements in system-metadata-keys and user-metadata-keys!")
  (is (empty? (set/intersection service-description-keys token-metadata-keys))
      "We found common elements in service-description-keys and token-metadata-keys!"))

(deftest test-default-service-description-builder-validate
  (let [constraints {"cpus" {:max 100}
                     "mem" {:max (* 32 1024)}}
        builder (create-default-service-description-builder {:constraints constraints})
        basic-service-description {"cpus" 1, "mem" 1, "cmd" "foo", "version" "bar", "run-as-user" "*"}
        some-user-service-description (assoc basic-service-description "run-as-user" "some-user")
        validation-settings {:allow-missing-required-fields? false}]

    (testing "validate-service-description-within-limits"
      (is (nil? (validate builder basic-service-description validation-settings))))

    (testing "validate-service-description-within-limits-missing-cpus"
      (let [service-description (dissoc basic-service-description "cpus")]
        (try
          (validate builder service-description validation-settings)
          (is false)
          (catch ExceptionInfo ex
            (is (= {:friendly-error-message "cpus must be a positive number."
                    :issue {}
                    :status http-400-bad-request
                    :type :service-description-error}
                   (select-keys (ex-data ex) [:friendly-error-message :issue :status :type])))))))

    (testing "validate-service-description-cpus-outside-limits"
      (let [service-description (assoc basic-service-description "cpus" 200)]
        (try
          (validate builder service-description validation-settings)
          (is false)
          (catch ExceptionInfo ex
            (is (= {:friendly-error-message (str "The following fields exceed their allowed limits: "
                                                 "cpus is 200 but the max allowed is 100")
                    :status http-400-bad-request
                    :type :service-description-error}
                   (select-keys (ex-data ex) [:friendly-error-message :status :type])))))))

    (testing "validate-service-description-cpus-and-mem-outside-limits"
      (let [service-description (assoc basic-service-description "cpus" 200 "mem" (* 40 1024))]
        (try
          (validate builder service-description validation-settings)
          (is false)
          (catch ExceptionInfo ex
            (is (= {:friendly-error-message (str "The following fields exceed their allowed limits: "
                                                 "cpus is 200 but the max allowed is 100, "
                                                 "mem is 40960 but the max allowed is 32768")
                    :status http-400-bad-request
                    :type :service-description-error}
                   (select-keys (ex-data ex) [:friendly-error-message :status :type])))))))

    (testing "validate-service-description-namespace-run-as-requester"
      (is (nil? (validate builder basic-service-description validation-settings)))
      (is (nil? (validate builder
                          (assoc basic-service-description "namespace" "*")
                          validation-settings)))
      (is (thrown-with-msg?
            Exception #"Service namespace must either be omitted or match the run-as-user"
            (validate builder
                      (assoc basic-service-description
                        "namespace" "some-user")
                      validation-settings))))

    (testing "validate-service-description-namespace-some-user"
      (is (nil? (validate builder some-user-service-description validation-settings)))
      (is (nil? (validate builder
                          (assoc some-user-service-description "namespace" "some-user")
                          validation-settings)))
      (is (thrown-with-msg?
            Exception #"Service namespace must either be omitted or match the run-as-user"
            (validate builder
                      (assoc basic-service-description
                        "namespace" "some-other-user")
                      validation-settings))))))

(deftest test-retrieve-most-recently-modified-token
  (testing "all tokens have last-update-time"
    (let [token-data-1 {"cmd" "c-1-A" "last-update-time" 1100}
          token-data-2 {"cmd" "c-2-A" "last-update-time" 1200}
          token-data-3 {"cmd" "c-3-A" "last-update-time" 1000}
          token->token-data {"token-1" token-data-1 "token-2" token-data-2 "token-3" token-data-3}]
      (is (= "token-2" (retrieve-most-recently-modified-token token->token-data)))))

  (testing "some tokens have last-update-time"
    (let [token-data-1 {"cmd" "c-1-A" "last-update-time" 1100}
          token-data-2 {"cmd" "c-2-A"}
          token-data-3 {"cmd" "c-3-A" "last-update-time" 1000}
          token->token-data {"token-1" token-data-1 "token-2" token-data-2 "token-3" token-data-3}]
      (is (= "token-1" (retrieve-most-recently-modified-token token->token-data)))))

  (testing "tokens tied for last-update-time"
    (let [token-data-1 {"cmd" "c-1-A" "last-update-time" 1100}
          token-data-2 {"cmd" "c-2-A" "last-update-time" 1100}
          token-data-3 {"cmd" "c-3-A" "last-update-time" 1000}
          token->token-data {"token-1" token-data-1 "token-2" token-data-2 "token-3" token-data-3}]
      (is (= "token-2" (retrieve-most-recently-modified-token (into (sorted-map) token->token-data))))))

  (testing "no tokens have last-update-time"
    (let [token-data-1 {"cmd" "c-1-B"}
          token-data-2 {"cmd" "c-2-B"}
          token-data-3 {"cmd" "c-3-B"}
          token->token-data {"token-1" token-data-1 "token-2" token-data-2 "token-3" token-data-3}]
      (is (= "token-3" (retrieve-most-recently-modified-token (into (sorted-map) token->token-data)))))))

(deftest test-token->token-hash
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        token "test-token"
        token-data {"cpus" 1 "mem" 2048 "last-update-time" 1100 "owner" "test-user"}]
    (kv/store kv-store token token-data)
    (is (= (token-data->token-hash token-data) (token->token-hash kv-store token)))))

(deftest test-retrieve-token-update-epoch-time
  (with-redefs [token-data->token-hash #(str "v" (get % "last-update-time"))]
    (let [token "test-token"]
      (let [token-data {"last-update-time" 90
                        "previous" {"last-update-time" 80
                                    "previous" {"last-update-time" 70
                                                "previous" {"last-update-time" 60
                                                            "previous" {"last-update-time" 50
                                                                        "previous" {}}}}}}]
        (testing "no token data"
          (is (nil? (retrieve-token-update-epoch-time token nil "v10")))
          (is (nil? (retrieve-token-update-epoch-time token {} "v20"))))

        (testing "token is current"
          (is (nil? (retrieve-token-update-epoch-time token token-data "v90"))))

        (testing "matching token version found and next update time returned"
          (is (= 90 (retrieve-token-update-epoch-time token token-data "v80")))
          (is (= 80 (retrieve-token-update-epoch-time token token-data "v70")))
          (is (= 70 (retrieve-token-update-epoch-time token token-data "v60")))
          (is (= 60 (retrieve-token-update-epoch-time token token-data "v50"))))

        (testing "matching token version not found return last known update time"
          (is (= 50 (retrieve-token-update-epoch-time token token-data "v40")))
          (is (= 50 (retrieve-token-update-epoch-time token token-data "v30")))
          (is (= 50 (retrieve-token-update-epoch-time token token-data "v20")))))

      (let [token-data {"last-update-time" (-> 90 tc/from-long du/date-to-str)}]
        (testing "string last-update-time"
          (is (= 90 (retrieve-token-update-epoch-time token token-data "v10"))))))))

(deftest test-retrieve-token-stale-info
  (with-redefs [token-data->token-hash #(str "v" (get % "last-update-time"))]
    (let [token->token-parameters {"t1" {"last-update-time" 90
                                       "previous" {"last-update-time" 40
                                                   "previous" {}}}
                                 "t2" {"last-update-time" 80
                                       "previous" {"last-update-time" 70
                                                   "previous" {}}}
                                 "t3" {"last-update-time" 60
                                       "previous" {"last-update-time" 50
                                                   "previous" {}}}}
        token->token-hash #(str "v" (get-in token->token-parameters [% "last-update-time"]))
        run-retrieve-token-stale-info (partial retrieve-token-stale-info token->token-hash token->token-parameters)]

      (testing "no source tokens"
        (is (= {:stale? false} (run-retrieve-token-stale-info []))))

      (testing "source tokens active"
        (let [source-tokens [{:token "t1" :version "v90"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t2" :version "v80"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t2" :version "v80"} {:token "t1" :version "v90"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t1" :version "v90"} {:token "t2" :version "v80"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t3" :version "v60"} {:token "t2" :version "v80"} {:token "t1" :version "v90"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens)))))

      (testing "some source tokens stale"
        (let [source-tokens [{:token "t2" :version "v80"} {:token "t1" :version "v40"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t1" :version "v90"} {:token "t2" :version "v70"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t3" :version "v60"} {:token "t2" :version "v70"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t-unknown" :version "v40"} {:token "t3" :version "v56"} {:token "t2" :version "v80"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t3" :version "v60"} {:token "t2" :version "v80"} {:token "t1" :version "v40"}]]
          (is (= {:stale? false} (run-retrieve-token-stale-info source-tokens)))))

      (testing "all source tokens stale"
        (let [source-tokens [{:token "t1" :version "v40"}]]
          (is (= {:stale? true :update-epoch-time 90} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t2" :version "v70"}]]
          (is (= {:stale? true :update-epoch-time 80} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t-unknown" :version "v40"}]]
          (is (= {:stale? true :update-epoch-time nil} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t2" :version "v70"} {:token "t1" :version "v40"}]]
          (is (= {:stale? true :update-epoch-time 90} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t1" :version "v40"} {:token "t2" :version "v70"}]]
          (is (= {:stale? true :update-epoch-time 90} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t3" :version "v50"} {:token "t2" :version "v70"}]]
          (is (= {:stale? true :update-epoch-time 80} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t-unknown" :version "v40"} {:token "t3" :version "v50"} {:token "t2" :version "v70"}]]
          (is (= {:stale? true :update-epoch-time 80} (run-retrieve-token-stale-info source-tokens))))
        (let [source-tokens [{:token "t3" :version "v50"} {:token "t2" :version "v70"} {:token "t1" :version "v40"}]]
          (is (= {:stale? true :update-epoch-time 90} (run-retrieve-token-stale-info source-tokens))))))))

(deftest test-source-tokens->gc-time-secs
  (let [fallback-period-secs-0 600
        fallback-period-secs-1 700
        fallback-period-secs-2 800
        stale-timeout-mins-0 15
        stale-timeout-mins-1 25
        stale-timeout-mins-2 35
        token-defaults {"fallback-period-secs" fallback-period-secs-0
                        "stale-timeout-mins" stale-timeout-mins-0}
        attach-token-defaults-fn (partial merge token-defaults)
        token->token-parameters {"t1" {"name" "t1"}
                                 "t2" {"name" "t2"
                                       "fallback-period-secs" fallback-period-secs-1}
                                 "t3" {"name" "t3"
                                       "stale-timeout-mins" stale-timeout-mins-1}
                                 "t4" {"name" "t4"
                                       "fallback-period-secs" fallback-period-secs-2
                                       "stale-timeout-mins" stale-timeout-mins-2}}
        run-source-tokens->gc-time-secs (partial source-tokens->gc-time-secs token->token-parameters attach-token-defaults-fn)
        mins->secs #(-> % (t/minutes) (t/in-seconds))]

    (is (= (+ fallback-period-secs-0 (mins->secs stale-timeout-mins-0))
           (run-source-tokens->gc-time-secs [[{:token "t0"}]])))
    (is (= (+ fallback-period-secs-0 (mins->secs stale-timeout-mins-0))
           (run-source-tokens->gc-time-secs [[{:token "t1"}]])))
    (is (= (+ fallback-period-secs-1 (mins->secs stale-timeout-mins-0))
           (run-source-tokens->gc-time-secs [[{:token "t2"}]])))
    (is (= (+ fallback-period-secs-0 (mins->secs stale-timeout-mins-1))
           (run-source-tokens->gc-time-secs [[{:token "t3"}]])))
    (is (= (+ fallback-period-secs-2 (mins->secs stale-timeout-mins-2))
           (run-source-tokens->gc-time-secs [[{:token "t4"}]])))
    (is (= (+ fallback-period-secs-2 (mins->secs stale-timeout-mins-2))
           (run-source-tokens->gc-time-secs [[{:token "t0"}] [{:token "t1"}] [{:token "t3"}][{:token "t4"}]])))
    (is (= (+ fallback-period-secs-2 (mins->secs stale-timeout-mins-2))
           (run-source-tokens->gc-time-secs [[{:token "t4"} {:token "t0"}]])))
    (is (= (+ fallback-period-secs-1 (mins->secs stale-timeout-mins-2))
           (run-source-tokens->gc-time-secs [[{:token "t4"} {:token "t2"}]])))
    (is (= (+ fallback-period-secs-2 (mins->secs stale-timeout-mins-1))
           (run-source-tokens->gc-time-secs [[{:token "t4"} {:token "t3"}]])))
    (is (= (+ fallback-period-secs-1 (mins->secs stale-timeout-mins-1))
           (run-source-tokens->gc-time-secs [[{:token "t2"} {:token "t3"}]])))))

(deftest test-accumulate-stale-info
  (doseq [s [true false]]
    (is (= {:stale? s :update-epoch-time nil}
           (accumulate-stale-info {:stale? s :update-epoch-time nil} {:stale? false :update-epoch-time 200})))
    (is (= {:stale? s :update-epoch-time 100}
           (accumulate-stale-info {:stale? s :update-epoch-time 100} {:stale? false :update-epoch-time 200}))))
  (doseq [s [true false]]
    (is (= {:stale? true :update-epoch-time 200}
           (accumulate-stale-info {:stale? true :update-epoch-time 200} {:stale? s :update-epoch-time nil})))
    (is (= {:stale? true :update-epoch-time 200}
           (accumulate-stale-info {:stale? s :update-epoch-time nil} {:stale? true :update-epoch-time 200}))))
  (is (= {:stale? true :update-epoch-time 200}
         (accumulate-stale-info {:stale? true :update-epoch-time 200} {:stale? true :update-epoch-time 400}))))

(deftest test-references->stale-info
  (let [reference-type->stale-info-fn (constantly :stale-info)
        run-references->stale-info (partial references->stale-info reference-type->stale-info-fn)]
    (is (= {:stale? false :update-epoch-time nil}
           (run-references->stale-info [{}])))
    (is (= {:stale? false :update-epoch-time nil}
           (run-references->stale-info [{"type1" {:stale-info {:stale? false :update-epoch-time 100}}}])))
    (is (= {:stale? true :update-epoch-time 100}
           (run-references->stale-info [{"type1" {:stale-info {:stale? true :update-epoch-time 100}}}])))
    (is (= {:stale? false :update-epoch-time nil}
           (run-references->stale-info [{"type1" {:stale-info {:stale? false :update-epoch-time 100}}
                                         "type2" {:stale-info {:stale? false :update-epoch-time 200}}}])))
    (is (= {:stale? true :update-epoch-time 100}
           (run-references->stale-info [{"type1" {:stale-info {:stale? true :update-epoch-time 100}}
                                         "type2" {:stale-info {:stale? false :update-epoch-time 200}}}])))
    (is (= {:stale? true :update-epoch-time 300}
           (run-references->stale-info [{"type1" {:stale-info {:stale? true :update-epoch-time nil}}
                                         "type2" {:stale-info {:stale? false :update-epoch-time nil}}
                                         "type3" {:stale-info {:stale? true :update-epoch-time 300}}
                                         "type4" {:stale-info {:stale? true :update-epoch-time nil}}
                                         "type5" {:stale-info {:stale? true :update-epoch-time 500}}}])))
    (is (= {:stale? false :update-epoch-time nil}
           (run-references->stale-info [{}
                                        {"type1" {:stale-info {:stale? false :update-epoch-time 100}}}])))
    (is (= {:stale? false :update-epoch-time nil}
           (run-references->stale-info [{}
                                        {"type1" {:stale-info {:stale? false :update-epoch-time 100}}}
                                        {"type1" {:stale-info {:stale? true :update-epoch-time 100}}}])))
    (is (= {:stale? true :update-epoch-time 200}
           (run-references->stale-info [{"type1" {:stale-info {:stale? true :update-epoch-time 100}}}
                                        {"type2" {:stale-info {:stale? true :update-epoch-time 200}}}])))
    (is (= {:stale? true :update-epoch-time 400}
           (run-references->stale-info [{"type1" {:stale-info {:stale? true :update-epoch-time 400}}}
                                        {"type2" {:stale-info {:stale? true :update-epoch-time nil}}}
                                        {"type3" {:stale-info {:stale? true :update-epoch-time 300}}}
                                        {"type4" {:stale-info {:stale? true :update-epoch-time nil}}}])))))

(deftest test-service->gc-time
  (let [fallback-period-secs 150
        profile-fallback-period-secs 100
        idle-timeout-mins 25
        profile-idle-timeout-mins 20
        stale-timeout-mins 5
        profile-stale-timeout-mins 10
        token-defaults {"fallback-period-secs" fallback-period-secs
                        "stale-timeout-mins" stale-timeout-mins}
        profile->defaults {"webapp1" {"fallback-period-secs" profile-fallback-period-secs
                                      "idle-timeout-mins" profile-idle-timeout-mins
                                      "load-balancing" "random"
                                      "stale-timeout-mins" profile-stale-timeout-mins}
                           "webapp2" {"idle-timeout-mins" profile-idle-timeout-mins
                                      "load-balancing" "random"
                                      "stale-timeout-mins" profile-stale-timeout-mins}
                           "webapp3" {"fallback-period-secs" profile-fallback-period-secs
                                      "idle-timeout-mins" profile-idle-timeout-mins
                                      "load-balancing" "random"}}
        attach-token-defaults-fn (fn attach-token-defaults-fn [token-parameters]
                                   (attach-token-defaults token-parameters token-defaults profile->defaults))
        service-id "test-service-id-"
        service-id->service-description-fn (fn [in-service-id]
                                             (is (str/starts-with? in-service-id service-id))
                                             {"idle-timeout-mins" idle-timeout-mins})
        token->token-data-factory (fn [token->token-data]
                                    (let [token->token-data (fn [in-token] (get token->token-data in-token))
                                          token->token-hash (fn [in-token] (str in-token ".latest"))
                                          token->token-parameters token->token-data]
                                      {:reference-type->stale-info-fn {:token #(retrieve-token-stale-info token->token-hash token->token-parameters (:sources %))}
                                       :token->token-data token->token-data
                                       :token->token-hash token->token-hash
                                       :token->token-parameters token->token-parameters}))
        t1000 (tc/from-long 10000)
        t2000 (tc/from-long 20000)
        t3000 (tc/from-long 30000)
        t4000 (tc/from-long 40000)
        last-modified-time (tc/from-long 5000)]

    (testing "service created without token"
      (let [token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s0")))
                                        #{})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s0") last-modified-time))))
      (let [token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s0")))
                                        #{{}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s0") last-modified-time)))))

    (testing "service created with missing token parameters"
      (let [token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s1")))
                                        #{{:token {:sources [{:token "t-miss" :version "t-miss.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (-> last-modified-time
                 (t/plus (t/seconds fallback-period-secs))
                 (t/plus (t/minutes stale-timeout-mins)))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s1") last-modified-time)))))

    (testing "service with single token is active"
      (let [token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s1")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s1") last-modified-time)))))

    (testing "direct access service is active"
      (doseq [token->token-data
              [{"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}}
               {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000) "profile" "webapp1"}}
               {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000) "profile" "webapp2"}}
               {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000) "profile" "webapp3"}}]]
        (let [service-id->references-fn (fn [in-service-id]
                                          (is (= in-service-id (str service-id "s2")))
                                          #{{}})
              {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
          (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
                 (service->gc-time
                   service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                   attach-token-defaults-fn (str service-id "s2") last-modified-time))))))

    (testing "service with multiple tokens is active"
      (let [token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}
                               "t2" {"mem" 2048 "last-update-time" (tc/to-long t2000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s3")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"} {:token "t2" :version "t2.latest"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s3") last-modified-time)))))

    (testing "service outdated but fallback not configured"
      (doseq [{:keys [expected-fallback-period-secs expected-stale-timeout-mins token->token-data]}
              [{:expected-fallback-period-secs fallback-period-secs
                :expected-stale-timeout-mins stale-timeout-mins
                :token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}
                                    "t2" {"mem" 2048 "last-update-time" (tc/to-long t2000)}}}
               {:expected-fallback-period-secs profile-fallback-period-secs
                :expected-stale-timeout-mins profile-stale-timeout-mins
                :token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000) "profile" "webapp1"}
                                    "t2" {"mem" 2048 "last-update-time" (tc/to-long t2000)}}}
               {:expected-fallback-period-secs fallback-period-secs
                :expected-stale-timeout-mins profile-stale-timeout-mins
                :token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000) "profile" "webapp2"}
                                    "t2" {"mem" 2048 "last-update-time" (tc/to-long t2000)}}}
               {:expected-fallback-period-secs profile-fallback-period-secs
                :expected-stale-timeout-mins stale-timeout-mins
                :token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000) "profile" "webapp3"}
                                    "t2" {"mem" 2048 "last-update-time" (tc/to-long t2000)}}}]]
        (let [service-id->references-fn (fn [in-service-id]
                                          (is (= in-service-id (str service-id "s4")))
                                          #{{:token {:sources [{:token "t1" :version "t1.old"}]}}})
              {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
          (is (= (-> t1000
                   (t/plus (t/seconds expected-fallback-period-secs))
                   (t/plus (t/minutes expected-stale-timeout-mins)))
                 (service->gc-time
                   service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                   attach-token-defaults-fn (str service-id "s4") last-modified-time))))
        (let [service-id->references-fn (fn [in-service-id]
                                          (is (= in-service-id (str service-id "s5")))
                                          #{{:token {:sources [{:token "t2" :version "t2.old"}]}}})
              {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
          (is (= (-> t2000
                   (t/plus (t/seconds fallback-period-secs))
                   (t/plus (t/minutes stale-timeout-mins)))
                 (service->gc-time
                   service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                   attach-token-defaults-fn (str service-id "s5") last-modified-time))))))

    (testing "service outdated with tokens but direct access possible"
      (let [token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000)}
                               "t2" {"mem" 2048 "last-update-time" (tc/to-long t2000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s5")))
                                        #{{:token {:sources [{:token "t1" :version "t1.old"}]}}
                                          {}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s5") last-modified-time)))))

    (testing "service outdated and fallback configured on one token"
      (let [token->token-data {"t1" {"cpus" 1 "last-update-time" (tc/to-long t1000) "fallback-period-secs" 300}
                               "t2" {"mem" 2048 "last-update-time" (tc/to-long t2000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s6")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"} {:token "t2" :version "t2.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s6") last-modified-time)))))

    (testing "service outdated on some tokens and fallback and timeout configured on all tokens"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "last-update-time" (tc/to-long t2000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900 "last-update-time" (tc/to-long t3000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s7")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"}
                                                             {:token "t2" :version "t2.old"}
                                                             {:token "t3" :version "t3.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s7") last-modified-time))))
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "last-update-time" (tc/to-long t2000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900 "last-update-time" (tc/to-long t3000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s7")))
                                        #{{:token {:sources [{:token "t1" :version "t1.old"}
                                                             {:token "t2" :version "t2.old"}
                                                             {:token "t3" :version "t3.latest"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s7") last-modified-time)))))

    (testing "service outdated on every token and fallback and timeout configured on all tokens"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "last-update-time" (tc/to-long t4000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900 "last-update-time" (tc/to-long t3000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s8")))
                                        #{{:token {:sources [{:token "t1" :version "t1.old"}
                                                             {:token "t2" :version "t2.old"}
                                                             {:token "t3" :version "t3.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus t4000 (t/minutes (-> 900 t/seconds t/in-minutes (+ stale-timeout-mins))))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s8") last-modified-time)))))

    (testing "service outdated on every token and fallback disabled"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 0 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "last-update-time" (tc/to-long t2000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "last-update-time" (tc/to-long t3000)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s8")))
                                        #{{:token {:sources [{:token "t1" :version "t1.old"}
                                                             {:token "t3" :version "t3.old"}
                                                             {:token "t2" :version "t2.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus t3000 (t/minutes stale-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s8") last-modified-time)))))

    (testing "service using latest of one partial token among many"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "last-update-time" (tc/to-long t2000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900 "last-update-time" (tc/to-long t3000)}
                               "t4" {"fallback-period-secs" 1200 "stale-timeout-mins" (+ stale-timeout-mins 15)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s9")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"} {:token "t2" :version "t2.old"}]}}
                                          {:token {:sources [{:token "t3" :version "t3.old"} {:token "t4" :version "t4.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s9") last-modified-time)))))

    (testing "service using latest versions of multiple tokens"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "last-update-time" (tc/to-long t2000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900 "last-update-time" (tc/to-long t3000)}
                               "t4" {"fallback-period-secs" 1200 "stale-timeout-mins" (+ stale-timeout-mins 15)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s10")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"} {:token "t2" :version "t2.latest"}]}}
                                          {:token {:sources [{:token "t3" :version "t3.latest"} {:token "t4" :version "t4.latest"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s10") last-modified-time)))))

    (testing "service using latest of one set of token entries"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "last-update-time" (tc/to-long t2000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900 "last-update-time" (tc/to-long t3000)}
                               "t4" {"fallback-period-secs" 1200 "stale-timeout-mins" (+ stale-timeout-mins 15)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s11")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"} {:token "t2" :version "t2.latest"}]}}
                                          {:token {:sources [{:token "t3" :version "t3.old"} {:token "t4" :version "t4.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus last-modified-time (t/minutes idle-timeout-mins))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s11") last-modified-time)))))

    (testing "service outdated and fallback and timeout configured on multiple source tokens"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300 "last-update-time" (tc/to-long t1000)}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "last-update-time" (tc/to-long t2000) "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900 "last-update-time" (tc/to-long t3000)}
                               "t4" {"fallback-period-secs" 1200 "last-update-time" (tc/to-long t4000) "stale-timeout-mins" (+ stale-timeout-mins 15)}}
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s12")))
                                        #{{:token {:sources [{:token "t1" :version "t1.old"} {:token "t2" :version "t2.old"}]}}
                                          {:token {:sources [{:token "t3" :version "t3.old"} {:token "t4" :version "t4.old"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (= (t/plus
                 t4000
                 (t/minutes
                   (max (-> 900 t/seconds t/in-minutes (+ stale-timeout-mins))
                        (-> 1200 t/seconds t/in-minutes (+ stale-timeout-mins 15)))))
               (service->gc-time
                 service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                 attach-token-defaults-fn (str service-id "s12") last-modified-time)))))

    (testing "service with single token is stale but GC disabled"
      (let [idle-timeout-mins 0
            token->token-data {"t1" {"cpus" 1 "idle-timeout-mins" idle-timeout-mins "last-update-time" (tc/to-long t1000)}}
            service-id->service-description-fn (fn [in-service-id]
                                                 (is (str/starts-with? in-service-id service-id))
                                                 {"idle-timeout-mins" idle-timeout-mins})
            service-id->references-fn (fn [in-service-id]
                                        (is (= in-service-id (str service-id "s1")))
                                        #{{:token {:sources [{:token "t1" :version "t1.latest"}]}}})
            {:keys [token->token-data reference-type->stale-info-fn]} (token->token-data-factory token->token-data)]
        (is (nil?
              (service->gc-time
                service-id->service-description-fn service-id->references-fn token->token-data reference-type->stale-info-fn
                attach-token-defaults-fn (str service-id "s1") last-modified-time)))))))

(defn- synchronize-fn
  [lock f]
  (locking lock
    (f)))

(deftest test-store-source-tokens!
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id "test-service-id"
        token-data-1 {"cmd" "ls" "cpus" 1 "mem" 32}
        token-data-2 {"run-as-user" "ru1" "version" "foo"}
        token-data-3 {"mem" 64 "run-as-user2" "ru" "version" "foo"}
        source-tokens-1 [(source-tokens-entry "token-1" token-data-1)
                         (source-tokens-entry "token-2" token-data-2)]
        source-tokens-2 [(source-tokens-entry "token-1" token-data-1)
                         (source-tokens-entry "token-2" token-data-2)]
        source-tokens-3 [(source-tokens-entry "token-3" token-data-3)
                         (source-tokens-entry "token-2" token-data-2)]
        source-tokens-4 [(source-tokens-entry "token-2" token-data-2)
                         (source-tokens-entry "token-3" token-data-3)]]

    (store-source-tokens! synchronize-fn kv-store service-id source-tokens-1)
    (is (= #{source-tokens-1}
           (service-id->source-tokens-entries kv-store service-id)))

    (store-source-tokens! synchronize-fn kv-store service-id source-tokens-2)
    (is (= #{source-tokens-1}
           (service-id->source-tokens-entries kv-store service-id)))

    (store-source-tokens! synchronize-fn kv-store service-id source-tokens-3)
    (is (= #{source-tokens-1 source-tokens-3}
           (service-id->source-tokens-entries kv-store service-id)))

    (store-source-tokens! synchronize-fn kv-store service-id source-tokens-1)
    (is (= #{source-tokens-1 source-tokens-3}
           (service-id->source-tokens-entries kv-store service-id)))

    (store-source-tokens! synchronize-fn kv-store service-id source-tokens-4)
    (is (= #{source-tokens-1 source-tokens-3 source-tokens-4}
           (service-id->source-tokens-entries kv-store service-id)))))

(deftest test-store-reference!
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id "test-service-id"
        references-1 {:token {:sources [{:token "token-1" :version "v1"}]}}
        references-1-copy {:sources [{:token "token-1" :version "v1"}]
                           :type :token}
        references-2 {:token {:sources [{:token "token-1" :version "v1"}
                                        {:token "token-2" :version "v2"}]}}
        references-3 {:token {:sources [{:token "token-3" :version "v3"}
                                        {:token "token-2" :version "v2"}]}}
        references-3-copy {:token {:sources [{:token "token-3" :version "v3"}
                                             {:token "token-2" :version "v2"}]}}
        references-4 {:token {:sources [{:token "token-2" :version "v2"}
                                        {:token "token-3" :version "v3"}]}}]

    (store-reference! synchronize-fn kv-store service-id references-1)
    (is (contains? (service-id->references kv-store service-id) references-1))

    (store-reference! synchronize-fn kv-store service-id references-1-copy)
    (is (contains? (service-id->references kv-store service-id) references-1))

    (store-reference! synchronize-fn kv-store service-id references-1)
    (is (contains? (service-id->references kv-store service-id) references-1))

    (store-reference! synchronize-fn kv-store service-id references-2)
    (is (contains? (service-id->references kv-store service-id) references-1))
    (is (contains? (service-id->references kv-store service-id) references-2))

    (store-reference! synchronize-fn kv-store service-id references-3)
    (is (contains? (service-id->references kv-store service-id) references-1))
    (is (contains? (service-id->references kv-store service-id) references-2))
    (is (contains? (service-id->references kv-store service-id) references-3))

    (store-reference! synchronize-fn kv-store service-id references-1)
    (is (contains? (service-id->references kv-store service-id) references-1))
    (is (contains? (service-id->references kv-store service-id) references-2))
    (is (contains? (service-id->references kv-store service-id) references-3))

    (store-reference! synchronize-fn kv-store service-id references-3-copy)
    (is (contains? (service-id->references kv-store service-id) references-1))
    (is (contains? (service-id->references kv-store service-id) references-2))
    (is (contains? (service-id->references kv-store service-id) references-3))

    (store-reference! synchronize-fn kv-store service-id references-4)
    (is (contains? (service-id->references kv-store service-id) references-1))
    (is (contains? (service-id->references kv-store service-id) references-2))
    (is (contains? (service-id->references kv-store service-id) references-3))
    (is (contains? (service-id->references kv-store service-id) references-4))))

(deftest test-service-description-builder-state
  (is {} (state (create-default-service-description-builder {}))))

(deftest test-retrieve-most-recently-modified-token-update-time
  (let [descriptor {:sources {:token->token-data {}}}]
    (is (= 0 (retrieve-most-recently-modified-token-update-time descriptor))))
  (let [descriptor {:sources {:token->token-data {"t1" {}}}}]
    (is (= 0 (retrieve-most-recently-modified-token-update-time descriptor))))
  (let [descriptor {:sources {:token->token-data {"t1" {"last-update-time" 100}}}}]
    (is (= 100 (retrieve-most-recently-modified-token-update-time descriptor))))
  (let [descriptor {:sources {:token->token-data {"t1" {"last-update-time" 200}
                                                  "t2" {}}}}]
    (is (= 200 (retrieve-most-recently-modified-token-update-time descriptor))))
  (let [descriptor {:sources {:token->token-data {"t1" {"last-update-time" 200}
                                                  "t2" {"last-update-time" 150}}}}]
    (is (= 200 (retrieve-most-recently-modified-token-update-time descriptor))))
  (let [descriptor {:sources {:token->token-data {"t1" {"last-update-time" 200}
                                                  "t2" {"last-update-time" 150}
                                                  "t3" {"last-update-time" 250}}}}]
    (is (= 250 (retrieve-most-recently-modified-token-update-time descriptor)))))

(deftest test-run-as-requester?
  (is (false? (run-as-requester? {})))
  (is (false? (run-as-requester? {"run-as-user" "john.doe*"})))
  (is (false? (run-as-requester? {"run-as-user" "jane.doe*"})))
  (is (true? (run-as-requester? {"run-as-user" "*"})))
  (is (false? (run-as-requester? {"run-as-user" "john.doe*" "permitted-user" "*"})))
  (is (false? (run-as-requester? {"run-as-user" "jane.doe*" "permitted-user" "*"})))
  (is (true? (run-as-requester? {"run-as-user" "*" "permitted-user" "*"}))))

(deftest test-requires-parameters?
  (is (false? (requires-parameters? {"allowed-params" #{}})))
  (is (true? (requires-parameters? {"allowed-params" #{"LOREM"}})))
  (is (false? (requires-parameters? {"allowed-params" #{"LOREM"} "env" {"LOREM" "v1"}})))
  (is (false? (requires-parameters? {"allowed-params" #{"LOREM"} "env" {"LOREM" "v1" "IPSUM" "v2"}})))
  (is (true? (requires-parameters? {"allowed-params" #{"LOREM" "IPSUM"}})))
  (is (true? (requires-parameters? {"allowed-params" #{"LOREM" "IPSUM"} "env" {"LOREM" "v1"}})))
  (is (false? (requires-parameters? {"allowed-params" #{"LOREM" "IPSUM"} "env" {"LOREM" "v1" "IPSUM" "v2"}})))
  (is (false? (requires-parameters? {})))
  (is (false? (requires-parameters? {"env" {"LOREM" "v1"}})))
  (is (false? (requires-parameters? {"env" {"LOREM" "v1" "IPSUM" "v2"}})))
  (is (false? (requires-parameters? {"run-as-user" "john.doe*"})))
  (is (false? (requires-parameters? {"run-as-user" "jane.doe*"}))))