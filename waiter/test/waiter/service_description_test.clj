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
  (:require [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [waiter.authorization :as authz]
            [waiter.kv :as kv]
            [waiter.service-description :refer :all]
            [waiter.util.cache-utils :as cu])
  (:import (clojure.lang ExceptionInfo)
           (org.joda.time DateTime)))

(deftest test-validate-service-description-schema
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"})))
  (is (nil? (s/check service-description-schema {"cpus" 1.5
                                                 "mem" 1.5
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"})))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "name" "testname123"
                                                 "health-check-url" "http://www.example.com/test/status"
                                                 "permitted-user" "testuser2"
                                                 "disk" 1
                                                 "ports" 1})))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "name" "testname123"
                                                 "health-check-url" "http://www.example.com/test/status"
                                                 "permitted-user" "testuser2"
                                                 "disk" 1
                                                 "ports" 5})))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "name" "testname123"
                                                      "health-check-url" "http://www.example.com/test/status"
                                                      "permitted-user" "testuser2"
                                                      "disk" 1
                                                      "ports" 11}))))
  (is (not (nil? (s/check service-description-schema {"mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 0
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" -1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" "1"
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 0
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" -1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" "1"
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" ""
                                                      "version" "v123"
                                                      "run-as-user" "test-user"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1, "mem" 1, "cmd" "test command", "version" "v123"}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" ""}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "name" ""}))))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "name" "testName123"})))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "name" "test.name"})))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "name" "test.n&me"})))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "grace-period-secs" (t/in-seconds (t/minutes 75))}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "grace-period-secs" -1}))))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "grace-period-secs" 5})))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "cmd-type" "shell"
                                                 "run-as-user" "test-user"})))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "cmd-type" "shell"
                                                 "run-as-user" "test-user"})))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "cmd-type" ""
                                                      "run-as-user" "test-user"}))))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "metadata" {"a" "b", "c-e" "d"}})))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "metadata" {"a" "b", "c" {"d" "e"}}}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "metadata" {"a" "b", "c" 1}}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "metadata" {"a" "b", "1c" "e"}}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "metadata" (zipmap (take 400 (iterate #(str % "a") "a"))
                                                                         (take 400 (iterate #(str % "a") "a")))}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "concurrency-level" -1}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "concurrency-level" 20000000}))))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "concurrency-level" 5})))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "env" {"MY_VAR" "1", "MY_VAR_2" "2"}})))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "env" {"2MY_VAR" "1", "MY_OTHER_VAR" "2"}}))))
  (is (nil? (s/check service-description-schema {"cpus" 1
                                                 "mem" 1
                                                 "cmd" "test command"
                                                 "version" "v123"
                                                 "run-as-user" "test-user"
                                                 "env" {"MY_VAR" "1", "MY_other_VAR" "2"}})))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "env" {"MY_VAR" 1, "MY_other_VAR" "2"}}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "env" {(str/join (take 513 (repeat "A"))) "A"
                                                             "MY_other_VAR" "2"}}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "env" {"MY_VAR" (str/join (take 513 (repeat "A")))
                                                             "MY_other_VAR" "2"}}))))
  (is (not (nil? (s/check service-description-schema {"cpus" 1
                                                      "mem" 1
                                                      "cmd" "test command"
                                                      "version" "v123"
                                                      "run-as-user" "test-user"
                                                      "env" (zipmap (take 150 (iterate #(str % "A") "a"))
                                                                    (take 150 (iterate #(str % "A") "a")))})))))

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
                                (str/includes? token "run") (assoc "run-as-user" "ruser"))
                              {}))
        build-source-tokens (fn [& tokens]
                              (mapv (fn [token] (source-tokens-entry token (create-token-data token))) tokens))]
    (with-redefs [kv/fetch (fn [in-kv-store token]
                             (is (= kv-store in-kv-store))
                             (create-token-data token))]
      (let [service-description-defaults {"name" "default-name" "health-check-url" "/ping"}
            token-defaults {"fallback-period-secs" 300}
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name" "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 600
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"health-check-url" "/ping"
                                                "name" "default-name"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"health-check-url" "/ping"
                                                "name" "default-name"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"health-check-url" "/ping"
                                                "name" "default-name"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
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
                          :expected {:defaults {"name" "default-name"
                                                "health-check-url" "/ping"}
                                     :fallback-period-secs 300
                                     :headers {"cpus" "1"
                                               "env" {"1" "quux"
                                                      "FOO-BAR" "bar"}}
                                     :service-description-template {},
                                     :source-tokens []
                                     :token->token-data {},
                                     :token-authentication-disabled false,
                                     :token-preauthorized false,
                                     :token-sequence []}}
                         )]
        (doseq [{:keys [expected name passthrough-headers waiter-headers]} test-cases]
          (testing (str "Test " name)
            (let [actual (prepare-service-description-sources
                           {:passthrough-headers passthrough-headers
                            :waiter-headers waiter-headers}
                           kv-store waiter-hostnames service-description-defaults token-defaults)]
              (when (not= expected actual)
                (println name)
                (println "Expected: " (into (sorted-map) expected))
                (println "Actual:   " (into (sorted-map) actual)))
              (is (= expected actual)))))))))

(deftest test-prepare-service-description-sources-with-authentication-disabled
  (let [kv-store (Object.)
        waiter-hostname "waiter-hostname.app.example.com"
        test-token "test-token-name"
        service-description-defaults {"name" "default-name" "health-check-url" "/ping"}
        token-defaults {"fallback-period-secs" 300}]
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
                         kv-store waiter-hostname service-description-defaults token-defaults)
                expected {:defaults service-description-defaults
                          :fallback-period-secs 300
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
                         kv-store waiter-hostname service-description-defaults token-defaults)
                expected {:defaults service-description-defaults
                          :fallback-period-secs 300
                          :headers {}
                          :service-description-template (select-keys token-data service-parameter-keys)
                          :source-tokens [(source-tokens-entry test-token token-data)]
                          :token->token-data {test-token token-data}
                          :token-authentication-disabled false
                          :token-preauthorized true
                          :token-sequence [test-token]}]
            (is (= expected actual))))))))

(defn- compute-service-description-helper
  ([sources & {:keys [assoc-run-as-user-approved? kv-store waiter-headers]}]
   (with-redefs [metric-group-filter (fn [sd _] sd)
                 service-description-schema {s/Str s/Any}]
     (let [assoc-run-as-user-approved? (or assoc-run-as-user-approved? (constantly false))
           kv-store (or kv-store (kv/->LocalKeyValueStore (atom {})))
           waiter-headers (or waiter-headers {})]
       (compute-service-description sources waiter-headers {} kv-store "test-service-" "current-request-user"
                                    [] (create-default-service-description-builder {})
                                    assoc-run-as-user-approved?)))))

(defn- service-description
  ([sources & {:keys [assoc-run-as-user-approved? kv-store waiter-headers]}]
   (let [{:keys [service-description]}
         (compute-service-description-helper
           sources
           :assoc-run-as-user-approved? assoc-run-as-user-approved?
           :kv-store kv-store
           :waiter-headers waiter-headers)]
     service-description)))

(deftest test-compute-service-description-on-the-fly?
  (let [defaults {"health-check-url" "/ping", "permitted-user" "bob"}
        sources {:defaults defaults :service-description-template {"cmd" "token-cmd"}}
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
        source-tokens (Object.)
        sources {:defaults defaults
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
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :service-description-template {"cmd" "token-cmd"}}))))

    (testing "only token from host without permitted-user in defaults"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :service-description-template {"cmd" "token-cmd"}}))))

    (testing "only token from header without permitted-user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :service-description-template {"cmd" "token-cmd"}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "only token from header with permitted-user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-user"
              "run-as-user" "token-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "permitted-user" "token-user"
                                                                  "run-as-user" "token-user"}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "token and run-as-user from header with permitted-user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"run-as-user" "on-the-fly-ru"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "permitted-user" "token-user"
                                                                  "run-as-user" "token-user"}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"
                                                   "x-waiter-run-as-user" "on-the-fly-ru"}))))

    (testing "only token from host with defaults missing permitted user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :service-description-template {"cmd" "token-cmd"}}))))

    (testing "only token from header with defaults missing permitted user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :service-description-template {"cmd" "token-cmd"}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "only token from host with dummy header"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :service-description-template {"cmd" "token-cmd"}}
                                  :waiter-headers {"x-waiter-dummy" "value-does-not-matter"}))))

    (testing "only on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"}}))))

    (testing "token host with non-intersecting values"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"version" "on-the-fly-version"}
                                   :service-description-template {"cmd" "token-cmd"}}))))

    (testing "token header with non-intersecting values"
      (is (= {"cmd" "token-cmd"
              "concurrency-level" 5
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"version" "on-the-fly-version"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "concurrency-level" 5}}))))

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
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"param" {"VAR_1" "VALUE-1"
                                                      "VAR_2" "VALUE-2"}
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "run-as-user" "test-user"}}))))

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
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"param" {"VAR_3" "VALUE-3p"
                                                      "VAR_4" "VALUE-4p"}
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}))))

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
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"param" {"VAR_2" "VALUE-2p"
                                                      "VAR_3" "VALUE-3p"}
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}))))

    (testing "token + param header - token due to param"
      (is (= {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
              "cmd" "token-cmd"
              "concurrency-level" 5
              "env" {"VAR_1" "VALUE-1"
                     "VAR_2" "VALUE-2"}
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "test-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"param" {"VAR_1" "VALUE-1"
                                                      "VAR_2" "VALUE-2"}}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "run-as-user" "test-user"}}))))

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
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"param" {"VAR_3" "VALUE-3p"
                                                      "VAR_4" "VALUE-4p"}}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}))))

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
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"param" {"VAR_2" "VALUE-2p"
                                                      "VAR_3" "VALUE-3p"}}
                                   :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                  "cmd" "token-cmd"
                                                                  "concurrency-level" 5
                                                                  "env" {"VAR_1" "VALUE-1e"
                                                                         "VAR_2" "VALUE-2e"}
                                                                  "run-as-user" "test-user"}}))))

    (testing "token host with intersecting values"
      (is (= {"cmd" "on-the-fly-cmd"
              "concurrency-level" 6
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "concurrency-level" 6}
                                   :service-description-template {"cmd" "token-cmd"}}))))

    (testing "token header with intersecting values"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"cmd" "token-cmd"}}))))

    (testing "intersecting values with additional fields"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "name" "token-name"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "version" "on-the-fly-version"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "name" "token-name"}}))))

    (testing "permitted user from token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"permitted-user" "token-pu"}}))))

    (testing "permitted user from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "on-the-fly-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "permitted-user" "on-the-fly-pu"}}))))

    (testing "permitted user intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "on-the-fly-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "permitted-user" "on-the-fly-pu"}
                                   :service-description-template {"permitted-user" "token-pu"}}))))

    (testing "run as user and permitted user only in token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"run-as-user" "token-ru"
                                                                  "permitted-user" "token-pu"}}))))

    (testing "run as user and permitted user from token and no on-the-fly headers"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "token-ru"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "token-ru"
                                                                  "permitted-user" "token-pu"}}))))

    (testing "missing permitted user in token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "token-ru"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "token-ru"}}))))

    (testing "run as user from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "run-as-user" "on-the-fly-ru"}}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-run-as-user" "on-the-fly-ru"}))))

    (testing "run as user intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "run-as-user" "on-the-fly-ru"}
                                   :service-description-template {"run-as-user" "token-ru"}}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-run-as-user" "on-the-fly-ru"}))))

    (testing "run as user provided from on-the-fly header with hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "chris"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"run-as-user" "chris"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "alice"}}
                                  :waiter-headers {"x-waiter-run-as-user" "chris"}))))

    (testing "run as user star from on-the-fly header with hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"run-as-user" "*"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "alice"}}
                                  :waiter-headers {"x-waiter-run-as-user" "*"}))))

    (testing "run as user star from hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "run-as-user" "*"}}
                                  :waiter-headers {}))))

    (testing "run as user star from on-the-fly token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {}
                                   :service-description-template {"cmd" "token-cmd", "run-as-user" "*"}}
                                  :waiter-headers {"x-waiter-token" "on-the-fly-token"}))))

    (testing "run as user star from on-the-fly headers without permitted-user"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "run-as-user" "*"}
                                   :service-description-template {"run-as-user" "token-ru"}}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-run-as-user" "*"}))))

    (testing "run as user star from on-the-fly headers with permitted-user"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "alice"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "permitted-user" "alice"
                                             "run-as-user" "*"}
                                   :service-description-template {"run-as-user" "token-ru"}}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-permitted-user" "alice"
                                                   "x-waiter-run-as-user" "*"}))))

    (testing "run as user in headers with permitted-user * in tokens"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "header-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :headers {"run-as-user" "header-user"}
                                   :service-description-template {"run-as-user" "*"
                                                                  "permitted-user" "*"
                                                                  "cmd" "token-cmd"}}
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
          (is (= (-> basic-service-description
                     (assoc "health-check-url" "/ping" "permitted-user" "bob" "scale-factor" 0.3))
                 (service-description {:defaults {"health-check-url" "/ping"
                                                  "permitted-user" "bob"
                                                  "scale-factor" 1}
                                       :headers {"cmd" "on-the-fly-cmd"
                                                 "run-as-user" "on-the-fly-ru"}}
                                      :kv-store kv-store))))

        (testing "inactive"
          (clear-service-description-overrides
            kv-store
            basic-service-id
            "current-request-user")
          (is (= (-> basic-service-description
                     (assoc "health-check-url" "/ping" "permitted-user" "bob"))
                 (service-description {:defaults {"health-check-url" "/ping"
                                                  "permitted-user" "bob"}
                                       :headers {"cmd" "on-the-fly-cmd"
                                                 "run-as-user" "on-the-fly-ru"}}
                                      :kv-store kv-store))))))

    (testing "override token metadata from headers"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "metadata" {"e" "f"}}
             (service-description {:defaults {"health-check-url" "/ping"
                                              "permitted-user" "bob"}
                                   :headers {"metadata" {"e" "f"}}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "metadata" {"a" "b", "c" "d"}}}))))

    (testing "sanitize metadata"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"
              "metadata" {"abc" "DEF"}}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :service-description-template {"cmd" "token-cmd"
                                                                  "metadata" {"Abc" "DEF"}}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "metric group from token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "token-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :headers {"cmd" "on-the-fly-cmd"}
                                   :service-description-template {"metric-group" "token-mg"}}))))

    (testing "metric group from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "on-the-fly-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "metric-group" "on-the-fly-mg"}}))))

    (testing "metric group intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "on-the-fly-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :headers {"cmd" "on-the-fly-cmd"
                                             "metric-group" "on-the-fly-mg"}
                                   :service-description-template {"metric-group" "token-mg"}}))))

    (testing "auto-populate run-as-user"
      (is (= {"cmd" "some-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "permitted-user" "current-request-user"
              "metric-group" "token-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :service-description-template {"cmd" "some-cmd"
                                                                  "metric-group" "token-mg"}}
                                  :assoc-run-as-user-approved? (constantly true)))))

    (testing "disable instance-expiry"
      (is (= {"cmd" "some-cmd"
              "health-check-url" "/health"
              "instance-expiry-mins" 0}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :service-description-template {"cmd" "some-cmd"
                                                                  "instance-expiry-mins" 0}}))))))

(deftest test-compute-service-description-error-scenarios
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id-prefix "test-service-"
        test-user "test-header-user"]
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" "/ping"}
                                               :headers {}
                                               :service-description-template {"cmd" "test command"
                                                                              "cpus" "one"
                                                                              "mem" 200
                                                                              "version" "a1b2c3"
                                                                              "run-as-user" test-user}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (create-default-service-description-builder {})
                                              (constantly false))))
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" 1}
                                               :headers {}
                                               :service-description-template {"cmd" "test command"
                                                                              "cpus" 1
                                                                              "mem" 200
                                                                              "version" "a1b2c3"
                                                                              "run-as-user" test-user}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (create-default-service-description-builder {})
                                              (constantly false))))
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" 1}
                                               :headers {}
                                               :service-description-template {}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (create-default-service-description-builder {})
                                              (constantly false))))
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" "/health"}
                                               :service-description-template {"cmd" "cmd for missing run-as-user"
                                                                              "cpus" 1
                                                                              "mem" 200
                                                                              "version" "a1b2c3"}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (create-default-service-description-builder {})
                                              (constantly false))))

    (testing "invalid allowed params - reserved"
      (is (thrown? Exception
                   (compute-service-description {:defaults {"health-check-url" "/ping"
                                                            "permitted-user" "bob"}
                                                 :headers {}
                                                 :service-description-template {"allowed-params" #{"HOME" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                "cmd" "token-cmd"
                                                                                "cpus" 1
                                                                                "mem" 200
                                                                                "run-as-user" "test-user"
                                                                                "version" "a1b2c3"}}
                                                {} {} kv-store service-id-prefix test-user []
                                                (create-default-service-description-builder {})
                                                (constantly false)))))

    (testing "invalid allowed params - bad naming"
      (is (thrown? Exception
                   (compute-service-description {:defaults {"health-check-url" "/ping"
                                                            "permitted-user" "bob"}
                                                 :headers {}
                                                 :service-description-template {"allowed-params" #{"VAR.1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                "cmd" "token-cmd"
                                                                                "cpus" 1
                                                                                "mem" 200
                                                                                "run-as-user" "test-user"
                                                                                "version" "a1b2c3"}}
                                                {} {} kv-store service-id-prefix test-user []
                                                (create-default-service-description-builder {})
                                                (constantly false)))))

    (testing "invalid allowed params - reserved and bad naming"
      (is (thrown? Exception
                   (compute-service-description {:defaults {"health-check-url" "/ping"
                                                            "permitted-user" "bob"}
                                                 :headers {}
                                                 :service-description-template {"allowed-params" #{"USER" "VAR.1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                "cmd" "token-cmd"
                                                                                "cpus" 1
                                                                                "mem" 200
                                                                                "run-as-user" "test-user"
                                                                                "version" "a1b2c3"}}
                                                {} {} kv-store service-id-prefix test-user []
                                                (create-default-service-description-builder {})
                                                (constantly false)))))

    (testing "token + disallowed param header - on-the-fly"
      (is (thrown? Exception
                   (compute-service-description {:defaults {"health-check-url" "/ping"
                                                            "permitted-user" "bob"}
                                                 :headers {"param" {"VAR_1" "VALUE-1"
                                                                    "ANOTHER_VAR_2" "VALUE-2"}
                                                           "version" "on-the-fly-version"}
                                                 :service-description-template {"allowed-params" #{"VAR_1" "VAR_2" "VAR_3" "VAR_4" "VAR_5"}
                                                                                "cmd" "token-cmd"
                                                                                "concurrency-level" 5
                                                                                "run-as-user" "test-user"}}
                                                {} {} kv-store service-id-prefix test-user []
                                                (create-default-service-description-builder {})
                                                (constantly false)))))

    (testing "token + no allowed params - on-the-fly"
      (is (thrown? Exception
                   (compute-service-description {:defaults {"health-check-url" "/ping"
                                                            "permitted-user" "bob"}
                                                 :headers {"param" {"VAR_1" "VALUE-1"
                                                                    "VAR_2" "VALUE-2"}
                                                           "version" "on-the-fly-version"}
                                                 :service-description-template {"allowed-params" #{}
                                                                                "cmd" "token-cmd"
                                                                                "concurrency-level" 5
                                                                                "run-as-user" "test-user"}}
                                                {} {} kv-store service-id-prefix test-user []
                                                (create-default-service-description-builder {})
                                                (constantly false)))))

    (testing "instance-expiry-mins"
      (let [core-service-description {"cmd" "cmd for missing run-as-user"
                                      "cpus" 1
                                      "mem" 200
                                      "run-as-user" test-user
                                      "version" "a1b2c3"}
            run-compute-service-description (fn [service-description]
                                              (compute-service-description {:defaults {"health-check-url" "/health"}
                                                                            :service-description-template service-description}
                                                                           {} {} kv-store service-id-prefix test-user []
                                                                           (create-default-service-description-builder {})
                                                                           (constantly false)))]
        (is (thrown? Exception (run-compute-service-description (assoc core-service-description "instance-expiry-mins" -1))))
        (is (run-compute-service-description (assoc core-service-description "instance-expiry-mins" 0)))
        (is (run-compute-service-description (assoc core-service-description "instance-expiry-mins" 1)))))))

(deftest test-compute-service-description-service-preauthorized-and-authentication-disabled
  (letfn [(execute-test [service-description-template header-parameters]
            (let [{:keys [service-authentication-disabled service-preauthorized]}
                  (compute-service-description-helper {:headers header-parameters
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

    (refresh-service-descriptions cache-kv-store #{service-id-1 service-id-2 service-id-3 service-id-4})

    (is (= (service-id->service-description service-id-1) (kv/fetch cache-kv-store (service-id->key service-id-1))))
    (is (= (service-id->service-description service-id-2) (kv/fetch cache-kv-store (service-id->key service-id-2))))
    (is (= (service-id->service-description service-id-3) (kv/fetch cache-kv-store (service-id->key service-id-3))))
    (is (= (service-id->service-description service-id-4) (kv/fetch cache-kv-store (service-id->key service-id-4))))))

(deftest test-service-id->service-description
  (let [service-id "test-service-1"
        service-key (str "^SERVICE-ID#" service-id)
        fetch-service-description (fn [kv-store]
                                    (service-id->service-description kv-store service-id {} [] :effective? false))]

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
        (validate-schema (assoc service-description "metadata" {"a" "b" "c" 1}) {s/Str s/Any} nil)
        (is false "Exception should have been thrown for invalid service description.")
        (catch ExceptionInfo ex
          (let [friendly-message (get-in (ex-data ex) [:friendly-error-message :metadata])]
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
        (validate-schema (assoc service-description "env" {"abc" "def", "ABC" 1}) {s/Str s/Any} nil)
        (is false "Exception should have been thrown for invalid service description")
        (catch ExceptionInfo ex
          (let [friendly-message (get-in (ex-data ex) [:friendly-error-message :env])]
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
    (testing "should incorporate metric group mappings"
      (is (= {"name" "foo", "metric-group" "bar"}
             (merge-defaults {"name" "foo"} {} [[#"f.." "bar"]]))))))

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
    (is (= nil (consent-cookie-value clock nil nil nil nil)))
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
        validation-settings {:allow-missing-required-fields? false}]

    (testing "validate-service-description-within-limits"
      (is (nil? (validate builder basic-service-description validation-settings))))

    (testing "validate-service-description-within-limits-missing-cpus"
      (let [service-description (dissoc basic-service-description "cpus")]
        (try
          (validate builder service-description validation-settings)
          (is false)
          (catch ExceptionInfo ex
            (is (= {:issue {"cpus" 'missing-required-key}
                    :status 400
                    :type :service-description-error}
                   (select-keys (ex-data ex) [:issue :status :type])))))))

    (testing "validate-service-description-cpus-outside-limits"
      (let [service-description (assoc basic-service-description "cpus" 200)]
        (try
          (validate builder service-description validation-settings)
          (is false)
          (catch ExceptionInfo ex
            (is (= {:friendly-error-message (str "The following fields exceed their allowed limits: "
                                                 "cpus is 200 but the max allowed is 100")
                    :status 400
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
                    :status 400
                    :type :service-description-error}
                   (select-keys (ex-data ex) [:friendly-error-message :status :type])))))))))

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

(deftest test-service-id->idle-timeout
  (let [fallback-period-secs 150
        stale-timeout-mins 5
        token-defaults {"fallback-period-secs" fallback-period-secs
                        "stale-timeout-mins" stale-timeout-mins}
        idle-timeout-mins 25
        service-id "test-service-id"
        token->token-hash (fn [in-token] (str in-token ".hash1"))
        token->token-metadata-fn (fn [token->token-data]
                                   (fn token->token-metadata [in-token]
                                     (-> in-token
                                         token->token-data
                                         (select-keys token-metadata-keys))))]
    (testing "service with single token is active"
      (let [token->token-data {"t1" {"cpus" 1}}
            service-id->service-description-fn (fn [in-service-id]
                                                 (is (= service-id in-service-id))
                                                 {"idle-timeout-mins" idle-timeout-mins})
            service-id->source-token-entries-fn (fn [in-service-id]
                                                  (is (= service-id in-service-id))
                                                  #{[{"token" "t1" "version" "t1.hash1"}]})
            token->token-metadata (token->token-metadata-fn token->token-data)]
        (is (= idle-timeout-mins
               (service-id->idle-timeout
                 service-id->service-description-fn service-id->source-token-entries-fn token->token-hash
                 token->token-metadata token-defaults service-id)))))

    (testing "service with multiple tokens is active"
      (let [token->token-data {"t1" {"cpus" 1}
                               "t2" {"mem" 2048}}
            service-id->service-description-fn (fn [in-service-id]
                                                 (is (= service-id in-service-id))
                                                 {"idle-timeout-mins" idle-timeout-mins})
            service-id->source-token-entries-fn (fn [in-service-id]
                                                  (is (= service-id in-service-id))
                                                  #{[{"token" "t1" "version" "t1.hash1"} {"token" "t2" "version" "t2.hash1"}]})
            token->token-metadata (token->token-metadata-fn token->token-data)]
        (is (= idle-timeout-mins
               (service-id->idle-timeout
                 service-id->service-description-fn service-id->source-token-entries-fn token->token-hash
                 token->token-metadata token-defaults service-id)))))

    (testing "service outdated but fallback not configured"
      (let [token->token-data {"t1" {"cpus" 1}
                               "t2" {"mem" 2048}}
            service-id->service-description-fn (fn [in-service-id]
                                                 (is (= service-id in-service-id))
                                                 {"idle-timeout-mins" idle-timeout-mins})
            service-id->source-token-entries-fn (fn [in-service-id]
                                                  (is (= service-id in-service-id))
                                                  #{[{"token" "t1" "version" "t1.hash0"}]})
            token->token-metadata (token->token-metadata-fn token->token-data)]
        (is (= (-> (+ fallback-period-secs (dec (-> 1 t/minutes t/in-seconds)))
                   t/seconds
                   t/in-minutes
                   (+ stale-timeout-mins))
               (service-id->idle-timeout
                 service-id->service-description-fn service-id->source-token-entries-fn token->token-hash
                 token->token-metadata token-defaults service-id)))))

    (testing "service outdated and fallback configured on one token"
      (let [token->token-data {"t1" {"cpus" 1 "fallback-period-secs" 300}
                               "t2" {"mem" 2048}}
            service-id->service-description-fn (fn [in-service-id]
                                                 (is (= service-id in-service-id))
                                                 {"idle-timeout-mins" idle-timeout-mins})
            service-id->source-token-entries-fn (fn [in-service-id]
                                                  (is (= service-id in-service-id))
                                                  #{[{"token" "t1" "version" "t1.hash1"} {"token" "t2" "version" "t2.hash0"}]})
            token->token-metadata (token->token-metadata-fn token->token-data)]
        (is (= (-> 300 t/seconds t/in-minutes (+ stale-timeout-mins))
               (service-id->idle-timeout
                 service-id->service-description-fn service-id->source-token-entries-fn token->token-hash
                 token->token-metadata token-defaults service-id)))))

    (testing "service outdated and fallback and timeout configured on all tokens"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900}}
            service-id->service-description-fn (fn [in-service-id]
                                                 (is (= service-id in-service-id))
                                                 {"idle-timeout-mins" idle-timeout-mins})
            service-id->source-token-entries-fn (fn [in-service-id]
                                                  (is (= service-id in-service-id))
                                                  #{[{"token" "t1" "version" "t1.hash1"}
                                                     {"token" "t2" "version" "t2.hash0"}
                                                     {"token" "t3" "version" "t3.hash0"}]})
            token->token-metadata (token->token-metadata-fn token->token-data)]
        (is (= (-> 900 t/seconds t/in-minutes (+ stale-timeout-mins))
               (service-id->idle-timeout
                 service-id->service-description-fn service-id->source-token-entries-fn token->token-hash
                 token->token-metadata token-defaults service-id)))))

    (testing "service outdated and fallback and timeout configured on multiple source tokens"
      (let [stale-timeout-mins 45
            token->token-data {"t1" {"cpus" 123 "fallback-period-secs" 300}
                               "t2" {"cmd" "tc" "fallback-period-secs" 600 "stale-timeout-mins" stale-timeout-mins}
                               "t3" {"cmd" "tc" "fallback-period-secs" 900}
                               "t4" {"fallback-period-secs" 1200 "stale-timeout-mins" (+ stale-timeout-mins 15)}}
            service-id->service-description-fn (fn [in-service-id]
                                                 (is (= service-id in-service-id))
                                                 {"idle-timeout-mins" idle-timeout-mins})
            service-id->source-token-entries-fn (fn [in-service-id]
                                                  (is (= service-id in-service-id))
                                                  #{[{"token" "t1" "version" "t1.hash1"} {"token" "t2" "version" "t2.hash0"}]
                                                    [{"token" "t3" "version" "t3.hash0"} {"token" "t4" "version" "t4.hash0"}]})
            token->token-metadata (token->token-metadata-fn token->token-data)]
        (is (= (max (-> 900 t/seconds t/in-minutes (+ stale-timeout-mins))
                    (-> 1200 t/seconds t/in-minutes (+ stale-timeout-mins 15)))
               (service-id->idle-timeout
                 service-id->service-description-fn service-id->source-token-entries-fn token->token-hash
                 token->token-metadata token-defaults service-id)))))))

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
