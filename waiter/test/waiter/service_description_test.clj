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
(ns waiter.service-description-test
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [waiter.kv :as kv]
            [waiter.service-description :refer :all])
  (:import (clojure.lang ExceptionInfo)))

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
                                                 "ports" [8080]})))
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
              (retrieve-token-from-service-description-or-hostname service-desc request-headers waiter-hostname)]
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
        waiter-hostname "waiter-hostname.app.example.com"]
    (with-redefs [token->service-description-template (fn [_ token & _]
                                                        (if (and token (not (str/includes? token "no-token")))
                                                          (cond-> {"name" token, "cmd" token-user, "version" "token", "owner" "token-owner"}
                                                                  (str/includes? token "cpus") (assoc "cpus" "1")
                                                                  (str/includes? token "mem") (assoc "mem" "2")
                                                                  (str/includes? token "run") (assoc "run-as-user" "ruser")
                                                                  (str/includes? token "per") (assoc "permitted-user" "puser"))
                                                          {}))]
      (let [service-description-defaults {"name" "default-name" "health-check-url" "/ping"}
            test-cases (list
                         {:name "prepare-service-description-sources:WITH Service Desc specific Waiter Headers except run-as-user"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc",
                                           "x-waiter-cpus" 1, "x-waiter-mem" 1024,
                                           "x-waiter-cmd" "test-cmd",
                                           "x-waiter-version" "test-version",
                                           "x-waiter-run-as-user" test-user}
                          :passthrough-headers {"host" "test-host", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-host", "cmd" "token-user", "version" "token"},
                                     :headers {"cpus" 1,
                                               "mem" 1024,
                                               "cmd" "test-cmd",
                                               "version" "test-version",
                                               "run-as-user" "test-header-user"}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:WITH Waiter Hostname"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc",
                                           "x-waiter-cpus" 1, "x-waiter-mem" 1024,
                                           "x-waiter-cmd" "test-cmd",
                                           "x-waiter-version" "test-version",
                                           "x-waiter-run-as-user" test-user}
                          :passthrough-headers {"host" waiter-hostname, "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {},
                                     :headers {"cpus" 1,
                                               "mem" 1024,
                                               "cmd" "test-cmd",
                                               "version" "test-version",
                                               "run-as-user" "test-header-user"}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:WITH Service Desc specific Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc",
                                           "x-waiter-cpus" 1, "x-waiter-mem" 1024,
                                           "x-waiter-cmd" "test-cmd",
                                           "x-waiter-version" "test-version",
                                           "x-waiter-run-as-user" test-user}
                          :passthrough-headers {"host" "test-host", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-host", "cmd" "token-user", "version" "token"},
                                     :headers {"cpus" 1,
                                               "mem" 1024,
                                               "cmd" "test-cmd",
                                               "version" "test-version",
                                               "run-as-user" "test-header-user"}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:WITH Service Desc specific Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc",
                                           "x-waiter-cpus" 1, "x-waiter-mem" 1024,
                                           "x-waiter-cmd" "test-cmd",
                                           "x-waiter-version" "test-version",
                                           "x-waiter-run-as-user" test-user}
                          :passthrough-headers {"host" "test-host-no-token", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {},
                                     :headers {"cpus" 1,
                                               "mem" 1024,
                                               "cmd" "test-cmd",
                                               "version" "test-version",
                                               "run-as-user" "test-header-user"}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:WITHOUT Service Desc specific Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc"}
                          :passthrough-headers {"host" "test-host", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-host", "cmd" "token-user", "version" "token"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Token in Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc", "x-waiter-token" "test-token"}
                          :passthrough-headers {"host" "test-host", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-token", "cmd" "token-user", "version" "token"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Two tokens in Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc", "x-waiter-token" "test-token,test-token2"}
                          :passthrough-headers {"host" "test-host", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-token2", "cmd" "token-user", "version" "token"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Multiple tokens in Waiter Headers"
                          :waiter-headers {"x-waiter-foo" "bar", "x-waiter-source" "serv-desc", "x-waiter-token" "test-token,test-token2,test-cpus-token,test-mem-token"}
                          :passthrough-headers {"host" "test-host", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-mem-token", "cmd" "token-user", "cpus" "1", "mem" "2", "version" "token"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Using Host with missing values"
                          :waiter-headers {}
                          :passthrough-headers {"host" "test-host", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-host", "cmd" "token-user", "version" "token"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Using Host without port with missing values"
                          :waiter-headers {}
                          :passthrough-headers {"host" "test-host:1234", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-host", "cmd" "token-user", "version" "token"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Using Token with run-as-user"
                          :waiter-headers {"x-waiter-token" "test-token-run"}
                          :passthrough-headers {"host" "test-host:1234", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-token-run", "cmd" "token-user", "version" "token", "run-as-user" "ruser"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Using Token with permitted-user"
                          :waiter-headers {"x-waiter-token" "test-token-per"}
                          :passthrough-headers {"host" "test-host:1234", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-token-per", "cmd" "token-user", "version" "token", "permitted-user" "puser"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Using Token with run-as-user and permitted-user and another token"
                          :waiter-headers {"x-waiter-token" "test-token-per-run"}
                          :passthrough-headers {"host" "test-host:1234", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-token-per-run", "cmd" "token-user", "version" "token", "run-as-user" "ruser", "permitted-user" "puser"},
                                     :headers {}
                                     :token-preauthorized true}
                          }
                         {:name "prepare-service-description-sources:Using Token with run-as-user and permitted-user"
                          :waiter-headers {"x-waiter-token" "test-token-per-run,test-cpus-token"}
                          :passthrough-headers {"host" "test-host:1234", "fee" "foe"}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {"name" "test-cpus-token", "cmd" "token-user", "version" "token", "cpus" "1", "run-as-user" "ruser", "permitted-user" "puser"},
                                     :headers {}
                                     :token-preauthorized false}
                          }
                         {:name "prepare-service-description-sources:Parse metadata headers"
                          :waiter-headers {"x-waiter-metadata-foo" "bar", "x-waiter-metadata-baz" "quux", "x-waiter-cpus" "1"}
                          :passthrough-headers {}
                          :expected {:defaults {"name" "default-name", "health-check-url" "/ping"},
                                     :tokens {},
                                     :headers {"metadata" {"foo" "bar", "baz" "quux"}, "cpus" "1"}
                                     :token-preauthorized false}
                          }
                         )]
        (doseq [{:keys [name waiter-headers passthrough-headers expected]} test-cases]
          (testing (str "Test " name)
            (let [actual (prepare-service-description-sources
                           {:waiter-headers waiter-headers
                            :passthrough-headers passthrough-headers}
                           kv-store waiter-hostname service-description-defaults)]
              (when (not= expected actual)
                (log/info name)
                (log/info "Expected: " (into (sorted-map) expected))
                (log/info "Actual:   " (into (sorted-map) actual)))
              (is (= expected actual)))))))))

(defn- service-description
  ([sources & {:keys [waiter-headers kv-store assoc-run-as-user-approved?]
               :or {waiter-headers {}
                    kv-store (kv/->LocalKeyValueStore (atom {}))
                    assoc-run-as-user-approved? (constantly false)}}]
   (with-redefs [metric-group-filter (fn [sd _] sd)
                 service-description-schema {s/Str s/Any}]
     (:service-description
       (compute-service-description sources waiter-headers {} kv-store "test-service-" "current-request-user"
                                    [] (->DefaultServiceDescriptionBuilder nil) assoc-run-as-user-approved?)))))

(deftest test-compute-service-description
  (testing "Service description computation"

    (testing "only token from host with permitted-user in defaults"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd"}}))))

    (testing "only token from host without permitted-user in defaults"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"cmd" "token-cmd"}}))))

    (testing "only token from header"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd"}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "only token from host with defaults missing permitted user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"cmd" "token-cmd"}}))))

    (testing "only token from header with defaults missing permitted user"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"cmd" "token-cmd"}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "only token from host with dummy header"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"cmd" "token-cmd"}}
                                  :waiter-headers {"x-waiter-dummy" "value-does-not-matter"}))))

    (testing "only on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd"}}))))

    (testing "token host with non-intersecting values"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd"}
                                   :headers {"version" "on-the-fly-version"}}))))

    (testing "token header with non-intersecting values"
      (is (= {"cmd" "token-cmd"
              "concurrency-level" 5
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "concurrency-level" 5}
                                   :headers {"version" "on-the-fly-version"}}))))

    (testing "token host with intersecting values"
      (is (= {"cmd" "on-the-fly-cmd"
              "concurrency-level" 6
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd"}
                                   :headers {"cmd" "on-the-fly-cmd", "concurrency-level" 6}}))))

    (testing "token header with intersecting values"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd"}
                                   :headers {"cmd" "on-the-fly-cmd"}}))))

    (testing "intersecting values with additional fields"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "name" "token-name"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "version" "on-the-fly-version"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "name" "token-name"}
                                   :headers {"cmd" "on-the-fly-cmd", "version" "on-the-fly-version"}}))))

    (testing "permitted user from token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"permitted-user" "token-pu"}
                                   :headers {"cmd" "on-the-fly-cmd"}}))))

    (testing "permitted user from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "on-the-fly-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :headers {"cmd" "on-the-fly-cmd", "permitted-user" "on-the-fly-pu"}}))))

    (testing "permitted user intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "on-the-fly-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"permitted-user" "token-pu"}
                                   :headers {"cmd" "on-the-fly-cmd", "permitted-user" "on-the-fly-pu"}}))))

    (testing "run as user and permitted user only in token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"run-as-user" "token-ru", "permitted-user" "token-pu"}
                                   :headers {"cmd" "on-the-fly-cmd"}}))))

    (testing "run as user and permitted user from token and no on-the-fly headers"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "token-pu"
              "run-as-user" "token-ru"}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"cmd" "token-cmd"
                                            "run-as-user" "token-ru"
                                            "permitted-user" "token-pu"}}))))

    (testing "missing permitted user in token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "token-ru"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "run-as-user" "token-ru"}}))))

    (testing "run as user from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :headers {"cmd" "on-the-fly-cmd", "run-as-user" "on-the-fly-ru"}}))))

    (testing "run as user intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "on-the-fly-ru"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"run-as-user" "token-ru"},
                                   :headers {"cmd" "on-the-fly-cmd", "run-as-user" "on-the-fly-ru"}}))))

    (testing "run as user provided from on-the-fly header with hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "chris"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "run-as-user" "alice"},
                                   :headers {"run-as-user" "chris"}}
                                  :waiter-headers {"x-waiter-run-as-user" "chris"}))))

    (testing "run as user star from on-the-fly header with hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "run-as-user" "alice"},
                                   :headers {"run-as-user" "*"}}
                                  :waiter-headers {"x-waiter-run-as-user" "*"}))))

    (testing "run as user star from hostname token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "run-as-user" "*"},
                                   :headers {}}
                                  :waiter-headers {}))))

    (testing "run as user star from on-the-fly token"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "run-as-user" "*"},
                                   :headers {}}
                                  :waiter-headers {"x-waiter-token" "on-the-fly-token"}))))

    (testing "run as user star from on-the-fly headers without permitted-user"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"run-as-user" "token-ru"},
                                   :headers {"cmd" "on-the-fly-cmd", "run-as-user" "*"}}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd", "x-waiter-run-as-user" "*"}))))

    (testing "run as user star from on-the-fly headers with permitted-user"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"run-as-user" "token-ru"},
                                   :headers {"cmd" "on-the-fly-cmd", "permitted-user" "alice", "run-as-user" "*"}}
                                  :waiter-headers {"x-waiter-cmd" "on-the-fly-cmd"
                                                   "x-waiter-permitted-user" "alice"
                                                   "x-waiter-run-as-user" "*"}))))

    (testing "active overrides"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))]
        (store-service-description-overrides
          kv-store
          "test-service-activeoverride-00de822338af921fbefacd263d092c8a"
          "current-request-user"
          {"scale-factor" 0.3})
        (is (= {"cmd" "on-the-fly-cmd"
                "health-check-url" "/ping"
                "permitted-user" "bob"
                "run-as-user" "on-the-fly-ru"
                "name" "active-override"
                "scale-factor" 0.3}
               (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                     :headers {"cmd" "on-the-fly-cmd"
                                               "run-as-user" "on-the-fly-ru"
                                               "name" "active-override"}}
                                    :kv-store kv-store)))))

    (testing "inactive overrides"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))]
        (store-service-description-overrides
          kv-store
          "test-service-inactiveoverride-b72d04dd1527e9730d1e8f5bc6bcf341"
          "current-request-user"
          {"scale-factor" 0.3})
        (clear-service-description-overrides
          kv-store
          "test-service-inactiveoverride-b72d04dd1527e9730d1e8f5bc6bcf341"
          "current-request-user")
        (is (= {"cmd" "on-the-fly-cmd"
                "health-check-url" "/ping"
                "permitted-user" "bob"
                "run-as-user" "on-the-fly-ru"
                "name" "inactive-override"
                "scale-factor" 1}
               (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob", "scale-factor" 1}
                                     :headers {"cmd" "on-the-fly-cmd"
                                               "run-as-user" "on-the-fly-ru"
                                               "name" "inactive-override"}}
                                    :kv-store kv-store)))))

    (testing "override token metadata from headers"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "bob"
              "run-as-user" "current-request-user"
              "metadata" {"e" "f"}}
             (service-description {:defaults {"health-check-url" "/ping", "permitted-user" "bob"}
                                   :tokens {"cmd" "token-cmd", "metadata" {"a" "b", "c" "d"}}
                                   :headers {"metadata" {"e" "f"}}}))))

    (testing "sanitize metadata"
      (is (= {"cmd" "token-cmd"
              "health-check-url" "/ping"
              "permitted-user" "current-request-user"
              "run-as-user" "current-request-user"
              "metadata" {"abc" "DEF"}}
             (service-description {:defaults {"health-check-url" "/ping"}
                                   :tokens {"cmd" "token-cmd", "metadata" {"Abc" "DEF"}}}
                                  :waiter-headers {"x-waiter-token" "value-does-not-matter"}))))

    (testing "metric group from token"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "token-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :tokens {"metric-group" "token-mg"}
                                   :headers {"cmd" "on-the-fly-cmd"}}))))

    (testing "metric group from on-the-fly"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "on-the-fly-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :headers {"cmd" "on-the-fly-cmd", "metric-group" "on-the-fly-mg"}}))))

    (testing "metric group intersecting"
      (is (= {"cmd" "on-the-fly-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "metric-group" "on-the-fly-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :tokens {"metric-group" "token-mg"}
                                   :headers {"cmd" "on-the-fly-cmd", "metric-group" "on-the-fly-mg"}}))))

    (testing "auto-populate run-as-user"
      (is (= {"cmd" "some-cmd"
              "health-check-url" "/health"
              "run-as-user" "current-request-user"
              "permitted-user" "current-request-user"
              "metric-group" "token-mg"}
             (service-description {:defaults {"health-check-url" "/health"}
                                   :tokens {"cmd" "some-cmd", "metric-group" "token-mg"}}
                                  :assoc-run-as-user-approved? (constantly true)))))))

(deftest test-compute-service-description-error-scenarios
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id-prefix "test-service-"
        test-user "test-header-user"]
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" "/ping"},
                                               :tokens {"cmd" "test command"
                                                        "cpus" "one"
                                                        "mem" 200
                                                        "version" "a1b2c3"
                                                        "run-as-user" test-user}
                                               :headers {}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (->DefaultServiceDescriptionBuilder nil)
                                              (constantly false))))
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" 1},
                                               :tokens {"cmd" "test command"
                                                        "cpus" 1
                                                        "mem" 200
                                                        "version" "a1b2c3"
                                                        "run-as-user" test-user}
                                               :headers {}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (->DefaultServiceDescriptionBuilder nil)
                                              (constantly false))))
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" 1}
                                               :tokens {}
                                               :headers {}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (->DefaultServiceDescriptionBuilder nil)
                                              (constantly false))))
    (is (thrown? Exception
                 (compute-service-description {:defaults {"health-check-url" "/health"}
                                               :tokens {"cmd" "cmd for missing run-as-user"
                                                        "cpus" 1
                                                        "mem" 200
                                                        "version" "a1b2c3"}}
                                              {} {} kv-store service-id-prefix test-user []
                                              (->DefaultServiceDescriptionBuilder nil)
                                              (constantly false))))))

(deftest test-service-id-and-token-storing
  (with-redefs [service-description->service-id (fn [prefix sd] (str prefix (hash (select-keys sd service-description-keys))))]
    (let [lock (Object.)
          synchronize-fn (fn [_ f]
                           (locking lock
                             (f)))
          kv-store (kv/->LocalKeyValueStore (atom {}))
          service-id-prefix "test#"
          token "test-token"
          service-description {"cmd" "tc", "cpus" 1, "mem" 200, "version" "a1b2c3", "token" token,
                               "run-as-user" "tu1", "permitted-user" "tu2", "owner" "tu3"}
          service-id (service-description->service-id service-id-prefix service-description)]
      ; prepare
      (kv/store kv-store token service-description)
      ; test
      (testing "test:token->service-description-1"
        (is (nil? (kv/fetch kv-store service-id))))
      (testing "test:token->service-description-2"
        (is (= service-description (token->service-description-template kv-store token)))
        (is (= {} (token->service-description-template kv-store "invalid-token" :error-on-missing false)))
        (is (thrown? ExceptionInfo (token->service-description-template kv-store "invalid-token")))))))

(deftest test-service-suspend-resume
  (let [kv-store (kv/->LocalKeyValueStore (atom {}))
        service-id-1 "test-service-1"
        service-id-2 "test-service-2"
        username "test-user"
        service-description {"cmd" "tc", "cpus" 1, "mem" 200, "version" "a1b2c3", "run-as-user" "tu1", "permitted-user" "tu2"}
        service-description-1 (assoc service-description "run-as-user" username)
        service-description-2 (assoc service-description "run-as-user" (str username "2"))
        authorized? (fn [subject _ {:keys [user]}] (= subject user))
        validate-description (constantly true)]
    (testing "test-service-suspend-resume"
      (store-core kv-store service-id-1 service-description-1 validate-description)
      (store-core kv-store service-id-2 service-description-2 validate-description)
      (is (can-manage-service? kv-store service-id-1 authorized? username))
      (is (not (can-manage-service? kv-store service-id-2 authorized? username)))
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
        service-description-1 {"cmd" "tc", "cpus" 1, "mem" 200, "version" "a1b2c3", "run-as-user" "tu1a", "permitted-user" "tu2"}
        service-description-2 (assoc service-description-1 "run-as-user" username-1)
        service-description-3 (assoc service-description-1 "run-as-user" "tu2")
        authorized? (fn [subject verb {:keys [user]}]
                      (and (= verb :manage) (or (str/includes? user subject) (= admin-username subject))))
        validate-description (constantly true)]
    (testing "test-service-suspend-resume"
      (store-core kv-store service-id-1 service-description-1 validate-description)
      (store-core kv-store service-id-2 service-description-2 validate-description)
      (store-core kv-store service-id-3 service-description-3 validate-description)
      (is (can-manage-service? kv-store service-id-1 authorized? username-1))
      (is (can-manage-service? kv-store service-id-2 authorized? username-1))
      (is (not (can-manage-service? kv-store service-id-3 authorized? username-1)))
      (is (not (can-manage-service? kv-store service-id-1 authorized? username-2)))
      (is (not (can-manage-service? kv-store service-id-2 authorized? username-2)))
      (is (can-manage-service? kv-store service-id-3 authorized? username-2))
      (is (can-manage-service? kv-store service-id-1 authorized? admin-username))
      (is (can-manage-service? kv-store service-id-2 authorized? admin-username))
      (is (can-manage-service? kv-store service-id-3 authorized? admin-username)))))

(deftest test-metadata-error-message
  (let [service-description {"cpus" 1, "mem" 1, "cmd" "exit 0", "version" "1", "run-as-user" "someone"}]
    (testing "metdata schema error"
      (try
        (validate-schema (assoc service-description "metadata" {"a" "b" "c" 1}) nil)
        (is false "Exception should have been thrown for invalid service description.")
        (catch ExceptionInfo ex
          (let [friendly-message (get-in (ex-data ex) [:friendly-error-message :metadata])]
            (is (str/includes? friendly-message "Metadata values must be strings.") friendly-message)
            (is (str/includes? friendly-message "did not have string values: c: 1.") friendly-message)))))
    (testing "too many metadata keys"
      (let [error-msg (generate-friendly-metadata-error-message (s/check service-description-schema
                                                                         (assoc service-description "metadata"
                                                                                                    (zipmap (take 200 (iterate #(str % "a") "a"))
                                                                                                            (take 200 (iterate #(str % "a") "a"))))))]
        (is (str/includes? error-msg "200") error-msg)))
    (testing "not a map"
      (let [error-msg (generate-friendly-metadata-error-message (s/check service-description-schema
                                                                         (assoc service-description "metadata" 12)))]
        (is (str/includes? error-msg "Metadata must be a map") error-msg)))
    (testing "invalid keys"
      (let [error-msg (generate-friendly-metadata-error-message (s/check service-description-schema
                                                                         (assoc service-description "metadata" {1 "a", 2 "b"})))]
        (is (str/includes? error-msg "The following metadata keys are invalid: 1, 2") error-msg)
        (is (not (str/includes? error-msg "Metadata values must be strings.")) error-msg)))
    (testing "invalid keys and values"
      (let [error-msg (generate-friendly-metadata-error-message (s/check service-description-schema
                                                                         (assoc service-description "metadata" {1 "a" "b" 2})))]
        (is (str/includes? error-msg "The following metadata keys are invalid: 1") error-msg)
        (is (str/includes? error-msg "did not have string values: b: 2.") error-msg)))))

(deftest test-environment-variable-schema
  (let [service-description {"cpus" 1, "mem" 1, "cmd" "exit 0", "version" "1", "run-as-user" "someone"}]
    (testing "environment variable schema error"
      (try
        (validate-schema (assoc service-description "env" {"abc" "def", "ABC" 1}) nil)
        (is false "Exception should have been thrown for invalid service description")
        (catch ExceptionInfo ex
          (let [friendly-message (get-in (ex-data ex) [:friendly-error-message :env])]
            (is (str/includes? friendly-message "values must be strings") friendly-message)
            (is (str/includes? friendly-message "did not have string values: ABC: 1.") friendly-message)))))

    (testing "too many environment variables"
      (let [error-msg (generate-friendly-environment-variable-error-message (s/check service-description-schema
                                                                                     (assoc service-description "env"
                                                                                                                (zipmap (take 200 (iterate #(str % "A") "A"))
                                                                                                                        (take 200 (iterate #(str % "a") "a"))))))]
        (is (str/includes? error-msg "200") error-msg)))

    (testing "not a map"
      (let [error-msg (generate-friendly-environment-variable-error-message (s/check service-description-schema
                                                                                     (assoc service-description "env" 12)))]
        (is (str/includes? error-msg "Environment variables must be a map") error-msg)))

    (testing "invalid keys"
      (let [error-msg (generate-friendly-environment-variable-error-message (s/check service-description-schema
                                                                                     (assoc service-description "env" {1 "a", 2 "b"})))]
        (is (str/includes? error-msg "The following environment variable keys are invalid: 1, 2") error-msg)
        (is (not (str/includes? error-msg "Environment variable values must be strings.")) error-msg)
        (is (not (str/includes? error-msg "cannot be assigned")) error-msg)))

    (testing "invalid keys and values"
      (let [error-msg (generate-friendly-environment-variable-error-message (s/check service-description-schema
                                                                                     (assoc service-description "env" {1 "a" "B" 2})))]
        (is (str/includes? error-msg "The following environment variable keys are invalid: 1") error-msg)
        (is (str/includes? error-msg "did not have string values: B: 2.") error-msg)))
    (testing "using reserved variables"
      (let [error-msg (generate-friendly-environment-variable-error-message (s/check service-description-schema
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

      (testing "should use 'other' when metric group not specified and name not mapped"
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
      (validate (->DefaultServiceDescriptionBuilder nil) {} {})
      (validate (->DefaultServiceDescriptionBuilder nil) {"cmd-type" "shell"} {})
      (is (thrown? Exception (validate (->DefaultServiceDescriptionBuilder nil) {"cmd-type" ""} {})))
      (is (thrown-with-msg? Exception #"Command type invalid is not supported"
                            (validate (->DefaultServiceDescriptionBuilder nil) {"cmd-type" "invalid"} {}))))))

(deftest test-consent-cookie-value
  (let [current-time-ms (System/currentTimeMillis)
        clock (constantly current-time-ms)]
    (is (= nil (consent-cookie-value clock nil nil nil nil)))
    (is (= ["unsupported" current-time-ms] (consent-cookie-value clock "unsupported" nil nil nil)))
    (is (= ["service" current-time-ms] (consent-cookie-value clock "service" nil nil nil)))
    (is (= ["service" current-time-ms "service-id"] (consent-cookie-value clock "service" "service-id" nil nil)))
    (is (= ["token" current-time-ms] (consent-cookie-value clock "token" nil nil nil)))
    (is (= ["token" current-time-ms] (consent-cookie-value clock "token" nil nil {"owner" "user"})))
    (is (= ["token" current-time-ms] (consent-cookie-value clock "token" nil "token-id" {})))
    (is (= ["token" current-time-ms "token-id" "user"] (consent-cookie-value clock "token" nil "token-id" {"owner" "user"})))))

(deftest test-assoc-run-as-user-approved?
  (let [current-time-ms (System/currentTimeMillis)
        clock (constantly current-time-ms)
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
