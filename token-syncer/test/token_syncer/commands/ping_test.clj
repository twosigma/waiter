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
(ns token-syncer.commands.ping-test
  (:require [clojure.test :refer :all]
            [token-syncer.cli :as cli]
            [token-syncer.commands.ping :refer :all]))

(deftest test-ping-token
  (let [test-token "test-token"
        queue-timeout-ms 10000]

    (testing "exception on health check"
      (let [test-cluster-urls ["http://c1.com"]
            waiter-api {:health-check-token (fn [cluster-url token in-timeout-ms]
                                              (is (= (first test-cluster-urls) cluster-url))
                                              (is (= test-token token))
                                              (is (= queue-timeout-ms in-timeout-ms))
                                              (throw (ex-info "thrown from test" {:cluster-url cluster-url})))}]
        (is (= {:details {"http://c1.com"
                          {:exit-code 1
                           :message (str "unable to ping token test-token on http://c1.com, "
                                         "reason: thrown from test")}}
                :exit-code 1
                :message "pinging token test-token on [http://c1.com] failed"
                :token test-token}
               (ping-token waiter-api test-cluster-urls test-token queue-timeout-ms)))))

    (testing "unsuccessful health check"
      (let [test-cluster-urls ["http://c1.com"]
            waiter-api {:health-check-token (fn [cluster-url token in-timeout-ms]
                                              (is (= (first test-cluster-urls) cluster-url))
                                              (is (= test-token token))
                                              (is (= queue-timeout-ms in-timeout-ms))
                                              {:body "failure" :status 400})}]
        (is (= {:details {"http://c1.com"
                          {:exit-code 1
                           :message (str "unable to ping token test-token on http://c1.com, "
                                         "reason: health check returned status code 400")}}
                :exit-code 1
                :message "pinging token test-token on [http://c1.com] failed"
                :token test-token}
               (ping-token waiter-api test-cluster-urls test-token queue-timeout-ms)))))

    (testing "unsuccessful health check"
      (let [test-cluster-urls ["http://c1.com" "http://c2.com"]
            waiter-api {:health-check-token (fn [cluster-url token in-timeout-ms]
                                              (is (= test-token token))
                                              (is (= queue-timeout-ms in-timeout-ms))
                                              {:body (str "failure " cluster-url) :status 400})}]
        (is (= {:details {"http://c1.com"
                          {:exit-code 1
                           :message (str "unable to ping token test-token on http://c1.com, "
                                         "reason: health check returned status code 400")}
                          "http://c2.com"
                          {:exit-code 1
                           :message (str "unable to ping token test-token on http://c2.com, "
                                         "reason: health check returned status code 400")}}
                :exit-code 2
                :message "pinging token test-token on [http://c1.com http://c2.com] failed"
                :token test-token}
               (ping-token waiter-api test-cluster-urls test-token queue-timeout-ms)))))

    (testing "single unsuccessful health check"
      (let [test-cluster-urls ["http://c1.com" "http://c2.com"]
            waiter-api {:health-check-token (fn [cluster-url token in-timeout-ms]
                                              (is (= test-token token))
                                              (is (= queue-timeout-ms in-timeout-ms))
                                              (if (= cluster-url "http://c1.com")
                                                {:body (str "success " cluster-url) :status 200}
                                                {:body (str "failure " cluster-url) :status 400}))}]
        (is (= {:details {"http://c1.com"
                          {:exit-code 0
                           :message (str "successfully pinged token test-token on http://c1.com, "
                                         "reason: health check returned status code 200")}
                          "http://c2.com"
                          {:exit-code 1
                           :message (str "unable to ping token test-token on http://c2.com, "
                                         "reason: health check returned status code 400")}}
                :exit-code 1
                :message "pinging token test-token on [http://c1.com http://c2.com] failed"
                :token test-token}
               (ping-token waiter-api test-cluster-urls test-token queue-timeout-ms)))))

    (testing "successful health check"
      (let [test-cluster-urls ["http://c1.com" "http://c2.com"]
            waiter-api {:health-check-token (fn [cluster-url token in-timeout-ms]
                                              (is (= test-token token))
                                              (is (= queue-timeout-ms in-timeout-ms))
                                              {:body (str "success " cluster-url) :status 200})}]
        (is (= {:details {"http://c1.com"
                          {:exit-code 0
                           :message (str "successfully pinged token test-token on http://c1.com, "
                                         "reason: health check returned status code 200")}
                          "http://c2.com"
                          {:exit-code 0
                           :message (str "successfully pinged token test-token on http://c2.com, "
                                         "reason: health check returned status code 200")}}
                :exit-code 0
                :message "pinging token test-token on [http://c1.com http://c2.com] was successful"
                :token test-token}
               (ping-token waiter-api test-cluster-urls test-token queue-timeout-ms)))))))

(deftest test-ping-token-config
  (let [test-command-config (assoc ping-token-config :command-name "test-command")
        waiter-api {:health-check-token (constantly {})}
        context {:waiter-api waiter-api}]
    (testing "sub-command token config"
      (let [args []]
        (is (= {:exit-code 1
                :message "test-command: no arguments provided, usage TOKEN URL..."}
               (cli/process-command test-command-config context args))))
      (with-out-str
        (let [args ["-h"]]
          (is (= {:exit-code 0
                  :message "test-command: displayed documentation"}
                 (cli/process-command test-command-config context args)))))
      (let [args ["http://c1.com" "http://c2.com"]]
        (is (= {:exit-code 1
                :message (str "test-command: token is not valid: "
                              "{:arguments [http://c1.com http://c2.com],"
                              " :pattern [a-zA-Z]([a-zA-Z0-9\\-_$\\.])+,"
                              " :token http://c1.com}")}
               (cli/process-command test-command-config context args))))
      (let [args ["my-token"]]
        (with-redefs [ping-token (fn [in-waiter-api in-cluster-urls token queue-timeout-ms]
                                   (is (= waiter-api in-waiter-api))
                                   (is (= #{"http://c1.com"} in-cluster-urls))
                                   (is (= "my-token" token))
                                   (is (= 12000 queue-timeout-ms))
                                   {:exit-code 0})]
          (is (= {:exit-code 1
                  :message "test-command: at least one cluster url required, provided: []"}
                 (cli/process-command test-command-config context args)))))
      (let [args ["my-token" "http://c1.com"]]
        (with-redefs [ping-token (fn [in-waiter-api in-cluster-urls token queue-timeout-ms]
                                   (is (= waiter-api in-waiter-api))
                                   (is (= #{"http://c1.com"} in-cluster-urls))
                                   (is (= "my-token" token))
                                   (is (= 12000 queue-timeout-ms))
                                   {:exit-code 0})]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args)))))
      (let [args ["-q" "123456" "my-token" "http://c1.com"]]
        (with-redefs [ping-token (fn [in-waiter-api in-cluster-urls token queue-timeout-ms]
                                   (is (= waiter-api in-waiter-api))
                                   (is (= #{"http://c1.com"} in-cluster-urls))
                                   (is (= "my-token" token))
                                   (is (= 123456 queue-timeout-ms))
                                   {:exit-code 0})]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args)))))
      (let [args ["my-token" "http://c1.com" "http://c2.com"]]
        (with-redefs [ping-token (fn [in-waiter-api in-cluster-urls token queue-timeout-ms]
                                   (is (= waiter-api in-waiter-api))
                                   (is (= #{"http://c1.com" "http://c2.com"} in-cluster-urls))
                                   (is (= "my-token" token))
                                   (is (= 12000 queue-timeout-ms))
                                   {:exit-code 0})]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args))))))))
