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
(ns token-syncer.commands.cleanup-test
  (:require [clojure.test :refer :all]
            [token-syncer.cli :as cli]
            [token-syncer.commands.cleanup :refer :all]
            [token-syncer.utils :as utils]))

(deftest test-cleanup
  (let [test-cluster-url "http://www.test-cluster.com"
        current-time-ms (System/currentTimeMillis)
        one-week-ago-ms (- current-time-ms (* 7 24 60 60 1000))
        deleted-tokens-atom (atom #{})
        waiter-api {:hard-delete-token (fn [cluster-url token etag]
                                         (is (= test-cluster-url cluster-url))
                                         (is (= etag (str "etag-" token)))
                                         (swap! deleted-tokens-atom conj token))
                    :load-token-list (fn [cluster-url]
                                       (is (= test-cluster-url cluster-url))
                                       [{"etag" "etag-token-1"
                                         "last-update-time" (utils/millis->iso8601 (- one-week-ago-ms 10000))
                                         "token" "token-1"}
                                        {"deleted" true
                                         "etag" "etag-token-2"
                                         "last-update-time" (utils/millis->iso8601 (- one-week-ago-ms 10000))
                                         "token" "token-2"}
                                        {"deleted" true
                                         "etag" "etag-token-3"
                                         "last-update-time" (utils/millis->iso8601 (+ one-week-ago-ms 10000))
                                         "token" "token-3"}
                                        {"deleted" true
                                         "etag" "etag-token-4"
                                         "last-update-time" (utils/millis->iso8601 (- one-week-ago-ms 30000))
                                         "token" "token-4"}
                                        {"deleted" false
                                         "etag" "etag-token-5"
                                         "last-update-time" (utils/millis->iso8601 (- one-week-ago-ms 10000))
                                         "token" "token-5"}])}
        actual-result (cleanup-tokens waiter-api test-cluster-url current-time-ms)
        expected-result #{"token-2" "token-3" "token-4"}]

    (is (= expected-result actual-result))
    (is (= expected-result @deleted-tokens-atom))))

(deftest test-cleanup-tokens-config
  (let [test-command-config (assoc cleanup-tokens-config :command-name "test-command")
        waiter-api {:load-token (constantly {})
                    :load-token-list (constantly {})}
        context {:waiter-api waiter-api}]
    (let [args []]
      (is (= {:exit-code 1
              :message "test-command: expected one argument URL, provided 0: []"}
             (cli/process-command test-command-config context args))))
    (with-out-str
      (let [args ["-h"]]
        (is (= {:exit-code 0
                :message "test-command: displayed documentation"}
               (cli/process-command test-command-config context args)))))
    (let [args ["some-file.txt" "cluster-1.com"]]
      (is (= {:exit-code 1
              :message "test-command: expected one argument URL, provided 2: [\"some-file.txt\" \"cluster-1.com\"]"}
             (cli/process-command test-command-config context args))))
    (let [date-input (str (System/currentTimeMillis))
          args ["--before" date-input "cluster-1.com"]]
      (is (= {:data [(str "Error while parsing option \"--before " date-input "\": "
                          "java.lang.IllegalArgumentException: "
                          "Unable to parse " date-input " as a ISO8601 date")]
              :exit-code 1
              :message "test-command: error in parsing arguments"}
             (cli/process-command test-command-config context args))))

    (let [cluster-url "http://cluster-1.com"
          make-cleanup-tokens (fn [invocation-promise]
                                (fn [in-waiter-api in-cluster-url before-epoch-time]
                                  (is (= waiter-api in-waiter-api))
                                  (is (= cluster-url in-cluster-url))
                                  (is (number? before-epoch-time))
                                  (deliver invocation-promise ::invoked)))]
      (let [args [cluster-url]
            invocation-promise (promise)]
        (with-redefs [cleanup-tokens (make-cleanup-tokens invocation-promise)]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args)))
          (is (= ::invoked (deref invocation-promise 0 ::un-initialized)))))
      (let [args ["--before" (-> (System/currentTimeMillis) (- 100000) utils/millis->iso8601) cluster-url]
            invocation-promise (promise)]
        (with-redefs [cleanup-tokens (make-cleanup-tokens invocation-promise)]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args)))
          (is (= ::invoked (deref invocation-promise 0 ::un-initialized))))))))
