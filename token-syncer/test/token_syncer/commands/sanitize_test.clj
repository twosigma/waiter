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
(ns token-syncer.commands.sanitize-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [token-syncer.cli :as cli]
            [token-syncer.commands.sanitize :refer :all]
            [token-syncer.utils :as utils])
  (:import (java.util.regex Pattern)))

(deftest test-sanitize
  (let [test-cluster-url "http://www.test-cluster.com"
        limit 3
        loaded-tokens-atom (atom #{})
        stored-tokens-atom (atom #{})
        waiter-api {:load-token (fn [cluster-url token]
                                  (swap! loaded-tokens-atom conj token)
                                  (is (= test-cluster-url cluster-url))
                                  (when-not (= token "token-1")
                                    {:description {"metric-group" token "name" token}
                                     :token-etag (str "etag-" token)}))
                    :load-token-list (fn [cluster-url]
                                       (is (= test-cluster-url cluster-url))
                                       [{"etag" "etag-token-1"
                                         "last-update-time" (utils/millis->iso8601 10000)
                                         "token" "token-1"}
                                        {"deleted" true
                                         "etag" "etag-token-B"
                                         "last-update-time" (utils/millis->iso8601 20000)
                                         "token" "token-B"}
                                        {"deleted" true
                                         "etag" "etag-token-3"
                                         "last-update-time" (utils/millis->iso8601 30000)
                                         "token" "token-3"}
                                        {"deleted" true
                                         "etag" "etag-token-4"
                                         "last-update-time" (utils/millis->iso8601 40000)
                                         "token" "token-4"}
                                        {"deleted" false
                                         "etag" "etag-token-E"
                                         "last-update-time" (utils/millis->iso8601 50000)
                                         "token" "token-E"}
                                        {"deleted" false
                                         "etag" "etag-token-6"
                                         "last-update-time" (utils/millis->iso8601 60000)
                                         "token" "token-6"}])
                    :store-token (fn [cluster-url token token-etag description]
                                   (swap! stored-tokens-atom conj token)
                                   (is (= test-cluster-url cluster-url))
                                   (is (= (str "etag-" token) token-etag))
                                   (is (= {"metric-group" token "name" token} description))
                                   {:headers {"etag" (str token-etag (when (= token "token-3") "-new"))}
                                    :status 200})}
        regex (re-pattern "token-\\d+")
        actual-result (sanitize-tokens waiter-api test-cluster-url limit regex)]

    (is (= {:missing #{"token-1"}
            :processed #{"token-1" "token-3" "token-4"}
            :updated #{"token-3"}}
           actual-result))
    (is (= #{"token-1" "token-3" "token-4"} @loaded-tokens-atom))
    (is (= #{"token-3" "token-4"} @stored-tokens-atom))))

(deftest test-sanitize-tokens-config
  (let [test-command-config (assoc sanitize-tokens-config :command-name "test-command")
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
    (let [limit-input (str 200000)
          args ["--limit" limit-input "cluster-1.com"]]
      (is (= {:data [(str "Failed to validate \"--limit " limit-input "\": "
                          "Must be between 1 and 10000")]
              :exit-code 1
              :message "test-command: error in parsing arguments"}
             (cli/process-command test-command-config context args))))

    (let [regex-input 200000
          args ["--regex" regex-input "cluster-1.com"]
          actual-result (cli/process-command test-command-config context args)]
      (is (= {:exit-code 1
              :message "test-command: error in parsing arguments"}
             (dissoc actual-result :data)))
      (is (str/includes? (-> actual-result :data first) (str "Error while parsing option \"--regex " regex-input "\": ")))
      (is (str/includes? (-> actual-result :data first) "java.lang.ClassCastException"))
      (is (str/includes? (-> actual-result :data first) "java.lang.Long"))
      (is (str/includes? (-> actual-result :data first) "java.lang.String")))

    (let [cluster-url "http://cluster-1.com"
          make-sanitize-tokens (fn [invocation-promise]
                                (fn [in-waiter-api in-cluster-url count-limit token-regex]
                                  (is (= waiter-api in-waiter-api))
                                  (is (= cluster-url in-cluster-url))
                                  (is (number? count-limit))
                                  (is (instance? Pattern token-regex))
                                  (deliver invocation-promise ::invoked)))]
      (let [args [cluster-url]
            invocation-promise (promise)]
        (with-redefs [sanitize-tokens (make-sanitize-tokens invocation-promise)]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args)))
          (is (= ::invoked (deref invocation-promise 0 ::un-initialized)))))
      (let [args ["--limit" (str 100) "--regex" ".*foo.*" cluster-url]
            invocation-promise (promise)]
        (with-redefs [sanitize-tokens (make-sanitize-tokens invocation-promise)]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args)))
          (is (= ::invoked (deref invocation-promise 0 ::un-initialized))))))))
