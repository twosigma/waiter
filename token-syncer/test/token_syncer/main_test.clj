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
(ns token-syncer.main-test
  (:require [clojure.test :refer :all]
            [clojure.tools.cli :as tools-cli]
            [token-syncer.cli :as cli]
            [token-syncer.main :refer :all]))

(deftest test-parse-cli-options
  (let [option-specs (:option-specs base-command-config)
        parse-cli-options (fn [args] (tools-cli/parse-opts args option-specs))]
    (is (= ["Unknown option: \"-c\""]
           (:errors (parse-cli-options ["-c" "abcd"]))))
    (is (= ["Unknown option: \"-c\""]
           (:errors (parse-cli-options ["-c" "abcd,abcd"]))))
    (is (= {:connection-timeout-ms 1000, :idle-timeout-ms 30000}
           (:options (parse-cli-options ["-c" "abcd,efgh"]))))
    (is (= {:connection-timeout-ms 1000, :dry-run true, :idle-timeout-ms 30000}
           (:options (parse-cli-options ["-d" "abcd,efgh"]))))
    (is (= {:connection-timeout-ms 1000, :idle-timeout-ms 10000}
           (:options (parse-cli-options ["-i" "10000"]))))
    (is (= {:connection-timeout-ms 10000, :idle-timeout-ms 30000}
           (:options (parse-cli-options ["-t" "10000"]))))
    (is (= {:connection-timeout-ms 10000, :idle-timeout-ms 20000}
           (:options (parse-cli-options ["-i" "20000" "-t" "10000"]))))
    (let [parsed-arguments (parse-cli-options ["-i" "20000" "-t" "10000" "c1.com" "c2.com" "c2.com"])]
      (is (= ["c1.com" "c2.com" "c2.com"] (:arguments parsed-arguments)))
      (is (= {:connection-timeout-ms 10000, :idle-timeout-ms 20000} (:options parsed-arguments))))))

(deftest test-base-command-config
  (let [test-sub-command-config {:execute-command (fn execute-base-command
                                                    [context {:keys [options]} arguments]
                                                    {:arguments arguments
                                                     :context context
                                                     :options options
                                                     :exit-code 0})
                                 :option-specs [["-a" "--activate" "For test only, activate"]]
                                 :retrieve-documentation (constantly "")}
        sub-command->config {"test-sub-command" test-sub-command-config}
        context {:sub-command->config sub-command->config}
        test-command-config (assoc base-command-config :command-name "test-command")]
    (let [args ["-d" "test-sub-command" "-h"]]
      (with-out-str
        (is (= {:exit-code 0
                :message "test-command: test-sub-command: displayed documentation"}
               (cli/process-command test-command-config context args)))))
    (let [args ["-d" "test-sub-command" "-a"]
          result (cli/process-command test-command-config context args)]
      (is (= {:arguments []
              :exit-code 0
              :message "test-command: test-sub-command: exiting"
              :options {:activate true}}
             (dissoc result :context)))
      (is (every? #(contains? (:context result) %) [:options :sub-command->config :waiter-api])))))
