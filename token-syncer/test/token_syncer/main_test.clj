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
                                                    [_ {:keys [options]} arguments]
                                                    {:arguments arguments
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
    (let [args ["-d" "test-sub-command" "-a"]]
      (is (= {:arguments []
              :exit-code 0
              :message "test-command: test-sub-command: exiting"
              :options {:activate true}}
             (cli/process-command test-command-config context args))))))
