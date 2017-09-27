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
            [token-syncer.main :refer :all]))

(deftest test-parse-cli-options
  (is (= ["Failed to validate \"-c abcd\": Must provide at least two different semi-colon separated cluster urls"]
         (:errors (parse-cli-options ["-c" "abcd"]))))
  (is (= ["Failed to validate \"-c abcd;abcd\": Must provide at least two different semi-colon separated cluster urls"]
         (:errors (parse-cli-options ["-c" "abcd;abcd"]))))
  (is (= {:cluster-urls ["abcd" "efgh"], :connection-timeout-ms 1000, :idle-timeout-ms 30000}
         (:options (parse-cli-options ["-c" "abcd;efgh"]))))
  (is (= {:cluster-urls [], :connection-timeout-ms 1000, :help true, :idle-timeout-ms 30000}
         (:options (parse-cli-options ["-h"]))))
  (is (= {:cluster-urls [], :connection-timeout-ms 1000, :idle-timeout-ms 10000}
         (:options (parse-cli-options ["-i" "10000"]))))
  (is (= {:cluster-urls [], :connection-timeout-ms 10000, :idle-timeout-ms 30000}
         (:options (parse-cli-options ["-t" "10000"]))))
  (is (= {:cluster-urls [], :connection-timeout-ms 10000, :idle-timeout-ms 20000}
         (:options (parse-cli-options ["-i" "20000" "-t" "10000"])))))