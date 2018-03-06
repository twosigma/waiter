;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns token-syncer.cli-test
  (:require [clojure.test :refer :all]
            [token-syncer.cli :refer :all]))

(deftest test-process-command
  (let [build-context (fn [parent-context {:keys [options]}]
                        (assoc parent-context :options options))
        command-name "test-command"
        execute-command (fn [context arguments]
                          {:data {:arguments arguments
                                  :context context}
                           :exit-code 0})
        option-specs [["-a" "--activate" "For test only, configures the activate flag"]
                      ["-b" "--build-number BUILD" "For test only, name of binary"
                       :default 1
                       :parse-fn #(Integer/parseInt %)
                       :validate [pos? "Must be positive"]]
                      ["-f" "--fee NAME" "For test only, the fee name"]
                      [nil "--fum" "For test only, the fum flag"]]
        command-config {:build-context build-context
                        :command-name command-name
                        :execute-command execute-command
                        :option-specs option-specs}
        parent-context {}]

    (is (= {:data {:arguments ["sub-command" "-x" "--y" "z"]
                   :context {:options {:activate true :build-number 100 :fee "fie" :fum true}}}
            :exit-code 0
            :message (str command-name ": exiting")}
           (->> ["-a" "-b" "100" "--fee" "fie" "--fum" "sub-command" "-x" "--y" "z"]
                (process-command command-config parent-context))))

    (is (= {:data ["Unknown option: \"-c\"" "Unknown option: \"-d\""]
            :exit-code 1
            :message (str command-name ": error in parsing arguments")}
           (->> ["-a" "-b" "200" "-c" "-d" "400" "sub-command" "-x" "--y" "z"]
                (process-command command-config parent-context))))))
