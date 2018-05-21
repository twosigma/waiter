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
(ns token-syncer.cli-test
  (:require [clojure.test :refer :all]
            [token-syncer.cli :refer :all]))

(deftest test-process-command
  (let [command-name "test-command"
        execute-command (fn [context {:keys [options]} arguments]
                          {:data {:arguments arguments
                                  :context context
                                  :options options}
                           :exit-code 0})
        option-specs [["-a" "--activate" "For test only, configures the activate flag"]
                      ["-b" "--build-number BUILD" "For test only, name of binary"
                       :default 1
                       :parse-fn #(Integer/parseInt %)
                       :validate [pos? "Must be positive"]]
                      ["-f" "--fee NAME" "For test only, the fee name"]
                      [nil "--fum" "For test only, the fum flag"]]
        command-config {:command-name command-name
                        :execute-command execute-command
                        :option-specs option-specs}
        parent-context {}]

    (is (= {:data {:arguments ["sub-command" "-x" "--y" "z"]
                   :context {}
                   :options {:activate true :build-number 100 :fee "fie" :fum true}}
            :exit-code 0
            :message (str command-name ": exiting")}
           (->> ["-a" "-b" "100" "--fee" "fie" "--fum" "sub-command" "-x" "--y" "z"]
                (process-command command-config parent-context))))

    (with-out-str
      (binding [*err* *out*]
        (is (= {:data ["Unknown option: \"-c\"" "Unknown option: \"-d\""]
                :exit-code 1
                :message (str command-name ": error in parsing arguments")}
               (->> ["-a" "-b" "200" "-c" "-d" "400" "sub-command" "-x" "--y" "z"]
                    (process-command command-config parent-context))))))))
