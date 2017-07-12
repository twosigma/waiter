(ns waiter.shell-scheduler-integration-test
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.shell-scheduler :refer :all]
            [waiter.utils :as utils])
  (:import (java.nio.file Files
                          Paths)
           java.nio.file.attribute.FileAttribute))


(deftest ^:parallel ^:integration-fast ^:dev test-kill-process
  (let [working-dir (Files/createTempDirectory nil
                                               (make-array FileAttribute 0))
        test-cmd (-> (Paths/get "test-files/test-command.sh" (make-array String 0))
                     .toAbsolutePath
                     .toString)
        port->reservation-atom (atom {})
        instance (launch-instance "abc" working-dir test-cmd "http" {} 1
                                  port->reservation-atom [40000 50000])
        pid (:shell-scheduler/pid instance)]
    (Thread/sleep 100)
    (is (= 4 (count (str/split (:out (sh/sh "pgrep" "-g" (str pid)))
                               #"\n"))))
    (kill-process! instance port->reservation-atom 100)
    (Thread/sleep 100)
    (is (= "" (:out (sh/sh "pgrep" "-g" (str pid)))))))
