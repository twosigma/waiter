(ns waiter.shell-scheduler-integration-test
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.scheduler.shell-scheduler :refer :all]
            [waiter.test-helpers :refer :all]
            [waiter.utils :as utils])
  (:import (java.nio.file Files
                          Paths)
           java.nio.file.attribute.FileAttribute))

(deftest ^:parallel ^:integration-fast test-kill-process
  (let [working-dir (Files/createTempDirectory nil
                                               (make-array FileAttribute 0))
        test-cmd (-> (Paths/get "test-files/test-command.sh" (make-array String 0))
                     .toAbsolutePath
                     .toString)
        port->reservation-atom (atom {})
        service-description {"backend-proto" "http"
                             "cmd" test-cmd
                             "ports" 1}
        {:keys [shell-scheduler/pid] :as instance} (launch-instance "abc" service-description working-dir {}
                                                                    port->reservation-atom [40000 50000])]
    ; There are 4 processes spawned by the instance:
    ; 1. The wrapper process launched by the scheduler
    ; 2. The `test-command.sh` script
    ; 3/4 sleep commands forked by `test-command.sh`
    (is (wait-for #(= 4 (count (str/split (:out (sh/sh "pgrep" "-g" (str pid)))
                                          #"\n")))
                  :interval 1 :timeout 10))
    (kill-process! instance port->reservation-atom 100)
    (is (wait-for #(= "" (:out (sh/sh "pgrep" "-g" (str pid))))
                  :interval 1 :timeout 10))))
