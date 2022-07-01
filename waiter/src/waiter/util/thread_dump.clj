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
(ns waiter.util.thread-dump
  (:require [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [waiter.util.date-utils :as du])
  (:import (java.lang.management ManagementFactory ThreadInfo)))

(defn generate-and-log-thread-dump
  "Generates and logs the thread dump."
  [show-locked-monitors show-locked-synchronizers]
  (let [thread-bean (ManagementFactory/getThreadMXBean)
        str-builder (StringBuilder.)
        new-line (System/getProperty "line.separator")
        dumped-thread-info (.dumpAllThreads thread-bean show-locked-monitors show-locked-synchronizers)]
    (.append str-builder (str "Thread dump at " (du/date-to-str (t/now))))
    (.append str-builder new-line)
    (.append str-builder (str "Total number of peak threads: " (.getPeakThreadCount thread-bean)))
    (.append str-builder new-line)
    (.append str-builder (str "Total number of threads created and started: " (.getTotalStartedThreadCount thread-bean)))
    (.append str-builder new-line)
    (.append str-builder (str "Total number of live daemon threads: " (.getDaemonThreadCount thread-bean)))
    (.append str-builder new-line)
    (.append str-builder (str "Total number of live total threads: " (.getThreadCount thread-bean)))
    (.append str-builder new-line)
    (doseq [thread-info (sort-by (fn [^ThreadInfo ti] (.getThreadName ti)) dumped-thread-info)]
      (.append str-builder new-line)
      (.append str-builder (str thread-info)))
    (log/log "ThreadDump" :info nil (str str-builder))))

(defn start-thread-dump-daemon
  "Launches daemon that logs a thread dump that provides a snapshot of all the threads in the current program.
   The state of each thread is followed by a stack trace containing the information about the thread activity."
  [{:keys [interval-secs show-locked-monitors show-locked-synchronizers]}]
  (if (pos? interval-secs)
    (let [interval-ms (-> interval-secs (t/seconds) (t/in-millis))]
      (log/info "thread dump daemon will run every" interval-secs "secs")
      (du/start-timer-task
        (t/millis interval-ms)
        (fn run-thread-dump-daemon-task []
          (try
            (log/info "generating thread dump")
            (generate-and-log-thread-dump show-locked-monitors show-locked-synchronizers)
            (catch Exception ex
              (log/error ex "error logging thread dump"))))
        :delay-ms interval-ms))
    (log/info "thread dump daemon has been disabled")))
