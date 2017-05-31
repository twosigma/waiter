;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.test-helpers
  (:require [clj-time.core :as t]
            [clojure.data :as data]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.tools.namespace.find :as find]
            [waiter.client-tools :as ct]
            [waiter.correlation-id :as cid]))

(def ^:const ANSI-RESET "\033[0m")
(def ^:const ANSI-BLUE "\033[34m")
(def ^:const ANSI-MAGENTA "\033[1m\033[35m")

(defonce ^:private replaced-layout
         (future (cid/replace-pattern-layout-in-log4j-appenders)))

(defn blue [message] (str ANSI-BLUE message ANSI-RESET))
(defn magenta [message] (str ANSI-MAGENTA message ANSI-RESET))

(defn- full-test-name
  [m]
  (let [test-meta (-> m :var meta)]
    (str (-> test-meta :ns (.getName)) "/" (:name test-meta))))

(let [running-tests (atom {})]
  (defn- log-running-tests []
    (let [tests @running-tests]
      (log/debug (count tests) "running test(s):" tests)))

  (defmethod clojure.test/report :begin-test-var [m]
    (let [test-name (full-test-name m)]
      @replaced-layout
      (log/info (magenta "START:") test-name)
      (swap! running-tests #(assoc % test-name (str (t/now))))
      (log-running-tests)))

  (defmethod clojure.test/report :end-test-var [m]
    (let [test-name (full-test-name m)]
      (log/info (blue "FINISH:") test-name)
      (swap! running-tests #(dissoc % test-name))
      (log-running-tests))))

(defn- elapsed-millis [start-nanos finish-nanos]
  (->
    finish-nanos
    (- start-nanos)
    (double)
    (/ 1000000.0)))

(deftest test-elapsed-millis
  (testing "Elapsed milliseconds calculation"
    (testing "should convert from nanoseconds to milliseconds"
      (is (= 1.0 (elapsed-millis 1000000 2000000)))
      (is (= 1.5 (elapsed-millis 1000000 2500000))))))

(defn wait-for
  "Invoke predicate every interval (default 10) seconds until it returns true,
   or timeout (default 150) seconds have elapsed. E.g.:
       (wait-for #(< (rand) 0.2) :interval 1 :timeout 10)
   Returns nil if the timeout elapses before the predicate becomes true, otherwise
   the value of the predicate on its last evaluation."
  [predicate & {:keys [interval timeout unit-multiplier]
                :or {interval 10
                     timeout 150
                     unit-multiplier 1000}}]
  (ct/wait-for predicate :interval interval :timeout timeout :unit-multiplier unit-multiplier))

(defn diff-message
  "Returns a string with any differences between a and b"
  [a b]
  (when (not= a b)
    (let [[only-in-a only-in-b _] (data/diff a b)]
      (str
        (when only-in-a
          (str "Only in a: " only-in-a \newline))
        (when only-in-b
          (str "Only in b: " only-in-b \newline))))))

(defmethod assert-expr '=
  [msg form]
  `(if (= ~(count form) 3)
     (let [a# ~(nth form 1)
           b# ~(nth form 2)]
       (let [result# (= a# b#)]
         (if result#
           (do-report {:type :pass, :message ~msg, :expected a#, :actual b#})
           (if (and (map? a#) (map? b#))
             (do-report {:type :fail, :message (diff-message a# b#), :expected '~form, :actual "See differences below"})
             (do-report {:type :fail, :message ~msg, :expected a#, :actual b#})))
         result#))
     (do-report {:type :fail
                 :message (str "Form size was " ~(count form) ", expected exactly 3")
                 :expected '~form
                 :actual "n/a"})))

(defn run-all-unit-tests
  "Finds all namespaces in the test directory and runs all tests in
  them, returning a map of shape:
    {:test ..., :pass ..., :fail ..., :error ..., :type :summary}"
  []
  (let [namespaces (find/find-namespaces-in-dir (io/file "./test"))]
    (dorun (map #(require %) namespaces))
    (apply run-tests namespaces)))

(defn run-all-unit-tests-and-throw
  "Calls run-all-unit-tests and throws if anything failed or
  errored. Call this from the REPL in a dotimes if you want to try
  and reproduce flakes:

    (require '[waiter.test-helpers :as th])
    (dotimes [i 10]
      (println i)
      (th/run-all-unit-tests-and-throw))
  "
  []
  (let [{:keys [test pass fail error] :as m} (run-all-unit-tests)]
    (when-not (> test 0)
      (throw (ex-info "0 tests ran" m)))
    (when-not (> pass 0)
      (throw (ex-info "0 assertions passed" m)))
    (when-not (= fail 0)
      (throw (ex-info (str fail " failure(s)") m)))
    (when-not (= error 0)
      (throw (ex-info (str error " error(s)") m)))))

