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
(ns waiter.test-helpers
  (:require [clj-jgit.porcelain :as jgit]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.tools.namespace.find :as find]
            [qbits.jet.client.http :as http]
            [waiter.correlation-id :as cid]
            [waiter.util.client-tools :as ct]
            [waiter.util.date-utils :as du])
  (:import java.io.ByteArrayOutputStream
           (java.net InetAddress)
           (javax.servlet ServletOutputStream ServletResponse)))

(def expected-html-response-headers {"content-type" "text/html"
                                     "server" "waiter"})

(def expected-json-response-headers {"content-type" "application/json"
                                     "server" "waiter"})

(def expected-text-response-headers {"content-type" "text/plain"
                                     "server" "waiter"})

(def ^:const ANSI-RESET "\033[0m")
(def ^:const ANSI-BLUE "\033[34m")
(def ^:const ANSI-CYAN "\033[36m")
(def ^:const ANSI-MAGENTA "\033[1m\033[35m")

(defonce ^:private replaced-layout
         (future (cid/replace-pattern-layout-in-log4j-appenders)))

(defonce ^:private test-metrics-es-url
         (System/getenv "TEST_METRICS_ES_URL"))

(defonce ^:private git-repo
         (when test-metrics-es-url
           (try
             (jgit/load-repo (str (System/getProperty "user.dir") "/.."))
             (catch Throwable _
                    (log/warn "Could not get git repo when trying to report test metrics.")))))

(defonce ^:private current-git-branch
         (when git-repo
           (jgit/git-branch-current git-repo)))

(defonce ^:private current-git-commit
         (when git-repo
           (-> git-repo jgit/git-log first .getId .name)))

(defonce ^:private test-name->num-fails-atom
         (atom {}))

(defonce ^:private test-name->num-errors-atom
         (atom {}))

(defonce ^:private test-name->num-passes-atom
         (atom {}))

(defonce ^:private http-client
         (when test-metrics-es-url
           (:http1-client (ct/make-http-clients))))

(defn- test-name-info
  [testing-var]
  (let [test-meta (meta testing-var)
        namespace (ns-name (:ns test-meta))
        name (:name test-meta)]
    {:full-name (str name "/" namespace)
     :name name
     :namespace namespace}))

(defn- full-test-name
  ([]
   (:full-name (test-name-info (first *testing-vars*))))
  ([m]
   (:full-name (test-name-info (:var m)))))

(defn- inc-test-counter
  [test-name->counter-atom]
  (swap! test-name->counter-atom (fn [map] (update-in map [(full-test-name)] #(inc (or % 0))))))

(defonce ^:private username
         (ct/retrieve-username))

(defonce ^:private hostname
         (.getCanonicalHostName (InetAddress/getLocalHost)))

(defn blue [message] (str ANSI-BLUE message ANSI-RESET))
(defn magenta [message] (str ANSI-MAGENTA message ANSI-RESET))
(defn cyan [message] (str ANSI-CYAN message ANSI-RESET))


(defn- log-memory-info
  "Logs memory usage information"
  []
  (let [runtime (Runtime/getRuntime)
        free-mem (.freeMemory runtime)
        total-mem (.totalMemory runtime)
        used-mem (- total-mem free-mem)
        max-mem (.maxMemory runtime)]
    (log/debug "free memory:" free-mem "total memory:" total-mem "used memory:" used-mem "max memory:" max-mem)))

(defn- format-duration
  "Formats a duration in ms."
  [ms]
  (cond (< ms 1e4) (str ms "ms")
        :else (str (int (/ ms 1e3)) "s")))

(let [running-tests (atom {})
      start-millis (atom {})
      test-durations (atom {})]
  (defn- log-running-tests []
    (let [tests @running-tests]
      (log/debug (count tests) "running test(s):" tests))
    (log-memory-info))

  (defmethod report :begin-test-var [m]
    (let [test-name (full-test-name m)]
      @replaced-layout
      (with-test-out
        (println \tab (magenta "START: ") test-name))
      (swap! start-millis #(assoc % test-name (System/currentTimeMillis)))
      (swap! running-tests #(assoc % test-name (str (t/now))))
      (log-running-tests)))

  (defmethod report :end-test-var [m]
    (let [test-name (full-test-name m)
          elapsed-millis (- (System/currentTimeMillis) (get @start-millis test-name))]
      (swap! test-durations #(assoc % test-name elapsed-millis))
      (swap! running-tests #(dissoc % test-name))
      (with-test-out
        (println \tab (blue "FINISH:") test-name (cyan (format-duration elapsed-millis))
                 (assoc @*report-counters* :running (count @running-tests)))
        (when test-metrics-es-url
          (let [{:keys [full-name name namespace]} (test-name-info (:var m))
                num-fails (or (@test-name->num-fails-atom full-name) 0)
                num-errors (or (@test-name->num-errors-atom full-name) 0)
                num-passes (or (@test-name->num-passes-atom full-name) 0)
                test-skipped? (= 0 (+ num-fails num-errors num-passes))
                test-failed? (> (+ num-fails num-errors) 0)
                result (if test-failed? "failed" (if test-skipped? "skipped" "passed"))
                es-index (str "waiter-tests-" (du/date-to-str (t/now) (f/formatters :basic-date)))
                doc-id (str namespace "_" name "_" (.getMillis (t/now)))]
            ;TODO: can check for outstanding commits: (println (jgit/git-status git-repo))
            ; ct/make-request strips trailing slash from test-metrics-es-url
            (ct/make-request test-metrics-es-url (str "/" es-index "/test-result/" doc-id)
                             :body (json/write-str {:timestamp (du/date-to-str (t/now))
                                                    :project "waiter"
                                                    :test-namespace namespace
                                                    :test-name name
                                                    :git-branch current-git-branch
                                                    :git-commit-hash current-git-commit
                                                    :git-branch-under-test (System/getenv "TEST_METRICS_BRANCH_UNDER_TEST")
                                                    :git-commit-hash-under-test (System/getenv "TEST_METRICS_COMMIT_HASH_UNDER_TEST")
                                                    :host hostname
                                                    :user username
                                                    :run-id (System/getenv "")
                                                    :build-id (System/getenv "")
                                                    :result result
                                                    :runtime-ms elapsed-millis})
                             :client http-client
                             :headers {"content-type" "application/json"}
                             :method :post))))
      (log-running-tests)))

  (defmethod report :summary [m]
    (with-test-out
      (println "\nLongest running tests:")
      (doseq [[test-name duration] (->> @test-durations
                                        (sort-by second)
                                        (reverse)
                                        (take 10))]
        (println test-name (cyan (format-duration duration))))
      (println "\nRan" (:test m) "tests containing"
               (+ (:pass m) (:fail m) (:error m)) "assertions.")
      (println (:fail m) "failures," (:error m) "errors.")
      (when http-client (http/stop-client! http-client)))))

;; Overrides the default reporter for :error so that the ex-data of
;; an exception is printed.  The default report doesn't print the ex-data.
(defmethod report :error
  [m]
  (with-test-out
    (inc-report-counter :error)
    (println "\nERROR in" (testing-vars-str m))
    (when (seq *testing-contexts*) (println (testing-contexts-str)))
    (when-let [message (:message m)] (println message))
    (println "expected:" (pr-str (:expected m)))
    (print "  actual: " (pr-str (:actual m)))
    (inc-test-counter test-name->num-errors-atom)))

(defmethod report :pass
  [m]
  (with-test-out
    (inc-report-counter :pass)
    (inc-test-counter test-name->num-passes-atom)))

(defmethod report :fail
  [m]
  (with-test-out
    (inc-report-counter :fail)
    (println "\nFAIL in" (testing-vars-str m))
    (when (seq *testing-contexts*) (println (testing-contexts-str)))
    (when-let [message (:message m)] (println message))
    (println "expected:" (pr-str (:expected m)))
    (println "  actual:" (pr-str (:actual m)))
    (inc-test-counter test-name->num-fails-atom)))

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


(defn json-response->str
  "Accepts a function that takes a ServletResponse and returns the body generated"
  [body]
  (let [baos (ByteArrayOutputStream.)
        sos (proxy [ServletOutputStream] []
              (write
                ([b] (.write baos b))
                ([b o l] (.write baos b o l))))
        response (proxy [ServletResponse] []
                   (getOutputStream [] sos))]
    (body response)
    (str baos)))

(defn- process-streaming-body [{:keys [body headers] :as resp}]
  (if (and (= "application/json" (get headers "content-type"))
           (fn? body))
    (assoc resp :body (json-response->str body))
    resp))

(defn wrap-handler-json-response
  "Wraps a handler which returns a streaming json response and converts it to a string"
  [handler]
  (fn [& args]
    (process-streaming-body (apply handler args))))

(defn wrap-async-handler-json-response
  "Wraps an async handler which returns a streaming json response and converts it to a string"
  [handler]
  (fn [& args]
    (async/go
      (process-streaming-body (async/<! (apply handler args))))))

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
    (run! require namespaces)
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
    (when-not (pos? test)
      (throw (ex-info "0 tests ran" m)))
    (when-not (pos? pass)
      (throw (ex-info "0 assertions passed" m)))
    (when-not (zero? fail)
      (throw (ex-info (str fail " failure(s)") m)))
    (when-not (zero? error)
      (throw (ex-info (str error " error(s)") m)))))

