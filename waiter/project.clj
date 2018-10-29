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
(defproject waiter "0.1.0-SNAPSHOT"
  :test-paths ["test" "integration"]
  :test-selectors {:default (every-pred
                              (complement :dev)
                              (complement :explicit)
                              (complement :integration-fast)
                              (complement :integration-slow)
                              (complement :perf))
                   :explicit :explicit
                   :integration (every-pred (some-fn :integration-slow :integration-fast) (complement :explicit))
                   :integration-slow (every-pred :integration-slow (complement :explicit))
                   :integration-fast (every-pred :integration-fast (complement :explicit))
                   :integration-heavy (every-pred :resource-heavy (some-fn :integration-slow :integration-fast) (complement :explicit))
                   :integration-lite (every-pred (complement :resource-heavy) (some-fn :integration-slow :integration-fast) (complement :explicit))
                   :dev :dev
                   :perf (every-pred :perf (complement :explicit))}

  :dependencies [[bidi "2.1.4"
                  :exclusions [prismatic/schema ring/ring-core]]
                 [twosigma/jet "0.7.10-20180627_133335-g2a9429e"]
                 [twosigma/clj-http "1.0.2-20180124_201819-gcdf23e5"
                  :exclusions [commons-codec commons-io org.clojure/tools.reader potemkin slingshot]]
                 [clj-time "0.15.1"
                  :exclusions [joda-time]]
                 [com.google.guava/guava "20.0"]
                 [com.taoensso/nippy "2.14.0"
                  :exclusions [org.clojure/clojure org.clojure/tools.reader]]
                 [comb "0.1.1"
                  :exclusions [org.clojure/clojure]]
                 [digest "1.4.8"
                  :exclusions [org.clojure/clojure]]
                 [fullcontact/full.async "1.0.0"
                  :exclusions [org.clojure/clojure org.clojure/core.async]]
                 [jarohen/chime "0.2.2"
                  :exclusions
                  [org.clojure/clojure
                   clj-time/clj-time
                   org.clojure/core.async]]
                 [joda-time "2.10"]
                 [twosigma/metrics-clojure "2.6.0-20180124_201441-g72cee16"
                  :exclusions [org.clojure/clojure io.netty/netty org.slf4j/slf4j-api]]
                 [metrics-clojure-jvm "2.10.0"
                  :exclusions [io.dropwizard.metrics/metrics-core
                               io.netty/netty
                               metrics-clojure
                               org.clojure/clojure
                               org.slf4j/slf4j-api]]
                 [org.apache.curator/curator-framework "2.11.0"
                  :exclusions [io.netty/netty org.slf4j/slf4j-api]]
                 [org.apache.curator/curator-recipes "2.11.0"
                  :exclusions [io.netty/netty org.slf4j/slf4j-api]]
                 [org.apache.curator/curator-test "2.11.0"
                  :exclusions [com.google.guava/guava
                               io.netty/netty]]
                 [org.apache.curator/curator-x-discovery "2.11.0"
                  :exclusions [io.netty/netty org.slf4j/slf4j-api]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.4.474"
                  :exclusions [org.clojure/clojure org.clojure/tools.reader]]
                 [org.clojure/core.memoize "0.7.1"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/data.codec "0.1.1"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.priority-map "0.0.10"]
                 [org.clojure/tools.cli "0.4.1"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.clojure/tools.reader "1.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.25"
                  :exclusions [log4j]]
                 [potemkin "0.4.5"]
                 [prismatic/plumbing "0.5.5"]
                 [prismatic/schema "1.1.9"]
                 [ring/ring-core "1.7.0"
                  :exclusions [org.clojure/tools.reader]]
                 [ring-basic-authentication "1.0.5"]
                 [slingshot "0.12.2"]
                 [try-let "1.2.0"
                  :exclusions [org.clojure/clojure]]]

  :resource-paths ["resources"]
  :main waiter.main
  :plugins [[test2junit "1.2.2"]
            [com.holychao/parallel-test "0.3.1"]]
  ; In case of kerberos problems, export KRB5_KTNAME=/var/spool/keytabs/$(id -un)
  :jvm-opts ["-server"
             "-Dsun.security.jgss.lib=/opt/mitkrb5/lib/libgssapi_krb5.so"
             "-Djava.security.krb5.conf=/etc/krb5.conf"
             "-Dsun.security.jgss.native=true"
             "-Dsun.security.krb5.debug=true"
             "-Djavax.security.auth.useSubjectCredsOnly=false"
             "-Dclojure.core.async.pool-size=64"
             ~(str "-Dwaiter.logFilePrefix=" (System/getenv "WAITER_LOG_FILE_PREFIX"))
             "-XX:+UseG1GC"
             "-XX:MaxGCPauseMillis=50"
             "-XX:PermSize=1g"]
  :filespecs [{:type :fn
               :fn (fn [p]
                     {:type :bytes :path "git-log"
                      :bytes (.trim (:out (clojure.java.shell/sh
                                            "git" "rev-parse" "HEAD")))})}]
  :profiles {:debug {:jvm-opts
                     ;; enable remote debugger to connect on port 5005
                     ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"]}
             :test {:jvm-opts
                    [~(str "-Dwaiter.test.kitchen.cmd=" (or (System/getenv "WAITER_TEST_KITCHEN_CMD")
                                                            (.getCanonicalPath (clojure.java.io/file "../kitchen/bin/kitchen"))))]
                    :parallel-test {:pools {:serial (constantly 1)
                                            :parallel (fn []
                                                        (or (some-> (System/getenv "LEIN_TEST_THREADS") Long/valueOf)
                                                            (.availableProcessors (Runtime/getRuntime))))}}}
             :test-console {:jvm-opts
                            ["-Dlog4j.configuration=log4j-console.properties"]}
             :test-log {:jvm-opts
                        ["-Dlog4j.configuration=log4j-test.properties"]}
             :test-repl {:jvm-opts
                         ["-Dlog4j.configuration=log4j-repl.properties"
                          "-XX:+PrintGCDetails"
                          "-XX:+PrintGCTimeStamps"
                          "-XX:+PrintReferenceGC"
                          "-XX:+PrintAdaptiveSizePolicy"
                          "-Xmx512m"
                          "-Xloggc:log/gc.log"]}
             :override-maven {:local-repo ~(System/getenv "WAITER_MAVEN_LOCAL_REPO")}}
  :uberjar-name ~(System/getenv "UBERJAR_NAME"))
