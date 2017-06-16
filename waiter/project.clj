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
                   :dev :dev
                   :perf (every-pred :perf (complement :explicit))}

  :dependencies [[bidi "2.0.16"
                  :exclusions [prismatic/schema ring/ring-core]]
                 ^{:voom {:repo "https://github.com/twosigma/jet.git" :branch "waiter-patch"}}
                 [cc.qbits/jet "0.7.10-20170524_203939-gd23596c"]
                 ^{:voom {:repo "https://github.com/twosigma/clj-http.git" :branch "waiter-patch"}}
                 [clj-http "1.0.2-20170524_085846-g161c42f"
                  :exclusions [commons-io org.clojure/tools.reader potemkin slingshot]]
                 [clj-time "0.12.0"
                  :exclusions
                  [joda-time]]
                 [com.taoensso/nippy "2.12.2"
                  :exclusions [org.clojure/clojure org.clojure/tools.reader]]
                 [comb "0.1.0"
                  :exclusions [org.clojure/clojure]]
                 [digest "1.4.5"]
                 [fullcontact/full.async "0.9.0"
                  :exclusions [org.clojure/clojure org.clojure/core.async]]
                 [jarohen/chime "0.1.9"
                  :exclusions
                  [org.clojure/clojure
                   clj-time/clj-time
                   org.clojure/core.async]]
                 [joda-time "2.9.4"]
                 [marathonclj "0.1.1"
                  :exclusions
                  [clj-http
                   org.clojure/clojure
                   org.clojure/data.json]]
                 ^{:voom {:repo "https://github.com/twosigma/metrics-clojure.git" :branch "waiter-patch"}}
                 [metrics-clojure "2.6.0-20170531_164957-gd0d2c2c"
                  :exclusions [org.clojure/clojure io.netty/netty org.slf4j/slf4j-api]]
                 [org.apache.curator/curator-framework "2.11.0"
                  :exclusions [io.netty/netty org.slf4j/slf4j-api]]
                 [org.apache.curator/curator-recipes "2.11.0"
                  :exclusions [io.netty/netty org.slf4j/slf4j-api]]
                 [org.apache.curator/curator-test "2.11.0"
                  :exclusions [io.netty/netty]]
                 [org.apache.curator/curator-x-discovery "2.11.0"
                  :exclusions [io.netty/netty org.slf4j/slf4j-api]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.442"
                  :exclusions [org.clojure/clojure org.clojure/tools.reader]]
                 [org.clojure/core.cache "0.6.5"]
                 [org.clojure/core.memoize "0.5.9"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/data.codec "0.1.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.priority-map "0.0.7"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.clojure/tools.reader "0.10.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.21"
                  :exclusions [log4j]]
                 [potemkin "0.4.1"] ;; needed by clj-http, upgraded for clojure 1.8.0
                 [prismatic/plumbing "0.5.3"]
                 [prismatic/schema "1.1.3"
                  :exclusions [potemkin]]
                 [ring/ring-core "1.5.0"
                  :exclusions [org.clojure/tools.reader]]
                 [ring-basic-authentication "1.0.5"]
                 [slingshot "0.12.2"]]

  :resource-paths ["resources"]
  :main waiter.main
  :plugins [[lein-voom "0.1.0-20150115_230705-gd96d771"
             :exclusions [org.clojure/clojure]]
            [test2junit "1.2.2"]
            [com.holychao/parallel-test "0.3.1"]]
  :global-vars {*warn-on-reflection* true}
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
  :profiles {:test-log {:jvm-opts
                        ["-Dlog4j.configuration=log4j-test.properties"]}
             :test-repl {:jvm-opts
                         ["-Dlog4j.configuration=log4j-repl.properties"
                          "-XX:+PrintGCDetails"
                          "-XX:+PrintGCTimeStamps"
                          "-XX:+PrintReferenceGC"
                          "-XX:+PrintAdaptiveSizePolicy"
                          "-Xmx512m"
                          "-Xloggc:log/gc.log"]}
             :test-console {:jvm-opts
                            ["-Dlog4j.configuration=log4j-console.properties"]}
             :test {:jvm-opts
                    [~(str "-Dwaiter.test.kitchen.cmd=" (or
                                                          (System/getenv "WAITER_TEST_KITCHEN_CMD")
                                                          (.getCanonicalPath (clojure.java.io/file "../kitchen/bin/run.sh"))))]}}
  :uberjar-name ~(System/getenv "UBERJAR_NAME"))
