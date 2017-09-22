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
(defproject token-syncer "0.1.0-SNAPSHOT"
  :dependencies [^{:voom {:repo "https://github.com/twosigma/jet.git" :branch "waiter-patch"}}
                 [cc.qbits/jet "0.7.10-20170801_153701-gf517b70"]
                 [commons-codec/commons-codec "1.10"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.5"]
                 [prismatic/plumbing "0.5.4"]]
  :filespecs [{:type :fn
               :fn (fn [p]
                     {:type :bytes :path "git-log"
                      :bytes (.trim (:out (clojure.java.shell/sh
                                            "git" "rev-parse" "HEAD")))})}]
  :main ^:skip-aot token-syncer.main
  :plugins [[lein-voom "0.1.0-20150115_230705-gd96d771"]]
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
             :uberjar {:aot :all}}
  :resource-paths ["resources"]
  :target-path "target/%s"
  :test-paths ["test" "integration"]
  :test-selectors {:default (every-pred (complement :dev) (complement :integration))
                   :dev :dev
                   :integration (every-pred :integration (complement :explicit))}
  :uberjar-name ~(System/getenv "UBERJAR_NAME"))
