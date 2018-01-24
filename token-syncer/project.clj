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
  :aliases {"test" ["with-profile" "+test" "test"]}
  :dependencies [^{:voom {:repo "https://github.com/twosigma/jet.git" :branch "waiter-patch"}}
[cc.qbits/jet "0.7.10-20180124_201515-g857338f"]
                 [commons-codec/commons-codec "1.10"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.21"]
                 [prismatic/plumbing "0.5.4"]]
  :main ^:skip-aot token-syncer.main
  :plugins [[lein-voom "0.1.0-20171225_233657-g7962d1d"]]
  :profiles {:default {:jvm-opts ["-Dlog4j.configuration=log4j.properties"]}
             :test {:jvm-opts ["-Dlog4j.configuration=log4j-test.properties"
                               "-XX:+PrintAdaptiveSizePolicy"
                               "-XX:+PrintGCDetails"
                               "-XX:+PrintGCTimeStamps"
                               "-XX:+PrintReferenceGC"
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
