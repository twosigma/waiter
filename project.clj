(defproject kitchen "0.1.0-SNAPSHOT"
  :dependencies [^{:voom {:repo "https://gitlab.twosigma.com/platform-services/jet.git"}}
                 [cc.qbits/jet "0.7.9-20160712_151431-g5eef908"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [prismatic/plumbing "0.5.3"]
                 [ring/ring-core "1.4.0"]]
  :plugins [[lein-voom "0.1.0-20150115_230705-gd96d771"]]
  :main ^:skip-aot kitchen.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :uberjar-name ~(System/getenv "UBERJAR_NAME")
  :resource-paths ["resources"])
