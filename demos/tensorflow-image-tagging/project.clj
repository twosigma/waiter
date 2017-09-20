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
(defproject tensorflow-image-tagging "0.1.0-SNAPSHOT"
  :dependencies [[cc.qbits/jet "0.7.10"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ring/ring-core "1.4.0"]
                 [ring-basic-authentication "1.0.5"]]
  :main ^:skip-aot demoapp.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :uberjar-name ~(System/getenv "UBERJAR_NAME")
  :resource-paths ["resources"])
