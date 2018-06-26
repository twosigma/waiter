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
(defproject kitchen "0.1.0-SNAPSHOT"
  :dependencies [^{:voom {:repo "https://github.com/twosigma/jet.git" :branch "untyped-content-provider-default"}}
                 [cc.qbits/jet "0.7.10-20180626_194651-g716c5e0"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [prismatic/plumbing "0.5.3"]
                 [ring/ring-core "1.4.0"]
                 [ring-basic-authentication "1.0.5"]]
  :plugins [[lein-voom "0.1.0-20171225_233657-g7962d1d"]]
  :main ^:skip-aot kitchen.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :override-maven {:local-repo ~(System/getenv "WAITER_MAVEN_LOCAL_REPO")}}
  :uberjar-name ~(System/getenv "UBERJAR_NAME")
  :resource-paths ["resources"])
