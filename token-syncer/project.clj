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
(defproject token-syncer "0.1.0-SNAPSHOT"
  :dependencies [[twosigma/jet "0.7.10-20191230_163338-g0dabe28"]
                 [clj-time "0.15.2"]
                 [commons-codec/commons-codec "1.13"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "0.5.527"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.4.2"]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.29"]
                 [prismatic/plumbing "0.5.5"]]
  :jvm-opts ["-server"
             "-XX:+UseG1GC"
             "-XX:MaxGCPauseMillis=50"]
  :main ^:skip-aot token-syncer.main
  :profiles {:default {:jvm-opts ["-Dlog4j.configuration=log4j.properties"]}
             :spnego {:jvm-opts ["-Djava.security.krb5.conf=/etc/krb5.conf"
                                 "-Djavax.security.auth.useSubjectCredsOnly=false"
                                 "-Dsun.security.jgss.lib=/opt/mitkrb5/lib/libgssapi_krb5.so"
                                 "-Dsun.security.jgss.native=true"
                                 "-Dsun.security.krb5.debug=true"]}
             :test {:jvm-opts ["-Dlog4j.configuration=log4j-test.properties"]}
             :uberjar {:aot :all}}
  :resource-paths ["resources"]
  :target-path "target/%s"
  :test-paths ["test" "integration"]
  :test-selectors {:default (every-pred (complement :dev) (complement :integration))
                   :dev :dev
                   :integration (every-pred :integration (complement :explicit))}
  :uberjar-name ~(System/getenv "UBERJAR_NAME"))
