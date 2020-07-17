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
(defproject jwks-server "0.1.0-SNAPSHOT"
  :dependencies [[buddy/buddy-sign "3.1.0"]
                 [cheshire "5.9.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "0.4.500"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.28"]
                 [prismatic/plumbing "0.5.5"]
                 [ring/ring-core "1.7.1"
                  :exclusions [org.clojure/tools.reader]]
                 [twosigma/jet "0.7.10-20200709_171405-gea4b7b6"
                  :exclusions [org.eclipse.jetty/jetty-client
                               org.eclipse.jetty.alpn/alpn-api
                               org.eclipse.jetty.http2/http2-client
                               org.eclipse.jetty.websocket/websocket-client
                               org.eclipse.jetty/jetty-alpn-java-client
                               org.mortbay.jetty.alpn/alpn-boot]]]
  :jvm-opts ["-server"
             "-XX:+UseG1GC"
             "-XX:MaxGCPauseMillis=50"]
  :main ^:skip-aot jwks-server.main
  :profiles {:uberjar {:aot :all}}
  :resource-paths ["resources"]
  :target-path "target/%s"
  :test-selectors {:default (every-pred (complement :dev) (complement :integration))
                   :dev :dev
                   :integration (every-pred :integration (complement :explicit))}
  :uberjar-name ~(System/getenv "UBERJAR_NAME"))
