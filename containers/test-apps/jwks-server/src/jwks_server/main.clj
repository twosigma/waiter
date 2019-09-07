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
(ns jwks-server.main
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [jwks-server.config :as config]
            [jwks-server.handler :as handler]
            [plumbing.core :as pc]
            [qbits.jet.server :as server])
  (:gen-class))

(defn- setup-exception-handler
  "Sets up the UncaughtExceptionHandler."
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread throwable]
        (log/error throwable (str (.getName thread) " threw exception: " (.getMessage throwable)))))))

(defn exit
  "Helper function that prints the message and triggers a System exit."
  [status message]
  (if (zero? status)
    (log/info message)
    (log/error message))
  (System/exit status))

(defn start-server
  "Starts the JWKS server on the specified port."
  [port]
  (log/info "starting server on port" port)
  (server/run-jetty {:host "127.0.0.1"
                     :join? false
                     :port port
                     :ring-handler handler/request-handler
                     :send-server-version? false}))

(defn -main
  "The main entry point."
  [& args]
  (setup-exception-handler)
  (try
    (log/info "command-line arguments:" (vec args))
    (when (< (count args) 3)
      (exit 1 "usage: port jwks-file settings-file"))
    (let [port (nth args 0)
          jwks-file (nth args 1)
          settings-file (nth args 2)]
      (when-not port
        (exit 1 "No port specified as first argument on the command-line"))
      (when-not jwks-file
        (exit 1 "No jwks file specified as second argument on the command-line"))
      (when-not settings-file
        (exit 1 "No settings file specified as third argument on the command-line"))
      (log/info "port:" port)
      (log/info "jwks file:" jwks-file)
      (log/info "settings file:" settings-file)
      (let [port-int (Integer/parseInt port)
            jwks (-> jwks-file slurp json/read-str pc/keywordize-map)
            settings (-> settings-file slurp edn/read-string)]
        (config/initialize-jwks jwks)
        (config/initialize-settings settings)
        (start-server port-int)))
    (catch Exception e
      (log/error e "error in initializing jwks server")
      (exit 1 (str "encountered error running jwks-server: " (.getMessage e))))))
