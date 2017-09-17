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
(ns waiter.main
  (:require [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [metrics.jvm.core :as jvm-metrics]
            [plumbing.core :as pc]
            [plumbing.graph :as graph]
            [qbits.jet.server :as server]
            [schema.core :as s]
            [waiter.core :as core]
            [waiter.correlation-id :as cid]
            [waiter.settings :as settings]
            [waiter.utils :as utils])
  (:import clojure.core.async.impl.channels.ManyToManyChannel
           java.io.IOException
           javax.servlet.ServletInputStream)
  (:gen-class))

(defn retrieve-git-version []
  (try
    (let [git-log-resource (io/resource "git-log")
          git-version-str (if git-log-resource
                            (do
                              (log/info "Retrieving version from git-log file")
                              (slurp git-log-resource))
                            (do
                              (log/info "Attempting to retrieve version from git repository")
                              (str/trim (:out (sh/sh "git" "rev-parse" "HEAD")))))]
      (log/info "Git version:" git-version-str)
      git-version-str)
    (catch Exception e
      (log/error e "version unavailable")
      "n/a")))

(defn- consume-request-stream [handler]
  (fn [{:keys [body] :as request}]
    (let [resp (handler request)]
      (if (and (instance? ServletInputStream body)
               (not (.isFinished body))
               (not (instance? ManyToManyChannel resp)))
        (try
          (slurp body)
          (catch IOException e
            (log/error e "Unable to consume request stream"))))
      resp)))

(defn wire-app
  [settings]
  {:settings (pc/fnk dummy-symbol-for-fnk-schema-logic :- settings/settings-schema [] settings)
   :curator core/curator
   :routines core/routines
   :daemons core/daemons
   :handlers core/request-handlers
   :state core/state
   :http-server (pc/fnk [[:routines waiter-request?-fn websocket-request-authenticator]
                         [:settings host port websocket-config support-info]
                         handlers] ; Insist that all systems are running before we start server
                  (let [options (merge websocket-config
                                       {:ring-handler (-> (core/ring-handler-factory waiter-request?-fn handlers)
                                                          core/correlation-id-middleware
                                                          (core/wrap-support-info support-info)
                                                          consume-request-stream)
                                        :websocket-acceptor websocket-request-authenticator
                                        :websocket-handler (-> (core/websocket-handler-factory handlers)
                                                               core/correlation-id-middleware
                                                               (core/wrap-support-info support-info))
                                        :host host
                                        :join? false
                                        :max-threads 250
                                        :port port
                                        :request-header-size 32768})]
                    (server/run-jetty options)))})

(defn start-waiter [config-file]
  (try
    (cid/replace-pattern-layout-in-log4j-appenders)
    (log/info "starting waiter...")
    (let [async-threads (System/getProperty "clojure.core.async.pool-size")
          settings (assoc (settings/load-settings config-file (retrieve-git-version))
                     :async-threads async-threads
                     :started-at (utils/date-to-str (t/now)))]
      (log/info "core.async threadpool configured to use" async-threads "threads.")
      (log/info "loaded settings:\n" (with-out-str (clojure.pprint/pprint settings)))
      (let [app-map (wire-app settings)]
        ((graph/eager-compile app-map) {})))
    (catch Throwable e
      (log/fatal e "encountered exception starting waiter")
      (utils/exit 1 (str "Exiting: " (.getMessage e))))))

(defn validate-config-schema
  [config-file]
  (println "Validating schema of" config-file "...")
  (try
    (s/validate settings/settings-schema (settings/load-settings config-file (retrieve-git-version)))
    (utils/exit 0 "Schema is valid.")
    (catch Throwable e
      (println "Schema is invalid.")
      (utils/exit 1 (.getMessage e)))))

(def cli-options
  [["-s" "--schema-validate" "Only validate the configuration schema"]])

(defn parse-options
  [args]
  (let [{:keys [options]} (cli/parse-opts args cli-options)
        validate-config (:schema-validate options)]
    {:validate-config validate-config}))

(defn -main
  [config & args]
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread throwable]
        (log/error throwable (str (.getName thread) " threw exception: " (.getMessage throwable))))))
  (jvm-metrics/instrument-jvm)
  (let [{:keys [validate-config]} (parse-options args)]
    (if validate-config
      (validate-config-schema config)
      (start-waiter config))))
