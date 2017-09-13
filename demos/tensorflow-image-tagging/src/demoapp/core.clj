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
(ns demoapp.core
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [demoapp.search :as search]
            [demoapp.tagging :as tagging]
            [demoapp.utils :as utils]
            [qbits.jet.server :as server]
            [ring.middleware.basic-authentication :as basic-authentication]
            [ring.middleware.params :as params]
            [clojure.string :as str])
  (:gen-class))

(defn- image-search-handler
  "Performs image search."
  [{:keys [query-params]}]
  (log/info "query params:" query-params)
  (try
    (let [{:strs [search]} query-params
          image-entries (search/image-search search)]
      (utils/map->json-response {:result image-entries}))
    (catch Exception e
      (log/error e "encountered exception while performing image search")
      (utils/exception->json-response e))))

(defn- image-tagging-handler
  "Performs image tagging."
  [{:keys [query-params]}]
  (log/info "query params:" query-params)
  (try
    (let [{:strs [id name url predictions]} query-params
          {:keys [exit-code] :as tagging-result} (tagging/perform-image-tagging id name url predictions)]
      (utils/map->json-response tagging-result :status (if (zero? exit-code) 200 500)))
    (catch Exception e
      (log/error e "encountered exception while tagging image")
      (utils/exception->json-response e))))

(defn status-handler
  "The handler for status requests."
  [_]
  {:body "OK"
   :headers {"content-type" "text/plain"}
   :status 200})

(defn demo-ui-handler
  "The handler for rendering the demo ui."
  [{:keys [headers]}]
  (let [{:strs [host]} headers]
    {:body (-> "demo.html"
               io/resource
               slurp
               (str/replace "127.0.0.1:9091" host))
     :headers {"content-type" "text/html"}
     :status 200}))

(defn http-handler
  [{:keys [uri] :as request}]
  (try
    (case uri
      "/image-search" (image-search-handler request)
      "/image-tagging" (image-tagging-handler request)
      "/status" (status-handler request)
      (demo-ui-handler request))
    (catch Exception e
      (log/error e "handler: encountered exception")
      (utils/exception->json-response e))))

(defn basic-auth-middleware
  "Adds support for basic authentication when both the provided username and password are not nil.
   /status urls always bypass basic auth check."
  [username password handler]
  (if (not (and username password))
    (do
      (log/info "basic authentication is disabled since username or password is missing")
      handler)
    (fn basic-auth-middleware-fn [{:keys [uri] :as request}]
      (cond
        (= "/status" uri) (status-handler request)
        :else ((basic-authentication/wrap-basic-authentication
                 handler
                 (fn [u p]
                   (let [result (and (= username u)
                                     (= password p))]
                     (log/info "authenticating" u (if result "successful" "failed"))
                     result)))
                request)))))

(defn request-log-middleware
  "Logs the request."
  [handler]
  (fn correlation-id-middleware-fn [{:keys [headers request-method uri] :as request}]
    (log/info (str "request received uri:" uri ", method" request-method ", headers:" (into (sorted-map) headers)))
    (handler request)))

(defn -main
  [& args]
  (log/info "command line arguments:" (vec args))
  (let [cli-options [["-h" "--help"]
                     ["-p" "--port PORT" "Port number"
                      :default 8080
                      :parse-fn #(Integer/parseInt %)
                      :validate [#(< 1000 % 0x10000) "Must be between 1000 and 65536"]]]
        {:keys [options summary]} (cli/parse-opts args cli-options)
        {:keys [help port]} options
        username (System/getenv "WAITER_USERNAME")
        password (System/getenv "WAITER_PASSWORD")]
    (try
      (if help
        (println summary)
        (do
          (log/info "demoapp running on port" port)
          (let [server-options {:port port
                                :request-header-size 32768
                                :ring-handler (->> (basic-auth-middleware username password http-handler)
                                                   request-log-middleware
                                                   params/wrap-params)}]
            (server/run-jetty server-options))))
      (shutdown-agents)
      (catch Exception e
        (log/fatal e "Encountered error starting demoapp with" options)
        (System/exit 1)))))
