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
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [demoapp.search :as search]
            [demoapp.tagging :as tagging]
            [demoapp.utils :as utils]
            [qbits.jet.server :as server]
            [ring.middleware.params :as params])
  (:import (java.util UUID))
  (:gen-class))

(defn- image-search-handler
  "Performs image search."
  [{:keys [query-params]}]
  (log/info "query params:" query-params)
  (let [{:strs [search]} query-params
        image-entries (search/image-search search)]
    (utils/map->json-response {:result image-entries})))

(defn- image-tagging-handler
  "Performs image tagging."
  [{:keys [query-params]}]
  (log/info "query params:" query-params)
  (let [{:strs [id name url predictions]} query-params
        {:keys [exit-code] :as tagging-result} (tagging/perform-image-tagging id name url predictions)]
    (utils/map->json-response tagging-result :status (if (zero? exit-code) 200 500))))

(defn status-handler
  "The handler for status requests."
  [_]
  {:body "OK"
   :headers {"content-type" "text/plain"}
   :status 200})

(defn demo-ui-handler
  "The handler for rendering the demo ui."
  [{:keys [query-params]}]
  (let [{:strs [waiter-port]} query-params
        waiter-port (or waiter-port "9091")]
    {:body (-> "demo.html"
               io/resource
               slurp
               (str/replace "var waiter_port = '9091'"
                            (str "var waiter_port = '" waiter-port "'")))
     :headers {"content-type" "text/html"}
     :status 200}))

(defn http-handler
  [{:keys [uri] :as request}]
  (case uri
    "/image-search" (image-search-handler request)
    "/image-tagging" (image-tagging-handler request)
    "/status" (status-handler request)
    (demo-ui-handler request)))

(defn attach-request-id-middleware
  "Attaches a request id to the request and to the response header."
  [handler]
  (fn attach-request-id-middleware-fn [request]
    (let [request-id (str (UUID/randomUUID))
          request' (assoc request :request-id request-id)
          response (handler request')]
      (update response :headers assoc "x-request-id" request-id))))

(defn request-log-middleware
  "Logs the request."
  [handler]
  (fn request-log-middleware-fn [{:keys [headers request-id request-method uri] :as request}]
    (log/info (str "request received request-id: " request-id
                   ", uri:" uri
                   ", method" request-method
                   ", headers:" (into (sorted-map) headers)))
    (handler request)))

(defn render-exception-middleware
  "Renders exceptions thrown by the nested handler."
  [handler]
  (fn render-exception-middleware-fn [{:keys [request-id] :as request}]
    (try
      (handler request)
      (catch Exception e
        (log/error e "encountered exception while processing request-id" request-id)
        (utils/exception->json-response e)))))

(defn allow-cors-request-middleware
  "Attaches the access-control headers to allow CORS requests."
  [handler]
  (fn allow-cors-request-middleware-fn [request]
    (let [{:keys [headers] :as response} (handler request)
          {:strs [content-type]} headers]
      (cond-> response
              (= content-type "application/json")
              (update :headers assoc
                      "access-control-allow-origin" "*"
                      "access-control-allow-headers", "origin, x-requested-with, content-type, accept")))))

(defn -main
  [& args]
  (log/info "command line arguments:" (vec args))
  (let [cli-options [["-h" "--help"]
                     ["-p" "--port PORT" "Port number"
                      :default 8080
                      :parse-fn #(Integer/parseInt %)
                      :validate [#(< 1000 % 0x10000) "Must be between 1000 and 65536"]]]
        {:keys [options summary]} (cli/parse-opts args cli-options)
        {:keys [help port]} options]
    (try
      (if help
        (println summary)
        (do
          (log/info "demoapp running on port" port)
          (let [server-options {:port port
                                :request-header-size 32768
                                :ring-handler (->> http-handler
                                                   render-exception-middleware
                                                   allow-cors-request-middleware
                                                   request-log-middleware
                                                   attach-request-id-middleware
                                                   params/wrap-params)}]
            (server/run-jetty server-options))))
      (shutdown-agents)
      (catch Exception e
        (log/fatal e "Encountered error starting demoapp with" options)
        (System/exit 1)))))
