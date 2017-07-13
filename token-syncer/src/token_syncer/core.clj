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
(ns token-syncer.core
  (:require [clojure.tools.logging :as log]
            [qbits.jet.client.http :as http]
            [ring.middleware.basic-authentication :as basic-authentication]
            [token-syncer.correlation-id :as cid]
            [token-syncer.syncer :as syncer]
            [token-syncer.utils :as utils])
  (:gen-class)
  (:import (org.eclipse.jetty.client HttpClient)))

;;;;;;;;;;; HTTP handlers related ;;;;;;;;;;;;;;;;;;;;;;;;

(defn default-handler
  "The default handler of requests."
  [_]
  {:body "OK" :status 200})

(defn settings-handler
  "The default handler of requests."
  [settings _]
  (utils/map->json-response (into (sorted-map) settings)))

(defn sync-tokens-handler
  "The handler of token sync requests."
  [http-client {:keys [query-params]}]
  (let [router-url-param (get query-params "router-url")
        router-urls (if (string? router-url-param) [router-url-param] router-url-param)
        _ (when-not (seq router-urls)
            (throw (ex-info "missing router-url parameter!" {:query-params query-params})))
        result (syncer/sync-tokens http-client router-urls)]
    (utils/map->json-response (into (sorted-map) result))))

(defn- ^HttpClient http-client-factory
  "Creates an instance of HttpClient with the specified timeout."
  [{:keys [connection-timeout-ms idle-timeout-ms]}]
  (let [client (http/client {:connect-timeout connection-timeout-ms
                             :idle-timeout idle-timeout-ms
                             :follow-redirects? false})
        _ (.clear (.getContentDecoderFactories client))]
    client))

(defn http-handler-factory
  "Returns the http handler to be used by the token-syncer."
  [{:keys [http-client-properties] :as settings}]
  (let [http-client (http-client-factory http-client-properties)]
    (fn http-handler [{:keys [uri] :as request}]
      (try
        (condp = uri
          "/settings" (settings-handler settings request)
          "/sync-tokens" (sync-tokens-handler http-client request)
          (default-handler request))
        (catch Exception e
          (log/error e "handler: encountered exception")
          (utils/exception->json-response e))))))

;;;;;;;;;;; Basic Authentication ;;;;;;;;;;;;;;;;;;;;;;;;

(defn basic-auth-middleware [username password handler]
  "Performs basic authentication on incoming requests using the provided username and password."
  (fn basic-auth-handler [{:keys [uri] :as request}]
    (cond
      (not (and username password)) (handler request)
      (= "/status" uri) (handler request)
      :else ((basic-authentication/wrap-basic-authentication
               handler
               (fn [u p]
                 (cid/info "authenticating user" u)
                 (and (= username u)
                      (= password p))))
              request))))
