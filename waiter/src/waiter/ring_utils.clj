;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.ring-utils
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [ring.middleware.params :as ring-params]
            [ring.util.request :as ring-request]
            [waiter.async-utils :as au]))

(defn update-response
  "Updates a response, handling the case where it may be a chan."
  [response response-fn]
  (if (au/chan? response)
    (async/go (response-fn (async/<! response)))
    (response-fn response)))

(defn json-request
  "Tries to parse a request body as JSON, if error, throw 400."
  [{:keys [body] {:strs [content-type]} :headers :as request}]
  (try
    (assoc request :body (-> body slurp (json/read-str)))
    (catch Exception e
      (throw (ex-info "Invalid JSON payload" {:status 400} e)))))

(defn query-params-request
  "Like Ring's params-request, but doesn't try to pull params from the body."
  [request]
  (let [encoding (or (ring-request/character-encoding request) "UTF-8")]
    (ring-params/assoc-query-params request encoding)))
