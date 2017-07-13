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
(ns token-syncer.correlation-id
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [token-syncer.utils :as utils])
  (:gen-class))

(def ^:dynamic dynamic-correlation-id "UNKNOWN-CID")

(defmacro with-correlation-id
  "Executes the body with the specified value of correlation-id."
  [correlation-id & body]
  `(binding [dynamic-correlation-id ~correlation-id]
     ~@body))

(defn- ensure-cid
  [http-entity cid-fn]
  (->> (fn ensure-cid-helper [cid] (or cid (str (cid-fn))))
       (update-in http-entity [:headers "x-cid"])))

(defn info
  ([& messages]
   (log/logp :info dynamic-correlation-id (str/join \space messages))))

(defn error
  ([exception & messages]
   (log/logp :error exception dynamic-correlation-id (str/join \space messages))))

(defn correlation-id-middleware
  "Attaches an x-cid header to the request and response if one is not already provided."
  [handler]
  (fn correlation-id-handler [request]
    (let [request-cid (or (get-in request [:headers "x-cid"])
                          (str "cid.t" (.getId (Thread/currentThread)) "."
                               (apply str (repeatedly 10 #(rand-nth (map char (range 97 122)))))))
          constantly-request-cid (fn get-request-cid [] request-cid)
          {:keys [headers request-method uri] :as request} (ensure-cid request constantly-request-cid)]
      (with-correlation-id
        request-cid
        (info (when request-method (-> request-method name str/upper-case))
              (when uri uri)
              "headers:" (->> headers
                              (pc/map-vals #(utils/truncate % 50))
                              (into (sorted-map))
                              print
                              with-out-str))
        (let [response (handler request)]
          (if (map? response)
            (ensure-cid response constantly-request-cid)
            (async/go (ensure-cid (async/<! response) constantly-request-cid))))))))
