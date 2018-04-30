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
(ns waiter.middleware
  (:require [clojure.core.async :as async]
            [full.async :refer (<?? <? go-try)]
            [waiter.util.async-utils :as au]
            [waiter.util.utils :as utils]))

(defn wrap-update
  "Wraps a handler, calling update on the request and the response.
  If there was an error, also updates the exception."
  ([handler update-fn]
   (wrap-update handler update-fn update-fn))
  ([handler req-update-fn res-update-fn]
   (fn [request]
     (try
       (let [response (handler (req-update-fn request))]
         (if (au/chan? response)
           (async/go
             (try
               (res-update-fn (<? response))
               (catch Exception e
                 (utils/update-exception e res-update-fn))))
           (res-update-fn response)))
       (catch Exception e
         (throw (utils/update-exception e res-update-fn)))))))

(defn wrap-assoc
  "Wraps a handler, calling assoc on the request and assoc-if-absent on the response.
  If there was an error, also calls assoc on the exception."
  [handler k v]
  (let [req-update-fn #(assoc % k v)
        res-update-fn #(utils/assoc-if-absent % k v)]
    (wrap-update handler req-update-fn res-update-fn)))

(defn wrap-merge
  "Wraps a handler, calling merge on the request and the response.
  If there was an error, also calls merge on the exception."
  [handler m]
  (wrap-update handler #(merge % m)))
