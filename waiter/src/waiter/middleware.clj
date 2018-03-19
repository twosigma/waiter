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
            [waiter.async-utils :as au]
            [waiter.utils :as utils]))

(defn wrap-update
  "Wraps a handler, calling update on the request and the response.
  If there was an error, also updates the exception."
  [handler update-fn]
  (fn [request]
    (try
      (let [response (handler (update-fn request))]
        (if (au/chan? response)
          (async/go
            (try
              (update-fn (<? response))
              (catch Exception e
                (utils/update-exception e update-fn))))
          (update-fn response)))
      (catch Exception e
        (throw (utils/update-exception e update-fn))))))

(defn wrap-assoc
  "Wraps a handler, calling assoc on the request and the response.
  If there was an error, also calls assoc on the exception."
  [handler k v]
  (wrap-update handler #(assoc % k v)))

(defn wrap-merge
  "Wraps a handler, calling merge on the request and the response.
  If there was an error, also calls merge on the exception."
  [handler m]
  (wrap-update handler #(merge % m)))
