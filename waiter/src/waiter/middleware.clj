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
  "Wraps a handler, calling update on the request and response."
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
