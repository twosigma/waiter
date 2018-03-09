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

(defn wrap-context
  "Wraps a handler, ensuring the request and response (or exception) contains data in the context map."
  [handler context]
  (fn [request]
    (try
      (let [response (handler (merge request context))]
        (if (au/chan? response)
          (async/go
            (try
              (merge (<? response) context)
              (catch Exception e
                (utils/merge-exception e context))))
          (merge response context)))
      (catch Exception e
        (throw (utils/merge-exception e context))))))
