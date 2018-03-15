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
            [waiter.async-utils :as au]))

(defn update-response
  "Updates a response, handling the case where it may be a chan."
  [response response-fn]
  (if (au/chan? response)
    (async/go (response-fn (async/<! response)))
    (response-fn response)))
