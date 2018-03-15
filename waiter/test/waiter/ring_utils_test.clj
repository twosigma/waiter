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
(ns waiter.ring-utils-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [waiter.ring-utils :refer :all]))

(deftest test-update-response
  (let [update-fn (fn [response] (assoc response :k :v))]
    (is (= {:k :v} (update-response {} update-fn)))
    (is (= {:k :v} (async/<!! (update-response (async/go {}) update-fn))))))
