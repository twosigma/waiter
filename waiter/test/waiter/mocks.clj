;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns waiter.mocks
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [full.async :refer [<? <?? go-try]]))

(defn mock-reservation-system
  [instance-rpc-chan mock-fns]
  (async/thread
    (try
      (loop [mock (first mock-fns)
             remaining (rest mock-fns)]
        (let [{:keys [response-chan]} (async/<!! instance-rpc-chan)
              c (async/chan 1)]
          (async/>!! response-chan c)
          (mock (async/<!! c)))
        (recur (first remaining) (rest remaining)))
      (catch Exception e
        (log/info e)
        (.printStackTrace e)))))
