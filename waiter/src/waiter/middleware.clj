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
  "Wraps a handler, calling assoc on the request and the response.
  If there was an error, also calls assoc on the exception."
  [handler k v]
  (wrap-update handler #(assoc % k v)))

(defn wrap-merge
  "Wraps a handler, calling merge on the request and the response.
  If there was an error, also calls merge on the exception."
  [handler m]
  (wrap-update handler #(merge % m)))
