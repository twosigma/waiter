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
(ns waiter.util.cache-utils
  (:require [clojure.core.cache :as cache]))

(defn atom-cache-get-or-load
  "Gets a value from a cache based upon the key.
   On cache miss, call get-fn with the key and place result into the cache in {:data value} form.
   This allows us to handle nil values as results of the get-fn."
  [cache key get-fn]
  (let [d (delay (get-fn))
        _ (swap! cache #(cache/through (fn [_] {:data @d}) % key))
        out (cache/lookup @cache key)]
    (if-not (nil? out) (:data out) @d)))

(defn atom-cache-evict
  "Evicts a key from an atom-based cache."
  [cache key]
  (swap! cache #(cache/evict % key)))
