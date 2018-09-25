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

(defn cache-factory
  "Creates a cache wrapped in an atom.
   The argument to the method is a map that contains the threshold and ttl keys.
   The threshold argument defines the maximum number of elements in the cache.
   The ttl argument that defines the time (in millis) that entries are allowed to reside in the cache."
  [{:keys [threshold ttl]}]
  (cond-> {}
    threshold (cache/fifo-cache-factory :threshold threshold)
    ttl (cache/ttl-cache-factory :ttl ttl)
    true atom))

(defn cache-contains?
  "Checks if the cache contains a value associated with cache-key."
  [cache-atom cache-key]
  (cache/has? @cache-atom cache-key))

(defn atom-cache-get-or-load
  "Gets a value from a cache based upon the key.
   On cache miss, call get-fn with the key and place result into the cache in {:data value} form.
   This allows us to handle nil values as results of the get-fn."
  [cache-atom key get-fn]
  (let [d (delay (get-fn))
        _ (swap! cache-atom #(cache/through (fn [_] {:data @d}) % key))
        out (cache/lookup @cache-atom key)]
    (if-not (nil? out) (:data out) @d)))

(defn atom-cache-evict
  "Evicts a key from an atom-based cache."
  [cache-atom key]
  (swap! cache-atom #(cache/evict % key)))
