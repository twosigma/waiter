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
  (:require [clojure.tools.logging :as log])
  (:import [com.google.common.cache Cache CacheBuilder RemovalListener RemovalNotification]
           [java.util.concurrent TimeUnit]))

(defn cache-factory
  "Creates a cache wrapped in an atom.
   The argument to the method is a map that contains the threshold and ttl keys.
   The threshold argument defines the maximum number of elements in the cache.
   The ttl argument that defines the time (in millis) that entries are allowed to reside in the cache."
  [{:keys [threshold ttl]}]
  (-> (cond-> (CacheBuilder/newBuilder)
        threshold (.maximumSize threshold)
        ttl (.expireAfterWrite ttl TimeUnit/MILLISECONDS))
      (.removalListener (proxy [RemovalListener] []
                          (onRemoval [^RemovalNotification n]
                            (log/info "cache entry removal notification"
                                      {:evicted (.wasEvicted n)
                                       :key (.getKey n)}))))
      (.build)))

(defn cache-contains?
  "Checks if the cache contains a value associated with cache-key."
  [^Cache cache cache-key]
  (-> (.getIfPresent cache cache-key)
      nil?
      not))

(defn cache-size
  "Returns the approximate number of entries in this cache."
  [^Cache cache]
  (.size cache))

(defn cache-get-or-load
  "Gets a value from a cache based upon the key.
   On cache miss, call get-fn with the key and place result into the cache in {:data value} form.
   This allows us to handle nil values as results of the get-fn."
  [^Cache cache key get-fn]
  (-> cache
      (.get key (fn cache-loader [] {:data (get-fn)}))
      :data))

(defn cache-evict
  "Evicts a key from an atom-based cache."
  [^Cache cache key]
  (.invalidate cache key))

(defn cache->map
  "Returns the entries stored in the cache as a map."
  [^Cache cache]
  (into {} (.asMap cache)))
