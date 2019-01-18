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
(ns waiter.kv
  (:require [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [digest]
            [metrics.meters :as meters]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.compression :as compression]
            [waiter.curator :as curator]
            [waiter.metrics :as metrics]
            [waiter.util.cache-utils :as cu]
            [waiter.util.utils :as utils])
  (:import java.util.Arrays
           org.apache.curator.framework.CuratorFramework))

(defprotocol KeyValueStore
  "A protocol for a simple key value store."
  (retrieve [this key refresh]
    "Fetch a value from the key value store.  If the key is missing,
    nil will be returned. `refresh` is used to ensure the 'freshest'
    copy is returned. This parameter is meant to be used by implementations
    that cache key-value pairs and is there to work around the Clojure
    limitation of not supporting variable arguments in defprotocol.")
  (store [this key value]
    "Store a value in the key value store.  If a key is already present,
    update the key's value with the supplied value.  There are no
    requirements or gaurantees as to the return value provided by this
    function.")
  (delete [this key]
    "Deletes a key-value from the key value store if the key exists.
     There are no guarantees for what happens when the key does not
     exist in the store.")
  (state [this]
    "Retrieves the state of the key-value store."))

(defn fetch
  "Wrapper to work around Clojure's limitation of not supporting variable argument lists in defprotocol.
  This method supports optional arguments and delegates to the protocol retrieve method."
  [kv-protocol key & {:keys [refresh] :or {refresh false}}]
  (retrieve kv-protocol key refresh))

(defn array? [o]
  (let [c (class o)] (.isArray c)))

(defn hashcode [val]
  (cond (nil? val) 0
        (array? val) (Arrays/hashCode val)
        :else (hash val)))

;; Local, memory-based KV store

(defrecord LocalKeyValueStore [store]
  KeyValueStore
  (retrieve [_ key _] (let [value (@store key)]
                        (log/debug (str "(local) FETCH " key " => " (hashcode value)))
                        value))
  (store [_ key value] (do (log/debug (str "(local) STORE " key " => " (hashcode value)))
                           (swap! store assoc key value)))
  (delete [_ key] (do (log/debug (str "(local) DELETE " key))
                      (swap! store dissoc key)))
  (state [_]
    (let [store-data @store]
      {:store {:count (count store-data), :data store-data}
       :variant "in-memory"})))

(defn new-local-kv-store [_]
  (LocalKeyValueStore. (atom {})))

;; ZooKeeper KV store
(defn key->zk-path
  "Converts a key to a valid ZooKeeper path.
  Keys are hashed into 16^4 buckets an attempt to ensure that base-path
  does not contain too many children."
  [base-path key]
  (let [key-hash (digest/md5 key)]
    (str base-path "/" (subs key-hash 0 4) "/" key)))

(defn validate-zk-key
  "Validates a key such that it does not contain special meaning
  given the underlying ZooKeeper implementation."
  [key]
  (when (re-matches #".*/.*" key)
    (throw (ex-info "Key may not contain '/'" {:key key})))
  (when (re-matches #"^\..*" key)
    (throw (ex-info "Key may not begin with '.'" {:key key}))))

(defn zk-keys
  "Create a lazy sequence of keys."
  ([curator base-path]
   (lazy-seq
     (let [buckets (seq (curator/children curator base-path :ignore-does-not-exist true))]
       (zk-keys curator base-path buckets))))
  ([curator base-path buckets]
   (lazy-seq
     (when (seq buckets)
       (let [ks (seq (curator/children curator (str base-path "/" (first buckets))))]
         (zk-keys curator base-path (rest buckets) ks)))))
  ([curator base-path buckets [f & r]]
   (lazy-seq
     (if f
       (cons f (zk-keys curator base-path buckets r))
       (zk-keys curator base-path buckets)))))

;; KV store that uses curator api to use zookeeper as its backing data source
(defrecord ZooKeeperKeyValueStore [^CuratorFramework curator-client base-path sync-timeout-ms]
  KeyValueStore
  (retrieve [_ key refresh]
    (validate-zk-key key)
    (meters/mark! (metrics/waiter-meter "core" "kv-zk" "retrieve"))
    (let [path (key->zk-path base-path key)]
      (when refresh
        (let [response-promise (promise)]
          (log/debug "(zk) SYNC" path)
          (.sync curator-client path response-promise)
          (log/debug "awaiting response from sync call")
          (let [response (deref response-promise sync-timeout-ms :unrealized)]
            (log/info "proceeding past sync() call with" response))))
      (let [{:keys [data]} (curator/read-path curator-client path
                                              :nil-on-missing? true
                                              :serializer :nippy)]
        (log/debug "(zk) FETCH" path "=>" (hashcode data))
        data)))
  (store [_ key value]
    (validate-zk-key key)
    (meters/mark! (metrics/waiter-meter "core" "kv-zk" "store"))
    (let [path (key->zk-path base-path key)]
      (log/debug "(zk) STORE" path "=>" (hashcode value))
      (curator/write-path curator-client path value
                          :serializer :nippy
                          :mode :persistent
                          :create-parent-zknodes? true)))
  (delete [_ key]
    (validate-zk-key key)
    (meters/mark! (metrics/waiter-meter "core" "kv-zk" "delete"))
    (let [path (key->zk-path base-path key)]
      (log/debug "(zk) DELETE" path)
      (curator/delete-path curator-client path :ignore-does-not-exist true)))
  (state [_]
    {:base-path base-path, :variant "zookeeper"}))

(defn new-zk-kv-store
  "Creates a new ZooKeeperKeyValueStore"
  [{:keys [curator base-path sync-timeout-ms]}]
  {:pre [(instance? CuratorFramework curator)
         (string? base-path)
         (utils/pos-int? sync-timeout-ms)]}
  (->ZooKeeperKeyValueStore curator base-path sync-timeout-ms))

;; File-based persistent KV store

(defrecord FileBasedKeyValueStore [target-file store]
  KeyValueStore
  (retrieve [_ key _]
    (validate-zk-key (str key)) ;; to maintain same behavior as ZK kv-store
    (@store key))
  (store [_ key value]
    (validate-zk-key (str key)) ;; to maintain same behavior as ZK kv-store
    (locking store
      (swap! store assoc key value)
      (log/info "writing latest data after store to" target-file)
      (nippy/freeze-to-file target-file @store)))
  (delete [_ key]
    (validate-zk-key (str key)) ;; to maintain same behavior as ZK kv-store
    (locking store
      (swap! store dissoc key)
      (log/info "writing latest data after delete to" target-file)
      (nippy/freeze-to-file target-file @store)))
  (state [_]
    (let [store-data @store]
      {:store {:count (count store-data)
               :data store-data}
       :variant "file-based"})))

(defn new-file-based-kv-store [{:keys [target-file]}]
  (let [store (atom {})]
    (io/make-parents target-file)
    (when (-> target-file io/as-file .exists)
      (log/info "loading existing data from" target-file)
      (reset! store (nippy/thaw-from-file target-file)))
    (FileBasedKeyValueStore. target-file store)))

;; Encryption KV store that uses another KV store as its backing data source
(defrecord EncryptedKeyValueStore [inner-kv-store passwords]
  KeyValueStore
  (retrieve [_ key refresh]
    (when-let [encrypted-value (retrieve inner-kv-store key refresh)]
      ;; Here, we're trying to decrypt the data with each password in turn
      (some #(try
               (nippy/thaw encrypted-value {:password %
                                            :compressor compression/lzma2-compressor
                                            :v1-compatibility? false})
               (catch Exception _
                 (log/warn "Failed to decrypt hash:" (Arrays/hashCode ^bytes encrypted-value) "for" key)
                 nil))
            passwords)))
  (store [_ key value]
    (let [password (first passwords)
          encrypted-value (nippy/freeze value {:password password
                                               :compressor compression/lzma2-compressor})]
      (store inner-kv-store key encrypted-value)))
  (delete [_ key]
    (delete inner-kv-store key))
  (state [_]
    {:inner-state (state inner-kv-store), :variant "encrypted"}))

(defn new-encrypted-kv-store [passwords kv-store]
  (if (or (empty? passwords) (some empty? passwords))
    (throw (.IllegalArgumentException "Passwords should not be empty!"))
    (EncryptedKeyValueStore. kv-store passwords)))

;; Caching KV store that uses another KV store as its backing data source
(defrecord CachedKeyValueStore [inner-kv-store cache]
  KeyValueStore
  (retrieve [_ key refresh]
    (when refresh
      (if (cu/cache-contains? cache key)
        (do
          (log/info "evicting entry for" key "from cache")
          (cu/cache-evict cache key))
        (log/info "refresh is a no-op as cache does not contain" key)))
    (cu/cache-get-or-load cache key #(retrieve inner-kv-store key refresh)))
  (store [_ key value]
    (cu/cache-evict cache key)
    (store inner-kv-store key value))
  (delete [_ key]
    (log/info "evicting deleted entry" key "from cache")
    (cu/cache-evict cache key)
    (delete inner-kv-store key))
  (state [_]
    {:cache {:count (cu/cache-size cache)
             :data (cu/cache->map cache)}
     :inner-state (state inner-kv-store)
     :variant "cache"}))

(defn new-cached-kv-store [{:keys [threshold ttl]} kv-store]
  (->> {:threshold threshold
        :ttl (-> ttl t/seconds t/in-millis)}
       cu/cache-factory
       (CachedKeyValueStore. kv-store)))

(defn- conditional-kv-wrapper
  "Decorator pattern around the given kv-impl"
  [kv-impl name condition factory-fn]
  (if condition
    (do (log/info "using key value store with" name)
        (factory-fn kv-impl))
    (do (log/info "using key value store without" name)
        kv-impl)))

(defn new-kv-store
  "Returns a new key/value store using the given configuration"
  [{:keys [cache encrypt relative-path] :as config} curator base-path passwords]
  (let [kv-base-path (str base-path "/" relative-path)
        kv-context {:base-path kv-base-path, :curator curator}
        kv-impl (utils/create-component config :context kv-context)]
    (-> kv-impl
        ; Note: Order is important as we want cache lookups to be done before encryption
        (conditional-kv-wrapper "encryption" encrypt (partial new-encrypted-kv-store passwords))
        (conditional-kv-wrapper "caching" cache (partial new-cached-kv-store cache)))))
