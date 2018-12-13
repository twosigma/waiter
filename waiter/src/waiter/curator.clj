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
(ns waiter.curator
  (:require [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy])
  (:import clojure.lang.ExceptionInfo
           java.io.Closeable
           java.net.ServerSocket
           java.util.concurrent.TimeUnit
           org.apache.curator.framework.CuratorFramework
           org.apache.curator.framework.recipes.locks.InterProcessMutex
           org.apache.curator.test.TestingServer
           org.apache.zookeeper.KeeperException$NodeExistsException
           org.apache.zookeeper.KeeperException$NoNodeException
           org.apache.zookeeper.CreateMode
           org.apache.zookeeper.data.Stat))

(defn close
  [^Closeable c]
  (.close c))

(def create-modes
  {:ephemeral CreateMode/EPHEMERAL
   CreateMode/EPHEMERAL CreateMode/EPHEMERAL
   :ephemeral-seq CreateMode/EPHEMERAL_SEQUENTIAL
   CreateMode/EPHEMERAL_SEQUENTIAL CreateMode/EPHEMERAL_SEQUENTIAL
   :persistent CreateMode/PERSISTENT
   CreateMode/PERSISTENT CreateMode/PERSISTENT
   :persistent-seq CreateMode/PERSISTENT_SEQUENTIAL
   CreateMode/PERSISTENT_SEQUENTIAL CreateMode/PERSISTENT_SEQUENTIAL})

(defn serialize
  [serializer data]
  (condp = serializer
    :nippy (nippy/freeze data)
    :none data
    (throw (ex-info "Unknown serializer" {:serializer serializer}))))

(defn deserialize
  [serializer data]
  (condp = serializer
    :nippy (try
             (nippy/thaw data)
             (catch ExceptionInfo e
               (log/error "Error in deserializing data" (.getMessage e))
               ;; remove password from exception thrown by nippy
               (throw (ex-info (.getMessage e)
                               (update-in (ex-data e) [:opts :password] (fn [password] (when password "***")))))))
    :none data
    (throw (ex-info "Unknown serializer" {:serializer serializer}))))

(defn create-path
  ([^CuratorFramework curator path & {:keys [mode noop-on-exists? create-parent-zknodes?]
                                      :or {mode :persistent ;match curator default
                                           noop-on-exists? true
                                           create-parent-zknodes? false}}]
   (when-not (create-modes mode)
     (throw (ex-info "Unknown mode requested" {:mode mode :modes-available create-modes})))
   (when create-parent-zknodes?
     (.. (.newNamespaceAwareEnsurePath curator path)
         (excludingLast)
         (ensure (.getZookeeperClient curator))))
   (try
     (.. curator
         (create)
         (withMode (create-modes mode))
         (forPath path))
     (catch KeeperException$NodeExistsException e
       (when-not noop-on-exists?
         (throw e))))))

(defn write-path
  ([^CuratorFramework curator path data & {:keys [mode create-parent-zknodes? serializer]
                                           :or {mode :persistent
                                                create-parent-zknodes? false
                                                serializer :none}}]
   (when-not (create-modes mode)
     (throw (ex-info "Unknown mode requested" {:mode mode :modes-available create-modes})))
   (when create-parent-zknodes?
     (.. (.newNamespaceAwareEnsurePath curator path)
         (excludingLast)
         (ensure (.getZookeeperClient curator))))
   (let [create-path (fn create-path []
                       (.. curator
                           (create)
                           (withMode (create-modes mode))
                           (forPath path (serialize serializer data))))
         update-path (fn update-path []
                       (.. curator
                           (setData)
                           (forPath path (serialize serializer data))))]
     (try
       (if (-> curator (.checkExists) (.forPath path) (nil?))
         (create-path)
         (update-path))
       (catch KeeperException$NodeExistsException _
         (log/warn "Error in writing to existing path" path)
         (update-path))))))

(defn read-path
  ([^CuratorFramework curator path & {:keys [serializer nil-on-missing?]
                                      :or {serializer :none nil-on-missing? false}}]
   (try
     (let [stat (Stat.)]
       {:data (deserialize serializer
                           (.. curator
                               (getData)
                               (storingStatIn stat)
                               (forPath path)))
        :stat (bean stat)})
     (catch KeeperException$NoNodeException e
       (when-not nil-on-missing?
         (throw e))))))

(defn delete-path
  "Deletes the contents of a specified path."
  ([^CuratorFramework curator path & {:keys [delete-children ignore-does-not-exist throw-exceptions]
                                      :or {delete-children false
                                           ignore-does-not-exist false
                                           throw-exceptions true}}]
   (try
     (cond-> (.delete curator)
             delete-children (.deletingChildrenIfNeeded)
             true (.forPath path))
     {:result :success}
     (catch KeeperException$NoNodeException e
       (log/info path "does not exist!")
       (if-not ignore-does-not-exist
         (throw e)
         {:result :no-node-exists}))
     (catch Exception e
       (log/error e "Unable to delete path:" path)
       (when throw-exceptions
         (throw e))))))

(defn children
  "Gets the children of a ZK node."
  ([^CuratorFramework curator path & {:keys [ignore-does-not-exist]
                                      :or {ignore-does-not-exist false}}]
   (try 
     (.. curator
         (getChildren)
         (forPath path))
     (catch KeeperException$NoNodeException e
       (log/info path "does not exist!")
       (if-not ignore-does-not-exist
         (throw e))))))

(defn start-in-process-zookeeper []
  (let [ss (ServerSocket. 0)
        available-port (.getLocalPort ss)
        _ (.close ss)
        _ (log/info "Starting in-process zookeeper on port" available-port)
        zk-server (TestingServer. available-port true)]
    {:zk-server zk-server
     :zk-connection-string (.getConnectString zk-server)}))

(defn synchronize
  "Creates a function that takes a function and ensures that it runs
  only once at the same time, globally across the cluster."
  [^CuratorFramework curator lock-path timeout-ms f]
  (let [mutex (InterProcessMutex. curator lock-path)]
    (when-not (.acquire mutex timeout-ms TimeUnit/MILLISECONDS)
      (throw (ex-info "Could not acquire lock." 
                      {:timeout-ms timeout-ms
                       :lock-path lock-path})))
    (try 
      (f)
      (catch Exception e
        (log/error e "Error during synchronized")
        (throw e))
      (finally (.release mutex)))))
