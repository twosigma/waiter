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
(ns waiter.discovery
  (:require [clojure.tools.logging :as log]
            [plumbing.core :as pc])
  (:import (java.net Inet4Address)
           (java.util HashMap)
           (org.apache.curator.framework CuratorFramework)
           (org.apache.curator.x.discovery ServiceCache ServiceDiscovery ServiceDiscoveryBuilder ServiceInstance ServiceInstanceBuilder UriSpec)
           (org.apache.curator.x.discovery.details JsonInstanceSerializer)))

(defn- ->service-instance
  [id service-name {:keys [host port router-fqdn router-ssl-port]}]
  ;; Note that the payload type must be consistent for a given ZK discovery-path.
  ;; If the payload type changes here, then there is a runtime risk that existing
  ;; services in ZK will no longer be deserializable. This will prevent Waiter from
  ;; starting and impact releases/rollbacks. If you change the payload type,
  ;; then you MUST pair the change with an updated discovery-path suffix
  ;; (see versioned-discovery-path).
  (let [payload (doto (HashMap.)
                  (.put "router-fqdn" router-fqdn)
                  (.put "router-ssl-port" router-ssl-port))
        builder (-> (ServiceInstance/builder)
                    (.id id)
                    (.name service-name)
                    (.uriSpec (UriSpec. "{scheme}://{address}:{port}/{endpoint}"))
                    (.port port)
                    (.address (if (= host "0.0.0.0")
                                (let [inet-addresses (ServiceInstanceBuilder/getAllLocalIPs)
                                      ipv4-address (some #(when (instance? Inet4Address %) %) inet-addresses)]
                                  (if ipv4-address
                                    (.getHostAddress ipv4-address)
                                    (throw (ex-info "No IPv4 address found for host"
                                                    {:id id
                                                     :inet-addresses inet-addresses
                                                     :service-name service-name}))))
                                host))
                    (.payload payload))]
    (.build builder)))

(defn- ->service-discovery
  [^CuratorFramework curator base-path instance]
  (let [payload-class (class (HashMap.))]
    (-> (ServiceDiscoveryBuilder/builder payload-class)
        (.client curator)
        (.basePath base-path)
        (.serializer (JsonInstanceSerializer. payload-class))
        (.thisInstance instance)
        (.build))))

(defn- ->service-cache
  [^ServiceDiscovery service-discovery service-name]
  (-> service-discovery
      (.serviceCacheBuilder)
      (.name service-name)
      (.build)))

(defn- get-instance-url [service-instance protocol endpoint]
  (str protocol "://" (.getAddress service-instance) ":" (.getPort service-instance) "/" endpoint))

(defn- create-unregistration-hook
  "Registers a JVM shutdown hook to unregister the given ServiceInstance"
  [^ServiceDiscovery service-discovery ^ServiceInstance service-instance]
  (let [^Runnable unregister (fn unregister-hook []
                               (try
                                 (.unregisterService service-discovery service-instance)
                                 (log/info "Unregistered service" service-instance "from discovery")
                                 (catch Exception e
                                   (log/error e "Failed to unregister" service-instance "from discovery"))))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. unregister "discovery-unregister"))))

(defn- routers
  "Returns all known routers that exclude items that pass the exclude-set."
  [discovery exclude-set]
  (let [my-service-name (.getName (:service-instance discovery))
        all-instances (.getInstances ^ServiceCache (:service-cache discovery))
        waiter-instances (filter #(= my-service-name (.getName %)) all-instances)]
    (remove #(contains? exclude-set (.getId %)) waiter-instances)))

(defn router-ids
  "Returns all known router ids that exclude items that pass the exclude-set."
  [discovery & {:keys [exclude-set] :or {exclude-set #{}}}]
  (let [filtered-instances (routers discovery exclude-set)]
    (map #(str (.getId %)) filtered-instances)))

(defn router-id->endpoint-url
  "Returns a mapping from all known router-ids to instances."
  [discovery protocol endpoint & {:keys [exclude-set] :or {exclude-set #{}}}]
  (let [filtered-instances (routers discovery exclude-set)]
    (zipmap (map #(str (.getId %)) filtered-instances)
            (map #(get-instance-url % protocol endpoint) filtered-instances))))

(defn- router-details
  "Returns a map representing the details of the given router."
  [instance]
  (let [custom-details (pc/map-keys keyword (.getPayload instance))
        details {:address (.getAddress instance)
                 :custom-details custom-details
                 :id (.getId instance)
                 :name (.getName instance)
                 :port (.getPort instance)}]
    details))

(defn router-id->details
  "For all routers that pass the exclude-set, returns a mapping from router-id to its details."
  [discovery & {:keys [exclude-set] :or {exclude-set #{}}}]
  (let [filtered-instances (routers discovery exclude-set)]
    (zipmap (map #(str (.getId %)) filtered-instances)
            (map router-details filtered-instances))))

(defn cluster-size
  "Returns the number of routers particpating in the ZooKeeper cluster."
  [discovery]
  (count (routers discovery #{})))

(defn versioned-discovery-path
  "Returns a versioned discovery path. See the note in ->service-instance about payload types
   and discovery paths."
  [discovery-path]
  (str discovery-path "-v2"))

(defn register
  [router-id curator service-name discovery-path-base {:keys [host port router-fqdn router-ssl-port]}]
  (let [discovery-path (versioned-discovery-path discovery-path-base)
        instance (->service-instance router-id service-name {:host host :port port :router-fqdn router-fqdn :router-ssl-port router-ssl-port})
        discovery (->service-discovery curator discovery-path instance)
        cache (->service-cache discovery service-name)]
    (log/info "Using service name:" service-name "for router id:" router-id)
    (.start discovery)
    (create-unregistration-hook discovery instance)
    (.start cache)
    (log/info "Started service cache with peers:" (.getInstances cache))
    {:service-cache cache
     :service-discovery discovery
     :service-instance instance}))
