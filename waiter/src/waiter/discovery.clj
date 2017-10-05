;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.discovery
  (:require [clojure.tools.logging :as log])
  (:import (java.net Inet4Address)
           (org.apache.curator.framework CuratorFramework)
           (org.apache.curator.x.discovery ServiceCache ServiceDiscovery ServiceDiscoveryBuilder
                                           ServiceInstance ServiceInstanceBuilder UriSpec)))

(defn- ->service-instance
  [id svc-name {:keys [host port]}]
  (let [builder (-> (ServiceInstance/builder)
                    (.id id)
                    (.name svc-name)
                    (.uriSpec (UriSpec. "{scheme}://{address}:{port}/{endpoint}"))
                    (.port port)
                    (.address (if (= host "0.0.0.0")
                                (let [local-ips (ServiceInstanceBuilder/getAllLocalIPs)]
                                  (->> (or (some #(when (instance? Inet4Address %) %) local-ips)
                                           (first local-ips))
                                       .getHostAddress))
                                host)))]
    (.build builder)))

(defn- ->service-discovery
  [^CuratorFramework curator base-path instance]
  (-> (ServiceDiscoveryBuilder/builder Void)
      (.client curator)
      (.basePath base-path)
      (.thisInstance instance)
      (.build)))

(defn- ->service-cache
  [^ServiceDiscovery service-discovery svc-name]
  (-> service-discovery
      (.serviceCacheBuilder)
      (.name svc-name)
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

(defn cluster-size
  "Returns the number of routers particpating in the ZooKeeper cluster."
  [discovery]
  (count (routers discovery #{})))

(defn register
  [router-id curator service-name discovery-path {:keys [host port]}]
  (let [instance (->service-instance router-id service-name {:host host :port port})
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
