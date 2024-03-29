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
(ns waiter.discovery-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [waiter.curator :as curator]
            [waiter.discovery :refer :all])
  (:import (java.util HashMap)
           (org.apache.curator.framework CuratorFrameworkFactory)
           (org.apache.curator.retry RetryNTimes)
           (org.apache.curator.x.discovery ServiceCache ServiceInstance)))

(defn- prepare-discovery
  [service-name instance-ids]
  {:service-instance (-> (ServiceInstance/builder) (.name service-name) (.build))
   :service-cache (reify ServiceCache
                    (getInstances [_]
                      (map #(-> (ServiceInstance/builder)
                                (.address (str %1 ".test.com"))
                                (.id %1)
                                (.name service-name)
                                (.port 1234)
                                (.payload (doto (HashMap.)
                                            (.put "router-fqdn" (str "route." %1 ".test.com"))
                                            (.put "router-ssl-port" 12345)))
                                (.build))
                           instance-ids)))})

(deftest test-router-ids
  (is (empty? (router-ids (prepare-discovery "waiter" []))))
  (is (empty? (router-ids (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{"r1" "r2" "r3"})))
  (is (= #{"r1"} (set (router-ids (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{"r2" "r3"}))))
  (is (= #{"r1" "r2"} (set (router-ids (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{"r3"}))))
  (is (= #{"r1" "r2" "r3"} (set (router-ids (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{})))))

(deftest test-router-id->endpoint-url
  (is (empty? (router-id->endpoint-url (prepare-discovery "waiter" []) "http" "endpoint")))
  (is (empty? (router-id->endpoint-url (prepare-discovery "waiter" ["r1" "r2" "r3"]) "http" "endpoint" :exclude-set #{"r1" "r2" "r3"})))
  (is (= {"r1" "http://r1.test.com:1234/endpoint"}
         (router-id->endpoint-url (prepare-discovery "waiter" ["r1" "r2" "r3"]) "http" "endpoint" :exclude-set #{"r2" "r3"})))
  (is (= {"r1" "http://r1.test.com:1234/endpoint"
          "r2" "http://r2.test.com:1234/endpoint"}
         (router-id->endpoint-url (prepare-discovery "waiter" ["r1" "r2" "r3"]) "http" "endpoint" :exclude-set #{"r3"})))
  (is (= {"r1" "http://r1.test.com:1234/endpoint"
          "r2" "http://r2.test.com:1234/endpoint"
          "r3" "http://r3.test.com:1234/endpoint"}
         (router-id->endpoint-url (prepare-discovery "waiter" ["r1" "r2" "r3"]) "http" "endpoint" :exclude-set #{}))))

(deftest test-cluster-size
  (is (zero? (cluster-size (prepare-discovery "waiter" []))))
  (is (= 1 (cluster-size (prepare-discovery "waiter" ["r1"]))))
  (is (= 2 (cluster-size (prepare-discovery "waiter" ["r1" "r2"]))))
  (is (= 3 (cluster-size (prepare-discovery "waiter" ["r1" "r2" "r3"])))))

(defn- prepare-router-details
  [name id]
  {:address (str id ".test.com")
   :custom-details {:router-fqdn (str "route." id ".test.com")
                    :router-ssl-port 12345}
   :id id
   :name name
   :port 1234})

(deftest test-router-id->details
  (is (empty? (router-id->details (prepare-discovery "waiter" []))))
  (is (empty? (router-id->details (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{"r1" "r2" "r3"})))
  (is (= {"r1" (prepare-router-details "waiter" "r1")}
         (router-id->details (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{"r2" "r3"})))
  (is (= {"r1" (prepare-router-details "waiter" "r1")
          "r2" (prepare-router-details "waiter" "r2")}
         (router-id->details (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{"r3"})))
  (is (= {"r1" (prepare-router-details "waiter" "r1")
          "r2" (prepare-router-details "waiter" "r2")
          "r3" (prepare-router-details "waiter" "r3")}
         (router-id->details (prepare-discovery "waiter" ["r1" "r2" "r3"]) :exclude-set #{}))))

(deftest test-versioned-discovery-path
  (is (str/ends-with? (versioned-discovery-path "/base") "-v2")
      "unexpected discovery path suffix - see discovery/->service-instance for a note on discovery paths and payload types"))

(deftest test-service-discover-payload-type
  (testing "service instances should include expected HashMap payload"
      (let [zk (curator/start-in-process-zookeeper)
            zk-server (:zk-server zk)
            curator (CuratorFrameworkFactory/newClient (:zk-connection-string zk) (RetryNTimes. 10 100))]
        (try
          (.start curator)
          (let [{:keys [service-instance] :as discovery}
                (register "r1" curator "waiter" "/base/discovery" {:host "waiter.com" :port 1234 :router-fqdn "r1.waiter.com" :router-ssl-port 12345})
                payload (.getPayload service-instance)
                expected-payload {"router-fqdn" "r1.waiter.com" "router-ssl-port" 12345}]
            (is (= HashMap (type payload)) "unexpected payload type - see discovery/->service-instance for a note on changes to payload types")
            (is (= expected-payload payload))
            (let [details (router-id->details discovery)]
              (is (= 1 (count details)))
              (is (contains? details "r1"))
              (is (= (walk/keywordize-keys expected-payload) (:custom-details (get details "r1"))))))
          (finally
            (.close curator)
            (.stop zk-server))))))