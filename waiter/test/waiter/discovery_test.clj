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
  (:require [clojure.test :refer :all]
            [waiter.discovery :refer :all])
  (:import (org.apache.curator.x.discovery ServiceCache ServiceInstance)))

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
