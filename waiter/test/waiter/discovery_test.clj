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
(ns waiter.discovery-test
  (:require [clojure.test :refer :all]
            [waiter.discovery :refer :all])
  (:import (org.apache.curator.x.discovery ServiceInstance ServiceCache)))

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
