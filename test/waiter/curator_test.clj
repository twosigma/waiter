;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.curator-test
  (:require [clojure.set :as set]
            [clojure.test :refer :all]
            [waiter.curator :refer :all])
  (:import (org.apache.curator.framework CuratorFrameworkFactory)
           (org.apache.curator.retry RetryNTimes)
           (org.apache.curator.test TestingServer)))

(deftest test-delete-path
  (let [zk (start-in-process-zookeeper)
        ^TestingServer zk-server (:zk-server zk)
        curator (CuratorFrameworkFactory/newClient (:zk-connection-string zk) (RetryNTimes. 10 100))
        services-base-path "/test-delete-path/base-path"
        paths-to-keep #{"service2" "service4" "service6" "service8"}
        paths-to-delete #{"service3" "service5" "service7"}]
    (try
      (.start curator)
      (testing "curator-delete-path"
        ; set up: create paths
        (doseq [service-id (concat paths-to-delete paths-to-keep)]
          (.forPath (.creatingParentsIfNeeded (.create curator)) (str services-base-path "/" service-id)))
        ; delete paths
        (doseq [service-id paths-to-delete]
          (delete-path curator (str services-base-path "/" service-id)))
        (do
          ; ensure only the intended nodes were deleted
          (is (every? #(nil? (.forPath (.checkExists curator) (str services-base-path "/" %))) paths-to-delete))
          (is (every? #(not (nil? (.forPath (.checkExists curator) (str services-base-path "/" %)))) paths-to-keep)))
        ; delete using invalid paths
        (doseq [service-id (concat paths-to-delete paths-to-keep)]
          (let [result (delete-path curator (str services-base-path "/" service-id "DoesNotExist") :ignore-does-not-exist true)]
            (is (= {:result :no-node-exists} result))
            (is (every? #(not (nil? (.forPath (.checkExists curator) (str services-base-path "/" %)))) paths-to-keep))))
        ; try to delete a node with children
        (let [result (delete-path curator services-base-path :delete-children false :throw-exceptions false)]
          (is (nil? result))
          (is (every? #(not (nil? (.forPath (.checkExists curator) (str services-base-path "/" %)))) paths-to-keep)))
        (let [result (delete-path curator services-base-path :delete-children true :throw-exceptions false)]
          (is (= {:result :success} result))
          (is (every? #(nil? (.forPath (.checkExists curator) (str services-base-path "/" %))) paths-to-keep))))
      (finally
        (.close curator)
        (.stop zk-server)))))

(deftest test-read-write-path
  (let [zk (start-in-process-zookeeper)
        ^TestingServer zk-server (:zk-server zk)
        curator (CuratorFrameworkFactory/newClient (:zk-connection-string zk) (RetryNTimes. 10 100))
        serializer :nippy
        services-base-path "/test-write-path/base-path"
        paths-already-exist #{"service2" "service4" "service6" "service8"}
        paths-to-create #{"service3" "service5" "service7"}
        paths-to-work-on (set/union paths-already-exist paths-to-create)]
    (try
      (.start curator)
      (testing "curator-write-path"
        (doseq [service-id (concat paths-to-create paths-already-exist)]
          (.forPath (.creatingParentsIfNeeded (.create curator)) (str services-base-path "/" service-id)))
        (doseq [service-id paths-to-work-on]
          (write-path curator (str services-base-path "/" service-id) {:service-id service-id} :serializer serializer))
        (is (every? #(not (nil? (.forPath (.checkExists curator) (str services-base-path "/" %)))) paths-to-work-on))
        (doseq [service-id paths-to-work-on]
          (is (= {:service-id service-id}
                 (->> (read-path curator (str services-base-path "/" service-id) :serializer serializer) (:data)))
              (str "Data not equal for " service-id)))
        (doseq [service-id paths-to-work-on]
          (write-path curator (str services-base-path "/" service-id) {:service-id service-id, :pass 2} :serializer serializer))
        (is (every? #(not (nil? (.forPath (.checkExists curator) (str services-base-path "/" %)))) paths-to-work-on))
        (doseq [service-id paths-to-work-on]
          (is (= {:service-id service-id, :pass 2}
                 (->> (read-path curator (str services-base-path "/" service-id) :serializer serializer) (:data)))
              (str "Data not equal for " service-id))))

      (testing "write-on-new-nested-paths"
        (doseq [service-id paths-to-work-on]
          (write-path curator (str services-base-path "/new/" service-id) {:service-id service-id}
                      :create-parent-zknodes? true :serializer serializer))
        (is (every? #(not (nil? (.forPath (.checkExists curator) (str services-base-path "/" %)))) paths-to-work-on))
        (doseq [service-id paths-to-work-on]
          (is (= {:service-id service-id}
                 (->> (read-path curator (str services-base-path "/new/" service-id) :serializer serializer) (:data)))
              (str "Data not equal for " service-id))))
      (finally
        (.close curator)
        (.stop zk-server)))))