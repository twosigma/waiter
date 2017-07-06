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
(ns waiter.kv-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [waiter.curator :as curator]
            [waiter.kv :as kv])
  (:import (java.util Arrays)
           (org.apache.curator.framework CuratorFrameworkFactory CuratorFramework)
           (org.apache.curator.retry RetryNTimes)))

(deftest test-local-kv-store
  (let [test-store (kv/new-local-kv-store {})
        bytes (byte-array 10)]
    (Arrays/fill bytes (byte 1))
    (is (nil? (kv/fetch test-store :a)))
    (kv/store test-store :a bytes)
    (is (Arrays/equals bytes ^bytes (kv/fetch test-store :a)))
    (kv/store test-store :a 3)
    (is (= 3 (kv/fetch test-store :a)))
    (is (nil? (kv/fetch test-store :b)))
    (is (= {:store {:count 1, :data {:a 3}}, :variant "in-memory"}
           (kv/state test-store)))
    (kv/delete test-store :a)
    (is (nil? (kv/fetch test-store :a)))
    (is (nil? (kv/fetch test-store :b)))
    (kv/delete test-store :does-not-exist)
    (is (= {:store {:count 0, :data {}}, :variant "in-memory"}
           (kv/state test-store)))))

(deftest test-encrypted-kv-store
  (let [passwords ["test1" "test2" "test3"]
        processed-passwords (mapv #(vector :cached %) passwords)
        local-kv-store (kv/new-local-kv-store {})
        encrypted-kv-store (kv/new-encrypted-kv-store processed-passwords local-kv-store)]
    (is (nil? (kv/fetch local-kv-store :a)))
    (is (nil? (kv/fetch encrypted-kv-store :a)))
    (is (nil? (kv/fetch local-kv-store :b)))
    (is (nil? (kv/fetch encrypted-kv-store :b)))
    ; store propagates to underlying store
    (kv/store encrypted-kv-store :b 2)
    (is (not (nil? (kv/fetch local-kv-store :b))))
    (is (= 2 (kv/fetch encrypted-kv-store :b)))
    ; store does not corrupt underlying store
    (kv/store encrypted-kv-store :a 5)
    (is (not (nil? (kv/fetch local-kv-store :a))))
    (is (= 5 (kv/fetch encrypted-kv-store :a)))
    (is (not (nil? (kv/fetch local-kv-store :b))))
    (is (= 2 (kv/fetch encrypted-kv-store :b)))
    ; store updates underlying store
    (kv/store encrypted-kv-store :b 11)
    (is (not (nil? (kv/fetch local-kv-store :b))))
    (is (= 11 (kv/fetch encrypted-kv-store :b)))
    (is (= "encrypted" (get (kv/state encrypted-kv-store) :variant)))
    (is (= 2 (get-in (kv/state encrypted-kv-store) [:inner-state :store :count])))
    (is (= #{:a :b} (set (keys (get-in (kv/state encrypted-kv-store) [:inner-state :store :data])))))
    ; delete :a and :b
    (kv/delete encrypted-kv-store :a)
    (is (nil? (kv/fetch local-kv-store :a)))
    (is (nil? (kv/fetch encrypted-kv-store :a)))
    (kv/delete encrypted-kv-store :b)
    (is (nil? (kv/fetch local-kv-store :b)))
    (is (nil? (kv/fetch encrypted-kv-store :b)))
    (is (= "encrypted" (get (kv/state encrypted-kv-store) :variant)))
    (is (= {:count 0, :data {}} (get-in (kv/state encrypted-kv-store) [:inner-state :store])))
    (kv/delete encrypted-kv-store :does-not-exist)))

(deftest test-cached-kv-store
  (let [cache-config {:threshold 1000 :ttl (-> 60 t/seconds t/in-millis)}
        local-kv-store (kv/new-local-kv-store {})
        cached-kv-store (kv/new-cached-kv-store cache-config local-kv-store)]
    (is (nil? (kv/fetch local-kv-store :a)))
    (is (nil? (kv/fetch cached-kv-store :a)))
    (kv/store local-kv-store :a 1)
    ; cache looks up underlying store during miss
    (is (kv/fetch local-kv-store :a))
    (is (= 1 (kv/fetch local-kv-store :a)))
    (is (nil? (kv/fetch cached-kv-store :a)))
    ; store to cache propagates to underlying store 
    (kv/store cached-kv-store :b 2)
    (is (= 2 (kv/fetch cached-kv-store :b)))
    (is (= 2 (kv/fetch local-kv-store :b)))
    (kv/store cached-kv-store :b 11)
    (is (= 11 (kv/fetch cached-kv-store :b)))
    (is (= 11 (kv/fetch local-kv-store :b)))
    ; cache works with refresh call
    (kv/store cached-kv-store :b 13)
    (is (= 13 (kv/fetch cached-kv-store :b)))
    (kv/store local-kv-store :b 17)
    (is (= 13 (kv/fetch cached-kv-store :b)))
    (is (= 17 (kv/fetch local-kv-store :b)))
    (is (= 17 (kv/fetch cached-kv-store :b :refresh true)))
    (is (= "cache" (get (kv/state cached-kv-store) :variant)))
    (is (= {:count 2, :data {:a 1, :b 17}}
           (get-in (kv/state cached-kv-store) [:inner-state :store])))
    ; delete removes entry from cache
    (kv/delete cached-kv-store :b)
    (is (nil? (kv/fetch local-kv-store :b)))
    (is (nil? (kv/fetch cached-kv-store :b)))
    (is (nil? (kv/fetch cached-kv-store :b :refresh true)))
    (is (= "cache" (get (kv/state cached-kv-store) :variant)))
    (is (= {:count 1, :data {:a 1}}
           (get-in (kv/state cached-kv-store) [:inner-state :store])))))

(deftest test-validate-zk-key
  (kv/validate-zk-key "test-key")
  (is (thrown-with-msg? Exception #"Key may not contain '/'" (kv/validate-zk-key "evil-key/evil-key")))
  (is (thrown-with-msg? Exception #"Key may not begin with '.'" (kv/validate-zk-key ".."))))

(deftest test-key->zk-key
  (is (= "/base/6f1e/blah" (kv/key->zk-path "/base" "blah")))
  (is (= "/base2/42d3/blahblah" (kv/key->zk-path "/base2" "blahblah"))))

(deftest test-zk-kv-store
  (let [zk (curator/start-in-process-zookeeper)
        zk-server (:zk-server zk)
        curator (CuratorFrameworkFactory/newClient (:zk-connection-string zk) (RetryNTimes. 10 100))
        services-base-path "/test-zk-kv-store/base-path"]
    (is (kv/new-zk-kv-store {:curator curator
                             :base-path "/waiter-tokens"
                             :sync-timeout-ms 1}))
    (try
      (.start curator)
      (testing "in-memory-zk"
        (let [get-value-from-curator (fn [key]
                                       (.forPath (.checkExists curator)
                                                 (kv/key->zk-path services-base-path key)))
              test-store (kv/new-zk-kv-store {:curator curator
                                              :base-path services-base-path
                                              :sync-timeout-ms 1})
              bytes (byte-array 10)]
          (Arrays/fill bytes (byte 1))
          (is (nil? (kv/fetch test-store "a")))
          (is (nil? (get-value-from-curator "a")))
          (kv/store test-store "a" bytes)
          (is (Arrays/equals bytes ^bytes (kv/fetch test-store "a")))
          (is (not (nil? (get-value-from-curator "a"))))
          (kv/store test-store "a" 3)
          (is (= 3 (kv/fetch test-store "a")))
          (is (not (nil? (get-value-from-curator "a"))))
          (is (nil? (kv/fetch test-store "b")))
          (is (nil? (get-value-from-curator "b")))
          (kv/delete test-store "a")
          (is (nil? (kv/fetch test-store "a")))
          (is (nil? (get-value-from-curator "a")))
          (is (nil? (kv/fetch test-store "b")))
          (is (nil? (get-value-from-curator "b")))
          (kv/delete test-store "does-not-exist")
          (is (nil? (get-value-from-curator "does-not-exist")))
          (is (= {:base-path services-base-path, :variant "zookeeper"} (kv/state test-store)))))
      (finally
        (.close curator)
        (.stop zk-server)))))

(deftest test-new-kv-store
  (let [base-path "/waiter"
        kv-config {:kind :zk
                   :zk {:factory-fn 'waiter.kv/new-zk-kv-store
                        :sync-timeout-ms 2000}
                   :relative-path "tokens"}
        zk (curator/start-in-process-zookeeper)
        zk-server (:zk-server zk)
        curator (CuratorFrameworkFactory/newClient (:zk-connection-string zk) (RetryNTimes. 10 100))
        kv-store (kv/new-kv-store kv-config curator base-path nil)]
    (try
      (.start curator)
      (kv/store kv-store "foo" "bar")
      (is (= "bar" (kv/retrieve kv-store "foo" true)))
      (finally
        (.close curator)
        (.stop zk-server)))))

(deftest test-new-zk-kv-store
  (testing "Creating a new ZooKeeper key/value store"
    (testing "should throw on non-integer sync-timeout-ms"
      (is (thrown? AssertionError (kv/new-zk-kv-store {:curator (reify CuratorFramework)
                                                       :base-path ""
                                                       :sync-timeout-ms 1.1}))))))

(deftest test-zk-keys
  (testing "List ZK keys"
    (let [base-path "/waiter"
          kv-config {:kind :zk
                     :zk {:factory-fn 'waiter.kv/new-zk-kv-store
                          :sync-timeout-ms 2000}
                     :relative-path "tokens"}
          zk (curator/start-in-process-zookeeper)
          zk-server (:zk-server zk)
          curator (CuratorFrameworkFactory/newClient (:zk-connection-string zk) (RetryNTimes. 10 100))
          kv-store (kv/new-kv-store kv-config curator base-path nil)]
      (try
        (.start curator)
        (kv/store kv-store "foo" "bar")
        (kv/store kv-store "foo2" "bar2")
        (kv/store kv-store "foo3" "bar3")
        (is (= #{"foo" "foo2" "foo3"} (set (kv/zk-keys curator (str base-path "/" (:relative-path kv-config))))))
        (finally
          (.close curator)
          (.stop zk-server))))))
