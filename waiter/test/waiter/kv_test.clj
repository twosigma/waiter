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
(ns waiter.kv-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [waiter.curator :as curator]
            [waiter.kv :as kv]
            [waiter.util.cache-utils :as cu])
  (:import (java.util Arrays)
           (org.apache.curator.framework CuratorFramework CuratorFrameworkFactory)
           (org.apache.curator.retry RetryNTimes)))

(deftest test-local-kv-store
  (let [test-store (kv/new-local-kv-store {})
        bytes (byte-array 10)
        include-flags #{"data"}
        time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 1))
        time-2 (t/plus time-0 (t/seconds 2))]
    (Arrays/fill bytes (byte 1))
    (is (nil? (kv/fetch test-store :a)))
    (with-redefs [t/now (constantly time-1)]
      (kv/store test-store :a bytes))
    (is (Arrays/equals bytes ^bytes (kv/fetch test-store :a)))
    (is (= {:creation-time (tc/to-long time-1)
            :modified-time (tc/to-long time-1)}
           (kv/stats test-store :a)))
    (is (= {:store {:count 1
                    :data {:a {:stats {:creation-time (tc/to-long time-1)
                                       :modified-time (tc/to-long time-1)}
                               :value bytes}}}
            :supported-include-params ["data"]
            :variant "in-memory"}
           (kv/state test-store include-flags)))
    (with-redefs [t/now (constantly time-2)]
      (kv/store test-store :a 3))
    (is (= 3 (kv/fetch test-store :a)))
    (is (nil? (kv/fetch test-store :b)))
    (is (= {:creation-time (tc/to-long time-1)
            :modified-time (tc/to-long time-2)}
           (kv/stats test-store :a)))
    (is (= {:store {:count 1
                    :data {:a {:stats {:creation-time (tc/to-long time-1)
                                       :modified-time (tc/to-long time-2)}
                               :value 3}}}
            :supported-include-params ["data"]
            :variant "in-memory"}
           (kv/state test-store include-flags)))
    (kv/delete test-store :a)
    (is (nil? (kv/fetch test-store :a)))
    (is (nil? (kv/fetch test-store :b)))
    (kv/delete test-store :does-not-exist)
    (is (= {:store {:count 0, :data {}}
            :supported-include-params ["data"]
            :variant "in-memory"}
           (kv/state test-store include-flags)))))

(defn work-dir
  "Returns the canonical path for the ./kv-store directory"
  []
  (-> "./kv-store" (io/file) (.getCanonicalPath)))

(deftest test-file-based-kv-store
  (let [target-file (str (work-dir) "/foo.bin")
        include-flags #{"data"}
        time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 1))
        time-2 (t/plus time-0 (t/seconds 2))]
    (let [test-store (kv/new-file-based-kv-store {:target-file target-file})
          bytes (byte-array 10)]
      (Arrays/fill bytes (byte 1))
      (is (nil? (kv/fetch test-store :a)))
      (with-redefs [t/now (constantly time-1)]
        (kv/store test-store :a bytes))
      (is (= {:creation-time (tc/to-long time-1)
              :modified-time (tc/to-long time-1)}
             (kv/stats test-store :a)))
      (is (Arrays/equals bytes ^bytes (kv/fetch test-store :a)))
      (with-redefs [t/now (constantly time-2)]
        (kv/store test-store :a 3))
      (is (= 3 (kv/fetch test-store :a)))
      (is (nil? (kv/fetch test-store :b)))
      (is (= {:creation-time (tc/to-long time-1)
              :modified-time (tc/to-long time-2)}
             (kv/stats test-store :a)))
      (is (= {:store {:count 1
                      :data {:a {:stats {:creation-time (tc/to-long time-1)
                                         :modified-time (tc/to-long time-2)}
                                 :value 3}}}
              :supported-include-params ["data"]
              :variant "file-based"}
             (kv/state test-store include-flags))))
    ;; testing data was persisted in the file
    (let [test-store (kv/new-file-based-kv-store {:target-file target-file})]
      (is (= {:creation-time (tc/to-long time-1)
              :modified-time (tc/to-long time-2)}
             (kv/stats test-store :a)))
      (is (= {:store {:count 1
                      :data {:a {:stats {:creation-time (tc/to-long time-1)
                                         :modified-time (tc/to-long time-2)}
                                 :value 3}}}
              :supported-include-params ["data"]
              :variant "file-based"}
             (kv/state test-store include-flags)))
      (kv/delete test-store :a)
      (is (nil? (kv/fetch test-store :a)))
      (is (nil? (kv/fetch test-store :b)))
      (kv/delete test-store :does-not-exist)
      (is (= {:store {:count 0, :data {}}
              :supported-include-params ["data"]
              :variant "file-based"}
             (kv/state test-store include-flags))))))

(deftest test-encrypted-kv-store
  (let [passwords ["test1" "test2" "test3"]
        processed-passwords (mapv #(vector :cached %) passwords)
        local-kv-store (kv/new-local-kv-store {})
        include-flags #{"data"}
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
    (is (= "encrypted" (get (kv/state encrypted-kv-store include-flags) :variant)))
    (is (= 2 (get-in (kv/state encrypted-kv-store include-flags) [:inner-state :store :count])))
    (is (= #{:a :b} (set (keys (get-in (kv/state encrypted-kv-store include-flags) [:inner-state :store :data])))))
    ; delete :a and :b
    (kv/delete encrypted-kv-store :a)
    (is (nil? (kv/fetch local-kv-store :a)))
    (is (nil? (kv/fetch encrypted-kv-store :a)))
    (kv/delete encrypted-kv-store :b)
    (is (nil? (kv/fetch local-kv-store :b)))
    (is (nil? (kv/fetch encrypted-kv-store :b)))
    (is (= "encrypted" (get (kv/state encrypted-kv-store include-flags) :variant)))
    (is (= {:count 0, :data {}} (get-in (kv/state encrypted-kv-store include-flags) [:inner-state :store])))
    (kv/delete encrypted-kv-store :does-not-exist)))

(deftest test-cached-kv-store
  (let [cache-config {:threshold 1000 :ttl (-> 60 t/seconds t/in-millis)}
        local-kv-store (kv/new-local-kv-store {})
        include-flags #{"data"}
        {:keys [cache] :as cached-kv-store} (kv/new-cached-kv-store cache-config local-kv-store)
        time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 1))
        time-2 (t/plus time-0 (t/seconds 2))
        time-3 (t/plus time-0 (t/seconds 3))
        time-4 (t/plus time-0 (t/seconds 4))
        time-5 (t/plus time-0 (t/seconds 5))]
    (is (nil? (kv/fetch local-kv-store :a)))
    (is (nil? (kv/fetch cached-kv-store :a)))
    (with-redefs [t/now (constantly time-1)]
      (kv/store local-kv-store :a 1))
    (is (= {:creation-time (tc/to-long time-1)
            :modified-time (tc/to-long time-1)}
           (kv/stats local-kv-store :a)))
    ; cache looks up underlying store during miss
    (is (kv/fetch local-kv-store :a))
    (is (= 1 (kv/fetch local-kv-store :a)))
    (is (nil? (kv/fetch cached-kv-store :a)))
    ; store to cache propagates to underlying store 
    (with-redefs [t/now (constantly time-2)]
      (kv/store cached-kv-store :b 2))
    (is (= {:creation-time (tc/to-long time-2)
            :modified-time (tc/to-long time-2)}
           (kv/stats local-kv-store :b)))
    (is (true? (cu/cache-contains? cache :b)))
    (is (= 2 (kv/fetch cached-kv-store :b)))
    (is (= 2 (kv/fetch local-kv-store :b)))
    (with-redefs [t/now (constantly time-3)]
      (kv/store cached-kv-store :b 11))
    (is (= 11 (kv/fetch cached-kv-store :b)))
    (is (= 11 (kv/fetch local-kv-store :b)))
    ; cache works with refresh call
    (with-redefs [t/now (constantly time-4)]
      (kv/store cached-kv-store :b 13))
    (is (true? (cu/cache-contains? cache :b)))
    (is (= 13 (kv/fetch cached-kv-store :b)))
    (with-redefs [t/now (constantly time-5)]
      (kv/store local-kv-store :b 17))
    (is (= {:creation-time (tc/to-long time-2)
            :modified-time (tc/to-long time-5)}
           (kv/stats local-kv-store :b)))
    (is (= 13 (kv/fetch cached-kv-store :b)))
    (is (= 17 (kv/fetch local-kv-store :b)))
    (is (= 17 (kv/fetch cached-kv-store :b :refresh true)))
    (is (= "cache" (get (kv/state cached-kv-store include-flags) :variant)))
    (is (= {:count 2
            :data {:a {:stats {:creation-time (tc/to-long time-1)
                               :modified-time (tc/to-long time-1)}
                       :value 1}
                   :b {:stats {:creation-time (tc/to-long time-2)
                               :modified-time (tc/to-long time-5)}
                       :value 17}}}
           (get-in (kv/state cached-kv-store include-flags) [:inner-state :store])))
    ; delete removes entry from cache
    (kv/delete cached-kv-store :b)
    (is (false? (cu/cache-contains? cache :b)))
    (is (nil? (kv/fetch local-kv-store :b)))
    (is (nil? (kv/fetch cached-kv-store :b)))
    (is (nil? (kv/fetch cached-kv-store :b :refresh true)))
    (is (= "cache" (get (kv/state cached-kv-store include-flags) :variant)))
    (is (= {:count 1
            :data {:a {:stats {:creation-time (tc/to-long time-1)
                               :modified-time (tc/to-long time-1)}
                       :value 1}}}
           (get-in (kv/state cached-kv-store include-flags) [:inner-state :store])))))

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
              bytes (byte-array 10)
              include-flags #{"data"}
              creation-time-promise (promise)]
          (Arrays/fill bytes (byte 1))
          (is (nil? (kv/fetch test-store "a")))
          (is (nil? (get-value-from-curator "a")))
          (kv/store test-store "a" bytes)
          (let [{:keys [stat]} (curator/read-path curator
                                                  (kv/key->zk-path services-base-path "a")
                                                  :nil-on-missing? true
                                                  :serializer :nippy)
                {:keys [ctime mtime]} stat]
            (is (= {:creation-time ctime :modified-time mtime}
                   (kv/stats test-store "a")))
            (is (= ctime mtime))
            (deliver creation-time-promise ctime))
          (is (Arrays/equals bytes ^bytes (kv/fetch test-store "a")))
          (is (not (nil? (get-value-from-curator "a"))))
          (kv/store test-store "a" 3)
          (let [{:keys [stat]} (curator/read-path curator
                                                  (kv/key->zk-path services-base-path "a")
                                                  :nil-on-missing? true
                                                  :serializer :nippy)
                {:keys [ctime mtime]} stat]
            (is (= {:creation-time ctime :modified-time mtime}
                   (kv/stats test-store "a")))
            (is (= @creation-time-promise ctime))
            (is (< @creation-time-promise mtime)))
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
          (is (= {:base-path services-base-path, :variant "zookeeper"} (kv/state test-store include-flags)))))
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
