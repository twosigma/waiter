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
(ns waiter.migration-config-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [waiter.migration-config :refer :all]
            [waiter.util.utils :as utils]
            [waiter.util.date-utils :as du])
  (:import (java.io File)))

(deftest test-config-activated?
  (let [time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 10))
        time-2 (t/plus time-0 (t/seconds 20))]
    (is (true? (config-activated? nil time-1)))
    (is (true? (config-activated? {:start-time nil} time-1)))
    (is (true? (config-activated? {:start-time time-0} time-1)))
    (is (true? (config-activated? {:start-time time-0} time-2)))
    (is (false? (config-activated? {:start-time time-1} time-0)))
    (is (true? (config-activated? {:start-time time-1} time-1)))
    (is (true? (config-activated? {:start-time time-1} time-2)))))

(deftest test-filter-config
  (let [time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 10))
        time-2 (t/plus time-0 (t/seconds 20))
        config-entry {:excludes [{:entries ["fee"]}
                                 {:entries ["fie"] :start-time time-0}
                                 {:entries ["foe"] :start-time time-1}
                                 {:entries ["fum"] :start-time time-2}]
                      :includes [{:entries ["tee"] :start-time time-0}
                                 {:entries ["tie"]}
                                 {:entries ["toe"] :start-time time-1}
                                 {:entries ["tum"] :start-time time-2}]}
        expected-config {:excludes [{:entries ["fee"]}
                                    {:entries ["fie"] :start-time time-0}
                                    {:entries ["foe"] :start-time time-1}]
                         :includes [{:entries ["tee"] :start-time time-0}
                                    {:entries ["tie"]}
                                    {:entries ["toe"] :start-time time-1}]}]
    (is (= expected-config (filter-config config-entry time-1)))))

(deftest test-state
  (let [time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 10))
        time-2 (t/plus time-0 (t/seconds 20))
        config-entry {:excludes [{:entries ["fee"]}
                                 {:entries ["fie"] :start-time time-0}
                                 {:entries ["foe"] :start-time time-1}
                                 {:entries ["fum"] :start-time time-2}]
                      :includes [{:entries ["tee"] :start-time time-0}
                                 {:entries ["tie"]}
                                 {:entries ["toe"] :start-time time-1}
                                 {:entries ["tum"] :start-time time-2}]}
        activated-config {:excludes [{:entries ["fee"]}
                                     {:entries ["fie"] :start-time time-0}
                                     {:entries ["foe"] :start-time time-1}]
                          :includes [{:entries ["tee"] :start-time time-0}
                                     {:entries ["tie"]}
                                     {:entries ["toe"] :start-time time-1}]}
        config-atom (atom {:config config-entry
                           :time time-0})]
    (with-redefs [t/now (constantly time-1)]
      (is (= {:name "MigrationConfigLoader"
              :supported-include-params ["activated-config" "config"]
              :time time-0}
             (state {:config-atom config-atom} #{})))
      (is (= {:config config-entry
              :name "MigrationConfigLoader"
              :supported-include-params ["activated-config" "config"]
              :time time-0}
             (state {:config-atom config-atom} #{"config"})))
      (is (= {:activated-config activated-config
              :name "MigrationConfigLoader"
              :supported-include-params ["activated-config" "config"]
              :time time-0}
             (state {:config-atom config-atom} #{"activated-config"}))))))

(deftest test-load-config
  (let [work-directory (System/getProperty "user.dir")
        config-file (str work-directory (File/separator) "test-files/test-migration-config.edn")
        time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 10))
        config-atom (atom {:config nil :time time-0})]
    (with-redefs [t/now (constantly time-1)
                  utils/exit (constantly nil) 2]
      (load-config config-file config-atom)
      (is (= time-1 (:time @config-atom)))
      (is (= 3 (-> @config-atom :config :reverse-proxy :excludes (count))))
      (is (= 2 (-> @config-atom :config :reverse-proxy :includes (count)))))))

(deftest test-retrieve-config
  (let [time-0 (t/now)
        time-1 (t/plus time-0 (t/seconds 10))
        time-2 (t/plus time-0 (t/seconds 20))
        config-atom (atom {:config {:reverse-proxy {:excludes [{:entries ["fee"]}
                                                               {:entries ["fie"] :start-time time-0}
                                                               {:entries ["foe"] :start-time time-1}
                                                               {:entries ["fum"] :start-time time-2}]
                                                    :includes [{:entries ["tee"] :start-time time-0}
                                                               {:entries ["tie"]}
                                                               {:entries ["toe"] :start-time time-1}
                                                               {:entries ["tum"] :start-time time-2}]}}
                           :time time-2})
        actual-config (retrieve-config {:config-atom config-atom} time-1 [:reverse-proxy])
        expected-config {:excludes [{:entries ["fee"]}
                                    {:entries ["fie"] :start-time time-0}
                                    {:entries ["foe"] :start-time time-1}]
                         :includes [{:entries ["tee"] :start-time time-0}
                                    {:entries ["tie"]}
                                    {:entries ["toe"] :start-time time-1}]}]
    (is (= expected-config actual-config))))

(deftest test-determine-selection
  (let [config {:excludes [{:entries ["fee"]}
                           {:entries ["fie"]}
                           {:entries ["foe"]}]
                :includes [{:entries ["fee"]}
                           {:entries ["fum"]}]}
        make-match-fn (fn [parameter-name]
                        (fn [entry] (= entry parameter-name)))]
    (is (= :exclude (determine-selection config (make-match-fn "fee") :default)))
    (is (= :exclude (determine-selection config (make-match-fn "fie") :default)))
    (is (= :exclude (determine-selection config (make-match-fn "foe") :default)))
    (is (= :include (determine-selection config (make-match-fn "fum") :default)))
    (is (= :default (determine-selection config (make-match-fn "bar") :default)))))

(deftest test-initialize-config-loader
  (let [work-directory (System/getProperty "user.dir")
        config-file (str work-directory (File/separator) "test-files/test-migration-config.edn")]
    (let [context {:config-file config-file :reload-interval-ms 100}
          {:keys [cancel-fn config-atom]} (initialize-config-loader context)]
      (try
        (let [time-0 (-> @config-atom :time)
              _ (Thread/sleep 200)
              time-1 (-> @config-atom :time)
              _ (Thread/sleep 200)
              time-2 (-> @config-atom :time)]
          (is (t/after? time-1 time-0))
          (is (t/after? time-2 time-1)))
        (finally
          (when cancel-fn
            (cancel-fn)))))))