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
(ns waiter.settings-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [waiter.settings :refer :all]
            [waiter.utils :as utils]))

(deftest test-load-missing-edn-file
  (let [exit-called-atom (atom false)]
    (with-redefs [utils/exit (fn [status msg]
                               (is (= 1 status))
                               (is msg)
                               (reset! exit-called-atom true))]
      (load-settings-file "a-file-that-does-not-exist")
      (is @exit-called-atom))))

(deftest test-load-existing-edn-file
  (let [test-cases (list
                     {:name "load-clj-file-foo-clj"
                      :input "test-files/test-foo.edn"
                      :expected {
                                 :foo1 "one.${foo2}"
                                 :foo2 2
                                 :foo3 [3]
                                 :foo4 {:key 4}
                                 :common "from-foo"}}
                     {:name "load-clj-file-bar-bar"
                      :input "test-files/test-bar.edn"
                      :expected {:bar1 "one.${system.user.name}"
                                 :bar2 2
                                 :bar3 [3]
                                 :bar4 {:key 4}
                                 :common "from-bar"}})]
    (doseq [{:keys [name input expected]} test-cases]
      (testing (str "Test " name)
        (is (= expected (load-settings-file input)))))))

(deftest test-validate-nested-merge-settings
  (testing "Test validating nested merge settings"
    (with-redefs [load-settings-file (fn [file-name]
                                       (is (= "some-config.edn" file-name))
                                       {:kv-config {:kind :zk
                                                    :foo "foo"
                                                    :bar "bar"
                                                    :encrypt "fie"}
                                        :scheduler-gc-config {:broken-service-min-hosts 10
                                                              :broken-service-timeout-mins 300}})]
      (let [loaded-settings (load-settings "some-config.edn" "some-git-version")]
        (is (= (-> settings-defaults
                   (assoc :git-version "some-git-version")
                   (assoc-in [:kv-config :foo] "foo")
                   (assoc-in [:kv-config :bar] "bar")
                   (assoc-in [:kv-config :encrypt] "fie")
                   (assoc-in [:scheduler-gc-config :broken-service-min-hosts] 10)
                   (assoc-in [:scheduler-gc-config :broken-service-timeout-mins] 300))
               loaded-settings))))))

(deftest test-validate-minimal-settings
  (testing "Test validating minimal settings"
    (let [loaded-settings (load-settings "config-minimal.edn" "some-git-version")]
      (is (nil? (s/check settings-schema loaded-settings))))))

(deftest test-validate-full-settings
  (testing "Test validating full settings"
    (let [loaded-settings (load-settings "config-full.edn" "some-git-version")]
      (is (nil? (s/check settings-schema loaded-settings))))))

(deftest test-validate-full-settings-without-defaults
  (testing "Test validating full settings"
    (let [loaded-settings (load-settings-file "config-full.edn")]
      (is (nil? (s/check settings-schema (assoc loaded-settings :git-version  "some-git-version")))))))
