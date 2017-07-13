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
(ns token-syncer.settings-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [token-syncer.settings :refer :all]
            [token-syncer.utils :as utils])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-validate-minimal-settings
  (testing "Test validating minimal settings"
    (is (nil? (s/check settings-schema settings-defaults)))))

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

(deftest test-load-settings-from-file
  (testing "Load valid settings"
    (with-redefs [load-settings-file (fn [_]
                                       {:http-client-properties {:connection-timeout-ms 1000
                                                                 :idle-timeout-ms 30000
                                                                 :use-spnego false}})]
      (let [loaded-settings (load-settings "some-config.edn" "some-git-version")]
        (is (= {:git-version "some-git-version"
                :http-client-properties {:connection-timeout-ms 1000
                                         :idle-timeout-ms 30000
                                         :use-spnego false}}
               loaded-settings)))))

  (testing "Load config-full.edn settings"
    (let [loaded-settings (load-settings "config-full.edn" "some-git-version")]
      (is (= {:git-version "some-git-version"
              :http-client-properties {:connection-timeout-ms 5000
                                       :idle-timeout-ms 30000
                                       :use-spnego false}}
             loaded-settings))))

  (testing "Load invalid settings"
    (with-redefs [load-settings-file (fn [file-name]
                                       {:router-id->url {"foo" (str "foo." file-name ".com")}})]
      (is (thrown-with-msg? ExceptionInfo #"Value does not match schema"
                            (load-settings "some-config.edn" "some-git-version"))))))
