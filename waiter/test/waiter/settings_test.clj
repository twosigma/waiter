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
                                                    :zk {:foo "foo"
                                                         :bar "bar"}
                                                    :encrypt "fie"}
                                        :scheduler-gc-config {:broken-service-min-hosts 10
                                                              :broken-service-timeout-mins 300}})]
      (let [loaded-settings (load-settings "some-config.edn" "some-git-version")]
        (is (= (-> settings-defaults
                   (assoc :git-version "some-git-version")
                   (assoc-in [:kv-config :zk :foo] "foo")
                   (assoc-in [:kv-config :zk :bar] "bar")
                   (assoc-in [:kv-config :encrypt] "fie")
                   (assoc-in [:scheduler-gc-config :broken-service-min-hosts] 10)
                   (assoc-in [:scheduler-gc-config :broken-service-timeout-mins] 300))
               loaded-settings))))))

(defn- load-config-file
  "Calls load-settings on config-file with a fake git version string"
  [config-file]
  (load-settings config-file "some-git-version"))

(defn- load-full-settings
  "Loads config-full.edn"
  []
  (load-config-file "config-full.edn"))

(defn- load-min-settings
  "Loads config-minimal.edn"
  []
  (load-config-file "config-minimal.edn"))

(defn- load-minimesos-settings
  "Loads config-minimesos.edn"
  []
  (load-config-file "config-minimesos.edn"))

(defn- load-shell-settings
  "Loads config-shell.edn"
  []
  (load-config-file "config-shell.edn"))

(deftest test-validate-minimal-settings
  (testing "Test validating minimal settings"
    (is (nil? (s/check settings-schema (load-min-settings))))))

(deftest test-validate-full-settings
  (testing "Test validating full settings"
    (is (nil? (s/check settings-schema (load-full-settings))))))

(deftest test-validate-full-settings-without-defaults
  (testing "Test validating full settings"
    (let [loaded-settings (load-settings-file "config-full.edn")]
      (is (nil? (s/check settings-schema (assoc loaded-settings :git-version "some-git-version")))))))

(defn- settings-with-bogus-factory-fn
  "Returns a settings map with the given key's :kind
  sub-map containing a bogus (non-symbol) :factory-fn"
  [k]
  (let [$ (load-full-settings)]
    (assoc-in $ [k (get-in $ [k :kind]) :factory-fn] "not-a-symbol")))

(deftest test-factory-fn-should-be-symbol
  (is (some? (s/check settings-schema (settings-with-bogus-factory-fn :cors-config))))
  (is (some? (s/check settings-schema (settings-with-bogus-factory-fn :entitlement-config))))
  (is (some? (s/check settings-schema (settings-with-bogus-factory-fn :kv-config))))
  (is (some? (s/check settings-schema (settings-with-bogus-factory-fn :password-store-config))))
  (is (some? (s/check settings-schema (settings-with-bogus-factory-fn :scheduler-config))))
  (is (some? (s/check settings-schema (settings-with-bogus-factory-fn :service-description-builder-config)))))

(defn- settings-with-missing-kind-sub-map
  "Returns a settings map with the given key's :kind sub-map removed"
  [k]
  (let [$ (load-full-settings)]
    (update-in $ [k] #(dissoc % (get-in $ [k :kind])))))

(deftest test-kind-sub-map-should-be-present
  (is (some? (s/check settings-schema (settings-with-missing-kind-sub-map :cors-config))))
  (is (some? (s/check settings-schema (settings-with-missing-kind-sub-map :entitlement-config))))
  (is (some? (s/check settings-schema (settings-with-missing-kind-sub-map :kv-config))))
  (is (some? (s/check settings-schema (settings-with-missing-kind-sub-map :password-store-config))))
  (is (some? (s/check settings-schema (settings-with-missing-kind-sub-map :scheduler-config))))
  (is (some? (s/check settings-schema (settings-with-missing-kind-sub-map :service-description-builder-config)))))

(deftest test-deep-merge-settings
  (testing "Deep merging of configuration settings"

    (testing "should support partial configuration of :kind implementations"
      (let [defaults {:scheduler-config {:kind :marathon
                                         :marathon {:factory-fn waiter.scheduler.marathon/marathon-scheduler
                                                    :home-path-prefix "/home/"
                                                    :http-options {:conn-timeout 10000
                                                                   :socket-timeout 10000}
                                                    :force-kill-after-ms 60000
                                                    :framework-id-ttl 900000}}}
            configured {:scheduler-config {:kind :marathon
                                           :marathon {:url "http://marathon.example.com:8080"}}}]
        (is (= {:scheduler-config {:kind :marathon
                                   :marathon {:factory-fn waiter.scheduler.marathon/marathon-scheduler
                                              :home-path-prefix "/home/"
                                              :http-options {:conn-timeout 10000
                                                             :socket-timeout 10000}
                                              :force-kill-after-ms 60000
                                              :framework-id-ttl 900000
                                              :url "http://marathon.example.com:8080"}}}
               (deep-merge-settings defaults configured)))))

    (testing "should support defaulting the fields of the non-default :kind"
      (let [defaults {:scheduler-config {:kind :foo
                                         :foo {:bar 1
                                               :baz 2}
                                         :shell {:factory-fn 'waiter.scheduler.shell-scheduler/shell-scheduler
                                                 :health-check-interval-ms 10000
                                                 :health-check-timeout-ms 200
                                                 :port-grace-period-ms 120000
                                                 :port-range [10000 10999]
                                                 :work-directory "scheduler"}}}
            configured {:scheduler-config {:kind :shell}}]
        (is (= {:scheduler-config {:kind :shell
                                   :foo {:bar 1
                                         :baz 2}
                                   :shell {:factory-fn 'waiter.scheduler.shell-scheduler/shell-scheduler
                                           :health-check-interval-ms 10000
                                           :health-check-timeout-ms 200
                                           :port-grace-period-ms 120000
                                           :port-range [10000 10999]
                                           :work-directory "scheduler"}}}
               (deep-merge-settings defaults configured)))))

    (testing "should support partial configuration of the non-default :kind"
      (let [defaults {:scheduler-config {:kind :foo
                                         :foo {:bar 1
                                               :baz 2}
                                         :shell {:factory-fn 'waiter.scheduler.shell-scheduler/shell-scheduler
                                                 :health-check-interval-ms 10000
                                                 :health-check-timeout-ms 200
                                                 :port-grace-period-ms 120000
                                                 :port-range [10000 10999]
                                                 :work-directory "scheduler"}}}
            configured {:scheduler-config {:kind :shell
                                           :shell {:health-check-interval-ms 1}}}]
        (is (= {:scheduler-config {:kind :shell
                                   :foo {:bar 1
                                         :baz 2}
                                   :shell {:factory-fn 'waiter.scheduler.shell-scheduler/shell-scheduler
                                           :health-check-interval-ms 1
                                           :health-check-timeout-ms 200
                                           :port-grace-period-ms 120000
                                           :port-range [10000 10999]
                                           :work-directory "scheduler"}}}
               (deep-merge-settings defaults configured)))))

    (testing "should not merge sub-maps not related to the configured :kind"
      (let [defaults {:scheduler-config {:kind :foo
                                         :foo {:bar 1
                                               :baz 2}
                                         :qux {:one "a"
                                               :two "b"}}}
            configured {:scheduler-config {:kind :qux
                                           :qux {:two "c"}
                                           :foo {:other 3}}}]
        (is (= {:scheduler-config {:kind :qux
                                   :foo {:other 3}
                                   :qux {:one "a"
                                         :two "c"}}}
               (deep-merge-settings defaults configured)))))))

(deftest test-validate-minimesos-settings
  (testing "Test validating minimesos settings"
    (let [port 12345
          run-as-user "foo"
          marathon "bar"
          minimesos-network-gateway "baz"
          zk-connect-string "qux"]
      (with-redefs [env (fn [name]
                          (case name
                            "WAITER_PORT" (str port)
                            "WAITER_AUTH_RUN_AS_USER" run-as-user
                            "WAITER_MARATHON" marathon
                            "MINIMESOS_NETWORK_GATEWAY" minimesos-network-gateway
                            "WAITER_ZOOKEEPER_CONNECT_STRING" zk-connect-string
                            (throw (ex-info "Unexpected environment variable" {:name name}))))]
        (let [settings (load-minimesos-settings)]
          (is (nil? (s/check settings-schema settings)))
          (is (= port (:port settings)))
          (is (= run-as-user (get-in settings [:authenticator-config :one-user :run-as-user])))
          (is (= marathon (get-in settings [:scheduler-config :marathon :url])))
          (is (= minimesos-network-gateway (:hostname settings)))
          (is (= zk-connect-string (get-in settings [:zookeeper :connect-string]))))))))

(deftest test-validate-shell-settings
  (testing "Test validating shell scheduler settings"
    (let [port 12345
          run-as-user "foo"]
      (with-redefs [env (fn [name]
                          (case name
                            "WAITER_PORT" (str port)
                            "WAITER_AUTH_RUN_AS_USER" run-as-user
                            (throw (ex-info "Unexpected environment variable" {:name name}))))]
        (let [settings (load-shell-settings)]
          (is (nil? (s/check settings-schema settings)))
          (is (= port (:port settings)))
          (is (= run-as-user (get-in settings [:authenticator-config :one-user :run-as-user]))))))))
