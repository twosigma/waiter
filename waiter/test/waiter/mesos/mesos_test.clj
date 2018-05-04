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
(ns waiter.mesos.mesos-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.mesos.mesos :refer :all]
            [waiter.util.http-utils :as http-utils]))

(deftest test-mesos-api
  (let [http-client (Object.)
        marathon-url "http://www.marathon.com:1234"
        slave-port 9876
        slave-directory "/some/directory"
        mesos-api (api-factory http-client {} slave-port slave-directory)
        assert-endpoint-request-method (fn [expected-method expected-url]
                                         (fn [in-http-client in-request-url & {:keys [request-method]}]
                                           (is (= http-client in-http-client))
                                           (is (= expected-method request-method))
                                           (let [expected-absolute-url (if (str/starts-with? expected-url "http://")
                                                                         expected-url
                                                                         (str marathon-url expected-url))]
                                             (is (= expected-absolute-url in-request-url)))))]

    (testing "build-sandbox-path"
      (let [slave-id "salve-is"
            framework-id "framework-id"
            instance-id "instance-id"]
        (is (= (str slave-directory "/" slave-id "/frameworks/" framework-id "/executors/" instance-id "/runs/latest")
               (build-sandbox-path mesos-api slave-id framework-id instance-id)))
        (is (nil? (build-sandbox-path mesos-api nil framework-id instance-id)))
        (is (nil? (build-sandbox-path mesos-api slave-id nil instance-id)))
        (is (nil? (build-sandbox-path mesos-api slave-id framework-id nil)))))

    (testing "list-directory-content"
      (let [host "www.host.com"]
        (with-redefs [http-utils/http-request
                      (assert-endpoint-request-method :get (str "http://" host ":" slave-port "/files/browse"))]
          (list-directory-content mesos-api host "/some/directory"))))

    (testing "build-directory-download-link"
      (let [host "www.host.com"
            directory "/some/directory/instance-1/runs"]
        (is (= (str "http://" host ":" slave-port "/files/download?path=" directory)
               (build-directory-download-link mesos-api host directory)))
        (is (nil? (build-directory-download-link mesos-api nil directory)))
        (is (nil? (build-directory-download-link mesos-api host nil)))))

    (testing "get-agent-state"
      (let [host "www.host.com"]
        (with-redefs [http-utils/http-request
                      (assert-endpoint-request-method :get (str "http://" host ":" slave-port "/state.json"))]
          (get-agent-state mesos-api host))))))
