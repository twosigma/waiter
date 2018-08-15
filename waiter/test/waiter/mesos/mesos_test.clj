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
(ns waiter.mesos.mesos-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.mesos.mesos :refer :all]
            [waiter.util.http-utils :as http-utils]
            [waiter.util.utils :as utils]))

(deftest test-mesos-api
  (let [http-client (Object.)
        marathon-url "http://marathon.localtest.me:1234"
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
