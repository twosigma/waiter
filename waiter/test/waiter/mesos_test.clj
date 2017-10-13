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
(ns waiter.mesos-test
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [qbits.jet.client.http :as http]
            [waiter.mesos :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-http-request
  (testing "successful-response"
    (let [http-client (Object.)
          expected-body {:foo "bar"
                         :fee {:fie {:foe "fum"}}
                         :solve 42}]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (json/write-str expected-body))
                                     (async/>!! response-chan {:body body-chan, :status 200})
                                     response-chan))]
        (is (= expected-body (http-request http-client "some-url"))))))

  (testing "error-in-response"
    (let [http-client (Object.)
          expected-exception (IllegalStateException. "Test Exception")]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)]
                                     (async/>!! response-chan {:error expected-exception})
                                     response-chan))]
        (is (thrown-with-msg? IllegalStateException #"Test Exception"
                              (http-request http-client "some-url"))))))

  (testing "non-2XX-response-without-throw-exceptions"
    (let [http-client (Object.)
          expected-body {:foo "bar"
                         :fee {:fie {:foe "fum"}}
                         :solve 42}]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (json/write-str expected-body))
                                     (async/>!! response-chan {:body body-chan, :status 400})
                                     response-chan))]
        (is (= expected-body (http-request http-client "some-url" :throw-exceptions false))))))

  (testing "non-2XX-response-with-throw-exceptions"
    (let [http-client (Object.)]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (json/write-str {}))
                                     (async/>!! response-chan {:body body-chan, :status 400})
                                     response-chan))]
        (is (thrown? ExceptionInfo (http-request http-client "some-url")))))))

(deftest test-mesos-api
  (let [http-client (Object.)
        marathon-url "http://www.marathon.com:1234"
        slave-port 9876
        slave-directory "/some/directory"
        mesos-api (mesos-api-factory http-client {} slave-port slave-directory)
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
        (with-redefs [http-request (assert-endpoint-request-method :get (str "http://" host ":" slave-port "/files/browse"))]
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
        (with-redefs [http-request (assert-endpoint-request-method :get (str "http://" host ":" slave-port "/state.json"))]
          (get-agent-state mesos-api host))))))

(deftest test-marathon-rest-api-endpoints
  (let [http-client (Object.)
        marathon-url "http://www.marathon.com:1234"
        app-id "test-app-id"
        task-id "test-app-id.test-task-id"
        marathon-api (marathon-rest-api-factory http-client {} marathon-url)
        assert-endpoint-request-method (fn [expected-method expected-url]
                                         (fn [in-http-client in-request-url & {:keys [request-method]}]
                                           (is (= http-client in-http-client))
                                           (is (= expected-method request-method))
                                           (let [expected-absolute-url (if (str/starts-with? expected-url "http://")
                                                                         expected-url
                                                                         (str marathon-url expected-url))]
                                             (is (= expected-absolute-url in-request-url)))))]

    (testing "create-app"
      (with-redefs [http-request (assert-endpoint-request-method :post "/v2/apps")]
        (create-app marathon-api {})))

    (testing "delete-app"
      (with-redefs [http-request (assert-endpoint-request-method :delete (str "/v2/apps/" app-id))]
        (delete-app marathon-api app-id)))

    (testing "get-apps"
      (with-redefs [http-request (assert-endpoint-request-method :get "/v2/apps")]
        (get-apps marathon-api)))

    (testing "get-deployments"
      (with-redefs [http-request (assert-endpoint-request-method :get "/v2/deployments")]
        (get-deployments marathon-api)))

    (testing "get-info"
      (with-redefs [http-request (assert-endpoint-request-method :get "/v2/info")]
        (get-info marathon-api)))

    (testing "kill-task"
      (with-redefs [http-request (assert-endpoint-request-method :delete (str "/v2/apps/" app-id "/tasks/" task-id))]
        (kill-task marathon-api app-id task-id false true)))

    (testing "update-app"
      (with-redefs [http-request (assert-endpoint-request-method :put (str "/v2/apps/" app-id))]
        (update-app marathon-api app-id {})))))
