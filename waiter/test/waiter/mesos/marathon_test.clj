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
(ns waiter.mesos.marathon-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.mesos.marathon :refer :all]
            [waiter.mesos.utils :as mutils]))

(deftest test-marathon-rest-api-endpoints
  (let [http-client (Object.)
        marathon-url "http://www.marathon.com:1234"
        app-id "test-app-id"
        task-id "test-app-id.test-task-id"
        marathon-api (api-factory http-client {} marathon-url)
        assert-endpoint-request-method (fn [expected-method expected-url]
                                         (fn [in-http-client in-request-url & {:keys [request-method]}]
                                           (is (= http-client in-http-client))
                                           (is (= expected-method request-method))
                                           (let [expected-absolute-url (if (str/starts-with? expected-url "http://")
                                                                         expected-url
                                                                         (str marathon-url expected-url))]
                                             (is (= expected-absolute-url in-request-url)))))]

    (testing "create-app"
      (with-redefs [mutils/http-request (assert-endpoint-request-method :post "/v2/apps")]
        (create-app marathon-api {})))

    (testing "delete-app"
      (with-redefs [mutils/http-request (assert-endpoint-request-method :delete (str "/v2/apps/" app-id))]
        (delete-app marathon-api app-id)))

    (testing "get-apps"
      (with-redefs [mutils/http-request (assert-endpoint-request-method :get "/v2/apps")]
        (get-apps marathon-api)))

    (testing "get-deployments"
      (with-redefs [mutils/http-request (assert-endpoint-request-method :get "/v2/deployments")]
        (get-deployments marathon-api)))

    (testing "get-info"
      (with-redefs [mutils/http-request (assert-endpoint-request-method :get "/v2/info")]
        (get-info marathon-api)))

    (testing "kill-task"
      (with-redefs [mutils/http-request (assert-endpoint-request-method :delete (str "/v2/apps/" app-id "/tasks/" task-id))]
        (kill-task marathon-api app-id task-id false true)))

    (testing "update-app"
      (with-redefs [mutils/http-request (assert-endpoint-request-method :put (str "/v2/apps/" app-id))]
        (update-app marathon-api app-id {})))))
