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
(ns waiter.mesos.marathon-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [waiter.mesos.marathon :refer :all]
            [waiter.util.http-utils :as http-utils]))

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
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :post "/v2/apps")]
        (create-app marathon-api {})))

    (testing "delete-app"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :delete (str "/v2/apps/" app-id))]
        (delete-app marathon-api app-id)))

    (testing "get-apps"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :get "/v2/apps")]
        (get-apps marathon-api)))

    (testing "get-deployments"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :get "/v2/deployments")]
        (get-deployments marathon-api)))

    (testing "get-info"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :get "/v2/info")]
        (get-info marathon-api)))

    (testing "kill-task"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :delete (str "/v2/apps/" app-id "/tasks/" task-id))]
        (kill-task marathon-api app-id task-id false true)))

    (testing "update-app"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :put (str "/v2/apps/" app-id))]
        (update-app marathon-api app-id {})))))
