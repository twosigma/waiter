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
  (:require [clojure.test :refer :all]
            [waiter.mesos.marathon :refer :all]
            [waiter.util.http-utils :as http-utils]))

(def ^:private http-client (Object.))

(def ^:private marathon-url "http://marathon.localtest.me:1234")

(defn- assert-endpoint-request-method
  ([expected-method expected-url]
    (assert-endpoint-request-method expected-method expected-url nil))
  ([expected-method expected-url expected-query-string]
    (fn [in-http-client in-request-url & {:keys [query-string request-method]}]
      (is (= http-client in-http-client))
      (is (= expected-method request-method))
      (when expected-query-string
        (is (= expected-query-string query-string)))
      (let [expected-absolute-url (str marathon-url expected-url)]
        (is (= expected-absolute-url in-request-url))))))

(deftest test-marathon-rest-api-endpoints
  (let [app-id "test-app-id"
        task-id "test-app-id.test-task-id"
        marathon-api (api-factory http-client {} marathon-url)]

    (testing "create-app"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :post "/v2/apps")]
        (create-app marathon-api {})))

    (testing "delete-service"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :delete (str "/v2/apps/" app-id))]
        (delete-service marathon-api app-id)))

    (testing "delete-deployment"
      (let [deployment-id "d1234"
            endpoint (str "/v2/deployments/" deployment-id)
            query-string {"force" true}]
        (with-redefs [http-utils/http-request (assert-endpoint-request-method :delete endpoint query-string)]
          (delete-deployment marathon-api deployment-id))))

    (testing "get-apps"
      (let [query-string {"embed" ["apps.lastTaskFailure" "apps.tasks"]}]
        (with-redefs [http-utils/http-request (assert-endpoint-request-method :get "/v2/apps" query-string)]
          (get-apps marathon-api query-string))))

    (testing "get-deployments"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :get "/v2/deployments")]
        (get-deployments marathon-api)))

    (testing "get-info"
      (with-redefs [http-utils/http-request (assert-endpoint-request-method :get "/v2/info")]
        (get-info marathon-api)))

    (testing "kill-task"
      (let [endpoint (str "/v2/apps/" app-id "/tasks/" task-id)
            query-string {"force" true "scale" false}]
        (with-redefs [http-utils/http-request (assert-endpoint-request-method :delete endpoint query-string)]
          (kill-task marathon-api app-id task-id false true))))

    (testing "update-app"
      (let [endpoint (str "/v2/apps/" app-id)
            query-string {"force" true}]
        (with-redefs [http-utils/http-request (assert-endpoint-request-method :put endpoint query-string)]
          (update-app marathon-api app-id {}))))))
