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
(ns waiter.marathon-api-test
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [qbits.jet.client.http :as http]
            [waiter.marathon-api :refer :all])
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

(deftest test-marathon-rest-api-endpoints
  (let [http-client (Object.)]
    (with-redefs [http/client (constantly http-client)]
      (let [marathon-url "http://www.marathon.com:1234"
            app-id "test-app-id"
            task-id "test-app-id.test-task-id"
            marathon-api (marathon-rest-api-factory {} marathon-url)
            assert-endpoint-request-method (fn [expected-method expected-url]
                                             (fn [in-http-client in-request-url & {:keys [request-method]}]
                                               (is (= http-client in-http-client))
                                               (is (= expected-method request-method))
                                               (let [expected-absolute-url (if (str/starts-with? expected-url "http://")
                                                                             expected-url
                                                                             (str marathon-url expected-url))]
                                                 (is (= expected-absolute-url in-request-url)))))]

        (testing "agent-directory-content"
          (let [host "www.host.com"
                port 9876]
            (with-redefs [http-request (assert-endpoint-request-method :get (str "http://" host ":" port "/files/browse"))]
              (agent-directory-content marathon-api host port "/some/directory"))))

        (testing "agent-state"
          (let [host "www.host.com"
                port 9876]
            (with-redefs [http-request (assert-endpoint-request-method :get (str "http://" host ":" port "/state.json"))]
              (agent-state marathon-api host port))))

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
            (update-app marathon-api app-id {})))))))
