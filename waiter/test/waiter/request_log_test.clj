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
(ns waiter.request-log-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [waiter.request-log :refer :all]))

(deftest test-request->context
  (let [request {:client-protocol "HTTP/2.0"
                 :headers {"content-length" "20"
                           "content-type" "application/json"
                           "host" "host"
                           "origin" "www.origin.org"
                           "user-agent" "test-user-agent"
                           "x-cid" "123"}
                 :internal-protocol "HTTP/1.1"
                 :query-string "a=1"
                 :remote-addr "127.0.0.1"
                 :request-id "abc"
                 :request-method :post
                 :request-time (t/date-time 2018 4 11)
                 :scheme "http"
                 :server-port 9090
                 :uri "/"}]
    (is (= {:cid "123"
            :client-protocol "HTTP/2.0"
            :host "host"
            :internal-protocol "HTTP/1.1"
            :method "POST"
            :origin "www.origin.org"
            :path "/"
            :query-string "a=1"
            :remote-addr "127.0.0.1"
            :request-content-length "20"
            :request-content-type "application/json"
            :request-id "abc"
            :request-time "2018-04-11T00:00:00.000Z"
            :scheme "http"
            :server-port 9090
            :user-agent "test-user-agent"}
           (request->context request)))))

(deftest test-response->context
  (let [response {:authorization/method :cookie
                  :authorization/principal "principal@DOMAIN.COM"
                  :backend-response-latency-ns 1000
                  :descriptor {:service-id "service-id"
                               :service-description {"metric-group" "service-metric-group"
                                                     "name" "service-name"
                                                     "run-as-user" "john.doe"
                                                     "version" "service-version"}
                               :source-tokens [{"token" "test-token1" "version" "E-1234"}
                                               {"token" "test-token2" "version" "E-4321"}]}
                  :error-class "java.lang.Exception"
                  :get-instance-latency-ns 500
                  :handle-request-latency-ns 2000
                  :headers {"content-length" "40"
                            "content-type" "application/xml"
                            "grpc-status" "13"
                            "location" "/foo/bar"
                            "server" "foo-bar"}
                  :instance {:host "instance-host"
                             :id "instance-id"
                             :k8s/node-name "test-node-name"
                             :k8s/pod-name "test-pod-name"
                             :port 123}
                  :instance-proto "instance-proto"
                  :latest-service-id "latest-service-id"
                  :protocol "HTTP/2.0"
                  :request-type "test-request"
                  :status 200
                  :waiter-api-call? false}]
    (is (= {:authentication-method "cookie"
            :backend-response-latency-ns 1000
            :backend-protocol "HTTP/2.0"
            :get-instance-latency-ns 500
            :grpc-status "13"
            :handle-request-latency-ns 2000
            :instance-host "instance-host"
            :instance-id "instance-id"
            :instance-port 123
            :instance-proto "instance-proto"
            :k8s-node-name "test-node-name"
            :k8s-pod-name "test-pod-name"
            :latest-service-id "latest-service-id"
            :metric-group "service-metric-group"
            :principal "principal@DOMAIN.COM"
            :request-type "test-request"
            :response-content-length "40"
            :response-content-type "application/xml"
            :response-location "/foo/bar"
            :run-as-user "john.doe"
            :server "foo-bar"
            :service-id "service-id"
            :service-name "service-name"
            :service-version "service-version"
            :status 200
            :token "test-token1,test-token2"
            :waiter-api false
            :waiter-error-class "java.lang.Exception"}
           (response->context response)))))

(deftest test-log-request!-with-redactions
  (let [request {:client-protocol "HTTP/2.0"
                 :headers {"content-length" "20"
                           "content-type" "application/json"
                           "host" "host"
                           "origin" "www.origin.org"
                           "user-agent" "test-user-agent"
                           "x-cid" "123"}
                 :internal-protocol "HTTP/1.1"
                 :query-string "a=1"
                 :remote-addr "127.0.0.1"
                 :request-id "abc"
                 :request-method :post
                 :request-time (t/date-time 2018 4 11)
                 :scheme "http"
                 :server-port 9090
                 :uri "/"}
        response {:authorization/method :cookie
                  :authorization/principal "principal@DOMAIN.COM"
                  :backend-response-latency-ns 1000
                  :descriptor {:service-id "service-id"
                               :service-description {"metric-group" "service-metric-group"
                                                     "name" "service-name"
                                                     "run-as-user" "john.doe"
                                                     "version" "service-version"
                                                     "env" {"REDACTED_REQUEST_FIELDS" "path,principal"}}
                               :source-tokens [{"token" "test-token1" "version" "E-1234"}
                                               {"token" "test-token2" "version" "E-4321"}]}
                  :error-class "java.lang.Exception"
                  :get-instance-latency-ns 500
                  :handle-request-latency-ns 2000
                  :headers {"content-length" "40"
                            "content-type" "application/xml"
                            "grpc-status" "13"
                            "location" "/foo/bar"
                            "server" "foo-bar"}
                  :instance {:host "instance-host"
                             :id "instance-id"
                             :k8s/node-name "test-node-name"
                             :k8s/pod-name "test-pod-name"
                             :port 123}
                  :instance-proto "instance-proto"
                  :latest-service-id "latest-service-id"
                  :protocol "HTTP/2.0"
                  :request-type "test-request"
                  :status 200
                  :waiter-api-call? false}
        log-entries (atom [])]
    (with-redefs [log (fn [log-data]
                        (swap! log-entries conj log-data))]
      (let [_ (log-request! request response)
            log-entry (first @log-entries)]
        (is (= {:cid "123"
                :client-protocol "HTTP/2.0"
                :host "host"
                :internal-protocol "HTTP/1.1"
                :method "POST"
                :origin "www.origin.org"
                :query-string "a=1"
                :remote-addr "127.0.0.1"
                :request-content-length "20"
                :request-content-type "application/json"
                :request-id "abc"
                :request-time "2018-04-11T00:00:00.000Z"
                :scheme "http"
                :server-port 9090
                :user-agent "test-user-agent"
                :authentication-method "cookie"
                :backend-response-latency-ns 1000
                :backend-protocol "HTTP/2.0"
                :get-instance-latency-ns 500
                :grpc-status "13"
                :handle-request-latency-ns 2000
                :instance-host "instance-host"
                :instance-id "instance-id"
                :instance-port 123
                :instance-proto "instance-proto"
                :k8s-node-name "test-node-name"
                :k8s-pod-name "test-pod-name"
                :latest-service-id "latest-service-id"
                :metric-group "service-metric-group"
                :request-type "test-request"
                :response-content-length "40"
                :response-content-type "application/xml"
                :response-location "/foo/bar"
                :run-as-user "john.doe"
                :server "foo-bar"
                :service-id "service-id"
                :service-name "service-name"
                :service-version "service-version"
                :status 200
                :token "test-token1,test-token2"
                :waiter-api false
                :waiter-error-class "java.lang.Exception"}
               log-entry))))))

(deftest test-wrap-log
  (let [log-entries (atom [])]
    (with-redefs [log (fn [log-data]
                        (swap! log-entries conj log-data))]
      (let [handler (wrap-log (fn [_] {:status 200}))
            request {:headers {"content-type" "text/plain"
                               "host" "host"
                               "x-cid" "123"}
                     :remote-addr "127.0.0.1"
                     :request-id "abc"
                     :scheme :http
                     :uri "/path"}
            _ (handler request)
            log-entry (first @log-entries)]

        (is (:handle-request-latency-ns log-entry))
        (is (= {:cid "123"
                :host "host"
                :path "/path"
                :remote-addr "127.0.0.1"
                :request-content-type "text/plain"
                :request-id "abc"
                :scheme "http"
                :status 200}
               (dissoc log-entry :handle-request-latency-ns)))))))
