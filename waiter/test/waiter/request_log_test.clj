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
(ns waiter.request-log-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [waiter.request-log :refer :all]))

(deftest test-request->context
  (let [request {:headers {"host" "host"
                           "user-agent" "test-user-agent"
                           "x-cid" "123"}
                 :request-method :post
                 :query-string "a=1"
                 :remote-addr "127.0.0.1"
                 :request-id "abc"
                 :request-time (t/date-time 2018 4 11)
                 :scheme "http"
                 :uri "/" }]
    (is (= {:cid "123"
            :host "host"
            :method "POST"
            :path "/"
            :query-string "a=1"
            :remote-addr "127.0.0.1"
            :request-id "abc"
            :request-time "2018-04-11T00:00:00.000Z"
            :scheme "http"
            :user-agent "test-user-agent"}
           (request->context request)))))

(deftest test-response->context
  (let [response {:authorization/principal "principal@DOMAIN.COM"
                  :backend-response-latency-ns 1000
                  :descriptor {:service-id "service-id"
                               :service-description {"metric-group" "service-metric-group"
                                                     "name" "service-name"
                                                     "version" "service-version"}}
                  :get-instance-latency-ns 500
                  :handle-request-latency-ns 2000
                  :instance {:host "instance-host"
                             :id "instance-id"
                             :port 123
                             :protocol "instance-proto"}
                  :latest-service-id "latest-service-id"
                  :status 200}]
    (is (= {:backend-response-latency-ns 1000
            :get-instance-latency-ns 500
            :handle-request-latency-ns 2000
            :instance-host "instance-host"
            :instance-id "instance-id"
            :instance-port 123
            :instance-proto "instance-proto"
            :latest-service-id "latest-service-id"
            :metric-group "service-metric-group"
            :principal "principal@DOMAIN.COM"
            :service-id "service-id"
            :service-name "service-name"
            :service-version "service-version"
            :status 200}
           (response->context response)))))

(deftest test-wrap-log
  (let [log-entries (atom [])]
    (with-redefs [log (fn [log-data]
                        (swap! log-entries conj log-data))]
      (let [handler (-> (fn [request] {:status 200})
                        wrap-log)
            request {:headers {"x-cid" "123"
                               "host" "host"}
                     :remote-addr "127.0.0.1"
                     :request-id "abc"
                     :scheme :http
                     :uri "/path"}
            response (handler request)
            log-entry (first @log-entries)]

        (is (:handle-request-latency-ns log-entry))
        (is (= {:cid "123"
                :host "host"
                :path "/path"
                :remote-addr "127.0.0.1"
                :request-id "abc"
                :scheme "http"
                :status 200}
               (dissoc log-entry :handle-request-latency-ns)))))))
