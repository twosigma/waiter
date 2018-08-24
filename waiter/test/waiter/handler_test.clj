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
(ns waiter.handler-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [full.async :as fa]
            [plumbing.core :as pc]
            [waiter.authorization :as authz]
            [waiter.core :as core]
            [waiter.handler :refer :all]
            [waiter.interstitial :as interstitial]
            [waiter.kv :as kv]
            [waiter.scheduler :as scheduler]
            [waiter.service-description :as sd]
            [waiter.statsd :as statsd]
            [waiter.test-helpers :refer :all]
            [waiter.util.utils :as utils])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (clojure.lang ExceptionInfo)
           (java.io StringBufferInputStream StringReader)))

(deftest test-complete-async-handler
  (testing "missing-request-id"
    (let [src-router-id "src-router-id"
          service-id "test-service-id"
          request {:basic-authentication {:src-router-id src-router-id}
                   :headers {"accept" "application/json"}
                   :route-params {:service-id service-id}
                   :uri (str "/waiter-async/complete//" service-id)}
          async-request-terminate-fn (fn [_] (throw (Exception. "unexpected call!")))
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn request)]
      (is (= 400 status))
      (is (= {"content-type" "application/json"} headers))
      (is (str/includes? body "No request-id specified"))))

  (testing "missing-service-id"
    (let [src-router-id "src-router-id"
          request-id "test-req-123456"
          request {:basic-authentication {:src-router-id src-router-id}
                   :headers {"accept" "application/json"}
                   :route-params {:request-id request-id}
                   :uri (str "/waiter-async/complete/" request-id "/")}
          async-request-terminate-fn (fn [_] (throw (Exception. "unexpected call!")))
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn request)]
      (is (= 400 status))
      (is (= {"content-type" "application/json"} headers))
      (is (str/includes? body "No service-id specified"))))

  (testing "valid-request-id"
    (let [src-router-id "src-router-id"
          service-id "test-service-id"
          request-id "test-req-123456"
          request {:basic-authentication {:src-router-id src-router-id}
                   :route-params {:request-id request-id, :service-id service-id}
                   :uri (str "/waiter-async/complete/" request-id "/" service-id)}
          async-request-terminate-fn (fn [in-request-id] (= request-id in-request-id))
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn request)]
      (is (= 200 status))
      (is (= {"content-type" "application/json"} headers))
      (is (= {:request-id request-id, :success true} (pc/keywordize-map (json/read-str body))))))

  (testing "unable-to-terminate-request"
    (let [src-router-id "src-router-id"
          service-id "test-service-id"
          request-id "test-req-123456"
          request {:basic-authentication {:src-router-id src-router-id}
                   :route-params {:request-id request-id, :service-id service-id}
                   :uri (str "/waiter-async/complete/" request-id "/" service-id)}
          async-request-terminate-fn (fn [_] false)
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn request)]
      (is (= 200 status))
      (is (= {"content-type" "application/json"} headers))
      (is (= {:request-id request-id, :success false} (pc/keywordize-map (json/read-str body)))))))

(deftest test-async-result-handler-errors
  (let [my-router-id "my-router-id"
        service-id "test-service-id"
        make-route-params (fn [code]
                            {:host "host"
                             :location (when (not= code "missing-location") "location/1234")
                             :port "port"
                             :request-id (when (not= code "missing-request-id") "req-1234")
                             :router-id (when (not= code "missing-router-id") my-router-id)
                             :service-id service-id})
        service-id->service-description-fn (fn [in-service-id]
                                             (is (= service-id in-service-id))
                                             {"backend-proto" "http", "metric-group" "test-metric-group"})]
    (testing "missing-location"
      (let [request {:headers {"accept" "application/json"}
                     :route-params (make-route-params "missing-location")}
            {:keys [body headers status]}
            (async/<!!
              (async-result-handler nil nil service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "Missing host, location, port, request-id, router-id or service-id in uri"))))

    (testing "missing-request-id"
      (let [request {:headers {"accept" "application/json"}
                     :route-params (make-route-params "missing-request-id")}
            {:keys [body headers status]}
            (async/<!!
              (async-result-handler nil nil service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "Missing host, location, port, request-id, router-id or service-id in uri"))))

    (testing "missing-router-id"
      (let [request {:headers {"accept" "application/json"}
                     :route-params (make-route-params "missing-router-id")}
            {:keys [body headers status]}
            (async/<!!
              (async-result-handler nil nil service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "Missing host, location, port, request-id, router-id or service-id in uri"))))

    (testing "error-in-checking-backend-status"
      (let [request {:authorization/pricipal "test-user@DOMAIN"
                     :authorization/user "test-user"
                     :headers {"accept" "application/json"}
                     :request-method :http-method
                     :route-params (make-route-params "local")}
            make-http-request-fn (fn [instance in-request end-route metric-group]
                                   (is (= {:host "host" :port "port" :protocol "http" :service-id service-id}
                                          (select-keys instance [:host :port :protocol :service-id])))
                                   (is (= request in-request))
                                   (is (= (-> request :route-params :location) end-route))
                                   (is (= "test-metric-group" metric-group))
                                   (async/go {:error (ex-info "backend-status-error" {:status 502})}))
            async-trigger-terminate-fn (fn [in-router-id in-service-id in-request-id]
                                         (is (= my-router-id in-router-id))
                                         (is (= service-id in-service-id))
                                         (is (= "req-1234" in-request-id)))
            {:keys [body headers status]}
            (async/<!!
              (async-result-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))]
        (is (= 502 status))
        (is (= {"content-type" "application/json"} headers))
        (is (every? #(str/includes? body %) ["backend-status-error"]))))))

(deftest test-async-result-handler-with-return-codes
  (let [my-router-id "my-router-id"
        service-id "test-service-id"
        remote-router-id "remote-router-id"
        make-route-params (fn [code]
                            {:host "host"
                             :location (if (= code "local") "location/1234" "location/6789")
                             :port "port"
                             :request-id (if (= code "local") "req-1234" "req-6789")
                             :router-id (if (= code "local") my-router-id remote-router-id)
                             :service-id service-id})
        service-id->service-description-fn (fn [in-service-id]
                                             (is (= service-id in-service-id))
                                             {"backend-proto" "http", "metric-group" "test-metric-group"})
        request-id-fn (fn [router-type] (if (= router-type "local") "req-1234" "req-6789"))]
    (letfn [(execute-async-result-check
              [{:keys [request-method return-status router-type]}]
              (let [terminate-call-atom (atom false)
                    async-trigger-terminate-fn (fn [target-router-id in-service-id request-id]
                                                 (reset! terminate-call-atom true)
                                                 (is (= (if (= router-type "local") my-router-id remote-router-id) target-router-id))
                                                 (is (= service-id in-service-id))
                                                 (is (= (request-id-fn router-type) request-id)))
                    request {:authorization/principal "test-user@DOMAIN"
                             :authorization/user "test-user"
                             :headers {"accept" "application/json"}
                             :request-method request-method,
                             :route-params (make-route-params router-type)}
                    make-http-request-fn (fn [instance in-request end-route metric-group]
                                           (is (= {:host "host" :port "port" :protocol "http" :service-id service-id}
                                                  (select-keys instance [:host :port :protocol :service-id])))
                                           (is (= request in-request))
                                           (is (= (-> request :route-params :location) end-route))
                                           (is (= "test-metric-group" metric-group))
                                           (async/go {:body "async-result-response", :headers {}, :status return-status}))
                    {:keys [status headers]}
                    (async/<!!
                      (async-result-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))]
                {:terminated @terminate-call-atom, :return-status status, :return-headers headers}))]
      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-result-check {:request-method :get, :return-status 200, :router-type "local"})))
      (is (= {:terminated true, :return-status 303, :return-headers {}}
             (execute-async-result-check {:request-method :get, :return-status 303, :router-type "local"})))
      (is (= {:terminated true, :return-status 410, :return-headers {}}
             (execute-async-result-check {:request-method :get, :return-status 410, :router-type "local"})))
      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-result-check {:request-method :get, :return-status 200, :router-type "remote"})))
      (is (= {:terminated true, :return-status 303, :return-headers {}}
             (execute-async-result-check {:request-method :get, :return-status 303, :router-type "remote"})))
      (is (= {:terminated true, :return-status 410, :return-headers {}}
             (execute-async-result-check {:request-method :get, :return-status 410, :router-type "remote"})))
      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 200, :router-type "local"})))
      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-result-check {:request-method :post, :return-status 200, :router-type "local"})))
      (is (= {:terminated true, :return-status 303, :return-headers {}}
             (execute-async-result-check {:request-method :post, :return-status 303, :router-type "local"})))
      (is (= {:terminated true, :return-status 410, :return-headers {}}
             (execute-async-result-check {:request-method :post, :return-status 410, :router-type "local"})))
      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-result-check {:request-method :post, :return-status 200, :router-type "remote"})))
      (is (= {:terminated true, :return-status 303, :return-headers {}}
             (execute-async-result-check {:request-method :post, :return-status 303, :router-type "remote"})))
      (is (= {:terminated true, :return-status 410, :return-headers {}}
             (execute-async-result-check {:request-method :post, :return-status 410, :router-type "remote"})))
      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 200, :router-type "local"})))
      (is (= {:terminated true, :return-status 204, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 204, :router-type "local"})))
      (is (= {:terminated true, :return-status 404, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 404, :router-type "local"})))
      (is (= {:terminated true, :return-status 405, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 405, :router-type "local"})))
      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 200, :router-type "remote"})))
      (is (= {:terminated true, :return-status 204, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 204, :router-type "remote"})))
      (is (= {:terminated true, :return-status 404, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 404, :router-type "remote"})))
      (is (= {:terminated true, :return-status 405, :return-headers {}}
             (execute-async-result-check {:request-method :delete, :return-status 405, :router-type "remote"}))))))

(deftest test-async-status-handler-errors
  (let [my-router-id "my-router-id"
        service-id "test-service-id"
        make-route-params (fn [code]
                            {:host "host"
                             :location (when (not= code "missing-location") "location/1234")
                             :port "port"
                             :request-id (when (not= code "missing-request-id") "req-1234")
                             :router-id (when (not= code "missing-router-id") my-router-id)
                             :service-id service-id})
        service-id->service-description-fn (fn [in-service-id]
                                             (is (= service-id in-service-id))
                                             {"backend-proto" "http", "metric-group" "test-metric-group"})]
    (testing "missing-code"
      (let [request {:headers {"accept" "application/json"}
                     :query-string ""}
            {:keys [body headers status]} (async/<!! (async-status-handler nil nil service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "Missing host, location, port, request-id, router-id or service-id in uri"))))

    (testing "missing-location"
      (let [request {:headers {"accept" "application/json"}
                     :route-params (make-route-params "missing-location")}
            {:keys [body headers status]} (async/<!! (async-status-handler nil nil service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "Missing host, location, port, request-id, router-id or service-id in uri"))))

    (testing "missing-request-id"
      (let [request {:headers {"accept" "application/json"}
                     :route-params (make-route-params "missing-request-id")}
            {:keys [body headers status]} (async/<!! (async-status-handler nil nil service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "Missing host, location, port, request-id, router-id or service-id in uri"))))

    (testing "missing-router-id"
      (let [request {:headers {"accept" "application/json"}
                     :route-params (make-route-params "missing-router-id")}
            {:keys [body headers status]} (async/<!! (async-status-handler nil nil service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (str/includes? body "Missing host, location, port, request-id, router-id or service-id in uri"))))

    (testing "error-in-checking-backend-status"
      (let [request {:authorization/principal "test-user@DOMAIN"
                     :authorization/user "test-user"
                     :headers {"accept" "application/json"}
                     :route-params (make-route-params "local")
                     :request-method :http-method}
            make-http-request-fn (fn [instance in-request end-route metric-group]
                                   (is (= {:host "host" :port "port" :protocol "http" :service-id service-id}
                                          (select-keys instance [:host :port :protocol :service-id])))
                                   (is (= request in-request))
                                   (is (= (-> request :route-params :location) end-route))
                                   (is (= "test-metric-group" metric-group))
                                   (async/go {:error (ex-info "backend-status-error" {:status 400})}))
            async-trigger-terminate-fn nil
            {:keys [body headers status]} (async/<!! (async-status-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))]
        (is (= 400 status))
        (is (= {"content-type" "application/json"} headers))
        (is (every? #(str/includes? body %) ["backend-status-error"]))))))

(deftest test-async-status-handler-with-return-codes
  (let [my-router-id "my-router-id"
        service-id "test-service-id"
        remote-router-id "remote-router-id"
        make-route-params (fn [code]
                            {:host "host"
                             :location (if (= code "local") "query/location/1234" "query/location/6789")
                             :port "port"
                             :request-id (if (= code "local") "req-1234" "req-6789")
                             :router-id (if (= code "local") my-router-id remote-router-id)
                             :service-id service-id})
        service-id->service-description-fn (fn [in-service-id]
                                             (is (= service-id in-service-id))
                                             {"backend-proto" "http", "metric-group" "test-metric-group"})
        request-id-fn (fn [router-type] (if (= router-type "local") "req-1234" "req-6789"))
        result-location-fn (fn [router-type & {:keys [include-host-port] :or {include-host-port false}}]
                             (str (when include-host-port "http://www.example.com:8521")
                                  "/path/to/result-" (if (= router-type "local") "1234" "6789")))
        async-result-location (fn [router-type & {:keys [include-host-port location] :or {include-host-port false}}]
                                (if include-host-port
                                  (result-location-fn router-type :include-host-port include-host-port)
                                  (str "/waiter-async/result/" (request-id-fn router-type) "/"
                                       (if (= router-type "local") my-router-id remote-router-id) "/"
                                       service-id "/host/port"
                                       (or location (result-location-fn router-type :include-host-port false)))))]
    (letfn [(execute-async-status-check
              [{:keys [request-method result-location return-status router-type]}]
              (let [terminate-call-atom (atom false)
                    async-trigger-terminate-fn (fn [target-router-id in-service-id request-id]
                                                 (reset! terminate-call-atom true)
                                                 (is (= (if (= router-type "local") my-router-id remote-router-id) target-router-id))
                                                 (is (= service-id in-service-id))
                                                 (is (= (request-id-fn router-type) request-id)))
                    request {:authorization/principal "test-user@DOMAIN"
                             :authorization/user "test-user"
                             :request-method request-method
                             :route-params (make-route-params router-type)}
                    make-http-request-fn (fn [instance in-request end-route metric-group]
                                           (is (= {:host "host" :port "port" :protocol "http" :service-id service-id}
                                                  (select-keys instance [:host :port :protocol :service-id])))
                                           (is (= request in-request))
                                           (is (= (-> request :route-params :location) end-route))
                                           (is (= "test-metric-group" metric-group))
                                           (async/go {:body "status-check-response"
                                                      :headers (if (= return-status 303) {"location" (or result-location (result-location-fn router-type))} {})
                                                      :status return-status}))
                    {:keys [status headers]}
                    (async/<!!
                      (async-status-handler async-trigger-terminate-fn make-http-request-fn service-id->service-description-fn request))]
                {:terminated @terminate-call-atom, :return-status status, :return-headers headers}))]
      (is (= {:terminated false, :return-status 200, :return-headers {}}
             (execute-async-status-check {:request-method :get, :return-status 200, :router-type "local"})))
      (let [result-location (async-result-location "local" :include-host-port false)]
        (is (= {:terminated false, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :return-status 303, :router-type "local"}))))
      (let [result-location (async-result-location "local" :include-host-port false :location "/query/location/another/path/to/result")]
        (is (= {:terminated false, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :result-location "another/path/to/result", :return-status 303, :router-type "local"}))))
      (let [result-location (async-result-location "local" :include-host-port false :location "/query/location/another/path/to/result")]
        (is (= {:terminated false, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :result-location "./another/path/to/result", :return-status 303, :router-type "local"}))))
      (let [result-location (async-result-location "local" :include-host-port false :location "/query/another/path/to/result")]
        (is (= {:terminated false, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :result-location "../another/path/to/result", :return-status 303, :router-type "local"}))))
      (let [result-location (async-result-location "local" :include-host-port false :location "/another/path/to/result")]
        (is (= {:terminated false, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :result-location "../../another/path/to/result", :return-status 303, :router-type "local"}))))
      (let [result-location (str "http://www.example.com:1234" (async-result-location "local" :include-host-port true))]
        (is (= {:terminated true, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :result-location result-location, :return-status 303, :router-type "local"}))))
      (is (= {:terminated true, :return-status 410, :return-headers {}}
             (execute-async-status-check {:request-method :get, :return-status 410, :router-type "local"})))

      (is (= {:terminated false, :return-status 200, :return-headers {}}
             (execute-async-status-check {:request-method :get, :return-status 200, :router-type "remote"})))
      (let [result-location (async-result-location "remote" :include-host-port false)]
        (is (= {:terminated false, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :return-status 303, :router-type "remote"}))))
      (let [result-location (str "http://www.example.com:1234" (async-result-location "remote" :include-host-port true))]
        (is (= {:terminated true, :return-status 303, :return-headers {"location" result-location}}
               (execute-async-status-check {:request-method :get, :result-location result-location, :return-status 303, :router-type "remote"}))))
      (is (= {:terminated true, :return-status 410, :return-headers {}}
             (execute-async-status-check {:request-method :get, :return-status 410, :router-type "remote"})))

      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 200, :router-type "local"})))
      (is (= {:terminated true, :return-status 204, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 204, :router-type "local"})))
      (is (= {:terminated false, :return-status 404, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 404, :router-type "local"})))
      (is (= {:terminated false, :return-status 405, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 405, :router-type "local"})))

      (is (= {:terminated true, :return-status 200, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 200, :router-type "remote"})))
      (is (= {:terminated true, :return-status 204, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 204, :router-type "remote"})))
      (is (= {:terminated false, :return-status 404, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 404, :router-type "remote"})))
      (is (= {:terminated false, :return-status 405, :return-headers {}}
             (execute-async-status-check {:request-method :delete, :return-status 405, :router-type "remote"}))))))

(deftest test-list-services-handler
  (let [test-user "test-user"
        test-user-services #{"service1" "service2" "service3" "service7" "service8" "service9"}
        other-user-services #{"service4" "service5" "service6"}
        healthy-services #{"service1" "service2" "service4" "service6" "service7" "service8" "service9"}
        unhealthy-services #{"service2" "service3" "service5"}
        service-id->source-tokens {"service1" [{:token "t1.org" :version "v1"} {:token "t2.com" :version "v2"}]
                                   "service3" [{:token "t2.com" :version "v2"} {:token "t3.edu" :version "v3"}]
                                   "service4" [{:token "t1.org" :version "v1"} {:token "t2.com" :version "v2"}]
                                   "service5" [{:token "t1.org" :version "v1"} {:token "t3.edu" :version "v3"}]
                                   "service7" [{:token "t1.org" :version "v2"} {:token "t2.com" :version "v1"}]
                                   "service9" [{:token "t2.com" :version "v3"}]}
        all-services (set/union other-user-services test-user-services)
        query-state-fn (constantly {:all-available-service-ids all-services
                                    :service-id->healthy-instances (pc/map-from-keys (constantly []) healthy-services)
                                    :service-id->unhealthy-instances (pc/map-from-keys (constantly []) unhealthy-services)})
        request {:authorization/user test-user}
        instance-counts-present (fn [body]
                                  (let [parsed-body (-> body (str) (json/read-str) (walk/keywordize-keys))]
                                    (every? (fn [service-entry]
                                              (is (contains? service-entry :instance-counts))
                                              (is (every? #(contains? (get service-entry :instance-counts) %)
                                                          [:healthy-instances, :unhealthy-instances])))
                                            parsed-body)))
        prepend-waiter-url identity
        entitlement-manager (reify authz/EntitlementManager
                              (authorized? [_ user action {:keys [service-id]}]
                                (and (= user test-user)
                                     (= action :manage)
                                     (some #(= % service-id) test-user-services))))
        list-services-handler (wrap-handler-json-response list-services-handler)
        assert-successful-json-response (fn [{:keys [body headers status]}]
                                          (is (= 200 status))
                                          (is (= "application/json" (get headers "content-type")))
                                          (is (instance-counts-present body)))]
    (letfn [(service-id->service-description-fn [service-id & _]
              (cond-> {"run-as-user" (if (contains? test-user-services service-id) test-user "another-user")}
                (contains? service-id->source-tokens service-id)
                (assoc "source-tokens" (-> service-id service-id->source-tokens walk/stringify-keys))))
            (service-id->metrics-fn []
              {})]

      (testing "list-services-handler:success-regular-user"
        (let [{:keys [body] :as response}
              (list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                     service-id->service-description-fn service-id->metrics-fn request)]
          (assert-successful-json-response response)
          (is (= test-user-services (->> body json/read-str walk/keywordize-keys (map :service-id) set)))))

      (testing "list-services-handler:success-regular-user-with-filter-for-another-user"
        (let [request (assoc request :query-string "run-as-user=another-user")]
          (let [{:keys [body] :as response}
                (list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                       service-id->service-description-fn service-id->metrics-fn request)]
            (assert-successful-json-response response)
            (is (= other-user-services (->> body json/read-str walk/keywordize-keys (map :service-id) set))))))

      (testing "list-services-handler:success-regular-user-with-filter-for-same-user"
        (let [entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ _ _ _]
                                      ; use (constantly true) for authorized? to verify that filter still applies
                                      true))
              request (assoc request :authorization/user "another-user" :query-string "run-as-user=another-user")]
          (let [{:keys [body] :as response}
                (list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                       service-id->service-description-fn service-id->metrics-fn request)]
            (assert-successful-json-response response)
            (is (= other-user-services (->> body json/read-str walk/keywordize-keys (map :service-id) set))))))

      (testing "list-services-handler:failure"
        (let [query-state-fn (constantly {:all-available-service-ids #{"service1"}
                                          :service-id->healthy-instances {"service1" []}})
              request {:authorization/user test-user}
              exception-message "Custom message from test case"
              prepend-waiter-url (fn [_] (throw (ex-info exception-message {:status 400})))
              list-services-handler (core/wrap-error-handling
                                      #(list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                                              service-id->service-description-fn service-id->metrics-fn %))
              {:keys [body headers status]} (list-services-handler request)]
          (is (= 400 status))
          (is (= "text/plain" (get headers "content-type")))
          (is (str/includes? (str body) exception-message))))

      (testing "list-services-handler:success-super-user-sees-all-apps"
        (let [entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ user action {:keys [service-id]}]
                                      (and (= user test-user)
                                           (= :manage action)
                                           (contains? all-services service-id))))
              {:keys [body] :as response}
              ; without a run-as-user, should return all apps
              (list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                     service-id->service-description-fn service-id->metrics-fn request)]
          (assert-successful-json-response response)
          (is (= all-services (->> body json/read-str walk/keywordize-keys (map :service-id) set)))))

      (testing "list-services-handler:success-filter-tokens"
        (doseq [[query-param filter-fn]
                {"t1.com" #(= % "t1.com")
                 "t2.org" #(= % "t2.org")
                 "tn.none" #(= % "tn.none")
                 "*o*" #(str/includes? % "o")
                 "*t*" #(str/includes? % "t")
                 "t*" #(str/starts-with? % "t")
                 "*com" #(str/ends-with? % "com")
                 "*org" #(str/ends-with? % "org")}]
          (let [request (assoc request :query-string (str "token=" query-param))
                {:keys [body] :as response}
                ; without a run-as-user, should return all apps
                (list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                       service-id->service-description-fn service-id->metrics-fn request)]
            (assert-successful-json-response response)
            (is (= (->> service-id->source-tokens
                        (filter (fn [[_ source-tokens]]
                                  (->> source-tokens (map :token) (some filter-fn))))
                        keys
                        set
                        (set/intersection test-user-services))
                   (->> body json/read-str walk/keywordize-keys (map :service-id) set))))))

      (testing "list-services-handler:success-filter-version"
        (doseq [[query-param filter-fn]
                {"v1" #(= % "v1")
                 "v2" #(= % "v2")
                 "vn" #(= % "vn")
                 "*v*" #(str/includes? % "v")
                 "v*" #(str/starts-with? % "v")
                 "*1" #(str/ends-with? % "1")
                 "*2" #(str/ends-with? % "2")}]
          (let [request (assoc request :query-string (str "token-version=" query-param))
                {:keys [body] :as response}
                ; without a run-as-user, should return all apps
                (list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                       service-id->service-description-fn service-id->metrics-fn request)]
            (assert-successful-json-response response)
            (is (= (->> service-id->source-tokens
                        (filter (fn [[_ source-tokens]]
                                  (->> source-tokens (map :version) (some filter-fn))))
                        keys
                        set
                        (set/intersection test-user-services))
                   (->> body json/read-str walk/keywordize-keys (map :service-id) set))))))

      (testing "list-services-handler:success-filter-token-and-version"
        (let [request (assoc request :query-string "token=t1&token-version=v1")
              {:keys [body] :as response}
              ; without a run-as-user, should return all apps
              (list-services-handler entitlement-manager query-state-fn prepend-waiter-url
                                     service-id->service-description-fn service-id->metrics-fn request)]
          (assert-successful-json-response response)
          (is (= (->> service-id->source-tokens
                      (filter (fn [[_ source-tokens]]
                                (and (->> source-tokens (map :token) (some #(= % "t1")))
                                     (->> source-tokens (map :version) (some #(= % "v1"))))))
                      keys
                      set
                      (set/intersection test-user-services))
                 (->> body json/read-str walk/keywordize-keys (map :service-id) set))))))))

(deftest test-delete-service-handler
  (let [test-user "test-user"
        test-service-id "service-1"
        allowed-to-manage-service?-fn (fn [service-id user] (and (= test-service-id service-id) (= test-user user)))]
    (let [core-service-description {"run-as-user" test-user}]

      (testing "delete-service-handler:success-regular-user"
        (let [scheduler (reify scheduler/ServiceScheduler
                          (delete-service [_ service-id]
                            (is (= test-service-id service-id))
                            {:result :deleted
                             :message "Worked!"}))
              request {:authorization/user test-user}
              {:keys [body headers status]} (delete-service-handler test-service-id core-service-description scheduler allowed-to-manage-service?-fn request)]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (every? #(str/includes? (str body) (str %)) ["Worked!"]))))

      (testing "delete-service-handler:success-regular-user-deleting-for-another-user"
        (let [scheduler (reify scheduler/ServiceScheduler
                          (delete-service [_ service-id]
                            (is (= test-service-id service-id))
                            {:deploymentId "good"}))
              request {:authorization/user "another-user"}]
          (is (thrown-with-msg? ExceptionInfo #"User not allowed to delete service"
                                (delete-service-handler test-service-id core-service-description scheduler allowed-to-manage-service?-fn request))))))))

(deftest test-work-stealing-handler
  (let [test-service-id "test-service-id"
        test-router-id "router-1"
        instance-rpc-chan-factory (fn [response-status]
                                    (let [instance-rpc-chan (async/chan 1)
                                          work-stealing-chan (async/chan 1)]
                                      (async/go
                                        (let [instance-rpc-content (async/<! instance-rpc-chan)
                                              {:keys [method service-id response-chan]} instance-rpc-content]
                                          (is (= :offer method))
                                          (is (= test-service-id service-id))
                                          (async/>! response-chan work-stealing-chan)))
                                      (async/go
                                        (let [offer-params (async/<! work-stealing-chan)]
                                          (is (= test-service-id (:service-id offer-params)))
                                          (async/>!! (:response-chan offer-params) response-status)))
                                      instance-rpc-chan))
        test-cases [{:name "valid-parameters-rejected-response"
                     :request-body {:cid "cid-1"
                                    :instance {:id "instance-1", :service-id test-service-id}
                                    :request-id "request-1"
                                    :router-id test-router-id
                                    :service-id test-service-id}
                     :response-status :promptly-rejected
                     :expected-status 200
                     :expected-body-fragments ["cid" "request-id" "response-status" "promptly-rejected"
                                               test-service-id test-router-id]}
                    {:name "valid-parameters-success-response"
                     :request-body {:cid "cid-1"
                                    :instance {:id "instance-1", :service-id test-service-id}
                                    :request-id "request-1"
                                    :router-id test-router-id
                                    :service-id test-service-id}
                     :response-status :success
                     :expected-status 200
                     :expected-body-fragments ["cid" "request-id" "response-status" "success"
                                               test-service-id test-router-id]}
                    {:name "missing-cid"
                     :request-body {:instance {:id "instance-1", :service-id test-service-id}
                                    :request-id "request-1"
                                    :router-id test-router-id
                                    :service-id test-service-id}
                     :response-status :success
                     :expected-status 400
                     :expected-body-fragments ["Missing one of"]}
                    {:name "missing-instance"
                     :request-body {:cid "cid-1"
                                    :request-id "request-1"
                                    :router-id test-router-id
                                    :service-id test-service-id}
                     :response-status :success
                     :expected-status 400
                     :expected-body-fragments ["Missing one of"]}
                    {:name "missing-request-id"
                     :request-body {:cid "cid-1"
                                    :instance {:id "instance-1", :service-id test-service-id}
                                    :router-id test-router-id
                                    :service-id test-service-id}
                     :response-status :success
                     :expected-status 400
                     :expected-body-fragments ["Missing one of"]}
                    {:name "missing-router-id"
                     :request-body {:cid "cid-1"
                                    :instance {:id "instance-1", :service-id test-service-id}
                                    :request-id "request-1"
                                    :service-id test-service-id}
                     :response-status :success
                     :expected-status 400
                     :expected-body-fragments ["Missing one of"]}
                    {:name "missing-service-id"
                     :request-body {:cid "cid-1"
                                    :instance {:id "instance-1", :service-id test-service-id}
                                    :request-id "request-1"
                                    :router-id test-router-id}
                     :response-status :success
                     :expected-status 400
                     :expected-body-fragments ["Missing one of"]}]]
    (doseq [{:keys [name request-body response-status expected-status expected-body-fragments]} test-cases]
      (testing name
        (let [instance-rpc-chan (instance-rpc-chan-factory response-status)
              request {:uri (str "/work-stealing")
                       :request-method :post
                       :body (StringBufferInputStream. (utils/clj->json (walk/stringify-keys request-body)))}
              {:keys [status body]} (fa/<?? (work-stealing-handler instance-rpc-chan request))]
          (is (= expected-status status))
          (is (every? #(str/includes? (str body) %) expected-body-fragments)))))))

(deftest test-work-stealing-handler-cannot-find-channel
  (let [instance-rpc-chan (async/chan)
        test-service-id "test-service-id"
        request {:body (StringBufferInputStream.
                         (utils/clj->json
                           {"cid" "test-cid"
                            "instance" {"id" "test-instance-id", "service-id" test-service-id}
                            "request-id" "test-request-id"
                            "router-id" "test-router-id"
                            "service-id" test-service-id}))}
        response-chan (work-stealing-handler instance-rpc-chan request)]
    (async/thread
      (let [{:keys [cid method response-chan service-id]} (async/<!! instance-rpc-chan)]
        (is (= :offer method))
        (is (= test-service-id service-id))
        (is cid)
        (is (instance? ManyToManyChannel response-chan))
        (async/close! response-chan)))
    (let [{:keys [status]} (async/<!! response-chan)]
      (is (= 500 status)))))

(deftest test-get-router-state
  (let [state-atom (atom nil)
        query-state-fn (fn [] @state-atom)
        test-fn (fn [router-id query-state-fn request]
                  (let [handler (wrap-handler-json-response get-router-state)]
                    (handler router-id query-state-fn request)))
        router-id "test-router-id"]

    (reset! state-atom {:state-data {}})

    (testing "Getting router state"
      (testing "should handle exceptions gracefully"
        (let [bad-request {:scheme 1} ;; integer scheme will throw error
              {:keys [status body]} (test-fn router-id query-state-fn bad-request)]
          (is (str/includes? (str body) "Internal error"))
          (is (= 500 status))))

      (testing "display router state"
        (let [{:keys [status body]} (test-fn router-id query-state-fn {})]
          (is (every? #(str/includes? (str body) %1)
                      ["fallback" "interstitial" "kv-store" "leader" "local-usage" "maintainer" "router-metrics"
                       "scheduler" "statsd"])
              (str "Body did not include necessary JSON keys:\n" body))
          (is (= 200 status)))))))

(deftest test-get-kv-store-state
  (let [router-id "test-router-id"
        test-fn (wrap-handler-json-response get-kv-store-state)]
    (testing "successful response"
      (let [kv-store (kv/new-local-kv-store {})
            state (-> (kv/state kv-store) walk/stringify-keys)
            {:keys [body status]} (test-fn router-id kv-store {})]
        (is (= 200 status))
        (is (= (-> body json/read-str) {"router-id" router-id, "state" state}))))

    (testing "exception response"
      (let [kv-store (Object.)
            {:keys [body status]} (test-fn router-id kv-store {})]
        (is (= 500 status))
        (is (str/includes? body "Waiter Error 500"))))))

(deftest test-get-local-usage-state
  (let [router-id "test-router-id"
        test-fn (wrap-handler-json-response get-local-usage-state)]
    (testing "successful response"
      (let [last-request-time-state {"foo" 1234, "bar" 7890}
            last-request-time-agent (agent last-request-time-state)
            {:keys [body status]} (test-fn router-id last-request-time-agent {})]
        (is (= 200 status))
        (is (= (-> body json/read-str) {"router-id" router-id, "state" last-request-time-state}))))

    (testing "exception response"
      (let [handler (core/wrap-error-handling #(test-fn router-id nil %))
            {:keys [body status]} (handler {})]
        (is (= 500 status))
        (is (str/includes? body "Waiter Error 500"))))))

(deftest test-get-leader-state
  (let [router-id "test-router-id"
        leader-id-fn (constantly router-id)
        test-fn (wrap-handler-json-response get-leader-state)]
    (testing "successful response"
      (let [leader?-fn (constantly true)
            state {"leader?" (leader?-fn), "leader-id" (leader-id-fn)}
            {:keys [body status]} (test-fn router-id leader?-fn leader-id-fn {})]
        (is (= 200 status))
        (is (= (-> body json/read-str) {"router-id" router-id, "state" state}))))

    (testing "exception response"
      (let [leader?-fn (fn [] (throw (Exception. "Test Exception")))
            {:keys [body status]} (test-fn router-id leader?-fn leader-id-fn {})]
        (is (= 500 status))
        (is (str/includes? body "Waiter Error 500"))))))

(deftest test-get-chan-latest-state-handler
  (let [router-id "test-router-id"
        test-fn (wrap-handler-json-response get-chan-latest-state-handler)]
    (testing "successful response"
      (let [state-atom (atom nil)
            query-state-fn (fn [] @state-atom)
            state {"foo" "bar"}
            _ (reset! state-atom state)
            {:keys [body status]} (test-fn router-id query-state-fn {})]
        (is (= 200 status))
        (is (= (-> body json/read-str) {"router-id" router-id, "state" state}))))))

(deftest test-get-router-metrics-state
  (let [router-id "test-router-id"
        test-fn (wrap-handler-json-response get-router-metrics-state)]
    (testing "successful response"
      (let [state {"router-metrics" "foo"}
            router-metrics-state-fn (constantly state)
            {:keys [body status]} (test-fn router-id router-metrics-state-fn {})]
        (is (= 200 status))
        (is (= {"router-id" router-id "state" state}
               (-> body json/read-str)))))

    (testing "exception response"
      (let [router-metrics-state-fn (fn [] (throw (Exception. "Test Exception")))
            {:keys [body status]} (test-fn router-id router-metrics-state-fn {})]
        (is (= 500 status))
        (is (str/includes? body "Waiter Error 500"))))))

(deftest test-get-query-chan-state-handler
  (let [router-id "test-router-id"
        test-fn (wrap-async-handler-json-response get-query-chan-state-handler)]
    (testing "successful response"
      (let [scheduler-chan (async/promise-chan)
            state {"foo" "bar"}
            _ (async/go
                (let [{:keys [response-chan]} (async/<! scheduler-chan)]
                  (async/>! response-chan state)))
            {:keys [body status]} (async/<!! (test-fn router-id scheduler-chan {}))]
        (is (= 200 status))
        (is (= (-> body json/read-str) {"router-id" router-id, "state" state}))))))

(deftest test-get-statsd-state
  (let [router-id "test-router-id"
        test-fn (wrap-handler-json-response get-statsd-state)]
    (testing "successful response"
      (let [state (-> (statsd/state) walk/stringify-keys)
            {:keys [body status]} (test-fn router-id {})]
        (is (= 200 status))
        (is (= (-> body json/read-str) {"router-id" router-id, "state" state}))))))

(deftest test-get-service-state
  (let [router-id "router-id"
        service-id "service-1"
        local-usage-agent (agent {service-id {"last-request-time" "foo"}})]
    (testing "returns 400 for missing service id"
      (is (= 400 (:status (async/<!! (get-service-state router-id nil local-usage-agent "" {} {}))))))
    (let [instance-rpc-chan (async/chan 1)
          query-state-chan (async/chan 1)
          query-work-stealing-chan (async/chan 1)
          maintainer-state-chan (async/chan 1)
          responder-state {:state "responder state"}
          work-stealing-state {:state "work-stealing state"}
          maintainer-state {:state "maintainer state"}
          start-instance-rpc-fn (fn []
                                  (async/go
                                    (dotimes [_ 2]
                                      (let [{:keys [method response-chan]} (async/<! instance-rpc-chan)]
                                        (condp = method
                                          :query-state (async/>! response-chan query-state-chan)
                                          :query-work-stealing (async/>! response-chan query-work-stealing-chan))))))
          start-query-chan-fn (fn []
                                (async/go
                                  (let [{:keys [response-chan]} (async/<! query-state-chan)]
                                    (async/>! response-chan responder-state)))
                                (async/go
                                  (let [{:keys [response-chan]} (async/<! query-work-stealing-chan)]
                                    (async/>! response-chan work-stealing-state))))
          start-maintainer-fn (fn []
                                (async/go (let [{:keys [service-id response-chan]} (async/<! maintainer-state-chan)]
                                            (async/>! response-chan (assoc maintainer-state :service-id service-id)))))
          get-service-state (wrap-async-handler-json-response get-service-state)]
      (start-instance-rpc-fn)
      (start-query-chan-fn)
      (start-maintainer-fn)
      (let [query-sources {:autoscaler-state (fn [{:keys [service-id]}]
                                               {:service-id service-id :source "autoscaler"})
                           :maintainer-state maintainer-state-chan}
            response (->> (get-service-state router-id instance-rpc-chan local-usage-agent service-id query-sources {})
                          (async/<!!))
            service-state (json/read-str (:body response) :key-fn keyword)]
        (is (= router-id (get-in service-state [:router-id])))
        (is (= responder-state (get-in service-state [:state :responder-state])))
        (is (= {:last-request-time "foo"}
               (get-in service-state [:state :local-usage])))
        (is (= work-stealing-state (get-in service-state [:state :work-stealing-state])))
        (is (= (assoc maintainer-state :service-id service-id) (get-in service-state [:state :maintainer-state])))
        (is (= {:service-id service-id :source "autoscaler"} (get-in service-state [:state :autoscaler-state])))))))

(deftest test-acknowledge-consent-handler
  (let [current-time-ms (System/currentTimeMillis)
        clock (constantly current-time-ms)
        test-token "www.example.com"
        test-service-description {"cmd" "some-cmd", "cpus" 1, "mem" 1024}
        token->service-description-template (fn [token]
                                              (when (= token test-token)
                                                (assoc test-service-description
                                                  "source-tokens" [(sd/source-tokens-entry test-token test-service-description)])))
        token->token-metadata (fn [token] (when (= token test-token) {"owner" "user"}))
        service-description->service-id (fn [service-description]
                                          (str "service-" (count service-description) "." (count (str service-description))))
        test-user "test-user"
        test-service-id (-> test-service-description
                            (assoc "permitted-user" test-user
                                   "run-as-user" test-user
                                   "source-tokens" [(sd/source-tokens-entry test-token test-service-description)])
                            service-description->service-id)
        add-encoded-cookie (fn [response cookie-name cookie-value consent-expiry-days]
                             (assoc-in response [:cookie cookie-name] {:value cookie-value, :age consent-expiry-days}))
        consent-expiry-days 1
        consent-cookie-value (fn consent-cookie-value [mode service-id token {:strs [owner]}]
                               (when mode
                                 (-> [mode (clock)]
                                     (concat (case mode
                                               "service" (when service-id [service-id])
                                               "token" (when (and owner token) [token owner])
                                               nil))
                                     vec)))
        acknowledge-consent-handler-fn (fn [request]
                                         (let [request' (-> request
                                                            (update :authorization/user #(or %1 test-user))
                                                            (update :request-method #(or %1 :post))
                                                            (update :scheme #(or %1 :http)))]
                                           (acknowledge-consent-handler
                                             token->service-description-template token->token-metadata
                                             service-description->service-id consent-cookie-value add-encoded-cookie
                                             consent-expiry-days request')))]
    (testing "unsupported request method"
      (let [request {:request-method :get}
            {:keys [body headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 405 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (str/includes? body "Only POST supported"))))

    (testing "host and origin mismatch"
      (let [request {:headers {"host" "www.example2.com"
                               "origin" (str "http://" test-token)}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Origin is not the same as the host"))))

    (testing "referer and origin mismatch"
      (let [request {:headers {"host" test-token
                               "origin" (str "http://" test-token)
                               "referer" "http://www.example2.com/consent"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Referer does not start with origin"))))

    (testing "mismatch in x-requested-with"
      (let [request {:headers {"host" test-token
                               "origin" (str "http://" test-token)
                               "referer" (str "http://" test-token "/consent")
                               "x-requested-with" "AJAX"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Header x-requested-with does not match expected value"))))

    (testing "missing mode param"
      (let [request {:headers {"host" test-token
                               "origin" (str "http://" test-token)
                               "referer" (str "http://" test-token "/consent")
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"service-id" "service-id-1"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Missing or invalid mode"))))

    (testing "invalid mode param"
      (let [request {:authorization/user test-user
                     :headers {"host" test-token
                               "origin" (str "http://" test-token)
                               "referer" (str "http://" test-token "/consent")
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "unsupported", "service-id" "service-id-1"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Missing or invalid mode"))))

    (testing "missing service-id param"
      (let [request {:authorization/user test-user
                     :headers {"host" test-token
                               "origin" (str "http://" test-token)
                               "referer" (str "http://" test-token "/consent")
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "service"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Missing service-id"))))

    (testing "missing service description for token"
      (let [test-host (str test-token ".test2")
            request {:authorization/user test-user
                     :headers {"host" test-host
                               "origin" (str "http://" test-host)
                               "referer" (str "http://" test-host "/consent")
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "service", "service-id" "service-id-1"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Unable to load description for token"))))

    (testing "invalid service-id param"
      (let [request {:authorization/user test-user
                     :headers {"host" (str test-token ":1234")
                               "origin" "http://www.example.com:1234"
                               "referer" "http://www.example.com:1234/consent"
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "service", "service-id" "service-id-1"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 400 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (nil? cookie))
        (is (str/includes? body "Invalid service-id for specified token"))))

    (testing "valid service mode request"
      (let [request {:authorization/user test-user
                     :headers {"host" (str test-token ":1234")
                               "origin" "http://www.example.com:1234"
                               "referer" "http://www.example.com:1234/consent"
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "service", "service-id" test-service-id}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 200 status))
        (is (= {"x-waiter-consent" {:value ["service" current-time-ms test-service-id], :age consent-expiry-days}} cookie))
        (is (= {} headers))
        (is (str/includes? body "Added cookie x-waiter-consent"))))

    (testing "valid service mode request with missing origin"
      (let [request {:authorization/user test-user
                     :headers {"host" (str test-token ":1234")
                               "referer" "http://www.example.com:1234/consent"
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "service", "service-id" test-service-id}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 200 status))
        (is (= {"x-waiter-consent" {:value ["service" current-time-ms test-service-id], :age consent-expiry-days}} cookie))
        (is (= {} headers))
        (is (str/includes? body "Added cookie x-waiter-consent"))))

    (testing "valid token mode request"
      (let [request {:authorization/user test-user
                     :headers {"host" (str test-token ":1234")
                               "origin" "http://www.example.com:1234"
                               "referer" "http://www.example.com:1234/consent"
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "token"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 200 status))
        (is (= {"x-waiter-consent" {:value ["token" current-time-ms test-token "user"], :age consent-expiry-days}} cookie))
        (is (= {} headers))
        (is (str/includes? body "Added cookie x-waiter-consent"))))

    (testing "valid token mode request with missing origin"
      (let [request {:authorization/user test-user
                     :headers {"host" (str test-token ":1234")
                               "referer" "http://www.example.com:1234/consent"
                               "x-requested-with" "XMLHttpRequest"}
                     :params {"mode" "token"}}
            {:keys [body cookie headers status]} (acknowledge-consent-handler-fn request)]
        (is (= 200 status))
        (is (= {"x-waiter-consent" {:value ["token" current-time-ms test-token "user"], :age consent-expiry-days}} cookie))
        (is (= {} headers))
        (is (str/includes? body "Added cookie x-waiter-consent"))))))

(deftest test-render-consent-template
  (let [context {:auth-user "test-user"
                 :consent-expiry-days 1
                 :service-description-template {"cmd" "some-cmd", "cpus" 1, "mem" 1024}
                 :service-id "service-5.97"
                 :target-url "http://www.example.com:6789/some-path"
                 :token "www.example.com"}
        body (render-consent-template context)]
    (is (str/includes? body "Run Web App? - Waiter"))
    (is (str/includes? body "http://www.example.com:6789/some-path"))))

(deftest test-request-consent-handler
  (let [request-time (t/now)
        basic-service-description {"cmd" "some-cmd" "cpus" 1 "mem" 1024}
        token->service-description-template
        (fn [token]
          (let [service-description (condp = token
                                      "www.example.com" basic-service-description
                                      "www.example-i0.com" (assoc basic-service-description
                                                             "interstitial-secs" 0)
                                      "www.example-i10.com" (assoc basic-service-description
                                                              "interstitial-secs" 10)
                                      nil)]
            (cond-> service-description
              (seq service-description)
              (assoc "source-tokens" [(sd/source-tokens-entry token service-description)]))
            service-description))
        service-description->service-id (fn [service-description]
                                          (str "service-" (count service-description) "." (count (str service-description))))
        consent-expiry-days 1
        test-user "test-user"
        request-consent-handler-fn (fn [request]
                                     (let [request' (-> request
                                                        (update :authorization/user #(or %1 test-user))
                                                        (update :request-method #(or %1 :get)))]
                                       (request-consent-handler
                                         token->service-description-template service-description->service-id
                                         consent-expiry-days request')))
        io-resource-fn (fn [file-path]
                         (is (= "web/consent.html" file-path))
                         (StringReader. "some-content"))
        expected-service-id (fn [token]
                              (-> (token->service-description-template token)
                                  (assoc "permitted-user" test-user "run-as-user" test-user)
                                  service-description->service-id))
        template-eval-factory (fn [scheme]
                                (fn [{:keys [token] :as data}]
                                  (let [service-description-template (token->service-description-template token)]
                                    (is (= {:auth-user test-user
                                            :consent-expiry-days 1
                                            :service-description-template service-description-template
                                            :service-id (expected-service-id token)
                                            :target-url (str scheme "://" token ":6789/some-path"
                                                             (when (some-> (get service-description-template "interstitial-secs") pos?)
                                                               (str "?" (interstitial/request-time->interstitial-param-string request-time))))
                                            :token token}
                                           data)))
                                  "template:some-content"))]
    (testing "unsupported request method"
      (let [request {:authorization/user test-user
                     :request-method :post
                     :request-time request-time
                     :scheme :http}
            {:keys [body headers status]} (request-consent-handler-fn request)]
        (is (= 405 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (str/includes? body "Only GET supported"))))

    (testing "token without service description"
      (let [request {:authorization/user test-user
                     :headers {"host" "www.example2.com:6789"}
                     :request-method :get
                     :request-time request-time
                     :route-params {:path "some-path"}
                     :scheme :http}
            {:keys [body headers status]} (request-consent-handler-fn request)]
        (is (= 404 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (str/includes? body "Unable to load description for token"))))

    (with-redefs [io/resource io-resource-fn
                  render-consent-template (template-eval-factory "http")]
      (testing "token without service description - http scheme"
        (let [request {:authorization/user test-user
                       :headers {"host" "www.example.com:6789"}
                       :request-time request-time
                       :route-params {:path "some-path"}
                       :scheme :http}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {"content-type" "text/html"} headers))
          (is (= body "template:some-content")))))

    (with-redefs [io/resource io-resource-fn
                  render-consent-template (template-eval-factory "https")]
      (testing "token without service description - https scheme"
        (let [request {:authorization/user test-user
                       :headers {"host" "www.example.com:6789"}
                       :request-time request-time
                       :route-params {:path "some-path"}
                       :scheme :https}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {"content-type" "text/html"} headers))
          (is (= body "template:some-content")))))

    (with-redefs [io/resource io-resource-fn
                  render-consent-template (template-eval-factory "https")]
      (testing "token without service description - https x-forwarded-proto"
        (let [request {:authorization/user test-user
                       :headers {"host" "www.example.com:6789", "x-forwarded-proto" "https"}
                       :request-time request-time
                       :route-params {:path "some-path"}
                       :scheme :http}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {"content-type" "text/html"} headers))
          (is (= body "template:some-content")))))

    (with-redefs [io/resource io-resource-fn
                  render-consent-template (template-eval-factory "https")]
      (testing "token without service description - https x-forwarded-proto"
        (let [request {:authorization/user test-user
                       :headers {"host" "www.example-i0.com:6789", "x-forwarded-proto" "https"}
                       :request-time request-time
                       :route-params {:path "some-path"}
                       :scheme :http}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {"content-type" "text/html"} headers))
          (is (= body "template:some-content")))))

    (with-redefs [io/resource io-resource-fn
                  render-consent-template (template-eval-factory "https")]
      (testing "token without service description - https x-forwarded-proto"
        (let [request {:authorization/user test-user
                       :headers {"host" "www.example-i10.com:6789", "x-forwarded-proto" "https"}
                       :request-time request-time
                       :route-params {:path "some-path"}
                       :scheme :http}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {"content-type" "text/html"} headers))
          (is (= body "template:some-content")))))

    (with-redefs [io/resource io-resource-fn
                  render-consent-template (template-eval-factory "https")]
      (testing "token without service description - https x-forwarded-proto"
        (let [request {:authorization/user test-user
                       :headers {"host" "www.example.com:6789", "x-forwarded-proto" "https"}
                       :request-time request-time
                       :route-params {:path "some-path"}
                       :scheme :http}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {"content-type" "text/html"} headers))
          (is (= body "template:some-content")))))))

(deftest test-blacklist-instance-cannot-find-channel
  (let [notify-instance-killed-fn (fn [instance] (throw (ex-info "Unexpected call" {:instance instance})))
        instance-rpc-chan (async/chan)
        test-service-id "test-service-id"
        request {:body (StringBufferInputStream.
                         (utils/clj->json
                           {"instance" {"id" "test-instance-id", "service-id" test-service-id}
                            "period-in-ms" 1000
                            "reason" "blacklist"}))}
        response-chan (blacklist-instance notify-instance-killed-fn instance-rpc-chan request)]
    (async/thread
      (let [{:keys [cid method response-chan service-id]} (async/<!! instance-rpc-chan)]
        (is (= :blacklist method))
        (is (= test-service-id service-id))
        (is cid)
        (is (instance? ManyToManyChannel response-chan))
        (async/close! response-chan)))
    (let [{:keys [status]} (async/<!! response-chan)]
      (is (= 400 status)))))

(deftest test-blacklist-killed-instance
  (let [notify-instance-chan (async/promise-chan)
        notify-instance-killed-fn (fn [instance] (async/>!! notify-instance-chan instance))
        instance-rpc-chan (async/chan)
        test-service-id "test-service-id"
        instance {:id "test-instance-id"
                  :service-id test-service-id
                  :started-at nil}
        request {:body (StringBufferInputStream.
                         (utils/clj->json
                           {"instance" instance
                            "period-in-ms" 1000
                            "reason" "killed"}))}]
    (with-redefs []
      (let [response-chan (blacklist-instance notify-instance-killed-fn instance-rpc-chan request)
            blacklist-chan (async/promise-chan)]
        (async/thread
          (let [{:keys [cid method response-chan service-id]} (async/<!! instance-rpc-chan)]
            (is (= :blacklist method))
            (is (= test-service-id service-id))
            (is cid)
            (is (instance? ManyToManyChannel response-chan))
            (async/>!! response-chan blacklist-chan)))
        (async/thread
          (let [[{:keys [blacklist-period-ms instance-id]} repsonse-chan] (async/<!! blacklist-chan)]
            (is (= 1000 blacklist-period-ms))
            (is (= (:id instance) instance-id))
            (async/>!! repsonse-chan :blacklisted)))
        (let [{:keys [status]} (async/<!! response-chan)]
          (is (= 200 status))
          (is (= instance (async/<!! notify-instance-chan))))))))

(deftest test-get-blacklisted-instances-cannot-find-channel
  (let [instance-rpc-chan (async/chan)
        test-service-id "test-service-id"
        response-chan (get-blacklisted-instances instance-rpc-chan test-service-id {})]
    (async/thread
      (let [{:keys [cid method response-chan service-id]} (async/<!! instance-rpc-chan)]
        (is (= :query-state method))
        (is (= test-service-id service-id))
        (is cid)
        (is (instance? ManyToManyChannel response-chan))
        (async/close! response-chan)))
    (let [{:keys [status]} (async/<!! response-chan)]
      (is (= 500 status)))))
