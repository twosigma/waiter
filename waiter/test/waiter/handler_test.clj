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
(ns waiter.handler-test
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [comb.template :as template]
            [full.async :as fa]
            [plumbing.core :as pc]
            [waiter.authorization :as authz]
            [waiter.handler :refer :all]
            [waiter.kv :as kv]
            [waiter.scheduler :as scheduler]
            [waiter.test-helpers :refer :all])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (clojure.lang ExceptionInfo)
           (java.io StringBufferInputStream StringReader)
           (waiter.scheduler ServiceScheduler)))

(deftest test-complete-async-handler
  (testing "missing-request-id"
    (let [src-router-id "src-router-id"
          service-id "test-service-id"
          request {:headers {"accept" "application/json"}
                   :route-params {:service-id service-id}
                   :uri (str "/waiter-async/complete//" service-id)}
          async-request-terminate-fn (fn [_] (throw (Exception. "unexpected call!")))
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn src-router-id request)]
      (is (= 400 status))
      (is (= {"content-type" "application/json"} headers))
      (is (str/includes? body "No request-id specified"))))

  (testing "missing-service-id"
    (let [src-router-id "src-router-id"
          request-id "test-req-123456"
          request {:headers {"accept" "application/json"}
                   :route-params {:request-id request-id}
                   :uri (str "/waiter-async/complete/" request-id "/")}
          async-request-terminate-fn (fn [_] (throw (Exception. "unexpected call!")))
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn src-router-id request)]
      (is (= 400 status))
      (is (= {"content-type" "application/json"} headers))
      (is (str/includes? body "No service-id specified"))))

  (testing "valid-request-id"
    (let [src-router-id "src-router-id"
          service-id "test-service-id"
          request-id "test-req-123456"
          request {:route-params {:request-id request-id, :service-id service-id}
                   :uri (str "/waiter-async/complete/" request-id "/" service-id)}
          async-request-terminate-fn (fn [in-request-id] (= request-id in-request-id))
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn src-router-id request)]
      (is (= 200 status))
      (is (= {"content-type" "application/json"} headers))
      (is (= {:request-id request-id, :success true} (pc/keywordize-map (json/read-str body))))))

  (testing "unable-to-terminate-request"
    (let [src-router-id "src-router-id"
          service-id "test-service-id"
          request-id "test-req-123456"
          request {:route-params {:request-id request-id, :service-id service-id}
                   :uri (str "/waiter-async/complete/" request-id "/" service-id)}
          async-request-terminate-fn (fn [_] false)
          {:keys [body headers status]} (complete-async-handler async-request-terminate-fn src-router-id request)]
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
      (let [request {:authorization/user "test-user"
                     :authenticated-principal "test-user@DOMAIN"
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
                    request {:authorization/user "test-user"
                             :authenticated-principal "test-user@DOMAIN"
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
      (let [request {:authorization/user "test-user"
                     :authenticated-principal "test-user@DOMAIN"
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
                    request {:authorization/user "test-user"
                             :authenticated-principal "test-user@DOMAIN"
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
        test-user-services ["service1" "service2" "service3"]
        state-chan (async/chan 1)
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
        list-services-handler (wrap-handler-json-response list-services-handler)]
    (let [service-id->service-description-fn
          (fn [service-id & _] {"run-as-user" (if (some #(= service-id %) test-user-services) test-user "another-user")})]

      (testing "list-services-handler:success-regular-user"
        (async/>!! state-chan {:service-id->healthy-instances {"service1" []
                                                               "service2" []
                                                               "service3" []
                                                               "service4" []
                                                               "service6" []}
                               :service-id->unhealthy-instances {"service3" []
                                                                 "service5" []}})
        (let [{:keys [body headers status]}
              (list-services-handler entitlement-manager state-chan prepend-waiter-url service-id->service-description-fn request)]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (every? #(str/includes? (str body) (str "service" %)) (range 1 4)))
          (is (not-any? #(str/includes? (str body) (str "service" %)) (range 4 7)))
          (is (instance-counts-present body))))

      (testing "list-services-handler:success-regular-user-with-filter-for-another-user"
        (let [request (assoc request :query-string "run-as-user=another-user")]
          (async/>!! state-chan {:service-id->healthy-instances {"service1" []
                                                                 "service2" []
                                                                 "service3" []
                                                                 "service4" []
                                                                 "service6" []}
                                 :service-id->unhealthy-instances {"service3" []
                                                                   "service5" []}})
          (let [{:keys [body headers status]}
                (list-services-handler entitlement-manager state-chan prepend-waiter-url service-id->service-description-fn request)]
            (is (= 200 status))
            (is (= "application/json" (get headers "content-type")))
            (is (not-any? #(str/includes? (str body) (str "service" %)) (range 1 4)))
            (is (every? #(str/includes? (str body) (str "service" %)) (range 4 7)))
            (is (instance-counts-present body)))))

      (testing "list-services-handler:success-regular-user-with-filter-for-same-user"
        (let [entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ _ _ _]
                                      ; use (constantly true) for authorized? to verify that filter still applies
                                      true))
              request (assoc request :authorization/user "another-user" :query-string "run-as-user=another-user")]
          (async/>!! state-chan {:service-id->healthy-instances {"service1" []
                                                                 "service2" []
                                                                 "service3" []
                                                                 "service4" []
                                                                 "service6" []}
                                 :service-id->unhealthy-instances {"service3" []
                                                                   "service5" []}})
          (let [{:keys [body headers status]}

                (list-services-handler entitlement-manager state-chan prepend-waiter-url service-id->service-description-fn request)]
            (is (= 200 status))
            (is (= "application/json" (get headers "content-type")))
            (is (not-any? #(str/includes? (str body) (str "service" %)) (range 1 4)))
            (is (every? #(str/includes? (str body) (str "service" %)) (range 4 7)))
            (is (instance-counts-present body)))))

      (testing "list-services-handler:failure"
        (async/>!! state-chan {:service-id->healthy-instances {"service1" []}})
        (let [request {:authorization/user test-user}
              exception-message "Custom message from test case"
              prepend-waiter-url (fn [_] (throw (ex-info exception-message {:status 400})))
              {:keys [body headers status]}
              (list-services-handler entitlement-manager state-chan prepend-waiter-url service-id->service-description-fn request)]
          (is (= 400 status))
          (is (= "text/plain" (get headers "content-type")))
          (is (str/includes? (str body) exception-message))))

      (testing "list-services-handler:success-super-user-sees-all-apps"
        (async/>!! state-chan {:service-id->healthy-instances {"service1" []
                                                               "service2" []
                                                               "service3" []
                                                               "service4" []
                                                               "service6" []}
                               :service-id->unhealthy-instances {"service3" []
                                                                 "service5" []}})
        (let [entitlement-manager (reify authz/EntitlementManager
                                    (authorized? [_ user action {:keys [service-id]}]
                                      (and (= user test-user)
                                           (= :manage action)
                                           (some #(= (str "service" %) service-id) (range 1 7)))))
              {:keys [body headers status]}
              ; without a run-as-user, should return all apps
              (list-services-handler entitlement-manager state-chan prepend-waiter-url service-id->service-description-fn request)]
          (is (= 200 status))
          (is (= "application/json" (get headers "content-type")))
          (is (every? #(str/includes? (str body) (str "service" %)) (range 1 7)))
          (is (instance-counts-present body)))))))

(deftest test-delete-service-handler
  (let [test-user "test-user"
        test-service-id "service-1"
        allowed-to-manage-service?-fn (fn [service-id user] (and (= test-service-id service-id) (= test-user user)))]
    (let [core-service-description {"run-as-user" test-user}]

      (testing "delete-service-handler:success-regular-user"
        (let [scheduler (reify scheduler/ServiceScheduler
                          (delete-app [_ service-id]
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
                          (delete-app [_ service-id]
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
                                              [mode service-id _ chan-resp-chan] instance-rpc-content]
                                          (is (= :offer mode))
                                          (is (= test-service-id service-id))
                                          (async/>! chan-resp-chan work-stealing-chan)))
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
                       :body (StringBufferInputStream. (json/write-str (walk/stringify-keys request-body)))}
              {:keys [status body]} (fa/<?? (work-stealing-handler instance-rpc-chan request))]
          (is (= expected-status status))
          (is (every? #(str/includes? (str body) %) expected-body-fragments)))))))

(deftest test-work-stealing-handler-cannot-find-channel
  (let [instance-rpc-chan (async/chan)
        service-id "test-service-id"
        request {:body (StringBufferInputStream.
                         (json/write-str
                           {"cid" "test-cid"
                            "instance" {"id" "test-instance-id", "service-id" service-id}
                            "request-id" "test-request-id"
                            "router-id" "test-router-id"
                            "service-id" service-id}))}
        response-chan (work-stealing-handler instance-rpc-chan request)]
    (async/thread
      (let [[method in-service-id correlation-id result-chan] (async/<!! instance-rpc-chan)]
        (is (= :offer method))
        (is (= service-id in-service-id))
        (is correlation-id)
        (is (instance? ManyToManyChannel result-chan))
        (async/close! result-chan)))
    (let [{:keys [status]} (async/<!! response-chan)]
      (is (= 500 status)))))

(deftest test-get-router-state
  (let [get-router-state (wrap-handler-json-response get-router-state)]
    (testing "Getting router state"
      (testing "should handle exceptions gracefully"
        (let [state-chan (async/chan)
              scheduler-chan (async/chan)
              router-metrics-state-fn (fn [] {})
              kv-store (kv/new-local-kv-store {})
              leader?-fn (constantly true)
              scheduler (reify ServiceScheduler (state [_] nil))]
          (async/put! state-chan []) ; vector instead of a map to trigger an error
          (async/go
            (let [{:keys [response-chan]} (async/<! scheduler-chan)]
              (async/>! response-chan [])))
          (let [{:keys [status body]} (get-router-state state-chan scheduler-chan router-metrics-state-fn kv-store leader?-fn scheduler {})]
            (is (str/includes? (str body) "Internal error"))
            (is (= 500 status)))))

      (testing "display router state"
        (let [state-chan (async/chan)
              scheduler-chan (async/chan)
              router-metrics-state-fn (fn [] {})
              kv-store (kv/new-local-kv-store {})
              leader?-fn (constantly true)
              scheduler (reify ServiceScheduler (state [_] nil))]
          (async/put! state-chan {:state-data []}) ; vector instead of a map to trigger an error
          (async/go
            (let [{:keys [response-chan]} (async/<! scheduler-chan)]
              (async/>! response-chan {:state []})))
          (let [{:keys [status body]} (get-router-state state-chan scheduler-chan router-metrics-state-fn kv-store leader?-fn scheduler {})]
            (is (every? #(str/includes? (str body) %1) ["state-data", "leader", "kv-store", "router-metrics-state", "statsd"])
                (str "Body did not include necessary JSON keys:\n" body))
            (is (= 200 status))))))))

(deftest test-get-service-state
  (let [router-id "router-id"
        service-id "service-1"]
    (testing "returns 400 for missing service id"
      (is (= 400 (:status (async/<!! (get-service-state router-id nil "" {} {}))))))
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
                                      (let [[method _ _ resp-chan] (async/<! instance-rpc-chan)]
                                        (condp = method
                                          :query-state (async/>! resp-chan query-state-chan)
                                          :query-work-stealing (async/>! resp-chan query-work-stealing-chan))))))
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
      (let [response (async/<!! (get-service-state router-id instance-rpc-chan service-id {:maintainer-state maintainer-state-chan} {}))
            service-state (json/read-str (:body response) :key-fn keyword)]
        (is (= router-id (get-in service-state [:router-id])))
        (is (= responder-state (get-in service-state [:state :responder-state])))
        (is (= work-stealing-state (get-in service-state [:state :work-stealing-state])))
        (is (= (assoc maintainer-state :service-id service-id) (get-in service-state [:state :maintainer-state])))))))

(deftest test-acknowledge-consent-handler
  (let [current-time-ms (System/currentTimeMillis)
        clock (constantly current-time-ms)
        test-token "www.example.com"
        test-service-description {"cmd" "some-cmd", "cpus" 1, "mem" 1024, "token" test-token}
        token->token-description (fn [token]
                                   (when (= token test-token)
                                     {:service-description-template test-service-description
                                      :token-metadata {"owner" "user"}}))
        service-description->service-id (fn [service-description]
                                          (str "service-" (count service-description) "." (count (str service-description))))
        test-user "test-user"
        test-service-id (service-description->service-id (assoc test-service-description "permitted-user" test-user "run-as-user" test-user))
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
                                           (acknowledge-consent-handler token->token-description service-description->service-id
                                                                        consent-cookie-value add-encoded-cookie consent-expiry-days request')))]
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

(deftest test-request-consent-handler
  (let [token->service-description-template (fn [token]
                                              (when (= token "www.example.com")
                                                {"cmd" "some-cmd", "cpus" 1, "mem" 1024})) ;; produces service-4.67
        service-description->service-id (fn [service-description]
                                          (str "service-" (count service-description) "." (count (str service-description))))
        consent-expiry-days 1
        request-consent-handler-fn (fn [request]
                                     (let [request' (-> request
                                                        (update :authorization/user #(or %1 "test-user"))
                                                        (update :request-method #(or %1 :get)))]
                                       (request-consent-handler token->service-description-template service-description->service-id
                                                                consent-expiry-days request')))
        io-resource-fn (fn [file-path]
                         (is (= "web/consent.html" file-path))
                         (StringReader. "some-content"))
        template-eval-factory (fn [scheme]
                                (fn [content data]
                                  (is (= {:auth-user "test-user"
                                          :consent-expiry-days 1
                                          :service-description-template {"cmd" "some-cmd", "cpus" 1, "mem" 1024}
                                          :service-id "service-5.97"
                                          :target-url (str scheme "://www.example.com:6789/some-path")
                                          :token "www.example.com"}
                                         data))
                                  (str "template:" content))) ]
    (testing "unsupported request method"
      (let [request {:authorization/user "test-user"
                     :request-method :post
                     :scheme :http}
            {:keys [body headers status]} (request-consent-handler-fn request)]
        (is (= 405 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (str/includes? body "Only GET supported"))))

    (testing "token without service description"
      (let [request {:authorization/user "test-user"
                     :headers {"host" "www.example2.com:6789"}
                     :request-method :get
                     :route-params {:path "some-path"}
                     :scheme :http}
            {:keys [body headers status]} (request-consent-handler-fn request)]
        (is (= 404 status))
        (is (= {"content-type" "text/plain"} headers))
        (is (str/includes? body "Unable to load description for token"))))

    (with-redefs [io/resource io-resource-fn
                  template/eval (template-eval-factory "http")]
      (testing "token without service description - http scheme"
        (let [request {:authorization/user "test-user"
                       :headers {"host" "www.example.com:6789"}
                       :route-params {:path "some-path"}
                       :scheme :http}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {} headers))
          (is (= body "template:some-content")))))

    (with-redefs [io/resource io-resource-fn
                  template/eval (template-eval-factory "https")]
      (testing "token without service description - http scheme"
        (let [request {:authorization/user "test-user"
                       :headers {"host" "www.example.com:6789"}
                       :route-params {:path "some-path"}
                       :scheme :https}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {} headers))
          (is (= body "template:some-content")))))

    (with-redefs [io/resource io-resource-fn
                  template/eval (template-eval-factory "https")]
      (testing "token without service description - https x-forwarded-proto"
        (let [request {:authorization/user "test-user"
                       :headers {"host" "www.example.com:6789", "x-forwarded-proto" "https"}
                       :route-params {:path "some-path"}
                       :scheme :http}
              {:keys [body headers status]} (request-consent-handler-fn request)]
          (is (= 200 status))
          (is (= {} headers))
          (is (= body "template:some-content")))))))

(deftest test-blacklist-instance-cannot-find-channel
  (let [instance-rpc-chan (async/chan)
        service-id "test-service-id"
        request {:body (StringBufferInputStream.
                         (json/write-str
                           {"instance" {"id" "test-instance-id", "service-id" service-id}
                            "period-in-ms" 1000
                            "reason" "blacklist"}))}
        response-chan (blacklist-instance instance-rpc-chan request)]
    (async/thread
      (let [[method in-service-id correlation-id result-chan] (async/<!! instance-rpc-chan)]
        (is (= :blacklist method))
        (is (= service-id in-service-id))
        (is correlation-id)
        (is (instance? ManyToManyChannel result-chan))
        (async/close! result-chan)))
    (let [{:keys [status]} (async/<!! response-chan)]
      (is (= 400 status)))))

(deftest test-get-blacklisted-instances-cannot-find-channel
  (let [instance-rpc-chan (async/chan)
        service-id "test-service-id"
        response-chan (get-blacklisted-instances instance-rpc-chan service-id {})]
    (async/thread
      (let [[method in-service-id correlation-id result-chan] (async/<!! instance-rpc-chan)]
        (is (= :query-state method))
        (is (= service-id in-service-id))
        (is correlation-id)
        (is (instance? ManyToManyChannel result-chan))
        (async/close! result-chan)))
    (let [{:keys [status]} (async/<!! response-chan)]
      (is (= 500 status)))))
