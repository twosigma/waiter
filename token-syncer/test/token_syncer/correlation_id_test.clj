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
(ns token-syncer.correlation-id-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [token-syncer.correlation-id :refer :all]))

(deftest test-correlation-id-middleware
  (testing "request-without-cid:response-without-cid"
    (let [cid-promise (promise)
          request {:headers {}, :uri "/test-correlation-id-middleware"}
          response {:body "test", :headers {"source" "test"}}
          handler (fn [in-request]
                    (let [cid-header (get-in in-request [:headers "x-cid"])]
                      (is (= (assoc-in request [:headers "x-cid"] cid-header) in-request))
                      (deliver cid-promise cid-header))
                    response)
          cid-handler (correlation-id-middleware handler)
          out-response (cid-handler request)]
      (is @cid-promise)
      (is (= (assoc-in response [:headers "x-cid"] @cid-promise) out-response))))

  (testing "request-without-cid:response-chan-without-cid"
    (let [cid-promise (promise)
          request {:headers {}, :uri "/test-correlation-id-middleware"}
          response {:body "test", :headers {"source" "test"}}
          handler (fn [in-request]
                    (let [cid-header (get-in in-request [:headers "x-cid"])]
                      (is (= (assoc-in request [:headers "x-cid"] cid-header) in-request))
                      (deliver cid-promise cid-header))
                    (let [response-chan (async/promise-chan)]
                      (async/>!! response-chan response)
                      response-chan))
          cid-handler (correlation-id-middleware handler)
          out-response (async/<!! (cid-handler request))]
      (is @cid-promise)
      (is (= (assoc-in response [:headers "x-cid"] @cid-promise) out-response))))

  (testing "request-without-cid:response-with-cid"
    (let [request {:headers {}, :uri "/test-correlation-id-middleware"}
          response-cid (str "cid-" (rand-int 100000))
          response {:body "test", :headers {"source" "test", "x-cid" response-cid}}
          handler (fn [in-request]
                    (let [cid-header (get-in in-request [:headers "x-cid"])]
                      (is (= (assoc-in request [:headers "x-cid"] cid-header) in-request)))
                    response)
          cid-handler (correlation-id-middleware handler)
          out-response (cid-handler request)]
      (is (= response out-response))))

  (testing "request-with-cid:response-without-cid"
    (let [request-cid (str "cid-" (rand-int 100000))
          request {:headers {"x-cid" request-cid}, :uri "/test-correlation-id-middleware"}
          response {:body "test", :headers {"source" "test"}}
          handler (fn [in-request]
                    (let [cid-header (get-in in-request [:headers "x-cid"])]
                      (is (= request-cid cid-header))
                      (is (= request in-request)))
                    response)
          cid-handler (correlation-id-middleware handler)
          out-response (cid-handler request)]
      (is (= (assoc-in response [:headers "x-cid"] request-cid) out-response))))

  (testing "request-with-cid:response-with-cid"
    (let [request {:headers {}, :uri "/test-correlation-id-middleware"}
          response-cid (str "cid-" (rand-int 100000))
          response {:body "test", :headers {"source" "test", "x-cid" response-cid}}
          handler (fn [in-request]
                    (let [cid-header (get-in in-request [:headers "x-cid"])]
                      (is (= (assoc-in request [:headers "x-cid"] cid-header) in-request)))
                    response)
          cid-handler (correlation-id-middleware handler)
          out-response (cid-handler request)]
      (is (= response out-response)))))
