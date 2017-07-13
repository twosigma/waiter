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
(ns token-syncer.waiter-test
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [qbits.jet.client.http :as http]
            [token-syncer.waiter :refer :all]))

(defn- send-response
  [body-data & {:keys [status]}]
  (let [response-chan (async/promise-chan)
        body-chan (async/promise-chan)]
    (async/put! body-chan body-data)
    (async/put! response-chan (cond-> {:body body-chan}
                                      status (assoc :status status)))
    response-chan))

(deftest test-make-http-request
  (deliver use-spnego-promise false)
  (let [http-client (Object.)
        test-endpoint "/foo/bar"
        test-body "test-body-data"
        test-headers {"header" "value", "source" "test"}
        test-query-params {"foo" "bar", "lorem" "ipsum"}]
    (with-redefs [http/get (fn get-wrapper [in-http-client in-endopint-url in-options]
                             (is (= http-client in-http-client))
                             (is (= test-endpoint in-endopint-url))
                             (is (= {:body test-body
                                     :headers test-headers
                                     :fold-chunked-response? true
                                     :follow-redirects? false
                                     :query-string test-query-params}
                                    in-options)))]
      (make-http-request http-client test-endpoint
                         :body test-body
                         :headers test-headers
                         :query-params test-query-params))))

(deftest test-load-token-list
  (deliver use-spnego-promise false)
  (let [http-client (Object.)
        test-router-url "http://www.test.com:1234"]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (empty? in-options))
                                          (let [response-chan (async/promise-chan)]
                                            (async/put! response-chan {:error error})
                                            response-chan))]
          (is (thrown-with-msg? Exception #"exception from test"
                                (load-token-list http-client test-router-url))))))

    (testing "successful response"
      (let [token-response (json/write-str [{:owner "test-1", :token "token-1"}
                                            {:owner "test-2", :token "token-2"}
                                            {:owner "test-3", :token "token-3"}])]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (empty? in-options))
                                          (send-response token-response))]
          (is (= #{"token-1" "token-2" "token-3"}
                 (load-token-list http-client test-router-url))))))))

(deftest test-load-token-on-router
  (let [http-client (Object.)
        test-router-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        expected-options {:headers {"x-waiter-token" test-token}
                          :query-params {"include-deleted" "true"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (let [response-chan (async/promise-chan)]
                                            (async/put! response-chan {:error error})
                                            response-chan))]
          (is (= {:error error}
                 (load-token-on-router http-client test-router-url test-token))))))

    (testing "successful response"
      (let [token-response (json/write-str {:foo :bar
                                            :lorem :ipsum})]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 200))]
          (is (= {:description {"foo" "bar",
                                "lorem" "ipsum"}
                  :status 200}
                 (load-token-on-router http-client test-router-url test-token))))))))

(deftest test-store-token-on-router
  (deliver use-spnego-promise false)
  (let [http-client (Object.)
        test-router-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        test-description {"foo" "bar"
                          "lorem" "ipsum"}
        expected-options {:body (json/write-str (assoc test-description :token test-token))
                          :method http/post
                          :query-params {"update-mode" "admin"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (let [response-chan (async/promise-chan)]
                                            (async/put! response-chan {:error error})
                                            response-chan))]
          (is (thrown-with-msg? Exception #"exception from test"
                                (store-token-on-router http-client test-router-url test-token test-description))))))

    (testing "error in status code"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 300))]
          (is (thrown-with-msg? Exception #"Token store failed"
                                (store-token-on-router http-client test-router-url test-token test-description))))))

    (testing "successful response"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 200))]
          (is (= {:body token-response, :status 200}
                 (store-token-on-router http-client test-router-url test-token test-description))))))))

(deftest test-hard-delete-token-on-router
  (deliver use-spnego-promise false)
  (let [http-client (Object.)
        test-router-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        expected-options {:headers {"x-waiter-token" test-token}
                          :method http/delete
                          :query-params {"hard-delete" "true"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (let [response-chan (async/promise-chan)]
                                            (async/put! response-chan {:error error})
                                            response-chan))]
          (is (thrown-with-msg? Exception #"exception from test"
                                (hard-delete-token-on-router http-client test-router-url test-token))))))

    (testing "error in status code"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 300))]
          (is (thrown-with-msg? Exception #"Token hard-delete failed"
                                (hard-delete-token-on-router http-client test-router-url test-token))))))

    (testing "successful response"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client in-endopint-url & in-options]
                                          (is (= http-client in-http-client))
                                          (is (= (str test-router-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 200))]
          (is (= {:body token-response, :status 200}
                 (hard-delete-token-on-router http-client test-router-url test-token))))))))
