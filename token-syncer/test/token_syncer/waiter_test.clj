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
  (let [body-chan (async/promise-chan)]
    (async/put! body-chan body-data)
    (cond-> {:body body-chan}
            status (assoc :status status))))

(deftest test-make-http-request
  (let [http-client-wrapper (Object.)
        test-endpoint "/foo/bar"
        test-body "test-body-data"
        test-headers {"header" "value", "source" "test"}
        test-query-params {"foo" "bar", "lorem" "ipsum"}]

    (testing "simple request"
      (with-redefs [http/get (fn get-wrapper [in-http-client in-endopint-url in-options]
                               (is (= http-client-wrapper in-http-client))
                               (is (= test-endpoint in-endopint-url))
                               (is (not (contains? in-options :auth)))
                               (is (= {:body test-body
                                       :headers test-headers
                                       :fold-chunked-response? true
                                       :follow-redirects? false
                                       :query-string test-query-params}
                                      in-options))
                               (let [response-chan (async/promise-chan)]
                                 (async/put! response-chan {})
                                 response-chan))]
        (make-http-request http-client-wrapper test-endpoint
                           :body test-body
                           :headers test-headers
                           :query-params test-query-params)))

    (testing "spengo auth"
      (let [call-counter (atom 0)]
        (with-redefs [http/get (fn get-wrapper [in-http-client in-endopint-url in-options]
                                 (swap! call-counter inc)
                                 (is (= http-client-wrapper in-http-client))
                                 (is (= test-endpoint in-endopint-url))
                                 (when (= @call-counter 2)
                                   (is (contains? in-options :auth)))
                                 (is (= {:body test-body
                                         :headers test-headers
                                         :fold-chunked-response? true
                                         :follow-redirects? false
                                         :query-string test-query-params}
                                        (dissoc in-options :auth)))
                                 (let [response-chan (async/promise-chan)
                                       response (cond-> {}
                                                        (not (:auth in-options))
                                                        (assoc :status 401 :headers {"www-authenticate" "Negotiate"}))]
                                   (async/put! response-chan response)
                                   response-chan))]
          (make-http-request http-client-wrapper test-endpoint
                             :body test-body
                             :headers test-headers
                             :query-params test-query-params)
          (is (= 2 @call-counter)))))))

(deftest test-load-token-list
  (let [http-client-wrapper (Object.)
        test-cluster-url "http://www.test.com:1234"]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= [:headers {"accept" "application/json"}] in-options))
                                          {:error error})]
          (is (thrown-with-msg? Exception #"exception from test"
                                (load-token-list http-client-wrapper test-cluster-url))))))

    (testing "successful response"
      (let [token-response (json/write-str [{:owner "test-1", :token "token-1"}
                                            {:owner "test-2", :token "token-2"}
                                            {:owner "test-3", :token "token-3"}])]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= [:headers {"accept" "application/json"}] in-options))
                                          (send-response token-response :status 200))]
          (is (= #{"token-1" "token-2" "token-3"}
                 (load-token-list http-client-wrapper test-cluster-url))))))))

(deftest test-load-token
  (let [http-client-wrapper (Object.)
        test-cluster-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        expected-options {:headers {"accept" "application/json"
                                    "x-waiter-token" test-token}
                          :query-params {"include-deleted" "true"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          {:error error})]
          (is (= {:error error}
                 (load-token http-client-wrapper test-cluster-url test-token))))))

    (testing "successful response"
      (let [token-response (json/write-str {:foo :bar
                                            :lorem :ipsum})]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 200))]
          (is (= {:description {"foo" "bar",
                                "lorem" "ipsum"}
                  :status 200}
                 (load-token http-client-wrapper test-cluster-url test-token))))))))

(deftest test-store-token
  (let [http-client-wrapper (Object.)
        test-cluster-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        test-description {"foo" "bar"
                          "lorem" "ipsum"}
        expected-options {:body (json/write-str (assoc test-description :token test-token))
                          :headers {"accept" "application/json"}
                          :method http/post
                          :query-params {"update-mode" "admin"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          {:error error})]
          (is (thrown-with-msg? Exception #"exception from test"
                                (store-token http-client-wrapper test-cluster-url test-token test-description))))))

    (testing "error in status code"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 300))]
          (is (thrown-with-msg? Exception #"Token store failed"
                                (store-token http-client-wrapper test-cluster-url test-token test-description))))))

    (testing "successful response"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 200))]
          (is (= {:body token-response, :status 200}
                 (store-token http-client-wrapper test-cluster-url test-token test-description))))))))

(deftest test-hard-delete-token
  (let [http-client-wrapper (Object.)
        test-cluster-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        expected-options {:headers {"accept" "application/json"
                                    "x-waiter-token" test-token}
                          :method http/delete
                          :query-params {"hard-delete" "true"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          {:error error})]
          (is (thrown-with-msg? Exception #"exception from test"
                                (hard-delete-token http-client-wrapper test-cluster-url test-token))))))

    (testing "error in status code"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 300))]
          (is (thrown-with-msg? Exception #"Token hard-delete failed"
                                (hard-delete-token http-client-wrapper test-cluster-url test-token))))))

    (testing "successful response"
      (let [token-response "token response"]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-response token-response :status 200))]
          (is (= {:body token-response, :status 200}
                 (hard-delete-token http-client-wrapper test-cluster-url test-token))))))))
