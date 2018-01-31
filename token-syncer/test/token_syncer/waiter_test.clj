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

(defn- send-json-response
  [body-data & {:keys [headers status]}]
  (let [body-chan (async/promise-chan)]
    (->> body-data
         json/write-str
         (async/put! body-chan))
    (cond-> {:body body-chan
             :headers (or headers {})}
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
                                      (update in-options :headers dissoc "x-cid")))
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
                                        (-> in-options
                                            (dissoc :auth)
                                            (update :headers dissoc "x-cid"))))
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
                                          (throw error))]
          (is (thrown-with-msg? Exception #"exception from test"
                                (load-token-list http-client-wrapper test-cluster-url))))))

    (testing "successful response"
      (let [token-response [{"last-update-time" 1000, "owner" "test-1", "token" "token-1"}
                            {"last-update-time" 2000, "owner" "test-2", "token" "token-2"}
                            {"last-update-time" 3000, "owner" "test-3", "token" "token-3"}]]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= [:headers {"accept" "application/json"}] in-options))
                                          (send-json-response token-response :status 200))]
          (is (= token-response (load-token-list http-client-wrapper test-cluster-url))))))))

(deftest test-load-token
  (let [http-client-wrapper (Object.)
        test-cluster-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        expected-options {:headers {"accept" "application/json"
                                    "x-waiter-token" test-token}
                          :query-params {"include" ["deleted" "metadata"]}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (throw error))]
          (is (= {:error error}
                 (load-token http-client-wrapper test-cluster-url test-token))))))

    (testing "successful response"
      (let [token-response {"foo" "bar", "lorem" "ipsum"}
            current-time-ms (-> (System/currentTimeMillis)
                                (mod 1000)
                                (* 1000))
            last-modified-str current-time-ms]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-json-response token-response
                                                              :headers {"etag" last-modified-str}
                                                              :status 200))]
          (is (= {:description {"foo" "bar",
                                "lorem" "ipsum"}
                  :headers {"etag" last-modified-str}
                  :status 200
                  :token-etag current-time-ms}
                 (load-token http-client-wrapper test-cluster-url test-token))))))))

(deftest test-store-token
  (let [http-client-wrapper (Object.)
        test-cluster-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        test-token-etag (System/currentTimeMillis)
        test-description {"foo" "bar"
                          "lorem" "ipsum"}
        expected-options {:body (json/write-str (assoc test-description :token test-token))
                          :headers {"accept" "application/json"
                                    "if-match" test-token-etag}
                          :method :post
                          :query-params {"update-mode" "admin"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (throw error))]
          (is (thrown-with-msg? Exception #"exception from test"
                                (store-token http-client-wrapper test-cluster-url test-token test-token-etag
                                             test-description))))))

    (testing "error in status code"
      (let [token-response {"message" "failed"}]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-json-response token-response :status 300))]
          (is (thrown-with-msg? Exception #"Token store failed"
                                (store-token http-client-wrapper test-cluster-url test-token test-token-etag
                                             test-description))))))

    (testing "successful response"
      (let [token-response {"message" "success"}]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-json-response token-response :status 200))]
          (is (= {:body token-response, :headers {}, :status 200}
                 (store-token http-client-wrapper test-cluster-url test-token test-token-etag
                              test-description))))))))

(deftest test-hard-delete-token
  (let [http-client-wrapper (Object.)
        test-cluster-url "http://www.test.com:1234"
        test-token "lorem-ipsum"
        test-token-etag (System/currentTimeMillis)
        expected-options {:headers {"accept" "application/json"
                                    "if-match" test-token-etag
                                    "x-waiter-token" test-token}
                          :method :delete
                          :query-params {"hard-delete" "true"}}]

    (testing "error in response"
      (let [error (Exception. "exception from test")]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (throw error))]
          (is (thrown-with-msg? Exception #"exception from test"
                                (hard-delete-token http-client-wrapper test-cluster-url test-token
                                                   test-token-etag))))))

    (testing "error in status code"
      (let [token-response {"message" "failed"}]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-json-response token-response :status 300))]
          (is (thrown-with-msg? Exception #"Token hard-delete failed"
                                (hard-delete-token http-client-wrapper test-cluster-url test-token
                                                   test-token-etag))))))

    (testing "successful response"
      (let [token-response {"message" "success"}]
        (with-redefs [make-http-request (fn [in-http-client-wrapper in-endopint-url & in-options]
                                          (is (= http-client-wrapper in-http-client-wrapper))
                                          (is (= (str test-cluster-url "/token")) in-endopint-url)
                                          (is (= expected-options (apply hash-map in-options)))
                                          (send-json-response token-response :status 200))]
          (is (= {:body token-response, :headers {}, :status 200}
                 (hard-delete-token http-client-wrapper test-cluster-url test-token test-token-etag))))))))
