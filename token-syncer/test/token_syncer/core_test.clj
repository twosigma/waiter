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
(ns token-syncer.core-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [token-syncer.core :refer :all]
            [token-syncer.syncer :as syncer]
            [token-syncer.utils :as utils])
  (:import (org.apache.commons.codec.binary Base64)))

(deftest test-default-handler
  (testing "routing to default-handler"
    (let [http-handler (http-handler-factory {})]
      (is (= {:body "OK", :status 200}
             (http-handler {:uri "/default"}))))))

(deftest test-settings-handler
  (testing "routing to settings-handler"
    (let [settings {:foo "lorem"
                    :bar "ipsum"}
          http-handler (http-handler-factory settings)]
      (is (= {:body (json/write-str {"bar" "ipsum"
                                     "foo" "lorem"})
              :headers {"Content-Type" "application/json"}
              :status 200}
             (http-handler {:uri "/settings"}))))))

(deftest test-sync-handler
  (testing "routing to sync-handler"
    (testing "successful response - single router-url"
      (let [sync-result {:sync-success true}]
        (with-redefs [syncer/sync-tokens (fn sync-tokens [_ router-urls]
                                           (is (= #{"a"} (set router-urls)))
                                           sync-result)]
          (let [http-handler (http-handler-factory {})]
            (is (= {:body (json/write-str sync-result)
                    :headers {"Content-Type" "application/json"}
                    :status 200}
                   (http-handler {:query-params {"router-url" "a"}
                                  :uri "/sync-tokens"})))))))

    (testing "successful response - multiple router-url"
      (let [sync-result {:sync-success true}]
        (with-redefs [syncer/sync-tokens (fn sync-tokens [_ router-urls]
                                           (is (= #{"a" "b"} (set router-urls)))
                                           sync-result)]
          (let [http-handler (http-handler-factory {})]
            (is (= {:body (json/write-str sync-result)
                    :headers {"Content-Type" "application/json"}
                    :status 200}
                   (http-handler {:query-params {"router-url" ["a" "b"]}
                                  :uri "/sync-tokens"})))))))

    (testing "exception in response -  missing router urls"
      (let [http-handler (http-handler-factory {})
            response (http-handler {:uri "/sync-tokens"})]
        (is (= {:headers {"Content-Type" "application/json"}
                :status 500}
               (dissoc response :body)))))

    (testing "exception in response - from sync-tokens"
      (let [sync-exception (Exception. "from test-sync-handler")]
        (with-redefs [syncer/sync-tokens (fn sync-tokens [_ _] (throw sync-exception))]
          (let [http-handler (http-handler-factory {})]
            (is (= (utils/exception->json-response sync-exception)
                   (http-handler {:query-params {"router-url" "a"}
                                  :uri "/sync-tokens"})))))))))

(deftest test-basic-auth
  (testing "no username and password"
    (let [handler (basic-auth-middleware nil nil (constantly {:status 200}))]
      (is (= {:status 200} (handler {:uri "/handler"})))))

  (let [handler (basic-auth-middleware "waiter" "test" (constantly {:status 200}))
        auth (str "Basic " (String. (Base64/encodeBase64 (.getBytes "waiter:test"))))
        bad-auth-1 (str "Basic " (String. (Base64/encodeBase64 (.getBytes "waiter:badtest"))))
        bad-auth-2 (str "Basic " (String. (Base64/encodeBase64 (.getBytes "badwaiter:test"))))]

    (testing "Do not authenticate /status"
      (is (= {:status 200} (handler {:uri "/status"}))))

    (testing "Return 401 on missing auth"
      (is (= 401 (:status (handler {:uri "/handler", :headers {}})))))

    (testing "Return 401 on bad authentication"
      (is (= 401 (:status (handler {:uri "/handler", :headers {"authorization" bad-auth-1}})))))

    (testing "Return 401 on bad authentication"
      (is (= 401 (:status (handler {:uri "/handler", :headers {"authorization" bad-auth-2}})))))

    (testing "Successful authentication"
      (is (= {:status 200} (handler {:uri "/handler", :headers {"authorization" auth}}))))))