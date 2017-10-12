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
(ns waiter.cookie-support-test
  (:require [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.test :refer :all]
            [taoensso.nippy :as nippy]
            [waiter.cookie-support :refer :all])
  (:import (clojure.lang ExceptionInfo)
           org.eclipse.jetty.util.UrlEncoded))

(deftest test-url-decode
  (is (= "testtest" (url-decode "testtest")))
  (is (= "test test" (url-decode "test%20test")))
  (is (= nil (url-decode nil))))

(deftest test-cookie-value
  (let [cookie-string "user=john; mode=test; product-name=waiter; special=\"quotes\"abound\""]
    (is (nil? (cookie-value cookie-string #"username")))
    (is (= "john" (cookie-value cookie-string #"user")))
    (is (= "test" (cookie-value cookie-string #"mode")))
    (is (= "waiter" (cookie-value cookie-string #"product-name")))
    (is (= "quotes\"abound" (cookie-value cookie-string #"special")))))

(deftest test-remove-cookie
  (is (= "" (remove-cookie "x-waiter-auth=foo" "x-waiter-auth")))
  (is (= "bar=baz" (remove-cookie "x-waiter-auth=foo; bar=baz" "x-waiter-auth")))
  (is (= "bar=baz" (remove-cookie "bar=baz; x-waiter-auth=foo" "x-waiter-auth")))
  (is (= "bar=\"x-waiter-auth=this is a real cookie\"" (remove-cookie "x-waiter-auth=auth-value; bar=\"x-waiter-auth=this is a real cookie\""
                                                                      "x-waiter-auth")))
  (is (= "bar=x-waiter-auth=this is a real cookie" (remove-cookie "x-waiter-auth=auth-value; bar=x-waiter-auth=this is a real cookie"
                                                                      "x-waiter-auth")))
  (is (= "a=b; c=d" (remove-cookie "a=b; x-waiter-auth=auth; c=d" "x-waiter-auth"))))

(deftest test-add-encoded-cookie
  (let [cookie-attrs ";Max-Age=864000;Path=/;HttpOnly=true"
        user-cookie (str "user=" (UrlEncoded/encodeString "data:john") cookie-attrs)]
    (with-redefs [b64/encode (fn [^String data-string] (.getBytes data-string))
                  nippy/freeze (fn [input _] (str "data:" input))]
      (is (= {:headers {"set-cookie" user-cookie}}
             (add-encoded-cookie {} [:cached "password"] "user" "john" 10)))
      (is (= {:headers {"set-cookie" ["foo=bar" user-cookie]}}
             (add-encoded-cookie {:headers {"set-cookie" "foo=bar"}} [:cached "password"] "user" "john" 10)))
      (is (= {:headers {"set-cookie" ["foo=bar" "baz=quux" user-cookie]}}
             (add-encoded-cookie {:headers {"set-cookie" ["foo=bar" "baz=quux"]}} [:cached "password"] "user" "john" 10)))
      (let [response-chan (async/promise-chan)]
        (async/>!! response-chan {})
        (is (= {:headers {"set-cookie" user-cookie}}
               (async/<!! (add-encoded-cookie response-chan [:cached "password"] "user" "john" 10))))))))

(deftest test-decode-cookie
  (with-redefs [b64/decode (fn [value-bytes] (String. ^bytes value-bytes "utf-8"))
                nippy/thaw (fn [input _] (str "data:" input))]
    (is (= "data:john" (decode-cookie "john" [:cached "password"])))))

(deftest test-decode-cookie-exception

  (try
    (nippy/thaw (.getBytes "some-string") {:password [:cached "password"] :v1-compatibility? false :compressor nil})
    (is false "Exception not thrown")
    (catch Exception e
      (is (instance? ExceptionInfo e))
      (is (get-in (ex-data e) [:opts :password]))))

  (with-redefs [b64/decode (fn [value-bytes] value-bytes)]
    (try
      (decode-cookie "john" [:cached "password"])
      (is false "Exception not thrown")
      (catch Exception e
        (is (instance? ExceptionInfo e))
        (is (= "***" (get-in (ex-data e) [:opts :password])))))))

(deftest test-decode-cookie-cached
  (let [first-key (str "user1-" (rand-int 100000))
        second-key (str "user2-" (rand-int 100000))]
    (is (thrown? Exception (decode-cookie-cached first-key [:cached "password"])))
    (with-redefs [b64/decode (fn [value-bytes] (String. ^bytes value-bytes "utf-8"))
                  nippy/thaw (fn [input _] (str "data:" input))]
      (is (= (str "data:" first-key) (decode-cookie-cached first-key [:cached "password"]))))
    (is (= (str "data:" first-key) (decode-cookie-cached first-key [:cached "password"])))
    (is (thrown? Exception (decode-cookie-cached second-key [:cached "password"])))))
