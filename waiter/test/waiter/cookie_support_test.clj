;;
;;       Copyright (c) Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.cookie-support-test
  (:require [clojure.core.async :as async]
            [clojure.data.codec.base64 :as b64]
            [clojure.test :refer :all]
            [taoensso.nippy :as nippy]
            [waiter.cookie-support :refer :all]))

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

(deftest test-correct-cookies-as-vector
  (is (= {:headers {"test" "a", "Set-Cookie" nil}} (correct-cookies-as-vector {:headers {"test" "a"}})))
  (is (= ["a"] (get-in (correct-cookies-as-vector {:headers {"Set-Cookie" "a"}}) [:headers "Set-Cookie"])))
  (is (= ["a"] (get-in (correct-cookies-as-vector {:headers {"Set-Cookie" ["a"]}}) [:headers "Set-Cookie"])))
  (is (= ["a" "b"] (get-in (correct-cookies-as-vector {:headers {"Set-Cookie" ["a" "b"]}}) [:headers "Set-Cookie"]))))

(deftest test-cookies-async-response
  (is (= {:status 200}
         (cookies-async-response {:status 200})))
  (is (= {:headers {"Set-Cookie" []}}
         (cookies-async-response {:cookies {}})))
  (is (= {:headers {"Set-Cookie" ["name=john;Max-Age=60"]}}
         (cookies-async-response {:cookies {"name" {:value "john", :max-age 60}}})))
  (is (= {:headers {"Set-Cookie" ["name=john;Expires=2017-12-30"]}}
         (cookies-async-response {:cookies {"name" {:value "john", :expires "2017-12-30"}}})))
  (let [response-chan (async/promise-chan)]
    (async/>!! response-chan {:cookies {"name" {:value "john", :max-age 60}}})
    (is (= {:headers {"Set-Cookie" ["name=john;Max-Age=60"]}}
           (async/<!! (cookies-async-response response-chan))))))

(deftest test-add-encoded-cookie
  (with-redefs [b64/encode (fn [^String data-string] (.getBytes data-string))
                nippy/freeze (fn [input _] (str "data:" input))]
    (is (= {:cookies {"user" {:value "data:john", :max-age 864000}}}
           (add-encoded-cookie {} [:cached "password"] "user" "john" 10)))
    (let [response-chan (async/promise-chan)]
      (async/>!! response-chan {})
      (is (= {:cookies {"user" {:value "data:john", :max-age 864000}}}
             (async/<!! (add-encoded-cookie response-chan [:cached "password"] "user" "john" 10)))))))

(deftest test-decode-cookie
  (with-redefs [b64/decode (fn [value-bytes] (String. ^bytes value-bytes "utf-8"))
                nippy/thaw (fn [input _] (str "data:" input))]
    (is (= "data:john" (decode-cookie "john" [:cached "password"])))))

(deftest test-decode-cookie-cached
  (let [first-key (str "user1-" (rand-int 100000))
        second-key (str "user2-" (rand-int 100000))]
    (is (thrown? Exception (decode-cookie-cached first-key [:cached "password"])))
    (with-redefs [b64/decode (fn [value-bytes] (String. ^bytes value-bytes "utf-8"))
                  nippy/thaw (fn [input _] (str "data:" input))]
      (is (= (str "data:" first-key) (decode-cookie-cached first-key [:cached "password"]))))
    (is (= (str "data:" first-key) (decode-cookie-cached first-key [:cached "password"])))
    (is (thrown? Exception (decode-cookie-cached second-key [:cached "password"])))))
