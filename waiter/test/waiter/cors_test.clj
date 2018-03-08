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
(ns waiter.cors-test
  (:require [clojure.test :refer :all]
            [waiter.cors :refer :all]
            [waiter.core :as core])
  (:import waiter.cors.PatternBasedCorsValidator))

(deftest pattern-validator-test
  (let [validator (pattern-based-validator {:allowed-origins [#"^http://[^\.]+\.example\.org(:80)?$"
                                                              #"^https://anotherapp.example.org:12345$"]})
        create-request-with-origin (fn [origin] {:headers {"origin" origin}})]
    (is (preflight-allowed? validator (create-request-with-origin "http://myapp.example.org")))
    (is (preflight-allowed? validator (create-request-with-origin "http://myapp.example.org:80")))
    (is (preflight-allowed? validator (create-request-with-origin "https://anotherapp.example.org:12345")))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://anotherapp.example.org:12345"))))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://anotherapp.example.org:12346"))))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://myapp.baddomain.com"))))
    (is (not (preflight-allowed? validator (create-request-with-origin "http://myapp.baddomain.com:8080"))))
    (is (request-allowed? validator {:headers {"origin" "http://example.com"
                                               "host" "example.com"}
                                     :scheme :http}))
    (is (not (request-allowed? validator {:headers {"origin" "http://bad.example.com"
                                                    "host" "bad.example.com"}
                                          :scheme :https})))
    (is (not (request-allowed? validator {:headers {"origin" "http://bad.example.com"
                                                    "host" "good.example.com"}
                                          :scheme :http})))))

(deftest test-pattern-based-validator
  (is (thrown? Throwable (pattern-based-validator {})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins nil})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins ["foo"]})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins [#"foo" "bar"]})))
  (is (thrown? Throwable (pattern-based-validator {:allowed-origins [#"foo" #"bar" "baz"]})))
  (is (instance? PatternBasedCorsValidator (pattern-based-validator {:allowed-origins [#"foo" #"bar" #"baz"]}))))

(deftest test-wrap-cors
  (testing "cors request denied"
    (let [deny-all (deny-all-validator {})
          request {:headers {"origin" "doesnt.matter"}}
          handler (-> (fn [request] {:status 200})
                      (wrap-cors deny-all)
                      (core/wrap-error-handling))
          {:keys [status] :as response} (handler request)]
      (is (= 403 status))))
  (testing "cors request allowed"
    (let [allow-all (allow-all-validator {})
          request {:headers {"origin" "doesnt.matter"}}
          handler (-> (fn [request] {:status 200})
                      (wrap-cors allow-all))
          {:keys [headers status] :as response} (handler request)]
      (is (= 200 status))
      (is (= "doesnt.matter" (get headers "Access-Control-Allow-Origin")))
      (is (= "true" (get headers "Access-Control-Allow-Credentials"))))))
