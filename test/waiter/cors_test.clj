;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.cors-test
  (:require [clojure.test :refer :all]
            [waiter.cors :refer :all]))

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

(deftest create-cors-validator-test
  (is (create-cors-validator {:kind :custom
                              :allowed-origins []
                              :custom-impl "waiter.cors/pattern-based-validator"}))
  (is (thrown? AssertionError (create-cors-validator {:kind :patterns})))
  (is (create-cors-validator {:kind :patterns
                              :allowed-origins []})))
