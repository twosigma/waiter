;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns waiter.util.http-utils-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [qbits.jet.client.http :as http]
            [waiter.util.http-utils :refer :all]
            [waiter.util.utils :as utils])
  (:import clojure.lang.ExceptionInfo))

(deftest test-http-request
  (testing "successful-response"
    (let [http-client (Object.)
          expected-body {:foo "bar"
                         :fee {:fie {:foe "fum"}}
                         :solve 42}]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (utils/clj->json expected-body))
                                     (async/>!! response-chan {:body body-chan, :status 200})
                                     response-chan))]
        (is (= expected-body (http-request http-client "some-url"))))))

  (testing "error-in-response"
    (let [http-client (Object.)
          expected-exception (IllegalStateException. "Test Exception")]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)]
                                     (async/>!! response-chan {:error expected-exception})
                                     response-chan))]
        (is (thrown-with-msg? IllegalStateException #"Test Exception"
                              (http-request http-client "some-url"))))))

  (testing "non-2XX-response-without-throw-exceptions"
    (let [http-client (Object.)
          expected-body {:foo "bar"
                         :fee {:fie {:foe "fum"}}
                         :solve 42}]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (utils/clj->json expected-body))
                                     (async/>!! response-chan {:body body-chan, :status 400})
                                     response-chan))]
        (is (= expected-body (http-request http-client "some-url" :throw-exceptions false))))))

  (testing "non-2XX-response-with-throw-exceptions"
    (let [http-client (Object.)]
      (with-redefs [http/request (constantly
                                   (let [response-chan (async/promise-chan)
                                         body-chan (async/promise-chan)]
                                     (async/>!! body-chan (utils/clj->json {}))
                                     (async/>!! response-chan {:body body-chan, :status 400})
                                     response-chan))]
        (is (thrown? ExceptionInfo (http-request http-client "some-url")))))))
