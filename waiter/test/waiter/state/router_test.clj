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
(ns waiter.state.router-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [waiter.discovery :as discovery]
            [waiter.state.router :refer :all]))

(deftest test-retrieve-peer-routers
  (testing "successful-retrieval-from-discovery"
    (let [router-id->details {"router-1" {}, "router-2" {}}
          router-id->endpoint-url {"router-1" "url-1", "router-2" "url-2"}]
      (with-redefs [discovery/router-id->details (constantly router-id->details)
                    discovery/router-id->endpoint-url (constantly router-id->endpoint-url)]
        (let [discovery (Object.)
              router-chan (async/chan 1)]
          (retrieve-peer-routers discovery router-chan)
          (is (= {:router-id->details router-id->details :router-id->endpoint-url router-id->endpoint-url}
                 (async/<!! router-chan)))))))
  (testing "exception-on-retrieval-from-discovery"
    (with-redefs [discovery/router-id->endpoint-url (fn [_ _ _]
                                                      (throw (RuntimeException. "Expected exception thrown from test")))]
      (let [discovery (Object.)
            router-chan (async/chan 1)]
        (is (thrown-with-msg? RuntimeException #"Expected exception thrown from test"
                              (retrieve-peer-routers discovery router-chan)))))))

