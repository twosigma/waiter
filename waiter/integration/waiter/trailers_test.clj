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
(ns waiter.trailers-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.http-utils :as hu]))

(defn- run-sediment-trailers-support-test
  [waiter-url backend-proto]
  (testing "request and response trailers"
    (let [request-length 100000
          long-request (apply str (take request-length (cycle "abcdefghijklmnopqrstuvwxyz")))
          sediment-command (sediment-server-command "${PORT0}")
          request-headers {:x-waiter-backend-proto backend-proto
                           :x-waiter-cmd sediment-command
                           :x-waiter-debug true
                           :x-waiter-mem 512
                           :x-waiter-name (rand-name)}
          {:keys [service-id] :as canary-response}
          (make-request-with-debug-info request-headers #(make-shell-request waiter-url % :path "/status"))]
      (assert-response-status canary-response http-200-ok)
      (with-service-cleanup
        service-id
        (testing "jet returns some trailers"
          (let [waiter-url (cond-> waiter-url
                             (= "h2c" backend-proto) retrieve-h2c-url)
                response (make-shell-request
                           waiter-url
                           (assoc request-headers
                             "x-cid" (rand-name))
                           :body (make-chunked-body long-request 4096 20)
                           :path "/trailers"
                           :protocol backend-proto)
                body-json (try
                            (some-> response :body str json/read-str)
                            (catch Exception ex
                              (log/error ex "unable to parse response as json")
                              (is false (str "unable to parse response as json" (:body response)))))
                http-version (hu/backend-protocol->http-version backend-proto)]
            (assert-response-status response http-200-ok)
            (is (= http-version (get body-json "protocol")) (str body-json))
            (when (= "http" backend-proto)
              (is (= "chunked" (get-in body-json ["headers" "Transfer-Encoding"]))
                  (str body-json))
              (is (= "chunked" (get-in response [:headers "transfer-encoding"]))
                  (-> response :headers str)))
            (is (= {} (get body-json "trailers"))
                (-> response :headers str))
            (is (nil? (some-> response :trailers))
                (-> response :headers str))))

        (let [request-trailer-delay-ms 100
              response-trailer-delay-ms 100]
          (doseq [response-status [http-200-ok http-400-bad-request http-500-internal-server-error]]
            (testing (str {:backend-proto backend-proto
                           :request-trailer-delay-ms request-trailer-delay-ms
                           :response-status response-status
                           :response-trailer-delay-ms response-trailer-delay-ms})
              (let [request-trailers {(rand-name "foo") (rand-name "bar")
                                      (rand-name "lorem") (rand-name "ipsum")}
                    response-trailers {(rand-name "fee") (rand-name "fie")
                                       (rand-name "foe") (rand-name "fum")}
                    waiter-url (cond-> waiter-url
                                 (= "h2c" backend-proto) retrieve-h2c-url)
                    response (make-shell-request
                               waiter-url
                               (reduce
                                 (fn [request-headers [k v]]
                                   (assoc request-headers
                                     (str "x-sediment-response-trailer-" k) (str v)))
                                 (assoc request-headers
                                   "x-sediment-response-status" response-status
                                   "x-sediment-sleep-before-response-trailer-ms" response-trailer-delay-ms
                                   "x-sediment-sleep-after-chunk-send-ms" 100)
                                 (seq response-trailers))
                               :body (make-chunked-body long-request 4096 20)
                               :path "/trailers"
                               :protocol backend-proto
                               :trailers-fn (fn []
                                              (Thread/sleep request-trailer-delay-ms)
                                              request-trailers))
                    _ (log/info "response headers:" (:headers response))
                    _ (assert-response-status response response-status)
                    body-json (try
                                (some-> response :body str json/read-str)
                                (catch Exception ex
                                  (log/error ex "unable to parse response as json")
                                  (is false (str "unable to parse response as json" (:body response)))))
                    http-version (hu/backend-protocol->http-version backend-proto)]
                (is (= http-version (get body-json "protocol")))
                (when (= "http" backend-proto)
                  (is (= "chunked" (get-in body-json ["headers" "Transfer-Encoding"]))
                      (str body-json))
                  (is (= "chunked" (get-in response [:headers "transfer-encoding"]))
                      (-> response :headers str)))
                ;; we do not sent trailers to http/1 backends
                (is (= (if (hu/http2? http-version) request-trailers {})
                       (get body-json "trailers"))
                    (-> response :headers str))
                (is (= response-trailers (some-> response :trailers))
                    (-> response :headers str))))))))))

(deftest ^:parallel ^:integration-fast test-trailers-support-http-proto-sediment
  (testing-using-waiter-url
    (run-sediment-trailers-support-test waiter-url "http")))

(deftest ^:parallel ^:integration-fast test-trailers-support-h2c-proto-sediment
  (testing-using-waiter-url
    (run-sediment-trailers-support-test waiter-url "h2c")))

(defn- run-kitchen-trailers-support-test
  [waiter-url backend-proto]
  (testing "request and response trailers"
    (let [request-length 100000
          long-request (apply str (take request-length (cycle "abcdefghijklmnopqrstuvwxyz")))
          request-headers {:x-waiter-backend-proto backend-proto
                           :x-waiter-debug true
                           :x-waiter-name (rand-name)}
          {:keys [service-id] :as canary-response}
          (make-request-with-debug-info request-headers #(make-kitchen-request waiter-url % :path "/status"))]
      (assert-response-status canary-response http-200-ok)
      (with-service-cleanup
        service-id
        (doseq [response-trailer-delay-ms [0 1000]]
          (testing (str {:backend-proto backend-proto
                         :response-trailer-delay-ms response-trailer-delay-ms})
            (let [response-trailers {(rand-name "fee") (rand-name "fie")
                                     (rand-name "foe") (rand-name "fum")}
                  response (make-kitchen-request
                             waiter-url
                             (reduce
                               (fn [request-headers [k v]]
                                 (assoc request-headers
                                   (str "x-kitchen-trailer-" k) (str v)))
                               (assoc request-headers
                                 "x-kitchen-pre-trailer-sleep-ms" response-trailer-delay-ms)
                               (seq response-trailers))
                             :body (make-chunked-body long-request 4096 20)
                             :path "/chunked"
                             :protocol backend-proto)]
              (log/info "response headers:" (:headers response))
              (assert-response-status response http-200-ok)
              (when (= "http" backend-proto)
                (is (= "chunked" (get-in response [:headers "transfer-encoding"]))
                    (-> response :headers str)))
              (is (= response-trailers (some-> response :trailers))
                  (-> response :headers str)))))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-trailers-support-http-proto-kitchen
  (testing-using-waiter-url
    (run-kitchen-trailers-support-test waiter-url "http")))
