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
(ns waiter.proto-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]))

(defn- run-backend-proto-service-test
  "Helper method to run tests with various backend protocols"
  ([waiter-url backend-proto health-check-port-index backend-scheme backend-proto-version]
   (run-backend-proto-service-test
     waiter-url backend-proto backend-proto health-check-port-index backend-scheme backend-proto-version))
  ([waiter-url backend-proto health-check-proto health-check-port-index backend-scheme backend-proto-version]
   (let [nginx-command (nginx-server-command backend-proto)
         request-headers {:x-waiter-backend-proto backend-proto
                          :x-waiter-cmd nginx-command
                          :x-waiter-env-kitchen_cmd (kitchen-cmd)
                          :x-waiter-health-check-port-index health-check-port-index
                          :x-waiter-health-check-proto health-check-proto
                          :x-waiter-name (rand-name)
                          :x-waiter-ports 3}
         {:keys [headers request-headers service-id] :as response}
         (make-request-with-debug-info request-headers #(make-shell-request waiter-url % :path "/request-info"))]
     (with-service-cleanup
       service-id
       (is service-id)
       (assert-response-status response 200)
       (let [{:strs [x-nginx-client-proto x-nginx-client-scheme x-waiter-backend-proto]} headers]
         (is (= backend-proto-version x-nginx-client-proto))
         (is (= backend-scheme x-nginx-client-scheme))
         (is (= backend-proto x-waiter-backend-proto)))
       (let [service-settings (service-settings waiter-url service-id)]
         (is (= backend-proto (get-in service-settings [:service-description :backend-proto])))
         (is (= nginx-command (get-in service-settings [:service-description :cmd]))))
       (testing "streaming"
         (let [kitchen-response-size 200000
               request-headers (assoc request-headers
                                 :x-kitchen-chunk-delay 10
                                 :x-kitchen-chunk-size 2000
                                 :x-kitchen-response-size kitchen-response-size)
               response (make-shell-request waiter-url request-headers :path "/chunked")]
           (assert-response-status response 200)
           (is (= kitchen-response-size (-> response :body str .getBytes count)))))))))

(deftest ^:parallel ^:integration-fast test-http-backend-proto-service
  (testing-using-waiter-url
    (run-backend-proto-service-test waiter-url "http" 1 "http" "HTTP/1.1")))

(deftest ^:parallel ^:integration-fast test-https-backend-proto-service
  (testing-using-waiter-url
    (run-backend-proto-service-test waiter-url "https" 1 "https" "HTTP/1.1")))

(deftest ^:parallel ^:integration-fast test-h2c-backend-proto-service
  (testing-using-waiter-url
    (run-backend-proto-service-test waiter-url "h2c" 1 "http" "HTTP/2.0")))

(deftest ^:parallel ^:integration-fast test-h2-backend-proto-service
  (testing-using-waiter-url
    (run-backend-proto-service-test waiter-url "h2" 1 "https" "HTTP/2.0")))

(deftest ^:parallel ^:integration-fast test-h2-backend-proto-service-health-check-on-port0
  (testing-using-waiter-url
    (run-backend-proto-service-test waiter-url "h2" 0 "https" "HTTP/2.0")))

(deftest ^:parallel ^:integration-fast test-health-check-proto
  (testing-using-waiter-url
    ;; PORT2 is running kitchen without SSL enabled
    (run-backend-proto-service-test waiter-url "h2" "https" 1 "https" "HTTP/2.0")))

;; disabling http/2 tests temporarily
(deftest ^:explicit ^:parallel ^:integration-fast test-internal-protocol
  (testing-using-waiter-url
    (let [{:keys [http2c? http2? ssl-port]} (:server-options (waiter-settings waiter-url))
          retrieve-client-protocol #(get-in % ["request-info" "client-protocol"])
          retrieve-internal-protocol #(get-in % ["request-info" "internal-protocol"])
          retrieve-scheme (fn [body-json]
                            (or (get-in body-json ["request-info" "headers" "x-forwarded-proto"])
                                (get-in body-json ["request-info" "scheme"])))]
      (testing "HTTP/1.1 cleartext request"
        (let [{:keys [body] :as response} (make-request waiter-url
                                                        "/status"
                                                        :client http1-client
                                                        :query-params {"include" "request-info"})
              body-json (some-> body str json/read-str)]
          (assert-response-status response 200)
          (is (= "HTTP/1.1" (retrieve-client-protocol body-json)) (str body-json))
          (is (= "HTTP/1.1" (retrieve-internal-protocol body-json)) (str body-json))
          (is (= "http" (retrieve-scheme body-json)) (str body-json))))
      (when http2c?
        (testing "HTTP/2.0 cleartext request"
          (let [{:keys [body] :as response} (make-request (retrieve-h2c-url waiter-url)
                                                          "/status"
                                                          :client http2-client
                                                          :query-params {"include" "request-info"})
                body-json (some-> body str json/read-str)]
            (assert-response-status response 200)
            (is (= "HTTP/2.0" (retrieve-client-protocol body-json)) (str body-json))
            (is (= "HTTP/2.0" (retrieve-internal-protocol body-json)) (str body-json))
            (is (= "http" (retrieve-scheme body-json)) (str body-json)))))
      (when ssl-port
        (testing "HTTP/1.1 secure request"
          (let [{:keys [body] :as response} (make-request (retrieve-ssl-url waiter-url ssl-port)
                                                          "/status"
                                                          :client http1-client
                                                          :query-params {"include" "request-info"}
                                                          :scheme "https")
                body-json (some-> body str json/read-str)]
            (assert-response-status response 200)
            (is (= "HTTP/1.1" (retrieve-client-protocol body-json)) (str body-json))
            (is (= "HTTP/1.1" (retrieve-internal-protocol body-json)) (str body-json))
            (is (= "https" (retrieve-scheme body-json)) (str body-json))))
        (when http2?
          (testing "HTTP/2.0 secure request"
            (let [{:keys [body] :as response} (make-request (retrieve-ssl-url waiter-url ssl-port)
                                                            "/status"
                                                            :client http2-client
                                                            :query-params {"include" "request-info"}
                                                            :scheme "https")
                  body-json (some-> body str json/read-str)]
              (assert-response-status response 200)
              (is (= "HTTP/2.0" (retrieve-client-protocol body-json)) (str body-json))
              (is (= "HTTP/2.0" (retrieve-internal-protocol body-json)) (str body-json))
              (is (= "https" (retrieve-scheme body-json)) (str body-json)))))))))
