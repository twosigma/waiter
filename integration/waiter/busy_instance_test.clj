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
(ns waiter.busy-instance-test
  (:require [clj-http.client :as http]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :as walk]
            [waiter.client-tools :refer :all]))

(deftest ^:parallel ^:integration-slow test-busy-instance-not-reserved
  (testing-using-waiter-url
    (let [url (str "http://" waiter-url "/endpoint")
          req-headers (walk/stringify-keys
                        (merge (kitchen-request-headers)
                               {:x-waiter-name (rand-name "testbusyinstance")
                                :x-waiter-debug true}))]

      ;; Make requests to get instances started and avoid shuffling among routers later
      (parallelize-requests 8 10 #(http/get url {:headers (assoc req-headers :x-kitchen-delay-ms 4000)
                                                 :spnego-auth true}))

      ;; Make a request that returns a 503
      (let [start-millis (System/currentTimeMillis)
            {:keys [headers]} (http/get url {:headers (assoc req-headers "x-kitchen-act-busy" "true")
                                             :spnego-auth true
                                             :throw-exceptions false})
            router-id (get headers "X-Waiter-Router-Id")
            backend-id (get headers "X-Waiter-Backend-Id")
            blacklist-time-millis (get-in (waiter-settings waiter-url) [:blacklist-config :blacklist-backoff-base-time-ms])]

        (is (integer? blacklist-time-millis))

        ;; We shouldn't see the same instance for blacklist-time-millis from the same router
        (let [results (parallelize-requests
                        2
                        30
                        #(let [{:keys [headers]} (http/get url {:headers req-headers :spnego-auth true})]
                          (when (-> (System/currentTimeMillis) (- start-millis) (< (- blacklist-time-millis 1000)))
                            (and (= backend-id (get headers "X-Waiter-Backend-Id"))
                                 (= router-id (get headers "X-Waiter-Router-Id"))))))]
          (is (every? #(not %) results))))

      (delete-service waiter-url (retrieve-service-id waiter-url req-headers)))))

(deftest ^:parallel ^:integration-fast test-max-queue-length
  (testing-using-waiter-url
    (let [extra-headers {:x-waiter-name (rand-name "testmaxqueuelength")
                         :x-waiter-max-instances 1
                         :x-waiter-max-queue-length 5
                         :x-kitchen-delay-ms 1000}
          responses (atom [])
          request-fn (fn [& {:keys [cookies] :or {cookies {}}}]
                       (let [response (make-kitchen-request waiter-url extra-headers :cookies cookies)]
                         (swap! responses conj response)
                         response))
          {:keys [cookies] :as first-request} (request-fn)
          service-id (retrieve-service-id waiter-url (:request-headers first-request))]

      (parallelize-requests 50 1 (fn [] (request-fn :cookies cookies)))
      (let [responses-with-503 (filter #(= 503 (:status %)) @responses)]
        (is (not (empty? responses-with-503)))
        (is (< (count responses-with-503) 50))
        (is (every? (fn [response]
                      (every? #(str/includes? (str (:body response)) (str %))
                              ["Max queue length exceeded!" "\"max-queue-length\":5" "current-queue-length" service-id]))
                    responses-with-503)))
      (delete-service waiter-url service-id))))
