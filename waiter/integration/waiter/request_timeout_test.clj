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
(ns waiter.request-timeout-test
  (:require [clj-time.core :as time]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [qbits.jet.client.http :as http]
            [waiter.client-tools :refer :all])
  (:import java.util.concurrent.CountDownLatch))

(defmacro assert-failed-request [service-name response-body start-time-ms timeout-period-sec faulty-app?]
  `(let [end-time-ms# (System/currentTimeMillis)
         elapsed-time-secs# (time/in-seconds (time/millis (- end-time-ms# ~start-time-ms)))]
     (is (>= elapsed-time-secs# ~timeout-period-sec))
     (is (str/includes? ~response-body (str "After " ~timeout-period-sec " seconds, no instance available to handle request.")))
     (when ~faulty-app?
       (is (str/includes? ~response-body "Check that your service is able to start properly!")))
     (is (and (str/includes? ~response-body "outstanding-requests")
              (not (str/includes? ~response-body "outstanding-requests 0"))))
     (is (str/includes? ~response-body "requests-waiting-to-stream 0"))
     (is (str/includes? ~response-body "waiting-for-available-instance 1"))
     (is (str/includes? ~response-body ~service-name))
     (log/info "Ran assertions on timed-out request")))

(defn- make-request-verbose
  "Makes a request with the provided headers and cookies, and with verbose logging"
  [waiter-url request-headers cookies]
  (let [response (make-request waiter-url "/secrun"
                               :http-method-fn http/post
                               :headers request-headers
                               :cookies cookies
                               :verbose true)
        service-name (str (get-in response [:request-headers :x-waiter-name]))]
    (log/info "response:" (:body response))
    (assoc response :service-name service-name)))

(defn- make-request-and-assert-timeout
  [waiter-url request-headers start-time-ms timeout-period-sec faulty-app? & {:keys [cookies] :or {cookies {}}}]
  (let [{:keys [body service-name] :as response} (make-request-verbose waiter-url request-headers cookies)]
    (assert-response-status response 503)
    (assert-failed-request service-name body start-time-ms timeout-period-sec faulty-app?)))

(defn- make-successful-request
  [waiter-url request-headers & {:keys [cookies endpoint] :or {cookies {}, endpoint "/secrun"}}]
  (let [response (make-request-with-debug-info
                   request-headers
                   #(make-request waiter-url endpoint
                                  :http-method-fn http/post
                                  :headers %
                                  :cookies cookies
                                  :verbose true))]
    (log/info "response: " (:body response))
    response))

(deftest ^:parallel ^:integration-fast test-request-client-timeout
  (testing-using-waiter-url
    (log/info (str "request-client-timeout-test: if we can't get an instance quickly inside client timeout"))
    (let [timeout-period 2000
          extra-headers {:x-waiter-name (rand-name)
                         :x-waiter-timeout timeout-period
                         :x-waiter-debug "true"
                         :x-kitchen-delay-ms (+ 2000 timeout-period)}
          response (make-kitchen-request waiter-url extra-headers)
          response-headers (:headers response)
          response-body (:body response)
          service-id (retrieve-service-id waiter-url (:request-headers response))]
      (assert-response-status response 503)
      (log/info "Response code check executed.")
      (is (str/includes? response-body "Connection error while sending request to instance"))
      (log/info "Response body check executed.")
      (is (not (str/blank? (get response-headers "x-waiter-backend-id"))))
      (is (not (str/blank? (get response-headers "x-waiter-backend-host"))))
      (is (not (str/blank? (get response-headers "x-waiter-backend-port"))))
      (is (not (str/blank? (get response-headers "x-waiter-backend-proto"))))
      (is (not (str/blank? (get response-headers "x-cid"))))
      (log/info "Response headers check executed.")
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-request-queue-timeout-slow-start-app
  (testing-using-waiter-url
    (let [timeout-period-sec 30
          timeout-period-ms (time/in-millis (time/seconds timeout-period-sec))
          start-time-ms (System/currentTimeMillis)
          start-up-sleep-ms (* 2 timeout-period-ms)
          request-headers (walk/stringify-keys
                            (merge (kitchen-request-headers)
                                   {:x-waiter-name (rand-name)
                                    :x-waiter-cmd (kitchen-cmd (str "-p $PORT0 --start-up-sleep-ms " start-up-sleep-ms))
                                    :x-waiter-queue-timeout timeout-period-ms}))]
      (make-request-and-assert-timeout waiter-url request-headers start-time-ms timeout-period-sec true)
      (delete-service waiter-url request-headers))))

(deftest ^:parallel ^:integration-slow test-request-queue-timeout-faulty-app
  (testing-using-waiter-url
    (log/info (str "request-queue-timeout-faulty-app: if we can't get an instance quickly (should take " (colored-time "~1 minute") ")"))
    (let [timeout-period-sec 60
          start-time-ms (System/currentTimeMillis)
          request-headers (walk/stringify-keys
                            {:x-waiter-name (rand-name)
                             :x-waiter-cpus 1
                             :x-waiter-mem 100
                             :x-waiter-version "a-version"
                             :x-waiter-cmd-type "shell"
                             :x-waiter-cmd "sleep 5"
                             :x-waiter-health-check-url "/status"
                             :x-waiter-queue-timeout (time/in-millis (time/seconds timeout-period-sec))})]
      (make-request-and-assert-timeout waiter-url request-headers start-time-ms timeout-period-sec false)
      (delete-service waiter-url request-headers))))

; Marked explicit due to:
; FAIL in (test-request-queue-timeout-unable-to-scale-app)
;  Body:Hello World
; expected: (clojure.core/= 503 actual-status__18382__auto__)
;   actual: (not (clojure.core/= 503 200))
;
; FAIL in (test-request-queue-timeout-unable-to-scale-app)
; expected: (>= elapsed-time-secs timeout-period-sec)
;   actual: (not (>= 0 15))
;
; FAIL in (test-request-queue-timeout-unable-to-scale-app)
; expected: (str/includes? response-body "requests-waiting-to-stream: 0")
;   actual: (not (str/includes? "Hello World" "requests-waiting-to-stream: 0"))
;
; FAIL in (test-request-queue-timeout-unable-to-scale-app)
; expected: (str/includes? response-body "waiting-for-available-instance: 1")
;   actual: (not (str/includes? "Hello World" "waiting-for-available-instance: 1"))
(deftest ^:parallel ^:integration-slow ^:explicit test-request-queue-timeout-unable-to-scale-app
  (testing-using-waiter-url
    (let [timeout-period-sec 15
          long-request-period-ms 30000
          service-name (rand-name)
          request-headers (walk/stringify-keys
                            (merge (kitchen-request-headers)
                                   {:x-waiter-name service-name
                                    :x-waiter-cmd (kitchen-cmd "-p $PORT0")
                                    :x-waiter-max-instances 1}))
          {:keys [cookies service-id] :as response} (make-successful-request waiter-url request-headers)
          _ (assert-response-status response 200)
          router-id->router-url (routers waiter-url)
          num-routers (count router-id->router-url)
          started-latch (CountDownLatch. num-routers)
          completed-latch (CountDownLatch. num-routers)]
      (doseq [[router-id router-url] (seq router-id->router-url)] ; make multiple requests to disable work-stealing
        (let [request-cid-success (str service-name "-success." router-id)]
          (async/thread
            (log/info "starting long request" request-cid-success "for" long-request-period-ms "ms at" router-url)
            (.countDown started-latch)
            (try
              (assert-response-status
                (make-successful-request router-url
                                         (assoc request-headers
                                           :x-cid request-cid-success
                                           :x-kitchen-delay-ms long-request-period-ms)
                                         :cookies cookies)
                200)
              (log/info "long request" request-cid-success "complete")
              (catch Exception e
                (log/error e "long request" request-cid-success "encountered error")
                (is false (str e))))
            (.countDown completed-latch))))
      (log/info "awaiting long request(s) to start...")
      (.await started-latch)
      (Thread/sleep 1000)
      (let [request-cid-timeout (str service-name "-timeout")
            request-headers (assoc request-headers
                              :x-cid request-cid-timeout
                              :x-waiter-queue-timeout (time/in-millis (time/seconds timeout-period-sec)))
            start-time-ms (System/currentTimeMillis)]
        (log/info "making request" request-cid-timeout "that is expected to timeout")
        (let [{:keys [body service-name] :as response} (make-request-verbose waiter-url request-headers cookies)]
          (assert-response-status response 503)
          (assert-failed-request service-name body start-time-ms timeout-period-sec false)))
      (log/info "awaiting long request(s) to complete...")
      (.await completed-latch)
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-grace-period-with-tokens
  (testing-using-waiter-url
    (let [grace-period (time/minutes 2)
          token (rand-name)]
      (try
        (log/info "Creating token for" token)
        (let [{:keys [status body]}
              (post-token waiter-url {:cmd (kitchen-cmd (str "-p $PORT0 " (time/in-millis grace-period)))
                                      :version "not-used"
                                      :cpus 1
                                      :mem 1024
                                      :health-check-url "/status"
                                      :permitted-user "*"
                                      :token token
                                      :name (str "test" token)
                                      :grace-period-secs (time/in-seconds grace-period)
                                      :cmd-type "shell"})]
          (is (= 200 status) (str "Did not get a 200 response. " body)))
        (log/info "Making request for" token)
        (let [{:keys [status body service-id] :as response} (make-request-with-debug-info
                                                              {:x-waiter-token token}
                                                              #(make-request waiter-url "/secrun" :headers %))]
          (is (= 200 status) (str "Did not get a 200 response. " body))
          (when (and (= 200 status) (can-query-for-grace-period? waiter-url))
            (log/info "Verifying app grace period for" token)
            (is (= (time/in-seconds grace-period) (service-id->grace-period waiter-url service-id))))
          (delete-service waiter-url service-id))
        (finally
          (delete-token-and-assert waiter-url token))))))
