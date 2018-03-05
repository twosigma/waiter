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
(ns waiter.async-request-test
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [waiter.async-request :refer :all]
            [waiter.service :as service])
  (:import java.net.URLDecoder))

(deftest test-monitor-async-request
  (let [check-interval-ms 10
        request-timeout-ms 105
        correlation-id "test-monitor-async-request-cid"
        status-endpoint "http://www.example-host.com/waiter-async-status"]

    (testing "error-in-response-from-make-request"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            exception-message "exception for testing purposes"
            make-http-request (fn [] (async/go {:body (async/chan 1), :error (Exception. exception-message)}))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (= [:instance-error] @release-status-atom))
        (is (= :make-request-error monitor-result))))

    (testing "status-check-timed-out"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go {:body (async/chan 1), :status 200}))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (= (int (Math/ceil (/ (double request-timeout-ms) check-interval-ms))) @make-request-counter))
        (is (= [:success] @release-status-atom))
        (is (= :monitor-timed-out monitor-result))))

    (testing "status-request-terminated"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go {:body (async/chan 1), :status 200}))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            _ (async/go
                (async/<! (async/timeout check-interval-ms))
                (async/put! exit-chan :exit))
            monitor-result (async/<!! response-chan)]
        (is (> (int (Math/ceil (/ (double request-timeout-ms) check-interval-ms))) @make-request-counter))
        (is (= [:success] @release-status-atom))
        (is (= :request-terminated monitor-result))))

    (testing "status-check-request-no-longer-active"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go {:body (async/chan 1), :status 200}))
            calls-to-in-active 6
            request-still-active? (fn [] (< @make-request-counter calls-to-in-active))
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (= calls-to-in-active @make-request-counter))
        (is (= [:success] @release-status-atom))
        (is (= :request-no-longer-active monitor-result))))

    (testing "status-check-eventually-201-created"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            calls-to-non-200 6
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go
                                  (if (= calls-to-non-200 @make-request-counter)
                                    {:body (async/chan 1), :status 201, :headers {"location" "/result"}}
                                    {:body (async/chan 1), :status 200})))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (= calls-to-non-200 @make-request-counter))
        (is (= [:success] @release-status-atom))
        (is (= :unknown-status-code monitor-result))))

    (testing "status-check-eventually-303-see-other-repeated"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            calls-to-non-200 6
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go
                                  (if (> calls-to-non-200 @make-request-counter)
                                    {:body (async/chan 1), :status 303, :headers {"location" "/result"}}
                                    {:body (async/chan 1), :status 200})))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (> @make-request-counter calls-to-non-200))
        (is (= [:success] @release-status-atom))
        (is (= :monitor-timed-out monitor-result))))

    (testing "status-check-eventually-303-relative-url-in-location"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            calls-to-non-200 6
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go
                                  (if (= calls-to-non-200 @make-request-counter)
                                    {:body (async/chan 1), :status 303, :headers {"location" "../result"}}
                                    {:body (async/chan 1), :status 200})))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (> @make-request-counter calls-to-non-200))
        (is (= [:success] @release-status-atom))
        (is (= :monitor-timed-out monitor-result))))

    (testing "status-check-eventually-303-absolute-url-in-location"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            calls-to-non-200 6
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go
                                  (if (= calls-to-non-200 @make-request-counter)
                                    {:body (async/chan 1), :status 303, :headers {"location" "http://www.example.com/result"}}
                                    {:body (async/chan 1), :status 200})))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (= @make-request-counter calls-to-non-200))
        (is (= [:success] @release-status-atom))
        (is (= :status-see-other monitor-result))))

    (testing "status-check-eventually-303-see-other-evetually-410-gone"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            calls-to-non-200 6
            calls-to-non-200-and-non-303 10
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go
                                  (if (> calls-to-non-200 @make-request-counter)
                                    {:body (async/chan 1), :status 200}
                                    (if (> calls-to-non-200-and-non-303 @make-request-counter)
                                      {:body (async/chan 1), :status 303, :headers {"location" "/result"}}
                                      {:body (async/chan 1), :status 410}))))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (= @make-request-counter calls-to-non-200-and-non-303))
        (is (= [:success] @release-status-atom))
        (is (= :status-gone monitor-result))))

    (testing "status-check-eventually-410-gone"
      (let [release-status-atom (atom [])
            complete-async-request (fn [status] (swap! release-status-atom conj status))
            calls-to-non-200 6
            make-request-counter (atom 0)
            make-http-request (fn []
                                (swap! make-request-counter inc)
                                (async/go
                                  (if (= calls-to-non-200 @make-request-counter)
                                    {:body (async/chan 1), :status 410}
                                    {:body (async/chan 1), :status 200})))
            request-still-active? (constantly true)
            exit-chan (async/chan 1)
            response-chan (monitor-async-request make-http-request complete-async-request request-still-active? status-endpoint
                                                 check-interval-ms request-timeout-ms correlation-id exit-chan)
            monitor-result (async/<!! response-chan)]
        (is (= calls-to-non-200 @make-request-counter))
        (is (= [:success] @release-status-atom))
        (is (= :status-gone monitor-result))))))

(deftest test-complete-async-request-locally
  (testing "valid-request-id"
    (let [request-id "test-req-0"
          initial-state {request-id :pending, "req-1" :pending, "req-2" :pending}
          async-request-store-atom (atom initial-state)
          release-status-atom (atom [])
          release-instance-fn (fn [status] (swap! release-status-atom conj status))]
      (complete-async-request-locally async-request-store-atom release-instance-fn request-id :instance-error)
      (is (= [:instance-error] @release-status-atom))
      (is (= (dissoc initial-state request-id) @async-request-store-atom))))

  (testing "invalid-request-id"
    (let [request-id "test-req-0"
          initial-state {"req-0" :pending, "req-1" :pending, "req-2" :pending}
          async-request-store-atom (atom initial-state)
          release-instance-fn (fn [_] (throw (Exception. "Unexpected call!")))]
      (complete-async-request-locally async-request-store-atom release-instance-fn request-id :instance-error)
      (is (= initial-state @async-request-store-atom)))))

(deftest test-terminate-request
  (testing "valid-request-id"
    (let [request-id "test-req-0"
          exit-chan (async/chan 1)
          initial-state {request-id {:exit-chan exit-chan}, "req-1" :pending, "req-2" :pending}
          async-request-store-atom (atom initial-state)]
      (async-request-terminate async-request-store-atom request-id)
      (is (= :exit (async/<!! exit-chan)))
      (is (= initial-state @async-request-store-atom))))

  (testing "invalid-request-id"
    (let [request-id "test-req-0"
          exit-chan (async/chan 1)
          initial-state {"req-0" :pending, "req-1" :pending, "req-2" :pending}
          async-request-store-atom (atom initial-state)]
      (async-request-terminate async-request-store-atom request-id)
      (async/>!! exit-chan :test-value)
      (is (= :test-value (async/<!! exit-chan)))
      (is (= initial-state @async-request-store-atom)))))

(deftest test-trigger-terminate
  (let [local-router-id "local-router-id"
        remote-router-id "remote-router-id"
        service-id "test-service-id"
        request-id "req-123456"
        terminate-call-atom (atom false)
        async-request-terminate-fn (fn [in-request-id]
                                     (reset! terminate-call-atom "local")
                                     (is (= request-id in-request-id)))
        make-inter-router-requests-fn (fn [endpoint fn-key acceptable-router? method-key method-val]
                                        (reset! terminate-call-atom "remote")
                                        (is (= (str "waiter-async/complete/" request-id "/" service-id) endpoint))
                                        (is (= :acceptable-router? fn-key))
                                        (is (= :method method-key))
                                        (is (= :get method-val))
                                        (is (acceptable-router? remote-router-id)))]

    (testing "local-trigger-terminate"
      (reset! terminate-call-atom "")
      (async-trigger-terminate async-request-terminate-fn make-inter-router-requests-fn local-router-id local-router-id service-id request-id)
      (is (= "local" @terminate-call-atom)))

    (testing "remote-trigger-terminate"
      (reset! terminate-call-atom "")
      (async-trigger-terminate async-request-terminate-fn make-inter-router-requests-fn local-router-id remote-router-id service-id request-id)
      (is (= "remote" @terminate-call-atom)))))

(deftest test-post-process-async-request-response
  (let [{:keys [host port] :as instance} {:host "www.example.com", :port 1234, :protocol "proto"}
        router-id "my-router-id"
        service-id "test-service-id"
        metric-group "test-metric-group"
        async-request-store-atom (atom {})
        request-id "request-2394613984619"
        reason-map {:request-id request-id}
        request-properties {:async-check-interval-ms 100, :async-request-timeout-ms 200}
        response {}
        location (str "/location/" request-id)
        make-http-request-fn (fn [in-instance in-request end-route metric-group]
                               (is (= instance in-instance))
                               (is (= {:body nil, :headers {}, :request-method :get} in-request))
                               (is (= "/location/request-2394613984619" end-route))
                               (is (= "test-metric-group" metric-group)))
        instance-rpc-chan (async/chan 1)
        complete-async-request-atom (atom nil)]
    (with-redefs [service/release-instance-go (constantly nil)
                  monitor-async-request
                  (fn [make-get-request-fn complete-async-request-fn request-still-active? _
                       async-check-interval-ms async-request-timeout-ms correlation-id exit-chan]
                    (is (request-still-active?))
                    (is (= 100 async-check-interval-ms))
                    (is (= 200 async-request-timeout-ms))
                    (is correlation-id)
                    (is exit-chan)
                    (make-get-request-fn)
                    (reset! complete-async-request-atom complete-async-request-fn))]
      (let [{:keys [headers]} (post-process-async-request-response
                                router-id async-request-store-atom make-http-request-fn
                                instance-rpc-chan response service-id metric-group instance
                                reason-map request-properties location)]
        (is (= (str "/waiter-async/status/" request-id "/" router-id "/" service-id "/" host "/" port location)
               (get headers "location"))))
      (is (get @async-request-store-atom request-id))
      (let [complete-async-request-fn @complete-async-request-atom]
        (is complete-async-request-fn)
        (complete-async-request-fn :success)
        (is (nil? (get @async-request-store-atom request-id)))))))

(deftest test-route-params-and-uri-generation
  (let [uri->route-params (fn [prefix uri]
                            (when (str/starts-with? (str uri) prefix)
                              (let [route-uri (subs (str uri) (count prefix))
                                    [request-id router-id service-id host port & remaining] (str/split (str route-uri) #"/")
                                    decode #(URLDecoder/decode %1 "UTF-8")]
                                {:host (when (not (str/blank? host)) host)
                                 :location (when (seq remaining) (str "/" (str/join "/" remaining)))
                                 :port (when (not (str/blank? port)) port)
                                 :request-id (when (not (str/blank? request-id)) (decode request-id))
                                 :router-id (when (not (str/blank? router-id)) (decode router-id))
                                 :service-id (when (not (str/blank? service-id)) service-id)})))
        execute-test (fn [params]
                       (let [prefix "/my-test-prefix/"
                             uri (route-params->uri prefix params)
                             decoded-params (uri->route-params prefix uri)]
                         (is (str/starts-with? uri prefix))
                         (is (every? (fn [v] (if v (str/includes? uri (str v)) true)) (vals params)))
                         (is (= (merge {:host nil, :location nil, :port nil, :request-id nil, :router-id nil, :service-id nil}
                                       (pc/map-vals #(if (integer? %1) (str %1) %1) params))
                                decoded-params))))]
    (testing "empty-params" (execute-test {}))
    (testing "only-host" (execute-test {:host "105.123.025.36"}))
    (testing "only-location" (execute-test {:location "/status-location"}))
    (testing "only-port" (execute-test {:port 3254}))
    (testing "only-request-id" (execute-test {:request-id "6546540.6406460"}))
    (testing "only-router-id" (execute-test {:router-id "6546540.6406460"}))
    (testing "only-service-id" (execute-test {:service-id "test-service-id"}))
    (testing "all-params" (execute-test {:host "105.123.025.36"
                                         :location "/status-location"
                                         :port 3254
                                         :request-id "6546540.6406460"
                                         :router-id "6546540.6406460"
                                         :service-id "test-service-id"}))))

(deftest test-normalize-location-header
  (is (= ""
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "")))
  (is (= "/path/to/status/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "http://www.example.com:1234/path/to/status/result")))
  (is (= "/path/to/status/result?a=b&c=d"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "http://www.example.com:1234/path/to/status/result?a=b&c=d")))
  (is (= "http://www.test.com:1234/path/to/status/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "http://www.test.com:1234/path/to/status/result")))
  (is (= "http://www.test.com:1234/path/to/status/result?a=b&c=d"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "http://www.test.com:1234/path/to/status/result?a=b&c=d")))
  (is (= "http://www.example.com:3456/path/to/status/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "http://www.example.com:3456/path/to/status/result")))
  (is (= "http://www.example.com/path/to/status/result?a=b&c=d"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "http://www.example.com/path/to/status/result?a=b&c=d")))
  (is (= "http://www.example.com/path/to/status/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html"
                                    "http://www.example.com/path/to/status/result")))
  (is (= "/path/to/status/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html" "result")))
  (is (= "/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html" "/result")))
  (is (= "/path/to/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html" "../result")))
  (is (= "/path/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html" "../../result")))
  (is (= "/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html" "../../../result")))
  (is (= "/path/to/retrieve/result"
         (normalize-location-header "http://www.example.com:1234/path/to/status/1234.html" "../retrieve/result"))))
