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
(ns waiter.websocket-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [qbits.jet.client.websocket :as ws-client]
            [waiter.auth.authentication :as auth]
            [waiter.cookie-support :as cs]
            [waiter.correlation-id :as cid]
            [waiter.test-helpers]
            [waiter.websocket :refer :all])
  (:import (java.net HttpCookie SocketTimeoutException URLDecoder)
           (java.util ArrayList Collection)
           (org.eclipse.jetty.websocket.api MessageTooLargeException UpgradeRequest)
           (org.eclipse.jetty.websocket.client ClientUpgradeRequest)
           (org.eclipse.jetty.websocket.servlet ServletUpgradeResponse)))

(defn- reified-upgrade-request
  [config-map]
  (let [cookies-coll (map (fn [[name value]] (HttpCookie. name value)) (:cookies config-map))
        cookies-arraylist (ArrayList. ^Collection (or (seq cookies-coll) []))]
    (reify UpgradeRequest
      (getCookies [_] cookies-arraylist)
      (getHeaders [_ name]
        (let [header-value (get-in config-map [:headers name])
              result-list (ArrayList.)]
          (if (string? header-value)
            (.add result-list header-value)
            (doseq [value header-value]
              (.add result-list value)))
          result-list)))))

(defn- reified-upgrade-response
  []
  (let [response-reason-atom (atom nil)
        response-status-atom (atom 0)
        response-subprotocol-atom (atom nil)]
    (proxy [ServletUpgradeResponse] [nil]
      (getAcceptedSubProtocol [] @response-subprotocol-atom)
      (getStatusCode [] @response-status-atom)
      (getStatusReason [] @response-reason-atom)
      (sendError [status reason]
        (reset! response-status-atom status)
        (reset! response-reason-atom reason))
      (sendForbidden [reason]
        (reset! response-status-atom 403)
        (reset! response-reason-atom reason))
      (setAcceptedSubProtocol [subprotocol] (reset! response-subprotocol-atom subprotocol)))))

(deftest test-request-authenticator
  (let [password (Object.)
        cookie-value "test-cookie-string"
        auth-user "test-user"
        auth-principal (str auth-user "@test.com")
        auth-time (System/currentTimeMillis)]
    (testing "successful-auth"
      (let [request (reified-upgrade-request {:cookies {"x-waiter-auth" cookie-value}})
            response (reified-upgrade-response)]
        (with-redefs [auth/get-auth-cookie-value identity
                      auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                [auth-principal auth-time])]
          (is (request-authenticator password request response))
          (is (zero? (.getStatusCode response))))))

    (testing "successful-auth:multiple-cookies"
      (let [request (reified-upgrade-request {:cookies {"fee" "fie", "foo" "bar", "x-waiter-auth" cookie-value}})
            response (reified-upgrade-response)]
        (with-redefs [auth/get-auth-cookie-value identity
                      auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                [auth-principal auth-time])]
          (is (request-authenticator password request response))
          (is (zero? (.getStatusCode response))))))

    (testing "unsuccessful-auth:auth-header-absent"
      (let [request (reified-upgrade-request {:cookies {"fee" "fie", "foo" "bar"}})
            response (reified-upgrade-response)]
        (with-redefs [auth/get-auth-cookie-value identity
                      auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                nil)]
          (is (not (request-authenticator password request response)))
          (is (= 403 (.getStatusCode response)))
          (is (= "Unauthorized" (.getStatusReason response))))))

    (testing "unsuccessful-auth:auth-header-present"
      (let [request (reified-upgrade-request {:cookies {"x-waiter-auth" cookie-value}})
            response (reified-upgrade-response)]
        (with-redefs [auth/get-auth-cookie-value identity
                      auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                nil)]
          (is (not (request-authenticator password request response)))
          (is (= 403 (.getStatusCode response)))
          (is (= "Unauthorized" (.getStatusReason response))))))

    (testing "unsuccessful-auth:exception"
      (let [request (reified-upgrade-request {:cookies {"x-waiter-auth" cookie-value}})
            response (reified-upgrade-response)]
        (with-redefs [auth/get-auth-cookie-value identity
                      auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                (throw (Exception. "Test-Exception")))]
          (is (not (request-authenticator password request response)))
          (is (= 500 (.getStatusCode response)))
          (is (= "Test-Exception" (.getStatusReason response))))))))

(deftest test-request-subprotocol-acceptor
  (let []
    (testing "sec-websocket-protocol:missing"
      (let [request (reified-upgrade-request {:headers {}})
            response (reified-upgrade-response)]
        (is (request-subprotocol-acceptor request response))
        (is (zero? (.getStatusCode response)))))

    (testing "sec-websocket-protocol:single"
      (let [request (reified-upgrade-request {:headers {"sec-websocket-protocol" "foo"}})
            response (reified-upgrade-response)]
        (is (request-subprotocol-acceptor request response))
        (is (zero? (.getStatusCode response)))))

    (testing "sec-websocket-protocol:multiple"
      (let [request (reified-upgrade-request {:headers {"sec-websocket-protocol" ["foo" "bar"]}})
            response (reified-upgrade-response)]
        (is (not (request-subprotocol-acceptor request response)))
        (is (= 500 (.getStatusCode response)))
        (is (= "waiter does not yet support multiple subprotocols in websocket requests: [\"foo\" \"bar\"]"
               (.getStatusReason response)))))

    (testing "sec-websocket-protocol:exception"
      (let [request (reified-upgrade-request {:headers {"sec-websocket-protocol" (Object.)}})
            response (reified-upgrade-response)]
        (is (not (request-subprotocol-acceptor request response)))
        (is (= 500 (.getStatusCode response)))
        (is (= "Don't know how to create ISeq from: java.lang.Object"
               (.getStatusReason response)))))))

(deftest test-inter-router-request-middleware
  (let [router-id "test-router-id"
        password [:cached "test-password"]
        cookie-juc-list (ArrayList.)
        request (reify UpgradeRequest
                  (getCookies [_] cookie-juc-list))]
    (inter-router-request-middleware router-id password request)
    (let [cookie-list (seq cookie-juc-list)
          auth-cookie-value (some (fn auth-filter [^HttpCookie cookie]
                                    (when (= auth/AUTH-COOKIE-NAME (.getName cookie))
                                      (.getValue cookie)))
                                  cookie-list)
          decoded-auth-value (cs/decode-cookie (URLDecoder/decode auth-cookie-value) password)]
      (is (= 1 (count cookie-list)))
      (is auth-cookie-value)
      (is (= 2 (count decoded-auth-value)))
      (is (= (str router-id "@waiter-peer-router") (first decoded-auth-value))))))

(deftest test-request-handler
  (let [password (Object.)
        cookie-value "test-cookie-string"
        headers {"cookie" cookie-value}
        auth-user "test-user"
        auth-principal (str auth-user "@test.com")
        auth-time (System/currentTimeMillis)]

    (testing "successful-auth"
      (let [output-channel (async/promise-chan)
            request {:headers headers, :out output-channel}
            process-request-atom (atom false)
            process-request-fn (fn process-request-fn [in-request]
                                 (is (= in-request
                                        (assoc request :authorization/principal auth-principal
                                                       :authorization/time auth-time
                                                       :authorization/user auth-user)))
                                 (reset! process-request-atom true)
                                 {})]
        (with-redefs [auth/get-auth-cookie-value identity
                      auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                [auth-principal auth-time])]
          (request-handler password process-request-fn request))
        (is @process-request-atom)))))

(deftest test-abort-request-callback-factory
  (let [in (async/promise-chan)
        out (async/promise-chan)
        response {:request {:in in, :out out}}
        abort-request-callback (abort-request-callback-factory response)]
    (abort-request-callback (Exception.))
    ;; channels should be closed
    (is (not (async/>!! in :in-data)))
    (is (not (async/>!! out :out-data)))))

(deftest test-make-request
  (let [instance {:host "www.host.com" :port 1234 :service-id "test-service-id"}
        request {}
        request-properties {:async-request-timeout-ms 1
                            :connection-timeout-ms 2
                            :initial-socket-timeout-ms 3}
        test-cid "239874623hk2er7908245"
        passthrough-headers {"accept" "text/html"
                             "accept-charset" "ISO-8859-1,utf-8;q=0.7,*;q=0.7"
                             "accept-language" "en-us"
                             "accept-encoding" "gzip,deflate"
                             "authorization" "test-user-authorization"
                             "cache-control" "no-cache"
                             "connection" "keep-alive"
                             "content-length" "12341234"
                             "content-MD5" "Q2hlY2sgSW50ZWdyaXR5IQ=="
                             "content-type" "application/x-www-form-urlencoded"
                             "cookie" "$Version=1; Skin=new;"
                             "date" "Tue, 15 Nov 2015 08:12:31 GMT"
                             "expect" "100-continue"
                             "expires" "2200-08-09"
                             "forwarded" "for=192.0.2.43, for=198.51.100.17"
                             "from" "user@example.com"
                             "host" "www.test-source.com"
                             "if-match" "737060cd8c284d8af7ad3082f209582d"
                             "if-modified-since" "Sat, 29 Oct 2015 19:43:31 GMT"
                             "keep-alive" "300"
                             "origin" "http://example.example.com"
                             "pragma" "no-cache"
                             "proxy-authenticate" "proxy-authenticate value"
                             "proxy-authorization" "proxy-authorization value"
                             "referer" "http://www.test-referer.com"
                             "sec-websocket-accept" "lorem-ipsum"
                             "sec-websocket-extensions" "deflate"
                             "sec-websocket-key" "do-re-mi"
                             "sec-websocket-protocol" "sec-websocket-accept"
                             "sec-websocket-version" "13"
                             "te" "trailers, deflate"
                             "trailers" "trailer-name-1, trailer-name-2"
                             "transfer-encoding" "trailers, deflate"
                             "upgrade" "HTTP/2.0, HTTPS/1.3, IRC/6.9, RTA/x11, websocket"
                             "user-agent" "Test/App"
                             "x-cid" test-cid
                             "x-forwarded-for" "client1, proxy1, proxy2"
                             "x-http-method-override" "DELETE"}
        assertion-headers (dissoc passthrough-headers
                                  "content-length" "expect" "authorization"
                                  "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                                  "te" "trailers" "transfer-encoding" "upgrade"
                                  "cache-control" "cookie" "connection" "host" "pragma"
                                  "sec-websocket-accept" "sec-websocket-extensions" "sec-websocket-key"
                                  "sec-websocket-protocol" "sec-websocket-version" "upgrade")
        end-route "/end-route"
        service-id->password-fn (fn service-id->password-fn [service-id]
                                  (is (= (:service-id instance) service-id))
                                  "password")
        connect-request (Object.)
        assert-request-headers (fn assert-request-headers [upgrade-request]
                                 (doseq [[header-name header-value] assertion-headers]
                                   (is (= header-value (.getHeader upgrade-request header-name)) header-name)))
        proto-version "HTTP/1.1"
        request-control-chan (async/promise-chan)]

    (testing "successful-connect-ws"
      (with-redefs [ws-client/connect! (fn [_ instance-endpoint request-callback {:keys [middleware] :as request-properties}]
                                         (is (= (str "ws://www.host.com:1234" end-route) instance-endpoint))
                                         (is (= {:async-write-timeout 1, :connect-timeout 2, :max-idle-timeout 3}
                                                (select-keys request-properties [:async-write-timeout :connect-timeout :max-idle-timeout])))
                                         (is middleware)
                                         (let [upgrade-request (ClientUpgradeRequest.)]
                                           (middleware nil upgrade-request)
                                           (assert-request-headers upgrade-request))
                                         (request-callback connect-request))]
        (cid/with-correlation-id
          test-cid
          (let [websocket-client nil
                response (make-request websocket-client service-id->password-fn instance request request-properties
                                       passthrough-headers end-route nil "http" proto-version request-control-chan)
                response-map (async/<!! response)]
            (is (= #{:ctrl-mult :request} (-> response-map keys set)))
            (is (= connect-request (:request response-map)))))))

    (testing "successful-connect-wss"
      (with-redefs [ws-client/connect! (fn [_ instance-endpoint request-callback {:keys [middleware] :as request-properties}]
                                         (is (= (str "wss://www.host.com:1234" end-route) instance-endpoint))
                                         (is (= {:async-write-timeout 1, :connect-timeout 2, :max-idle-timeout 3}
                                                (select-keys request-properties [:async-write-timeout :connect-timeout :max-idle-timeout])))
                                         (is middleware)
                                         (let [upgrade-request (ClientUpgradeRequest.)]
                                           (middleware nil upgrade-request)
                                           (assert-request-headers upgrade-request))
                                         (request-callback connect-request))]
        (cid/with-correlation-id
          test-cid
          (let [websocket-client nil
                response (make-request websocket-client service-id->password-fn instance request request-properties
                                       passthrough-headers end-route nil "https" proto-version request-control-chan)
                response-map (async/<!! response)]
            (is (= #{:ctrl-mult :request} (-> response-map keys set)))
            (is (= connect-request (:request response-map)))))))

    (testing "unsuccessful-connect"
      (let [test-exception (Exception. "Thrown-from-test")]
        (with-redefs [ws-client/connect! (fn [_ instance-endpoint request-callback {:keys [middleware] :as request-properties}]
                                           (is (= (str "ws://www.host.com:1234" end-route) instance-endpoint))
                                           (is (= {:async-write-timeout 1, :connect-timeout 2, :max-idle-timeout 3}
                                                  (select-keys request-properties [:async-write-timeout :connect-timeout :max-idle-timeout])))
                                           (is middleware)
                                           (is request-callback)
                                           (throw test-exception))]
          (let [websocket-client nil
                response (make-request websocket-client service-id->password-fn instance request request-properties
                                       passthrough-headers end-route nil "http" proto-version request-control-chan)]
            (is (= {:error test-exception} (async/<!! response)))))))))

(deftest test-watch-ctrl-chan
  (letfn [(ensure-test-timeout-fn [request-close-promise-chan]
            (async/go
              (async/<! (async/timeout 5000))
              (async/>! request-close-promise-chan :test-timed-out)))]
    (testing "success"
      (let [request-name :test
            ctrl-chan (async/chan 1)
            ctrl-mult (async/mult ctrl-chan)
            reservation-status-promise (promise)
            request-close-promise-chan (async/promise-chan)
            status-callback-atom (atom nil)
            on-close-callback #(reset! status-callback-atom %1)]
        (watch-ctrl-chan request-name ctrl-mult reservation-status-promise request-close-promise-chan on-close-callback)
        (async/>!! ctrl-chan [:qbits.jet.websocket/close 1000])
        (ensure-test-timeout-fn request-close-promise-chan)
        (is (= [:test :success 1000 nil] (async/<!! request-close-promise-chan)))
        (is (not (realized? reservation-status-promise)))
        (is (= 1000 @status-callback-atom))))

    (testing "unknown-error"
      (let [request-name :test
            ctrl-chan (async/chan 1)
            ctrl-mult (async/mult ctrl-chan)
            reservation-status-promise (promise)
            request-close-promise-chan (async/promise-chan)
            status-callback-atom (atom nil)
            on-close-callback #(reset! status-callback-atom %1)]
        (watch-ctrl-chan request-name ctrl-mult reservation-status-promise request-close-promise-chan on-close-callback)
        (async/>!! ctrl-chan [:unknown-code 4444])
        (ensure-test-timeout-fn request-close-promise-chan)
        (is (= [:test :unknown 4444 nil] (async/<!! request-close-promise-chan)))
        (is (not (realized? reservation-status-promise)))
        (is (= 4444 @status-callback-atom))))

    (testing "timeout-error"
      (let [request-name :test
            ctrl-chan (async/chan 1)
            ctrl-mult (async/mult ctrl-chan)
            reservation-status-promise (promise)
            request-close-promise-chan (async/promise-chan)
            status-callback-atom (atom nil)
            on-close-callback #(reset! status-callback-atom %1)
            exception (SocketTimeoutException.)]
        (watch-ctrl-chan request-name ctrl-mult reservation-status-promise request-close-promise-chan on-close-callback)
        (async/>!! ctrl-chan [:qbits.jet.websocket/error exception])
        (ensure-test-timeout-fn request-close-promise-chan)
        (is (= [:test :socket-timeout exception nil] (async/<!! request-close-promise-chan)))
        (is (= :socket-timeout @reservation-status-promise))
        (is (= server-termination-on-unexpected-condition @status-callback-atom))))

    (testing "message-too-large-error"
      (let [request-name :test
            ctrl-chan (async/chan 1)
            ctrl-mult (async/mult ctrl-chan)
            reservation-status-promise (promise)
            request-close-promise-chan (async/promise-chan)
            status-callback-atom (atom nil)
            on-close-callback #(reset! status-callback-atom %1)
            exception (MessageTooLargeException. "from test-watch-ctrl-chan")]
        (watch-ctrl-chan request-name ctrl-mult reservation-status-promise request-close-promise-chan on-close-callback)
        (async/>!! ctrl-chan [:qbits.jet.websocket/error exception])
        (ensure-test-timeout-fn request-close-promise-chan)
        (is (= [:test :generic-error exception nil] (async/<!! request-close-promise-chan)))
        (is (= :generic-error @reservation-status-promise))
        (is (= server-termination-on-unexpected-condition @status-callback-atom))))

    (testing "generic-error"
      (let [request-name :test-name
            ctrl-chan (async/chan 1)
            ctrl-mult (async/mult ctrl-chan)
            reservation-status-promise (promise)
            request-close-promise-chan (async/promise-chan)
            status-callback-atom (atom nil)
            on-close-callback #(reset! status-callback-atom %1)
            exception (Exception.)]
        (watch-ctrl-chan request-name ctrl-mult reservation-status-promise request-close-promise-chan on-close-callback)
        (async/>!! ctrl-chan [:qbits.jet.websocket/error exception])
        (ensure-test-timeout-fn request-close-promise-chan)
        (is (= [:test-name :test-name-error exception nil] (async/<!! request-close-promise-chan)))
        (is (= :test-name-error @reservation-status-promise))
        (is (= server-termination-on-unexpected-condition @status-callback-atom))))))

(deftest test-wrap-ws-close-on-error
  (testing "error case"
    (let [out (async/chan 1)
          request {:out out}
          handler (wrap-ws-close-on-error (fn [_] {:status 500}))
          {:keys [status]} (handler request)]
      ;; response should indicate an error
      (is (= 500 status))
      ;; channels should be closed
      (is (not (async/>!! out :out-data)))))
  (testing "non-error case"
    (let [out (async/chan 1)
          request {:out out}
          handler (-> (fn [{:keys [out]}]
                        (async/go
                          (async/>! out :data)
                          {:status 200}))
                      wrap-ws-close-on-error)
          {:keys [status]} (async/<!! (handler request))]
      ;; response should indicate an internal server error
      (is (= 200 status))
      ;; channels should contain data and not be closed
      (is (= :data (async/<!! out))))))
