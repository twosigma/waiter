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
(ns waiter.websocket-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [qbits.jet.client.websocket :as ws-client]
            [waiter.auth.authentication :as auth]
            [waiter.cookie-support :as cs]
            [waiter.test-helpers]
            [waiter.websocket :refer :all])
  (:import (java.net HttpCookie SocketTimeoutException URLDecoder)
           (java.util ArrayList Collection)
           (org.eclipse.jetty.websocket.api UpgradeRequest)
           (org.eclipse.jetty.websocket.client ClientUpgradeRequest)
           (org.eclipse.jetty.websocket.servlet ServletUpgradeResponse)))

(defn- reified-upgrade-request
  [cookies-map]
  (let [cookies-coll (map (fn [[name value]] (HttpCookie. name value)) cookies-map)
        cookies-arraylist (ArrayList. ^Collection (seq cookies-coll))]
    (reify UpgradeRequest
      (getCookies [_] cookies-arraylist))))

(deftest test-request-authenticator
  (let [password (Object.)
        cookie-value "test-cookie-string"
        auth-user "test-user"
        auth-principal (str auth-user "@test.com")
        auth-time (System/currentTimeMillis)]
    (testing "successful-auth"
      (let [response-code-atom (atom nil)
            request (reified-upgrade-request {"x-waiter-auth" cookie-value})
            response (Object.)]
        (with-redefs [auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                [auth-principal auth-time])]
          (is (request-authenticator password request response))
          (is (nil? @response-code-atom)))))

    (testing "successful-auth:multiple-cookies"
      (let [response-code-atom (atom nil)
            request (reified-upgrade-request {"fee" "fie", "foo" "bar", "x-waiter-auth" cookie-value})
            response (Object.)]
        (with-redefs [auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                [auth-principal auth-time])]
          (is (request-authenticator password request response))
          (is (nil? @response-code-atom)))))

    (testing "unsuccessful-auth:auth-header-absent"
      (let [response-code-atom (atom nil)
            request (reified-upgrade-request {"fee" "fie", "foo" "bar"})
            response (proxy [ServletUpgradeResponse] [nil]
                       (sendForbidden [code] (reset! response-code-atom code)))]
        (with-redefs [auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                nil)]
          (is (not (request-authenticator password request response)))
          (is (= "Unauthorized" @response-code-atom)))))

    (testing "unsuccessful-auth:auth-header-present"
      (let [response-code-atom (atom nil)
            request (reified-upgrade-request {"x-waiter-auth" cookie-value})
            response (proxy [ServletUpgradeResponse] [nil]
                       (sendForbidden [code] (reset! response-code-atom code)))]
        (with-redefs [auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                nil)]
          (is (not (request-authenticator password request response)))
          (is (= "Unauthorized" @response-code-atom)))))

    (testing "unsuccessful-auth:exception"
      (let [response-code-atom (atom nil)
            request (reified-upgrade-request {"x-waiter-auth" cookie-value})
            response (proxy [ServletUpgradeResponse] [nil]
                       (sendError [status code] (reset! response-code-atom (str status ":" code))))]
        (with-redefs [auth/decode-auth-cookie (fn [in-cookie in-password]
                                                (is (= cookie-value in-cookie))
                                                (is (= password in-password))
                                                (throw (Exception. "Test-Exception")))]
          (is (not (request-authenticator password request response)))
          (is (= "500:Test-Exception" @response-code-atom)))))))

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
        cookies {auth/AUTH-COOKIE-NAME {:value cookie-value}}
        auth-user "test-user"
        auth-principal (str auth-user "@test.com")
        auth-time (System/currentTimeMillis)]

    (testing "successful-auth"
      (let [output-channel (async/promise-chan)
            request {:cookies cookies, :out output-channel}
            process-request-atom (atom false)
            process-request-fn (fn process-request-fn [in-request]
                                 (is (= in-request
                                        (assoc request :authorization/time auth-time
                                                       :authorization/user auth-user
                                                       :authenticated-principal auth-principal)))
                                 (reset! process-request-atom true))]
        (with-redefs [auth/decode-auth-cookie (fn [in-cookie in-password]
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
  (let [instance {:host "www.host.com", :port 1234}
        request {}
        request-properties {:async-request-timeout-ms 1
                            :connection-timeout-ms 2
                            :initial-socket-timeout-ms 3}
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
                             "x-cid" "239874623hk2er7908245"
                             "x-forwarded-for" "client1, proxy1, proxy2"
                             "x-http-method-override" "DELETE"}
        end-route "/end-route"
        password "password"
        connect-request (Object.)
        assert-request-headers (fn assert-request-headers [upgrade-request]
                                 (is (every? (fn [[header-name header-value]]
                                               (= header-value (.getHeader upgrade-request header-name)))
                                             (dissoc passthrough-headers
                                                     "content-length" "expect" "authorization"
                                                     "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                                                     "te" "trailers" "transfer-encoding" "upgrade"
                                                     "cache-control" "cookie" "connection" "host" "pragma"
                                                     "sec-websocket-accept" "sec-websocket-extensions" "sec-websocket-key"
                                                     "sec-websocket-protocol" "sec-websocket-version" "upgrade"))))]

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
        (let [websocket-client nil
              instance (assoc instance :protocol "http")
              response (make-request websocket-client instance request request-properties passthrough-headers end-route password nil)
              response-map (async/<!! response)]
          (is (= #{:ctrl-mult :request} (-> response-map keys set)))
          (is (= connect-request (-> response-map :request))))))

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
        (let [websocket-client nil
              instance (assoc instance :protocol "https")
              response (make-request websocket-client instance request request-properties passthrough-headers end-route password nil)
              response-map (async/<!! response)]
          (is (= #{:ctrl-mult :request} (-> response-map keys set)))
          (is (= connect-request (-> response-map :request))))))

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
                instance (assoc instance :protocol "http")
                response (make-request websocket-client instance request request-properties passthrough-headers end-route password nil)]
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
        (is (nil? @status-callback-atom))))

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
        (is (nil? @status-callback-atom))))))
