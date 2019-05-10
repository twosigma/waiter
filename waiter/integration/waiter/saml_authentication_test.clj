(ns waiter.saml-authentication-test
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]
            [waiter.util.http-utils :as http]
            [clojure.core.async :as async]))

(deftest ^:parallel ^:integration-fast test-default-dynamic-authenticator
  (testing-using-waiter-url
    (when (using-shell? waiter-url)
      (let [run-as-user (System/getenv "WAITER_AUTH_RUN_AS_USER")
            _ (is (not (string/blank? run-as-user)) "You must provide the :one-user authenticator login in the WAITER_AUTH_RUN_AS_USER environment variable")
            {:keys [body headers]} (make-kitchen-request waiter-url {} :path "/request-info")
            body-json (json/read-str (str body))]
        (is (= run-as-user (get-in body-json ["headers" "x-waiter-auth-principal"])))))))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-saml-authentication
  (testing-using-waiter-url
    (when (using-shell? waiter-url)
      (let [run-as-user (or (System/getenv "SAML_AUTH_USER") "user2")
            token (str (rand-name) ".localtest.me")
            {:keys [service-id request-headers body headers status]} (post-token waiter-url (-> (kitchen-params)
                                         (assoc
                                           :authentication "saml"
                                           :name token
                                           :permitted-user (retrieve-username)
                                           :run-as-user (retrieve-username)
                                           :token token)
                                         ))
            {:keys [service-id request-headers body headers status]} (make-request-with-debug-info {:x-waiter-token token} #(make-request waiter-url "/request-info" :headers %))
            _ (is (= 302 status))
            _ (println (.getCookies (.getCookieStore http1-client)))
            response (async/<!! (qbits.jet.client.http/request http1-client {:follow-redirects? false :url (get headers "location")}))
            _ (println (.getCookies (.getCookieStore http1-client)))
            _ (println (get (:headers response) "location"))
            body (async/<!! (:body response))
            _ (println body)
            response (async/<!! (qbits.jet.client.http/request http1-client {:follow-redirects? false :url (get (:headers response) "location")}))
            _ (println (.getCookies (.getCookieStore http1-client)))
            _ (println (get (:headers response) "location"))
            body (async/<!! (:body response))
            _ (println body)
            response (async/<!! (qbits.jet.client.http/request http1-client {:follow-redirects? false :url (get (:headers response) "location")}))
            _ (println (.getCookies (.getCookieStore http1-client)))
            _ (println (get (:headers response) "location"))
            body (async/<!! (:body response))
            _ (println body)
            ;{:keys [body headers]} (make-kitchen-request waiter-url {:x-waiter-authentication "saml"} :path "/request-info")
            ;_ (println body)
            ;_ (println headers)
            body-json (json/read-str (str body))]
        (with-service-cleanup
          service-id
          (is (= run-as-user (get-in body-json ["headers" "x-waiter-auth-principal"]))))))))


;(deftest ^:parallel ^:integration-fast test-kubernetes-watch-state-update
;  (testing-using-waiter-url
;    (when (using-k8s? waiter-url)
;      (let [cookies (all-cookies waiter-url)
;            router-url (-> waiter-url routers first val)
;            {:keys [body] :as response} (make-request router-url "/state/scheduler" :method :get :cookies cookies)
;            _ (assert-response-status response 200)
;            body-json (-> body str try-parse-json)
;            watch-state-json (get-watch-state body-json)
;            initial-pods-snapshot-version (get-in watch-state-json ["pods-metadata" "version" "snapshot"])
;            initial-pods-watch-version (get-in watch-state-json ["pods-metadata" "version" "watch"])
;            initial-rs-snapshot-version (get-in watch-state-json ["rs-metadata" "version" "snapshot"])
;            initial-rs-watch-version (get-in watch-state-json ["rs-metadata" "version" "watch"])
;            {:keys [service-id request-headers]} (make-request-with-debug-info
;                                                   {:x-waiter-name (rand-name)}
;                                                   #(make-kitchen-request waiter-url % :path "/hello"))]
;        (with-service-cleanup
;          service-id
;          (let [{:keys [body] :as response} (make-request router-url "/state/scheduler" :method :get :cookies cookies)
;                _ (assert-response-status response 200)
;                body-json (-> body str try-parse-json)
;                watch-state-json (get-watch-state body-json)
;                pods-snapshot-version' (get-in watch-state-json ["pods-metadata" "version" "snapshot"])
;                pods-watch-version' (get-in watch-state-json ["pods-metadata" "version" "watch"])
;                rs-snapshot-version' (get-in watch-state-json ["rs-metadata" "version" "snapshot"])
;                rs-watch-version' (get-in watch-state-json ["rs-metadata" "version" "watch"])]
;            (is (or (nil? initial-pods-watch-version)
;                    (< initial-pods-snapshot-version initial-pods-watch-version)))
;            (is (<= initial-pods-snapshot-version pods-snapshot-version'))
;            (is (< pods-snapshot-version' pods-watch-version'))
;            (is (or (nil? initial-rs-watch-version)
;                    (< initial-rs-snapshot-version initial-rs-watch-version)))
;            (is (<= initial-rs-snapshot-version rs-snapshot-version'))
;            (is (< rs-snapshot-version' rs-watch-version'))))))))