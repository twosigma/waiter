(ns waiter.saml-authentication-test
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]
            [clojure.java.shell :as shell])
  (:import (java.net URLEncoder)))

(deftest ^:parallel ^:integration-fast test-default-composite-authenticator
  (testing-using-waiter-url
    (when (using-shell? waiter-url)
      (let [run-as-user (System/getenv "WAITER_AUTH_RUN_AS_USER")
            _ (is (not (string/blank? run-as-user)) "You must provide the :one-user authenticator login in the WAITER_AUTH_RUN_AS_USER environment variable")
            {:keys [body headers]} (make-kitchen-request waiter-url {} :path "/request-info")
            body-json (json/read-str (str body))]
        (is (= run-as-user (get-in body-json ["headers" "x-waiter-auth-principal"])))))))

(defn- xpath-query
  "run xpath query on an html file"
  [html-file xpath-query]
  (string/trim-newline (:out (shell/sh "bash" "-c" (str "cat " html-file
                                                        " | python -c \"import sys, requests;\\\n"
                                                        "from lxml import html;\\\n"
                                                        "tree = html.fromstring(sys.stdin.read());\\\n"
                                                        "print tree.xpath('" xpath-query "')\"")))))

(defn- perform-saml-authentication
  "Default implementation of performing authentication wtih an identity provider service. Return map of saml-response and relay-state"
  [saml-redirect-location waiter-url]
  (let [cookie-jar-file (java.io.File/createTempFile "cookie-jar" ".txt")
        cookie-jar-path (.getAbsolutePath cookie-jar-file)
        curl-output-file (java.io.File/createTempFile "curl-output" ".txt")
        curl-output-path (.getAbsolutePath curl-output-file)
        curl-verbose-info (:err (shell/sh "bash" "-c" (str "curl '" saml-redirect-location "' -k -c " cookie-jar-path " -L -v > " curl-output-path)))
        login-form-location (second (re-matches #"(?ms).*Location: ([^?]+).*" curl-verbose-info))
        login-form-action (xpath-query curl-output-path "string(//*/form/@action)")
        auth-state (xpath-query curl-output-path "string(//*/form/input[@name=\\\"AuthState\\\"]/@value)")
        _ (is (= 0 (:exit (shell/sh "bash" "-c" (str "curl '" (str login-form-location login-form-action) "' -k -b " cookie-jar-path " -F 'AuthState=" auth-state "' -F 'username=user2' -F 'password=user2pass' > " curl-output-path)))))
        waiter-saml-acs-endpoint (xpath-query curl-output-path "string(//*/form/@action)")
        saml-response (xpath-query curl-output-path "string(//*/form/input[@name=\\\"SAMLResponse\\\"]/@value)")
        relay-state (xpath-query curl-output-path "string(//*/form/input[@name=\\\"RelayState\\\"]/@value)")
        _ (is (= (str "http://" waiter-url "/request-info") relay-state))
        _ (.delete curl-output-file)
        _ (.delete cookie-jar-file)]
    {:relay-state relay-state :saml-response saml-response :waiter-saml-acs-endpoint waiter-saml-acs-endpoint}))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-saml-authentication
  (testing-using-waiter-url
    (when (using-shell? waiter-url)
      (let [auth-principal (or (System/getenv "SAML_AUTH_USER") "user2")
            token (rand-name)
            {:keys [status]} (post-token waiter-url (-> (kitchen-params)
                                                        (assoc
                                                          :authentication "saml"
                                                          :name token
                                                          :permitted-user "*"
                                                          :run-as-user (retrieve-username)
                                                          :token token)))
            _ (is (= 200 status))
            {:keys [headers status]} (make-request-with-debug-info {:x-waiter-token token} #(make-request waiter-url "/request-info" :headers %))
            _ (is (= 302 status))
            saml-redirect-location (get headers "location")
            {:keys [relay-state saml-response waiter-saml-acs-endpoint]} (perform-saml-authentication saml-redirect-location waiter-url)
            _ (is (= (str "http://" waiter-url "/request-info") relay-state))
            curl-output-file (java.io.File/createTempFile "curl-output" ".txt")
            curl-output-path (.getAbsolutePath curl-output-file)
            _ (is (= 0 (:exit (shell/sh "bash" "-c" (str "curl '" waiter-saml-acs-endpoint "' -k -d 'SAMLResponse=" (URLEncoder/encode saml-response) "&RelayState=" (URLEncoder/encode relay-state) "' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Expect:' > " curl-output-path)))))
            waiter-saml-auth-redirect-endpoint (xpath-query curl-output-path "string(//*/form/@action)")
            saml-auth-data (xpath-query curl-output-path "string(//*/form/input[@name=\\\"saml-auth-data\\\"]/@value)")
            _ (.delete curl-output-file)
            port (:port (waiter-settings waiter-url))
            _ (is (= (str "https://" (string/replace waiter-url (str ":" port) "") "/waiter-auth/saml/auth-redirect") waiter-saml-auth-redirect-endpoint))
            {:keys [cookies headers status]} (make-request-with-debug-info
                                       {}
                                       #(make-request waiter-url "/waiter-auth/saml/auth-redirect"
                                                      :method :post
                                                      :headers (assoc % "Content-Type" "application/x-www-form-urlencoded")
                                                      :body (str "saml-auth-data=" (URLEncoder/encode saml-auth-data))))
            _ (is (= 303 status))
            _ (is (= relay-state (get headers "location")))
            {:keys [body status service-id]} (make-request-with-debug-info
                                               {:x-waiter-token token}
                                               #(make-request waiter-url "/request-info" :headers % :cookies cookies))
            _ (is (= 200 status))

            ;; don't know how to do the curl commands with qbits.jet.client:
            ;
            ;response (async/<!! (qbits.jet.client.http/request http1-client {:follow-redirects? false :url (get headers "location")}))
            ;_ (println (qbits.jet.client.cookies/get-cookies (.getCookieStore http1-client)))
            ;_ (println (get (:headers response) "location"))
            ;body (async/<!! (:body response))
            ;_ (println body)
            ;response (async/<!! (qbits.jet.client.http/request http1-client {:follow-redirects? false :url (get (:headers response) "location")}))
            ;_ (println (qbits.jet.client.cookies/get-cookies (.getCookieStore http1-client)))
            ;_ (println (get (:headers response) "location"))
            ;body (async/<!! (:body response))
            ;_ (println body)
            ;response (async/<!! (qbits.jet.client.http/request http1-client {:follow-redirects? false :url (get (:headers response) "location")}))
            ;_ (println (qbits.jet.client.cookies/get-cookies (.getCookieStore http1-client)))
            ;_ (println (get (:headers response) "location"))
            ;body (async/<!! (:body response))
            ;_ (println body)
            ;;{:keys [body headers]} (make-kitchen-request waiter-url {:x-waiter-authentication "saml"} :path "/request-info")
            ;;_ (println body)
            ;;_ (println headers)

            body-json (json/read-str (str body))]
        (with-service-cleanup
          service-id
          (is (= auth-principal (get-in body-json ["headers" "x-waiter-auth-principal"]))))))))

