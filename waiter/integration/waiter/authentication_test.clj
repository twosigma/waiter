(ns waiter.authentication-test
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [reaver :as reaver]
            [waiter.util.client-tools :refer :all])
  (:import (java.net URLEncoder)))

(deftest ^:parallel ^:integration-fast test-default-composite-authenticator
  (testing-using-waiter-url
    (when (using-composite-authenticator? waiter-url)
      (let [{:keys [service-id body]} (make-request-with-debug-info {} #(make-kitchen-request waiter-url % :path "/request-info"))
            body-json (json/read-str (str body))]
        (with-service-cleanup
          service-id
          (is (= (retrieve-username) (get-in body-json ["headers" "x-waiter-auth-principal"]))))))))

(deftest ^:parallel ^:integration-fast test-token-authentication-parameter-error
  (testing-using-waiter-url
    (when (using-composite-authenticator? waiter-url)
      (let [authentication-providers (map name (keys (get-in (waiter-settings waiter-url)
                                                             [:authenticator-config :composite :authentication-providers])))
            error-message (str "authentication must be one of: '"
                               (string/join "', '" (sort (into #{"disabled" "standard"} authentication-providers)))
                               "'")]
        (let [token (rand-name)
              {:keys [status body]} (post-token waiter-url (assoc (kitchen-params)
                                                             :authentication "invalid"
                                                             :name token
                                                             :permitted-user "*"
                                                             :run-as-user (retrieve-username)
                                                             :token token))]
          (is (= 400 status))
          (is (string/includes? body error-message)))
        (let [token (rand-name)
              {:keys [status body]} (post-token waiter-url (assoc (kitchen-params)
                                                             :authentication ""
                                                             :name token
                                                             :permitted-user "*"
                                                             :run-as-user (retrieve-username)
                                                             :token token))]
          (is (= 400 status))
          (is (string/includes? body error-message)))))))

(defn- perform-saml-authentication
  "Default implementation of performing authentication wtih an identity provider service. Return map of waiter acs endpoint, saml-response and relay-state"
  [saml-redirect-location]
  (let [{:keys [headers cookies]} (make-request saml-redirect-location "")
        saml-redirect-location-2 (get headers "location")
        login-form-location (first (string/split saml-redirect-location-2 #"\?"))
        {:keys [cookies body]} (make-request saml-redirect-location-2 "" :cookies cookies)
        {:keys [login-form-action auth-state]}
        (reaver/extract (reaver/parse body) [:login-form-action :auth-state]
                        "form" (reaver/attr :action)
                        "form input[name=AuthState]" (reaver/attr :value))
        {:keys [body]} (make-request (str login-form-location login-form-action) ""
                                     :body (str "AuthState=" (URLEncoder/encode auth-state) "&username=user2&password=user2pass")
                                     :cookies cookies
                                     :headers {"Content-Type" "application/x-www-form-urlencoded"}
                                     :method :post)]
    (reaver/extract (reaver/parse body) [:waiter-saml-acs-endpoint :saml-response :relay-state]
                    "form" (reaver/attr :action)
                    "form input[name=SAMLResponse]" (reaver/attr :value)
                    "form input[name=RelayState]" (reaver/attr :value))))

(defn- perform-saml-authentication-kerberos
  "Implementation of performing authentication wtih an identity provider service using kerberos. Return map of waiter acs endpoint, saml-response and relay-state"
  [saml-redirect-location]
  (let [

        cookie-jar-file (java.io.File/createTempFile "cookie-jar" ".txt")
        cookie-jar-path (.getAbsolutePath cookie-jar-file)
        curl-output-file (java.io.File/createTempFile "curl-output" ".txt")
        curl-output-path (.getAbsolutePath curl-output-file)
        _ (is (= 0 (:exit (shell/sh "bash" "-c" (str "curl -u: --negotiate '" saml-redirect-location "' -c " cookie-jar-path " -L -v > " curl-output-path)))))
        rval (reaver/extract (reaver/parse (slurp curl-output-path)) [:waiter-saml-acs-endpoint :saml-response :relay-state]
                             "form" (reaver/attr :action)
                             "form input[name=SAMLResponse]" (reaver/attr :value)
                             "form input[name=RelayState]" (reaver/attr :value))
        _ (.delete curl-output-file)
        _ (.delete cookie-jar-file)


        ; make-request currently fails with EofException when using org.eclipse.jetty.io.ssl.SslConnection

        ;{:keys [body]} (make-request saml-redirect-location "")

        ]
    ;(extract (parse body) [:waiter-saml-acs-endpoint :saml-response :relay-state]
    ;         "form" (attr :action)
    ;         "form input[name=SAMLResponse]" (attr :value)
    ;         "form input[name=RelayState]" (attr :value))

    rval
    ))

(deftest ^:parallel ^:integration-fast test-saml-authentication
  (testing-using-waiter-url
    (when (using-composite-authenticator? waiter-url)
      (let [auth-principal (or (System/getenv "SAML_AUTH_USER") (retrieve-username))
            token (rand-name)
            {:keys [status]} (post-token waiter-url (-> (kitchen-params)
                                                      (assoc
                                                        :authentication "saml"
                                                        :name token
                                                        :permitted-user "*"
                                                        :run-as-user (retrieve-username)
                                                        :token token)))
            _ (is (= 200 status))
            {:keys [headers status]} (make-request waiter-url "/request-info" :headers {:x-waiter-token token})
            _ (is (= 302 status))
            saml-redirect-location (get headers "location")
            saml-authentication-fn (if use-spnego perform-saml-authentication-kerberos perform-saml-authentication)
            {:keys [relay-state saml-response waiter-saml-acs-endpoint]} (saml-authentication-fn saml-redirect-location)
            {:keys [body]} (make-request waiter-saml-acs-endpoint ""
                                         :body (str "SAMLResponse=" (URLEncoder/encode saml-response) "&RelayState=" (URLEncoder/encode relay-state))
                                         :headers {"Content-Type" "application/x-www-form-urlencoded"}
                                         :method :post)
            {:keys [waiter-saml-auth-redirect-endpoint saml-auth-data]}
            (reaver/extract (reaver/parse body) [:waiter-saml-auth-redirect-endpoint :saml-auth-data]
                            "form" (reaver/attr :action)
                            "form input[name=saml-auth-data]" (reaver/attr :value))
            _ (is (= (str "http://" waiter-url "/waiter-auth/saml/auth-redirect") waiter-saml-auth-redirect-endpoint))
            {:keys [cookies headers status]} (make-request waiter-url "/waiter-auth/saml/auth-redirect"
                                                           :body (str "saml-auth-data=" (URLEncoder/encode saml-auth-data))
                                                           :headers {"Content-Type" "application/x-www-form-urlencoded"}
                                                           :method :post)
            _ (is (= 303 status))
            _ (is (= (str "http://" waiter-url "/request-info") (get headers "location")))
            {:keys [body status service-id]} (make-request-with-debug-info
                                               {:x-waiter-token token}
                                               #(make-request waiter-url "/request-info" :headers % :cookies cookies))
            _ (is (= 200 status))
            body-json (json/read-str (str body))]
        (with-service-cleanup
          service-id
          (is (= auth-principal (get-in body-json ["headers" "x-waiter-auth-principal"]))))))))

