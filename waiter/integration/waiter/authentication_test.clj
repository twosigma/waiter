(ns waiter.authentication-test
  (:require [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [reaver :as reaver]
            [waiter.util.client-tools :refer :all])
  (:import (java.net URL URLEncoder)))

(deftest ^:parallel ^:integration-fast test-default-composite-authenticator
  (testing-using-waiter-url
    (when (using-composite-authenticator? waiter-url)
      (let [token (rand-name)
            response (post-token waiter-url (dissoc (assoc (kitchen-params)
                                                      :name token
                                                      :permitted-user "*"
                                                      :run-as-user (retrieve-username)
                                                      :token token)
                                                    :authentication))]
        (assert-response-status response 200)
        (let [{:keys [service-id body]} (make-request-with-debug-info
                                          {:x-waiter-token token}
                                          #(make-kitchen-request waiter-url % :path "/request-info"))
              body-json (json/read-str (str body))]
          (with-service-cleanup
            service-id
            (is (= (retrieve-username) (get-in body-json ["headers" "x-waiter-auth-principal"])))))))))

(deftest ^:parallel ^:integration-fast test-token-authentication-parameter-error
  (testing-using-waiter-url
    (when (using-composite-authenticator? waiter-url)
      (let [authentication-providers (map name (keys (get-in
                                                       (waiter-settings waiter-url)
                                                       [:authenticator-config :composite :authentication-providers])))
            error-message (str "authentication must be one of: '"
                               (string/join "', '" (sort (into #{"disabled" "standard"} authentication-providers)))
                               "'")]
        (let [token (rand-name)
              {:keys [body] :as response} (post-token waiter-url (assoc (kitchen-params)
                                                                   :authentication "invalid"
                                                                   :name token
                                                                   :permitted-user "*"
                                                                   :run-as-user (retrieve-username)
                                                                   :token token))]
          (assert-response-status response 400)
          (is (string/includes? body error-message)))
        (let [token (rand-name)
              {:keys [body] :as response} (post-token waiter-url (assoc (kitchen-params)
                                                                   :authentication ""
                                                                   :name token
                                                                   :permitted-user "*"
                                                                   :run-as-user (retrieve-username)
                                                                   :token token))]
          (assert-response-status response 400)
          (is (string/includes? body error-message)))))))

(defn- perform-saml-authentication
  "Default implementation of performing authentication with an identity provider service.
   Return map of waiter acs endpoint, saml-response and relay-state"
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
                                     :headers {"content-type" "application/x-www-form-urlencoded"}
                                     :method :post)]
    (reaver/extract (reaver/parse body) [:waiter-saml-acs-endpoint :saml-response :relay-state]
                    "form" (reaver/attr :action)
                    "form input[name=SAMLResponse]" (reaver/attr :value)
                    "form input[name=RelayState]" (reaver/attr :value))))

(defn- perform-saml-authentication-kerberos
  "Implementation of performing authentication with an identity provider service using kerberos.
   Return map of waiter acs endpoint, saml-response and relay-state"
  [saml-redirect-location]
  (let [make-connection (fn [request-url]
                          (let [http-connection (.openConnection (URL. request-url))]
                            (.setDoOutput http-connection false)
                            (.setDoInput http-connection true)
                            (.setRequestMethod http-connection "GET")
                            (.connect http-connection)
                            http-connection))
        conn (make-connection saml-redirect-location)]
    (is (= 200 (.getResponseCode conn)))
    (reaver/extract (reaver/parse (slurp (.getInputStream conn))) [:waiter-saml-acs-endpoint :saml-response :relay-state]
                    "form" (reaver/attr :action)
                    "form input[name=SAMLResponse]" (reaver/attr :value)
                    "form input[name=RelayState]" (reaver/attr :value))))

;; need to temporarily turn off this test until ADFS setting is updated
(deftest ^:parallel ^:integration-fast ^:explicit test-saml-authentication
  (testing-using-waiter-url
    (when (supports-saml-authentication? waiter-url)
      (let [auth-principal (or (System/getenv "SAML_AUTH_USER") (retrieve-username))
            token (rand-name)
            response (post-token waiter-url (-> (kitchen-params)
                                              (assoc
                                                :authentication "saml"
                                                :name token
                                                :permitted-user "*"
                                                :run-as-user (retrieve-username)
                                                :token token)))
            _ (assert-response-status response 200)
            {:keys [headers] :as response} (make-request waiter-url "/request-info" :headers {:x-waiter-token token})
            _ (assert-response-status response 302)
            saml-redirect-location (get headers "location")
            saml-authentication-fn (if use-spnego perform-saml-authentication-kerberos perform-saml-authentication)
            {:keys [relay-state saml-response waiter-saml-acs-endpoint]} (saml-authentication-fn saml-redirect-location)
            {:keys [body]} (make-request waiter-saml-acs-endpoint ""
                                         :body (str "SAMLResponse=" (URLEncoder/encode saml-response) "&RelayState=" (URLEncoder/encode relay-state))
                                         :headers {"content-type" "application/x-www-form-urlencoded"}
                                         :method :post)
            {:keys [waiter-saml-auth-redirect-endpoint saml-auth-data]}
            (reaver/extract (reaver/parse body) [:waiter-saml-auth-redirect-endpoint :saml-auth-data]
                            "form" (reaver/attr :action)
                            "form input[name=saml-auth-data]" (reaver/attr :value))
            _ (is (= (str "http://" waiter-url "/waiter-auth/saml/auth-redirect") waiter-saml-auth-redirect-endpoint))
            {:keys [cookies headers] :as response} (make-request waiter-url "/waiter-auth/saml/auth-redirect"
                                                                 :body (str "saml-auth-data=" (URLEncoder/encode saml-auth-data))
                                                                 :headers {"content-type" "application/x-www-form-urlencoded"}
                                                                 :method :post)
            _ (assert-response-status response 303)
            cookie-fn (fn [cookies name] (some #(when (= name (:name %)) %) cookies))
            auth-cookie (cookie-fn cookies "x-waiter-auth")
            _ (is (not (nil? auth-cookie)))
            _ (is (> (:max-age auth-cookie) (-> 1 t/hour t/in-seconds)))
            _ (is (= (str "http://" waiter-url "/request-info") (get headers "location")))
            {:keys [body service-id] :as response} (make-request-with-debug-info
                                                     {:x-waiter-token token}
                                                     #(make-request waiter-url "/request-info" :headers % :cookies cookies))
            _ (assert-response-status response 200)
            body-json (json/read-str (str body))]
        (with-service-cleanup
          service-id
          (is (= auth-principal (get-in body-json ["headers" "x-waiter-auth-principal"]))))))))

