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
(ns waiter.auth.saml
  (:require [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [ring.middleware.params :as ring-params]
            [saml20-clj.routes :as saml-routes]
            [saml20-clj.shared :as saml-shared]
            [saml20-clj.sp :as saml-sp]
            [waiter.auth.authentication :as auth]
            [waiter.middleware :as middleware]
            [waiter.util.utils :as utils]))

(defrecord SamlAuthenticator [hostname idp-cert idp-uri password saml-req-factory!]
  auth/Authenticator
  (wrap-auth-handler [_ request-handler]
    (fn saml-authenticator-handler [{:keys [headers query-string request-method uri] :as request}]
      (let [waiter-cookie (auth/get-auth-cookie-value (get headers "cookie"))
            [auth-principal _ :as decoded-auth-cookie] (auth/decode-auth-cookie waiter-cookie password)]
        (cond
          ;; Use the cookie, if not expired
          (auth/decoded-auth-valid? decoded-auth-cookie)
          (let [auth-params-map (auth/auth-params-map auth-principal)
                request-handler' (middleware/wrap-merge request-handler auth-params-map)]
            (request-handler' request))
          :else
          (let [saml-request (saml-req-factory!)
                relay-state (str hostname uri "?" query-string)]
            (saml-sp/get-idp-redirect idp-uri saml-request relay-state)))))))

(defn saml-authenticator
  "Factory function for creating SAML authenticator middleware"
  [{:keys [idp-cert-resource-path idp-cert-uri idp-uri hostname password]}]
  {:pre [(or (not-empty idp-cert-resource-path) (not-empty idp-cert-uri))
         (not-empty idp-uri)
         (not-empty hostname)
         (not-empty password)]}
  (let [acs-uri (str "https://" hostname "/waiter-auth/saml/acs")
        idp-cert (if idp-cert-resource-path
                   (slurp (clojure.java.io/resource idp-cert-resource-path))
                   (slurp idp-cert-uri))
        saml-req-factory! (saml-sp/create-request-factory #(str "WAITER-" (utils/make-uuid))
                                                          (constantly nil)
                                                          nil
                                                          idp-uri
                                                          saml-routes/saml-format
                                                          "waiter"
                                                          acs-uri)]
    (->SamlAuthenticator hostname idp-cert idp-uri password saml-req-factory!)))

(defn certificate-x509
  "Takes in a raw X.509 certificate string, parses it, and creates a Java certificate."
  [x509-string]
  (let [fty (java.security.cert.CertificateFactory/getInstance "X.509")
        bais (io/input-stream (.getBytes x509-string))]
    (.generateCertificate fty bais)))

(defn validate-saml-assertion-signature
  "Checks that the SAML assertion has a valid signature."
  [assertion idp-cert]
  (if-let [signature (.getSignature assertion)]
    (let [idp-pubkey (-> idp-cert certificate-x509 saml-shared/jcert->public-key)
          public-creds (doto (new org.opensaml.xml.security.x509.BasicX509Credential)
                         (.setPublicKey idp-pubkey))
          validator (new org.opensaml.xml.signature.SignatureValidator public-creds)]
      (try
        (.validate validator signature)
        true
        (catch org.opensaml.xml.validation.ValidationException ex
          (log/warn "Signature NOT valid" (.getMessage ex))
          false)))
    false ;; if not signature is present
    ))

(defn saml-acs-handler
  "Endpoint for POSTs to Waiter with IdP-signed credentials. If signature is valid, redirect to originally requested resource."
  [request {:keys [idp-cert password]}]
  {:pre [(not-empty idp-cert)
         (not-empty password)]}
  (let [{:keys [form-params]} (ring-params/params-request request)
        relay-state (-> form-params (get "RelayState"))
        saml-response (-> form-params (get "SAMLResponse")
                          (saml-shared/base64->inflate->str)
                          (saml-sp/xml-string->saml-resp))
        assertions (.getAssertions saml-response)
        _ (when-not (= 1 (count assertions))
            (throw (ex-info (str "Could not authenticate user. Invalid SAML response. "
                                 "Must have exactly one assertion but got " (count assertions))
                            {:status 400})))
        assertion (first assertions)
        _ (when-not (validate-saml-assertion-signature assertion idp-cert)
            (throw (ex-info "Could not authenticate user. Invalid SAML assertion signature."
                            {:status 400})))
        {:keys [attrs confirmation name-id]} (saml-sp/parse-saml-assertion assertion)
        _ (when-not (t/before? (t/now) (clj-time.coerce/from-sql-time (:not-on-or-after confirmation)))
            (throw (ex-info "Could not authenticate user. Expired SAML assertion."
                            {:status 400
                             :saml-assertion-not-on-or-after (:not-on-or-after confirmation)})))
        saml-principal (first (get attrs "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn"
                                   [(get name-id :value)]))
        {:keys [authorization/principal authorization/user]} (auth/auth-params-map saml-principal)]
    (auth/handle-request-auth (constantly {:status 303
                                           :headers {"Location" relay-state}
                                           :body ""})
                              request user principal password)))