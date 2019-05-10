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
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [ring.middleware.params :as ring-params]
            [saml20-clj.routes :as saml-routes]
            [saml20-clj.shared :as saml-shared]
            [saml20-clj.sp :as saml-sp]
            [waiter.auth.authentication :as auth]
            [waiter.middleware :as middleware]))

(defrecord SamlAuthenticator [hostname idp-cert idp-uri mutables password saml-req-factory! prune-fn!]
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
        mutables (saml-sp/generate-mutables)
        saml-req-factory! (saml-sp/create-request-factory mutables
                                                          idp-uri
                                                          saml-routes/saml-format
                                                          "waiter"
                                                          acs-uri)
        prune-fn! (partial saml-sp/prune-timed-out-ids! (:saml-id-timeouts mutables))
        state {:mutables mutables
               :saml-req-factory! saml-req-factory!
               :timeout-pruner-fn! prune-fn!}]
    (->SamlAuthenticator hostname idp-cert idp-uri mutables password saml-req-factory! prune-fn!)))

(defn certificate-x509
  "Takes in a raw X.509 certificate string, parses it, and creates a Java certificate."
  [x509-string]
  (let [fty (java.security.cert.CertificateFactory/getInstance "X.509")
        bais (io/input-stream (.getBytes x509-string))]
    (.generateCertificate fty bais)))

(defn validate-saml-response-signature
  "Checks (if exists) the signature of SAML Response given the IdP certificate"
  [saml-resp idp-cert]
  (if-let [signature (.getSignature saml-resp)]
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
  (let [{:keys [form-params]} (ring-params/params-request request)
        relay-state (-> form-params (get "RelayState"))
        saml-response (-> form-params (get "SAMLResponse")
                          (saml-shared/base64->inflate->str)
                          (saml-sp/xml-string->saml-resp))
        valid-signature? (if idp-cert
                           (validate-saml-response-signature saml-response idp-cert)
                           false)]
    (when-not valid-signature?
      (throw (ex-info "Could not authenticate user. Invalid SAML response signature." {:status 400})))
    (let [saml-info (when valid-signature? (saml-sp/saml-resp->assertions saml-response nil))
          saml-attrs (-> saml-info (:assertions) (first) (:attrs))
          ; https://docs.microsoft.com/en-us/windows-server/identity/ad-fs/technical-reference/the-role-of-claims
          saml-principal (get saml-attrs "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn"
                              (get saml-attrs "email"))
          {:keys [authorization/principal authorization/user]} (auth/auth-params-map (first saml-principal))]
      (auth/handle-request-auth (constantly {:status 303
                                             :headers {"Location" relay-state}
                                             :body ""})
                                request user principal password))))