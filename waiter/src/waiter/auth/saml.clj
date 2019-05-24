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
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.data.codec.base64 :as b64]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [comb.template :as template]
            [hiccup.core :as hiccup]
            [hiccup.page]
            [ring.middleware.params :as ring-params]
            [ring.util.codec :refer [form-encode url-encode base64-encode]]
            [ring.util.response :refer [redirect]]
            [waiter.auth.authentication :as auth]
            [waiter.middleware :as middleware]
            [waiter.util.utils :as utils])
  (:import [javax.xml.parsers DocumentBuilderFactory]))

(def charset-format (java.nio.charset.Charset/forName "UTF-8"))

(defn read-to-end
  [stream]
  (let [sb (StringBuilder.)]
    (with-open [reader (-> stream
                           java.io.InputStreamReader.
                           java.io.BufferedReader.)]
      (loop [c (.read reader)]
        (if (neg? c)
          (str sb)
          (do
            (.append sb (char c))
            (recur (.read reader))))))))

(defn str->bytes
  [some-string]
  (.getBytes some-string charset-format))

(defn bytes->str
  [some-bytes]
  (String. some-bytes charset-format))

(defn byte-deflate
  [str-bytes]
  (let [out (java.io.ByteArrayOutputStream.)
        deflater (java.util.zip.DeflaterOutputStream.
                   out
                   (java.util.zip.Deflater. -1 true) 1024)]
    (.write deflater str-bytes)
    (.close deflater)
    (.toByteArray out)))

(defn str->deflate->base64
  [deflatable-str]
  (let [byte-str (str->bytes deflatable-str)]
    (bytes->str (b64/encode (byte-deflate byte-str)))))

(defn base64->str
  [string]
  (let [byte-str (str->bytes string)]
    (bytes->str (b64/decode byte-str))))

(defn uri-query-str
  [clean-hash]
  (form-encode clean-hash))

(defn get-idp-redirect
  "Return Ring response for HTTP 302 redirect."
  [idp-url saml-request relay-state]
  (redirect
    (str idp-url
         "?"
         (let [saml-request (str->deflate->base64 saml-request)]
           (uri-query-str
             {:SAMLRequest saml-request :RelayState relay-state})))))

(defrecord SamlAuthenticator [auth-redirect-endpoint idp-cert idp-uri password saml-acs-handler-fn saml-auth-redirect-handler-fn saml-request-factory]
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
          (case request-method
            :get (let [saml-request (saml-request-factory)
                       scheme (name (utils/request->scheme request))
                       host (get headers "host")
                       request-url (str scheme "://" host uri (if query-string "?" "") query-string)
                       relay-state (utils/map->base-64-string {:host host :request-url request-url :scheme scheme} password)]
                   (get-idp-redirect idp-uri saml-request relay-state))
            (throw (ex-info "Invalid request method for use with SAML authentication. Only GET supported."
                            {:log-level :info :request-method request-method :status 405}))))))))

(defn certificate-x509
  "Takes in a raw X.509 certificate string, parses it, and creates a Java certificate."
  [x509-string]
  (let [fty (java.security.cert.CertificateFactory/getInstance "X.509")
        bais (io/input-stream (.getBytes x509-string))]
    (.generateCertificate fty bais)))

(defn jcert->public-key
  "Extracts a public key object from a java cert object."
  [java-cert-obj]
  (.getPublicKey java-cert-obj))

(defn validate-saml-assertion-signature
  "Checks that the SAML assertion has a valid signature."
  [assertion idp-cert]
  (if-let [signature (.getSignature assertion)]
    (let [idp-pubkey (-> idp-cert certificate-x509 jcert->public-key)
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

(defn new-doc-builder
  []
  (let [doc (DocumentBuilderFactory/newInstance)]
    (.setNamespaceAware doc true)
    (.setFeature doc "http://xml.org/sax/features/external-general-entities" false)
    (.setFeature doc "http://xml.org/sax/features/external-parameter-entities" false)
    (.setFeature doc "http://apache.org/xml/features/nonvalidating/load-external-dtd" false)
    (.setAttribute doc "http://www.oracle.com/xml/jaxp/properties/entityExpansionLimit" "2000")
    (.setAttribute doc "http://www.oracle.com/xml/jaxp/properties/totalEntitySizeLimit" "100000")
    (.setAttribute doc "http://www.oracle.com/xml/jaxp/properties/maxParameterEntitySizeLimit" "10000")
    (.setAttribute doc "http://www.oracle.com/xml/jaxp/properties/maxElementDepth" "100")
    (.setExpandEntityReferences doc false)
    (.newDocumentBuilder doc)))

(defn str->inputstream
  "Unravels a string into an input stream so we can work with Java constructs."
  [unravel]
  (java.io.ByteArrayInputStream. (.getBytes unravel charset-format)))

(defn str->xmldoc
  [parsable-str]
  (let [document (new-doc-builder)]
    (.parse document (str->inputstream parsable-str))))

(defn xml-string->saml-resp
  "Parses a SAML response (XML string) from IdP and returns the corresponding (Open)SAML Response object"
  [xml-string]
  (let [xmldoc (.getDocumentElement (str->xmldoc xml-string))
        unmarshallerFactory (org.opensaml.Configuration/getUnmarshallerFactory)
        unmarshaller (.getUnmarshaller unmarshallerFactory xmldoc)
        saml-resp (.unmarshall unmarshaller xmldoc)]
    saml-resp))

;; https://www.purdue.edu/apps/account/docs/Shibboleth/Shibboleth_information.jsp
;;  Or
;; https://wiki.library.ucsf.edu/display/IAM/EDS+Attributes
(def saml2-attr->name
  (let [names {"urn:oid:0.9.2342.19200300.100.1.1" "uid"
               "urn:oid:0.9.2342.19200300.100.1.3" "mail"
               "urn:oid:2.16.840.1.113730.3.1.241" "displayName"
               "urn:oid:2.5.4.3" "cn"
               "urn:oid:2.5.4.4" "sn"
               "urn:oid:2.5.4.12" "title"
               "urn:oid:2.5.4.20" "phone"
               "urn:oid:2.5.4.42" "givenName"
               "urn:oid:2.5.6.8" "organizationalRole"
               "urn:oid:2.16.840.1.113730.3.1.3" "employeeNumber"
               "urn:oid:2.16.840.1.113730.3.1.4" "employeeType"
               "urn:oid:1.3.6.1.4.1.5923.1.1.1.1" "eduPersonAffiliation"
               "urn:oid:1.3.6.1.4.1.5923.1.1.1.2" "eduPersonNickname"
               "urn:oid:1.3.6.1.4.1.5923.1.1.1.6" "eduPersonPrincipalName"
               "urn:oid:1.3.6.1.4.1.5923.1.1.1.9" "eduPersonScopedAffiliation"
               "urn:oid:1.3.6.1.4.1.5923.1.1.1.10" "eduPersonTargetedID"
               "urn:oid:1.3.6.1.4.1.5923.1.6.1.1" "eduCourseOffering"}]
    (fn [attr-oid]
      (get names attr-oid attr-oid))))

;; http://kevnls.blogspot.gr/2009/07/processing-saml-in-java-using-opensaml.html
;; http://stackoverflow.com/questions/9422545/decrypting-encrypted-assertion-using-saml-2-0-in-java-using-opensaml
(defn parse-saml-assertion
  "Returns the attributes and the 'audiences' for the given SAML assertion"
  [assertion]
  (let [statements (.getAttributeStatements assertion)
        subject (.getSubject assertion)
        subject-confirmation-data (.getSubjectConfirmationData
                                    (first (.getSubjectConfirmations subject)))
        name-id (.getNameID subject)
        attributes (mapcat #(.getAttributes %) statements)
        attrs (apply merge
                     (map (fn [a] {(saml2-attr->name (.getName a)) ;; Or (.getFriendlyName a) ??
                                   (map #(-> % (.getDOM) (.getTextContent))
                                        (.getAttributeValues a))})
                          attributes))
        conditions (.getConditions assertion)
        audiences (mapcat #(let [audiences (.getAudiences %)]
                             (map (fn [a] (.getAudienceURI a)) audiences))
                          (.getAudienceRestrictions conditions))]
    {:attrs attrs :audiences audiences
     :name-id
     {:value (when name-id (.getValue name-id))
      :format (when name-id (.getFormat name-id))}
     :confirmation
     {:in-response-to (.getInResponseTo subject-confirmation-data)
      :not-before (tc/to-timestamp (.getNotBefore subject-confirmation-data))
      :not-on-or-after (tc/to-timestamp (.getNotOnOrAfter subject-confirmation-data))
      :recipient (.getRecipient subject-confirmation-data)}}))

(let [authenticated-redirect-template-fn
      (template/fn
        [{:keys [auth-redirect-uri saml-auth-data]}]
        (slurp (io/resource "web/authenticated-redirect.html")))]
  (defn render-authenticated-redirect-template
    "Render the authenticated redirect html page."
    [context]
    (authenticated-redirect-template-fn context)))

(defn saml-acs-handler
  "Endpoint for POSTs to Waiter with IdP-signed credentials. If signature is valid, return principal and original request."
  [request {:keys [auth-redirect-endpoint idp-cert password]}]
  {:pre [(not-empty idp-cert)
         (not-empty password)]}
  (let [{:keys [form-params]} (ring-params/params-request request)
        {:keys [host request-url scheme]} (try (-> form-params (get "RelayState") (utils/base-64-string->map password))
                                               (catch Exception e
                                                 (throw (ex-info "Could not parse SAML RelayState"
                                                                 {:status 400
                                                                  :saml-relay-state (get form-params "RelayState")
                                                                  :inner-exception e}))))
        saml-response (-> form-params (get "SAMLResponse")
                          (base64->str)
                          (xml-string->saml-resp))
        assertions (.getAssertions saml-response)
        _ (when-not (= 1 (count assertions))
            (throw (ex-info (str "Could not authenticate user. Invalid SAML response. "
                                 "Must have exactly one assertion but got " (count assertions))
                            {:status 400})))
        assertion (first assertions)
        _ (when-not (validate-saml-assertion-signature assertion idp-cert)
            (throw (ex-info "Could not authenticate user. Invalid SAML assertion signature."
                            {:status 400})))
        {:keys [attrs confirmation name-id]} (parse-saml-assertion assertion)
        not-on-or-after (clj-time.coerce/from-sql-time (:not-on-or-after confirmation))
        t-now (t/now)
        _ (when-not (t/before? t-now not-on-or-after)
            (throw (ex-info "Could not authenticate user. Expired SAML assertion."
                            {:status 400
                             :saml-assertion-not-on-or-after not-on-or-after
                             :t-now t-now})))
        email (first (get attrs "email"))
        ; https://docs.microsoft.com/en-us/windows-server/identity/ad-fs/technical-reference/the-role-of-claims
        upn (first (get attrs "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn"))
        name-id-value (get name-id :value)
        saml-principal (or upn name-id-value)
        {:keys [authorization/principal authorization/user]} (auth/auth-params-map saml-principal)
        saml-principal' (if (and email (= principal user)) email saml-principal)
        saml-auth-data (utils/map->base-64-string
                         {:not-on-or-after not-on-or-after :redirect-url request-url :saml-principal saml-principal'}
                         password)]
    {:body (render-authenticated-redirect-template {:auth-redirect-uri (str scheme "://" host auth-redirect-endpoint)
                                                    :saml-auth-data saml-auth-data})
     :status 200}))

(defn saml-auth-redirect-handler
  "Endpoint for POSTs to Waiter with IdP-signed credentials. If signature is valid, return principal and original request."
  [request {:keys [password]}]
  {:pre [(not-empty password)]}
  (if-let [saml-auth-data (get-in (ring-params/params-request request) [:form-params "saml-auth-data"])]
    (let [{:keys [not-on-or-after redirect-url saml-principal]}
          (try
            (utils/base-64-string->map saml-auth-data password)
            (catch Exception e
              (throw (ex-info "Could not parse saml-auth-data." {:status 400
                                                                 :saml-auth-data saml-auth-data
                                                                 :inner-exception e}))))
          t-now (t/now)
          _ (when-not (t/before? t-now not-on-or-after)
              (throw (ex-info "Could not authenticate user. Expired SAML assertion."
                              {:status 400
                               :saml-assertion-not-on-or-after not-on-or-after
                               :t-now t-now})))
          age-in-seconds (t/in-seconds (t/interval t-now (t/min-date (t/plus t-now (t/days 1)) not-on-or-after)))
          {:keys [authorization/principal authorization/user] :as auth-params-map}
          (auth/auth-params-map saml-principal)]
      (auth/handle-request-auth (constantly {:status 303
                                             :headers {"Location" redirect-url}
                                             :body ""})
                                request principal auth-params-map password age-in-seconds))
    (throw (ex-info "Missing saml-auth-data from SAML authenticated redirect message"
                    {:status 400}))))

(defn create-request
  "Return XML elements that represent a SAML 2.0 auth request."
  [time-issued saml-service-name saml-id acs-url idp-uri]
  (str
    (hiccup.page/xml-declaration "UTF-8")
    (hiccup/html
      [:samlp:AuthnRequest
       {:xmlns:samlp "urn:oasis:names:tc:SAML:2.0:protocol"
        :ID saml-id
        :Version "2.0"
        :IssueInstant time-issued
        :ProtocolBinding "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
        :ProviderName saml-service-name
        :IsPassive false
        :Destination idp-uri
        :AssertionConsumerServiceURL acs-url}
       [:saml:Issuer
        {:xmlns:saml "urn:oasis:names:tc:SAML:2.0:assertion"}
        saml-service-name]])))

(def instant-format (f/formatters :date-time-no-ms))

(defn make-issue-instant
  "Converts a date-time to a SAML 2.0 time string."
  []
  (f/unparse instant-format (t/now)))

(let [opensaml-bootstrapped-atom (atom false)]
  (defn create-request-factory!
    "Creates new requests for a particular service, format, and acs-url."
    [idp-uri saml-service-name acs-url]
    ;;; Bootstrap opensaml when we create a request factory.
    (when (compare-and-set! opensaml-bootstrapped-atom false true)
      (org.opensaml.DefaultBootstrap/bootstrap))
    #(create-request (make-issue-instant)
                     saml-service-name
                     (str "WAITER-" (utils/make-uuid))
                     acs-url
                     idp-uri)))

(defn saml-authenticator
  "Factory function for creating SAML authenticator middleware"
  [{:keys [idp-cert-resource-path idp-cert-uri idp-uri hostname password]}]
  {:pre [(or (not-empty idp-cert-resource-path) (not-empty idp-cert-uri))
         (not-empty idp-uri)
         (not-empty hostname)
         (not-empty password)]}
  (let [acs-uri (str "https://" hostname "/waiter-auth/saml/acs")
        auth-redirect-endpoint "/waiter-auth/saml/auth-redirect"
        idp-cert (if idp-cert-resource-path
                   (slurp (clojure.java.io/resource idp-cert-resource-path))
                   (slurp idp-cert-uri))
        saml-request-factory (create-request-factory! idp-uri
                                                  "waiter"
                                                  acs-uri)]
    (->SamlAuthenticator auth-redirect-endpoint idp-cert idp-uri password saml-acs-handler saml-auth-redirect-handler saml-request-factory)))
