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
            [clojure.string :as string]
            [comb.template :as template]
            [metrics.counters :as counters]
            [plumbing.core :as pc]
            [ring.middleware.params :as ring-params]
            [ring.util.codec :as codec]
            [ring.util.response :as response]
            [waiter.auth.authentication :as auth]
            [waiter.middleware :as middleware]
            [waiter.util.date-utils :as du]
            [waiter.util.utils :as utils]
            [waiter.metrics :as metrics])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (java.nio.charset Charset)
           (java.security.cert CertificateFactory)
           (java.util.zip Deflater DeflaterOutputStream)
           (javax.xml.parsers DocumentBuilderFactory)
           (org.opensaml Configuration DefaultBootstrap)
           (org.opensaml.xml.security.x509 BasicX509Credential)
           (org.opensaml.xml.signature SignatureValidator)
           (org.opensaml.xml.validation ValidationException)))

(def charset-format (Charset/forName "UTF-8"))

(defn str->bytes
  "Get bytes of a string"
  [some-string]
  (.getBytes some-string charset-format))

(defn bytes->str
  "Get string from bytes"
  [some-bytes]
  (String. some-bytes charset-format))

(defn deflate-bytes
  "Gzip compress bytes"
  [str-bytes]
  (let [out (ByteArrayOutputStream.)
        deflater (DeflaterOutputStream.
                   out
                   (Deflater. -1 true) 1024)]
    (.write deflater str-bytes)
    (.close deflater)
    (.toByteArray out)))

(defn deflate-and-base64-encode
  "Gzip compress bytes and base 64 encode"
  [deflatable-str]
  (let [byte-str (str->bytes deflatable-str)]
    (bytes->str (b64/encode (deflate-bytes byte-str)))))

(defn base64->str
  "Decode base 64 string"
  [string]
  (let [byte-str (str->bytes string)]
    (bytes->str (b64/decode byte-str))))

(defn get-idp-redirect
  "Return Ring response for HTTP 302 redirect."
  [idp-url saml-request relay-state]
  (response/redirect
    (str idp-url
         "?"
         (let [saml-request (deflate-and-base64-encode saml-request)]
           (codec/form-encode
             {:SAMLRequest saml-request :RelayState relay-state})))))

(defn validate-saml-assertion-signature
  "Checks that the SAML assertion has a valid signature."
  [assertion saml-signature-validator]
  (if-let [signature (.getSignature assertion)]
    (try
      (.validate saml-signature-validator signature)
      (catch ValidationException ex
        (throw (ex-info "Could not authenticate user. Invalid SAML assertion signature."
                        {:status 400} ex))))
    (throw (ex-info "Could not authenticate user. SAML assertion is not signed."
                    {:status 400}))))

(defn create-document-builder
  "Create new xml document builder"
  []
  (.newDocumentBuilder
    (doto (DocumentBuilderFactory/newInstance)
      (.setNamespaceAware true)
      (.setFeature "http://xml.org/sax/features/external-general-entities" false)
      (.setFeature "http://xml.org/sax/features/external-parameter-entities" false)
      (.setFeature "http://apache.org/xml/features/nonvalidating/load-external-dtd" false)
      (.setAttribute "http://www.oracle.com/xml/jaxp/properties/entityExpansionLimit" "2000")
      (.setAttribute "http://www.oracle.com/xml/jaxp/properties/totalEntitySizeLimit" "100000")
      (.setAttribute "http://www.oracle.com/xml/jaxp/properties/maxParameterEntitySizeLimit" "10000")
      (.setAttribute "http://www.oracle.com/xml/jaxp/properties/maxElementDepth" "100")
      (.setExpandEntityReferences false))))

(defn str->input-stream
  "Unravels a string into an input stream so we can work with Java constructs."
  [unravel]
  (ByteArrayInputStream. (.getBytes unravel charset-format)))

(defn str->xmldoc
  [parsable-str]
  (let [document (create-document-builder)]
    (.parse document (str->input-stream parsable-str))))

(defn xml-string->saml-resp
  "Parses a SAML response (XML string) from IdP and returns the corresponding (Open)SAML Response object"
  [xml-string]
  ;; We use org.opensaml.xml here since we already depend on org.opensaml for SAML assertion signature validation
  (let [xmldoc (.getDocumentElement (str->xmldoc xml-string))
        unmarshaller-factory (Configuration/getUnmarshallerFactory)
        unmarshaller (.getUnmarshaller unmarshaller-factory xmldoc)
        saml-resp (.unmarshall unmarshaller xmldoc)]
    saml-resp))

(def saml2-attr->name
  "Get friendly name for a SAML2 attribute
   https://www.purdue.edu/apps/account/docs/Shibboleth/Shibboleth_information.jsp
    Or
   https://wiki.library.ucsf.edu/display/IAM/EDS+Attributes"
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

(defn parse-saml-assertion
  "Returns the attributes and the 'audiences' for the given SAML assertion
   http://kevnls.blogspot.gr/2009/07/processing-saml-in-java-using-opensaml.html
   http://stackoverflow.com/questions/9422545/decrypting-encrypted-assertion-using-saml-2-0-in-java-using-opensaml"
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
    {:attrs attrs
     :audiences audiences
     :name-id (if name-id {:value (.getValue name-id)
                           :format (.getFormat name-id)}
                          {})
     :confirmation {:in-response-to (.getInResponseTo subject-confirmation-data)
                    :not-before (tc/to-timestamp (.getNotBefore subject-confirmation-data))
                    :not-on-or-after (tc/to-timestamp (.getNotOnOrAfter subject-confirmation-data))
                    :recipient (.getRecipient subject-confirmation-data)}}))

(let [authenticated-redirect-template-fn
      (template/fn
        [{:keys [auth-redirect-uri saml-auth-data]}]
        (slurp (io/resource "auth/authenticated-redirect.html")))]
  (defn render-authenticated-redirect-template
    "Render the authenticated redirect html page."
    [context]
    (authenticated-redirect-template-fn context)))

(defn saml-acs-handler
  "Endpoint for POSTs to Waiter with IdP-signed credentials. If signature is valid, return self-posting form
   that posts authentication data to the original hostname of the application."
  [{:keys [auth-redirect-endpoint password saml-signature-validator]} request]
  {:pre [saml-signature-validator
         (not-empty password)]}
  (let [{:keys [form-params]} (ring-params/params-request request)
        {:keys [host request-url scheme]} (try (-> form-params (get "RelayState") (utils/base-64-string->map password))
                                               (catch Exception e
                                                 (throw (ex-info "Could not parse SAML RelayState"
                                                                 {:inner-exception e
                                                                  :saml-relay-state (get form-params "RelayState")
                                                                  :status 400} e))))
        saml-response (-> form-params
                        (get "SAMLResponse")
                        base64->str
                        xml-string->saml-resp)
        assertions (.getAssertions saml-response)
        _ (when-not (= 1 (count assertions))
            (throw (ex-info (str "Could not authenticate user. Invalid SAML response. "
                                 "Must have exactly one assertion but got " (count assertions))
                            {:status 400})))
        assertion (first assertions)
        _ (validate-saml-assertion-signature assertion saml-signature-validator)
        {:keys [attrs confirmation name-id]} (parse-saml-assertion assertion)
        not-on-or-after (clj-time.coerce/from-sql-time (:not-on-or-after confirmation))
        current-time (t/now)
        _ (when-not (t/before? current-time not-on-or-after)
            (throw (ex-info "Could not authenticate user. Expired SAML assertion."
                            {:current-time current-time
                             :expiry-time not-on-or-after
                             :status 400})))
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
  "Endpoint for POST back to Waiter with SAML authentication data. If data is still valid,
   add waiter authentication cookie and redirect back to the original user app."
  [{:keys [password]} request]
  {:pre [(not-empty password)]}
  (if-let [saml-auth-data (get-in (ring-params/params-request request) [:form-params "saml-auth-data"])]
    (let [{:keys [not-on-or-after redirect-url saml-principal]}
          (try
            (utils/base-64-string->map saml-auth-data password)
            (catch Exception e
              (throw (ex-info "Could not parse saml-auth-data." {:inner-exception e
                                                                 :saml-auth-data saml-auth-data
                                                                 :status 400} e))))
          current-time (t/now)
          _ (when-not (t/before? current-time not-on-or-after)
              (throw (ex-info "Could not authenticate user. Expired SAML assertion."
                              {:current-time current-time
                               :expiry-time not-on-or-after
                               :status 400})))
          auth-cookie-expiry-date (t/min-date not-on-or-after (t/plus current-time (t/days 1)))
          auth-cookie-age-in-seconds (-> current-time
                                       (t/interval auth-cookie-expiry-date)
                                       t/in-seconds)
          {:keys [authorization/principal authorization/user] :as auth-params-map}
          (auth/auth-params-map saml-principal)]
      (auth/handle-request-auth (constantly {:body ""
                                             :headers {"location" redirect-url}
                                             :status 303})
                                request principal auth-params-map password auth-cookie-age-in-seconds))
    (throw (ex-info "Missing saml-auth-data from SAML authenticated redirect message"
                    {:status 400}))))

(defn- escape-xml-string
  "Escape a string for use in an XML document."
  [str]
  (string/escape str {\' "&apos;"
                      \" "&quot;"
                      \& "&amp;"
                      \< "&lt;"
                      \> "&gt;"}))

(let [saml-authentication-request-template-fn
      (template/fn
        [{:keys [time-issued saml-service-name saml-id acs-url idp-uri]}]
        (slurp (io/resource "auth/saml-authentication-request.xml")))]
  (defn render-saml-authentication-request-template
    "Render the SAML authentication request XML."
    [context]
    (saml-authentication-request-template-fn (pc/map-vals escape-xml-string context))))

(defn create-request
  "Return XML elements that represent a SAML 2.0 auth request."
  [time-issued saml-service-name saml-id acs-url idp-uri]
  (render-saml-authentication-request-template (utils/keys-map time-issued saml-service-name saml-id acs-url idp-uri)))

(def instant-format (f/formatters :date-time-no-ms))

(defn make-issue-instant
  "Converts a date-time to a SAML 2.0 time string."
  []
  (du/date-to-str (t/now) instant-format))

(defn create-request-factory!
  "Creates new requests for a particular service, format, and acs-url."
  [idp-uri saml-service-name acs-url]
  ;;; Bootstrap opensaml when we create a request factory.
  (DefaultBootstrap/bootstrap)
  #(create-request (make-issue-instant)
                   saml-service-name
                   (str "WAITER-" (utils/unique-identifier))
                   acs-url
                   idp-uri))

(defmacro endpoint-with-waiter-metrics
  "Calls body, wrapping with timer, count, concurrent count, and rate metrics"
  [classifier nested-path & body]
  `(:out (metrics/with-timer
           (metrics/waiter-timer ~classifier ~@nested-path) ; timer has both timer and rate
           (do
             (counters/inc! (metrics/waiter-counter ~classifier ~@nested-path))
             (counters/inc! (metrics/waiter-counter ~classifier ~@(conj nested-path "concurrent")))
             (try
               (do ~@body)
               (finally
                 (counters/dec! (metrics/waiter-counter ~classifier ~@(conj nested-path "concurrent")))))))))

(defrecord SamlAuthenticator [auth-redirect-endpoint idp-uri password saml-request-factory saml-signature-validator]
  auth/Authenticator
  (process-callback [this {{:keys [operation]} :route-params :as request}]
    (case operation
      "acs" (endpoint-with-waiter-metrics "auth" ["saml" "acs"] (saml-acs-handler this request))
      "auth-redirect" (endpoint-with-waiter-metrics "auth" ["saml" "auth-redirect"] (saml-auth-redirect-handler this request))
      (throw (ex-info (str "Unknown SAML authenticator operation: " operation)
                      {:operation operation
                       :status 400}))))
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
          (endpoint-with-waiter-metrics
            "auth" ["saml" "auth-handler"]
            (case request-method
              :get (let [saml-request (saml-request-factory)
                         scheme (name (utils/request->scheme request))
                         host (get headers "host")
                         request-url (str scheme "://" host uri (when query-string "?") query-string)
                         relay-state (utils/map->base-64-string (utils/keys-map host request-url scheme) password)]
                     (get-idp-redirect idp-uri saml-request relay-state))
              (throw (ex-info "Invalid request method for use with SAML authentication. Only GET supported."
                              {:log-level :info :request-method request-method :status 405})))))))))

(defn saml-authenticator
  "Factory function for creating SAML authenticator middleware"
  [{:keys [idp-cert-resource-path idp-cert-uri idp-uri hostname password]}]
  {:pre [(or (not (string/blank? idp-cert-resource-path)) (not (string/blank? idp-cert-uri)))
         (not (string/blank? idp-uri))
         (not (string/blank? hostname))
         (not-empty password)]}
  (let [acs-uri (str "https://" hostname "/waiter-auth/saml/acs")
        auth-redirect-endpoint "/waiter-auth/saml/auth-redirect"
        idp-cert (if idp-cert-resource-path
                   (slurp (clojure.java.io/resource idp-cert-resource-path))
                   (slurp idp-cert-uri))
        idp-public-key (-> (CertificateFactory/getInstance "X.509")
                         (.generateCertificate (io/input-stream (.getBytes idp-cert)))
                         .getPublicKey)
        public-credential (doto (new BasicX509Credential)
                            (.setPublicKey idp-public-key))
        saml-signature-validator (new SignatureValidator public-credential)
        saml-request-factory (create-request-factory! idp-uri "waiter" acs-uri)]
    (->SamlAuthenticator auth-redirect-endpoint idp-uri password saml-request-factory saml-signature-validator)))
