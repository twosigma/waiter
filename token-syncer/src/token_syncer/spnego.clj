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
(ns token-syncer.spnego
  (:require [clojure.tools.logging :as log])
  (:import (java.net URI)
           (org.apache.commons.codec.binary Base64)
           (org.eclipse.jetty.client.api Authentication$Result Request)
           (org.eclipse.jetty.http HttpHeader)
           (org.ietf.jgss GSSManager GSSContext GSSName Oid)))

(def ^Oid spnego-oid (Oid. "1.3.6.1.5.5.2"))

(def ^Base64 base64 (Base64.))

(defn spnego-authentication
  "Returns an Authentication$Result for endpoint which will use SPNEGO to generate an Authorization header"
  [^URI endpoint]
  (reify Authentication$Result
    (getURI [_] endpoint)

    (^void apply [_ ^Request request]
      (try
        (let [gss-manager (GSSManager/getInstance)
              server-princ (str "HTTP@" (.getHost endpoint))
              server-name (.createName gss-manager server-princ GSSName/NT_HOSTBASED_SERVICE spnego-oid)
              gss-context (.createContext gss-manager server-name spnego-oid nil GSSContext/DEFAULT_LIFETIME)
              _ (.requestMutualAuth gss-context true)
              token (.initSecContext gss-context (make-array Byte/TYPE 0) 0 0)
              header (str "Negotiate " (String. (.encode base64 token)))]
          (.header request HttpHeader/AUTHORIZATION header))
        (catch Exception e
          (log/error e "failure during spnego authentication"))))))
