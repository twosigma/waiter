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
(ns jwks-server.handler
  (:require [buddy.core.keys :as buddy-keys]
            [buddy.sign.jwt :as jwt]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [jwks-server.config :as config]
            [plumbing.core :as pc]
            [ring.middleware.params :as ring-params]
            [ring.util.request :as ring-request]))

(defn prepare-response
  "Prepares and returns a standard response"
  ([{:keys [query-string request-method uri]} message status]
   (prepare-response (cond-> {"message" message
                              "request-method" request-method
                              "uri" uri}
                       (not (str/blank? query-string))
                       (assoc "query-string" query-string))
                     status))
  ([data-map status]
   {:body (json/write-str data-map)
    :headers {"content-type" "application/json"
              "server" "jwks-server"}
    :status status}))

(defn generate-jwt-access-token
  "Generates the JWT access token using the provided private key."
  [alg jwk-entry payload header]
  (let [private-key (-> jwk-entry pc/keywordize-map buddy-keys/jwk->private-key)
        options {:alg alg :header header}]
    (jwt/sign payload private-key options)))

(defn- request->query-params
  "Like Ring's params-request, but doesn't try to pull params from the body."
  [request]
  (->> (or (ring-request/character-encoding request) "UTF-8")
    (ring-params/assoc-query-params request)
    :query-params))

(defn process-get-token-request
  "Retrieves an JWT access token generated using a random EdDSA key."
  [{:keys [query-string] :as request}]
  (log/info "query string:" query-string)
  (let [{:strs [host]} (request->query-params request)
        _ (when (str/blank? host)
            (throw (ex-info "host query parameter not provided" {:status 400})))
        {:keys [keys]} (config/retrieve-jwks)
        eddsa-keys (filter (fn [{:keys [crv]}] (= "Ed25519" crv)) keys)
        {:keys [kid] :as entry} (rand-nth eddsa-keys)
        _ (log/info "selected kid:" kid)
        principal (System/getProperty "user.name")
        _ (log/info "principal:" principal)
        {:keys [issuer subject-key token-type]} (config/retrieve-settings)
        subject-key (keyword subject-key)
        expiry-time-secs (+ (long (/ (System/currentTimeMillis) 1000)) 600)
        payload (cond-> {:aud (str host) :exp expiry-time-secs :iss issuer :sub principal}
                  (not= :sub subject-key) (assoc subject-key principal))
        header {:kid kid :typ token-type}
        access-token (generate-jwt-access-token :eddsa entry payload header)]
    (prepare-response
      {"access_token" access-token
       "kid" kid
       "issuer" issuer
       "principal" principal
       "realm" host
       "subject-key" subject-key
       "token-type" token-type}
      200)))

(defn process-get-keys-request
  "Returns the JWKS managed bu this server."
  [_]
  (prepare-response
    (update (config/retrieve-jwks)
            :keys
            (fn [keys]
              (map (fn [entry] (dissoc entry :d)) keys)))
    200))

(defn request-handler
  "Factory for the ring request handler."
  [{:keys [headers request-method uri] :as request}]
  (log/info "received" request-method "request as path" uri)
  (log/info "request headers:" headers)
  (try
    (cond
      (= uri "/get-token")
      (if (= request-method :get)
        (process-get-token-request request)
        (prepare-response request "method not allowed" 405))
      (= uri "/keys")
      (if (= request-method :get)
        (process-get-keys-request request)
        (prepare-response request "method not allowed" 405))
      :else
      (prepare-response request "unsupported endpoint" 404))
    (catch Throwable th
      (log/error th "error in processing request")
      (prepare-response request (.getMessage th) (:status (ex-data th) 500)))))
