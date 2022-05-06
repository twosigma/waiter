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
(ns waiter.request-log
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [waiter.config :as config]
            [waiter.metrics :as metrics]
            [waiter.util.date-utils :as du]
            [waiter.util.ring-utils :as ru]
            [waiter.util.utils :as utils]))

(defn log
  "Log log-data as JSON"
  [log-data]
  (log/log "RequestLog" :info nil (utils/clj->json log-data)))

(defn- headers->entries
  "Extracts the request log entries attaching the provided entry prefix in the keys from the provided headers."
  [headers header-names entry-prefix]
  (->> (select-keys headers header-names)
       (pc/map-keys (fn [k] (->> k (str entry-prefix) (keyword))))))

(defn request->context
  "Convert a request into a context suitable for logging."
  [{:keys [client-protocol headers internal-protocol query-string remote-addr request-id
           request-method request-time server-port uri] :as request}
   header-names]
  (let [{:strs [content-length content-type cookie host origin referer user-agent x-cid x-forwarded-for]} headers
        remote-address (or x-forwarded-for remote-addr)]
    (cond-> {:cid x-cid
             :host host
             :path uri
             :request-header-count (count headers)
             :request-id request-id
             :scheme (-> request utils/request->scheme name)}
      (seq header-names) (merge (headers->entries headers header-names "request-header-"))
      origin (assoc :origin origin)
      request-method (assoc :method (-> request-method name str/upper-case))
      client-protocol (assoc :client-protocol client-protocol)
      cookie (assoc :cookie-header-length (-> cookie (str) (count)))
      internal-protocol (assoc :internal-protocol internal-protocol)
      query-string (assoc :query-string query-string)
      remote-address (assoc :remote-addr remote-address)
      content-length (assoc :request-content-length content-length)
      content-type (assoc :request-content-type content-type)
      referer (assoc :referer referer)
      request-time (assoc :request-time (du/date-to-str request-time))
      server-port (assoc :server-port server-port)
      user-agent (assoc :user-agent user-agent))))

(defn response->context
  "Convert a response into a context suitable for logging."
  [{:keys [authorization/method authorization/principal backend-response-latency-ns descriptor error-class
           get-instance-latency-ns handle-request-latency-ns headers instance instance-proto latest-service-id
           protocol request-type status waiter-api-call? waiter/oidc-identifier waiter/oidc-mode waiter/oidc-redirect-uri]
    :as response}
   header-names]
  (let [{:keys [service-id service-description source-tokens]} descriptor
        token (or (some->> source-tokens (map #(get % "token")) seq (str/join ","))
                  ;; allow non-proxy requests to provide tokens for use in the request log
                  (:waiter/token response))
        {:strs [image metric-group profile run-as-user version]} service-description
        {:strs [content-length content-type grpc-status location server x-raven-response-flags x-waiter-operation-result]} headers
        {:keys [k8s/node-name k8s/pod-name]} instance]
    (cond-> {:response-header-count (count headers)}
      (seq header-names) (merge (headers->entries headers header-names "response-header-"))
      status (assoc :status status)
      method (assoc :authentication-method (name method))
      backend-response-latency-ns (assoc :backend-response-latency-ns backend-response-latency-ns)
      content-length (assoc :response-content-length content-length)
      content-type (assoc :response-content-type content-type)
      descriptor (assoc :metric-group metric-group
                        :run-as-user run-as-user
                        :service-id service-id
                        :service-name (get service-description "name")
                        :service-version version)
      get-instance-latency-ns (assoc :get-instance-latency-ns get-instance-latency-ns)
      grpc-status (assoc :grpc-status grpc-status)
      image (assoc :service-image image)
      instance (assoc :instance-host (:host instance)
                      :instance-port (:port instance))
      (:id instance) (assoc :instance-id (:id instance))
      instance-proto (assoc :instance-proto instance-proto)
      latest-service-id (assoc :fallback-triggered (not= service-id latest-service-id)
                               :latest-service-id latest-service-id)
      oidc-identifier (assoc :oidc-identifier oidc-identifier)
      oidc-mode (assoc :oidc-mode oidc-mode)
      oidc-redirect-uri (assoc :oidc-redirect-uri oidc-redirect-uri)
      x-waiter-operation-result (assoc :operation-result x-waiter-operation-result)
      node-name (assoc :k8s-node-name node-name)
      pod-name (assoc :k8s-pod-name pod-name)
      principal (assoc :principal principal)
      profile (assoc :service-profile profile)
      protocol (assoc :backend-protocol protocol)
      request-type (assoc :request-type request-type)
      server (assoc :server server)
      handle-request-latency-ns (assoc :handle-request-latency-ns handle-request-latency-ns)
      location (assoc :response-location location)
      token (assoc :token token)
      (some? waiter-api-call?) (assoc :waiter-api waiter-api-call?)
      x-raven-response-flags (assoc :raven-response-flags x-raven-response-flags)
      error-class (assoc :waiter-error-class error-class))))

(defn log-request!
  "Log a request"
  [request response]
  (let [request-header-names (config/retrieve-request-log-request-headers)
        response-header-names (config/retrieve-request-log-response-headers)
        redacted-request-fields-string (get-in response [:descriptor :service-description "env" "WAITER_CONFIG_REDACTED_REQUEST_FIELDS"])
        redacted-request-fields (when-not (str/blank? redacted-request-fields-string)
                                  (map keyword (str/split redacted-request-fields-string #",")))]
    (-> (merge (request->context request request-header-names)
               (response->context response response-header-names))
      (utils/remove-keys redacted-request-fields)
      (log))))

(defn wrap-log
  "Wraps a handler logging data from requests and responses."
  [handler]
  (fn [request]
    (let [timer (timers/start (metrics/waiter-timer "handle-request"))
          response (handler request)]
      (ru/update-response
        response
        (fn [response]
          (let [elapsed-ns (timers/stop timer)]
            (log-request! request (assoc response :handle-request-latency-ns elapsed-ns)))
          response)))))
