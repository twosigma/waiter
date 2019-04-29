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
(ns waiter.headers
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [plumbing.core :as pc]
            [waiter.util.utils :as utils]))

(def ^:const waiter-header-prefix "x-waiter-")

;; authentication is intentionally missing from this list as we do not support it as an on-the-fly header
(def ^:const params-with-str-value
  #{"allowed-params" "backend-proto" "cmd" "cmd-type" "distribution-scheme" "endpoint-path" "health-check-proto"
    "health-check-url" "image" "metric-group" "name" "namespace" "permitted-user" "run-as-user" "token" "version"})

(def ^:const waiter-headers-with-str-value (set (map #(str waiter-header-prefix %) params-with-str-value)))

(defn get-waiter-header
  "Retrieves the waiter header value."
  ([waiter-headers name] (get waiter-headers (str waiter-header-prefix name)))
  ([waiter-headers name not-found] (get waiter-headers (str waiter-header-prefix name) not-found)))

(defn parse-header-value
  "Parse the header value as json."
  [^String header-name ^String header-value]
  (if (or (contains? waiter-headers-with-str-value header-name)
          (str/starts-with? header-name (str waiter-header-prefix "env-"))
          (str/starts-with? header-name (str waiter-header-prefix "metadata-"))
          (str/starts-with? header-name (str waiter-header-prefix "param-")))
    header-value
    (try
      (json/parse-string header-value)
      (catch Exception _ ; rely on schema validate to flag this as an error if interpreting as string is incorrect
        (log/warn "unable to parse header:" header-name " defaulting to string:" header-value)
        header-value))))

(defn split-headers
  "Split headers into two maps, those describing the requested service (starting with x-waiter), and the rest.
   With the headers that describe the requested service, parse the key and value."
  [headers]
  (loop [[[^String k v] & kvs] (seq headers)
         waiter-headers (transient {})
         passthrough-headers (transient {})]
    (if k
      ; Split headers into waiter-specific headers and passthrough headers
      (if (str/starts-with? k waiter-header-prefix)
        (recur kvs
               (assoc! waiter-headers k (parse-header-value k v))
               passthrough-headers)
        (recur kvs
               waiter-headers
               (assoc! passthrough-headers k v)))
      {:passthrough-headers (persistent! passthrough-headers)
       :waiter-headers (persistent! waiter-headers)})))

(defn drop-waiter-header-prefix
  "Return a map with the `waiter-header-prefix` dropped from the keys."
  [waiter-headers]
  (pc/map-keys #(subs % (count waiter-header-prefix)) waiter-headers))

(defn contains-waiter-header
  "Returns truthy value if a waiter header with a keys from `search-keys` exists in `waiter-headers`."
  [waiter-headers search-keys]
  (some #(contains? waiter-headers (str waiter-header-prefix %)) search-keys))

(defn truncate-header-values
  "Truncates all values in headers map except x-waiter-token's to at
  most 80 characters for the purpose of logging"
  [headers]
  (let [token (headers "x-waiter-token")
        truncated-headers (pc/map-vals #(utils/truncate % 80) headers)]
    (if token
      (assoc truncated-headers "x-waiter-token" token)
      truncated-headers)))

(defn- retrieve-proto-specific-hop-by-hop-headers
  "Determines the protocol version specific hop-by-hop headers."
  [proto-version]
  (when (not= "HTTP/2.0" proto-version) ["te"]))

(defn dissoc-hop-by-hop-headers
  "Proxies must remove hop-by-hop headers before forwarding messages — both requests and responses.
   Remove the hop-by-hop headers (except te) specified in:
   https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.5.1
   The te header is not removed for HTTP/2.0 requests as it is needed by grpc to detect incompatible proxies:
   https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md"
  [{:strs [connection] :as headers} proto-version]
  (let [force-remove-headers (retrieve-proto-specific-hop-by-hop-headers proto-version)
        connection-headers (map str/trim (str/split (str connection) #","))]
    (cond-> (dissoc headers "connection" "keep-alive" "proxy-authenticate" "proxy-authorization"
                   "trailers" "transfer-encoding" "upgrade")
      (seq force-remove-headers) (as-> $ (apply dissoc $ force-remove-headers))
      (seq connection-headers) (as-> $ (apply dissoc $ connection-headers)))))

(defn assoc-auth-headers
  "`assoc`s the x-waiter-auth-principal and x-waiter-authenticated-principal headers if the
   username and principal are non-nil, respectively."
  [headers username principal]
  (cond-> headers
          username (assoc "x-waiter-auth-principal" username)
          principal (assoc "x-waiter-authenticated-principal" principal)))
