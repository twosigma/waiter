;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.headers
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [plumbing.core :as pc]
            [waiter.utils :as utils]))

(def ^:const waiter-header-prefix "x-waiter-")

(def ^:const waiter-headers-with-str-value
  (set (map #(str waiter-header-prefix %)
            #{"name" "cmd" "version" "endpoint-path" "health-check-url" "permitted-user" "run-as-user" "token"
              "cmd-type" "metric-group" "distribution-scheme"})))

(defn get-waiter-header
  "Retrieves the waiter header value."
  ([waiter-headers name] (get waiter-headers (str waiter-header-prefix name)))
  ([waiter-headers name not-found] (get waiter-headers (str waiter-header-prefix name) not-found)))

(defn parse-header-value
  "Parse the header value as json."
  [^String header-name ^String header-value]
  (if (or (contains? waiter-headers-with-str-value header-name)
          (str/starts-with? header-name (str waiter-header-prefix "metadata-")))
    header-value
    (try
      (json/parse-string header-value)
      (catch Exception _                                    ; rely on schema validate to flag this as an error if interpreting as string is incorrect
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
      {:waiter-headers (persistent! waiter-headers)
       :passthrough-headers (persistent! passthrough-headers)})))

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
