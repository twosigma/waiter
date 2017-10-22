;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.cookie-support
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy]
            [waiter.utils :as utils])
  (:import clojure.lang.ExceptionInfo
           org.eclipse.jetty.util.UrlEncoded))

(defn url-decode
  "Decode a URL-encoded string.  java.util.URLDecoder is super slow."
  [^String string]
  (when string
    (UrlEncoded/decodeString string)))

(defn- strip-double-quotes
  [value]
  (let [value-length (count value)]
    (if (and (> value-length 1) (str/starts-with? value "\"") (str/ends-with? value "\""))
      (subs value 1 (dec value-length))
      value)))

(defn cookie-value
  "Retrieves the value corresponding to the cookie name."
  [cookie-string cookie-name]
  (when cookie-string
    (let [name-regex (re-pattern (str "(?i)" cookie-name "=([^;]+)"))]
      (when-let [^String value (second (re-find name-regex cookie-string))]
        (-> value url-decode strip-double-quotes)))))

(defn remove-cookie
  "Removes the specified cookie"
  [cookie-string cookie-name]
  (when cookie-string
    (str/replace cookie-string (re-pattern (str "(?i)(^" cookie-name "=[^;]+(; )?)|(; " cookie-name "=[^;]+)")) "")))

(defn encode-cookie
  "Encodes the cookie value."
  [value password]
  (let [value-bytes (b64/encode (nippy/freeze value {:password password :compressor nil}))]
    (String. ^bytes value-bytes "utf-8")))

(defn add-encoded-cookie
  "Inserts the provided name-value pair as a Set-Cookie header in the response"
  [response password name value max-age-in-seconds]
  (letfn [(add-cookie-into-response [response]
            (let [encoded-cookie (-> (encode-cookie value password)
                                     UrlEncoded/encodeString)
                  path "/"
                  set-cookie-header (str name "=" encoded-cookie ";Max-Age=" max-age-in-seconds ";Path=" path ";HttpOnly=true")
                  existing-header (get-in response [:headers "set-cookie"])
                  new-header (cond
                               (nil? existing-header) set-cookie-header
                               (string? existing-header) [existing-header set-cookie-header]
                               :else (conj existing-header set-cookie-header))]
              (assoc-in response [:headers "set-cookie"] new-header)))]
    (if (map? response)
      (add-cookie-into-response response)
      (async/go (add-cookie-into-response (async/<! response))))))

(defn decode-cookie
  "Decode Waiter encoded cookie."
  [^String waiter-cookie password]
  (try
    (-> waiter-cookie
        (.getBytes)
        (b64/decode)
        (nippy/thaw {:password password :v1-compatibility? false :compressor nil}))
    (catch ExceptionInfo e
      (log/error "Error in decoding cookie" (.getMessage e))
      ;; remove password from exception throw by nippy
      (throw (ex-info (.getMessage e)
                      (-> (ex-data e)
                          (update-in [:opts :password] (fn [password] (when password "***")))))))))

(let [cookie-cache (-> {}
                       (cache/ttl-cache-factory :ttl (-> 300 t/seconds t/in-millis))
                       atom)]
  (defn decode-cookie-cached
    "Decode Waiter encoded cookie."
    [^String waiter-cookie password]
    (utils/atom-cache-get-or-load
      cookie-cache waiter-cookie
      (fn [] (decode-cookie waiter-cookie password)))))
