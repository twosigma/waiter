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
            [ring.middleware.cookies :as cookies]
            [taoensso.nippy :as nippy]
            [waiter.utils :as utils])
  (:import clojure.lang.ExceptionInfo
           java.nio.charset.StandardCharsets
           org.eclipse.jetty.util.UrlEncoded))

(defn url-decode
  "Decode a URL-encoded string.  java.util.URLDecoder is super slow.  Also Jetty 9.3 adds an overload
  to decodeString that takes just a string.  This implementation should use that once we upgrade."
  [^String string]
  (when string
    (UrlEncoded/decodeString string 0 (count string) StandardCharsets/UTF_8)))

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

(defn correct-cookies-as-vector
  "Ring expects the Set-Cookie header to be a vector of cookies. This puts them in the 'right' format"
  [response]
  (update-in response [:headers "Set-Cookie"] #(if (string? %) [%] %)))

(defn cookies-async-response
  "For responses with :cookies, adds Set-Cookie header and returns response without :cookies."
  [response]
  (log/debug "making a response with cookies: " response)
  (if (map? response)
    (cookies/cookies-response response)
    (async/go
      (-> (async/<! response)
          correct-cookies-as-vector
          cookies/cookies-response))))

(defn encode-cookie
  "Encodes the cookie value."
  [value password]
  (let [value-bytes (b64/encode (nippy/freeze value {:password password :compressor nil}))]
    (String. ^bytes value-bytes "utf-8")))

(defn add-encoded-cookie
  "Inserts the provided name-value pair as a Cookie in the :cookies map of the response."
  [response password name value age-in-days]
  (letfn [(add-cookie-into-response [response]
            (let [cookie-value {:value (encode-cookie value password), :max-age (-> age-in-days t/days t/in-seconds), :path "/"}]
              (assoc-in response [:cookies name] cookie-value)))]
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
