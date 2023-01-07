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
(ns waiter.util.utils
  (:require [cheshire.core :as cheshire]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [comb.template :as template]
            [digest]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.compression :as compression]
            [waiter.status-codes :refer :all]
            [waiter.util.date-utils :as du]
            [waiter.util.http-utils :as hu])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (clojure.lang ExceptionInfo)
           (java.io ByteArrayInputStream ByteArrayOutputStream InputStream InputStreamReader OutputStreamWriter SequenceInputStream)
           (java.lang Process)
           (java.net ServerSocket URI)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.security MessageDigest)
           (java.util Base64 Collections UUID)
           (java.util.concurrent ThreadLocalRandom)
           (java.util.regex Pattern)
           (javax.servlet ServletResponse)
           (org.joda.time DateTime)
           (schema.utils ValidationError)))

(defn select-keys-pred
  "Returns a map with only the keys, k, for which (pred k) is true."
  [pred m]
  (reduce-kv (fn [m k v]
               (if (pred k)
                 (assoc m k v)
                 m))
             {} m))

(defn keyset
  "Returns a set that contains the keys of the map."
  [m]
  (-> m (keys) (set)))

(defn keys->nested-map
  "Takes a map with string keys and returns a map with a nested structure where
   the string keys were split using the regex `key-split` to create the nested
   structure.

   Example:

   (keys->nested-map {\"this.is.an.example\" 1 \"this.is.an.example2\" 2} #\"\\.\")
   => {\"this\" {\"is\" {\"an\" {\"example\" 1 \"example2\" 2}}}}"
  [original-map key-split]
  (reduce-kv (fn [m k v]
               (assoc-in m (str/split k key-split) v))
             {} original-map))

(defmacro filterm
  "Returns a map of the entries in map for which (pred entry) returns true.
   pred must be free of side-effects.
   Inspired from filterv.
   (filterm pred map) ==> (into {} (filter pred map))"
  [pred map]
  `(into {} (filter ~pred ~map)))

(defn sub-map?
  "Returns true if the map mc is a subset of the map mf."
  [mc mf]
  (if (= mc mf)
    true
    (loop [[[k v] & rem-key-val] (seq mc)]
      (let [vf (get mf k)
            val-match? (if (and (map? v) (map? vf))
                         (sub-map? v vf)
                         (= v vf))]
        (if (or (not val-match?) (nil? rem-key-val))
          val-match?
          (recur rem-key-val))))))

(defn is-uuid?
  "Returns true if `s` is an uuid"
  [s]
  (if (instance? UUID s)
    s
    (try
      (UUID/fromString s)
      (catch Exception _
        nil))))

(defn truncate [in-str max-len]
  (let [ellipsis "..."
        ellipsis-len (count ellipsis)]
    (if (and (string? in-str) (> (count in-str) max-len) (> max-len ellipsis-len))
      (str (subs in-str 0 (- max-len ellipsis-len)) ellipsis)
      in-str)))

(defn non-neg? [x]
  (or (zero? x) (pos? x)))

(defn non-neg-int? [x]
  "Returns true if x is a non-negative integer"
  (and (integer? x) (non-neg? x)))

(defn nil-safe-max
  "If an argument is nil, returns the other argument.
   Else returns the max of the arguments."
  [x y]
  (cond
    (nil? x) y
    (nil? y) x
    :else (max x y)))

(defn nil-safe-min
  "If an argument is nil, returns the other argument.
   Else returns the min of the arguments."
  [x y]
  (cond
    (nil? x) y
    (nil? y) x
    :else (min x y)))

(defn assoc-if-absent
  "If the specified key, k, is not already associated with a value, v, in the map, m, associate k with v in m."
  [m k v]
  (cond-> m
    (not (contains? m k)) (assoc k v)))

(defn generate-secret-word
  [src-id dest-id processed-passwords]
  (let [password (second (first processed-passwords))
        secret-word (digest/md5 (str src-id ":" dest-id ":" password))]
    (log/debug "generate-secret-word" [src-id dest-id] "->" secret-word)
    secret-word))

(defn attach-waiter-namespace-keys
  "Attaches the waiter namespace keys from waiter-data into response-map."
  [response-map waiter-data]
  (if (map? waiter-data)
    (reduce
      (fn [acc-map [k v]]
        (cond-> acc-map
          (and (keyword? k) (= (namespace k) "waiter")) (assoc k v)))
      response-map
      waiter-data)
    response-map))

(defn attach-waiter-source
  "Attaches the :waiter/response-source entry into the response map."
  ([response] (attach-waiter-source response :waiter))
  ([response source] (assoc response :waiter/response-source source)))

(defn waiter-generated-response?
  "Returns true if the response is tagged as generated by waiter."
  [response]
  (= :waiter (get response :waiter/response-source)))

(defn keyword->str
  "Converts keyword to string including the namespace."
  [k]
  (str (.-sym k)))

(defn- stringify-keys
  [k]
  (cond
    (keyword? k) (keyword->str k)
    (nil? k) "_nil_"
    :else (str k)))

(defn stringify-elements
  [k v]
  (cond
    (sequential? v) (mapv (partial stringify-elements k) v)
    (keyword? v) (keyword->str v)
    (instance? DateTime v) (du/date-to-str v)
    (instance? UUID v) (str v)
    (instance? Pattern v) (str v)
    (instance? ManyToManyChannel v) (str v)
    (instance? Process v) (str v)
    (instance? ValidationError v) (str v)
    (symbol? v) (str v)
    (or
      (and (double? v) (or (Double/isInfinite v) (Double/isNaN v)))
      (and (float? v) (or (Float/isInfinite v) (Float/isNaN v))))
    (do
      (log/warn "Unsupported json value for number where" k "maps to" v)
      (str v))
    :else v))

(defn try-parse-json
  "parses json and throws exception with the input string"
  ([s] (try-parse-json s identity))
  ([s key-fn]
   (try
     (json/read-str s :key-fn key-fn)
     (catch Exception e
       (throw (ex-info "Couldn't parse JSON" {:string s} e))))))

(defn clj->json
  "Convert the input Clojure data structure into a json string."
  [data-map]
  (json/write-str
    data-map
    :escape-slash false ; escaping the slashes makes the json harder to read
    :key-fn stringify-keys
    :value-fn stringify-elements))

(defn clj->json-stream
  "Convert the input Clojure data structure into a json string input stream."
  [data-map]
  (-> data-map (clj->json) (str) (.getBytes) (ByteArrayInputStream.)))

(defn clj->json-response
  "Convert the input data into a json response."
  [data-map & {:keys [headers status] :or {headers {} status http-200-ok }}]
  (-> {:body (clj->json data-map)
       :status status
       :headers (assoc headers
                  "content-type" "application/json")}
    (attach-waiter-namespace-keys data-map)
    (attach-waiter-source)))

(defn clj->streaming-json-response
  "Converts the data into a json response which can be streamed back to the client."
  [data-map & {:keys [status] :or {status http-200-ok }}]
  (let [data-map (doall data-map)]
    (-> {:status status
         :headers {"content-type" "application/json"}
         :body (fn [^ServletResponse resp]
                 (let [writer (OutputStreamWriter. (.getOutputStream resp))]
                   (try
                     (json/write
                       data-map
                       writer
                       :escape-slash false
                       :key-fn stringify-keys
                       :value-fn stringify-elements)
                     (catch Exception e
                       (log/error e "Exception creating streaming json response")
                       (throw e))
                     (finally
                       (.flush writer)))))}
      (attach-waiter-namespace-keys data-map)
      (attach-waiter-source))))

(let [b64-decoder (java.util.Base64/getUrlDecoder)]
  (defn b64-url-json-decode [^String json-str]
    "Decode a URL-safe Base64 JSON string into a Clojure data structure."
    (-> (.decode b64-decoder json-str)
        (String. StandardCharsets/UTF_8)
        (try-parse-json keyword))))

(let [b64-encoder (.withoutPadding (java.util.Base64/getUrlEncoder))]
  (defn b64-url-json-encode [data]
    "Encode a Clojure data structure as a JSON object in a URL-safe Base64 string."
    (as-> data $
      (clj->json $)
      (.getBytes $ StandardCharsets/UTF_8)
      (.encodeToString b64-encoder $))))

(defn escape-html
  "Change special characters into HTML character entities to prevent XSS."
  [text]
  (when text
    (-> text
        (str/replace #"&" "&amp;")
        (str/replace #"<" "&lt;")
        (str/replace #">" "&gt;")
        (str/replace #"\"" "&quot;"))))

(defn urls->html-links
  "Converts any URLs in a string to HTML links."
  [message]
  (when message
    (str/replace message #"(https?://[^\s]+)" "<a href=\"$1\">$1</a>")))

(defn request->content-type
  "Determines best content-type for a response given a request.
  In the case of no Accept header, assume application/json if the
  request content-type is application/json."
  [{{:strs [accept content-type]} :headers :as request}]
  (cond
    (and accept (str/includes? accept "application/json")) "application/json"
    (and accept (str/includes? accept "text/html")) "text/html"
    (and accept (str/includes? accept "text/plain")) "text/plain"
    (= "application/grpc" content-type) "application/grpc"
    (= "application/json" content-type) "application/json"
    :else "text/plain"))

;; waiter error page titles
(def ^:const error-class->error-title
  {error-class-cors-preflight-forbidden "CORS Preflight Forbidden"
   error-class-cors-request-forbidden "CORS Request Forbidden"
   error-class-deployment-error "Service Deployment Error"
   error-class-kerberos-negotiate "Waiter Auth Error: Kerberos Negotiate"
   error-class-kerberos-queue-length "Waiter Auth Error: Kerberos Queue Length Exceeded"
   error-class-maintenance "Service in Maintenance"
   error-class-prestashed-tickets "Waiter Auth Error: Prestashed Tickets"
   error-class-queue-length "Service Queue Length Exceeded"
   error-class-request-timeout "Request Timeout"
   error-class-stream-failure "Request Stream Failure"
   error-class-stream-timeout "Request Stream Timeout"
   error-class-service-forbidden "Service Forbidden"
   error-class-service-misconfigured "Service Configured Incorrectly"
   error-class-service-unidentified "Service Cannot be Identified"
   error-class-suspended "Service Suspended"
   error-class-unsupported-auth "Unsupported Authentication Method"})

(defn- build-error-context
  "Creates a context from a data map and a request.
   The data map is expected to contain the following keys: details, message, and status."
  [{:keys [details message status] :as data-map}
   {:keys [headers query-string request-method request-time support-info uri waiter-images-url] :as request}]
  (let [{:strs [host x-cid]} headers
        {:keys [authorization/principal descriptor instance]} (merge request details)
        {:keys [service-id]} descriptor
        {:keys [error-class waiter/error-image]} details
        error-title (or (error-class->error-title error-class)
                        (str "Waiter Error " status))]
    (cond-> {:cid x-cid
             :details details
             :host host
             :instance-id (:id instance)
             :message message
             :principal principal
             :query-string query-string
             :request-method (-> (or request-method "") name str/upper-case)
             :service-id service-id
             :status status
             :support-info support-info
             :timestamp (du/date-to-str request-time)
             :title error-title
             :uri uri}
      (and waiter-images-url error-image)
      (assoc :image-url (str waiter-images-url "/" error-image)))))

(defn- build-maintenance-context
  "Creates a context from a data map and request.
   The data map is expected to contain the following keys: details, message, and status.
   The key details is a map with keys: name, token and token-owner"
  [{{:keys [name token token-owner]} :details :as data-map} request]
  (merge
    (build-error-context data-map request)
    {:name name :token token :token-owner token-owner}))

(let [html-fn (template/fn
                [{:keys [cid details host image-url instance-id message principal query-string request-method
                         service-id status support-info timestamp title uri]}]
                (slurp (io/resource "web/error.html")))]
  (defn- render-error-html
    "Renders error html"
    [context]
    (html-fn context)))

(let [text-fn (template/fn
                [{:keys [cid details host instance-id message principal query-string request-method
                         service-id status support-info timestamp title uri]}]
                (slurp (io/resource "web/error.txt")))]
  (defn- render-error-text
    "Renders error text"
    [context]
    (text-fn context)))

(let [html-fn (template/fn
                [{:keys [cid host instance-id message name query-string request-method
                         support-info timestamp token token-owner uri]}]
                (slurp (io/resource "web/maintenance.html")))]
  (defn- render-maintenance-mode-html
    "Renders maintenance mode html"
    [context]
    (html-fn context)))

(let [text-fn (template/fn
                [{:keys [cid host instance-id message name query-string request-method
                         support-info timestamp token token-owner uri]}]
                (slurp (io/resource "web/maintenance.txt")))]
  (defn- render-maintenance-mode-text
    "Renders maintenance mode text"
    [context]
    (text-fn context)))

(defn- error-context->json-body
  "Converts the error-context to the response body json string."
  [error-context]
  (json/write-str {:waiter-error error-context}
                  :escape-slash false
                  :key-fn stringify-keys
                  :value-fn stringify-elements))

(defn error-context->html-body
  "Converts the error-context to the response body html string."
  [error-context render-fn]
  (-> error-context
    (update :message escape-html)
    (update :message urls->html-links)
    (update :details #(with-out-str (pprint/pprint %)))
    render-fn))

(defn- error-context->text-body
  "Converts the error-context to the response body plaintext string."
  [error-context render-fn]
  (-> error-context
    (update :details (fn [v]
                       (when v
                         (str/replace (with-out-str (pprint/pprint v)) #"\n" "\n  "))))
    render-fn
    (str/replace #"\n" "\n  ")
    (str/replace #"\n  $" "\n")))

(defn add-grpc-status-headers
  "Finds and attaches the equivalent grpc status codes for the provided http status code."
  [{:keys [headers status] :as response} {:keys [message]}]
  (if-let [grpc-status-data (cond
                              (= status http-400-bad-request) [grpc-3-invalid-argument "Bad Request"]
                              (= status http-401-unauthorized) [grpc-16-unauthenticated "Unauthorized"]
                              (= status http-403-forbidden) [grpc-7-permission-denied "Permission Denied"]
                              (= status http-429-too-many-requests) [grpc-14-unavailable "Too Many Requests"]
                              (= status http-500-internal-server-error) [grpc-13-internal "Internal Server Error"]
                              (= status http-502-bad-gateway) [grpc-14-unavailable "Bad Gateway"]
                              (= status http-503-service-unavailable) [grpc-14-unavailable "Service Unavailable"]
                              (= status http-504-gateway-timeout) [grpc-4-deadline-exceeded "Gateway Timeout"])]
    (let [[grpc-status standard-message] grpc-status-data
          grpc-message (if (string? message) (str/replace message #"\n" "; ") standard-message)
          new-headers (assoc headers
                        "content-type" "application/grpc"
                        "grpc-message" grpc-message
                        "grpc-status" (str grpc-status))]
      (assoc response :headers new-headers))
    response))

(defn attach-grpc-status
  "Attaches grpc-status on Waiter generated responses based on http status codes for grpc requests."
  [response error-context {:keys [client-protocol headers]}]
  (cond-> response
    (hu/grpc? headers client-protocol)
    (add-grpc-status-headers error-context)))

(defn attach-error-class
  "Attaches error-class on Waiter generated responses when it is available in the provided error data."
  [response {:keys [error-class]}]
  (cond-> response
    error-class (assoc :error-class error-class)))

(defn- data-map->response
  "Converts the provided data map and render functions into a ring response.
   The data map is expected to contain the following keys: details, headers, message, and status."
  [{:keys [details headers status] :or {status http-400-bad-request} :as data-map} build-context-fn render-html-fn render-text-fn request]
  (let [error-context (build-context-fn data-map request)
        content-type (request->content-type request)]
    (-> {:body (case content-type
                 ;; grpc error responses should not have a body as the client will try to parse it into a proto object
                 "application/grpc" (byte-array 0) ;; ensures header frames are emitted with endStream=true
                 "application/json" (error-context->json-body error-context)
                 "text/html" (error-context->html-body error-context render-html-fn)
                 "text/plain" (error-context->text-body error-context render-text-fn))
         :headers (-> headers
                    (assoc-if-absent "content-type" content-type))
         :status status}
      (attach-error-class details)
      (attach-grpc-status error-context request)
      (attach-waiter-namespace-keys details)
      (attach-waiter-source))))

(defn data->error-response
  "Converts the provided data map into a ring response and renders as a generic error.
   The data map is expected to contain the following keys: details, headers, message, and status."
  [data-map request]
  (data-map->response data-map build-error-context render-error-html render-error-text request))

(defn data->maintenance-mode-response
  "Converts the provided data map into a ring response and renders as a maintenance mode error.
   The data map is expected to contain the following keys: details, headers, message, and status.
   The message field should correspond with the maintenance message and details is a map with keys:
   name, token and token-owner"
  [data-map request]
  (data-map->response data-map build-maintenance-context render-maintenance-mode-html render-maintenance-mode-text request))

(defn- wrap-unhandled-exception
  "Wraps any exception that doesn't already set status in a parent
  exception with a generic error message and a 500 status."
  [ex]
  (let [{:keys [status] :as error-data} (ex-data ex)]
    (if status
      ex
      (ex-info (str "Internal error: " (.getMessage ex)) (assoc error-data :status http-500-internal-server-error) ex))))

(defn exception->response-metadata
  "Converts an exception into response metadata."
  [^Exception ex]
  (let [wrapped-ex (wrap-unhandled-exception ex)
        {:keys [friendly-error-message headers log-level message status] :as data} (ex-data wrapped-ex)
        response-msg (if (or message friendly-error-message)
                       (str/trim (str message \newline friendly-error-message))
                       (.getMessage wrapped-ex))
        processed-headers (into {} (for [[k v] headers] [(name k) (str v)]))]
    (condp = log-level
      :info (log/info (.getMessage wrapped-ex))
      :warn (log/warn wrapped-ex response-msg)
      (log/error wrapped-ex response-msg))
    {:details data
     :headers processed-headers
     :message response-msg
     :status status}))

(defn exception->response
  "Converts an exception into a ring response."
  [^Exception ex request]
  (-> (exception->response-metadata ex)
      (data->error-response request)))

(defmacro log-and-suppress-when-exception-thrown
  "Executes the body inside a try-catch block and suppresses any thrown exceptions."
  [error-message & body]
  `(try
     ~@body
     (catch Exception e#
       (log/error e# ~error-message))))

;; source: https://github.com/clojure/core.incubator/blob/master/src/main/clojure/clojure/core/incubator.clj#L62
;; clojure.core.incubator
(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn remove-keys
  "Returns a map with any key in ks dissoc-ed from m.
   Provides an alternate to (apply dissoc m ks)."
  [m ks]
  (reduce dissoc m ks))

(defn sleep
  "Helper function that wraps sleep call to java.lang.Thread"
  [time]
  (Thread/sleep time))

(defn retry-strategy
  "Return a retry function using the specified retry config.
   The returned function accepts a no-args function to be executed until it returns without throwing an error.

   `delay-multiplier` each previous delay is multiplied by delay-multiplier to generate the next delay.
   `initial-delay-ms` the initial delay for the first retry.
   `max-attempts`     limit the number of attempts.
   `max-delay-ms`     the delay cap for exponential backoff delay.
   `max-retries`      DEPRECATED. use more accurately named max-attempts.
   `retry?-fn`        predicate taking a result (not successful), returns true if it should be retried.
   "
  [{:keys [delay-multiplier initial-delay-ms max-delay-ms max-retries max-attempts retry?-fn]
    :or {delay-multiplier 1.0
         initial-delay-ms 100
         max-delay-ms 300000 ; 300k millis = 5 minutes
         max-retries 10
         max-attempts max-retries
         retry?-fn (constantly true)}}]
  (fn [body-function]
    (loop [num-tries 1
           current-delay-ms initial-delay-ms]
      (let [{:keys [success result]}
            (try
              {:success true, :result (body-function)}
              (catch Exception ex
                {:success false, :result ex}))]
        (cond
          success result
          (and (retry?-fn result) (< num-tries max-attempts))
          (let [delay-ms (long (min max-delay-ms current-delay-ms))]
            (log/info "sleeping" delay-ms "ms before retry" (str "#" num-tries))
            (sleep delay-ms)
            (recur (inc num-tries) (* delay-ms delay-multiplier)))
          :else (throw result))))))

(defn async-retry-strategy
  "Return a async retry function using the specified retry config.
   The returned function accepts a no-args async body-function to be executed. The returned function returns a channel
   where the result of the retry strategy will be sent to.
   The body-function must return a channel (i.e. async/go). If the channel receives an Exception, the body-function
   will be considered for retry. Otherwise the message received from the channel will be returned.

   `delay-multiplier` each previous delay is multiplied by delay-multiplier to generate the next delay.
   `initial-delay-ms` the initial delay for the first retry.
   `max-delay-ms` the delay cap for exponential backoff delay.
   `max-retries`  limit the number of retries.
   "
  [{:keys [delay-multiplier initial-delay-ms max-delay-ms max-retries]
    :or {delay-multiplier 1.0
         initial-delay-ms 100
         max-delay-ms 300000 ; 300k millis = 5 minutes
         max-retries 10}}]
  (fn [body-function]
    (async/go-loop [num-tries 1
                    current-delay-ms initial-delay-ms]
      (let [result (async/<! (body-function))
            success (not (instance? Throwable result))]
        (cond
          success result
          (>= num-tries max-retries) result
          :else (let [delay-ms (long (min max-delay-ms current-delay-ms))]
                  (log/info "sleeping" delay-ms "ms before retry" (str "#" num-tries))
                  (async/<! (async/timeout delay-ms))
                  (recur (inc num-tries) (* delay-ms delay-multiplier))))))))

(defn unique-identifier
  "Generates a new unique id using the time and a random value.
   Faster than UUID/randomUUID, but not necessarily globally unique."
  []
  (let [thread-local-random (ThreadLocalRandom/current)]
    (str (Long/toString (System/nanoTime) 16) "-" (Long/toString (.nextLong thread-local-random Long/MAX_VALUE) 16))))

(defn deep-sort-map
  "Deep sorts entries in the map by their keys."
  [input-map]
  (walk/postwalk #(if (map? %) (into (sorted-map) (remove (comp nil? val) %)) %)
                 (into (sorted-map) input-map)))

(defn deep-merge-maps
  "Deep merges corresponding leaf entries in the two input maps using the provided `merge-fn`"
  [merge-fn map-1 map-2]
  (merge-with
    (fn [x y]
      (if (and (map? x) (map? y))
        (deep-merge-maps merge-fn x y)
        (merge-fn x y)))
    map-1 map-2))

(defn map->compressed-bytes
  "Compresses the data into a byte array along with encryption."
  [data-map encryption-key]
  (let [data-bytes (nippy/freeze data-map {:password encryption-key, :compressor compression/lzma2-compressor})]
    (ByteBuffer/wrap data-bytes)))

(defn- data->byte-array
  "Converts a byte buffer to a byte array"
  ^bytes [byte-buffer]
  (.clear byte-buffer)
  (let [result-bytes (byte-array (.capacity byte-buffer))]
    (.get byte-buffer result-bytes)
    result-bytes))

(defn bytes->str
  "Constructs a new String by decoding the specified array of bytes."
  [byte-array]
  (String. ^bytes byte-array "utf-8"))

(defn compressed-bytes->map
  "Decompresses the byte array and converts it into a clojure data-structure."
  [byte-buffer decryption-key]
  (let [data-bytes (data->byte-array byte-buffer)]
    (nippy/thaw data-bytes {:password decryption-key, :compressor compression/lzma2-compressor})))

(defn map->base-64-string
  "Serializes data to a base 64 string along with encryption."
  [data-map encryption-key]
  (let [encrypted-bytes (nippy/freeze data-map {:compressor nil :password encryption-key})]
    (.encodeToString (Base64/getUrlEncoder) encrypted-bytes)))

(defn base-64-string->map
  "Deserializes and decrypts a base 64 string."
  [^String b64-string decryption-key]
  (let [decoded-bytes (.decode (Base64/getUrlDecoder) b64-string)]
    (nippy/thaw decoded-bytes {:compressor nil :password decryption-key :v1-compatibility? false})))

(defn b64-encode-sha256
  "Returns the url encoding of the input string using SHA256."
  [clear-text]
  (let [clear-text-bytes (.getBytes clear-text StandardCharsets/UTF_8)
        message-digest (MessageDigest/getInstance "SHA-256")
        sha256-bytes (.digest message-digest clear-text-bytes)
        b64-encoder (.withoutPadding (Base64/getUrlEncoder))
        result-bytes (.encode b64-encoder sha256-bytes)]
    (String. result-bytes)))

(defn encode-url-safe-b64
  "Performs url safe base 64 encoding of input string."
  [^String data-string]
  (.encodeToString (Base64/getUrlEncoder) (.getBytes data-string)))

(defn decode-url-safe-b64
  "Performs url safe base 64 decoding of input string."
  [^String data-string]
  (String. ^bytes (.decode (Base64/getUrlDecoder) data-string)))

(let [messages (atom {})]
  (defn message
    "Returns the message corresponding to the provided key"
    [key]
    (@messages key))

  (defn formatted-message
    "Returns the message corresponding to the provided key with placeholders from the context replaced."
    [key context]
    (when-let [template-message (message key)]
      (loop [[[k v] & remaining-context] (seq context)
             loop-message template-message]
        (if (nil? k)
          loop-message
          (recur remaining-context
                 (str/replace loop-message (str "{" k "}") v))))))

  (defn load-messages
    "Loads m into the messages map"
    [m]
    (reset! messages m)
    (log/info "Messages have been initialized to" @messages)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn parse-boolean
  "Parses the string argument as a boolean value."
  [value]
  (Boolean/parseBoolean (str value)))

(defn request-flag
  "Returns true if and only if flag is present and
  'true' in the provided request params"
  [params flag]
  (parse-boolean (get params flag "false")))

(defn parse-int
  "Returns either the input as an integer or nil if there was an error in parsing."
  [value]
  (try
    (when value
      (Integer/parseInt (str value)))
    (catch Exception _
      (log/info "cannot convert value to an int:" value)
      nil)))

(defn parse-double
  "Returns either the input as an double or nil if there was an error in parsing."
  ([value]
   (parse-double value 0))
  ([value default-value]
   (try
     (when value
       (Double/parseDouble (str value)))
     (catch Exception _
       (log/info "cannot convert value to a double:" value)
       default-value))))

(defn param-contains?
  "Returns true if and only if request parameter k is present in params and has a value equal to v."
  [params k v]
  (let [param-value (get params k)]
    (cond
      (string? param-value) (= param-value v)
      (seq param-value) (->> param-value seq (some #(= v %)))
      :else false)))

(defn authority->host
  "Retrieves the host from the authority."
  [authority]
  (let [port-index (str/index-of (str authority) ":")]
    (cond-> authority port-index (subs 0 port-index))))

(defn authority->port
  "Retrieves the port from the authority."
  [authority & {:keys [default]}]
  (let [port-index (str/index-of (str authority) ":")]
    (if port-index (subs authority (inc port-index)) (str default))))

(defn uri-string->host
  "Parses the uri-string as a URI and extracts the authority from it."
  [uri-string]
  ;; we do not use (.getHost) on URI directly since our test tokens don't parse as hosts
  (some-> uri-string (URI.) (.getAuthority) (authority->host)))

(defn request->scheme
  "Extracts the scheme from the request, and returns it as a keyword."
  [{:keys [headers scheme]}]
  (let [{:strs [x-forwarded-proto]} headers]
    (or (some-> x-forwarded-proto str/lower-case keyword)
        scheme)))

(defn request->host
  "Extracts the host header from the request."
  [request]
  (get-in request [:headers "host"]))

(defn request->discovered-token
  "Extracts the discovered token from the request."
  [request]
  (get-in request [:waiter-discovery :token]))

(defn same-origin
  "Returns true if the host and origin are non-nil and are equivalent."
  [{:keys [headers] :as request}]
  (let [{:strs [host origin]} headers
        scheme (request->scheme request)]
    (when (and host origin scheme)
      (= origin (str (name scheme) "://" host)))))

(defn resolve-symbol
  "Resolve the given symbol to the corresponding Var."
  [sym]
  {:pre [(symbol? sym)]}
  (if-let [target-ns (some-> sym namespace symbol)]
    (require target-ns)
    (log/warn "Unable to load namespace for symbol" sym))
  (log/info "Dynamically loading Clojure var:" sym)
  (resolve sym))

(defn resolve-symbol!
  "Resolve the given symbol to the corresponding Var. Throw an exception if resolved object is falsy."
  [sym]
  (if-let [resolved-sym (resolve-symbol sym)]
    resolved-sym
    (throw (ex-info "Unable to resolve symbol" {:sym sym :namespace (namespace sym)}))))

(defn create-component
  "Creates a component based on the specified :kind"
  [{:keys [kind] :as component-config} & {:keys [context]}]
  (log/info "component:" kind "with config" component-config (if context (str "and context " context) ""))
  (let [kind-config (get component-config kind)
        factory-fn (:factory-fn kind-config)]
    (if factory-fn
      (if-let [resolved-fn (resolve-symbol factory-fn)]
        (resolved-fn (merge context kind-config))
        (throw (ex-info "Unable to resolve factory function" (assoc component-config :ns (namespace factory-fn)))))
      (throw (ex-info "No :factory-fn specified" component-config)))))

(defn port-available?
  "Returns true if port is not in use"
  [port]
  (try
    (let [ss (ServerSocket. port)]
      (.setReuseAddress ss true)
      (.close ss)
      true)
    (catch Exception _
      false)))

(defn request->debug-enabled?
  "Parses the request header to determine if debug mode has been enabled."
  [request]
  (boolean (get-in request [:headers "x-waiter-debug"])))

(defn merge-by
  "Returns a map that consists of the rest of the maps conj-ed onto the first.
   If a key occurs in more than one map,
   the mapping(s) from the latter (left-to-right) will be combined with
   the mapping in the result by calling (f key val-in-result val-in-latter)."
  {:added "1.0"
   :static true}
  [f & maps]
  (when (some identity maps)
    (let [merge-entry (fn [m e]
                        (let [k (key e)
                              v (val e)]
                          (if (contains? m k)
                            (assoc m k (f k (get m k) v))
                            (assoc m k v))))
          merge2 (fn [m1 m2]
                   (reduce merge-entry (or m1 {}) (seq m2)))]
      (reduce merge2 maps))))

(defn wrap-identity
  "A wrapper middleware that does nothing."
  [handler]
  handler)

(defn update-exception
  "Updates an exception, regardless of whether it's an ExceptionInfo or just Exception."
  [^Exception e update-fn]
  (if (instance? ExceptionInfo e)
    (ex-info (.getMessage e) (update-fn (ex-data e)) (or (.getCause e) e))
    (ex-info (.getMessage e) (update-fn {}) e)))

(defn scale-amount->scaling-state
  "Determines the scale mode from the scaling-amount."
  [scale-amount]
  (cond
    (pos? scale-amount) :scale-up
    (neg? scale-amount) :scale-down
    :else :stable))

(defn sanitize-history
  "Limits the history length stored in the nested map."
  [data history-length history-key]
  (dissoc-in data (repeat history-length history-key)))

(defn principal->username
  "Extracts the user name from the provided principal by dropping the domain part."
  [principal]
  (when principal
    (first (str/split principal #"@" 2))))

(defn chan-to-seq!!
  "Takes a channel and returns a lazy sequence of channel messages"
  [c]
  (lazy-seq
    (when-some [v (async/<!! c)]
      (cons v (chan-to-seq!! c)))))

(defn chan-to-json-seq!!
  "Takes a channel of string fragments and returns a lazy sequence of parsed json blobs"
  [c]
  (->> c
       chan-to-seq!!
       (map (fn [chunk] (-> chunk .getBytes ByteArrayInputStream.)))
       Collections/enumeration
       SequenceInputStream.
       ; need to convert channel to an InputStreamReader to use underlying stream json parsing library
       InputStreamReader.
       cheshire/parsed-seq))

(defn send-event-to-channels!
  "Given a list of watch channels and the event to send to each channel, send the event in a non blocking fashion and
  close channels that error the async/put! operation. Returns the set of open channels."
  [watch-chans event]
  (reduce
    (fn send-event! [open-chans watch-chan]
      (try
        (if (async/put! watch-chan event)
          (conj open-chans watch-chan)
          (do
            (log/info "removing closed watch-chan" watch-chan)
            open-chans))
        (catch AssertionError e
          (log/error e "removing and closing watch-chan" watch-chan)
          (async/close! watch-chan)
          open-chans)))
    #{}
    watch-chans))

(defn str->filter-fn
  "Returns a value-filtering function given a user-provided value as filter string."
  [value star-means-all?]
  (let [value-str (str value)
        pattern (when-not (or (= "" value-str) (= "*" value-str))
                  (try
                    (re-pattern value-str)
                    (catch Exception _
                      nil)))]
    #(or (= value-str %)
         (when (and star-means-all? (= "*" value-str))
           (pos? (count (str %))))
         (and pattern (re-matches pattern %)))))

(defn strs->filter-fn
  "Returns a value-filtering function that matches on any of the given sequence of user-provided values as filter string."
  [values star-means-all?]
  (let [filter-fns (map #(str->filter-fn % star-means-all?) values)]
    (fn [value] (some #(%1 value) filter-fns))))

(defn match-yes-like
  "Check if string matches a yes-like value.
   Case-insensitive matches prefixes: ENABLE, TRUE, YES, ON
   Returns input string on match, nil otherwise."
  [s]
  (when (re-find #"^(?:[EeTtYy]|[Oo][Nn])" (str s)) s))

(defn match-no-like
  "Check if string matches a no-like value.
   Case-insensitive matches prefixes: DISABLE, FALSE, NO, OFF
   Returns input string on match, nil otherwise."
  [s]
  (when (re-find #"^(?:[DdFfNn]|[Oo][Ff])" (str s)) s))

(defn raven-response-flags
  "Returns the response flags from a backend sidecar proxy,
   and nil when no flags were set or no backend proxy is present.
   See the Envoy Proxy documentation for details on the response flags format:
   https://www.envoyproxy.io/docs/envoy/latest/configuration/observability/access_log/usage#config-access-log-format-response-flags"
  [response]
  (when-let [result (get-in response [:headers "x-raven-response-flags"])]
    (when (not= result envoy-empty-response-flags)
      result)))

(defn raven-status-details
  "Returns the response status details from a backend sidecar proxy,
   and nil when no flags were set or no backend proxy is present.
   See the Envoy Proxy documentation for details on the response flags format:
   https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_conn_man/response_code_details"
  [response]
  (get-in response [:headers "x-raven-status-code-details"]))

(defn raven-proxy-response?
  "Returns true if the response is from a backend sidecar proxy."
  [response]
  (boolean (get-in response [:headers "x-raven-response-flags"])))

(defn slurp-bytes
  "Slurp the bytes from an input stream."
  [^InputStream in]
  (with-open [out (ByteArrayOutputStream.)]
    (io/copy (io/input-stream in) out)
    (.toByteArray out)))

(defn load-resource-bytes
  "Returns bytes for the provided resource."
  [resource-path]
  (let [class-loader (.getContextClassLoader (Thread/currentThread))
        resource-stream (.getResourceAsStream class-loader resource-path)]
    (slurp-bytes resource-stream)))

(defn compute-error-delay-ms
  "Computes the delay in milliseconds that will be introduced into an error response."
  [{:keys [max-delay-ms step-delay-ms step-size-per-min]} errors-per-min]
  (-> errors-per-min (/ step-size-per-min) (int) (* step-delay-ms) (min max-delay-ms)))
