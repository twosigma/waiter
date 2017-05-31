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
(ns waiter.utils
  (:require [chime]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [digest]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.compression :as compression])
  (:import clojure.core.async.impl.channels.ManyToManyChannel
           clojure.lang.PersistentQueue
           java.nio.ByteBuffer
           java.util.UUID
           java.util.concurrent.ThreadLocalRandom
           java.util.regex.Pattern
           (org.joda.time DateTime ReadablePeriod)))

(defn select-keys-pred
  "Returns a map with only the keys, k, for which (pred k) is true."
  [pred m]
  (reduce-kv (fn [m k v]
               (if (pred k)
                 (assoc m k v)
                 m))
             {} m))

(defn keys->nested-map
  "Takes a map with string keys and returns a map with a nested structure where
   the string keys were split using the regex `key-split` to create the nested
   structure.

   Example:

   (keys->nested-map {\"this.is.an.example\" 1 \"this.is.an.example2\" 2} #\"\\.\")
   => {\"this\" {\"is\" {\"an\" {\"example\" 1 \"example2\" 2}}}}"
  [original-map key-split]
  (reduce-kv (fn [m k v]
               (assoc-in m (clojure.string/split k key-split) v))
             {} original-map))

(defmacro filterm
  "Returns a map of the entries in map for which (pred entry) returns true.
   pred must be free of side-effects.
   Inspired from filterv.
   (filterm pred map) ==> (into {} (filter pred map))"
  [pred map]
  `(into {} (filter ~pred ~map)))

(defn is-uuid?
  "Returns true if `s` is an uuid"
  [s]
  (if (instance? UUID s)
    s
    (try
      (UUID/fromString s)
      (catch Exception _
        nil))))


(defn atom-cache-get-or-load
  "Gets a value from a cache based upon the key.
   On cache miss, call get-fn with the key and place result into the cache in {:data value} form.
   This allows us to handle nil values as results of the get-fn."
  [cache key get-fn]
  (let [d (delay (get-fn))
        _ (swap! cache #(cache/through (fn [_] {:data @d}) % key))
        out (cache/lookup @cache key)]
    (if-not (nil? out) (:data out) @d)))

(defn atom-cache-evict
  "Evicts a key from an atom-based cache."
  [cache key]
  (swap! cache #(cache/evict % key)))


(defn resolve-fn
  "Accepts either a clojure function or a namespaced function name.
  If the input is a clojure function, this function just return the input clojure function.
  If the input is a namespaced function name, this function resolves it and returns the clojure function."
  [function-arg]
  (if (fn? function-arg)
    function-arg
    (let [function-name (str function-arg)
          index-of-slash (.lastIndexOf function-name "/")
          result-fn (if (neg? index-of-slash)
                      (do
                        (log/fatal "Function name must be namespaced, e.g. waiter.password-store/->ConfiguredPasswordProvider")
                        (System/exit 1))
                      (let [lookup-env (use (symbol (subs function-name 0 index-of-slash)))
                            fn-symbol (symbol (subs function-name (inc index-of-slash)))]
                        ; `(resolve (read-string function-name))` worked in unit tests but not when using `lein run`
                        (resolve lookup-env fn-symbol)))]
      (when (nil? result-fn)
        (log/fatal "Unresolved function:" function-name)
        (System/exit 1))
      result-fn)))

(defn evaluate-config-fn
  "A common pattern in our configuration is a map that 
  contains a key :custom-impl with a value that names a 
  function to be called.  The function is called with the
  same config map."
  [config]
  (let [fully-qualified-factory-name (:custom-impl config)
        factory-fn (resolve-fn fully-qualified-factory-name)]
    (factory-fn config)))

(defn extract-expired-keys
  "Extracts the expired keys from the input map (key->expiry time) given the expiry time."
  [input-map expiry-time]
  (->> input-map
       (remove
         (fn [[k v]]
           (let [alive? (or (nil? expiry-time)
                            (nil? v)
                            (t/after? v expiry-time))]
             (when-not alive? (log/info "Filtering expired entry:" (str "[" k "->" v "]")))
             alive?)))
       (map first)))

(defn truncate [in-str max-len]
  (let [ellipsis "..."
        ellipsis-len (count ellipsis)]
    (if (and (string? in-str) (> (count in-str) max-len) (> max-len ellipsis-len))
      (str (subs in-str 0 (- max-len ellipsis-len)) ellipsis)
      in-str)))

(defn date-to-str [date-time]
  (f/unparse
    (f/with-zone (f/formatter "yyyy-MM-dd HH:mm:ss.SSS") (t/default-time-zone))
    date-time))

(defn non-neg? [x]
  (or (zero? x) (pos? x)))

(defn generate-secret-word
  [src-id dest-id processed-passwords]
  (let [password (second (first processed-passwords))
        secret-word (digest/md5 (str src-id ":" dest-id ":" password))]
    (log/info "generate-secret-word" [src-id dest-id] "->" secret-word)
    secret-word))

(defn stringify-elements
  [k v]
  (if (vector? v)
    (map (partial stringify-elements k) v)
    (cond
      (instance? DateTime v) (date-to-str v)
      (instance? UUID v) (str v)
      (instance? Pattern v) (str v)
      (instance? PersistentQueue v) (vec v)
      (instance? ManyToManyChannel v) (str v)
      (= k :time) (str v)
      :else v)))

(defn map->json-response
  "Convert the input data into a json response."
  [data-map & {:keys [status] :or {status 200}}]
  {:body (json/write-str data-map :value-fn stringify-elements)
   :status status
   :headers {"Content-Type" "application/json"}})

(defn exception->strs
  "Converts the exception stacktrace into a string list."
  [^Exception e]
  (let [ex-data (ex-data e)
        ex-data-list (map (fn [[key value]] (str (name key) ": " (str value))) ex-data)
        exception-to-list-fn (fn [^Exception ex] (when ex
                                                   (concat (cons (.getMessage ex) ex-data-list)
                                                           (vec (map str (.getStackTrace ^Throwable ex))))))]
    (or (:friendly-error-message ex-data)
        (vec (concat (exception-to-list-fn e) (exception-to-list-fn (.getCause e)))))))

(defn exception->response
  "Converts an exception into a 400 plain text response with the stack trace."
  [log-message ^Exception e & {:keys [headers status] :or {headers {}, status 400}}]
  (let [processed-headers (into {} (for [[k v] headers] [(name k) (str v)]))
        ex-data (ex-data e)]
    (when-not (:supress-logging ex-data)
      (log/error e log-message processed-headers))
    {:body (:message ex-data (str/join (System/lineSeparator) (exception->strs e)))
     :status (:status ex-data status)
     :headers (merge {"Content-Type" "text/plain"} processed-headers)}))

(defn exception->json-response
  "Convert the input data into a json response."
  [^Exception e & {:keys [status] :or {status 400}}]
  {:body (json/write-str {:exception (exception->strs e)} :value-fn stringify-elements)
   :status (:status (ex-data e) status)
   :headers {"Content-Type" "application/json"}})

(defmacro log-and-suppress-when-exception-thrown
  "Executes the body inside a try-catch block and suppresses any thrown exceptions."
  [error-message & body]
  `(try
     ~@body
     (catch Exception e#
       (log/error e# ~error-message))))

(defn time-seq
  "Returns a sequence of date-time values growing over specific period.
  Takes as input the starting value and the growing value, returning a
  lazy infinite sequence."
  [start ^ReadablePeriod period]
  (iterate (fn [^DateTime t] (.plus t period)) start))

(defn start-timer-task
  "Executes the callback functions sequentially as specified intervals. Returns
  a function that will cancel the timer when called."
  [interval-period callback-fn & {:keys [delay-ms] :or {delay-ms 0}}]
  (chime/chime-at
    (time-seq (t/plus (t/now) (t/millis delay-ms)) interval-period)
    (fn [_] (callback-fn))
    {:error-handler (fn [ex] (log/error ex (str "Exception in timer task.")))}))

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

(defn sleep
  "Helper function that wraps sleep call to java.lang.Thread"
  [time]
  (Thread/sleep time))

(defn retry-strategy
  "Return a retry function using the specified retry config.
   The returned function accepts a no-args function to be executed until it returns without throwing an error.

   `delay-multiplier` each previous delay is multiplied by delay-multiplier to generate the next delay.
   `inital-delay-ms` the initial delay for the first retry.
   `max-retries`  limit the number of retries.
   "
  [{:keys [delay-multiplier inital-delay-ms max-retries]
    :or {delay-multiplier 1.0
         inital-delay-ms 100
         max-retries 10}}]
  (fn [body-function]
    (loop [num-tries 1]
      (let [{:keys [success result]}
            (try
              {:success true, :result (body-function)}
              (catch Exception ex
                {:success false, :result ex}))]
        (cond
          success result
          (>= num-tries max-retries) (throw result)
          :else (let [delay-ms (long (* inital-delay-ms
                                        (Math/pow delay-multiplier (dec num-tries))))]
                  (log/info "sleeping" delay-ms "ms before retry" (str "#" num-tries))
                  (sleep delay-ms)
                  (recur (inc num-tries))))))))

(defn unique-identifier
  "Generates a new unique id using the time and a random value."
  []
  (let [thread-local-random (ThreadLocalRandom/current)]
    (str (Long/toString (System/nanoTime) 16) "-" (Long/toString (.nextLong thread-local-random Long/MAX_VALUE) 16))))

(defn older-than? [current-time duration {:keys [started-at]}]
  (if (and duration (not (empty? started-at)))
    (t/after? current-time
              (t/plus (f/parse (f/formatters :date-time) started-at)
                      duration))
    false))

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

(defn compressed-bytes->map
  "Decompresses the byte array and converts it into a clojure data-structure."
  [byte-buffer decryption-key]
  (let [data-bytes (data->byte-array byte-buffer)]
    (nippy/thaw data-bytes {:password decryption-key, :compressor compression/lzma2-compressor})))

(defn compute-help-required
  "Computes the number of slots (requests that can be made to instances) of help required at a router given the values for:
     outstanding: the number of outstanding requests at the router;
     slots-available: the number of slots available (where available = not in use and not blacklisted) from those assigned
                      to the router by the distrbution algorithm;
     slots-in-use: the number of slots used by the router from those that were assigned to it by the distribution
                   algorithm at some point in time, it may include slots from instances that the router no longer owns; and
     slots-offered: the number of slots offered as help to the router from other routers via work-stealing.
   The slots-in-use allows us to account for instances being used by a router that it no longer owns.
   If the function returns positive, say +x, it means the router needs x slots of help to service requests.
   If the function returns zero, it means the router does not need help.
   If the function returns negative, say -x, then the router needs no help and has x extra unused slots that were
   either assigned to it by the distribution algorithm or received from work-stealing offers."
  ([{:strs [outstanding slots-available slots-in-use slots-offered]
     :or {outstanding 0, slots-available 0, slots-in-use 0, slots-offered 0}}]
   (compute-help-required slots-in-use slots-available slots-offered outstanding))
  ([slots-in-use slots-available slots-offered outstanding]
   (- outstanding (+ slots-in-use slots-available slots-offered))))

(let [messages (atom {})]
  (defn message
    "Returns the message corresponding to the provided key"
    [key]
    (@messages key))

  (defn load-messages
    "Loads m into the messages map"
    [m]
    (reset! messages m)
    (log/info "Messages have been initialized to" @messages)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn request-flag
  "Returns true if and only if flag is present and
  'true' in the provided request params"
  [params flag]
  (Boolean/parseBoolean (str (get params flag "false"))))
