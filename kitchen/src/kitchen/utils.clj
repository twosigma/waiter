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
(ns kitchen.utils
  ; Despite not being explicitly used, the clojure.core.async
  ; require is needed because lein uberjar fails in some
  ; environments without it (https://github.com/twosigma/waiter/pull/10).
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str])
  (:import clojure.core.async.impl.channels.ManyToManyChannel
           (java.util Arrays UUID)
           java.util.regex.Pattern
           org.joda.time.DateTime))

(defn date-to-str [date-time & {:keys [format] :or {format "yyyy-MM-dd HH:mm:ss"}}]
  (f/unparse
    (f/with-zone (f/formatter format) (t/default-time-zone))
    date-time))

(defn stringify-elements
  [k v]
  (if (vector? v)
    (map (partial stringify-elements k) v)
    (cond
      (instance? DateTime v) (date-to-str v)
      (instance? UUID v) (str v)
      (instance? Pattern v) (str v)
      (instance? ManyToManyChannel v) (str v)
      (= k :time) (str v)
      :else v)))

(defn- exception->strs
  "Converts the exception stacktrace into a string list."
  [e]
  (let [ex-data (ex-data e)
        ex-data-str (str ex-data)
        exception-to-list-fn (fn [ex] (when ex
                                        (cons (str (.getMessage ex) (if (str/blank? ex-data-str) "" (str " " ex-data-str)))
                                              (into [] (map str (.getStackTrace ^Throwable ex))))))]
    (if (:friendly-error-message ex-data)
      (:friendly-error-message ex-data)
      (vec (concat (exception-to-list-fn e) (exception-to-list-fn (.getCause e)))))))

(defn exception->json-response
  "Convert the input data into a json response."
  [e & {:keys [status] :or {status 500}}]
  {:body (json/write-str {:exception (exception->strs e)} :value-fn stringify-elements)
   :status status
   :headers {"Content-Type" "application/json"}})

(defn map->json-response
  "Convert the input data into a json response."
  [data-map & {:keys [status] :or {status 200}}]
  {:body (json/write-str data-map :value-fn stringify-elements)
   :status status
   :headers {"Content-Type" "application/json"}})

(defn parse-positive-int 
  "Parse a positive integer.  Return a default value if parsing fails."
  [input default]
  (let [proc-input (re-matches #"[\d]+" input)]
    (if proc-input (read-string proc-input) default)))

(defn- randomly-populate-array
  "Randomly updates elements in the array from a randomly chosen element from the seed-data."
  [array-length result-array seed-data]
  (loop [remaining-iters (min 4096 array-length)]
    (when (pos? remaining-iters)
      (aset result-array (rand-int array-length) (rand-nth seed-data))
      (recur (dec remaining-iters)))))

(defn generate-random-string
  "Generates a random string of the specified length."
  [string-length]
  (let [result-chars (char-array string-length)
        all-chars (vec (map char (range (-> \A int) (-> \Z int inc))))]
    (Arrays/fill result-chars \A)
    (randomly-populate-array string-length result-chars all-chars)
    (String. ^chars result-chars)))

(defn generate-random-byte-array
  "Generates a random byte array of the specified length."
  [array-length]
  (let [result-bytes (byte-array array-length)
        all-bytes (vec (map byte (range 0 128)))]
    (randomly-populate-array array-length result-bytes all-bytes)
    result-bytes))
