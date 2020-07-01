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
(ns token-syncer.file-utils
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (java.io File)))

(defn read-from-file
  "Reads the contents of the file as a json string."
  [filename]
  (if (.exists ^File (io/as-file filename))
    (do
      (log/info "reading contents from" filename)
      (let [file-content (slurp filename)
            result (if (str/blank? file-content) {} (json/read-str file-content))]
        (if (and (map? result)
                 (every? string? (keys result)))
          (do
            (log/info "found" (count result) "token(s) in" filename ":" (-> result  keys sort))
            result)
          (log/warn "contents of" filename "will be ignored as it is not a map with string keys:" result))))
    (log/warn "not reading contents from" filename "as it does not exist!")))

(defn write-to-file
  "Writes the content to the specified file as a json string."
  [filename content]
  (log/info "writing" (count content) "token(s) to" filename ":" (-> content keys sort))
  (->> content
    json/write-str
    (spit filename)))

(defn init-file-operations-api
  "Creates the map of methods used to read/write tokens from/to a file."
  [dry-run]
  {:read-from-file read-from-file
   :write-to-file (if dry-run
                    (fn write-to-file-dry-run-version [filename token->token-description]
                      (log/info "[dry-run] writing to" filename "content:" token->token-description))
                    write-to-file)})

(defn create-temp-file
  "Creates a temporary file."
  []
  (File/createTempFile "temp-" ".json"))

(defn file->path
  "Returns the absolute path of the file."
  [^File file]
  (.getAbsolutePath file))

(defmacro with-temp-file
  "Uses a temporary file for some content and deletes it immediately after running the body."
  [bindings & body]
  (cond
    (and (= (count bindings) 2) (symbol? (bindings 0)))
    `(let ~bindings
       (try
         ~@body
         (finally
           (.delete ~(bindings 0)))))
    :else (throw (IllegalArgumentException.
                   "with-temp-file requires a single Symbol in bindings"))))
