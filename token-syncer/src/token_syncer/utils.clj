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
(ns token-syncer.utils
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defn retrieve-git-version
  []
  (try
    (let [git-log-resource (io/resource "git-log")
          git-version-str (if git-log-resource
                            (do
                              (log/info "Retrieving version from git-log file")
                              (slurp git-log-resource))
                            (do
                              (log/info "Attempting to retrieve version from git repository")
                              (str/trim (:out (sh/sh "git" "rev-parse" "HEAD")))))]
      (log/info "Git version:" git-version-str)
      git-version-str)
    (catch Exception e
      (log/error e "version unavailable")
      "n/a")))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn truncate [in-str max-len]
  (let [ellipsis "..."
        ellipsis-len (count ellipsis)]
    (if (and (string? in-str) (> (count in-str) max-len) (> max-len ellipsis-len))
      (str (subs in-str 0 (- max-len ellipsis-len)) ellipsis)
      in-str)))

(defn- exception->strs
  "Converts the exception stacktrace into a string list."
  [e]
  (let [ex-data (ex-data e)
        ex-data-str (str ex-data)
        exception-to-list-fn (fn [ex] (when ex
                                        (cons (str (.getMessage ex) (if (str/blank? ex-data-str) "" (str " " ex-data-str)))
                                              (into [] (map str (.getStackTrace ^Throwable ex))))))]
    (vec (concat (exception-to-list-fn e) (exception-to-list-fn (.getCause e))))))

(defn stringify-elements
  [k v]
  (if (vector? v)
    (map (partial stringify-elements k) v)
    (cond
      (instance? Exception v) (->> (exception->strs v)
                                   (str/join \space))
      :else v)))

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
