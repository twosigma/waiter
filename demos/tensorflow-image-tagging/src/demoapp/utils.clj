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
(ns demoapp.utils
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

(defn stringify-elements
  [k v]
  (if (vector? v)
    (map (partial stringify-elements k) v)
    v))

(defn- exception->strs
  "Converts the exception stacktrace into a string list."
  [e]
  (let [ex-data (ex-data e)
        ex-data-str (str ex-data)]
    (letfn [(exception-to-list-fn [ex]
              (when ex
                (cons (str (.getMessage ex) (if (str/blank? ex-data-str) "" (str " " ex-data-str)))
                      (into [] (map str (.getStackTrace ^Throwable ex))))))]
      (vec (concat (exception-to-list-fn e) (exception-to-list-fn (.getCause e)))))))

(defn exception->json-response
  "Convert the input data into a json response."
  [e & {:keys [status] :or {status 500}}]
  {:body (json/write-str {:exception (exception->strs e)} :value-fn stringify-elements)
   :status status
   :headers {"content-type" "application/json"}})

(defn map->json-response
  "Convert the input data into a json response."
  [data-map & {:keys [status] :or {status 200}}]
  {:body (json/write-str data-map :value-fn stringify-elements)
   :status status
   :headers {"content-type" "application/json"}})
