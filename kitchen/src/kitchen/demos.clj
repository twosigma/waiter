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
(ns kitchen.demos
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [kitchen.utils :as utils])
  (:import (java.io File)))

(defn request->cid [request]
  (get-in request [:headers "x-cid"]))

(defn- request->cid-string [request]
  (str "[CID=" (request->cid request) "]"))

(defn printlog
  ([request & messages]
   (log/info (request->cid-string request) (str/join " " messages))))

(defn image-search-handler
  ""
  [request]
  )

(defn- directory-location
  [dir-name]
  (let [base-dir (or (System/getenv "MESOS_DIRECTORY") (System/getProperty "user.dir"))]
    (str base-dir File/separator dir-name)))

(defn- copy-uri-to-file
  [uri file]
  (with-open [in (io/input-stream uri)
              out (io/output-stream file)]
    (io/copy in out)))

(defn image-tagging-handler
  "Performs image tagging using tensorflow."
  [{:keys [query-params] :as request}]
  (printlog request "query params:" query-params)
  (try
    (let [{:strs [id name url predictions]} query-params
          image-location (directory-location (str "images" File/separator name))
          classifier-file-name "classify_image.py"
          classifier-file (directory-location (str "images" File/separator classifier-file-name))]

      (-> image-location str File. .getParentFile .mkdirs)
      (-> classifier-file str File. .getParentFile .mkdirs)

      (printlog request "downloading" url "to" image-location)
      (copy-uri-to-file url image-location)

      (printlog request "classifier resource:" (io/resource classifier-file-name))
      (when-not (-> classifier-file str File. .exists)
        (printlog request "saving" classifier-file-name "to" classifier-file)
        (copy-uri-to-file (io/resource classifier-file-name) classifier-file))

      (let [{:keys [err exit out]}
            (sh/sh "python3" classifier-file (str "--image_file=" image-location) (str "--num_top_predictions=" predictions))
            result-lines (filter #(str/includes? % "(score =") (str/split-lines out))]
        (printlog request "classifier exit code:" exit)
        (printlog request "classifier stdout:" out)
        (printlog request "classifier stderr:" err)
        (utils/map->json-response {:result result-lines}
                                  :status (if (zero? exit) 200 500))))
    (catch Exception e
      (log/error e "encountered exception")
      (utils/exception->json-response e))))

(defn demo-handler
  [{:keys [uri] :as request}]
  (case uri
    "/demos/image-search" (image-search-handler request)
    "/demos/image-tagging" (image-tagging-handler request)
    {:status 200
     :headers {"content-type" "text/html"}
     :body (io/input-stream (io/resource "demo.html"))}))

