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
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [kitchen.utils :as utils])
  (:import (java.io File)
           (java.net URLEncoder)))

(defn- request->cid [request]
  (get-in request [:headers "x-cid"]))

(defn- request->cid-string [request]
  (str "[CID=" (request->cid request) "]"))

(defn- printlog
  ([request & messages]
   (log/info (request->cid-string request) (str/join " " messages))))

(defn- search->image-titles
  [search-query]
  (let [extract-titles (fn extract-titles [page-entries]
                         (mapcat (fn [page-entry] (map #(get % "title") (get page-entry "images"))) page-entries))
        search-url (str "https://en.wikipedia.org/w/api.php?action=query&prop=images&format=json&titles=" (URLEncoder/encode search-query))
        initial-result (-> search-url slurp json/read-str)]
    (-> initial-result
        (get-in ["query" "pages"])
        vals
        extract-titles)))

(defn- extract-entries
  [page-entries]
  (-> (map (fn [page-entry]
             {"title" (get page-entry "title")
              "url" (-> page-entry
                        (get "imageinfo")
                        first
                        (get "url"))})
           page-entries)
      vec
      first))

(defn- image-title->image-entry
  [image-title]
  (-> (str "https://en.wikipedia.org/w/api.php?action=query&prop=imageinfo&iiprop=url&format=json&titles=" (URLEncoder/encode image-title))
      slurp
      json/read-str
      (get-in ["query" "pages"])
      vals
      extract-entries))

(defn- image-search-handler
  ""
  [{:keys [query-params] :as request}]
  (printlog request "query params:" query-params)
  (try
    (let [{:strs [search]} query-params
          search-url (str "https://en.wikipedia.org/w/api.php?action=query&prop=images&format=json&titles=" search)
          image-titles (search->image-titles search)
          _ (printlog request "image titles:" (vec image-titles))
          image-entries (map image-title->image-entry image-titles)]
      (printlog request "image entries:" (vec image-entries))
      (utils/map->json-response {:result image-entries}))
    (catch Exception e
      (log/error e "encountered exception while performing image search")
      (utils/exception->json-response e))))

(defn- directory-location
  [dir-name]
  (let [base-dir (or (System/getenv "MESOS_DIRECTORY") (System/getProperty "user.dir"))]
    (str base-dir File/separator dir-name)))

(defn- copy-uri-to-file
  [uri file]
  (with-open [in (io/input-stream uri)
              out (io/output-stream file)]
    (io/copy in out)))

(let [image-tagging-lock (Object.)]
  (defn- retrieve-image-tags
    [classifier-file image-location predictions]
    (let [result-atom (atom nil)]
      (locking image-tagging-lock
        (reset! result-atom (sh/sh "python3"
                                   classifier-file
                                   (str "--image_file=" image-location)
                                   (str "--num_top_predictions=" predictions))))
      @result-atom)))

(defn- image-tagging-handler
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

      (printlog request "classifying image" image-location)
      (let [{:keys [err exit out]} (retrieve-image-tags classifier-file image-location predictions)
            result-lines (filter #(str/includes? % "(score =") (str/split-lines out))]
        (printlog request "classifier exit code:" exit)
        (printlog request "classifier stdout:" out)
        (printlog request "classifier stderr:" err)
        (utils/map->json-response (cond-> {:id id, :out out, :predictions result-lines}
                                          (not (str/blank? err))
                                          (assoc :err err))
                                  :status (if (zero? exit) 200 500))))
    (catch Exception e
      (log/error e "encountered exception while tagging image")
      (utils/exception->json-response e))))

(defn demo-handler
  [{:keys [uri] :as request}]
  (case uri
    "/demos/image-search" (image-search-handler request)
    "/demos/image-tagging" (image-tagging-handler request)
    {:status 200
     :headers {"content-type" "text/html"}
     :body (io/input-stream (io/resource "demo.html"))}))

