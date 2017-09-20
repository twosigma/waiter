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
(ns demoapp.search
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:import (java.net URLEncoder)))

(def ^:const ^:private search-base-url "https://en.wikipedia.org/w/api.php")
(def ^:const ^:private image-search-url (str search-base-url "?action=query&prop=images&format=json&titles="))
(def ^:const ^:private title-search-url (str search-base-url "?action=query&prop=imageinfo&iiprop=url&format=json&titles="))

(defn- search->image-titles
  [search-query]
  (let [extract-titles (fn extract-titles [page-entries]
                         (mapcat (fn [page-entry] (map #(get % "title") (get page-entry "images"))) page-entries))
        search-url (str image-search-url (URLEncoder/encode search-query))
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
  (-> (str title-search-url (URLEncoder/encode image-title))
      slurp
      json/read-str
      (get-in ["query" "pages"])
      vals
      extract-entries))

(defn image-search
  [search]
  (let [image-titles (search->image-titles search)
        _ (log/info "image titles:" (vec image-titles))
        image-entries (map image-title->image-entry image-titles)]
    (log/info "image entries:" (vec image-entries))
    image-entries))
