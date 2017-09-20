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
(ns demoapp.tagging
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (java.io File)))

(def ^:const ^:private classifier-url "https://raw.githubusercontent.com/tensorflow/models/master/tutorials/image/imagenet/classify_image.py")

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
        (log/info "classifying image" image-location)
        (reset! result-atom (sh/sh "python3"
                                   classifier-file
                                   (str "--image_file=" image-location)
                                   (str "--num_top_predictions=" predictions))))
      @result-atom)))

(defn perform-image-tagging
  [image-id image-name image-url predictions]
  (let [image-location (directory-location (str "images" File/separator image-name))
        classifier-file-name "classify_image.py"
        classifier-file-location (directory-location (str "images" File/separator classifier-file-name))]

    (-> image-location str File. .getParentFile .mkdirs)
    (-> classifier-file-location str File. .getParentFile .mkdirs)

    (log/info "downloading" image-url "to" image-location)
    (copy-uri-to-file image-url image-location)

    (log/info "classifier location" classifier-file-location)
    (when-not (-> classifier-file-location str File. .exists)
      (log/info "saving" classifier-file-name "to" classifier-file-location "from" classifier-url)
      (copy-uri-to-file classifier-url classifier-file-location))

    (let [{:keys [err exit out]} (retrieve-image-tags classifier-file-location image-location predictions)
          result-lines (filter #(str/includes? % "(score =") (str/split-lines out))]
      (log/info "classifier exit code:" exit)
      (log/debug "classifier stdout:" out)
      (log/debug "classifier stderr:" err)

      (log/info "deleting file" image-location)
      (io/delete-file image-location true)

      {:err err
       :exit-code exit
       :id image-id
       :out out
       :predictions result-lines})))
