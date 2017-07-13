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
(ns token-syncer.settings
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [token-syncer.utils :as utils])
  (:import (java.net URL)))

(defn valid-url?
  [url-string]
  (try
    ;; using standard jdk https://stackoverflow.com/a/17894617/356645
    (let [url (URL. url-string)]
      (and (.toURI url)
           (contains? #{"http" "https"} (.getProtocol url))))
    (catch Exception e
      (log/error url-string "is not a valid url:" (.getMessage e))
      false)))

(def non-empty-string (s/both s/Str (s/pred #(not (str/blank? %)) 'non-empty-string)))

(def valid-url-string (s/both s/Str (s/pred #(valid-url? %) 'valid-url-string)))

(def settings-schema
  {(s/required-key :http-client-properties) {(s/required-key :connection-timeout-ms) s/Int
                                             (s/required-key :idle-timeout-ms) s/Int
                                             (s/required-key :use-spnego) s/Bool}})

(def settings-defaults
  {:http-client-properties {:connection-timeout-ms 1000
                            :idle-timeout-ms 30000
                            :use-spnego false}})

(defn load-settings-file
  "Loads the edn config in the specified file, it relies on having the filename being a path to the file."
  [filename]
  (let [config-file (-> filename str io/file)
        config-file-path (.getAbsolutePath config-file)]
    (if (.exists config-file)
      (do
        (log/info "reading settings from file:" config-file-path)
        (let [settings (-> config-file-path slurp edn/read-string)]
          (log/info "configured settings:\n" (with-out-str (clojure.pprint/pprint settings)))
          settings))
      (do
        (log/info "unable to find configuration file:" config-file-path)
        (utils/exit 1 (str "Unable to find configuration file: " config-file-path))))))

(defn- load-settings-from-file
  "Loads the settings file and merges it with the default settings"
  [config-file]
  (if (-> config-file str/blank? not)
    (->> (load-settings-file config-file)
         (merge settings-defaults))
    settings-defaults))

(defn load-settings
  "Loads the settings, merges it with the defaults and validates it before returning the settings."
  [config-file git-version]
  (let [loaded-settings (load-settings-from-file config-file)]
    (s/validate settings-schema loaded-settings)
    (merge loaded-settings {:git-version git-version})))