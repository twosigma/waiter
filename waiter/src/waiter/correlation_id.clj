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
(ns waiter.correlation-id
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (clojure.lang Var)
           (java.util Collections)
           (org.apache.log4j Appender EnhancedPatternLayout Logger PatternLayout)))

; Use lower-case to preserve consistency with Ring's representation of headers
(def ^:const HEADER-CORRELATION-ID "x-cid")

(defn http-object->correlation-id [http-object]
  (get-in http-object [:headers HEADER-CORRELATION-ID]))

(defn ensure-correlation-id
  "Ensures that the correlation-d is present in the http-object header,
   else it modifies the http-object by attaching the 'x-cid' header with
   value created using the `id-factory` function."
  [http-object id-factory]
  (let [current-cid (http-object->correlation-id http-object)]
    (if (nil? current-cid)
      (let [new-cid (id-factory)]
        (log/debug "attaching correlation id" new-cid "to http-object.")
        (update-in http-object [:headers] #(assoc % HEADER-CORRELATION-ID new-cid)))
      http-object)))

; The value of UNKNOWN should never be used, correlation-id should be dynamically bound in the with-correlation-id body.
(def default-correlation-id "UNKNOWN")
(def ^:dynamic dynamic-correlation-id default-correlation-id)

(defn get-correlation-id
  "Retrieve the value of the current correlation-id."
  []
  dynamic-correlation-id)

(defmacro with-correlation-id
  "Executes the body with the specified value of correlation-id."
  [correlation-id & body]
  `(let [correlation-id# ~correlation-id]
     (binding [dynamic-correlation-id correlation-id#]
       (let [start-thread# (Thread/currentThread)
             start-frame# (Var/getThreadBindingFrame)
             result# (do ~@body)
             end-thread# (Thread/currentThread)
             end-frame# (Var/getThreadBindingFrame)]
         (when-not (identical? start-frame# end-frame#)
           (log/warn "with-correlation-id binding executed in different contexts, correcting frame"
                     {:end {:correlation-id (get-correlation-id)
                            :thread-id (.getId end-thread#)
                            :thread-name (.getName end-thread#)}
                      :start {:correlation-id correlation-id#
                              :thread-id (.getId start-thread#)
                              :thread-name (.getName start-thread#)}})
           (Var/resetThreadBindingFrame start-frame#))
         result#))))


(defmacro correlation-id->str
  "Retrieves the string representation of the correlation-id."
  [correlation-id]
  `(str "[CID=" ~correlation-id "]"))

(defmacro cloghelper
  [loglevel correlation-id message & args]
  `(with-correlation-id
     ~correlation-id
     (log/logp ~loglevel ~message ~@args)))

(defmacro ctrace
  [correlation-id & args]
  `(cloghelper :trace ~correlation-id ~@args))

(defmacro cdebug
  [correlation-id & args]
  `(cloghelper :debug ~correlation-id ~@args))

(defmacro cinfo
  [correlation-id & args]
  `(cloghelper :info ~correlation-id ~@args))

(defmacro cwarn
  [correlation-id & args]
  `(cloghelper :warn ~correlation-id ~@args))

(defmacro cerror
  [correlation-id & args]
  `(cloghelper :error ~correlation-id ~@args))

(defmacro cfatal
  [correlation-id & args]
  `(cloghelper :fatal ~correlation-id ~@args))

(defn- replace-pattern-layout
  [^Appender appender ^String pattern]
  (let [new-layout (proxy [EnhancedPatternLayout]
                          [pattern]
                     (format [logging-event]
                       (let [parent-format (proxy-super format logging-event)
                             correlation-id (get-correlation-id)
                             display-cid-str (when (not= correlation-id default-correlation-id)
                                               (correlation-id->str correlation-id))]
                         (str/replace parent-format "[CID]" (str display-cid-str)))))]
    (.setLayout appender new-layout)))

(defn replace-pattern-layout-in-log4j-appenders
  "Replaces instances of `PatternLayout` in appenders with instance of `CidEnhancedPatternLayout`."
  []
  (let [logger (Logger/getRootLogger)
        appenders (.getAllAppenders logger)]
    (doseq [^Appender appender (seq (Collections/list appenders))]
      (let [layout (.getLayout ^Appender appender)]
        (when (instance? PatternLayout layout)
          (println "Replacing the layout in" (.getName appender) (.getClass appender))
          (replace-pattern-layout appender (.getConversionPattern ^PatternLayout layout)))
        (when (instance? EnhancedPatternLayout layout)
          (println "Replacing the layout in" (.getName appender) (.getClass appender))
          (replace-pattern-layout appender (.getConversionPattern ^EnhancedPatternLayout layout)))))))
