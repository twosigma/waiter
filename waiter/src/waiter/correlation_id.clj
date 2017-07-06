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
(ns waiter.correlation-id
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (java.util Collections)
           (org.apache.log4j Appender EnhancedPatternLayout Logger PatternLayout)))

; Use lower-case to preserve consistency with Ring's representation of headers
(def ^:const HEADER-CORRELATION-ID "x-cid")

(defn request->correlation-id [request]
  (get-in request [:headers HEADER-CORRELATION-ID]))

(defn ensure-correlation-id
  "Ensures that the correlation-d is present in the request header,
   else it modifies the request by attaching the 'x-cid' header with
   value created using the `id-factory` function."
  [request id-factory]
  (let [current-cid (request->correlation-id request)]
    (if (nil? current-cid)
      (let [new-cid (id-factory)]
        (log/debug "attaching correlation id" new-cid "to request.")
        (update-in request [:headers] #(assoc % HEADER-CORRELATION-ID new-cid)))
      request)))

; The value of UNKNOWN should never be used, correlation-id should be dynamically bound in the with-correlation-id body.
(def default-correlation-id "UNKNOWN")
(def ^:dynamic dynamic-correlation-id default-correlation-id)

(defmacro with-correlation-id
  "Executes the body with the specified value of correlation-id."
  [correlation-id & body]
  `(binding [dynamic-correlation-id ~correlation-id]
     ~@body))

(defn get-correlation-id
  "Retrieve the value of the current correlation-id."
  []
  dynamic-correlation-id)


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
