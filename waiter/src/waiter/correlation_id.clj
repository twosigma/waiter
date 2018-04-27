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
           (org.apache.log4j MDC)))

; Use lower-case to preserve consistency with Ring's representation of headers
(def ^:const HEADER-CORRELATION-ID "x-cid")
(def ^:const CORRELATION-ID-KEY "waiter.correlation-id")

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

(defmacro with-correlation-id
  "Executes the body with the specified value of correlation-id."
  [correlation-id & body]
  `(do
     (MDC/put CORRELATION-ID-KEY ~correlation-id)
     ~@body))

(defmacro without-correlation-id
  "Executes the body without an implicit correlation-id."
  [& body]
  `(with-correlation-id nil ~@body))

(defn get-correlation-id
  "Retrieve the value of the current correlation-id."
  []
  (MDC/get CORRELATION-ID-KEY))

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
