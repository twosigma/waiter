;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.schema
  (:require [clojure.string :as str]
            [schema.core :as s])
  (:import (java.util.regex Pattern)))

(def positive-int (s/both s/Int (s/pred pos? 'pos?)))
(def positive-num (s/both s/Num (s/pred pos? 'pos?)))
(def non-negative-num (s/both s/Num (s/pred #(not (neg? %)) 'non-neg?)))
(def positive-fraction-less-than-1 (s/pred #(< 0 % 1) 'positive-fraction-less-than-1))
(def non-empty-string (s/both s/Str (s/pred #(not (str/blank? %)) 'non-empty-string)))
(def valid-string-length (s/constrained s/Str #(and (pos? (count %)) (< (count %) 1000))))
(def positive-number-greater-than-or-equal-to-1 (s/pred #(and (pos? %) (>= % 1))
                                                        'positive-number-greater-than-or-equal-to-1))
(def positive-fraction-less-than-or-equal-to-1 (s/pred #(and (pos? %) (<= % 1))
                                                       'positive-fraction-less-than-or-equal-to-1))
(def greater-than-or-equal-to-0-less-than-1 (s/pred #(and (<= 0 %) (< % 1))
                                                    'greater-than-or-equal-to-0-less-than-1))

(def valid-metric-group
  "Validator for metric group names. Valid names must:
   - be between 2 and 32 characters long (inclusive)
   - only contain lowercase letters, numbers, dashes, and underscores
   - start with a lowercase letter
   - only use dash and/or underscore as separators between alphanumeric portions"
  (s/constrained s/Str #(and (<= 2 (count %) 32)
                             (re-matches #"^[a-z][a-z0-9]*(?:[_\-][a-z0-9]+)*$" %))))

(def valid-metric-group-mappings
  "Validator for the metric group mappings structure. The
  structure is a vector of vectors, where each internal
  vector must have two elements, the first of which is a
  regex pattern, and the second of which is a string (which
  represents the metric group to use)"
  [(s/constrained [s/Any] #(and (= 2 (count %))
                                (instance? Pattern (first %))
                                (string? (second %))))])

(def valid-number-of-ports
  "Validator for number of ports."
  (s/pred #(<= 1 % 10) 'between-1-and-10))

(def valid-zookeeper-connect-config
  "Validator for the Zookeeper connection configuration. We allow either
  a non-empty string (representing a connection string), or the keyword
  :in-process, indicating that ZK should be started in-process"
  (s/either non-empty-string (s/constrained s/Keyword #(= :in-process %))))

(def require-symbol-factory-fn
  "In any of the configs that accept :kind, we use this to require that any
  nested map that defines :factory-fn must define it as a symbol"
  (s/if map? {(s/optional-key :factory-fn) s/Symbol, s/Keyword s/Any} s/Any))

(defn contains-kind-sub-map?
  "Returns true if there is a valid sub-map for the configured :kind"
  [{:keys [kind] :as config}]
  (nil? (s/check {(s/required-key :factory-fn) s/Symbol, s/Keyword s/Any} (get config kind))))

