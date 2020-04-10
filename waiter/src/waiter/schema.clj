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
(ns waiter.schema
  (:require [clojure.string :as str]
            [schema.core :as s])
  (:import (java.util.regex Pattern)))

(def positive-int (s/both s/Int (s/pred pos? 'pos?)))
(def non-negative-int (s/both s/Int (s/pred #(not (neg? %)) 'non-neg?)))
(def positive-num (s/both s/Num (s/pred pos? 'pos?)))
(def non-negative-num (s/both s/Num (s/pred #(not (neg? %)) 'non-neg?)))
(def positive-fraction-less-than-1 (s/pred #(< 0 % 1) 'positive-fraction-less-than-1))
(def non-empty-string (s/both s/Str (s/pred #(not (str/blank? %)) 'non-empty-string)))
(def valid-string-length (s/constrained s/Str #(and (pos? (count %)) (< (count %) 1000))))
(def regex-pattern (s/pred #(instance? Pattern %) 'regex-pattern))
(def positive-number-greater-than-or-equal-to-1 (s/pred #(and (pos? %) (>= % 1))
                                                        'positive-number-greater-than-or-equal-to-1))
(def positive-fraction-less-than-or-equal-to-1 (s/pred #(and (pos? %) (<= % 1))
                                                       'positive-fraction-less-than-or-equal-to-1))
(def positive-fraction-less-than-or-equal-to-2 (s/pred #(and (pos? %) (<= % 2))
                                                       'positive-fraction-less-than-or-equal-to-2))
(def greater-than-or-equal-to-0-less-than-1 (s/pred #(and (<= 0 %) (< % 1))
                                                    'greater-than-or-equal-to-0-less-than-1))
(def regex-pattern (s/pred #(re-pattern %) 'is-a-valid-regular-expression?))

(def http-methods #{"CONNECT" "DELETE" "GET" "HEAD" "OPTIONS" "PATCH" "POST" "PUT" "TRACE"})
(def http-method (s/pred #(contains? http-methods %) 'is-an-http-method?))

(let [valid-backend-protos #{"h2" "h2c" "http" "https"}
      valid-health-check-protos (conj valid-backend-protos nil)]

  (def valid-backend-proto
    "Validator for the backend-proto parameter.
     Valid values are 'h2', 'h2c', 'http', or 'https'."
    (s/pred #(contains? valid-backend-protos %) 'invalid-backend-proto))

  (def valid-health-check-proto
    "Validator for the health-check-proto parameter.
     Valid non-nil values are 'h2', 'h2c', 'http', or 'https'."
    (s/pred #(contains? valid-health-check-protos %) 'invalid-health-check-proto)))

(def valid-load-balancing
  "Validator for the load-balancing parameter.
   Valid values are 'oldest', 'youngest' or 'random'."
  (s/pred #(contains? #{"oldest" "random" "youngest"} %) 'invalid-backend-proto))

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

(def valid-health-check-authentication
  "Validator for health check authentication.
   Valid values are 'disabled' and 'standard'."
  (s/constrained non-empty-string #{"disabled" "standard"} 'invalid-health-check-authentication))

(def valid-health-check-port-index
  "Validator for health check port index."
  (s/pred #(<= 0 % 9) 'between-0-and-9))

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

(def valid-jwt-issuer-config
  "Validator for the JWT issuer field.
   The issuer field is either
   - a string,
   - a pattern, or
   - a non-empty vector of string / regex patterns."
  (s/conditional
    string? non-empty-string
    #(instance? Pattern %) regex-pattern
    :else (s/constrained [(s/either non-empty-string regex-pattern)] not-empty)))

(def valid-jwt-authenticator-config
  "Validator for the Zookeeper connection configuration. We allow either
  a non-empty string (representing a connection string), or the keyword
  :in-process, indicating that ZK should be started in-process"
  (s/either
    {(s/optional-key :allow-bearer-auth-api?) s/Bool
     (s/optional-key :allow-bearer-auth-services?) s/Bool
     (s/required-key :http-options) {s/Keyword s/Any}
     (s/required-key :issuer) valid-jwt-issuer-config
     (s/required-key :jwks-url) s/Str
     (s/optional-key :max-expiry-duration-ms) positive-int
     (s/required-key :subject-key) s/Keyword
     (s/optional-key :subject-regex) s/Regex
     (s/required-key :supported-algorithms) #{s/Keyword}
     (s/required-key :token-type) non-empty-string
     (s/required-key :update-interval-ms) positive-int}
    (s/constrained s/Keyword #(= :disabled %))))

(def profile-definition
  "Validator for profile parameters."
  {(s/required-key :service-parameters) {non-empty-string s/Any}})
