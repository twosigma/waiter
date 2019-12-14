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
(ns token-syncer.utils
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as f]))

(defn successful?
  "Returns true if the response has a 2XX status code."
  [{:keys [status]}]
  (and (integer? status) (<= 200 status 299)))

(defn iso8601->millis
  "Convert the ISO 8601 string to numeric milliseconds."
  [date-str]
  (-> (:date-time f/formatters)
    (f/with-zone (t/default-time-zone))
    (f/parse date-str)
    .getMillis))

(defn millis->iso8601
  "Convert the numeric milliseconds to ISO 8601 string."
  [epoch-time]
  (-> (:date-time f/formatters)
    (f/with-zone (t/default-time-zone))
    (f/unparse (tc/from-long epoch-time))))
