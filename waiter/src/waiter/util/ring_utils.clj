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
(ns waiter.util.ring-utils
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [ring.middleware.params :as ring-params]
            [ring.util.request :as ring-request]
            [waiter.util.async-utils :as au]))

(defn update-response
  "Updates a response, handling the case where it may be a chan."
  [response response-fn]
  (if (au/chan? response)
    (async/go (response-fn (async/<! response)))
    (response-fn response)))

(defn json-request
  "Tries to parse a request body as JSON, if error, throw 400."
  [{:keys [body] {:strs [content-type]} :headers :as request}]
  (try
    (assoc request :body (-> body slurp json/read-str))
    (catch Exception e
      (throw (ex-info "Invalid JSON payload" {:status 400} e)))))

(defn query-params-request
  "Like Ring's params-request, but doesn't try to pull params from the body."
  [request]
  (let [encoding (or (ring-request/character-encoding request) "UTF-8")]
    (ring-params/assoc-query-params request encoding)))

(defn error-response?
  "Determines if a response is an error"
  [{:keys [status]}]
  (and status
       (>= status 400)
       (<= status 599)))
