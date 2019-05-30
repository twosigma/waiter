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
(ns waiter.auth.composite
  (:require [clojure.string :as string]
            [plumbing.core :as pc]
            [waiter.auth.authentication :as auth]
            [waiter.util.utils :as utils]))

(defrecord CompositeAuthenticator [authenticators default-authenticator]
  auth/Authenticator

  (process-callback [_ {{:keys [authentication-scheme]} :route-params :as request}]
    (if-let [authenticator (get authenticators authentication-scheme)]
      (auth/process-callback authenticator request)
      (throw (ex-info (str "Unknown authentication scheme " authentication-scheme)
                      {:composite-authenticators (keys authenticators)
                       :authentication-scheme authentication-scheme
                       :status 400}))))
  (wrap-auth-handler [_ request-handler]
    (let [default-handler (auth/wrap-auth-handler default-authenticator request-handler)
          handlers (merge {"standard" default-handler} (pc/map-vals #(auth/wrap-auth-handler % request-handler) authenticators))]
      (fn composite-authenticator-handler [request]
        (let [authentication (get-in request [:waiter-discovery :service-parameter-template "authentication"])]
          (if authentication
            (if-let [handler (get handlers authentication)]
              (handler request)
              (throw (ex-info (str "No authenticator found for " authentication " authentication.")
                              {:composite-authenticators (keys handlers)
                               :request-authentication authentication
                               :status 400})))
            (default-handler request)))))))

(defn- make-authenticator
  "Create an authenticator from an authentication-scheme"
  [{:keys [authenticator-factory-fn] :as authentication-scheme} context]
  {:pre [(symbol? authenticator-factory-fn)]}
  (let [resolved-factory-fn (utils/resolve-symbol! authenticator-factory-fn)
        authenticator (resolved-factory-fn (merge context authentication-scheme))]
    (when-not (satisfies? auth/Authenticator authenticator)
      (throw (ex-info "Authenticator factory did not create an instance of Authenticator"
                      {:authentication-scheme authentication-scheme
                       :authenticator authenticator
                       :resolved-factory-fn resolved-factory-fn})))
    authenticator))

(defn composite-authenticator
  "Factory function for creating composite authenticator middleware"
  [{:keys [authentication-schemes default-scheme] :as context}]
  {:pre [(not-empty authentication-schemes)
         (not (string/blank? default-scheme))
         (contains? authentication-schemes default-scheme)]}
  (let [authenticators (pc/map-vals
                         #(make-authenticator % context)
                         authentication-schemes)]
    (->CompositeAuthenticator authenticators (get authenticators default-scheme))))