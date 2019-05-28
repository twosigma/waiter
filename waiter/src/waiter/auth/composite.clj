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
  (:require [plumbing.core :as pc]
            [waiter.auth.authentication :as auth]
            [waiter.util.utils :as utils]))

(defn- request->authentication
  "Gets authentication service parameter for a request."
  [request]
  (get-in request [:waiter-discovery :service-parameter-template "authentication"]))

(defrecord CompositeAuthenticator [authenticators default-authenticator]
  auth/Authenticator

  (process-callback [_ request scheme operation]
    (if-let [authenticator (get authenticators (keyword scheme))]
      (auth/process-callback authenticator request scheme operation)
      (throw (ex-info (str "Unknown authentication scheme " scheme)
                      {:composite-authenticators (keys authenticators)
                       :scheme scheme
                       :status 400}))))
  (wrap-auth-handler [_ request-handler]
    (let [default-handler (auth/wrap-auth-handler default-authenticator request-handler)
          handlers (pc/map-vals #(auth/wrap-auth-handler % request-handler) authenticators)]
      (fn composite-authenticator-handler [request]
        (let [authentication (get-in request [:waiter-discovery :service-parameter-template "authentication"])]
          (if authentication
            (if-let [handler (get handlers (keyword authentication))]
              (handler request)
              (throw (ex-info (str "No authenticator found for " authentication " authentication.")
                              {:composite-authenticators (keys authenticators)
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
  [{:keys [default-scheme authentication-schemes] :as context}]
  {:pre [(not-empty authentication-schemes)
         (keyword? default-scheme)
         (contains? authentication-schemes default-scheme)]}
  (let [authenticators (pc/map-vals
                         #(make-authenticator % context)
                         authentication-schemes)]
    (->CompositeAuthenticator authenticators (default-scheme authenticators))))