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

(defrecord CompositeAuthenticator [default-authentication provider-name->authenticator]
  auth/Authenticator
  (wrap-auth-handler [_ request-handler]
    (let [provider-name->handler (pc/map-vals #(auth/wrap-auth-handler % request-handler) provider-name->authenticator)]
      (fn composite-authenticator-handler [request]
        (let [authentication (or (get-in request [:waiter-discovery :service-parameter-template "authentication"])
                                 default-authentication)]
          (if-let [handler (get provider-name->handler authentication)]
            (handler request)
            (throw (ex-info (str "No authenticator found for " authentication " authentication.")
                            {:available-authenticators (keys provider-name->handler)
                             :request-authentication authentication
                             :status 400})))))))

  auth/CompositeAuthenticator
  (get-authentication-providers [_]
    (keys provider-name->authenticator))

  auth/CallbackAuthenticator
  (process-callback [_ {{:keys [authentication-provider]} :route-params :as request}]
    (if-let [authenticator (get provider-name->authenticator authentication-provider)]
      (auth/process-callback authenticator request)
      (throw (ex-info (str "Unknown authentication provider " authentication-provider)
                      {:available-authenticators (keys provider-name->authenticator)
                       :authentication-provider authentication-provider
                       :status 400})))))

(defn- make-authenticator
  "Create an authenticator from an authentication-provider"
  [{:keys [factory-fn] :as authentication-provider} context]
  {:pre [(symbol? factory-fn)]}
  (let [resolved-factory-fn (utils/resolve-symbol! factory-fn)
        authenticator (resolved-factory-fn (merge context authentication-provider))]
    (when-not (satisfies? auth/Authenticator authenticator)
      (throw (ex-info "Authenticator factory did not create an instance of Authenticator"
                      {:authentication-provider authentication-provider
                       :authenticator authenticator
                       :resolved-factory-fn resolved-factory-fn})))
    authenticator))

(defn composite-authenticator
  "Factory function for creating composite authenticator middleware"
  [{:keys [authentication-providers default-authentication default-authentication-provider] :as context}]
  {:pre [(not-empty authentication-providers)
         (not (string/blank? default-authentication))
         (not (string/blank? default-authentication-provider))
         (contains? authentication-providers default-authentication-provider)]}
  (let [provider-name->authenticator (as-> (pc/map-vals
                                             #(make-authenticator % context)
                                             authentication-providers) map
                                       (merge {"standard" (get map default-authentication-provider)} map))]
    (->CompositeAuthenticator default-authentication provider-name->authenticator)))