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
            [waiter.auth.saml :as saml]
            [waiter.util.utils :as utils]))

(defrecord CompositeAuthenticator [authenticators saml-acs-handler-fn saml-auth-redirect-handler-fn]
  auth/Authenticator
  (wrap-auth-handler [_ request-handler]
    (let [handlers (pc/map-vals #(auth/wrap-auth-handler % request-handler) authenticators)]
      (fn composite-authenticator-handler [request]
        (let [authentication (get-in request [:waiter-discovery :service-parameter-template "authentication"])]
          (if authentication
            (if-let [handler (get handlers (keyword authentication))]
              (handler request)
              (throw (ex-info (str "No authenticator found for " authentication " authentication.")
                              {:composite-authenticators (keys authenticators)
                               :request-authentication authentication
                               :status 400})))
            ((:default handlers) request)))))))

(defn composite-authenticator
  "Factory function for creating composite authenticator middleware"
  [{:keys [authenticator-config default-kind] :as context}]
  {:pre [(not-empty authenticator-config)
         (keyword? default-kind)
         (contains? authenticator-config default-kind)]}
  (let [make-authenticator #(utils/create-component (assoc authenticator-config :kind %) :context context)
        authenticators (pc/map-from-keys
                         make-authenticator
                         (filter #(not (contains? #{:kind :composite} %))
                                 (keys authenticator-config)))
        saml-authenticator (:saml authenticators)]
    (->CompositeAuthenticator (assoc authenticators :default (default-kind authenticators))
                              (fn [request _] (saml/saml-acs-handler request saml-authenticator))
                              (fn [request _] (saml/saml-auth-redirect-handler request saml-authenticator)))))