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
(ns waiter.auth.dynamic
  (:require [plumbing.core :as pc]
            [waiter.auth.authentication :as auth]
            [waiter.auth.saml :as saml]
            [waiter.util.utils :as utils]))

(defrecord DynamicAuthenticator [authenticators saml-acs-handler-fn]
  auth/Authenticator
  (wrap-auth-handler [_ request-handler]
    (let [handlers (pc/map-vals #(auth/wrap-auth-handler % request-handler) authenticators)]
      (fn dynamic-authenticator-handler [{{:strs [x-waiter-authentication]} :headers :as request}]
        (let [authentication (get-in request [:waiter-discovery :service-parameter-template "authentication"])
              authenticator-name (or authentication x-waiter-authentication)]
          (when (and authentication x-waiter-authentication (not= authentication x-waiter-authentication))
            (throw (ex-info "Both x-waiter-authentication header and authentication are set but are not the same."
                            {:request-authentication authentication
                             :request-x-waiter-authentication x-waiter-authentication
                             :status 400})))
          (if authenticator-name
            (if-let [handler ((keyword authenticator-name) handlers)]
              (handler request)
              (throw (ex-info (str "No authenticator found for " authenticator-name " authentication.")
                              {:request-authentication authentication
                               :request-x-waiter-authentication x-waiter-authentication
                               :dynamic-authenticators (keys authenticators)
                               :status 400})))
            ((:default handlers) request)))))))

(defn dynamic-authenticator
  "Factory function for creating dynamic authenticator middleware"
  [{:keys [authenticator-config default-kind] :as context}]
  {:pre [(not-empty authenticator-config)
         (keyword? default-kind)
         (contains? authenticator-config default-kind)]}
  (let [make-authenticator #(utils/create-component (assoc authenticator-config :kind %) :context context)
        authenticators (pc/map-from-keys
                         make-authenticator
                         (filter #(not (contains? #{:kind :dynamic} %))
                                 (keys authenticator-config)))
        saml-authenticator (:saml authenticators)]
    (->DynamicAuthenticator (assoc authenticators :default (default-kind authenticators))
                            (fn [request _] (saml/saml-acs-handler request saml-authenticator)))))