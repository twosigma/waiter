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
(ns waiter.util.http-utils
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [qbits.jet.client.http :as http]
            [slingshot.slingshot :as ss]
            [waiter.auth.spnego :as spnego])
  (:import java.net.URI
           org.eclipse.jetty.client.HttpClient))

(defn http-request
  "Wrapper over the qbits.jet.client.http/request function.
   It performs a blocking read on the response and the response body.
   The body is assumed to be json and is parsed into a clojure data structure.
   If the status of the response in not 2XX, the response is thrown as an exception."
  [http-client request-url & {:keys [accept body content-type headers query-string request-method
                                     spnego-auth throw-exceptions]
                              :or {spnego-auth false
                                   throw-exceptions true}}]
  (let [request-map (cond-> {:as :string
                             :method (or request-method :get)
                             :url request-url}
                            spnego-auth (assoc :auth (spnego/spnego-authentication (URI. request-url)))
                            accept (assoc :accept accept)
                            body (assoc :body body)
                            content-type (assoc :content-type content-type)
                            (seq headers) (assoc :headers headers)
                            query-string (assoc :query-string query-string))
        raw-response (http/request http-client request-map)
        {:keys [error status] :as response} (async/<!! raw-response)]
    (when error
      (throw error))
    (when (and throw-exceptions (not (<= 200 status 299)))
      (ss/throw+ response))
    (-> response
        :body
        async/<!!
        json/read-str
        walk/keywordize-keys)))

(defn ^HttpClient http-client-factory
  "Creates a HttpClient."
  [{:keys [conn-timeout socket-timeout]}]
  (http/client {:connect-timeout conn-timeout, :idle-timeout socket-timeout}))
