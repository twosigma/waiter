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
(ns token-syncer.waiter
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [qbits.jet.client.http :as http]
            [token-syncer.correlation-id :as cid]
            [token-syncer.spnego :as spnego])
  (:import (org.eclipse.jetty.client HttpClient)
           (java.net URI)))

;; promise that stores the state of whether to use SPNEGO auth or not
(def use-spnego-promise (promise))

(defn make-http-request
  "Makes an asynchronous request to the request-url."
  [^HttpClient http-client request-url &
   {:keys [body headers method query-params]
    :or {body ""
         headers {}
         method http/get
         query-params {}}}]
  (when-not (realized? use-spnego-promise)
    (throw (IllegalStateException. "use-spnego-promise has not been initialized!")))
  (method http-client request-url
          (cond-> {:body body
                   :headers headers
                   :fold-chunked-response? true
                   :follow-redirects? false
                   :query-string query-params}

                  @use-spnego-promise
                  (assoc :auth (spnego/spnego-authentication (URI. request-url))))))

(defn load-token-list
  "Loads the list of tokens on a specific router."
  [^HttpClient http-client router-url]
  (let [token-list-url (str router-url "/tokens")
        {:keys [body error]} (async/<!! (make-http-request http-client token-list-url))]
    (when error
      (cid/error error "error in retrieving tokens from" router-url)
      (throw error))
    (->> body
         (async/<!!)
         str
         json/read-str
         (map (fn entry->token [entry] (get entry "token")))
         set)))

(defn load-token-on-router
  "Loads the description of a token on a router."
  [^HttpClient http-client router-url token]
  (try
    (let [token-get-url (str router-url "/token")
          {:keys [body error status]} (async/<!! (make-http-request http-client token-get-url
                                                                    :headers {"x-waiter-token" token}
                                                                    :query-params {"include-deleted" "true"}))]
      (when error
        (cid/error error "error in retrieving tokens from" router-url)
        (throw error))
      {:description (if (= status 200)
                      (->> body
                           (async/<!!)
                           str
                           json/read-str)
                      (->> body
                           (async/<!!)
                           str))
       :status status})
    (catch Exception ex
      (cid/error ex "unable to retrieve token" token "from" router-url)
      {:error ex})))

(defn store-token-on-router
  "Stores the token description on a specific router."
  [^HttpClient http-client router-url token token-description]
  (cid/info "storing token:" token-description ", soft-delete:" (true? (get token-description "deleted")) "on" router-url)
  (let [{:keys [body error status]}
        (async/<!!
          (make-http-request http-client
                             (str router-url "/token")
                             :body (json/write-str (assoc token-description :token token))
                             :method http/post
                             :query-params {"update-mode" "admin"}))
        body-data (when (not error) (async/<!! body))]
    (when error
      (throw error))
    (cid/info "status:" status ", body:" body-data)
    (when (or (nil? status)
              (< status 200)
              (> status 299))
      (throw (ex-info "Token store failed"
                      {:body body-data, :status status, :token-data token-description})))
    {:body body-data
     :status status}))

(defn hard-delete-token-on-router
  "Hard-delete a token on a specific router."
  [^HttpClient http-client router-url token]
  (cid/info "hard-delete" token "on" router-url)
  (let [{:keys [body error status]}
        (async/<!!
          (make-http-request http-client
                             (str router-url "/token")
                             :headers {"x-waiter-token" token}
                             :method http/delete
                             :query-params {"hard-delete" "true"}))
        body-data (when (not error) (async/<!! body))]
    (when error
      (throw error))
    (cid/info "status:" status ", body:" body-data)
    (when (or (nil? status)
              (< status 200)
              (> status 299))
      (throw (ex-info "Token hard-delete failed"
                      {:body body-data, :status status, :token token})))
    {:body body-data
     :status status}))
