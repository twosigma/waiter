(ns waiter.token-watch-integration-test
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(defn- await-goal-response-for-all-routers
  "Returns true if the goal-response-fn was satisfied with the response from request-fn for all routers before
  timeout-ms by polling every interval-ms."
  [goal-response-fn request-fn router-urls & {:keys [interval-ms timeout-ms]
                                              :or {interval-ms 100 timeout-ms 5000}}]
  (wait-for
    (fn []
      (let [responses (for [router-url router-urls]
                        (request-fn router-url))]
        (every? goal-response-fn responses)))
    :interval interval-ms
    :timeout timeout-ms
    :unit-multiplier 1))

(deftest ^:parallel ^:integration-fast test-token-watch-maintainer
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          routers (routers waiter-url)
          router_urls (vals routers)
          {:keys [body] :as response} (get-token-watch-maintainer-state waiter-url)
          {:strs [router-id state]} (try-parse-json body)
          watch-state-request-fn (fn [router-url]
                                   (get-token-watch-maintainer-state router-url
                                                                     :query-params "include=token->index"
                                                                     :cookies cookies))
          token (create-token-name waiter-url ".")
          router-url (router-endpoint waiter-url router-id)
          default-state-fields #{"last-update-time" "watch-count"}]

      (testing "no query parameters provide default state fields"
        (assert-response-status response 200)
        (is (= (set (keys state))
               default-state-fields)))

      (testing "include token->index map"
        (let [{:keys [body] :as response}
              (get-token-watch-maintainer-state router-url :query-params "include=token->index" :cookies cookies)
              {:strs [state]} (try-parse-json body)]
          (assert-response-status response 200)
          (is (= (set (keys state))
                 (set/union default-state-fields #{"token->index"})))))

      (testing "creating token reflects change in token-watch-state"
        (let [last-update-time (System/currentTimeMillis)
              response (post-token waiter-url
                                   {:token token :cpus 1 :last-update-time last-update-time}
                                   :query-params {"update-mode" "admin"})
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (= (get-in state ["token->index" token])
                             {"deleted" false
                              "etag" (token->etag waiter-url token)
                              "last-update-time" last-update-time
                              "maintenance" false
                              "owner" (retrieve-username)
                              "token" token})))]
          (assert-response-status response 200)
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls))))

      (testing "updating a token reflects change in token-watch-state"
        (let [last-update-time (System/currentTimeMillis)
              response (post-token waiter-url
                                   {:token token :cpus 2 :last-update-time last-update-time}
                                   :headers {"if-match" (token->etag waiter-url token)}
                                   :query-params {"update-mode" "admin"})
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (= (get-in state ["token->index" token])
                             {"deleted" false
                              "etag" (token->etag waiter-url token)
                              "last-update-time" last-update-time
                              "maintenance" false
                              "owner" (retrieve-username)
                              "token" token})))]
          (assert-response-status response 200)
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls))))

      (testing "soft deleting a token reflects change in token-watch-state"
        (let [_ (delete-token-and-assert waiter-url token :hard-delete false)
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (= (-> state
                                 (get-in ["token->index" token])
                                 (dissoc "last-update-time"))
                             {"deleted" true
                              "etag" nil
                              "maintenance" false
                              "owner" (retrieve-username)
                              "token" token})))]
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls))))

      (testing "hard deleting a token reflects change in token-watch-state"
        (let [_ (delete-token-and-assert waiter-url token)
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (nil? (get-in state ["token->index" token]))))]
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls)))))))

(defn- start-watch
  [router-url cookies & {:keys [query-params] :or {query-params {"include" ["metadata", "deleted"]
                                                                 "watch" "true"}}}]
  (let [{:keys [body error]} (make-request router-url "/tokens"
                                           :cookies cookies
                                           :fold-chunked-response? false
                                           :query-params query-params)
        token->index-atom (atom {})
        query-state-fn (fn [] @token->index-atom)
        exit-chan (async/promise-chan)
        go-chan
        (async/go-loop [token->index @token->index-atom
                        prefix ""]
          (reset! token->index-atom token->index)
          (let [[msg chan] (async/alts! [body exit-chan] :priority true)
                [next-token->index next-prefix]
                (condp = chan
                  body
                  (when (some? msg)
                    (try
                      (let [{:strs [object type]} (try-parse-json (str prefix msg))]
                        (case type
                          "INITIAL"
                          [(reduce
                             (fn [token->index index]
                               (assoc token->index (get index "token") index))
                             {}
                             object)
                           ""]

                          "EVENTS"
                          [(reduce
                             (fn [token->index {:strs [object type]}]
                               (case type
                                 "UPDATE"
                                 (assoc token->index (get object "token") object)
                                 "DELETE"
                                 (dissoc token->index (get object "token") object)
                                 (throw (ex-info "Unknown event type received in EVENTS object" {:event object}))))
                             token->index
                             object)
                           ""]
                          (throw (ex-info "Unknown event type received from watch" {:event msg}))))
                      (catch ExceptionInfo _
                        [token->index (str prefix msg)])))

                  exit-chan
                  (do
                    (async/close! body)
                    [false ""]))]
            (when next-token->index
              (recur next-token->index next-prefix))))]
    {:exit-chan exit-chan
     :error-chan error
     :go-chan go-chan
     :query-state-fn query-state-fn}))

(defn- start-watches
  [router-urls cookies]
  (mapv
    #(start-watch % cookies)
    router-urls))

(defn- stop-watch
  [{:keys [exit-chan go-chan]}]
  (async/close! exit-chan)
  (async/<!! go-chan))

(defn- stop-watches
  [watches]
  (doseq [watch watches]
    (stop-watch watch)))

(defmacro assert-watch-token-index-entry
  [watch token entry]
  `(let [watch# ~watch
         token# ~token
         entry# ~entry
         query-state-fn# (:query-state-fn watch#)]
     (is (wait-for
           #(= (get (query-state-fn#) token#)
               entry#)
           :interval 1 :timeout 5))))

(defmacro assert-watches-token-index-entry
  [watches token entry]
  `(let [watches# ~watches
         token# ~token
         entry# ~entry]
     (doseq [watch# watches#]
       (assert-watch-token-index-entry watch# token# entry#))))

(defn- get-token-index
  [waiter-url token &
   {:keys [cookies headers query-params] :or {cookies [] headers {} query-params {}}}]
  (let [{:keys [body] :as index-response}
        (make-request waiter-url "/tokens" :query-params {"include" ["metadata", "deleted"]} :cookies cookies)]
    (assert-response-status index-response 200)
    (->> body
         try-parse-json
         (filter #(= token (get % "token")))
         first)))

(deftest ^:parallel ^:integration-fast test-token-watch-maintainer-watches
  (testing-using-waiter-url
    (let [routers (routers waiter-url)
          router-urls (vals routers)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")]

      (testing "watch stream gets initial list of tokens"
        (let [token-name (create-token-name waiter-url ".")
              response (post-token waiter-url (assoc (kitchen-params) :token token-name) :cookies cookies)
              watches (start-watches router-urls cookies)]
          (assert-response-status response 200)
          (try
            (let [entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-watches-token-index-entry watches token-name entry))
            (finally
              (stop-watches watches)
              (delete-token-and-assert waiter-url token-name)))))

      (testing "stream receives index UPDATE (create, update, soft-delete) events for all routers"
        (let [token-name (create-token-name waiter-url ".")
              watches (start-watches router-urls cookies)]
          (assert-watches-token-index-entry watches token-name nil)
          (try
            (let [response (post-token waiter-url (assoc (kitchen-params) :token token-name) :cookies cookies)
                  entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-response-status response 200)
              (assert-watches-token-index-entry watches token-name entry))
            (let [response (post-token waiter-url (assoc (kitchen-params) :token token-name
                                                                          :version "updated-version")
                                       :cookies cookies)
                  entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-response-status response 200)
              (assert-watches-token-index-entry watches token-name entry))
            (delete-token-and-assert waiter-url token-name :hard-delete false)
            (let [entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-watches-token-index-entry watches token-name entry))
            (finally
              (stop-watches watches)
              (delete-token-and-assert waiter-url token-name)))))

      (testing "stream receives DELETE events for all routers"
        (let [token-name (create-token-name waiter-url ".")
              response (post-token waiter-url (assoc (kitchen-params) :token token-name) :cookies cookies)
              watches (start-watches router-urls cookies)]
          (assert-response-status response 200)
          (try
            (let [entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-watches-token-index-entry watches token-name entry)
              (delete-token-and-assert waiter-url token-name)
              (assert-watches-token-index-entry watches token-name nil))
            (finally
              (stop-watches watches)))))

      (testing "stream does not include soft deleted token events without include=deleted query param"
        (let [token-1 (create-token-name waiter-url ".")
              token-2 (create-token-name waiter-url ".")
              res-1 (post-token waiter-url (assoc (kitchen-params) :token token-1) :cookies cookies)
              res-2 (post-token waiter-url (assoc (kitchen-params) :token token-2) :cookies cookies)]
          (assert-response-status res-1 200)
          (assert-response-status res-2 200)
          (try
            (delete-token-and-assert waiter-url token-1 :hard-delete false)
            (let [entry (get-token-index waiter-url token-2 :cookies cookies)
                  watch (start-watch waiter-url cookies :query-params {"include" "metadata" "watch" "true"})]
              (assert-watch-token-index-entry watch token-1 nil)
              (assert-watch-token-index-entry watch token-2 entry)
              (delete-token-and-assert waiter-url token-2 :hard-delete false)
              (async/<!! (async/timeout 4000))
              (assert-watch-token-index-entry watch token-2 entry)
              (stop-watches [watch]))
            (finally
              (delete-token-and-assert waiter-url token-1)
              (delete-token-and-assert waiter-url token-2)))))

      (testing "stream does not include metadata in events without include=metadata query param"
        (let [token-1 (create-token-name waiter-url ".")
              token-2 (create-token-name waiter-url ".")
              res-1 (post-token waiter-url (assoc (kitchen-params) :token token-1) :cookies cookies)
              res-2 (post-token waiter-url (assoc (kitchen-params) :token token-2) :cookies cookies)]
          (assert-response-status res-1 200)
          (assert-response-status res-2 200)
          (try
            (let [watch (start-watch waiter-url cookies :query-params {"watch" "true"})]
              (assert-watch-token-index-entry watch token-1 {"token" token-1
                                                             "owner" (retrieve-username)
                                                             "maintenance" false})
              (assert-watch-token-index-entry watch token-2 {"token" token-2
                                                             "owner" (retrieve-username)
                                                             "maintenance" false})
              (post-token waiter-url (assoc (kitchen-params) :token token-1 :version "update-1"))
              (async/<!! (async/timeout 4000))
              (assert-watch-token-index-entry watch token-1 {"token" token-1
                                                             "owner" (retrieve-username)
                                                             "maintenance" false})
              (assert-watch-token-index-entry watch token-2 {"token" token-2
                                                             "owner" (retrieve-username)
                                                             "maintenance" false})
              (stop-watch watch))
            (finally
              (delete-token-and-assert waiter-url token-1)
              (delete-token-and-assert waiter-url token-2)))))

      (testing "stream filters out tokens in maintenance mode when query param maintenance=false"
        (let [token-1 (create-token-name waiter-url ".")
              token-2 (create-token-name waiter-url ".")
              res-1 (post-token waiter-url (assoc (kitchen-params) :token token-1) :cookies cookies)
              res-2 (post-token waiter-url (assoc (kitchen-params) :token token-2
                                                                   :maintenance {:message "maintenance message"})
                                :cookies cookies)]
          (assert-response-status res-1 200)
          (assert-response-status res-2 200)
          (try
            (let [watch (start-watch waiter-url cookies :query-params {"maintenance" "false" "watch" "true"})]
              (assert-watch-token-index-entry watch token-1 {"token" token-1
                                                             "owner" (retrieve-username)
                                                             "maintenance" false})
              (assert-watch-token-index-entry watch token-2 nil)
              (post-token waiter-url (assoc (kitchen-params) :token token-1 :maintenance {:message "maintenance message"}))
              (async/<!! (async/timeout 4000))
              (assert-watch-token-index-entry watch token-1 {"token" token-1
                                                             "owner" (retrieve-username)
                                                             "maintenance" false})
              (assert-watch-token-index-entry watch token-2 nil)
              (stop-watch watch))
            (finally
              (delete-token-and-assert waiter-url token-1)
              (delete-token-and-assert waiter-url token-2)))))

      (testing "stream filters out tokens not in maintenance mode when query param maintenance=true"
        (let [token-1 (create-token-name waiter-url ".")
              token-2 (create-token-name waiter-url ".")
              res-1 (post-token waiter-url (assoc (kitchen-params) :token token-1) :cookies cookies)
              res-2 (post-token waiter-url (assoc (kitchen-params) :token token-2
                                                                   :maintenance {:message "maintenance message"})
                                :cookies cookies)]
          (assert-response-status res-1 200)
          (assert-response-status res-2 200)
          (try
            (let [watch (start-watch waiter-url cookies :query-params {"maintenance" "true" "watch" "true"})]
              (assert-watch-token-index-entry watch token-1 nil)
              (assert-watch-token-index-entry watch token-2 {"token" token-2
                                                             "owner" (retrieve-username)
                                                             "maintenance" true})
              (post-token waiter-url (assoc (kitchen-params) :token token-1 :maintenance {:message "maintenance message"}))
              (post-token waiter-url (assoc (kitchen-params) :token token-2))
              (async/<!! (async/timeout 4000))
              (assert-watch-token-index-entry watch token-1 {"token" token-1
                                                             "owner" (retrieve-username)
                                                             "maintenance" true})
              (assert-watch-token-index-entry watch token-2 {"token" token-2
                                                             "owner" (retrieve-username)
                                                             "maintenance" true})
              (stop-watch watch))
            (finally
              (delete-token-and-assert waiter-url token-1)
              (delete-token-and-assert waiter-url token-2))))))))
