(ns waiter.token-watch-integration-test
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]))

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
                              "reference-type->reference-name" {}
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
                              "reference-type->reference-name" {}
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
                              "reference-type->reference-name" {}
                              "token" token})))]
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls))))

      (testing "hard deleting a token reflects change in token-watch-state"
        (let [_ (delete-token-and-assert waiter-url token)
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (nil? (get-in state ["token->index" token]))))]
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls)))))))

(defn get-token-index
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
    (let [timeout-secs 5
          routers (routers waiter-url)
          router-urls (vals routers)
          {:keys [cookies]} (make-request waiter-url "/waiter-auth")]

      (testing "watch stream gets initial list of tokens"
        (let [token-name (create-token-name waiter-url ".")
              response (post-token waiter-url (assoc (kitchen-params) :token token-name) :cookies cookies)
              watches (start-tokens-watches router-urls cookies)]
          (assert-response-status response 200)
          (try
            (let [entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-watches-token-index-entry watches timeout-secs token-name entry))
            (finally
              (stop-tokens-watches watches)
              (delete-token-and-assert waiter-url token-name)))))

      (testing "stream receives index UPDATE (create, update, soft-delete) events for all routers"
        (let [token-name (create-token-name waiter-url ".")
              watches (start-tokens-watches router-urls cookies)]
          (assert-watches-token-index-entry watches timeout-secs token-name nil)
          (try
            (let [response (post-token waiter-url (assoc (kitchen-params) :token token-name) :cookies cookies)
                  entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-response-status response 200)
              (assert-watches-token-index-entry watches timeout-secs token-name entry))
            (let [response (post-token waiter-url (assoc (kitchen-params) :token token-name
                                                                          :version "updated-version")
                                       :cookies cookies)
                  entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-response-status response 200)
              (assert-watches-token-index-entry watches timeout-secs token-name entry))
            (delete-token-and-assert waiter-url token-name :hard-delete false)
            (let [entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-watches-token-index-entry watches timeout-secs token-name entry))
            (finally
              (stop-tokens-watches watches)
              (delete-token-and-assert waiter-url token-name)))))

      (testing "stream receives DELETE events for all routers"
        (let [token-name (create-token-name waiter-url ".")
              response (post-token waiter-url (assoc (kitchen-params) :token token-name) :cookies cookies)
              watches (start-tokens-watches router-urls cookies)]
          (assert-response-status response 200)
          (try
            (let [entry (get-token-index waiter-url token-name :cookies cookies)]
              (assert-watches-token-index-entry watches timeout-secs token-name entry)
              (delete-token-and-assert waiter-url token-name)
              (assert-watches-token-index-entry watches timeout-secs token-name nil))
            (finally
              (stop-tokens-watches watches)))))

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
                  watch (start-tokens-watch waiter-url cookies :query-params {"include" "metadata" "watch" "true"})]
              (assert-watch-token-index-entry watch timeout-secs token-1 nil)
              (assert-watch-token-index-entry watch timeout-secs token-2 entry)
              (delete-token-and-assert waiter-url token-2 :hard-delete false)
              (assert-watch-token-index-entry-does-not-change watch timeout-secs token-2 entry)
              (stop-tokens-watch watch))
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
            (let [watch (start-tokens-watch waiter-url cookies :query-params {"watch" "true"})]
              (assert-watch-token-index-entry watch timeout-secs token-1 {"token" token-1
                                                                          "owner" (retrieve-username)
                                                                          "maintenance" false
                                                                          "reference-type->reference-name" {}})
              (assert-watch-token-index-entry watch timeout-secs token-2 {"token" token-2
                                                                          "owner" (retrieve-username)
                                                                          "maintenance" false
                                                                          "reference-type->reference-name" {}})
              (post-token waiter-url (assoc (kitchen-params) :token token-1 :version "update-1"))
              (assert-watch-token-index-entry-does-not-change watch timeout-secs token-1 {"token" token-1
                                                                                          "owner" (retrieve-username)
                                                                                          "maintenance" false
                                                                                          "reference-type->reference-name" {}})
              (assert-watch-token-index-entry-does-not-change watch timeout-secs token-2 {"token" token-2
                                                                                          "owner" (retrieve-username)
                                                                                          "maintenance" false
                                                                                          "reference-type->reference-name" {}})
              (stop-tokens-watch watch))
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
            (let [watch (start-tokens-watch waiter-url cookies :query-params {"maintenance" "false" "watch" "true"})]
              (assert-watch-token-index-entry watch timeout-secs token-1 {"token" token-1
                                                                          "owner" (retrieve-username)
                                                                          "maintenance" false
                                                                          "reference-type->reference-name" {}})
              (assert-watch-token-index-entry watch timeout-secs token-2 nil)
              (post-token waiter-url (assoc (kitchen-params) :token token-1 :maintenance {:message "maintenance message"}))
              (assert-watch-token-index-entry-does-not-change watch timeout-secs token-1 {"token" token-1
                                                                                          "owner" (retrieve-username)
                                                                                          "maintenance" false
                                                                                          "reference-type->reference-name" {}})
              (assert-watch-token-index-entry-does-not-change watch timeout-secs token-2 nil)
              (stop-tokens-watch watch))
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
            (let [watch (start-tokens-watch waiter-url cookies :query-params {"maintenance" "true" "watch" "true"})]
              (assert-watch-token-index-entry watch timeout-secs token-1 nil)
              (assert-watch-token-index-entry watch timeout-secs token-2 {"token" token-2
                                                                          "owner" (retrieve-username)
                                                                          "maintenance" true
                                                                          "reference-type->reference-name" {}})
              (post-token waiter-url (assoc (kitchen-params) :token token-1 :maintenance {:message "maintenance message"}))
              (post-token waiter-url (assoc (kitchen-params) :token token-2))
              (assert-watch-token-index-entry watch timeout-secs token-1 {"token" token-1
                                                                          "owner" (retrieve-username)
                                                                          "maintenance" true
                                                                          "reference-type->reference-name" {}})
              (assert-watch-token-index-entry-does-not-change watch timeout-secs token-2 {"token" token-2
                                                                                          "owner" (retrieve-username)
                                                                                          "maintenance" true
                                                                                          "reference-type->reference-name" {}})
              (stop-tokens-watch watch))
            (finally
              (delete-token-and-assert waiter-url token-1)
              (delete-token-and-assert waiter-url token-2))))))))

(deftest ^:parallel ^:integration-fast test-token-watch-streaming-timeout
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          streaming-timeout-ms 5000
          {:keys [exit-fn go-chan headers query-state-fn]}
          (start-tokens-watch waiter-url cookies :query-params {"include" ["metadata"]
                                                                "name" "test-token-watch-streaming-timeout"
                                                                "streaming-timeout" (str streaming-timeout-ms)
                                                                "watch" "true"})
          _ (async/alts!! [go-chan (async/timeout (* 2 streaming-timeout-ms))] :priority true)
          end-time-epoch-ms (System/currentTimeMillis)
          {:keys [initial-event-time-epoch-ms token->index]} (query-state-fn)
          _ (exit-fn)
          _ (is (some? initial-event-time-epoch-ms))
          elapsed-time-ms (- end-time-epoch-ms initial-event-time-epoch-ms)
          assertion-message (str {:elapsed-time-ms elapsed-time-ms
                                  :headers headers})]
      (is (empty? token->index) assertion-message)
      (is (<= (- streaming-timeout-ms 1000) elapsed-time-ms (+ streaming-timeout-ms 1000)) assertion-message))))
