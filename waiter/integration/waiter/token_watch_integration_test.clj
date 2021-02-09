(ns waiter.token-watch-integration-test
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
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
