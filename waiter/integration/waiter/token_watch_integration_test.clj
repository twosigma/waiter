(ns waiter.token-watch-integration-test
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.test :refer :all]
            [waiter.status-codes :refer :all]
            [waiter.util.async-utils :as au]
            [waiter.util.client-tools :refer :all]))

(defn await-goal-response-for-all-routers
  "Returns true if the goal-response-fn was satisfied with the response from request-fn for all routers before
  timeout-ms by polling every interval-ms."
  [goal-response-fn request-fn router-urls timeout-ms interval-ms]
  (let [timeout-ch (async/timeout timeout-ms)
        timer-ch (au/timer-chan interval-ms)]
    (loop []
      (let [[_ ch] (async/alts!! [timer-ch timeout-ch] :priority true)
            responses (for [router-url router-urls]
                        (request-fn router-url))]
        (cond
          (every? goal-response-fn responses) true
          (= ch timeout-ch) false
          :else (recur))))))

(deftest ^:parallel ^:integration-fast test-token-watch-maintainer
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          routers (routers waiter-url)
          router_urls (vals routers)
          {:keys [body] :as response} (get-token-watch-maintainer-state waiter-url)
          {:strs [router-id state]} (try-parse-json body)
          watch-state-request-fn (fn [router-url]
                                   (get-token-watch-maintainer-state router-url
                                                                     :query-params "include=token-index-map"
                                                                     :cookies cookies))
          token (create-token-name waiter-url ".")
          router-url (router-endpoint waiter-url router-id)
          default-state-fields #{"last-update-time" "watch-count"}]

      (testing "no query parameters provide default state fields"
        (assert-response-status response 200)
        (is (= (set (keys state))
               default-state-fields)))

      (testing "include token-index-map map"
        (let [{:keys [body] :as response}
              (get-token-watch-maintainer-state router-url :query-params "include=token-index-map" :cookies cookies)
              {:strs [state]} (try-parse-json body)]
          (assert-response-status response 200)
          (is (= (set (keys state))
                 (set/union default-state-fields #{"token-index-map"})))))

      (testing "creating token reflects change in token-watch-state"
        (let [last-update-time (System/currentTimeMillis)
              response (post-token waiter-url
                                   {:token token :cpus 1 :last-update-time last-update-time}
                                   :query-params {"update-mode" "admin"})
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (= (get-in state ["token-index-map" token])
                             {"deleted" false
                              "etag" (token->etag waiter-url token)
                              "last-update-time" last-update-time
                              "maintenance" false
                              "owner" (retrieve-username)
                              "token" token})))]
          (assert-response-status response 200)
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls 5000 100))))

      (testing "updating a token reflects change in token-watch-state"
        (let [token (create-token-name waiter-url ".")
              last-update-time (System/currentTimeMillis)
              response (post-token waiter-url
                                   {:token token :cpus 2 :last-update-time last-update-time}
                                   :query-params {"update-mode" "admin"})
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (= (get-in state ["token-index-map" token])
                             {"deleted" false
                              "etag" (token->etag waiter-url token)
                              "last-update-time" last-update-time
                              "maintenance" false
                              "owner" (retrieve-username)
                              "token" token})))]
          (assert-response-status response 200)
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls 5000 100))))

      (testing "soft deleting a token reflects change in token-watch-state"
        (let [_ (delete-token-and-assert waiter-url token :hard-delete false)
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (= (-> state
                                 (get-in ["token-index-map" token])
                                 (dissoc "last-update-time"))
                             {"deleted" true
                              "etag" nil
                              "maintenance" false
                              "owner" (retrieve-username)
                              "token" token})))]
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls 5000 100))))

      (testing "hard deleting a token reflects change in token-watch-state"
        (let [_ (delete-token-and-assert waiter-url token)
              goal-fn (fn [{:keys [body] :as response}]
                        (let [{:strs [state]} (try-parse-json body)]
                          (assert-response-status response 200)
                          (nil? (get-in state ["token-index-map" token]))))]
          (is (await-goal-response-for-all-routers goal-fn watch-state-request-fn router_urls 5000 100)))))))
