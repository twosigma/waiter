(ns waiter.instance-tracker-integration-test
  (:require [clojure.test :refer :all]
            [waiter.util.client-tools :refer :all]))

(defn- get-instance-tracker-state
  [waiter-url & {:keys [cookies query-params]}]
  (make-request waiter-url "/state/instance-tracker"
                :cookies cookies
                :query-params query-params
                :method :get))

(deftest ^:parallel ^:integration-fast test-instance-tracker-daemon
  (testing-using-waiter-url
    (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
          routers (routers waiter-url)
          router-urls (vals routers)])

    (testing "no query parameters provides the default state fields"
      (let [{:keys [body] :as response} (get-instance-tracker-state waiter-url)
            default-state-fields #{"last-update-time" "supported-include-params"}]
        (assert-response-status response 200)
        (let [{:strs [state]} (try-parse-json body)
              {:strs [supported-include-params]} state]
          (is (= (set (keys state))
                 default-state-fields))
          (is (= (set supported-include-params)
                 #{"id->failed-instance" "instance-failure-handler"})))))))
