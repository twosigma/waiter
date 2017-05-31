;;
;;       Copyright (c) 2017 Two Sigma Investments, LLC.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LLC.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.new-app-test
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [waiter.client-tools :refer :all]))

(defn- new-app-stress-test [waiter-url]
  (log/info (str "Stress test create new app"))
  (let [service-id-prefix (rand-name "teststressnewapp")
        service-ids-atom (atom (set []))
        num-apps 5
        num-threads 200
        cookies-atom (atom {})]
    (parallelize-requests
      num-threads
      10
      (fn [& {:keys [cookies] :or {cookies {}}}]
        (let [service-id-header (str service-id-prefix "-" (rand-int num-apps))
              extra-headers {:x-waiter-name service-id-header}
              {:keys [cookies service-id]}
              (make-request-with-debug-info
                extra-headers
                #(make-kitchen-request waiter-url % :cookies @cookies-atom))]
          (when-not (seq @cookies-atom)
            (reset! cookies-atom cookies))
          (swap! service-ids-atom conj service-id))))
    (is (= num-apps (count @service-ids-atom)))
    (is (every? #(str/includes? % service-id-prefix) @service-ids-atom))
    (doall (map #(delete-service waiter-url %) @service-ids-atom))))

(deftest ^:parallel ^:integration-fast test-new-app
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name "test-new-app"), :x-kitchen-echo "true"}
          lorem-ipsum "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
          {:keys [service-id body]}
          (make-request-with-debug-info headers #(make-kitchen-request waiter-url % :body lorem-ipsum))]
      (is (= lorem-ipsum body))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-new-app-stress
  (testing-using-waiter-url (new-app-stress-test waiter-url)))

(deftest ^:parallel ^:integration-slow test-new-app-gc
  (testing-using-waiter-url
    (let [idle-timeout-in-mins 1
          {:keys [service-id]} (make-request-with-debug-info
                                 {:x-waiter-name (rand-name "test-new-app-gc")
                                  :x-waiter-idle-timeout-mins idle-timeout-in-mins}
                                 #(make-kitchen-request waiter-url %))
          marathon-url (str (marathon-url waiter-url) "/v2/apps/" service-id)
          marathon-headers {:spnego-auth true, :throw-exceptions false}
          marathon-app #(let [response (http/get marathon-url marathon-headers)]
                         (log/debug response)
                         response)]
      (log/debug "Waiting for" service-id "to show up...")
      (is (wait-for #(= 200 (:status (marathon-app))) :interval 1))
      (log/debug "Waiting for" service-id "to go away...")
      (is (wait-for #(= 404 (:status (marathon-app))) :interval 10))
      (when (not= 404 (:status (marathon-app)))
        (delete-service waiter-url service-id)))))

(defn- grace-period
  "Fetches from Marathon and returns the grace period in seconds for the given app"
  [waiter-url service-id]
  (let [marathon-url (marathon-url waiter-url)
        app-info-url (str marathon-url "/v2/apps/" service-id)
        app-info-response (http/get app-info-url {:spnego-auth true})
        app-info-map (walk/keywordize-keys (json/read-str (:body app-info-response)))]
    (:gracePeriodSeconds (first (:healthChecks (:app app-info-map))))))

(deftest ^:parallel ^:integration-fast test-default-grace-period
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name "test-default-grace-period")}
          {:keys [service-id]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          settings-json (waiter-settings waiter-url)
          default-grace-period (get-in settings-json [:service-description-defaults :grace-period-secs])]
      (is (= default-grace-period (grace-period waiter-url service-id)))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-fast test-custom-grace-period
  (testing-using-waiter-url
    (let [custom-grace-period-secs 120
          headers {:x-waiter-name (rand-name "test-custom-grace-period")
                   :x-waiter-grace-period-secs custom-grace-period-secs}
          {:keys [service-id]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))]
      (is (= custom-grace-period-secs (grace-period waiter-url service-id)))
      (delete-service waiter-url service-id))))
