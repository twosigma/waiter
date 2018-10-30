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
(ns waiter.killed-instance-test
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [waiter.util.client-tools :refer :all]
            [waiter.util.date-utils :as du]))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-delegate-kill-instance
  (testing-using-waiter-url
    (let [requests-per-thread 5
          router-count (count (routers waiter-url))
          parallelism router-count
          extra-headers {:x-waiter-min-instances 0
                         :x-waiter-distribution-scheme "simple"
                         :x-waiter-scale-down-factor 0.99
                         :x-waiter-scale-up-factor 0.99
                         :x-kitchen-delay-ms 5000
                         :x-waiter-name (rand-name)}
          canceled (promise)
          request-fn (fn []
                       (log/info "making kitchen request")
                       (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %)))
          _ (log/info "making canary request")
          {:keys [service-id] :as canary-response} (request-fn)]
      (assert-response-status canary-response 200)
      (with-service-cleanup
        service-id
        (future (parallelize-requests parallelism requests-per-thread #(request-fn)
                                      :verbose true :canceled? (partial realized? canceled)))
        (wait-for #(<= router-count (num-instances waiter-url service-id)) :timeout 180)
        (deliver canceled :canceled)
        (wait-for #(= 0 (num-instances waiter-url service-id)) :timeout 180)))))

(defn- trigger-blacklisting-of-instance [target-url request-headers cookies]
  (log/info "requesting killing of instance")
  (let [{:keys [instance-id] :as response}
        (make-request-with-debug-info
          request-headers
          #(make-kitchen-request target-url % :cookies cookies :path "/bad-status" :query-params {"status" 503}))]
    (log/info "triggered blacklisting of instance" instance-id "on" target-url)
    (assert-response-status response 503)
    instance-id))

(defn- instance-blacklisted-by-router? [router-url service-id instance-id cookies]
  (let [service-state (service-state router-url service-id :cookies cookies)
        responder-state (get-in service-state [:state :responder-state])
        instance-keyword (keyword instance-id)
        instance-state (get-in responder-state [:instance-id->state instance-keyword])]
    (when (some #(= "blacklisted" %) (:status-tags instance-state))
      (get-in responder-state [:instance-id->blacklist-expiry-time instance-keyword]))))

(deftest ^:parallel ^:integration-slow ^:explicit test-blacklisted-instance-not-reserved
  ;; Verifies that a blacklisted instance is not used to process a request.
  ;; The test first blacklists an instance on all routers.
  ;; It then makes a few requests and verifies if they responded inside the blacklist
  ;; expiry period that the blacklisted instance was not used to process the request.
  (testing-using-waiter-url
    (log/info "Testing blacklisted instance is not reserved")
    (let [router-id->router-url (routers waiter-url)
          num-routers (count router-id->router-url)
          extra-headers {:x-waiter-blacklist-on-503 true
                         :x-waiter-concurrency-level (* 2 num-routers)
                         :x-waiter-name (rand-name)
                         :x-waiter-scale-up-factor 0.99}
          make-request-fn (fn make-request-fn []
                            (make-request-with-debug-info
                              extra-headers #(make-kitchen-request waiter-url %)))
          {:keys [cookies request-headers instance-id service-id] :as response} (make-request-fn)]
      (assert-response-status response 200)
      (log/info "canary instance-id:" instance-id)
      (with-service-cleanup
        service-id
        (dotimes [_ 4] ;; incrementally cause longer blacklist duration for instance
          (->> router-id->router-url
               (map (fn [[_ router-url]]
                      (launch-thread
                        (trigger-blacklisting-of-instance router-url request-headers cookies))))
               (map async/<!!)
               doall))

        (let [router-id->blacklist-expiry-time-str
              (pc/map-from-keys
                (fn [router-id]
                  (let [router-url (router-id->router-url router-id)]
                    (instance-blacklisted-by-router? router-url service-id instance-id cookies)))
                (keys router-id->router-url))]
          (doseq [[router-id _] router-id->router-url]
            (is (-> router-id router-id->blacklist-expiry-time-str str/blank? not)
                (str "instance not blacklisted on router"
                     {:instance-id instance-id
                      :router-id router-id
                      :router-id->blacklist-expiry-time-str router-id->blacklist-expiry-time-str})))

          (let [router-id->blacklist-expiry-time (pc/map-vals du/str-to-date router-id->blacklist-expiry-time-str)
                instance-id->request-count (atom {})]
            (parallelize-requests
              (max 10 (* 4 num-routers))
              1
              (fn []
                (let [request-start-time (t/now)
                      {:keys [instance-id router-id]} (make-request-fn)]
                  (if (t/before? request-start-time (router-id->blacklist-expiry-time router-id))
                    (swap! instance-id->request-count update instance-id (fnil inc 0))
                    (log/warn "request responded after blacklist period, not including instance" instance-id)))))
            (log/info "instance-id->request-count:" @instance-id->request-count)
            (if-not (seq @instance-id->request-count)
              (log/warn "no requests completed after blacklist expiry time of" instance-id)
              (is (not (contains? @instance-id->request-count instance-id))
                  (str {:instance-id instance-id :instance-id->request-count @instance-id->request-count})))))))))
