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
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [waiter.status-codes :refer :all]
            [waiter.util.client-tools :refer :all]))

(deftest ^:parallel ^:integration-slow ^:resource-heavy test-scale-down-via-delegated-kill
  ;; relies on simple distribution spreading instances across routers, in particular
  ;; to a router that is not triggering the kills (i.e. the leader performing scaling logic)
  (testing-using-waiter-url
    (let [router-count (count (routers waiter-url))]
      (if (> router-count 1)
        (let [requests-per-thread 5
              parallelism router-count
              min-instances 1
              extra-headers {:x-kitchen-delay-ms 5000
                             :x-waiter-distribution-scheme "simple"
                             :x-waiter-min-instances min-instances
                             :x-waiter-name (rand-name)
                             :x-waiter-scale-down-factor 0.99
                             :x-waiter-scale-up-factor 0.99}
              canceled (promise)
              request-fn (fn []
                           (log/info "making kitchen request")
                           (make-request-with-debug-info extra-headers #(make-kitchen-request waiter-url %)))
              _ (log/info "making canary request")
              {:keys [service-id] :as canary-response} (request-fn)]
          (assert-response-status canary-response http-200-ok)
          (with-service-cleanup
            service-id
            (future (parallelize-requests parallelism requests-per-thread #(request-fn)
                                          :verbose true :canceled? (partial realized? canceled)))
            (wait-for #(<= router-count (num-instances waiter-url service-id)) :timeout 180)
            (deliver canceled :canceled)
            (wait-for #(<= min-instances (num-instances waiter-url service-id)) :timeout 180)))
        (log/warn "skipping test since multiple routers were not detected")))))

(defn- trigger-ejecting-of-instance [target-url request-headers cookies]
  (log/info "issuing request which will respond with a 503 on" target-url)
  (let [{:keys [instance-id] :as response}
        (make-request-with-debug-info
          request-headers
          #(make-kitchen-request target-url % :cookies cookies :path "/bad-status" :query-params {"status" http-503-service-unavailable}))]
    (log/info "triggered ejecting of instance" instance-id "on" target-url)
    (assert-response-status response http-503-service-unavailable)
    instance-id))

(defn- instance-ejected-by-router? [router-url service-id instance-id cookies]
  (let [service-state (service-state router-url service-id :cookies cookies)
        responder-state (get-in service-state [:state :responder-state])
        instance-keyword (keyword instance-id)
        instance-state (get-in responder-state [:instance-id->state instance-keyword])]
    (when (some #(= "blacklisted" %) (:status-tags instance-state))
      (get-in responder-state [:instance-id->blacklist-expiry-time instance-keyword]))))

(deftest ^:parallel ^:integration-fast ^:explicit test-instance-ejected-on-503
  ;; Verifies the instance ejected on a 503 response behavior.
  ;; Separate unit tests assert that a ejected instance is not used to process a request.
  (testing-using-waiter-url
    (log/info "Testing instance is ejected on 503 response.")
    (let [router-id->router-url (routers waiter-url)
          num-routers (count router-id->router-url)
          extra-headers {:x-waiter-concurrency-level (* 2 num-routers)
                         :x-waiter-name (rand-name)
                         :x-waiter-scale-up-factor 0.99}
          make-request-fn (fn make-request-fn []
                            (make-request-with-debug-info
                              extra-headers #(make-kitchen-request waiter-url %)))
          {:keys [cookies request-headers instance-id service-id] :as response} (make-request-fn)]
      (assert-response-status response http-200-ok)
      (log/info "canary instance-id:" instance-id)
      (with-service-cleanup
        service-id
        (->> router-id->router-url
          (map (fn [[_ router-url]]
                 (launch-thread
                   (trigger-ejecting-of-instance router-url request-headers cookies))))
          (map async/<!!)
          doall)

        (let [router-id->eject-expiry-time-str
              (pc/map-from-keys
                (fn [router-id]
                  (let [router-url (router-id->router-url router-id)]
                    (instance-ejected-by-router? router-url service-id instance-id cookies)))
                (keys router-id->router-url))]
          (doseq [[router-id _] router-id->router-url]
            (is (-> router-id router-id->eject-expiry-time-str str/blank? not true?)
                (str "instance ejected on router"
                     {:instance-id instance-id
                      :router-id router-id
                      :router-id->eject-expiry-time-str router-id->eject-expiry-time-str}))))))))
