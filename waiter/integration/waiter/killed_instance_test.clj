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
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [plumbing.core :as pc]
            [waiter.util.client-tools :refer :all]))

(defn- trigger-killing-of-instance [waiter-url request-headers]
  (log/info "requesting killing of instance")
  (let [request-headers (assoc (pc/keywordize-map request-headers)
                          :x-kitchen-delay-ms 10000 ;; delay-ms must be greater than die-after-ms
                          :x-kitchen-die-after-ms 1000)
        {:keys [headers]} (make-request waiter-url "/die" :headers request-headers :verbose true)
        instance-id (get (pc/map-keys str/lower-case headers) "x-waiter-backend-id")]
    (log/info "triggered killing of instance" instance-id)
    instance-id))

(defn- assert-killed-instance-blacklisted-by-routers [waiter-url killed-instance-id]
  (let [service-id (subs killed-instance-id 0 (str/index-of killed-instance-id "."))
        router->endpoint (routers waiter-url)
        blacklisted-instances-fn (fn [router-id]
                                   (let [router-url (str (get router->endpoint router-id))
                                         blacklisted-instances-response (make-request router-url (str "/blacklist/" service-id))
                                         blacklisted-instances (set (get (json/read-str (:body blacklisted-instances-response))
                                                                         "blacklisted-instances"))]
                                     (contains? blacklisted-instances killed-instance-id)))]
    (is (some #(blacklisted-instances-fn %) (keys router->endpoint))
        (str "No router has blacklisted " killed-instance-id ", routers: " (keys router->endpoint)))))

(deftest ^:parallel ^:integration-slow test-delegate-kill-instance
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

; Marked explicit due to:
; FAIL in (test-blacklisted-instance-not-reserved) (killed_instance_test.clj)
; expected: (not (contains? (clojure.core/deref other-request-instances) killed-instance-id2))
;   actual: (not (not true))
(deftest ^:parallel ^:integration-slow ^:explicit test-blacklisted-instance-not-reserved
  (testing-using-waiter-url
    (log/info (str "Testing blacklisted instance is not reserved (should take " (colored-time "~2 minutes") ")"))
    (let [num-requests-per-thread 20
          extra-headers {:x-waiter-name (rand-name)
                         :x-waiter-debug "true"}
          request-fn (fn [time instance-map-atom & {:keys [cookies] :or {cookies {}}}]
                       (let [{:keys [headers] :as response}
                             (make-kitchen-request waiter-url (assoc extra-headers :x-kitchen-delay-ms time) :cookies cookies)
                             response-headers (pc/map-keys str/lower-case headers)
                             instance-id (get response-headers "x-waiter-backend-id")
                             correlation-id (get response-headers "x-cid")]
                         (when instance-map-atom (swap! instance-map-atom #(update-in %1 [instance-id] conj correlation-id)))
                         response))
          _ (log/info "making canary request...")
          {:keys [cookies request-headers]} (request-fn 400 nil)
          service-id (retrieve-service-id waiter-url request-headers)
          bombard-with-requests-fn (fn [parallelism time instance-map-atom]
                                     (log/info "making" (str parallelism "x" num-requests-per-thread) "requests to" service-id)
                                     (time-it (str service-id ":" parallelism "x" num-requests-per-thread)
                                              (parallelize-requests
                                                parallelism num-requests-per-thread
                                                #(request-fn time instance-map-atom :cookies cookies))))]
      (log/info "making concurrent requests to scale up service.")
      (bombard-with-requests-fn 5 1000 nil)
      (let [num-tasks-running (num-tasks-running waiter-url service-id)]
        (if (> num-tasks-running 2)
          (let [killed-instance-id1 (trigger-killing-of-instance waiter-url request-headers)
                killed-instance-id2 (trigger-killing-of-instance waiter-url request-headers)
                blacklist-check-chan (async/thread
                                       (assert-killed-instance-blacklisted-by-routers waiter-url killed-instance-id1)
                                       (assert-killed-instance-blacklisted-by-routers waiter-url killed-instance-id2))
                other-request-instances (atom {})]
            (bombard-with-requests-fn 20 400 other-request-instances)
            (log/info "checking whether killed instance was subsequently reserved.")
            (is (not (contains? @other-request-instances killed-instance-id1))
                (str killed-instance-id1 "was used to service requests" (get @other-request-instances killed-instance-id1)))
            (is (not (contains? @other-request-instances killed-instance-id2))
                (str killed-instance-id2 "was used to service requests" (get @other-request-instances killed-instance-id2)))
            (async/<!! blacklist-check-chan))
          (log/warn "skipping assertions as only" num-tasks-running "instances running for" service-id)))
      (delete-service waiter-url service-id)
      (log/info "testing blacklisted instance reservation avoided completed."))))
