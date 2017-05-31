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
(ns waiter.work-stealing-integration-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.client-tools :refer :all]))

(deftest ^:parallel ^:integration-slow test-work-stealing-load-balancing
  (testing-using-waiter-url
    (let [request-fn (fn [router-url waiter-headers & {:keys [cookies] :or {cookies nil}}]
                       (make-request-with-debug-info
                         waiter-headers
                         #(make-request router-url "/endpoint" :headers % :cookies cookies)))
          max-instances 6
          extra-headers (merge (kitchen-request-headers)
                               {:x-waiter-name (rand-name "testworkstealingloadbalancing")
                                :x-waiter-max-instances max-instances
                                :x-waiter-scale-up-factor 0.999
                                :x-waiter-scale-down-factor 0.001
                                :x-waiter-work-stealing true})
          {:keys [service-id]} (request-fn waiter-url extra-headers)
          print-metrics (fn [service-id]
                          (let [router-ids (keys (routers waiter-url))
                                service-metrics (:metrics (service-settings waiter-url service-id))]
                            (log/info "Aggregate process metrics:" (get-in service-metrics [:aggregate :timers :process]))
                            (doseq [router-id router-ids]
                              (log/info router-id " process metrics:" (get-in service-metrics [:routers (keyword router-id) :timers :process])))))]

      ;; set up
      (log/info "Service ID:" service-id)
      (parallelize-requests (+ max-instances 2) 2 #(request-fn waiter-url (assoc extra-headers :x-kitchen-delay-ms 6000)))
      (log/info "Num tasks running" (num-tasks-running waiter-url service-id))
      (print-metrics service-id)
      ;; actual test
      (let [{:keys [cookies]} (make-request waiter-url "/waiter-auth")
            router->endpoint (routers waiter-url)
            router (-> router->endpoint (keys) (sort) (first))
            router-url (get router->endpoint router)]
        (when (= 1 (count router->endpoint))
          (log/info "Assertions will be trivially true as only one router is running"))
        (log/info "Forwarding all requests to router" router "at url" router-url)
        (let [responses (parallelize-requests (* 4 max-instances)
                                              1
                                              #(request-fn router-url (assoc extra-headers :x-kitchen-delay-ms 16000)
                                                           :cookies cookies)
                                              :verbose true)
              _ (log/debug "Num responses:" (count responses))
              responses-map (reduce (fn [accum {:keys [instance-id]}]
                                      (update-in accum [instance-id] (fnil inc 0)))
                                    {} responses)]
          (print-metrics service-id)
          (is (pos? (count responses-map))
              "Error in receiving responses, a possible bug in parallelize-requests!")
          (log/info (str "Response distribution:" responses-map))
          (let [service-settings (service-settings waiter-url service-id)
                num-instances (count (get-in service-settings [:instances :active-instances]))]
            (log/info (str "Num instances:" num-instances))
            (when (not= num-instances (count responses-map))
              (log/info "Instances:" (get service-settings :instances)))
            (is (<= num-instances (count responses-map))))
          (is (every? (fn [[_ num-requests]] (pos? num-requests)) responses-map)
              (str "Response distribution:" responses-map)))
        ;; cleanup
        (log/info "Deleting" service-id)
        (delete-service waiter-url service-id)))))

(deftest ^:parallel ^:integration-slow test-slots-in-use-consistency
  (testing-using-waiter-url
    (let [headers {:x-waiter-name (rand-name "test-slots-in-use-consistency")
                   :x-waiter-max-instances 7
                   :x-kitchen-delay-ms 4000}
          router->endpoint (routers waiter-url)
          {:keys [service-id request-headers cookies]} (make-request-with-debug-info headers #(make-kitchen-request waiter-url %))
          _ (parallelize-requests 7 20 #(let [router (-> router->endpoint (keys) (rand-nth))
                                              target-url (router->endpoint router)]
                                         (make-kitchen-request target-url (dissoc request-headers "x-cid") :cookies cookies)))
          waiter-settings (waiter-settings waiter-url)
          _ (Thread/sleep (* 4 (get-in waiter-settings [:work-stealing :offer-help-interval-ms])))
          service-settings (service-settings waiter-url service-id)
          routers (keys (get-in service-settings [:metrics :routers]))]
      (when (= 1 (count router->endpoint))
        (log/info "Assertions will be trivially true as only one router is running"))
      (doseq [router routers]
        (let [slots-in-use (get-in service-settings [:metrics :routers router :counters :instance-counts :slots-in-use])]
          (is (zero? slots-in-use) (str "Expected zero slots-in-use, but found" slots-in-use "in router" (name router)))))
      (delete-service waiter-url service-id))))