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
(ns waiter.simulator
  (:require [clojure.data.json :as json]
            [clojure.walk :as walk]
            [waiter.scaling :as scaling]
            [waiter.util.utils :refer [non-neg?]])
  (:import (java.util Random)))

(let [random (Random.)]
  (defn- rand-gaussian []
    (.nextGaussian random)))

(defn randomized
  "Returns a normal value near a mean."
  [mean]
  (int (max 1 (+ mean (* (rand-gaussian) mean 0.1)))))

(defn- dotick
  "Simulates a tick of traffic.  Takes in a state map and returns the state after the tick.

  Configuration:
    clients-exit-immediately  When clients exit, do they do so immediately or wait for the current requests to complete.
    idle-ticks      The mean number of ticks a client waits between requests.
    randomize-times Whether to randomize the event times (e.g. startup, request) or use the face value.
    request-ticks   The mean number of ticks that it takes to make and complete a request.
    startup-ticks   The mean amount of time it takes for a instance to start.

  State:
    clients-to-kill   A count of the clients to kill in this tick.
    idle-clients      A vector of clients, represented by the tick at which they activate (start a request).
    queued-clients    A vector of clients that are sitting in the queue waiting for an instance, represented by the tick at which they arrived.
    idle-servers      A count of servers that are idle waiting for requests.
    active-requests   A vector of requests, represented by the tick at which they complete.
    starting-servers  A vector servers that are starting up, represented by the tick at which their startup is complete.
    tick              The current tick.
  
  Parameters:
    scale-amount          The integer amount (+ or -) by which to scale. 
    target-instances      The continous (float) number of instances we are targetting.  This is state used by the scaling function.
    client-change-amount  The integer amount (+ or -) by which to increase or decrease the number of clients. 

  Stats:
    total-queue-time       The total amount of ticks that clients have been sitting in the queue.
    total-requests         The total number of requests made so far.
    total-idle-server-time The total amount of ticks that servers have been sitting idle.
    total-utilization      The total number of server ticks that have been used to process requests.
    total-waste            The total number of server ticks that have been wasted as the servers were idle.
    utilization-percent    The percentage of servers that are in use during the current tick.
    wait-time-max          The maximum wait time of any queued request during the current tick.
    wait-time-max-ema      The EMA of the maximum wait time with a weight coefficient of 0.1.
    waste-percent          The percentage of servers that are not in use during the current tick.
    scale-ups              The total number of scale up operations.
    scale-downs            The total number of scale down operations."
  [{:strs [clients-exit-immediately idle-ticks randomize-times request-ticks startup-ticks]}
   {:keys [clients-to-kill idle-clients queued-clients wait-time-max wait-time-max-ema idle-servers
           active-requests starting-servers total-queue-time scale-ups scale-downs total-idle-server-time
           total-requests total-utilization total-waste]
    :as current-state}
   tick scale-amount target-instances client-change-amount]
  {:pre [(non-neg? clients-to-kill)
         (pos? idle-ticks)
         (pos? request-ticks)
         (pos? startup-ticks)
         (vector? idle-clients)
         (vector? queued-clients)
         (every? non-neg? queued-clients)
         (every? integer? queued-clients)
         (non-neg? wait-time-max)
         (integer? wait-time-max)
         (non-neg? wait-time-max-ema)
         (float? wait-time-max-ema)
         (non-neg? idle-servers)
         (integer? idle-servers)
         (vector? active-requests)
         (every? non-neg? active-requests)
         (every? integer? active-requests)
         (vector? starting-servers)
         (non-neg? total-queue-time)
         (non-neg? total-utilization)
         (integer? total-utilization)
         (non-neg? total-waste)
         (integer? total-waste)
         (non-neg? scale-ups)
         (non-neg? scale-downs)
         (non-neg? total-idle-server-time)
         (non-neg? tick)
         (integer? scale-amount)
         (non-neg? target-instances)
         (integer? client-change-amount)]}
  (let [generate-data-fn (if randomize-times randomized identity)
        completing-requests (count (filter #{tick} active-requests))
        activating-clients (count (filter #{tick} idle-clients))
        new-clients (if (pos? client-change-amount) client-change-amount 0)
        clients-to-kill (+ clients-to-kill (if (neg? client-change-amount) (* client-change-amount -1) 0))
        completing-request-clients-to-kill (min completing-requests clients-to-kill)
        remaining-completing-requests (- completing-requests completing-request-clients-to-kill)
        clients-to-kill (- clients-to-kill completing-request-clients-to-kill)
        idle-clients-to-kill (min (count (filter #(< tick %) idle-clients)) clients-to-kill)
        clients-to-kill (- clients-to-kill idle-clients-to-kill)
        idle-clients' (vec (concat (drop idle-clients-to-kill (filter #(< tick %) idle-clients))
                                   (map (fn [_] (+ tick (generate-data-fn idle-ticks)))
                                        (range (+ new-clients remaining-completing-requests)))))
        instances-to-kill (if (neg? scale-amount) (* scale-amount -1) 0)
        starting-servers-to-kill (min (count (filter #(< tick %) starting-servers)) instances-to-kill)
        idle-servers-to-kill (max 0 (- instances-to-kill starting-servers-to-kill))
        remaining-starting-servers (drop-last starting-servers-to-kill starting-servers)
        starting-servers' (vec (concat (filter #(< tick %) remaining-starting-servers)
                                       (if (pos? scale-amount)
                                         (map (fn [_] (+ tick (generate-data-fn startup-ticks)))
                                              (range scale-amount)) [])))
        servers-started (count (filter #(= tick %) remaining-starting-servers))
        eligible-servers (+ idle-servers completing-requests servers-started)
        available-servers (max 0 (- eligible-servers idle-servers-to-kill))
        activating-clients-to-kill (min activating-clients clients-to-kill)
        remaining-activating-clients (- activating-clients activating-clients-to-kill)
        clients-to-kill (- clients-to-kill activating-clients-to-kill)
        queued-clients-to-kill (if clients-exit-immediately
                                 (min (count queued-clients) clients-to-kill)
                                 0)
        remaining-queued-clients (- (count queued-clients) queued-clients-to-kill)
        clients-to-kill (- clients-to-kill queued-clients-to-kill)
        interested-clients (+ remaining-queued-clients remaining-activating-clients)
        requests-created (min interested-clients available-servers)
        queued-clients' (concat queued-clients (repeat remaining-activating-clients tick))
        wait-time-max' (->> queued-clients' (map #(- tick %)) (reduce max 0))
        wait-time-max-ema' (+ (* 0.1 wait-time-max') (* 0.9 wait-time-max-ema))
        num-assigned-clients (+ queued-clients-to-kill requests-created)
        queued-clients'' (vec (drop num-assigned-clients queued-clients'))
        requests-to-cancel (if clients-exit-immediately
                             (min (count (filter #(< tick %) active-requests)) clients-to-kill)
                             0)
        active-requests' (vec (concat (drop requests-to-cancel (filter #(< tick %) active-requests))
                                      (map (fn [_] (+ tick (generate-data-fn request-ticks))) (range requests-created))))
        idle-servers' (+ (max 0 (- available-servers interested-clients)) requests-to-cancel)
        num-active-requests (count active-requests')
        total-instances (+ (count starting-servers') num-active-requests idle-servers')
        utilization-percent (if (pos? total-instances)
                              (* 100.0 (/ num-active-requests total-instances))
                              0.0)
        waste-percent (if (pos? total-instances)
                        (- 100.0 utilization-percent)
                        0.0)]
    (assoc current-state
      :activating-clients activating-clients
      :active-requests active-requests'
      :available-servers available-servers
      :clients-to-kill clients-to-kill
      :completing-requests completing-requests
      :eligible-servers eligible-servers
      :healthy-instances (+ num-active-requests idle-servers')
      :idle-clients idle-clients'
      :idle-servers idle-servers'
      :interested-clients interested-clients
      :outstanding-requests (+ num-active-requests (count queued-clients''))
      :queued-clients queued-clients''
      :scale-amount scale-amount
      :scale-ups (+ scale-ups (if (pos? scale-amount) 1 0))
      :scale-downs (+ scale-downs (if (neg? scale-amount) 1 0))
      :servers-started servers-started
      :starting-servers starting-servers'
      :target-instances target-instances
      :tick tick
      :total-clients (+ num-active-requests (count idle-clients') (count queued-clients''))
      :total-idle-server-time (+ total-idle-server-time idle-servers)
      :total-instances total-instances
      :total-queue-time (+ total-queue-time (count queued-clients))
      :total-requests (+ total-requests remaining-activating-clients)
      :total-utilization (+ total-utilization num-active-requests)
      :total-waste (+ total-waste (- total-instances num-active-requests))
      :utilization-percent utilization-percent
      :wait-time-max wait-time-max'
      :wait-time-max-ema wait-time-max-ema'
      :waste-percent waste-percent)))

(let [default-initial-state {:clients-to-kill 0
                             :idle-clients []
                             :queued-clients []
                             :idle-servers 0
                             :active-requests []
                             :starting-servers []
                             :outstanding-requests 0
                             :total-instances 0
                             :healthy-instances 0
                             :expired-instances 0
                             :total-queue-time 0
                             :total-idle-server-time 0
                             :target-instances 0
                             :scale-ups 0
                             :scale-downs 0
                             :total-requests 0
                             :total-utilization 0
                             :total-waste 0
                             :utilization-percent 0.0
                             :wait-time-max 0
                             :wait-time-max-ema 0.0
                             :waste-percent 0.0}
      simulation-defaults {"clients-exit-immediately" true
                           "cpus" 2
                           "concurrency-level" 1
                           "mem" 256
                           "idle-ticks" 2
                           "request-ticks" 5
                           "startup-ticks" 500
                           "scale-factor" 1
                           "scale-up-factor" 0.1
                           "scale-down-factor" 0.01
                           "scale-ticks" 1
                           "jitter-threshold" 0.5
                           "min-instances" 1
                           "max-instances" 1000
                           "total-ticks" 3600
                           "expired-instance-restart-rate" 0.1}]
  (defn simulate
    "Simulates traffic in order to test out the scaling function.
    client-curve is a function from tick to client-change-amount."
    [config initial-state client-curve]
    (let [max-total-ticks (* 2 60 60)
          max-total-clients 100
          {:strs [quanta-cpus quanta-mem scale-ticks total-ticks use-quanta] :as config} (merge simulation-defaults config)
          quanta-constraints {:cpus quanta-cpus
                              :mem quanta-mem}
          initial-state (merge default-initial-state initial-state)]
      (loop [{:keys [total-clients target-instances] :as state} initial-state
             tick 0
             tick-state []]
        (if (<= tick (min max-total-ticks total-ticks))
          (let [{:keys [scale-amount target-instances]}
                (if (zero? (mod tick scale-ticks))
                  (-> (scaling/scale-service config state)
                      (update :scale-amount
                              (fn [scale-amount]
                                (if (and use-quanta (pos? scale-amount))
                                  (scaling/compute-scale-amount-restricted-by-quanta config quanta-constraints scale-amount)
                                  scale-amount))))
                  {:scale-amount 0 :target-instances target-instances})]
            (let [client-change-amount (client-curve tick 0)
                  total-clients (or total-clients 0)
                  new-total-clients (+ client-change-amount total-clients)
                  clipped-total-clients (max 0 (min max-total-clients new-total-clients))
                  sane-client-change-amount (- clipped-total-clients total-clients)
                  state' (dotick config state tick scale-amount target-instances sane-client-change-amount)]
              (recur state'
                     (inc tick)
                     (conj tick-state state))))
          tick-state)))))

(defn handle-sim-request
  [{:keys [request-method uri body]}]
  (case uri
    "/sim" (case request-method
             :get {:body (slurp (clojure.java.io/resource "web/sim.html"))}
             :post {:body (let [{:strs [client-curve config]} (json/read-str (slurp body))]
                            (json/write-str (simulate (walk/stringify-keys config) {} (eval (read-string client-curve)))))
                    :headers {"Content-Type" "application/json"}})))
