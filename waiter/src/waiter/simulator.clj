;;
;;       Copyright (c) 2017 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.simulator
  (:require [clojure.data.json :as json]
            [clojure.walk :as walk]
            [waiter.scaling :as scaling]
            [waiter.utils :refer [non-neg?]])
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
    idle-ticks      The mean number of ticks a client waits between requests.
    request-ticks   The mean number of ticks that it takes to make and complete a request.
    startup-ticks   The mean amount of time it takes for a instance to start.

  State:
    idle-clients      A vector of clients, represented by the tick at which they activate (start a request).
    queued-clients    A count of clients that are sitting in the queue waiting for an instance.
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
    total-idle-server-time The total amount of ticks that servers have been sitting idle.
    scale-ups              The total number of scale up operations.
    scale-downs            The total number of scale down operations."
  [{:strs [idle-ticks request-ticks startup-ticks]}
   {:keys [idle-clients queued-clients idle-servers active-requests starting-servers total-queue-time
           scale-ups scale-downs total-idle-server-time] :as current-state}
   tick scale-amount target-instances client-change-amount]
  {:pre [(pos? idle-ticks)
         (pos? request-ticks)
         (pos? startup-ticks)
         (vector? idle-clients)
         (non-neg? queued-clients)
         (non-neg? idle-servers)
         (integer? idle-servers)
         (integer? queued-clients)
         (vector? active-requests)
         (vector? starting-servers)
         (non-neg? total-queue-time)
         (non-neg? scale-ups)
         (non-neg? scale-downs)
         (non-neg? total-idle-server-time)
         (non-neg? tick)
         (integer? scale-amount)
         (non-neg? target-instances)
         (integer? client-change-amount)]}
  (let [completing-requests (count (filter #{tick} active-requests))
        activating-clients (count (filter #{tick} idle-clients))
        new-clients (if (pos? client-change-amount) client-change-amount 0)
        clients-to-kill (if (neg? client-change-amount) (* client-change-amount -1) 0)
        completing-request-clients-to-kill (min completing-requests clients-to-kill)
        remaining-completing-requests (- completing-requests completing-request-clients-to-kill)
        clients-to-kill (- clients-to-kill completing-request-clients-to-kill)
        idle-clients-to-kill (min (count (filter #(< tick %) idle-clients)) clients-to-kill)
        clients-to-kill (- clients-to-kill idle-clients-to-kill)
        idle-clients' (vec (concat (drop idle-clients-to-kill (filter #(< tick %) idle-clients))
                                   (map (fn [_] (+ tick (randomized idle-ticks)))
                                        (range (+ new-clients remaining-completing-requests)))))
        instances-to-kill (if (neg? scale-amount) (* scale-amount -1) 0)
        starting-servers-to-kill (min (count (filter #(< tick %) starting-servers)) instances-to-kill)
        idle-servers-to-kill (max 0 (- instances-to-kill starting-servers-to-kill))
        remaining-starting-servers (drop-last starting-servers-to-kill starting-servers)
        starting-servers' (vec (concat (filter #(< tick %) remaining-starting-servers)
                                       (if (pos? scale-amount)
                                         (map (fn [_] (+ tick (randomized startup-ticks)))
                                              (range scale-amount)) [])))
        servers-started (count (filter #(= tick %) remaining-starting-servers))
        eligible-servers (+ idle-servers completing-requests servers-started)
        available-servers (max 0 (- eligible-servers idle-servers-to-kill))
        activating-clients-to-kill (min activating-clients clients-to-kill)
        remaining-activating-clients (- activating-clients activating-clients-to-kill)
        clients-to-kill (- clients-to-kill activating-clients-to-kill)
        queued-clients-to-kill (min queued-clients clients-to-kill)
        remaining-queued-clients (- queued-clients queued-clients-to-kill)
        clients-to-kill (- clients-to-kill queued-clients-to-kill)
        interested-clients (+ remaining-queued-clients remaining-activating-clients)
        requests-created (min interested-clients available-servers)
        requests-to-cancel (min (count (filter #(< tick %) active-requests)) clients-to-kill)
        active-requests' (vec (concat (drop requests-to-cancel (filter #(< tick %) active-requests))
                                      (map (fn [_] (+ tick (randomized request-ticks))) (range requests-created))))
        queued-clients' (max 0 (- interested-clients available-servers))
        idle-servers' (+ (max 0 (- available-servers interested-clients)) requests-to-cancel)]
    (assoc current-state :idle-clients idle-clients'
                         :interested-clients interested-clients
                         :activating-clients activating-clients
                         :servers-started servers-started
                         :completing-requests completing-requests
                         :available-servers available-servers
                         :eligible-servers eligible-servers
                         :queued-clients queued-clients'
                         :idle-servers idle-servers'
                         :active-requests active-requests'
                         :starting-servers starting-servers'
                         :outstanding-requests (+ (count active-requests') queued-clients')
                         :total-clients (+ (count active-requests') (count idle-clients') queued-clients')
                         :total-instances (+ (count starting-servers') (count active-requests') idle-servers')
                         :total-queue-time (+ total-queue-time queued-clients)
                         :total-idle-server-time (+ total-idle-server-time idle-servers)
                         :healthy-instances (+ (count active-requests') idle-servers')
                         :scale-amount scale-amount
                         :target-instances target-instances
                         :tick tick
                         :scale-ups (+ scale-ups (if (pos? scale-amount) 1 0))
                         :scale-downs (+ scale-downs (if (neg? scale-amount) 1 0)))))

(let [default-initial-state {:idle-clients []
                             :queued-clients 0
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
                             :scale-downs 0}
      simulation-defaults {"concurrency-level" 1
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
          {:strs [total-ticks scale-ticks] :as config} (merge simulation-defaults config)
          initial-state (merge default-initial-state initial-state)]
      (loop [{:keys [total-clients target-instances] :as state} initial-state
             tick 0
             tickstate []]
        (if (<= tick (min max-total-ticks total-ticks))
          (let [{:keys [scale-amount target-instances]}
                (if (zero? (mod tick scale-ticks))
                  (scaling/scale-app config state)
                  {:scale-amount 0 :target-instances target-instances})]
            (let [client-change-amount (client-curve tick 0)
                  total-clients (or total-clients 0)
                  new-total-clients (+ client-change-amount total-clients)
                  clipped-total-clients (max 0 (min max-total-clients new-total-clients))
                  sane-client-change-amount (- clipped-total-clients total-clients)
                  state' (dotick config state tick scale-amount target-instances sane-client-change-amount)]
              (recur state'
                     (inc tick)
                     (conj tickstate state))))
          tickstate)))))

(defn handle-sim-request
  [{:keys [request-method uri body]}]
  (case uri
    "/sim" (case request-method
             :get {:body (slurp (clojure.java.io/resource "web/sim.html"))}
             :post {:body (let [{:strs [client-curve config]} (json/read-str (slurp body))]
                            (json/write-str (simulate (walk/stringify-keys config) {} (eval (read-string client-curve)))))
                    :headers {"Content-Type" "application/json"}})))
