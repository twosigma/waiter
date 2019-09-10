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
(ns waiter.util.semaphore)

(defprotocol Semaphore
  (total-permits [this]
    "Returns the number of total permits allowed.")
  (available-permits [this]
    "Returns the number of available permits.")
  (state [this]
    "Returns internal state of the semaphore as a map.")
  (try-acquire! [this]
    "Acquires a permit, if one is available and returns immediately, with the value true, reducing the number of available permits by one.
     If no permit is available then this method will return immediately with the value false.")
  (release! [this]
    "Releases a permit, increasing the number of available permits by one.
     Returns the number of acquired permits."))

;; Restricts the semaphore to only non-blocking operations.
;; Unlike java.util.concurrent.Semaphore, provides max permits allowed.
(defrecord NonBlockingSemaphore [acquired-permits-counter max-permits]
  Semaphore
  (total-permits [_]
    max-permits)
  (available-permits [_]
    (- max-permits @acquired-permits-counter))
  (state [this]
    {:allowed (total-permits this)
     :available (available-permits this)})
  (try-acquire! [_]
    (let [acquired-atom (atom false)]
      (swap! acquired-permits-counter
             (fn acquire-permit [acquired-permits]
               (if (< acquired-permits max-permits)
                 (do
                   (reset! acquired-atom true)
                   (inc acquired-permits))
                 acquired-permits)))
      @acquired-atom))
  (release! [_]
    (swap! acquired-permits-counter
           (fn release-permit [acquired-permits]
             (when-not (pos? acquired-permits)
               (throw (IllegalStateException. "No acquired permits to release!")))
             (dec acquired-permits)))))

(defn create-semaphore
  "Creates a Semaphore with the given number of permits."
  [max-permits]
  {:pre [(pos? max-permits)]}
  (->NonBlockingSemaphore (atom 0) max-permits))