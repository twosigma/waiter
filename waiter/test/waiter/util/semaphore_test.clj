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
(ns waiter.util.semaphore-test
  (:require [clojure.test :refer :all]
            [waiter.util.semaphore :refer :all])
  (:import (waiter.util.semaphore NonBlockingSemaphore)))

(deftest test-counting-semaphore-creation
  (is (thrown? AssertionError (create-semaphore -1)))
  (is (thrown? AssertionError (create-semaphore 0)))
  (is (instance? NonBlockingSemaphore (create-semaphore 1)))
  (is (instance? NonBlockingSemaphore (create-semaphore 10))))

(deftest test-counting-semaphore-operations
  (let [max-permits 4
        semaphore (create-semaphore max-permits)]
    (is (= max-permits (total-permits semaphore)))
    (is (= max-permits (available-permits semaphore)))
    (dotimes [n max-permits]
      (is (try-acquire! semaphore))
      (is (= (- max-permits n 1) (available-permits semaphore))))
    (dotimes [_ 10]
      (is (not (try-acquire! semaphore))))
    (is (= (- max-permits 1) (release! semaphore)))
    (is (= 1 (available-permits semaphore)))
    (is (try-acquire! semaphore))
    (is (zero? (available-permits semaphore)))
    (dotimes [n max-permits]
      (is (= (- max-permits n 1) (release! semaphore)))
      (is (= (inc n) (available-permits semaphore))))
    (is (thrown? IllegalStateException (release! semaphore)))
    (is (thrown? IllegalStateException (release! semaphore)))
    (is (try-acquire! semaphore))
    (is (= (dec max-permits) (available-permits semaphore)))))
