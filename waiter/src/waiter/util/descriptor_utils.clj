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
(ns waiter.util.descriptor-utils)

(defn retrieve-update-time
  "Returns the time at which the specific version of a input map went stale.
   If no such version can be found, then the stale time is unknown and we, conservatively,
   use the last known edit time of the history data.
   Else if the data is not stale or no data is known, nil is returned."
  [data->version data->update-time data->previous current-data data-version]
  (when (and (seq current-data)
             (not= data-version (data->version current-data)))
    (loop [loop-previous-data (data->previous current-data)
           loop-update-time (data->update-time current-data)]
      (if (or (empty? loop-previous-data)
              (= data-version (data->version loop-previous-data)))
        loop-update-time
        (recur (data->previous loop-previous-data)
               (data->update-time loop-previous-data))))))
