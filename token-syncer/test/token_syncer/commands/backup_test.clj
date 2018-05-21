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
(ns token-syncer.commands.backup-test
  (:require [clojure.test :refer :all]
            [token-syncer.cli :as cli]
            [token-syncer.commands.backup :refer :all])
  (:import (java.io File)))

(defn- create-temp-file
  "Creates a temporary file."
  []
  (File/createTempFile "temp-" ".json"))

(defn- file->path
  "Returns the absolute path of the file."
  [^File file]
  (.getAbsolutePath file))

(defmacro with-temp-file
  "Uses a temporary file for some content and deletes it immediately after running the body."
  [bindings & body]
  (cond
    (and (= (count bindings) 2) (symbol? (bindings 0)))
    `(let ~bindings
       (try
         ~@body
         (finally
           (.delete ~(bindings 0)))))
    :else (throw (IllegalArgumentException.
                   "with-temp-file requires a single Symbol in bindings"))))

(deftest test-read-from-and-write-to-file
  (with-temp-file
    [my-temp-file (create-temp-file)]
    (let [my-temp-file-path (file->path my-temp-file)
          initial-content {"token-1" {"foo1" "bar", "hello" "world1"}
                           "token-3" {"foo3" "bar", "hello" "world3"}
                           "token-5" {"foo5" "bar", "hello" "world5"}}]
      (write-to-file my-temp-file-path initial-content)
      (is (= initial-content (read-from-file my-temp-file-path))))))

(deftest test-backup
  (let [test-cluster-url "http://www.test-cluster.com"
        waiter-api {:load-token (fn [cluster-url token]
                                  (is (= test-cluster-url cluster-url))
                                  {:description {"cpus" 1, "mem" 1024, "name" token, "owner" "test-user"}
                                   :token-etag (str "etag-" token)})
                    :load-token-list (fn [cluster-url]
                                       (is (= test-cluster-url cluster-url))
                                       [{"etag" "etag-token-1", "token" "token-1"}
                                        {"etag" "etag-token-2", "token" "token-2"}
                                        {"etag" "etag-token-3", "token" "token-3"}])}
        file-operations-api {:read-from-file read-from-file
                             :write-to-file write-to-file}]

    (testing "backup in accrete mode"
      (with-temp-file
        [my-temp-file (create-temp-file)]
        (let [my-temp-file-path (file->path my-temp-file)
              initial-content {"token-1" {"foo1" "bar", "hello" "world1", "token-etag" "etag-token-1-old"}
                               "token-3" {"foo3" "bar", "hello" "world3", "token-etag" "etag-token-3-old"}
                               "token-5" {"foo5" "bar", "hello" "world5", "token-etag" "etag-token-5-old"}
                               "token-7" {"foo7" "bar", "hello" "world7", "token-etag" "etag-token-7-old"}}]
          (write-to-file my-temp-file-path initial-content)

          (backup-tokens waiter-api file-operations-api test-cluster-url my-temp-file-path true)

          (is (= {"token-1" {"description" {"cpus" 1, "mem" 1024, "name" "token-1", "owner" "test-user"}
                             "token-etag" "etag-token-1"}
                  "token-2" {"description" {"cpus" 1, "mem" 1024, "name" "token-2", "owner" "test-user"}
                             "token-etag" "etag-token-2"}
                  "token-3" {"description" {"cpus" 1, "mem" 1024, "name" "token-3", "owner" "test-user"}
                             "token-etag" "etag-token-3"}
                  "token-5" {"foo5" "bar", "hello" "world5", "token-etag" "etag-token-5-old"}
                  "token-7" {"foo7" "bar", "hello" "world7", "token-etag" "etag-token-7-old"}}
                 (read-from-file my-temp-file-path))))))

    (testing "backup without accrete mode"
      (with-temp-file
        [my-temp-file (create-temp-file)]
        (let [my-temp-file-path (file->path my-temp-file)
              initial-content {"token-1" {"foo1" "bar", "hello" "world1"}
                               "token-3" {"foo3" "bar", "hello" "world3"}
                               "token-5" {"foo5" "bar", "hello" "world5"}
                               "token-7" {"foo7" "bar", "hello" "world7"}}]
          (write-to-file my-temp-file-path initial-content)

          (backup-tokens waiter-api file-operations-api test-cluster-url my-temp-file-path false)

          (is (= {"token-1" {"description" {"cpus" 1, "mem" 1024, "name" "token-1", "owner" "test-user"}
                             "token-etag" "etag-token-1"}
                  "token-2" {"description" {"cpus" 1, "mem" 1024, "name" "token-2", "owner" "test-user"}
                             "token-etag" "etag-token-2"}
                  "token-3" {"description" {"cpus" 1, "mem" 1024, "name" "token-3", "owner" "test-user"}
                             "token-etag" "etag-token-3"}}
                 (read-from-file my-temp-file-path))))))))

(deftest test-backup-tokens-config
  (let [test-command-config (assoc backup-tokens-config :command-name "test-command")
        waiter-api {:load-token (constantly {})
                    :load-token-list (constantly {})}
        context {:waiter-api waiter-api}
        file-operations-api {:read-from-file (constantly {})
                             :write-to-file (constantly {})}]
    (with-redefs [init-file-operations-api (constantly file-operations-api)]
      (let [args []]
        (is (= {:exit-code 1
                :message "test-command: expected 2 arguments FILE URL, provided 0: []"}
               (cli/process-command test-command-config context args))))
      (with-out-str
        (let [args ["-h"]]
          (is (= {:exit-code 0
                  :message "test-command: displayed documentation"}
                 (cli/process-command test-command-config context args)))))
      (let [args ["some-file.txt" "http://cluster-1.com" "http://cluster-2.com"]]
        (is (= {:exit-code 1
                :message "test-command: expected 2 arguments FILE URL, provided 3: [\"some-file.txt\" \"http://cluster-1.com\" \"http://cluster-2.com\"]"}
               (cli/process-command test-command-config context args))))
      (let [args ["some-file.txt" "http://cluster-1.com"]]
        (with-redefs [backup-tokens (fn [in-waiter-api in-file-operations-api cluster-url file accrete]
                                      (is (= waiter-api in-waiter-api))
                                      (is (= file-operations-api in-file-operations-api))
                                      (println "backup-tokens:" cluster-url file accrete))]
          (is (= {:exit-code 0
                  :message "test-command: exiting with code 0"}
                 (cli/process-command test-command-config context args))))))))
