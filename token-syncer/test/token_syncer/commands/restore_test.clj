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
(ns token-syncer.commands.restore-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [token-syncer.cli :as cli]
            [token-syncer.commands.restore :refer :all]
            [token-syncer.file-utils :as file-utils]
            [token-syncer.utils :as utils]))

(defn write-to-file
  "Writes the content to the specified file as a json string."
  [filename content]
  (log/info "writing" (count content) "tokens to" filename ":" (keys content))
  (->> content
    json/write-str
    (spit filename)))

(deftest test-restore
  (let [test-cluster-url "http://www.test-cluster.com"
        make-waiter-api (fn [token->data-store]
                          {:load-token-list (fn load-token-list-in-test-restore [cluster-url]
                                              (is (= test-cluster-url cluster-url))
                                              [{"etag" "te1C" "last-update-time" (utils/millis->iso8601 2020062100) "token" "t1"}
                                               {"etag" "te2C" "last-update-time" (utils/millis->iso8601 2020062200) "token" "t2"}
                                               {"etag" "te3A" "last-update-time" (utils/millis->iso8601 2020060300) "token" "t3"}
                                               {"etag" "te5B" "last-update-time" (utils/millis->iso8601 2020061500) "token" "t5"}])
                           :store-token (fn store-token-in-test-restore [cluster-url token etag token-description]
                                          (is (= test-cluster-url cluster-url))
                                          (is (not (contains? @token->data-store token)))
                                          (swap! token->data-store
                                                 assoc token {:etag etag :token-description token-description}))})
        file-operations-api {:read-from-file file-utils/read-from-file}]

    (testing "restore in force mode"
      (file-utils/with-temp-file
        [my-temp-file (file-utils/create-temp-file)]
        (let [my-temp-file-path (file-utils/file->path my-temp-file)
              token->data-store (atom {})
              waiter-api (make-waiter-api token->data-store)
              backup-content {"t1" {"description" {"etag" "te1B" "last-update-time" 2020061100 "version" "v1B"}}
                              "t3" {"description" {"etag" "te3B" "last-update-time" 2020061300 "version" "v3B"}}
                              "t5" {"description" {"etag" "te5B" "last-update-time" 2020061500 "version" "v5B"}}
                              "t7" {"description" {"etag" "te7B" "last-update-time" 2020061700 "version" "v7B"}}}]
          (write-to-file my-temp-file-path backup-content)

          (restore-tokens waiter-api file-operations-api test-cluster-url my-temp-file-path true)

          (is (= {"t1" {:etag "te1C" :token-description {"etag" "te1B" "last-update-time" 2020061100 "version" "v1B"}}
                  "t3" {:etag "te3A" :token-description {"etag" "te3B" "last-update-time" 2020061300 "version" "v3B"}}
                  "t5" {:etag "te5B" :token-description {"etag" "te5B" "last-update-time" 2020061500 "version" "v5B"}}
                  "t7" {:etag nil :token-description {"etag" "te7B" "last-update-time" 2020061700 "version" "v7B"}}}
                 @token->data-store)))))

    (testing "restore without force mode"
      (file-utils/with-temp-file
        [my-temp-file (file-utils/create-temp-file)]
        (let [my-temp-file-path (file-utils/file->path my-temp-file)
              token->data-store (atom {})
              waiter-api (make-waiter-api token->data-store)
              backup-content {"t1" {"description" {"etag" "te1B" "last-update-time" 2020061100 "version" "v1B"}}
                              "t3" {"description" {"etag" "te3B" "last-update-time" 2020061300 "version" "v3B"}}
                              "t5" {"description" {"etag" "te5B" "last-update-time" 2020061500 "version" "v5B"}}
                              "t7" {"description" {"etag" "te7B" "last-update-time" 2020061700 "version" "v7B"}}}]
          (write-to-file my-temp-file-path backup-content)

          (restore-tokens waiter-api file-operations-api test-cluster-url my-temp-file-path false)

          (is (= {"t3" {:etag "te3A" :token-description {"etag" "te3B" "last-update-time" 2020061300 "version" "v3B"}}
                  "t7" {:etag nil :token-description {"etag" "te7B" "last-update-time" 2020061700 "version" "v7B"}}}
                 @token->data-store)))))))

(deftest test-restore-tokens-config
  (let [test-command-config (assoc restore-tokens-config :command-name "test-command")
        waiter-api {:load-token (constantly {})
                    :load-token-list (constantly {})}
        context {:waiter-api waiter-api}]
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
    (let [file "some-file.txt"
          cluster-url "http://cluster-1.com"
          args [file cluster-url]
          invocation-promise (promise)]
      (with-redefs [restore-tokens (fn [in-waiter-api in-file-operations-api in-cluster-url in-file in-force]
                                     (is (= waiter-api in-waiter-api))
                                     (is (contains? in-file-operations-api :read-from-file))
                                     (is (= cluster-url in-cluster-url))
                                     (is (= file in-file))
                                     (is (not in-force))
                                     (deliver invocation-promise ::invoked)
                                     {:error 4})]
        (is (= {:exit-code 4
                :message "test-command: exiting with code 4"}
               (cli/process-command test-command-config context args)))
        (is (= ::invoked (deref invocation-promise 0 ::un-initialized)))))))
