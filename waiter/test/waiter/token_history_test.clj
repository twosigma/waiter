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
(ns waiter.token-history-test
  (:require [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clojure.test :refer :all]
            [waiter.descriptor :refer :all]
            [waiter.kv :as kv]
            [waiter.service-description :as sd]
            [waiter.status-codes :refer :all]))

(def ^:private profile->defaults {"webapp" {"concurrency-level" 120
                                   "fallback-period-secs" 100}
                         "service" {"concurrency-level" 30
                                    "fallback-period-secs" 90
                                    "permitted-user" "*"}})

(defn- generate-default-builder
  [kv-store]
  (let [metric-group-mappings []
        service-description-defaults {"metric-group" "other" "permitted-user" "*"}
        constraints {"cpus" {:max 100} "mem" {:max 1024}}
        builder-context {:constraints constraints
                         :kv-store kv-store
                         :metric-group-mappings metric-group-mappings
                         :profile->defaults profile->defaults
                         :service-description-defaults service-description-defaults}]
    (sd/create-default-service-description-builder builder-context)))

(defn- generate-descriptor
  ([kv-store builder service-description-template token-data-map token]
   "uses service-description-template for service-description-template in :sources as well as :service-description
    and :core-service-description in the resultant descriptor"
   (generate-descriptor kv-store builder service-description-template token-data-map token service-description-template))
  ([kv-store builder service-description-template token-data-map token service-description-effective]
   "uses service-description-effective for actual values of both :service-description and :core-service-description in
    resultant descriptor"
   (let [service-id-prefix "service-prefix-"
         service-description-defaults {}
         token-defaults {"fallback-period-secs" 300
                         "service-mapping" "legacy"}
         metric-group-mappings []
         attach-service-defaults-fn #(sd/merge-defaults % service-description-defaults profile->defaults metric-group-mappings)
         attach-token-defaults-fn #(sd/attach-token-defaults % token-defaults profile->defaults)
         username "test-user"
         assoc-run-as-user-approved? (constantly false)
         build-service-description-and-id-helper (sd/make-build-service-description-and-id-helper
                                                  kv-store service-id-prefix username builder assoc-run-as-user-approved?)
         sources {:headers {}
                         :service-description-template service-description-template
                         :token->token-data {token token-data-map}
                         :token-authentication-disabled false
                         :token-preauthorized false
                         :token-sequence [token]}
         passthrough-headers {}
         waiter-headers {}]
       (-> {:passthrough-headers passthrough-headers
            :sources sources
            :waiter-headers waiter-headers}
           (build-service-description-and-id-helper false)
           (attach-token-fallback-source
            attach-service-defaults-fn attach-token-defaults-fn build-service-description-and-id-helper)
           (assoc :service-description service-description-effective :core-service-description service-description-effective)))))

(deftest test-descriptor->comprehensive-history
  (let [image "foo"
        service-description-1 {"cmd" "ls"
                               "cpus" 1
                               "mem" 32
                               "run-as-user" "ru"
                               "version" "na"
                               "cmd-type" "shell"
                               "image" image}
        token "token-1"
        time-1 (t/date-time 2022 1)
        token-data-1 (assoc service-description-1 "last-update-time" (tc/to-long time-1))

        service-description-2 (assoc service-description-1 "mem" 64)
        time-2 (t/date-time 2022 2)
        token-data-2 (assoc service-description-2
                            "last-update-time" (tc/to-long time-2)
                            "previous" token-data-1)

        service-description-3 (assoc service-description-1 "cmd" "echo some-command")
        time-3 (t/date-time 2022 3)
        token-data-3 (assoc service-description-3
                            "last-update-time" (tc/to-long time-3)
                            "previous" token-data-2)

        max-history 5]

    (testing "token with no updates and no effective fields addition"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            builder (generate-default-builder kv-store)
            descriptor (generate-descriptor kv-store builder service-description-1 token-data-1 token)
            token-history (descriptor->comprehensive-history kv-store builder max-history token descriptor)]

        (is (= 1 (count token-history)))
        (is (= time-1 (-> token-history first :update-time)))
        (is (= :token (-> token-history first :source-component)))
        (is (= service-description-1 (-> token-history first :core-service-description)))
        (is (= (:service-id descriptor) (-> token-history first :service-id)))))

    (testing "token with no updates and different image-effective"
      (let [image-effective "foo-with-specific-hash 1234567"
            service-description-1-effective (assoc service-description-1 "image" image-effective)
            kv-store (kv/->LocalKeyValueStore (atom {}))
            builder (generate-default-builder kv-store)
            descriptor (generate-descriptor kv-store builder service-description-1 token-data-1 token service-description-1-effective)
            token-history (descriptor->comprehensive-history kv-store builder max-history token descriptor)]

        (is (= 1 (count token-history)))
        (is (= time-1 (-> token-history first :update-time)))
        (is (= :token (-> token-history first :source-component)))
        (is (= {"cmd" "ls" "cpus" 1 "mem" 32 "run-as-user" "ru" "version" "na" "cmd-type" "shell" "image" image "image-effective" image-effective}
               (-> token-history first :core-service-description)))
        (is (= (:service-id descriptor) (-> token-history first :service-id)))))

    (testing "token with history of exclusively direct updates"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            builder (generate-default-builder kv-store)
            descriptor (generate-descriptor kv-store builder service-description-3 token-data-3 token)
            token-history (descriptor->comprehensive-history kv-store builder max-history token descriptor)]
        (is (= 3 (count token-history)))
        (is (= [time-3 time-2 time-1] (mapv :update-time token-history)))
        (is (= [:token :token :token] (mapv :source-component token-history)))
        (is (= [service-description-3 service-description-2 service-description-1] (mapv :core-service-description token-history)))
        ))

    (testing "token with history of exclusively indirect updates (with effective expansion)"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            builder (generate-default-builder kv-store)
            image-effective-1 "foo-with-specific-hash 1234"
            descriptor (generate-descriptor kv-store builder service-description-1 token-data-1 token (assoc service-description-1 "image" image-effective-1))
            image-effective-2 "foo-with-specific-hash 5678"
            new-time (t/date-time 2022 4)
            indirect-update-descriptor (-> (generate-descriptor kv-store builder service-description-1 token-data-1 token (assoc service-description-1 "image" image-effective-2))
                                           (assoc-in [:component->previous-descriptor-fns :image] {:retrieve-previous-descriptor (constantly descriptor)
                                                                                                   :retrieve-last-update-time (constantly (tc/to-long new-time))}))
            token-history (descriptor->comprehensive-history kv-store builder max-history token indirect-update-descriptor)]
        (is (= 2 (count token-history)))
        (is (= new-time (-> token-history first :update-time)))
        (is (= time-1 (-> token-history second :update-time)))
        (is (= :image (-> token-history first :source-component)))
        (is (= :token (-> token-history second :source-component)))
        (let [orig-core-serv-desc (assoc service-description-1 "image-effective" image-effective-1)
              new-core-serv-desc (assoc service-description-1 "image-effective" image-effective-2)]
          (is (= new-core-serv-desc (-> token-history first :core-service-description)))
          (is (= orig-core-serv-desc (-> token-history second :core-service-description))))))

    (testing "token with mixed direct/indirect update history"
      (let [kv-store (kv/->LocalKeyValueStore (atom {}))
            builder (generate-default-builder kv-store)
            image-effective-1 "foo-with-specific-hash 1234"
            desc-1 (generate-descriptor kv-store builder service-description-1 token-data-1 token (assoc service-description-1 "image" image-effective-1))
            desc-2 (-> (generate-descriptor kv-store builder service-description-2 token-data-2 token (assoc service-description-2 "image" image-effective-1))
                       (assoc-in [:component->previous-descriptor-fns :token :retrieve-previous-descriptor] (constantly desc-1)) )

            image-effective-2 "foo-with-specific-hash 5678"
            desc-3 (-> (generate-descriptor kv-store builder service-description-2 token-data-2 token (assoc service-description-2 "image" image-effective-2))
                       (assoc-in [:component->previous-descriptor-fns :image] {:retrieve-previous-descriptor (constantly desc-2)
                                                                               :retrieve-last-update-time (constantly (tc/to-long time-3))}))
            time-4 (t/date-time 2022 4)
            service-description-4 service-description-3
            token-data-4 (assoc service-description-4 "last-update-time" (tc/to-long time-4) "previous" token-data-2)

            desc-4 (-> (generate-descriptor kv-store builder service-description-4 token-data-4 token (assoc service-description-4 "image" image-effective-2))
                       (assoc-in [:component->previous-descriptor-fns :image] {:retrieve-previous-descriptor (constantly desc-2)
                                                                               :retrieve-last-update-time (constantly (tc/to-long time-3))})
                       (assoc-in [:component->previous-descriptor-fns :token] {:retrieve-previous-descriptor (constantly desc-3)
                                                                               :retrieve-last-update-time (constantly (tc/to-long time-4))}))

            token-history (descriptor->comprehensive-history kv-store builder max-history token desc-4)]
        (is (= 4 (count token-history)))
        (is (= [time-4 time-3 time-2 time-1] (mapv :update-time token-history)))
        (is (= [:token :image :token :token] (mapv :source-component token-history)))
        (let [core-serv-desc-1 (assoc service-description-1 "image-effective" image-effective-1)
              core-serv-desc-2 (assoc service-description-2 "image-effective" image-effective-1)
              core-serv-desc-3 (assoc service-description-2 "image-effective" image-effective-2)
              core-serv-desc-4 (assoc service-description-4 "image-effective" image-effective-2)]
          (is (= [core-serv-desc-4 core-serv-desc-3 core-serv-desc-2 core-serv-desc-1] (mapv :core-service-description token-history))))))

     (testing "token where history gets cut off due to max-history"
       (let [kv-store (kv/->LocalKeyValueStore (atom {}))
             builder (generate-default-builder kv-store)
             descriptor (generate-descriptor kv-store builder service-description-3 token-data-3 token)
             max-history 2
             token-history (descriptor->comprehensive-history kv-store builder max-history token descriptor)]
         (is (= 2 (count token-history)))

         (is (= time-3 (-> token-history first :update-time)))
         (is (= time-2 (-> token-history second :update-time)))
         (is (= :token (-> token-history first :source-component)))
         (is (= :token (-> token-history second :source-component)))
         (is (= service-description-3 (-> token-history first :core-service-description)))
         (is (= service-description-2 (-> token-history second :core-service-description)))))))