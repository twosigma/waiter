;;
;;       Copyright (c) 2018 Two Sigma Investments, LP.
;;       All Rights Reserved
;;
;;       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
;;       Two Sigma Investments, LP.
;;
;;       The copyright notice above does not evidence any
;;       actual or intended publication of such source code.
;;
(ns waiter.interstitial-test
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [plumbing.core :as pc]
            [waiter.async-utils :as au]
            [waiter.client-tools :as ct]
            [waiter.interstitial :refer :all]))

(deftest test-ensure-service-interstitial!
  (testing "new-entry"
    (let [interstitial-state-atom (atom {:initialized? false
                                         :service-id->interstitial-promise {"service-id-1" (promise)}})
          service-id "test-service-id"
          process-interstitial-counter (atom 0)
          interstitial-secs 10]
      (with-redefs [install-interstitial-timeout! (fn [in-service-id in-interstitial-secs in-interstitial-promise]
                                                    (is (= service-id in-service-id))
                                                    (is (= interstitial-secs in-interstitial-secs))
                                                    (is in-interstitial-promise)
                                                    (swap! process-interstitial-counter inc))]
        (let [interstitial-promise (ensure-service-interstitial! interstitial-state-atom service-id interstitial-secs)]
          (is (= 1 @process-interstitial-counter))
          (is (not (get @interstitial-state-atom :initialized?)))
          (is (not (realized? interstitial-promise)))
          (is (contains? (get @interstitial-state-atom :service-id->interstitial-promise) service-id))
          (is (->> (get-in @interstitial-state-atom [:service-id->interstitial-promise service-id])
                   (identical? interstitial-promise)))))))

  (testing "existing-entry"
    (let [initial-interstitial-promise (promise)
          service-id "test-service-id"
          interstitial-state-atom (atom {:initialized? false
                                         :service-id->interstitial-promise {"service-id-1" (promise)
                                                                            service-id initial-interstitial-promise}})
          process-interstitial-counter (atom 0)
          interstitial-secs 10]
      (with-redefs [install-interstitial-timeout! (fn [in-service-id in-interstitial-secs in-interstitial-promise]
                                                    (is (= service-id in-service-id))
                                                    (is (= interstitial-secs in-interstitial-secs))
                                                    (is in-interstitial-promise)
                                                    (swap! process-interstitial-counter inc))]
        (let [interstitial-promise (ensure-service-interstitial! interstitial-state-atom service-id interstitial-secs)]
          (is (zero? @process-interstitial-counter))
          (is (not (get @interstitial-state-atom :initialized?)))
          (is (not (realized? interstitial-promise)))
          (is (contains? (get @interstitial-state-atom :service-id->interstitial-promise) service-id))
          (is (identical? initial-interstitial-promise interstitial-promise))
          (is (->> (get-in @interstitial-state-atom [:service-id->interstitial-promise service-id])
                   (identical? interstitial-promise)))))))

  (testing "new-entry with concurrency"
    (let [interstitial-state-atom (atom {:initialized? false
                                         :service-id->interstitial-promise {"service-id-1" (promise)}})
          service-id "test-service-id"
          process-interstitial-counter (atom 0)
          interstitial-secs 10]
      (with-redefs [install-interstitial-timeout! (fn [in-service-id in-interstitial-secs in-interstitial-promise]
                                                    (is (= service-id in-service-id))
                                                    (is (= interstitial-secs in-interstitial-secs))
                                                    (is in-interstitial-promise)
                                                    (swap! process-interstitial-counter inc))]
        (let [interstitial-promises (->> (ensure-service-interstitial! interstitial-state-atom service-id interstitial-secs)
                                         (fn [])
                                         (ct/parallelize-requests 20 10))]
          (let [interstitial-promise (get-in @interstitial-state-atom [:service-id->interstitial-promise service-id])]
            (is (= 1 @process-interstitial-counter))
            (is (not (get @interstitial-state-atom :initialized?)))
            (is (not (realized? interstitial-promise)))
            (is (every? #(= interstitial-promise %) interstitial-promises))))))))

(deftest test-remove-resolved-interstitial-promises!
  (let [resolved-service-ids #{"service-1" "service-3" "service-5" "service-7"}
        unresolved-service-ids #{"service-2" "service-4" "service-6"}
        all-service-ids (set/union resolved-service-ids unresolved-service-ids)
        interstitial-state-atom (->> all-service-ids
                                     (pc/map-from-keys (fn [service-id]
                                                         (let [p (promise)]
                                                           (when (contains? resolved-service-ids service-id)
                                                             (deliver p :resolved-from-test))
                                                           p)))
                                     (assoc {:initialized? true} :service-id->interstitial-promise)
                                     atom)]
    (is (every? #(contains? (get-in @interstitial-state-atom [:service-id->interstitial-promise]) %) all-service-ids))
    (is (every? #(realized? (get-in @interstitial-state-atom [:service-id->interstitial-promise %])) resolved-service-ids))
    (is (not-any? #(realized? (get-in @interstitial-state-atom [:service-id->interstitial-promise %])) unresolved-service-ids))
    (remove-resolved-interstitial-promises! interstitial-state-atom all-service-ids)
    (is (not-any? #(contains? (get-in @interstitial-state-atom [:service-id->interstitial-promise]) %) resolved-service-ids))
    (is (every? #(contains? (get-in @interstitial-state-atom [:service-id->interstitial-promise]) %) unresolved-service-ids))))

(deftest test-install-interstitial-timeout!
  (testing "zero interstitial-secs"
    (let [service-id "test-service-id"
          interstitial-secs 0
          interstitial-promise (promise)]
      (is (nil? (install-interstitial-timeout! service-id interstitial-secs interstitial-promise)))))

  (testing "non-zero interstitial-secs"
    (let [service-id "test-service-id"
          interstitial-secs 100
          interstitial-promise (promise)
          timeout-chan (async/chan 1)]
      (with-redefs [async/timeout (fn [^long timeout-ms]
                                    (is (= (* interstitial-secs 1000) timeout-ms))
                                    timeout-chan)]
        (let [r (install-interstitial-timeout! service-id interstitial-secs interstitial-promise)]
          (is (au/chan? r))
          (is (= :not-initialized (deref interstitial-promise 0 :not-initialized)))
          (async/>!! timeout-chan :timeout)
          (async/<!! r)
          (is (= :interstitial-timeout (deref interstitial-promise 0 :not-initialized))))))))

(deftest test-interstitial-maintainer
  (let [resolved-service-ids #{"service-1" "service-3" "service-5" "service-7"}
        unresolved-service-ids #{"service-2" "service-4" "service-6"}
        all-service-ids (set/union resolved-service-ids unresolved-service-ids)
        interstitial-state-atom (->> all-service-ids
                                     (pc/map-from-keys (fn [service-id]
                                                         (let [p (promise)]
                                                           (when (contains? resolved-service-ids service-id)
                                                             (deliver p :resolved-from-test))
                                                           p)))
                                     (assoc {:initialized? false} :service-id->interstitial-promise)
                                     atom)
        available-service-ids' ["service-0" "service-7" "service-8" "service-9"]
        scheduler-messages [[:update-available-apps {:available-apps available-service-ids'}]
                            [:update-app-instances {:healthy-instances [{:id "service-0.1"}]
                                                    :service-id "service-0"}]
                            [:update-app-instances {:healthy-instances [{:id "service-6.1"}]
                                                    :service-id "service-6"}]
                            [:update-app-instances {:healthy-instances [{:id "service-8.1"}]
                                                    :service-id "service-8"}]
                            [:update-app-instances {:service-id "service-9"
                                                    :unhealthy-instances [{:id "service-9.1"}]}]]
        service-id->service-description (fn [service-id]
                                          {"interstitial-secs" (->> (str/last-index-of service-id "-")
                                                                    inc
                                                                    (subs service-id)
                                                                    Integer/parseInt)})
        scheduler-state-chan (au/latest-chan)
        initial-state {:available-service-ids all-service-ids}
        {:keys [exit-chan query-chan]}
        (interstitial-maintainer service-id->service-description scheduler-state-chan interstitial-state-atom initial-state)]

    (async/>!! scheduler-state-chan scheduler-messages)
    (let [response-chan (async/promise-chan)
          _ (async/>!! query-chan {:response-chan response-chan})
          state (async/<!! response-chan)]
      (is (= (set/union (set available-service-ids') unresolved-service-ids)
             (set (get-in state [:maintainer :available-service-ids]))))
      (is (get-in state [:interstitial :initialized?]))
      (is (= {"service-2" :not-realized
              "service-4" :not-realized
              "service-6" :healthy-instance-found
              "service-7" :resolved-from-test
              "service-8" :healthy-instance-found
              "service-9" :not-realized}
             (get-in state [:interstitial :service-id->interstitial-promise]))))
    (async/>!! exit-chan :exit)))

(deftest test-wrap-interstitial
  (let [handler (fn [request] (-> (select-keys request [:query-string :request-id])
                                  (assoc :status 201)))
        service-id (str "test-service-id-" (rand-int 100000))]

    (testing "zero interstitial secs"
      (let [interstitial-state-atom (atom {:initialized? false})
            request {:descriptor {:service-description {"interstitial-secs" 0}
                                  :service-id service-id}
                     :request-id :interstitial-disabled}
            response ((wrap-interstitial handler interstitial-state-atom) request)]
        (is (= {:query-string nil :request-id :interstitial-disabled :status 201} response))))

    (testing "non-html accept"
      (let [interstitial-state-atom (atom {:initialized? false})
            request {:descriptor {:service-description {"interstitial-secs" 10}
                                  :service-id service-id}
                     :headers {"accept" "text/css"}
                     :request-id :non-html-accept}
            response ((wrap-interstitial handler interstitial-state-atom) request)]
        (is (= {:query-string nil :request-id :non-html-accept :status 201} response))))

    (testing "interstitial state not initialized"
      (let [interstitial-state-atom (atom {:initialized? false})
            request {:descriptor {:service-description {"interstitial-secs" 10}
                                  :service-id service-id}
                     :headers {"accept" "text/html"}
                     :request-id :interstitial-not-initialized}
            response ((wrap-interstitial handler interstitial-state-atom) request)]
        (is (= {:query-string nil :request-id :interstitial-not-initialized :status 201} response))))

    (testing "interstitial promise resolved"
      (let [interstitial-promise (promise)
            _ (deliver interstitial-promise :resolved)
            interstitial-state-atom (atom {:initialized? true
                                           :service-id->interstitial-promise {service-id interstitial-promise}})
            request {:descriptor {:service-description {"interstitial-secs" 10}
                                  :service-id service-id}
                     :headers {"accept" "text/html", "host" "www.example.com"}
                     :request-id :interstitial-promise-resolved}
            response ((wrap-interstitial handler interstitial-state-atom) request)]
        (is (= {:query-string nil :request-id :interstitial-promise-resolved :status 201} response))))

    (testing "on-the-fly request"
      (let [interstitial-promise (promise)
            interstitial-state-atom (atom {:initialized? true
                                           :service-id->interstitial-promise {service-id interstitial-promise}})
            request {:descriptor {:service-description {"interstitial-secs" 10}
                                  :service-id service-id}
                     :headers {"accept" "text/html", "host" "www.example.com", "x-waiter-cpus" "1"}
                     :request-id :interstitial-promise-resolved}
            response ((wrap-interstitial handler interstitial-state-atom) request)]
        (is (= {:query-string nil :request-id :interstitial-promise-resolved :status 201} response))))

    (testing "interstitial promise absent"
      (let [interstitial-state-atom (atom {:initialized? true
                                           :service-id->interstitial-promise {}})
            service-description {"interstitial-secs" 10}
            request {:descriptor {:service-description service-description
                                  :service-id service-id}
                     :headers {"accept" "text/html", "host" "www.example.com"}
                     :request-id :interstitial-bypass
                     :scheme :https
                     :uri "/test"}
            response ((wrap-interstitial handler interstitial-state-atom) request)]
        (is (= {:headers {"location" (str "/waiter-interstitial/test")
                          "x-waiter-interstitial" "true"}
                :status 303}
               response))
        (is (some-> @interstitial-state-atom
                    :service-id->interstitial-promise
                    (get service-id)
                    realized?
                    not))))

    (testing "interstitial promise unresolved"
      (let [interstitial-promise (promise)
            interstitial-state-atom (atom {:initialized? true
                                           :service-id->interstitial-promise {service-id interstitial-promise}})]
        (testing "bypass interstitial"
          (testing "no-custom-params"
            (let [request {:descriptor {:service-description {"interstitial-secs" 10}
                                        :service-id service-id}
                           :headers {"accept" "text/html", "host" "www.example.com"}
                           :query-string "x-waiter-bypass-interstitial=1"
                           :request-id :interstitial-bypass}
                  response ((wrap-interstitial handler interstitial-state-atom) request)]
              (is (= {:query-string "" :request-id :interstitial-bypass :status 201} response))))

          (testing "some-custom-params"
            (let [request {:descriptor {:service-description {"interstitial-secs" 10}
                                        :service-id service-id}
                           :headers {"accept" "text/html", "host" "www.example.com"}
                           :query-string "a=b&c=d&x-waiter-bypass-interstitial=1"
                           :request-id :interstitial-bypass}
                  response ((wrap-interstitial handler interstitial-state-atom) request)]
              (is (= {:query-string "a=b&c=d" :request-id :interstitial-bypass :status 201} response)))))

        (testing "trigger interstitial"
          (let [request {:descriptor {:service-description {"interstitial-secs" 10}
                                      :service-id service-id}
                         :headers {"accept" "text/html", "host" "www.example.com"}
                         :query-string "a=b"
                         :request-id :interstitial-bypass
                         :scheme :http}
                response ((wrap-interstitial handler interstitial-state-atom) request)]
            (is (= {:headers {"location" (str "/waiter-interstitial?a=b")
                              "x-waiter-interstitial" "true"}
                    :status 303}
                   response)))

          (let [request {:descriptor {:service-description {"interstitial-secs" 10}
                                      :service-id service-id}
                         :headers {"accept" "text/html", "host" "www.example.com"}
                         :query-string "c=d&x-waiter-bypass-interstitial=1&a=b" ;; incorrectly bypass param not at end
                         :request-id :interstitial-bypass
                         :scheme :http}
                response ((wrap-interstitial handler interstitial-state-atom) request)]
            (is (= {:headers {"location" (str "/waiter-interstitial?c=d&x-waiter-bypass-interstitial=1&a=b")
                              "x-waiter-interstitial" "true"}
                    :status 303}
                   response)))

          (let [request {:descriptor {:service-description {"interstitial-secs" 10}
                                      :service-id service-id}
                         :headers {"accept" "text/html", "host" "www.example.com"}
                         :request-id :interstitial-bypass
                         :scheme :https
                         :uri "/test"}
                response ((wrap-interstitial handler interstitial-state-atom) request)]
            (is (= {:headers {"location" (str "/waiter-interstitial/test")
                              "x-waiter-interstitial" "true"}
                    :status 303}
                   response))))))))

(deftest test-display-interstitial-handler
  (with-redefs [render-interstitial-template identity]
    (let [service-id "test-service-id"
          service-description {"cmd" "lorem ipsum dolor sit amet"
                               "interstitial-secs" 10}
          descriptor {:service-description service-description
                      :service-id service-id}]
      (let [request {:descriptor descriptor
                     :route-params {:path "test"
                                    :service-id service-id}}
            response (display-interstitial-handler request)]
        (is (= {:body {:service-description service-description
                       :service-id service-id
                       :target-url (str "/test?x-waiter-bypass-interstitial=1")}
                :status 200}
               response)))
      (let [request {:descriptor descriptor
                     :query-string "a=b&c=d"
                     :route-params {:path "test"
                                    :service-id service-id}}
            response (display-interstitial-handler request)]
        (is (= {:body {:service-description service-description
                       :service-id service-id
                       :target-url (str "/test?a=b&c=d&x-waiter-bypass-interstitial=1")}
                :status 200}
               response))))))
