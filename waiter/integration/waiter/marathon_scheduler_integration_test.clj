(ns waiter.marathon-scheduler-integration-test
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [waiter.util.client-tools :refer :all]))

(defn- validate-kubernetes-custom-image
  [waiter-url custom-image]
  (let [{:keys [body service-id]} (make-request-with-debug-info
                                    {:x-waiter-name (rand-name)
                                     :x-waiter-image custom-image
                                     :x-waiter-cmd "echo -n $INTEGRATION_TEST_SENTINEL_VALUE > index.html && python3 -m http.server $PORT0"
                                     :x-waiter-health-check-url "/"}
                                    #(make-kitchen-request waiter-url % :method :get :path "/"))]
    (is (= "Integration Test Sentinel Value" body))
    (delete-service waiter-url service-id)))

; test that we can provide a custom docker image that contains /tmp/index.html with "Integration Test Image" in it
(deftest ^:parallel ^:integration-slow test-kubernetes-custom-image-xx
  (testing-using-waiter-url
    (when (using-marathon? waiter-url)
      (let [custom-image (System/getenv "INTEGRATION_TEST_CUSTOM_IMAGE")
            _ (is (not (string/blank? custom-image)) "You must provide a custom image in the INTEGRATION_TEST_CUSTOM_IMAGE environment variable")]
        (validate-kubernetes-custom-image waiter-url custom-image)))))

(deftest ^:parallel ^:integration-slow test-kubernetes-image-alias-x
  (testing-using-waiter-url
    (when (using-marathon? waiter-url)
      (let [custom-image (System/getenv "INTEGRATION_TEST_CUSTOM_IMAGE_ALIAS")
            _ (is (not (string/blank? custom-image)) "You must provide a custom image in the INTEGRATION_TEST_CUSTOM_IMAGE_ALIAS environment variable")]
        (validate-kubernetes-custom-image waiter-url custom-image)))))



(deftest ^:parallel ^:integration-slow xxx
  (testing-using-waiter-url
    (when (using-marathon? waiter-url)
      (let [custom-image (System/getenv "INTEGRATION_TEST_CUSTOM_IMAGE_ALIAS")
            _ (is (not (string/blank? custom-image)) "You must provide a custom image in the INTEGRATION_TEST_CUSTOM_IMAGE_ALIAS environment variable")]

        (let [{:keys [body service-id]} (make-request-with-debug-info
                                          {:x-waiter-name (rand-name)
                                           :x-waiter-image custom-image
                                           :x-waiter-health-check-url "/"}
                                          #(make-kitchen-request waiter-url % :method :get :path "/shell"
                                                                 :query-params {"cmd" "pwd"}))]
          (print body)

          (delete-service waiter-url service-id))

        ))))


