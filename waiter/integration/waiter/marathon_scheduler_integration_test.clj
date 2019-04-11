(ns waiter.marathon-scheduler-integration-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [waiter.util.http-utils :as http]
            [waiter.util.client-tools :refer :all]))

(defn- verify-marathon-image-alias
  [waiter-url custom-image-alias constraint-attribute constraint-value]
  (dotimes [_ 10]
    (let [{:keys [service-id] {:strs [x-waiter-backend-host]} :headers}
          (make-request-with-debug-info
            {:x-waiter-name (rand-name)
             :x-waiter-image custom-image-alias}
            #(make-kitchen-request waiter-url % :method :get :path "/"))
          attributes (:attributes (http/http-request
                                    http-client
                                    (str "http://" x-waiter-backend-host ":5051/state.json")))]
      (is (= constraint-value ((keyword constraint-attribute) attributes)))
      (delete-service waiter-url service-id))))

(deftest ^:parallel ^:integration-slow ^:explicit test-marathon-image-alias
  (testing-using-waiter-url
    (when (using-marathon? waiter-url)
      (let [custom-image-alias (System/getenv "INTEGRATION_TEST_IMAGE_CONSTRAINT_ALIAS")
            _ (is (not (string/blank? custom-image-alias)) "You must provide an image alias in the INTEGRATION_TEST_IMAGE_CONSTRAINT_ALIAS environment variable")
            constraint-attribute (System/getenv "INTEGRATION_TEST_IMAGE_CONSTRAINT_ATTRIBUTE")
            _ (is (not (string/blank? constraint-attribute)) "You must provide a constraint attribute in the INTEGRATION_TEST_IMAGE_CONSTRAINT_ATTRIBUTE environment variable")
            constraint-value (System/getenv "INTEGRATION_TEST_IMAGE_CONSTRAINT_VALUE")
            _ (is (not (string/blank? constraint-value)) "You must provide a constraint value in the INTEGRATION_TEST_IMAGE_CONSTRAINT_VALUE environment variable")]
        (verify-marathon-image-alias waiter-url custom-image-alias constraint-attribute constraint-value)))))


(deftest ^:parallel ^:integration-slow ^:explicit test-marathon-image-alias-2
  (testing-using-waiter-url
    (when (using-marathon? waiter-url)
      (let [custom-image-alias (System/getenv "INTEGRATION_TEST_IMAGE_CONSTRAINT_ALIAS_2")
            constraint-attribute (System/getenv "INTEGRATION_TEST_IMAGE_CONSTRAINT_ATTRIBUTE_2")
            constraint-value (System/getenv "INTEGRATION_TEST_IMAGE_CONSTRAINT_VALUE_2")]
        (when (and custom-image-alias constraint-attribute constraint-value)
          (verify-marathon-image-alias waiter-url custom-image-alias constraint-attribute constraint-value))))))

