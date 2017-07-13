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
(ns token-syncer.main-test
  (:require [clojure.test :refer :all]
            [qbits.jet.server :as server]
            [token-syncer.main :refer :all]
            [token-syncer.settings :as settings]))

(deftest test-main-server-options
  (testing "server-options"
    (let [target-config "some-config.edn"
          target-port 12345]
      (with-redefs [server/run-jetty (fn run-jetty ([options]
                                                    (is (contains? options :ring-handler))
                                                    (is (= {:port 12345, :request-header-size 32768}
                                                           (dissoc options :ring-handler)))))
                    settings/load-settings (fn load-settings [in-config _]
                                             (is (= target-config in-config))
                                             {})]
        (-main target-config "--port" (str target-port))))))