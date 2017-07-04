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
(ns waiter.zk-test
  (:import (org.apache.curator.test TestingServer)
           (org.apache.curator.retry BoundedExponentialBackoffRetry)
           (org.apache.curator.framework CuratorFrameworkFactory)))

;; https://github.com/Factual/skuld/blob/master/test/skuld/zk_test.clj
(defmacro with-zk
  "Evaluates body with a zookeeper server running, and the connect string bound
  to the given variable. Ensures the ZK server is shut down at the end of the
  body. Example:

  (with-zk [zk-string]
  (connect-to zk-string)
  ...)"
  [[connect-string] & body]
  `(let [zk#             (TestingServer.)
         ~connect-string (.getConnectString zk#)]
     (try
       ~@body
       (finally
         (.close zk#)))))

(defmacro with-curator
  "Evaluates body with a Curator instance running, and the curator client bound
  to the given variable. Ensures the Curator is shut down at the end of the
  body. Example:

  (with-curator [zk-string curator]
  (.start curator)
  ...)"
  [[connect-string curator] & body]
  `(let [retry-policy#   (BoundedExponentialBackoffRetry. 100 120000 10)
         ~curator (CuratorFrameworkFactory/newClient ~connect-string
                                                     180000
                                                     30000
                                                     retry-policy#)]
     (try
       (.start ~curator)
       ~@body
       (finally
         (.close ~curator)))))
