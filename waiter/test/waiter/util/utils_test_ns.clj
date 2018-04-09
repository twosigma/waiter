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
(ns waiter.util.utils-test-ns)

(defn foo
  "This function (and namespace) exist solely to allow for testing the
  utils/create-component function on a symbol whose namespace has not
  yet been loaded (because nothing calls require or use on this)"
  [_]
  :bar)
