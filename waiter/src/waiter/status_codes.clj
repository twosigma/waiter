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
(ns waiter.status-codes
  (:import (org.eclipse.jetty.http HttpStatus)
           (org.eclipse.jetty.websocket.api StatusCode)))

(def ^:const http-200-ok HttpStatus/OK_200)
(def ^:const http-201-created HttpStatus/CREATED_201)
(def ^:const http-202-accepted HttpStatus/ACCEPTED_202)
(def ^:const http-204-no-content HttpStatus/NO_CONTENT_204)
(def ^:const http-300-multiple-choices HttpStatus/MULTIPLE_CHOICES_300)
(def ^:const http-301-moved-permanently HttpStatus/MOVED_PERMANENTLY_301)
(def ^:const http-302-moved-temporarily HttpStatus/MOVED_TEMPORARILY_302)
(def ^:const http-303-see-other HttpStatus/SEE_OTHER_303)
(def ^:const http-307-temporary-redirect HttpStatus/TEMPORARY_REDIRECT_307)
(def ^:const http-308-permanent-redirect HttpStatus/PERMANENT_REDIRECT_308)
(def ^:const http-400-bad-request HttpStatus/BAD_REQUEST_400)
(def ^:const http-401-unauthorized HttpStatus/UNAUTHORIZED_401)
(def ^:const http-402-payment-required HttpStatus/PAYMENT_REQUIRED_402)
(def ^:const http-403-forbidden HttpStatus/FORBIDDEN_403)
(def ^:const http-404-not-found HttpStatus/NOT_FOUND_404)
(def ^:const http-405-method-not-allowed HttpStatus/METHOD_NOT_ALLOWED_405)
(def ^:const http-409-conflict HttpStatus/CONFLICT_409)
(def ^:const http-410-gone HttpStatus/GONE_410)
(def ^:const http-423-locked HttpStatus/LOCKED_423)
(def ^:const http-429-too-many-requests HttpStatus/TOO_MANY_REQUESTS_429)
(def ^:const http-500-internal-server-error HttpStatus/INTERNAL_SERVER_ERROR_500)
(def ^:const http-501-not-implemented HttpStatus/NOT_IMPLEMENTED_501)
(def ^:const http-502-bad-gateway HttpStatus/BAD_GATEWAY_502)
(def ^:const http-503-service-unavailable HttpStatus/SERVICE_UNAVAILABLE_503)
(def ^:const http-504-gateway-timeout HttpStatus/GATEWAY_TIMEOUT_504)

(def ^:const websocket-1000-normal StatusCode/NORMAL)
(def ^:const websocket-1001-shutdown StatusCode/SHUTDOWN)
(def ^:const websocket-1002-protocol StatusCode/PROTOCOL)
(def ^:const websocket-1003-bad-data StatusCode/BAD_DATA)
(def ^:const websocket-1004-undefined StatusCode/UNDEFINED)
(def ^:const websocket-1005-no-code StatusCode/NO_CODE)
(def ^:const websocket-1006-abnormal StatusCode/ABNORMAL)
(def ^:const websocket-1007-bad-payload StatusCode/BAD_PAYLOAD)
(def ^:const websocket-1008-policy-violation StatusCode/POLICY_VIOLATION)
(def ^:const websocket-1009-message-too-large StatusCode/MESSAGE_TOO_LARGE)
(def ^:const websocket-1010-required-extension StatusCode/REQUIRED_EXTENSION)
(def ^:const websocket-1011-server-error StatusCode/SERVER_ERROR)
(def ^:const websocket-1012-service-restart StatusCode/SERVICE_RESTART)
(def ^:const websocket-1013-try-again-later StatusCode/TRY_AGAIN_LATER)
(def ^:const websocket-1014-invalid-upstream-response StatusCode/INVALID_UPSTREAM_RESPONSE)
(def ^:const websocket-1015-failed-tls-handshake StatusCode/FAILED_TLS_HANDSHAKE)

;; Constants from io.grpc.Status, we don't want to add a compile-time dependency on it
(def ^:const grpc-0-ok 0)
(def ^:const grpc-1-cancelled 1)
(def ^:const grpc-2-unknown 2)
(def ^:const grpc-3-invalid-argument 3)
(def ^:const grpc-4-deadline-exceeded 4)
(def ^:const grpc-5-not-found 5)
(def ^:const grpc-6-already-exists 6)
(def ^:const grpc-7-permission-denied 7)
(def ^:const grpc-8-resource-exhausted 8)
(def ^:const grpc-9-failed-precondition 9)
(def ^:const grpc-10-aborted 10)
(def ^:const grpc-11-out-of-range 11)
(def ^:const grpc-12-unimplemented 12)
(def ^:const grpc-13-internal 13)
(def ^:const grpc-14-unavailable 14)
(def ^:const grpc-15-data-loss 15)
(def ^:const grpc-16-unauthenticated 16)
