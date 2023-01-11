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

(def ^:const posix-sigkill 9)
(def ^:const posix-sigterm 15)

(def ^:const http-101-switching-protocols HttpStatus/SWITCHING_PROTOCOLS_101)
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
(def ^:const http-408-request-timeout HttpStatus/REQUEST_TIMEOUT_408)
(def ^:const http-409-conflict HttpStatus/CONFLICT_409)
(def ^:const http-410-gone HttpStatus/GONE_410)
(def ^:const http-415-unsupported-media-type HttpStatus/UNSUPPORTED_MEDIA_TYPE_415)
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

;; Envoy "%RESPONSE_FLAGS%" values returned by Raven for error diagnostics
;; https://www.envoyproxy.io/docs/envoy/latest/configuration/observability/access_log/usage#config-access-log-format-response-flags
(def ^:const envoy-empty-response-flags "-")
(def ^:const envoy-stream-idle-timeout "SI")
(def ^:const envoy-upstream-connection-failure "UF")
(def ^:const envoy-upstream-connection-termination "UC")
(def ^:const envoy-upstream-request-timeout "UT")

;; waiter error class constants
(def ^:const error-class-cors-preflight-forbidden "waiter.CorsPreflightForbidden")
(def ^:const error-class-cors-request-forbidden "waiter.CorsRequestForbidden")
(def ^:const error-class-deployment-error "waiter.DeploymentError")
(def ^:const error-class-kerberos-negotiate "waiter.KerberosNegotiate")
(def ^:const error-class-kerberos-queue-length "waiter.KerberosQueueLength")
(def ^:const error-class-maintenance "waiter.Maintenance")
(def ^:const error-class-prestashed-tickets "waiter.PrestashedTickets")
(def ^:const error-class-queue-length "waiter.QueueLength")
(def ^:const error-class-request-timeout "waiter.RequestTimeout")
(def ^:const error-class-reserve-back-pressure "waiter.ReserveBackPressure")
(def ^:const error-class-stream-failure "waiter.StreamFailure")
(def ^:const error-class-stream-timeout "waiter.StreamTimeout")
(def ^:const error-class-service-forbidden "waiter.ServiceForbidden")
(def ^:const error-class-service-misconfigured "waiter.ServiceMisconfigured")
(def ^:const error-class-service-unidentified "waiter.ServiceUnknown")
(def ^:const error-class-suspended "waiter.Suspended")
(def ^:const error-class-unsupported-auth "waiter.UnsupportedAuth")

;; waiter error images
(def ^:const error-image-400-bad-request "http-400-bad-request.png")
(def ^:const error-image-401-unauthorized "http-401-unauthorized.png")
(def ^:const error-image-403-forbidden "http-403-forbidden.png")
(def ^:const error-image-408-request-timeout "http-408-request-timeout.png")
(def ^:const error-image-429-too-many-requests "http-429-too-many-requests.png")
(def ^:const error-image-500-internal-server-error "http-500-internal-server-error.png")
(def ^:const error-image-502-connection-failed "http-502-connection-failed.png")
(def ^:const error-image-503-deployment-error "http-503-deployment-error.png")
(def ^:const error-image-503-instance-unavailable "http-503-instance-unavailable.png")
(def ^:const error-image-503-service-suspended "http-503-maintenance.png")
(def ^:const error-image-503-maintenance "http-503-maintenance.png")
(def ^:const error-image-503-service-overloaded "http-503-service-overloaded.png")
(def ^:const error-image-504-gateway-timeout "http-504-gateway-timeout.png")


