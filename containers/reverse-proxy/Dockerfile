FROM envoyproxy/envoy-alpine:v1.14.4

COPY run-envoy-sidecar.sh /opt/waiter/envoy/bin
COPY envoy-config-template.sh /opt/waiter/envoy/bin

ENTRYPOINT /bin/bash -x ./opt/waiter/envoy/bin/run-envoy-sidecar.sh ${SERVICE_PORT} ${PORT0}