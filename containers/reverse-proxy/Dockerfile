FROM envoyproxy/envoy-alpine:v1.14.4

COPY envoy-start /opt/waiter/envoy/bin/
COPY envoy-config-template.sh /opt/waiter/envoy/bin/

ENTRYPOINT /bin/sh /opt/waiter/envoy/bin/envoy-start