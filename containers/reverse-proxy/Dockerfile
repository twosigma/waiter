FROM envoyproxy/envoy-alpine:v1.14.4

COPY envoy-start /opt/waiter/envoy/bin/
COPY envoy1.yaml /etc/envoy/
COPY envoy2.yaml /etc/envoy/


ENTRYPOINT /opt/waiter/envoy/bin/envoy-start
