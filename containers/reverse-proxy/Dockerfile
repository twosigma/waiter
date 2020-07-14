FROM envoyproxy/envoy-alpine:v1.14.4

COPY envoy-start /opt/waiter/envoy/bin/
COPY envoy.yaml /etc/envoy/

RUN chmod +x /opt/waiter/envoy/bin/envoy-start
ENTRYPOINT /opt/waiter/envoy/bin/envoy-start
