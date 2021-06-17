FROM envoyproxy/envoy-alpine:v1.18.2

RUN apk add --no-cache openssl

COPY envoy-start /opt/waiter/envoy/bin/
COPY envoy.template.yaml /etc/envoy/

ENTRYPOINT /opt/waiter/envoy/bin/envoy-start
