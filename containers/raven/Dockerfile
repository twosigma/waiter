FROM envoyproxy/envoy-alpine:v1.18.3

RUN apk add --no-cache openssl

COPY raven-start /opt/waiter/raven/bin/
COPY envoy.template.yaml /etc/envoy/

ENTRYPOINT /opt/waiter/raven/bin/raven-start
