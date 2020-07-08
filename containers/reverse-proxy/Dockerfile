FROM envoyproxy/envoy-alpine:v1.14.4

ARG SERVICE_PORT
ARG PORT0 

COPY create-envoy-config.sh /
RUN sh -x create-envoy-config.sh ${SERVICE_PORT} ${PORT0} > /etc/envoy/envoy.yaml
