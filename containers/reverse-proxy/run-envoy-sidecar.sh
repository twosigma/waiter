#!/bin/sh

SERVICE_PORT=$1
PORT0=$2

sh -x ./opt/waiter/envoy/bin/envoy-config-template.sh $SERVICE_PORT $PORT0 > /etc/envoy/envoy.yaml
exec envoy -c /etc/envoy/envoy.yaml --base-id 1