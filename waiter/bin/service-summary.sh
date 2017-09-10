#!/usr/bin/env bash

# Usage: service-summary.sh [PORT]
#
# Examples:
#   service-summary.sh 9091
#   service-summary.sh
#   watch -c service-summary.sh
#
# Queries Waiter and prints a summary of running services.  Meant to be called by watch.

export WAITER_PORT=${1:-9091}

curl -s localhost:${WAITER_PORT}/apps | jq -c -C '.[] | {id: .["service-id"], healthy: .["instance-counts"]["healthy-instances"], unhealthy: .["instance-counts"]["unhealthy-instances"]}'
