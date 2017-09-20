#!/usr/bin/env bash

# Usage: kill-all-services.sh [PORT]
#
# Examples:
#   kill-all-services.sh 9091
#   kill-all-services.sh
#
# Kills all services running on Waiter

export WAITER_PORT=${1:-9091}

curl -s localhost:${WAITER_PORT}/apps | jq -r '.[]["service-id"]' |
while read service; do
    echo "Killing $service"
    curl -s -X DELETE localhost:${WAITER_PORT}/apps/$service
    echo
done 
