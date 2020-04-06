#!/usr/bin/env bash
# Usage: run-using-marathon.sh [PORT]
#
# Examples:
#   run-using-marathon.sh 9091
#   run-using-marathon.sh
#
# Runs Waiter, configured to use a local marathon

export WAITER_PORT=${1:-9091}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export WAITER_MARATHON=http://localhost:8080
export WAITER_ZOOKEEPER_CONNECT_STRING=localhost:2181
export GRAPHITE_SERVER_PORT=5555

echo "Starting waiter..."
cd ${DIR}/..
WAITER_AUTH_RUN_AS_USER=$(id -un) WAITER_LOG_FILE_PREFIX=${WAITER_PORT}- lein do clean, compile, run config-minimesos.edn
