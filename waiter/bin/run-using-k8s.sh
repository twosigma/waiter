#!/usr/bin/env bash
# Usage: run-using-k8s-scheduler.sh [PORT]
#
# Examples:
#   run-using-k8s-scheduler.sh 9091
#   run-using-k8s-scheduler.sh
#
# Runs Waiter, configured to use the Kubernetes scheduler.

export WAITER_PORT=${1:-9091}
export GRAPHITE_SERVER_PORT=5555
export JWKS_PORT=8040
export JWKS_SERVER_URL="http://127.0.0.1:${JWKS_PORT}/keys"
export WAITER_TEST_JWT_ACCESS_TOKEN_URL="http://127.0.0.1:${JWKS_PORT}/get-token?host={HOST}"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting waiter..."
cd ${DIR}/..
WAITER_AUTH_RUN_AS_USER=$(id -un) lein do clean, compile, run config-k8s.edn
