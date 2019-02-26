#!/usr/bin/env bash
# Usage: run-using-k8s-scheduler.sh [PORT]
#
# Examples:
#   run-using-k8s-scheduler.sh 9091
#   run-using-k8s-scheduler.sh
#
# Runs Waiter, configured to use the Kubernetes scheduler.

export WAITER_PORT=${1:-9091}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting waiter..."
cd ${DIR}/..
WAITER_AUTH_RUN_AS_USER=$(id -un) lein do clean, compile, run config-k8s.edn
