#!/usr/bin/env bash
# Usage: run-using-shell-scheduler.sh [PORT] [RECOMPILE]
#
# Examples:
#   run-using-shell-scheduler.sh 9091 waiter 1
#   run-using-shell-scheduler.sh 9091 waiter
#   run-using-shell-scheduler.sh 9091
#   run-using-shell-scheduler.sh
#
# Runs Waiter, configured to use the local "shell scheduler" (not for production use).

export WAITER_PORT=${1:-9091}
export WAITER_CLUSTER_NAME=${2:-waiter}
export RECOMPILE=${3:-1}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting waiter..."
cd ${DIR}/..


if [ "${RECOMPILE}" != "0" ]; then
  lein do clean, compile
fi

WAITER_AUTH_RUN_AS_USER=$(id -un) lein run config-shell.edn

EXIT_CODE=$?

echo "Waiter quit with code $EXIT_CODE"
exit $EXIT_CODE
