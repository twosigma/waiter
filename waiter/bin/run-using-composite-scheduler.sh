#!/usr/bin/env bash
# Usage: run-using-composite-scheduler.sh [PORT] [RECOMPILE]
#
# Examples:
#   run-using-composite-scheduler.sh 9091 1
#   run-using-composite-scheduler.sh 9091
#   run-using-composite-scheduler.sh
#
# Runs Waiter, configured to use the local "composite scheduler" (not for production use).

export WAITER_PORT=${1:-9091}
export RECOMPILE=${2:-1}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting waiter..."
cd ${DIR}/..


if [ "${RECOMPILE}" != "0" ]; then
  lein do clean, compile
fi

WAITER_AUTH_RUN_AS_USER=$(id -un) lein run config-composite.edn

EXIT_CODE=$?

echo "Waiter quit with code $EXIT_CODE"
exit $EXIT_CODE
