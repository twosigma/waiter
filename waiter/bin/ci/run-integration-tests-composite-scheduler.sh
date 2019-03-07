#!/usr/bin/env bash
# Usage: run-integration-tests-composite-scheduler.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests-composite-scheduler.sh parallel-test integration-fast
#   run-integration-tests-composite-scheduler.sh parallel-test integration-slow
#   run-integration-tests-composite-scheduler.sh parallel-test
#   run-integration-tests-composite-scheduler.sh
#
# Runs the Waiter integration tests using the (local) composite scheduler.

set -e

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen
NGINX_SERVER_DIR=${WAITER_DIR}/../nginx-server

# Start waiter
: ${WAITER_PORT:=9091}
${WAITER_DIR}/bin/run-using-composite-scheduler.sh ${WAITER_PORT} &

# Run the integration tests
export WAITER_TEST_KITCHEN_CMD=${KITCHEN_DIR}/bin/kitchen
export WAITER_TEST_NGINX_SERVER_CMD=${NGINX_SERVER_DIR}/bin/run-nginx-server.sh
export WAITER_URI=127.0.0.1:${WAITER_PORT}
${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}
