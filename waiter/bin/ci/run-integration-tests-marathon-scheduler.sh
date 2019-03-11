#!/usr/bin/env bash
# Usage: run-integration-tests.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests.sh parallel-test integration-fast
#   run-integration-tests.sh parallel-test integration-slow
#   run-integration-tests.sh parallel-test
#   run-integration-tests.sh
#
# Runs the Waiter integration tests.

set -e

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Build mesos agent container with Kitchen packed in
${KITCHEN_DIR}/bin/build-docker-image.sh

# Start minimesos
export MINIMESOS_CMD=${DIR}/minimesos
pushd ${WAITER_DIR}
${MINIMESOS_CMD} up
popd

# Start waiter
: ${WAITER_PORT:=9091}
${WAITER_DIR}/bin/run-using-minimesos.sh ${WAITER_PORT} &

# Run the integration tests
export WAITER_TEST_KITCHEN_CMD=/opt/kitchen/kitchen
export WAITER_TEST_NGINX_SERVER_CMD=/opt/nginx-server/bin/run-nginx-server.sh
export WAITER_URI=127.0.0.1:${WAITER_PORT}
${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}
