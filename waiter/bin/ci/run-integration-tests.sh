#!/usr/bin/env bash
# Usage: run-integration-tests.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests.sh parallel-test integration-fast
#   run-integration-tests.sh parallel-test integration-slow
#   run-integration-tests.sh parallel-test
#   run-integration-tests.sh
#
# Runs the Waiter integration tests, and dumps log files if the tests fail.

set -ev

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Build mesos agent with kitchen backed in
export PATH=${KITCHEN_DIR}/..:$PATH
${KITCHEN_DIR}/bin/build-docker-image.sh

# Start minimesos
export PATH=${DIR}:${PATH}
which minimesos
pushd ${WAITER_DIR}
minimesos up
popd

# Start waiter
WAITER_PORT=9091
${WAITER_DIR}/bin/run-using-minimesos.sh ${WAITER_PORT} &

# Run the integration tests
WAITER_TEST_KITCHEN_CMD=/opt/kitchen/container-run.sh WAITER_URI=127.0.0.1:${WAITER_PORT} ${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR} || test_failures=true

# If there were failures, dump the logs
if [ "$test_failures" = true ]; then
    echo "Uploading logs..."
    ${WAITER_DIR}/bin/ci/upload_logs.sh
    exit 1
fi
