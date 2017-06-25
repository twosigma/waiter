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

set -v

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Build mesos agent with kitchen backed in
${KITCHEN_DIR}/bin/build-docker-image.sh

# Start minimesos
export PATH=${DIR}:${PATH}
which minimesos
pushd ${WAITER_DIR}
minimesos up
MINIMESOS_EXIT_CODE=$?
popd
if [ ${MINIMESOS_EXIT_CODE} -ne 0 ]; then
    echo "minimesos failed to startup -- exiting"
    exit ${MINIMESOS_EXIT_CODE}
fi

# Start waiter
WAITER_PORT=9091
${WAITER_DIR}/bin/run-using-minimesos.sh ${WAITER_PORT} &

# Run the integration tests
WAITER_TEST_KITCHEN_CMD=/opt/kitchen/container-run.sh WAITER_URI=127.0.0.1:${WAITER_PORT} ${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}
TESTS_EXIT_CODE=$?

# If there were failures, dump the logs
if [ ${TESTS_EXIT_CODE} -ne 0 ]; then
    echo "integration tests failed -- dumping logs"
    tail -n +1 -- log/*.log
fi

exit ${TESTS_EXIT_CODE}
