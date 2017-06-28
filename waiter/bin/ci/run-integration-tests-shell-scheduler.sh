#!/usr/bin/env bash
# Usage: run-integration-tests-shell-scheduler.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests-shell-scheduler.sh parallel-test integration-fast
#   run-integration-tests-shell-scheduler.sh parallel-test integration-slow
#   run-integration-tests-shell-scheduler.sh parallel-test
#   run-integration-tests-shell-scheduler.sh
#
# Runs the Waiter integration tests using the (local) shell scheduler, and dumps log files if the tests fail.

set -v

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Build kitchen if needed
JAR=${KITCHEN_DIR}/target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar
if [ ! -f ${JAR} ]; then
    echo "The kitchen jar file was not found! Attempting to build it now."
    pushd ${KITCHEN_DIR}
    lein uberjar
    popd
fi

# Start waiter
WAITER_PORT=9091
${WAITER_DIR}/bin/run-using-shell-scheduler.sh ${WAITER_PORT} &

# Run the integration tests
WAITER_TEST_KITCHEN_CMD=${KITCHEN_DIR}/bin/run.sh WAITER_URI=127.0.0.1:${WAITER_PORT} ${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}
TESTS_EXIT_CODE=$?

# If there were failures, dump the logs
if [ ${TESTS_EXIT_CODE} -ne 0 ]; then
    echo "integration tests failed -- dumping logs"
    tail -n +1 -- log/*.log
fi

exit ${TESTS_EXIT_CODE}
