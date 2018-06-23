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

set -ev

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Start waiter
WAITER_PORT=9091
${WAITER_DIR}/bin/run-using-shell-scheduler.sh ${WAITER_PORT} &

# Run the integration tests
WAITER_TEST_KITCHEN_CMD=${KITCHEN_DIR}/bin/kitchen WAITER_URI=127.0.0.1:${WAITER_PORT} ${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR} || test_failures=true

# If there were failures, dump the logs
if [ "$test_failures" = true ]; then
    echo "Uploading logs..."
    ${WAITER_DIR}/bin/ci/upload_logs.sh
    exit 1
fi
