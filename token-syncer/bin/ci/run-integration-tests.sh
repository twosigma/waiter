#!/usr/bin/env bash
# Usage: run-integration-tests.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests.sh parallel-test integration
#   run-integration-tests.sh parallel-test
#   run-integration-tests.sh
#
# Runs the Waiter integration tests, and dumps log files if the tests fail.

set -ev

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../../../waiter
SYNCER_DIR=${WAITER_DIR}/../token-syncer

# Start waiter
WAITER_PORT_1=9091
${WAITER_DIR}/bin/run-using-shell-scheduler.sh ${WAITER_PORT_1} &

WAITER_PORT_2=9092
${WAITER_DIR}/bin/run-using-shell-scheduler.sh ${WAITER_PORT_2} &

# Run the integration tests
export WAITER_URIS="http://127.0.0.1:${WAITER_PORT_1};http://127.0.0.1:${WAITER_PORT_2}"
${SYNCER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR} || test_failures=true

# If there were failures, dump the logs
if [ "$test_failures" = true ]; then
    echo "integration tests failed -- dumping logs"
    tail -n +1 -- log/*.log
    exit 1
fi
