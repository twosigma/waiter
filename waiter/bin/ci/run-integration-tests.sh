#!/usr/bin/env bash
# Usage: run-integration-tests.sh [SCHEDULER_NAME] [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests.sh shell parallel-test integration-fast
#   run-integration-tests.sh shell parallel-test integration-slow
#   run-integration-tests.sh shell parallel-test
#   run-integration-tests.sh shell
#   run-integration-tests.sh
#
# Runs the Waiter integration tests, and dumps log files if the tests fail.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
SCHEDULER="${1:-shell}-scheduler"
SUBCMD="${DIR}/run-integration-tests-${SCHEDULER}.sh ${2:-parallel-test} ${3:-integration}"

if [ "${TRAVIS}" == true ]; then
    # Capture integration test command output into a log file
    mkdir -p ${WAITER_DIR}/log
    bash -x -c "${SUBCMD}" &> >(tee ${WAITER_DIR}/log/travis.log)

    # If there were failures, dump the logs
    if [ $? -ne 0 ]; then
        echo 'Uploading logs...'
        ${DIR}/upload-logs.sh
        exit 1
    fi
else
    eval ${SUBCMD}
fi
