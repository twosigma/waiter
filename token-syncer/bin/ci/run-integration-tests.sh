#!/usr/bin/env bash
# Usage: run-integration-tests.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests.sh parallel-test integration
#   run-integration-tests.sh parallel-test
#   run-integration-tests.sh
#
# Runs the Token-Syncer integration tests after launching Waiter instances, and dumps log files if the tests fail.

set -ev

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../../../waiter
SYNCER_DIR=${WAITER_DIR}/../token-syncer

pushd ${WAITER_DIR}

lein do clean, compile

# Start waiter servers
WAITER_URIS=""
for waiter_port in 9093 9092 9091
do
  WAITER_URI="http://127.0.0.1:${waiter_port}"
  WAITER_URIS="${WAITER_URI},${WAITER_URIS}"
  bin/run-using-shell-scheduler.sh ${waiter_port} 0 &
done

popd

# Run the integration tests
export WAITER_URIS="${WAITER_URIS%?}"
${SYNCER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR} || {
  # If there were failures, dump the logs
  echo "integration tests failed -- dumping logs"
  tail -n +1 -- log/*.log
  exit 1
}
