#!/usr/bin/env bash
# Usage: run-integration-tests.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests.sh parallel-test integration
#   run-integration-tests.sh parallel-test
#   run-integration-tests.sh
#
# Runs the Token-Syncer integration tests, and dumps log files if the tests fail.

set -ev

function timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }

function wait_for_server {
    URI=${1}
    while ! curl -s ${URI} >/dev/null;
    do
        echo "$(date +%H:%M:%S) server is not listening on ${URI} yet"
        sleep 2.0
    done
    echo "$(date +%H:%M:%S) connected to server on ${URI}!"
}
export -f wait_for_server

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../../../waiter
SYNCER_DIR=${WAITER_DIR}/../token-syncer

pushd ${WAITER_DIR}

lein voom build-deps
lein do clean, compile

# Start waiter servers
for waiter_port in 9093 9092 9091
do
  bin/run-using-shell-scheduler.sh ${waiter_port} 0 &
done

# Wait for the waiter servers to start
WAITER_URIS=""
for waiter_port in 9093 9092 9091
do
  WAITER_URI="http://127.0.0.1:${waiter_port}"
  timeout 180s bash -c "wait_for_server ${WAITER_URI}"
  WAITER_URIS="${WAITER_URI},${WAITER_URIS}"
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
