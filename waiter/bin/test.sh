#!/usr/bin/env bash
# Usage: test.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   test.sh parallel-test integration-fast
#   test.sh parallel-test integration-slow
#   test.sh parallel-test
#   test.sh
#
# Waits for waiter to be listening and then runs the given test selector. Checks if WAITER_URI and
# WAITER_TEST_KITCHEN_CMD are set, and sets them to reasonable defaults if not.

set -v

function wait_for_waiter {
    URI=${1}
    while ! curl -s ${URI} >/dev/null;
    do
        echo "$(date +%H:%M:%S) waiter is not listening on ${URI} yet"
        sleep 2.0
    done
    echo "$(date +%H:%M:%S) connected to waiter on ${URI}!"
}
export -f wait_for_waiter

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

if [[ -z ${WAITER_URI+x} ]]; then
    export WAITER_URI=127.0.0.1:9091
    echo "WAITER_URI is unset, defaulting to ${WAITER_URI}"
else
    echo "WAITER_URI is set to ${WAITER_URI}"
fi

if [[ -z ${WAITER_TEST_KITCHEN_CMD+x} ]]; then
    export WAITER_TEST_KITCHEN_CMD=/opt/kitchen/kitchen
    echo "WAITER_TEST_KITCHEN_CMD is unset, defaulting to ${WAITER_TEST_KITCHEN_CMD}"
else
    echo "WAITER_TEST_KITCHEN_CMD is set to ${WAITER_TEST_KITCHEN_CMD}"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/..

# Wait for waiter to be listening
timeout 180s bash -c "wait_for_waiter ${WAITER_URI}"
if [[ $? -ne 0 ]]; then
  echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening, displaying waiter log"
  cat ${WAITER_DIR}/log/*waiter.log
  exit 1
fi
curl -s ${WAITER_URI}/state | jq .routers
curl -s ${WAITER_URI}/settings | jq .port

THREAD_DUMP_DIR=${WAITER_DIR}/log/thread-dump
mkdir -p ${THREAD_DUMP_DIR}
WAITER_PID=$(lsof -Pi :9091 -sTCP:LISTEN -t)
echo "$(date +%H:%M:%S) waiter pid is ${WAITER_PID}"
while true; do
  sleep 15
  file_name="$(date +%Y%m%d-%H%M%S).log"
  echo "$(date +%H:%M:%S) writing thread dump to ${THREAD_DUMP_DIR}/${file_name}"
  jstack ${WAITER_PID} > "${THREAD_DUMP_DIR}/${file_name}"
done &

# Run the integration tests
cd ${WAITER_DIR}
lein with-profiles +test-log ${TEST_COMMAND} :${TEST_SELECTOR} ${@:3}
