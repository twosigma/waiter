#!/usr/bin/env bash
# Usage: run-integration-tests.sh [TEST_SELECTOR] [TEST_COMMAND]
#
# Examples:
#   run-integration-tests.sh integration parallel-test
#   run-integration-tests.sh integration-fast
#   run-integration-tests.sh integration-slow
#   run-integration-tests.sh
#
# Runs the Waiter integration tests, and dumps log files if the tests fail.

set -v

function wait_for_waiter {
    WAITER_PORT=${1:-9091}
    while ! curl -s localhost:${WAITER_PORT} >/dev/null;
    do
        echo "$(date +%H:%M:%S) waiter is not listening on ${WAITER_PORT} yet"
        sleep 2.0
    done
    echo "$(date +%H:%M:%S) connected to waiter on ${WAITER_PORT}!"
}
export -f wait_for_waiter

TEST_SELECTOR=${1:-integration}
TEST_COMMAND=${2:-parallel-test}

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
$(minimesos info | grep MINIMESOS)

# Start two waiters
${WAITER_DIR}/bin/run-using-minimesos.sh 9091 &
${WAITER_DIR}/bin/run-using-minimesos.sh 9092 &

# Wait for waiters to be listening
timeout 180s bash -c "wait_for_waiter 9091"
if [ $? -ne 0 ]; then
  echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening, displaying waiter log"
  cat ${WAITER_DIR}/log/waiter.log
  exit 1
fi
timeout 180s bash -c "wait_for_waiter 9092"
if [ $? -ne 0 ]; then
  echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening, displaying waiter log"
  cat ${WAITER_DIR}/log/waiter.log
  exit 1
fi

# Start nginx
WAITERS="${MINIMESOS_NETWORK_GATEWAY}:9091;${MINIMESOS_NETWORK_GATEWAY}:9092"
NGINX_PORT=9300
NGINX_DAEMON=on
${WAITER_DIR}/bin/run-nginx.sh ${WAITERS} ${NGINX_PORT} ${NGINX_DAEMON}

# Set WAITER_URI, which is used by the integration tests
export WAITER_URI=localhost:${NGINX_PORT}
curl -s ${WAITER_URI}/state | jq .routers

# Nginx should be round-robin load balancing, this should show different ports
curl -s ${WAITER_URI}/settings | jq .port
curl -s ${WAITER_URI}/settings | jq .port

# Run the integration tests
WAITER_TEST_KITCHEN_CMD=/opt/kitchen/container-run.sh lein with-profiles +test-console ${TEST_COMMAND} :${TEST_SELECTOR}
TESTS_EXIT_CODE=$?

# If there were failures, dump the logs
if [ ${TESTS_EXIT_CODE} -ne 0 ]; then
    echo "integration tests failed -- dumping logs"
    tail -n +1 -- log/*.log
fi

exit ${TESTS_EXIT_CODE}
