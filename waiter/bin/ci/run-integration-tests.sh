#!/usr/bin/env bash
# Usage: run-integration-tests.sh
#
# Runs the Waiter integration tests, and dumps log files if the tests fail.

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
${WAITER_DIR}/bin/run-using-minimesos.sh 9091 &

# Wait for waiter to be listening
timeout 180s bash -c "wait_for_waiter 9091"
if [ $? -ne 0 ]; then
  echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening, displaying waiter log"
  cat ${WAITER_DIR}/log/waiter.log
  exit 1
fi

# Run the integration tests
WAITER_TEST_KITCHEN_CMD=/opt/kitchen/container-run.sh lein with-profiles +test-repl parallel-test :integration
TESTS_EXIT_CODE=$?

# If there were failures, dump the logs
if [ ${TESTS_EXIT_CODE} -ne 0 ]; then
    echo "integration tests failed -- dumping logs"
    tail -n +1 -- log/*.log
fi

exit ${TESTS_EXIT_CODE}
