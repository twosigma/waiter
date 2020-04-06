#!/usr/bin/env bash
# Usage: run-integration-tests.sh
#
# Runs the Waiter CLI integration tests using the (local) shell scheduler.

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CLI_DIR=${DIR}/../..
ROOT_DIR=${CLI_DIR}/..

export GRAPHITE_SERVER_PORT=5555
# Start netcat to listen to a port. The Codahale Graphite reporter will be able to report without failing and spamming logs.
nc -kl localhost ${GRAPHITE_SERVER_PORT} > /dev/null &

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

if [[ -z ${WAITER_URI+x} ]]; then
    export WAITER_URI=127.0.0.1:9091
    echo "WAITER_URI is unset, defaulting to ${WAITER_URI}"
else
    echo "WAITER_URI is set to ${WAITER_URI}"
fi

# Start first waiter
: ${WAITER_PORT:=9091}
${ROOT_DIR}/waiter/bin/run-using-shell-scheduler.sh ${WAITER_PORT} waiter1 &

# Wait for waiter to be listening
timeout 180s bash -c "wait_for_waiter ${WAITER_URI}"
if [[ $? -ne 0 ]]; then
  echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening"
  exit 1
fi
curl -s ${WAITER_URI}/state

if [[ -z ${WAITER_URI_2+x} ]]; then
    export WAITER_URI_2=127.0.0.1:9191
    echo "WAITER_URI_2 is unset, defaulting to ${WAITER_URI_2}"
else
    echo "WAITER_URI_2 is set to ${WAITER_URI_2}"
fi

# Start a second waiter, no need to recompile Waiter code
: ${WAITER_PORT_2:=9191}
${ROOT_DIR}/waiter/bin/run-using-shell-scheduler.sh ${WAITER_PORT_2} waiter2 0 &

# Wait for second waiter to be listening
timeout 180s bash -c "wait_for_waiter ${WAITER_URI_2}"
if [[ $? -ne 0 ]]; then
  echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening"
  exit 1
fi
curl -s ${WAITER_URI_2}/state

# Run the integration tests
export WAITER_URI=127.0.0.1:${WAITER_PORT}
export WAITER_CLI_TEST_DEFAULT_CMD="${ROOT_DIR}/containers/test-apps/kitchen/bin/kitchen --port \${PORT0}"
export WAITER_TEST_MULTI_CLUSTER=true
cd ${CLI_DIR}/integration
pytest -m "not serial"
pytest -m "serial" -n0
