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

# Start waiter
: ${WAITER_PORT:=9091}
${ROOT_DIR}/waiter/bin/run-using-shell-scheduler.sh ${WAITER_PORT} &

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


# Wait for waiter to be listening
timeout 180s bash -c "wait_for_waiter ${WAITER_URI}"
if [[ $? -ne 0 ]]; then
  echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening"
  exit 1
fi
curl -s ${WAITER_URI}/state | jq .routers
curl -s ${WAITER_URI}/settings | jq .port

# Run the integration tests
export WAITER_URI=127.0.0.1:${WAITER_PORT}
export WAITER_CLI_TEST_DEFAULT_CMD="${ROOT_DIR}/kitchen/bin/kitchen --port \${PORT0}"
cd ${CLI_DIR}/integration
pytest
