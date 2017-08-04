#!/usr/bin/env bash
# Usage: test.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   test.sh test integration
#   test.sh test :only token-syncer.basic-test
#   test.sh test
#   test.sh
#
# Waits for waiter servers to be listening and then runs the token-syncer tests using the provided test selector.

set -v

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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SYNCER_DIR=${DIR}/..
WAITER_DIR=${DIR}/../../waiter

TEST_COMMAND=${1:-test}
TEST_SELECTOR=${2:-integration}

export WAITER_URIS=${WAITER_URIS:-127.0.0.1:9091;127.0.0.1:9092}
export WAITER_USERNAME=testrunner
export WAITER_PASSWORD=opensesame

# Launch the token syncer
cd ${SYNCER_DIR}
lein do clean, compile, run config-full.edn --port 9100 &
export SYNCER_URI=127.0.0.1:9100

# Wait for waiter to be listening
for WAITER_URI in ${WAITER_URIS//;/ }
do
  timeout 180s bash -c "wait_for_server ${WAITER_URI}"
  if [ $? -ne 0 ]; then
    echo "$(date +%H:%M:%S) timed out waiting for waiter to start listening on ${WAITER_URI}, displaying waiter log"
    cat ${WAITER_DIR}/log/*waiter.log
    exit 1
  fi
done

timeout 180s bash -c "wait_for_server ${SYNCER_URI}"
if [ $? -ne 0 ]; then
  echo "$(date +%H:%M:%S) timed out waiting for token-syncer to start listening on ${SYNCER_URI}, displaying syncer log"
  cat ${SYNCER_DIR}/log/*token-syncer.log
  exit 1
fi

# Run the integration tests
cd ${SYNCER_DIR}
lein with-profiles +test-log ${TEST_COMMAND} :${TEST_SELECTOR} ${@:3}
