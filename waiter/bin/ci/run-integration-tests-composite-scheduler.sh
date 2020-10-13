#!/usr/bin/env bash
# Usage: run-integration-tests-composite-scheduler.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests-composite-scheduler.sh eftest integration-fast
#   run-integration-tests-composite-scheduler.sh eftest integration-slow
#   run-integration-tests-composite-scheduler.sh eftest
#   run-integration-tests-composite-scheduler.sh
#
# Runs the Waiter integration tests using the (local) composite scheduler.

set -e

TEST_COMMAND=${1:-eftest}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export WAITER_DIR=${DIR}/../..
TEST_APPS_DIR=${WAITER_DIR}/../containers/test-apps
COURIER_DIR=${TEST_APPS_DIR}/courier
KITCHEN_DIR=${TEST_APPS_DIR}/kitchen
NGINX_DIR=${TEST_APPS_DIR}/nginx
SEDIMENT_DIR=${TEST_APPS_DIR}/sediment

# prepare courier server build
pushd ${COURIER_DIR}
mvn clean package
popd

# prepare sediment server build
pushd ${SEDIMENT_DIR}
mvn clean package
popd

# start the JWKS server
JWKS_PORT=6666
${WAITER_DIR}/bin/ci/jwks-server-setup.sh ${JWKS_PORT}
export JWKS_SERVER_URL="http://127.0.0.1:${JWKS_PORT}/keys"
export OIDC_AUTHORIZE_URL="http://127.0.0.1:${JWKS_PORT}/authorize"
export OIDC_TOKEN_URL="http://127.0.0.1:${JWKS_PORT}/id-token"
export WAITER_TEST_JWT_ACCESS_TOKEN_URL="http://127.0.0.1:${JWKS_PORT}/get-token?host={HOST}"

# Start waiter
${WAITER_DIR}/bin/run-using-composite-scheduler.sh ${WAITER_PORT} &

# Run the integration tests
export WAITER_TEST_COURIER_CMD=${COURIER_DIR}/bin/run-courier-server.sh
export WAITER_TEST_KITCHEN_CMD=${KITCHEN_DIR}/bin/kitchen
export WAITER_TEST_NGINX_CMD=${NGINX_DIR}/bin/run-nginx-server.sh
export WAITER_TEST_SEDIMENT_CMD=${SEDIMENT_DIR}/bin/run-sediment-server.sh
${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}
