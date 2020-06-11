#!/usr/bin/env bash
# Usage: run-integration-tests.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests.sh eftest integration-fast
#   run-integration-tests.sh eftest integration-slow
#   run-integration-tests.sh eftest
#   run-integration-tests.sh
#
# Runs the Waiter integration tests.

set -e

: ${WAITER_PORT:=9091}
TEST_COMMAND=${1:-eftest}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
TEST_APPS_DIR=${WAITER_DIR}/../containers/test-apps

# Build mesos agent container with Kitchen packed in
${TEST_APPS_DIR}/bin/build-docker-image.sh

if [ -n "$CONTINUOUS_INTEGRATION" ]; then
    # Start minimesos
    export MINIMESOS_CMD=${DIR}/minimesos
    pushd ${WAITER_DIR}
    ${MINIMESOS_CMD} up
    popd

    # start the JWKS server
    JWKS_PORT=6666
    ${WAITER_DIR}/bin/ci/jwks-server-setup.sh ${JWKS_PORT}
    export JWKS_SERVER_URL="http://127.0.0.1:${JWKS_PORT}/keys"
    export OIDC_AUTHORIZE_URL="http://127.0.0.1:${JWKS_PORT}/authorize"
    export OIDC_TOKEN_URL="http://127.0.0.1:${JWKS_PORT}/id-token"
    export WAITER_TEST_JWT_ACCESS_TOKEN_URL="http://127.0.0.1:${JWKS_PORT}/get-token?host={HOST}"

    # Start waiter
    ${WAITER_DIR}/bin/run-using-minimesos.sh ${WAITER_PORT} &
fi

# Run the integration tests
export WAITER_TEST_COURIER_CMD=/opt/courier/bin/run-courier-server.sh
export WAITER_TEST_KITCHEN_CMD=/opt/kitchen/kitchen
export WAITER_TEST_NGINX_CMD=/opt/nginx/bin/run-nginx-server.sh
export WAITER_TEST_SEDIMENT_CMD=/opt/sediment/bin/run-sediment-server.sh
export WAITER_URI=127.0.0.1:${WAITER_PORT}
${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}
