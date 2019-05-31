#!/usr/bin/env bash
# Usage: run-integration-tests-composite-scheduler.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests-composite-scheduler.sh parallel-test integration-fast
#   run-integration-tests-composite-scheduler.sh parallel-test integration-slow
#   run-integration-tests-composite-scheduler.sh parallel-test
#   run-integration-tests-composite-scheduler.sh
#
# Runs the Waiter integration tests using the (local) composite scheduler.

set -e

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
TEST_APPS_DIR=${WAITER_DIR}/../containers/test-apps
COURIER_DIR=${TEST_APPS_DIR}/courier
KITCHEN_DIR=${TEST_APPS_DIR}/kitchen
NGINX_DIR=${TEST_APPS_DIR}/nginx
SEDIMENT_DIR=${TEST_APPS_DIR}/sediment

# set SAML authenticator variables
export SAML_IDP_URI="https://localhost:8443/simplesaml/saml2/idp/SSOService.php"
export SAML_IDP_CERT_URI="${WAITER_DIR}/test-files/saml/idp.crt"
export SAML_AUTH_USER="user2"
if [[ $TEST_SELECTOR =~ fast$ ]]; then
    # Start SAML IdP test server
    ${DIR}/saml-idp-server-setup.sh
fi

# prepare courier server build
pushd ${COURIER_DIR}
mvn clean package
popd

# prepare sediment server build
pushd ${SEDIMENT_DIR}
mvn clean package
popd

# Start waiter
: ${WAITER_PORT:=9091}
${WAITER_DIR}/bin/run-using-composite-scheduler.sh ${WAITER_PORT} &

# Run the integration tests
export WAITER_TEST_COURIER_CMD=${COURIER_DIR}/bin/run-courier-server.sh
export WAITER_TEST_KITCHEN_CMD=${KITCHEN_DIR}/bin/kitchen
export WAITER_TEST_NGINX_CMD=${NGINX_DIR}/bin/run-nginx-server.sh
export WAITER_TEST_SEDIMENT_CMD=${SEDIMENT_DIR}/bin/run-sediment-server.sh
export WAITER_URI=127.0.0.1:${WAITER_PORT}
${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR}
