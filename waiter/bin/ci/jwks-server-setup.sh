#!/bin/bash
# Usage: jwks-server-setup.sh [JWKS_PORT]
#
# Examples:
#   jwks-server-setup.sh 9040
#
# Run a  JSON Web Key Set (JWKS) server that returns a fixed set of keys.
# JWKS retrieval request can be routed to: http://localhost:JWKS_PORT/keys
# When the JWKS_PORT is not specified, a default of 8040 is used.

set -e

JWKS_PORT=${1:-8040}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
JWT_DIR=${WAITER_DIR}/../containers/test-apps/jwks-server

JWT_SUBJECT="$(id -un)@localtest.me"
echo "Configuring JWT subject to ${JWT_SUBJECT} from JWKS server"
export JWT_SUBJECT="${JWT_SUBJECT}"

echo "Starting JWKS server on port ${JWKS_PORT}"
( pushd ${JWT_DIR} && lein run ${JWKS_PORT} resources/jwks.json resources/settings.edn && popd ) &

echo "Waiting for JWKS server..."
while ! curl -k http://127.0.0.1:${JWKS_PORT}/keys &>/dev/null; do
    echo -n .
    sleep 3
done
echo
echo "JWKS server started successfully"
