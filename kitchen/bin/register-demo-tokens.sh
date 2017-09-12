#!/usr/bin/env bash
# Usage: register-demo-tokens.sh
#
# Registers the demo tokens on Waiter.
# Checks if WAITER_URI and WAITER_TEST_KITCHEN_CMD are set, and sets them to reasonable defaults if not.

set -ev

if [ -z ${WAITER_URI+x} ]; then
    export WAITER_URI=127.0.0.1:9091
    echo "WAITER_URI is unset, defaulting to ${WAITER_URI}"
else
    echo "WAITER_URI is set to ${WAITER_URI}"
fi

if [ -z ${WAITER_TEST_KITCHEN_CMD+x} ]; then
    export WAITER_TEST_KITCHEN_CMD=/opt/kitchen/container-run.sh
    echo "WAITER_TEST_KITCHEN_CMD is unset, defaulting to ${WAITER_TEST_KITCHEN_CMD}"
else
    echo "WAITER_TEST_KITCHEN_CMD is set to ${WAITER_TEST_KITCHEN_CMD}"
fi


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/..

RUN_AS_USER="$(id -un)"

echo "Registering image-tagging token"
temp_file=$(mktemp)
sed -i -- 's/COMMAND/${WAITER_TEST_KITCHEN_CMD}/g' *
rm ${temp_file}